"""
Sincroniza observações do Mushroom Observer para espécies com scientific_name cadastrado.

- Full sync por padrão
- UPSERT via INSERT ... ON CONFLICT DO UPDATE
- Reconcilia observações removendo, no final, o que não veio mais da API
- ThreadPoolExecutor com MAX_WORKERS threads
- Kill switch por MAX_RUNTIME_SECONDS
"""

import os
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import requests
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.sql import func

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from app import create_app  # noqa: E402
from app.extensions import db  # noqa: E402
from app.models.observation import Observation  # noqa: E402
from app.models.species import Species  # noqa: E402
from app.services.cache_service import CacheService  # noqa: E402

MO_API_URL = os.getenv("MUSHROOM_OBSERVER_API_URL", "https://mushroomobserver.org/api2")
REQUEST_TIMEOUT = 30
SLEEP_AFTER_REQUEST = float(os.getenv("MO_SLEEP_AFTER_REQUEST", "12"))
MAX_WORKERS = int(os.getenv("MO_MAX_WORKERS", "1"))
MAX_RUNTIME_SECONDS = int("3540")
RETRYABLE_STATUS_CODES = {429, 500, 502, 503, 504}
BLOCK_STATUS_CODES = {403}
HEADERS = {"User-Agent": "BEM-api/1.0 (bem.uneb.br; contact: bem.g2bc@gmail.com)"}

app = create_app()


def _log(msg):
    print(msg, flush=True)


def _center_of_bbox(loc):
    try:
        lat = (float(loc["latitude_north"]) + float(loc["latitude_south"])) / 2
        lng = (float(loc["longitude_east"]) + float(loc["longitude_west"])) / 2
        return round(lat, 6), round(lng, 6)
    except (KeyError, TypeError, ValueError):
        return None, None


def _parse_coords(obs):
    lat, lng = obs.get("latitude"), obs.get("longitude")
    if lat is not None and lng is not None:
        try:
            return float(lat), float(lng)
        except (TypeError, ValueError):
            pass
    loc = obs.get("location")
    if isinstance(loc, dict):
        return _center_of_bbox(loc)
    return None, None


def _photo_url(obs):
    primary = obs.get("primary_image")
    if not primary:
        return None
    url = primary.get("original_url", "")
    return url.replace("/orig/", "/640/") if url else None


BRAZIL_BBOX = {"north": 5.272, "south": -33.750, "east": -34.793, "west": -73.983}


def _fetch_page(name_id, scientific_name, page):
    if name_id:
        params = {"format": "json", "detail": "high", "name_id": name_id, "page": page}
    else:
        params = {"format": "json", "detail": "high", "name": scientific_name, "page": page}
    params.update(BRAZIL_BBOX)

    try:
        r = requests.get(
            f"{MO_API_URL}/observations",
            params=params,
            headers=HEADERS,
            timeout=REQUEST_TIMEOUT,
        )
        if r.status_code in BLOCK_STATUS_CODES:
            raise RuntimeError(f"Possível bloqueio do Mushroom Observer: HTTP {r.status_code}")
        if r.status_code in RETRYABLE_STATUS_CODES:
            raise RuntimeError(
                f"Mushroom Observer indisponível ou limitando requests: HTTP {r.status_code}"
            )
        r.raise_for_status()
        return r.json()
    finally:
        time.sleep(SLEEP_AFTER_REQUEST)


def _upsert(rows):
    if not rows:
        return 0
    stmt = pg_insert(Observation).values(rows)
    stmt = stmt.on_conflict_do_update(
        constraint="uq_observation_source_external_id",
        set_={
            "species_id": stmt.excluded.species_id,
            "latitude": stmt.excluded.latitude,
            "longitude": stmt.excluded.longitude,
            "location_obscured": stmt.excluded.location_obscured,
            "observed_on": stmt.excluded.observed_on,
            "quality_grade": stmt.excluded.quality_grade,
            "photo_url": stmt.excluded.photo_url,
            "url": stmt.excluded.url,
            "updated_at": func.now(),
        },
    )
    db.session.execute(stmt)
    db.session.commit()
    return len(rows)


def _delete_stale_observations(species_id, seen_external_ids):
    current_ids = {
        external_id
        for (external_id,) in db.session.query(Observation.external_id).filter_by(
            species_id=species_id,
            source="mushroom_observer",
        )
    }
    stale_ids = current_ids - seen_external_ids
    if not stale_ids:
        return 0

    deleted = 0
    stale_ids = list(stale_ids)
    for i in range(0, len(stale_ids), 1000):
        chunk = stale_ids[i:i + 1000]
        deleted += db.session.query(Observation).filter(
            Observation.species_id == species_id,
            Observation.source == "mushroom_observer",
            Observation.external_id.in_(chunk),
        ).delete(synchronize_session=False)
    db.session.commit()
    return deleted


def _sync_species(species_id, scientific_name, mo_name_id, start_time):
    with app.app_context():
        seen_external_ids = set()
        discovered_name_id = None
        total = 0
        page = 1

        while True:
            if time.time() - start_time > MAX_RUNTIME_SECONDS:
                _log(f"  [{species_id}] Kill switch — abortando")
                return total, 0, False

            data = _fetch_page(mo_name_id, scientific_name, page)
            if data.get("errors"):
                raise RuntimeError(f"Erro da API MO: {data['errors']}")

            results = data.get("results") or []
            if not results:
                break

            rows_by_external_id = {}
            for obs in results:
                # salva o name_id do primeiro resultado se ainda não temos
                if discovered_name_id is None and not mo_name_id:
                    discovered_name_id = (obs.get("consensus") or {}).get("id")

                lat, lng = _parse_coords(obs)
                if lat is None:
                    continue
                has_precise = obs.get("latitude") is not None
                obs_id = str(obs["id"])
                rows_by_external_id[obs_id] = {
                    "species_id": species_id,
                    "source": "mushroom_observer",
                    "external_id": obs_id,
                    "latitude": lat,
                    "longitude": lng,
                    "location_obscured": bool(obs.get("gps_hidden", False)) or not has_precise,
                    "observed_on": obs.get("date") or None,
                    "quality_grade": None,
                    "photo_url": _photo_url(obs),
                    "url": f"https://mushroomobserver.org/observations/{obs_id}",
                }
                seen_external_ids.add(obs_id)

            rows = list(rows_by_external_id.values())
            total += _upsert(rows)

            total_pages = data.get("number_of_pages", 1)
            _log(
                f"  [{species_id}] página {page}/{total_pages}: {len(results)} obs, "
                f"{len(rows)} válidas (via {'id' if mo_name_id else 'name'})"
            )

            if page >= total_pages:
                break
            page += 1

        # persiste o name_id descoberto para os próximos syncs
        if discovered_name_id:
            species = db.session.get(Species, species_id)
            if species and not species.mushroom_observer_name_id:
                species.mushroom_observer_name_id = discovered_name_id

        db.session.commit()
        deleted = _delete_stale_observations(species_id, seen_external_ids)

        prefix = app.config.get("OBSERVATIONS_CACHE_PREFIX", "observations")
        CacheService.delete(f"{prefix}:{species_id}:all")
        CacheService.delete(f"{prefix}:{species_id}:mushroom_observer")

        db.session.remove()

        return total, deleted, True


def main():
    start_time = time.time()

    raw_bem_ids = os.getenv("BEM_ID", "")
    bem_ids = [int(v) for raw in raw_bem_ids.split(",") if (v := raw.strip()).isdigit()]

    _log("=== Sync Mushroom Observer ===")
    _log(
        f"workers={MAX_WORKERS} | sleep_request={SLEEP_AFTER_REQUEST}s | "
        f"limite={MAX_RUNTIME_SECONDS}s"
    )

    with app.app_context():
        query = db.session.query(
            Species.id, Species.scientific_name, Species.mushroom_observer_name_id
        ).filter(Species.scientific_name.isnot(None))
        if bem_ids:
            query = query.filter(Species.id.in_(bem_ids))
        species_rows = query.all()

    _log(f"Espécies: {len(species_rows)}")

    total = 0
    total_deleted = 0
    errors = 0
    blocked = False

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
        futures = {
            pool.submit(
                _sync_species,
                row.id,
                row.scientific_name,
                row.mushroom_observer_name_id,
                start_time,
            ): row.id
            for row in species_rows
        }
        for future in as_completed(futures):
            species_id = futures[future]
            try:
                inserted, deleted, completed = future.result()
                total += inserted
                total_deleted += deleted
                _log(f"[OK] species={species_id} upsert={inserted} removidos={deleted}")
                if not completed:
                    pool.shutdown(wait=False, cancel_futures=True)
                    break
            except Exception as e:
                errors += 1
                _log(f"[ERRO] species={species_id}: {e}")
                if (
                    "Possível bloqueio" in str(e)
                    or "limitando requests" in str(e)
                    or "indisponível" in str(e)
                ):
                    blocked = True
                    pool.shutdown(wait=False, cancel_futures=True)
                    break

    _log(
        f"Total upsert: {total} | Removidos: {total_deleted} | "
        f"Erros: {errors} | Tempo: {int(time.time() - start_time)}s"
    )
    if blocked:
        raise SystemExit(2)


if __name__ == "__main__":
    main()
