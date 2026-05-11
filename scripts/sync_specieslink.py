"""
Sincroniza ocorrências do speciesLink para espécies com scientific_name cadastrado.

- Full sync por padrão
- UPSERT via INSERT ... ON CONFLICT DO UPDATE
- Reconcilia observações removendo, no final, o que não veio mais da API
- Paginação via offset/limit
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

SPECIESLINK_API_URL = os.getenv(
    "SPECIESLINK_API_URL",
    "https://specieslink.net/ws/1.0/search",
)
SPECIESLINK_API_KEY = os.getenv("SPECIESLINK_API_KEY", "")
PAGE_SIZE = 500
REQUEST_TIMEOUT = 30
SLEEP_BETWEEN_PAGES = 3.0
MAX_WORKERS = 2
MAX_RUNTIME_SECONDS = int("3540")
MAX_RETRIES = 3
HEADERS = {"User-Agent": "BEM-api/1.0 (bem.uneb.br; contact: bem.g2bc@gmail.com)"}

app = create_app()


def _log(msg):
    print(msg, flush=True)


def _build_date(props):
    year = props.get("yearcollected")
    month = props.get("monthcollected")
    day = props.get("daycollected")
    if not year:
        return None
    parts = [year.zfill(4)]
    if month:
        parts.append(month.zfill(2))
        if day:
            parts.append(day.zfill(2))
    return "-".join(parts) if len(parts) == 3 else None


def _external_id(props):
    collection_id = props.get("collectionid")
    catalog_number = props.get("catalognumber")
    barcode = props.get("barcode")

    if collection_id and catalog_number:
        return f"collection:{collection_id}:catalog:{catalog_number}"
    if barcode:
        return f"barcode:{barcode}"
    if catalog_number:
        return f"catalog:{catalog_number}"
    return None


def _fetch_page(scientific_name, offset):
    params = {
        "apikey": SPECIESLINK_API_KEY,
        "scientificName": scientific_name,
        "country": "Brazil",
        "offset": offset,
        "limit": PAGE_SIZE,
    }
    for attempt in range(MAX_RETRIES):
        r = requests.get(
            SPECIESLINK_API_URL,
            params=params,
            headers=HEADERS,
            timeout=REQUEST_TIMEOUT,
        )
        if r.status_code == 429:
            wait = 2 ** attempt * 10
            _log(f"  429 recebido — aguardando {wait}s (tentativa {attempt + 1}/{MAX_RETRIES})")
            time.sleep(wait)
            continue
        r.raise_for_status()
        return r.json()

    raise RuntimeError("Máximo de tentativas atingido (429)")


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
            source="specieslink",
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
            Observation.source == "specieslink",
            Observation.external_id.in_(chunk),
        ).delete(synchronize_session=False)
    db.session.commit()
    return deleted


def _sync_species(species_id, scientific_name, start_time):
    with app.app_context():
        seen_external_ids = set()
        total = 0
        offset = 0
        page = 0

        while True:
            if time.time() - start_time > MAX_RUNTIME_SECONDS:
                _log(f"  [{species_id}] Kill switch — abortando")
                return total, 0, False

            data = _fetch_page(scientific_name, offset)
            features = data.get("features") or []
            number_matched = data.get("numberMatched", 0)
            rows_by_external_id = {}

            for feature in features:
                geom = feature.get("geometry") or {}
                coords = geom.get("coordinates")
                if not coords or len(coords) < 2:
                    continue

                lng, lat = coords[0], coords[1]
                props = feature.get("properties") or {}
                external_id = _external_id(props)
                if not external_id:
                    continue

                collection_id = props.get("collectionid")
                catalog_number = props.get("catalognumber")
                if collection_id and catalog_number:
                    url = f"https://specieslink.net/search/records/collectioncode/{collection_id}/catalognumber/{catalog_number}"
                else:
                    url = None

                rows_by_external_id[external_id] = {
                    "species_id": species_id,
                    "source": "specieslink",
                    "external_id": external_id,
                    "latitude": float(lat),
                    "longitude": float(lng),
                    "location_obscured": False,
                    "observed_on": _build_date(props),
                    "quality_grade": props.get("basisofrecord"),
                    "photo_url": None,
                    "url": url,
                }
                seen_external_ids.add(external_id)

            rows = list(rows_by_external_id.values())
            total += _upsert(rows)

            page += 1
            _log(
                f"  [{species_id}] página {page}: {len(features)} features, "
                f"{len(rows)} válidas (total matched: {number_matched})"
            )

            offset += PAGE_SIZE
            if offset >= number_matched or not features:
                break

            time.sleep(SLEEP_BETWEEN_PAGES)

        deleted = _delete_stale_observations(species_id, seen_external_ids)

        prefix = app.config.get("OBSERVATIONS_CACHE_PREFIX", "observations")
        CacheService.delete(f"{prefix}:{species_id}:all")
        CacheService.delete(f"{prefix}:{species_id}:specieslink")

        db.session.remove()
        return total, deleted, True


def main():
    start_time = time.time()

    raw_bem_ids = os.getenv("BEM_ID", "")
    bem_ids = [int(v) for raw in raw_bem_ids.split(",") if (v := raw.strip()).isdigit()]

    _log("=== Sync speciesLink ===")
    _log(f"workers={MAX_WORKERS} | limite={MAX_RUNTIME_SECONDS}s")

    with app.app_context():
        query = db.session.query(Species.id, Species.scientific_name).filter(
            Species.scientific_name.isnot(None)
        )
        if bem_ids:
            query = query.filter(Species.id.in_(bem_ids))
        species_rows = query.all()

    _log(f"Espécies: {len(species_rows)}")

    total = 0
    total_deleted = 0
    errors = 0

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
        futures = {
            pool.submit(_sync_species, row.id, row.scientific_name, start_time): row.id
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

    _log(
        f"Total upsert: {total} | Removidos: {total_deleted} | "
        f"Erros: {errors} | Tempo: {int(time.time() - start_time)}s"
    )


if __name__ == "__main__":
    main()
