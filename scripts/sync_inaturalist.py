"""
Sincroniza observações do iNaturalist para espécies com inaturalist_taxon_id cadastrado.

- Processa espécies agrupadas pelo campo `bem` (BEM1, BEM2, ..., P1, P2)
- Checkpoint por run no Redis (bem:sync:inaturalist:done:{data}) — retoma de onde parou no mesmo dia
- Lock distribuído (bem:sync:inaturalist:lock) — evita execução paralela
- Pausa de INAT_GROUP_PAUSE_SECONDS entre grupos
- Para se grupo inteiro falhar
- Full sync por padrão
- UPSERT via INSERT ... ON CONFLICT DO UPDATE
- Reconcilia observações removendo, no final, o que não veio mais da API
- Kill switch por MAX_RUNTIME_SECONDS
"""

import os
import re
import sys
import time
from datetime import UTC, datetime, timezone
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

INAT_API_URL = os.getenv("INATURALIST_API_URL", "https://api.inaturalist.org/v1")
INAT_API_KEY = os.getenv("INATURALIST_API_KEY")
PER_PAGE = 200
REQUEST_TIMEOUT = 30
MAX_RETRIES = 5
MAX_RUNTIME_SECONDS = int(os.getenv("INAT_MAX_RUNTIME_SECONDS", "14100"))
GROUP_PAUSE_SECONDS = int(os.getenv("INAT_GROUP_PAUSE_SECONDS", "180"))
REDIS_URL = os.getenv("REDIS_URL", "").strip()
CHECKPOINT_TTL = 60 * 60 * 25  # 25 horas
LOCK_KEY = "bem:sync:inaturalist:lock"
LOCK_TTL = MAX_RUNTIME_SECONDS + 300

app = create_app()


def _log(msg):
    print(msg, flush=True)


# ---------------------------------------------------------------------------
# Redis checkpoint helpers
# ---------------------------------------------------------------------------

def _get_redis():
    if not REDIS_URL:
        return None
    try:
        import redis
        client = redis.from_url(REDIS_URL, decode_responses=True, socket_timeout=3)
        client.ping()
        return client
    except Exception as exc:
        _log(f"[AVISO] Redis indisponível — checkpoint desativado: {exc}")
        return None


def _acquire_lock(r):
    if not r:
        return True
    return bool(r.set(LOCK_KEY, "1", nx=True, ex=LOCK_TTL))


def _release_lock(r):
    if r:
        r.delete(LOCK_KEY)


def _done_key(run_date):
    return f"bem:sync:inaturalist:done:{run_date}"


def _failed_key(run_date):
    return f"bem:sync:inaturalist:failed:{run_date}"


def _is_done(r, run_date, species_id):
    if not r:
        return False
    try:
        return bool(r.sismember(_done_key(run_date), str(species_id)))
    except Exception:
        return False


def _mark_done(r, run_date, species_id):
    if not r:
        return
    try:
        key = _done_key(run_date)
        r.sadd(key, str(species_id))
        r.expire(key, CHECKPOINT_TTL)
    except Exception:
        pass


def _mark_failed(r, run_date, species_id):
    if not r:
        return
    try:
        key = _failed_key(run_date)
        r.sadd(key, str(species_id))
        r.expire(key, CHECKPOINT_TTL)
    except Exception:
        pass


# ---------------------------------------------------------------------------
# Group ordering: BEM1 < BEM2 < ... < BEM10 < P1 < P2
# ---------------------------------------------------------------------------

def _group_sort_key(name):
    m = re.match(r"^([A-Za-z]+)(\d+)$", name or "")
    if m:
        return (m.group(1), int(m.group(2)))
    return (name or "", 0)


# ---------------------------------------------------------------------------
# iNaturalist API helpers
# ---------------------------------------------------------------------------

def _parse_location(location):
    if not location:
        return None, None
    try:
        lat, lng = location.split(",", 1)
        return float(lat), float(lng)
    except Exception:
        return None, None


def _fetch_page(taxon_id, id_above):
    params = {
        "taxon_id": taxon_id,
        "place_id": 6878,  # Brasil
        "has[]": "geo",
        "per_page": PER_PAGE,
        "order": "asc",
        "order_by": "id",
    }
    if id_above:
        params["id_above"] = id_above
    headers = {"Authorization": f"Bearer {INAT_API_KEY}"} if INAT_API_KEY else {}

    for attempt in range(MAX_RETRIES):
        r = requests.get(
            f"{INAT_API_URL}/observations",
            params=params,
            headers=headers,
            timeout=REQUEST_TIMEOUT,
        )
        if r.status_code == 429:
            wait = 2 ** attempt * 10
            _log(f"  429 do iNaturalist — aguardando {wait}s (tentativa {attempt + 1}/{MAX_RETRIES})")
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
            source="inaturalist",
        )
    }
    stale_ids = current_ids - seen_external_ids
    if not stale_ids:
        return 0

    deleted = 0
    stale_ids = list(stale_ids)
    for i in range(0, len(stale_ids), 1000):
        chunk = stale_ids[i : i + 1000]
        deleted += (
            db.session.query(Observation)
            .filter(
                Observation.species_id == species_id,
                Observation.source == "inaturalist",
                Observation.external_id.in_(chunk),
            )
            .delete(synchronize_session=False)
        )
    db.session.commit()
    return deleted


def _sync_species(species_id, taxon_id, start_time):
    with app.app_context():
        _log(f"  [{species_id}] taxon={taxon_id}")

        seen_external_ids = set()
        id_above = None
        total = 0
        page = 0

        while True:
            if time.time() - start_time > MAX_RUNTIME_SECONDS:
                _log(f"  [{species_id}] Kill switch — abortando")
                return total, 0, False

            page += 1
            data = _fetch_page(taxon_id, id_above)
            results = data.get("results", [])
            if not results:
                break

            rows = []
            for obs in results:
                lat, lng = _parse_location(obs.get("location"))
                if lat is None:
                    continue
                photos = obs.get("photos") or []
                raw_url = photos[0].get("url") if photos else None
                rows.append({
                    "species_id": species_id,
                    "source": "inaturalist",
                    "external_id": str(obs["id"]),
                    "latitude": lat,
                    "longitude": lng,
                    "location_obscured": bool(obs.get("obscured", False)),
                    "observed_on": obs.get("observed_on"),
                    "quality_grade": obs.get("quality_grade"),
                    "photo_url": raw_url.replace("/square.", "/medium.") if raw_url else None,
                    "url": obs.get("uri"),
                })
                seen_external_ids.add(str(obs["id"]))

            total += _upsert(rows)
            _log(f"  [{species_id}] página {page}: +{len(rows)}")

            if len(results) < PER_PAGE:
                break
            id_above = results[-1]["id"]

        deleted = _delete_stale_observations(species_id, seen_external_ids)

        species = db.session.get(Species, species_id)
        if species:
            species.last_inaturalist_sync_at = datetime.now(UTC)
            db.session.commit()

        prefix = app.config.get("OBSERVATIONS_CACHE_PREFIX", "observations")
        CacheService.delete(f"{prefix}:{species_id}:all")
        CacheService.delete(f"{prefix}:{species_id}:inaturalist")

        db.session.remove()
        return total, deleted, True


def _process_group(group_name, species_rows, run_date, r, start_time):
    """Processa um grupo de espécies sequencialmente.

    Retorna True se o job deve ser interrompido (grupo inteiro falhou).
    """
    total = 0
    total_deleted = 0
    attempted = 0
    failed = 0

    for row in species_rows:
        if _is_done(r, run_date, row.id):
            _log(f"  [{row.id}] checkpoint — já processado hoje, pulando")
            continue

        if time.time() - start_time > MAX_RUNTIME_SECONDS:
            _log(f"  [{row.id}] Kill switch — abortando")
            break

        attempted += 1

        try:
            inserted, deleted, completed = _sync_species(row.id, row.inaturalist_taxon_id, start_time)
            total += inserted
            total_deleted += deleted
            _mark_done(r, run_date, row.id)
            _log(f"[OK] {group_name} species={row.id} upsert={inserted} removidos={deleted}")
        except Exception as exc:
            failed += 1
            _log(f"[ERRO] {group_name} species={row.id}: {exc}")
            _mark_failed(r, run_date, row.id)

    _log(
        f"Grupo {group_name}: upsert={total} removidos={total_deleted} "
        f"falhas={failed}/{attempted}"
    )

    return attempted > 0 and failed == attempted


def main():
    start_time = time.time()
    run_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    raw_bem_ids = os.getenv("BEM_ID", "")
    bem_ids = [int(v) for raw in raw_bem_ids.split(",") if (v := raw.strip()).isdigit()]

    _log("=== Sync iNaturalist ===")
    _log(
        f"limite={MAX_RUNTIME_SECONDS}s | pausa_entre_grupos={GROUP_PAUSE_SECONDS}s"
    )

    r = _get_redis()

    if not _acquire_lock(r):
        _log("[ABORT] Outra execução já está em andamento (lock Redis ativo)")
        sys.exit(1)

    try:
        with app.app_context():
            query = db.session.query(
                Species.id, Species.inaturalist_taxon_id, Species.bem
            ).filter(Species.inaturalist_taxon_id.isnot(None))
            if bem_ids:
                query = query.filter(Species.id.in_(bem_ids))
            species_rows = query.all()

        _log(f"Espécies: {len(species_rows)}")

        if bem_ids:
            _log("Modo manual — sem agrupamento por bem")
            _process_group("manual", species_rows, run_date, r, start_time)
        else:
            groups: dict[str, list] = {}
            for row in species_rows:
                group = row.bem or "sem_grupo"
                groups.setdefault(group, []).append(row)

            sorted_group_names = sorted(groups.keys(), key=_group_sort_key)
            _log(f"Grupos: {sorted_group_names}")

            for i, group_name in enumerate(sorted_group_names):
                if time.time() - start_time > MAX_RUNTIME_SECONDS:
                    _log("[KILL SWITCH] Tempo máximo atingido")
                    break

                group_species = groups[group_name]
                _log(f"\n=== Grupo {group_name} ({len(group_species)} espécies) ===")

                should_stop = _process_group(group_name, group_species, run_date, r, start_time)

                if should_stop:
                    _log(f"[STOP] Grupo {group_name} falhou completamente — encerrando sync")
                    break

                if i < len(sorted_group_names) - 1:
                    _log(f"Pausa de {GROUP_PAUSE_SECONDS}s antes do próximo grupo...")
                    time.sleep(GROUP_PAUSE_SECONDS)

    finally:
        _release_lock(r)

    _log(f"Tempo total: {int(time.time() - start_time)}s")


if __name__ == "__main__":
    main()
