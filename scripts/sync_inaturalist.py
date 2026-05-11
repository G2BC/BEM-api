"""
Sincroniza observações do iNaturalist para espécies com inaturalist_taxon_id cadastrado.

- Full sync por padrão
- UPSERT via INSERT ... ON CONFLICT DO UPDATE
- Reconcilia observações removendo, no final, o que não veio mais da API
- Kill switch por INAT_MAX_RUNTIME_SECONDS
- Grupos por campo `bem` com checkpoint Redis (via sync_base.SyncRunner)
"""

import os
import sys
import time
from datetime import UTC, datetime
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
from scripts.sync_base import SyncRunner  # noqa: E402

INAT_API_URL = os.getenv("INATURALIST_API_URL", "https://api.inaturalist.org/v1")
INAT_API_KEY = os.getenv("INATURALIST_API_KEY")
PER_PAGE = 200
REQUEST_TIMEOUT = 30
MAX_RETRIES = 5


def _log(msg):
    print(msg, flush=True)


def _parse_location(location):
    if not location:
        return None, None
    try:
        lat, lng = location.split(",", 1)
        return float(lat), float(lng)
    except Exception:
        return None, None


def _fetch_page(taxon_id, id_above, max_runtime, start_time):
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
        r = requests.get(f"{INAT_API_URL}/observations", params=params, headers=headers, timeout=REQUEST_TIMEOUT)
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


def _delete_stale(species_id, seen_external_ids):
    current_ids = {
        eid for (eid,) in db.session.query(Observation.external_id).filter_by(
            species_id=species_id, source="inaturalist"
        )
    }
    stale = list(current_ids - seen_external_ids)
    if not stale:
        return 0
    deleted = 0
    for i in range(0, len(stale), 1000):
        chunk = stale[i : i + 1000]
        deleted += db.session.query(Observation).filter(
            Observation.species_id == species_id,
            Observation.source == "inaturalist",
            Observation.external_id.in_(chunk),
        ).delete(synchronize_session=False)
    db.session.commit()
    return deleted


# ---------------------------------------------------------------------------
# SyncRunner subclass
# ---------------------------------------------------------------------------

class InatSyncRunner(SyncRunner):
    source_name = "inaturalist"
    env_prefix = "INAT"

    def get_species_rows(self, session, bem_ids):
        q = session.query(Species.id, Species.inaturalist_taxon_id, Species.bem).filter(
            Species.inaturalist_taxon_id.isnot(None)
        )
        if bem_ids:
            q = q.filter(Species.id.in_(bem_ids))
        return q.all()

    def sync_species(self, row, start_time):
        species_id = row.id
        taxon_id = row.inaturalist_taxon_id
        max_runtime = self.max_runtime

        _log(f"  [{species_id}] taxon={taxon_id}")

        seen_external_ids = set()
        id_above = None
        total = 0
        page = 0

        while True:
            if time.time() - start_time > max_runtime:
                _log(f"  [{species_id}] Kill switch — abortando")
                return total, 0, False

            page += 1
            data = _fetch_page(taxon_id, id_above, max_runtime, start_time)
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

        deleted = _delete_stale(species_id, seen_external_ids)

        species = db.session.get(Species, species_id)
        if species:
            species.last_inaturalist_sync_at = datetime.now(UTC)
            db.session.commit()

        prefix = os.getenv("OBSERVATIONS_CACHE_PREFIX", "bem:observations")
        CacheService.delete(f"{prefix}:{species_id}:all")
        CacheService.delete(f"{prefix}:{species_id}:inaturalist")

        return total, deleted, True


app = create_app()

if __name__ == "__main__":
    InatSyncRunner(app).run()
