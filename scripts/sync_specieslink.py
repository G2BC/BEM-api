"""
Sincroniza ocorrências do speciesLink para espécies com scientific_name cadastrado.

- Full sync por padrão
- UPSERT via INSERT ... ON CONFLICT DO UPDATE
- Reconcilia observações removendo, no final, o que não veio mais da API
- Paginação via offset/limit
- Kill switch por SL_MAX_RUNTIME_SECONDS
- Grupos por campo `bem` com checkpoint Redis (via sync_base.SyncRunner)
"""

import datetime
import os
import sys
import time
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

SPECIESLINK_API_URL = os.getenv("SPECIESLINK_API_URL", "https://specieslink.net/ws/1.0/search")
SPECIESLINK_API_KEY = os.getenv("SPECIESLINK_API_KEY", "")
PAGE_SIZE = 500
REQUEST_TIMEOUT = 30
SLEEP_BETWEEN_PAGES = 3.0
MAX_RETRIES = 3
HEADERS = {"User-Agent": "BEM-api/1.0 (bem.uneb.br; contact: bem.g2bc@gmail.com)"}


def _log(msg):
    print(msg, flush=True)


def _build_date(props):
    try:
        y = int(props.get("yearcollected") or 0)
        m = int(props.get("monthcollected") or 0)
        d = int(props.get("daycollected") or 0)
        if y and m and d:
            return str(datetime.date(y, m, d))
    except ValueError:
        pass
    return None


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
        r = requests.get(SPECIESLINK_API_URL, params=params, headers=HEADERS, timeout=REQUEST_TIMEOUT)
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


def _delete_stale(species_id, seen_external_ids):
    current_ids = {
        eid for (eid,) in db.session.query(Observation.external_id).filter_by(
            species_id=species_id, source="specieslink"
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
            Observation.source == "specieslink",
            Observation.external_id.in_(chunk),
        ).delete(synchronize_session=False)
    db.session.commit()
    return deleted


# ---------------------------------------------------------------------------
# SyncRunner subclass
# ---------------------------------------------------------------------------

class SpeciesLinkSyncRunner(SyncRunner):
    source_name = "specieslink"
    env_prefix = "SL"

    def get_species_rows(self, session, bem_ids):
        q = session.query(Species.id, Species.scientific_name, Species.bem).filter(
            Species.scientific_name.isnot(None)
        )
        if bem_ids:
            q = q.filter(Species.id.in_(bem_ids))
        return q.all()

    def sync_species(self, row, start_time):
        species_id = row.id
        scientific_name = row.scientific_name
        max_runtime = self.max_runtime

        seen_external_ids = set()
        total = 0
        offset = 0
        page = 0

        while True:
            if time.time() - start_time > max_runtime:
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
                if not (-33.75 <= lat <= 5.27 and -73.99 <= lng <= -28.65):
                    continue

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
            _log(f"  [{species_id}] página {page}: {len(features)} features, {len(rows)} válidas (total: {number_matched})")

            offset += PAGE_SIZE
            if offset >= number_matched or not features:
                break

            time.sleep(SLEEP_BETWEEN_PAGES)

        deleted = _delete_stale(species_id, seen_external_ids)

        prefix = os.getenv("OBSERVATIONS_CACHE_PREFIX", "bem:observations")
        CacheService.delete(f"{prefix}:{species_id}:all")
        CacheService.delete(f"{prefix}:{species_id}:specieslink")

        return total, deleted, True


app = create_app()

if __name__ == "__main__":
    SpeciesLinkSyncRunner(app).run()
