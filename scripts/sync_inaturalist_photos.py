"""
Sincroniza fotos do iNaturalist para espécies com inaturalist_taxon_id cadastrado.

- Busca fotos via /v1/taxa/{taxon_id} (default_photo + taxon_photos)
- UPSERT por photo_id
- Apenas licenças CC (fixo)
- Kill switch por INAT_PHOTOS_MAX_RUNTIME_SECONDS
- Grupos por campo `bem` com checkpoint Redis (via sync_base.SyncRunner)
"""

import os
import sys
import time
from pathlib import Path
from typing import Any, Dict, List, Optional, Set

import requests

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from app import create_app  # noqa: E402
from app.extensions import db  # noqa: E402
from app.models.species import Species  # noqa: E402
from app.models.species_photo import SpeciesPhoto  # noqa: E402
from scripts.sync_base import SyncRunner  # noqa: E402

INAT_API = "https://api.inaturalist.org/v1/taxa"
SLEEP_SEC = float(os.getenv("INAT_PHOTOS_SLEEP_SEC", "1.0"))
MAX_RETRIES = 5
USER_AGENT = os.getenv("INAT_USER_AGENT", "BEM-api/1.0 (bem@uneb.br)")


def _log(msg):
    print(msg, flush=True)


def _norm_photo(p: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    if not p:
        return None
    pid = p.get("id")
    medium = p.get("original") or p.get("medium_url") or p.get("url")
    if not pid or not medium:
        return None
    return {
        "photo_id": pid,
        "medium_url": medium,
        "original_url": p.get("original_url"),
        "license_code": p.get("license_code"),
        "attribution": p.get("attribution"),
        "rights_holder": p.get("rights_holder"),
        "source_url": p.get("native_page_url") or f"https://www.inaturalist.org/photos/{pid}",
    }


def _fetch_taxon_photos(sess: requests.Session, taxon_id: int) -> List[Dict[str, Any]]:
    for attempt in range(MAX_RETRIES):
        r = sess.get(f"{INAT_API}/{taxon_id}", timeout=60)
        if r.status_code == 429:
            wait = 2 ** attempt * 10
            _log(f"  429 do iNaturalist — aguardando {wait}s (tentativa {attempt + 1}/{MAX_RETRIES})")
            time.sleep(wait)
            continue
        r.raise_for_status()

        results = (r.json().get("results") or [])
        if not results:
            return []

        tx = results[0]
        photos: List[Dict[str, Any]] = []

        dp = _norm_photo(tx.get("default_photo") or {})
        if dp:
            photos.append(dp)
        for tp in tx.get("taxon_photos") or []:
            p = _norm_photo((tp or {}).get("photo") or {})
            if p:
                photos.append(p)

        # Apenas licenças CC
        photos = [p for p in photos if p.get("license_code")]

        seen: Set[int] = set()
        unique: List[Dict[str, Any]] = []
        for p in photos:
            if p["photo_id"] not in seen:
                seen.add(p["photo_id"])
                unique.append(p)

        return unique

    raise RuntimeError(f"Máximo de tentativas atingido para taxon_id={taxon_id}")


def _upsert_photos(species_id: int, photos: List[Dict[str, Any]]) -> int:
    if not photos:
        return 0

    existing = {
        pid for (pid,) in db.session.query(SpeciesPhoto.photo_id)
        .filter(SpeciesPhoto.species_id == species_id).all()
    }

    inserted = 0
    for p in photos:
        if p["photo_id"] in existing:
            db.session.query(SpeciesPhoto).filter_by(
                species_id=species_id, photo_id=p["photo_id"]
            ).update({
                "medium_url": p["medium_url"],
                "original_url": p.get("original_url"),
                "license_code": p.get("license_code"),
                "attribution": p.get("attribution"),
                "rights_holder": p.get("rights_holder"),
                "source_url": p.get("source_url"),
            }, synchronize_session=False)
        else:
            db.session.add(SpeciesPhoto(
                species_id=species_id,
                photo_id=p["photo_id"],
                medium_url=p["medium_url"],
                original_url=p.get("original_url"),
                license_code=p.get("license_code"),
                attribution=p.get("attribution"),
                rights_holder=p.get("rights_holder"),
                source_url=p.get("source_url"),
                source="iNaturalist",
            ))
            inserted += 1

    db.session.commit()
    return inserted


# ---------------------------------------------------------------------------
# SyncRunner subclass
# ---------------------------------------------------------------------------

class InatPhotosSyncRunner(SyncRunner):
    source_name = "inaturalist-photos"
    env_prefix = "INAT_PHOTOS"

    def __init__(self, app):
        super().__init__(app)
        self.sess = requests.Session()
        self.sess.headers.update({"User-Agent": USER_AGENT})

    def get_species_rows(self, session, bem_ids):
        q = session.query(Species.id, Species.inaturalist_taxon_id, Species.bem).filter(
            Species.inaturalist_taxon_id.isnot(None)
        ).order_by(Species.id.asc())
        if bem_ids:
            q = q.filter(Species.id.in_(bem_ids))
        return q.all()

    def sync_species(self, row, start_time):
        photos = _fetch_taxon_photos(self.sess, row.inaturalist_taxon_id)
        inserted = _upsert_photos(row.id, photos)
        time.sleep(SLEEP_SEC)
        return inserted, 0, True

    def _log(self, msg):
        print(msg, flush=True)


app = create_app()

if __name__ == "__main__":
    InatPhotosSyncRunner(app).run()
