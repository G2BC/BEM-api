"""
Popula species_photos com fotos do iNaturalist a partir de species.inaturalist_taxon_id.

Exemplo:
    export ONLY_CC="true" # pular fotos sem licença CC
    export SLEEP_SEC="1.0" # pausa entre chamadas
    export PHOTO_LIMIT="0" # 0 = todas; >0 = máx de fotos por espécie
    export INAT_USER_AGENT="PlanneoLUMM/1.0 (seu-email@dominio)"
    export LIMIT_SPECIES="0" # 0=todas; >0 = processa apenas N espécies (debug)

    python import_inaturalist_photos_release_1.py
"""

import os
import time
from typing import Any, Dict, List, Optional, Set

import requests
from app import create_app
from app.extensions import db
from app.models.species import Species, SpeciesPhoto

INAT_API = "https://api.inaturalist.org/v1/taxa"

ONLY_CC = os.getenv("ONLY_CC", "true") == "true"
SLEEP_SEC = float(os.getenv("SLEEP_SEC", "1.0"))
PHOTO_LIMIT = int(os.getenv("PHOTO_LIMIT", "0"))
LIMIT_SPECIES = int(os.getenv("LIMIT_SPECIES", "0"))
USER_AGENT = os.getenv("USER_AGENT", "LUMM/1.0 (lumm@uneb.br)")


def get_session() -> requests.Session:
    s = requests.Session()
    s.headers.update({"User-Agent": USER_AGENT})
    return s


def _norm_photo(p: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    if not p:
        return None
    pid = p.get("id")
    medium = p.get("medium_url") or p.get("url")
    if not pid or not medium:
        return None
    return {
        "photo_id": pid,
        "medium_url": medium,
        "original_url": p.get("original_url"),
        "license_code": p.get("license_code"),
        "attribution": p.get("attribution"),
    }


def fetch_taxon_photos(sess: requests.Session, taxon_id: int) -> List[Dict[str, Any]]:
    """Retorna lista de fotos (dicts normalizados) para um táxon."""

    for attempt in range(5):
        r = sess.get(f"{INAT_API}/{taxon_id}", timeout=60)
        if r.status_code == 429:
            time.sleep(2 * (attempt + 1))
            continue
        r.raise_for_status()
        data = r.json()
        break
    else:
        return []

    results = data.get("results") or []
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

    if ONLY_CC:
        photos = [p for p in photos if p.get("license_code")]

    seen: Set[int] = set()
    unique: List[Dict[str, Any]] = []
    for p in photos:
        pid = p["photo_id"]
        if pid not in seen:
            seen.add(pid)
            unique.append(p)

    if PHOTO_LIMIT and PHOTO_LIMIT > 0:
        unique = unique[:PHOTO_LIMIT]

    return unique


def upsert_species_photos(species_id: int, photos: List[Dict[str, Any]]) -> int:
    if not photos:
        return 0

    existing = {
        pid
        for (pid,) in db.session.query(SpeciesPhoto.photo_id)
        .filter(SpeciesPhoto.species_id == species_id)
        .all()
    }

    inserted = 0
    for p in photos:
        if p["photo_id"] in existing:
            db.session.query(SpeciesPhoto).filter_by(
                species_id=species_id, photo_id=p["photo_id"]
            ).update(
                {
                    "medium_url": p["medium_url"],
                    "original_url": p.get("original_url"),
                    "license_code": p.get("license_code"),
                    "attribution": p.get("attribution"),
                },
                synchronize_session=False,
            )
        else:
            db.session.add(
                SpeciesPhoto(
                    species_id=species_id,
                    photo_id=p["photo_id"],
                    medium_url=p["medium_url"],
                    original_url=p.get("original_url"),
                    license_code=p.get("license_code"),
                    attribution=p.get("attribution"),
                    source="iNaturalist",
                )
            )
            inserted += 1

    return inserted


app = create_app()


def main():
    sess = get_session()

    BATCH_COMMIT = int(os.getenv("BATCH_COMMIT", "20"))

    with app.app_context():
        rows = (
            db.session.query(Species.id, Species.inaturalist_taxon_id)
            .filter(Species.inaturalist_taxon_id.isnot(None))
            .order_by(Species.id.asc())
            .all()
        )

        total = len(rows)
        processed = 0
        total_inserted = 0

        for species_id, taxon_id in rows:
            if LIMIT_SPECIES and processed >= LIMIT_SPECIES:
                break
            processed += 1

            try:
                photos = fetch_taxon_photos(sess, taxon_id)
                ins = upsert_species_photos(species_id, photos)
                total_inserted += ins

                if processed % BATCH_COMMIT == 0:
                    db.session.commit()
            except Exception as e:
                db.session.rollback()
                print(f"• ERRO species_id={species_id} tid={taxon_id}: {e}")

            print(f"• {processed}/{total} • species_id={species_id} tid={taxon_id} +{ins} fotos")
            time.sleep(SLEEP_SEC)

        db.session.commit()
        db.session.close()

        print(f"FIM • espécies processadas: {processed} • fotos novas: {total_inserted}")


if __name__ == "__main__":
    main()
