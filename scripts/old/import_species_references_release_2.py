import math
import os
import sys
from pathlib import Path

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from app import create_app
from app.extensions import db
from app.models.species import Species
from app.models.reference import Reference

EXCEL_PATH = os.getenv("EXCEL_PATH", "data/Bioluminescent fungi_Perryetal2025.xlsx")
SHEET = os.getenv("SHEET", "REFERENCES")


def _is_nan(x):
    return isinstance(x, float) and math.isnan(x)


def _txt(v):
    if v is None or _is_nan(v):
        return None
    s = str(v).strip()
    return s or None


def _i(v):
    if v is None or _is_nan(v):
        return None
    try:
        return int(float(v))
    except Exception:
        return None


app = create_app()


def main():
    print("INICIANDO IMPORTAÇÃO DE REFERÊNCIAS")
    df = pd.read_excel(EXCEL_PATH, sheet_name=SHEET)

    associated = 0
    created_refs = 0
    skipped = 0

    with app.app_context():
        for row in df.to_dict(orient="records"):
            mb_id = _i(row.get("MycoBank_IndexFungorumID"))
            doi = _txt(row.get("DOI"))
            url = _txt(row.get("URL"))
            apa = _txt(row.get("APA"))

            if not mb_id:
                skipped += 1
                continue
        
            if not doi and not url and not apa:
                skipped += 1
                continue

            species = Species.query.filter_by(
                mycobank_index_fungorum_id=mb_id
            ).one_or_none()

            if species is None:
                print(f"[WARN] Espécie não encontrada para MycoBank_IndexFungorumID={mb_id}")
                skipped += 1
                continue

            if not doi and not url and not apa:
                print(f"[WARN] Referência vazia para MycoBank_IndexFungorumID={mb_id}")
                skipped += 1
                continue

            ref = None

            if doi:
                ref = Reference.query.filter_by(doi=doi).one_or_none()

            if ref is None and url:
                ref = Reference.query.filter_by(url=url).one_or_none()

            if ref is None and apa:
                ref = Reference.query.filter_by(apa=apa).one_or_none()

            if ref is None:
                ref = Reference(
                    doi=doi,
                    url=url,
                    apa=apa,
                )
                db.session.add(ref)
                db.session.flush()
                created_refs += 1

            already_linked = any(existing.id == ref.id for existing in species.references)
            if already_linked:
                continue

            species.references.append(ref)
            associated += 1

        db.session.commit()

    print(f"ASSOCIAÇÕES CRIADAS: {associated}")
    print(f"REFERÊNCIAS CRIADAS: {created_refs}")
    print(f"PULADOS: {skipped}")
    print("IMPORTAÇÃO CONCLUÍDA")


if __name__ == "__main__":
    main()