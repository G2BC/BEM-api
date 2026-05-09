"""
Importa dados de espécies para a tabela `species` a partir de um arquivo .xlsx (release 1.0).

Antes de executar, defina as ENVS:
- EXCEL_PATH: caminho do arquivo .xlsx.
- SHEET: nome da planilha (aba) com os dados.

Exemplo:
    export EXCEL_PATH="/caminho/arquivo.xlsx"
    export SHEET="SPECIES"

    python import_species_release_1.py
"""

import math
import os
import sys
import pandas as pd
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from app import create_app
from app.extensions import db
from app.models.species import Species
from app.models.distribution import Distribution

EXCEL_PATH = os.getenv("EXCEL_PATH", "data/Bioluminescent fungi_Perryetal2025.xlsx")
SHEET = os.getenv("SHEET", "SPECIES")


def _is_nan(x):
    return isinstance(x, float) and math.isnan(x)


def _txt(v):
    if v is None or _is_nan(v):
        return None
    s = str(v).strip()
    return s or None


def _parse_distribution_values(v):
    raw = _txt(v)
    if not raw:
        return []

    values = []
    for item in raw.split(","):
        code = item.strip().upper()
        if code:
            values.append(code)

    return list(dict.fromkeys(values))


app = create_app()


def main():
    print("INICIANDO IMPORTAÇÃO")
    df = pd.read_excel(EXCEL_PATH, sheet_name=SHEET)

    updated, skipped = 0, 0

    with app.app_context():
        distributions_by_slug = {
            d.slug.upper(): d
            for d in Distribution.query.all()
        }

        for row in df.to_dict(orient="records"):
            sci = _txt(row.get("TAXON"))
            if not sci:
                skipped += 1
                continue

            obj = Species.query.filter_by(scientific_name=sci).one_or_none()
            if obj is None:
                continue

            distribution_codes = _parse_distribution_values(row.get("Distribution"))
            if not distribution_codes:
                updated += 1
                continue

            existing_slugs = {d.slug.upper() for d in obj.distributions}

            for code in distribution_codes:
                distribution = distributions_by_slug.get(code)
                if not distribution:
                    print(f"[WARN] Distribuição inválida para {sci}: {code}")
                    continue

                if code in existing_slugs:
                    continue

                obj.distributions.append(distribution)
                existing_slugs.add(code)

            updated += 1

        db.session.commit()

    print(f"ATUALIZADOS: {updated}")
    print(f"PULADOS (SEM TAXON): {skipped}")
    print("IMPORTAÇÃO CONCLUÍDA")


if __name__ == "__main__":
    main()