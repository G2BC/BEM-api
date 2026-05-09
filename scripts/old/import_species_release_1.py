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

import pandas as pd
from app import create_app
from app.extensions import db
from app.models.species import Species

EXCEL_PATH = os.getenv("EXCEL_PATH", "Bioluminescent fungi_Perryetal2025.xlsx")
SHEET = os.getenv("SHEET", "SPECIES")


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


def _b(v):
    if v is None or _is_nan(v):
        return None
    s = str(v).strip().lower()
    if s in {"true", "t", "1", "yes", "y", "sim", "+"}:
        return True
    if s in {"false", "f", "0", "no", "n", "nao", "não", "-"}:
        return False
    return None  # Se desconhecido NULL


app = create_app()


def main():
    print("INICIANDO IMPORTAÇÃO")
    df = pd.read_excel(EXCEL_PATH, sheet_name=SHEET)

    inserted = updated = skipped = 0

    with app.app_context():
        for row in df.to_dict(orient="records"):
            sci = _txt(row.get("TAXON"))
            if not sci:
                skipped += 1
                continue

            obj = Species.query.filter_by(scientific_name=sci).one_or_none()
            is_new = obj is None
            if is_new:
                obj = Species(scientific_name=sci)
                db.session.add(obj)

            obj.authors_abbrev = _txt(row.get("AUTHORS (abbreviated)"))
            obj.publication_year = _i(row.get("YEAR (of effective publication)"))
            obj.lineage = _txt(row.get("LINEAGE"))
            obj.family = _txt(row.get("FAMILY"))
            obj.group_name = _txt(row.get("GROUP"))
            obj.section = _txt(row.get("SECTION"))

            obj.lum_mycelium = _b(row.get("luminescent_mycelium"))
            obj.lum_basidiome = _b(row.get("luminescent_basidiome"))
            obj.lum_stipe = _b(row.get("luminescent_stipe"))
            obj.lum_pileus = _b(row.get("luminescent_pileus"))
            obj.lum_lamellae = _b(row.get("luminescent_lamellae"))
            obj.lum_spores = _b(row.get("luminescent_spores"))

            obj.type_country = _txt(row.get("TypeCountry"))

            obj.distribution_regions = obj.distribution_regions or []

            obj.mycobank_index_fungorum_id = _i(row.get("MycoBank_IndexFungorumID"))
            obj.mycobank_type = _txt(row.get("MycoBankType"))
            obj.ncbi_taxonomy_id = _i(row.get("NCBITaxonomyID"))
            obj.inaturalist_taxon_id = _i(row.get("iNaturalistTaxa"))
            obj.iucn_redlist = _txt(row.get("IUCNRedList"))
            obj.unite_taxon_id = _i(row.get("UNITETaxonId"))

            obj.references_raw = _txt(row.get("References"))

            inserted += 1 if is_new else 1 if not is_new else 0
            updated += 0 if is_new else 1

        db.session.commit()

    print(f"INSERIDOS/ATUALIZADOS: {inserted}/{updated}")
    print(f"PULADOS (SEM TAXON): {skipped}")
    print("IMPORTAÇÃO CONCLUÍDA")


if __name__ == "__main__":
    main()
