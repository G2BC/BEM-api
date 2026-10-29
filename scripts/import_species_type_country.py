"""
Popula `species.type_country` a partir da coluna `CountryType` da aba Fungi do
arquivo BEM em XLSX (BEM-60).

Regras:
- A correspondência entre planilha e banco é feita apenas pelo MycoBankID.
- CountryType vazio não sobrescreve o valor existente no banco.
- MycoBankID sem espécie no banco é ignorado e registrado no log.
- MycoBankID duplicado na planilha: vale a primeira ocorrência; as demais são
  ignoradas e registradas no log.
- MycoBankID que corresponde a mais de uma espécie no banco: o valor vai apenas
  para a espécie cujo nome científico é o Taxon da linha (aba Taxonomy, alinhada
  à aba Fungi pela ordem das linhas); as demais são ignoradas e registradas no
  log para revisão da curadoria.
- Apenas `type_country` é alterado; nenhuma espécie é criada.
- Seguro para re-execução: só grava quando o valor muda.

Exemplo:
    EXCEL_PATH="/caminho/Brazilian_Edible_Mushrooms_18-05-26.xlsx" \
    uv run python scripts/import_species_type_country.py

    # Simulação sem gravar no banco
    DRY_RUN=1 EXCEL_PATH="..." uv run python scripts/import_species_type_country.py
"""

from __future__ import annotations

import os
import sys
from collections import defaultdict
from pathlib import Path

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from app import create_app
from app.extensions import db
from app.models.species import Species

EXCEL_PATH = os.getenv("EXCEL_PATH", "Brazilian_Edible_Mushrooms_18-05-26.xlsx")
SHEET = os.getenv("SHEET", "Fungi")
TAXONOMY_SHEET = os.getenv("TAXONOMY_SHEET", "Taxonomy")
DRY_RUN = os.getenv("DRY_RUN", "").strip().lower() in {"1", "true", "yes"}

ID_COLUMN = "MycoBankID"
COUNTRY_COLUMN = "CountryType"
TAXON_COLUMN = "Taxon"


def _txt(value):
    if value is None or pd.isna(value):
        return None
    text = str(value).strip()
    return text or None


def _i(value):
    if value is None or pd.isna(value):
        return None
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return None


def _species_by_mycobank_id() -> dict[int, list[Species]]:
    species_by_id = defaultdict(list)
    query = Species.query.filter(Species.mycobank_index_fungorum_id.isnot(None))
    for species in query.order_by(Species.id):
        species_by_id[species.mycobank_index_fungorum_id].append(species)
    return species_by_id


def _read_data() -> pd.DataFrame:
    df = pd.read_excel(EXCEL_PATH, sheet_name=SHEET)
    taxonomy = pd.read_excel(EXCEL_PATH, sheet_name=TAXONOMY_SHEET)

    for sheet_name, frame, column in (
        (SHEET, df, ID_COLUMN),
        (SHEET, df, COUNTRY_COLUMN),
        (TAXONOMY_SHEET, taxonomy, ID_COLUMN),
        (TAXONOMY_SHEET, taxonomy, TAXON_COLUMN),
    ):
        if column not in frame.columns:
            raise ValueError(f"A aba {sheet_name!r} nao tem a coluna {column}")

    if (
        len(df) != len(taxonomy)
        or (
            df[ID_COLUMN].reset_index(drop=True) != taxonomy[ID_COLUMN].reset_index(drop=True)
        ).any()
    ):
        raise ValueError(f"MycoBankID desalinhado entre as abas {SHEET!r} e {TAXONOMY_SHEET!r}")

    df[TAXON_COLUMN] = taxonomy[TAXON_COLUMN]
    return df


app = create_app()


def main():
    print("INICIANDO IMPORTACAO DE TYPE_COUNTRY" + (" (DRY RUN)" if DRY_RUN else ""))
    df = _read_data()

    processed = updated = unchanged = 0
    skipped_empty = not_found = duplicates = conflicts = invalid_id = 0
    first_row_by_id: dict[int, tuple[int, str | None]] = {}

    with app.app_context():
        species_by_id = _species_by_mycobank_id()

        for index, row in enumerate(df.to_dict(orient="records")):
            line = index + 2  # linha 1 é o cabeçalho
            processed += 1

            mb_id = _i(row.get(ID_COLUMN))
            country = _txt(row.get(COUNTRY_COLUMN))

            if mb_id is None:
                print(f"[WARN] Linha {line}: MycoBankID invalido ({row.get(ID_COLUMN)!r})")
                invalid_id += 1
                continue

            if mb_id in first_row_by_id:
                first_line, first_country = first_row_by_id[mb_id]
                print(
                    f"[WARN] Linha {line}: MycoBankID={mb_id} duplicado "
                    f"(CountryType={country!r}); mantida a linha {first_line} "
                    f"(CountryType={first_country!r})"
                )
                duplicates += 1
                continue
            first_row_by_id[mb_id] = (line, country)

            if country is None:
                skipped_empty += 1
                continue

            matches = species_by_id.get(mb_id)
            if not matches:
                print(f"[WARN] Linha {line}: especie nao encontrada para MycoBankID={mb_id}")
                not_found += 1
                continue

            if len(matches) > 1:
                taxon = _txt(row.get(TAXON_COLUMN))
                targets = [s for s in matches if s.scientific_name == taxon]
                for species in matches:
                    if species in targets:
                        continue
                    print(
                        f"[WARN] MycoBankID {mb_id} ({species.scientific_name}, id {species.id}) "
                        "ignorado — possível MycoBankID incorreto na planilha. Requer revisão "
                        "manual pela curadoria para obter o CountryType correto."
                    )
                    conflicts += 1
                matches = targets

            for species in matches:
                if species.type_country == country:
                    unchanged += 1
                    continue
                species.type_country = country
                updated += 1

        if DRY_RUN:
            db.session.rollback()
        else:
            db.session.commit()

    print(f"TOTAL PROCESSADO: {processed}")
    print(f"ATUALIZADOS: {updated}")
    print(f"SEM ALTERACAO (VALOR JA IGUAL): {unchanged}")
    print(f"IGNORADOS (COUNTRYTYPE VAZIO): {skipped_empty}")
    print(f"NAO ENCONTRADOS NO BANCO: {not_found}")
    print(f"DUPLICATAS NA PLANILHA: {duplicates}")
    print(f"IGNORADOS (DUPLICATA COM CONFLITO): {conflicts}")
    if invalid_id:
        print(f"IGNORADOS (MYCOBANKID INVALIDO): {invalid_id}")
    print("IMPORTACAO CONCLUIDA" + (" (DRY RUN, NADA FOI GRAVADO)" if DRY_RUN else ""))


if __name__ == "__main__":
    main()
