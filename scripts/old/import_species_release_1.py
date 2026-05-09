"""
Importa os dados iniciais de espécies a partir do arquivo BEM em XLSX.

O arquivo atual tem os dados distribuídos em 3 abas alinhadas pela ordem das
linhas:
- Taxonomy: MycoBankID, Taxon
- Fungi: MycoBankID, CommonName, BEM, Brazilian type, IDs externos etc.
- Ocurrences_Literature: MycoBankID, Brazilian state, Biome

`Brazilian state` cria/associa registros em distributions por sigla.
`Biome` cria/associa registros em habitats via species_characteristics.

Exemplo:
    EXCEL_PATH="/caminho/Brazilian_Edible_Mushrooms_12-07-25.xlsx" \
    python scripts/old/import_species_release_1.py
"""

from __future__ import annotations

import math
import os
import re
import sys
from pathlib import Path

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from app import create_app
from app.extensions import db
from app.models.distribution import Distribution
from app.models.habitat import Habitat
from app.models.species import Species
from app.models.species_characteristics import SpeciesCharacteristics

EXCEL_PATH = os.getenv("EXCEL_PATH", "Brazilian_Edible_Mushrooms_12-07-25.xlsx")
TAXONOMY_SHEET = os.getenv("TAXONOMY_SHEET", "Taxonomy")
FUNGI_SHEET = os.getenv("FUNGI_SHEET", "Fungi")
OCCURRENCES_SHEET = os.getenv("OCCURRENCES_SHEET", "Ocurrences_Literature")

BRAZILIAN_STATES = {
    "AC": ("Acre", "Acre"),
    "AL": ("Alagoas", "Alagoas"),
    "AM": ("Amazonas", "Amazonas"),
    "AP": ("Amapá", "Amapá"),
    "BA": ("Bahia", "Bahia"),
    "CE": ("Ceará", "Ceará"),
    "DF": ("Distrito Federal", "Distrito Federal"),
    "ES": ("Espírito Santo", "Espírito Santo"),
    "GO": ("Goiás", "Goiás"),
    "MA": ("Maranhão", "Maranhão"),
    "MG": ("Minas Gerais", "Minas Gerais"),
    "MS": ("Mato Grosso do Sul", "Mato Grosso do Sul"),
    "MT": ("Mato Grosso", "Mato Grosso"),
    "PA": ("Pará", "Pará"),
    "PB": ("Paraíba", "Paraíba"),
    "PE": ("Pernambuco", "Pernambuco"),
    "PI": ("Piauí", "Piauí"),
    "PR": ("Paraná", "Paraná"),
    "RJ": ("Rio de Janeiro", "Rio de Janeiro"),
    "RN": ("Rio Grande do Norte", "Rio Grande do Norte"),
    "RO": ("Rondônia", "Rondônia"),
    "RR": ("Roraima", "Roraima"),
    "RS": ("Rio Grande do Sul", "Rio Grande do Sul"),
    "SC": ("Santa Catarina", "Santa Catarina"),
    "SE": ("Sergipe", "Sergipe"),
    "SP": ("São Paulo", "São Paulo"),
    "TO": ("Tocantins", "Tocantins"),
}

HABITAT_LABELS = {
    "amazon rainforest": ("amazon-rainforest", "Amazon Rainforest", "Floresta Amazônica"),
    "atlantic rainforest": ("atlantic-rainforest", "Atlantic Rainforest", "Mata Atlântica"),
    "caatinga": ("caatinga", "Caatinga", "Caatinga"),
    "cerrado": ("cerrado", "Cerrado", "Cerrado"),
    "corn plantation": ("corn-plantation", "Corn Plantation", "Plantação de milho"),
    "eucalyptus plantation": (
        "eucalyptus-plantation",
        "Eucalyptus Plantation",
        "Plantação de eucalipto",
    ),
    "exotic trees": ("exotic-trees", "Exotic Trees", "Árvores exóticas"),
    "on cattle dung": ("cattle-dung", "On Cattle Dung", "Esterco bovino"),
    "pampa": ("pampa", "Pampa", "Pampa"),
    "pecan plantation": ("pecan-plantation", "Pecan Plantation", "Plantação de pecan"),
    "pinus plantation": ("pinus-plantation", "Pinus Plantation", "Plantação de pinus"),
    "sand dunes": ("sand-dunes", "Sand Dunes", "Dunas"),
}

UNIQUE_ID_FIELDS = {
    "inaturalist_taxon_id": "iNaturalist",
    "ncbi_taxonomy_id": "NCBITaxonomyID",
    "unite_taxon_id": "UNITETaxonId",
}


def _is_nan(value):
    return isinstance(value, float) and math.isnan(value)


def _txt(value):
    if value is None or _is_nan(value):
        return None
    text = str(value).strip()
    return text or None


def _i(value):
    if value is None or _is_nan(value):
        return None
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return None


def _normalize_key(value: str) -> str:
    return re.sub(r"\s+", " ", value.strip().lower())


def _slugify(value: str) -> str:
    slug = re.sub(r"[^a-z0-9]+", "-", value.strip().lower())
    return slug.strip("-")


def _parse_semicolon_values(value) -> list[str]:
    raw = _txt(value)
    if not raw:
        return []

    values = []
    for item in raw.split(";"):
        text = _txt(item)
        if text and text not in values:
            values.append(text)

    return values


def _last_path_segment(value):
    text = _txt(value)
    if not text:
        return None
    parts = [part.strip() for part in text.split("/") if part.strip()]
    return parts[-1] if parts else None


def _read_initial_data() -> pd.DataFrame:
    taxonomy = pd.read_excel(EXCEL_PATH, sheet_name=TAXONOMY_SHEET)
    fungi = pd.read_excel(EXCEL_PATH, sheet_name=FUNGI_SHEET)
    occurrences = pd.read_excel(EXCEL_PATH, sheet_name=OCCURRENCES_SHEET)

    lengths = {len(taxonomy), len(fungi), len(occurrences)}
    if len(lengths) != 1:
        raise ValueError(
            "As abas precisam ter a mesma quantidade de linhas: "
            f"{TAXONOMY_SHEET}={len(taxonomy)}, "
            f"{FUNGI_SHEET}={len(fungi)}, "
            f"{OCCURRENCES_SHEET}={len(occurrences)}"
        )

    for sheet_name, frame in (
        (TAXONOMY_SHEET, taxonomy),
        (FUNGI_SHEET, fungi),
        (OCCURRENCES_SHEET, occurrences),
    ):
        if "MycoBankID" not in frame.columns:
            raise ValueError(f"A aba {sheet_name!r} nao tem a coluna MycoBankID")

    mismatched = (
        taxonomy["MycoBankID"].reset_index(drop=True)
        != fungi["MycoBankID"].reset_index(drop=True)
    ) | (
        taxonomy["MycoBankID"].reset_index(drop=True)
        != occurrences["MycoBankID"].reset_index(drop=True)
    )
    if mismatched.any():
        rows = ", ".join(str(i + 2) for i in mismatched[mismatched].index[:10])
        raise ValueError(f"MycoBankID desalinhado entre abas nas linhas: {rows}")

    data = taxonomy[["MycoBankID", "Taxon"]].copy()
    for column in fungi.columns:
        if column != "MycoBankID":
            data[column] = fungi[column]
    for column in occurrences.columns:
        if column != "MycoBankID":
            data[column] = occurrences[column]

    return data


def _ensure_distributions(state_codes: set[str]) -> dict[str, Distribution]:
    distributions = {d.slug.upper(): d for d in Distribution.query.all()}

    for raw_code in sorted(state_codes):
        slug = raw_code.upper()
        if slug in distributions:
            continue
        label_en, label_pt = BRAZILIAN_STATES.get(slug, (slug, slug))
        distribution = Distribution(slug=slug, label_en=label_en, label_pt=label_pt)
        db.session.add(distribution)
        distributions[slug] = distribution

    return distributions


def _ensure_habitats(biome_names: set[str]) -> dict[str, Habitat]:
    habitats = {h.slug: h for h in Habitat.query.all()}

    for biome_name in sorted(biome_names):
        key = _normalize_key(biome_name)
        slug, label_en, label_pt = HABITAT_LABELS.get(
            key,
            (_slugify(biome_name), biome_name.strip(), biome_name.strip()),
        )
        if slug in habitats:
            continue
        habitat = Habitat(slug=slug, label_en=label_en, label_pt=label_pt)
        db.session.add(habitat)
        habitats[slug] = habitat

    return habitats


def _habitat_slug(biome_name: str) -> str:
    key = _normalize_key(biome_name)
    return HABITAT_LABELS.get(key, (_slugify(biome_name), "", ""))[0]


def _assign_unique_external_id(obj, field_name, value, used_by_field, warnings):
    if value is None:
        setattr(obj, field_name, None)
        return

    used_by = used_by_field[field_name].get(value)
    if used_by and used_by != obj.scientific_name:
        warnings.append(
            f"{field_name}={value} ja usado por {used_by}; "
            f"ignorado em {obj.scientific_name}"
        )
        setattr(obj, field_name, None)
        return

    existing = Species.query.filter(getattr(Species, field_name) == value).one_or_none()
    if existing and existing.id != obj.id and existing.scientific_name != obj.scientific_name:
        warnings.append(
            f"{field_name}={value} ja existe no banco para "
            f"{existing.scientific_name}; ignorado em {obj.scientific_name}"
        )
        setattr(obj, field_name, None)
        return

    setattr(obj, field_name, value)
    used_by_field[field_name][value] = obj.scientific_name


app = create_app()


def main():
    print("INICIANDO IMPORTACAO")
    data = _read_initial_data()

    inserted = updated = skipped = 0
    warnings = []
    used_by_field = {field: {} for field in UNIQUE_ID_FIELDS}
    state_codes = {
        value.upper()
        for raw in data["Brazilian state"]
        for value in _parse_semicolon_values(raw)
    }
    biome_names = {
        value
        for raw in data["Biome"]
        for value in _parse_semicolon_values(raw)
    }

    with app.app_context():
        distributions_by_slug = _ensure_distributions(state_codes)
        habitats_by_slug = _ensure_habitats(biome_names)

        for row in data.to_dict(orient="records"):
            scientific_name = _txt(row.get("Taxon"))
            if not scientific_name:
                skipped += 1
                continue

            obj = Species.query.filter_by(scientific_name=scientific_name).one_or_none()
            is_new = obj is None
            if is_new:
                obj = Species(scientific_name=scientific_name)
                db.session.add(obj)

            obj.mycobank_index_fungorum_id = _i(row.get("MycoBankID"))
            obj.common_name = _txt(row.get("CommonName"))
            obj.bem = _txt(row.get("BEM"))
            obj.brazilian_type = _txt(row.get("Brazilian type"))
            obj.brazilian_type_synonym = _txt(row.get("Brazilian type synonym"))
            obj.dna = _txt(row.get("DNA"))
            obj.iucn_redlist = _last_path_segment(row.get("IUCNRedList"))
            obj.is_visible = True

            for field_name, column_name in UNIQUE_ID_FIELDS.items():
                _assign_unique_external_id(
                    obj,
                    field_name,
                    _i(row.get(column_name)),
                    used_by_field,
                    warnings,
                )

            distribution_slugs = [
                value.upper()
                for value in _parse_semicolon_values(row.get("Brazilian state"))
            ]
            obj.distributions = [
                distributions_by_slug[slug]
                for slug in distribution_slugs
                if slug in distributions_by_slug
            ]

            habitat_slugs = [
                _habitat_slug(value)
                for value in _parse_semicolon_values(row.get("Biome"))
            ]
            if habitat_slugs or obj.characteristics is not None:
                if obj.characteristics is None:
                    obj.characteristics = SpeciesCharacteristics()
                obj.characteristics.habitats = [
                    habitats_by_slug[slug]
                    for slug in habitat_slugs
                    if slug in habitats_by_slug
                ]

            inserted += 1 if is_new else 0
            updated += 0 if is_new else 1

        db.session.commit()

    for warning in warnings:
        print(f"[WARN] {warning}")

    print(f"INSERIDOS: {inserted}")
    print(f"ATUALIZADOS: {updated}")
    print(f"PULADOS (SEM TAXON): {skipped}")
    print("IMPORTACAO CONCLUIDA")


if __name__ == "__main__":
    main()
