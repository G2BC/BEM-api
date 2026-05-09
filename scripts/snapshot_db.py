"""
Gera snapshot do banco em XLSX e JSON (PT e EN) e faz upload no MinIO.
"""

from __future__ import annotations

import io
import json
import os
from datetime import UTC, datetime
from pathlib import Path
import sys

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

import pandas as pd
from minio import Minio
from sqlalchemy.orm import joinedload, selectinload

from app import create_app
from app.models.species import Species
from app.models.species_characteristics import SpeciesCharacteristics


app = create_app()

REPORT_BUCKET = os.getenv("MINIO_DB_BUCKET", "lumm-db")
FILE_PREFIX = "lumm"


def _log(message: str, level: str = "INFO") -> None:
    print(f"[{level}] {message}")


def _get_minio_client() -> Minio:
    endpoint = app.config.get("MINIO_ENDPOINT", "")
    access_key = app.config.get("MINIO_ACCESS_KEY", "")
    secret_key = app.config.get("MINIO_SECRET_KEY", "")
    secure = app.config.get("MINIO_SECURE", False)

    if not endpoint:
        raise RuntimeError("MINIO_ENDPOINT não configurado")

    for scheme in ("http://", "https://"):
        if endpoint.startswith(scheme):
            endpoint = endpoint[len(scheme):]
            break

    _log(f"MinIO endpoint: {repr(endpoint)}")
    _log(f"MinIO access_key: {repr(access_key)}")

    return Minio(endpoint, access_key=access_key, secret_key=secret_key, secure=secure)


def _id(value) -> str | None:
    """Converte IDs numéricos para string, preservando None."""
    return str(value) if value is not None else None


def _fmt(v):
    """Formata listas e dicts para exibição tabular."""
    if isinstance(v, list):
        return ", ".join(str(i) for i in v) if v else None
    if isinstance(v, dict):
        return ", ".join(f"{k}: {_fmt(val)}" for k, val in v.items())
    return v


def _fix_int_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Converte colunas float64 que são inteiros (ex: meses) para Int64 nullable."""
    for col in df.select_dtypes(include="float64").columns:
        if df[col].dropna().apply(lambda x: x % 1 == 0).all():
            df[col] = df[col].astype("Int64")
    return df


def _query_species() -> list:
    return (
        Species.query.options(
            joinedload(Species.taxonomy),
            joinedload(Species.characteristics).selectinload(SpeciesCharacteristics.habitats),
            joinedload(Species.characteristics).selectinload(SpeciesCharacteristics.growth_forms),
            joinedload(Species.characteristics).selectinload(SpeciesCharacteristics.substrates),
            joinedload(Species.characteristics).selectinload(SpeciesCharacteristics.nutrition_modes),
            selectinload(Species.distributions),
            selectinload(Species.similar_species_links),
        )
        .order_by(Species.id)
        .all()
    )


def _build_row_pt(s) -> dict:
    t = s.taxonomy
    c = s.characteristics
    return {
        "LUMM_ID": s.id,
        "Nome científico": s.scientific_name,
        "Linhagem": s.lineage,
        "Família": s.family,
        "Grupo": s.group_name,
        "Seção": s.section,
        "País do espécime tipo": s.type_country,
        "MycoBank #": _id(s.mycobank_index_fungorum_id),
        "Tipo Mycobank": s.mycobank_type,
        "NCBI Taxonomy ID": _id(s.ncbi_taxonomy_id),
        "iNaturalist Taxon ID": _id(s.inaturalist_taxon_id),
        "IUCN Redlist ID": s.iucn_redlist,
        "Unite Taxon ID": _id(s.unite_taxon_id),
        "Taxonomia": {
            "Classificação": t.classification if t else None,
            "Sinônimos": t.synonyms if t else None,
            "Tipo do nome": t.name_type if t else None,
            "Gênero": t.gender if t else None,
            "Ano da publicação": t.years_of_effective_publication if t else None,
            "Autores": t.authors if t else None,
        },
        "Características": {
            "Anatomia bioluminescente": {
                "Micélio": c.lum_mycelium if c else None,
                "Basidioma": c.lum_basidiome if c else None,
                "Estipe (Caule)": c.lum_stipe if c else None,
                "Píleo (Chapéu)": c.lum_pileus if c else None,
                "Lamelas": c.lum_lamellae if c else None,
                "Esporos": c.lum_spores if c else None,
            },
            "Biomas": [h.label_pt for h in c.habitats] if c else [],
            "Forma de crescimento": [g.label_pt for g in c.growth_forms] if c else [],
            "Substrato": [sub.label_pt for sub in c.substrates] if c else [],
            "Estilo de vida": [n.label_pt for n in c.nutrition_modes] if c else [],
            "Status de conservação": c.conservation_status if c else None,
            "Ano da avaliação IUCN": c.iucn_assessment_year if c else None,
            "Link da avaliação IUCN": c.iucn_assessment_url if c else None,
            "Comestível": c.edible if c else None,
            "Cultivo possível": c.cultivation_possible if c else None,
            "Cultivo": c.cultivation_pt if c else None,
            "Dicas para encontrar": c.finding_tips_pt if c else None,
            "Espécies vegetais associadas": c.nearby_trees_pt if c else None,
            "Curiosidades": c.curiosities_pt if c else None,
            "Descrição geral": c.general_description_pt if c else None,
            "Cores": c.colors_pt if c else None,
            "Altura média do basidioma (cm)": c.size_cm if c else None,
            "Estação de frutificação (Início)": c.season_start_month if c else None,
            "Estação de frutificação (Fim)": c.season_end_month if c else None,
            "Regiões biogeográficas de ocorrência": [f"({d.slug}) {d.label_pt}" for d in s.distributions],
            "Espécies similares (LUMM_ID)": [_id(link.similar_species_id) for link in s.similar_species_links],
        },
    }


def _build_row_en(s) -> dict:
    t = s.taxonomy
    c = s.characteristics
    return {
        "LUMM_ID": s.id,
        "Scientific name": s.scientific_name,
        "Lineage": s.lineage,
        "Family": s.family,
        "Group": s.group_name,
        "Section": s.section,
        "Type specimen country": s.type_country,
        "MycoBank #": _id(s.mycobank_index_fungorum_id),
        "MycoBank type": s.mycobank_type,
        "NCBI Taxonomy ID": _id(s.ncbi_taxonomy_id),
        "iNaturalist Taxon ID": _id(s.inaturalist_taxon_id),
        "IUCN Redlist ID": s.iucn_redlist,
        "Unite Taxon ID": _id(s.unite_taxon_id),
        "Taxonomy": {
            "Classification": t.classification if t else None,
            "Synonyms": t.synonyms if t else None,
            "Name type": t.name_type if t else None,
            "Gender": t.gender if t else None,
            "Year of publication": t.years_of_effective_publication if t else None,
            "Authors": t.authors if t else None,
        },
        "Characteristics": {
            "Bioluminescent anatomy": {
                "Mycelium": c.lum_mycelium if c else None,
                "Basidiome": c.lum_basidiome if c else None,
                "Stipe (Stem)": c.lum_stipe if c else None,
                "Pileus (Cap)": c.lum_pileus if c else None,
                "Lamellae": c.lum_lamellae if c else None,
                "Spores": c.lum_spores if c else None,
            },
            "Biomes": [h.label_en for h in c.habitats] if c else [],
            "Growth form": [g.label_en for g in c.growth_forms] if c else [],
            "Substrate": [sub.label_en for sub in c.substrates] if c else [],
            "Lifestyle": [n.label_en for n in c.nutrition_modes] if c else [],
            "Conservation status": c.conservation_status if c else None,
            "IUCN assessment year": c.iucn_assessment_year if c else None,
            "IUCN assessment link": c.iucn_assessment_url if c else None,
            "Edible": c.edible if c else None,
            "Cultivation possible": c.cultivation_possible if c else None,
            "Cultivation": c.cultivation if c else None,
            "Finding tips": c.finding_tips if c else None,
            "Associated plant species": c.nearby_trees if c else None,
            "Curiosities": c.curiosities if c else None,
            "General description": c.general_description if c else None,
            "Colors": c.colors if c else None,
            "Mean basidiome height (cm)": c.size_cm if c else None,
            "Fruiting season start": c.season_start_month if c else None,
            "Fruiting season end": c.season_end_month if c else None,
            "Biogeographic regions of occurrence": [f"({d.slug}) {d.label_en}" for d in s.distributions],
            "Similar species (LUMM_ID)": [_id(link.similar_species_id) for link in s.similar_species_links],
        },
    }


def collect_data() -> tuple[list[dict], list[dict]]:
    """Executa uma única query e retorna os dados em PT e EN."""
    species_list = _query_species()
    data_pt = [_build_row_pt(s) for s in species_list]
    data_en = [_build_row_en(s) for s in species_list]
    return data_pt, data_en


def build_files(data: list[dict], lang: str, version: int, generated_at: datetime) -> tuple[bytes, bytes]:
    """Gera os bytes do XLSX e JSON para um idioma."""
    if not data:
        raise ValueError("Nenhum dado para gerar os arquivos")

    taxonomy_key = "Taxonomia" if lang == "pt" else "Taxonomy"
    characteristics_key = "Características" if lang == "pt" else "Characteristics"

    df_species = _fix_int_columns(pd.DataFrame([
        {k: v for k, v in row.items() if k not in (taxonomy_key, characteristics_key)}
        for row in data
    ]))

    df_taxonomy = _fix_int_columns(pd.DataFrame([
        {"LUMM_ID": row["LUMM_ID"], **row[taxonomy_key]}
        for row in data
    ]))

    df_characteristics = _fix_int_columns(pd.DataFrame([
        {"LUMM_ID": row["LUMM_ID"], **{k: _fmt(v) for k, v in row[characteristics_key].items()}}
        for row in data
    ]))

    # XLSX — 3 sheets
    xlsx_buffer = io.BytesIO()
    with pd.ExcelWriter(xlsx_buffer, engine="openpyxl") as writer:
        sheet_names = ("Espécies", "Taxonomia", "Características") if lang == "pt" else ("Species", "Taxonomy", "Characteristics")
        df_species.to_excel(writer, index=False, sheet_name=sheet_names[0])
        df_taxonomy.to_excel(writer, index=False, sheet_name=sheet_names[1])
        df_characteristics.to_excel(writer, index=False, sheet_name=sheet_names[2])
    xlsx_bytes = xlsx_buffer.getvalue()

    # JSON — estrutura aninhada completa
    payload = {
        "version": version,
        "lang": lang,
        "generated_at": generated_at.isoformat(),
        "total_records": len(data),
        "data": data,
    }
    json_bytes = json.dumps(payload, ensure_ascii=False, indent=2).encode("utf-8")

    return xlsx_bytes, json_bytes


def upload(client: Minio, bucket: str, key: str, body: bytes, content_type: str) -> None:
    client.put_object(bucket, key, io.BytesIO(body), len(body), content_type=content_type)
    _log(f"Upload concluído: s3://{bucket}/{key}", "OK")


def upload_lang(client, data: list[dict], lang: str, version: int, now: datetime) -> None:
    base = f"v{version}/{lang}/{FILE_PREFIX}"
    _log(f"Gerando arquivos [{lang.upper()}]...")
    xlsx_bytes, json_bytes = build_files(data, lang, version, now)
    upload(client, REPORT_BUCKET, f"{base}.xlsx", xlsx_bytes, "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
    upload(client, REPORT_BUCKET, f"{base}.json", json_bytes, "application/json")
    _log(f"Snapshot {base} concluído", "OK")


def main() -> None:
    _log("=== lumm snapshot: início ===")

    now = datetime.now(UTC)
    version = int(os.getenv("LUMM_DB_VERSION", "1"))

    with app.app_context():
        _log("Coletando dados...")
        data_pt, data_en = collect_data()
        _log(f"Registros coletados: {len(data_pt)}", "OK")

    _log("Conectando ao MinIO...")
    client = _get_minio_client()
    _log(f"Versão: v{version}", "OK")

    upload_lang(client, data_pt, "pt", version, now)
    upload_lang(client, data_en, "en", version, now)

    _log("=== lumm_db snapshot: fim ===")


if __name__ == "__main__":
    main()
