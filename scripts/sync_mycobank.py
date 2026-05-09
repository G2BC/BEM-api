import math
import os
import re
import shutil
import sys
from pathlib import Path
from zipfile import ZipFile

import pandas as pd
import requests

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from app import create_app  # noqa: E402
from app.extensions import db  # noqa: E402
from app.models.species import Species  # noqa: E402
from app.models.taxon import Taxon  # noqa: E402

MBLIST_URL = "https://www.MycoBank.org/images/MBList.zip"
MBLIST_SHEET = "Sheet1"

app = create_app()

TAXONOMY_FIELDS = (
    "kingdom",
    "phylum",
    "class_name",
    "order",
    "family",
    "genus",
    "specific_epithet",
)

def _txt(v):
    if v in (None, "") or (isinstance(v, float) and math.isnan(v)):
        return None

    value = str(v).strip()
    return value or None


def _i(v):
    if v in (None, "") or (isinstance(v, float) and math.isnan(v)):
        return None
    try:
        return int(float(str(v).strip()))
    except Exception:
        return None


def _log(message: str, level: str = "INFO") -> None:
    print(f"[{level}] {message}")


def parse_classification_raw(value: str | None) -> dict[str, str | None]:
    classification = _txt(value)
    if not classification:
        return {}

    parts = [
        part
        for part in (normalize_text(part) for part in re.split(r"\s*[;|,]\s*", classification))
        if part
    ]

    if not parts:
        return {}

    parsed = {"kingdom": parts[0]}
    lineage_parts = parts[1:]

    if len(parts) > 2 and parts[-1][:1].islower() and not _is_rank_name(parts[-1]):
        parsed["genus"] = parts[-2]
        parsed["specific_epithet"] = parts[-1]
        lineage_parts = parts[1:-2]
    elif len(parts) > 1:
        parsed["genus"] = parts[-1]
        lineage_parts = parts[1:-1]

    for part in lineage_parts:
        if part.endswith("mycota"):
            parsed["phylum"] = part
        elif part.endswith("mycetes"):
            parsed["class_name"] = part
        elif part.endswith("ales"):
            parsed["order"] = part
        elif part.endswith("aceae"):
            parsed["family"] = part

    return parsed


def _is_rank_name(value: str) -> bool:
    return value.endswith(("mycota", "mycetes", "ales", "aceae"))


def parse_specific_epithet(taxon_name: str | None) -> str | None:
    value = _txt(taxon_name)
    if not value:
        return None

    parts = [part for part in re.split(r"\s+", value) if part]
    if len(parts) < 2:
        return None

    candidate = parts[1]
    if candidate in {"subsp.", "ssp.", "var.", "f.", "forma"}:
        return None
    if not re.match(r"^[a-z][a-z-]*$", candidate):
        return None

    return candidate


def download_and_read_mblist_filtered(
    mb_ids: set[int],
    url: str = MBLIST_URL,
    pasta_base: str = "data",
    sheet_name: str = MBLIST_SHEET,
) -> tuple[pd.DataFrame, Path]:
    base_dir = Path(pasta_base)
    zip_path = base_dir / "MBList.zip"
    extract_dir = base_dir / "mblist"
    xlsx_path = extract_dir / "MBList.xlsx"

    base_dir.mkdir(parents=True, exist_ok=True)
    extract_dir.mkdir(parents=True, exist_ok=True)

    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/122.0.0.0 Safari/537.36"
        ),
        "Accept": "application/zip,application/octet-stream,*/*",
        "Referer": "https://www.mycobank.org/",
    }

    _log(f"Baixando MBList de {url}")
    response = requests.get(url, headers=headers, timeout=120)
    response.raise_for_status()
    _log("MBList baixado com sucesso", "OK")
    zip_path.write_bytes(response.content)

    with ZipFile(zip_path, "r") as zip_ref:
        zip_ref.extractall(extract_dir)

    if not xlsx_path.exists():
        raise FileNotFoundError(f"Arquivo não encontrado: {xlsx_path}")

    usecols = [
        "MycoBank #",
        "Current MycoBank #",
        "Classification",
        "Synonymy",
        "Authors",
        "Year of effective publication",
        "Taxon name",
    ]

    df = pd.read_excel(
        xlsx_path,
        sheet_name=sheet_name,
        usecols=usecols,
        engine="openpyxl",
    )

    before = len(df)

    df["MycoBank #"] = df["MycoBank #"].apply(_i)
    df["Current MycoBank #"] = df["Current MycoBank #"].apply(_i)

    df = df[df["MycoBank #"].isin(mb_ids)]

    df = df.rename(
        columns={
            "MycoBank #": "mb_id",
            "Current MycoBank #": "current_mb_id",
            "Classification": "classification_raw",
            "Synonymy": "synonyms",
            "Authors": "authors",
            "Year of effective publication": "year",
            "Taxon name": "taxon_name",
        }
    )

    _log(f"XLSX filtrado: {len(df)}/{before} linhas correspondentes", "OK")

    return df, xlsx_path


def parse_basionym_and_synonyms(text: str) -> tuple[str | None, str | None]:
    if not text:
        return None, None

    basionym_match = re.search(r"Basionym:\s*(.*?\[MB#\d+\])", text)
    basionym = basionym_match.group(1).strip() if basionym_match else None

    synonyms_list = [
        item.strip()
        for item in re.findall(r"-\s+(.*?\[MB#\d+\])", text)
        if item.strip() and not item.strip().startswith("Current name:")
    ]

    synonyms = "\n".join(synonyms_list) if synonyms_list else None

    return basionym, synonyms


def normalize_text(value):
    if value is None:
        return None

    value = value.strip()
    return value or None


def sync_text_field(target, field_name: str, value: str | None) -> bool:
    if value != getattr(target, field_name):
        setattr(target, field_name, value)
        return True
    return False


def parse_csv_values(value: str | None) -> list[str]:
    return [part for raw in (value or "").split(",") if (part := raw.strip())]


def main():
    bem_ids = [v for raw in (os.environ.get("BEM_ID") or "").split(",") if (v := _i(raw.strip()))]
    bem_codes = parse_csv_values(os.environ.get("BEM"))

    _log("=== Sync MycoBank: inicio ===")
    if bem_ids:
        _log(f"Modo individual: BEM_IDs={bem_ids}")
    if bem_codes:
        _log(f"Modo individual: BEMs={bem_codes}")
    _log("Carregando chaves do banco")
    with app.app_context():
        query = db.session.query(
            Species.id,
            Species.bem,
            Species.scientific_name,
            Species.mycobank_index_fungorum_id,
            Species.is_outdated_mycobank,
        ).filter(Species.mycobank_index_fungorum_id.isnot(None))

        if bem_ids:
            query = query.filter(Species.id.in_(bem_ids))
        if bem_codes:
            query = query.filter(Species.bem.in_(bem_codes))

        species_rows = query.all()

    mb_ids = {
        _i(r.mycobank_index_fungorum_id)
        for r in species_rows
        if _i(r.mycobank_index_fungorum_id)
    }
    _log(f"Especies com MycoBank ID no banco: {len(mb_ids)}", "OK")

    _log("=== Coleta do MBList ===")
    _log("Baixando e lendo MBList")
    df, xlsx_path = download_and_read_mblist_filtered(mb_ids=mb_ids, pasta_base="/tmp/mycobank")
    _log(f"XLSX utilizado: {xlsx_path}", "OK")

    inserted = 0
    updated = 0
    linked = 0

    with app.app_context():
        _log("=== Sincronizacao no banco ===")
        species_by_mb = {
            _i(r.mycobank_index_fungorum_id): r
            for r in species_rows
            if _i(r.mycobank_index_fungorum_id)
        }
        total_rows = len(df)

        for idx, row in enumerate(df.to_dict(orient="records"), start=1):
            mb_id = _i(row.get("mb_id"))
            current_mb_id = _i(row.get("current_mb_id"))

            if not mb_id:
                continue

            srow = species_by_mb.get(mb_id)
            if not srow:
                continue

            taxon = Taxon.query.filter_by(species_id=srow.id).one_or_none()
            row_changed = False

            if not taxon:
                taxon = Taxon(species_id=srow.id)
                db.session.add(taxon)
                inserted += 1
                row_changed = True

            if (val := _txt(row.get("taxon_name"))) and val != srow.scientific_name:
                db.session.query(Species).filter_by(id=srow.id).update(
                    {"scientific_name": val},
                    synchronize_session=False,
                )
                row_changed = True

            classification_raw = _txt(row.get("classification_raw"))
            if classification_raw and classification_raw != taxon.classification_raw:
                taxon.classification_raw = classification_raw
                row_changed = True

            parsed_classification = parse_classification_raw(classification_raw)
            if specific_epithet := parse_specific_epithet(row.get("taxon_name")):
                parsed_classification["specific_epithet"] = specific_epithet

            for field in TAXONOMY_FIELDS:
                if field not in parsed_classification:
                    continue
                val = parsed_classification[field]
                if sync_text_field(taxon, field, val):
                    row_changed = True

            raw_synonyms = _txt(row.get("synonyms"))
            if raw_synonyms:
                basionym, synonyms = parse_basionym_and_synonyms(raw_synonyms)
            else:
                basionym, synonyms = None, None

            if basionym != taxon.basionym:
                taxon.basionym = basionym
                row_changed = True

            if synonyms != taxon.synonyms:
                taxon.synonyms = synonyms
                row_changed = True

            if (val := _txt(row.get("authors"))) and val != taxon.authors:
                taxon.authors = val
                row_changed = True

            if (val := _txt(row.get("year"))) and val != taxon.years_of_effective_publication:
                taxon.years_of_effective_publication = val
                row_changed = True

            is_outdated = (
                current_mb_id is not None
                and current_mb_id != _i(srow.mycobank_index_fungorum_id)
            )

            if is_outdated != srow.is_outdated_mycobank:
                db.session.query(Species).filter_by(id=srow.id).update(
                    {"is_outdated_mycobank": is_outdated},
                    synchronize_session=False,
                )
                row_changed = True

            if taxon.id is not None and row_changed:
                updated += 1

            linked += 1
            print(
                f"[{idx:>5}/{total_rows:<5}] "
                f"species_id={srow.id} "
                f"MycoBank={mb_id} "
                f"Current={current_mb_id} "
                f"outdated={is_outdated}"
            )

        db.session.commit()
    _log("Sincronizacao finalizada", "OK")

    base_dir = Path("/tmp/mycobank")
    shutil.rmtree(base_dir, ignore_errors=True)
    _log("Arquivos temporarios removidos", "OK")


if __name__ == "__main__":
    main()
