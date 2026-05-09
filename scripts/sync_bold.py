"""
Sincroniza registros georreferenciados do BOLD para espécies com scientific_name cadastrado.

- Full sync por padrão, limitado por BOLD_MAX_RECORDS_PER_SPECIES
- UPSERT via INSERT ... ON CONFLICT DO UPDATE
- Reconcilia observações removendo, no final, o que não veio mais da API
- Paginação via /api/documents/{query_id}
- ThreadPoolExecutor com BOLD_MAX_WORKERS threads
- Kill switch por MAX_RUNTIME_SECONDS
"""

import hashlib
import os
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date
from pathlib import Path
from urllib.parse import quote

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

BOLD_API_URL = os.getenv("BOLD_API_URL", "https://portal.boldsystems.org/api").rstrip("/")
PAGE_SIZE = int(os.getenv("BOLD_PAGE_SIZE", "500"))
REQUEST_TIMEOUT = float(os.getenv("BOLD_REQUEST_TIMEOUT", "45"))
SLEEP_BETWEEN_REQUESTS = float(os.getenv("BOLD_SLEEP_BETWEEN_REQUESTS", "1.0"))
MAX_WORKERS = int(os.getenv("BOLD_MAX_WORKERS", "2"))
MAX_RUNTIME_SECONDS = int(os.getenv("BOLD_MAX_RUNTIME_SECONDS", "3540"))
MAX_RETRIES = int(os.getenv("BOLD_MAX_RETRIES", "3"))
MAX_RECORDS_PER_SPECIES = int(os.getenv("BOLD_MAX_RECORDS_PER_SPECIES", "20000"))
BOLD_GEO_QUERY = os.getenv("BOLD_GEO_QUERY", "").strip()
BOLD_SKIP_PREPROCESSOR = os.getenv("BOLD_SKIP_PREPROCESSOR", "").strip().lower() in {
    "1",
    "true",
    "yes",
}
HEADERS = {"User-Agent": "LUMM-server/1.0 (lumm.uneb.br; contact: lumm.g2bc@gmail.com)"}

app = create_app()


class BoldNoTaxonMatch(RuntimeError):
    pass


class BoldBlocked(RuntimeError):
    pass


def _log(msg):
    print(msg, flush=True)


def _normalize_text(value):
    return " ".join(str(value or "").strip().lower().split())


def _request_json(path, params):
    url = f"{BOLD_API_URL}{path}"
    last_error = None
    for attempt in range(MAX_RETRIES):
        try:
            response = requests.get(url, params=params, headers=HEADERS, timeout=REQUEST_TIMEOUT)
            if response.status_code == 403:
                raise BoldBlocked(
                    "BOLD retornou HTTP 403; provável bloqueio temporário ou proteção anti-abuso"
                )
            if response.status_code == 429:
                wait = 2**attempt * 10
                _log(f"  429 recebido do BOLD — aguardando {wait}s")
                time.sleep(wait)
                continue
            if response.status_code >= 500:
                wait = 2**attempt * 5
                _log(
                    f"  BOLD HTTP {response.status_code} — aguardando {wait}s "
                    f"(tentativa {attempt + 1}/{MAX_RETRIES})"
                )
                time.sleep(wait)
                continue
            response.raise_for_status()
            return response.json()
        except requests.RequestException as exc:
            last_error = exc
            if attempt + 1 < MAX_RETRIES:
                time.sleep(2**attempt * 5)
                continue
            raise

    if last_error:
        raise last_error
    raise RuntimeError("Máximo de tentativas atingido")


def _value_from_path(document, path):
    current = document
    for part in path.split("."):
        if not isinstance(current, dict) or part not in current:
            return None
        current = current[part]
    return current


def _first_value(document, paths):
    for path in paths:
        value = _value_from_path(document, path)
        if value not in (None, ""):
            return value
    return None


def _text_value(value):
    if value in (None, ""):
        return None
    if isinstance(value, dict):
        for key in ("name", "value", "label", "matched", "id"):
            text = _text_value(value.get(key))
            if text:
                return text
        return None
    if isinstance(value, list):
        for item in value:
            text = _text_value(item)
            if text:
                return text
        return None
    return str(value).strip() or None


def _float_value(value):
    if value in (None, ""):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _extract_coordinates(document):
    coord = _first_value(document, ("coord", "coordinates", "collection.coord"))
    if isinstance(coord, dict):
        lat = _float_value(
            _first_value(
                coord,
                (
                    "lat",
                    "latitude",
                    "decimalLatitude",
                    "decimal_latitude",
                    "y",
                ),
            )
        )
        lng = _float_value(
            _first_value(
                coord,
                (
                    "lon",
                    "lng",
                    "long",
                    "longitude",
                    "decimalLongitude",
                    "decimal_longitude",
                    "x",
                ),
            )
        )
        return lat, lng
    if isinstance(coord, list) and len(coord) >= 2:
        first = _float_value(coord[0])
        second = _float_value(coord[1])
        if first is None or second is None:
            return None, None
        if abs(first) <= 90 and abs(second) <= 180:
            return first, second
        return second, first
    if isinstance(coord, str):
        parts = [part.strip() for part in coord.replace(";", ",").split(",")]
        if len(parts) >= 2:
            first = _float_value(parts[0])
            second = _float_value(parts[1])
            if first is None or second is None:
                return None, None
            if abs(first) <= 90 and abs(second) <= 180:
                return first, second
            return second, first

    lat = _float_value(
        _first_value(
            document,
            (
                "latitude",
                "lat",
                "decimalLatitude",
                "decimal_latitude",
                "collection.latitude",
                "collection.decimalLatitude",
            ),
        )
    )
    lng = _float_value(
        _first_value(
            document,
            (
                "longitude",
                "lng",
                "lon",
                "decimalLongitude",
                "decimal_longitude",
                "collection.longitude",
                "collection.decimalLongitude",
            ),
        )
    )
    return lat, lng


def _parse_date(value):
    text = _text_value(value)
    if not text:
        return None
    text = text[:10]
    try:
        return date.fromisoformat(text).isoformat()
    except ValueError:
        return None


def _external_id(document):
    value = _first_value(
        document,
        (
            "processid",
            "process_id",
            "ids.processid",
            "ids.process_id",
            "sampleid",
            "sample_id",
            "ids.sampleid",
            "ids.sample_id",
            "record_id",
            "id",
        ),
    )
    text = _text_value(value)
    if text:
        return text

    raw = repr(document).encode("utf-8", errors="ignore")
    return f"hash:{hashlib.sha256(raw).hexdigest()[:32]}"


def _record_url(external_id):
    return f"https://portal.boldsystems.org/record/{quote(external_id, safe='')}"


def _resolved_query(scientific_name):
    tokens = [f"tax:species:{scientific_name}"]
    if BOLD_GEO_QUERY:
        tokens.append(BOLD_GEO_QUERY)
    submitted_query = ";".join(tokens)

    if BOLD_SKIP_PREPROCESSOR:
        return submitted_query

    data = _request_json("/query/preprocessor", {"query": submitted_query})
    terms = data.get("successful_terms") or []
    resolved = []
    has_tax_term = False

    for term in terms:
        if not isinstance(term, dict):
            continue
        matched = _text_value(term.get("matched"))
        if not matched or matched.count(":") < 2:
            continue

        if matched.startswith("tax:"):
            resolved.append(matched)
            has_tax_term = True
            continue

        if matched.startswith("geo:"):
            resolved.append(matched)

    if not has_tax_term:
        raise BoldNoTaxonMatch(f"BOLD não resolveu tax:species para {scientific_name!r}")

    if not resolved:
        return submitted_query
    return ";".join(resolved)


def _query_id(query):
    data = _request_json("/query", {"query": query, "extent": "full"})
    query_id = data.get("query_id")
    if not query_id:
        raise RuntimeError(f"BOLD não retornou query_id para query={query!r}")
    return query_id


def _fetch_documents(query_id, start):
    return _request_json(f"/documents/{query_id}", {"length": PAGE_SIZE, "start": start})


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


def _delete_stale_observations(species_id, seen_external_ids):
    current_ids = {
        external_id
        for (external_id,) in db.session.query(Observation.external_id).filter_by(
            species_id=species_id,
            source="bold",
        )
    }
    stale_ids = current_ids - seen_external_ids
    if not stale_ids:
        return 0

    deleted = 0
    stale_ids = list(stale_ids)
    for i in range(0, len(stale_ids), 1000):
        chunk = stale_ids[i:i + 1000]
        deleted += db.session.query(Observation).filter(
            Observation.species_id == species_id,
            Observation.source == "bold",
            Observation.external_id.in_(chunk),
        ).delete(synchronize_session=False)
    db.session.commit()
    return deleted


def _build_row(species_id, scientific_name, document):
    lat, lng = _extract_coordinates(document)
    if lat is None or lng is None:
        return None

    if not (-90 <= lat <= 90 and -180 <= lng <= 180):
        return None

    bold_species = _text_value(
        _first_value(
            document,
            (
                "species",
                "taxonomy.species",
                "identification.species",
                "tax.species",
            ),
        )
    )
    if bold_species and _normalize_text(bold_species) != _normalize_text(scientific_name):
        return None

    external_id = _external_id(document)
    quality_parts = [
        _text_value(_first_value(document, ("marker_code", "marker.code", "marker"))),
        _text_value(_first_value(document, ("bin_uri", "bin.uri"))),
        _text_value(_first_value(document, ("inst", "institution", "institution_storing"))),
    ]
    quality_grade = " | ".join(part for part in quality_parts if part) or None

    return {
        "species_id": species_id,
        "source": "bold",
        "external_id": external_id,
        "latitude": round(lat, 6),
        "longitude": round(lng, 6),
        "location_obscured": False,
        "observed_on": _parse_date(
            _first_value(
                document,
                (
                    "collection_date_start",
                    "collection.date_start",
                    "eventDate",
                    "event_date",
                    "collection_date",
                ),
            )
        ),
        "quality_grade": quality_grade,
        "photo_url": None,
        "url": _record_url(external_id),
    }


def _sync_species(species_id, scientific_name, start_time):
    with app.app_context():
        seen_external_ids = set()
        total = 0
        start = 0
        completed = False

        try:
            query = _resolved_query(scientific_name)
        except BoldNoTaxonMatch as exc:
            _log(f"  [{species_id}] {exc}; ignorando espécie")
            db.session.remove()
            return 0, 0, True

        query_id = _query_id(query)
        _log(f"  [{species_id}] query={query}")

        while True:
            if time.time() - start_time > MAX_RUNTIME_SECONDS:
                _log(f"  [{species_id}] Kill switch — abortando")
                return total, 0, False

            if start >= MAX_RECORDS_PER_SPECIES:
                _log(
                    f"  [{species_id}] limite BOLD_MAX_RECORDS_PER_SPECIES="
                    f"{MAX_RECORDS_PER_SPECIES} atingido; sem reconciliação de antigos"
                )
                return total, 0, True

            data = _fetch_documents(query_id, start)
            documents = data.get("data") or []
            records_total = data.get("recordsTotal")
            rows_by_external_id = {}

            for document in documents:
                if not isinstance(document, dict):
                    continue
                row = _build_row(species_id, scientific_name, document)
                if not row:
                    continue
                rows_by_external_id[row["external_id"]] = row
                seen_external_ids.add(row["external_id"])

            rows = list(rows_by_external_id.values())
            total += _upsert(rows)

            page = start // PAGE_SIZE + 1
            _log(
                f"  [{species_id}] página {page}: {len(documents)} docs, "
                f"{len(rows)} válidos (total BOLD: {records_total})"
            )

            start += PAGE_SIZE
            if not documents or len(documents) < PAGE_SIZE:
                completed = True
                break
            if records_total is not None and start >= int(records_total):
                completed = True
                break

            time.sleep(SLEEP_BETWEEN_REQUESTS)

        deleted = _delete_stale_observations(species_id, seen_external_ids) if completed else 0

        prefix = app.config.get("OBSERVATIONS_CACHE_PREFIX", "observations")
        CacheService.delete(f"{prefix}:{species_id}:all")
        CacheService.delete(f"{prefix}:{species_id}:bold")

        db.session.remove()
        return total, deleted, completed


def main():
    start_time = time.time()

    raw_lumm_ids = os.getenv("LUMM_ID", "")
    lumm_ids = [int(v) for raw in raw_lumm_ids.split(",") if (v := raw.strip()).isdigit()]

    _log("=== Sync BOLD ===")
    _log(
        f"workers={MAX_WORKERS} | page_size={PAGE_SIZE} | "
        f"limite={MAX_RUNTIME_SECONDS}s | max_records_species={MAX_RECORDS_PER_SPECIES}"
    )
    if BOLD_GEO_QUERY:
        _log(f"geo_query={BOLD_GEO_QUERY}")

    with app.app_context():
        query = db.session.query(Species.id, Species.scientific_name).filter(
            Species.scientific_name.isnot(None)
        )
        if lumm_ids:
            query = query.filter(Species.id.in_(lumm_ids))
        species_rows = query.all()

    _log(f"Espécies: {len(species_rows)}")

    total = 0
    total_deleted = 0
    errors = 0

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
        futures = {
            pool.submit(_sync_species, row.id, row.scientific_name, start_time): row.id
            for row in species_rows
        }
        for future in as_completed(futures):
            species_id = futures[future]
            try:
                inserted, deleted, completed = future.result()
                total += inserted
                total_deleted += deleted
                _log(f"[OK] species={species_id} upsert={inserted} removidos={deleted}")
                if not completed:
                    pool.shutdown(wait=False, cancel_futures=True)
                    break
            except BoldBlocked as e:
                errors += 1
                _log(f"[BLOQUEADO] species={species_id}: {e}")
                pool.shutdown(wait=False, cancel_futures=True)
                break
            except Exception as e:
                errors += 1
                _log(f"[ERRO] species={species_id}: {e}")

    _log(
        f"Total upsert: {total} | Removidos: {total_deleted} | "
        f"Erros: {errors} | Tempo: {int(time.time() - start_time)}s"
    )


if __name__ == "__main__":
    main()
