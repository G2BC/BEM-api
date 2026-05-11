"""
Sincroniza registros georreferenciados do BOLD para espécies com scientific_name cadastrado.

- Processa espécies agrupadas pelo campo `bem` (BEM1, BEM2, ..., P1, P2)
- Checkpoint por run no Redis (bem:sync:bold:done:{data}) — retoma de onde parou no mesmo dia
- Lock distribuído (bem:sync:bold:lock) — evita execução paralela
- Pausa de BOLD_GROUP_PAUSE_SECONDS entre grupos
- Para se grupo inteiro falhar ou se IP for bloqueado (403 sem recuperação)
- Full sync por padrão, limitado por BOLD_MAX_RECORDS_PER_SPECIES
- UPSERT via INSERT ... ON CONFLICT DO UPDATE
- Reconcilia observações removendo, no final, o que não veio mais da API
- Paginação via /api/documents/{query_id}
- Kill switch por MAX_RUNTIME_SECONDS
"""

import hashlib
import os
import re
import sys
import time
from datetime import date, datetime, timezone
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
MAX_RUNTIME_SECONDS = int(os.getenv("BOLD_MAX_RUNTIME_SECONDS", "3540"))
MAX_RETRIES = int(os.getenv("BOLD_MAX_RETRIES", "3"))
MAX_RECORDS_PER_SPECIES = int(os.getenv("BOLD_MAX_RECORDS_PER_SPECIES", "20000"))
BOLD_GEO_QUERY = os.getenv("BOLD_GEO_QUERY", "geo:country:Brazil").strip()
BOLD_SKIP_PREPROCESSOR = os.getenv("BOLD_SKIP_PREPROCESSOR", "").strip().lower() in {
    "1",
    "true",
    "yes",
}
GROUP_PAUSE_SECONDS = int(os.getenv("BOLD_GROUP_PAUSE_SECONDS", "180"))
REDIS_URL = os.getenv("REDIS_URL", "").strip()
CHECKPOINT_TTL = 60 * 60 * 25  # 25 horas — cobre uma re-execução no mesmo dia
LOCK_KEY = "bem:sync:bold:lock"
LOCK_TTL = MAX_RUNTIME_SECONDS + 300

HEADERS = {"User-Agent": "BEM-api/1.0 (bem.uneb.br; contact: bem.g2bc@gmail.com)"}

app = create_app()


class BoldNoTaxonMatch(RuntimeError):
    pass


class BoldBlocked(RuntimeError):
    pass


def _log(msg):
    print(msg, flush=True)


def _normalize_text(value):
    return " ".join(str(value or "").strip().lower().split())


# ---------------------------------------------------------------------------
# Redis checkpoint helpers
# ---------------------------------------------------------------------------

def _get_redis():
    if not REDIS_URL:
        return None
    try:
        import redis
        client = redis.from_url(REDIS_URL, decode_responses=True, socket_timeout=3)
        client.ping()
        return client
    except Exception as exc:
        _log(f"[AVISO] Redis indisponível — checkpoint desativado: {exc}")
        return None


def _acquire_lock(r):
    if not r:
        return True
    return bool(r.set(LOCK_KEY, "1", nx=True, ex=LOCK_TTL))


def _release_lock(r):
    if r:
        r.delete(LOCK_KEY)


def _done_key(run_date):
    return f"bem:sync:bold:done:{run_date}"


def _failed_key(run_date):
    return f"bem:sync:bold:failed:{run_date}"


def _is_done(r, run_date, species_id):
    if not r:
        return False
    try:
        return bool(r.sismember(_done_key(run_date), str(species_id)))
    except Exception:
        return False


def _mark_done(r, run_date, species_id):
    if not r:
        return
    try:
        key = _done_key(run_date)
        r.sadd(key, str(species_id))
        r.expire(key, CHECKPOINT_TTL)
    except Exception:
        pass


def _mark_failed(r, run_date, species_id):
    if not r:
        return
    try:
        key = _failed_key(run_date)
        r.sadd(key, str(species_id))
        r.expire(key, CHECKPOINT_TTL)
    except Exception:
        pass


# ---------------------------------------------------------------------------
# Group ordering: BEM1 < BEM2 < ... < BEM10 < P1 < P2
# ---------------------------------------------------------------------------

def _group_sort_key(name):
    m = re.match(r"^([A-Za-z]+)(\d+)$", name or "")
    if m:
        return (m.group(1), int(m.group(2)))
    return (name or "", 0)


# ---------------------------------------------------------------------------
# BOLD API helpers
# ---------------------------------------------------------------------------

def _request_json(path, params):
    url = f"{BOLD_API_URL}{path}"
    last_error = None
    for attempt in range(MAX_RETRIES):
        try:
            response = requests.get(url, params=params, headers=HEADERS, timeout=REQUEST_TIMEOUT)
            if response.status_code == 403:
                if attempt + 1 < MAX_RETRIES:
                    wait = 2 ** attempt * 30
                    _log(f"  403 do BOLD — aguardando {wait}s (tentativa {attempt + 1}/{MAX_RETRIES})")
                    time.sleep(wait)
                    continue
                raise BoldBlocked("BOLD retornou HTTP 403 após todas as tentativas; IP provavelmente bloqueado")
            if response.status_code == 429:
                wait = 2 ** attempt * 10
                _log(f"  429 recebido do BOLD — aguardando {wait}s")
                time.sleep(wait)
                continue
            if response.status_code >= 500:
                wait = 2 ** attempt * 5
                _log(
                    f"  BOLD HTTP {response.status_code} — aguardando {wait}s "
                    f"(tentativa {attempt + 1}/{MAX_RETRIES})"
                )
                time.sleep(wait)
                continue
            response.raise_for_status()
            return response.json()
        except BoldBlocked:
            raise
        except requests.RequestException as exc:
            last_error = exc
            if attempt + 1 < MAX_RETRIES:
                time.sleep(2 ** attempt * 5)
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
    has_geo_term = False

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
            has_geo_term = True

    if not has_tax_term:
        raise BoldNoTaxonMatch(f"BOLD não resolveu tax:species para {scientific_name!r}")

    if not resolved:
        return submitted_query

    if BOLD_GEO_QUERY and not has_geo_term:
        resolved.append(BOLD_GEO_QUERY)

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
        chunk = stale_ids[i : i + 1000]
        deleted += (
            db.session.query(Observation)
            .filter(
                Observation.species_id == species_id,
                Observation.source == "bold",
                Observation.external_id.in_(chunk),
            )
            .delete(synchronize_session=False)
        )
    db.session.commit()
    return deleted


_BRAZIL_BBOX = (-33.75, -73.99, 5.27, -28.85)  # (lat_min, lon_min, lat_max, lon_max)
_BRAZIL_COUNTRY_NAMES = {"brazil", "brasil", "br"}


def _country_from_document(document):
    value = _first_value(
        document,
        (
            "collection.country",
            "country",
            "geo.country",
            "location.country",
            "collection.country_ocean",
            "country_ocean",
        ),
    )
    return _normalize_text(_text_value(value) or "")


def _build_row(species_id, scientific_name, document):
    """Returns (row_dict, None) on success or (None, reason_str) on rejection."""
    country = _country_from_document(document)
    if country:
        if country not in _BRAZIL_COUNTRY_NAMES:
            return None, f"país não é Brasil ({country!r})"
    else:
        # País ausente no documento — usa bounding box como fallback
        lat, lng = _extract_coordinates(document)
        if lat is None or lng is None:
            return None, "sem coordenadas e sem país"
        if not (-90 <= lat <= 90 and -180 <= lng <= 180):
            return None, f"coordenadas inválidas ({lat}, {lng})"
        lat_min, lon_min, lat_max, lon_max = _BRAZIL_BBOX
        if not (lat_min <= lat <= lat_max and lon_min <= lng <= lon_max):
            return None, f"fora do Brasil pelo bbox ({lat}, {lng})"

    lat, lng = _extract_coordinates(document)
    if lat is None or lng is None:
        return None, "sem coordenadas"

    if not (-90 <= lat <= 90 and -180 <= lng <= 180):
        return None, f"coordenadas inválidas ({lat}, {lng})"

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
        return None, f"espécie divergente (BOLD={bold_species!r} vs esperado={scientific_name!r})"

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
    }, None


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

            skip_counts: dict[str, int] = {}
            for document in documents:
                if not isinstance(document, dict):
                    skip_counts["documento inválido"] = skip_counts.get("documento inválido", 0) + 1
                    continue
                row, reason = _build_row(species_id, scientific_name, document)
                if row is None:
                    skip_counts[reason] = skip_counts.get(reason, 0) + 1
                    continue
                rows_by_external_id[row["external_id"]] = row
                seen_external_ids.add(row["external_id"])

            rows = list(rows_by_external_id.values())
            total += _upsert(rows)

            page = start // PAGE_SIZE + 1
            skip_summary = (
                " | recusados: " + ", ".join(f"{r}={n}" for r, n in sorted(skip_counts.items()))
                if skip_counts
                else ""
            )
            _log(
                f"  [{species_id}] página {page}: {len(documents)} docs, "
                f"{len(rows)} válidos (total BOLD: {records_total}){skip_summary}"
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


def _process_group(group_name, species_rows, run_date, r, start_time):
    """Processa um grupo de espécies sequencialmente.

    Retorna True se o job deve ser interrompido (IP bloqueado ou grupo inteiro falhou).
    """
    total = 0
    total_deleted = 0
    attempted = 0
    failed = 0

    for row in species_rows:
        if _is_done(r, run_date, row.id):
            _log(f"  [{row.id}] checkpoint — já processado hoje, pulando")
            continue

        if time.time() - start_time > MAX_RUNTIME_SECONDS:
            _log(f"  [{row.id}] Kill switch — abortando")
            break

        attempted += 1

        try:
            inserted, deleted, completed = _sync_species(row.id, row.scientific_name, start_time)
            total += inserted
            total_deleted += deleted
            _mark_done(r, run_date, row.id)
            _log(f"[OK] {group_name} species={row.id} upsert={inserted} removidos={deleted}")
        except BoldBlocked as exc:
            _log(f"[BLOQUEADO] {group_name} species={row.id}: {exc}")
            _mark_failed(r, run_date, row.id)
            return True  # IP bloqueado — para tudo
        except Exception as exc:
            failed += 1
            _log(f"[ERRO] {group_name} species={row.id}: {exc}")
            _mark_failed(r, run_date, row.id)

    _log(
        f"Grupo {group_name}: upsert={total} removidos={total_deleted} "
        f"falhas={failed}/{attempted}"
    )

    return attempted > 0 and failed == attempted  # grupo inteiro falhou


def main():
    start_time = time.time()
    run_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    raw_bem_ids = os.getenv("BEM_ID", "")
    bem_ids = [int(v) for raw in raw_bem_ids.split(",") if (v := raw.strip()).isdigit()]

    _log("=== Sync BOLD ===")
    _log(
        f"page_size={PAGE_SIZE} | limite={MAX_RUNTIME_SECONDS}s | "
        f"max_records_species={MAX_RECORDS_PER_SPECIES} | pausa_entre_grupos={GROUP_PAUSE_SECONDS}s"
    )
    if BOLD_GEO_QUERY:
        _log(f"geo_query={BOLD_GEO_QUERY}")

    r = _get_redis()

    if not _acquire_lock(r):
        _log("[ABORT] Outra execução já está em andamento (lock Redis ativo)")
        sys.exit(1)

    try:
        with app.app_context():
            query = db.session.query(Species.id, Species.scientific_name, Species.bem).filter(
                Species.scientific_name.isnot(None)
            )
            if bem_ids:
                query = query.filter(Species.id.in_(bem_ids))
            species_rows = query.all()

        _log(f"Espécies: {len(species_rows)}")

        if bem_ids:
            # Modo manual: processa IDs específicos sem agrupamento
            _log("Modo manual — sem agrupamento por bem")
            _process_group("manual", species_rows, run_date, r, start_time)
        else:
            # Agrupa por campo `bem` e ordena naturalmente
            groups: dict[str, list] = {}
            for row in species_rows:
                group = row.bem or "sem_grupo"
                groups.setdefault(group, []).append(row)

            sorted_group_names = sorted(groups.keys(), key=_group_sort_key)
            _log(f"Grupos: {sorted_group_names}")

            for i, group_name in enumerate(sorted_group_names):
                if time.time() - start_time > MAX_RUNTIME_SECONDS:
                    _log("[KILL SWITCH] Tempo máximo atingido")
                    break

                group_species = groups[group_name]
                _log(f"\n=== Grupo {group_name} ({len(group_species)} espécies) ===")

                should_stop = _process_group(group_name, group_species, run_date, r, start_time)

                if should_stop:
                    _log(f"[STOP] Grupo {group_name} falhou completamente — encerrando sync")
                    break

                if i < len(sorted_group_names) - 1:
                    _log(f"Pausa de {GROUP_PAUSE_SECONDS}s antes do próximo grupo...")
                    time.sleep(GROUP_PAUSE_SECONDS)

    finally:
        _release_lock(r)

    _log(f"Tempo total: {int(time.time() - start_time)}s")


if __name__ == "__main__":
    main()
