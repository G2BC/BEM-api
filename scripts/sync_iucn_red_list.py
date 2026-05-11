"""
Sincroniza dados da IUCN Red List para espécies cadastradas no BEM.

Atualiza:
- species.iucn_redlist com o assessment_id mais recente
- species_characteristics.conservation_status com a categoria IUCN
- species_characteristics.iucn_assessment_year e iucn_assessment_url
"""

import os
import sys
import time
from pathlib import Path

import requests

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from app import create_app  # noqa: E402
from app.extensions import db  # noqa: E402
from app.models.species import Species  # noqa: E402
from app.models.species_characteristics import SpeciesCharacteristics  # noqa: E402

app = create_app()

def _i(value):
    if value in (None, ""):
        return None
    try:
        return int(str(value).strip())
    except (TypeError, ValueError):
        return None


def _log(message: str, level: str = "INFO") -> None:
    print(f"[{level}] {message}")


def parse_csv_values(value: str | None) -> list[str]:
    return [part for raw in (value or "").split(",") if (part := raw.strip())]


def _response_error_message(response: requests.Response) -> str:
    body = (response.text or "").strip().replace("\n", " ")
    if len(body) > 300:
        body = f"{body[:300]}..."
    return f"HTTP {response.status_code}" + (f" | {body}" if body else "")


def _is_cloudflare_challenge(response: requests.Response) -> bool:
    server = response.headers.get("server", "").lower()
    body = (response.text or "").lower()
    return (
        response.status_code == 403
        and ("cloudflare" in server or "just a moment" in body or "cf-ray" in response.headers)
    )


def _request_headers(api_key: str) -> dict[str, str]:
    return {
        "authorization": api_key,
        "accept": "application/json",
        "accept-language": "en-US,en;q=0.9",
        "referer": "https://api.iucnredlist.org/api-docs/index.html",
        "sec-fetch-dest": "empty",
        "sec-fetch-mode": "cors",
        "sec-fetch-site": "same-origin",
        "user-agent": (
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/148.0.0.0 Safari/537.36"
        ),
    }


def _request_cookies() -> dict[str, str]:
    cf_clearance = os.getenv("IUCN_CF_CLEARANCE", "").strip()
    if not cf_clearance:
        return {}
    return {"cf_clearance": cf_clearance}


def main():
    bem_ids = [v for raw in (os.environ.get("BEM_ID") or "").split(",") if (v := _i(raw.strip()))]
    bem_codes = parse_csv_values(os.environ.get("BEM"))
    api_key = os.getenv("IUCN_API_KEY")

    _log("=== Import IUCN Red List: inicio ===")
    if bem_ids:
        _log(f"Modo individual: BEM_IDs={bem_ids}")
    if bem_codes:
        _log(f"Modo individual: BEMs={bem_codes}")

    if not api_key:
        raise RuntimeError("IUCN_API_KEY nao configurada")

    if not os.getenv("IUCN_CF_CLEARANCE", "").strip():
        _log("IUCN_CF_CLEARANCE nao informado — requisicoes podem ser bloqueadas pelo Cloudflare", "AVISO")

    with app.app_context():
        query = Species.query.filter(Species.scientific_name.isnot(None))

        if bem_ids:
            query = query.filter(Species.id.in_(bem_ids))
        if bem_codes:
            query = query.filter(Species.bem.in_(bem_codes))

        species_list = query.all()
        total = len(species_list)

        updated = 0
        invalid_name = 0
        api_errors = 0
        invalid_response = 0
        no_latest_assessment = 0

        _log(f"Especies carregadas: {total}", "OK")

        for idx, species in enumerate(species_list, start=1):
            _log(f"[{idx}/{total}] Buscando: {species.scientific_name}")

            name_parts = (species.scientific_name or "").split()
            if len(name_parts) < 2:
                invalid_name += 1
                _log(
                    f"[{idx}/{total}] {species.scientific_name} - nome cientifico invalido",
                    "ERRO",
                )
                continue

            genus_name = name_parts[0]
            species_name = " ".join(name_parts[1:])

            response = requests.get(
                "https://api.iucnredlist.org/api/v4/taxa/scientific_name",
                headers=_request_headers(api_key),
                cookies=_request_cookies(),
                params={
                    "genus_name": genus_name,
                    "species_name": species_name,
                },
                timeout=30,
            )

            if response.status_code != 200:
                api_errors += 1
                _log(
                    (
                        f"[{idx}/{total}] {species.scientific_name} - "
                        f"{_response_error_message(response)}"
                    ),
                    "ERRO",
                )
                if _is_cloudflare_challenge(response):
                    raise RuntimeError(
                        "Cloudflare bloqueou a API da IUCN para este ambiente/IP. "
                        "Abortando para nao tentar todas as especies."
                    )
                time.sleep(1)
                continue

            data = response.json()

            if not isinstance(data, dict):
                invalid_response += 1
                _log(
                    f"[{idx}/{total}] {species.scientific_name} - resposta invalida",
                    "ERRO",
                )
                time.sleep(1)
                continue

            assessments = data.get("assessments")
            if not isinstance(assessments, list):
                invalid_response += 1
                _log(
                    f"[{idx}/{total}] {species.scientific_name} - assessments invalido",
                    "ERRO",
                )
                time.sleep(1)
                continue

            latest_assessment = next(
                (assessment for assessment in assessments if assessment.get("latest", False)),
                None,
            )

            if not latest_assessment:
                no_latest_assessment += 1
                _log(
                    f"[{idx}/{total}] {species.scientific_name} - sem assessment latest",
                    "ERRO",
                )
            else:
                conservation_status = latest_assessment.get("red_list_category_code")
                assessment_id = latest_assessment.get("assessment_id")
                iucn_assessment_year = latest_assessment.get("year_published")
                url = latest_assessment.get("url")

                species.iucn_redlist = str(assessment_id) if assessment_id is not None else None

                characteristics = species.characteristics
                if not characteristics:
                    characteristics = SpeciesCharacteristics(species_id=species.id)
                    db.session.add(characteristics)

                characteristics.conservation_status = conservation_status
                characteristics.iucn_assessment_year = (
                    str(iucn_assessment_year) if iucn_assessment_year is not None else None
                )
                characteristics.iucn_assessment_url = url

                db.session.commit()
                updated += 1
                _log(
                    (
                        f"[{idx}/{total}] Atualizado: "
                        f"{species.scientific_name} -> {conservation_status}"
                    ),
                    "OK",
                )

            time.sleep(1)

    _log(
        "Atualizadas: "
        f"{updated} | Nome invalido: {invalid_name} | Erros API: {api_errors} | "
        f"Resposta invalida: {invalid_response} | Sem latest: {no_latest_assessment}",
        "RESUMO",
    )
    _log("Importacao finalizada", "OK")


if __name__ == "__main__":
    main()
