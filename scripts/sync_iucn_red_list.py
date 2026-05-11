"""
Sincroniza dados da IUCN Red List para espécies cadastradas no BEM.

Atualiza por espécie:
- species.iucn_redlist com o assessment_id mais recente
- species_characteristics.conservation_status com a categoria IUCN
- species_characteristics.iucn_assessment_year e iucn_assessment_url

- Kill switch por IUCN_MAX_RUNTIME_SECONDS
- Grupos por campo `bem` com checkpoint Redis (via sync_base.SyncRunner)
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
from scripts.sync_base import SyncRunner  # noqa: E402


class CloudflareBlocked(RuntimeError):
    pass


def _log(msg):
    print(msg, flush=True)


def _i(value):
    if value in (None, ""):
        return None
    try:
        return int(str(value).strip())
    except (TypeError, ValueError):
        return None


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


def _request_headers(api_key: str) -> dict:
    return {
        "authorization": api_key,
        "accept": "application/json",
        "user-agent": (
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/148.0.0.0 Safari/537.36"
        ),
    }


# ---------------------------------------------------------------------------
# SyncRunner subclass
# ---------------------------------------------------------------------------

class IucnSyncRunner(SyncRunner):
    source_name = "iucn-red-list"
    env_prefix = "IUCN"

    def __init__(self, app):
        super().__init__(app)
        self.api_key = os.getenv("IUCN_API_KEY")

    def get_species_rows(self, session, bem_ids):
        q = session.query(Species.id, Species.scientific_name, Species.bem).filter(
            Species.scientific_name.isnot(None)
        )
        if bem_ids:
            q = q.filter(Species.id.in_(bem_ids))
        return q.all()

    def is_fatal_error(self, exc):
        return isinstance(exc, CloudflareBlocked)

    def sync_species(self, row, start_time):
        if not self.api_key:
            raise RuntimeError("IUCN_API_KEY não configurada")

        scientific_name = row.scientific_name
        name_parts = (scientific_name or "").split()
        if len(name_parts) < 2:
            _log(f"  [{row.id}] nome científico inválido — ignorando")
            return 0, 0, True

        genus_name = name_parts[0]
        species_name = " ".join(name_parts[1:])

        response = requests.get(
            "https://api.iucnredlist.org/api/v4/taxa/scientific_name",
            headers=_request_headers(self.api_key),
            params={"genus_name": genus_name, "species_name": species_name},
            timeout=30,
        )

        if response.status_code != 200:
            if _is_cloudflare_challenge(response):
                raise CloudflareBlocked(
                    "Cloudflare bloqueou a API da IUCN — abortando para não tentar todas as espécies"
                )
            _log(f"  [{row.id}] {scientific_name} — {_response_error_message(response)}")
            time.sleep(1)
            return 0, 0, True

        data = response.json()

        if not isinstance(data, dict):
            _log(f"  [{row.id}] {scientific_name} — resposta inválida")
            time.sleep(1)
            return 0, 0, True

        assessments = data.get("assessments")
        if not isinstance(assessments, list):
            _log(f"  [{row.id}] {scientific_name} — assessments inválido")
            time.sleep(1)
            return 0, 0, True

        latest = next((a for a in assessments if a.get("latest", False)), None)

        if not latest:
            _log(f"  [{row.id}] {scientific_name} — sem assessment latest")
            time.sleep(1)
            return 0, 0, True

        conservation_status = latest.get("red_list_category_code")
        assessment_id = latest.get("assessment_id")
        iucn_year = latest.get("year_published")
        url = latest.get("url")

        species = db.session.get(Species, row.id)
        species.iucn_redlist = str(assessment_id) if assessment_id is not None else None

        characteristics = species.characteristics
        if not characteristics:
            characteristics = SpeciesCharacteristics(species_id=row.id)
            db.session.add(characteristics)

        characteristics.conservation_status = conservation_status
        characteristics.iucn_assessment_year = str(iucn_year) if iucn_year is not None else None
        characteristics.iucn_assessment_url = url

        db.session.commit()
        _log(f"  [{row.id}] {scientific_name} → {conservation_status}")

        time.sleep(1)
        return 1, 0, True


app = create_app()

if __name__ == "__main__":
    runner = IucnSyncRunner(app)
    if not runner.api_key:
        print("[ERRO] IUCN_API_KEY não configurada")
        sys.exit(1)
    runner.run()
