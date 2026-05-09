from functools import partial
from typing import Any
from urllib.parse import quote_plus
from urllib.request import urlopen as urllib_urlopen

from app.exceptions import AppError, AppRuntimeError
from app.repositories.species_repository import SpeciesRepository
from app.services.cache_service import CacheService
from Bio import Entrez
from flask import current_app


class NCBIService:
    @staticmethod
    def get_data(
        species: str | None = "", include_cache_meta: bool = False
    ) -> dict[str, Any] | tuple[dict[str, Any], bool]:
        ncbi_email = current_app.config.get("NCBI_EMAIL", "").strip()
        ncbi_api_key = current_app.config.get("NCBI_API_KEY", "").strip()

        if not ncbi_email or not ncbi_api_key:
            if include_cache_meta:
                return {}, False
            return {}

        species = (species or "").strip()
        if not species:
            raise AppError(pt="Espécie inválida.", en="Invalid species.")

        taxid = SpeciesRepository.get_ncbi_taxon_id(species)
        if not taxid:
            raise AppError(
                pt="Espécie sem NCBI taxonomy ID cadastrado.",
                en="Species has no NCBI taxonomy ID registered.",
                status=404,
            )

        cache_prefix = (
            current_app.config.get("NCBI_CACHE_PREFIX", "ncbi:species").strip() or "ncbi:species"
        )
        cache_ttl_seconds = int(current_app.config.get("NCBI_CACHE_TTL_SECONDS", 86400))
        cache_key = f"{cache_prefix}:{taxid}:v1"

        cached_result = CacheService.get_json(cache_key)
        if isinstance(cached_result, dict):
            if include_cache_meta:
                return cached_result, True
            return cached_result

        resultado = NCBIService._fetch_from_ncbi(taxid, ncbi_email, ncbi_api_key)

        CacheService.set_json(cache_key, resultado, ttl_seconds=cache_ttl_seconds)

        if include_cache_meta:
            return resultado, False
        return resultado

    @staticmethod
    def _fetch_from_ncbi(taxid: int, email: str, api_key: str) -> dict[str, Any]:
        Entrez.email = email
        Entrez.api_key = api_key or None
        Entrez.max_tries = int(current_app.config.get("NCBI_MAX_TRIES", 1))
        Entrez.sleep_between_tries = float(
            current_app.config.get("NCBI_SLEEP_BETWEEN_TRIES_SECONDS", 0.25)
        )
        ncbi_timeout_seconds = float(current_app.config.get("NCBI_REQUEST_TIMEOUT_SECONDS", 8))
        Entrez.urlopen = partial(urllib_urlopen, timeout=ncbi_timeout_seconds)

        direct_term = f"txid{taxid}[Organism:noexp]"
        subtree_term = f"txid{taxid}[Organism:exp]"

        databases = {
            "taxonomy": {
                "label": "Taxonomy",
                "link": f"https://www.ncbi.nlm.nih.gov/Taxonomy/Browser/wwwtax.cgi?id={taxid}",
            },
            "bioproject": {
                "label": "BioProject",
                "search_url": "https://www.ncbi.nlm.nih.gov/bioproject/?term=",
            },
            "biosample": {
                "label": "BioSample",
                "search_url": "https://www.ncbi.nlm.nih.gov/biosample/?term=",
            },
            "assembly": {
                "label": "Assembly",
                "search_url": "https://www.ncbi.nlm.nih.gov/assembly/?term=",
            },
            "nuccore": {
                "label": "Nucleotide",
                "search_url": "https://www.ncbi.nlm.nih.gov/nuccore/?term=",
            },
            "gene": {
                "label": "Gene",
                "search_url": "https://www.ncbi.nlm.nih.gov/gene/?term=",
            },
            "gds": {
                "label": "GEO DataSets",
                "search_url": "https://www.ncbi.nlm.nih.gov/gds/?term=",
            },
            "sra": {
                "label": "SRA",
                "search_url": "https://www.ncbi.nlm.nih.gov/sra/?term=",
            },
            "protein": {
                "label": "Protein",
                "search_url": "https://www.ncbi.nlm.nih.gov/protein/?term=",
            },
            "ipg": {
                "label": "Identical Protein Groups",
                "search_url": "https://www.ncbi.nlm.nih.gov/ipg/?term=",
            },
            "structure": {
                "label": "Structure",
                "search_url": "https://www.ncbi.nlm.nih.gov/structure/?term=",
            },
            "pmc": {
                "label": "PubMed Central",
                "search_url": "https://pmc.ncbi.nlm.nih.gov/?term=",
            },
        }

        def count_for(db: str, term: str) -> int:
            with Entrez.esearch(db=db, term=term, retmax=0) as handle:
                record = Entrez.read(handle)
                return int(record["Count"])

        resultado = {}

        for db_key, info in databases.items():
            label = info["label"]
            try:
                if db_key == "taxonomy":
                    direct_count = count_for("taxonomy", direct_term)
                    subtree_count = count_for("taxonomy", subtree_term)
                    if direct_count <= 0 and subtree_count <= 0:
                        continue
                    resultado[label] = {
                        "direct_links": {"quantity": direct_count, "link": info["link"]},
                        "subtree_links": {"quantity": subtree_count, "link": info["link"]},
                    }
                    continue

                direct_count = count_for(db_key, direct_term)
                subtree_count = count_for(db_key, subtree_term)
                if direct_count <= 0 and subtree_count <= 0:
                    continue

                resultado[label] = {
                    "direct_links": {
                        "quantity": direct_count,
                        "link": f"{info['search_url']}{quote_plus(direct_term)}",
                    },
                    "subtree_links": {
                        "quantity": subtree_count,
                        "link": f"{info['search_url']}{quote_plus(subtree_term)}",
                    },
                }
            except Exception as exc:
                raise AppRuntimeError(
                    pt=f"Falha ao consultar dados do NCBI na base '{db_key}'",
                    en=f"Failed to query NCBI data in database '{db_key}'",
                ) from exc

        return resultado
