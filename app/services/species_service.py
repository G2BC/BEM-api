from typing import Any

from app.exceptions import AppError
from app.models.distribution import Distribution
from app.models.species import Species
from app.models.species_similarity import SpeciesSimilarity
from app.repositories.species_change_request_repository import SpeciesChangeRequestRepository
from app.repositories.species_repository import SpeciesRepository
from app.services.species_change_request.validation import SpeciesChangeRequestValidation
from app.utils.pagination import build_page_response, resolve_page_params
from sqlalchemy.exc import IntegrityError


class SpeciesService:
    DEFAULT_PER_PAGE = 30
    DELETE_AUTO_REVIEW_NOTE = "Solicitação reprovada automaticamente porque a espécie foi excluída"
    PATCH_RELATION_FIELD_MAP = {
        "growth_forms": "growth_form_ids",
        "substrates": "substrate_ids",
        "nutrition_modes": "nutrition_mode_ids",
        "habitats": "habitat_ids",
        "decay_types": "decay_type_ids",
    }
    PATCH_BIGINT_FIELDS = {
        "mycobank_index_fungorum_id",
        "ncbi_taxonomy_id",
        "inaturalist_taxon_id",
        "unite_taxon_id",
    }

    @classmethod
    def search(
        cls,
        search: str | None = "",
        country: str | None = "",
        is_visible: bool | None = None,
        page: int | None = None,
        per_page: int | None = None,
        distributions: list[str] | None = None,
    ) -> dict[str, Any]:
        search = (search or "").strip()
        country = (country or "").strip()

        if is_visible is not None and not isinstance(is_visible, bool):
            raise AppError(pt="`is_visible` deve ser booleano", en="`is_visible` must be boolean")

        if page is not None:
            if not isinstance(page, int) or page < 1:
                raise AppError(
                    pt="`page` deve ser um inteiro >= 1", en="`page` must be an integer >= 1"
                )
            per_page = per_page or cls.DEFAULT_PER_PAGE

        result = SpeciesRepository.list(
            search=search,
            country=country,
            is_visible=is_visible,
            page=page,
            per_page=per_page,
            distributions=distributions,
        )
        return build_page_response(result, page, per_page)

    @classmethod
    def list_outdated(cls, page: int | None = None, per_page: int | None = None) -> dict:
        page, per_page = resolve_page_params(page, per_page, default_per_page=cls.DEFAULT_PER_PAGE)
        result = SpeciesRepository.list_outdated(page, per_page)
        return build_page_response(result, page, per_page)

    @staticmethod
    def country_select(search: str | None = ""):
        return SpeciesRepository.country_select(search)

    @staticmethod
    def bem_select(search: str | None = ""):
        return SpeciesRepository.bem_select(search)

    def distributions_select():
        return SpeciesRepository.distributions_select()

    @staticmethod
    def statistics() -> dict[str, int]:
        return SpeciesRepository.statistics()

    @staticmethod
    def species_select(
        search: str | None = "",
        exclude_species_id: int | None = None,
    ):
        if exclude_species_id is not None and exclude_species_id < 1:
            raise AppError(
                pt="`exclude_species_id` deve ser um inteiro >= 1",
                en="`exclude_species_id` must be an integer >= 1",
            )

        return SpeciesRepository.species_select(search, exclude_species_id)

    @staticmethod
    def domain_select(domain: str, search: str | None = ""):
        return SpeciesRepository.domain_select(domain, search)

    @staticmethod
    def get(species: str | None = "", is_visible: bool | None = None):
        if is_visible is not None and not isinstance(is_visible, bool):
            raise AppError(pt="`is_visible` deve ser booleano", en="`is_visible` must be boolean")

        found = SpeciesRepository.get(species, is_visible=is_visible)
        if not found:
            raise AppError(pt="Espécie não encontrada.", en="Species not found.", status=404)
        return found

    @classmethod
    def create(cls, payload: dict[str, Any]):
        normalized_payload = cls._normalize_patch_payload(payload or {})
        mycobank_index_fungorum_id = normalized_payload.get("mycobank_index_fungorum_id")
        if mycobank_index_fungorum_id is None:
            raise AppError(
                pt="`mycobank_index_fungorum_id` é obrigatório",
                en="`mycobank_index_fungorum_id` is required",
            )

        scientific_name = normalized_payload.get("scientific_name")
        if scientific_name is None:
            normalized_payload.pop("scientific_name", None)
        elif not isinstance(scientific_name, str):
            raise AppError(
                pt="`scientific_name` deve ser string ou null",
                en="`scientific_name` must be a string or null",
            )
        else:
            normalized_name = scientific_name.strip()
            normalized_payload["scientific_name"] = normalized_name or None

        similar_species_ids = normalized_payload.pop("similar_species_ids", None)
        distribution_ids = normalized_payload.pop("distributions", None)
        species = Species(
            scientific_name=normalized_payload.get("scientific_name"),
            mycobank_index_fungorum_id=mycobank_index_fungorum_id,
        )

        try:
            SpeciesRepository.stage(species)

            normalized_payload = cls._enrich_season_payload_with_current(
                species,
                normalized_payload,
            )
            SpeciesChangeRequestValidation.validate_proposed_data(
                normalized_payload,
                species_id=species.id,
            )
            cls._validate_similar_species_ids(species.id, similar_species_ids)

            SpeciesChangeRequestRepository.apply_species_updates(species, normalized_payload)
            if similar_species_ids is not None:
                species.similar_species_links = [
                    SpeciesSimilarity(similar_species_id=similar_species_id)
                    for similar_species_id in similar_species_ids
                ]
            if distribution_ids is not None:
                species.distributions = cls._fetch_distributions(distribution_ids)

            SpeciesRepository.save(species)
        except AppError:
            SpeciesRepository.rollback()
            raise
        except IntegrityError as exc:
            SpeciesRepository.rollback()
            raise AppError(
                pt=("Já existe espécie com `scientific_name` ou identificadores únicos informados"),
                en=(
                    "A species with the given `scientific_name`"
                    " or unique identifiers already exists"
                ),
            ) from exc

        return SpeciesRepository.get(str(species.id))

    @classmethod
    def update(cls, species_id: int, payload: dict[str, Any]):
        if not isinstance(species_id, int) or species_id < 1:
            raise AppError(pt="`species_id` inválido", en="Invalid `species_id`")

        species = SpeciesChangeRequestRepository.get_species_by_id(species_id)
        if not species:
            raise AppError(pt="Espécie não encontrada.", en="Species not found.", status=404)

        normalized_payload = cls._normalize_patch_payload(payload or {})
        similar_species_ids = normalized_payload.pop("similar_species_ids", None)
        distribution_ids = normalized_payload.pop("distributions", None)

        normalized_payload = cls._enrich_season_payload_with_current(species, normalized_payload)
        SpeciesChangeRequestValidation.validate_proposed_data(
            normalized_payload,
            species_id=species_id,
        )
        cls._validate_similar_species_ids(species_id, similar_species_ids)

        SpeciesChangeRequestRepository.apply_species_updates(species, normalized_payload)
        if similar_species_ids is not None:
            species.similar_species_links = [
                SpeciesSimilarity(similar_species_id=similar_species_id)
                for similar_species_id in similar_species_ids
            ]
        if distribution_ids is not None:
            species.distributions = cls._fetch_distributions(distribution_ids)

        SpeciesRepository.save(species)

        return SpeciesRepository.get(str(species_id))

    @classmethod
    def delete(cls, species_id: int):
        if not isinstance(species_id, int) or species_id < 1:
            raise AppError(pt="`species_id` inválido", en="Invalid `species_id`")

        species = SpeciesChangeRequestRepository.get_species_by_id(species_id)
        if not species:
            raise AppError(pt="Espécie não encontrada.", en="Species not found.", status=404)

        try:
            SpeciesRepository.delete(species)
            return
        except IntegrityError:
            SpeciesRepository.rollback()

        species = SpeciesChangeRequestRepository.get_species_by_id(species_id)
        if not species:
            return

        try:
            SpeciesChangeRequestRepository.reject_pending_by_species_id(
                species_id=species_id,
                review_note=cls.DELETE_AUTO_REVIEW_NOTE,
            )
            SpeciesChangeRequestRepository.delete_all_by_species_id(species_id)
            SpeciesRepository.delete_similarities_by_species_id(species_id)
            SpeciesRepository.delete(species)
        except IntegrityError as exc:
            SpeciesRepository.rollback()
            raise AppError(
                pt="Não foi possível excluir a espécie por vínculos relacionados no banco",
                en="Unable to delete the species due to related database constraints",
            ) from exc

    @classmethod
    def _normalize_patch_payload(cls, payload: dict[str, Any]) -> dict[str, Any]:
        normalized = {}
        for field, value in payload.items():
            mapped_field = cls.PATCH_RELATION_FIELD_MAP.get(field, field)
            if mapped_field in cls.PATCH_BIGINT_FIELDS:
                normalized[mapped_field] = cls._parse_nullable_bigint(mapped_field, value)
                continue
            normalized[mapped_field] = value
        return normalized

    @staticmethod
    def _parse_nullable_bigint(field_name: str, value: Any) -> int | None:
        if value is None:
            return None
        if isinstance(value, bool):
            raise AppError(
                pt=f"`{field_name}` deve ser inteiro >= 1 ou null",
                en=f"`{field_name}` must be an integer >= 1 or null",
            )
        if isinstance(value, int):
            if value < 1:
                raise AppError(
                    pt=f"`{field_name}` deve ser inteiro >= 1 ou null",
                    en=f"`{field_name}` must be an integer >= 1 or null",
                )
            return value
        if isinstance(value, str):
            raw = value.strip()
            if not raw:
                return None
            if not raw.isdigit():
                raise AppError(
                    pt=f"`{field_name}` deve ser inteiro >= 1 ou null",
                    en=f"`{field_name}` must be an integer >= 1 or null",
                )
            parsed = int(raw)
            if parsed < 1:
                raise AppError(
                    pt=f"`{field_name}` deve ser inteiro >= 1 ou null",
                    en=f"`{field_name}` must be an integer >= 1 or null",
                )
            return parsed

        raise AppError(
            pt=f"`{field_name}` deve ser inteiro >= 1 ou null",
            en=f"`{field_name}` must be an integer >= 1 or null",
        )

    @staticmethod
    def _enrich_season_payload_with_current(
        species: Species, payload: dict[str, Any]
    ) -> dict[str, Any]:
        has_start = "season_start_month" in payload
        has_end = "season_end_month" in payload
        if not (has_start ^ has_end):
            return payload

        characteristics = getattr(species, "characteristics", None)
        current_start = (
            getattr(characteristics, "season_start_month", None) if characteristics else None
        )
        current_end = (
            getattr(characteristics, "season_end_month", None) if characteristics else None
        )

        normalized = dict(payload)
        if not has_start:
            normalized["season_start_month"] = current_start
        if not has_end:
            normalized["season_end_month"] = current_end
        return normalized

    @staticmethod
    def _validate_similar_species_ids(
        species_id: int,
        similar_species_ids: list[int] | None,
    ) -> None:
        if similar_species_ids is None:
            return

        if not isinstance(similar_species_ids, list):
            raise AppError(
                pt="`similar_species_ids` deve ser uma lista de inteiros",
                en="`similar_species_ids` must be a list of integers",
            )

        normalized: list[int] = []
        for value in similar_species_ids:
            if isinstance(value, bool) or not isinstance(value, int):
                raise AppError(
                    pt="`similar_species_ids` deve conter apenas inteiros",
                    en="`similar_species_ids` must contain only integers",
                )
            if value < 1:
                raise AppError(
                    pt="`similar_species_ids` deve conter apenas inteiros >= 1",
                    en="`similar_species_ids` must contain only integers >= 1",
                )
            if value == species_id:
                raise AppError(
                    pt="`similar_species_ids` não pode conter a própria espécie",
                    en="`similar_species_ids` cannot contain the species itself",
                )
            normalized.append(value)

        unique_ids = sorted(set(normalized))
        if len(unique_ids) != len(normalized):
            raise AppError(
                pt="`similar_species_ids` contém IDs duplicados",
                en="`similar_species_ids` contains duplicate IDs",
            )

        if not unique_ids:
            return

        existing_ids = {
            found_id
            for (found_id,) in Species.query.with_entities(Species.id)
            .filter(Species.id.in_(unique_ids))
            .all()
        }
        missing_ids = [candidate for candidate in unique_ids if candidate not in existing_ids]
        if missing_ids:
            raise AppError(
                pt="`similar_species_ids` contém IDs de espécies inexistentes",
                en="`similar_species_ids` contains non-existent species IDs",
            )

    @staticmethod
    def _fetch_distributions(distribution_ids: list[int]) -> list:
        if not isinstance(distribution_ids, list):
            raise AppError(
                pt="`distributions` deve ser uma lista de inteiros",
                en="`distributions` must be a list of integers",
            )
        for value in distribution_ids:
            if isinstance(value, bool) or not isinstance(value, int) or value < 1:
                raise AppError(
                    pt="`distributions` deve conter apenas inteiros >= 1",
                    en="`distributions` must contain only integers >= 1",
                )

        if not distribution_ids:
            return []

        unique_ids = list(set(distribution_ids))
        found = Distribution.query.filter(Distribution.id.in_(unique_ids)).all()
        if len(found) != len(unique_ids):
            raise AppError(
                pt="`distributions` contém IDs de distribuições inexistentes",
                en="`distributions` contains non-existent distribution IDs",
            )
        return found
