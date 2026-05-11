import json

from flask import Response, current_app, request
from flask.views import MethodView
from flask_smorest import Blueprint

from app.exceptions import AppError, AppRuntimeError
from app.extensions import db
from app.models.observation import Observation
from app.models.species import Species
from app.schemas import DomainSelectSchema, SelectSchema
from app.schemas.distribution_schemas import DistributionSchema
from app.schemas.observation_schemas import ObservationListSchema
from app.schemas.reference_schemas import (
    ReferenceAssociateExistingSchema,
    ReferenceCreateAndAssociateSchema,
    ReferenceSchema,
)
from app.schemas.species_change_request_schemas import SpeciesPhotoUploadUrlResponseSchema
from app.schemas.species_schemas import (
    SpeciesCreateRequestSchema,
    SpeciesDetailSchema,
    SpeciesOutdatedPaginationSchema,
    SpeciesPatchRequestSchema,
    SpeciesPhotoCreateRequestSchema,
    SpeciesPhotoCreateResponseSchema,
    SpeciesPhotoUpdateRequestSchema,
    SpeciesPhotoUploadUrlPayloadSchema,
    SpeciesSelectSchema,
    SpeciesStatisticsSchema,
    SpeciesWithPhotosPaginationSchema,
)
from app.services.cache_service import CacheService
from app.services.ncbi_service import NCBIService
from app.services.species_photo_service import SpeciesPhotoService
from app.services.species_reference_service import SpeciesReferenceService
from app.services.species_service import SpeciesService
from app.utils.bilingual import bilingual_response
from app.utils.permissions import require_curator_or_admin

specie_bp = Blueprint(
    "species",
    "species",
)
admin_specie_bp = Blueprint(
    "admin_species",
    "admin_species",
)


def _parse_optional_bool_query(name: str) -> bool | None:
    raw = request.args.get(name, type=str)
    if raw is None:
        return None

    normalized = raw.strip().lower()
    if not normalized:
        return None
    if normalized in {"true", "1", "t", "yes", "y"}:
        return True
    if normalized in {"false", "0", "f", "no", "n"}:
        return False

    raise AppError(
        pt=f"`{name}` deve ser booleano (`true` ou `false`)",
        en=f"`{name}` must be boolean (`true` or `false`)",
    )


@specie_bp.route("")
class SpeciesCollection(MethodView):
    @specie_bp.response(200, SpeciesWithPhotosPaginationSchema)
    @specie_bp.alt_response(400, description="Parâmetros inválidos")
    def get(self):
        search = request.args.get("search", type=str)
        country = request.args.get("country", type=str)
        bem = request.args.get("bem", type=str)
        page = request.args.get("page", type=int)
        per_page = request.args.get("per_page", type=int)
        distributions_raw = request.args.get("distributions", type=str)
        distributions = (
            [s for s in distributions_raw.split(",") if s.strip()] if distributions_raw else None
        )

        try:
            is_visible = _parse_optional_bool_query("is_visible")
            return SpeciesService.search(
                search=search,
                country=country,
                bem=bem,
                is_visible=is_visible,
                page=page,
                per_page=per_page,
                distributions=distributions,
            )
        except AppError as exc:
            return bilingual_response(exc.status, exc.pt, exc.en)

@specie_bp.route("/options/countries")
class SpeciesCountrySelect(MethodView):
    @specie_bp.response(200, SelectSchema(many=True))
    def get(self):
        search = request.args.get("search", type=str)
        return SpeciesService.country_select(search)


@specie_bp.route("/options/bem")
class SpeciesBemSelect(MethodView):
    @specie_bp.response(200, SelectSchema(many=True))
    def get(self):
        search = request.args.get("search", type=str)
        return SpeciesService.bem_select(search)


@specie_bp.route("/options/species")
class SpeciesSelect(MethodView):
    @specie_bp.response(200, SpeciesSelectSchema(many=True))
    @specie_bp.alt_response(400, description="Parâmetros inválidos")
    def get(self):
        search = request.args.get("search", type=str)
        exclude_species_id = request.args.get("exclude_species_id", type=int)

        try:
            return SpeciesService.species_select(search, exclude_species_id)
        except AppError as exc:
            return bilingual_response(exc.status, exc.pt, exc.en)


@specie_bp.route("/options/domains")
class SpeciesDomainsSelect(MethodView):
    @specie_bp.response(200, DomainSelectSchema(many=True))
    @specie_bp.alt_response(400, description="Parâmetros inválidos")
    def get(self):
        domain = request.args.get("domain", type=str)
        search = request.args.get("search", type=str)

        try:
            return SpeciesService.domain_select(domain, search)
        except AppError as exc:
            return bilingual_response(exc.status, exc.pt, exc.en)


@specie_bp.route("/options/distributions")
class SpeciesDistributionsSelect(MethodView):
    @specie_bp.response(200, DistributionSchema(many=True))
    def get(self):
        return SpeciesService.distributions_select()


@specie_bp.route("/statistics")
class SpeciesStatistics(MethodView):
    @specie_bp.response(200, SpeciesStatisticsSchema)
    def get(self):
        return SpeciesService.statistics()


@specie_bp.route("/distribution-statistics")
class SpeciesDistributionStatistics(MethodView):
    @specie_bp.response(200)
    def get(self):
        return SpeciesService.distribution_occurrence_statistics()


@admin_specie_bp.route("/outdated")
class SpeciesOutdated(MethodView):
    @require_curator_or_admin
    @admin_specie_bp.response(200, SpeciesOutdatedPaginationSchema)
    @admin_specie_bp.alt_response(400, description="Parâmetros inválidos")
    @admin_specie_bp.alt_response(403, description="Acesso permitido apenas para curadores/admins")
    def get(self):
        page = request.args.get("page", type=int)
        per_page = request.args.get("per_page", type=int)
        try:
            return SpeciesService.list_outdated(page, per_page)
        except AppError as exc:
            return bilingual_response(exc.status, exc.pt, exc.en)


@specie_bp.route("/<int:species_id>")
class UpdateSpecies(MethodView):
    @specie_bp.response(200, SpeciesDetailSchema)
    @specie_bp.alt_response(400, description="Parâmetros inválidos")
    @specie_bp.alt_response(404, description="Espécie não encontrada")
    def get(self, species_id: int):
        try:
            is_visible = _parse_optional_bool_query("is_visible")
            return SpeciesService.get(str(species_id), is_visible=is_visible)
        except AppError as exc:
            return bilingual_response(exc.status, exc.pt, exc.en)


@admin_specie_bp.route("")
class AdminSpeciesCollection(MethodView):
    @require_curator_or_admin
    @admin_specie_bp.arguments(SpeciesCreateRequestSchema, location="json")
    @admin_specie_bp.response(201, SpeciesDetailSchema)
    @admin_specie_bp.alt_response(400, description="Erro de validação/regra de negócio")
    @admin_specie_bp.alt_response(403, description="Acesso permitido apenas para curadores/admins")
    def post(self, payload):
        try:
            return SpeciesService.create(payload)
        except AppError as exc:
            return bilingual_response(exc.status, exc.pt, exc.en)


@admin_specie_bp.route("/<int:species_id>")
class AdminSpeciesDetail(MethodView):
    @require_curator_or_admin
    @admin_specie_bp.arguments(SpeciesPatchRequestSchema, location="json")
    @admin_specie_bp.response(200, SpeciesDetailSchema)
    @admin_specie_bp.alt_response(400, description="Erro de validação/regra de negócio")
    @admin_specie_bp.alt_response(403, description="Acesso permitido apenas para curadores/admins")
    @admin_specie_bp.alt_response(404, description="Espécie não encontrada")
    def patch(self, payload, species_id: int):
        try:
            return SpeciesService.update(species_id, payload)
        except AppError as exc:
            return bilingual_response(exc.status, exc.pt, exc.en)

    @require_curator_or_admin
    @admin_specie_bp.response(204)
    @admin_specie_bp.alt_response(400, description="Erro de validação/regra de negócio")
    @admin_specie_bp.alt_response(403, description="Acesso permitido apenas para curadores/admins")
    @admin_specie_bp.alt_response(404, description="Espécie não encontrada")
    def delete(self, species_id: int):
        try:
            SpeciesService.delete(species_id)
            return None
        except AppError as exc:
            return bilingual_response(exc.status, exc.pt, exc.en)


@specie_bp.route("/by-name/<path:species>")
class GetSpecies(MethodView):
    @specie_bp.response(200, SpeciesDetailSchema)
    @specie_bp.alt_response(400, description="Parâmetros inválidos")
    @specie_bp.alt_response(404, description="Espécie não encontrada")
    def get(self, species: str):
        try:
            is_visible = _parse_optional_bool_query("is_visible")
            return SpeciesService.get(species, is_visible=is_visible)
        except AppError as exc:
            return bilingual_response(exc.status, exc.pt, exc.en)


@admin_specie_bp.route("/<int:species_id>/photo-upload-urls")
class SpeciesPhotoUploadUrl(MethodView):
    @require_curator_or_admin
    @admin_specie_bp.arguments(SpeciesPhotoUploadUrlPayloadSchema, location="json")
    @admin_specie_bp.response(200, SpeciesPhotoUploadUrlResponseSchema)
    @admin_specie_bp.alt_response(400, description="Erro de validação/regra de negócio")
    @admin_specie_bp.alt_response(403, description="Acesso permitido apenas para curadores/admins")
    @admin_specie_bp.alt_response(404, description="Espécie não encontrada")
    def post(self, payload, species_id: int):
        try:
            return SpeciesPhotoService.generate_upload_url(
                species_id=species_id,
                filename=payload["filename"],
                mime_type=payload["mime_type"],
                size_bytes=payload["size_bytes"],
            )
        except AppError as exc:
            return bilingual_response(exc.status, exc.pt, exc.en)


@admin_specie_bp.route("/<int:species_id>/photos")
class SpeciesPhotos(MethodView):
    @require_curator_or_admin
    @admin_specie_bp.arguments(SpeciesPhotoCreateRequestSchema, location="json")
    @admin_specie_bp.response(201, SpeciesPhotoCreateResponseSchema)
    @admin_specie_bp.alt_response(400, description="Erro de validação/regra de negócio")
    @admin_specie_bp.alt_response(403, description="Acesso permitido apenas para curadores/admins")
    @admin_specie_bp.alt_response(404, description="Espécie não encontrada")
    def post(self, payload, species_id: int):
        try:
            return SpeciesPhotoService.create_photo(species_id=species_id, payload=payload)
        except AppError as exc:
            return bilingual_response(exc.status, exc.pt, exc.en)


@admin_specie_bp.route("/<int:species_id>/photos/<string:photo_id>")
class SpeciesPhotoDetail(MethodView):
    @require_curator_or_admin
    @admin_specie_bp.arguments(SpeciesPhotoUpdateRequestSchema, location="json")
    @admin_specie_bp.response(200, SpeciesPhotoCreateResponseSchema)
    @admin_specie_bp.alt_response(400, description="Erro de validação/regra de negócio")
    @admin_specie_bp.alt_response(403, description="Acesso permitido apenas para curadores/admins")
    @admin_specie_bp.alt_response(404, description="Espécie/foto não encontrada")
    def patch(self, payload, species_id: int, photo_id: str):
        try:
            return SpeciesPhotoService.update_photo(
                species_id=species_id,
                photo_id=photo_id,
                payload=payload,
            )
        except AppError as exc:
            return bilingual_response(exc.status, exc.pt, exc.en)

    @require_curator_or_admin
    @admin_specie_bp.response(204)
    @admin_specie_bp.alt_response(400, description="Erro de validação/regra de negócio")
    @admin_specie_bp.alt_response(403, description="Acesso permitido apenas para curadores/admins")
    @admin_specie_bp.alt_response(404, description="Espécie/foto não encontrada")
    def delete(self, species_id: int, photo_id: str):
        try:
            SpeciesPhotoService.delete_photo(species_id=species_id, photo_id=photo_id)
            return None
        except AppError as exc:
            return bilingual_response(exc.status, exc.pt, exc.en)


@admin_specie_bp.route("/<int:species_id>/reference-associations")
class SpeciesReferenceAssociate(MethodView):
    @require_curator_or_admin
    @admin_specie_bp.arguments(ReferenceAssociateExistingSchema, location="json")
    @admin_specie_bp.response(201, ReferenceSchema)
    @admin_specie_bp.alt_response(400, description="Erro de validação/regra de negócio")
    @admin_specie_bp.alt_response(403, description="Acesso permitido apenas para curadores/admins")
    @admin_specie_bp.alt_response(404, description="Espécie ou referência não encontrada")
    def post(self, payload, species_id: int):
        """Associate an already-existing reference to a species."""
        try:
            return SpeciesReferenceService.associate_existing(
                species_id=species_id,
                reference_id=payload["reference_id"],
            ), 201
        except AppError as exc:
            return bilingual_response(exc.status, exc.pt, exc.en)


@admin_specie_bp.route("/<int:species_id>/references")
class SpeciesReferences(MethodView):
    @require_curator_or_admin
    @admin_specie_bp.arguments(ReferenceCreateAndAssociateSchema, location="json")
    @admin_specie_bp.response(201, ReferenceSchema)
    @admin_specie_bp.alt_response(400, description="Erro de validação/regra de negócio")
    @admin_specie_bp.alt_response(403, description="Acesso permitido apenas para curadores/admins")
    @admin_specie_bp.alt_response(404, description="Espécie não encontrada")
    def post(self, payload, species_id: int):
        """Create a new reference and associate it to a species."""
        try:
            return SpeciesReferenceService.create_and_associate(
                species_id=species_id,
                apa=payload.get("apa"),
                doi=payload.get("doi"),
                url=payload.get("url"),
            ), 201
        except AppError as exc:
            return bilingual_response(exc.status, exc.pt, exc.en)


@admin_specie_bp.route("/<int:species_id>/references/<int:reference_id>")
class SpeciesReferenceDetail(MethodView):
    @require_curator_or_admin
    @admin_specie_bp.response(204)
    @admin_specie_bp.alt_response(403, description="Acesso permitido apenas para curadores/admins")
    @admin_specie_bp.alt_response(404, description="Espécie ou referência não encontrada")
    def delete(self, species_id: int, reference_id: int):
        """Disassociate a reference from a species (and delete it if orphaned)."""
        try:
            SpeciesReferenceService.disassociate(
                species_id=species_id,
                reference_id=reference_id,
            )
            return None
        except AppError as exc:
            return bilingual_response(exc.status, exc.pt, exc.en)


@specie_bp.route("/<int:species_id>/ncbi")
@specie_bp.route("/by-name/<path:species>/ncbi")
class GetNCBISpeciesData(MethodView):
    @specie_bp.response(200)
    @specie_bp.alt_response(400, description="Parâmetros inválidos")
    @specie_bp.alt_response(404, description="Espécie não encontrada")
    @specie_bp.alt_response(502, description="Falha ao consultar serviço externo")
    def get(self, species: str | None = None, species_id: int | None = None):
        try:
            species_key = str(species_id) if species_id is not None else species
            data, is_cached = NCBIService.get_data(species_key, include_cache_meta=True)
            return Response(
                json.dumps(data, ensure_ascii=False, sort_keys=False),
                status=200,
                mimetype="application/json",
                headers={"X-Cache": "HIT" if is_cached else "MISS"},
            )
        except AppRuntimeError as exc:
            return bilingual_response(502, exc.pt, exc.en)
        except AppError as exc:
            return bilingual_response(exc.status, exc.pt, exc.en)


@specie_bp.route("/<int:species_id>/observations")
class SpeciesObservations(MethodView):
    @specie_bp.response(200, ObservationListSchema)
    @specie_bp.alt_response(404, description="Espécie não encontrada")
    def get(self, species_id: int):
        source = (request.args.get("source", type=str) or "").strip()

        cache_prefix = current_app.config.get("OBSERVATIONS_CACHE_PREFIX", "observations")
        cache_ttl = int(current_app.config.get("OBSERVATIONS_CACHE_TTL_SECONDS", 3600))
        cache_key = f"{cache_prefix}:{species_id}:{source or 'all'}"

        cached = CacheService.get(cache_key)
        if cached is not None:
            return Response(
                cached, status=200, mimetype="application/json", headers={"X-Cache": "HIT"}
            )

        if not db.session.get(Species, species_id):
            return bilingual_response(404, "Espécie não encontrada.", "Species not found.")

        query = Observation.query.filter_by(species_id=species_id)
        if source:
            query = query.filter_by(source=source)

        observations = query.order_by(Observation.observed_on.desc().nullslast()).all()

        result = {"observations": observations, "total": len(observations)}
        body = json.dumps(ObservationListSchema().dump(result), ensure_ascii=False)

        CacheService.set(cache_key, body, ttl_seconds=cache_ttl)

        return Response(body, status=200, mimetype="application/json", headers={"X-Cache": "MISS"})
