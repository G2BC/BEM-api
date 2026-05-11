from flask import request
from flask.views import MethodView
from flask_jwt_extended import get_jwt, get_jwt_identity, verify_jwt_in_request
from flask_smorest import Blueprint

from app.exceptions import AppError
from app.schemas.species_change_request_schemas import (
    SpeciesChangeRequestCreateSchema,
    SpeciesChangeRequestPaginationSchema,
    SpeciesChangeRequestReviewSchema,
    SpeciesChangeRequestSchema,
    SpeciesPhotoUploadUrlRequestSchema,
    SpeciesPhotoUploadUrlResponseSchema,
    SpeciesTmpCleanupResponseSchema,
)
from app.services.species_change_request import SpeciesChangeRequestService
from app.utils.bilingual import bilingual_response
from app.utils.permissions import require_curator_or_admin

species_change_request_bp = Blueprint(
    "species_change_requests",
    "species_change_requests",
)
admin_species_change_request_bp = Blueprint(
    "admin_species_change_requests",
    "admin_species_change_requests",
)


@species_change_request_bp.route("")
class SpeciesChangeRequests(MethodView):
    @species_change_request_bp.arguments(SpeciesChangeRequestCreateSchema)
    @species_change_request_bp.response(201, SpeciesChangeRequestSchema)
    @species_change_request_bp.alt_response(400, description="Erro de validação/regra de negócio")
    def post(self, payload):
        verify_jwt_in_request(optional=True)
        identity = get_jwt_identity() if get_jwt() else None

        try:
            return SpeciesChangeRequestService.create_request(payload, identity)
        except AppError as exc:
            return bilingual_response(exc.status, exc.pt, exc.en)


@admin_species_change_request_bp.route("")
class AdminSpeciesChangeRequests(MethodView):
    @require_curator_or_admin
    @admin_species_change_request_bp.response(200, SpeciesChangeRequestPaginationSchema)
    @admin_species_change_request_bp.alt_response(400, description="Parâmetros inválidos")
    @admin_species_change_request_bp.alt_response(
        403, description="Acesso permitido apenas para curadores/admins"
    )
    def get(self):
        status = request.args.get("status", type=str)
        page = request.args.get("page", type=int)
        per_page = request.args.get("per_page", type=int)

        try:
            return SpeciesChangeRequestService.list_requests(status, page, per_page)
        except AppError as exc:
            return bilingual_response(exc.status, exc.pt, exc.en)


@admin_species_change_request_bp.route("/<string:request_id>")
class GetSpeciesChangeRequest(MethodView):
    @require_curator_or_admin
    @admin_species_change_request_bp.response(200, SpeciesChangeRequestSchema)
    @admin_species_change_request_bp.alt_response(
        403, description="Acesso permitido apenas para curadores/admins"
    )
    @admin_species_change_request_bp.alt_response(404, description="Solicitação não encontrada")
    def get(self, request_id: str):
        try:
            return SpeciesChangeRequestService.get_request(request_id)
        except AppError as exc:
            return bilingual_response(exc.status, exc.pt, exc.en)


@species_change_request_bp.route("/upload-urls")
class SpeciesChangeRequestUploadUrl(MethodView):
    @species_change_request_bp.arguments(SpeciesPhotoUploadUrlRequestSchema, location="json")
    @species_change_request_bp.response(200, SpeciesPhotoUploadUrlResponseSchema)
    @species_change_request_bp.alt_response(400, description="Erro de validação/regra de negócio")
    def post(self, payload):
        try:
            return SpeciesChangeRequestService.generate_upload_url(
                filename=payload["filename"],
                mime_type=payload["mime_type"],
                size_bytes=payload["size_bytes"],
                species_id=payload.get("species_id"),
            )
        except AppError as exc:
            return bilingual_response(exc.status, exc.pt, exc.en)


@admin_species_change_request_bp.route("/temp-uploads/cleanup")
class CleanupSpeciesTmpUploads(MethodView):
    @require_curator_or_admin
    @admin_species_change_request_bp.response(200, SpeciesTmpCleanupResponseSchema)
    @admin_species_change_request_bp.alt_response(400, description="Parâmetros inválidos")
    @admin_species_change_request_bp.alt_response(
        403, description="Acesso permitido apenas para curadores/admins"
    )
    def post(self):
        retention_days = request.args.get("retention_days", type=int)
        dry_run = request.args.get("dry_run", default="true", type=str).lower() != "false"

        try:
            return SpeciesChangeRequestService.cleanup_tmp_objects(
                retention_days=retention_days,
                dry_run=dry_run,
            )
        except AppError as exc:
            return bilingual_response(exc.status, exc.pt, exc.en)


@admin_species_change_request_bp.route("/<string:request_id>/review")
class ReviewSpeciesChangeRequest(MethodView):
    @require_curator_or_admin
    @admin_species_change_request_bp.arguments(SpeciesChangeRequestReviewSchema)
    @admin_species_change_request_bp.response(200, SpeciesChangeRequestSchema)
    @admin_species_change_request_bp.alt_response(
        400, description="Erro de validação/regra de negócio"
    )
    @admin_species_change_request_bp.alt_response(
        403, description="Acesso permitido apenas para curadores/admins"
    )
    @admin_species_change_request_bp.alt_response(404, description="Solicitação não encontrada")
    def patch(self, payload, request_id: str):
        identity = get_jwt_identity()

        try:
            return SpeciesChangeRequestService.review_request(
                request_id=request_id,
                reviewer_user_id=str(identity),
                decision=payload.get("decision"),
                review_note=payload.get("review_note"),
                proposed_data_decision=payload.get("proposed_data_decision"),
                proposed_data_fields=payload.get("proposed_data_fields") or [],
                photo_decisions=payload.get("photos") or [],
            )
        except AppError as exc:
            return bilingual_response(exc.status, exc.pt, exc.en)
