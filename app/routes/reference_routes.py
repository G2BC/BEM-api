from flask import request
from flask.views import MethodView
from flask_smorest import Blueprint

from app.exceptions import AppError
from app.schemas.reference_schemas import ReferenceSchema, ReferenceUpdateSchema
from app.services.species_reference_service import SpeciesReferenceService
from app.utils.bilingual import bilingual_response
from app.utils.permissions import require_curator_or_admin

reference_bp = Blueprint(
    "references",
    "references",
    url_prefix="/references",
)


@reference_bp.route("/select")
class ReferencesSelect(MethodView):
    @require_curator_or_admin
    @reference_bp.response(200, ReferenceSchema(many=True))
    @reference_bp.alt_response(403, description="Acesso permitido apenas para curadores/admins")
    def get(self):
        """Search references for autocomplete (by APA, DOI or URL)."""
        search = request.args.get("search", type=str)
        limit = request.args.get("limit", default=30, type=int)
        limit = max(1, min(limit, 100))
        return SpeciesReferenceService.search(search, limit)


@reference_bp.route("/<int:reference_id>")
class ReferenceDetail(MethodView):
    @require_curator_or_admin
    @reference_bp.arguments(ReferenceUpdateSchema, location="json")
    @reference_bp.response(200, ReferenceSchema)
    @reference_bp.alt_response(400, description="Erro de validação/regra de negócio")
    @reference_bp.alt_response(403, description="Acesso permitido apenas para curadores/admins")
    @reference_bp.alt_response(404, description="Referência não encontrada")
    def patch(self, payload, reference_id: int):
        """Update fields of an existing reference."""
        try:
            return SpeciesReferenceService.update(
                reference_id=reference_id,
                apa=payload.get("apa"),
                doi=payload.get("doi"),
                url=payload.get("url"),
            )
        except AppError as exc:
            return bilingual_response(exc.status, exc.pt, exc.en)
