from typing import Any

from app.exceptions import AppError
from app.models.species_change_request import SpeciesChangeRequest
from app.repositories.species_change_request_repository import SpeciesChangeRequestRepository
from app.repositories.user_repository import UserRepository
from app.utils.pagination import build_page_response, resolve_page_params

from .enrichment import SpeciesChangeRequestEnrichment
from .storage import SpeciesChangeRequestStorage
from .validation import SpeciesChangeRequestValidation


class SpeciesChangeRequestService:
    DEFAULT_PER_PAGE = 20
    MAX_PER_PAGE = 100
    ALLOWED_SPECIES_FIELDS = {
        "scientific_name",
        "lum_mycelium",
        "lum_basidiome",
        "lum_stipe",
        "lum_pileus",
        "lum_lamellae",
        "lum_spores",
        "edible",
        "cultivation",
        "cultivation_pt",
        "finding_tips",
        "finding_tips_pt",
        "nearby_trees",
        "nearby_trees_pt",
        "curiosities",
        "curiosities_pt",
        "general_description",
        "general_description_pt",
        "colors",
        "colors_pt",
        "size_cm",
        "growth_form_ids",
        "substrate_ids",
        "nutrition_mode_ids",
        "habitat_ids",
        "decay_type_ids",
        "season_start_month",
        "season_end_month",
        "distribution_regions",
        "references_raw",
    }

    @classmethod
    def create_request(cls, payload: dict[str, Any], requester_user_id: str | None = None):
        species_id = payload["species_id"]
        species = SpeciesChangeRequestRepository.get_species_by_id(species_id)
        if not species:
            raise AppError(pt="Espécie não encontrada.", en="Species not found.", status=404)

        proposed_data = payload.get("proposed_data") or {}
        source_lang = (payload.get("source_lang") or "pt").strip().lower()
        if source_lang not in {"pt", "en"}:
            raise AppError(
                pt="`source_lang` deve ser `pt` ou `en`",
                en="`source_lang` must be `pt` or `en`",
            )

        proposed_data = SpeciesChangeRequestValidation.normalize_translatable_fields(
            proposed_data, source_lang
        )
        invalid_fields = sorted(set(proposed_data.keys()) - cls.ALLOWED_SPECIES_FIELDS)
        if invalid_fields:
            raise AppError(
                pt=f"Campos não permitidos em `proposed_data`: {', '.join(invalid_fields)}",
                en=f"Fields not allowed in `proposed_data`: {', '.join(invalid_fields)}",
            )
        SpeciesChangeRequestValidation.validate_proposed_data(proposed_data, species_id=species_id)

        photos_payload = payload.get("photos") or []
        SpeciesChangeRequestValidation.validate_photos_payload(photos_payload)
        SpeciesChangeRequestStorage.validate_uploaded_objects(photos_payload)

        requested_by_user_id = None
        if requester_user_id is not None:
            user = UserRepository.get_by_id(requester_user_id)
            if user:
                requested_by_user_id = user.id

        return SpeciesChangeRequestRepository.create(
            species_id=species_id,
            proposed_data=proposed_data,
            request_note=(payload.get("request_note") or None),
            requester_name=(payload.get("requester_name") or None),
            requester_email=(payload.get("requester_email") or None),
            requester_institution=(payload.get("requester_institution") or None),
            requested_by_user_id=requested_by_user_id,
            photos_payload=photos_payload,
        )

    @classmethod
    def list_requests(cls, status=None, page=None, per_page=None):
        normalized_status = (status or "").strip().lower() or None
        if normalized_status and normalized_status not in SpeciesChangeRequest.STATUSES:
            raise AppError(
                pt="`status` inválido. Use: pending, approved, partial_approved, rejected",
                en="Invalid `status`. Use: pending, approved, partial_approved, rejected",
            )

        page, per_page = resolve_page_params(
            page, per_page, default_per_page=cls.DEFAULT_PER_PAGE, max_per_page=cls.MAX_PER_PAGE
        )
        result = SpeciesChangeRequestRepository.list(normalized_status, page, per_page)
        items = result if page is None else result.items
        SpeciesChangeRequestEnrichment.enrich_requests(items)
        return build_page_response(result, page, per_page)

    @staticmethod
    def get_request(request_id: str):
        req = SpeciesChangeRequestRepository.get_by_id(
            SpeciesChangeRequestValidation.parse_id(request_id)
        )
        if not req:
            raise AppError(pt="Solicitação não encontrada.", en="Request not found.", status=404)
        SpeciesChangeRequestEnrichment.enrich_requests([req])
        return req

    @staticmethod
    def review_request(
        request_id: str,
        reviewer_user_id: str,
        decision: str | None,
        review_note: str | None,
        proposed_data_decision: str | None = None,
        proposed_data_fields: list[dict[str, Any]] | None = None,
        photo_decisions: list[dict[str, Any]] | None = None,
    ):
        req = SpeciesChangeRequestRepository.get_by_id(
            SpeciesChangeRequestValidation.parse_id(request_id)
        )
        if not req:
            raise AppError(pt="Solicitação não encontrada.", en="Request not found.", status=404)
        if req.status != SpeciesChangeRequest.STATUS_PENDING:
            raise AppError(pt="Solicitação já revisada.", en="Request already reviewed.")

        reviewer = UserRepository.get_by_id(reviewer_user_id)
        if not reviewer:
            raise AppError(
                pt="Usuário autenticado não encontrado.",
                en="Authenticated user not found.",
                status=404,
            )

        normalized_decision = SpeciesChangeRequestValidation.normalize_review_decision(
            decision, "decision"
        )
        normalized_proposed_data_decision = (
            SpeciesChangeRequestValidation.normalize_review_decision(
                proposed_data_decision, "proposed_data_decision"
            )
        )
        normalized_proposed_data_fields = (
            SpeciesChangeRequestValidation.normalize_proposed_data_field_decisions(
                proposed_data_fields or []
            )
        )
        proposed_data_decision_map = {
            item["field"]: item["decision"] for item in normalized_proposed_data_fields
        }
        proposed_data_decision_map = (
            SpeciesChangeRequestValidation.expand_translatable_decision_map(
                proposed_data_decision_map, req.proposed_data or {}
            )
        )
        normalized_photo_decisions = SpeciesChangeRequestValidation.normalize_photo_decisions(
            photo_decisions or []
        )
        photo_decision_map = {
            item["photo_request_id"]: item["decision"] for item in normalized_photo_decisions
        }

        has_proposed_data = bool(req.proposed_data or {})
        if not normalized_decision:
            proposed_fields_count = len((req.proposed_data or {}).keys())
            if (
                proposed_fields_count > 0
                and len(proposed_data_decision_map) < proposed_fields_count
                and not normalized_proposed_data_decision
            ) or (len(req.photos) > 0 and len(photo_decision_map) < len(req.photos)):
                raise AppError(
                    pt=(
                        "Forneça `decision` global ou decisões individuais"
                        " para todos os campos/fotos"
                    ),
                    en=(
                        "Provide a global `decision` or individual decisions for all fields/photos"
                    ),
                )

        if (
            not normalized_decision
            and not normalized_proposed_data_decision
            and not proposed_data_decision_map
        ):
            if has_proposed_data:
                raise AppError(
                    pt=(
                        "Forneça `decision`, `proposed_data_decision` ou"
                        " `proposed_data_fields` se a solicitação tiver `proposed_data`"
                    ),
                    en=(
                        "Provide `decision`, `proposed_data_decision` or"
                        " `proposed_data_fields` if the request has `proposed_data`"
                    ),
                )

        approved_proposed_data = {}
        approved_items = 0
        rejected_items = 0

        if has_proposed_data:
            proposed_data_keys = list((req.proposed_data or {}).keys())
            unknown_proposed_fields = sorted(
                set(proposed_data_decision_map.keys()) - set(proposed_data_keys)
            )
            if unknown_proposed_fields:
                joined = ", ".join(unknown_proposed_fields)
                raise AppError(
                    pt=f"Campos inválidos em `proposed_data_fields`: {joined}",
                    en=f"Invalid fields in `proposed_data_fields`: {joined}",
                )

            for field in proposed_data_keys:
                field_final_decision = (
                    proposed_data_decision_map.get(field)
                    or normalized_proposed_data_decision
                    or normalized_decision
                )
                if not field_final_decision:
                    raise AppError(
                        pt=f"Decisão ausente para o campo `{field}`",
                        en=f"Missing decision for field `{field}`",
                    )

                if field_final_decision == "approve":
                    approved_proposed_data[field] = req.proposed_data[field]
                    approved_items += 1
                else:
                    rejected_items += 1

            if approved_proposed_data:
                species = SpeciesChangeRequestRepository.get_species_by_id(req.species_id)
                if not species:
                    raise AppError(
                        pt="Espécie não encontrada.", en="Species not found.", status=404
                    )
                SpeciesChangeRequestRepository.apply_species_updates(
                    species, approved_proposed_data
                )

        photo_ids = {photo.id for photo in req.photos}
        unknown_photo_ids = sorted(set(photo_decision_map.keys()) - photo_ids)
        if unknown_photo_ids:
            joined = ", ".join(str(pid) for pid in unknown_photo_ids)
            raise AppError(
                pt=f"IDs de foto inválidos em `photos`: {joined}",
                en=f"Invalid photo IDs in `photos`: {joined}",
            )

        for photo in req.photos:
            photo_final_decision = photo_decision_map.get(photo.id) or normalized_decision
            if not photo_final_decision:
                raise AppError(
                    pt=f"Decisão ausente para foto {photo.id}",
                    en=f"Missing decision for photo {photo.id}",
                )

            if photo_final_decision == "approve":
                promoted_bucket, promoted_key = SpeciesChangeRequestStorage.promote_object_to_final(
                    photo, req.species_id
                )
                photo.bucket_name = promoted_bucket
                photo.object_key = promoted_key
                SpeciesChangeRequestRepository.create_or_skip_species_photo_from_request(
                    req.species_id, photo
                )
                photo.status = SpeciesChangeRequest.STATUS_APPROVED
                approved_items += 1
            else:
                SpeciesChangeRequestStorage.delete_tmp_object_if_exists(photo)
                photo.status = SpeciesChangeRequest.STATUS_REJECTED
                rejected_items += 1

        if approved_items > 0 and rejected_items > 0:
            status = SpeciesChangeRequest.STATUS_PARTIAL_APPROVED
        elif approved_items > 0:
            status = SpeciesChangeRequest.STATUS_APPROVED
        else:
            status = SpeciesChangeRequest.STATUS_REJECTED

        reviewed = SpeciesChangeRequestRepository.save_review(
            req=req,
            status=status,
            reviewed_by_user_id=reviewer.id,
            review_note=review_note,
        )
        SpeciesChangeRequestEnrichment.enrich_requests([reviewed])
        return reviewed

    @staticmethod
    def generate_upload_url(filename: str, mime_type: str, size_bytes: int, species_id=None):
        return SpeciesChangeRequestStorage.generate_upload_url(
            filename, mime_type, size_bytes, species_id
        )

    @staticmethod
    def cleanup_tmp_objects(retention_days: int | None = None, dry_run: bool = True):
        return SpeciesChangeRequestStorage.cleanup_tmp_objects(retention_days, dry_run)
