from marshmallow import EXCLUDE, Schema, ValidationError, fields, validate, validates_schema

from app.utils.pagination import make_pagination_schema


class SpeciesPhotoRequestInputSchema(Schema):
    object_key = fields.String(required=True)
    bucket_name = fields.String(allow_none=True)
    original_filename = fields.String(allow_none=True)
    mime_type = fields.String(allow_none=True)
    size_bytes = fields.Integer(allow_none=True)
    checksum_sha256 = fields.String(allow_none=True)
    caption = fields.String(allow_none=True)
    license_code = fields.String(allow_none=True)
    attribution = fields.String(allow_none=True)
    rights_holder = fields.String(allow_none=True)
    source_url = fields.Url(allow_none=True)
    declaration_accepted_at = fields.DateTime(allow_none=True)


class SpeciesChangeRequestCreateSchema(Schema):
    class Meta:
        unknown = EXCLUDE

    species_id = fields.Integer(required=True)
    proposed_data = fields.Dict(required=False, load_default=dict)
    source_lang = fields.String(
        required=False, load_default="pt", validate=validate.OneOf(["pt", "en"])
    )
    request_note = fields.String(allow_none=True)
    requester_name = fields.String(allow_none=True)
    requester_email = fields.Email(allow_none=True)
    requester_institution = fields.String(allow_none=True)
    photos = fields.List(
        fields.Nested(SpeciesPhotoRequestInputSchema),
        required=False,
        load_default=list,
    )

    @validates_schema
    def validate_has_changes(self, data, **kwargs):
        has_data = bool(data.get("proposed_data") or {})
        has_photos = bool(data.get("photos") or [])
        if not has_data and not has_photos:
            raise ValidationError(
                "Informe ao menos `proposed_data` ou uma foto em `photos`.",
                field_name="proposed_data",
            )


class SpeciesPhotoRequestSchema(Schema):
    id = fields.String(dump_only=True)
    object_key = fields.String(dump_only=True)
    bucket_name = fields.String(allow_none=True, dump_only=True)
    original_filename = fields.String(allow_none=True, dump_only=True)
    mime_type = fields.String(allow_none=True, dump_only=True)
    size_bytes = fields.Integer(allow_none=True, dump_only=True)
    checksum_sha256 = fields.String(allow_none=True, dump_only=True)
    caption = fields.String(allow_none=True, dump_only=True)
    license_code = fields.String(allow_none=True, dump_only=True)
    attribution = fields.String(allow_none=True, dump_only=True)
    rights_holder = fields.String(allow_none=True, dump_only=True)
    source_url = fields.String(allow_none=True, dump_only=True)
    preview_url = fields.String(allow_none=True, dump_only=True)
    declaration_accepted_at = fields.DateTime(allow_none=True, dump_only=True)
    status = fields.String(dump_only=True)
    created_at = fields.DateTime(dump_only=True)


class SpeciesChangeRequestSchema(Schema):
    id = fields.String(dump_only=True)
    species_id = fields.String(dump_only=True)
    requested_by_user_id = fields.String(allow_none=True, dump_only=True)
    requester_name = fields.String(allow_none=True, dump_only=True)
    requester_email = fields.String(allow_none=True, dump_only=True)
    requester_institution = fields.String(allow_none=True, dump_only=True)
    request_note = fields.String(allow_none=True, dump_only=True)
    proposed_data = fields.Method("get_proposed_data", dump_only=True)
    current_data = fields.Dict(allow_none=True, dump_only=True)
    status = fields.String(dump_only=True)
    review_note = fields.String(allow_none=True, dump_only=True)
    reviewed_by_user_id = fields.String(allow_none=True, dump_only=True)
    reviewed_at = fields.DateTime(allow_none=True, dump_only=True)
    created_at = fields.DateTime(dump_only=True)
    updated_at = fields.DateTime(dump_only=True)
    photos = fields.List(fields.Nested(SpeciesPhotoRequestSchema), dump_only=True)

    @staticmethod
    def get_proposed_data(obj):
        if hasattr(obj, "proposed_data_enriched"):
            return obj.proposed_data_enriched
        return getattr(obj, "proposed_data", None) or {}


SpeciesChangeRequestPaginationSchema = make_pagination_schema(SpeciesChangeRequestSchema)


class SpeciesPhotoReviewDecisionInputSchema(Schema):
    class Meta:
        unknown = EXCLUDE

    photo_request_id = fields.Integer(required=True)
    decision = fields.String(required=True)


class SpeciesProposedDataReviewDecisionInputSchema(Schema):
    class Meta:
        unknown = EXCLUDE

    field = fields.String(required=True)
    decision = fields.String(required=True)


class SpeciesChangeRequestReviewSchema(Schema):
    class Meta:
        unknown = EXCLUDE

    decision = fields.String(allow_none=True)
    proposed_data_decision = fields.String(allow_none=True)
    proposed_data_fields = fields.List(
        fields.Nested(SpeciesProposedDataReviewDecisionInputSchema),
        required=False,
        load_default=list,
    )
    photos = fields.List(
        fields.Nested(SpeciesPhotoReviewDecisionInputSchema),
        required=False,
        load_default=list,
    )
    review_note = fields.String(allow_none=True)

    @validates_schema
    def validate_decision(self, data, **kwargs):
        decision = (data.get("decision") or "").lower()
        proposed_data_decision = (data.get("proposed_data_decision") or "").lower()
        proposed_data_fields = data.get("proposed_data_fields") or []
        photos = data.get("photos") or []

        if not decision and not proposed_data_decision and not proposed_data_fields and not photos:
            raise ValidationError(
                "Informe `decision` geral ou decisões granulares.",
                field_name="decision",
            )

        if decision and decision not in {"approve", "reject"}:
            raise ValidationError(
                "`decision` deve ser `approve` ou `reject`.", field_name="decision"
            )

        if proposed_data_decision and proposed_data_decision not in {"approve", "reject"}:
            raise ValidationError(
                "`proposed_data_decision` deve ser `approve` ou `reject`.",
                field_name="proposed_data_decision",
            )

        seen_fields = set()
        for index, item in enumerate(proposed_data_fields):
            field_name = (item.get("field") or "").strip()
            if not field_name:
                raise ValidationError(
                    f"`proposed_data_fields[{index}].field` é obrigatório.",
                    field_name="proposed_data_fields",
                )
            if field_name in seen_fields:
                raise ValidationError(
                    f"`field` duplicado em `proposed_data_fields`: {field_name}.",
                    field_name="proposed_data_fields",
                )
            seen_fields.add(field_name)

            field_decision = (item.get("decision") or "").lower()
            if field_decision not in {"approve", "reject"}:
                raise ValidationError(
                    f"`proposed_data_fields[{index}].decision` deve ser `approve` ou `reject`.",
                    field_name="proposed_data_fields",
                )

        seen_photo_ids = set()
        for index, item in enumerate(photos):
            photo_id = item.get("photo_request_id")
            if photo_id is None:
                raise ValidationError(
                    f"`photos[{index}].photo_request_id` é obrigatório.",
                    field_name="photos",
                )
            try:
                photo_id = int(photo_id)
            except (TypeError, ValueError):
                raise ValidationError(
                    f"`photos[{index}].photo_request_id` deve ser inteiro.",
                    field_name="photos",
                )
            if photo_id < 1:
                raise ValidationError(
                    f"`photos[{index}].photo_request_id` deve ser >= 1.",
                    field_name="photos",
                )
            if photo_id in seen_photo_ids:
                raise ValidationError(
                    f"`photo_request_id` duplicado em `photos`: {photo_id}.",
                    field_name="photos",
                )
            seen_photo_ids.add(photo_id)

            photo_decision = (item.get("decision") or "").lower()
            if photo_decision not in {"approve", "reject"}:
                raise ValidationError(
                    f"`photos[{index}].decision` deve ser `approve` ou `reject`.",
                    field_name="photos",
                )


class SpeciesPhotoUploadUrlRequestSchema(Schema):
    filename = fields.String(required=True)
    mime_type = fields.String(required=True)
    size_bytes = fields.Integer(required=True)
    species_id = fields.Integer(allow_none=True)


class SpeciesPhotoUploadUrlResponseSchema(Schema):
    upload_url = fields.String(required=True, dump_only=True)
    form_fields = fields.Dict(
        required=True,
        dump_only=True,
        attribute="fields",
        data_key="fields",
    )
    bucket_name = fields.String(required=True, dump_only=True)
    object_key = fields.String(required=True, dump_only=True)
    expires_at = fields.DateTime(required=True, dump_only=True)


class SpeciesTmpCleanupResponseSchema(Schema):
    bucket = fields.String(required=True, dump_only=True)
    retention_days = fields.Integer(required=True, dump_only=True)
    candidates = fields.Integer(required=True, dump_only=True)
    deleted = fields.Integer(required=True, dump_only=True)
    dry_run = fields.Boolean(required=True, dump_only=True)
