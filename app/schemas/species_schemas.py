from marshmallow import EXCLUDE, Schema, ValidationError, fields, validates_schema

from app.utils.object_storage import normalize_object_url
from app.utils.pagination import make_pagination_schema
from app.utils.photo_attribution import format_attribution_display

from .distribution_schemas import DistributionSchema
from .reference_schemas import ReferenceSchema
from .taxon_schemas import TaxonSchema


class SpeciesPhotoSchema(Schema):
    photo_id = fields.Integer(dump_only=True)
    medium_url = fields.Method("get_medium_url", dump_only=True)
    original_url = fields.Method("get_original_url", dump_only=True)
    license_code = fields.String(allow_none=True, dump_only=True)
    attribution = fields.String(allow_none=True, dump_only=True)
    attribution_display = fields.Method("get_attribution_display", dump_only=True)
    rights_holder = fields.String(allow_none=True, dump_only=True)
    source_url = fields.String(allow_none=True, dump_only=True)
    declaration_accepted_at = fields.DateTime(allow_none=True, dump_only=True)
    source = fields.String(dump_only=True)
    fetched_at = fields.DateTime(dump_only=True)
    featured = fields.Boolean(allow_none=True)

    @staticmethod
    def get_medium_url(obj):
        return normalize_object_url(getattr(obj, "medium_url", None))

    @staticmethod
    def get_original_url(obj):
        return normalize_object_url(getattr(obj, "original_url", None))

    @staticmethod
    def get_attribution_display(obj):
        return format_attribution_display(
            attribution=getattr(obj, "attribution", None),
            rights_holder=getattr(obj, "rights_holder", None),
            license_code=getattr(obj, "license_code", None),
        )


class SpeciesPhotoCreateResponseSchema(SpeciesPhotoSchema):
    pass


class SpeciesPhotoCreateRequestSchema(Schema):
    class Meta:
        unknown = EXCLUDE

    object_key = fields.String(required=True)
    bucket_name = fields.String(required=True)
    original_filename = fields.String(required=True)
    mime_type = fields.String(required=True)
    size_bytes = fields.Integer(required=True)
    license_code = fields.String(required=True)
    attribution = fields.String(required=True)
    rights_holder = fields.String(required=True)
    source_url = fields.Url(allow_none=True)
    featured = fields.Boolean(required=True)


class SpeciesPhotoUpdateRequestSchema(Schema):
    class Meta:
        unknown = EXCLUDE

    license_code = fields.String(required=False, allow_none=True)
    attribution = fields.String(required=False, allow_none=True)
    rights_holder = fields.String(required=False, allow_none=True)
    source_url = fields.Url(required=False, allow_none=True)
    featured = fields.Boolean(required=False)

    @validates_schema
    def validate_has_any_field(self, data, **kwargs):
        if not data:
            raise ValidationError(
                "Informe ao menos um campo para atualização da foto.",
                field_name="license_code",
            )


class SpeciesPatchRequestSchema(Schema):
    class Meta:
        unknown = EXCLUDE

    scientific_name = fields.String(required=False, allow_none=True)
    type_country = fields.String(required=False, allow_none=True)
    mycobank_index_fungorum_id = fields.Raw(required=False, allow_none=True)
    ncbi_taxonomy_id = fields.Raw(required=False, allow_none=True)
    inaturalist_taxon_id = fields.Raw(required=False, allow_none=True)
    unite_taxon_id = fields.Raw(required=False, allow_none=True)
    iucn_redlist = fields.String(required=False, allow_none=True)
    references_raw = fields.String(required=False, allow_none=True)
    distributions = fields.List(fields.Integer(strict=True), required=False)
    is_visible = fields.Boolean(required=False)
    is_outdated_mycobank = fields.Boolean(required=False)

    cultivation_possible = fields.Boolean(required=False, allow_none=True)

    colors = fields.String(required=False, allow_none=True)
    colors_pt = fields.String(required=False, allow_none=True)
    cultivation = fields.String(required=False, allow_none=True)
    cultivation_pt = fields.String(required=False, allow_none=True)
    finding_tips = fields.String(required=False, allow_none=True)
    finding_tips_pt = fields.String(required=False, allow_none=True)
    nearby_trees = fields.String(required=False, allow_none=True)
    nearby_trees_pt = fields.String(required=False, allow_none=True)
    curiosities = fields.String(required=False, allow_none=True)
    curiosities_pt = fields.String(required=False, allow_none=True)
    general_description = fields.String(required=False, allow_none=True)
    general_description_pt = fields.String(required=False, allow_none=True)

    size_cm = fields.Float(required=False, allow_none=True)
    season_start_month = fields.Integer(required=False, allow_none=True)
    season_end_month = fields.Integer(required=False, allow_none=True)

    growth_forms = fields.List(fields.Integer(strict=True), required=False)
    nutrition_modes = fields.List(fields.Integer(strict=True), required=False)
    substrates = fields.List(fields.Integer(strict=True), required=False)
    habitats = fields.List(fields.Integer(strict=True), required=False)
    decay_types = fields.List(fields.Integer(strict=True), required=False)
    similar_species_ids = fields.List(fields.Integer(strict=True), required=False)

    @validates_schema
    def validate_has_any_field(self, data, **kwargs):
        if not data:
            raise ValidationError(
                "Informe ao menos um campo para atualização da espécie.",
                field_name="scientific_name",
            )


class SpeciesCreateRequestSchema(SpeciesPatchRequestSchema):
    scientific_name = fields.String(required=False, allow_none=True)
    mycobank_index_fungorum_id = fields.Raw(required=True, allow_none=False)


class SpeciesPhotoUploadUrlPayloadSchema(Schema):
    class Meta:
        unknown = EXCLUDE

    filename = fields.String(required=True)
    mime_type = fields.String(required=True)
    size_bytes = fields.Integer(required=True)


class SpeciesSelectSchema(Schema):
    id = fields.Integer(required=True)
    label = fields.String(required=True)
    photo = fields.String(allow_none=True)


class SpeciesCharacteristicsSchema(Schema):
    species_id = fields.Integer(allow_none=True)
    cultivation = fields.String(allow_none=True)
    cultivation_pt = fields.String(allow_none=True)
    finding_tips = fields.String(allow_none=True)
    finding_tips_pt = fields.String(allow_none=True)
    nearby_trees = fields.String(allow_none=True)
    nearby_trees_pt = fields.String(allow_none=True)
    curiosities = fields.String(allow_none=True)
    curiosities_pt = fields.String(allow_none=True)
    general_description = fields.String(allow_none=True)
    general_description_pt = fields.String(allow_none=True)
    conservation_status = fields.String(allow_none=True)
    colors = fields.String(allow_none=True)
    colors_pt = fields.String(allow_none=True)
    size_cm = fields.Float(allow_none=True)
    growth_forms = fields.Method("get_growth_forms", allow_none=True)
    substrates = fields.Method("get_substrates", allow_none=True)
    nutrition_modes = fields.Method("get_nutrition_modes", allow_none=True)
    season_start_month = fields.Integer(allow_none=True)
    season_end_month = fields.Integer(allow_none=True)
    habitats = fields.Method("get_habitats", allow_none=True)
    decay_types = fields.Method("get_decay_types", allow_none=True)
    similar_species = fields.Method("get_similar_species", allow_none=True)
    cultivation_possible = fields.Boolean(allow_none=True)
    iucn_assessment_year = fields.String(allow_none=True)
    iucn_assessment_url = fields.String(allow_none=True)

    @staticmethod
    def get_habitats(obj):
        habitats = getattr(obj, "habitats", None) or []
        return [
            {
                "id": habitat.id,
                "label_pt": habitat.label_pt,
                "label_en": habitat.label_en,
            }
            for habitat in habitats
        ]

    @staticmethod
    def get_growth_forms(obj):
        growth_forms = getattr(obj, "growth_forms", None) or []
        return [
            {
                "id": growth_form.id,
                "label_pt": growth_form.label_pt,
                "label_en": growth_form.label_en,
            }
            for growth_form in growth_forms
        ]

    @staticmethod
    def get_substrates(obj):
        substrates = getattr(obj, "substrates", None) or []
        return [
            {
                "id": substrate.id,
                "label_pt": substrate.label_pt,
                "label_en": substrate.label_en,
            }
            for substrate in substrates
        ]

    @staticmethod
    def get_nutrition_modes(obj):
        nutrition_modes = getattr(obj, "nutrition_modes", None) or []
        return [
            {
                "id": nutrition_mode.id,
                "label_pt": nutrition_mode.label_pt,
                "label_en": nutrition_mode.label_en,
            }
            for nutrition_mode in nutrition_modes
        ]

    @staticmethod
    def get_decay_types(obj):
        decay_types = getattr(obj, "decay_types", None) or []
        return [
            {
                "id": decay_type.id,
                "label_pt": decay_type.label_pt,
                "label_en": decay_type.label_en,
            }
            for decay_type in decay_types
        ]

    @staticmethod
    def get_similar_species(obj):
        species = getattr(obj, "species", None)
        links = getattr(species, "similar_species_links", None) or []
        items = []
        for link in links:
            related = getattr(link, "similar_species", None)
            label = getattr(related, "scientific_name", None)
            items.append({"id": link.similar_species_id, "label": label})
        return sorted(items, key=lambda item: item["id"])


class SpeciesWithPhotosSchema(Schema):
    id = fields.Integer(dump_only=True)
    scientific_name = fields.String(allow_none=True)
    common_name = fields.String(allow_none=True)
    is_visible = fields.Boolean()
    cultivation = fields.Method("get_cultivation", allow_none=True)
    finding_tips = fields.Method("get_finding_tips", allow_none=True)
    nearby_trees = fields.Method("get_nearby_trees", allow_none=True)
    curiosities = fields.Method("get_curiosities", allow_none=True)
    general_description = fields.Method("get_general_description", allow_none=True)
    conservation_status = fields.Method("get_conservation_status", allow_none=True)
    colors = fields.Method("get_colors", allow_none=True)
    size_cm = fields.Method("get_size_cm", allow_none=True)
    growth_forms = fields.Method("get_growth_forms", allow_none=True)
    substrates = fields.Method("get_substrates", allow_none=True)
    habitats = fields.Method("get_habitats", allow_none=True)
    decay_types = fields.Method("get_decay_types", allow_none=True)
    similar_species_ids = fields.Method("get_similar_species_ids", allow_none=True)
    season_start_month = fields.Method("get_season_start_month", allow_none=True)
    season_end_month = fields.Method("get_season_end_month", allow_none=True)
    type_country = fields.String(allow_none=True)
    mycobank_index_fungorum_id = fields.String(allow_none=True)
    species_characteristics = fields.Nested(
        SpeciesCharacteristicsSchema,
        attribute="characteristics",
        allow_none=True,
    )
    taxonomy = fields.Nested(TaxonSchema, dump_only=True)
    photos = fields.List(
        fields.Nested(
            SpeciesPhotoSchema(
                only=(
                    "photo_id",
                    "medium_url",
                    "original_url",
                    "license_code",
                    "attribution",
                    "attribution_display",
                    "rights_holder",
                    "source_url",
                    "declaration_accepted_at",
                    "featured",
                )
            ),
            dump_only=True,
        )
    )

    @staticmethod
    def _get_characteristic_value(obj, field_name):
        characteristics = getattr(obj, "characteristics", None)
        if characteristics is not None:
            return getattr(characteristics, field_name, None)
        return None

    def get_cultivation(self, obj):
        return self._get_characteristic_value(obj, "cultivation")

    def get_finding_tips(self, obj):
        return self._get_characteristic_value(obj, "finding_tips")

    def get_nearby_trees(self, obj):
        return self._get_characteristic_value(obj, "nearby_trees")

    def get_curiosities(self, obj):
        return self._get_characteristic_value(obj, "curiosities")

    def get_general_description(self, obj):
        return self._get_characteristic_value(obj, "general_description")

    def get_conservation_status(self, obj):
        return self._get_characteristic_value(obj, "conservation_status")

    def get_colors(self, obj):
        return self._get_characteristic_value(obj, "colors")

    def get_size_cm(self, obj):
        return self._get_characteristic_value(obj, "size_cm")

    def get_growth_forms(self, obj):
        characteristics = getattr(obj, "characteristics", None)
        if not characteristics:
            return []
        growth_forms = getattr(characteristics, "growth_forms", None) or []
        return [
            {
                "id": growth_form.id,
                "label_pt": growth_form.label_pt,
                "label_en": growth_form.label_en,
            }
            for growth_form in growth_forms
        ]

    def get_substrates(self, obj):
        characteristics = getattr(obj, "characteristics", None)
        if not characteristics:
            return []
        substrates = getattr(characteristics, "substrates", None) or []
        return [
            {
                "id": substrate.id,
                "label_pt": substrate.label_pt,
                "label_en": substrate.label_en,
            }
            for substrate in substrates
        ]

    def get_habitats(self, obj):
        characteristics = getattr(obj, "characteristics", None)
        if not characteristics:
            return []
        habitats = getattr(characteristics, "habitats", None) or []
        return [
            {
                "id": habitat.id,
                "label_pt": habitat.label_pt,
                "label_en": habitat.label_en,
            }
            for habitat in habitats
        ]

    def get_decay_types(self, obj):
        characteristics = getattr(obj, "characteristics", None)
        if not characteristics:
            return []
        decay_types = getattr(characteristics, "decay_types", None) or []
        return [
            {
                "id": decay_type.id,
                "label_pt": decay_type.label_pt,
                "label_en": decay_type.label_en,
            }
            for decay_type in decay_types
        ]

    def get_similar_species_ids(self, obj):
        links = getattr(obj, "similar_species_links", None) or []
        return sorted(link.similar_species_id for link in links)

    def get_season_start_month(self, obj):
        return self._get_characteristic_value(obj, "season_start_month")

    def get_season_end_month(self, obj):
        return self._get_characteristic_value(obj, "season_end_month")


SpeciesWithPhotosPaginationSchema = make_pagination_schema(SpeciesWithPhotosSchema)


class SpeciesStatisticsSchema(Schema):
    edible_brazil_species = fields.Integer(dump_only=True)
    observations = fields.Integer(dump_only=True)
    extinction_risk_species = fields.Integer(dump_only=True)
    brazilian_type_species = fields.Integer(dump_only=True)


class SpeciesOutdatedSchema(Schema):
    id = fields.Integer(dump_only=True)
    scientific_name = fields.String(dump_only=True)
    mycobank_index_fungorum_id = fields.String(dump_only=True, allow_none=True)


SpeciesOutdatedPaginationSchema = make_pagination_schema(SpeciesOutdatedSchema)


class SpeciesDetailSchema(Schema):
    id = fields.Integer(dump_only=True)
    scientific_name = fields.String(allow_none=True)
    common_name = fields.String(allow_none=True)
    is_visible = fields.Boolean()
    ncbi_taxonomy_id = fields.Integer(allow_none=True)
    type_country = fields.String(allow_none=True)
    mycobank_index_fungorum_id = fields.String(allow_none=True)
    is_outdated_mycobank = fields.Boolean(allow_none=True)
    iucn_redlist = fields.String(allow_none=True)
    inaturalist_taxon_id = fields.String(allow_none=True)
    unite_taxon_id = fields.String(allow_none=True)
    references = fields.List(fields.Nested(ReferenceSchema), dump_only=True)
    distributions = fields.List(fields.Nested(DistributionSchema), dump_only=True)
    species_characteristics = fields.Nested(
        SpeciesCharacteristicsSchema,
        attribute="characteristics",
        allow_none=True,
    )
    taxonomy = fields.Nested(TaxonSchema, dump_only=True)
    photos = fields.List(
        fields.Nested(
            SpeciesPhotoSchema(
                only=(
                    "photo_id",
                    "medium_url",
                    "original_url",
                    "license_code",
                    "attribution",
                    "attribution_display",
                    "rights_holder",
                    "source_url",
                    "declaration_accepted_at",
                    "featured",
                )
            ),
            dump_only=True,
        )
    )


class SnapshotDownloadResponseSchema(Schema):
    url = fields.String(dump_only=True)
    expires_in_seconds = fields.Integer(dump_only=True)
    version = fields.Integer(dump_only=True)
    lang = fields.String(dump_only=True)
    format = fields.String(dump_only=True)
