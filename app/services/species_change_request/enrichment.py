from typing import Any

import app.utils.object_storage as object_storage
from app.repositories.species_change_request_repository import SpeciesChangeRequestRepository
from botocore.exceptions import BotoCoreError, ClientError
from flask import current_app


class SpeciesChangeRequestEnrichment:
    @classmethod
    def enrich_requests(cls, requests) -> None:
        species_cache = {}
        relation_label_cache = {
            field: {} for field in SpeciesChangeRequestRepository.RELATION_FIELD_MODELS.keys()
        }
        for req in requests:
            cls.attach_preview_urls(req)
            cls.attach_enriched_proposed_data(req, relation_label_cache)

            species_id = getattr(req, "species_id", None)
            if species_id not in species_cache:
                species_cache[species_id] = (
                    SpeciesChangeRequestRepository.get_species_by_id(species_id)
                    if species_id is not None
                    else None
                )

            species = species_cache.get(species_id)
            cls.attach_current_data(req, species, relation_label_cache)

    @classmethod
    def attach_preview_urls(cls, req) -> None:
        expires_in = int(current_app.config.get("SPECIES_REQUEST_PREVIEW_URL_EXPIRES_SECONDS", 900))
        for photo in req.photos:
            photo.preview_url = cls.build_preview_url(photo, expires_in)

    @classmethod
    def attach_enriched_proposed_data(cls, req, relation_label_cache: dict[str, dict]) -> None:
        proposed_data = getattr(req, "proposed_data", None) or {}
        enriched = {}

        for field, value in proposed_data.items():
            model = SpeciesChangeRequestRepository.RELATION_FIELD_MODELS.get(field)
            if model and isinstance(value, list):
                enriched[field] = cls.resolve_relation_items(
                    value,
                    model,
                    relation_label_cache[field],
                )
                continue
            enriched[field] = value

        req.proposed_data_enriched = enriched

    @staticmethod
    def resolve_relation_items(
        ids: list[Any],
        model,
        cache: dict[int, dict[str, Any]],
    ) -> list[dict[str, Any]]:
        normalized_ids = []
        for value in ids:
            if isinstance(value, bool):
                continue
            try:
                parsed = int(value)
            except (TypeError, ValueError):
                continue
            if parsed < 1:
                continue
            normalized_ids.append(parsed)

        if not normalized_ids:
            return []

        missing_ids = sorted(set(normalized_ids) - set(cache.keys()))
        if missing_ids:
            rows = model.query.filter(model.id.in_(missing_ids)).order_by(model.id.asc()).all()
            for row in rows:
                cache[row.id] = {
                    "id": row.id,
                    "label_pt": row.label_pt,
                    "label_en": row.label_en,
                }

        return [
            cache.get(item_id, {"id": item_id, "label_pt": None, "label_en": None})
            for item_id in normalized_ids
        ]

    @classmethod
    def attach_current_data(cls, req, species, relation_label_cache: dict[str, dict]) -> None:
        proposed_data = getattr(req, "proposed_data", None) or {}
        current_data = {}
        characteristics = getattr(species, "characteristics", None) if species else None

        for field in proposed_data.keys():
            if field == "habitat_ids" and species:
                if characteristics is None:
                    current_data[field] = []
                else:
                    habitats = getattr(characteristics, "habitats", None) or []
                    current_data[field] = [
                        {"id": h.id, "label_pt": h.label_pt, "label_en": h.label_en}
                        for h in habitats
                    ]
                    for h in habitats:
                        relation_label_cache[field][h.id] = {
                            "id": h.id,
                            "label_pt": h.label_pt,
                            "label_en": h.label_en,
                        }
                continue
            if field == "growth_form_ids" and species:
                if characteristics is None:
                    current_data[field] = []
                else:
                    growth_forms = getattr(characteristics, "growth_forms", None) or []
                    current_data[field] = [
                        {"id": g.id, "label_pt": g.label_pt, "label_en": g.label_en}
                        for g in growth_forms
                    ]
                    for g in growth_forms:
                        relation_label_cache[field][g.id] = {
                            "id": g.id,
                            "label_pt": g.label_pt,
                            "label_en": g.label_en,
                        }
                continue
            if field == "substrate_ids" and species:
                if characteristics is None:
                    current_data[field] = []
                else:
                    substrates = getattr(characteristics, "substrates", None) or []
                    current_data[field] = [
                        {"id": s.id, "label_pt": s.label_pt, "label_en": s.label_en}
                        for s in substrates
                    ]
                    for s in substrates:
                        relation_label_cache[field][s.id] = {
                            "id": s.id,
                            "label_pt": s.label_pt,
                            "label_en": s.label_en,
                        }
                continue
            if field == "nutrition_mode_ids" and species:
                if characteristics is None:
                    current_data[field] = []
                else:
                    nutrition_modes = getattr(characteristics, "nutrition_modes", None) or []
                    current_data[field] = [
                        {"id": n.id, "label_pt": n.label_pt, "label_en": n.label_en}
                        for n in nutrition_modes
                    ]
                    for n in nutrition_modes:
                        relation_label_cache[field][n.id] = {
                            "id": n.id,
                            "label_pt": n.label_pt,
                            "label_en": n.label_en,
                        }
                continue
            if field in SpeciesChangeRequestRepository.CHARACTERISTICS_FIELDS and species:
                if characteristics is not None:
                    current_data[field] = getattr(characteristics, field, None)
                else:
                    current_data[field] = getattr(species, field, None)
                continue
            current_data[field] = getattr(species, field, None) if species else None

        req.current_data = current_data

    @staticmethod
    def build_preview_url(photo, expires_in_seconds: int) -> str | None:
        source_url = (getattr(photo, "source_url", None) or "").strip()
        if source_url.startswith(("http://", "https://")):
            return source_url

        bucket = (getattr(photo, "bucket_name", None) or "").strip()
        key = (getattr(photo, "object_key", None) or "").strip()
        if not bucket or not key:
            return None

        try:
            return object_storage.generate_presigned_get_url(bucket, key, expires_in_seconds)
        except (object_storage.ObjectStorageError, BotoCoreError, ClientError):
            return None
