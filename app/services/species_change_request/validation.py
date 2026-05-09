from typing import Any

import requests
from app.exceptions import AppError
from app.models.decay_type import DecayType
from app.models.growth_form import GrowthForm
from app.models.habitat import Habitat
from app.models.nutrition_mode import NutritionMode
from app.models.substrate import Substrate
from flask import current_app


class SpeciesChangeRequestValidation:
    TRANSLATABLE_FIELDS = {
        "colors": "colors_pt",
        "cultivation": "cultivation_pt",
        "finding_tips": "finding_tips_pt",
        "nearby_trees": "nearby_trees_pt",
        "curiosities": "curiosities_pt",
        "general_description": "general_description_pt",
    }
    TRANSLATABLE_PAIR_MAP = {
        **TRANSLATABLE_FIELDS,
        **{pt_field: en_field for en_field, pt_field in TRANSLATABLE_FIELDS.items()},
    }

    # ------------------------------------------------------------------ #
    # Translation                                                          #
    # ------------------------------------------------------------------ #

    @staticmethod
    def normalize_optional_text(value: Any) -> str:
        if value is None:
            return ""
        if not isinstance(value, str):
            raise AppError(
                pt="Campos textuais traduzíveis devem ser string",
                en="Translatable text fields must be strings",
            )
        return value.strip()

    @classmethod
    def normalize_translatable_fields(
        cls, proposed_data: dict[str, Any], source_lang: str
    ) -> dict[str, Any]:
        normalized = dict(proposed_data or {})
        queued_texts: list[str] = []
        queued_fields: list[tuple[str, str, str]] = []

        for en_field, pt_field in cls.TRANSLATABLE_FIELDS.items():
            source_text = cls.normalize_optional_text(
                normalized.get(en_field) or normalized.get(pt_field)
            )
            normalized.pop(en_field, None)
            normalized.pop(pt_field, None)
            if not source_text:
                continue
            queued_fields.append((en_field, pt_field, source_text))
            queued_texts.append(source_text)

        if not queued_fields:
            return normalized

        translated_texts = cls.translate_texts_with_deepl(queued_texts, source_lang=source_lang)
        if len(translated_texts) != len(queued_fields):
            raise AppError(
                pt="Falha ao traduzir campos textuais",
                en="Failed to translate text fields",
            )

        for index, (en_field, pt_field, source_text) in enumerate(queued_fields):
            translated_text = (translated_texts[index] or "").strip() or source_text
            if source_lang == "pt":
                normalized[pt_field] = source_text
                normalized[en_field] = translated_text
            else:
                normalized[en_field] = source_text
                normalized[pt_field] = translated_text

        return normalized

    @classmethod
    def translate_texts_with_deepl(cls, texts: list[str], source_lang: str) -> list[str]:
        if not texts:
            return []

        api_key = (current_app.config.get("DEEPL_API_KEY") or "").strip()

        env = current_app.config.get("APP_ENV")
        if env == "development" and not api_key:
            return texts

        if not api_key:
            raise AppError(
                pt="DEEPL_API_KEY não configurada no servidor",
                en="DEEPL_API_KEY not configured on the server",
            )

        api_url = (
            current_app.config.get("DEEPL_API_URL") or "https://api-free.deepl.com/v2/translate"
        ).strip()
        timeout_seconds = float(current_app.config.get("DEEPL_TIMEOUT_SECONDS", 45))

        source = "PT" if source_lang == "pt" else "EN"
        target = "EN" if source_lang == "pt" else "PT-BR"

        payload: list[tuple[str, str]] = [("source_lang", source), ("target_lang", target)]
        for text in texts:
            payload.append(("text", text))

        try:
            response = requests.post(
                api_url,
                data=payload,
                headers={
                    "Authorization": f"DeepL-Auth-Key {api_key}",
                    "Accept": "application/json",
                },
                timeout=timeout_seconds,
            )
        except requests.RequestException as exc:
            raise AppError(
                pt=f"Falha ao conectar no serviço de tradução: {exc}",
                en=f"Failed to connect to translation service: {exc}",
            ) from exc

        if response.status_code >= 400:
            details = (response.text or "").strip()
            raise AppError(
                pt=f"Erro na tradução automática (DeepL): {details or response.status_code}",
                en=f"Automatic translation error (DeepL): {details or response.status_code}",
            )

        data = response.json() if response.content else {}
        translations = data.get("translations") if isinstance(data, dict) else None
        if not isinstance(translations, list):
            raise AppError(
                pt="Resposta inválida da tradução automática (DeepL)",
                en="Invalid response from automated translation (DeepL)",
            )

        result: list[str] = []
        for item in translations:
            if not isinstance(item, dict):
                continue
            text = item.get("text")
            if isinstance(text, str):
                result.append(text)

        return result

    # ------------------------------------------------------------------ #
    # Input validation                                                     #
    # ------------------------------------------------------------------ #

    @staticmethod
    def validate_proposed_data(
        proposed_data: dict[str, Any], species_id: int | None = None
    ) -> None:
        edible = proposed_data.get("edible")
        if edible is not None and not isinstance(edible, bool):
            raise AppError(
                pt="`edible` deve ser booleano ou null", en="`edible` must be boolean or null"
            )

        size_cm = proposed_data.get("size_cm")
        if size_cm is not None:
            if isinstance(size_cm, bool) or not isinstance(size_cm, int | float):
                raise AppError(pt="`size_cm` deve ser numérico", en="`size_cm` must be numeric")
            if size_cm < 0:
                raise AppError(pt="`size_cm` deve ser >= 0", en="`size_cm` must be >= 0")

        growth_form_ids = proposed_data.get("growth_form_ids")
        if growth_form_ids is not None:
            if not isinstance(growth_form_ids, list):
                raise AppError(
                    pt="`growth_form_ids` deve ser uma lista de inteiros",
                    en="`growth_form_ids` must be a list of integers",
                )
            normalized_growth_form_ids = []
            for growth_form_value in growth_form_ids:
                if isinstance(growth_form_value, bool) or not isinstance(growth_form_value, int):
                    raise AppError(
                        pt="`growth_form_ids` deve conter apenas inteiros",
                        en="`growth_form_ids` must contain only integers",
                    )
                if growth_form_value < 1:
                    raise AppError(
                        pt="`growth_form_ids` deve conter apenas inteiros >= 1",
                        en="`growth_form_ids` must contain only integers >= 1",
                    )
                normalized_growth_form_ids.append(growth_form_value)
            unique_growth_form_ids = sorted(set(normalized_growth_form_ids))
            if len(unique_growth_form_ids) != len(normalized_growth_form_ids):
                raise AppError(
                    pt="`growth_form_ids` contém IDs duplicados",
                    en="`growth_form_ids` contains duplicate IDs",
                )
            if unique_growth_form_ids:
                active_count = GrowthForm.query.filter(
                    GrowthForm.id.in_(unique_growth_form_ids),
                    GrowthForm.is_active.is_(True),
                ).count()
                if active_count != len(unique_growth_form_ids):
                    raise AppError(
                        pt="`growth_form_ids` contém IDs inválidos ou inativos",
                        en="`growth_form_ids` contains invalid or inactive IDs",
                    )

        substrate_ids = proposed_data.get("substrate_ids")
        if substrate_ids is not None:
            if not isinstance(substrate_ids, list):
                raise AppError(
                    pt="`substrate_ids` deve ser uma lista de inteiros",
                    en="`substrate_ids` must be a list of integers",
                )
            normalized_substrate_ids = []
            for substrate_value in substrate_ids:
                if isinstance(substrate_value, bool) or not isinstance(substrate_value, int):
                    raise AppError(
                        pt="`substrate_ids` deve conter apenas inteiros",
                        en="`substrate_ids` must contain only integers",
                    )
                if substrate_value < 1:
                    raise AppError(
                        pt="`substrate_ids` deve conter apenas inteiros >= 1",
                        en="`substrate_ids` must contain only integers >= 1",
                    )
                normalized_substrate_ids.append(substrate_value)
            unique_substrate_ids = sorted(set(normalized_substrate_ids))
            if len(unique_substrate_ids) != len(normalized_substrate_ids):
                raise AppError(
                    pt="`substrate_ids` contém IDs duplicados",
                    en="`substrate_ids` contains duplicate IDs",
                )
            if unique_substrate_ids:
                active_count = Substrate.query.filter(
                    Substrate.id.in_(unique_substrate_ids),
                    Substrate.is_active.is_(True),
                ).count()
                if active_count != len(unique_substrate_ids):
                    raise AppError(
                        pt="`substrate_ids` contém IDs inválidos ou inativos",
                        en="`substrate_ids` contains invalid or inactive IDs",
                    )

        nutrition_mode_ids = proposed_data.get("nutrition_mode_ids")
        if nutrition_mode_ids is not None:
            if not isinstance(nutrition_mode_ids, list):
                raise AppError(
                    pt="`nutrition_mode_ids` deve ser uma lista de inteiros",
                    en="`nutrition_mode_ids` must be a list of integers",
                )
            normalized_nutrition_mode_ids = []
            for nutrition_mode_value in nutrition_mode_ids:
                if isinstance(nutrition_mode_value, bool) or not isinstance(
                    nutrition_mode_value, int
                ):
                    raise AppError(
                        pt="`nutrition_mode_ids` deve conter apenas inteiros",
                        en="`nutrition_mode_ids` must contain only integers",
                    )
                if nutrition_mode_value < 1:
                    raise AppError(
                        pt="`nutrition_mode_ids` deve conter apenas inteiros >= 1",
                        en="`nutrition_mode_ids` must contain only integers >= 1",
                    )
                normalized_nutrition_mode_ids.append(nutrition_mode_value)

            unique_nutrition_mode_ids = sorted(set(normalized_nutrition_mode_ids))
            if len(unique_nutrition_mode_ids) != len(normalized_nutrition_mode_ids):
                raise AppError(
                    pt="`nutrition_mode_ids` contém IDs duplicados",
                    en="`nutrition_mode_ids` contains duplicate IDs",
                )

            if unique_nutrition_mode_ids:
                active_count = NutritionMode.query.filter(
                    NutritionMode.id.in_(unique_nutrition_mode_ids),
                    NutritionMode.is_active.is_(True),
                ).count()
                if active_count != len(unique_nutrition_mode_ids):
                    raise AppError(
                        pt="`nutrition_mode_ids` contém IDs inválidos ou inativos",
                        en="`nutrition_mode_ids` contains invalid or inactive IDs",
                    )

        habitat_ids = proposed_data.get("habitat_ids")
        if habitat_ids is not None:
            if not isinstance(habitat_ids, list):
                raise AppError(
                    pt="`habitat_ids` deve ser uma lista de inteiros",
                    en="`habitat_ids` must be a list of integers",
                )
            normalized_habitat_ids = []
            for hid in habitat_ids:
                if isinstance(hid, bool) or not isinstance(hid, int):
                    raise AppError(
                        pt="`habitat_ids` deve conter apenas inteiros",
                        en="`habitat_ids` must contain only integers",
                    )
                if hid < 1:
                    raise AppError(
                        pt="`habitat_ids` deve conter apenas inteiros >= 1",
                        en="`habitat_ids` must contain only integers >= 1",
                    )
                normalized_habitat_ids.append(hid)

            unique_habitat_ids = sorted(set(normalized_habitat_ids))
            if len(unique_habitat_ids) != len(normalized_habitat_ids):
                raise AppError(
                    pt="`habitat_ids` contém IDs duplicados",
                    en="`habitat_ids` contains duplicate IDs",
                )

            if unique_habitat_ids:
                active_count = Habitat.query.filter(
                    Habitat.id.in_(unique_habitat_ids),
                    Habitat.is_active.is_(True),
                ).count()
                if active_count != len(unique_habitat_ids):
                    raise AppError(
                        pt="`habitat_ids` contém IDs inválidos ou inativos",
                        en="`habitat_ids` contains invalid or inactive IDs",
                    )

        decay_type_ids = proposed_data.get("decay_type_ids")
        if decay_type_ids is not None:
            if not isinstance(decay_type_ids, list):
                raise AppError(
                    pt="`decay_type_ids` deve ser uma lista de inteiros",
                    en="`decay_type_ids` must be a list of integers",
                )
            normalized_decay_type_ids = []
            for dtid in decay_type_ids:
                if isinstance(dtid, bool) or not isinstance(dtid, int):
                    raise AppError(
                        pt="`decay_type_ids` deve conter apenas inteiros",
                        en="`decay_type_ids` must contain only integers",
                    )
                if dtid < 1:
                    raise AppError(
                        pt="`decay_type_ids` deve conter apenas inteiros >= 1",
                        en="`decay_type_ids` must contain only integers >= 1",
                    )
                normalized_decay_type_ids.append(dtid)

            unique_decay_type_ids = sorted(set(normalized_decay_type_ids))
            if len(unique_decay_type_ids) != len(normalized_decay_type_ids):
                raise AppError(
                    pt="`decay_type_ids` contém IDs duplicados",
                    en="`decay_type_ids` contains duplicate IDs",
                )

            if unique_decay_type_ids:
                active_count = DecayType.query.filter(
                    DecayType.id.in_(unique_decay_type_ids),
                    DecayType.is_active.is_(True),
                ).count()
                if active_count != len(unique_decay_type_ids):
                    raise AppError(
                        pt="`decay_type_ids` contém IDs inválidos ou inativos",
                        en="`decay_type_ids` contains invalid or inactive IDs",
                    )

        start = proposed_data.get("season_start_month")
        end = proposed_data.get("season_end_month")

        if start is None and end is None:
            return
        if start is None or end is None:
            raise AppError(
                pt="`season_start_month` e `season_end_month` devem ser informados juntos",
                en="`season_start_month` and `season_end_month` must be provided together",
            )
        if not isinstance(start, int) or not isinstance(end, int):
            raise AppError(
                pt="`season_start_month` e `season_end_month` devem ser inteiros",
                en="`season_start_month` and `season_end_month` must be integers",
            )
        if start < 1 or start > 12 or end < 1 or end > 12:
            raise AppError(
                pt="`season_start_month` e `season_end_month` devem estar entre 1 e 12",
                en="`season_start_month` and `season_end_month` must be between 1 and 12",
            )

    @staticmethod
    def validate_photos_payload(photos_payload: list[dict[str, Any]]) -> None:
        max_photos = int(current_app.config["SPECIES_REQUEST_MAX_PHOTOS"])
        if len(photos_payload) > max_photos:
            raise AppError(
                pt=f"Máximo de {max_photos} fotos por solicitação",
                en=f"Maximum of {max_photos} photos per request",
            )

    # ------------------------------------------------------------------ #
    # Review normalization                                                 #
    # ------------------------------------------------------------------ #

    @staticmethod
    def normalize_review_decision(value: str | None, field_name: str) -> str | None:
        normalized = (value or "").strip().lower()
        if not normalized:
            return None
        if normalized not in {"approve", "reject"}:
            raise AppError(
                pt=f"`{field_name}` deve ser `approve` ou `reject`",
                en=f"`{field_name}` must be `approve` or `reject`",
            )
        return normalized

    @classmethod
    def expand_translatable_decision_map(
        cls, decision_map: dict[str, str], proposed_data: dict[str, Any]
    ) -> dict[str, str]:
        if not decision_map:
            return decision_map

        expanded = dict(decision_map)
        for field, decision in list(decision_map.items()):
            paired_field = cls.TRANSLATABLE_PAIR_MAP.get(field)
            if not paired_field or paired_field not in proposed_data:
                continue

            paired_decision = expanded.get(paired_field)
            if paired_decision and paired_decision != decision:
                raise AppError(
                    pt=(
                        f"Campos traduzíveis devem ter a mesma decisão:"
                        f" `{field}` e `{paired_field}`"
                    ),
                    en=(
                        f"Translatable fields must have the same decision:"
                        f" `{field}` and `{paired_field}`"
                    ),
                )
            expanded[paired_field] = decision

        return expanded

    @classmethod
    def normalize_proposed_data_field_decisions(
        cls, proposed_data_fields: list[dict[str, Any]]
    ) -> list[dict[str, str]]:
        normalized = []
        seen_fields = set()

        for item in proposed_data_fields:
            field_name = str(item.get("field") or "").strip()
            if not field_name:
                raise AppError(
                    pt="`proposed_data_fields.field` é obrigatório",
                    en="`proposed_data_fields.field` is required",
                )
            if field_name in seen_fields:
                raise AppError(
                    pt=f"`field` duplicado em `proposed_data_fields`: {field_name}",
                    en=f"Duplicate `field` in `proposed_data_fields`: {field_name}",
                )
            seen_fields.add(field_name)

            decision = cls.normalize_review_decision(
                item.get("decision"), "proposed_data_fields.decision"
            )
            if not decision:
                raise AppError(
                    pt="`proposed_data_fields.decision` é obrigatório",
                    en="`proposed_data_fields.decision` is required",
                )

            normalized.append({"field": field_name, "decision": decision})

        return normalized

    @classmethod
    def normalize_photo_decisions(
        cls, photo_decisions: list[dict[str, Any]]
    ) -> list[dict[str, Any]]:
        normalized = []
        seen_ids = set()

        for item in photo_decisions:
            raw_photo_id = item.get("photo_request_id")
            try:
                photo_id = int(raw_photo_id)
            except (TypeError, ValueError):
                raise AppError(
                    pt="`photo_request_id` deve ser inteiro positivo",
                    en="`photo_request_id` must be a positive integer",
                )
            if photo_id < 1:
                raise AppError(
                    pt="`photo_request_id` deve ser inteiro positivo",
                    en="`photo_request_id` must be a positive integer",
                )
            if photo_id in seen_ids:
                raise AppError(
                    pt=f"`photo_request_id` duplicado: {photo_id}",
                    en=f"Duplicate `photo_request_id`: {photo_id}",
                )
            seen_ids.add(photo_id)

            decision = cls.normalize_review_decision(item.get("decision"), "photos.decision")
            if not decision:
                raise AppError(
                    pt="`photos.decision` é obrigatório", en="`photos.decision` is required"
                )
            normalized.append({"photo_request_id": photo_id, "decision": decision})

        return normalized

    @staticmethod
    def parse_id(raw_id: str) -> int:
        try:
            value = int(raw_id)
        except (TypeError, ValueError):
            raise AppError(pt="ID inválido", en="Invalid ID")
        if value < 1:
            raise AppError(pt="ID inválido", en="Invalid ID")
        return value
