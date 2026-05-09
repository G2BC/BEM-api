import app.utils.object_storage as object_storage
from app.extensions import db
from app.models.decay_type import DecayType
from app.models.growth_form import GrowthForm
from app.models.habitat import Habitat
from app.models.nutrition_mode import NutritionMode
from app.models.species import Species
from app.models.species_change_request import SpeciesChangeRequest, SpeciesPhotoRequest
from app.models.species_characteristics import SpeciesCharacteristics
from app.models.species_photo import SpeciesPhoto
from app.models.substrate import Substrate
from sqlalchemy.orm import selectinload


class SpeciesChangeRequestRepository:
    RELATION_FIELD_MODELS = {
        "growth_form_ids": GrowthForm,
        "substrate_ids": Substrate,
        "nutrition_mode_ids": NutritionMode,
        "habitat_ids": Habitat,
        "decay_type_ids": DecayType,
    }
    CHARACTERISTICS_FIELDS = {
        "cultivation_possible",
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
    }

    @classmethod
    def create(
        cls,
        species_id: int,
        proposed_data: dict,
        request_note: str | None,
        requester_name: str | None,
        requester_email: str | None,
        requester_institution: str | None,
        requested_by_user_id: int | None,
        photos_payload: list[dict],
    ) -> SpeciesChangeRequest:
        req = SpeciesChangeRequest(
            species_id=species_id,
            proposed_data=proposed_data or {},
            request_note=request_note,
            requester_name=requester_name,
            requester_email=requester_email,
            requester_institution=requester_institution,
            requested_by_user_id=requested_by_user_id,
            status=SpeciesChangeRequest.STATUS_PENDING,
        )

        for photo in photos_payload:
            req.photos.append(
                SpeciesPhotoRequest(
                    object_key=photo["object_key"],
                    bucket_name=photo.get("bucket_name"),
                    original_filename=photo.get("original_filename"),
                    mime_type=photo.get("mime_type"),
                    size_bytes=photo.get("size_bytes"),
                    checksum_sha256=photo.get("checksum_sha256"),
                    caption=photo.get("caption"),
                    license_code=photo.get("license_code"),
                    attribution=photo.get("attribution"),
                    rights_holder=photo.get("rights_holder"),
                    source_url=photo.get("source_url"),
                    declaration_accepted_at=photo.get("declaration_accepted_at"),
                )
            )

        db.session.add(req)
        db.session.commit()
        return cls.get_by_id(req.id)

    @classmethod
    def list(
        cls,
        status: str | None = None,
        page: int | None = None,
        per_page: int | None = None,
    ):
        query = SpeciesChangeRequest.query.options(
            selectinload(SpeciesChangeRequest.photos)
        ).order_by(SpeciesChangeRequest.created_at.desc())

        if status:
            query = query.filter(SpeciesChangeRequest.status == status)

        if page:
            return query.paginate(page=page, per_page=per_page, error_out=False)

        return query.all()

    @classmethod
    def get_by_id(cls, request_id: int) -> SpeciesChangeRequest | None:
        return (
            SpeciesChangeRequest.query.options(selectinload(SpeciesChangeRequest.photos))
            .filter(SpeciesChangeRequest.id == request_id)
            .first()
        )

    @classmethod
    def get_species_by_id(cls, species_id: int) -> Species | None:
        return (
            Species.query.options(
                selectinload(Species.characteristics).selectinload(
                    SpeciesCharacteristics.nutrition_modes
                ),
                selectinload(Species.characteristics).selectinload(
                    SpeciesCharacteristics.growth_forms
                ),
                selectinload(Species.characteristics).selectinload(
                    SpeciesCharacteristics.substrates
                ),
                selectinload(Species.characteristics).selectinload(
                    SpeciesCharacteristics.decay_types
                ),
                selectinload(Species.similar_species_links),
            )
            .filter(Species.id == species_id)
            .first()
        )

    @classmethod
    def apply_species_updates(cls, species: Species, proposed_data: dict) -> Species:
        characteristics = species.characteristics

        # Garante a regra do banco: start/end devem ser ambos nulos ou ambos preenchidos.
        has_start = "season_start_month" in proposed_data
        has_end = "season_end_month" in proposed_data
        if has_start or has_end:
            current_start = (
                getattr(characteristics, "season_start_month", None) if characteristics else None
            )
            current_end = (
                getattr(characteristics, "season_end_month", None) if characteristics else None
            )
            next_start = proposed_data.get("season_start_month", current_start)
            next_end = proposed_data.get("season_end_month", current_end)
            if (next_start is None) != (next_end is None):
                raise ValueError(
                    "`season_start_month` e `season_end_month` devem ser informados juntos."
                )

        for field, value in proposed_data.items():
            if field in cls.CHARACTERISTICS_FIELDS:
                if characteristics is None:
                    characteristics = SpeciesCharacteristics(species_id=species.id)
                    species.characteristics = characteristics
                if field == "habitat_ids":
                    habitat_ids = value or []
                    if habitat_ids:
                        habitats = (
                            Habitat.query.filter(
                                Habitat.id.in_(habitat_ids),
                                Habitat.is_active.is_(True),
                            )
                            .order_by(Habitat.id.asc())
                            .all()
                        )
                    else:
                        habitats = []
                    characteristics.habitats = habitats
                    continue
                if field == "growth_form_ids":
                    growth_form_ids = value or []
                    if growth_form_ids:
                        growth_forms = (
                            GrowthForm.query.filter(
                                GrowthForm.id.in_(growth_form_ids),
                                GrowthForm.is_active.is_(True),
                            )
                            .order_by(GrowthForm.id.asc())
                            .all()
                        )
                    else:
                        growth_forms = []
                    characteristics.growth_forms = growth_forms
                    continue
                if field == "substrate_ids":
                    substrate_ids = value or []
                    if substrate_ids:
                        substrates = (
                            Substrate.query.filter(
                                Substrate.id.in_(substrate_ids),
                                Substrate.is_active.is_(True),
                            )
                            .order_by(Substrate.id.asc())
                            .all()
                        )
                    else:
                        substrates = []
                    characteristics.substrates = substrates
                    continue
                if field == "nutrition_mode_ids":
                    nutrition_mode_ids = value or []
                    if nutrition_mode_ids:
                        nutrition_modes = (
                            NutritionMode.query.filter(
                                NutritionMode.id.in_(nutrition_mode_ids),
                                NutritionMode.is_active.is_(True),
                            )
                            .order_by(NutritionMode.id.asc())
                            .all()
                        )
                    else:
                        nutrition_modes = []
                    characteristics.nutrition_modes = nutrition_modes
                    continue
                if field == "decay_type_ids":
                    decay_type_ids = value or []
                    if decay_type_ids:
                        decay_types = (
                            DecayType.query.filter(
                                DecayType.id.in_(decay_type_ids),
                                DecayType.is_active.is_(True),
                            )
                            .order_by(DecayType.id.asc())
                            .all()
                        )
                    else:
                        decay_types = []
                    characteristics.decay_types = decay_types
                    continue
                setattr(characteristics, field, value)
                continue
            setattr(species, field, value)

        if characteristics is not None:
            db.session.add(characteristics)
        db.session.add(species)
        return species

    @classmethod
    def create_or_skip_species_photo_from_request(
        cls, species_id: int, photo_request: SpeciesPhotoRequest
    ) -> bool:
        url = cls._build_object_url(photo_request)
        already_exists = (
            SpeciesPhoto.query.filter(
                SpeciesPhoto.species_id == species_id,
                SpeciesPhoto.original_url == url,
            ).first()
            is not None
        )
        if already_exists:
            return False

        db.session.add(
            SpeciesPhoto(
                species_id=species_id,
                photo_id=-photo_request.id,
                medium_url=url,
                original_url=url,
                license_code=photo_request.license_code,
                attribution=(photo_request.attribution or "").strip() or None,
                rights_holder=(photo_request.rights_holder or "").strip() or None,
                source_url=photo_request.source_url,
                declaration_accepted_at=photo_request.declaration_accepted_at,
                source="BEM-Upload",
            )
        )
        return True

    @staticmethod
    def _build_object_url(photo_request: SpeciesPhotoRequest) -> str:
        if photo_request.bucket_name:
            return object_storage.build_public_object_url(
                photo_request.bucket_name, photo_request.object_key
            )
        return f"minio://{photo_request.object_key}"

    @classmethod
    def save_review(
        cls,
        req: SpeciesChangeRequest,
        status: str,
        reviewed_by_user_id: int,
        review_note: str | None,
    ) -> SpeciesChangeRequest:
        req.status = status
        req.reviewed_by_user_id = reviewed_by_user_id
        req.review_note = review_note
        req.reviewed_at = db.func.now()

        db.session.add(req)
        db.session.commit()
        return cls.get_by_id(req.id)

    @classmethod
    def reject_pending_by_species_id(
        cls,
        species_id: int,
        review_note: str | None = None,
        reviewed_by_user_id: int | None = None,
    ) -> int:
        pending_requests = (
            SpeciesChangeRequest.query.options(selectinload(SpeciesChangeRequest.photos))
            .filter(
                SpeciesChangeRequest.species_id == species_id,
                SpeciesChangeRequest.status == SpeciesChangeRequest.STATUS_PENDING,
            )
            .all()
        )

        if not pending_requests:
            return 0

        normalized_note = (review_note or "").strip() or None
        for req in pending_requests:
            req.status = SpeciesChangeRequest.STATUS_REJECTED
            req.review_note = normalized_note
            req.reviewed_at = db.func.now()
            if reviewed_by_user_id is not None:
                req.reviewed_by_user_id = reviewed_by_user_id

            for photo in req.photos:
                if photo.status == SpeciesChangeRequest.STATUS_PENDING:
                    photo.status = SpeciesChangeRequest.STATUS_REJECTED

            db.session.add(req)

        db.session.flush()
        return len(pending_requests)

    @classmethod
    def delete_all_by_species_id(cls, species_id: int) -> int:
        request_ids = [
            request_id
            for (request_id,) in db.session.query(SpeciesChangeRequest.id)
            .filter(SpeciesChangeRequest.species_id == species_id)
            .all()
        ]

        if not request_ids:
            return 0

        (
            SpeciesPhotoRequest.query.filter(
                SpeciesPhotoRequest.request_id.in_(request_ids)
            ).delete(synchronize_session=False)
        )
        deleted_requests = SpeciesChangeRequest.query.filter(
            SpeciesChangeRequest.id.in_(request_ids)
        ).delete(synchronize_session=False)
        db.session.flush()
        return int(deleted_requests or 0)
