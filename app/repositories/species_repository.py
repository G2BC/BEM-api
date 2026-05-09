from app.extensions import db
from app.models.decay_type import DecayType
from app.models.distribution import Distribution
from app.models.growth_form import GrowthForm
from app.models.habitat import Habitat
from app.models.nutrition_mode import NutritionMode
from app.models.observation import Observation
from app.models.reference import Reference  # noqa: F401 – needed for selectinload
from app.models.species import Species
from app.models.species_characteristics import SpeciesCharacteristics
from app.models.species_photo import SpeciesPhoto
from app.models.species_similarity import SpeciesSimilarity
from app.models.substrate import Substrate
from app.utils.object_storage import normalize_object_url
from sqlalchemy import case, exists, func, or_
from sqlalchemy.orm import selectinload


class SpeciesRepository:
    DOMAIN_MODELS = {
        "growth_form": GrowthForm,
        "nutrition_mode": NutritionMode,
        "substrate": Substrate,
        "habitat": Habitat,
        "decay_type": DecayType,
    }
    THREATENED_IUCN_CATEGORIES = {
        "CR",
        "EN",
        "VU",
    }

    @classmethod
    def list(
        cls,
        search: str | None = "",
        country: str | None = "",
        bem: str | None = "",
        is_visible: bool | None = None,
        page: int | None = None,
        per_page: int | None = None,
        distributions: list[str] | None = None,
    ):
        observations_count = (
            db.session.query(
                Observation.species_id,
                func.count(Observation.id).label("observations_count"),
            )
            .group_by(Observation.species_id)
            .subquery()
        )
        has_any_photo = exists().where(SpeciesPhoto.species_id == Species.id)
        photo_priority = case(
            (Species.id == 116, 0),
            (has_any_photo, 1),
            else_=2,
        )

        base = (
            db.session.query(
                Species,
                func.coalesce(observations_count.c.observations_count, 0).label(
                    "observations_count"
                ),
            )
            .outerjoin(observations_count, observations_count.c.species_id == Species.id)
            .options(
                selectinload(Species.photos),
                selectinload(Species.characteristics).selectinload(
                    SpeciesCharacteristics.nutrition_modes
                ),
                selectinload(Species.characteristics).selectinload(SpeciesCharacteristics.habitats),
                selectinload(Species.characteristics).selectinload(
                    SpeciesCharacteristics.growth_forms
                ),
                selectinload(Species.characteristics).selectinload(
                    SpeciesCharacteristics.substrates
                ),
                selectinload(Species.characteristics).selectinload(
                    SpeciesCharacteristics.decay_types
                ),
                selectinload(Species.similar_species_links).selectinload(
                    SpeciesSimilarity.similar_species
                ),
            )
            .order_by(photo_priority, Species.scientific_name.asc())
        )

        filters = []

        if search := (search or "").strip():
            filters.append(Species.scientific_name.ilike(f"%{search}%"))

        if country:
            filters.append(Species.type_country.ilike(f"%{country}%"))

        if bem:
            filters.append(Species.bem == bem)

        if is_visible is not None:
            filters.append(Species.is_visible.is_(is_visible))

        if distributions:
            slugs = [s.strip().upper() for s in distributions if s.strip()]
            if slugs:
                base = base.filter(Species.distributions.any(Distribution.slug.in_(slugs)))

        if filters:
            base = base.filter(*filters)

        if page:
            result = base.paginate(page=page, per_page=per_page, error_out=False)
            result.items = [cls._attach_observations_count(row) for row in result.items]
            return result

        return [cls._attach_observations_count(row) for row in base.all()]

    @staticmethod
    def _attach_observations_count(row):
        species, observations_count = row
        species.observations_count = int(observations_count or 0)
        return species

    @classmethod
    def statistics(cls) -> dict[str, int]:
        is_bem = Species.bem.in_(["BEM1", "BEM2", "BEM3", "BEM4", "BEM5", "BEM6"])
        is_brazilian_type = (Species.brazilian_type == "T") | (
            Species.brazilian_type_synonym == "TS"
        )
        normalized_iucn = func.upper(func.trim(SpeciesCharacteristics.conservation_status))

        species_counts = Species.query.outerjoin(
            SpeciesCharacteristics,
            SpeciesCharacteristics.species_id == Species.id,
        ).with_entities(
            func.count(Species.id).filter(is_bem).label("edible_brazil_species"),
            func.count(Species.id)
            .filter(normalized_iucn.in_(cls.THREATENED_IUCN_CATEGORIES))
            .label("extinction_risk_species"),
            func.count(Species.id).filter(is_brazilian_type).label("brazilian_type_species"),
        ).one()

        observations = Observation.query.with_entities(func.count(Observation.id)).scalar() or 0

        return {
            "edible_brazil_species": int(species_counts.edible_brazil_species or 0),
            "observations": int(observations),
            "extinction_risk_species": int(species_counts.extinction_risk_species or 0),
            "brazilian_type_species": int(species_counts.brazilian_type_species or 0),
        }

    @classmethod
    def get(cls, species: str | None = "", is_visible: bool | None = None):
        if not species:
            return None

        base = Species.query.options(
            selectinload(Species.photos),
            selectinload(Species.taxonomy),
            selectinload(Species.references),
            selectinload(Species.distributions),
            selectinload(Species.characteristics).selectinload(
                SpeciesCharacteristics.nutrition_modes
            ),
            selectinload(Species.characteristics).selectinload(SpeciesCharacteristics.habitats),
            selectinload(Species.characteristics).selectinload(SpeciesCharacteristics.growth_forms),
            selectinload(Species.characteristics).selectinload(SpeciesCharacteristics.substrates),
            selectinload(Species.characteristics).selectinload(SpeciesCharacteristics.decay_types),
            selectinload(Species.similar_species_links).selectinload(
                SpeciesSimilarity.similar_species
            ),
        ).order_by(Species.scientific_name.asc())

        if is_visible is not None:
            base = base.filter(Species.is_visible.is_(is_visible))

        if species.isdigit():
            id = int(species)
            base = base.filter(Species.id == id)
        else:
            name = species.replace("+", " ")
            base = base.filter(Species.scientific_name.ilike(f"%{name}%"))

        return base.first()

    @classmethod
    def country_select(
        cls,
        search: str | None = "",
    ):
        search = (search or "").strip()

        query = Species.query.with_entities(Species.type_country).distinct()

        if search:
            query = query.filter(Species.type_country.ilike(f"%{search}%"))

        query = query.order_by(Species.type_country.asc())

        countries = query.all()

        options = [{"label": country, "value": country} for (country,) in countries if country]

        return options

    @classmethod
    def bem_select(
        cls,
        search: str | None = "",
    ):
        search = (search or "").strip()

        query = (
            Species.query.with_entities(Species.bem)
            .filter(Species.bem.isnot(None), func.trim(Species.bem) != "")
            .group_by(Species.bem)
        )

        if search:
            query = query.filter(Species.bem.ilike(f"%{search}%"))

        query = query.order_by(
            case((Species.bem.ilike("BEM%"), 0), else_=1).asc(),
            func.length(Species.bem).asc(),
            Species.bem.asc()
        )

        bems = query.all()

        options = [{"label": bem, "value": bem} for (bem,) in bems if bem]

        return options

    @classmethod
    def distributions_select(cls):
        distributions = Distribution.query.order_by(Distribution.slug.asc()).all()
        return distributions

    @classmethod
    def species_select(
        cls,
        search: str | None = "",
        exclude_species_id: int | None = None,
    ):
        search = (search or "").strip()
        query = Species.query.options(selectinload(Species.photos))

        if search:
            query = query.filter(Species.scientific_name.ilike(f"%{search}%"))
        if exclude_species_id is not None:
            query = query.filter(Species.id != exclude_species_id)

        species_list = query.order_by(Species.scientific_name.asc()).all()

        def pick_photo(species: Species) -> str | None:
            photos = getattr(species, "photos", None) or []
            if not photos:
                return None

            ordered = sorted(photos, key=lambda photo: getattr(photo, "photo_id", 0))
            featured = next(
                (photo for photo in ordered if bool(getattr(photo, "featured", False))),
                None,
            )
            chosen = featured or ordered[0]
            return normalize_object_url(getattr(chosen, "medium_url", None))

        return [
            {
                "id": item.id,
                "label": item.scientific_name,
                "photo": pick_photo(item),
            }
            for item in species_list
        ]

    @classmethod
    def domain_select(
        cls,
        domain: str,
        search: str | None = "",
    ):
        model = cls.DOMAIN_MODELS.get((domain or "").strip().lower())
        if not model:
            allowed = ", ".join(sorted(cls.DOMAIN_MODELS.keys()))
            raise ValueError(f"`domain` inválido. Use um de: {allowed}")

        search = (search or "").strip()
        query = model.query.filter(model.is_active.is_(True))

        if search:
            query = query.filter(
                (model.label_pt.ilike(f"%{search}%"))
                | (model.label_en.ilike(f"%{search}%"))
                | (model.slug.ilike(f"%{search}%"))
            )

        items = query.order_by(model.label_pt.asc()).all()

        return [
            {
                "value": item.id,
                "label_pt": item.label_pt,
                "label_en": item.label_en,
            }
            for item in items
        ]

    @classmethod
    def get_ncbi_taxon_id(cls, species_id: str | None = ""):
        species_id = (species_id or "").strip()
        if not species_id or not species_id.isdigit():
            return None

        species = (
            Species.query.filter(Species.id == int(species_id))
            .where(Species.ncbi_taxonomy_id.is_not(None))
            .first()
        )

        if not species:
            return None

        return species.ncbi_taxonomy_id

    @staticmethod
    def get_by_id(species_id: int) -> "Species | None":
        return (
            Species.query.options(selectinload(Species.references))
            .filter(Species.id == species_id)
            .first()
        )

    @classmethod
    def exists_by_id(cls, species_id: str | None = "") -> bool:
        if not species_id:
            return False

        species = Species.query.with_entities(Species.id).filter(Species.id == species_id).first()
        return species is not None

    @classmethod
    def list_outdated(cls, page: int | None = None, per_page: int | None = None):
        base = (
            Species.query.with_entities(
                Species.id, Species.scientific_name, Species.mycobank_index_fungorum_id
            )
            .filter(Species.is_outdated_mycobank.is_(True))
            .order_by(Species.scientific_name.asc())
        )

        if page:
            return base.paginate(page=page, per_page=per_page, error_out=False)
        return base.all()

    @staticmethod
    def stage(species) -> None:
        """add + flush to generate the species.id without committing."""
        db.session.add(species)
        db.session.flush()

    @staticmethod
    def save(species) -> None:
        """add + commit."""
        db.session.add(species)
        db.session.commit()

    @staticmethod
    def rollback() -> None:
        db.session.rollback()

    @staticmethod
    def delete(species) -> None:
        """delete + commit. IntegrityError propagates to the caller."""
        db.session.delete(species)
        db.session.commit()

    @staticmethod
    def delete_similarities_by_species_id(species_id: int) -> None:
        SpeciesSimilarity.query.filter(
            or_(
                SpeciesSimilarity.species_id == species_id,
                SpeciesSimilarity.similar_species_id == species_id,
            )
        ).delete(synchronize_session=False)
