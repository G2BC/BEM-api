from app.exceptions import AppError
from app.extensions import db
from app.models.reference import Reference
from app.models.species import Species
from app.repositories.reference_repository import ReferenceRepository
from app.repositories.species_repository import SpeciesRepository


class SpeciesReferenceService:
    @staticmethod
    def search(search: str | None = "", limit: int = 30) -> list[Reference]:
        return ReferenceRepository.search(search, limit)

    @classmethod
    def _get_species(cls, species_id: int) -> Species:
        species = SpeciesRepository.get_by_id(species_id)
        if not species:
            raise AppError(pt="Espécie não encontrada.", en="Species not found.", status=404)
        return species

    @classmethod
    def associate_existing(cls, species_id: int, reference_id: int) -> Reference:
        """Associate an already existing reference with a species."""
        species = cls._get_species(species_id)

        reference = ReferenceRepository.get_by_id(reference_id)
        if not reference:
            raise AppError(
                pt="Referência não encontrada.",
                en="Reference not found.",
                status=404,
            )

        if reference in species.references:
            raise AppError(
                pt="Esta referência já está associada a esta espécie.",
                en="This reference is already associated with this species.",
            )

        species.references.append(reference)
        ReferenceRepository.commit()
        return reference

    @classmethod
    def create_and_associate(
        cls, species_id: int, apa: str, doi: str | None, url: str | None
    ) -> Reference:
        """Create a new reference and immediately associate it with a species."""
        species = cls._get_species(species_id)

        apa = (apa or "").strip()
        doi = (doi or "").strip() or None
        url = (url or "").strip() or None

        if not apa:
            raise AppError(
                pt="A citação (APA) é obrigatória.",
                en="The APA citation is required.",
            )

        reference = Reference(apa=apa, doi=doi, url=url)
        db.session.add(reference)
        species.references.append(reference)
        db.session.commit()
        return reference

    @classmethod
    def update(
        cls, reference_id: int, apa: str | None, doi: str | None, url: str | None
    ) -> Reference:
        """Update fields of an existing reference."""
        reference = ReferenceRepository.get_by_id(reference_id)
        if not reference:
            raise AppError(
                pt="Referência não encontrada.",
                en="Reference not found.",
                status=404,
            )

        next_apa = (apa or "").strip()
        next_doi = (doi or "").strip() or None
        next_url = (url or "").strip() or None

        if not next_apa:
            raise AppError(
                pt="A citação (APA) é obrigatória.",
                en="The APA citation is required.",
            )

        reference.apa = next_apa
        reference.doi = next_doi
        reference.url = next_url

        ReferenceRepository.commit()
        return reference

    @classmethod
    def disassociate(cls, species_id: int, reference_id: int) -> None:
        """Remove association between a species and a reference.

        If the reference becomes orphaned (no other species use it), it is deleted.
        """
        species = cls._get_species(species_id)

        reference = ReferenceRepository.get_by_id(reference_id)
        if not reference or reference not in species.references:
            raise AppError(
                pt="Referência não encontrada nesta espécie.",
                en="Reference not found in this species.",
                status=404,
            )

        # Count before removal — if this is the only species using the reference, delete it too
        total_associations = ReferenceRepository.count_species(reference_id)

        species.references.remove(reference)

        if total_associations <= 1:
            db.session.delete(reference)

        db.session.commit()
