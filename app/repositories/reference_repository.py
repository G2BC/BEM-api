from app.extensions import db
from app.models.reference import Reference
from app.models.species_reference import SpeciesReference


class ReferenceRepository:
    @staticmethod
    def search(search: str | None = "", limit: int = 30) -> list[Reference]:
        q = Reference.query
        if search := (search or "").strip():
            term = f"%{search}%"
            q = q.filter(
                db.or_(
                    Reference.apa.ilike(term),
                    Reference.doi.ilike(term),
                    Reference.url.ilike(term),
                )
            )
        return q.order_by(Reference.apa.asc()).limit(limit).all()

    @staticmethod
    def get_by_id(reference_id: int) -> Reference | None:
        return db.session.get(Reference, reference_id)

    @staticmethod
    def count_species(reference_id: int) -> int:
        return (
            db.session.query(SpeciesReference)
            .filter(SpeciesReference.reference_id == reference_id)
            .count()
        )

    @staticmethod
    def commit() -> None:
        db.session.commit()
