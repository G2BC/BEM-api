from app.extensions import db
from app.models.species_photo import SpeciesPhoto
from sqlalchemy import func


class SpeciesPhotoRepository:
    @staticmethod
    def next_manual_photo_id(species_id: int) -> int:
        min_photo_id = (
            db.session.query(func.min(SpeciesPhoto.photo_id))
            .filter(SpeciesPhoto.species_id == species_id)
            .scalar()
        )
        if min_photo_id is None or min_photo_id >= 0:
            return -1
        return int(min_photo_id) - 1

    @staticmethod
    def save(photo) -> None:
        """add + commit."""
        db.session.add(photo)
        db.session.commit()

    @staticmethod
    def commit() -> None:
        db.session.commit()

    @staticmethod
    def delete(photo) -> None:
        """delete + commit."""
        db.session.delete(photo)
        db.session.commit()
