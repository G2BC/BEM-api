from sqlalchemy.sql import func

from app.extensions import db


class SpeciesPhoto(db.Model):
    __tablename__ = "species_photos"
    __table_args__ = (
        db.PrimaryKeyConstraint("species_id", "photo_id", name="pk_species_photo"),
        db.Index("idx_species_photos_species", "species_id"),
    )

    species_id = db.Column(
        db.BigInteger,
        db.ForeignKey("species.id", ondelete="CASCADE"),
        nullable=False,
    )
    photo_id = db.Column(db.BigInteger, nullable=False)
    medium_url = db.Column(db.Text, nullable=False)
    original_url = db.Column(db.Text)
    license_code = db.Column(db.Text)
    attribution = db.Column(db.Text)
    rights_holder = db.Column(db.Text)
    source_url = db.Column(db.Text)
    declaration_accepted_at = db.Column(db.DateTime(timezone=True), nullable=True)
    source = db.Column(db.Text, nullable=False, server_default="iNaturalist")
    fetched_at = db.Column(
        db.DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
    )
    lumm = db.Column(db.Boolean, nullable=True)
    featured = db.Column(db.Boolean, nullable=True)

    species = db.relationship("Species", back_populates="photos")

    def __repr__(self):
        return f"<SpeciesPhoto species_id={self.species_id} photo_id={self.photo_id}>"
