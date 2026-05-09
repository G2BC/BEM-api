from sqlalchemy.sql import func

from app.extensions import db


class Observation(db.Model):
    __tablename__ = "observations"
    __table_args__ = (
        db.UniqueConstraint("source", "external_id", name="uq_observation_source_external_id"),
        db.Index("idx_observation_species_id", "species_id"),
        db.Index("idx_observation_source", "source"),
    )

    id = db.Column(db.BigInteger, primary_key=True, autoincrement=True)

    species_id = db.Column(
        db.BigInteger,
        db.ForeignKey("species.id", ondelete="CASCADE"),
        nullable=False,
    )

    source = db.Column(
        db.Text,
        nullable=False,
    )  # "inaturalist" | "mushroom_observer" | "specieslink" | "bold"
    external_id = db.Column(db.Text, nullable=False)

    latitude = db.Column(db.Numeric(10, 6), nullable=True)
    longitude = db.Column(db.Numeric(10, 6), nullable=True)
    location_obscured = db.Column(db.Boolean, nullable=False, server_default=db.false())

    observed_on = db.Column(db.Date, nullable=True)
    quality_grade = db.Column(db.Text, nullable=True)
    photo_url = db.Column(db.Text, nullable=True)
    url = db.Column(db.Text, nullable=True)

    created_at = db.Column(
        db.DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
    )
    updated_at = db.Column(
        db.DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
        onupdate=func.now(),
    )

    species = db.relationship("Species", back_populates="observations")

    def __repr__(self):
        return f"<Observation id={self.id} source={self.source!r} external_id={self.external_id!r}>"
