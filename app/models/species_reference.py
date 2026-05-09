from app.extensions import db


class SpeciesReference(db.Model):
    __tablename__ = "species_references"

    species_id = db.Column(
        db.BigInteger,
        db.ForeignKey("species.id", ondelete="CASCADE"),
        primary_key=True,
    )
    reference_id = db.Column(
        db.Integer,
        db.ForeignKey("references.id", ondelete="CASCADE"),
        primary_key=True,
    )
