from app.extensions import db


class SpeciesDistribution(db.Model):
    __tablename__ = "species_distributions"

    species_id = db.Column(
        db.BigInteger,
        db.ForeignKey("species.id", ondelete="CASCADE"),
        primary_key=True,
    )
    distribution_id = db.Column(
        db.Integer,
        db.ForeignKey("distributions.id", ondelete="CASCADE"),
        primary_key=True,
    )
