from app.extensions import db


class SpeciesCharacteristicsSubstrate(db.Model):
    __tablename__ = "species_characteristics_substrates"
    __table_args__ = (db.Index("idx_scs_substrate_id", "substrate_id"),)

    species_id = db.Column(
        db.BigInteger,
        db.ForeignKey("species_characteristics.species_id", ondelete="CASCADE"),
        primary_key=True,
    )
    substrate_id = db.Column(
        db.Integer,
        db.ForeignKey("substrates.id", ondelete="CASCADE"),
        primary_key=True,
    )
