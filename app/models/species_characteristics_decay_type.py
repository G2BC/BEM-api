from app.extensions import db


class SpeciesCharacteristicsDecayType(db.Model):
    __tablename__ = "species_characteristics_decay_types"
    __table_args__ = (db.Index("idx_scdt_decay_type_id", "decay_type_id"),)

    species_id = db.Column(
        db.BigInteger,
        db.ForeignKey("species_characteristics.species_id", ondelete="CASCADE"),
        primary_key=True,
    )
    decay_type_id = db.Column(
        db.Integer,
        db.ForeignKey("decay_types.id", ondelete="CASCADE"),
        primary_key=True,
    )
