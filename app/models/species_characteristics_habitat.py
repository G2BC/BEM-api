from app.extensions import db


class SpeciesCharacteristicsHabitat(db.Model):
    __tablename__ = "species_characteristics_habitats"
    __table_args__ = (db.Index("idx_sch_habitat_id", "habitat_id"),)

    species_id = db.Column(
        db.BigInteger,
        db.ForeignKey("species_characteristics.species_id", ondelete="CASCADE"),
        primary_key=True,
    )
    habitat_id = db.Column(
        db.Integer,
        db.ForeignKey("habitats.id", ondelete="CASCADE"),
        primary_key=True,
    )
