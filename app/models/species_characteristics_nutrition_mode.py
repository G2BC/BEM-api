from app.extensions import db


class SpeciesCharacteristicsNutritionMode(db.Model):
    __tablename__ = "species_characteristics_nutrition_modes"
    __table_args__ = (db.Index("idx_scnm_nutrition_mode_id", "nutrition_mode_id"),)

    species_id = db.Column(
        db.BigInteger,
        db.ForeignKey("species_characteristics.species_id", ondelete="CASCADE"),
        primary_key=True,
    )
    nutrition_mode_id = db.Column(
        db.Integer,
        db.ForeignKey("nutrition_modes.id", ondelete="CASCADE"),
        primary_key=True,
    )
