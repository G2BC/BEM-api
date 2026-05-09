from app.extensions import db


class SpeciesCharacteristicsGrowthForm(db.Model):
    __tablename__ = "species_characteristics_growth_forms"
    __table_args__ = (db.Index("idx_scgf_growth_form_id", "growth_form_id"),)

    species_id = db.Column(
        db.BigInteger,
        db.ForeignKey("species_characteristics.species_id", ondelete="CASCADE"),
        primary_key=True,
    )
    growth_form_id = db.Column(
        db.Integer,
        db.ForeignKey("growth_forms.id", ondelete="CASCADE"),
        primary_key=True,
    )
