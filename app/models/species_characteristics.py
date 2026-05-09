from app.extensions import db


class SpeciesCharacteristics(db.Model):
    __tablename__ = "species_characteristics"

    species_id = db.Column(
        db.BigInteger,
        db.ForeignKey("species.id", ondelete="CASCADE"),
        primary_key=True,
    )
    lum_mycelium = db.Column(db.Boolean)
    lum_basidiome = db.Column(db.Boolean)
    lum_stipe = db.Column(db.Boolean)
    lum_pileus = db.Column(db.Boolean)
    lum_lamellae = db.Column(db.Boolean)
    lum_spores = db.Column(db.Boolean)
    edible = db.Column(db.Boolean)
    cultivation_possible = db.Column(db.Boolean)
    cultivation = db.Column(db.Text)
    cultivation_pt = db.Column(db.Text)
    finding_tips = db.Column(db.Text)
    finding_tips_pt = db.Column(db.Text)
    nearby_trees = db.Column(db.Text)
    nearby_trees_pt = db.Column(db.Text)
    curiosities = db.Column(db.Text)
    curiosities_pt = db.Column(db.Text)
    general_description = db.Column(db.Text)
    general_description_pt = db.Column(db.Text)
    conservation_status = db.Column(db.Text)
    colors = db.Column(db.Text)
    colors_pt = db.Column(db.Text)
    size_cm = db.Column(db.Float)
    season_start_month = db.Column(db.SmallInteger)
    season_end_month = db.Column(db.SmallInteger)
    iucn_assessment_year = db.Column(db.Text)
    iucn_assessment_url = db.Column(db.Text)

    species = db.relationship("Species", back_populates="characteristics")
    growth_forms = db.relationship(
        "GrowthForm",
        secondary="species_characteristics_growth_forms",
        back_populates="species_characteristics",
    )
    substrates = db.relationship(
        "Substrate",
        secondary="species_characteristics_substrates",
        back_populates="species_characteristics",
    )
    nutrition_modes = db.relationship(
        "NutritionMode",
        secondary="species_characteristics_nutrition_modes",
        back_populates="species_characteristics",
    )
    habitats = db.relationship(
        "Habitat",
        secondary="species_characteristics_habitats",
        back_populates="species_characteristics",
    )
    decay_types = db.relationship(
        "DecayType",
        secondary="species_characteristics_decay_types",
        back_populates="species_characteristics",
    )

    def __repr__(self):
        return f"<SpeciesCharacteristics species_id={self.species_id}>"
