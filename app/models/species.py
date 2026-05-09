from sqlalchemy.sql import func

from app.extensions import db
from app.utils.generate_uuid import generate_uuid


class Species(db.Model):
    __tablename__ = "species"
    __table_args__ = (
        db.UniqueConstraint("scientific_name", name="uq_species_scientific_name"),
        db.UniqueConstraint("uid", name="uq_species_uid"),
        db.UniqueConstraint("common_name", name="uq_species_common_name"),
    )

    id = db.Column(db.BigInteger, primary_key=True)
    uid = db.Column(db.String(36), nullable=False, default=generate_uuid)
    bem = db.Column(db.Text, nullable=True)
    brazilian_type = db.Column(db.Text, nullable=True)
    brazilian_type_synonym = db.Column(db.Text, nullable=True)
    dna = db.Column(db.Text, nullable=True)

    # Identidade científica
    scientific_name = db.Column(db.Text, nullable=True)
    common_name = db.Column(db.Text, nullable=True)

    # Localidade tipo + distribuição
    type_country = db.Column(db.Text)

    # IDs externos
    mycobank_index_fungorum_id = db.Column(db.BigInteger)
    inaturalist_taxon_id = db.Column(db.BigInteger, unique=True)
    ncbi_taxonomy_id = db.Column(db.BigInteger, unique=True)
    unite_taxon_id = db.Column(db.BigInteger, unique=True)
    iucn_redlist = db.Column(db.Text)
    mushroom_observer_name_id = db.Column(db.BigInteger, nullable=True, unique=True)

    # Flags
    is_visible = db.Column(db.Boolean, nullable=False, server_default=db.false())
    is_outdated_mycobank = db.Column(db.Boolean, nullable=False, server_default=db.false())

    # Sync timestamps
    last_inaturalist_sync_at = db.Column(db.DateTime(timezone=True), nullable=True)

    # Timestamps
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

    # Relações
    photos = db.relationship(
        "SpeciesPhoto",
        back_populates="species",
        cascade="all, delete-orphan",
        passive_deletes=True,
    )
    taxonomy = db.relationship(
        "Taxon",
        back_populates="species",
        uselist=False,
        cascade="all, delete-orphan",
        passive_deletes=True,
    )
    characteristics = db.relationship(
        "SpeciesCharacteristics",
        back_populates="species",
        uselist=False,
        cascade="all, delete-orphan",
        passive_deletes=True,
    )
    similar_species_links = db.relationship(
        "SpeciesSimilarity",
        foreign_keys="SpeciesSimilarity.species_id",
        back_populates="species",
        cascade="all, delete-orphan",
        passive_deletes=True,
    )
    distributions = db.relationship(
        "Distribution",
        secondary="species_distributions",
        back_populates="species",
    )
    references = db.relationship(
        "Reference",
        secondary="species_references",
        back_populates="species",
    )
    observations = db.relationship(
        "Observation",
        back_populates="species",
        cascade="all, delete-orphan",
        passive_deletes=True,
    )

    def __repr__(self):
        return f"<Specie id={self.id} scientific_name={self.scientific_name!r}>"
