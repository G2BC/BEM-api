from sqlalchemy.sql import func

from app.extensions import db


class Taxon(db.Model):
    __tablename__ = "taxonomies"
    __table_args__ = (db.UniqueConstraint("species_id", name="uq_taxonomies_species_id"),)

    id = db.Column(db.BigInteger, primary_key=True)
    species_id = db.Column(
        db.BigInteger,
        db.ForeignKey("species.id", ondelete="CASCADE"),
        nullable=False,
    )
    classification_raw = db.Column(db.Text, nullable=True)
    kingdom = db.Column(db.Text, nullable=True)
    phylum = db.Column(db.Text, nullable=True)
    class_name = db.Column(db.Text, nullable=True)
    order = db.Column(db.Text, nullable=True)
    family = db.Column(db.Text, nullable=True)
    genus = db.Column(db.Text, nullable=True)
    section = db.Column(db.Text, nullable=True)
    group = db.Column(db.Text, nullable=True)
    specific_epithet = db.Column(db.Text, nullable=True)
    synonyms = db.Column(db.Text, nullable=True)
    basionym = db.Column(db.Text, nullable=True)
    name_type = db.Column(db.Text, nullable=True)
    gender = db.Column(db.Text, nullable=True)
    years_of_effective_publication = db.Column(db.Text, nullable=True)
    authors = db.Column(db.Text, nullable=True)

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

    species = db.relationship("Species", back_populates="taxonomy")

    def __repr__(self):
        return f"<Taxon species_id={self.species_id}>"
