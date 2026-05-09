from app.extensions import db


class SpeciesSimilarity(db.Model):
    __tablename__ = "species_similarities"
    __table_args__ = (
        db.CheckConstraint(
            "species_id <> similar_species_id", name="ck_species_similarity_not_self"
        ),
        db.Index("idx_species_similarities_similar_species", "similar_species_id"),
    )

    species_id = db.Column(
        db.BigInteger,
        db.ForeignKey("species.id", ondelete="CASCADE"),
        primary_key=True,
        nullable=False,
    )
    similar_species_id = db.Column(
        db.BigInteger,
        db.ForeignKey("species.id", ondelete="CASCADE"),
        primary_key=True,
        nullable=False,
    )

    species = db.relationship(
        "Species",
        foreign_keys=[species_id],
        back_populates="similar_species_links",
    )
    similar_species = db.relationship("Species", foreign_keys=[similar_species_id])

    def __repr__(self):
        return (
            f"<SpeciesSimilarity species_id={self.species_id} "
            f"similar_species_id={self.similar_species_id}>"
        )
