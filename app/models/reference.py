from app.extensions import db


class Reference(db.Model):
    __tablename__ = "references"
    __table_args__ = (
        db.CheckConstraint(
            "doi IS NOT NULL OR url IS NOT NULL OR apa IS NOT NULL",
            name="ck_references_at_least_one_identifier",
        ),
    )

    id = db.Column(db.Integer, primary_key=True)
    doi = db.Column(db.Text, nullable=True)
    url = db.Column(db.Text, nullable=True)
    apa = db.Column(db.Text, nullable=True)

    species = db.relationship(
        "Species",
        secondary="species_references",
        back_populates="references",
    )

    def __repr__(self):
        return f"<Reference id={self.id} doi={self.doi!r}>"
