from app.extensions import db


class Distribution(db.Model):
    __tablename__ = "distributions"

    id = db.Column(db.Integer, primary_key=True)
    slug = db.Column(db.Text, nullable=False, unique=True)
    label_en = db.Column(db.Text, nullable=False)
    label_pt = db.Column(db.Text, nullable=False)

    species = db.relationship(
        "Species",
        secondary="species_distributions",
        back_populates="distributions",
    )

    def __repr__(self):
        return f"<Distribution id={self.id} slug={self.slug!r}>"
