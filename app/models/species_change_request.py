from sqlalchemy.sql import func

from app.extensions import db


class SpeciesChangeRequest(db.Model):
    __tablename__ = "species_change_requests"
    STATUS_PENDING = "pending"
    STATUS_APPROVED = "approved"
    STATUS_PARTIAL_APPROVED = "partial_approved"
    STATUS_REJECTED = "rejected"
    STATUSES = (STATUS_PENDING, STATUS_APPROVED, STATUS_PARTIAL_APPROVED, STATUS_REJECTED)

    __table_args__ = (
        db.CheckConstraint(
            "status IN ('pending', 'approved', 'partial_approved', 'rejected')",
            name="ck_species_change_requests_status_valid",
        ),
        db.Index("idx_species_change_requests_species", "species_id"),
        db.Index("idx_species_change_requests_status", "status"),
    )

    id = db.Column(db.BigInteger, primary_key=True)
    species_id = db.Column(
        db.BigInteger,
        db.ForeignKey("species.id", ondelete="CASCADE"),
        nullable=False,
    )
    requested_by_user_id = db.Column(
        db.BigInteger,
        db.ForeignKey("users.id", ondelete="SET NULL"),
        nullable=True,
    )
    requester_name = db.Column(db.String(120), nullable=True)
    requester_email = db.Column(db.String(150), nullable=True)
    requester_institution = db.Column(db.String(150), nullable=True)
    request_note = db.Column(db.Text, nullable=True)
    proposed_data = db.Column(db.JSON, nullable=False, server_default=db.text("'{}'::json"))

    status = db.Column(
        db.String(20),
        nullable=False,
        default=STATUS_PENDING,
        server_default=STATUS_PENDING,
    )
    review_note = db.Column(db.Text, nullable=True)
    reviewed_by_user_id = db.Column(
        db.BigInteger,
        db.ForeignKey("users.id", ondelete="SET NULL"),
        nullable=True,
    )
    reviewed_at = db.Column(db.DateTime(timezone=True), nullable=True)

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

    species = db.relationship("Species")
    requested_by_user = db.relationship("User", foreign_keys=[requested_by_user_id])
    reviewed_by_user = db.relationship("User", foreign_keys=[reviewed_by_user_id])
    photos = db.relationship(
        "SpeciesPhotoRequest",
        back_populates="request",
        cascade="all, delete-orphan",
        passive_deletes=True,
    )


class SpeciesPhotoRequest(db.Model):
    __tablename__ = "species_photo_requests"
    __table_args__ = (
        db.CheckConstraint(
            "status IN ('pending', 'approved', 'rejected')",
            name="ck_species_photo_requests_status_valid",
        ),
        db.Index("idx_species_photo_requests_request", "request_id"),
    )

    id = db.Column(db.BigInteger, primary_key=True)
    request_id = db.Column(
        db.BigInteger,
        db.ForeignKey("species_change_requests.id", ondelete="CASCADE"),
        nullable=False,
    )
    object_key = db.Column(db.Text, nullable=False)
    bucket_name = db.Column(db.String(100), nullable=True)
    original_filename = db.Column(db.String(255), nullable=True)
    mime_type = db.Column(db.String(100), nullable=True)
    size_bytes = db.Column(db.BigInteger, nullable=True)
    checksum_sha256 = db.Column(db.String(64), nullable=True)
    caption = db.Column(db.Text, nullable=True)
    license_code = db.Column(db.Text, nullable=True)
    attribution = db.Column(db.Text, nullable=True)
    rights_holder = db.Column(db.Text, nullable=True)
    source_url = db.Column(db.Text, nullable=True)
    declaration_accepted_at = db.Column(db.DateTime(timezone=True), nullable=True)
    status = db.Column(
        db.String(20),
        nullable=False,
        default=SpeciesChangeRequest.STATUS_PENDING,
        server_default=SpeciesChangeRequest.STATUS_PENDING,
    )

    created_at = db.Column(
        db.DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
    )

    request = db.relationship("SpeciesChangeRequest", back_populates="photos")
