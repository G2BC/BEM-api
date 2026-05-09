from datetime import datetime

from werkzeug.security import check_password_hash, generate_password_hash

from app.extensions import db


class User(db.Model):
    __tablename__ = "users"
    ROLE_RESEARCHER = "researcher"
    ROLE_CURATOR = "curator"
    ROLE_ADMIN = "admin"
    ROLES = (ROLE_RESEARCHER, ROLE_CURATOR, ROLE_ADMIN)
    __table_args__ = (
        db.CheckConstraint(
            "role IN ('researcher', 'curator', 'admin')",
            name="ck_users_role_valid",
        ),
        db.CheckConstraint(
            "((role = 'admin') = is_admin)",
            name="ck_users_role_is_admin_consistent",
        ),
    )

    id = db.Column(db.BigInteger, primary_key=True)
    name = db.Column(db.String(100), nullable=False)
    institution = db.Column(db.String(150), nullable=True)
    email = db.Column(db.String(150), nullable=False, unique=True)
    password = db.Column(db.String(255), nullable=False)
    is_admin = db.Column(db.Boolean, default=False, nullable=False)
    role = db.Column(db.String(20), nullable=False, default=ROLE_RESEARCHER)
    is_active = db.Column(db.Boolean, default=False, nullable=False)
    must_change_password = db.Column(db.Boolean, default=False, nullable=False)
    created_at = db.Column(db.DateTime, default=datetime.now)

    def set_password(self, password):
        self.password = generate_password_hash(password)

    def check_password(self, password):
        return check_password_hash(self.password, password)

    @property
    def is_curator(self):
        return self.role in (self.ROLE_CURATOR, self.ROLE_ADMIN)

    def to_dict(self):
        return {
            "id": self.id,
            "name": self.name,
            "email": self.email,
            "institution": self.institution,
            "is_admin": self.is_admin,
            "role": self.role,
            "is_curator": self.is_curator,
            "is_active": self.is_active,
            "must_change_password": self.must_change_password,
            "created_at": self.created_at.isoformat(),
        }
