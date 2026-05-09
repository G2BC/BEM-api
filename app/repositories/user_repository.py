import re

from app.extensions import db
from app.models.user import User
from sqlalchemy import func, or_
from sqlalchemy.exc import IntegrityError

EXACT_EMAIL_RE = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")


class UserRepository:
    @staticmethod
    def _normalize_role(role: str | None) -> str:
        normalized = (role or User.ROLE_RESEARCHER).strip().lower()
        if normalized not in User.ROLES:
            raise ValueError("Role inválida")
        return normalized

    @classmethod
    def _build_users_query(
        cls,
        search: str | None = None,
        is_active: bool | None = None,
        exclude_user_id: int | None = None,
    ):
        query = User.query.order_by(User.id.asc())

        if search := (search or "").strip():
            if EXACT_EMAIL_RE.match(search):
                query = query.filter(User.email == search.lower())
            else:
                query = query.filter(
                    or_(
                        User.name.ilike(f"%{search}%"),
                        User.email.ilike(f"%{search}%"),
                    )
                )

        if is_active is not None:
            query = query.filter(User.is_active == is_active)
        if exclude_user_id is not None:
            query = query.filter(User.id != exclude_user_id)

        return query

    @classmethod
    def get_users(
        cls,
        search: str | None = None,
        is_active: bool | None = None,
        exclude_user_id: int | None = None,
    ):
        return cls._build_users_query(search, is_active, exclude_user_id).all()

    @classmethod
    def get_users_pagination(
        cls,
        page,
        per_page,
        search: str | None = None,
        is_active: bool | None = None,
        exclude_user_id: int | None = None,
    ):
        return cls._build_users_query(search, is_active, exclude_user_id).paginate(
            page=page, per_page=per_page, error_out=False
        )

    @classmethod
    def get_by_email(cls, email: str):
        return User.query.filter(func.lower(User.email) == email.strip().lower()).first()

    @classmethod
    def get_by_id(cls, id: str):
        try:
            parsed_id = int(id)
        except (TypeError, ValueError):
            return None

        return User.query.filter_by(id=parsed_id).first()

    @classmethod
    def create_user(
        cls,
        name: str,
        institution: str | None,
        email: str,
        password: str,
        role: str | None = None,
        is_active: bool = True,
    ) -> User:
        normalized_role = cls._normalize_role(role)
        user = User(
            name=name,
            institution=institution,
            email=email.strip().lower(),
            role=normalized_role,
            is_admin=(normalized_role == User.ROLE_ADMIN),
            is_active=is_active,
        )
        user.set_password(password)
        db.session.add(user)
        try:
            db.session.commit()
        except IntegrityError:
            db.session.rollback()
            raise ValueError("Email já cadastrado")
        return user

    @classmethod
    def activate_user(cls, user: User) -> User:
        user.is_active = True
        db.session.add(user)
        db.session.commit()
        return user

    @classmethod
    def deactivate_user(cls, user: User) -> User:
        user.is_active = False
        db.session.add(user)
        db.session.commit()
        return user

    @classmethod
    def update_password(cls, user: User, new_password: str, must_change_password: bool) -> User:
        user.set_password(new_password)
        user.must_change_password = must_change_password
        db.session.add(user)
        db.session.commit()
        return user

    @classmethod
    def update_role(cls, user: User, role: str) -> User:
        normalized_role = cls._normalize_role(role)
        user.role = normalized_role
        user.is_admin = normalized_role == User.ROLE_ADMIN
        db.session.add(user)
        db.session.commit()
        return user

    @classmethod
    def update_user(cls, user: User) -> User:
        db.session.add(user)
        db.session.commit()
        return user