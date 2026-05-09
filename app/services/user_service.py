import secrets
import string

from app.exceptions import AppError
from app.models.user import User
from app.repositories.user_repository import UserRepository
from app.utils.pagination import build_page_response, resolve_page_params


class UserService:
    DEFAULT_PER_PAGE = 20
    MAX_PER_PAGE = 100

    @staticmethod
    def _parse_positive_user_id(value) -> int:
        try:
            parsed = int(value)
        except (TypeError, ValueError):
            raise AppError(pt="`current_user_id` inválido", en="Invalid `current_user_id`")
        if parsed < 1:
            raise AppError(pt="`current_user_id` inválido", en="Invalid `current_user_id`")
        return parsed

    @staticmethod
    def _generate_temporary_password(length: int = 12) -> str:
        alphabet = string.ascii_letters + string.digits
        while True:
            password = "".join(secrets.choice(alphabet) for _ in range(length))
            if (
                any(char.islower() for char in password)
                and any(char.isupper() for char in password)
                and any(char.isdigit() for char in password)
            ):
                return password

    @classmethod
    def list_users(
        cls,
        current_user_id=None,
        page=None,
        per_page=None,
        search=None,
        is_active=None,
    ):
        exclude_user_id = cls._parse_positive_user_id(current_user_id)
        page, per_page = resolve_page_params(
            page, per_page, default_per_page=cls.DEFAULT_PER_PAGE, max_per_page=cls.MAX_PER_PAGE
        )

        if page is None:
            result = UserRepository.get_users(search, is_active, exclude_user_id=exclude_user_id)
        else:
            result = UserRepository.get_users_pagination(
                page, per_page, search, is_active, exclude_user_id=exclude_user_id
            )

        return build_page_response(result, page, per_page)

    @staticmethod
    def create_user(data):
        email = data["email"].strip().lower()
        name = data["name"].strip()
        institution = data.get("institution")
        normalized_institution = institution.strip() if institution else None

        if UserRepository.get_by_email(email):
            raise AppError(pt="Email já cadastrado", en="Email already registered")

        return UserRepository.create_user(
            name=name,
            institution=normalized_institution,
            email=email,
            password=data["password"],
            is_active=True,
        )

    @staticmethod
    def get_user_by_id(id: str):
        user = UserRepository.get_by_id(id)

        if not user:
            raise AppError(pt="Usuário não encontrado.", en="User not found.", status=404)

        return user

    @staticmethod
    def approve_user(id: str):
        user = UserRepository.get_by_id(id)

        if not user:
            raise AppError(pt="Usuário não encontrado.", en="User not found.", status=404)

        if user.is_active:
            return user

        return UserRepository.activate_user(user)

    @staticmethod
    def deactivate_user(id: str):
        user = UserRepository.get_by_id(id)

        if not user:
            raise AppError(pt="Usuário não encontrado.", en="User not found.", status=404)

        if not user.is_active:
            return user

        return UserRepository.deactivate_user(user)

    @classmethod
    def admin_reset_password(cls, id: str):
        user = UserRepository.get_by_id(id)

        if not user:
            raise AppError(pt="Usuário não encontrado.", en="User not found.", status=404)

        temporary_password = cls._generate_temporary_password()
        UserRepository.update_password(
            user=user,
            new_password=temporary_password,
            must_change_password=True,
        )

        return {
            "user_id": str(user.id),
            "temporary_password": temporary_password,
            "must_change_password": True,
        }

    @staticmethod
    def update_role(actor_id: str, target_user_id: str, role: str):
        actor = UserRepository.get_by_id(actor_id)
        if not actor:
            raise AppError(
                pt="Usuário autenticado não encontrado.",
                en="Authenticated user not found.",
                status=404,
            )

        target_user = UserRepository.get_by_id(target_user_id)
        if not target_user:
            raise AppError(pt="Usuário não encontrado.", en="User not found.", status=404)

        normalized_role = (role or "").strip().lower()
        if normalized_role not in User.ROLES:
            raise AppError(pt="Role inválida", en="Invalid role")

        if actor.id == target_user.id and normalized_role != User.ROLE_ADMIN:
            raise AppError(
                pt="Você não pode revogar o próprio perfil de administrador",
                en="You cannot revoke your own administrator role",
            )

        if target_user.role == normalized_role:
            return target_user

        return UserRepository.update_role(target_user, normalized_role)

    @staticmethod
    def update_profile(user_id, data):
        user = UserRepository.get_by_id(user_id)
        if not user:
            raise AppError(pt="Usuário não encontrado.", en="User not found.", status=404)

        new_email = data.get("email", "").strip().lower()
        if new_email and new_email != user.email:
            if UserRepository.get_by_email(new_email):
                raise AppError(pt="Email já cadastrado", en="Email already registered")
            user.email = new_email

        if "name" in data:
            user.name = data["name"].strip()
        if "institution" in data:
            user.institution = data["institution"].strip() if data["institution"] else None

        if "new_password" in data and data["new_password"]:
            current_pw = data.get("current_password")
            if not current_pw or not user.check_password(current_pw):
                raise AppError(
                    pt="Senha atual incorreta", en="Incorrect current password", status=401
                )

            UserRepository.update_password(user, data["new_password"], False)

        return UserRepository.update_user(user)
