from app.exceptions import AppError, AppPermissionError
from app.repositories.user_repository import UserRepository
from flask_jwt_extended import create_access_token, create_refresh_token, get_jwt_identity


class AuthService:
    @staticmethod
    def _build_claims(user):
        return {
            "is_admin": user.is_admin,
            "role": user.role,
            "is_curator": user.is_curator,
            "email": user.email,
            "must_change_password": user.must_change_password,
        }

    @classmethod
    def create_tokens_for(cls, user):
        identity = str(user.id)
        additional_claims = cls._build_claims(user)
        return {
            "access_token": create_access_token(
                identity=identity, additional_claims=additional_claims
            ),
            "refresh_token": create_refresh_token(
                identity=identity, additional_claims=additional_claims
            ),
            "must_change_password": user.must_change_password,
        }

    @classmethod
    def login(cls, email: str, password: str):
        user = UserRepository.get_by_email(email)

        if not user or not user.check_password(password):
            raise AppError(pt="Credenciais inválidas", en="Invalid credentials", status=401)
        if not user.is_active:
            raise AppPermissionError(
                pt="Conta inativa. Aguarde aprovação do administrador",
                en="Inactive account. Awaiting administrator approval.",
            )

        identity = str(user.id)
        additional_claims = cls._build_claims(user)

        return {
            "access_token": create_access_token(
                identity=identity, additional_claims=additional_claims
            ),
            "refresh_token": create_refresh_token(
                identity=identity, additional_claims=additional_claims
            ),
            "must_change_password": user.must_change_password,
        }

    @classmethod
    def refresh(cls):
        identity = get_jwt_identity()
        user = UserRepository.get_by_id(identity)

        if not user:
            raise AppError(pt="Usuário não encontrado.", en="User not found.", status=404)
        if not user.is_active:
            raise AppPermissionError(
                pt="Conta inativa. Aguarde aprovação do administrador",
                en="Inactive account. Awaiting administrator approval.",
            )

        return {
            "access_token": create_access_token(
                identity=str(user.id),
                additional_claims=cls._build_claims(user),
            )
        }

    @staticmethod
    def get_current_user():
        identity = get_jwt_identity()
        user = UserRepository.get_by_id(identity)

        if not user:
            raise AppError(pt="Usuário não encontrado.", en="User not found.", status=404)

        return user

    @classmethod
    def change_password(cls, current_password: str, new_password: str):
        identity = get_jwt_identity()
        user = UserRepository.get_by_id(identity)

        if not user:
            raise AppError(pt="Usuário não encontrado.", en="User not found.", status=404)
        if not user.is_active:
            raise AppPermissionError(
                pt="Conta inativa. Aguarde aprovação do administrador",
                en="Inactive account. Awaiting administrator approval.",
            )
        if not user.check_password(current_password):
            raise AppError(pt="Senha atual inválida", en="Current password is invalid")

        UserRepository.update_password(
            user=user,
            new_password=new_password,
            must_change_password=False,
        )

        additional_claims = cls._build_claims(user)
        additional_claims["must_change_password"] = False
        return {
            "access_token": create_access_token(
                identity=str(user.id), additional_claims=additional_claims
            ),
            "refresh_token": create_refresh_token(
                identity=str(user.id), additional_claims=additional_claims
            ),
            "must_change_password": False,
        }
