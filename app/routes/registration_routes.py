from flask.views import MethodView
from flask_smorest import Blueprint

from app.exceptions import AppError
from app.schemas.login import TokenSchema
from app.schemas.user_schemas import UserCreateSchema
from app.services.auth import AuthService
from app.services.user_service import UserService
from app.utils.bilingual import bilingual_response

registration_bp = Blueprint(
    "registrations",
    "registrations",
)


@registration_bp.route("")
class Registrations(MethodView):
    @registration_bp.arguments(UserCreateSchema)
    @registration_bp.response(201, TokenSchema)
    @registration_bp.alt_response(400, description="Erro de validação/regra de negócio")
    def post(self, payload):
        try:
            user = UserService.create_user(payload)
            return AuthService.create_tokens_for(user)
        except AppError as exc:
            return bilingual_response(exc.status, exc.pt, exc.en)
