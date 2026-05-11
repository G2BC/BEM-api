from datetime import datetime

from flask.views import MethodView
from flask_jwt_extended import get_jwt_identity
from flask_smorest import Blueprint

from app.exceptions import AppError
from app.schemas.login import AdminResetPasswordSchema
from app.schemas.user_schemas import (
    UserListQuerySchema,
    UserPaginationSchema,
    UserRoleUpdateSchema,
    UserSchema,
    UserUpdateSchema,
)
from app.services.user_service import UserService
from app.utils.bilingual import bilingual_response
from app.utils.permissions import require_admin
from app.utils.send_email import send_email

user_bp = Blueprint(
    "users",
    "users",
)


@user_bp.route("")
class UsersList(MethodView):
    @require_admin
    @user_bp.arguments(UserListQuerySchema, location="query")
    @user_bp.response(200, UserPaginationSchema)
    @user_bp.alt_response(400, description="Parâmetros inválidos")
    def get(self, query_params):
        identity = get_jwt_identity()
        try:
            return UserService.list_users(
                current_user_id=str(identity),
                page=query_params.get("page"),
                per_page=query_params.get("per_page"),
                search=query_params.get("search"),
                is_active=query_params.get("is_active"),
            )
        except AppError as exc:
            return bilingual_response(exc.status, exc.pt, exc.en)

@user_bp.route("/<string:user_id>/approve")
class ApproveUser(MethodView):
    @require_admin
    @user_bp.response(200, UserSchema)
    @user_bp.alt_response(403, description="Acesso permitido apenas para administradores")
    @user_bp.alt_response(404, description="Usuário não encontrado")
    def patch(self, user_id):
        try:
            user = UserService.approve_user(user_id)
            name = (user.name or "").strip()
            email = (user.email or "").strip()
            current_year = datetime.now().year

            logo_html = """
                    <tr>
                        <td align="center" style="padding: 28px 28px 0 28px;">
                            <img
                                src="https://bem.uneb.br/bem512_rounded.png"
                                alt="BEM"
                                width="140"
                                style="display:block;border:0;outline:none;text-decoration:none;max-width:140px;height:auto;"
                            >
                        </td>
                    </tr>
                """
            cta_html = """
                    <tr>
                        <td align="center" style="padding: 8px 28px 0 28px;">
                            <a
                                href="https://bem.uneb.br/login"
                                target="_blank"
                                style="
                                    display:inline-block;
                                    background-color:#00C000;
                                    color:#ffffff;
                                    text-decoration:none;
                                    font-family:'Inter', Arial, sans-serif;
                                    font-size:15px;
                                    line-height:20px;
                                    font-weight:600;
                                    border-radius:8px;
                                    padding:12px 18px;
                                "
                            >Access platform</a>
                        </td>
                    </tr>
                """
            send_email(
                subject="[BEM] Registration Approved",
                content=f"""
                    <!DOCTYPE html>
                    <html lang="en">
                        <head>
                            <meta content="text/html; charset=UTF-8" http-equiv="Content-Type" />
                            <meta name="x-apple-disable-message-reformatting" />
                            <meta name="viewport" content="width=device-width, initial-scale=1.0" />
                            <title>Registration approved</title>
                        </head>
                        <body style="margin:0;padding:0;background-color:#f6f8fb;">
                            <table role="presentation" width="100%" border="0" cellpadding="0" cellspacing="0" style="background-color:#f6f8fb;">
                                <tbody>
                                    <tr>
                                        <td align="center" style="padding:24px 12px;">
                                            <table role="presentation" width="100%" border="0" cellpadding="0" cellspacing="0" style="max-width:600px;background-color:#ffffff;border:1px solid #e5e7eb;border-radius:12px;">
                                                <tbody>
                                                    {logo_html}
                                                    <tr>
                                                        <td style="padding:28px 28px 12px 28px;font-family:'Inter', -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Oxygen, Ubuntu, Cantarell, 'Open Sans', 'Helvetica Neue', sans-serif;color:#111827;">
                                                            <h1 style="margin:0 0 12px 0;font-size:24px;line-height:1.2;font-weight:700;">Registration approved</h1>
                                                            <p style="margin:0 0 12px 0;font-size:16px;line-height:1.6;">Hi, <strong>{name}</strong>.</p>
                                                            <p style="margin:0;font-size:16px;line-height:1.6;">Your registration on the BEM platform has been approved.</p>
                                                        </td>
                                                    </tr>
                                                    {cta_html}
                                                    <tr>
                                                        <td style="padding:20px 28px 28px 28px;font-family:'Inter', -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Oxygen, Ubuntu, Cantarell, 'Open Sans', 'Helvetica Neue', sans-serif;color:#374151;">
                                                            <hr style="border:none;border-top:1px solid #e5e7eb;margin:0 0 16px 0;">
                                                            <p style="margin:0;font-size:14px;line-height:1.5;text-align:center;">&copy; {current_year} BEM</p>
                                                        </td>
                                                    </tr>
                                                </tbody>
                                            </table>
                                        </td>
                                    </tr>
                                </tbody>
                            </table>
                        </body>
                    </html>
                """,  # noqa: E501
                to=email,
            )
            return user
        except AppError as exc:
            return bilingual_response(exc.status, exc.pt, exc.en)
        except Exception:
            return bilingual_response(
                500,
                "Usuário aprovado, mas falha ao enviar email",
                "User approved, but failed to send email",
            )


@user_bp.route("/<string:user_id>/deactivate")
class DeactivateUser(MethodView):
    @require_admin
    @user_bp.response(200, UserSchema)
    @user_bp.alt_response(403, description="Acesso permitido apenas para administradores")
    @user_bp.alt_response(404, description="Usuário não encontrado")
    def patch(self, user_id):
        try:
            return UserService.deactivate_user(user_id)
        except AppError as exc:
            return bilingual_response(exc.status, exc.pt, exc.en)


@user_bp.route("/<string:user_id>/reset-password")
class ResetUserPassword(MethodView):
    @require_admin
    @user_bp.response(200, AdminResetPasswordSchema)
    @user_bp.alt_response(403, description="Acesso permitido apenas para administradores")
    @user_bp.alt_response(404, description="Usuário não encontrado")
    def post(self, user_id):
        try:
            return UserService.admin_reset_password(user_id)
        except AppError as exc:
            return bilingual_response(exc.status, exc.pt, exc.en)


@user_bp.route("/<string:user_id>/role")
class UpdateUserRole(MethodView):
    @require_admin
    @user_bp.arguments(UserRoleUpdateSchema)
    @user_bp.response(200, UserSchema)
    @user_bp.alt_response(400, description="Erro de validação/regra de negócio")
    @user_bp.alt_response(403, description="Acesso permitido apenas para administradores")
    @user_bp.alt_response(404, description="Usuário não encontrado")
    def patch(self, payload, user_id):
        identity = get_jwt_identity()
        try:
            return UserService.update_role(
                actor_id=str(identity),
                target_user_id=user_id,
                role=payload["role"],
            )
        except AppError as exc:
            return bilingual_response(exc.status, exc.pt, exc.en)


@user_bp.route("/me")
class UpdateUserProfile(MethodView):
    @user_bp.arguments(UserUpdateSchema)
    @user_bp.response(200, UserSchema)
    def patch(self, data):
        try:
            user_id = get_jwt_identity()
            return UserService.update_profile(user_id, data)
        except AppError as exc:
            return bilingual_response(exc.status, exc.pt, exc.en)
