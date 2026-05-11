import os

import sentry_sdk
from dotenv_vault import load_dotenv
from flask import Flask, request
from flask_jwt_extended import get_jwt, verify_jwt_in_request
from flask_migrate import Migrate
from flask_smorest import Api

from .config.cors import init_cors
from .extensions import db, jwt
from .utils.require_api_key import enforce_api_key

migrate = Migrate()
load_dotenv(override=False)


def create_app():
    sentry_sdk.init(
        dsn=os.getenv("SENTRY_DSN"),
        send_default_pii=True,
    )

    app = Flask(__name__)
    app.config.from_object("app.config.Config")
    init_cors(app)

    db.init_app(app)
    jwt.init_app(app)
    migrate.init_app(app, db)

    api = Api(app)

    @jwt.unauthorized_loader
    def handle_missing_token(_reason):
        return {
            "code": 401,
            "status": "Unauthorized",
            "message_pt": "Token de acesso ausente.",
            "message_en": "Missing access token.",
        }, 401

    @jwt.invalid_token_loader
    def handle_invalid_token(_reason):
        return {
            "code": 401,
            "status": "Unauthorized",
            "message_pt": "Token inválido.",
            "message_en": "Invalid token.",
        }, 401

    @jwt.expired_token_loader
    def handle_expired_token(_jwt_header, _jwt_payload):
        return {
            "code": 401,
            "status": "Unauthorized",
            "message_pt": "Token expirado.",
            "message_en": "Token expired.",
        }, 401

    @app.before_request
    def require_api_key_for_api_routes():
        if request.method == "OPTIONS":
            return None

        path = request.path
        protected_prefixes = (
            "/admin",
            "/auth",
            "/registrations",
            "/species",
            "/species-change-requests",
            "/contact-messages",
            "/docs",
            "/openapi.json",
        )

        if any(path == prefix or path.startswith(f"{prefix}/") for prefix in protected_prefixes):
            result = enforce_api_key()
            if result is not None:
                return result

        return None

    @app.before_request
    def require_password_change_for_protected_routes():
        if request.method == "OPTIONS":
            return None

        path = request.path
        allowed_paths = {
            "/",
            "/health",
            "/auth/login",
            "/auth/refresh",
            "/auth/change-password",
        }
        if path in allowed_paths or path.startswith("/docs") or path.startswith("/openapi"):
            return None

        verify_jwt_in_request(optional=True)
        claims = get_jwt()

        if claims and claims.get("must_change_password", False):
            return {
                "code": 403,
                "status": "Forbidden",
                "must_change_password": True,
                "message_pt": "Troca de senha obrigatória antes de continuar.",
                "message_en": "Password change required before continuing.",
            }, 403

        return None

    @app.route("/", methods=["GET"])
    def index():
        return {"name": "BEM API"}, 200

    @app.route("/health", methods=["GET"])
    def health():
        return {"status": "ok"}, 200

    with app.app_context():
        from app import models  # noqa: F401
        from app.routes import register_blueprints

        register_blueprints(api)

    return app
