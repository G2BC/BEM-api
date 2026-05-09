from functools import wraps

from app.models.user import User
from app.utils.bilingual import bilingual_response
from flask_jwt_extended import get_jwt, verify_jwt_in_request


def require_admin(fn):
    @wraps(fn)
    def wrapper(*args, **kwargs):
        verify_jwt_in_request()
        if get_jwt().get("role") != User.ROLE_ADMIN:
            return bilingual_response(
                403,
                "Acesso permitido apenas para administradores",
                "Access allowed for administrators only",
            )
        return fn(*args, **kwargs)

    return wrapper


def require_curator_or_admin(fn):
    @wraps(fn)
    def wrapper(*args, **kwargs):
        verify_jwt_in_request()
        if get_jwt().get("role") not in {User.ROLE_CURATOR, User.ROLE_ADMIN}:
            return bilingual_response(
                403,
                "Acesso permitido apenas para curadores ou administradores",
                "Access allowed for curators or administrators only",
            )
        return fn(*args, **kwargs)

    return wrapper
