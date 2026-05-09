import hmac
from functools import wraps

from app.utils.bilingual import bilingual_response
from flask import current_app, request


def _is_valid_api_key(supplied: str | None, expected: str | None) -> bool:
    return bool(supplied and expected and hmac.compare_digest(supplied, expected))


def enforce_api_key():
    """Return a bilingual 401 response if the API key is missing/invalid, else None."""
    supplied = request.headers.get("X-API-Key")
    expected = current_app.config.get("API_KEY")
    if not _is_valid_api_key(supplied, expected):
        return bilingual_response(401, "API key inválida ou ausente", "Invalid or missing API key")
    return None


def require_api_key(fn):
    @wraps(fn)
    def wrapper(*args, **kwargs):
        result = enforce_api_key()
        if result is not None:
            return result
        return fn(*args, **kwargs)

    return wrapper
