"""
Bilingual error responses — PT + EN in every API error payload.

Usage in routes:
    return bilingual_response(exc.status, exc.pt, exc.en)   # AppError
    return bilingual_response(400, "Mensagem PT", "PT message EN")
"""

from flask import jsonify

_STATUS_NAMES: dict[int, str] = {
    400: "Bad Request",
    401: "Unauthorized",
    403: "Forbidden",
    404: "Not Found",
    409: "Conflict",
    422: "Unprocessable Entity",
    500: "Internal Server Error",
    502: "Bad Gateway",
}


def bilingual_response(status: int, pt: str, en: str | None = None):
    """Build a Flask JSON response with bilingual error fields.

    Args:
        status: HTTP status code.
        pt:     Portuguese error message.
        en:     English message. Falls back to ``pt`` when omitted.

    Returns:
        A ``(Response, int)`` tuple suitable for returning from a Flask view.
    """
    return (
        jsonify(
            {
                "code": status,
                "status": _STATUS_NAMES.get(status, "Error"),
                "message_pt": pt,
                "message_en": en if en is not None else pt,
            }
        ),
        status,
    )
