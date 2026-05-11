from flask import Flask, request

API_CORS_ORIGINS = ["*"]


def init_cors(app: Flask) -> None:
    @app.after_request
    def add_cors_headers(response):
        allowed_origin = _allowed_origin(app)
        if allowed_origin is None:
            return response

        response.headers["Access-Control-Allow-Origin"] = allowed_origin
        response.headers["Vary"] = "Origin"

        if request.method == "OPTIONS":
            response.headers["Access-Control-Allow-Methods"] = ", ".join(
                app.config["CORS_METHODS"]
            )
            response.headers["Access-Control-Allow-Headers"] = ", ".join(
                app.config["CORS_ALLOW_HEADERS"]
            )
            response.headers["Access-Control-Max-Age"] = "86400"

        return response


def _allowed_origin(app: Flask) -> str | None:
    origin = request.headers.get("Origin")
    if not origin:
        return None

    allowed_origins = API_CORS_ORIGINS if _is_api_cors_request() else app.config[
        "CORS_ALLOWED_ORIGINS"
    ]
    if "*" in allowed_origins:
        return "*"
    if origin in allowed_origins:
        return origin
    return None


def _is_api_cors_request() -> bool:
    path = request.path
    method = request.headers.get("Access-Control-Request-Method", request.method).upper()

    if _path_matches(path, "/admin"):
        return False

    public_methods = {
        "/snapshots": {"GET"},
        "/species": {"GET"},
        "/species-change-requests": {"POST"},
    }
    return any(
        _path_matches(path, prefix) and method in methods
        for prefix, methods in public_methods.items()
    )


def _path_matches(path: str, prefix: str) -> bool:
    return path == prefix or path.startswith(f"{prefix}/")
