import os
from datetime import timedelta

from dotenv_vault import load_dotenv

load_dotenv()


class Config:
    API_TITLE = "BEM"
    API_VERSION = "v1"
    OPENAPI_VERSION = "3.0.3"
    OPENAPI_URL_PREFIX = ""
    OPENAPI_SWAGGER_UI_PATH = "/docs"
    OPENAPI_SWAGGER_UI_URL = "https://cdn.jsdelivr.net/npm/swagger-ui-dist/"
    SQLALCHEMY_DATABASE_URI = os.getenv(
        "DATABASE_URL", "postgresql://postgres:postgres@db:5432/app"
    )
    SQLALCHEMY_TRACK_MODIFICATIONS = False
    SECRET_KEY = os.getenv("SECRET_KEY", "changeme")
    JWT_SECRET_KEY = os.getenv("JWT_SECRET_KEY", SECRET_KEY)
    JWT_ACCESS_TOKEN_EXPIRES = timedelta(
        seconds=int(os.getenv("JWT_ACCESS_TOKEN_EXPIRES", "14400"))
    )
    JWT_REFRESH_TOKEN_EXPIRES = timedelta(
        seconds=int(os.getenv("JWT_REFRESH_TOKEN_EXPIRES", "2592000"))
    )
    API_KEY = os.getenv("API_KEY", "")
    APP_ENV = os.getenv("APP_ENV", "development").strip().lower()
    _default_cors_origins = (
        "http://localhost:3000,http://localhost:5173,http://localhost:3333,http://127.0.0.1:3000,http://127.0.0.1:5173,http://127.0.0.1:3333"
        if APP_ENV == "development"
        else "https://bem.uneb.br"
    )
    CORS_ALLOWED_ORIGINS = [
        origin.strip()
        for origin in os.getenv("CORS_ALLOWED_ORIGINS", _default_cors_origins).split(",")
        if origin.strip()
    ]
    CORS_ALLOW_HEADERS = [
        header.strip()
        for header in "Content-Type,Authorization,X-API-Key".split(",")
        if header.strip()
    ]
    CORS_METHODS = [
        method.strip().upper()
        for method in "GET,POST,PUT,PATCH,DELETE,OPTIONS".split(",")
        if method.strip()
    ]
    MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "").strip()
    MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "").strip()
    MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "").strip()
    MINIO_SECURE = os.getenv("MINIO_SECURE", "false").strip().lower() in {"1", "true", "yes"}
    MINIO_REGION = os.getenv("MINIO_REGION", "us-east-1").strip()
    MINIO_PUBLIC_BASE_URL = os.getenv("MINIO_PUBLIC_BASE_URL", "").strip()
    MINIO_TMP_BUCKET = os.getenv("MINIO_TMP_BUCKET", "bem-web-tmp").strip()
    MINIO_FINAL_BUCKET = os.getenv("MINIO_FINAL_BUCKET", "bem-web").strip()
    MINIO_DB_BUCKET = os.getenv("MINIO_DB_BUCKET", "bem-db").strip()
    SPECIES_PHOTO_MAX_BYTES = int(os.getenv("SPECIES_PHOTO_MAX_BYTES", str(5 * 1024 * 1024)))
    SPECIES_PHOTO_ALLOWED_MIME_TYPES = [
        mime.strip().lower()
        for mime in os.getenv(
            "SPECIES_PHOTO_ALLOWED_MIME_TYPES", "image/jpeg,image/png,image/webp,image/gif"
        ).split(",")
        if mime.strip()
    ]
    SPECIES_REQUEST_MAX_PHOTOS = int(os.getenv("SPECIES_REQUEST_MAX_PHOTOS", "5"))
    SPECIES_TMP_RETENTION_DAYS = int(os.getenv("SPECIES_TMP_RETENTION_DAYS", "30"))
    SPECIES_REQUEST_PREVIEW_URL_EXPIRES_SECONDS = int(
        os.getenv("SPECIES_REQUEST_PREVIEW_URL_EXPIRES_SECONDS", "900")
    )
    DEEPL_API_KEY = os.getenv("DEEPL_API_KEY", "").strip()
    DEEPL_API_URL = os.getenv("DEEPL_API_URL", "https://api-free.deepl.com/v2/translate").strip()
    DEEPL_TIMEOUT_SECONDS = float(os.getenv("DEEPL_TIMEOUT_SECONDS", "45"))
    REDIS_URL = os.getenv("REDIS_URL", "").strip()
    REDIS_SOCKET_TIMEOUT_SECONDS = float(os.getenv("REDIS_SOCKET_TIMEOUT_SECONDS", "1.5"))
    REDIS_DEFAULT_TTL_SECONDS = int(os.getenv("REDIS_DEFAULT_TTL_SECONDS", "300"))
    OBSERVATIONS_CACHE_PREFIX = "observations"
    OBSERVATIONS_CACHE_TTL_SECONDS = int(os.getenv("OBSERVATIONS_CACHE_TTL_SECONDS", "86400"))
    NCBI_EMAIL = os.getenv("NCBI_EMAIL", "").strip()
    NCBI_API_KEY = os.getenv("NCBI_API_KEY", "").strip()
    NCBI_CACHE_PREFIX = "ncbi:species"
    NCBI_CACHE_TTL_SECONDS = int(os.getenv("NCBI_CACHE_TTL_SECONDS", "86400"))
    NCBI_MAX_TRIES = int(os.getenv("NCBI_MAX_TRIES", "1"))
    NCBI_SLEEP_BETWEEN_TRIES_SECONDS = float(os.getenv("NCBI_SLEEP_BETWEEN_TRIES_SECONDS", "0.25"))
    NCBI_REQUEST_TIMEOUT_SECONDS = float(os.getenv("NCBI_REQUEST_TIMEOUT_SECONDS", "8"))
