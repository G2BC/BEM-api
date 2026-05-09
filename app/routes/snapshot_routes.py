from flask import current_app, request
from flask.views import MethodView
from flask_smorest import Blueprint

from app.schemas.species_schemas import SnapshotDownloadResponseSchema
from app.utils.bilingual import bilingual_response
from app.utils.object_storage import generate_presigned_get_url, list_objects

snapshot_bp = Blueprint(
    "snapshot",
    "snapshot",
    url_prefix="/snapshot",
)

_FORMATS = {"xlsx", "json"}
_LANGS = {"pt", "en"}
_DOWNLOAD_EXPIRES_SECONDS = 300


def _latest_version(bucket: str) -> int | None:
    objects = list_objects(bucket, prefix="v")
    versions = set()
    for obj in objects:
        key = obj.get("Key", "")
        part = key.split("/")[0]
        if part.startswith("v") and part[1:].isdigit():
            versions.add(int(part[1:]))
    return max(versions) if versions else None


@snapshot_bp.route("/download")
class SnapshotDownload(MethodView):
    @snapshot_bp.response(200, SnapshotDownloadResponseSchema)
    @snapshot_bp.alt_response(400, description="Parâmetros inválidos")
    @snapshot_bp.alt_response(404, description="Snapshot não encontrado")
    def get(self):
        version = request.args.get("version", type=int)
        lang = request.args.get("lang", type=str)
        fmt = request.args.get("format", type=str)

        if version is not None and version < 1:
            return bilingual_response(
                400,
                "`version` deve ser um inteiro positivo",
                "`version` must be a positive integer",
            )
        if lang not in _LANGS:
            return bilingual_response(
                400,
                f"`lang` deve ser um dos valores: {', '.join(sorted(_LANGS))}",
                f"`lang` must be one of: {', '.join(sorted(_LANGS))}",
            )
        if fmt not in _FORMATS:
            return bilingual_response(
                400,
                f"`format` deve ser um dos valores: {', '.join(sorted(_FORMATS))}",
                f"`format` must be one of: {', '.join(sorted(_FORMATS))}",
            )

        bucket = current_app.config.get("MINIO_DB_BUCKET", "bem-db")

        if version is None:
            version = _latest_version(bucket)
            if version is None:
                return bilingual_response(
                    404,
                    "Nenhum snapshot disponível no momento",
                    "No snapshots available at this time",
                )

        key = f"v{version}/{lang}/bem.{fmt}"

        try:
            url = generate_presigned_get_url(bucket, key, _DOWNLOAD_EXPIRES_SECONDS)
        except Exception:
            return bilingual_response(
                404,
                "Snapshot não encontrado para os parâmetros informados",
                "Snapshot not found for the given parameters",
            )

        return {
            "url": url,
            "expires_in_seconds": _DOWNLOAD_EXPIRES_SECONDS,
            "version": version,
            "lang": lang,
            "format": fmt,
        }
