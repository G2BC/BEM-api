import mimetypes
import os
import re
import time
from datetime import timedelta
from typing import Any
from uuid import uuid4

import app.utils.object_storage as object_storage
from app.exceptions import AppError
from botocore.exceptions import BotoCoreError, ClientError
from flask import current_app


class SpeciesChangeRequestStorage:
    UPLOAD_EXPIRES_SECONDS = 900
    UPLOAD_HEAD_MAX_ATTEMPTS = 4
    UPLOAD_HEAD_RETRY_SECONDS = 0.35
    TMP_PREFIX = "species/pending/"
    FINAL_PREFIX = "species/"

    @classmethod
    def generate_upload_url(cls, filename: str, mime_type: str, size_bytes: int, species_id=None):
        normalized_mime = (mime_type or "").strip().lower()
        allowed_mimes = set(current_app.config.get("SPECIES_PHOTO_ALLOWED_MIME_TYPES", []))
        max_size = int(current_app.config["SPECIES_PHOTO_MAX_BYTES"])

        if normalized_mime not in allowed_mimes:
            raise AppError(pt="Tipo de arquivo não permitido", en="File type not allowed")
        if not isinstance(size_bytes, int) or size_bytes < 1:
            raise AppError(pt="`size_bytes` deve ser > 0", en="`size_bytes` must be > 0")
        if size_bytes > max_size:
            raise AppError(
                pt=f"Arquivo excede o limite de {max_size} bytes",
                en=f"File exceeds the limit of {max_size} bytes",
            )

        ext = cls.safe_extension(filename, normalized_mime)
        sid = f"{species_id}" if species_id is not None else "unknown"
        object_key = f"{cls.TMP_PREFIX}{sid}/{uuid4().hex}{ext}"
        tmp_bucket = current_app.config["MINIO_TMP_BUCKET"]

        try:
            signed = object_storage.generate_presigned_post(
                bucket=tmp_bucket,
                key=object_key,
                content_type=normalized_mime,
                max_size_bytes=max_size,
                expires_in_seconds=cls.UPLOAD_EXPIRES_SECONDS,
            )
        except (object_storage.ObjectStorageError, BotoCoreError, ClientError) as exc:
            raise AppError(
                pt=f"Falha ao gerar URL de upload: {exc}",
                en=f"Failed to generate upload URL: {exc}",
            )

        return {
            "upload_url": signed["url"],
            "fields": signed["fields"],
            "bucket_name": tmp_bucket,
            "object_key": object_key,
            "expires_at": object_storage.utc_now() + timedelta(seconds=cls.UPLOAD_EXPIRES_SECONDS),
        }

    @classmethod
    def validate_uploaded_objects(cls, photos_payload: list[dict[str, Any]]) -> None:
        if not photos_payload:
            return

        tmp_bucket = current_app.config["MINIO_TMP_BUCKET"]
        max_size = int(current_app.config["SPECIES_PHOTO_MAX_BYTES"])
        allowed_mimes = set(current_app.config.get("SPECIES_PHOTO_ALLOWED_MIME_TYPES", []))
        for photo in photos_payload:
            key = (photo.get("object_key") or "").strip()
            if not key.startswith(cls.TMP_PREFIX):
                raise AppError(
                    pt="`object_key` inválido para bucket temporário",
                    en="`object_key` is invalid for the temporary bucket",
                )

            declared_bucket = (photo.get("bucket_name") or tmp_bucket).strip()
            if declared_bucket != tmp_bucket:
                raise AppError(
                    pt="Fotos devem vir do bucket temporário",
                    en="Photos must come from the temporary bucket",
                )

            try:
                meta = cls.head_object_with_retry(tmp_bucket, key)
            except AppError:
                raise
            except (object_storage.ObjectStorageError, BotoCoreError, ClientError):
                raise AppError(
                    pt="Falha ao validar arquivo no bucket temporário",
                    en="Failed to validate file in temporary bucket",
                )

            content_length = int(meta.get("ContentLength") or 0)
            content_type = (meta.get("ContentType") or "").lower()
            if content_length < 1 or content_length > max_size:
                raise AppError(
                    pt="Arquivo com tamanho fora do limite permitido",
                    en="File size exceeds the allowed limit",
                )
            if content_type not in allowed_mimes:
                raise AppError(
                    pt="Arquivo com tipo MIME não permitido",
                    en="File MIME type not allowed",
                )

            photo["bucket_name"] = tmp_bucket
            photo["size_bytes"] = content_length
            if not photo.get("mime_type"):
                photo["mime_type"] = content_type

    @classmethod
    def head_object_with_retry(cls, bucket: str, key: str) -> dict[str, Any]:
        attempts = max(1, cls.UPLOAD_HEAD_MAX_ATTEMPTS)

        for attempt in range(1, attempts + 1):
            try:
                return object_storage.head_object(bucket, key)
            except (object_storage.ObjectStorageError, BotoCoreError, ClientError) as exc:
                if not cls.is_not_found_error(exc):
                    raise
                if attempt < attempts:
                    time.sleep(cls.UPLOAD_HEAD_RETRY_SECONDS)

        raise AppError(
            pt="Arquivo não encontrado no bucket temporário",
            en="File not found in temporary bucket",
            status=404,
        )

    @staticmethod
    def is_not_found_error(exc: Exception) -> bool:
        if not isinstance(exc, ClientError):
            return False

        code = str((exc.response.get("Error") or {}).get("Code") or "").strip()
        status = (exc.response.get("ResponseMetadata") or {}).get("HTTPStatusCode")
        return code in {"404", "NoSuchKey", "NotFound"} or status == 404

    @classmethod
    def promote_object_to_final(cls, photo, species_id: int) -> tuple[str, str]:
        src_bucket = (photo.bucket_name or current_app.config["MINIO_TMP_BUCKET"]).strip()
        src_key = photo.object_key
        tmp_bucket = current_app.config["MINIO_TMP_BUCKET"]
        final_bucket = current_app.config["MINIO_FINAL_BUCKET"]

        if src_bucket == final_bucket:
            return final_bucket, src_key
        if src_bucket != tmp_bucket:
            raise AppError(
                pt="Bucket de origem inválido para promoção",
                en="Invalid source bucket for promotion",
            )

        basename = os.path.basename(src_key)
        safe_name = re.sub(r"[^A-Za-z0-9._-]+", "_", basename)
        dest_key = f"{cls.FINAL_PREFIX}{species_id}/{photo.request_id}_{photo.id}_{safe_name}"
        try:
            object_storage.copy_object(src_bucket, src_key, final_bucket, dest_key)
            object_storage.delete_object(src_bucket, src_key)
        except (object_storage.ObjectStorageError, BotoCoreError, ClientError) as exc:
            raise AppError(
                pt=f"Falha ao promover foto para bucket final: {exc}",
                en=f"Failed to promote photo to final bucket: {exc}",
            )

        return final_bucket, dest_key

    @classmethod
    def delete_tmp_object_if_exists(cls, photo) -> None:
        src_bucket = (photo.bucket_name or current_app.config["MINIO_TMP_BUCKET"]).strip()
        src_key = photo.object_key
        tmp_bucket = current_app.config["MINIO_TMP_BUCKET"]
        if src_bucket != tmp_bucket:
            return
        try:
            object_storage.delete_object(src_bucket, src_key)
        except (object_storage.ObjectStorageError, BotoCoreError, ClientError):
            # Cleanup best-effort: a revisão não deve falhar por lixo já removido.
            return

    @staticmethod
    def safe_extension(filename: str, mime_type: str) -> str:
        guessed = mimetypes.guess_extension(mime_type) or ""
        source_ext = os.path.splitext(filename or "")[1].lower()
        if source_ext in {".jpg", ".jpeg", ".png", ".webp"}:
            return source_ext
        if guessed in {".jpe"}:
            return ".jpg"
        if guessed in {".jpg", ".jpeg", ".png", ".webp"}:
            return guessed
        return ".bin"

    @classmethod
    def cleanup_tmp_objects(cls, retention_days: int | None = None, dry_run: bool = True):
        days = retention_days or int(current_app.config["SPECIES_TMP_RETENTION_DAYS"])
        if days < 1:
            raise AppError(pt="`retention_days` deve ser >= 1", en="`retention_days` must be >= 1")

        threshold = object_storage.utc_now() - timedelta(days=days)
        tmp_bucket = current_app.config["MINIO_TMP_BUCKET"]
        objects = object_storage.list_objects(tmp_bucket, cls.TMP_PREFIX)

        deleted = 0
        candidates = 0
        for item in objects:
            last_modified = item.get("LastModified")
            if not last_modified or last_modified >= threshold:
                continue
            candidates += 1
            if not dry_run:
                object_storage.delete_object(tmp_bucket, item["Key"])
                deleted += 1

        return {
            "bucket": tmp_bucket,
            "retention_days": days,
            "candidates": candidates,
            "deleted": deleted,
            "dry_run": dry_run,
        }
