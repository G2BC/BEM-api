import mimetypes
import os
import time
from datetime import timedelta
from typing import Any
from urllib.parse import urlsplit
from uuid import uuid4

import app.utils.object_storage as object_storage
from app.exceptions import AppError
from app.models.species_photo import SpeciesPhoto
from app.repositories.species_photo_repository import SpeciesPhotoRepository
from app.repositories.species_repository import SpeciesRepository
from botocore.exceptions import BotoCoreError, ClientError
from flask import current_app


class SpeciesPhotoService:
    UPLOAD_EXPIRES_SECONDS = 900
    UPLOAD_HEAD_MAX_ATTEMPTS = 4
    UPLOAD_HEAD_RETRY_SECONDS = 0.35
    OBJECT_PREFIX = "species"
    SOURCE_LUMM_UPLOAD = "LUMM-Upload"

    @classmethod
    def generate_upload_url(cls, species_id: int, filename: str, mime_type: str, size_bytes: int):
        cls._ensure_species_exists(species_id)

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

        ext = cls._safe_extension(filename, normalized_mime)
        object_key = f"{cls.OBJECT_PREFIX}/{species_id}/{uuid4().hex}{ext}"
        bucket_name = cls._species_bucket()

        try:
            signed = object_storage.generate_presigned_post(
                bucket=bucket_name,
                key=object_key,
                content_type=normalized_mime,
                max_size_bytes=max_size,
                expires_in_seconds=cls.UPLOAD_EXPIRES_SECONDS,
            )
        except (object_storage.ObjectStorageError, BotoCoreError, ClientError) as exc:
            raise AppError(
                pt=f"Falha ao gerar URL de upload: {exc}",
                en=f"Failed to generate upload URL: {exc}",
            ) from exc

        return {
            "upload_url": signed["url"],
            "fields": signed["fields"],
            "bucket_name": bucket_name,
            "object_key": object_key,
            "expires_at": object_storage.utc_now() + timedelta(seconds=cls.UPLOAD_EXPIRES_SECONDS),
        }

    @classmethod
    def create_photo(cls, species_id: int, payload: dict[str, Any]) -> SpeciesPhoto:
        cls._ensure_species_exists(species_id)

        expected_bucket = cls._species_bucket()
        bucket_name = (payload.get("bucket_name") or "").strip()
        object_key = (payload.get("object_key") or "").strip().lstrip("/")
        original_filename = (payload.get("original_filename") or "").strip()
        mime_type = (payload.get("mime_type") or "").strip().lower()
        license_code = (payload.get("license_code") or "").strip()
        attribution = (payload.get("attribution") or "").strip()
        rights_holder = (payload.get("rights_holder") or "").strip()
        source_url = (payload.get("source_url") or "").strip() or None
        featured = bool(payload.get("featured", False))

        if bucket_name != expected_bucket:
            raise AppError(
                pt="`bucket_name` inválido para o fluxo de fotos oficiais",
                en="`bucket_name` is invalid for the official photos flow",
            )
        if not object_key.startswith(f"{cls.OBJECT_PREFIX}/{species_id}/"):
            raise AppError(
                pt="`object_key` inválido para a espécie informada",
                en="`object_key` is invalid for the given species",
            )
        if not original_filename:
            raise AppError(
                pt="`original_filename` é obrigatório", en="`original_filename` is required"
            )
        if not license_code:
            raise AppError(pt="`license_code` é obrigatório", en="`license_code` is required")
        if not attribution:
            raise AppError(pt="`attribution` é obrigatório", en="`attribution` is required")
        if not rights_holder:
            raise AppError(pt="`rights_holder` é obrigatório", en="`rights_holder` is required")

        allowed_mimes = set(current_app.config.get("SPECIES_PHOTO_ALLOWED_MIME_TYPES", []))
        max_size = int(current_app.config["SPECIES_PHOTO_MAX_BYTES"])
        declared_size = payload.get("size_bytes")
        if not isinstance(declared_size, int) or declared_size < 1:
            raise AppError(pt="`size_bytes` deve ser > 0", en="`size_bytes` must be > 0")
        if declared_size > max_size:
            raise AppError(
                pt=f"Arquivo excede o limite de {max_size} bytes",
                en=f"File exceeds the limit of {max_size} bytes",
            )
        if mime_type not in allowed_mimes:
            raise AppError(pt="Tipo de arquivo não permitido", en="File type not allowed")

        try:
            meta = cls._head_object_with_retry(bucket_name, object_key)
        except AppError:
            raise
        except (object_storage.ObjectStorageError, BotoCoreError, ClientError) as exc:
            raise AppError(
                pt=f"Falha ao validar arquivo no storage: {exc}",
                en=f"Failed to validate file in storage: {exc}",
            ) from exc

        content_length = int(meta.get("ContentLength") or 0)
        content_type = (meta.get("ContentType") or "").split(";", 1)[0].strip().lower()
        if content_length < 1 or content_length > max_size:
            raise AppError(
                pt="Arquivo com tamanho fora do limite permitido",
                en="File size exceeds the allowed limit",
            )
        if content_type not in allowed_mimes:
            raise AppError(
                pt="Arquivo com tipo MIME não permitido", en="File MIME type not allowed"
            )
        if declared_size != content_length:
            raise AppError(
                pt="`size_bytes` não confere com o arquivo enviado",
                en="`size_bytes` does not match the uploaded file",
            )
        if mime_type != content_type:
            raise AppError(
                pt="`mime_type` não confere com o arquivo enviado",
                en="`mime_type` does not match the uploaded file",
            )

        object_url = object_storage.build_public_object_url(bucket_name, object_key)
        already_exists = (
            SpeciesPhoto.query.filter(
                SpeciesPhoto.species_id == species_id,
                SpeciesPhoto.original_url == object_url,
            ).first()
            is not None
        )
        if already_exists:
            raise AppError(
                pt="Foto já cadastrada para esta espécie",
                en="Photo already registered for this species",
            )

        if featured:
            (
                SpeciesPhoto.query.filter(SpeciesPhoto.species_id == species_id).update(
                    {SpeciesPhoto.featured: False}, synchronize_session=False
                )
            )

        photo = SpeciesPhoto(
            species_id=species_id,
            photo_id=SpeciesPhotoRepository.next_manual_photo_id(species_id),
            medium_url=object_url,
            original_url=object_url,
            license_code=license_code,
            attribution=attribution,
            rights_holder=rights_holder,
            source_url=source_url,
            source=cls.SOURCE_LUMM_UPLOAD,
            featured=featured,
        )
        SpeciesPhotoRepository.save(photo)
        return photo

    @classmethod
    def update_photo(
        cls,
        species_id: int,
        photo_id: int | str,
        payload: dict[str, Any],
    ) -> SpeciesPhoto:
        photo = cls._find_photo(species_id, photo_id)

        if "license_code" in payload:
            value = payload.get("license_code")
            photo.license_code = (value or "").strip() or None
        if "attribution" in payload:
            value = payload.get("attribution")
            photo.attribution = (value or "").strip() or None
        if "rights_holder" in payload:
            value = payload.get("rights_holder")
            photo.rights_holder = (value or "").strip() or None
        if "source_url" in payload:
            value = payload.get("source_url")
            photo.source_url = (value or "").strip() or None
        if "featured" in payload:
            next_featured = bool(payload.get("featured"))
            photo.featured = next_featured
            if next_featured:
                (
                    SpeciesPhoto.query.filter(
                        SpeciesPhoto.species_id == species_id,
                        SpeciesPhoto.photo_id != photo.photo_id,
                    ).update({SpeciesPhoto.featured: False}, synchronize_session=False)
                )

        SpeciesPhotoRepository.commit()
        return photo

    @classmethod
    def delete_photo(cls, species_id: int, photo_id: int | str) -> None:
        photo = cls._find_photo(species_id, photo_id)

        if cls._is_system_photo_source(photo):
            storage_bucket, storage_key = cls._extract_storage_location(photo)
            if storage_bucket and storage_key:
                try:
                    object_storage.delete_object(storage_bucket, storage_key)
                except (object_storage.ObjectStorageError, BotoCoreError, ClientError) as exc:
                    if cls._is_not_found_error(exc):
                        pass
                    else:
                        raise AppError(
                            pt=f"Falha ao remover arquivo no storage: {exc}",
                            en=f"Failed to remove file from storage: {exc}",
                        ) from exc

        SpeciesPhotoRepository.delete(photo)

    @classmethod
    def _find_photo(cls, species_id: int, photo_id: int | str) -> SpeciesPhoto:
        cls._ensure_species_exists(species_id)
        parsed_photo_id = cls._parse_photo_id(photo_id)

        photo = SpeciesPhoto.query.filter(
            SpeciesPhoto.species_id == species_id,
            SpeciesPhoto.photo_id == parsed_photo_id,
        ).first()
        if not photo:
            raise AppError(
                pt="Foto não encontrada para esta espécie",
                en="Photo not found for this species",
                status=404,
            )
        return photo

    @staticmethod
    def _ensure_species_exists(species_id: int) -> None:
        if not isinstance(species_id, int) or species_id < 1:
            raise AppError(pt="`species_id` inválido", en="Invalid `species_id`")
        if not SpeciesRepository.exists_by_id(species_id):
            raise AppError(pt="Espécie não encontrada.", en="Species not found.", status=404)

    @staticmethod
    def _parse_photo_id(photo_id: int | str) -> int:
        if isinstance(photo_id, bool):
            raise AppError(pt="`photo_id` inválido", en="Invalid `photo_id`")
        try:
            return int(str(photo_id).strip())
        except (TypeError, ValueError) as exc:
            raise AppError(pt="`photo_id` inválido", en="Invalid `photo_id`") from exc

    @staticmethod
    def _safe_extension(filename: str, mime_type: str) -> str:
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
    def _head_object_with_retry(cls, bucket: str, key: str) -> dict[str, Any]:
        attempts = max(1, cls.UPLOAD_HEAD_MAX_ATTEMPTS)

        for attempt in range(1, attempts + 1):
            try:
                return object_storage.head_object(bucket, key)
            except (object_storage.ObjectStorageError, BotoCoreError, ClientError) as exc:
                if not cls._is_not_found_error(exc):
                    raise
                if attempt < attempts:
                    time.sleep(cls.UPLOAD_HEAD_RETRY_SECONDS)

        raise AppError(
            pt="Arquivo não encontrado no storage",
            en="File not found in storage",
            status=404,
        )

    @classmethod
    def _extract_storage_location(cls, photo: SpeciesPhoto) -> tuple[str | None, str | None]:
        candidate_urls = [
            (getattr(photo, "original_url", None) or "").strip(),
            (getattr(photo, "medium_url", None) or "").strip(),
        ]

        for candidate in candidate_urls:
            if not candidate:
                continue
            bucket, key = cls._parse_storage_url(candidate)
            if bucket and key:
                return bucket, key

        return None, None

    @classmethod
    def _is_system_photo_source(cls, photo: SpeciesPhoto) -> bool:
        source = (getattr(photo, "source", None) or "").strip().lower()
        return source == cls.SOURCE_LUMM_UPLOAD.lower()

    @classmethod
    def _parse_storage_url(cls, value: str) -> tuple[str | None, str | None]:
        raw = (value or "").strip()
        if not raw:
            return None, None

        if raw.startswith("minio://"):
            path = raw[len("minio://") :]
            if "/" not in path:
                return None, None
            bucket, key = path.split("/", 1)
            return bucket.strip() or None, key.strip().lstrip("/") or None

        parsed = urlsplit(raw)
        if not parsed.scheme or not parsed.netloc:
            return None, None

        path = (parsed.path or "").strip("/")
        if not path:
            return None, None

        bucket = cls._species_bucket()
        if path.startswith(f"{bucket}/"):
            return bucket, path[len(bucket) + 1 :]

        public_base = (current_app.config.get("MINIO_PUBLIC_BASE_URL") or "").strip().rstrip("/")
        if public_base:
            base_path = urlsplit(public_base).path.strip("/")
            if base_path and path.startswith(f"{base_path}/"):
                remainder = path[len(base_path) + 1 :]
                if remainder.startswith(f"{bucket}/"):
                    return bucket, remainder[len(bucket) + 1 :]
                return bucket, remainder

        return None, None

    @staticmethod
    def _is_not_found_error(exc: Exception) -> bool:
        if not isinstance(exc, ClientError):
            return False

        code = str((exc.response.get("Error") or {}).get("Code") or "").strip()
        status = (exc.response.get("ResponseMetadata") or {}).get("HTTPStatusCode")
        return code in {"404", "NoSuchKey", "NotFound"} or status == 404

    @staticmethod
    def _species_bucket() -> str:
        bucket = (current_app.config.get("MINIO_FINAL_BUCKET") or "").strip()
        if not bucket:
            raise AppError(
                pt="Bucket de fotos de espécie não configurado",
                en="Species photo bucket not configured",
            )
        return bucket
