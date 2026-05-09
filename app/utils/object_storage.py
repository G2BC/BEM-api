from __future__ import annotations

from datetime import UTC, datetime, timedelta
from functools import lru_cache
from typing import Any
from urllib.parse import urlsplit, urlunsplit

import boto3
from botocore.client import BaseClient
from botocore.config import Config
from flask import current_app


class ObjectStorageError(Exception):
    pass


@lru_cache(maxsize=1)
def _get_client() -> BaseClient:
    endpoint = current_app.config.get("MINIO_ENDPOINT")
    access_key = current_app.config.get("MINIO_ACCESS_KEY")
    secret_key = current_app.config.get("MINIO_SECRET_KEY")
    secure = bool(current_app.config.get("MINIO_SECURE", False))
    region = current_app.config.get("MINIO_REGION", "us-east-1")

    if not endpoint or not access_key or not secret_key:
        raise ObjectStorageError("Configuração MinIO incompleta")

    endpoint_url = endpoint
    if not endpoint_url.startswith("http://") and not endpoint_url.startswith("https://"):
        scheme = "https" if secure else "http"
        endpoint_url = f"{scheme}://{endpoint_url}"

    return boto3.client(
        "s3",
        endpoint_url=endpoint_url,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        region_name=region,
        config=Config(signature_version="s3v4", s3={"addressing_style": "path"}),
    )


@lru_cache(maxsize=1)
def _get_public_client() -> BaseClient:
    """Client whose endpoint_url matches MINIO_PUBLIC_BASE_URL.

    Presigned GET URLs embed the host in the signature, so the client must be
    created with the public endpoint so the signed host matches what browsers hit.
    Falls back to the regular client when MINIO_PUBLIC_BASE_URL is not set.
    """
    public_base = current_app.config.get("MINIO_PUBLIC_BASE_URL", "").strip()
    if not public_base:
        return _get_client()

    access_key = current_app.config.get("MINIO_ACCESS_KEY")
    secret_key = current_app.config.get("MINIO_SECRET_KEY")
    secure = bool(current_app.config.get("MINIO_SECURE", False))
    region = current_app.config.get("MINIO_REGION", "us-east-1")

    if not public_base.startswith(("http://", "https://")):
        scheme = "https" if secure else "http"
        public_base = f"{scheme}://{public_base}"

    return boto3.client(
        "s3",
        endpoint_url=public_base.rstrip("/"),
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        region_name=region,
        config=Config(signature_version="s3v4", s3={"addressing_style": "path"}),
    )


def clear_client_cache() -> None:
    _get_client.cache_clear()
    _get_public_client.cache_clear()


def generate_presigned_post(
    bucket: str,
    key: str,
    content_type: str,
    max_size_bytes: int,
    expires_in_seconds: int,
) -> dict[str, Any]:
    client = _get_client()
    conditions = [
        {"key": key},
        {"Content-Type": content_type},
        ["content-length-range", 1, max_size_bytes],
    ]
    fields = {
        "key": key,
        "Content-Type": content_type,
    }
    signed = client.generate_presigned_post(
        Bucket=bucket,
        Key=key,
        Fields=fields,
        Conditions=conditions,
        ExpiresIn=expires_in_seconds,
    )
    signed["url"] = _normalize_presigned_post_url(signed.get("url", ""))
    return signed


def generate_presigned_get_url(bucket: str, key: str, expires_in_seconds: int) -> str:
    client = _get_public_client()
    return client.generate_presigned_url(
        ClientMethod="get_object",
        Params={"Bucket": bucket, "Key": key},
        ExpiresIn=expires_in_seconds,
    )


def head_object(bucket: str, key: str) -> dict[str, Any]:
    client = _get_client()
    return client.head_object(Bucket=bucket, Key=key)


def copy_object(src_bucket: str, src_key: str, dest_bucket: str, dest_key: str) -> None:
    client = _get_client()
    copy_source = {"Bucket": src_bucket, "Key": src_key}
    client.copy_object(Bucket=dest_bucket, Key=dest_key, CopySource=copy_source)


def delete_object(bucket: str, key: str) -> None:
    client = _get_client()
    client.delete_object(Bucket=bucket, Key=key)


def list_objects(bucket: str, prefix: str) -> list[dict[str, Any]]:
    client = _get_client()
    paginator = client.get_paginator("list_objects_v2")
    result: list[dict[str, Any]] = []
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for item in page.get("Contents", []):
            result.append(item)
    return result


def _normalize_public_base_url(raw: str, secure_default: bool) -> str:
    value = (raw or "").strip()
    if not value:
        return ""
    if value.startswith(("http://", "https://")):
        return value.rstrip("/")
    scheme = "https" if secure_default else "http"
    return f"{scheme}://{value}".rstrip("/")


def _normalize_presigned_post_url(url: str) -> str:
    raw = (url or "").strip()
    if not raw:
        return raw

    base = _normalize_public_base_url(
        current_app.config.get("MINIO_PUBLIC_BASE_URL", ""),
        bool(current_app.config.get("MINIO_SECURE", False)),
    )
    if not base:
        return raw

    src = urlsplit(raw)
    target = urlsplit(base)
    base_path = (target.path or "").rstrip("/")
    src_path = src.path or ""
    merged_path = f"{base_path}{src_path}" if base_path else src_path

    return urlunsplit(
        (
            target.scheme or src.scheme,
            target.netloc or src.netloc,
            merged_path,
            src.query,
            src.fragment,
        )
    )


def build_public_object_url(bucket: str, key: str) -> str:
    safe_bucket = (bucket or "").strip()
    safe_key = (key or "").strip().lstrip("/")
    if not safe_bucket or not safe_key:
        return ""

    base = _normalize_public_base_url(
        current_app.config.get("MINIO_PUBLIC_BASE_URL", ""),
        bool(current_app.config.get("MINIO_SECURE", False)),
    )
    if not base:
        return f"minio://{safe_bucket}/{safe_key}"

    if base.endswith(f"/{safe_bucket}"):
        return f"{base}/{safe_key}"
    return f"{base}/{safe_bucket}/{safe_key}"


def normalize_object_url(url: str | None) -> str | None:
    raw = (url or "").strip()
    if not raw:
        return None
    if raw.startswith(("http://", "https://")):
        return raw
    if not raw.startswith("minio://"):
        return raw

    path = raw[len("minio://") :]
    if "/" not in path:
        return raw

    bucket, key = path.split("/", 1)
    normalized = build_public_object_url(bucket, key)
    return normalized or raw


def utc_now() -> datetime:
    return datetime.now(UTC)


def utc_in(seconds: int) -> datetime:
    return utc_now() + timedelta(seconds=seconds)
