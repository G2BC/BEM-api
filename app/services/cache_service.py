import json
from typing import Any, Optional

from flask import current_app

try:
    import redis
except Exception:  # pragma: no cover
    redis = None


class CacheService:
    _client = None
    _client_url: Optional[str] = None

    @classmethod
    def _is_enabled(cls) -> bool:
        return bool((current_app.config.get("REDIS_URL") or "").strip()) and redis is not None

    @classmethod
    def _client_or_none(cls):
        if not cls._is_enabled():
            return None

        redis_url = (current_app.config.get("REDIS_URL") or "").strip()
        if not redis_url:
            return None

        if cls._client is not None and cls._client_url == redis_url:
            return cls._client

        try:
            socket_timeout = float(current_app.config.get("REDIS_SOCKET_TIMEOUT_SECONDS", 1.5))
            client = redis.from_url(
                redis_url,
                decode_responses=True,
                socket_timeout=socket_timeout,
                socket_connect_timeout=socket_timeout,
            )
            client.ping()
            cls._client = client
            cls._client_url = redis_url
            return cls._client
        except Exception:
            cls._client = None
            cls._client_url = None
            return None

    @classmethod
    def get(cls, key: str) -> Optional[str]:
        client = cls._client_or_none()
        if not client:
            return None
        try:
            return client.get(key)
        except Exception:
            return None

    @classmethod
    def set(cls, key: str, value: str, ttl_seconds: Optional[int] = None) -> bool:
        client = cls._client_or_none()
        if not client:
            return False
        try:
            ttl = ttl_seconds or int(current_app.config.get("REDIS_DEFAULT_TTL_SECONDS", 300))
            client.set(name=key, value=value, ex=max(1, int(ttl)))
            return True
        except Exception:
            return False

    @classmethod
    def delete(cls, key: str) -> bool:
        client = cls._client_or_none()
        if not client:
            return False
        try:
            client.delete(key)
            return True
        except Exception:
            return False

    @classmethod
    def get_json(cls, key: str) -> Optional[Any]:
        raw_value = cls.get(key)
        if not raw_value:
            return None
        try:
            return json.loads(raw_value)
        except Exception:
            return None

    @classmethod
    def set_json(cls, key: str, value: Any, ttl_seconds: Optional[int] = None) -> bool:
        try:
            serialized = json.dumps(value, ensure_ascii=False)
        except Exception:
            return False
        return cls.set(key, serialized, ttl_seconds=ttl_seconds)
