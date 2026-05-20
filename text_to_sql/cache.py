"""Redis cache for text-to-SQL query results."""

import hashlib
import logging

import redis

logger = logging.getLogger(__name__)


class RedisCache:
    """Simple string cache backed by Redis.

    Args:
        redis_host: Redis server hostname.
        redis_port: Redis server port.
        ttl_seconds: Time-to-live for cached entries in seconds.
    """

    def __init__(
        self,
        redis_host: str = "localhost",
        redis_port: int = 6380,
        ttl_seconds: int = 3600,
    ) -> None:
        self._client = redis.Redis(
            host=redis_host,
            port=redis_port,
            decode_responses=True,
        )
        self._ttl = ttl_seconds
        logger.debug("RedisCache connected to %s:%d ttl=%ds", redis_host, redis_port, ttl_seconds)

    def make_key(self, text: str) -> str:
        """Return a stable cache key for the given input text.

        Uses SHA-256 so arbitrary-length questions map to a fixed-size key.

        Args:
            text: The natural-language question or arbitrary string.

        Returns:
            A prefixed hex digest string.
        """
        digest = hashlib.sha256(text.strip().lower().encode()).hexdigest()
        return f"t2sql:{digest}"

    def get(self, key: str) -> str | None:
        """Retrieve a cached value by key.

        Args:
            key: Cache key (typically produced by make_key).

        Returns:
            The cached string, or None on a cache miss.
        """
        value = self._client.get(key)
        if value is not None:
            logger.debug("Cache HIT key=%s", key[:20])
        else:
            logger.debug("Cache MISS key=%s", key[:20])
        return value

    def set(self, key: str, value: str) -> None:
        """Store a value in the cache with the configured TTL.

        Args:
            key: Cache key.
            value: String value to store.
        """
        self._client.setex(key, self._ttl, value)
        logger.debug("Cache SET key=%s ttl=%ds", key[:20], self._ttl)
