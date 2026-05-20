"""Redis connection singleton."""

import logging
import os

import redis as redis_lib

logger = logging.getLogger(__name__)

_REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
_REDIS_PORT = int(os.getenv("REDIS_PORT", "6380"))

_client: redis_lib.Redis | None = None


def get_redis_client() -> redis_lib.Redis:
    """Return the shared Redis client, creating it on first call.

    Uses a module-level singleton backed by redis-py's built-in connection
    pool, so concurrent requests share a pool rather than each opening a
    new TCP connection.
    """
    global _client
    if _client is None:
        _client = redis_lib.Redis(
            host=_REDIS_HOST,
            port=_REDIS_PORT,
            decode_responses=True,
        )
        logger.info("Redis client created: %s:%d", _REDIS_HOST, _REDIS_PORT)
    return _client
