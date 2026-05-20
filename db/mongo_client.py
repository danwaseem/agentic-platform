"""MongoDB connection singleton and collection accessors."""

import logging
import os

from pymongo import MongoClient
from pymongo.collection import Collection

logger = logging.getLogger(__name__)

_MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017")
_DB_NAME = os.getenv("MONGO_DB", "agent_memory")

_client: MongoClient | None = None


def get_mongo_client() -> MongoClient:
    """Return the shared MongoClient, creating it on first call.

    Uses a module-level singleton so the connection pool is reused across
    requests rather than opened and closed per call.
    """
    global _client
    if _client is None:
        _client = MongoClient(_MONGO_URI)
        logger.info("MongoDB client created: uri=%s db=%s", _MONGO_URI, _DB_NAME)
    return _client


def get_tasks_collection() -> Collection:
    """Return the 'traces' collection used to store agent task traces."""
    return get_mongo_client()[_DB_NAME]["traces"]
