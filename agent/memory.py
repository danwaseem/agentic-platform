"""MongoDB-backed agent memory: trace storage and idempotency deduplication."""

import logging
from datetime import datetime, timezone, timedelta
from typing import Any

from pymongo import MongoClient, ASCENDING, DESCENDING
from pymongo.collection import Collection
from pymongo.errors import DuplicateKeyError

logger = logging.getLogger(__name__)


class AgentMemory:
    """Persist agent traces and track processed event idempotency keys in MongoDB.

    Args:
        mongo_uri: MongoDB connection string.
        database_name: Database to use.
        traces_collection: Collection name for trace documents.
        dedupe_collection: Collection name for idempotency key documents.
    """

    def __init__(
        self,
        mongo_uri: str = "mongodb://localhost:27017",
        database_name: str = "agent_memory",
        traces_collection: str = "traces",
        dedupe_collection: str = "dedupe",
    ) -> None:
        self._client: MongoClient = MongoClient(mongo_uri)
        db = self._client[database_name]
        self._traces: Collection = db[traces_collection]
        self._dedupe: Collection = db[dedupe_collection]
        self._ensure_indexes()
        logger.debug(
            "AgentMemory connected: uri=%s db=%s", mongo_uri, database_name
        )

    # ------------------------------------------------------------------
    # Index setup
    # ------------------------------------------------------------------

    def _ensure_indexes(self) -> None:
        """Create indexes on first use (idempotent — MongoDB skips if they exist)."""
        self._traces.create_index([("timestamp", DESCENDING)])
        self._traces.create_index([("task_id", ASCENDING), ("step", ASCENDING)])
        # unique constraint drives deduplication
        self._dedupe.create_index(
            [("idempotency_key", ASCENDING)], unique=True
        )
        logger.debug("Indexes ensured on traces and dedupe collections")

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def store_trace(
        self,
        event_id: str,
        task_id: str | None,
        event_type: str,
        step: int,
        input_data: dict[str, Any],
        output_data: dict[str, Any],
        timestamp: datetime,
    ) -> None:
        """Insert a single trace document recording one agent step.

        Args:
            event_id: Unique identifier of the originating event.
            task_id: Associated task identifier, or None for non-task events.
            event_type: String name of the EventType.
            step: Ordinal step number within the agent's ReAct loop.
            input_data: Data passed into this step.
            output_data: Data produced by this step.
            timestamp: UTC datetime of the step.
        """
        doc = {
            "event_id": event_id,
            "task_id": task_id,
            "event_type": event_type,
            "step": step,
            "input_data": input_data,
            "output_data": output_data,
            "timestamp": timestamp,
        }
        result = self._traces.insert_one(doc)
        logger.debug(
            "Stored trace _id=%s event_id=%s step=%d", result.inserted_id, event_id, step
        )

    def is_duplicate(self, idempotency_key: str) -> bool:
        """Return True if this idempotency key has already been processed.

        Args:
            idempotency_key: Deterministic key built from event type + payload hash.
        """
        found = self._dedupe.find_one({"idempotency_key": idempotency_key}) is not None
        logger.debug("is_duplicate key=%s → %s", idempotency_key, found)
        return found

    def mark_processed(self, idempotency_key: str, event_id: str) -> None:
        """Record an idempotency key as processed.

        Silently ignores a duplicate-key error so callers do not need to
        guard with is_duplicate first (check-then-act is not atomic).

        Args:
            idempotency_key: Key to record.
            event_id: Event that was processed under this key.
        """
        try:
            self._dedupe.insert_one(
                {
                    "idempotency_key": idempotency_key,
                    "event_id": event_id,
                    "recorded_at": datetime.now(tz=timezone.utc),
                }
            )
            logger.debug("Marked processed key=%s event_id=%s", idempotency_key, event_id)
        except DuplicateKeyError:
            logger.debug(
                "Idempotency key already exists (safe to ignore): key=%s", idempotency_key
            )

    def get_recent_traces(self, limit: int = 20) -> list[dict[str, Any]]:
        """Return the most recent trace documents across all tasks.

        Args:
            limit: Maximum number of documents to return.

        Returns:
            List of trace dicts sorted by timestamp descending, _id excluded.
        """
        cursor = (
            self._traces.find({}, {"_id": 0})
            .sort("timestamp", DESCENDING)
            .limit(limit)
        )
        traces = list(cursor)
        logger.debug("get_recent_traces limit=%d → %d rows", limit, len(traces))
        return traces

    def get_traces_for_task(self, task_id: str) -> list[dict[str, Any]]:
        """Return all traces for a specific task, ordered by step ascending.

        Args:
            task_id: Task identifier to filter on.

        Returns:
            List of trace dicts sorted by step ascending, _id excluded.
        """
        cursor = (
            self._traces.find({"task_id": task_id}, {"_id": 0})
            .sort("step", ASCENDING)
        )
        traces = list(cursor)
        logger.debug(
            "get_traces_for_task task_id=%s → %d rows", task_id, len(traces)
        )
        return traces

    def clear_old_traces(self, older_than_hours: int = 24) -> int:
        """Delete traces older than the given number of hours.

        Args:
            older_than_hours: Cutoff age in hours; traces older than this are removed.

        Returns:
            Number of documents deleted.
        """
        cutoff = datetime.now(tz=timezone.utc) - timedelta(hours=older_than_hours)
        result = self._traces.delete_many({"timestamp": {"$lt": cutoff}})
        logger.debug(
            "clear_old_traces older_than_hours=%d deleted=%d",
            older_than_hours,
            result.deleted_count,
        )
        return result.deleted_count

    def close(self) -> None:
        """Close the MongoDB connection."""
        self._client.close()
        logger.debug("AgentMemory connection closed")

    def __enter__(self) -> "AgentMemory":
        return self

    def __exit__(self, *_: Any) -> None:
        self.close()
