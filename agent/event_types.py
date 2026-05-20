"""Event type definitions, envelope model, and envelope construction helpers."""

import hashlib
import json
import uuid
from datetime import datetime, timezone
from enum import Enum
from typing import Any

from pydantic import BaseModel, Field


class EventType(str, Enum):
    TASK_CREATED = "TASK_CREATED"
    TASK_UPDATED = "TASK_UPDATED"
    TASK_COMPLETED = "TASK_COMPLETED"
    TASK_FAILED = "TASK_FAILED"
    SEARCH_REQUESTED = "SEARCH_REQUESTED"
    SEARCH_COMPLETED = "SEARCH_COMPLETED"
    SQL_QUERY_REQUESTED = "SQL_QUERY_REQUESTED"
    SQL_QUERY_COMPLETED = "SQL_QUERY_COMPLETED"
    AGENT_STARTED = "AGENT_STARTED"
    AGENT_STEP_COMPLETED = "AGENT_STEP_COMPLETED"
    AGENT_FINISHED = "AGENT_FINISHED"


class EventEnvelope(BaseModel):
    event_id: str
    event_type: EventType
    timestamp: datetime
    payload: dict[str, Any]
    idempotency_key: str
    schema_version: str = Field(default="1.0")


def stable_payload_hash(payload: dict[str, Any]) -> str:
    """Return a stable SHA-256 hex digest of a payload dict.

    Sorts keys before serialising so identical content always produces the
    same hash regardless of insertion order.
    """
    serialised = json.dumps(payload, sort_keys=True, default=str)
    return hashlib.sha256(serialised.encode()).hexdigest()


def build_idempotency_key(event_type: EventType, payload: dict[str, Any]) -> str:
    """Build a deterministic idempotency key from event type and payload.

    Two calls with the same event_type and logically equal payload will
    always return the same key, making it safe to use as a Kafka message key
    for deduplication.
    """
    payload_hash = stable_payload_hash(payload)
    return f"{event_type.value}:{payload_hash}"


def create_envelope(event_type: EventType, payload: dict[str, Any]) -> EventEnvelope:
    """Construct a fully-populated EventEnvelope ready for publishing."""
    return EventEnvelope(
        event_id=str(uuid.uuid4()),
        event_type=event_type,
        timestamp=datetime.now(tz=timezone.utc),
        payload=payload,
        idempotency_key=build_idempotency_key(event_type, payload),
    )
