"""Unit tests for agent event types, ReAct loop, and memory deduplication."""

from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import pytest

from agent.event_types import (
    EventEnvelope,
    EventType,
    build_idempotency_key,
    create_envelope,
    stable_payload_hash,
)
from agent.react_agent import ReactAgent


# ------------------------------------------------------------------ #
# Helpers
# ------------------------------------------------------------------ #

class FakeMemory:
    """In-process substitute for AgentMemory — no MongoDB required."""

    def __init__(self) -> None:
        self._processed: set[str] = set()
        self.traces: list[dict] = []

    def is_duplicate(self, idempotency_key: str) -> bool:
        return idempotency_key in self._processed

    def mark_processed(self, idempotency_key: str, event_id: str) -> None:
        self._processed.add(idempotency_key)

    def store_trace(self, **kwargs) -> None:
        self.traces.append(kwargs)


class FakeProducer:
    """In-process substitute for KafkaEventProducer — no Kafka required."""

    def __init__(self) -> None:
        self.published: list[tuple[str, EventEnvelope]] = []

    def publish(self, topic: str, envelope: EventEnvelope) -> None:
        self.published.append((topic, envelope))

    def close(self) -> None:
        pass

    def __enter__(self):
        return self

    def __exit__(self, *_):
        self.close()


# ------------------------------------------------------------------ #
# Tests
# ------------------------------------------------------------------ #

class TestEventEnvelope:
    def test_idempotency_key_is_stable_regardless_of_payload_key_order(self) -> None:
        """Two payloads with the same content but different key insertion order
        must produce identical idempotency keys."""
        payload_a = {"task_id": "123", "description": "hello", "priority": 1}
        payload_b = {"priority": 1, "description": "hello", "task_id": "123"}

        key_a = build_idempotency_key(EventType.TASK_CREATED, payload_a)
        key_b = build_idempotency_key(EventType.TASK_CREATED, payload_b)

        assert key_a == key_b

    def test_idempotency_key_differs_for_different_payloads(self) -> None:
        """Different payload content must produce different idempotency keys."""
        key_a = build_idempotency_key(EventType.TASK_CREATED, {"task_id": "1"})
        key_b = build_idempotency_key(EventType.TASK_CREATED, {"task_id": "2"})

        assert key_a != key_b

    def test_idempotency_key_differs_for_different_event_types(self) -> None:
        """Same payload under different event types must produce different keys."""
        payload = {"task_id": "42"}
        key_a = build_idempotency_key(EventType.TASK_CREATED, payload)
        key_b = build_idempotency_key(EventType.TASK_COMPLETED, payload)

        assert key_a != key_b

    def test_stable_payload_hash_is_deterministic(self) -> None:
        """stable_payload_hash must return the same digest on repeated calls."""
        payload = {"x": 1, "y": "hello"}
        assert stable_payload_hash(payload) == stable_payload_hash(payload)

    def test_create_envelope_fields(self) -> None:
        """create_envelope must populate all required fields."""
        env = create_envelope(EventType.AGENT_STARTED, {"task_id": "t1"})

        assert env.event_type == EventType.AGENT_STARTED
        assert env.schema_version == "1.0"
        assert env.event_id  # non-empty UUID string
        assert env.idempotency_key.startswith("AGENT_STARTED:")
        assert env.timestamp.tzinfo is not None  # timezone-aware


class TestReactAgent:
    def _make_agent(self, max_steps: int = 5):
        producer = FakeProducer()
        memory = FakeMemory()
        agent = ReactAgent(producer=producer, memory=memory, max_steps=max_steps)
        return agent, producer, memory

    def test_run_returns_agent_finished_envelope(self) -> None:
        """run() must always return an AGENT_FINISHED envelope."""
        agent, _, _ = self._make_agent()
        task_event = create_envelope(
            EventType.TASK_CREATED,
            {"task_id": "t-test-1", "description": "Show total revenue"},
        )
        result = agent.run(task_event)

        assert result.event_type == EventType.AGENT_FINISHED
        assert "answer" in result.payload

    def test_run_does_not_exceed_max_steps(self) -> None:
        """The agent must not publish more AGENT_STEP_COMPLETED events
        than max_steps allows."""
        max_steps = 3
        agent, producer, _ = self._make_agent(max_steps=max_steps)
        task_event = create_envelope(
            EventType.TASK_CREATED,
            {"task_id": "t-test-2", "description": "Find top products by revenue"},
        )
        agent.run(task_event)

        step_events = [
            e for _, e in producer.published
            if e.event_type == EventType.AGENT_STEP_COMPLETED
        ]
        assert len(step_events) <= max_steps

    def test_run_publishes_agent_started_and_finished(self) -> None:
        """run() must publish exactly one AGENT_STARTED and one AGENT_FINISHED."""
        agent, producer, _ = self._make_agent()
        task_event = create_envelope(
            EventType.TASK_CREATED,
            {"task_id": "t-test-3", "description": "Search for climate data"},
        )
        agent.run(task_event)

        types = [e.event_type for _, e in producer.published]
        assert types.count(EventType.AGENT_STARTED) == 1
        assert types.count(EventType.AGENT_FINISHED) == 1

    def test_run_marks_event_as_processed(self) -> None:
        """After a successful run, the task event must be in the dedupe store."""
        agent, _, memory = self._make_agent()
        task_event = create_envelope(
            EventType.TASK_CREATED,
            {"task_id": "t-test-4", "description": "List all users"},
        )
        agent.run(task_event)

        assert memory.is_duplicate(task_event.idempotency_key)

    def test_run_skips_duplicate_event(self) -> None:
        """A task event whose idempotency key is already marked processed
        must be skipped and return a duplicate-flagged AGENT_FINISHED."""
        agent, producer, memory = self._make_agent()
        task_event = create_envelope(
            EventType.TASK_CREATED,
            {"task_id": "t-test-5", "description": "What is total revenue?"},
        )

        # First run — normal processing
        agent.run(task_event)
        published_count_after_first = len(producer.published)

        # Second run — must be a no-op
        result = agent.run(task_event)

        assert result.payload["answer"] == "duplicate — already processed"
        assert len(producer.published) == published_count_after_first  # nothing new published


class TestDuplicateDetection:
    def test_is_duplicate_returns_false_before_mark(self) -> None:
        memory = FakeMemory()
        assert memory.is_duplicate("some-key") is False

    def test_is_duplicate_returns_true_after_mark(self) -> None:
        memory = FakeMemory()
        memory.mark_processed("some-key", "evt-1")
        assert memory.is_duplicate("some-key") is True

    def test_mark_processed_is_idempotent(self) -> None:
        """Calling mark_processed twice with the same key must not raise."""
        memory = FakeMemory()
        memory.mark_processed("key-x", "evt-1")
        memory.mark_processed("key-x", "evt-1")  # must not raise
        assert memory.is_duplicate("key-x") is True
