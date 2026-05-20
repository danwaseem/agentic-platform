"""Smoke test: run the ReactAgent locally against live Kafka + MongoDB."""

import logging
import uuid
logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")

from agent.event_types import EventType, create_envelope
from agent.kafka_producer import KafkaEventProducer
from agent.memory import AgentMemory
from agent.react_agent import ReactAgent

# Unique run_id makes payloads distinct on every execution so the
# idempotency guard doesn't trigger from a previous run.
RUN_ID = uuid.uuid4().hex[:8]

TASKS = [
    {"task_id": f"t-001-{RUN_ID}", "description": "Show me revenue from orders last month"},
    {"task_id": f"t-002-{RUN_ID}", "description": "Search for documents about climate change"},
    {"task_id": f"t-003-{RUN_ID}", "description": "What is the capital of France?"},
]

with KafkaEventProducer() as producer:
    memory = AgentMemory()
    agent = ReactAgent(producer=producer, memory=memory)

    for task_payload in TASKS:
        print(f"\n{'='*60}")
        print(f"TASK: {task_payload['description']}")
        task_event = create_envelope(EventType.TASK_CREATED, task_payload)
        result = agent.run(task_event)
        print(f"ANSWER: {result.payload['answer']}")
        print(f"STEPS:  {result.payload['steps']}")

    # Idempotency check: re-run the first task — should be skipped
    print(f"\n{'='*60}")
    print("Re-running task t-001 (should be detected as duplicate)")
    task_event = create_envelope(EventType.TASK_CREATED, TASKS[0])
    # Manually set same idempotency key to simulate duplicate delivery
    from agent.event_types import build_idempotency_key
    task_event.idempotency_key = build_idempotency_key(EventType.TASK_CREATED, TASKS[0])
    result = agent.run(task_event)
    print(f"ANSWER: {result.payload['answer']}")

    memory.close()

print("\nSMOKE TEST PASSED")
