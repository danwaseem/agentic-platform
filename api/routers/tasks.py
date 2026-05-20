"""FastAPI router: task creation and trace retrieval."""

import logging
import uuid
from datetime import datetime, timezone
from typing import Any

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field

from agent.event_types import EventType, create_envelope
from agent.kafka_producer import KafkaEventProducer
from db.mongo_client import get_mongo_client, get_tasks_collection

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/tasks", tags=["tasks"])


# ------------------------------------------------------------------ #
# Request / response models
# ------------------------------------------------------------------ #

class TaskCreateRequest(BaseModel):
    description: str
    priority: int = Field(default=3, ge=1, le=5)


class TaskCreateResponse(BaseModel):
    task_id: str
    status: str
    event_id: str
    priority: int


class TraceStep(BaseModel):
    event_id: str
    event_type: str
    step: int
    input_data: dict[str, Any]
    output_data: dict[str, Any]
    timestamp: datetime


class TaskDocument(BaseModel):
    task_id: str
    description: str
    priority: int
    status: str
    created_at: datetime
    steps: list[TraceStep] = []


class TaskTracesResponse(BaseModel):
    task_id: str
    steps: list[TraceStep]


# ------------------------------------------------------------------ #
# Endpoints
# ------------------------------------------------------------------ #

@router.post("/create", response_model=TaskCreateResponse, status_code=201)
def create_task(request: TaskCreateRequest) -> TaskCreateResponse:
    """Create a new task, publish a TASK_CREATED event to Kafka.

    Generates a UUID task_id and wraps it in an EventEnvelope before
    publishing. If Kafka is unreachable the task_id is still returned
    with status='kafka_unavailable' so the caller is not left empty-handed.

    Args:
        request: Task description and priority (1-5).

    Returns:
        task_id, status, originating event_id, and priority.
    """
    task_id = str(uuid.uuid4())
    payload = {
        "task_id": task_id,
        "description": request.description,
        "priority": request.priority,
        "created_at": datetime.now(tz=timezone.utc).isoformat(),
    }
    envelope = create_envelope(EventType.TASK_CREATED, payload)

    # Store task document immediately so GET /tasks/{task_id} works right away.
    # Agent traces are appended to a separate 'traces' collection as the
    # ReactAgent processes the task.
    created_at = datetime.now(tz=timezone.utc)
    tasks_col = get_mongo_client()["agent_memory"]["tasks"]
    tasks_col.update_one(
        {"task_id": task_id},
        {"$setOnInsert": {
            "task_id": task_id,
            "description": request.description,
            "priority": request.priority,
            "status": "queued",
            "created_at": created_at,
            "event_id": envelope.event_id,
        }},
        upsert=True,
    )

    try:
        with KafkaEventProducer() as producer:
            producer.publish("agent-tasks", envelope)
        status = "queued"
        logger.info("Task queued task_id=%s event_id=%s", task_id, envelope.event_id)
    except Exception as exc:
        logger.error("Kafka publish failed for task_id=%s: %s", task_id, exc)
        status = "kafka_unavailable"
        tasks_col.update_one({"task_id": task_id}, {"$set": {"status": status}})

    return TaskCreateResponse(
        task_id=task_id,
        status=status,
        event_id=envelope.event_id,
        priority=request.priority,
    )


@router.get("/{task_id}", response_model=TaskDocument)
def get_task(task_id: str) -> TaskDocument:
    """Return a task and all its agent trace steps.

    Args:
        task_id: UUID of the task to look up.

    Returns:
        Task metadata plus any trace steps recorded by the ReactAgent.

    Raises:
        HTTPException 404: If no task document exists for task_id.
    """
    db = get_mongo_client()["agent_memory"]

    task_doc = db["tasks"].find_one({"task_id": task_id}, {"_id": 0})
    if not task_doc:
        raise HTTPException(status_code=404, detail=f"Task not found: {task_id}")

    trace_docs = list(db["traces"].find({"task_id": task_id}, {"_id": 0}).sort("step", 1))
    steps = [
        TraceStep(
            event_id=d.get("event_id", ""),
            event_type=d.get("event_type", ""),
            step=d.get("step", 0),
            input_data=d.get("input_data", {}),
            output_data=d.get("output_data", {}),
            timestamp=d.get("timestamp", datetime.now(tz=timezone.utc)),
        )
        for d in trace_docs
    ]
    logger.info("Fetched task_id=%s with %d trace step(s)", task_id, len(steps))
    return TaskDocument(
        task_id=task_id,
        description=task_doc.get("description", ""),
        priority=task_doc.get("priority", 3),
        status=task_doc.get("status", "queued"),
        created_at=task_doc.get("created_at", datetime.now(tz=timezone.utc)),
        steps=steps,
    )
