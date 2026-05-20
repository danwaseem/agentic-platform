"""Kafka producer wrapper that publishes EventEnvelope messages."""

import json
import logging
from typing import Any

from kafka import KafkaProducer
from kafka.errors import KafkaError

from agent.event_types import EventEnvelope

logger = logging.getLogger(__name__)

_DEFAULT_BOOTSTRAP = "localhost:9095"


class KafkaEventProducer:
    """Thin wrapper around KafkaProducer for publishing EventEnvelope objects.

    Args:
        bootstrap_servers: Kafka broker address(es).
    """

    def __init__(self, bootstrap_servers: str = _DEFAULT_BOOTSTRAP) -> None:
        self._producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            key_serializer=lambda k: k.encode("utf-8") if k else None,
            acks="all",
            retries=3,
        )
        logger.info("KafkaEventProducer connected to %s", bootstrap_servers)

    def publish(self, topic: str, envelope: EventEnvelope) -> None:
        """Serialise and publish an EventEnvelope to a Kafka topic.

        The idempotency_key is used as the Kafka message key so that messages
        with identical content are routed to the same partition and can be
        deduplicated by consumers.

        Args:
            topic: Target Kafka topic name.
            envelope: The EventEnvelope to publish.
        """
        payload: dict[str, Any] = json.loads(envelope.model_dump_json())

        try:
            future = self._producer.send(
                topic,
                value=payload,
                key=envelope.idempotency_key,
            )
            future.get(timeout=10)  # block to surface send errors immediately
            logger.info(
                "Published event_type=%s event_id=%s topic=%s",
                envelope.event_type.value,
                envelope.event_id,
                topic,
            )
        except KafkaError as exc:
            logger.error(
                "Failed to publish event_type=%s event_id=%s topic=%s: %s",
                envelope.event_type.value,
                envelope.event_id,
                topic,
                exc,
            )
            raise

    def close(self) -> None:
        """Flush pending messages and close the producer."""
        self._producer.flush()
        self._producer.close()
        logger.info("KafkaEventProducer closed")

    def __enter__(self) -> "KafkaEventProducer":
        return self

    def __exit__(self, *_: Any) -> None:
        self.close()
