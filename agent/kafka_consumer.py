"""Kafka consumer that deserialises messages into EventEnvelope objects."""

import json
import logging
from typing import Callable

from kafka import KafkaConsumer
from kafka.errors import KafkaError
from pydantic import ValidationError

from agent.event_types import EventEnvelope

logger = logging.getLogger(__name__)

_DEFAULT_BOOTSTRAP = "localhost:9095"


class KafkaEventConsumer:
    """Poll a set of Kafka topics and dispatch EventEnvelope objects to a handler.

    Offsets are committed only after the handler returns successfully, giving
    at-least-once delivery semantics.

    Args:
        topics: List of topic names to subscribe to.
        group_id: Kafka consumer group identifier.
        bootstrap_servers: Kafka broker address(es).
    """

    def __init__(
        self,
        topics: list[str],
        group_id: str,
        bootstrap_servers: str = _DEFAULT_BOOTSTRAP,
    ) -> None:
        self._topics = topics
        self._consumer = KafkaConsumer(
            *topics,
            bootstrap_servers=bootstrap_servers,
            group_id=group_id,
            auto_offset_reset="earliest",
            enable_auto_commit=False,  # manual commit after handler success
            value_deserializer=lambda b: json.loads(b.decode("utf-8")),
        )
        logger.info(
            "KafkaEventConsumer subscribed to %s (group=%s)", topics, group_id
        )

    def consume(self, handler: Callable[[EventEnvelope], None]) -> None:
        """Poll indefinitely, deserialise each message, and call handler.

        The consumer commits the offset only after handler returns without
        raising an exception. Deserialization errors and handler exceptions are
        logged and skipped so the loop never crashes.

        Args:
            handler: Callable that receives a validated EventEnvelope.
        """
        logger.info("Starting consume loop")
        try:
            for message in self._consumer:
                try:
                    envelope = EventEnvelope.model_validate(message.value)
                except (ValidationError, KeyError, TypeError) as exc:
                    logger.error(
                        "Failed to deserialise message offset=%s partition=%s: %s",
                        message.offset,
                        message.partition,
                        exc,
                    )
                    continue

                try:
                    handler(envelope)
                    self._consumer.commit()
                    logger.debug(
                        "Handled and committed event_type=%s event_id=%s",
                        envelope.event_type.value,
                        envelope.event_id,
                    )
                except Exception as exc:  # noqa: BLE001
                    logger.error(
                        "Handler raised for event_type=%s event_id=%s: %s",
                        envelope.event_type.value,
                        envelope.event_id,
                        exc,
                    )
                    # offset intentionally not committed — message will be redelivered

        except KafkaError as exc:
            logger.error("KafkaError in consume loop: %s", exc)
            raise
        finally:
            self.close()

    def close(self) -> None:
        """Close the underlying Kafka consumer."""
        self._consumer.close()
        logger.info("KafkaEventConsumer closed")
