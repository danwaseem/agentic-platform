from agent.event_types import EventType, create_envelope
from agent.kafka_producer import KafkaEventProducer

envelope = create_envelope(EventType.TASK_CREATED, {"task_id": "smoke-001"})
with KafkaEventProducer() as p:
    p.publish("agent-events", envelope)
print("PASSED:", envelope.event_id)
