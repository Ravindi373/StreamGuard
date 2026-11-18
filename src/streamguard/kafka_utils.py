"""Small helpers around confluent-kafka (imported lazily so the core stays Kafka-free)."""

from __future__ import annotations

import logging
import time

log = logging.getLogger(__name__)


def ensure_topics(bootstrap: str, topics: dict[str, int], attempts: int = 30) -> None:
    """Wait for the broker, then create any missing topics (name -> partitions)."""
    from confluent_kafka import KafkaError, KafkaException
    from confluent_kafka.admin import AdminClient, NewTopic

    admin = AdminClient({"bootstrap.servers": bootstrap})
    metadata = None
    for attempt in range(1, attempts + 1):
        try:
            metadata = admin.list_topics(timeout=5)
            break
        except KafkaException as exc:
            log.info("waiting for broker at %s (%d/%d): %s", bootstrap, attempt, attempts, exc)
            time.sleep(2)
    if metadata is None:
        raise RuntimeError(f"Kafka broker at {bootstrap} not reachable")

    missing = [
        NewTopic(name, num_partitions=parts, replication_factor=1)
        for name, parts in topics.items()
        if name not in metadata.topics
    ]
    if not missing:
        return
    for name, fut in admin.create_topics(missing).items():
        try:
            fut.result()
            log.info("created topic %s", name)
        except KafkaException as exc:
            if exc.args[0].code() != KafkaError.TOPIC_ALREADY_EXISTS:
                raise


def make_producer(bootstrap: str):
    from confluent_kafka import Producer

    return Producer(
        {
            "bootstrap.servers": bootstrap,
            "enable.idempotence": True,
            "linger.ms": 20,
            "compression.type": "lz4",
            "queue.buffering.max.messages": 500_000,
        }
    )


def produce(producer, topic: str, key, value: bytes, on_delivery=None) -> None:
    """produce() with back-pressure: if the local queue is full, wait for deliveries."""
    while True:
        try:
            producer.produce(topic, value=value, key=key, on_delivery=on_delivery)
            return
        except BufferError:
            producer.poll(0.1)
