"""Kafka consumer loop: consume -> validate -> process -> persist -> commit -> alert.

Delivery semantics: offsets are committed only *after* the database transaction
succeeds (at-least-once). Because every write is idempotent, a crash between the DB
commit and the offset commit just causes harmless re-processing, so the sink ends up
with exactly-once *effects*.
"""

from __future__ import annotations

import logging
import signal
import time
from datetime import datetime, timezone

from pydantic import ValidationError

from streamguard import metrics
from streamguard.alerts import AlertManager
from streamguard.config import Settings
from streamguard.kafka_utils import ensure_topics, make_producer
from streamguard.pipeline import Pipeline
from streamguard.schemas import dlq_record, parse_event
from streamguard.storage import PostgresSink

log = logging.getLogger(__name__)


class _Stop:
    requested = False


def _install_signal_handlers() -> None:
    def handler(signum, _frame):
        log.info("signal %s received, finishing current batch then exiting", signum)
        _Stop.requested = True

    for sig in (signal.SIGINT, signal.SIGTERM):
        signal.signal(sig, handler)


def run(settings: Settings) -> None:
    from confluent_kafka import Consumer, KafkaError

    _install_signal_handlers()
    metrics.start_metrics_server(settings.metrics_port)
    ensure_topics(
        settings.kafka_bootstrap,
        {settings.topic_raw: settings.topic_partitions, settings.topic_dlq: 1},
    )

    sink = PostgresSink(settings.postgres_dsn)
    sink.connect()
    pipeline = Pipeline.from_settings(settings)
    alerts = AlertManager(settings.alert_webhook_url, settings.alert_cooldown_s)
    dlq = make_producer(settings.kafka_bootstrap)
    consumer = Consumer(
        {
            "bootstrap.servers": settings.kafka_bootstrap,
            "group.id": settings.consumer_group,
            "enable.auto.commit": False,
            "auto.offset.reset": "earliest",
            "max.poll.interval.ms": 300_000,
        }
    )
    consumer.subscribe([settings.topic_raw])
    log.info("processor started: topic=%s group=%s", settings.topic_raw, settings.consumer_group)

    total = 0
    last_report = time.monotonic()
    try:
        while not _Stop.requested:
            msgs = consumer.consume(
                num_messages=settings.batch_size, timeout=settings.batch_timeout_s
            )
            if not msgs:
                continue

            started = time.perf_counter()
            events, invalid = [], 0
            for msg in msgs:
                if msg.error():
                    if msg.error().code() != KafkaError._PARTITION_EOF:
                        log.warning("consumer error: %s", msg.error())
                    continue
                try:
                    events.append(parse_event(msg.value()))
                except ValidationError as exc:
                    invalid += 1
                    first = exc.errors()[0] if exc.errors() else {"msg": str(exc)}
                    dlq.produce(
                        settings.topic_dlq,
                        key=msg.key(),
                        value=dlq_record(msg.value(), f"{first.get('loc')}: {first.get('msg')}"),
                    )

            out = pipeline.process_batch(events)
            sink.write(out)
            if invalid and dlq.flush(10) > 0:
                raise RuntimeError("dead-letter messages not delivered; refusing to commit")
            consumer.commit(asynchronous=False)

            # ---- post-commit: metrics + alerts (failures here never cause data loss)
            ok = len(events) - out.late - out.duplicates
            metrics.EVENTS.labels("ok").inc(ok)
            metrics.EVENTS.labels("late").inc(out.late)
            metrics.EVENTS.labels("duplicate").inc(out.duplicates)
            metrics.EVENTS.labels("invalid").inc(invalid)
            metrics.BATCH_SIZE.observe(len(msgs))
            metrics.BATCH_SECONDS.observe(time.perf_counter() - started)
            if out.newest_event_ts is not None:
                lag = (datetime.now(timezone.utc) - out.newest_event_ts).total_seconds()
                metrics.LAG_SECONDS.set(max(lag, 0.0))
            for a in out.anomalies:
                metrics.ANOMALIES.labels(a.detector).inc()
                if alerts.maybe_alert(a):
                    metrics.ALERTS.inc()

            total += len(msgs)
            if time.monotonic() - last_report > 10:
                log.info("processed=%d watermark=%s", total, pipeline.watermark)
                last_report = time.monotonic()
    finally:
        log.info("shutting down (processed %d messages)", total)
        consumer.close()
        dlq.flush(5)
        alerts.close()
        sink.close()
