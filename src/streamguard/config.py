"""Runtime configuration, read from environment variables."""

from __future__ import annotations

import os
from dataclasses import dataclass


def _f(name: str, default: float) -> float:
    return float(os.environ.get(name, default))


def _i(name: str, default: int) -> int:
    return int(os.environ.get(name, default))


@dataclass(frozen=True)
class Settings:
    # Infrastructure
    kafka_bootstrap: str = "localhost:19092"
    postgres_dsn: str = "postgresql://streamguard:streamguard@localhost:5432/streamguard"
    topic_raw: str = "events.raw"
    topic_dlq: str = "events.dlq"
    topic_partitions: int = 3
    consumer_group: str = "streamguard-processor"

    # Batching
    batch_size: int = 500
    batch_timeout_s: float = 1.0

    # Stream semantics
    window_seconds: int = 10
    allowed_lateness_s: float = 5.0
    dedup_capacity: int = 100_000

    # Detectors
    ewma_alpha: float = 0.05
    z_threshold: float = 4.0
    warmup: int = 30
    rolling_window: int = 60

    # Alerting
    alert_webhook_url: str = ""
    alert_cooldown_s: float = 60.0

    # Observability
    metrics_port: int = 8000
    log_level: str = "INFO"


def load_settings() -> Settings:
    """Build Settings from the environment (evaluated at call time)."""
    d = Settings()
    e = os.environ.get
    return Settings(
        kafka_bootstrap=e("KAFKA_BOOTSTRAP", d.kafka_bootstrap),
        postgres_dsn=e("POSTGRES_DSN", d.postgres_dsn),
        topic_raw=e("SG_TOPIC_RAW", d.topic_raw),
        topic_dlq=e("SG_TOPIC_DLQ", d.topic_dlq),
        topic_partitions=_i("SG_TOPIC_PARTITIONS", d.topic_partitions),
        consumer_group=e("SG_CONSUMER_GROUP", d.consumer_group),
        batch_size=_i("SG_BATCH_SIZE", d.batch_size),
        batch_timeout_s=_f("SG_BATCH_TIMEOUT_S", d.batch_timeout_s),
        window_seconds=_i("SG_WINDOW_SECONDS", d.window_seconds),
        allowed_lateness_s=_f("SG_ALLOWED_LATENESS_S", d.allowed_lateness_s),
        dedup_capacity=_i("SG_DEDUP_CAPACITY", d.dedup_capacity),
        ewma_alpha=_f("SG_EWMA_ALPHA", d.ewma_alpha),
        z_threshold=_f("SG_Z_THRESHOLD", d.z_threshold),
        warmup=_i("SG_WARMUP", d.warmup),
        rolling_window=_i("SG_ROLLING_WINDOW", d.rolling_window),
        alert_webhook_url=e("SG_ALERT_WEBHOOK_URL", d.alert_webhook_url),
        alert_cooldown_s=_f("SG_ALERT_COOLDOWN_S", d.alert_cooldown_s),
        metrics_port=_i("SG_METRICS_PORT", d.metrics_port),
        log_level=e("SG_LOG_LEVEL", d.log_level),
    )
