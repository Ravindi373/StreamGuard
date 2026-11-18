"""Prometheus metrics exposed by the processor on :SG_METRICS_PORT/metrics."""

from __future__ import annotations

from prometheus_client import Counter, Gauge, Histogram, start_http_server

EVENTS = Counter(
    "streamguard_events",
    "Events handled by the processor, by outcome.",
    ["status"],  # ok | late | duplicate | invalid
)
ANOMALIES = Counter("streamguard_anomalies", "Anomalies detected.", ["detector"])
ALERTS = Counter("streamguard_alerts", "Alerts fired after cooldown.")
BATCH_SECONDS = Histogram(
    "streamguard_batch_seconds",
    "Wall time to process + persist one batch.",
    buckets=(0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10),
)
BATCH_SIZE = Histogram(
    "streamguard_batch_events",
    "Messages per consumed batch.",
    buckets=(1, 10, 50, 100, 250, 500, 1000),
)
LAG_SECONDS = Gauge(
    "streamguard_end_to_end_lag_seconds",
    "Now minus the newest event timestamp in the last batch.",
)
DB_RETRIES = Counter("streamguard_db_retries", "Database write retries.")


def start_metrics_server(port: int) -> None:
    start_http_server(port)
