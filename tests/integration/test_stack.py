"""End-to-end test against the running docker compose stack.

    docker compose up -d --build
    RUN_INTEGRATION=1 pytest -m integration

It publishes a stable signal followed by one obvious spike, then waits for the
processor to persist the events and flag the spike.
"""

from __future__ import annotations

import json
import os
import time
import uuid
from datetime import UTC, datetime, timedelta

import pytest

pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(os.environ.get("RUN_INTEGRATION") != "1", reason="set RUN_INTEGRATION=1"),
]


def test_spike_flows_through_the_whole_pipeline():
    import psycopg

    from streamguard.config import load_settings
    from streamguard.kafka_utils import ensure_topics, make_producer

    s = load_settings()
    ensure_topics(s.kafka_bootstrap, {s.topic_raw: s.topic_partitions, s.topic_dlq: 1})
    source = f"it-{uuid.uuid4().hex[:8]}"
    producer = make_producer(s.kafka_bootstrap)
    t0 = datetime.now(UTC) - timedelta(seconds=120)

    def send(i: int, value: float) -> str:
        eid = f"{source}:{i}"
        body = {
            "event_id": eid,
            "source": source,
            "value": value,
            "ts": (t0 + timedelta(seconds=i)).isoformat(),
        }
        producer.produce(s.topic_raw, key=source, value=json.dumps(body).encode())
        return eid

    for i in range(120):
        send(i, 20.0 + (i % 5) * 0.1)
    spike_id = send(121, 500.0)
    producer.flush(15)

    deadline = time.time() + 60
    with psycopg.connect(s.postgres_dsn) as conn:
        while time.time() < deadline:
            conn.rollback()
            rows = conn.execute(
                "SELECT detector FROM anomalies WHERE event_id = %s", (spike_id,)
            ).fetchall()
            if rows:
                break
            time.sleep(2)
        stored = conn.execute(
            "SELECT count(*) FROM events WHERE source = %s", (source,)
        ).fetchone()[0]

    assert rows, "spike was not flagged within 60s"
    assert stored == 121
