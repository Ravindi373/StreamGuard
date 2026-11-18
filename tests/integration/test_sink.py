"""Sink idempotency against a real database.

docker compose up -d timescaledb
RUN_INTEGRATION=1 pytest -m integration tests/integration/test_sink.py
"""

from __future__ import annotations

import os
import uuid
from dataclasses import replace
from datetime import datetime, timedelta, timezone

import pytest

from streamguard.pipeline import BatchOutput, Pipeline
from streamguard.schemas import WindowRow, parse_event
from streamguard.simulator import SensorSimulator, SimConfig

pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(os.environ.get("RUN_INTEGRATION") != "1", reason="set RUN_INTEGRATION=1"),
]


@pytest.fixture
def dsn():
    from streamguard.config import load_settings

    return load_settings().postgres_dsn


def test_replaying_batches_does_not_change_stored_data(dsn):
    import psycopg

    from streamguard.storage import PostgresSink

    # Unique seed => unique run_id, so rows left by earlier runs never collide with this test.
    seed = uuid.uuid4().int % 2**31
    sim = SensorSimulator(SimConfig(seed=seed, sensors=3, fault_rate=0.01, dup_prob=0.02))
    t = datetime.now(timezone.utc) - timedelta(minutes=5)
    msgs = []
    for _ in range(3000):
        msgs.append(sim.next_message(t))
        t += timedelta(milliseconds=20)

    pipe, sink = Pipeline(), PostgresSink(dsn)
    sink.connect()
    batches = []
    for i in range(0, len(msgs), 500):
        out = pipe.process_batch([parse_event(m.payload) for m in msgs[i : i + 500]])
        batches.append(out)
        sink.write(out)

    pattern = f"%:{sim.run_id}:%"

    def counts():
        with psycopg.connect(dsn) as c:
            return (
                c.execute(
                    "SELECT count(*) FROM events WHERE event_id LIKE %s", (pattern,)
                ).fetchone()[0],
                c.execute(
                    "SELECT count(*) FROM anomalies WHERE event_id LIKE %s", (pattern,)
                ).fetchone()[0],
            )

    first = counts()
    assert first[0] == len({m.event_id for m in msgs})
    for out in batches:  # at-least-once redelivery of everything
        sink.write(out)
    assert counts() == first
    sink.close()


def test_partial_window_never_overwrites_a_complete_one(dsn):
    import psycopg

    from streamguard.storage import PostgresSink

    source = f"it-window-{uuid.uuid4().hex[:8]}"
    start = datetime(2026, 1, 1, tzinfo=timezone.utc)
    full = WindowRow(source, start, start + timedelta(seconds=10), 10, 5.0, 1.0, 3.0, 7.0)
    sink = PostgresSink(dsn)
    sink.connect()
    sink.write(BatchOutput(windows=[full]))
    sink.write(BatchOutput(windows=[replace(full, n=2, mean=-1.0)]))  # partial, post-restart
    with psycopg.connect(dsn) as c:
        n, mean = c.execute(
            "SELECT n, mean FROM window_stats WHERE source = %s", (source,)
        ).fetchone()
    assert (n, mean) == (10, 5.0)
    sink.close()
