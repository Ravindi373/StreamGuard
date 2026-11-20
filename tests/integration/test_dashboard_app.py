"""Renders the real Streamlit app headlessly against the database.

docker compose up -d
RUN_INTEGRATION=1 pytest -m integration tests/integration/test_dashboard_app.py -o addopts=""
"""

from __future__ import annotations

import os
import uuid
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(os.environ.get("RUN_INTEGRATION") != "1", reason="set RUN_INTEGRATION=1"),
]

APP = Path(__file__).resolve().parents[2] / "dashboard" / "app.py"


def _seed(dsn: str) -> None:
    """Write ~1 minute of fresh events (ending now) so the app has something to draw."""
    from streamguard.pipeline import Pipeline
    from streamguard.schemas import parse_event
    from streamguard.simulator import SensorSimulator, SimConfig
    from streamguard.storage import PostgresSink

    seed = uuid.uuid4().int % 2**31
    sim = SensorSimulator(SimConfig(seed=seed, sensors=2, fault_rate=0.02))
    t = datetime.now(UTC) - timedelta(seconds=60)
    events = []
    for _ in range(600):
        events.append(parse_event(sim.next_message(t).payload))
        t += timedelta(milliseconds=100)
    sink = PostgresSink(dsn)
    sink.connect()
    sink.write(Pipeline().process_batch(events))
    sink.close()


@pytest.fixture(autouse=True)
def _database_mode(monkeypatch):
    # Without POSTGRES_DSN the app would fall back to its in-process demo; these tests want the DB.
    monkeypatch.setenv("STREAMGUARD_DEMO", "0")


def _app():
    pytest.importorskip("streamlit")
    from streamlit.testing.v1 import AppTest

    return AppTest.from_file(str(APP), default_timeout=60)


def test_app_renders_even_when_the_database_is_empty():
    # Runs first (file order) so on a fresh CI database this really is the empty case;
    # on a used database it still checks the app never crashes.
    at = _app().run()
    assert not at.exception, [e.value for e in at.exception]
    assert not at.error, [e.value for e in at.error]


def test_app_renders_metrics_and_survives_interaction():
    from streamguard.config import load_settings

    _seed(load_settings().postgres_dsn)
    at = _app().run()
    assert not at.exception, [e.value for e in at.exception]
    assert not at.error, [e.value for e in at.error]
    assert {m.label for m in at.metric} >= {"Events in window", "Anomalies in window"}

    at.sidebar.selectbox[0].set_value("Last 5 minutes").run()
    assert not at.exception, [e.value for e in at.exception]
