"""Run the real Streamlit app in demo mode (needs no services)."""

from __future__ import annotations

from pathlib import Path

import pytest

pytest.importorskip("streamlit")
from streamlit.testing.v1 import AppTest  # noqa: E402

APP = Path(__file__).resolve().parents[1] / "dashboard" / "app.py"


def test_demo_app_renders_and_responds_to_controls(monkeypatch):
    monkeypatch.setenv("STREAMGUARD_DEMO", "1")
    at = AppTest.from_file(str(APP), default_timeout=90).run()
    assert not at.exception, [e.value for e in at.exception]
    assert not at.error, [e.value for e in at.error]
    labels = {m.label for m in at.metric}
    assert {"Events in window", "Anomalies in window", "Uptime"} <= labels
    assert [t.label for t in at.tabs] == [
        "Overview",
        "Anomalies",
        "Pipeline health",
    ]  # no Grafana tab

    at.sidebar.selectbox[0].set_value("Last 5 minutes").run()
    assert not at.exception, [e.value for e in at.exception]
    at.sidebar.multiselect[0].set_value([]).run()
    assert "Select at least one source in the sidebar." in [i.value for i in at.info]


def test_demo_is_chosen_automatically_when_no_database_is_configured(monkeypatch):
    monkeypatch.delenv("STREAMGUARD_DEMO", raising=False)
    monkeypatch.delenv("POSTGRES_DSN", raising=False)
    at = AppTest.from_file(str(APP), default_timeout=90).run()
    assert not at.exception, [e.value for e in at.exception]
    assert any("Live demo" in i.value for i in at.sidebar.info)
