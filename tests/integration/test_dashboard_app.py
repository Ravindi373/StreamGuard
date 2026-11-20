"""Renders the real Streamlit app headlessly against the database.

docker compose up -d
RUN_INTEGRATION=1 pytest -m integration tests/integration/test_dashboard_app.py -o addopts=""
"""

from __future__ import annotations

import os
from pathlib import Path

import pytest

pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(os.environ.get("RUN_INTEGRATION") != "1", reason="set RUN_INTEGRATION=1"),
]

APP = Path(__file__).resolve().parents[2] / "dashboard" / "app.py"


def test_app_renders_without_errors():
    pytest.importorskip("streamlit")
    from streamlit.testing.v1 import AppTest

    at = AppTest.from_file(str(APP), default_timeout=60).run()
    assert not at.exception, [e.value for e in at.exception]
    assert not at.error, [e.value for e in at.error]
    assert {m.label for m in at.metric} >= {"Events in window", "Anomalies in window"}

    at.sidebar.selectbox[0].set_value("Last 5 minutes").run()
    assert not at.exception
