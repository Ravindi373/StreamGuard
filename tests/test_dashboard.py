"""Tests for the dashboard's data layer (no Streamlit, no database needed)."""

from __future__ import annotations

import json
import threading
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, HTTPServer

import pytest

pytest.importorskip("pandas")
import sg_queries as q  # noqa: E402  (dashboard/ is on sys.path via pyproject)


def test_bucket_seconds_keeps_charts_light():
    assert q.bucket_seconds(300) == 2
    assert q.bucket_seconds(900) == 5
    assert q.bucket_seconds(3600) == 15
    assert q.bucket_seconds(21_600) == 120
    assert all(q.RANGES[r] / q.bucket_seconds(q.RANGES[r]) <= 240 for r in q.RANGES)


class _FakeProm(BaseHTTPRequestHandler):
    def do_GET(self):  # noqa: N802
        if "query_range" in self.path:
            result = [
                {
                    "metric": {"status": "late"},
                    "values": [[1_767_225_600, "0.5"], [1_767_225_605, "0.7"]],
                },
                {"metric": {"status": "invalid"}, "values": [[1_767_225_600, "0.1"]]},
            ]
        elif "boom" in self.path:
            self.send_response(500)
            self.end_headers()
            return
        elif "nan" in self.path:
            result = [{"metric": {}, "value": [1, "NaN"]}]
        elif "empty" in self.path:
            result = []
        else:
            result = [{"metric": {}, "value": [1_767_225_600, "42.5"]}]
        body = json.dumps({"status": "success", "data": {"result": result}}).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, *_):
        pass


@pytest.fixture
def fake_prom(monkeypatch):
    server = HTTPServer(("127.0.0.1", 0), _FakeProm)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    monkeypatch.setattr(q, "PROMETHEUS_URL", f"http://127.0.0.1:{server.server_port}")
    yield
    server.shutdown()


def test_prom_instant_parses_values(fake_prom):
    assert q.prom_instant("up") == 42.5
    assert q.prom_instant("empty_metric") is None
    assert q.prom_instant("nan_metric") is None  # NaN means 'no data yet'
    assert q.prom_instant("boom") is None  # HTTP 500 must not raise


def test_prom_range_builds_long_dataframe(fake_prom):
    now = datetime(2026, 1, 1, tzinfo=timezone.utc)
    df = q.prom_range("x_query_range", now, now, 5, "status")
    assert list(df.columns) == ["time", "series", "value"]
    assert sorted(df["series"].unique()) == ["invalid", "late"]
    assert len(df) == 3


def test_prometheus_unreachable_returns_none_and_empty(monkeypatch):
    monkeypatch.setattr(q, "PROMETHEUS_URL", "http://127.0.0.1:9")  # nothing listens here
    now = datetime(2026, 1, 1, tzinfo=timezone.utc)
    assert q.prom_instant("up") is None
    assert q.prom_range("up", now, now, 5, "x").empty
