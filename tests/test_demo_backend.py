"""Tests for the in-process demo backend (no database, no Kafka, no network)."""

from __future__ import annotations

import time
from datetime import UTC, datetime, timedelta

import pytest

pytest.importorskip("pandas")
import demo_backend as d  # noqa: E402  (dashboard/ is on sys.path via pyproject)


def _engine(**kw) -> d.DemoEngine:
    kw.setdefault("rate", 20.0)
    kw.setdefault("fault_rate", 0.05)
    kw.setdefault("seed", 7)
    return d.DemoEngine(**kw)


def _window(seconds: int):
    end = datetime.now(UTC) + timedelta(seconds=1)
    return end - timedelta(seconds=seconds), end


def test_warmup_fills_history_with_all_sources_and_some_anomalies():
    e = _engine()
    e.warmup(300)
    assert e.list_sources() == [f"sensor-0{i}" for i in range(5)]
    start, end = _window(600)
    k = e.kpis(start, end, e.list_sources())
    assert k["events"] > 5000 and k["anomalies"] > 0 and k["newest"] is not None
    assert not e.anomalies_by_source(start, end, e.list_sources()).empty


def test_messy_input_is_counted_not_crashed_on():
    e = _engine()
    e.warmup(600)
    c = e.counts
    assert c["invalid"] > 0 and c["duplicate"] > 0 and c["late"] > 0
    assert c["ok"] > 10 * (c["invalid"] + c["duplicate"] + c["late"]) / 4


def test_query_frames_have_the_shape_the_dashboard_expects():
    e = _engine()
    e.warmup(300)
    start, end = _window(600)
    src = e.list_sources()
    assert list(e.sensor_values(start, end, 5, src).columns) == ["time", "source", "value"]
    assert list(e.anomaly_points(start, end, src).columns) == [
        "time",
        "source",
        "detector",
        "value",
        "score",
    ]
    assert list(e.anomalies_over_time(start, end, 30, src).columns) == ["time", "detector", "count"]
    assert list(e.recent_anomalies(src, 10).columns) == [
        "time",
        "source",
        "detector",
        "value",
        "score",
    ]
    assert len(e.recent_anomalies(src, 10)) <= 10
    rr = e.reject_rates(start, end, 10)
    assert set(rr["series"]) == {"late", "duplicate", "invalid"}
    assert not e.latest_windows().empty


def test_source_filter_and_empty_selection():
    e = _engine()
    e.warmup(120)
    start, end = _window(300)
    one = e.sensor_values(start, end, 5, ["sensor-01"])
    assert set(one["source"]) == {"sensor-01"}
    assert e.sensor_values(start, end, 5, []).empty
    assert e.kpis(start, end, [])["events"] == 0


def test_old_data_is_trimmed_to_the_retention_window():
    e = _engine(retention_s=60)
    e.warmup(300)
    assert e._events[0][0] >= e._newest - 60 - 1
    assert len(e._events) < 20 * 61


def test_background_thread_keeps_the_stream_flowing_and_stops_cleanly():
    e = _engine(tick_s=0.1)
    e.warmup(30)
    before = e.counts["ok"]
    e.start()
    time.sleep(1.0)
    e.stop()
    assert e.counts["ok"] > before
    assert e.last_error is None
    assert not e._thread.is_alive()
    h = e.health()
    assert h["p95_batch_ms"] is not None and h["uptime_s"] > 0
