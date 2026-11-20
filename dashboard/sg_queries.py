"""Data access for the Streamlit dashboard: TimescaleDB queries and the Prometheus HTTP API.

Kept free of Streamlit imports so it can be unit-tested on its own.
"""

from __future__ import annotations

import json
import os
import urllib.error
import urllib.parse
import urllib.request
from datetime import UTC, datetime

import pandas as pd
import psycopg

DSN = os.environ.get(
    "POSTGRES_DSN", "postgresql://streamguard:streamguard@localhost:5432/streamguard"
)
PROMETHEUS_URL = os.environ.get("PROMETHEUS_URL", "http://localhost:9090")

RANGES = {
    "Last 5 minutes": 300,
    "Last 15 minutes": 900,
    "Last hour": 3600,
    "Last 6 hours": 21_600,
}

_BIN_ORIGIN = "TIMESTAMPTZ '2026-01-01 00:00:00+00'"


def bucket_seconds(range_s: int, max_points: int = 240) -> int:
    """Smallest 'nice' bucket that keeps a chart under `max_points` points."""
    for b in (1, 2, 5, 10, 15, 30, 60, 120, 300, 600):
        if range_s / b <= max_points:
            return b
    return 600


def query_df(sql: str, params: tuple | list = ()) -> pd.DataFrame:
    with psycopg.connect(DSN, connect_timeout=5) as conn, conn.cursor() as cur:
        cur.execute(sql, params)
        cols = [d.name for d in cur.description]
        return pd.DataFrame(cur.fetchall(), columns=cols)


# ------------------------------------------------------------------ database
def window_end(anchor_to_newest: bool) -> datetime:
    """End of the time window: 'now', or the newest stored event (for replayed data)."""
    if anchor_to_newest:
        newest = query_df("SELECT max(ts) AS newest FROM events")["newest"].iloc[0]
        if pd.notna(newest):
            return newest.to_pydatetime()
    return datetime.now(UTC)


def list_sources(hours: int = 6) -> list[str]:
    df = query_df(
        "SELECT DISTINCT source FROM events "
        "WHERE ts > now() - make_interval(hours => %s) ORDER BY 1",
        (hours,),
    )
    return df["source"].tolist()


def kpis(start: datetime, end: datetime, sources: list[str]) -> dict:
    row = query_df(
        """
        WITH ev AS (
          SELECT ts, is_late FROM events
          WHERE ts >= %(s)s AND ts < %(e)s AND source = ANY(%(src)s)
        )
        SELECT
          (SELECT count(*) FROM ev)                                   AS events,
          (SELECT count(*) FROM ev WHERE is_late)                     AS late,
          (SELECT count(*) FROM anomalies
             WHERE ts >= %(s)s AND ts < %(e)s AND source = ANY(%(src)s)) AS anomalies,
          (SELECT count(*) FROM ev WHERE ts >= %(e)s - interval '60 seconds') AS last_minute,
          (SELECT max(ts) FROM events)                                AS newest
        """,
        {"s": start, "e": end, "src": sources},
    ).iloc[0]
    newest = row["newest"]
    return {
        "events": int(row["events"]),
        "late": int(row["late"]),
        "anomalies": int(row["anomalies"]),
        "events_per_s": int(row["last_minute"]) / 60.0,
        "newest": None if pd.isna(newest) else newest.to_pydatetime(),
    }


def sensor_values(start, end, bucket_s: int, sources: list[str]) -> pd.DataFrame:
    return query_df(
        f"""
        SELECT date_bin(make_interval(secs => %(b)s::double precision), ts, {_BIN_ORIGIN}) AS time,
               source, avg(value) AS value
        FROM events
        WHERE ts >= %(s)s AND ts < %(e)s AND source = ANY(%(src)s)
        GROUP BY 1, 2 ORDER BY 1
        """,
        {"b": bucket_s, "s": start, "e": end, "src": sources},
    )


def anomaly_points(start, end, sources: list[str]) -> pd.DataFrame:
    return query_df(
        """
        SELECT ts AS time, source, detector, value, score
        FROM anomalies
        WHERE ts >= %s AND ts < %s AND source = ANY(%s)
        ORDER BY ts
        """,
        (start, end, sources),
    )


def anomalies_over_time(start, end, bin_s: int, sources: list[str]) -> pd.DataFrame:
    return query_df(
        f"""
        SELECT date_bin(make_interval(secs => %(b)s::double precision), ts, {_BIN_ORIGIN}) AS time,
               detector, count(*) AS count
        FROM anomalies
        WHERE ts >= %(s)s AND ts < %(e)s AND source = ANY(%(src)s)
        GROUP BY 1, 2 ORDER BY 1
        """,
        {"b": bin_s, "s": start, "e": end, "src": sources},
    )


def anomalies_by_source(start, end, sources: list[str]) -> pd.DataFrame:
    return query_df(
        """
        SELECT source, detector, count(*) AS count
        FROM anomalies
        WHERE ts >= %s AND ts < %s AND source = ANY(%s)
        GROUP BY 1, 2 ORDER BY 1
        """,
        (start, end, sources),
    )


def recent_anomalies(sources: list[str], limit: int = 100) -> pd.DataFrame:
    return query_df(
        """
        SELECT ts AS time, source, detector, round(value::numeric, 3)::float AS value,
               round(score::numeric, 2)::float AS score
        FROM anomalies WHERE source = ANY(%s) ORDER BY ts DESC LIMIT %s
        """,
        (sources, limit),
    )


def latest_windows() -> pd.DataFrame:
    return query_df(
        """
        SELECT DISTINCT ON (source) source, window_start, n,
               round(mean::numeric, 3)::float AS mean,
               round(std::numeric, 3)::float AS std,
               round(min_value::numeric, 3)::float AS min,
               round(max_value::numeric, 3)::float AS max
        FROM window_stats ORDER BY source, window_start DESC
        """
    )


# ---------------------------------------------------------------- prometheus
def _prom_get(path: str, params: dict) -> dict | None:
    url = f"{PROMETHEUS_URL.rstrip('/')}{path}?{urllib.parse.urlencode(params)}"
    try:
        with urllib.request.urlopen(url, timeout=3) as resp:  # noqa: S310 (operator-configured)
            body = json.loads(resp.read())
    except (urllib.error.URLError, TimeoutError, ValueError, OSError):
        return None
    return body.get("data") if body.get("status") == "success" else None


def prom_instant(expr: str) -> float | None:
    """First value of an instant query, or None if Prometheus is unreachable/empty."""
    data = _prom_get("/api/v1/query", {"query": expr})
    if not data or not data.get("result"):
        return None
    try:
        value = float(data["result"][0]["value"][1])
    except (KeyError, IndexError, ValueError):
        return None
    return None if value != value else value  # NaN -> None


def prom_range(expr: str, start: datetime, end: datetime, step_s: int, label: str) -> pd.DataFrame:
    """Range query -> long DataFrame [time, series, value]; empty if unavailable."""
    data = _prom_get(
        "/api/v1/query_range",
        {"query": expr, "start": start.timestamp(), "end": end.timestamp(), "step": step_s},
    )
    rows = []
    for series in (data or {}).get("result", []):
        name = series.get("metric", {}).get(label, "value")
        for ts, val in series.get("values", []):
            rows.append((datetime.fromtimestamp(float(ts), tz=UTC), name, float(val)))
    return pd.DataFrame(rows, columns=["time", "series", "value"])
