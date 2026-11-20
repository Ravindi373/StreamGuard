"""In-process demo backend: no Kafka, no database.

A background thread generates a simulated sensor stream and pushes it through the *same*
`streamguard.pipeline.Pipeline` the real processor uses (validation, de-duplication, lateness,
windows, detectors). Results are kept in memory for a short retention period, and this module
exposes the same query functions as `sg_queries`, so the dashboard code is identical in both modes.
"""

from __future__ import annotations

import logging
import os
import sys
import threading
import time
from collections import deque
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pandas as pd
from sg_common import bucket_seconds  # noqa: F401  (re-exported for the app)

log = logging.getLogger(__name__)

DB_ERRORS: tuple[type[BaseException], ...] = ()  # nothing database-related can fail here

RANGES = {"Last 5 minutes": 300, "Last 15 minutes": 900, "Last 30 minutes": 1800}

_EVENT_COLS = ["ts", "source", "value", "is_late"]
_ANOM_COLS = ["ts", "source", "detector", "value", "score"]


def _ensure_src_on_path() -> None:
    """Make `import streamguard` work when running straight from a repo checkout."""
    src = Path(__file__).resolve().parents[1] / "src"
    if src.is_dir() and str(src) not in sys.path:
        sys.path.insert(0, str(src))


def _to_time(epoch: pd.Series) -> pd.Series:
    return pd.to_datetime(epoch, unit="s", utc=True)


class DemoEngine:
    def __init__(
        self,
        rate: float = 10.0,
        sensors: int = 5,
        retention_s: int = 3600,
        fault_rate: float = 0.004,
        seed: int | None = None,
        tick_s: float = 0.5,
    ) -> None:
        _ensure_src_on_path()
        from streamguard.pipeline import Pipeline
        from streamguard.simulator import SensorSimulator, SimConfig

        self.rate = rate
        self.retention_s = retention_s
        self.tick_s = tick_s
        self._sim = SensorSimulator(
            SimConfig(
                sensors=sensors,
                fault_rate=fault_rate,
                late_prob=0.01,
                dup_prob=0.005,
                malformed_prob=0.005,
                seed=seed,
            )
        )
        self._pipe = Pipeline()
        self._lock = threading.Lock()
        self._events: deque[tuple] = deque()
        self._anomalies: deque[tuple] = deque()
        self._status_log: deque[tuple] = deque()  # (epoch, late, duplicate, invalid)
        self._windows: dict[str, object] = {}
        self._sources: set[str] = set()
        self._batch_ms: deque[float] = deque(maxlen=600)
        self._newest = 0.0
        self.counts = {"ok": 0, "late": 0, "duplicate": 0, "invalid": 0}
        self.started_at = time.time()
        self.last_error: str | None = None
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None

    # ------------------------------------------------------------------ ingest
    def ingest(self, count: int, end: datetime) -> None:
        """Generate `count` events ending at `end`, run them through the pipeline, store results."""
        from pydantic import ValidationError

        from streamguard.schemas import parse_event

        step = 1.0 / self.rate
        events, invalid = [], 0
        for i in range(count):
            ts = end - timedelta(seconds=(count - 1 - i) * step)
            msg = self._sim.next_message(ts)
            try:
                events.append(parse_event(msg.payload))
            except ValidationError:
                invalid += 1

        t0 = time.perf_counter()
        out = self._pipe.process_batch(events)
        elapsed_ms = (time.perf_counter() - t0) * 1000

        with self._lock:
            for e in out.events:
                self._events.append((e.ts.timestamp(), e.source, e.value, e.is_late))
                self._sources.add(e.source)
            for a in out.anomalies:
                self._anomalies.append((a.ts.timestamp(), a.source, a.detector, a.value, a.score))
            for w in out.windows:
                self._windows[w.source] = w
            ok = len(events) - out.late - out.duplicates
            self.counts["ok"] += ok
            self.counts["late"] += out.late
            self.counts["duplicate"] += out.duplicates
            self.counts["invalid"] += invalid
            self._status_log.append((end.timestamp(), out.late, out.duplicates, invalid))
            self._batch_ms.append(elapsed_ms)
            self._newest = max(self._newest, end.timestamp())
            cutoff = end.timestamp() - self.retention_s
            for dq in (self._events, self._anomalies, self._status_log):
                while dq and dq[0][0] < cutoff:
                    dq.popleft()

    def warmup(self, seconds: float = 900.0, chunk: int = 500) -> None:
        """Back-fill recent history instantly so visitors never land on an empty dashboard."""
        total = int(seconds * self.rate)
        begin = datetime.now(UTC) - timedelta(seconds=seconds)
        done = 0
        while done < total:
            c = min(chunk, total - done)
            done += c
            self.ingest(c, begin + timedelta(seconds=done / self.rate))

    # ------------------------------------------------------------------ thread
    def start(self) -> None:
        if self._thread and self._thread.is_alive():
            return
        self._stop.clear()
        self._thread = threading.Thread(target=self._run, name="sg-demo", daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        if self._thread:
            self._thread.join(timeout=5)

    def _run(self) -> None:
        carry, last = 0.0, time.monotonic()
        while not self._stop.wait(self.tick_s):
            try:
                now_m = time.monotonic()
                carry += self.rate * (now_m - last)
                last = now_m
                n = int(carry)
                carry -= n
                if n:
                    self.ingest(n, datetime.now(UTC))
            except Exception as exc:  # keep the stream alive no matter what
                self.last_error = repr(exc)
                log.exception("demo engine step failed")

    # ----------------------------------------------------------------- queries
    def _events_df(self, start, end, sources) -> pd.DataFrame:
        with self._lock:
            rows = list(self._events)
        df = pd.DataFrame(rows, columns=_EVENT_COLS)
        if df.empty:
            return df
        s, e = start.timestamp(), end.timestamp()
        return df[(df.ts >= s) & (df.ts < e) & df.source.isin(sources)]

    def _anoms_df(self, start, end, sources) -> pd.DataFrame:
        with self._lock:
            rows = list(self._anomalies)
        df = pd.DataFrame(rows, columns=_ANOM_COLS)
        if df.empty:
            return df
        s, e = start.timestamp(), end.timestamp()
        return df[(df.ts >= s) & (df.ts < e) & df.source.isin(sources)]

    def list_sources(self) -> list[str]:
        with self._lock:
            return sorted(self._sources)

    def kpis(self, start, end, sources) -> dict:
        df = self._events_df(start, end, sources)
        anoms = self._anoms_df(start, end, sources)
        last_min = 0 if df.empty else int((df.ts >= end.timestamp() - 60).sum())
        newest = datetime.fromtimestamp(self._newest, UTC) if self._newest else None
        return {
            "events": len(df),
            "late": 0 if df.empty else int(df.is_late.sum()),
            "anomalies": len(anoms),
            "events_per_s": last_min / 60.0,
            "newest": newest,
        }

    def sensor_values(self, start, end, bucket_s, sources) -> pd.DataFrame:
        df = self._events_df(start, end, sources)
        if df.empty:
            return pd.DataFrame(columns=["time", "source", "value"])
        df = df.assign(bucket=(df.ts // bucket_s) * bucket_s)
        out = df.groupby(["bucket", "source"], as_index=False)["value"].mean()
        out["time"] = _to_time(out["bucket"])
        return out[["time", "source", "value"]].sort_values("time")

    def anomaly_points(self, start, end, sources) -> pd.DataFrame:
        df = self._anoms_df(start, end, sources)
        if df.empty:
            return pd.DataFrame(columns=["time", "source", "detector", "value", "score"])
        df = df.assign(time=_to_time(df.ts))
        return df[["time", "source", "detector", "value", "score"]].sort_values("time")

    def anomalies_over_time(self, start, end, bin_s, sources) -> pd.DataFrame:
        df = self._anoms_df(start, end, sources)
        if df.empty:
            return pd.DataFrame(columns=["time", "detector", "count"])
        df = df.assign(bucket=(df.ts // bin_s) * bin_s)
        out = (
            df.groupby(["bucket", "detector"], as_index=False)
            .size()
            .rename(columns={"size": "count"})
        )
        out["time"] = _to_time(out["bucket"])
        return out[["time", "detector", "count"]].sort_values("time")

    def anomalies_by_source(self, start, end, sources) -> pd.DataFrame:
        df = self._anoms_df(start, end, sources)
        if df.empty:
            return pd.DataFrame(columns=["source", "detector", "count"])
        out = df.groupby(["source", "detector"], as_index=False).size()
        return out.rename(columns={"size": "count"})

    def recent_anomalies(self, sources, limit=100) -> pd.DataFrame:
        with self._lock:
            rows = list(self._anomalies)
        df = pd.DataFrame(rows, columns=_ANOM_COLS)
        df = df[df.source.isin(sources)].sort_values("ts", ascending=False).head(limit)
        if df.empty:
            return pd.DataFrame(columns=["time", "source", "detector", "value", "score"])
        df = df.assign(time=_to_time(df.ts), value=df.value.round(3), score=df.score.round(2))
        return df[["time", "source", "detector", "value", "score"]]

    def latest_windows(self) -> pd.DataFrame:
        with self._lock:
            wins = list(self._windows.values())
        rows = [
            (w.source, w.window_start, w.n, w.mean, w.std, w.min_value, w.max_value) for w in wins
        ]
        df = pd.DataFrame(
            rows, columns=["source", "window_start", "n", "mean", "std", "min", "max"]
        )
        if df.empty:
            return df
        num = ["mean", "std", "min", "max"]
        df[num] = df[num].round(3)
        return df.sort_values("source")

    def health(self) -> dict:
        now = time.time()
        with self._lock:
            batch = sorted(self._batch_ms)
            recent = sum(1 for e in self._events if e[0] >= now - 60 and not e[3])
            newest = self._newest
            counts = dict(self.counts)
        p95 = batch[int(0.95 * (len(batch) - 1))] if batch else None
        return {
            "events_per_s": recent / 60.0,
            "p95_batch_ms": p95,
            "event_age_s": (now - newest) if newest else None,
            "uptime_s": now - self.started_at,
            "counts": counts,
            "last_error": self.last_error,
        }

    def reject_rates(self, start, end, bucket_s) -> pd.DataFrame:
        with self._lock:
            rows = list(self._status_log)
        df = pd.DataFrame(rows, columns=["ts", "late", "duplicate", "invalid"])
        if df.empty:
            return pd.DataFrame(columns=["time", "series", "value"])
        df = df[(df.ts >= start.timestamp()) & (df.ts < end.timestamp())]
        if df.empty:
            return pd.DataFrame(columns=["time", "series", "value"])
        df = df.assign(bucket=(df.ts // bucket_s) * bucket_s)
        agg = df.groupby("bucket", as_index=False)[["late", "duplicate", "invalid"]].sum()
        long = agg.melt(id_vars="bucket", var_name="series", value_name="value")
        long["value"] = long["value"] / bucket_s
        long["time"] = _to_time(long["bucket"])
        return long[["time", "series", "value"]]


# ------------------------------------------------------- module-level facade
_ENGINE: DemoEngine | None = None
_ENGINE_LOCK = threading.Lock()


def ensure_started() -> DemoEngine:
    """Create, warm up and start the shared engine exactly once per server process."""
    global _ENGINE
    with _ENGINE_LOCK:
        if _ENGINE is None:
            engine = DemoEngine(rate=float(os.environ.get("DEMO_RATE", "10")))
            engine.warmup(900)
            engine.start()
            _ENGINE = engine
    return _ENGINE


def window_end(_anchor: bool = False) -> datetime:
    ensure_started()
    return datetime.now(UTC)


def list_sources() -> list[str]:
    return ensure_started().list_sources()


def kpis(start, end, sources) -> dict:
    return ensure_started().kpis(start, end, sources)


def sensor_values(start, end, bucket_s, sources):
    return ensure_started().sensor_values(start, end, bucket_s, sources)


def anomaly_points(start, end, sources):
    return ensure_started().anomaly_points(start, end, sources)


def anomalies_over_time(start, end, bin_s, sources):
    return ensure_started().anomalies_over_time(start, end, bin_s, sources)


def anomalies_by_source(start, end, sources):
    return ensure_started().anomalies_by_source(start, end, sources)


def recent_anomalies(sources, limit=100):
    return ensure_started().recent_anomalies(sources, limit)


def latest_windows():
    return ensure_started().latest_windows()


def health() -> dict:
    return ensure_started().health()


def reject_rates(start, end, bucket_s):
    return ensure_started().reject_rates(start, end, bucket_s)
