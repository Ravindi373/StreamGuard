"""TimescaleDB/PostgreSQL sink: one transaction per batch, idempotent by construction."""

from __future__ import annotations

import contextlib
import logging
import time

import psycopg

from streamguard import metrics
from streamguard.pipeline import BatchOutput

log = logging.getLogger(__name__)

SQL_EVENTS = (
    "INSERT INTO events (event_id, source, ts, value, is_late) VALUES (%s, %s, %s, %s, %s) "
    "ON CONFLICT DO NOTHING"
)
SQL_ANOMALIES = (
    "INSERT INTO anomalies (event_id, detector, source, ts, value, score) "
    "VALUES (%s, %s, %s, %s, %s, %s) ON CONFLICT DO NOTHING"
)
# After a crash a restarted processor may rebuild a *partial* window from replayed events.
# Only let a write replace the stored row if it saw more events.
SQL_WINDOWS = (
    "INSERT INTO window_stats "
    "(source, window_start, window_end, n, mean, std, min_value, max_value) "
    "VALUES (%s, %s, %s, %s, %s, %s, %s, %s) "
    "ON CONFLICT (source, window_start) DO UPDATE SET "
    "window_end = EXCLUDED.window_end, n = EXCLUDED.n, mean = EXCLUDED.mean, std = EXCLUDED.std, "
    "min_value = EXCLUDED.min_value, max_value = EXCLUDED.max_value "
    "WHERE EXCLUDED.n > window_stats.n"
)


class PostgresSink:
    def __init__(self, dsn: str, max_attempts: int = 8) -> None:
        self.dsn = dsn
        self.max_attempts = max_attempts
        self._conn: psycopg.Connection | None = None

    def _connection(self) -> psycopg.Connection:
        if self._conn is None or self._conn.closed:
            self._conn = psycopg.connect(self.dsn, autocommit=False)
        return self._conn

    def connect(self, attempts: int = 30) -> None:
        """Block until the database accepts connections (used at startup)."""
        for i in range(1, attempts + 1):
            try:
                self._connection()
                return
            except psycopg.OperationalError as exc:
                log.info("waiting for database (%d/%d): %s", i, attempts, exc)
                time.sleep(2)
        raise RuntimeError("database not reachable")

    def write(self, out: BatchOutput) -> None:
        """Persist a batch atomically. Safe to call twice with the same data."""
        if out.is_empty:
            return
        delay = 0.5
        for attempt in range(1, self.max_attempts + 1):
            try:
                conn = self._connection()
                with conn.cursor() as cur:
                    if out.events:
                        cur.executemany(
                            SQL_EVENTS,
                            [(e.event_id, e.source, e.ts, e.value, e.is_late) for e in out.events],
                        )
                    if out.anomalies:
                        cur.executemany(
                            SQL_ANOMALIES,
                            [
                                (a.event_id, a.detector, a.source, a.ts, a.value, a.score)
                                for a in out.anomalies
                            ],
                        )
                    if out.windows:
                        cur.executemany(
                            SQL_WINDOWS,
                            [
                                (
                                    w.source,
                                    w.window_start,
                                    w.window_end,
                                    w.n,
                                    w.mean,
                                    w.std,
                                    w.min_value,
                                    w.max_value,
                                )
                                for w in out.windows
                            ],
                        )
                conn.commit()
                return
            except (psycopg.OperationalError, psycopg.InterfaceError) as exc:
                metrics.DB_RETRIES.inc()
                log.warning("db write failed (%d/%d): %s", attempt, self.max_attempts, exc)
                self._reset()
                if attempt == self.max_attempts:
                    raise
                time.sleep(delay)
                delay = min(delay * 2, 10)

    def _reset(self) -> None:
        if self._conn is not None:
            with contextlib.suppress(Exception):
                self._conn.close()
        self._conn = None

    def close(self) -> None:
        self._reset()
