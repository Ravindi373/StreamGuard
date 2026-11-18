"""`streamguard verify`: prove no simulator events were lost (gap check per sensor/run).

Simulator event ids look like `sensor-03:1a2b3c4d:812`, with a per-run, per-sensor
sequence number, so a gap in the sequence means a lost event.
"""

from __future__ import annotations

import logging
import time

import psycopg

log = logging.getLogger(__name__)

SQL = """
WITH parsed AS (
  SELECT source,
         split_part(event_id, ':', 2) AS run,
         split_part(event_id, ':', 3)::bigint AS seq
  FROM events
  WHERE event_id ~ '^[^:]+:[0-9a-f]{8}:[0-9]+$'
)
SELECT source, run, count(*) AS stored, min(seq), max(seq),
       (max(seq) - min(seq) + 1 - count(*)) AS missing
FROM parsed GROUP BY source, run ORDER BY run, source
"""


def _query(dsn: str):
    with psycopg.connect(dsn) as conn, conn.cursor() as cur:
        cur.execute(SQL)
        return cur.fetchall()


def run(dsn: str, settle: float, timeout: float = 180.0) -> int:
    rows = _query(dsn)
    if settle > 0:
        # Wait until the stored count stops growing (processor caught up).
        deadline = time.monotonic() + timeout
        stable_since = time.monotonic()
        total = sum(r[2] for r in rows)
        while time.monotonic() < deadline and time.monotonic() - stable_since < settle:
            time.sleep(2)
            rows = _query(dsn)
            new_total = sum(r[2] for r in rows)
            if new_total != total:
                total, stable_since = new_total, time.monotonic()

    if not rows:
        print("no simulator events found in the database")
        return 1
    print(f"{'source':<12} {'run':<10} {'stored':>8} {'min_seq':>8} {'max_seq':>8} {'missing':>8}")
    missing_total = 0
    for source, run_id, stored, lo, hi, missing in rows:
        missing_total += missing
        print(f"{source:<12} {run_id:<10} {stored:>8} {lo:>8} {hi:>8} {missing:>8}")
    print(f"\nTOTAL stored={sum(r[2] for r in rows)} missing={missing_total}")
    print("PASS: no gaps" if missing_total == 0 else "FAIL: events are missing")
    return 0 if missing_total == 0 else 1
