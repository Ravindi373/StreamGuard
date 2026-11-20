"""`streamguard bench`: in-process throughput of parse + pipeline (no Kafka, no DB)."""

from __future__ import annotations

import time
from datetime import UTC, datetime, timedelta

from streamguard.pipeline import Pipeline
from streamguard.schemas import parse_event
from streamguard.simulator import SensorSimulator, SimConfig


def run(count: int = 200_000, sensors: int = 10, batch_size: int = 500) -> dict[str, float]:
    sim = SensorSimulator(SimConfig(sensors=sensors, seed=1))
    t = datetime(2026, 1, 1, tzinfo=UTC)
    step = timedelta(milliseconds=5)
    payloads = []
    for _ in range(count):
        payloads.append(sim.next_message(t).payload)
        t += step

    t0 = time.perf_counter()
    events = [parse_event(p) for p in payloads]
    t1 = time.perf_counter()

    pipe = Pipeline()
    for i in range(0, count, batch_size):
        pipe.process_batch(events[i : i + batch_size])
    t2 = time.perf_counter()

    return {
        "events": float(count),
        "parse_eps": count / (t1 - t0),
        "pipeline_eps": count / (t2 - t1),
        "combined_eps": count / (t2 - t0),
    }


def print_report(r: dict[str, float]) -> None:
    print(f"events:                 {int(r['events']):>12,}")
    print(f"parse + validate:       {r['parse_eps']:>12,.0f} events/s  (single core)")
    print(f"windows + detectors:    {r['pipeline_eps']:>12,.0f} events/s  (single core)")
    print(f"combined (no I/O):      {r['combined_eps']:>12,.0f} events/s  (single core)")
