"""`streamguard evaluate`: offline precision/recall of the detectors on labelled data."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

from streamguard.detectors import EWMADetector, RollingZScoreDetector
from streamguard.pipeline import Pipeline
from streamguard.schemas import parse_event
from streamguard.simulator import SensorSimulator, SimConfig


def evaluate(
    count: int = 200_000,
    sensors: int = 5,
    rate: float = 50.0,
    fault_rate: float = 0.002,
    seed: int = 42,
    threshold: float = 4.0,
    alpha: float = 0.05,
) -> dict[str, dict[str, float]]:
    sim = SensorSimulator(SimConfig(sensors=sensors, fault_rate=fault_rate, seed=seed))
    pipe = Pipeline(
        detector_factory=lambda: [
            EWMADetector(alpha=alpha, threshold=threshold),
            RollingZScoreDetector(threshold=threshold),
        ]
    )

    t = datetime(2026, 1, 1, tzinfo=UTC)
    step = timedelta(seconds=1.0 / rate)
    labels: dict[str, str | None] = {}
    flagged: dict[str, set[str]] = {}

    batch = []
    for _ in range(count):
        m = sim.next_message(t)
        t += step
        labels[m.event_id] = m.fault
        batch.append(parse_event(m.payload))
        if len(batch) == 500:
            _consume(pipe, batch, flagged)
            batch = []
    if batch:
        _consume(pipe, batch, flagged)

    flagged["any"] = set().union(*flagged.values()) if flagged else set()
    spikes = {e for e, f in labels.items() if f == "spike"}
    onsets = {e for e, f in labels.items() if f == "shift_start"}
    sustained = {e for e, f in labels.items() if f == "shift"}
    faulty = spikes | onsets | sustained
    clean = {e for e, f in labels.items() if f is None}

    report = {}
    for name, hits in flagged.items():
        report[name] = {
            "precision": len(hits & faulty) / len(hits) if hits else float("nan"),
            "spike_recall": len(hits & spikes) / len(spikes) if spikes else float("nan"),
            "shift_onset_recall": len(hits & onsets) / len(onsets) if onsets else float("nan"),
            "shift_sustained_recall": (
                len(hits & sustained) / len(sustained) if sustained else float("nan")
            ),
            "false_positive_rate": len(hits & clean) / len(clean) if clean else float("nan"),
            "flagged": float(len(hits)),
        }
    report["_dataset"] = {
        "events": float(count),
        "spikes": float(len(spikes)),
        "shift_onsets": float(len(onsets)),
        "shift_sustained": float(len(sustained)),
    }
    return report


def _consume(pipe: Pipeline, batch, flagged: dict[str, set[str]]) -> None:
    for a in pipe.process_batch(batch).anomalies:
        flagged.setdefault(a.detector, set()).add(a.event_id)


def print_report(report: dict[str, dict[str, float]]) -> None:
    ds = report.pop("_dataset")
    print(
        f"dataset: {int(ds['events'])} events, {int(ds['spikes'])} spikes, "
        f"{int(ds['shift_onsets'])} level shifts ({int(ds['shift_sustained'])} follow-on points)\n"
    )
    header = ("detector", "precision", "spike rec.", "onset rec.", "sustained", "FP rate")
    print(f"{header[0]:<12}" + "".join(f"{h:>12}" for h in header[1:]))
    for name, r in report.items():
        print(
            f"{name:<12}{r['precision']:>12.3f}{r['spike_recall']:>12.3f}"
            f"{r['shift_onset_recall']:>12.3f}{r['shift_sustained_recall']:>12.3f}"
            f"{r['false_positive_rate']:>12.5f}"
        )
