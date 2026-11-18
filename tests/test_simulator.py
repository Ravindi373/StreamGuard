import json
from datetime import datetime, timezone

import pytest
from pydantic import ValidationError

from streamguard.schemas import parse_event
from streamguard.simulator import SensorSimulator, SimConfig

NOW = datetime(2026, 1, 1, tzinfo=timezone.utc)


def test_same_seed_gives_identical_streams():
    a = SensorSimulator(SimConfig(seed=7, fault_rate=0.05))
    b = SensorSimulator(SimConfig(seed=7, fault_rate=0.05))
    assert [a.next_message(NOW).payload for _ in range(200)] == [
        b.next_message(NOW).payload for _ in range(200)
    ]


def test_event_ids_are_sequential_per_sensor():
    sim = SensorSimulator(SimConfig(seed=1, sensors=3))
    seen: dict[str, list[int]] = {}
    for _ in range(30):
        m = sim.next_message(NOW)
        source, run, seq = m.event_id.split(":")
        assert run == sim.run_id
        seen.setdefault(source, []).append(int(seq))
    assert all(v == list(range(1, 11)) for v in seen.values())


def test_faults_are_labelled_in_payload():
    sim = SensorSimulator(SimConfig(seed=2, fault_rate=0.2))
    msgs = [sim.next_message(NOW) for _ in range(300)]
    faulty = [m for m in msgs if m.fault]
    assert faulty
    for m in faulty:
        assert json.loads(m.payload)["attrs"]["fault"] == m.fault
    assert {"spike", "shift_start"} <= {m.fault for m in faulty}


def test_malformed_messages_are_all_rejected_by_the_schema():
    sim = SensorSimulator(SimConfig(seed=3, malformed_prob=1.0))
    seen = set()
    for _ in range(100):
        m = sim.next_message(NOW)
        assert m.kind == "malformed"
        seen.add(m.payload)
        with pytest.raises(ValidationError):
            parse_event(m.payload)
    assert len(seen) == 6  # every malformed variant is exercised


def test_duplicates_resend_identical_payload_and_late_events_are_old():
    sim = SensorSimulator(SimConfig(seed=4, dup_prob=0.5, late_prob=0.3))
    msgs = [sim.next_message(NOW) for _ in range(200)]
    dups = [m for m in msgs if m.kind == "duplicate"]
    assert dups
    first = {m.event_id: m.payload for m in msgs if m.kind != "duplicate"}
    assert all(first[d.event_id] == d.payload for d in dups)
    late = [m for m in msgs if m.kind == "late"]
    assert late and all((NOW - parse_event(m.payload).ts).total_seconds() >= 10 for m in late)
