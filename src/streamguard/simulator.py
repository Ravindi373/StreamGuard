"""Deterministic IoT-style sensor simulator with labelled fault injection.

Besides realistic signals it deliberately produces the messy things a real stream
contains, so every defensive code path in the pipeline gets exercised:
  * spikes and level shifts  (ground-truth label in `attrs.fault`)
  * late events              (timestamp far behind "now")
  * duplicate deliveries     (same event_id re-sent)
  * malformed payloads       (routed to the dead-letter topic)
"""

from __future__ import annotations

import json
import math
import random
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone


@dataclass
class SimConfig:
    sensors: int = 5
    fault_rate: float = 0.002
    late_prob: float = 0.0
    dup_prob: float = 0.0
    malformed_prob: float = 0.0
    late_min_s: float = 10.0
    late_max_s: float = 30.0
    seed: int | None = None


@dataclass(frozen=True)
class Message:
    key: str
    payload: bytes
    kind: str  # ok | late | duplicate | malformed
    event_id: str | None = None
    fault: str | None = None  # None | spike | shift_start | shift


@dataclass
class _Sensor:
    base: float
    amplitude: float
    period_s: float
    noise: float
    seq: int = 0
    shift_left: int = 0
    shift_delta: float = 0.0


_MALFORMED = (
    b"{not valid json",
    b'{"event_id": "x:1", "source": "sensor-00", "ts": "2026-01-01T00:00:00Z"}',
    b'{"event_id": "x:2", "source": "sensor-00", "ts": "2026-01-01T00:00:00Z", "value": "abc"}',
    b'{"event_id": "x:3", "source": "sensor-00", "ts": "not-a-time", "value": 1.0}',
    b'{"event_id": "", "source": "sensor-00", "ts": "2026-01-01T00:00:00Z", "value": 1.0}',
    b'{"event_id": "x:5", "source": "sensor-00", "ts": "2026-01-01T00:00:00Z", "value": NaN}',
)


class SensorSimulator:
    def __init__(self, cfg: SimConfig | None = None) -> None:
        self.cfg = cfg or SimConfig()
        self._rng = random.Random(self.cfg.seed)
        self.run_id = f"{self._rng.getrandbits(32):08x}"
        self._sensors = [
            _Sensor(
                base=self._rng.uniform(15, 80),
                amplitude=self._rng.uniform(1, 6),
                period_s=self._rng.uniform(120, 600),
                noise=self._rng.uniform(0.3, 1.5),
            )
            for _ in range(self.cfg.sensors)
        ]
        self._i = 0
        self._last_ok: Message | None = None

    def next_message(self, now: datetime | None = None) -> Message:
        now = now or datetime.now(timezone.utc)
        rng, cfg = self._rng, self.cfg

        if cfg.malformed_prob and rng.random() < cfg.malformed_prob:
            return Message("malformed", rng.choice(_MALFORMED), "malformed")
        if cfg.dup_prob and self._last_ok and rng.random() < cfg.dup_prob:
            m = self._last_ok
            return Message(m.key, m.payload, "duplicate", m.event_id, m.fault)

        idx = self._i % len(self._sensors)
        self._i += 1
        s = self._sensors[idx]
        source = f"sensor-{idx:02d}"
        s.seq += 1
        event_id = f"{source}:{self.run_id}:{s.seq}"

        ts = now
        kind = "ok"
        if cfg.late_prob and rng.random() < cfg.late_prob:
            ts = now - timedelta(seconds=rng.uniform(cfg.late_min_s, cfg.late_max_s))
            kind = "late"

        t = now.timestamp()
        value = s.base + s.amplitude * math.sin(2 * math.pi * t / s.period_s)
        value += rng.gauss(0.0, s.noise)

        fault: str | None = None
        if s.shift_left > 0:
            value += s.shift_delta
            s.shift_left -= 1
            fault = "shift"
        elif rng.random() < cfg.fault_rate:
            sign = rng.choice((-1.0, 1.0))
            if rng.random() < 0.8:
                value += sign * rng.uniform(3.0, 12.0) * s.noise
                fault = "spike"
            else:
                s.shift_delta = sign * rng.uniform(3.0, 6.0) * s.noise
                s.shift_left = rng.randint(20, 60)
                value += s.shift_delta
                fault = "shift_start"

        body = {
            "event_id": event_id,
            "source": source,
            "ts": ts.isoformat(),
            "value": round(value, 4),
            "attrs": {"fault": fault} if fault else None,
        }
        msg = Message(
            source, json.dumps(body, separators=(",", ":")).encode(), kind, event_id, fault
        )
        if kind == "ok":
            self._last_ok = msg
        return msg
