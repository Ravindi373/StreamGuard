"""The pure stream-processing core: no Kafka, no database, fully unit-testable.

Per batch of validated events it
  1. drops duplicates (bounded LRU of recent event ids),
  2. flags late events (older than `allowed_lateness_s` behind their source's newest event),
  3. feeds on-time events to windows and per-source detectors,
  4. closes windows the watermark has passed.
"""

from __future__ import annotations

from collections import OrderedDict
from collections.abc import Callable, Iterable
from dataclasses import dataclass, field
from datetime import datetime

from streamguard.config import Settings
from streamguard.detectors import Detector, EWMADetector, RollingZScoreDetector
from streamguard.schemas import AnomalyRow, Event, EventRow, WindowRow
from streamguard.windows import WindowAggregator


@dataclass
class BatchOutput:
    events: list[EventRow] = field(default_factory=list)
    anomalies: list[AnomalyRow] = field(default_factory=list)
    windows: list[WindowRow] = field(default_factory=list)
    late: int = 0
    duplicates: int = 0
    newest_event_ts: datetime | None = None

    @property
    def is_empty(self) -> bool:
        return not (self.events or self.anomalies or self.windows)


class _SourceState:
    __slots__ = ("detectors", "max_ts")

    def __init__(self, detectors: list[Detector]) -> None:
        self.detectors = detectors
        self.max_ts: float | None = None


DetectorFactory = Callable[[], list[Detector]]


class Pipeline:
    def __init__(
        self,
        *,
        window_seconds: int = 10,
        allowed_lateness_s: float = 5.0,
        dedup_capacity: int = 100_000,
        detector_factory: DetectorFactory | None = None,
    ) -> None:
        self.lateness = allowed_lateness_s
        self._dedup_capacity = dedup_capacity
        self._detector_factory = detector_factory or (
            lambda: [EWMADetector(), RollingZScoreDetector()]
        )
        self._windows = WindowAggregator(window_seconds)
        self._sources: dict[str, _SourceState] = {}
        self._seen: OrderedDict[str, None] = OrderedDict()
        self._global_max_ts: float | None = None

    @classmethod
    def from_settings(cls, s: Settings) -> Pipeline:
        def factory() -> list[Detector]:
            return [
                EWMADetector(alpha=s.ewma_alpha, threshold=s.z_threshold, warmup=s.warmup),
                RollingZScoreDetector(
                    window=s.rolling_window, threshold=s.z_threshold, warmup=s.warmup
                ),
            ]

        return cls(
            window_seconds=s.window_seconds,
            allowed_lateness_s=s.allowed_lateness_s,
            dedup_capacity=s.dedup_capacity,
            detector_factory=factory,
        )

    # ------------------------------------------------------------------ helpers
    def _is_duplicate(self, event_id: str) -> bool:
        if event_id in self._seen:
            self._seen.move_to_end(event_id)
            return True
        self._seen[event_id] = None
        if len(self._seen) > self._dedup_capacity:
            self._seen.popitem(last=False)
        return False

    def _state(self, source: str) -> _SourceState:
        st = self._sources.get(source)
        if st is None:
            st = self._sources[source] = _SourceState(self._detector_factory())
        return st

    # --------------------------------------------------------------------- main
    def process_batch(self, events: Iterable[Event]) -> BatchOutput:
        out = BatchOutput()
        newest: float | None = None

        for ev in events:
            if self._is_duplicate(ev.event_id):
                out.duplicates += 1
                continue

            ts = ev.ts.timestamp()
            st = self._state(ev.source)
            late = st.max_ts is not None and ts < st.max_ts - self.lateness
            if st.max_ts is None or ts > st.max_ts:
                st.max_ts = ts
            if self._global_max_ts is None or ts > self._global_max_ts:
                self._global_max_ts = ts
            if newest is None or ts > newest:
                newest = ts
                out.newest_event_ts = ev.ts

            # A non-late event can still miss its window if other sources advanced the
            # global watermark past it; treat that the same as lateness.
            if late or not self._windows.add(ev.source, ts, ev.value):
                out.late += 1
                out.events.append(EventRow(ev.event_id, ev.source, ev.ts, ev.value, True))
                continue

            out.events.append(EventRow(ev.event_id, ev.source, ev.ts, ev.value, False))
            for det in st.detectors:
                res = det.observe(ev.value)
                if res.is_anomaly:
                    out.anomalies.append(
                        AnomalyRow(ev.event_id, ev.source, ev.ts, ev.value, res.score, det.name)
                    )

        if self._global_max_ts is not None:
            out.windows = self._windows.close_ready(self._global_max_ts - self.lateness)
        return out

    @property
    def watermark(self) -> float | None:
        return None if self._global_max_ts is None else self._global_max_ts - self.lateness
