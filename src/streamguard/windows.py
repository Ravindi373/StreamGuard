"""Event-time tumbling windows with watermark-driven closing."""

from __future__ import annotations

import math
from datetime import UTC, datetime

from streamguard.schemas import WindowRow


class _Acc:
    __slots__ = ("m2", "max", "mean", "min", "n")

    def __init__(self) -> None:
        self.n = 0
        self.mean = 0.0
        self.m2 = 0.0
        self.min = math.inf
        self.max = -math.inf

    def add(self, x: float) -> None:
        # Welford's online algorithm: numerically stable mean/variance.
        self.n += 1
        delta = x - self.mean
        self.mean += delta / self.n
        self.m2 += delta * (x - self.mean)
        if x < self.min:
            self.min = x
        if x > self.max:
            self.max = x

    @property
    def std(self) -> float:
        return math.sqrt(self.m2 / (self.n - 1)) if self.n > 1 else 0.0


def _dt(epoch: float) -> datetime:
    return datetime.fromtimestamp(epoch, tz=UTC)


class WindowAggregator:
    """Aggregates values into fixed `size_seconds` windows per source.

    A window is emitted exactly once, when the watermark passes its end. Events that
    would land in an already-emitted window are rejected (`add` returns False) rather
    than silently re-emitting a partial window.
    """

    def __init__(self, size_seconds: int) -> None:
        if size_seconds <= 0:
            raise ValueError("size_seconds must be positive")
        self.size = size_seconds
        self._open: dict[tuple[str, float], _Acc] = {}
        self._closed_before = -math.inf

    def add(self, source: str, ts: float, value: float) -> bool:
        start = math.floor(ts / self.size) * self.size
        if start + self.size <= self._closed_before:
            return False
        key = (source, start)
        acc = self._open.get(key)
        if acc is None:
            acc = self._open[key] = _Acc()
        acc.add(value)
        return True

    def close_ready(self, watermark: float) -> list[WindowRow]:
        """Emit every window whose end is <= watermark (epoch seconds)."""
        ready = sorted(k for k in self._open if k[1] + self.size <= watermark)
        rows = []
        for key in ready:
            acc = self._open.pop(key)
            source, start = key
            rows.append(
                WindowRow(
                    source=source,
                    window_start=_dt(start),
                    window_end=_dt(start + self.size),
                    n=acc.n,
                    mean=acc.mean,
                    std=acc.std,
                    min_value=acc.min,
                    max_value=acc.max,
                )
            )
        self._closed_before = max(self._closed_before, watermark)
        return rows

    @property
    def open_windows(self) -> int:
        return len(self._open)
