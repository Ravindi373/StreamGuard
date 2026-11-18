"""Online (single-pass, O(1) memory per source) anomaly detectors.

Both detectors score a new observation against the state *before* it is absorbed,
so an outlier can never mask itself.
"""

from __future__ import annotations

import math
from collections import deque
from typing import NamedTuple, Protocol


class Result(NamedTuple):
    score: float
    is_anomaly: bool


class Detector(Protocol):
    name: str

    def observe(self, x: float) -> Result: ...


class EWMADetector:
    """Exponentially weighted mean/variance z-score.

    Robustness trick: when a point is flagged, it is clipped to the threshold band
    before updating the model, so one huge spike doesn't inflate the variance and
    hide the anomalies that follow it.
    """

    name = "ewma"

    def __init__(
        self,
        alpha: float = 0.05,
        threshold: float = 4.0,
        warmup: int = 30,
        min_std: float = 1e-6,
    ) -> None:
        if not 0 < alpha <= 1:
            raise ValueError("alpha must be in (0, 1]")
        self.alpha = alpha
        self.threshold = threshold
        self.warmup = warmup
        self.min_std = min_std
        self.n = 0
        self.mean = 0.0
        self.var = 0.0

    def observe(self, x: float) -> Result:
        if self.n == 0:
            self.mean, self.var, self.n = x, 0.0, 1
            return Result(0.0, False)

        std = math.sqrt(self.var)
        floor = max(self.min_std, 1e-6 * abs(self.mean))
        score = abs(x - self.mean) / max(std, floor)
        anomalous = self.n >= self.warmup and score > self.threshold

        x_upd = x
        if anomalous:
            band = self.threshold * max(std, floor)
            x_upd = min(max(x, self.mean - band), self.mean + band)

        # Cumulative average while warming up, then a fixed exponential decay.
        a = max(self.alpha, 1.0 / (self.n + 1))
        diff = x_upd - self.mean
        incr = a * diff
        self.mean += incr
        self.var = (1.0 - a) * (self.var + diff * incr)
        self.n += 1
        return Result(score, anomalous)


class RollingZScoreDetector:
    """Z-score against a sliding window of the last `window` observations.

    Unlike EWMA it fully forgets old data (and therefore adapts to a level shift
    after `window` points), at the cost of O(window) memory per source.
    """

    name = "rolling_z"
    _RESYNC_EVERY = 10_000

    def __init__(
        self,
        window: int = 60,
        threshold: float = 4.0,
        warmup: int = 30,
        min_std: float = 1e-6,
    ) -> None:
        if window < 3:
            raise ValueError("window must be >= 3")
        self.window = window
        self.threshold = threshold
        self.warmup = min(warmup, window)
        self.min_std = min_std
        self._buf: deque[float] = deque()
        self._ref: float | None = None  # values are stored relative to _ref for stability
        self._sum = 0.0
        self._sumsq = 0.0
        self._updates = 0

    def observe(self, x: float) -> Result:
        result = Result(0.0, False)
        n = len(self._buf)
        if n >= max(self.warmup, 2):
            mean_c = self._sum / n
            var = max(self._sumsq / n - mean_c * mean_c, 0.0) * n / (n - 1)
            mean = (self._ref or 0.0) + mean_c
            floor = max(self.min_std, 1e-6 * abs(mean))
            score = abs(x - mean) / max(math.sqrt(var), floor)
            result = Result(score, score > self.threshold)

        if self._ref is None:
            self._ref = x
        d = x - self._ref
        self._buf.append(d)
        self._sum += d
        self._sumsq += d * d
        if len(self._buf) > self.window:
            old = self._buf.popleft()
            self._sum -= old
            self._sumsq -= old * old

        self._updates += 1
        if self._updates % self._RESYNC_EVERY == 0:
            self._resync()
        return result

    def _resync(self) -> None:
        """Recompute running sums to cancel accumulated floating point drift."""
        ref = self._ref or 0.0
        actual = [v + ref for v in self._buf]
        new_ref = sum(actual) / len(actual)
        self._ref = new_ref
        self._buf = deque(v - new_ref for v in actual)
        self._sum = sum(self._buf)
        self._sumsq = sum(v * v for v in self._buf)
