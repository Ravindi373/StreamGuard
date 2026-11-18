import math

import pytest
from conftest import T0

from streamguard.windows import WindowAggregator

BASE = T0.timestamp()


def test_window_closes_only_after_watermark_passes_end():
    w = WindowAggregator(10)
    for i, v in enumerate([1.0, 2.0, 3.0]):
        w.add("a", BASE + i, v)
    assert w.close_ready(BASE + 9.9) == []
    rows = w.close_ready(BASE + 10)
    assert len(rows) == 1
    r = rows[0]
    assert (r.n, r.mean, r.min_value, r.max_value) == (3, 2.0, 1.0, 3.0)
    assert math.isclose(r.std, 1.0)
    assert (r.window_end - r.window_start).total_seconds() == 10


def test_sources_and_windows_are_separate():
    w = WindowAggregator(10)
    w.add("a", BASE + 1, 1)
    w.add("b", BASE + 1, 2)
    w.add("a", BASE + 11, 3)
    rows = w.close_ready(BASE + 100)
    assert [(r.source, r.n) for r in rows] == [("a", 1), ("a", 1), ("b", 1)]


def test_window_is_emitted_once_and_late_adds_are_rejected():
    w = WindowAggregator(10)
    assert w.add("a", BASE + 1, 1)
    assert len(w.close_ready(BASE + 10)) == 1
    assert w.add("a", BASE + 2, 5) is False  # window already emitted
    assert w.close_ready(BASE + 20) == []
    assert w.add("a", BASE + 22, 5) is True  # a later window is still open


def test_single_point_has_zero_std():
    w = WindowAggregator(10)
    w.add("a", BASE, 7.0)
    assert w.close_ready(BASE + 10)[0].std == 0.0


def test_invalid_size():
    with pytest.raises(ValueError):
        WindowAggregator(0)
