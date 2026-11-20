"""Helpers shared by the database backend and the in-process demo backend."""

from __future__ import annotations


def bucket_seconds(range_s: int, max_points: int = 240) -> int:
    """Smallest 'nice' bucket that keeps a chart under `max_points` points."""
    for b in (1, 2, 5, 10, 15, 30, 60, 120, 300, 600):
        if range_s / b <= max_points:
            return b
    return 600
