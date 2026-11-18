from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from streamguard.schemas import Event

T0 = datetime(2026, 1, 1, tzinfo=timezone.utc)


@pytest.fixture
def make_event():
    def _make(
        i: int = 0,
        *,
        source: str = "s1",
        value: float = 10.0,
        at: float | None = None,
        event_id: str | None = None,
    ) -> Event:
        ts = T0 + timedelta(seconds=i if at is None else at)
        return Event(event_id=event_id or f"{source}:{i}", source=source, ts=ts, value=value)

    return _make
