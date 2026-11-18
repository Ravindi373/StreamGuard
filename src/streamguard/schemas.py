"""Event schema, validation and dead-letter record helpers."""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, ValidationError, field_validator


class Event(BaseModel):
    """A single measurement. `ts` is *event time* (when it happened), always UTC."""

    model_config = ConfigDict(extra="ignore")

    event_id: str = Field(min_length=1, max_length=200)
    source: str = Field(min_length=1, max_length=100)
    ts: datetime
    value: float = Field(allow_inf_nan=False)
    attrs: dict[str, Any] | None = None

    @field_validator("ts")
    @classmethod
    def _to_utc(cls, v: datetime) -> datetime:
        if v.tzinfo is None:
            return v.replace(tzinfo=timezone.utc)
        return v.astimezone(timezone.utc)


def parse_event(raw: bytes | str) -> Event:
    """Parse and validate a JSON payload. Raises pydantic.ValidationError."""
    return Event.model_validate_json(raw)


def dlq_record(raw: bytes | None, error: str) -> bytes:
    """Wrap a rejected payload with the reason so it can be inspected/replayed later."""
    payload = (raw or b"").decode("utf-8", errors="replace")[:4096]
    return json.dumps({"error": error[:1000], "payload": payload}).encode("utf-8")


@dataclass(frozen=True, slots=True)
class EventRow:
    event_id: str
    source: str
    ts: datetime
    value: float
    is_late: bool


@dataclass(frozen=True, slots=True)
class AnomalyRow:
    event_id: str
    source: str
    ts: datetime
    value: float
    score: float
    detector: str


@dataclass(frozen=True, slots=True)
class WindowRow:
    source: str
    window_start: datetime
    window_end: datetime
    n: int
    mean: float
    std: float
    min_value: float
    max_value: float


__all__ = [
    "AnomalyRow",
    "Event",
    "EventRow",
    "ValidationError",
    "WindowRow",
    "dlq_record",
    "parse_event",
]
