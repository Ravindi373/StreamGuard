import json
from datetime import timezone

import pytest
from pydantic import ValidationError

from streamguard.schemas import dlq_record, parse_event


def test_parses_valid_event():
    ev = parse_event(b'{"event_id":"a:1","source":"s","ts":"2026-01-01T00:00:00Z","value":1.5}')
    assert ev.value == 1.5 and ev.ts.tzinfo is not None


def test_naive_timestamp_is_assumed_utc():
    ev = parse_event('{"event_id":"a:1","source":"s","ts":"2026-01-01T00:00:00","value":1}')
    assert ev.ts.tzinfo == timezone.utc


def test_offset_timestamp_converted_to_utc():
    ev = parse_event('{"event_id":"a:1","source":"s","ts":"2026-01-01T05:30:00+05:30","value":1}')
    assert ev.ts.hour == 0 and ev.ts.tzinfo == timezone.utc


@pytest.mark.parametrize(
    "raw",
    [
        b"{not json",
        b'{"event_id":"a","source":"s","ts":"2026-01-01T00:00:00Z"}',
        b'{"event_id":"a","source":"s","ts":"2026-01-01T00:00:00Z","value":"abc"}',
        b'{"event_id":"a","source":"s","ts":"nope","value":1}',
        b'{"event_id":"","source":"s","ts":"2026-01-01T00:00:00Z","value":1}',
        b'{"event_id":"a","source":"s","ts":"2026-01-01T00:00:00Z","value":NaN}',
    ],
)
def test_rejects_invalid(raw):
    with pytest.raises(ValidationError):
        parse_event(raw)


def test_dlq_record_keeps_payload_and_error():
    rec = json.loads(dlq_record(b"\xff garbage", "boom"))
    assert rec["error"] == "boom" and "garbage" in rec["payload"]
