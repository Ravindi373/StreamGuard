import json

from streamguard.live import trade_to_message
from streamguard.schemas import parse_event

TRADE = {
    "e": "trade",
    "s": "BTCUSDT",
    "t": 12345,
    "p": "60000.00",
    "q": "0.01",
    "T": 1_767_225_600_000,
}


def test_return_mode_skips_first_tick_then_emits_bps():
    last = {}
    assert trade_to_message(TRADE, last, "return") is None
    key, payload = trade_to_message({**TRADE, "t": 12346, "p": "60060.00"}, last, "return")
    ev = parse_event(payload)
    assert key == "BTCUSDT" and ev.event_id == "BTCUSDT:12346"
    assert 9.9 < ev.value < 10.1  # +0.1% ~= 10 bps
    assert json.loads(payload)["attrs"]["price"] == 60060.0


def test_price_mode_emits_raw_price():
    key, payload = trade_to_message(TRADE, {}, "price")
    assert parse_event(payload).value == 60000.0
