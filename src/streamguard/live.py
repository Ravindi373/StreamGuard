"""`streamguard live`: stream real trade ticks from Binance into Kafka.

Value mode `return` publishes the tick-to-tick log return in basis points (stationary,
so z-score detectors behave); `price` publishes the raw trade price.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import logging
import math
import os
import signal
from datetime import UTC, datetime

from streamguard.config import Settings

log = logging.getLogger(__name__)


def trade_to_message(
    data: dict, last_price: dict[str, float], mode: str
) -> tuple[str, bytes] | None:
    """Convert a Binance trade payload to (key, JSON bytes); None if nothing to emit."""
    symbol = data["s"]
    price = float(data["p"])
    prev = last_price.get(symbol)
    last_price[symbol] = price
    if mode == "return":
        if prev is None or prev <= 0:
            return None
        value = math.log(price / prev) * 10_000
    else:
        value = price
    body = {
        "event_id": f"{symbol}:{data['t']}",
        "source": symbol,
        "ts": datetime.fromtimestamp(data["T"] / 1000, tz=UTC).isoformat(),
        "value": value,
        "attrs": {"price": price, "qty": float(data["q"])},
    }
    return symbol, json.dumps(body, separators=(",", ":")).encode()


async def _stream(settings: Settings, symbols: list[str], mode: str, stop: asyncio.Event) -> None:
    import websockets

    from streamguard.kafka_utils import ensure_topics, make_producer, produce

    base = os.environ.get("BINANCE_WS_BASE", "wss://stream.binance.com:9443")
    streams = "/".join(f"{s.lower()}@trade" for s in symbols)
    url = f"{base}/stream?streams={streams}"

    ensure_topics(
        settings.kafka_bootstrap,
        {settings.topic_raw: settings.topic_partitions, settings.topic_dlq: 1},
    )
    producer = make_producer(settings.kafka_bootstrap)
    last_price: dict[str, float] = {}
    backoff, sent = 1.0, 0
    try:
        while not stop.is_set():
            try:
                log.info("connecting to %s", url)
                async with websockets.connect(url, ping_interval=20) as ws:
                    backoff = 1.0
                    async for raw in ws:
                        data = json.loads(raw).get("data", {})
                        if data.get("e") != "trade":
                            continue
                        item = trade_to_message(data, last_price, mode)
                        if item:
                            produce(producer, settings.topic_raw, item[0], item[1])
                            sent += 1
                        producer.poll(0)
                        if sent and sent % 1000 == 0:
                            log.info("published %d ticks", sent)
                        if stop.is_set():
                            break
            except Exception as exc:  # noqa: BLE001  reconnect on any network error
                log.warning("feed error: %s (reconnect in %.0fs)", exc, backoff)
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 30)
    finally:
        producer.flush(10)


def run(settings: Settings, args) -> None:
    async def main() -> None:
        stop = asyncio.Event()
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            try:
                loop.add_signal_handler(sig, stop.set)
            except NotImplementedError:  # Windows event loops
                signal.signal(sig, lambda *_: stop.set())
        task = asyncio.create_task(_stream(settings, args.symbols, args.value, stop))
        await stop.wait()
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task

    asyncio.run(main())
