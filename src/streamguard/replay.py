"""`streamguard replay`: re-publish a JSONL file of events (see `simulate --output`)."""

from __future__ import annotations

import json
import logging
import time

from streamguard.config import Settings

log = logging.getLogger(__name__)


def run(settings: Settings, args) -> None:
    from streamguard.kafka_utils import ensure_topics, make_producer, produce

    ensure_topics(
        settings.kafka_bootstrap,
        {settings.topic_raw: settings.topic_partitions, settings.topic_dlq: 1},
    )
    producer = make_producer(settings.kafka_bootstrap)
    started = time.perf_counter()
    first_ts: float | None = None
    sent = 0
    with open(args.file, "rb") as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            key = None
            try:
                body = json.loads(line)
                key = body.get("source")
                if args.speed > 0:
                    from datetime import datetime

                    ts = datetime.fromisoformat(body["ts"]).timestamp()
                    first_ts = ts if first_ts is None else first_ts
                    wait = (ts - first_ts) / args.speed - (time.perf_counter() - started)
                    if wait > 0:
                        time.sleep(wait)
            except (ValueError, KeyError, AttributeError):
                pass  # malformed lines are still published: the processor routes them to the DLQ
            produce(producer, settings.topic_raw, key, line)
            sent += 1
            producer.poll(0)
    producer.flush(30)
    log.info("replayed %d events in %.1fs", sent, time.perf_counter() - started)
