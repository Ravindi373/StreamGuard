"""`streamguard simulate`: publish simulated sensor events to Kafka (or dump to JSONL)."""

from __future__ import annotations

import logging
import signal
import time
from datetime import datetime, timedelta, timezone

from streamguard.config import Settings
from streamguard.simulator import SensorSimulator, SimConfig

log = logging.getLogger(__name__)


def _sim_from_args(args) -> SensorSimulator:
    return SensorSimulator(
        SimConfig(
            sensors=args.sensors,
            fault_rate=args.fault_rate,
            late_prob=args.late_prob,
            dup_prob=args.dup_prob,
            malformed_prob=args.malformed_prob,
            seed=args.seed,
        )
    )


def dump_to_file(args) -> None:
    """Generate `--count` events on a simulated clock (no Kafka needed)."""
    sim = _sim_from_args(args)
    t = datetime.now(timezone.utc) - timedelta(seconds=args.count / args.rate)
    step = timedelta(seconds=1.0 / args.rate)
    with open(args.output, "wb") as fh:
        for _ in range(args.count):
            fh.write(sim.next_message(t).payload + b"\n")
            t += step
    log.info("wrote %d events to %s", args.count, args.output)


def run(settings: Settings, args) -> None:
    if args.output:
        dump_to_file(args)
        return

    from streamguard.kafka_utils import ensure_topics, make_producer, produce

    stop = {"flag": False}

    def _stop(signum, _frame):
        log.info("signal %s received, flushing and exiting", signum)
        stop["flag"] = True

    signal.signal(signal.SIGINT, _stop)
    signal.signal(signal.SIGTERM, _stop)

    ensure_topics(
        settings.kafka_bootstrap,
        {settings.topic_raw: settings.topic_partitions, settings.topic_dlq: 1},
    )
    producer = make_producer(settings.kafka_bootstrap)
    sim = _sim_from_args(args)
    delivered = {"ok": 0, "failed": 0}

    def on_delivery(err, _msg):
        delivered["failed" if err else "ok"] += 1

    log.info(
        "simulating %d sensors at %.0f events/s (run_id=%s)", args.sensors, args.rate, sim.run_id
    )
    interval = 0.02
    next_tick = time.perf_counter()
    carry = 0.0
    sent = 0
    started = time.monotonic()
    last_report = started
    try:
        while not stop["flag"]:
            if args.duration and time.monotonic() - started >= args.duration:
                break
            if args.count and sent >= args.count:
                break
            sleep_for = next_tick - time.perf_counter()
            if sleep_for > 0:
                time.sleep(sleep_for)
            elif sleep_for < -1.0:
                next_tick = time.perf_counter()  # fell far behind: don't burst to catch up
            next_tick += interval

            carry += args.rate * interval
            n = int(carry)
            carry -= n
            for _ in range(n):
                msg = sim.next_message()
                produce(producer, settings.topic_raw, msg.key, msg.payload, on_delivery)
                sent += 1
            producer.poll(0)

            if time.monotonic() - last_report > 5:
                log.info(
                    "sent=%d delivered=%d failed=%d", sent, delivered["ok"], delivered["failed"]
                )
                last_report = time.monotonic()
    finally:
        remaining = producer.flush(15)
        log.info(
            "done: sent=%d delivered=%d failed=%d undelivered=%d",
            sent,
            delivered["ok"],
            delivered["failed"],
            remaining,
        )
