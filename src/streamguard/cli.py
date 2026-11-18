"""Command line entry point: `streamguard <command>` (or `python -m streamguard <command>`)."""

from __future__ import annotations

import argparse
import logging
import sys

from streamguard import __version__
from streamguard.config import load_settings


def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="streamguard", description=__doc__)
    p.add_argument("--version", action="version", version=__version__)
    sub = p.add_subparsers(dest="command", required=True)

    sub.add_parser("processor", help="run the stream processor (Kafka -> detectors -> DB)")

    s = sub.add_parser("simulate", help="publish simulated sensor events")
    s.add_argument("--rate", type=float, default=50.0, help="events per second (all sensors)")
    s.add_argument("--sensors", type=int, default=5)
    s.add_argument("--duration", type=float, default=0, help="stop after N seconds (0 = forever)")
    s.add_argument("--count", type=int, default=0, help="stop after N events (0 = unlimited)")
    s.add_argument("--fault-rate", type=float, default=0.002, help="P(fault) per event")
    s.add_argument("--late-prob", type=float, default=0.01, help="P(late arrival) per event")
    s.add_argument("--dup-prob", type=float, default=0.005, help="P(duplicate delivery)")
    s.add_argument("--malformed-prob", type=float, default=0.005, help="P(malformed payload)")
    s.add_argument("--seed", type=int, default=None)
    s.add_argument("--output", help="write JSONL to this file instead of Kafka")

    lv = sub.add_parser("live", help="stream real Binance trades (needs `pip install .[live]`)")
    lv.add_argument("--symbols", nargs="+", default=["BTCUSDT", "ETHUSDT"])
    lv.add_argument("--value", choices=["return", "price"], default="return")

    r = sub.add_parser("replay", help="re-publish a JSONL file of events")
    r.add_argument("file")
    r.add_argument("--speed", type=float, default=0, help="1 = real time, 0 = as fast as possible")

    v = sub.add_parser("verify", help="check the database for missing simulator events")
    v.add_argument("--settle", type=float, default=0, help="wait until counts are stable N seconds")

    b = sub.add_parser("bench", help="in-process throughput benchmark (no Kafka/DB)")
    b.add_argument("--count", type=int, default=200_000)

    e = sub.add_parser("evaluate", help="detector precision/recall on labelled synthetic data")
    e.add_argument("--count", type=int, default=200_000)
    e.add_argument("--seed", type=int, default=42)
    e.add_argument("--threshold", type=float, default=4.0)
    return p


def main(argv: list[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    settings = load_settings()
    logging.basicConfig(
        level=getattr(logging, settings.log_level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)-7s %(name)s: %(message)s",
    )

    if args.command == "processor":
        from streamguard import processor

        processor.run(settings)
    elif args.command == "simulate":
        from streamguard import simulate

        simulate.run(settings, args)
    elif args.command == "live":
        from streamguard import live

        live.run(settings, args)
    elif args.command == "replay":
        from streamguard import replay

        replay.run(settings, args)
    elif args.command == "verify":
        from streamguard import verify

        return verify.run(settings.postgres_dsn, args.settle)
    elif args.command == "bench":
        from streamguard import bench

        bench.print_report(bench.run(args.count))
    elif args.command == "evaluate":
        from streamguard import evaluate

        evaluate.print_report(
            evaluate.evaluate(count=args.count, seed=args.seed, threshold=args.threshold)
        )
    return 0


if __name__ == "__main__":
    sys.exit(main())
