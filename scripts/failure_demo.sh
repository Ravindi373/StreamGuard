#!/usr/bin/env bash
# Failure-injection demo: kill the processor mid-stream, bring it back, prove no data loss.
set -euo pipefail

echo "==> starting stack"
docker compose up -d --build
echo "==> letting data flow for 20s"; sleep 20

echo "==> KILLING the processor (simulator keeps producing)"
# Docker's restart policy may bring it back immediately; the later `start` is a no-op then.
docker compose kill processor
sleep 15

echo "==> restarting the processor"
docker compose start processor
sleep 20

echo "==> stopping the simulator gracefully (flushes in-flight messages)"
docker compose stop simulator

echo "==> verifying: no gaps in any sensor's sequence"
docker compose run --rm --no-deps --entrypoint streamguard processor verify --settle 10
