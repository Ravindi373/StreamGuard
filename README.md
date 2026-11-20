# StreamGuard

A real-time streaming pipeline in Python: it ingests an event stream, validates and de-duplicates it,
handles late data with event-time watermarks, detects anomalies online with two complementary detectors,
persists everything idempotently to TimescaleDB, and exposes live dashboards and metrics.

```
 simulator / Binance feed ──▶ Redpanda (Kafka API) ──▶ processor ──▶ TimescaleDB ──▶ Grafana
   (faults, late, dupes,        events.raw            │  validate        events          dashboards
    malformed injected)                               │  dedupe          anomalies
                                                      │  windows         window_stats
                                                      │  detectors
                                                      ├──▶ events.dlq  (rejected payloads + reason)
                                                      ├──▶ Prometheus  (throughput, latency, lag)
                                                      └──▶ Slack-style webhook alerts (with cooldown)
```

```mermaid
flowchart LR
    SIM[Sensor simulator] --> K[(Redpanda<br/>events.raw)]
    LIVE[Binance live feed] --> K
    K --> P[Processor]
    P -->|invalid| DLQ[(events.dlq)]
    P --> DB[(TimescaleDB)]
    P -->|/metrics| PROM[Prometheus]
    P -->|webhook| SLACK[Alerts]
    DB --> G[Grafana]
    PROM --> G
```

## Highlights

- **Correct under failure.** Offsets are committed only after the database transaction succeeds, and every write
  is idempotent, so a crash causes re-processing but never loss or duplicates
  (at-least-once delivery, exactly-once *effects*). `scripts/failure_demo.*` kills the processor mid-stream
  and proves no gaps remain.
- **Real stream semantics.** Event-time tumbling windows, per-source lateness, a global watermark,
  bounded de-duplication, and a dead-letter topic for malformed input.
- **Two online detectors** (single pass, O(1) per event): an exponentially-weighted z-score with
  outlier-clipped updates, and a rolling-window z-score. They fail differently, so using both is informative.
- **Observable.** Prometheus metrics (throughput, p95 batch time, end-to-end lag, rejects/late/duplicates),
  pre-provisioned Grafana dashboard, alert cooldowns.
- **Testable core.** The processing logic (`pipeline.py`) has no Kafka or DB dependency;
  the simulator is seeded and labels its own faults so detector quality is *measured*, not assumed.

## Quickstart

Requirements: Docker Desktop (Compose v2). Python 3.11+ (3.12 recommended) only if you want to run the CLI on your host.

```powershell
# PowerShell (Windows)
docker compose up -d --build
```
```bash
# bash / macOS / Linux
docker compose up -d --build
```

| What | URL |
| --- | --- |
| **Streamlit dashboard** (native charts + embedded Grafana tab) | http://localhost:8501 |
| Grafana dashboard (login `admin` / `admin`, anonymous viewing enabled) | http://localhost:3000 |
| Prometheus | http://localhost:9090 |
| Processor metrics | http://localhost:8000/metrics |
| Kafka API (from your host) | `localhost:19092` |
| TimescaleDB | `localhost:5432` (user/password/db: `streamguard`) |

The `simulator` service starts automatically (50 events/s, 5 sensors, with ~1% late, 0.5% duplicate and
0.5% malformed messages mixed in). Watch the **StreamGuard** dashboard populate.

Stream real market data too (Binance trades, converted to tick-to-tick returns in basis points):

```bash
docker compose --profile live up -d      # US users: set BINANCE_WS_BASE=wss://stream.binance.us:9443
```

Inspect results:

```bash
docker compose exec timescaledb psql -U streamguard -c \
  "SELECT ts, source, detector, round(value::numeric,2) v, round(score::numeric,1) z FROM anomalies ORDER BY ts DESC LIMIT 10"
docker compose exec timescaledb psql -U streamguard -c "SELECT * FROM events_10s ORDER BY bucket DESC LIMIT 5"
```

Stop everything: `docker compose down` (add `-v` to wipe data).

### Streamlit dashboard

`docker compose up -d --build` also starts a Streamlit app at **http://localhost:8501** that reads the same
TimescaleDB (and Prometheus for pipeline health). It offers a time-range picker, auto-refresh, per-source filters,
sensor values with anomalies overlaid, anomaly breakdowns, processor health, and a **Grafana** tab that embeds
the Grafana dashboard.

Run it on your host instead (hot-reload while you edit):

```powershell
pip install -e ".[dashboard]"
$env:POSTGRES_DSN="postgresql://streamguard:streamguard@localhost:5432/streamguard"
$env:PROMETHEUS_URL="http://localhost:9090"
streamlit run dashboard/app.py
```

Deploying publicly: Streamlit Community Cloud cannot reach a database on your laptop. Point `POSTGRES_DSN` at a
hosted Postgres/TimescaleDB (Timescale Cloud, Neon, Supabase, ...) and run the processor against it; the dashboard
then works anywhere. Omit `PROMETHEUS_URL` / `GRAFANA_URL` if those aren't exposed, and the app hides those panels
gracefully.

### Running the CLI on your host

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1          # bash: source .venv/bin/activate
pip install -e ".[dev,live]"
streamguard --help
```

| Command | Purpose |
| --- | --- |
| `streamguard processor` | Run the stream processor |
| `streamguard simulate --rate 5000 --duration 60` | Load-generate into Kafka |
| `streamguard simulate --count 100000 --output data.jsonl` | Write a dataset to a file (no Kafka) |
| `streamguard replay data.jsonl --speed 10` | Replay a file into Kafka (`--speed 0` = as fast as possible) |
| `streamguard live --symbols BTCUSDT ETHUSDT` | Stream real Binance trades |
| `streamguard verify --settle 10` | Gap check: prove no simulator events were lost |
| `streamguard evaluate` | Detector precision/recall on labelled synthetic data |
| `streamguard bench` | In-process throughput (no Kafka/DB) |

## Failure-injection demo

```powershell
.\scripts\failure_demo.ps1        # bash: ./scripts/failure_demo.sh
```

It starts the stack, **kills the processor with `docker compose kill`** while the simulator keeps producing,
restarts it, stops the simulator gracefully, then runs `streamguard verify`, which checks every
`(sensor, run)` sequence for gaps. Expected ending:

```
TOTAL stored=… missing=0
PASS: no gaps
```

Why it passes: Kafka retains what the dead processor didn't consume; the restarted processor resumes from the
last committed offset; the batch that was in flight at the kill is simply re-processed and the
`ON CONFLICT DO NOTHING` inserts absorb the duplicates.

## Configuration

All via environment variables (see `.env.example`).

| Variable | Default | Meaning |
| --- | --- | --- |
| `KAFKA_BOOTSTRAP` | `localhost:19092` | Broker address (`redpanda:9092` inside Compose) |
| `POSTGRES_DSN` | `postgresql://streamguard:streamguard@localhost:5432/streamguard` | Database |
| `SG_WINDOW_SECONDS` | `10` | Tumbling window size (event time) |
| `SG_ALLOWED_LATENESS_S` | `5` | An event this far behind its source's newest event is "late" |
| `SG_Z_THRESHOLD` | `4.0` | Anomaly threshold in standard deviations |
| `SG_EWMA_ALPHA` | `0.05` | EWMA smoothing (smaller = longer memory) |
| `SG_WARMUP` | `30` | Observations per source before a detector may flag |
| `SG_ROLLING_WINDOW` | `60` | Rolling detector window size |
| `SG_ALERT_WEBHOOK_URL` | *(empty)* | Slack-compatible webhook; empty = log only |
| `SG_ALERT_COOLDOWN_S` | `60` | Min seconds between alerts per source |
| `SG_BATCH_SIZE` / `SG_BATCH_TIMEOUT_S` | `500` / `1.0` | Consume batching |

## Data model

| Table | Content | Key |
| --- | --- | --- |
| `events` (hypertable on `ts`) | Every accepted event, `is_late` flag | `(event_id, ts)` |
| `anomalies` | One row per detector that fired | `(event_id, detector)` |
| `window_stats` | Processor-computed 10 s window aggregates | `(source, window_start)` |
| `events_10s` (view) | Exact aggregates recomputed from raw events | n/a |

## Measured results

> These numbers come from running the commands in this repo on a single shared sandbox core,
> so treat them as indicative and re-run on your hardware (and paste your own into this table).

**Detector quality**: `streamguard evaluate` (200,000 labelled events, 5 sensors, seed 42, threshold 4σ):

| Detector | Precision | Spike recall | Level-shift onset recall | False-positive rate |
| --- | ---: | ---: | ---: | ---: |
| `ewma` (α = 0.05) | 0.872 | 0.840 | 0.613 | 0.023 % |
| `rolling_z` (window 60) | 0.890 | 0.830 | 0.629 | 0.022 % |
| either (union) | 0.846 | 0.847 | 0.677 | 0.033 % |

Spikes are injected at 3–12σ and shifts at 3–6σ, so the misses are mostly the low-magnitude faults that are
statistically indistinguishable from noise at a 4σ threshold. "Sustained" recall (every point *inside* a shift)
is near zero by design: both detectors re-learn the new level, which is the desired behaviour for a level shift.
Lowering `SG_Z_THRESHOLD` trades precision for recall; try `streamguard evaluate --threshold 3`.

**Processing throughput** (`streamguard bench`, 200,000 events, no I/O, one core):

| Stage | Events/s |
| --- | ---: |
| JSON parse + schema validation | ~190,000 |
| Windows + both detectors | ~176,000 |
| Combined | ~91,000 |

**End-to-end** (Kafka + DB included): not pre-filled because it depends on your machine. To measure:

```bash
docker compose stop simulator
docker compose run --rm simulator simulate --rate 5000 --duration 120 --late-prob 0 --dup-prob 0 --malformed-prob 0
```
then read *Events / s*, *p95 batch time* and *End-to-end lag* on the dashboard and record them here:

| Rate offered | Sustained events/s | p95 batch time | Max lag |
| ---: | ---: | ---: | ---: |
| 1,000 | | | |
| 5,000 | | | |
| 10,000 | | | |

## Testing

```bash
pytest                                              # unit tests, no services needed
docker compose up -d --build
RUN_INTEGRATION=1 pytest -m integration -o addopts=""   # end-to-end + sink idempotency
ruff check src tests dashboard && ruff format --check src tests dashboard
```

CI (`.github/workflows/ci.yml`) runs lint, unit tests on Python 3.11 and 3.12, a Docker build,
and the integration tests against the real stack.

## Known limitations

- **Detector and window state is in memory.** After a restart, detectors re-warm (`SG_WARMUP` points) and
  any window that spanned the restart is written with only the post-restart events. The `window_stats` upsert
  never lets a smaller count replace a larger one, and the exact numbers are always available in `events_10s`.
  See `docs/DESIGN.md` for how to checkpoint state.
- **Windows close on event time.** If *all* sources stop, the last windows stay open until new events arrive.
- **Single processor instance** in Compose. Horizontal scaling works at the Kafka level (3 partitions, keyed by
  source, so run up to 3 processors in the same consumer group) but state isn't rebalanced between instances.
- Replication factor is 1 (single-node local Redpanda).

## Layout

```
src/streamguard/
  pipeline.py     pure processing core (dedupe, lateness, windows, detectors)
  detectors.py    EWMA and rolling z-score detectors
  windows.py      event-time tumbling windows
  processor.py    Kafka consume loop, DLQ, commit-after-write
  storage.py      idempotent batched Postgres writes with retry/backoff
  simulator.py    seeded, fault-labelled data generator
  live.py         Binance websocket -> Kafka
  evaluate.py     precision/recall harness   bench.py   throughput harness
dashboard/        Streamlit app (app.py) + data layer (sg_queries.py) + Dockerfile
docker/           Postgres schema, Prometheus, Grafana provisioning + dashboard
scripts/          failure-injection demo (PowerShell + bash)
tests/            unit tests;  tests/integration/ needs the compose stack
docs/DESIGN.md    design decisions and trade-offs
```
