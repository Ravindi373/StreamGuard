# Design decisions and trade-offs

## 1. Delivery semantics: at-least-once delivery, exactly-once effects

The processor loop is `consume → validate → process → DB transaction → commit offsets`.

- Offsets are committed **only after** the DB transaction commits. A crash before the commit means the batch is
  re-delivered. Nothing is ever skipped.
- Re-delivery is harmless because every write is idempotent: `events` and `anomalies` use
  `ON CONFLICT DO NOTHING` on natural keys (`event_id`), `window_stats` uses a guarded upsert.
- **Why not Kafka transactions (true exactly-once)?** Kafka's EOS covers Kafka→Kafka. Our sink is a database, so
  Kafka transactions can't span it without a two-phase protocol. Idempotent sinks are simpler, faster, and equally
  correct here. The cost: the sink needs a natural unique key (`event_id`) from the producer.
- The dead-letter producer is flushed **before** the offset commit, so a rejected message is never lost.
  If the DLQ can't be written the processor refuses to commit and crashes (and is restarted).
- Post-commit work (metrics, alerts) is best-effort and can never cause data loss.

## 2. Event time, lateness and watermarks

- `ts` is *event time*, chosen by the producer. Windows and lateness use it, not arrival time.
- **Lateness is per source**: an event is late if it is more than `allowed_lateness_s` behind the newest event
  *of the same source*. Per-source avoids one slow sensor being declared "late" by a fast one.
- **Windows close on a global watermark** (`max event ts seen − allowed lateness`). A window emitted once is
  never re-emitted, so an event that would land in an already-emitted window is treated as late instead of
  producing a second, partial row.
- Late events are **stored** (flagged `is_late`) but **not scored**. Feeding out-of-order points to an online
  detector corrupts its state, and the raw data is still available for batch reprocessing.
- Trade-off: a larger lateness gives fewer late drops but delays window emission and alerts.

## 3. De-duplication

Two layers:
1. A bounded LRU of recent `event_id`s inside the processor stops duplicates from reaching the detectors
   and windows (which would otherwise double-count).
2. The database primary key is the source of truth and also covers duplicates older than the LRU and
   re-delivery after a crash.

## 4. Detectors

| | EWMA z-score | Rolling z-score |
| --- | --- | --- |
| Memory per source | O(1) | O(window) |
| Adapts to a level shift | slowly (outliers are clipped before updating) | after `window` points |
| Spike masking | resists it: a flagged point is clipped before it updates the variance | affected until the spike leaves the window |
| Weakness | needs a sensible α | a burst of anomalies can inflate its own std |

Both score a point against the state **before** absorbing it. They share a warm-up so that startup noise
can't trigger alerts. Flagged points are reported per detector, so the dashboard shows where they agree.

Why not a heavier model (Isolation Forest, LSTM)? For univariate sensor/tick streams, simple online statistics
are fast (100k+ events/s per core), explainable (a z-score is self-describing), and need no training pipeline.
The `Detector` protocol (`observe(x) -> Result`) is the extension point if you want to add a learned model, e.g. with
[River](https://riverml.xyz) streaming algorithms.

## 5. Technology choices

- **Redpanda instead of Kafka+ZooKeeper/KRaft**: one container, Kafka-API compatible, starts in seconds, so
  the stack is easy to run locally. The code only speaks the Kafka protocol; swapping in Apache Kafka is a
  compose change.
- **Plain `confluent-kafka` consumer instead of Faust or Spark Structured Streaming.** the original Faust
  is unmaintained (community forks exist) and Spark needs a JVM cluster that is out of proportion for this workload. A plain consumer
  makes the delivery semantics explicit and keeps the core library-free and testable. The trade-off is that
  state management, windowing, and rebalancing are hand-rolled, which is exactly what this project demonstrates.
  Choose Spark/Flink when you need distributed state, large windows, or SQL.
- **TimescaleDB**: Postgres semantics (primary keys, `ON CONFLICT`) plus hypertables and `time_bucket` for
  time-series queries. The sink uses only standard SQL, so it runs on plain Postgres too (see the tests).
- **Batching** (`SG_BATCH_SIZE`): one DB transaction per Kafka batch amortises round-trips; `executemany` uses
  psycopg3 pipeline mode.

## 6. State and restarts

Detector and window state lives in memory. On restart:

- Detectors re-warm from the replayed/new events (`SG_WARMUP`). Alerts are suppressed during warm-up.
- A window that was open at crash time is rebuilt from only the events seen after restart; the guarded upsert keeps
  the larger existing row, and `events_10s` always has exact numbers.

How to remove this limitation (not implemented): periodically snapshot detector state
(`mean`, `var`, `n`, window buffer) to a compacted Kafka topic keyed by source, and restore it before consuming.
Snapshot offsets together with the state so replay starts from the matching position.

## 7. Back-pressure and failure handling

- Producer: `produce()` waits on a full local queue (`BufferError → poll`) instead of dropping.
- Database: transient errors retry with exponential backoff (capped at 10 s, 8 attempts); the offset is not
  committed meanwhile, so Kafka buffers. After the retries the process exits and Compose restarts it.
- Consumer lag is visible as end-to-end lag in Grafana.
- Alert delivery runs in a small thread pool and swallows errors: a broken webhook can't stall the pipeline.

## 8. What I'd do next

1. Checkpoint detector state (section 6).
2. Per-source adaptive thresholds and a seasonal baseline (STL / Holt-Winters) for strongly periodic signals.
3. Schema registry (Avro/Protobuf) in place of JSON + Pydantic.
4. Replace processor-side `window_stats` with a Timescale continuous aggregate over `events`.
5. Kubernetes manifests with a consumer-group autoscaler driven by lag.
