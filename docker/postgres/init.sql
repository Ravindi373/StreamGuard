CREATE EXTENSION IF NOT EXISTS timescaledb;

-- Raw events: a hypertable partitioned by event time. The primary key makes every
-- insert idempotent (ON CONFLICT DO NOTHING), which is what lets the processor use
-- at-least-once delivery and still end up with exactly-once effects.
CREATE TABLE IF NOT EXISTS events (
    event_id    text             NOT NULL,
    source      text             NOT NULL,
    ts          timestamptz      NOT NULL,
    value       double precision NOT NULL,
    is_late     boolean          NOT NULL DEFAULT false,
    ingested_at timestamptz      NOT NULL DEFAULT now(),
    PRIMARY KEY (event_id, ts)
);
SELECT create_hypertable('events', 'ts', if_not_exists => TRUE);
CREATE INDEX IF NOT EXISTS events_source_ts_idx ON events (source, ts DESC);

CREATE TABLE IF NOT EXISTS anomalies (
    event_id    text             NOT NULL,
    detector    text             NOT NULL,
    source      text             NOT NULL,
    ts          timestamptz      NOT NULL,
    value       double precision NOT NULL,
    score       double precision NOT NULL,
    detected_at timestamptz      NOT NULL DEFAULT now(),
    PRIMARY KEY (event_id, detector)
);
CREATE INDEX IF NOT EXISTS anomalies_ts_idx ON anomalies (ts DESC);

-- Low-latency window aggregates computed in the processor (event-time tumbling windows).
CREATE TABLE IF NOT EXISTS window_stats (
    source       text             NOT NULL,
    window_start timestamptz      NOT NULL,
    window_end   timestamptz      NOT NULL,
    n            bigint           NOT NULL,
    mean         double precision NOT NULL,
    std          double precision NOT NULL,
    min_value    double precision NOT NULL,
    max_value    double precision NOT NULL,
    PRIMARY KEY (source, window_start)
);

-- Exact aggregates straight from raw events (unaffected by processor restarts).
CREATE OR REPLACE VIEW events_10s AS
SELECT time_bucket('10 seconds', ts) AS bucket,
       source,
       count(*)                      AS n,
       avg(value)                    AS mean,
       stddev_samp(value)            AS std,
       min(value)                    AS min_value,
       max(value)                    AS max_value
FROM events
GROUP BY 1, 2;
