"""StreamGuard live dashboard.

    streamlit run dashboard/app.py

Two modes, chosen automatically:

* database mode: reads TimescaleDB (+ Prometheus) written by the real Kafka pipeline.
  Environment: POSTGRES_DSN, PROMETHEUS_URL (optional), GRAFANA_URL (optional, for the embed tab).
* demo mode: no services needed; a simulated stream is generated and processed inside this app
  (same pipeline code). Used when POSTGRES_DSN is not set. Force with STREAMGUARD_DEMO=1 (or 0).
"""

from __future__ import annotations

import os
from datetime import UTC, datetime, timedelta

import altair as alt
import pandas as pd
import streamlit as st

_FORCE = os.environ.get("STREAMGUARD_DEMO", "").strip().lower()
DEMO = _FORCE in {"1", "true", "yes"} or (
    _FORCE not in {"0", "false", "no"} and "POSTGRES_DSN" not in os.environ
)
if DEMO:
    import demo_backend as q
else:
    import sg_queries as q

REPO_URL = os.environ.get("REPO_URL", "https://github.com/Ravindi373/StreamGuard")
GRAFANA_URL = "" if DEMO else os.environ.get("GRAFANA_URL", "http://localhost:3000").rstrip("/")

st.set_page_config(page_title="StreamGuard", page_icon="🛡️", layout="wide")


# --------------------------------------------------------------------- sidebar
with st.sidebar:
    st.title("🛡️ StreamGuard")
    st.caption("Real-time anomaly detection")
    if DEMO:
        st.info(
            "**Live demo.** A simulated sensor stream is generated inside this app and "
            "processed by the same pipeline code (de-duplication, watermarks, windows, "
            "detectors) as the full Kafka version, including late, duplicate and malformed events."
        )
        st.link_button("Source code on GitHub", REPO_URL, width="stretch")
    range_label = st.selectbox("Time range", list(q.RANGES), index=1)
    refresh_label = st.selectbox("Auto-refresh", ["Off", "2s", "5s", "10s", "30s"], index=2)
    anchor = (
        False
        if DEMO
        else st.checkbox(
            "Anchor to newest event",
            help="End the window at the newest stored event instead of 'now' (for replayed data).",
        )
    )
    try:
        if DEMO:
            with st.spinner("Warming up the demo stream..."):
                q.ensure_started()
        all_sources = q.list_sources()
    except q.DB_ERRORS as exc:
        st.error(f"Cannot reach the database:\n\n{exc}")
        st.stop()
    selected = st.multiselect("Sources", all_sources, default=all_sources)
    st.button("Refresh now")  # any widget interaction re-runs the app, re-reading the sources

range_s = q.RANGES[range_label]
refresh_s = None if refresh_label == "Off" else refresh_label


def _empty(msg: str = "No data in this window yet.") -> None:
    st.info(msg)


def _values_chart(values: pd.DataFrame, points: pd.DataFrame) -> alt.Chart:
    line = (
        alt.Chart(values)
        .mark_line(strokeWidth=1.4)
        .encode(
            x=alt.X("time:T", title=None),
            y=alt.Y("value:Q", title="value", scale=alt.Scale(zero=False)),
            color=alt.Color("source:N", legend=alt.Legend(orient="bottom", title=None)),
            tooltip=["time:T", "source:N", alt.Tooltip("value:Q", format=".2f")],
        )
    )
    if points.empty:
        return line.properties(height=340)
    marks = (
        alt.Chart(points)
        .mark_point(filled=True, size=90, shape="diamond", color="#e8453c", stroke="white")
        .encode(
            x="time:T",
            y="value:Q",
            tooltip=[
                "time:T",
                "source:N",
                "detector:N",
                alt.Tooltip("value:Q", format=".2f"),
                alt.Tooltip("score:Q", format=".1f", title="z-score"),
            ],
        )
    )
    return alt.layer(line, marks).properties(height=340)


def _prom_metric(col, label: str, expr: str, fmt: str) -> None:
    value = q.prom_instant(expr)
    col.metric(label, "n/a" if value is None else fmt.format(value))


@st.fragment(run_every=refresh_s)
def live() -> None:
    if not selected:
        _empty("Select at least one source in the sidebar.")
        return
    try:
        end = q.window_end(anchor)
        start = end - timedelta(seconds=range_s)
        bucket = q.bucket_seconds(range_s)
        k = q.kpis(start, end, selected)
        values = q.sensor_values(start, end, bucket, selected)
        points = q.anomaly_points(start, end, selected)
        over_time = q.anomalies_over_time(start, end, max(10, bucket * 4), selected)
        by_source = q.anomalies_by_source(start, end, selected)
        recent = q.recent_anomalies(selected)
        windows = q.latest_windows()
    except q.DB_ERRORS as exc:
        st.error(f"Database error: {exc}")
        return

    freshness = None if k["newest"] is None else (datetime.now(UTC) - k["newest"]).total_seconds()
    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric("Events / s (last 60 s)", f"{k['events_per_s']:.1f}")
    c2.metric("Events in window", f"{k['events']:,}")
    c3.metric("Anomalies in window", f"{k['anomalies']:,}")
    c4.metric("Late events", f"{k['late']:,}")
    c5.metric("Newest event age", "n/a" if freshness is None else f"{freshness:.0f} s")

    tabs = ["Overview", "Anomalies", "Pipeline health"] + (["Grafana"] if GRAFANA_URL else [])
    t_over, t_anom, t_health, *t_graf = st.tabs(tabs)

    with t_over:
        left, right = st.columns([2, 1])
        with left:
            st.subheader("Sensor values")
            st.caption(f"Average per {bucket}s bucket · red diamonds are flagged anomalies")
            if values.empty:
                _empty()
            else:
                st.altair_chart(_values_chart(values, points), width="stretch")
        with right:
            st.subheader("Anomalies over time")
            if over_time.empty:
                _empty("No anomalies in this window.")
            else:
                bars = (
                    alt.Chart(over_time)
                    .mark_bar()
                    .encode(
                        x=alt.X("time:T", title=None),
                        y=alt.Y("count:Q", title="anomalies"),
                        color=alt.Color(
                            "detector:N", legend=alt.Legend(orient="bottom", title=None)
                        ),
                        tooltip=["time:T", "detector:N", "count:Q"],
                    )
                    .properties(height=340)
                )
                st.altair_chart(bars, width="stretch")

    with t_anom:
        a, b = st.columns([1, 2])
        with a:
            st.subheader("By source")
            if by_source.empty:
                _empty("No anomalies in this window.")
            else:
                chart = (
                    alt.Chart(by_source)
                    .mark_bar()
                    .encode(
                        y=alt.Y("source:N", title=None),
                        x=alt.X("count:Q", title="anomalies"),
                        color=alt.Color(
                            "detector:N", legend=alt.Legend(orient="bottom", title=None)
                        ),
                    )
                    .properties(height=300)
                )
                st.altair_chart(chart, width="stretch")
        with b:
            st.subheader("Most recent")
            if recent.empty:
                _empty("No anomalies recorded yet.")
            else:
                st.dataframe(
                    recent,
                    hide_index=True,
                    width="stretch",
                    height=300,
                    column_config={
                        "time": st.column_config.DatetimeColumn("time", format="HH:mm:ss.SSS"),
                        "score": st.column_config.NumberColumn("z-score", format="%.1f"),
                    },
                )

    with t_health:
        if DEMO:
            h = q.health()
            st.subheader("Pipeline (in-process)")
            m1, m2, m3, m4 = st.columns(4)
            m1.metric("Events / s (ok)", f"{h['events_per_s']:.1f}")
            m2.metric(
                "p95 batch time",
                "n/a" if h["p95_batch_ms"] is None else f"{h['p95_batch_ms']:.1f} ms",
            )
            m3.metric(
                "Newest event age",
                "n/a" if h["event_age_s"] is None else f"{h['event_age_s']:.1f} s",
            )
            m4.metric("Uptime", f"{h['uptime_s'] / 60:.0f} min")
            c = h["counts"]
            st.caption(
                f"Since start: {c['ok']:,} accepted · {c['late']:,} late · "
                f"{c['duplicate']:,} duplicates dropped · {c['invalid']:,} malformed rejected"
            )
            rejects = q.reject_rates(start, end, bucket)
            no_data_msg = "Collecting data..."
        else:
            st.subheader("Processor (from Prometheus)")
            m1, m2, m3, m4 = st.columns(4)
            _prom_metric(
                m1,
                "Events / s (ok)",
                'sum(rate(streamguard_events_total{status="ok"}[1m]))',
                "{:.1f}",
            )
            _prom_metric(
                m2,
                "p95 batch time",
                "histogram_quantile(0.95, sum(rate(streamguard_batch_seconds_bucket[1m])) by (le))",
                "{:.3f} s",
            )
            _prom_metric(m3, "End-to-end lag", "streamguard_end_to_end_lag_seconds", "{:.1f} s")
            _prom_metric(m4, "Alerts fired (total)", "sum(streamguard_alerts_total)", "{:.0f}")
            rejects = q.prom_range(
                'sum by (status) (rate(streamguard_events_total{status!="ok"}[1m]))',
                start,
                end,
                bucket,
                "status",
            )
            no_data_msg = (
                f"No Prometheus data (is it running at `{q.PROMETHEUS_URL}`?). "
                "Everything outside this tab comes straight from the database."
            )

        if rejects.empty:
            st.caption(no_data_msg)
        else:
            st.markdown("**Late / duplicate / invalid events per second**")
            chart = (
                alt.Chart(rejects)
                .mark_line()
                .encode(
                    x=alt.X("time:T", title=None),
                    y=alt.Y("value:Q", title="events/s"),
                    color=alt.Color("series:N", title=None, legend=alt.Legend(orient="bottom")),
                )
                .properties(height=240)
            )
            st.altair_chart(chart, width="stretch")

        st.subheader("Latest 10 s window per source")
        if windows.empty:
            _empty("No window statistics yet.")
        else:
            st.dataframe(windows, hide_index=True, width="stretch")

    if t_graf:
        with t_graf[0]:
            st.caption(
                f"Embedded from {GRAFANA_URL}. Needs `GF_SECURITY_ALLOW_EMBEDDING=true` "
                "and anonymous viewing (both set in docker-compose.yml)."
            )
            st.iframe(
                f"{GRAFANA_URL}/d/streamguard/streamguard?orgId=1&kiosk&refresh=5s&theme=light",
                height=900,
            )


live()
