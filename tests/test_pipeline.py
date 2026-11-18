import random

from conftest import T0

from streamguard.pipeline import Pipeline


def feed(pipe, make_event, n, source="s1", start=0, noise=0.5, seed=1):
    rng = random.Random(seed)
    out_all = []
    events = [
        make_event(
            start + i,
            source=source,
            value=20 + rng.gauss(0, noise),
            event_id=f"{source}:{start + i}",
        )
        for i in range(n)
    ]
    for i in range(0, n, 50):
        out_all.append(pipe.process_batch(events[i : i + 50]))
    return out_all


def test_duplicates_are_dropped(make_event):
    pipe = Pipeline()
    ev = make_event(1)
    out = pipe.process_batch([ev, ev, make_event(2)])
    assert out.duplicates == 1 and len(out.events) == 2


def test_late_events_are_stored_but_not_scored(make_event):
    pipe = Pipeline(allowed_lateness_s=5)
    pipe.process_batch([make_event(i) for i in range(100, 110)])
    late = make_event(0, at=50, value=9999.0, event_id="s1:late")  # 60s behind newest
    out = pipe.process_batch([late])
    assert out.late == 1
    assert out.events[0].is_late and out.anomalies == []


def test_spike_is_detected_once_warmed_up(make_event):
    pipe = Pipeline()
    feed(pipe, make_event, 200)
    out = pipe.process_batch([make_event(1000, value=60.0, event_id="s1:spike")])
    assert {a.detector for a in out.anomalies} == {"ewma", "rolling_z"}
    assert all(a.event_id == "s1:spike" for a in out.anomalies)


def test_windows_emitted_as_watermark_advances(make_event):
    pipe = Pipeline(window_seconds=10, allowed_lateness_s=5)
    outs = feed(pipe, make_event, 60)
    windows = [w for o in outs for w in o.windows]
    assert windows, "expected closed windows"
    assert all(w.n == 10 for w in windows)  # 1 event/s, 10s windows
    assert windows[0].window_start == T0


def test_sources_are_scored_independently(make_event):
    pipe = Pipeline()
    feed(pipe, make_event, 200, source="a")
    feed(pipe, make_event, 200, source="b", noise=5.0, seed=2)
    out = pipe.process_batch([make_event(1000, source="a", value=26.0, event_id="a:x")])
    assert out.anomalies  # +6 is huge for source a (sd 0.5)...
    out = pipe.process_batch([make_event(1000, source="b", value=26.0, event_id="b:x")])
    assert not out.anomalies  # ...but normal for noisy source b (sd 5)


def test_dedup_cache_is_bounded(make_event):
    pipe = Pipeline(dedup_capacity=10)
    pipe.process_batch([make_event(i) for i in range(100)])
    assert len(pipe._seen) == 10


def test_event_for_closed_window_counts_as_late(make_event):
    pipe = Pipeline(window_seconds=10, allowed_lateness_s=5)
    # Source b runs ahead and drags the global watermark past source a's old window.
    pipe.process_batch([make_event(1, source="a"), make_event(100, source="b")])
    out = pipe.process_batch([make_event(2, source="a", event_id="a:late-ish")])
    assert out.late == 1 and out.events[0].is_late
