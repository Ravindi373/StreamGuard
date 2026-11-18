from streamguard import bench
from streamguard.evaluate import evaluate


def test_detectors_catch_most_spikes_with_few_false_positives():
    r = evaluate(count=60_000, seed=42)
    for name in ("ewma", "rolling_z"):
        assert r[name]["spike_recall"] > 0.7
        assert r[name]["false_positive_rate"] < 0.002
        assert r[name]["precision"] > 0.6


def test_bench_runs():
    r = bench.run(count=5_000, sensors=3)
    assert r["combined_eps"] > 0
