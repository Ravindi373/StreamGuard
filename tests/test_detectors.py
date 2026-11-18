import random

import pytest

from streamguard.detectors import EWMADetector, RollingZScoreDetector


def noisy(n, mean=50.0, sd=1.0, seed=0):
    rng = random.Random(seed)
    return [rng.gauss(mean, sd) for _ in range(n)]


@pytest.mark.parametrize("det_cls", [EWMADetector, RollingZScoreDetector])
def test_detects_spike_after_warmup(det_cls):
    det = det_cls(threshold=4.0, warmup=30)
    for x in noisy(300):
        det.observe(x)
    assert det.observe(80.0).is_anomaly


@pytest.mark.parametrize("det_cls", [EWMADetector, RollingZScoreDetector])
def test_no_flags_during_warmup(det_cls):
    det = det_cls(threshold=4.0, warmup=30)
    results = [det.observe(x) for x in noisy(20)]
    results.append(det.observe(1000.0))
    assert not any(r.is_anomaly for r in results)


@pytest.mark.parametrize("det_cls", [EWMADetector, RollingZScoreDetector])
def test_low_false_positive_rate_on_gaussian_noise(det_cls):
    det = det_cls(threshold=4.0, warmup=30)
    flagged = sum(det.observe(x).is_anomaly for x in noisy(20_000, seed=3))
    assert flagged / 20_000 < 0.002


def test_ewma_spike_does_not_mask_following_spike():
    det = EWMADetector(alpha=0.05, threshold=4.0, warmup=30)
    for x in noisy(300):
        det.observe(x)
    assert det.observe(90.0).is_anomaly
    assert det.observe(90.0).is_anomaly  # variance was not inflated by the first spike


def test_constant_signal_then_change_is_flagged_without_error():
    det = EWMADetector()
    for _ in range(100):
        assert not det.observe(5.0).is_anomaly
    assert det.observe(6.0).is_anomaly


def test_rolling_adapts_to_level_shift():
    det = RollingZScoreDetector(window=50, threshold=4.0, warmup=30)
    for x in noisy(200, mean=10):
        det.observe(x)
    shifted = [det.observe(x).is_anomaly for x in noisy(200, mean=30, seed=9)]
    assert shifted[0] and not any(shifted[-20:])  # flagged at first, then re-learned


def test_rolling_resync_keeps_results_stable():
    det = RollingZScoreDetector(window=30, threshold=4.0, warmup=30)
    det._RESYNC_EVERY = 100  # force frequent resync
    for x in noisy(1000, seed=5):
        det.observe(x)
    assert det.observe(80.0).is_anomaly


def test_invalid_params():
    with pytest.raises(ValueError):
        EWMADetector(alpha=0)
    with pytest.raises(ValueError):
        RollingZScoreDetector(window=2)
