from datetime import UTC, datetime

from streamguard.alerts import AlertManager
from streamguard.schemas import AnomalyRow


def anomaly(source="s1"):
    return AnomalyRow("id", source, datetime(2026, 1, 1, tzinfo=UTC), 99.0, 7.5, "ewma")


def test_cooldown_per_source():
    now = {"t": 0.0}
    am = AlertManager(cooldown_s=60, clock=lambda: now["t"])
    assert am.maybe_alert(anomaly("a"))
    assert not am.maybe_alert(anomaly("a"))  # within cooldown
    assert am.maybe_alert(anomaly("b"))  # different source
    now["t"] = 61
    assert am.maybe_alert(anomaly("a"))  # cooldown elapsed
    am.close()


def test_webhook_receives_payload_and_failures_are_swallowed():
    sent = []

    def sender(url, payload):
        sent.append((url, payload))
        raise RuntimeError("network down")

    am = AlertManager("http://hook", cooldown_s=0, sender=sender)
    assert am.maybe_alert(anomaly())
    am.close()  # waits for the worker
    assert sent and sent[0][0] == "http://hook" and "anomaly on s1" in sent[0][1]["text"]


def test_no_webhook_means_no_send():
    sent = []
    am = AlertManager("", cooldown_s=0, sender=lambda u, p: sent.append(p))
    assert am.maybe_alert(anomaly())
    am.close()
    assert sent == []
