"""Alerting with per-source cooldown so a burst of anomalies sends one notification."""

from __future__ import annotations

import json
import logging
import time
import urllib.request
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor

from streamguard.schemas import AnomalyRow

log = logging.getLogger(__name__)

Sender = Callable[[str, dict], None]


def _post_json(url: str, payload: dict) -> None:
    req = urllib.request.Request(
        url,
        data=json.dumps(payload).encode("utf-8"),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=5) as resp:  # noqa: S310 (URL is operator-configured)
        resp.read()


class AlertManager:
    def __init__(
        self,
        webhook_url: str = "",
        cooldown_s: float = 60.0,
        sender: Sender | None = None,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self.webhook_url = webhook_url
        self.cooldown_s = cooldown_s
        self._sender = sender or _post_json
        self._clock = clock
        self._last: dict[str, float] = {}
        self._pool = ThreadPoolExecutor(max_workers=2, thread_name_prefix="alert")

    def maybe_alert(self, a: AnomalyRow) -> bool:
        """Fire an alert unless this source alerted within the cooldown. Returns True if fired."""
        now = self._clock()
        last = self._last.get(a.source)
        if last is not None and now - last < self.cooldown_s:
            return False
        self._last[a.source] = now

        text = (
            f":rotating_light: anomaly on {a.source} ({a.detector}): "
            f"value={a.value:.4g} score={a.score:.1f} at {a.ts.isoformat()}"
        )
        log.warning(text)
        if self.webhook_url:
            self._pool.submit(self._send, {"text": text})
        return True

    def _send(self, payload: dict) -> None:
        try:
            self._sender(self.webhook_url, payload)
        except Exception as exc:  # alert delivery must never break the pipeline
            log.error("alert delivery failed: %s", exc)

    def close(self) -> None:
        self._pool.shutdown(wait=True)
