"""Per-client metrics keyed by ``x-sluice-client-label``.

Tracks request-level counters (forwarded, succeeded, 429, queue-timeout)
per client label so operators can see which clients are driving traffic
and which are hitting limits — without modifying any client.

The label comes from the ``x-sluice-client-label`` header that clients
already send for QoS reserve (Plan 005).  When absent, requests are
attributed to ``"default"``.  The number of tracked labels is bounded
to prevent unbounded memory growth from adversarial or misconfigured
clients sending unique labels.

This is a **shell-level** module: it holds mutable state and is not part
of the pure core.  It performs no I/O and reads no clock.
"""

from __future__ import annotations

from dataclasses import dataclass, field

_DEFAULT_LABEL = "default"
_MAX_LABELS = 32
_MAX_LABEL_LEN = 64


def _sanitize_label(label: str) -> str:
    """Truncate and normalise a client label (H-3).

    Truncates to ``_MAX_LABEL_LEN`` chars to prevent memory exhaustion from
    adversarial header values.  An empty string is normalised to ``"default"``
    (L-1).
    """
    if not label:
        return _DEFAULT_LABEL
    return label[:_MAX_LABEL_LEN]


@dataclass
class LabelCounters:
    """Per-label request counters."""

    forwarded: int = 0
    succeeded: int = 0
    concurrency_429: int = 0
    rate_limit_429: int = 0
    gateway_429: int = 0
    queue_timeouts: int = 0


@dataclass
class ClientMetrics:
    """Bounded per-label metrics tracker.

    Tracks request-level counters keyed by the ``x-sluice-client-label``
    header value.  When the number of distinct labels exceeds
    ``max_labels``, new labels are collapsed into ``"overflow"`` to
    prevent unbounded memory growth.
    """

    max_labels: int = _MAX_LABELS
    _counters: dict[str, LabelCounters] = field(default_factory=dict)
    _overflow: LabelCounters = field(default_factory=LabelCounters)

    def _get(self, label: str | None) -> LabelCounters:
        sanitized = _sanitize_label(label or "")
        if sanitized in self._counters:
            return self._counters[sanitized]
        if len(self._counters) < self.max_labels:
            c = LabelCounters()
            self._counters[sanitized] = c
            return c
        return self._overflow

    def record_forwarded(self, label: str | None) -> None:
        self._get(label or _DEFAULT_LABEL).forwarded += 1

    def record_success(self, label: str | None) -> None:
        self._get(label or _DEFAULT_LABEL).succeeded += 1

    def record_concurrency_429(self, label: str | None) -> None:
        self._get(label or _DEFAULT_LABEL).concurrency_429 += 1

    def record_rate_limit_429(self, label: str | None) -> None:
        self._get(label or _DEFAULT_LABEL).rate_limit_429 += 1

    def record_gateway_429(self, label: str | None) -> None:
        self._get(label or _DEFAULT_LABEL).gateway_429 += 1

    def record_queue_timeout(self, label: str | None) -> None:
        self._get(label or _DEFAULT_LABEL).queue_timeouts += 1

    def to_dict(self) -> dict[str, dict[str, int]]:
        """Serialise to ``{label: {forwarded, succeeded, ...}}`` for JSON."""
        result: dict[str, dict[str, int]] = {}
        for label, c in self._counters.items():
            result[label] = {
                "forwarded": c.forwarded,
                "succeeded": c.succeeded,
                "concurrency_429": c.concurrency_429,
                "rate_limit_429": c.rate_limit_429,
                "gateway_429": c.gateway_429,
                "queue_timeouts": c.queue_timeouts,
            }
        if any(getattr(self._overflow, attr) > 0 for attr in (
            "forwarded", "succeeded", "concurrency_429",
            "rate_limit_429", "gateway_429", "queue_timeouts",
        )):
            result["__overflow__"] = {
                "forwarded": self._overflow.forwarded,
                "succeeded": self._overflow.succeeded,
                "concurrency_429": self._overflow.concurrency_429,
                "rate_limit_429": self._overflow.rate_limit_429,
                "gateway_429": self._overflow.gateway_429,
                "queue_timeouts": self._overflow.queue_timeouts,
            }
        return result

    def to_prometheus(self) -> str:
        """Render per-label counters as Prometheus text exposition."""
        lines: list[str] = []
        metrics = (
            ("sluice_client_forwarded", "Total requests forwarded per client label", "forwarded"),
            ("sluice_client_succeeded", "Total requests succeeded per client label", "succeeded"),
            ("sluice_client_concurrency_429", "Total concurrency 429s per client label", "concurrency_429"),
            ("sluice_client_rate_limit_429", "Total rate-limit 429s per client label", "rate_limit_429"),
            ("sluice_client_gateway_429", "Total gateway 429s per client label", "gateway_429"),
            ("sluice_client_queue_timeouts", "Total queue timeouts per client label", "queue_timeouts"),
        )
        for name, help_text, attr in metrics:
            has_data = any(getattr(c, attr) > 0 for c in self._counters.values()) or getattr(self._overflow, attr) > 0
            if not has_data:
                continue
            lines.append(f"# HELP {name} {help_text}")
            lines.append(f"# TYPE {name} counter")
            for label, c in self._counters.items():
                val = getattr(c, attr)
                if val > 0:
                    escaped = label.replace("\\", "\\\\").replace('"', '\\"').replace("\n", "\\n")
                    lines.append(f'{name}{{label="{escaped}"}} {val}')
            ov = getattr(self._overflow, attr)
            if ov > 0:
                lines.append(f'{name}{{label="__overflow__"}} {ov}')
        return "\n".join(lines) + "\n" if lines else ""

    @property
    def label_count(self) -> int:
        return len(self._counters)
