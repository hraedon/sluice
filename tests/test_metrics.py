"""Tests for per-client metrics tracking (WI-023 feature #4)."""

from __future__ import annotations

from sluice.metrics import ClientMetrics


def test_default_label_when_none():
    m = ClientMetrics()
    m.record_forwarded(None)
    m.record_success(None)
    d = m.to_dict()
    assert "default" in d
    assert d["default"]["forwarded"] == 1
    assert d["default"]["succeeded"] == 1


def test_explicit_label():
    m = ClientMetrics()
    m.record_forwarded("opencode")
    m.record_forwarded("opencode")
    m.record_success("opencode")
    m.record_concurrency_429("opencode")
    d = m.to_dict()
    assert d["opencode"]["forwarded"] == 2
    assert d["opencode"]["succeeded"] == 1
    assert d["opencode"]["concurrency_429"] == 1


def test_multiple_labels():
    m = ClientMetrics()
    m.record_forwarded("opencode")
    m.record_forwarded("hermes")
    m.record_forwarded("open-webui")
    d = m.to_dict()
    assert len(d) == 3
    assert d["opencode"]["forwarded"] == 1
    assert d["hermes"]["forwarded"] == 1
    assert d["open-webui"]["forwarded"] == 1


def test_overflow_when_max_labels_exceeded():
    m = ClientMetrics(max_labels=3)
    m.record_forwarded("a")
    m.record_forwarded("b")
    m.record_forwarded("c")
    m.record_forwarded("d")  # should go to overflow
    d = m.to_dict()
    assert len(d) == 4  # 3 tracked labels + overflow
    assert "__overflow__" in d
    assert d["__overflow__"]["forwarded"] == 1


def test_overflow_accumulates():
    m = ClientMetrics(max_labels=2)
    m.record_forwarded("a")
    m.record_forwarded("b")
    m.record_forwarded("c")
    m.record_forwarded("d")
    m.record_forwarded("e")
    d = m.to_dict()
    assert "__overflow__" in d
    assert d["__overflow__"]["forwarded"] == 3


def test_all_counter_types():
    m = ClientMetrics()
    m.record_forwarded("client")
    m.record_success("client")
    m.record_concurrency_429("client")
    m.record_rate_limit_429("client")
    m.record_gateway_429("client")
    m.record_queue_timeout("client")
    d = m.to_dict()
    c = d["client"]
    assert c["forwarded"] == 1
    assert c["succeeded"] == 1
    assert c["concurrency_429"] == 1
    assert c["rate_limit_429"] == 1
    assert c["gateway_429"] == 1
    assert c["queue_timeouts"] == 1


def test_to_prometheus_renders_metrics():
    m = ClientMetrics()
    m.record_forwarded("opencode")
    m.record_concurrency_429("opencode")
    m.record_forwarded("hermes")
    text = m.to_prometheus()
    assert "sluice_client_forwarded" in text
    assert 'label="opencode"' in text
    assert 'label="hermes"' in text
    assert "sluice_client_concurrency_429" in text
    assert "# TYPE sluice_client_forwarded counter" in text


def test_to_prometheus_empty():
    m = ClientMetrics()
    assert m.to_prometheus() == ""


def test_label_count():
    m = ClientMetrics()
    assert m.label_count == 0
    m.record_forwarded("a")
    assert m.label_count == 1
    m.record_forwarded("b")
    assert m.label_count == 2
    m.record_forwarded("a")  # existing label
    assert m.label_count == 2


def test_overflow_not_shown_when_empty():
    m = ClientMetrics(max_labels=2)
    m.record_forwarded("a")
    m.record_forwarded("b")
    d = m.to_dict()
    assert "__overflow__" not in d
