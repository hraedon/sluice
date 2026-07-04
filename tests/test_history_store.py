"""Tests for the SQLite history store, store integration, and bug-fix regressions."""

from __future__ import annotations

import asyncio
import os
import tempfile

import httpx

from sluice.control import (
    BreakerConfig,
    ControllerConfig,
    UsageReading,
)
from sluice.gate import PermitGate
from sluice.history import History, HistoryEntry
from sluice.history_store import SQLiteHistoryStore
from sluice.proxy import ProxyApp
from sluice.reconcile import ReconciliationLoop
from sluice.usage import CachedReading


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

CFG = ControllerConfig(target=3, min_floor=1, usage_fresh_ttl=15.0, stale_penalty=1, low_penalty=1, phantom_window=3)
BCFG = BreakerConfig(threshold=5, window_seconds=300.0, cooldown_seconds=60.0)


class FakeUsageClient:
    def __init__(self, reading: UsageReading) -> None:
        self._reading = reading
        self._fail = False
        self._last_ok_mono = 0.0

    async def fetch(self, *, now_monotonic: float) -> CachedReading:
        if self._fail:
            return CachedReading(
                reading=self._reading,
                fetched_at_monotonic=self._last_ok_mono,
                ok=False,
            )
        self._last_ok_mono = now_monotonic
        return CachedReading(
            reading=self._reading,
            fetched_at_monotonic=now_monotonic,
            ok=True,
        )

    def set_reading(self, reading: UsageReading) -> None:
        self._reading = reading

    def set_fail(self, fail: bool) -> None:
        self._fail = fail

    @property
    def last_cached(self) -> CachedReading | None:
        return None

    def record_response_headers(self, headers, status, *, now_monotonic) -> None:
        pass

    async def close(self) -> None:
        pass


def _reading(**kw) -> UsageReading:
    base: dict[str, object] = dict(concurrent_sessions=0, limit=4, hard_cap=8)
    base.update(kw)
    return UsageReading(**base)  # type: ignore[arg-type]


def _entry(**kw) -> HistoryEntry:
    defaults: dict[str, object] = dict(
        timestamp=1000.0,
        concurrent_sessions=0,
        local_in_flight=0,
        phantom_estimate=0,
        effective_permits=3,
        limit=4,
        hard_cap=8,
        band="normal",
        breaker="closed",
        priority_low=False,
        usage_age=0.0,
        stale=False,
        recent_429s=0,
        total_429s=0,
        queue_depth=0,
        queue_timeouts=0,
    )
    defaults.update(kw)
    return HistoryEntry(**defaults)  # type: ignore[arg-type]


def _make_store(tmpdir: str) -> SQLiteHistoryStore:
    return SQLiteHistoryStore(os.path.join(tmpdir, "test_history.db"))


# ---------------------------------------------------------------------------
# Unit tests: SQLiteHistoryStore
# ---------------------------------------------------------------------------


def test_store_append_and_load_recent():
    with tempfile.TemporaryDirectory() as tmp:
        store = _make_store(tmp)
        store.append(_entry(timestamp=1000.0, concurrent_sessions=1))
        store.append(_entry(timestamp=1001.0, concurrent_sessions=2))
        store.append(_entry(timestamp=1002.0, concurrent_sessions=3))
        entries = store.load_recent(10)
        assert len(entries) == 3
        assert entries[0].timestamp == 1000.0
        assert entries[0].concurrent_sessions == 1
        assert entries[2].timestamp == 1002.0
        assert entries[2].concurrent_sessions == 3
        store.close()


def test_store_load_recent_limit():
    with tempfile.TemporaryDirectory() as tmp:
        store = _make_store(tmp)
        for i in range(10):
            store.append(_entry(timestamp=1000.0 + i, concurrent_sessions=i))
        entries = store.load_recent(3)
        assert len(entries) == 3
        assert entries[0].timestamp == 1007.0
        assert entries[2].timestamp == 1009.0
        store.close()


def test_store_load_recent_zero_or_negative():
    with tempfile.TemporaryDirectory() as tmp:
        store = _make_store(tmp)
        store.append(_entry(timestamp=1000.0))
        assert store.load_recent(0) == []
        assert store.load_recent(-5) == []
        store.close()


def test_store_load_recent_empty():
    with tempfile.TemporaryDirectory() as tmp:
        store = _make_store(tmp)
        assert store.load_recent(10) == []
        store.close()


def test_store_prune_removes_old_entries():
    with tempfile.TemporaryDirectory() as tmp:
        store = _make_store(tmp)
        store.append(_entry(timestamp=1000.0))
        store.append(_entry(timestamp=2000.0))
        store.append(_entry(timestamp=3000.0))
        deleted = store.prune(ttl_seconds=500.0, now=3000.0)
        assert deleted == 2
        entries = store.load_recent(10)
        assert len(entries) == 1
        assert entries[0].timestamp == 3000.0
        store.close()


def test_store_prune_keeps_recent():
    with tempfile.TemporaryDirectory() as tmp:
        store = _make_store(tmp)
        store.append(_entry(timestamp=1000.0))
        store.append(_entry(timestamp=2000.0))
        deleted = store.prune(ttl_seconds=1400.0, now=2500.0)
        assert deleted == 1
        entries = store.load_recent(10)
        assert len(entries) == 1
        assert entries[0].timestamp == 2000.0
        store.close()


def test_store_prune_nothing_when_all_recent():
    with tempfile.TemporaryDirectory() as tmp:
        store = _make_store(tmp)
        store.append(_entry(timestamp=1000.0))
        store.append(_entry(timestamp=1100.0))
        deleted = store.prune(ttl_seconds=500.0, now=1100.0)
        assert deleted == 0
        assert len(store.load_recent(10)) == 2
        store.close()


def test_store_roundtrip_all_fields():
    with tempfile.TemporaryDirectory() as tmp:
        store = _make_store(tmp)
        entry = _entry(
            timestamp=1234567890.5,
            concurrent_sessions=7,
            local_in_flight=3,
            phantom_estimate=4,
            effective_permits=2,
            limit=10,
            hard_cap=20,
            band="reject",
            breaker="open",
            priority_low=True,
            usage_age=42.5,
            stale=True,
            recent_429s=3,
            total_429s=15,
            rate_limit_429s=7,
            queue_depth=5,
            queue_timeouts=2,
            tick_failed=True,
        )
        store.append(entry)
        entries = store.load_recent(1)
        assert len(entries) == 1
        e = entries[0]
        assert e.timestamp == 1234567890.5
        assert e.concurrent_sessions == 7
        assert e.local_in_flight == 3
        assert e.phantom_estimate == 4
        assert e.effective_permits == 2
        assert e.limit == 10
        assert e.hard_cap == 20
        assert e.band == "reject"
        assert e.breaker == "open"
        assert e.priority_low is True
        assert e.usage_age == 42.5
        assert e.stale is True
        assert e.recent_429s == 3
        assert e.total_429s == 15
        assert e.rate_limit_429s == 7
        assert e.queue_depth == 5
        assert e.queue_timeouts == 2
        assert e.tick_failed is True
        store.close()


def test_store_append_null_fields():
    with tempfile.TemporaryDirectory() as tmp:
        store = _make_store(tmp)
        store.append(_entry(
            timestamp=1000.0,
            concurrent_sessions=None,
            limit=None,
            hard_cap=None,
        ))
        entries = store.load_recent(1)
        assert len(entries) == 1
        assert entries[0].concurrent_sessions is None
        assert entries[0].limit is None
        assert entries[0].hard_cap is None
        store.close()


def test_store_fail_safe_on_corrupt_path():
    store = SQLiteHistoryStore("/nonexistent_dir/missing/deep/path.db")
    assert store._conn is None
    store.append(_entry(timestamp=1000.0))
    assert store.load_recent(10) == []
    assert store.prune(ttl_seconds=100.0, now=2000.0) == 0
    store.close()


def test_store_close_idempotent():
    with tempfile.TemporaryDirectory() as tmp:
        store = _make_store(tmp)
        store.append(_entry(timestamp=1000.0))
        store.close()
        assert not store.is_available
        store.close()
        store.append(_entry(timestamp=1001.0))
        assert store.load_recent(10) == []


def test_store_reopened_db_has_data():
    with tempfile.TemporaryDirectory() as tmp:
        path = os.path.join(tmp, "persist.db")
        store1 = SQLiteHistoryStore(path)
        store1.append(_entry(timestamp=1000.0, concurrent_sessions=1))
        store1.append(_entry(timestamp=1001.0, concurrent_sessions=2))
        store1.close()

        store2 = SQLiteHistoryStore(path)
        entries = store2.load_recent(10)
        assert len(entries) == 2
        assert entries[0].concurrent_sessions == 1
        assert entries[1].concurrent_sessions == 2
        store2.close()


def test_store_duplicate_timestamps():
    with tempfile.TemporaryDirectory() as tmp:
        store = _make_store(tmp)
        store.append(_entry(timestamp=1000.0, concurrent_sessions=1))
        store.append(_entry(timestamp=1000.0, concurrent_sessions=2))
        store.append(_entry(timestamp=1000.0, concurrent_sessions=3))
        entries = store.load_recent(10)
        assert len(entries) == 3
        store.close()


# ---------------------------------------------------------------------------
# Integration: ReconciliationLoop writes to store
# ---------------------------------------------------------------------------


def _make_loop_with_store(initial: UsageReading, tmpdir: str, *, maxlen: int = 100):
    m = [1000.0]
    w = [1_000_000.0]
    client = FakeUsageClient(initial)
    gate = PermitGate(initial_capacity=CFG.target, release_cooldown=0.0, clock=lambda: m[0])
    history = History(maxlen=maxlen)
    store = SQLiteHistoryStore(os.path.join(tmpdir, "reconcile.db"))
    loop = ReconciliationLoop(
        truth_source=client,
        gate=gate,
        controller_config=CFG,
        breaker_config=BCFG,
        poll_interval=5.0,
        monotonic_clock=lambda: m[0],
        wall_clock=lambda: w[0],
        history=history,
        history_store=store,
    )
    return loop, client, gate, m, w, history, store


async def test_tick_writes_to_store():
    with tempfile.TemporaryDirectory() as tmp:
        loop, client, gate, m, w, history, store = _make_loop_with_store(
            _reading(concurrent_sessions=0), tmp
        )
        await loop.tick()
        assert history.length == 1
        entries = store.load_recent(10)
        assert len(entries) == 1
        assert entries[0].concurrent_sessions == 0
        store.close()


async def test_multiple_ticks_write_to_store():
    with tempfile.TemporaryDirectory() as tmp:
        loop, client, gate, m, w, history, store = _make_loop_with_store(
            _reading(concurrent_sessions=2), tmp
        )
        for i in range(5):
            m[0] += 5
            w[0] += 5
            await loop.tick()
        assert history.length == 5
        entries = store.load_recent(10)
        assert len(entries) == 5
        assert entries[0].timestamp < entries[-1].timestamp
        store.close()


async def test_store_failure_degrades_gracefully():
    m = [1000.0]
    w = [1_000_000.0]
    client = FakeUsageClient(_reading(concurrent_sessions=0))
    gate = PermitGate(initial_capacity=CFG.target, release_cooldown=0.0, clock=lambda: m[0])
    history = History(maxlen=100)
    store = SQLiteHistoryStore("/nonexistent/deep/path.db")
    assert store._conn is None

    loop = ReconciliationLoop(
        truth_source=client,
        gate=gate,
        controller_config=CFG,
        breaker_config=BCFG,
        monotonic_clock=lambda: m[0],
        wall_clock=lambda: w[0],
        history=history,
        history_store=store,
    )
    await loop.tick()
    assert history.length == 1
    assert gate.capacity == 3
    store.close()


async def test_buffer_warming_on_startup():
    with tempfile.TemporaryDirectory() as tmp:
        path = os.path.join(tmp, "warm.db")
        store1 = SQLiteHistoryStore(path)
        for i in range(10):
            store1.append(_entry(timestamp=1000.0 + i, concurrent_sessions=i))
        store1.close()

        history = History(maxlen=100)
        store2 = SQLiteHistoryStore(path)
        warmed = store2.load_recent(100)
        for entry in warmed:
            history.append(entry)
        assert history.length == 10
        entries = history.entries()
        assert entries[0].concurrent_sessions == 0
        assert entries[9].concurrent_sessions == 9
        store2.close()


# ---------------------------------------------------------------------------
# Bug-fix regressions
# ---------------------------------------------------------------------------


async def test_last_permits_zero_on_tick_failure():
    m = [1000.0]
    w = [1_000_000.0]
    gate = PermitGate(initial_capacity=CFG.target, release_cooldown=0.0, clock=lambda: m[0])
    history = History(maxlen=100)

    class FailingTruthSource:
        @property
        def last_cached(self):
            return None

        async def fetch(self, *, now_monotonic: float):
            raise RuntimeError("simulated fetch failure")

        def record_response_headers(self, headers, status, *, now_monotonic):
            pass

        async def close(self):
            pass

    loop = ReconciliationLoop(
        truth_source=FailingTruthSource(),  # type: ignore[arg-type]
        gate=gate,
        controller_config=CFG,
        breaker_config=BCFG,
        poll_interval=0.01,
        monotonic_clock=lambda: m[0],
        wall_clock=lambda: w[0],
        history=history,
    )

    task = asyncio.create_task(loop.run())
    await asyncio.sleep(0.1)
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass

    assert loop.effective_permits_count == 0
    assert gate.capacity == 0


async def test_record_failed_tick_does_not_crash_loop():
    m = [1000.0]
    w = [1_000_000.0]
    gate = PermitGate(initial_capacity=CFG.target, release_cooldown=0.0, clock=lambda: m[0])
    history = History(maxlen=100)

    class FailingTruthSource:
        @property
        def last_cached(self):
            return None

        async def fetch(self, *, now_monotonic: float):
            raise RuntimeError("simulated fetch failure")

        def record_response_headers(self, headers, status, *, now_monotonic):
            pass

        async def close(self):
            pass

    loop = ReconciliationLoop(
        truth_source=FailingTruthSource(),  # type: ignore[arg-type]
        gate=gate,
        controller_config=CFG,
        breaker_config=BCFG,
        poll_interval=0.01,
        monotonic_clock=lambda: m[0],
        wall_clock=lambda: w[0],
        history=history,
        history_store=None,
    )

    task = asyncio.create_task(loop.run())
    await asyncio.sleep(0.15)
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass

    assert history.length >= 1
    assert history.entries()[-1].tick_failed is True
    assert history.entries()[-1].effective_permits == 0


async def test_prune_429s_in_default_controller():
    m = [1000.0]
    w = [1_000_000.0]
    client = FakeUsageClient(_reading(concurrent_sessions=0))
    gate = PermitGate(initial_capacity=CFG.target, release_cooldown=0.0, clock=lambda: m[0])
    history = History(maxlen=100)
    loop = ReconciliationLoop(
        truth_source=client,
        gate=gate,
        controller_config=CFG,
        breaker_config=BreakerConfig(threshold=100, window_seconds=10.0, cooldown_seconds=60.0),
        monotonic_clock=lambda: m[0],
        wall_clock=lambda: w[0],
        history=history,
    )
    loop.record_429()
    assert loop.recent_429_count == 1

    m[0] += 100
    await loop.tick()
    assert loop.recent_429_count == 0


async def test_record_failed_tick_with_prior_reading():
    with tempfile.TemporaryDirectory() as tmp:
        m = [1000.0]
        w = [1_000_000.0]
        client = FakeUsageClient(_reading(concurrent_sessions=5, limit=4, hard_cap=8))
        gate = PermitGate(initial_capacity=CFG.target, release_cooldown=0.0, clock=lambda: m[0])
        history = History(maxlen=100)
        store = SQLiteHistoryStore(os.path.join(tmp, "failed.db"))

        class FailingAfterFirstTruthSource:
            def __init__(self, client: FakeUsageClient) -> None:
                self._client = client
                self._call_count = 0

            @property
            def last_cached(self) -> CachedReading | None:
                return self._client.last_cached

            async def fetch(self, *, now_monotonic: float) -> CachedReading:
                self._call_count += 1
                if self._call_count > 1:
                    raise RuntimeError("simulated fetch failure after first tick")
                return await self._client.fetch(now_monotonic=now_monotonic)

            def record_response_headers(self, headers, status, *, now_monotonic) -> None:
                pass

            async def close(self) -> None:
                pass

        loop = ReconciliationLoop(
            truth_source=FailingAfterFirstTruthSource(client),  # type: ignore[arg-type]
            gate=gate,
            controller_config=CFG,
            breaker_config=BCFG,
            poll_interval=0.01,
            monotonic_clock=lambda: m[0],
            wall_clock=lambda: w[0],
            history=history,
            history_store=store,
        )

        await loop.tick()
        assert history.entries()[0].concurrent_sessions == 5

        m[0] += 100
        w[0] += 100

        task = asyncio.create_task(loop.run())
        await asyncio.sleep(0.15)
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass

        failed_entries = [e for e in history.entries() if e.tick_failed]
        assert len(failed_entries) >= 1
        e = failed_entries[-1]
        assert e.concurrent_sessions == 5
        assert e.limit == 4
        assert e.hard_cap == 8
        assert e.effective_permits == 0
        assert e.stale is True

        store_entries = store.load_recent(10)
        assert len(store_entries) >= 2
        store_failed = [e for e in store_entries if e.tick_failed]
        assert len(store_failed) >= 1
        store.close()


# ---------------------------------------------------------------------------
# Endpoint: ?limit edge cases
# ---------------------------------------------------------------------------


async def test_history_json_negative_limit():
    gate = PermitGate(initial_capacity=3)
    client = FakeUsageClient(UsageReading(concurrent_sessions=0, limit=4, hard_cap=8))
    history = History(maxlen=100)
    reconcile = ReconciliationLoop(
        truth_source=client,  # type: ignore[arg-type]
        gate=gate,
        controller_config=CFG,
        breaker_config=BCFG,
        history=history,
    )
    await reconcile.tick()
    await reconcile.tick()

    app = ProxyApp(
        upstream_base_url="https://upstream.example.com",
        gate=gate,
        reconcile=reconcile,
    )
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as c:
        resp = await c.get("/history.json?limit=-5")
    assert resp.status_code == 200
    body = resp.json()
    assert body["count"] == 0
    assert body["entries"] == []


async def test_history_json_malformed_limit():
    gate = PermitGate(initial_capacity=3)
    client = FakeUsageClient(UsageReading(concurrent_sessions=0, limit=4, hard_cap=8))
    history = History(maxlen=100)
    reconcile = ReconciliationLoop(
        truth_source=client,  # type: ignore[arg-type]
        gate=gate,
        controller_config=CFG,
        breaker_config=BCFG,
        history=history,
    )
    await reconcile.tick()

    app = ProxyApp(
        upstream_base_url="https://upstream.example.com",
        gate=gate,
        reconcile=reconcile,
    )
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as c:
        resp = await c.get("/history.json?limit=abc")
    assert resp.status_code == 200
    body = resp.json()
    assert body["count"] == 1
    assert len(body["entries"]) == 1


async def test_history_json_limit_exceeds_buffer():
    gate = PermitGate(initial_capacity=3)
    client = FakeUsageClient(UsageReading(concurrent_sessions=0, limit=4, hard_cap=8))
    history = History(maxlen=100)
    reconcile = ReconciliationLoop(
        truth_source=client,  # type: ignore[arg-type]
        gate=gate,
        controller_config=CFG,
        breaker_config=BCFG,
        history=history,
    )
    await reconcile.tick()

    app = ProxyApp(
        upstream_base_url="https://upstream.example.com",
        gate=gate,
        reconcile=reconcile,
    )
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as c:
        resp = await c.get("/history.json?limit=999999")
    assert resp.status_code == 200
    body = resp.json()
    assert body["count"] == 1


def test_to_dict_list_negative_limit():
    h = History(maxlen=10)
    h.append(_entry(timestamp=1000.0, concurrent_sessions=1))
    h.append(_entry(timestamp=1001.0, concurrent_sessions=2))
    h.append(_entry(timestamp=1002.0, concurrent_sessions=3))
    assert h.to_dict_list(limit=-1) == []
    assert h.to_dict_list(limit=-100) == []


# ---------------------------------------------------------------------------
# Store + endpoint: store is written but /history.json reads from memory
# ---------------------------------------------------------------------------


async def test_history_json_reads_from_memory_not_store():
    with tempfile.TemporaryDirectory() as tmp:
        gate = PermitGate(initial_capacity=3)
        client = FakeUsageClient(UsageReading(concurrent_sessions=0, limit=4, hard_cap=8))
        history = History(maxlen=100)
        store = SQLiteHistoryStore(os.path.join(tmp, "ep.db"))

        reconcile = ReconciliationLoop(
            truth_source=client,  # type: ignore[arg-type]
            gate=gate,
            controller_config=CFG,
            breaker_config=BCFG,
            history=history,
            history_store=store,
        )
        await reconcile.tick()

        store.append(_entry(timestamp=99999.0, concurrent_sessions=42))

        app = ProxyApp(
            upstream_base_url="https://upstream.example.com",
            gate=gate,
            reconcile=reconcile,
        )
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as c:
            resp = await c.get("/history.json")
        body = resp.json()
        assert body["count"] == 1
        assert body["entries"][0]["obs"] == 0
        store.close()


# ---------------------------------------------------------------------------
# Adversarial review: additional edge cases
# ---------------------------------------------------------------------------


def test_store_corrupt_db_file():
    with tempfile.TemporaryDirectory() as tmp:
        path = os.path.join(tmp, "corrupt.db")
        with open(path, "wb") as f:
            f.write(b"NOT A SQLITE DATABASE")
        store = SQLiteHistoryStore(path)
        store.append(_entry(timestamp=1000.0))
        assert store.load_recent(10) == []
        store.close()


def test_store_is_available_property():
    with tempfile.TemporaryDirectory() as tmp:
        store = _make_store(tmp)
        assert store.is_available is True
        store.close()
        assert store.is_available is False

    bad = SQLiteHistoryStore("/nonexistent/deep/path.db")
    assert bad.is_available is False
    bad.close()


async def test_store_writes_when_history_disabled():
    with tempfile.TemporaryDirectory() as tmp:
        m = [1000.0]
        w = [1_000_000.0]
        client = FakeUsageClient(_reading(concurrent_sessions=0))
        gate = PermitGate(initial_capacity=CFG.target, release_cooldown=0.0, clock=lambda: m[0])
        store = SQLiteHistoryStore(os.path.join(tmp, "store_only.db"))

        loop = ReconciliationLoop(
            truth_source=client,
            gate=gate,
            controller_config=CFG,
            breaker_config=BCFG,
            monotonic_clock=lambda: m[0],
            wall_clock=lambda: w[0],
            history=None,
            history_store=store,
        )
        await loop.tick()
        entries = store.load_recent(10)
        assert len(entries) == 1
        assert entries[0].concurrent_sessions == 0
        store.close()


async def test_prune_called_during_run():
    with tempfile.TemporaryDirectory() as tmp:
        m = [1000.0]
        w = [1_000_000.0]
        client = FakeUsageClient(_reading(concurrent_sessions=0))
        gate = PermitGate(initial_capacity=CFG.target, release_cooldown=0.0, clock=lambda: m[0])
        store = SQLiteHistoryStore(os.path.join(tmp, "prune.db"))

        for i in range(100):
            store.append(_entry(timestamp=1000.0 + i))

        loop = ReconciliationLoop(
            truth_source=client,
            gate=gate,
            controller_config=CFG,
            breaker_config=BCFG,
            poll_interval=0.01,
            monotonic_clock=lambda: m[0],
            wall_clock=lambda: w[0],
            history=None,
            history_store=store,
            history_ttl=10.0,
        )

        w[0] = 100000.0
        task = asyncio.create_task(loop.run())
        await asyncio.sleep(1.0)
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass

        entries = store.load_recent(1000)
        for e in entries:
            assert e.timestamp >= 100000.0 - 10.0
        store.close()


def test_store_duplicate_timestamp_ordering():
    with tempfile.TemporaryDirectory() as tmp:
        store = _make_store(tmp)
        store.append(_entry(timestamp=1000.0, concurrent_sessions=1))
        store.append(_entry(timestamp=1000.0, concurrent_sessions=2))
        store.append(_entry(timestamp=1000.0, concurrent_sessions=3))
        entries = store.load_recent(3)
        assert len(entries) == 3
        assert entries[0].concurrent_sessions == 1
        assert entries[1].concurrent_sessions == 2
        assert entries[2].concurrent_sessions == 3
        store.close()
