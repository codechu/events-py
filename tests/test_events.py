"""Event bus testleri — multichannel, kaynak yönetimi, thread-safety."""
from __future__ import annotations

import threading
import time

import pytest

import codechu_events as events


@pytest.fixture(autouse=True)
def _reset():
    events.reset_for_tests()
    yield
    events.reset_for_tests()


def test_emit_to_one_subscriber():
    sub = events.subscribe()
    events.emit("scan.started", panel="suggestion")
    ev = sub.queue.get(timeout=1)
    assert ev["event"] == "scan.started"
    assert ev["panel"] == "suggestion"
    assert "ts" in ev


def test_glob_channel_filter():
    sub_scan = events.subscribe(["scan.*"])
    sub_treemap = events.subscribe(["treemap.*"])
    sub_all = events.subscribe(["*"])

    events.emit("scan.started", panel="x")
    events.emit("treemap.drill", direction="in")
    events.emit("mount.changed", target="/")

    # scan.* sadece scan.started görür
    assert sub_scan.queue.qsize() == 1
    assert sub_scan.queue.get_nowait()["event"] == "scan.started"

    # treemap.* sadece treemap.drill
    assert sub_treemap.queue.qsize() == 1
    assert sub_treemap.queue.get_nowait()["event"] == "treemap.drill"

    # all hepsini
    assert sub_all.queue.qsize() == 3


def test_subscribe_ctx_unsubscribes_on_exit():
    assert events.subscriber_count() == 0
    with events.subscribe_ctx(["*"]) as _sub:
        assert events.subscriber_count() == 1
    assert events.subscriber_count() == 0


def test_unsubscribe_idempotent():
    sub = events.subscribe()
    events.unsubscribe(sub)
    events.unsubscribe(sub)  # ikinci çağrı hata vermemeli
    assert events.subscriber_count() == 0


def test_subscriber_limit():
    bus = events.default_bus()
    original = bus.max_subscribers
    bus.max_subscribers = 3
    try:
        s1 = events.subscribe()
        _s2 = events.subscribe()
        _s3 = events.subscribe()
        with pytest.raises(events.SubscriberLimitExceeded):
            events.subscribe()
        events.unsubscribe(s1)
        # one slot freed, next subscribe succeeds
        s4 = events.subscribe()
        assert s4 is not None
    finally:
        bus.max_subscribers = original


def test_backpressure_drops_when_queue_full():
    sub = events.subscribe()
    # QUEUE_MAX kadar doldur, sonra fazla emit drop edilmeli
    for i in range(events.QUEUE_MAX):
        events.emit("scan.progress", i=i)
    assert sub.dropped == 0
    for i in range(50):
        events.emit("scan.progress", i=i)
    assert sub.dropped == 50
    assert sub.queue.qsize() == events.QUEUE_MAX
    assert sub.received == events.QUEUE_MAX


def test_stats_reports_subscribers():
    _s1 = events.subscribe(["scan.*"])
    _s2 = events.subscribe(["*"])
    events.emit("scan.started")
    events.emit("mount.changed")

    st = events.stats()
    assert st["subscribers"] == 2
    assert st["total_emitted"] == 2
    # s1 only sees scan.*, queue depth 1
    # s2 sees both, queue depth 2
    depths = sorted(d["queue_depth"] for d in st["details"])
    assert depths == [1, 2]


def test_thread_safe_emit():
    """10 thread paralel emit yapsa subscriber tüm event'leri görür."""
    sub = events.subscribe()
    N_THREADS = 10
    N_PER_THREAD = 15

    def worker(tid: int) -> None:
        for i in range(N_PER_THREAD):
            events.emit("scan.progress", thread=tid, i=i)

    threads = [threading.Thread(target=worker, args=(t,)) for t in range(N_THREADS)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    # Hepsi queue'da olmalı (QUEUE_MAX = 200 > 150)
    assert sub.queue.qsize() == N_THREADS * N_PER_THREAD


def test_iter_yields_events():
    sub = events.subscribe(heartbeat_sec=0)  # heartbeat kapalı
    events.emit("scan.started", panel="x")
    events.emit("scan.finished", panel="x")
    # close ile iter'i sonlandır
    sub.close()
    got = list(sub.iter())
    assert len(got) == 2
    assert got[0]["event"] == "scan.started"
    assert got[1]["event"] == "scan.finished"


def test_iter_with_timeout_returns():
    sub = events.subscribe(heartbeat_sec=0)
    start = time.monotonic()
    got = list(sub.iter(timeout=0.2))
    elapsed = time.monotonic() - start
    assert got == []
    assert 0.15 < elapsed < 0.4  # ~0.2sn sonra döndü


def test_iter_emits_heartbeat_when_idle():
    sub = events.subscribe(heartbeat_sec=0.1)
    collected = []

    def consume():
        for ev in sub.iter():
            collected.append(ev)
            if len(collected) >= 2:
                sub.close()
                return

    t = threading.Thread(target=consume, daemon=True)
    t.start()
    t.join(timeout=2)
    assert len(collected) >= 2
    assert all(e["event"] == "_keepalive" for e in collected)


def test_close_terminates_iter():
    sub = events.subscribe(heartbeat_sec=0)
    events.emit("scan.started")
    sub.close()
    got = list(sub.iter())
    # close öncesi event geldi, sonra iter bitti
    assert len(got) == 1
    assert got[0]["event"] == "scan.started"


def test_async_iter():
    """asyncio bridge — aiter ile event al."""
    import asyncio

    sub = events.subscribe(heartbeat_sec=0)
    collected: list[dict] = []

    async def consume() -> None:
        async for ev in sub.aiter(heartbeat=False):
            collected.append(ev)
            if len(collected) >= 2:
                sub.close()
                return

    async def produce_and_consume() -> None:
        task = asyncio.create_task(consume())
        await asyncio.sleep(0.05)
        events.emit("scan.started")
        events.emit("scan.finished")
        await asyncio.wait_for(task, timeout=2)

    asyncio.run(produce_and_consume())
    assert len(collected) == 2
    assert collected[0]["event"] == "scan.started"
    assert collected[1]["event"] == "scan.finished"


# ── Multi-bus isolation ─────────────────────────────────────────────


def test_multiple_buses_are_isolated():
    bus_a = events.Bus()
    bus_b = events.Bus()

    sub_a = bus_a.subscribe(["*"])
    sub_b = bus_b.subscribe(["*"])

    bus_a.emit("foo")
    bus_b.emit("bar")

    # Each bus only sees its own events
    a_events = list(sub_a.iter(timeout=0.05))
    b_events = list(sub_b.iter(timeout=0.05))

    a_types = [e["event"] for e in a_events if not e["event"].startswith("_")]
    b_types = [e["event"] for e in b_events if not e["event"].startswith("_")]
    assert "foo" in a_types and "bar" not in a_types
    assert "bar" in b_types and "foo" not in b_types

    bus_a.reset()
    bus_b.reset()


def test_default_bus_independent_from_explicit_bus():
    custom = events.Bus()
    events.reset_for_tests()  # clear default bus

    default_sub = events.subscribe(["*"])
    custom_sub = custom.subscribe(["*"])

    events.emit("on-default")
    custom.emit("on-custom")

    default_events = [e["event"] for e in default_sub.iter(timeout=0.05)
                      if not e["event"].startswith("_")]
    custom_events = [e["event"] for e in custom_sub.iter(timeout=0.05)
                     if not e["event"].startswith("_")]

    assert "on-default" in default_events
    assert "on-custom" not in default_events
    assert "on-custom" in custom_events
    assert "on-default" not in custom_events

    events.unsubscribe(default_sub)
    custom.unsubscribe(custom_sub)


# ── Custom subscription class (matches override) ────────────────────


def test_custom_subscription_with_field_filter():
    """Subscription subclass may override matches() to filter on fields."""

    class PanelFilter(events.Subscription):
        """Only accept events where event['panel'] == 'suggestion'."""

        def matches(self, event_type, event=None):
            if event is None:
                return True  # cheap type-check pass — let push decide
            return event.get("panel") == "suggestion"

    bus = events.Bus()
    sub = bus.subscribe(["*"], subscription_class=PanelFilter)

    bus.emit("scan.started", panel="suggestion")
    bus.emit("scan.started", panel="treemap")
    bus.emit("scan.started")  # no panel

    seen = [e["event"] + ":" + e.get("panel", "?") for e in sub.iter(timeout=0.1)
            if not e["event"].startswith("_")]

    assert "scan.started:suggestion" in seen
    assert "scan.started:treemap" not in seen
    assert "scan.started:?" not in seen
    bus.reset()
