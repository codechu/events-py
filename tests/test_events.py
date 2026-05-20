"""Event bus tests — multichannel, resource management, thread-safety."""
from __future__ import annotations

import threading
import time

import pytest

from codechu_events import (
    QUEUE_MAX,
    Bus,
    SubscriberLimitExceeded,
    Subscription,
)


def test_emit_to_one_subscriber():
    bus = Bus()
    sub = bus.subscribe()
    bus.emit("scan.started", panel="suggestion")
    ev = sub.queue.get(timeout=1)
    assert ev["event"] == "scan.started"
    assert ev["panel"] == "suggestion"
    assert "ts" in ev


def test_glob_channel_filter():
    bus = Bus()
    sub_scan = bus.subscribe(["scan.*"])
    sub_treemap = bus.subscribe(["treemap.*"])
    sub_all = bus.subscribe(["*"])

    bus.emit("scan.started", panel="x")
    bus.emit("treemap.drill", direction="in")
    bus.emit("mount.changed", target="/")

    # scan.* only sees scan.started
    assert sub_scan.queue.qsize() == 1
    assert sub_scan.queue.get_nowait()["event"] == "scan.started"

    # treemap.* only sees treemap.drill
    assert sub_treemap.queue.qsize() == 1
    assert sub_treemap.queue.get_nowait()["event"] == "treemap.drill"

    # all sees everything
    assert sub_all.queue.qsize() == 3


def test_subscribe_ctx_unsubscribes_on_exit():
    bus = Bus()
    assert bus.subscriber_count() == 0
    with bus.subscribe_ctx(["*"]) as _sub:
        assert bus.subscriber_count() == 1
    assert bus.subscriber_count() == 0


def test_unsubscribe_idempotent():
    bus = Bus()
    sub = bus.subscribe()
    bus.unsubscribe(sub)
    bus.unsubscribe(sub)  # second call must not raise
    assert bus.subscriber_count() == 0


def test_subscriber_limit():
    bus = Bus(max_subscribers=3)
    s1 = bus.subscribe()
    _s2 = bus.subscribe()
    _s3 = bus.subscribe()
    with pytest.raises(SubscriberLimitExceeded):
        bus.subscribe()
    bus.unsubscribe(s1)
    # one slot freed, next subscribe succeeds
    s4 = bus.subscribe()
    assert s4 is not None


def test_backpressure_drops_when_queue_full():
    bus = Bus()
    sub = bus.subscribe()
    # fill QUEUE_MAX, then extra emits should be dropped
    for i in range(QUEUE_MAX):
        bus.emit("scan.progress", i=i)
    assert sub.dropped == 0
    for i in range(50):
        bus.emit("scan.progress", i=i)
    assert sub.dropped == 50
    assert sub.queue.qsize() == QUEUE_MAX
    assert sub.received == QUEUE_MAX


def test_stats_reports_subscribers():
    bus = Bus()
    _s1 = bus.subscribe(["scan.*"])
    _s2 = bus.subscribe(["*"])
    bus.emit("scan.started")
    bus.emit("mount.changed")

    st = bus.stats()
    assert st["subscribers"] == 2
    assert st["total_emitted"] == 2
    # s1 only sees scan.*, queue depth 1
    # s2 sees both, queue depth 2
    depths = sorted(d["queue_depth"] for d in st["details"])
    assert depths == [1, 2]


def test_thread_safe_emit():
    """10 threads emitting in parallel — subscriber sees every event."""
    bus = Bus()
    sub = bus.subscribe()
    N_THREADS = 10
    N_PER_THREAD = 15

    def worker(tid: int) -> None:
        for i in range(N_PER_THREAD):
            bus.emit("scan.progress", thread=tid, i=i)

    threads = [threading.Thread(target=worker, args=(t,)) for t in range(N_THREADS)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    # All must be in the queue (QUEUE_MAX = 200 > 150)
    assert sub.queue.qsize() == N_THREADS * N_PER_THREAD


def test_iter_yields_events():
    bus = Bus()
    sub = bus.subscribe(heartbeat_sec=0)  # heartbeat off
    bus.emit("scan.started", panel="x")
    bus.emit("scan.finished", panel="x")
    # close terminates iter
    sub.close()
    got = list(sub.iter())
    assert len(got) == 2
    assert got[0]["event"] == "scan.started"
    assert got[1]["event"] == "scan.finished"


def test_iter_with_timeout_returns():
    bus = Bus()
    sub = bus.subscribe(heartbeat_sec=0)
    start = time.monotonic()
    got = list(sub.iter(timeout=0.2))
    elapsed = time.monotonic() - start
    assert got == []
    assert 0.15 < elapsed < 0.4  # returned after ~0.2s


def test_iter_emits_heartbeat_when_idle():
    bus = Bus()
    sub = bus.subscribe(heartbeat_sec=0.1)
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
    bus = Bus()
    sub = bus.subscribe(heartbeat_sec=0)
    bus.emit("scan.started")
    sub.close()
    got = list(sub.iter())
    # event arrived before close, then iter ended
    assert len(got) == 1
    assert got[0]["event"] == "scan.started"


def test_async_iter():
    """asyncio bridge — consume events via aiter."""
    import asyncio

    bus = Bus()
    sub = bus.subscribe(heartbeat_sec=0)
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
        bus.emit("scan.started")
        bus.emit("scan.finished")
        await asyncio.wait_for(task, timeout=2)

    asyncio.run(produce_and_consume())
    assert len(collected) == 2
    assert collected[0]["event"] == "scan.started"
    assert collected[1]["event"] == "scan.finished"


# ── Multi-bus isolation ─────────────────────────────────────────────


def test_multiple_buses_are_isolated():
    bus_a = Bus()
    bus_b = Bus()

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


# ── Custom subscription class (matches override) ────────────────────


def test_custom_subscription_with_field_filter():
    """Subscription subclass may override matches() to filter on fields."""

    class PanelFilter(Subscription):
        """Only accept events where event['panel'] == 'suggestion'."""

        def matches(self, event_type, event=None):
            if event is None:
                return True  # cheap type-check pass — let push decide
            return event.get("panel") == "suggestion"

    bus = Bus()
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
