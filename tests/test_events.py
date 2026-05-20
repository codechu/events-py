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


# ── v0.3.0: replay buffer ──────────────────────────────────────────


def test_replay_disabled_by_default():
    """Without ``replay_size``, replay=True is a no-op (no buffered events)."""
    bus = Bus()
    bus.emit("scan.started")
    bus.emit("scan.finished")
    sub = bus.subscribe(["scan.*"], heartbeat_sec=0, replay=True)
    sub.close()
    got = list(sub.iter())
    assert got == []


def test_replay_delivers_past_events():
    """Late subscriber with replay=True receives full history first."""
    bus = Bus(replay_size=10)
    bus.emit("scan.started", i=0)
    bus.emit("scan.progress", i=1)
    bus.emit("scan.finished", i=2)
    sub = bus.subscribe(["scan.*"], heartbeat_sec=0, replay=True)
    bus.emit("scan.after", i=3)  # live event after subscribe
    sub.close()
    got = [e["event"] for e in sub.iter() if not e["event"].startswith("_")]
    assert got == ["scan.started", "scan.progress", "scan.finished", "scan.after"]


def test_replay_ring_buffer_caps_history():
    """replay_size acts as a ring buffer — only the last N events remain."""
    bus = Bus(replay_size=3)
    for i in range(10):
        bus.emit("x.y", i=i)
    sub = bus.subscribe(["x.*"], heartbeat_sec=0, replay=True)
    sub.close()
    got = [e["i"] for e in sub.iter() if not e["event"].startswith("_")]
    assert got == [7, 8, 9]


def test_replay_respects_glob_filter():
    """Replayed events still honor the subscription's glob filter."""
    bus = Bus(replay_size=10)
    bus.emit("scan.started")
    bus.emit("ui.click")
    bus.emit("scan.finished")
    sub = bus.subscribe(["scan.*"], heartbeat_sec=0, replay=True)
    sub.close()
    got = [e["event"] for e in sub.iter() if not e["event"].startswith("_")]
    assert got == ["scan.started", "scan.finished"]


def test_replay_bypasses_queue_backpressure():
    """Replay events are delivered ahead of the live queue, beyond queue_max."""
    bus = Bus(queue_max=5, replay_size=20)
    for i in range(20):
        bus.emit("x.y", i=i)
    sub = bus.subscribe(["x.*"], heartbeat_sec=0, replay=True)
    sub.close()
    got = [e["i"] for e in sub.iter() if not e["event"].startswith("_")]
    # All 20 replay events delivered even though queue_max=5.
    assert got == list(range(20))
    # Replay path does not touch the queue-full ``dropped`` counter.
    assert sub.dropped == 0


# ── v0.3.0: filter callback ────────────────────────────────────────


def test_filter_callback_rejects_events():
    """Events matching the glob but failing filter are dropped + counted."""
    bus = Bus()
    sub = bus.subscribe(
        ["scan.*"],
        heartbeat_sec=0,
        filter=lambda ev: ev.get("panel") == "suggestion",
    )
    bus.emit("scan.started", panel="suggestion")
    bus.emit("scan.started", panel="treemap")
    bus.emit("scan.started")  # no panel
    sub.close()
    got = [e for e in sub.iter() if not e["event"].startswith("_")]
    assert len(got) == 1
    assert got[0]["panel"] == "suggestion"
    assert sub.filtered == 2  # two rejections counted
    assert sub.dropped == 0    # no queue-full drops


def test_filter_callback_exception_treated_as_reject():
    """A filter that raises is treated as a rejection (event filtered)."""
    bus = Bus()

    def bad_filter(ev):
        raise RuntimeError("boom")

    sub = bus.subscribe(["*"], heartbeat_sec=0, filter=bad_filter)
    bus.emit("x.y")
    sub.close()
    got = [e for e in sub.iter() if not e["event"].startswith("_")]
    assert got == []
    assert sub.filtered == 1


# ── v0.3.0: queue_max bugfix ───────────────────────────────────────


def test_queue_max_actually_honored():
    """Bus(queue_max=N) → the per-subscription queue holds exactly N events."""
    bus = Bus(queue_max=5)
    sub = bus.subscribe(["*"], heartbeat_sec=0)
    assert sub.queue.maxsize == 5
    assert sub.queue_max == 5
    for i in range(20):
        bus.emit("x.y", i=i)
    assert sub.queue.qsize() == 5
    assert sub.received == 5
    assert sub.dropped == 15


def test_queue_max_default_unchanged():
    """Without queue_max kwarg, subscriptions still get the QUEUE_MAX default."""
    bus = Bus()
    sub = bus.subscribe()
    assert sub.queue.maxsize == QUEUE_MAX
    assert sub.queue_max == QUEUE_MAX


def test_stats_reports_per_subscription_queue_max_and_filtered():
    """stats() reflects the actual queue_max and filtered counter per sub."""
    bus = Bus(queue_max=7)
    bus.subscribe(["*"], heartbeat_sec=0, filter=lambda ev: False)
    bus.emit("x.y")
    st = bus.stats()
    assert st["details"][0]["queue_max"] == 7
    assert st["details"][0]["filtered"] == 1

