# Recipes — codechu-events

Patterns that map common needs onto the v0.2 API. All examples use an
explicit `Bus()` — there is no module-level default.

---

## 1. App-level singleton Bus — the right way to do globals

When you do want "one bus everyone uses", declare it at the app level.
The library stays unopinionated; your app owns the lifetime.

```python
# myapp/events.py
from codechu_events import Bus

bus = Bus()  # module attribute — imported wherever needed
```

```python
# myapp/scanner.py
from myapp.events import bus

def scan(path):
    bus.emit("scan.started", path=path)
    ...
    bus.emit("scan.finished", path=path, ok=True)
```

```python
# myapp/ui.py
from myapp.events import bus

with bus.subscribe_ctx(["scan.*"]) as sub:
    for ev in sub:
        update_progress_bar(ev)
```

This keeps ownership visible (one import, one attribute) without
hiding it inside the library.

---

## 2. Multiple isolated buses — UI events vs telemetry

A single process can run any number of buses. Useful when two domains
have different cardinality, retention, or subscriber profiles.

```python
# myapp/events.py
from codechu_events import Bus

ui_bus = Bus()                          # interactive, low-volume
telemetry_bus = Bus(max_subscribers=128) # metrics, many consumers
```

```python
# UI side
with ui_bus.subscribe_ctx(["ui.*"]) as sub:
    for ev in sub:
        ...

# Telemetry side — never sees ui.click, never competes for the queue
with telemetry_bus.subscribe_ctx(["metric.*"]) as sub:
    for ev in sub:
        ship_to_prometheus(ev)
```

Each bus has its own subscriber cap, its own counters, and its own
lock. They cannot interfere with each other.

---

## 3. Async consumption with `aiter()`

`Subscription.aiter()` is an async generator. It uses
`loop.run_in_executor` internally so the blocking `queue.get` lives in
an executor thread; no extra dependencies.

```python
import asyncio
from codechu_events import Bus

bus = Bus()

async def consume():
    with bus.subscribe_ctx(["scan.*"], heartbeat_sec=5.0) as sub:
        async for ev in sub.aiter():
            if ev["event"] == "_keepalive":
                continue  # idle tick, not a real event
            await handle(ev)

async def main():
    task = asyncio.create_task(consume())
    bus.emit("scan.started", path="/home")
    bus.emit("scan.finished", ok=True)
    await asyncio.sleep(0.1)
    task.cancel()

asyncio.run(main())
```

Cancelling the task closes the subscription on context exit.

---

## 4. Backpressure: handle slow subscribers with the `dropped` counter

`emit()` never blocks. If a subscriber's queue is full, the new event
is dropped for that subscriber and `sub.dropped` increments. The
publisher and every other subscriber are unaffected.

Use `dropped` to detect a consumer that has fallen behind:

```python
from codechu_events import Bus

bus = Bus()
sub = bus.subscribe(["scan.*"])

# ... producer thread emits a flood ...

last_dropped = 0
while running:
    ev = next(iter(sub))
    handle(ev)

    if sub.dropped > last_dropped:
        log.warning(
            "consumer fell behind: %d new drops (queue depth %d/%d)",
            sub.dropped - last_dropped,
            sub.queue.qsize(),
            sub.queue.maxsize,
        )
        last_dropped = sub.dropped
```

For coarse-grained monitoring, read `bus.stats()["details"]` instead
of inspecting each `Subscription` directly.

If drops are unacceptable for a given consumer, the right answer is
usually to make the consumer faster or to bound producer rate — the
library deliberately does not block emitters to preserve liveness.

---

## 5. Heartbeat for dead-connection detection

On idle queues the iterator emits `{"event": "_keepalive", "ts": ...}`
every `heartbeat_sec` seconds. Useful for SSE / WebSocket bridges
where you need to write *something* periodically to detect a peer that
went away.

```python
from codechu_events import Bus

bus = Bus()

def sse_stream():
    """Server-Sent Events generator — keepalive keeps the socket warm."""
    with bus.subscribe_ctx(["scan.*"], heartbeat_sec=15.0) as sub:
        for ev in sub:
            if ev["event"] == "_keepalive":
                yield ": keepalive\n\n"  # SSE comment, ignored by client
            else:
                yield f"data: {json.dumps(ev)}\n\n"
```

Set `heartbeat_sec=0` (or pass `heartbeat=False` to `iter()`/`aiter()`)
to disable the tick entirely.

---

## 6. Test pattern: fresh Bus per test, no global state

v0.2 removed `reset_for_tests()` because each test can simply build
its own bus. This eliminates test ordering bugs and accidental
cross-contamination.

```python
# tests/test_scanner.py
from codechu_events import Bus
from myapp.scanner import Scanner

def test_emits_start_and_finish():
    bus = Bus()
    scanner = Scanner(bus=bus)

    with bus.subscribe_ctx(["scan.*"]) as sub:
        scanner.scan("/tmp")
        events = list(sub.iter(timeout=0.5, heartbeat=False))

    assert [e["event"] for e in events] == ["scan.started", "scan.finished"]


def test_drops_under_backpressure():
    bus = Bus()
    sub = bus.subscribe(["x.*"])
    for i in range(500):           # QUEUE_MAX=200
        bus.emit("x.y", i=i)
    assert sub.received <= 200
    assert sub.dropped >= 300
    bus.unsubscribe(sub)
```

Inject the bus into the system-under-test (`Scanner(bus=bus)`) rather
than reaching for a module-level instance. Production code can still
use an app-level singleton (Recipe 1); tests instantiate their own.

Tips:

- Pass `timeout=…` and `heartbeat=False` to `sub.iter()` so the test
  cannot hang and doesn't have to filter `_keepalive` events.
- If you want to assert on `bus.stats()` directly, do it before
  exiting the `subscribe_ctx` block — `unsubscribe` removes the
  subscription from the details list.
