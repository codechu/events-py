# codechu-events

Thread-safe multi-channel event bus for Python. Pure stdlib, ~150 LOC.

```bash
pip install codechu-events
```

## What it gives you

- **Multi-channel pub/sub** with glob-pattern filtering: `["scan.*", "ui.click"]`
- **Thread-safe** — emit from any thread, never blocks
- **Bounded queues** per subscriber — slow consumers drop events, fast publishers never wait
- **Sync + async iteration** — `for ev in sub:` or `async for ev in sub.aiter():`
- **Context manager** for clean unsubscribe: `with subscribe_ctx([...]) as sub:`
- **Heartbeat** support for dead-connection detection on idle channels
- **Resource limits** — max subscribers + max queue depth, bounded by design
- **Stats** for monitoring (subscriber count, drop count, queue depth)

## Quick examples

### Default global bus (simple programs)

```python
import codechu_events as events

# Subscriber (sync iteration)
def consume():
    with events.subscribe_ctx(["scan.*", "ui.click"]) as sub:
        for ev in sub:
            print(ev["event"], ev)

# Publisher (any thread, never blocks)
events.emit("scan.started", path="/home")
events.emit("scan.progress", count=42)
events.emit("scan.finished", count=128, ok=True)
events.emit("ui.click", button="cancel")     # also delivered
events.emit("foo.bar")                        # filtered out
```

### Multiple isolated buses

A single process can run multiple independent buses — useful for
separating domains (e.g. UI events vs telemetry) or for testing:

```python
from codechu_events import Bus

ui_bus = Bus()
telemetry_bus = Bus(max_subscribers=128)  # larger cap for telemetry

ui_sub = ui_bus.subscribe(["ui.*"])
telemetry_sub = telemetry_bus.subscribe(["metric.*"])

ui_bus.emit("ui.click", button="ok")
telemetry_bus.emit("metric.fps", value=58)
# ui_sub only sees ui.click; telemetry_sub only sees metric.fps
```

### Custom subscription class (field-based filter)

For filtering beyond glob, subclass `Subscription` and override `matches()`:

```python
from codechu_events import Bus, Subscription

class PanelFilter(Subscription):
    """Only events with event['panel'] == 'suggestion'."""

    def matches(self, event_type, event=None):
        if event is None:
            return True  # cheap type-check pass; final check at push
        return event.get("panel") == "suggestion"

bus = Bus()
sub = bus.subscribe(["*"], subscription_class=PanelFilter)
bus.emit("scan.started", panel="suggestion")   # delivered
bus.emit("scan.started", panel="treemap")      # rejected
```

### Async iteration

```python
import asyncio, codechu_events as events

async def consume():
    with events.subscribe_ctx(["scan.*"], heartbeat_sec=5.0) as sub:
        async for ev in sub.aiter():
            print(ev)

asyncio.run(consume())
```

## API reference

| Function | Purpose |
|---|---|
| `emit(event_type, **fields)` | Publish event. Never blocks. |
| `subscribe(types=["*"], heartbeat_sec=5.0)` | Create a Subscription. Caller must `unsubscribe()`. |
| `subscribe_ctx(types, heartbeat_sec=5.0)` | Same as `subscribe()`, but a context manager (auto-unsubscribe). |
| `stats()` | Returns dict with subscriber count, total emitted, drop counts. |

### `Subscription` API

| Member | Purpose |
|---|---|
| `for ev in sub:` | Sync blocking iteration. Auto-emits `_keepalive` on idle. |
| `async for ev in sub.aiter():` | Async iteration with event loop. |
| `sub.dropped` | Count of events dropped due to slow consumer. |
| `sub.received` | Count of events accepted. |
| `sub.close()` | Stop iteration (sentinel injected). |

## Resource limits

| Constant | Default | Tweakable |
|---|---|---|
| `QUEUE_MAX` | 200 | Max events per subscriber queue (drops on overflow) |
| `MAX_SUBSCRIBERS` | 64 | Max concurrent subscribers |
| `DEFAULT_HEARTBEAT_SEC` | 5.0 | Idle keepalive interval |

Exceeding `MAX_SUBSCRIBERS` raises `SubscriberLimitExceeded`.

## License

MIT — see [LICENSE](LICENSE).

Part of [Codechu](https://github.com/codechu).
