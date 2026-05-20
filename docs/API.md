# API reference — codechu-events 0.3.0

Every public symbol exported from `codechu_events`. Import path:

```python
from codechu_events import (
    Bus,
    Subscription,
    SubscriberLimitExceeded,
    MAX_SUBSCRIBERS,
    QUEUE_MAX,
    DEFAULT_HEARTBEAT_SEC,
    __version__,
)
```

There are **no module-level singletons** or implicit defaults. Callers
construct a `Bus()` and own its lifetime.

---

## `Bus`

Thread-safe multi-channel pub/sub bus. Multiple `Bus` instances coexist
in a process; each has its own subscriber list, counters and locks.

### `Bus(*, max_subscribers=MAX_SUBSCRIBERS, queue_max=QUEUE_MAX, replay_size=0)`

Construct an independent bus.

| Parameter | Type | Default | Purpose |
|---|---|---|---|
| `max_subscribers` | `int` | `64` | Per-bus subscriber cap. Exceeding raises `SubscriberLimitExceeded`. |
| `queue_max` | `int` | `200` | Per-subscription queue depth. Each subscription's bounded queue is sized to this value. (Changed in 0.3.0: previously this argument was accepted but ignored.) |
| `replay_size` | `int` | `0` | Ring-buffer length for the recent-event history. `0` disables it. When `> 0`, the bus retains the last N emitted events and consumers may request them via `subscribe(..., replay=True)`. |

All arguments are keyword-only.

### `bus.subscribe(types=None, *, heartbeat_sec=DEFAULT_HEARTBEAT_SEC, subscription_class=Subscription, filter=None, replay=False) -> Subscription`

Create a new subscription on this bus.

| Parameter | Type | Default | Purpose |
|---|---|---|---|
| `types` | `list[str] \| None` | `None` → `["*"]` | Glob channel filter list. See [Channel glob syntax](#channel-glob-syntax). |
| `heartbeat_sec` | `float` | `5.0` | Idle keepalive interval. `0` disables it. |
| `subscription_class` | `type[Subscription]` | `Subscription` | Override to plug in a custom subclass (e.g. field-based filter). |
| `filter` | `Callable[[dict], bool] \| None` | `None` | Optional predicate applied after the glob match. Events the predicate rejects are dropped and counted in `sub.filtered` (separate from queue-full `sub.dropped`). A filter that raises is treated as a rejection. |
| `replay` | `bool` | `False` | If `True` and the bus was constructed with `replay_size > 0`, all matching buffered events are delivered to this subscription **ahead of live events** and **outside** the queue's backpressure path — they ride a separate replay list and are yielded first by `iter()` / `aiter()`. |

Raises `SubscriberLimitExceeded` if the bus is at its cap. The caller
must eventually call `bus.unsubscribe(sub)` — prefer `subscribe_ctx`
for automatic cleanup.

### `bus.subscribe_ctx(types=None, *, heartbeat_sec=DEFAULT_HEARTBEAT_SEC, subscription_class=Subscription, filter=None, replay=False)`

Context manager wrapping `subscribe` (same kwargs). The subscription is
removed and closed on exit, including on exceptions:

```python
with bus.subscribe_ctx(["scan.*"]) as sub:
    bus.emit("scan.started")
    for ev in sub:
        ...
```

### `bus.unsubscribe(sub) -> None`

Remove a subscription. Idempotent — calling twice is a no-op. Also
invokes `sub.close()` so any pending iterator terminates cleanly.

### `bus.emit(event_type, **fields) -> None`

Publish an event to every subscription whose filter matches. Never
blocks the publisher: each subscription has a bounded queue and uses
`put_nowait`; on overflow the event is dropped for that subscriber and
its `dropped` counter increments.

The emitted dict always carries:

- `event` — the `event_type` string
- `ts` — `time.time()` at publish

plus the user-supplied `**fields`. Names starting with `_` are
reserved for the library (`_keepalive`, `_closed`); do not emit them
yourself.

### `bus.stats() -> dict`

Snapshot for monitoring/debug:

```python
{
    "subscribers": 3,
    "max_subscribers": 64,
    "total_emitted": 1284,
    "replay_size": 0,
    "replay_buffered": 0,
    "details": [
        {
            "types": ["scan.*"],
            "queue_depth": 0,
            "queue_max": 200,
            "received": 412,
            "dropped": 0,
            "filtered": 0,
            "age_sec": 12.4,
        },
        ...
    ],
}
```

`total_emitted` is the number of `emit()` calls on this bus, not the
number of deliveries.

### `bus.subscriber_count() -> int`

Active subscription count on this bus.

### `bus.reset() -> None`

Close every subscription and zero `total_emitted`. Intended as a
shutdown/test helper. Note: with v0.2 the recommended test pattern is
to construct a fresh `Bus()` per test rather than mutating a shared
one — see [`RECIPES.md`](RECIPES.md).

---

## `Subscription`

A per-subscriber bounded queue + filter + iter API. Created via
`Bus.subscribe()` or `Bus.subscribe_ctx()`; do not instantiate
directly.

### Attributes

| Attribute | Type | Meaning |
|---|---|---|
| `types` | `list[str]` | Active glob patterns. Mutable, but consider this advanced use. |
| `queue` | `queue.Queue` | Underlying FIFO. Inspect `qsize()` for depth. |
| `queue_max` | `int` | Capacity of `queue` (mirrors `Bus(queue_max=…)`). |
| `dropped` | `int` | Events rejected because the queue was full (backpressure). |
| `filtered` | `int` | Events rejected by the user-supplied `filter` callback. Distinct from `dropped`. |
| `received` | `int` | Events successfully enqueued. |
| `created_at` | `float` | `time.time()` at construction. |
| `heartbeat_sec` | `float` | Idle keepalive interval. |

### `sub.matches(event_type, event=None) -> bool`

Returns whether this subscription should receive the event. Default
implementation: glob match against `self.types`. Subclasses may
override to implement regex or field-based filtering. The `event`
arg is `None` during cheap type-only checks and the full event dict
during final delivery — useful when a subclass needs to inspect
payload fields.

### `sub.push(event) -> None`

Called by `Bus.emit`; not part of the user-facing surface. Non-blocking.
Order of checks:

1. If a user-supplied `filter` callback was provided and rejects the
   event (or raises), `sub.filtered` increments and the event is dropped.
2. Otherwise, `put_nowait` into the bounded queue. On `queue.Full`,
   `sub.dropped` increments. `sub.received` counts successful enqueues.

### Replay events

When the subscription was created with `replay=True` on a bus with
`replay_size > 0`, the matching past events are buffered on the
subscription at construction time and delivered by `iter()` / `aiter()`
**before** any live event is read from the queue. Replay events:

- bypass the bounded queue and its `dropped` accounting,
- still honor the subscription's glob `matches()` and `filter` predicate
  (so a replay event the filter rejects is not delivered, but it is
  not counted in `filtered` either — filter accounting is a
  live-publish concern),
- are consumed once: a second pass over the iterator (after `close()`)
  will not re-deliver them.

### `sub.close() -> None`

Send a `_closed` sentinel so any pending `iter()` / `aiter()` consumer
exits its loop. Called automatically by `Bus.unsubscribe`.

### Sync iteration: `for ev in sub:` and `sub.iter(*, timeout=None, heartbeat=True)`

`__iter__` returns `self.iter()` with defaults. Use `sub.iter()`
directly to customize:

| Parameter | Type | Default | Purpose |
|---|---|---|---|
| `timeout` | `float \| None` | `None` | Stop iterating when no event arrives within this many seconds. `None` waits forever. |
| `heartbeat` | `bool` | `True` | If `True` and `heartbeat_sec > 0`, yield `{"event": "_keepalive", "ts": ...}` on idle. |

When the subscription is closed the iterator finishes after draining
remaining events.

### Async iteration: `async for ev in sub.aiter(*, heartbeat=True):`

Async generator backed by `loop.run_in_executor` around the blocking
`queue.get`. Keepalive semantics match the sync path. The loop exits
when `close()` is called.

### Heartbeat behavior

- Disabled if `heartbeat_sec == 0` *or* `heartbeat=False` is passed to
  `iter()` / `aiter()`.
- When enabled, on every idle window of `heartbeat_sec` seconds the
  iterator yields `{"event": "_keepalive", "ts": time.time()}` —
  consumers should treat this as "still alive, no real event" and
  ignore it for application logic.
- Real events reset the idle window.

---

## `SubscriberLimitExceeded`

```python
class SubscriberLimitExceeded(Exception):
    """Raised when subscribe() is called after MAX_SUBSCRIBERS is reached."""
```

Raised by `Bus.subscribe()` (and therefore `subscribe_ctx`) when the
bus is at its `max_subscribers` cap.

---

## Constants

| Name | Default | Meaning |
|---|---|---|
| `MAX_SUBSCRIBERS` | `64` | Default per-bus subscriber cap. Override per-instance via `Bus(max_subscribers=...)`. |
| `QUEUE_MAX` | `200` | Per-subscription queue depth. |
| `DEFAULT_HEARTBEAT_SEC` | `5.0` | Default idle keepalive interval. `0` disables. |
| `__version__` | `"0.3.0"` | Package version string. |

---

## Channel glob syntax

The filter list passed to `subscribe()` uses **`fnmatch` case-sensitive
glob** semantics (`fnmatch.fnmatchcase`). An event matches the
subscription if **any** pattern in the list matches its `event_type`.

| Pattern | Matches | Does not match |
|---|---|---|
| `"*"` | every event (`scan.started`, `ui.click`, `metric.fps`) | — |
| `"scan.*"` | `scan.started`, `scan.finished`, `scan.foo` | `scan` (no dot), `scan.foo.bar` (`*` matches one segment greedily, including dots — see note) |
| `"ui.click"` | `ui.click` exactly | `ui.clicked`, `UI.click`, `ui.click.ok` |
| `"scan.*"`, `"ui.*"` | `scan.started`, `ui.click` | `metric.fps` |
| `"*.error"` | `scan.error`, `db.error` | `error` |
| `"scan.[sf]*"` | `scan.started`, `scan.finished` | `scan.progress` |

### Important fnmatch semantics

- **`*` is greedy across dots.** `fnmatch` treats `.` as a normal
  character, so `scan.*` will also match `scan.foo.bar`. If you need
  strict single-segment matching, override `Subscription.matches`.
- **Case-sensitive.** `scan.*` does not match `Scan.started`.
- **Character classes work.** `[abc]`, `[!abc]`, `[a-z]` are all
  honored.
- **Empty / None filter** becomes `["*"]` (match everything).

### Reserved event names

Names beginning with `_` are library-internal:

- `_keepalive` — emitted by the iterator on idle.
- `_closed` — internal sentinel injected by `close()`; never delivered
  to consumers (the iterator returns instead).

Do not `emit()` these from application code.
