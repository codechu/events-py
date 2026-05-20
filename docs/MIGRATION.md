# Migration guide

## v0.2 → v0.3

v0.3 is additive (replay buffer + filter callback) with one
behavior-change worth flagging.

### `Bus(queue_max=N)` is now actually honored

In v0.2 the `queue_max` constructor kwarg was accepted but ignored —
every subscription's bounded queue was sized from the module-level
`QUEUE_MAX` constant (200), regardless of what you passed. In v0.3 the
value flows through to each `Subscription`'s queue.

This is backwards-compatible if you never relied on the broken
behavior (the default value is unchanged at 200, and code that
omitted `queue_max` is unaffected). Two scenarios where you may see a
difference:

- **You passed `queue_max=N` expecting a smaller queue.** You now get
  one. If you actually wanted the old 200-deep queue, drop the kwarg
  or pass `queue_max=200`.
- **You passed `queue_max=N` expecting a larger queue.** Same: you
  now get one. Slow consumers that previously fit into the silent
  200-deep queue may now overflow and bump `sub.dropped`. Tune
  upward as needed.

`Bus.stats()["details"][i]["queue_max"]` now reports the actual
per-subscription capacity instead of the global constant.

### Additive: opt-in features

The following are off by default; you only see them if you ask:

- `Bus(replay_size=N)` — recent-event ring buffer (see
  [`RECIPES.md`](RECIPES.md) §7).
- `bus.subscribe(..., filter=callable)` — predicate filter beyond
  the glob (see [`RECIPES.md`](RECIPES.md) §8).
- `sub.filtered` counter and per-subscription `queue_max` /
  `filtered` fields in `bus.stats()`.

No imports or method signatures were removed in v0.3; existing v0.2
code keeps working unchanged.

---

# v0.1 → v0.2

v0.2 removes module-level singletons. The package no longer exports a
default `Bus`, the module-level shim functions, or the
`reset_for_tests()` helper. Callers construct and own a `Bus()`
instance explicitly.

This is the only breaking change. The semantics of `Bus`,
`Subscription`, glob filtering, heartbeat, and resource limits are
unchanged.

---

## Why

In v0.1, `codechu_events.emit(...)` and friends quietly forwarded to a
hidden module-level `Bus`. That worked, but it:

- Made test isolation awkward (`reset_for_tests()` had to exist).
- Hid ownership — there was no way to ask "who owns this bus?".
- Prevented multiple isolated buses without renaming the import.

v0.2 makes the bus explicit. Apps that want a singleton still get one
— they just declare it themselves. See
[`RECIPES.md` § "App-level singleton Bus"](RECIPES.md).

---

## What was removed

| v0.1 symbol | v0.2 replacement |
|---|---|
| `codechu_events.default_bus()` | Construct your own `Bus()` and pass it around. |
| `codechu_events.subscribe(...)` | `bus.subscribe(...)` |
| `codechu_events.subscribe_ctx(...)` | `bus.subscribe_ctx(...)` |
| `codechu_events.unsubscribe(sub)` | `bus.unsubscribe(sub)` |
| `codechu_events.emit(type, **f)` | `bus.emit(type, **f)` |
| `codechu_events.stats()` | `bus.stats()` |
| `codechu_events.subscriber_count()` | `bus.subscriber_count()` |
| `codechu_events.reset_for_tests()` | Construct a fresh `Bus()` per test. |

`Bus`, `Subscription`, `SubscriberLimitExceeded`, `MAX_SUBSCRIBERS`,
`QUEUE_MAX`, `DEFAULT_HEARTBEAT_SEC` are unchanged.

---

## Side-by-side

### Producing events

```python
# v0.1
import codechu_events as ev
ev.emit("scan.started", path="/home")

# v0.2
from codechu_events import Bus
bus = Bus()  # construct once at app startup; pass it where needed
bus.emit("scan.started", path="/home")
```

### Subscribing

```python
# v0.1
import codechu_events as ev
with ev.subscribe_ctx(["scan.*"]) as sub:
    for e in sub:
        ...

# v0.2
from codechu_events import Bus
bus = Bus()
with bus.subscribe_ctx(["scan.*"]) as sub:
    for e in sub:
        ...
```

### Stats / inspection

```python
# v0.1
import codechu_events as ev
print(ev.stats())
print(ev.subscriber_count())

# v0.2
print(bus.stats())
print(bus.subscriber_count())
```

### Test isolation

```python
# v0.1
import codechu_events as ev

def setup_function():
    ev.reset_for_tests()

def test_emit():
    with ev.subscribe_ctx(["x.*"]) as sub:
        ev.emit("x.y")
        assert next(iter(sub))["event"] == "x.y"

# v0.2 — no global state to reset
from codechu_events import Bus

def test_emit():
    bus = Bus()
    with bus.subscribe_ctx(["x.*"]) as sub:
        bus.emit("x.y")
        assert next(iter(sub))["event"] == "x.y"
```

---

## Mechanical migration

For apps that already relied on the module-level shims, the smallest
diff is:

1. Pick one place near app startup to construct the bus:

   ```python
   # app/events.py
   from codechu_events import Bus
   bus = Bus()
   ```

2. Replace `import codechu_events as ev` with `from app.events import bus`.
3. Replace `ev.emit(...)` → `bus.emit(...)`, `ev.subscribe_ctx(...)` →
   `bus.subscribe_ctx(...)`, etc.
4. Delete any `reset_for_tests()` calls; rewrite affected tests to
   construct a fresh `Bus()` instead of importing the app-level one.

If you have many call sites, a project-wide search/replace from
`ev\.(emit|subscribe|subscribe_ctx|unsubscribe|stats|subscriber_count)\(`
to `bus.\1(` covers the common cases.

---

## Recipes that depended on globals

The v0.1 mental model of "there is one bus, everyone touches it" maps
cleanly to the v0.2 pattern of [app-level singleton Bus](RECIPES.md).
You keep the convenience of one shared instance without paying for it
in test isolation.
