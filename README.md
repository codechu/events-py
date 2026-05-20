```text
━━━━━━━━━━━ c o d e c h u  ·  e v e n t s ━━━━━━━━━━━

   publishers                                  subscribers
   ─────────────                              ──────────────
   scanner ──┐                            ┌── ui.* listener
             │                            │   (queue 64, drops slow)
   gtk-thr ──┼─→  Bus  ─→  glob filter  ──┤
             │       ["scan.*", "ui.*"]   │
   worker  ──┘                            └── scan.* listener
                                              (queue 128)

━━━ thread-safe. bounded. publishers never block. ━━━
```

[![PyPI](https://img.shields.io/pypi/v/codechu-events.svg)](https://pypi.org/project/codechu-events/)
[![Python](https://img.shields.io/pypi/pyversions/codechu-events.svg)](https://pypi.org/project/codechu-events/)
[![CI](https://github.com/codechu/events-py/actions/workflows/ci.yml/badge.svg)](https://github.com/codechu/events-py/actions/workflows/ci.yml)
[![License: MIT](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)

> *Thread-safe multi-channel pub/sub — emit from anywhere, listen everywhere.*

# codechu-events

Thread-safe multi-channel event bus for Python. Emit from any
thread, subscribe with glob patterns, never block a publisher. Pure
stdlib, ~150 LOC, no module-level state — you own the `Bus()`.

## Install

```bash
pip install codechu-events
```

Python 3.10+. Zero third-party dependencies.

## Quick example

```python
from codechu_events import Bus

bus = Bus()                            # caller owns lifetime; no module singleton

with bus.subscribe_ctx(["scan.*"]) as sub:
    bus.emit("scan.started", path="/home")
    bus.emit("scan.progress", count=42)
    bus.emit("scan.finished", count=128, ok=True)
    bus.emit("ui.click")              # filtered out
    for ev in sub:
        print(ev["event"], ev)
```

`async for ev in sub.aiter():` works the same way for asyncio code.

## What you get

- **Multi-channel pub/sub** with glob-pattern filtering
  (`["scan.*", "ui.click"]`).
- **Thread-safe** — emit from any thread, never blocks the
  publisher.
- **Bounded per-subscriber queues** — slow consumers drop events,
  fast publishers stay fast.
- **Sync + async iteration** — `for ev in sub:` or
  `async for ev in sub.aiter():`.
- **Context-manager subscription** for guaranteed unsubscribe.
- **Heartbeat** for dead-connection detection on idle channels.
- **Resource limits** — max subscribers + max queue depth, with
  explicit `SubscriberLimitExceeded` errors instead of silent
  growth.
- **Stats** — subscriber count, drop count, queue depth for
  monitoring.

## Read more

- [API reference](docs/API.md) — every public symbol with full
  signatures.
- [Recipes](docs/RECIPES.md) — multiple isolated buses, custom
  subscription classes, async iteration, heartbeat patterns.
- [Migration guide](docs/MIGRATION.md) — between major versions.
- [Changelog](CHANGELOG.md)

## Family

| Library | Purpose |
|---------|---------|
| [codechu-ipc](https://pypi.org/project/codechu-ipc/) | Local IPC — Unix socket, FIFO, JSON-line protocol |
| [codechu-log](https://pypi.org/project/codechu-log/) | Structured logging — context, JSON, rotation |
| [codechu-xdg](https://pypi.org/project/codechu-xdg/) | XDG Base Directory helpers, vendor-namespaced |
| [codechu-config](https://pypi.org/project/codechu-config/) | Schema-driven config — atomic save, migrations |
| [codechu-cli](https://pypi.org/project/codechu-cli/) | CLI primitives — colors, progress, prompts |

Full ecosystem: [github.com/codechu](https://github.com/codechu).

## Credits

- Glob-pattern semantics borrow from POSIX shell glob and
  `fnmatch.translate`.
- Bounded-queue + dropping-consumer model inspired by LMAX
  Disruptor's slow-subscriber strategy.

## License

MIT — see [LICENSE](LICENSE).

Part of [Codechu](https://github.com/codechu).
