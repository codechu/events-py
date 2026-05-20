"""Thread-safe, multi-channel event bus — producers to subscribers.

Generic publish/subscribe bus with glob-channel filtering, bounded queues,
backpressure, sync + async iteration, and heartbeat support. Pure stdlib;
no UI / framework / domain coupling.

Example::

    from codechu_events import Bus

    bus = Bus()
    with bus.subscribe_ctx(["scan.*", "ui.click"]) as sub:
        bus.emit("scan.started", path="/home")
        for ev in sub:
            print(ev["event"], ev)

Design:

- **Multi-channel**: ``event`` type is dot-separated (``scan.started``);
  subscribers filter via glob list (``["scan.*", "treemap.drill"]``).
  A "channel" is a glob set.
- **Thread-safe**: all :class:`Bus` methods are lock-guarded; emit never
  blocks (each subscription has a bounded queue + ``put_nowait``).
- **Sync + async consumption**: :class:`Subscription` supports both
  ``for ev in sub:`` and ``async for ev in sub.aiter():``.
- **Context manager**: ``with bus.subscribe_ctx([...]) as sub`` auto-calls
  ``unsubscribe`` (no leaks).
- **Resource limits**: ``MAX_SUBSCRIBERS`` (default 64) — exceeding
  raises :class:`SubscriberLimitExceeded`. Each queue is bounded by
  ``QUEUE_MAX`` (200); slow subscribers drop new events,
  ``sub.dropped`` counts; publisher never waits.
- **Stats**: :meth:`Bus.stats` returns global counters + per-subscription
  details for monitoring / debug.
- **Heartbeat**: :class:`Subscription` with ``heartbeat=5.0`` emits
  ``_keepalive`` events on idle queues every N seconds (dead connection
  detection).

This package is split into focused modules:

- :mod:`codechu_events.subscription` — :class:`Subscription`
- :mod:`codechu_events.bus` — :class:`Bus`
- :mod:`codechu_events._exceptions` — exception types

No module-level singletons or implicit defaults — callers construct their
own :class:`Bus` instance.
"""
from __future__ import annotations

from ._exceptions import SubscriberLimitExceeded
from .bus import MAX_SUBSCRIBERS, Bus
from .subscription import DEFAULT_HEARTBEAT_SEC, QUEUE_MAX, Subscription

__version__ = "0.3.0"

__all__ = [
    "Bus",
    "DEFAULT_HEARTBEAT_SEC",
    "MAX_SUBSCRIBERS",
    "QUEUE_MAX",
    "SubscriberLimitExceeded",
    "Subscription",
    "__version__",
]
