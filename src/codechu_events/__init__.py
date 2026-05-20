"""Thread-safe, multi-channel event bus — producers to subscribers.

Generic publish/subscribe bus with glob-channel filtering, bounded queues,
backpressure, sync + async iteration, and heartbeat support. Pure stdlib;
no UI / framework / domain coupling.

Example::

    import codechu_events as events

    def consumer():
        with events.subscribe_ctx(["scan.*", "ui.click"]) as sub:
            for ev in sub:
                print(ev["event"], ev)

    events.emit("scan.started", path="/home")
    events.emit("scan.finished", path="/home", count=42)

Design:

- **Multi-channel**: ``event`` type is dot-separated (``scan.started``);
  subscribers filter via glob list (``["scan.*", "treemap.drill"]``).
  A "channel" is a glob set.
- **Thread-safe**: all public functions are lock-guarded; emit never
  blocks (each subscription has a bounded queue + ``put_nowait``).
- **Sync + async consumption**: :class:`Subscription` supports both
  ``for ev in sub:`` and ``async for ev in sub.aiter():``.
- **Context manager**: ``with subscribe([...]) as sub`` auto-calls
  ``unsubscribe`` (no leaks).
- **Resource limits**: ``MAX_SUBSCRIBERS`` (default 64) — exceeding
  raises :class:`SubscriberLimitExceeded`. Each queue is bounded by
  ``QUEUE_MAX`` (200); slow subscribers drop new events,
  ``sub.dropped`` counts; publisher never waits.
- **Stats**: :func:`stats` returns global counters + per-subscription
  details for monitoring / debug.
- **Heartbeat**: :class:`Subscription` with ``heartbeat=5.0`` emits
  ``_keepalive`` events on idle queues every N seconds (dead connection
  detection).

This package is split into focused modules:

- :mod:`codechu_events.subscription` — :class:`Subscription`
- :mod:`codechu_events.bus` — :class:`Bus`
- :mod:`codechu_events._exceptions` — exception types
- :mod:`codechu_events._testing` — test helpers (``reset_for_tests``)

All public names are re-exported here for backwards compatibility.
"""
from __future__ import annotations

from contextlib import contextmanager
from typing import Any, Iterator

from ._exceptions import SubscriberLimitExceeded
from ._testing import default_bus, reset_for_tests
from .bus import MAX_SUBSCRIBERS, Bus
from .subscription import DEFAULT_HEARTBEAT_SEC, QUEUE_MAX, Subscription

__version__ = "0.2.0"


# ---- module-level shims operating on the default bus ----


def subscribe(
    types: list[str] | None = None,
    *,
    heartbeat_sec: float = DEFAULT_HEARTBEAT_SEC,
    subscription_class: type[Subscription] = Subscription,
) -> Subscription:
    """Subscribe on the default global bus. See :meth:`Bus.subscribe`."""
    return default_bus().subscribe(
        types, heartbeat_sec=heartbeat_sec, subscription_class=subscription_class,
    )


def unsubscribe(sub: Subscription) -> None:
    """Unsubscribe from the default global bus."""
    default_bus().unsubscribe(sub)


@contextmanager
def subscribe_ctx(
    types: list[str] | None = None,
    *,
    heartbeat_sec: float = DEFAULT_HEARTBEAT_SEC,
    subscription_class: type[Subscription] = Subscription,
) -> Iterator[Subscription]:
    """``with subscribe_ctx([...]) as sub:`` on the default bus."""
    sub = subscribe(
        types, heartbeat_sec=heartbeat_sec, subscription_class=subscription_class,
    )
    try:
        yield sub
    finally:
        unsubscribe(sub)


def emit(event_type: str, **fields: Any) -> None:
    """Emit on the default global bus."""
    default_bus().emit(event_type, **fields)


def stats() -> dict[str, Any]:
    """Stats for the default global bus."""
    return default_bus().stats()


def subscriber_count() -> int:
    """Subscriber count on the default global bus."""
    return default_bus().subscriber_count()


__all__ = [
    "Bus",
    "DEFAULT_HEARTBEAT_SEC",
    "MAX_SUBSCRIBERS",
    "QUEUE_MAX",
    "SubscriberLimitExceeded",
    "Subscription",
    "__version__",
    "default_bus",
    "emit",
    "reset_for_tests",
    "stats",
    "subscribe",
    "subscribe_ctx",
    "subscriber_count",
    "unsubscribe",
]
