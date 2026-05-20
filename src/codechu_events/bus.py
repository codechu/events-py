"""Bus: thread-safe multi-subscriber event bus with glob filtering."""
from __future__ import annotations

import threading
import time
from contextlib import contextmanager
from typing import Any, Iterator

from ._exceptions import SubscriberLimitExceeded
from .subscription import DEFAULT_HEARTBEAT_SEC, QUEUE_MAX, Subscription

#: Maximum subscribers allowed on a single bus (resource cap).
MAX_SUBSCRIBERS: int = 64


class Bus:
    """Independent event bus with its own state.

    Multiple buses can coexist in a process — useful for isolating
    domains (e.g. UI bus + telemetry bus) or for test fixtures where
    each test wants its own clean state.

    The module-level functions (``subscribe``, ``emit``, ...) operate on
    a single global default bus — convenient for simple programs.

    Args:
        max_subscribers: per-bus subscriber limit (default 64)
        queue_max: per-subscription queue depth (default 200)
    """

    def __init__(
        self,
        *,
        max_subscribers: int = MAX_SUBSCRIBERS,
        queue_max: int = QUEUE_MAX,
    ) -> None:
        self.max_subscribers = max_subscribers
        self.queue_max = queue_max
        self._lock = threading.Lock()
        self._subs: list[Subscription] = []
        self._total_emitted: int = 0

    def subscribe(
        self,
        types: list[str] | None = None,
        *,
        heartbeat_sec: float = DEFAULT_HEARTBEAT_SEC,
        subscription_class: type[Subscription] = Subscription,
    ) -> Subscription:
        """Create a new subscription on this bus.

        ``subscription_class`` lets callers pass a custom Subscription
        subclass (for example, one overriding ``matches()`` to add
        field-based filtering).
        """
        with self._lock:
            if len(self._subs) >= self.max_subscribers:
                raise SubscriberLimitExceeded(
                    f"max {self.max_subscribers} subscribers, current: {len(self._subs)}"
                )
            sub = subscription_class(types or ["*"], heartbeat_sec=heartbeat_sec)
            self._subs.append(sub)
            return sub

    def unsubscribe(self, sub: Subscription) -> None:
        """Remove a subscription (idempotent). Also calls ``sub.close()``."""
        with self._lock:
            try:
                self._subs.remove(sub)
            except ValueError:
                pass
        sub.close()

    @contextmanager
    def subscribe_ctx(
        self,
        types: list[str] | None = None,
        *,
        heartbeat_sec: float = DEFAULT_HEARTBEAT_SEC,
        subscription_class: type[Subscription] = Subscription,
    ) -> Iterator[Subscription]:
        """``with bus.subscribe_ctx([...]) as sub:`` — auto-unsubscribe on exit."""
        sub = self.subscribe(types, heartbeat_sec=heartbeat_sec,
                              subscription_class=subscription_class)
        try:
            yield sub
        finally:
            self.unsubscribe(sub)

    def emit(self, event_type: str, **fields: Any) -> None:
        """Publish an event. Never blocks the publisher.

        Names starting with ``_`` (``_keepalive``, ``_closed``) are
        reserved for internal use. ``event`` and ``ts`` fields are added
        automatically.
        """
        event: dict[str, Any] = {
            "event": event_type,
            "ts": time.time(),
            **fields,
        }
        with self._lock:
            targets = [s for s in self._subs if s.matches(event_type, event)]
            self._total_emitted += 1
        for s in targets:
            s.push(event)

    def stats(self) -> dict[str, Any]:
        """Snapshot for monitoring: subscriber count, queue depth, drop count."""
        with self._lock:
            subs_info = [
                {
                    "types": s.types,
                    "queue_depth": s.queue.qsize(),
                    "queue_max": QUEUE_MAX,
                    "received": s.received,
                    "dropped": s.dropped,
                    "age_sec": round(time.time() - s.created_at, 1),
                }
                for s in self._subs
            ]
            return {
                "subscribers": len(self._subs),
                "max_subscribers": self.max_subscribers,
                "total_emitted": self._total_emitted,
                "details": subs_info,
            }

    def subscriber_count(self) -> int:
        """Number of active subscriptions on this bus."""
        with self._lock:
            return len(self._subs)

    def reset(self) -> None:
        """Close all subscriptions and zero counters (test / shutdown helper)."""
        with self._lock:
            for s in self._subs:
                s.close()
            self._subs.clear()
            self._total_emitted = 0


__all__ = ["Bus", "MAX_SUBSCRIBERS"]
