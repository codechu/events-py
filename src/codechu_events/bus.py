"""Bus: thread-safe multi-subscriber event bus with glob filtering."""
from __future__ import annotations

import threading
import time
from collections import deque
from contextlib import contextmanager
from typing import Any, Callable, Iterator

from ._exceptions import SubscriberLimitExceeded
from .subscription import DEFAULT_HEARTBEAT_SEC, QUEUE_MAX, Subscription

#: Maximum subscribers allowed on a single bus (resource cap).
MAX_SUBSCRIBERS: int = 64


class Bus:
    """Independent event bus with its own state.

    Multiple buses can coexist in a process — useful for isolating
    domains (e.g. UI bus + telemetry bus) or for test fixtures where
    each test wants its own clean state. Callers construct a :class:`Bus`
    explicitly; there is no module-level default.

    Args:
        max_subscribers: per-bus subscriber limit (default 64)
        queue_max: per-subscription queue depth (default 200). Each
            new subscription created via :meth:`subscribe` will use
            this depth for its bounded queue.
        replay_size: optional ring-buffer size for the recent-event
            history. ``0`` (the default) disables the buffer. When
            ``> 0``, the bus retains the last N emitted events and a
            consumer can request them via ``subscribe(..., replay=True)``.
    """

    def __init__(
        self,
        *,
        max_subscribers: int = MAX_SUBSCRIBERS,
        queue_max: int = QUEUE_MAX,
        replay_size: int = 0,
    ) -> None:
        self.max_subscribers = max_subscribers
        self.queue_max = queue_max
        self.replay_size = replay_size
        self._lock = threading.Lock()
        self._subs: list[Subscription] = []
        self._total_emitted: int = 0
        self._replay: deque[tuple[str, dict[str, Any]]] | None = (
            deque(maxlen=replay_size) if replay_size > 0 else None
        )

    def subscribe(
        self,
        types: list[str] | None = None,
        *,
        heartbeat_sec: float = DEFAULT_HEARTBEAT_SEC,
        subscription_class: type[Subscription] = Subscription,
        filter: Callable[[dict[str, Any]], bool] | None = None,
        replay: bool = False,
    ) -> Subscription:
        """Create a new subscription on this bus.

        Args:
            types: glob channel filter list. ``None`` → ``["*"]``.
            heartbeat_sec: idle keepalive interval (``0`` disables).
            subscription_class: custom :class:`Subscription` subclass
                (e.g. one overriding ``matches()`` for field-based filtering).
            filter: optional predicate ``Callable[[dict], bool]``. Events
                that match the glob list but are rejected by this predicate
                are dropped and counted in ``sub.filtered`` (separate from
                queue-full drops in ``sub.dropped``).
            replay: if ``True`` and the bus was constructed with
                ``replay_size > 0``, all matching events currently in the
                replay buffer are delivered to this subscription ahead of
                live events. Replay events bypass queue backpressure
                (they are buffered on the subscription and yielded by
                ``iter()`` / ``aiter()`` before the live queue is drained).
        """
        with self._lock:
            if len(self._subs) >= self.max_subscribers:
                raise SubscriberLimitExceeded(
                    f"max {self.max_subscribers} subscribers, current: {len(self._subs)}"
                )
            sub = subscription_class(
                types or ["*"],
                heartbeat_sec=heartbeat_sec,
                queue_max=self.queue_max,
                filter=filter,
            )
            if replay and self._replay is not None:
                # Stage matching past events on the subscription's replay
                # list — outside the bounded queue (no backpressure).
                for et, ev in self._replay:
                    if not sub.matches(et, ev):
                        continue
                    if filter is not None:
                        try:
                            if not filter(ev):
                                continue
                        except Exception:
                            continue
                    sub._replay.append(ev)
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
        filter: Callable[[dict[str, Any]], bool] | None = None,
        replay: bool = False,
    ) -> Iterator[Subscription]:
        """``with bus.subscribe_ctx([...]) as sub:`` — auto-unsubscribe on exit."""
        sub = self.subscribe(
            types,
            heartbeat_sec=heartbeat_sec,
            subscription_class=subscription_class,
            filter=filter,
            replay=replay,
        )
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
            if self._replay is not None:
                self._replay.append((event_type, event))
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
                    "queue_max": s.queue_max,
                    "received": s.received,
                    "dropped": s.dropped,
                    "filtered": s.filtered,
                    "age_sec": round(time.time() - s.created_at, 1),
                }
                for s in self._subs
            ]
            return {
                "subscribers": len(self._subs),
                "max_subscribers": self.max_subscribers,
                "total_emitted": self._total_emitted,
                "replay_size": self.replay_size,
                "replay_buffered": (
                    len(self._replay) if self._replay is not None else 0
                ),
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
            if self._replay is not None:
                self._replay.clear()


__all__ = ["Bus", "MAX_SUBSCRIBERS"]
