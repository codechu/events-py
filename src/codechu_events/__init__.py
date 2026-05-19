"""Thread-safe, multi-channel event bus — producers → subscribers.

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
"""
from __future__ import annotations

import fnmatch
import queue
import threading
import time
from contextlib import contextmanager
from typing import Any, AsyncIterator, Iterator

#: Tek subscription'ın kuyruğunda tutulabilen maks olay.
QUEUE_MAX: int = 200

#: Aynı anda izin verilen maks abone (kaynak korunumu).
MAX_SUBSCRIBERS: int = 64

#: Sync iter idle olduğunda heartbeat aralığı (sn). 0 = kapalı.
DEFAULT_HEARTBEAT_SEC: float = 5.0


class SubscriberLimitExceeded(Exception):
    """``subscribe`` çağrısı MAX_SUBSCRIBERS aşılırken yapıldı."""


class Subscription:
    """Tek subscriber için kuyruk + filtre + iter API.

    Doğrudan oluşturmayın; :func:`subscribe` veya
    :func:`subscribe_ctx` kullanın.
    """

    __slots__ = (
        "types", "queue", "dropped", "received",
        "created_at", "heartbeat_sec", "_closed",
    )

    def __init__(self, types: list[str], heartbeat_sec: float) -> None:
        self.types: list[str] = list(types) if types else ["*"]
        self.queue: queue.Queue[dict[str, Any]] = queue.Queue(maxsize=QUEUE_MAX)
        self.dropped: int = 0  # backpressure ile düşürülen
        self.received: int = 0  # kuyruğa başarıyla giren
        self.created_at: float = time.time()
        self.heartbeat_sec: float = heartbeat_sec
        self._closed: bool = False

    def matches(self, event_type: str, event: dict[str, Any] | None = None) -> bool:
        """Should this subscription receive this event?

        Default behavior: glob match against ``self.types``.

        Subclasses may override to implement custom filtering — for example,
        a regex filter or a field-based filter (only events where
        ``event["panel"] == "suggestion"``). The optional ``event`` argument
        lets such overrides inspect the full payload (it is `None` during
        cheap type-only checks, otherwise the full dict).
        """
        return any(fnmatch.fnmatchcase(event_type, t) for t in self.types)

    def push(self, event: dict[str, Any]) -> None:
        """Yayıncı tarafında çağrılır — non-blocking, kapasite dolu ise düşür."""
        if self._closed:
            return
        try:
            self.queue.put_nowait(event)
            self.received += 1
        except queue.Full:
            self.dropped += 1

    def close(self) -> None:
        """İter'i sonlandırmak için sentinel gönder."""
        self._closed = True
        try:
            self.queue.put_nowait({"event": "_closed"})
        except queue.Full:
            pass  # consumer get'ten sonra zaten close görecek

    # ---- Sync tüketim ----

    def iter(
        self,
        *,
        timeout: float | None = None,
        heartbeat: bool = True,
    ) -> Iterator[dict[str, Any]]:
        """Olayları sıralı döndür.

        ``timeout`` verilirse o kadar süre içinde event gelmezse iter biter.
        ``heartbeat=True`` ve ``self.heartbeat_sec > 0`` ise boş kuyrukta
        periyodik ``_keepalive`` event'i üretilir (consumer dead-detect
        edebilsin). ``close()`` çağrılırsa kuyruk drain edildikten sonra
        iter biter.
        """
        deadline: float | None = None
        if timeout is not None:
            deadline = time.monotonic() + timeout

        hb_interval = (
            self.heartbeat_sec if heartbeat and self.heartbeat_sec > 0 else None
        )

        while True:
            # Etkili get timeout: heartbeat ve deadline'ın min'i
            get_timeout = hb_interval
            if deadline is not None:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    return
                get_timeout = (
                    min(get_timeout, remaining) if get_timeout else remaining
                )
            try:
                event = self.queue.get(timeout=get_timeout)
            except queue.Empty:
                if hb_interval is not None:
                    yield {"event": "_keepalive", "ts": time.time()}
                    continue
                if deadline is not None:
                    return
                # heartbeat ve deadline yoksa sonsuza dek bekle — bu kola
                # düşemeyiz (get_timeout=None ile Empty olmaz).
                continue
            if event.get("event") == "_closed":
                return
            yield event

    def __iter__(self) -> Iterator[dict[str, Any]]:
        return self.iter()

    # ---- Async tüketim ----

    async def aiter(self, *, heartbeat: bool = True) -> AsyncIterator[dict[str, Any]]:
        """asyncio caller'lar için: ``async for ev in sub.aiter(): ...``.

        Implementasyon ``loop.run_in_executor`` ile blocking get'i bir
        executor thread'inde bekler — minimal asyncio entegrasyonu, ekstra
        bağımlılık yok.
        """
        import asyncio

        loop = asyncio.get_event_loop()
        get_timeout = self.heartbeat_sec if heartbeat and self.heartbeat_sec > 0 else None
        while not self._closed:
            event = await loop.run_in_executor(
                None, _blocking_get, self.queue, get_timeout
            )
            if event is _EMPTY_SENTINEL:
                if heartbeat and self.heartbeat_sec > 0:
                    yield {"event": "_keepalive", "ts": time.time()}
                continue
            if event.get("event") == "_closed":
                return
            yield event


# ---- internal helpers ----

_EMPTY_SENTINEL: dict[str, Any] = {"__empty__": True}


def _blocking_get(
    q: queue.Queue[dict[str, Any]],
    timeout: float | None,
) -> dict[str, Any]:
    """``run_in_executor`` için pickle'lanması gereken üst-seviye fonksiyon."""
    try:
        return q.get(timeout=timeout)
    except queue.Empty:
        return _EMPTY_SENTINEL


# ---- Bus class — encapsulated state ----


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


# ---- default global bus + module-level shims ----

_default = Bus()


def subscribe(
    types: list[str] | None = None,
    *,
    heartbeat_sec: float = DEFAULT_HEARTBEAT_SEC,
    subscription_class: type[Subscription] = Subscription,
) -> Subscription:
    """Subscribe on the default global bus. See :meth:`Bus.subscribe`."""
    return _default.subscribe(
        types, heartbeat_sec=heartbeat_sec, subscription_class=subscription_class,
    )


def unsubscribe(sub: Subscription) -> None:
    """Unsubscribe from the default global bus."""
    _default.unsubscribe(sub)


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
    _default.emit(event_type, **fields)


def stats() -> dict[str, Any]:
    """Stats for the default global bus."""
    return _default.stats()


def subscriber_count() -> int:
    """Subscriber count on the default global bus."""
    return _default.subscriber_count()


def reset_for_tests() -> None:
    """Reset the default global bus (used by tests)."""
    _default.reset()


def default_bus() -> Bus:
    """Return the module-level default bus."""
    return _default


__all__ = [
    "Bus",
    "DEFAULT_HEARTBEAT_SEC",
    "MAX_SUBSCRIBERS",
    "QUEUE_MAX",
    "SubscriberLimitExceeded",
    "Subscription",
    "default_bus",
    "emit",
    "reset_for_tests",
    "stats",
    "subscribe",
    "subscribe_ctx",
    "subscriber_count",
    "unsubscribe",
]
