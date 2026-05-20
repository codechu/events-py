"""Subscription class: per-subscriber bounded queue, filter, sync/async iter."""
from __future__ import annotations

import fnmatch
import queue
import time
from typing import Any, AsyncIterator, Callable, Iterator

#: Default maximum events held in a single subscription's queue.
QUEUE_MAX: int = 200

#: Heartbeat interval (seconds) when the sync iterator is idle. 0 disables it.
DEFAULT_HEARTBEAT_SEC: float = 5.0


class Subscription:
    """Per-subscriber queue + filter + iter API.

    Do not instantiate directly; use :meth:`Bus.subscribe` or
    :meth:`Bus.subscribe_ctx`.
    """

    __slots__ = (
        "types", "queue", "queue_max", "dropped", "filtered", "received",
        "created_at", "heartbeat_sec", "_filter", "_replay", "_closed",
    )

    def __init__(
        self,
        types: list[str],
        heartbeat_sec: float,
        *,
        queue_max: int = QUEUE_MAX,
        filter: Callable[[dict[str, Any]], bool] | None = None,
    ) -> None:
        self.types: list[str] = list(types) if types else ["*"]
        self.queue_max: int = queue_max
        self.queue: queue.Queue[dict[str, Any]] = queue.Queue(maxsize=queue_max)
        self.dropped: int = 0  # dropped under backpressure (queue full)
        self.filtered: int = 0  # rejected by user-supplied filter callback
        self.received: int = 0  # successfully enqueued
        self.created_at: float = time.time()
        self.heartbeat_sec: float = heartbeat_sec
        self._filter: Callable[[dict[str, Any]], bool] | None = filter
        # Buffered replay events delivered before live queue is drained.
        self._replay: list[dict[str, Any]] = []
        self._closed: bool = False

    def matches(self, event_type: str, event: dict[str, Any] | None = None) -> bool:
        """Should this subscription receive this event?

        Default behavior: glob match against ``self.types``.

        Subclasses may override to implement custom filtering — for example,
        a regex filter or a field-based filter (only events where
        ``event["panel"] == "suggestion"``). The optional ``event`` argument
        lets such overrides inspect the full payload (it is `None` during
        cheap type-only checks, otherwise the full dict).

        The user-supplied ``filter`` callback (passed to
        :meth:`Bus.subscribe`) is *not* consulted here; it is applied later
        in :meth:`push` so that filter-rejected events are counted in
        ``filtered`` rather than dropped silently.
        """
        return any(fnmatch.fnmatchcase(event_type, t) for t in self.types)

    def push(self, event: dict[str, Any]) -> None:
        """Called by the publisher — non-blocking, drops on full queue.

        If a user-supplied ``filter`` callback was provided and rejects
        the event, ``filtered`` is incremented and nothing is enqueued.
        """
        if self._closed:
            return
        if self._filter is not None:
            try:
                accepted = bool(self._filter(event))
            except Exception:
                accepted = False
            if not accepted:
                self.filtered += 1
                return
        try:
            self.queue.put_nowait(event)
            self.received += 1
        except queue.Full:
            self.dropped += 1

    def close(self) -> None:
        """Send sentinel so the iterator terminates."""
        self._closed = True
        try:
            self.queue.put_nowait({"event": "_closed"})
        except queue.Full:
            pass  # consumer will see _closed after draining

    # ---- Sync consumption ----

    def iter(
        self,
        *,
        timeout: float | None = None,
        heartbeat: bool = True,
    ) -> Iterator[dict[str, Any]]:
        """Yield events in order.

        Replay events (if any were buffered when this subscription was
        created with ``replay=True``) are yielded first, ahead of live
        queue contents and outside the queue's backpressure path.

        If ``timeout`` is given, the iterator stops when no event arrives
        within that window. When ``heartbeat=True`` and ``self.heartbeat_sec
        > 0``, a periodic ``_keepalive`` event is emitted on idle queues so
        consumers can detect a dead connection. When :meth:`close` is
        called, the iterator finishes after the queue is drained.
        """
        # Drain buffered replay events first, before any live event.
        if self._replay:
            replay, self._replay = self._replay, []
            for ev in replay:
                yield ev

        deadline: float | None = None
        if timeout is not None:
            deadline = time.monotonic() + timeout

        hb_interval = (
            self.heartbeat_sec if heartbeat and self.heartbeat_sec > 0 else None
        )

        while True:
            # Effective get timeout: min of heartbeat and deadline.
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
                # No heartbeat and no deadline — unreachable with
                # ``get_timeout=None`` (queue.get won't raise Empty).
                continue
            if event.get("event") == "_closed":
                return
            yield event

    def __iter__(self) -> Iterator[dict[str, Any]]:
        return self.iter()

    # ---- Async consumption ----

    async def aiter(self, *, heartbeat: bool = True) -> AsyncIterator[dict[str, Any]]:
        """For asyncio callers: ``async for ev in sub.aiter(): ...``.

        Replay events (if any) are yielded first, then live events drain
        from the queue via ``loop.run_in_executor``.
        """
        import asyncio

        # Yield buffered replay events first.
        if self._replay:
            replay, self._replay = self._replay, []
            for ev in replay:
                yield ev

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
    """Top-level helper picklable for ``run_in_executor``."""
    try:
        return q.get(timeout=timeout)
    except queue.Empty:
        return _EMPTY_SENTINEL


__all__ = [
    "DEFAULT_HEARTBEAT_SEC",
    "QUEUE_MAX",
    "Subscription",
]
