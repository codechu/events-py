# Changelog

[Keep a Changelog](https://keepachangelog.com/en/1.1.0/) + [SemVer](https://semver.org/).

## [Unreleased]

## [0.3.0] — 2026-05-20

### Added
- `Bus(replay_size=N)` — optional ring buffer of the last N emitted
  events. New subscribers may request the buffered history via
  `bus.subscribe(..., replay=True)`; replayed events are delivered
  ahead of live events and bypass per-subscription queue backpressure
  (they ride a separate replay list, not the bounded queue).
- `Bus.subscribe(..., filter=callable)` — user-supplied predicate
  applied after the glob match. Rejected events increment the new
  `Subscription.filtered` counter (distinct from queue-full
  `Subscription.dropped`). A filter that raises is treated as a
  rejection.
- `Subscription.filtered` and `Subscription.queue_max` attributes;
  both surfaced via `Bus.stats()`.
- `Bus.stats()` now also reports `replay_size` and `replay_buffered`.

### Fixed
- `Bus(queue_max=N)` is now actually honored. In v0.2 the kwarg was
  accepted but every subscription's queue was still sized from the
  module-level `QUEUE_MAX` constant. The value is now threaded into
  each subscription's bounded queue. Backwards-compatible if you
  never relied on the broken behavior; see
  [`docs/MIGRATION.md`](docs/MIGRATION.md).

## [0.2.0] — 2026-05-20

### Changed
- Split the 418-line `__init__.py` god module into focused submodules:
  `subscription.py` (Subscription + queue/iter helpers), `bus.py` (Bus),
  `_exceptions.py` (private exception types). The package root
  re-exports the narrowed public API.
- Docstrings converted from Turkish to English throughout.
- Added `__version__` attribute on the package.

### Removed (BREAKING)
- Module-level singletons and implicit defaults are gone. The
  `default_bus()`, `reset_for_tests`, and the module-level shim
  functions (`subscribe`, `unsubscribe`, `subscribe_ctx`, `emit`,
  `stats`, `subscriber_count`) have been removed. Callers must now
  construct their own `Bus()` and call methods on it. The new
  ergonomic entry point is `bus.subscribe_ctx(...)`.

## [0.1.0] — 2026-05-19

### Added
- Initial extraction from [codechu/disk-cleaner](https://github.com/codechu/disk-cleaner)
- Multi-channel event bus with glob filter (`["scan.*"]`)
- Thread-safe `emit()` — bounded queue, never blocks publisher
- `subscribe()` / `subscribe_ctx()` context manager
- Sync iteration (`for ev in sub:`) + async iteration (`sub.aiter()`)
- Heartbeat / keepalive on idle
- Resource limits: `MAX_SUBSCRIBERS=64`, `QUEUE_MAX=200`
- `stats()` for monitoring (per-subscriber + global counters)
- Drop count on slow subscribers
