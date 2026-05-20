# Changelog

[Keep a Changelog](https://keepachangelog.com/en/1.1.0/) + [SemVer](https://semver.org/).

## [Unreleased]

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
