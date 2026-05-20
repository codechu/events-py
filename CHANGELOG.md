# Changelog

[Keep a Changelog](https://keepachangelog.com/en/1.1.0/) + [SemVer](https://semver.org/).

## [Unreleased]

## [0.2.0] — 2026-05-20

### Changed
- Split the 418-line `__init__.py` god module into focused submodules:
  `subscription.py` (Subscription + queue/iter helpers), `bus.py` (Bus),
  `_exceptions.py` (private exception types), `_testing.py` (private —
  `reset_for_tests`, lazy `default_bus`). The package root re-exports
  the full public API, so `from codechu_events import Bus, Subscription,
  emit, ...` continues to work unchanged.
- Default bus is now lazily constructed on first access via
  `default_bus()`.
- Docstrings converted from Turkish to English throughout.
- Added `__version__` attribute on the package.

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
