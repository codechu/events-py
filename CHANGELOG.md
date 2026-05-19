# Changelog

[Keep a Changelog](https://keepachangelog.com/en/1.1.0/) + [SemVer](https://semver.org/).

## [Unreleased]

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
