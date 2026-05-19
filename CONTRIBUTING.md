# Contributing to codechu-events

Thanks for thinking about contributing. `codechu-events` is a small,
focused pub/sub library — pure stdlib, no I/O, no UI. Patches that
keep that invariant intact are warmly received.

This library was originally extracted from [Disk Cleaner](https://github.com/codechu/disk-cleaner),
but is maintained independently with its own release cadence.

## Development setup

```bash
git clone https://github.com/codechu/codechu-events-py.git
cd codechu-events-py
pip install -e ".[dev]"
pytest -q
ruff check src tests
```

## Workflow

- Branch names: `feature/<short>`, `fix/<short>`, `refactor/<short>`,
  `docs/<short>`, `test/<short>`.
- Commit messages: [Conventional Commits](https://www.conventionalcommits.org/)
  (`feat:`, `fix:`, `refactor:`, `docs:`, `test:`, `chore:`).
- Open a PR using the template; describe the *why* in the body.
- One change per PR — keep diffs reviewable.

## Bug reports

A useful bug report includes:

- Python version + OS.
- A minimal reproducer (≤30 lines, stdlib-only if possible).
- Expected vs observed behaviour. For race conditions, mention how
  many runs you needed to reproduce.

## Tests

- `pytest -q` must pass; coverage stays at **≥85 %**.
- New feature → new test. Concurrency-related changes need a
  multi-thread test that actually races (use `threading.Barrier` to
  align producers/consumers).
- Don't introduce sleeps for synchronization — use queues, events,
  or barriers. A flaky test is a broken test.
- Prefer fresh `Bus()` instances over the module-level default in
  tests, so suites stay independent.

## Public API discipline

The public surface is `Bus`, `default_bus()`, the module-level shims
(`subscribe`, `subscribe_ctx`, `emit`, `stats`), and `Subscription`.
Everything else is internal — please don't extend it without a
discussion first.

## Style

- `ruff check` + `ruff format` clean.
- Type hints on public APIs (`from __future__ import annotations`).
- Use `logging.getLogger(__name__)`; avoid `print`.

## Security

If you find a security issue, see [SECURITY.md](SECURITY.md) — do not
open a public issue for it.
