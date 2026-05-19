# Tests — codechu-events

Run the suite from the repo root:

```bash
pytest -q
```

With coverage:

```bash
pytest --cov=codechu_events --cov-report=term-missing
```

## Coverage gate

The coverage floor is **85 %**. PRs that drop below it are rejected;
add tests with your change.

## Conventions

- Prefer a fresh `Bus()` per test over the module-level default —
  suites stay independent.
- Concurrency tests use `threading.Barrier` to align producers /
  consumers. Do not use `time.sleep()` for synchronization.
- Async tests use `pytest-asyncio` with the default event-loop
  scope.
