# CLAUDE.md — codechu-events

Bootstrap per `codechu-org/ai/AGENTS.md` §0 before any work. Prefer
the local clone at `$org_home/codechu-org/ai/AGENTS.md` (if
`~/.config/codechu/config.toml` has `org_home` set); otherwise
WebFetch the public raw URL
<https://raw.githubusercontent.com/codechu/codechu-org/main/ai/AGENTS.md>.
This file lists only product-local overrides.

## Product-local notes

- Pure-Python, no GTK / no I/O. Tests in `tests/` must keep that
  invariant — do not introduce framework dependencies.
- Public API surface: `Bus`, `Subscription`, `SubscriberLimitExceeded`,
  and the constants `QUEUE_MAX`, `MAX_SUBSCRIBERS`,
  `DEFAULT_HEARTBEAT_SEC`. No module-level singletons, no implicit
  default bus — callers construct their own `Bus()`.
- `Bus.max_subscribers` is configurable per-instance — pass it to the
  constructor in tests rather than mutating a shared instance.
- Coverage target: ≥85 %. Current ~92 %.

## Discipline reminders (org rules apply)

- Conventional Commits, no AI signature.
- No `--no-verify`, no force push, no unapproved publish.
- See `codechu-org/ai/AGENTS.md` for the full list.
