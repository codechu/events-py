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
- Public API surface: `Bus`, `default_bus()`, module-level shims
  (`subscribe`, `publish`, etc.), `Subscription`. Anything else is
  internal.
- `Bus.max_subscribers` is configurable per-instance; the
  module-level default is for backwards compatibility — do not
  monkey-patch it in tests.
- Coverage target: ≥85 %. Current ~92 %.

## Discipline reminders (org rules apply)

- Conventional Commits, no AI signature.
- No `--no-verify`, no force push, no unapproved publish.
- See `codechu-org/ai/AGENTS.md` for the full list.
