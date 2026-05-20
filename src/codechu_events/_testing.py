"""Test helpers for codechu_events (private module).

These are separated from the production API to make their role explicit.
They remain re-exported from the package root for backwards compatibility.
"""
from __future__ import annotations

from .bus import Bus

# Lazy default bus; populated on first access via default_bus().
_default: Bus | None = None


def default_bus() -> Bus:
    """Return the module-level default bus, creating it on first access."""
    global _default
    if _default is None:
        _default = Bus()
    return _default


def reset_for_tests() -> None:
    """Reset the default global bus (used by tests)."""
    default_bus().reset()


__all__ = ["default_bus", "reset_for_tests"]
