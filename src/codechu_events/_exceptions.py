"""Exception types for codechu_events (private module).

Re-exported from the package root; import from ``codechu_events``.
"""
from __future__ import annotations


class SubscriberLimitExceeded(Exception):
    """Raised when ``subscribe`` is called after MAX_SUBSCRIBERS is reached."""


__all__ = ["SubscriberLimitExceeded"]
