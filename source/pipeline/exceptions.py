from __future__ import annotations


class BrowserCapacityUnavailable(RuntimeError):
    """Raised when Selenium has no usable browser capacity yet."""


class BrowserSessionLost(RuntimeError):
    """Raised when a running Selenium browser session disappears."""
