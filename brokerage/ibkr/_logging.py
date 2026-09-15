"""IBKR package logging."""

from __future__ import annotations

import logging
import sys
import time
from collections.abc import Callable

_timing_logger: Callable[[str, str, float], object] | None = None


def configure_timing(*, log_timing_event: Callable[[str, str, float], object]) -> None:
    """Bind the application's timing event sink before IBKR operations."""
    global _timing_logger
    _timing_logger = log_timing_event

logger = logging.getLogger("ibkr")

# Ensure at least stderr output when no root handlers are configured
# (standalone/CLI usage outside the monorepo).
if not logging.root.handlers and not logger.handlers:
    _handler = logging.StreamHandler(sys.stderr)
    _handler.setFormatter(logging.Formatter("%(asctime)s [%(name)s] %(levelname)s: %(message)s"))
    logger.addHandler(_handler)
    logger.setLevel(logging.INFO)


def log_event(logger: logging.Logger, level: int, event: str, msg: str = "", **fields) -> None:
    """Log with structured key=value fields.

    Output: [ibkr.connect] Connected to gateway client_id=20 elapsed_ms=142
    """
    parts = [f"[ibkr.{event}]"]
    if msg:
        parts.append(msg)
    for key, value in fields.items():
        if value is not None:
            parts.append(f"{key}={value}")
    logger.log(level, " ".join(parts))


class TimingContext:
    """Context manager for elapsed time measurement."""

    def __init__(self, name: str | None = None):
        self.name = name

    def __enter__(self):
        self.start = time.monotonic()
        return self

    def __exit__(self, *args):
        self.elapsed_ms = round((time.monotonic() - self.start) * 1000, 1)
        if self.name and _timing_logger is not None:
            try:
                _timing_logger("dependency", self.name, self.elapsed_ms)
            except Exception:
                pass
