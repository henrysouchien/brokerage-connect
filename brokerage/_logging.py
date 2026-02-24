"""Brokerage package-local logging shims with monorepo fallback behavior."""

from __future__ import annotations

import logging
import sys
from typing import Any


def _make_fallback_logger(name: str) -> logging.Logger:
    logger = logging.getLogger(f"brokerage.{name}")
    if not logger.handlers:
        handler = logging.StreamHandler(sys.stderr)
        handler.setFormatter(logging.Formatter("%(asctime)s [%(name)s] %(levelname)s: %(message)s"))
        logger.addHandler(handler)
    logger.setLevel(logging.INFO)
    return logger


try:
    from utils.logging import (
        log_error,
        log_portfolio_operation,
        portfolio_logger,
        trading_logger,
    )
except Exception:
    portfolio_logger = _make_fallback_logger("portfolio")
    trading_logger = _make_fallback_logger("trading")

    def log_error(module: str, operation: str, error: Any) -> None:
        portfolio_logger.warning(
            "[%s:%s] %s",
            module,
            operation,
            error,
        )

    def log_portfolio_operation(operation: str, details: Any) -> None:
        portfolio_logger.info("[%s] %s", operation, details)
