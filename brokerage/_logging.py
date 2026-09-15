"""Package-local logging; applications own handler setup and event delivery."""

from __future__ import annotations

import logging
from typing import Any

portfolio_logger = logging.getLogger("brokerage.portfolio")
trading_logger = logging.getLogger("brokerage.trading")
plaid_logger = logging.getLogger("brokerage.plaid")


def log_error(source: str, message: str, exc: Any = None, **details: Any) -> None:
    portfolio_logger.warning(
        "[%s] %s exception_type=%s", source, message, type(exc).__name__ if exc else None
    )


def log_event(event_type: str, message: str, **details: Any) -> None:
    portfolio_logger.info("[%s] %s", event_type, message)


def log_portfolio_operation(operation: str, portfolio_data: Any, **details: Any) -> None:
    log_event("portfolio_operation", operation, **details)


def log_alert(alert_type: str, severity: str, message: str, **details: Any) -> None:
    portfolio_logger.warning("[%s:%s] %s", alert_type, severity, message)


def log_critical_alert(alert_type: str, severity: str, message: str, **details: Any) -> None:
    portfolio_logger.critical("[%s:%s] %s", alert_type, severity, message)


def log_service_health(service_name: str, status: str, **details: Any) -> None:
    portfolio_logger.info("[%s] %s", service_name, status)
