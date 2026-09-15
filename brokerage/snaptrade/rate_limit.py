"""Shared failure contract for host-supplied SnapTrade trading rate limits."""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

class TradeRateLimitUnavailable(Exception):
    """Raised when compliant SnapTrade trade pacing cannot be guaranteed."""


def _unconfigured_trade_limiter(
    account_id: str,
    sdk_callable: Callable[[], Any],
) -> Any:
    raise TradeRateLimitUnavailable(
        "SnapTrade trading requires the host distributed rate-limit coordinator"
    )


__all__ = ["TradeRateLimitUnavailable"]
