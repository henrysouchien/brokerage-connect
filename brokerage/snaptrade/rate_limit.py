"""Host portability seam for compliant SnapTrade trading rate limits."""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

try:
    from services.snaptrade_trade_limiter import (
        TradeRateLimitUnavailable,
        run_account_trade_slot,
    )
except ImportError:

    class TradeRateLimitUnavailable(Exception):
        """Raised when no distributed SnapTrade trade coordinator is available."""

    def run_account_trade_slot(
        account_id: str,
        sdk_callable: Callable[[], Any],
        **_: Any,
    ) -> Any:
        del account_id, sdk_callable
        raise TradeRateLimitUnavailable(
            "SnapTrade trading requires the host distributed rate-limit coordinator"
        )


__all__ = ["TradeRateLimitUnavailable", "run_account_trade_slot"]
