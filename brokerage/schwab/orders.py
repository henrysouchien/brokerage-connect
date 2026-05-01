"""Optional schwab.orders helpers kept inside the Schwab boundary."""

from __future__ import annotations

from typing import Any

try:  # pragma: no cover - schwab SDK is optional in some environments
    from schwab.orders import equities
except Exception:  # pragma: no cover - handled by fallback order specs
    equities = None  # type: ignore[assignment]


def build_equity_order_spec(
    *,
    ticker: str,
    side: str,
    quantity: float,
    mapped_type: str,
    duration: str,
    limit_price: str | None,
    stop_price: str | None,
) -> dict[str, Any] | None:
    """Return a schwab.orders-built spec when the builder API is available."""
    if equities is None:
        return None

    side_upper = str(side or "").upper().strip()
    if side_upper == "COVER":
        return None

    qty = float(quantity)
    symbol = str(ticker).upper().strip()

    if mapped_type == "MARKET":
        builder = (
            equities.equity_buy_market(symbol, qty)
            if side_upper == "BUY"
            else equities.equity_sell_market(symbol, qty)
        )
    elif mapped_type == "LIMIT":
        builder = (
            equities.equity_buy_limit(symbol, qty, limit_price)
            if side_upper == "BUY"
            else equities.equity_sell_limit(symbol, qty, limit_price)
        )
    elif mapped_type == "STOP":
        builder = (
            equities.equity_buy_stop(symbol, qty, stop_price)
            if side_upper == "BUY"
            else equities.equity_sell_stop(symbol, qty, stop_price)
        )
    else:
        builder = (
            equities.equity_buy_stop_limit(symbol, qty, stop_price, limit_price)
            if side_upper == "BUY"
            else equities.equity_sell_stop_limit(symbol, qty, stop_price, limit_price)
        )

    if hasattr(builder, "set_duration"):
        builder.set_duration(duration)
    if hasattr(builder, "build"):
        built = builder.build()
        if isinstance(built, dict):
            return built
    return None
