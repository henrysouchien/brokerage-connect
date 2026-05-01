"""SnapTrade trading helpers for symbol search, preview, place, list, and cancel."""

from __future__ import annotations

from typing import Any, Callable, Dict, List, Optional

from brokerage._logging import log_error
from brokerage.snaptrade._shared import _budget_kwargs, _extract_snaptrade_body, _to_float
from brokerage.snaptrade.client import (
    _call_with_secret_rotation,
    _cancel_order_with_retry,
    _get_order_impact_with_retry,
    _get_user_account_orders_with_retry,
    _place_order_with_retry,
    _require_snaptrade_client,
    _symbol_search_user_account_with_retry,
)


def search_snaptrade_symbol(
    user_email: str,
    user_secret: str,
    account_id: str,
    ticker: str,
    *,
    on_secret_rotated: Callable[[str], None] | None = None,
    refresh_secret: Callable[[], str | None] | None = None,
    budget_user_id: int | None = None,
) -> Dict[str, Any]:
    """Search account-supported symbols and require exact ticker match."""
    client = _require_snaptrade_client()

    try:
        ticker_upper = (ticker or "").upper().strip()
        if not ticker_upper:
            raise ValueError("Ticker is required")

        response = _call_with_secret_rotation(
            user_email,
            user_secret,
            lambda user_id, secret: _symbol_search_user_account_with_retry(
                client,
                user_id,
                secret,
                account_id,
                ticker_upper,
                **_budget_kwargs(budget_user_id),
            ),
            on_secret_rotated=on_secret_rotated,
            refresh_secret=refresh_secret,
            operation_name="search_snaptrade_symbol",
            user_id=budget_user_id,
            budget_user_id=budget_user_id,
        )
        symbols = _extract_snaptrade_body(response) or []

        normalized: List[Dict[str, Any]] = []
        for item in symbols:
            entry = item if isinstance(item, dict) else {}
            symbol_value = entry.get("symbol")
            if isinstance(symbol_value, dict):
                symbol_text = (symbol_value.get("symbol") or "").upper().strip()
            else:
                symbol_text = str(symbol_value or "").upper().strip()
            normalized.append(
                {
                    "id": entry.get("id") or entry.get("universal_symbol_id"),
                    "symbol": symbol_text,
                    "raw_symbol": str(entry.get("raw_symbol") or "").upper().strip(),
                    "name": entry.get("description") or entry.get("name"),
                    "currency": entry.get("currency"),
                    "type": entry.get("type"),
                    "full": entry,
                }
            )

        exact_matches = [s for s in normalized if s.get("symbol") == ticker_upper]
        if not exact_matches:
            close_matches = [s.get("symbol") for s in normalized if s.get("symbol")]
            preview = ", ".join(close_matches[:8]) if close_matches else "none"
            raise ValueError(
                f"No exact symbol match for '{ticker_upper}' in account {account_id}. "
                f"Closest matches: {preview}"
            )

        exact = exact_matches[0]
        universal_symbol_id = exact.get("id")
        if not universal_symbol_id:
            raise ValueError(f"Exact symbol match for '{ticker_upper}' missing universal symbol id")

        return {
            "ticker": ticker_upper,
            "symbol": exact.get("symbol"),
            "universal_symbol_id": universal_symbol_id,
            "raw_symbol": exact.get("raw_symbol"),
            "name": exact.get("name"),
            "currency": exact.get("currency"),
            "type": exact.get("type"),
            "all_matches": normalized,
        }
    except Exception as e:
        log_error("snaptrade_trading", "search_symbol", e)
        raise


def preview_snaptrade_order(
    user_email: str,
    user_secret: str,
    account_id: str,
    ticker: str,
    side: str,
    quantity: float,
    order_type: str = "Market",
    time_in_force: str = "Day",
    limit_price: Optional[float] = None,
    stop_price: Optional[float] = None,
    universal_symbol_id: Optional[str] = None,
    *,
    on_secret_rotated: Callable[[str], None] | None = None,
    refresh_secret: Callable[[], str | None] | None = None,
    budget_user_id: int | None = None,
) -> Dict[str, Any]:
    """Preview an order via SnapTrade `get_order_impact` and parse key fields."""
    client = _require_snaptrade_client()

    try:
        side = (side or "").upper().strip()

        symbol_info = None
        resolved_symbol_id = universal_symbol_id
        if not resolved_symbol_id:
            symbol_info = search_snaptrade_symbol(
                user_email=user_email,
                user_secret=user_secret,
                account_id=account_id,
                ticker=ticker,
                on_secret_rotated=on_secret_rotated,
                refresh_secret=refresh_secret,
                **_budget_kwargs(budget_user_id),
            )
            resolved_symbol_id = symbol_info["universal_symbol_id"]

        response = _call_with_secret_rotation(
            user_email,
            user_secret,
            lambda user_id, secret: _get_order_impact_with_retry(
                client=client,
                user_id=user_id,
                user_secret=secret,
                account_id=account_id,
                side=side,
                universal_symbol_id=resolved_symbol_id,
                order_type=order_type,
                time_in_force=time_in_force,
                quantity=float(quantity),
                limit_price=_to_float(limit_price),
                stop_price=_to_float(stop_price),
                **_budget_kwargs(budget_user_id),
            ),
            on_secret_rotated=on_secret_rotated,
            refresh_secret=refresh_secret,
            operation_name="preview_snaptrade_order",
            user_id=budget_user_id,
            budget_user_id=budget_user_id,
        )

        impact = _extract_snaptrade_body(response) or {}
        trade = impact.get("trade") or {}
        trade_impacts = impact.get("trade_impacts") or []

        estimated_commission = 0.0
        for impact_row in trade_impacts:
            if isinstance(impact_row, dict):
                estimated_commission += _to_float(impact_row.get("estimated_commission")) or 0.0
                estimated_commission += _to_float(impact_row.get("forex_fees")) or 0.0

        estimated_price = _to_float(trade.get("price"))
        if estimated_price is None:
            estimated_price = _to_float(limit_price)
        if estimated_price is None:
            estimated_price = _to_float(stop_price)

        estimated_total = None
        if estimated_price is not None:
            estimated_total = (estimated_price * float(quantity)) + estimated_commission
        elif estimated_commission > 0:
            estimated_total = estimated_commission

        return {
            "account_id": account_id,
            "ticker": (ticker or "").upper().strip(),
            "side": side,
            "quantity": float(quantity),
            "order_type": order_type,
            "time_in_force": time_in_force,
            "limit_price": _to_float(limit_price),
            "stop_price": _to_float(stop_price),
            "universal_symbol_id": resolved_symbol_id,
            "symbol_info": symbol_info,
            "snaptrade_trade_id": trade.get("id"),
            "estimated_price": estimated_price,
            "estimated_commission": estimated_commission,
            "estimated_total": estimated_total,
            "combined_remaining_balance": impact.get("combined_remaining_balance"),
            "trade_impacts": trade_impacts,
            "impact_response": impact,
        }
    except Exception as e:
        log_error("snaptrade_trading", "preview_order", e)
        raise


def place_snaptrade_checked_order(
    user_email: str,
    user_secret: str,
    snaptrade_trade_id: str,
    wait_to_confirm: bool = True,
    *,
    on_secret_rotated: Callable[[str], None] | None = None,
    refresh_secret: Callable[[], str | None] | None = None,
    budget_user_id: int | None = None,
) -> Dict[str, Any]:
    """Submit previously previewed order by SnapTrade `trade_id`."""
    client = _require_snaptrade_client()

    try:
        response = _call_with_secret_rotation(
            user_email,
            user_secret,
            lambda user_id, secret: _place_order_with_retry(
                client=client,
                user_id=user_id,
                user_secret=secret,
                trade_id=snaptrade_trade_id,
                wait_to_confirm=wait_to_confirm,
                **_budget_kwargs(budget_user_id),
            ),
            on_secret_rotated=on_secret_rotated,
            refresh_secret=refresh_secret,
            operation_name="place_snaptrade_checked_order",
            user_id=budget_user_id,
            budget_user_id=budget_user_id,
        )
        return _extract_snaptrade_body(response) or {}
    except Exception as e:
        log_error("snaptrade_trading", "place_order", e)
        raise


def get_snaptrade_orders(
    user_email: str,
    user_secret: str,
    account_id: str,
    state: str = "all",
    days: int = 30,
    *,
    on_secret_rotated: Callable[[str], None] | None = None,
    refresh_secret: Callable[[], str | None] | None = None,
    budget_user_id: int | None = None,
) -> List[Dict[str, Any]]:
    """Fetch account orders from `account_information` namespace."""
    client = _require_snaptrade_client()

    try:
        response = _call_with_secret_rotation(
            user_email,
            user_secret,
            lambda user_id, secret: _get_user_account_orders_with_retry(
                client=client,
                user_id=user_id,
                user_secret=secret,
                account_id=account_id,
                state=state,
                days=days,
                **_budget_kwargs(budget_user_id),
            ),
            on_secret_rotated=on_secret_rotated,
            refresh_secret=refresh_secret,
            operation_name="get_snaptrade_orders",
            user_id=budget_user_id,
            budget_user_id=budget_user_id,
        )
        orders = _extract_snaptrade_body(response) or []
        return orders if isinstance(orders, list) else [orders]
    except Exception as e:
        log_error("snaptrade_trading", "get_orders", e)
        raise


def cancel_snaptrade_order(
    user_email: str,
    user_secret: str,
    account_id: str,
    order_id: str,
    *,
    on_secret_rotated: Callable[[str], None] | None = None,
    refresh_secret: Callable[[], str | None] | None = None,
    budget_user_id: int | None = None,
) -> Dict[str, Any]:
    """Cancel an existing brokerage order."""
    client = _require_snaptrade_client()

    try:
        response = _call_with_secret_rotation(
            user_email,
            user_secret,
            lambda user_id, secret: _cancel_order_with_retry(
                client=client,
                user_id=user_id,
                user_secret=secret,
                account_id=account_id,
                brokerage_order_id=order_id,
                **_budget_kwargs(budget_user_id),
            ),
            on_secret_rotated=on_secret_rotated,
            refresh_secret=refresh_secret,
            operation_name="cancel_snaptrade_order",
            user_id=budget_user_id,
            budget_user_id=budget_user_id,
        )
        return _extract_snaptrade_body(response) or {}
    except Exception as e:
        log_error("snaptrade_trading", "cancel_order", e)
        raise


__all__ = [
    "cancel_snaptrade_order",
    "get_snaptrade_orders",
    "place_snaptrade_checked_order",
    "preview_snaptrade_order",
    "search_snaptrade_symbol",
]
