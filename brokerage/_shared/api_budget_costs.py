"""API budget cost table bundled for standalone brokerage-connect installs."""

from __future__ import annotations

from decimal import Decimal
from typing import Literal

try:
    from config.api_budget_costs import (  # type: ignore[import-not-found]
        COST_PER_CALL,
        LLM_PRICES,
        SNAPTRADE_PER_CONNECTED_USER_MONTH_RATE,
        SNAPTRADE_SUBSCRIPTION_OPS,
        SUBSCRIPTION_COSTS_PER_ITEM_MONTH,
        get_cost_model_and_rate,
    )
except ModuleNotFoundError as exc:
    if exc.name not in {"config", "config.api_budget_costs"}:
        raise

    COST_PER_CALL: dict[tuple[str, str], Decimal] = {
        ("plaid", "accounts_balance_get"): Decimal("0.1000"),
        ("plaid", "investments_refresh"): Decimal("0.1200"),
        ("snaptrade", "connections.refresh_brokerage_authorization"): Decimal("0.0500"),
        ("snaptrade", "accounts.orders"): Decimal("0.0020"),
        ("schwab", "get_account"): Decimal("0.0000"),
        ("schwab", "get_accounts"): Decimal("0.0000"),
        ("ibkr", "reqPositions"): Decimal("0.0000"),
        ("ibkr", "reqAccountSummary"): Decimal("0.0000"),
        ("fmp", "fetch"): Decimal("0.0000"),
        ("fmp_estimates", "get"): Decimal("0.0000"),
    }

    SUBSCRIPTION_COSTS_PER_ITEM_MONTH: dict[tuple[str, str], Decimal] = {
        ("plaid", "investments_holdings_get"): Decimal("0.1800"),
        ("plaid", "investments_transactions_get"): Decimal("0.3500"),
        ("plaid", "transactions_get"): Decimal("0.3000"),
        ("plaid", "liabilities_get"): Decimal("0.2000"),
    }

    SNAPTRADE_PER_CONNECTED_USER_MONTH_RATE: Decimal = Decimal("1.5000")
    SNAPTRADE_SUBSCRIPTION_OPS: frozenset[str] = frozenset(
        {
            "authentication.register_snap_trade_user",
            "authentication.login_snap_trade_user",
            "authentication.delete_snap_trade_user",
            "authentication.reset_snap_trade_user_secret",
            "accounts.list",
            "accounts.positions",
            "accounts.balance",
            "accounts.activities",
            "connections.list_brokerage_authorizations",
            "connections.detail_brokerage_authorization",
            "connections.remove_brokerage_authorization",
            "reference_data.symbol_search_user_account",
            "trading.get_order_impact",
            "trading.place_order",
            "trading.cancel_order",
            "transactions_and_reporting.get_activities",
        }
    )

    LLM_PRICES: dict[str, dict[str, float]] = {
        "gpt-5.4-mini": {"input_per_1m_tokens": 0.75, "output_per_1m_tokens": 4.50},
        "gpt-4.1": {"input_per_1m_tokens": 2.00, "output_per_1m_tokens": 8.00},
        "gpt-4.1-mini": {"input_per_1m_tokens": 0.40, "output_per_1m_tokens": 1.60},
        "gpt-4o-mini": {"input_per_1m_tokens": 0.15, "output_per_1m_tokens": 0.60},
        "claude-sonnet-4-6": {"input_per_1m_tokens": 3.00, "output_per_1m_tokens": 15.00},
        "claude-haiku-4-5": {"input_per_1m_tokens": 1.00, "output_per_1m_tokens": 5.00},
    }
    _LLM_PROVIDERS_LOCAL = frozenset({"openai", "anthropic"})

    def get_cost_model_and_rate(
        provider: str, operation: str
    ) -> tuple[
        Literal["per_call", "per_item_month", "per_connected_user_month", "per_token"],
        Decimal | None,
    ]:
        provider_key = str(provider or "").strip().lower()
        operation_key = str(operation or "").strip()
        key = (provider_key, operation_key)

        plaid_sub_rate = SUBSCRIPTION_COSTS_PER_ITEM_MONTH.get(key)
        if plaid_sub_rate is not None:
            return ("per_item_month", plaid_sub_rate)

        if provider_key == "snaptrade" and operation_key in SNAPTRADE_SUBSCRIPTION_OPS:
            return ("per_connected_user_month", SNAPTRADE_PER_CONNECTED_USER_MONTH_RATE)

        if provider_key in _LLM_PROVIDERS_LOCAL:
            return ("per_token", None)

        return ("per_call", COST_PER_CALL.get(key, Decimal("0")))


__all__ = [
    "COST_PER_CALL",
    "LLM_PRICES",
    "SNAPTRADE_PER_CONNECTED_USER_MONTH_RATE",
    "SNAPTRADE_SUBSCRIPTION_OPS",
    "SUBSCRIPTION_COSTS_PER_ITEM_MONTH",
    "get_cost_model_and_rate",
]
