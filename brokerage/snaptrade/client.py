"""SnapTrade client bootstrap and retry-wrapped SDK operations."""

from __future__ import annotations

import functools
from typing import Any, Callable, Dict, Optional, TYPE_CHECKING

from app_platform.api_budget import guard_call
from brokerage._logging import log_error, portfolio_logger
from brokerage.snaptrade._shared import (
    ApiException,
    _budget_kwargs,
    _extract_snaptrade_body,
    _get_snaptrade_identity,
    is_snaptrade_secret_error,
    with_snaptrade_retry,
)
from brokerage.config import SNAPTRADE_CLIENT_ID, SNAPTRADE_CONSUMER_KEY
from config.api_budget_costs import COST_PER_CALL

if TYPE_CHECKING:
    from snaptrade_client import SnapTrade
else:
    try:
        from snaptrade_client import SnapTrade
        portfolio_logger.info("✅ SnapTrade SDK imported successfully")
    except ImportError as e:  # pragma: no cover - sdk optional in some environments
        portfolio_logger.warning("⚠️ SnapTrade SDK not available: %s", e)
        portfolio_logger.warning("Run: pip install snaptrade-python-sdk")

        class SnapTrade:  # type: ignore[no-redef]
            pass


def get_snaptrade_client(region_name: str = "us-east-1") -> Optional[SnapTrade]:
    """Initialize SnapTrade SDK client with credentials from environment variables."""
    del region_name
    if not SnapTrade:
        portfolio_logger.warning("⚠️ SnapTrade SDK not available")
        return None

    try:
        if not SNAPTRADE_CLIENT_ID or not SNAPTRADE_CONSUMER_KEY:
            raise RuntimeError("SNAPTRADE_CLIENT_ID and SNAPTRADE_CONSUMER_KEY are required")
        client = SnapTrade(
            consumer_key=SNAPTRADE_CONSUMER_KEY,
            client_id=SNAPTRADE_CLIENT_ID,
        )
        portfolio_logger.info("✅ SnapTrade client initialized successfully")
        return client
    except Exception as e:
        log_error("snaptrade_client", "initialization", e)
        portfolio_logger.error("❌ Failed to initialize SnapTrade client: %s", e)
        return None


@functools.lru_cache(maxsize=1)
def _get_or_create_client() -> Optional[SnapTrade]:
    return get_snaptrade_client()


def _require_snaptrade_client() -> SnapTrade:
    client = _get_or_create_client()
    if client is None:
        raise RuntimeError("SnapTrade client unavailable")
    return client


def is_snaptrade_available() -> bool:
    return _get_or_create_client() is not None


def _snaptrade_cost_per_call(operation: str) -> Any:
    return COST_PER_CALL.get(("snaptrade", operation), 0)


def _call_with_secret_rotation(
    user_email: str,
    user_secret: str,
    operation: Callable[[str, str], Any],
    *,
    on_secret_rotated: Callable[[str], None] | None = None,
    refresh_secret: Callable[[], str | None] | None = None,
    operation_name: str | None = None,
    user_id: int | None = None,
    budget_user_id: int | None = None,
) -> Any:
    from brokerage.snaptrade.recovery import get_snaptrade_rotation_lock, rotate_snaptrade_user_secret

    snaptrade_user_id, current_secret = _get_snaptrade_identity(user_email, user_secret)

    try:
        return operation(snaptrade_user_id, current_secret)
    except ApiException as exc:
        if not is_snaptrade_secret_error(exc):
            raise
        portfolio_logger.warning(
            "SnapTrade secret rejected during %s for user_id=%s; attempting rotation",
            operation_name or "operation",
            user_id,
        )

    lock = get_snaptrade_rotation_lock(user_email)
    with lock:
        if refresh_secret is not None:
            refreshed_secret = refresh_secret()
            if refreshed_secret and refreshed_secret != current_secret:
                snaptrade_user_id, refreshed_secret = _get_snaptrade_identity(user_email, refreshed_secret)
                result = operation(snaptrade_user_id, refreshed_secret)
                if on_secret_rotated is not None:
                    on_secret_rotated(refreshed_secret)
                return result

        rotated_secret = rotate_snaptrade_user_secret(
            user_email,
            current_secret,
            **_budget_kwargs(budget_user_id),
        )
        if on_secret_rotated is not None:
            on_secret_rotated(rotated_secret)
        snaptrade_user_id, rotated_secret = _get_snaptrade_identity(user_email, rotated_secret)
        return operation(snaptrade_user_id, rotated_secret)


@with_snaptrade_retry("register_snap_trade_user")
def _register_snap_trade_user_with_retry(
    client: SnapTrade,
    user_id: str,
    budget_user_id: int | None = None,
):
    return guard_call(
        provider="snaptrade",
        operation="authentication.register_snap_trade_user",
        budget_user_id=budget_user_id,
        cost_per_call=_snaptrade_cost_per_call("authentication.register_snap_trade_user"),
        fn=client.authentication.register_snap_trade_user,
        kwargs={"user_id": user_id},
    )


@with_snaptrade_retry("login_snap_trade_user")
def _login_snap_trade_user_with_retry(
    client: SnapTrade,
    user_id: str,
    user_secret: str,
    broker=None,
    immediate_redirect: bool = True,
    custom_redirect: str | None = None,
    connection_type: Optional[str] = None,
    reconnect: Optional[str] = None,
    budget_user_id: int | None = None,
):
    kwargs: Dict[str, Any] = dict(
        user_id=user_id,
        user_secret=user_secret,
        broker=broker,
        immediate_redirect=immediate_redirect,
        custom_redirect=custom_redirect,
    )
    if connection_type is not None:
        kwargs["connection_type"] = connection_type
    if reconnect is not None:
        kwargs["reconnect"] = reconnect
    return guard_call(
        provider="snaptrade",
        operation="authentication.login_snap_trade_user",
        budget_user_id=budget_user_id,
        cost_per_call=_snaptrade_cost_per_call("authentication.login_snap_trade_user"),
        fn=client.authentication.login_snap_trade_user,
        kwargs=kwargs,
    )


@with_snaptrade_retry("list_user_accounts")
def _list_user_accounts_with_retry(
    client: SnapTrade,
    user_id: str,
    user_secret: str,
    budget_user_id: int | None = None,
):
    return guard_call(
        provider="snaptrade",
        operation="accounts.list",
        budget_user_id=budget_user_id,
        cost_per_call=_snaptrade_cost_per_call("accounts.list"),
        fn=client.account_information.list_user_accounts,
        kwargs={
            "user_id": user_id,
            "user_secret": user_secret,
        },
    )


@with_snaptrade_retry("list_user_brokerage_authorizations")
def _list_user_brokerage_authorizations_with_retry(
    client: SnapTrade,
    user_id: str,
    user_secret: str,
    budget_user_id: int | None = None,
):
    return guard_call(
        provider="snaptrade",
        operation="connections.list_brokerage_authorizations",
        budget_user_id=budget_user_id,
        cost_per_call=_snaptrade_cost_per_call("connections.list_brokerage_authorizations"),
        fn=client.connections.list_brokerage_authorizations,
        kwargs={
            "user_id": user_id,
            "user_secret": user_secret,
        },
    )


@with_snaptrade_retry("detail_brokerage_authorization")
def _detail_brokerage_authorization_with_retry(
    client: SnapTrade,
    authorization_id: str,
    user_id: str,
    user_secret: str,
    budget_user_id: int | None = None,
):
    return guard_call(
        provider="snaptrade",
        operation="connections.detail_brokerage_authorization",
        budget_user_id=budget_user_id,
        cost_per_call=_snaptrade_cost_per_call("connections.detail_brokerage_authorization"),
        fn=client.connections.detail_brokerage_authorization,
        kwargs={
            "authorization_id": authorization_id,
            "user_id": user_id,
            "user_secret": user_secret,
        },
    )


@with_snaptrade_retry("get_user_account_positions")
def _get_user_account_positions_with_retry(
    client: SnapTrade,
    user_id: str,
    user_secret: str,
    account_id: str,
    budget_user_id: int | None = None,
):
    return guard_call(
        provider="snaptrade",
        operation="accounts.positions",
        budget_user_id=budget_user_id,
        cost_per_call=_snaptrade_cost_per_call("accounts.positions"),
        fn=client.account_information.get_user_account_positions,
        kwargs={
            "user_id": user_id,
            "user_secret": user_secret,
            "account_id": account_id,
        },
    )


@with_snaptrade_retry("get_user_account_balance")
def _get_user_account_balance_with_retry(
    client: SnapTrade,
    user_id: str,
    user_secret: str,
    account_id: str,
    budget_user_id: int | None = None,
):
    return guard_call(
        provider="snaptrade",
        operation="accounts.balance",
        budget_user_id=budget_user_id,
        cost_per_call=_snaptrade_cost_per_call("accounts.balance"),
        fn=client.account_information.get_user_account_balance,
        kwargs={
            "user_id": user_id,
            "user_secret": user_secret,
            "account_id": account_id,
        },
    )


@with_snaptrade_retry("get_account_activities")
def _get_account_activities_with_retry(
    client: SnapTrade,
    user_id: str,
    user_secret: str,
    account_id: str,
    *,
    start_date,
    end_date,
    offset: int,
    limit: int,
    budget_user_id: int | None = None,
):
    return guard_call(
        provider="snaptrade",
        operation="accounts.activities",
        budget_user_id=budget_user_id,
        cost_per_call=_snaptrade_cost_per_call("accounts.activities"),
        fn=client.account_information.get_account_activities,
        kwargs={
            "account_id": account_id,
            "user_id": user_id,
            "user_secret": user_secret,
            "start_date": start_date,
            "end_date": end_date,
            "offset": offset,
            "limit": limit,
        },
    )


@with_snaptrade_retry("remove_brokerage_authorization")
def _remove_brokerage_authorization_with_retry(
    client: SnapTrade,
    user_id: str,
    user_secret: str,
    authorization_id: str,
    budget_user_id: int | None = None,
):
    return guard_call(
        provider="snaptrade",
        operation="connections.remove_brokerage_authorization",
        budget_user_id=budget_user_id,
        cost_per_call=_snaptrade_cost_per_call("connections.remove_brokerage_authorization"),
        fn=client.connections.remove_brokerage_authorization,
        kwargs={
            "user_id": user_id,
            "user_secret": user_secret,
            "authorization_id": authorization_id,
        },
    )


@with_snaptrade_retry("delete_snap_trade_user")
def _delete_snap_trade_user_with_retry(
    client: SnapTrade,
    user_id: str,
    budget_user_id: int | None = None,
):
    return guard_call(
        provider="snaptrade",
        operation="authentication.delete_snap_trade_user",
        budget_user_id=budget_user_id,
        cost_per_call=_snaptrade_cost_per_call("authentication.delete_snap_trade_user"),
        fn=client.authentication.delete_snap_trade_user,
        kwargs={"user_id": user_id},
    )


@with_snaptrade_retry("reset_snap_trade_user_secret")
def _reset_snap_trade_user_secret_with_retry(
    client: SnapTrade,
    user_id: str,
    user_secret: str,
    budget_user_id: int | None = None,
):
    return guard_call(
        provider="snaptrade",
        operation="authentication.reset_snap_trade_user_secret",
        budget_user_id=budget_user_id,
        cost_per_call=_snaptrade_cost_per_call("authentication.reset_snap_trade_user_secret"),
        fn=client.authentication.reset_snap_trade_user_secret,
        kwargs={
            "user_id": user_id,
            "user_secret": user_secret,
        },
    )


@with_snaptrade_retry("refresh_brokerage_authorization")
def _refresh_brokerage_authorization_with_retry(
    client: SnapTrade,
    authorization_id: str,
    user_id: str,
    user_secret: str,
    budget_user_id: int | None = None,
):
    return guard_call(
        provider="snaptrade",
        operation="connections.refresh_brokerage_authorization",
        budget_user_id=budget_user_id,
        cost_per_call=_snaptrade_cost_per_call("connections.refresh_brokerage_authorization"),
        fn=client.connections.refresh_brokerage_authorization,
        kwargs={
            "authorization_id": authorization_id,
            "user_id": user_id,
            "user_secret": user_secret,
        },
    )


@with_snaptrade_retry("symbol_search_user_account")
def _symbol_search_user_account_with_retry(
    client: SnapTrade,
    user_id: str,
    user_secret: str,
    account_id: str,
    substring: str,
    budget_user_id: int | None = None,
):
    return guard_call(
        provider="snaptrade",
        operation="reference_data.symbol_search_user_account",
        budget_user_id=budget_user_id,
        cost_per_call=_snaptrade_cost_per_call("reference_data.symbol_search_user_account"),
        fn=client.reference_data.symbol_search_user_account,
        kwargs={
            "user_id": user_id,
            "user_secret": user_secret,
            "account_id": account_id,
            "substring": substring,
        },
    )


@with_snaptrade_retry("get_order_impact")
def _get_order_impact_with_retry(
    client: SnapTrade,
    user_id: str,
    user_secret: str,
    account_id: str,
    side: str,
    universal_symbol_id: str,
    order_type: str,
    time_in_force: str,
    quantity: float,
    limit_price: Optional[float] = None,
    stop_price: Optional[float] = None,
    budget_user_id: int | None = None,
):
    return guard_call(
        provider="snaptrade",
        operation="trading.get_order_impact",
        budget_user_id=budget_user_id,
        cost_per_call=_snaptrade_cost_per_call("trading.get_order_impact"),
        fn=client.trading.get_order_impact,
        kwargs={
            "user_id": user_id,
            "user_secret": user_secret,
            "account_id": account_id,
            "action": side,
            "universal_symbol_id": universal_symbol_id,
            "order_type": order_type,
            "time_in_force": time_in_force,
            "units": quantity,
            "price": limit_price,
            "stop": stop_price,
        },
    )


@with_snaptrade_retry("place_order")
def _place_order_with_retry(
    client: SnapTrade,
    user_id: str,
    user_secret: str,
    trade_id: str,
    wait_to_confirm: bool = True,
    budget_user_id: int | None = None,
):
    return guard_call(
        provider="snaptrade",
        operation="trading.place_order",
        budget_user_id=budget_user_id,
        cost_per_call=_snaptrade_cost_per_call("trading.place_order"),
        fn=client.trading.place_order,
        kwargs={
            "user_id": user_id,
            "user_secret": user_secret,
            "trade_id": trade_id,
            "wait_to_confirm": wait_to_confirm,
        },
    )


@with_snaptrade_retry("get_user_account_orders")
def _get_user_account_orders_with_retry(
    client: SnapTrade,
    user_id: str,
    user_secret: str,
    account_id: str,
    state: str = "all",
    days: int = 30,
    budget_user_id: int | None = None,
):
    return guard_call(
        provider="snaptrade",
        operation="accounts.orders",
        budget_user_id=budget_user_id,
        cost_per_call=_snaptrade_cost_per_call("accounts.orders"),
        fn=client.account_information.get_user_account_orders,
        kwargs={
            "user_id": user_id,
            "user_secret": user_secret,
            "account_id": account_id,
            "state": state,
            "days": days,
        },
    )


@with_snaptrade_retry("cancel_order")
def _cancel_order_with_retry(
    client: SnapTrade,
    user_id: str,
    user_secret: str,
    account_id: str,
    brokerage_order_id: str,
    budget_user_id: int | None = None,
):
    return guard_call(
        provider="snaptrade",
        operation="trading.cancel_order",
        budget_user_id=budget_user_id,
        cost_per_call=_snaptrade_cost_per_call("trading.cancel_order"),
        fn=client.trading.cancel_order,
        kwargs={
            "user_id": user_id,
            "user_secret": user_secret,
            "account_id": account_id,
            "brokerage_order_id": brokerage_order_id,
        },
    )


@with_snaptrade_retry("get_activities")
def _get_activities_with_retry(
    client: SnapTrade,
    user_id: str,
    user_secret: str,
    *,
    start_date,
    end_date,
    budget_user_id: int | None = None,
):
    return guard_call(
        provider="snaptrade",
        operation="transactions_and_reporting.get_activities",
        budget_user_id=budget_user_id,
        cost_per_call=_snaptrade_cost_per_call("transactions_and_reporting.get_activities"),
        fn=client.transactions_and_reporting.get_activities,
        kwargs={
            "user_id": user_id,
            "user_secret": user_secret,
            "start_date": start_date,
            "end_date": end_date,
        },
    )


def list_user_accounts(
    user_email: str,
    user_secret: str,
    *,
    on_secret_rotated: Callable[[str], None] | None = None,
    refresh_secret: Callable[[], str | None] | None = None,
    budget_user_id: int | None = None,
) -> list[dict[str, Any]]:
    client = _require_snaptrade_client()
    response = _call_with_secret_rotation(
        user_email,
        user_secret,
        lambda user_id, secret: _list_user_accounts_with_retry(
            client,
            user_id,
            secret,
            **_budget_kwargs(budget_user_id),
        ),
        on_secret_rotated=on_secret_rotated,
        refresh_secret=refresh_secret,
        operation_name="list_user_accounts",
        user_id=budget_user_id,
        budget_user_id=budget_user_id,
    )
    payload = _extract_snaptrade_body(response)
    if not isinstance(payload, list):
        return []
    return [row for row in payload if isinstance(row, dict)]


def get_account_activities(
    user_email: str,
    user_secret: str,
    account_id: str,
    *,
    start_date,
    end_date,
    offset: int,
    limit: int,
    on_secret_rotated: Callable[[str], None] | None = None,
    refresh_secret: Callable[[], str | None] | None = None,
    budget_user_id: int | None = None,
) -> dict[str, Any]:
    client = _require_snaptrade_client()
    response = _call_with_secret_rotation(
        user_email,
        user_secret,
        lambda user_id, secret: _get_account_activities_with_retry(
            client,
            user_id,
            secret,
            account_id,
            start_date=start_date,
            end_date=end_date,
            offset=offset,
            limit=limit,
            **_budget_kwargs(budget_user_id),
        ),
        on_secret_rotated=on_secret_rotated,
        refresh_secret=refresh_secret,
        operation_name="get_account_activities",
        user_id=budget_user_id,
        budget_user_id=budget_user_id,
    )
    payload = _extract_snaptrade_body(response)
    if isinstance(payload, dict):
        return payload
    if isinstance(payload, list):
        return {"data": [row for row in payload if isinstance(row, dict)]}
    return {"data": []}


def get_activities(
    user_email: str,
    user_secret: str,
    *,
    start_date,
    end_date,
    on_secret_rotated: Callable[[str], None] | None = None,
    refresh_secret: Callable[[], str | None] | None = None,
    budget_user_id: int | None = None,
) -> list[dict[str, Any]]:
    client = _require_snaptrade_client()
    response = _call_with_secret_rotation(
        user_email,
        user_secret,
        lambda user_id, secret: _get_activities_with_retry(
            client,
            user_id,
            secret,
            start_date=start_date,
            end_date=end_date,
            **_budget_kwargs(budget_user_id),
        ),
        on_secret_rotated=on_secret_rotated,
        refresh_secret=refresh_secret,
        operation_name="get_activities",
        user_id=budget_user_id,
        budget_user_id=budget_user_id,
    )
    payload = _extract_snaptrade_body(response)
    if not isinstance(payload, list):
        return []

    normalized: list[dict[str, Any]] = []
    for row in payload:
        if hasattr(row, "to_dict"):
            row = row.to_dict()
        if isinstance(row, dict):
            normalized.append(row)
    return normalized


__all__ = [
    "_call_with_secret_rotation",
    "_cancel_order_with_retry",
    "_delete_snap_trade_user_with_retry",
    "_detail_brokerage_authorization_with_retry",
    "_get_account_activities_with_retry",
    "_get_activities_with_retry",
    "_get_order_impact_with_retry",
    "_get_or_create_client",
    "_get_user_account_balance_with_retry",
    "_get_user_account_orders_with_retry",
    "_get_user_account_positions_with_retry",
    "_list_user_accounts_with_retry",
    "_list_user_brokerage_authorizations_with_retry",
    "_login_snap_trade_user_with_retry",
    "_place_order_with_retry",
    "_refresh_brokerage_authorization_with_retry",
    "_require_snaptrade_client",
    "_reset_snap_trade_user_secret_with_retry",
    "_register_snap_trade_user_with_retry",
    "_remove_brokerage_authorization_with_retry",
    "_symbol_search_user_account_with_retry",
    "get_account_activities",
    "get_activities",
    "get_snaptrade_client",
    "is_snaptrade_available",
    "list_user_accounts",
]
