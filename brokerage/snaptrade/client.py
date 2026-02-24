"""SnapTrade client bootstrap and retry-wrapped SDK operations."""

from __future__ import annotations

from typing import Any, Dict, Optional, TYPE_CHECKING

from brokerage._logging import log_error, portfolio_logger
from brokerage.snaptrade._shared import with_snaptrade_retry
from brokerage.snaptrade.secrets import get_snaptrade_app_credentials

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
    """Initialize SnapTrade SDK client with credentials from env/secrets."""
    if not SnapTrade:
        portfolio_logger.warning("⚠️ SnapTrade SDK not available")
        return None

    try:
        app_credentials = get_snaptrade_app_credentials(region_name)
        client = SnapTrade(
            consumer_key=app_credentials["consumer_key"],
            client_id=app_credentials["client_id"],
        )
        portfolio_logger.info("✅ SnapTrade client initialized successfully")
        return client
    except Exception as e:
        log_error("snaptrade_client", "initialization", e)
        portfolio_logger.error("❌ Failed to initialize SnapTrade client: %s", e)
        return None


@with_snaptrade_retry("register_snap_trade_user")
def _register_snap_trade_user_with_retry(client: SnapTrade, user_id: str):
    return client.authentication.register_snap_trade_user(user_id=user_id)


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
    return client.authentication.login_snap_trade_user(**kwargs)


@with_snaptrade_retry("list_user_accounts")
def _list_user_accounts_with_retry(client: SnapTrade, user_id: str, user_secret: str):
    return client.account_information.list_user_accounts(
        user_id=user_id,
        user_secret=user_secret,
    )


@with_snaptrade_retry("detail_brokerage_authorization")
def _detail_brokerage_authorization_with_retry(
    client: SnapTrade,
    authorization_id: str,
    user_id: str,
    user_secret: str,
):
    return client.connections.detail_brokerage_authorization(
        authorization_id=authorization_id,
        user_id=user_id,
        user_secret=user_secret,
    )


@with_snaptrade_retry("get_user_account_positions")
def _get_user_account_positions_with_retry(
    client: SnapTrade,
    user_id: str,
    user_secret: str,
    account_id: str,
):
    return client.account_information.get_user_account_positions(
        user_id=user_id,
        user_secret=user_secret,
        account_id=account_id,
    )


@with_snaptrade_retry("get_user_account_balance")
def _get_user_account_balance_with_retry(
    client: SnapTrade,
    user_id: str,
    user_secret: str,
    account_id: str,
):
    return client.account_information.get_user_account_balance(
        user_id=user_id,
        user_secret=user_secret,
        account_id=account_id,
    )


@with_snaptrade_retry("remove_brokerage_authorization")
def _remove_brokerage_authorization_with_retry(
    client: SnapTrade,
    user_id: str,
    user_secret: str,
    authorization_id: str,
):
    return client.connections.remove_brokerage_authorization(
        user_id=user_id,
        user_secret=user_secret,
        authorization_id=authorization_id,
    )


@with_snaptrade_retry("delete_snap_trade_user")
def _delete_snap_trade_user_with_retry(client: SnapTrade, user_id: str):
    return client.authentication.delete_snap_trade_user(user_id=user_id)


@with_snaptrade_retry("symbol_search_user_account")
def _symbol_search_user_account_with_retry(
    client: SnapTrade,
    user_id: str,
    user_secret: str,
    account_id: str,
    substring: str,
):
    return client.reference_data.symbol_search_user_account(
        user_id=user_id,
        user_secret=user_secret,
        account_id=account_id,
        substring=substring,
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
):
    return client.trading.get_order_impact(
        user_id=user_id,
        user_secret=user_secret,
        account_id=account_id,
        action=side,
        universal_symbol_id=universal_symbol_id,
        order_type=order_type,
        time_in_force=time_in_force,
        units=quantity,
        price=limit_price,
        stop=stop_price,
    )


@with_snaptrade_retry("place_order")
def _place_order_with_retry(
    client: SnapTrade,
    user_id: str,
    user_secret: str,
    trade_id: str,
    wait_to_confirm: bool = True,
):
    return client.trading.place_order(
        user_id=user_id,
        user_secret=user_secret,
        trade_id=trade_id,
        wait_to_confirm=wait_to_confirm,
    )


@with_snaptrade_retry("get_user_account_orders")
def _get_user_account_orders_with_retry(
    client: SnapTrade,
    user_id: str,
    user_secret: str,
    account_id: str,
    state: str = "all",
    days: int = 30,
):
    return client.account_information.get_user_account_orders(
        user_id=user_id,
        user_secret=user_secret,
        account_id=account_id,
        state=state,
        days=days,
    )


@with_snaptrade_retry("cancel_order")
def _cancel_order_with_retry(
    client: SnapTrade,
    user_id: str,
    user_secret: str,
    account_id: str,
    brokerage_order_id: str,
):
    return client.trading.cancel_order(
        user_id=user_id,
        user_secret=user_secret,
        account_id=account_id,
        brokerage_order_id=brokerage_order_id,
    )


snaptrade_client = get_snaptrade_client()


__all__ = [
    "_cancel_order_with_retry",
    "_delete_snap_trade_user_with_retry",
    "_detail_brokerage_authorization_with_retry",
    "_get_order_impact_with_retry",
    "_get_user_account_balance_with_retry",
    "_get_user_account_orders_with_retry",
    "_get_user_account_positions_with_retry",
    "_list_user_accounts_with_retry",
    "_login_snap_trade_user_with_retry",
    "_place_order_with_retry",
    "_register_snap_trade_user_with_retry",
    "_remove_brokerage_authorization_with_retry",
    "_symbol_search_user_account_with_retry",
    "get_snaptrade_client",
    "snaptrade_client",
]
