from __future__ import annotations

from brokerage.snaptrade.adapter import SnapTradeBrokerAdapter
from brokerage.snaptrade._shared import (
    ApiException as SnapTradeApiException,
    handle_snaptrade_api_exception,
    is_snaptrade_secret_error,
)
from brokerage.snaptrade.client import (
    get_account_activities,
    get_activities,
    is_snaptrade_available,
    list_user_accounts,
)
from brokerage.snaptrade.connections import (
    check_snaptrade_connection_health,
    create_snaptrade_connection_url,
    list_snaptrade_connections,
    list_user_brokerage_authorizations,
    remove_snaptrade_connection,
    upgrade_snaptrade_connection_to_trade,
)
from brokerage.snaptrade.recovery import (
    get_snaptrade_rotation_lock,
    recover_snaptrade_auth,
    rotate_snaptrade_user_secret,
)
from brokerage.snaptrade.trading import (
    cancel_snaptrade_order,
    get_snaptrade_orders,
    place_snaptrade_checked_order,
    preview_snaptrade_order,
    search_snaptrade_symbol,
)
from brokerage.snaptrade.users import (
    delete_snaptrade_user,
    get_snaptrade_user_id_from_email,
    register_snaptrade_user,
)


__all__ = [
    "SnapTradeBrokerAdapter",
    "SnapTradeApiException",
    "cancel_snaptrade_order",
    "check_snaptrade_connection_health",
    "create_snaptrade_connection_url",
    "delete_snaptrade_user",
    "get_account_activities",
    "get_activities",
    "get_snaptrade_orders",
    "get_snaptrade_rotation_lock",
    "get_snaptrade_user_id_from_email",
    "handle_snaptrade_api_exception",
    "is_snaptrade_available",
    "is_snaptrade_secret_error",
    "list_user_accounts",
    "list_user_brokerage_authorizations",
    "list_snaptrade_connections",
    "place_snaptrade_checked_order",
    "preview_snaptrade_order",
    "register_snaptrade_user",
    "remove_snaptrade_connection",
    "recover_snaptrade_auth",
    "rotate_snaptrade_user_secret",
    "search_snaptrade_symbol",
    "upgrade_snaptrade_connection_to_trade",
]
