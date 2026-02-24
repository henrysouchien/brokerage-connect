"""SnapTrade connection management helpers."""

from __future__ import annotations

import hashlib
from typing import Any, Dict, List, Optional

from brokerage._logging import log_error, portfolio_logger
from brokerage.config import FRONTEND_BASE_URL
from brokerage.snaptrade.client import (
    _detail_brokerage_authorization_with_retry,
    _get_user_account_balance_with_retry,
    _list_user_accounts_with_retry,
    _login_snap_trade_user_with_retry,
    _symbol_search_user_account_with_retry,
    get_snaptrade_client,
)
from brokerage.snaptrade.secrets import get_snaptrade_user_secret
from brokerage.snaptrade.users import get_snaptrade_user_id_from_email, register_snaptrade_user


def create_snaptrade_connection_url(
    user_email: str,
    client,
    connection_type: str = "trade",
) -> str:
    """Create a SnapTrade connection URL for account linking."""
    try:
        snaptrade_user_id = get_snaptrade_user_id_from_email(user_email)
        user_secret = get_snaptrade_user_secret(user_email)

        if not user_secret:
            user_secret = register_snaptrade_user(user_email, client)

        response = _login_snap_trade_user_with_retry(
            client,
            snaptrade_user_id,
            user_secret,
            broker=None,
            immediate_redirect=True,
            custom_redirect=f"{FRONTEND_BASE_URL}/snaptrade/success",
            connection_type=connection_type,
        )

        return response.body["redirectURI"]
    except Exception as e:
        log_error("snaptrade_connection", "create_url", e)
        raise


def upgrade_snaptrade_connection_to_trade(
    user_email: str,
    authorization_id: str,
    client=None,
) -> str:
    """Upgrade existing read-only authorization to trading-enabled."""
    if not client:
        client = get_snaptrade_client()
    if not client:
        raise ValueError("SnapTrade client unavailable")

    try:
        snaptrade_user_id = get_snaptrade_user_id_from_email(user_email)
        user_secret = get_snaptrade_user_secret(user_email)

        if not user_secret:
            raise ValueError(f"No SnapTrade user secret found for {user_email}")

        response = _login_snap_trade_user_with_retry(
            client,
            snaptrade_user_id,
            user_secret,
            immediate_redirect=False,
            connection_type="trade",
            reconnect=authorization_id,
        )

        redirect_uri = response.body["redirectURI"]
        portfolio_logger.info(
            "✅ Generated trading upgrade URL for authorization %s",
            authorization_id,
        )
        return redirect_uri

    except Exception as e:
        log_error("snaptrade_connection", "upgrade_to_trade", e)
        raise


def list_snaptrade_connections(user_email: str, client) -> List[Dict[str, Any]]:
    """List user's SnapTrade brokerage connections."""
    try:
        snaptrade_user_id = get_snaptrade_user_id_from_email(user_email)
        user_secret = get_snaptrade_user_secret(user_email)

        if not user_secret:
            return []

        accounts_response = _list_user_accounts_with_retry(
            client,
            snaptrade_user_id,
            user_secret,
        )
        accounts = accounts_response.body if hasattr(accounts_response, "body") else accounts_response

        connections: List[Dict[str, Any]] = []
        for account in accounts:
            connections.append(
                {
                    "authorization_id": account.get("brokerage_authorization"),
                    "brokerage_name": account.get("institution_name", "Unknown"),
                    "account_id": account.get("id"),
                    "account_name": account.get("name"),
                    "account_number": account.get("number"),
                    "account_type": account.get("meta", {}).get("type", "Unknown"),
                    "status": "active",
                }
            )

        return connections

    except Exception as e:
        log_error("snaptrade_connection", "list_connections", e)
        raise


def check_snaptrade_connection_health(
    user_email: str,
    client,
    probe_trading: bool = False,
) -> List[Dict[str, Any]]:
    """Check SnapTrade connection health grouped by authorization ID."""

    def _normalize_authorization_id(auth_value: Any) -> Optional[str]:
        if isinstance(auth_value, dict):
            auth_id = auth_value.get("id")
            return str(auth_id) if auth_id else None
        if auth_value:
            return str(auth_value)
        return None

    try:
        snaptrade_user_id = get_snaptrade_user_id_from_email(user_email)
        user_hash = hashlib.sha256(snaptrade_user_id.encode()).hexdigest()[:16]
        user_secret = get_snaptrade_user_secret(user_email)

        if not user_secret:
            return []

        portfolio_logger.debug(
            "Running SnapTrade connection health check for user_hash=%s, probe_trading=%s",
            user_hash,
            probe_trading,
        )

        try:
            accounts_response = _list_user_accounts_with_retry(
                client,
                snaptrade_user_id,
                user_secret,
            )
            accounts = accounts_response.body if hasattr(accounts_response, "body") else accounts_response
            if not isinstance(accounts, list):
                accounts = []
        except Exception as list_error:
            log_error("snaptrade_connection", "health_check_list_user_accounts", list_error)
            return []

        grouped: Dict[str, Dict[str, Any]] = {}
        for account in accounts:
            if not isinstance(account, dict):
                continue

            account_id = account.get("id")
            auth_id = _normalize_authorization_id(account.get("brokerage_authorization"))
            if not auth_id:
                auth_id = f"unknown:{account_id}" if account_id else "unknown"

            entry = grouped.setdefault(
                auth_id,
                {
                    "authorization_id": auth_id,
                    "brokerage_name": account.get("institution_name", "Unknown"),
                    "account_ids": [],
                    "probe_account_id": None,
                },
            )

            if account_id is not None:
                entry["account_ids"].append(str(account_id))
                if entry["probe_account_id"] is None:
                    entry["probe_account_id"] = str(account_id)

            if not entry.get("brokerage_name") and account.get("institution_name"):
                entry["brokerage_name"] = account.get("institution_name")

        health_results: List[Dict[str, Any]] = []
        for authorization_id, entry in grouped.items():
            brokerage_name = entry.get("brokerage_name") or "Unknown"
            connection_type = "unknown"
            disabled = False
            disabled_date = None

            try:
                detail_response = _detail_brokerage_authorization_with_retry(
                    client=client,
                    authorization_id=authorization_id,
                    user_id=snaptrade_user_id,
                    user_secret=user_secret,
                )
                detail = detail_response.body if hasattr(detail_response, "body") else detail_response
                if hasattr(detail, "to_dict"):
                    detail = detail.to_dict()
                if isinstance(detail, dict):
                    connection_type = detail.get("type") or detail.get("connection_type") or connection_type
                    disabled = bool(detail.get("disabled", False))
                    disabled_date = detail.get("disabled_date")

                    brokerage = detail.get("brokerage")
                    if isinstance(brokerage, dict):
                        brokerage_name = brokerage.get("name") or brokerage_name
                    brokerage_name = detail.get("brokerage_name") or brokerage_name
            except Exception as detail_error:
                log_error(
                    "snaptrade_connection",
                    "health_check_detail_brokerage_authorization",
                    detail_error,
                )

            probe_account_id = entry.get("probe_account_id")
            data_ok = False
            if probe_account_id:
                try:
                    _get_user_account_balance_with_retry(
                        client=client,
                        user_id=snaptrade_user_id,
                        user_secret=user_secret,
                        account_id=probe_account_id,
                    )
                    data_ok = True
                except Exception as balance_error:
                    log_error(
                        "snaptrade_connection",
                        "health_check_get_user_account_balance",
                        balance_error,
                    )

            trading_ok = None
            trading_error = None
            if probe_trading and probe_account_id:
                try:
                    _symbol_search_user_account_with_retry(
                        client=client,
                        user_id=snaptrade_user_id,
                        user_secret=user_secret,
                        account_id=probe_account_id,
                        substring="AAPL",
                    )
                    trading_ok = True
                except Exception as trading_probe_error:
                    trading_ok = False
                    trading_error = str(trading_probe_error)
                    log_error(
                        "snaptrade_connection",
                        "health_check_symbol_search_user_account",
                        trading_probe_error,
                    )

            health_results.append(
                {
                    "authorization_id": str(authorization_id),
                    "brokerage_name": brokerage_name,
                    "connection_type": connection_type,
                    "disabled": disabled,
                    "disabled_date": disabled_date,
                    "account_ids": entry.get("account_ids", []),
                    "data_ok": data_ok,
                    "trading_ok": trading_ok,
                    "trading_error": trading_error,
                }
            )

        return health_results

    except Exception as e:
        log_error("snaptrade_connection", "check_connection_health", e)
        return []


def remove_snaptrade_connection(user_email: str, authorization_id: str, client) -> None:
    """Remove one SnapTrade brokerage authorization."""
    try:
        snaptrade_user_id = get_snaptrade_user_id_from_email(user_email)
        user_secret = get_snaptrade_user_secret(user_email)

        if not user_secret:
            raise ValueError(f"No SnapTrade user secret found for {user_email}")

        client.connections.remove_brokerage_authorization(
            user_id=snaptrade_user_id,
            user_secret=user_secret,
            authorization_id=authorization_id,
        )

        portfolio_logger.info("✅ Removed SnapTrade connection: %s", authorization_id)

    except Exception as e:
        log_error("snaptrade_connection", "remove_connection", e)
        raise


__all__ = [
    "check_snaptrade_connection_health",
    "create_snaptrade_connection_url",
    "list_snaptrade_connections",
    "remove_snaptrade_connection",
    "upgrade_snaptrade_connection_to_trade",
]
