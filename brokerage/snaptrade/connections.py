"""SnapTrade connection management helpers."""

from __future__ import annotations

import hashlib
from typing import Any, Callable, Dict, List, Optional

from brokerage._logging import log_error, portfolio_logger
from brokerage.snaptrade._shared import (
    _budget_kwargs,
    _extract_snaptrade_body,
    _get_snaptrade_identity,
)
from brokerage.snaptrade.client import (
    _call_with_secret_rotation,
    _detail_brokerage_authorization_with_retry,
    _get_user_account_balance_with_retry,
    _list_user_brokerage_authorizations_with_retry,
    _login_snap_trade_user_with_retry,
    _refresh_brokerage_authorization_with_retry,
    _remove_brokerage_authorization_with_retry,
    _require_snaptrade_client,
    _symbol_search_user_account_with_retry,
    list_user_accounts,
)
from brokerage.snaptrade.users import get_snaptrade_user_id_from_email
from settings import FRONTEND_BASE_URL


def _normalize_payload_list(payload: Any) -> list[dict[str, Any]]:
    if not isinstance(payload, list):
        return []

    normalized: list[dict[str, Any]] = []
    for row in payload:
        if hasattr(row, "to_dict"):
            row = row.to_dict()
        if isinstance(row, dict):
            normalized.append(row)
    return normalized


def create_snaptrade_connection_url(
    user_email: str,
    user_secret: str,
    connection_type: str | None = None,
    *,
    budget_user_id: int | None = None,
) -> str:
    """Create a SnapTrade connection URL for account linking."""
    client = _require_snaptrade_client()

    try:
        snaptrade_user_id, user_secret = _get_snaptrade_identity(user_email, user_secret)

        response = _login_snap_trade_user_with_retry(
            client,
            snaptrade_user_id,
            user_secret,
            broker=None,
            immediate_redirect=True,
            custom_redirect=f"{FRONTEND_BASE_URL}/snaptrade/success",
            connection_type=connection_type,
            **_budget_kwargs(budget_user_id),
        )

        return response.body["redirectURI"]
    except Exception as e:
        log_error("snaptrade_connection", "create_url", e)
        raise


def upgrade_snaptrade_connection_to_trade(
    user_email: str,
    user_secret: str,
    authorization_id: str,
    *,
    budget_user_id: int | None = None,
) -> str:
    """Upgrade existing read-only authorization to trading-enabled."""
    client = _require_snaptrade_client()

    try:
        snaptrade_user_id, user_secret = _get_snaptrade_identity(user_email, user_secret)
        response = _login_snap_trade_user_with_retry(
            client,
            snaptrade_user_id,
            user_secret,
            immediate_redirect=False,
            connection_type="trade",
            reconnect=authorization_id,
            **_budget_kwargs(budget_user_id),
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


def list_user_brokerage_authorizations(
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
        lambda user_id, secret: _list_user_brokerage_authorizations_with_retry(
            client,
            user_id,
            secret,
            **_budget_kwargs(budget_user_id),
        ),
        on_secret_rotated=on_secret_rotated,
        refresh_secret=refresh_secret,
        operation_name="list_user_brokerage_authorizations",
        user_id=budget_user_id,
        budget_user_id=budget_user_id,
    )
    return _normalize_payload_list(_extract_snaptrade_body(response))


def list_snaptrade_connections(
    user_email: str,
    user_secret: str,
    *,
    on_secret_rotated: Callable[[str], None] | None = None,
    refresh_secret: Callable[[], str | None] | None = None,
    budget_user_id: int | None = None,
) -> List[Dict[str, Any]]:
    """List user's SnapTrade brokerage connections."""
    try:
        accounts = list_user_accounts(
            user_email,
            user_secret,
            on_secret_rotated=on_secret_rotated,
            refresh_secret=refresh_secret,
            **_budget_kwargs(budget_user_id),
        )
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
    user_secret: str,
    probe_trading: bool = False,
    *,
    on_secret_rotated: Callable[[str], None] | None = None,
    refresh_secret: Callable[[], str | None] | None = None,
    budget_user_id: int | None = None,
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
        current_secret = user_secret

        def _on_rotated(new_secret: str) -> None:
            nonlocal current_secret
            current_secret = new_secret
            if on_secret_rotated is not None:
                on_secret_rotated(new_secret)

        def _refresh_current() -> str | None:
            nonlocal current_secret
            if refresh_secret is None:
                return current_secret
            refreshed = refresh_secret()
            if refreshed:
                current_secret = refreshed
            return refreshed

        user_hash = hashlib.sha256(snaptrade_user_id.encode()).hexdigest()[:16]
        client = _require_snaptrade_client()
        accounts = list_user_accounts(
            user_email,
            current_secret,
            on_secret_rotated=_on_rotated,
            refresh_secret=_refresh_current,
            **_budget_kwargs(budget_user_id),
        )
        if not accounts:
            return []

        _user_id, current_secret = _get_snaptrade_identity(user_email, current_secret)

        portfolio_logger.debug(
            "Running SnapTrade connection health check for user_hash=%s, probe_trading=%s",
            user_hash,
            probe_trading,
        )

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
                    user_secret=current_secret,
                    **_budget_kwargs(budget_user_id),
                )
                detail = _extract_snaptrade_body(detail_response)
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
                        user_secret=current_secret,
                        account_id=probe_account_id,
                        **_budget_kwargs(budget_user_id),
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
                        user_secret=current_secret,
                        account_id=probe_account_id,
                        substring="AAPL",
                        **_budget_kwargs(budget_user_id),
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


def refresh_brokerage_authorization(
    authorization_id: str,
    user_email: str,
    user_secret: str,
    *,
    budget_user_id: int | None = None,
):
    client = _require_snaptrade_client()
    user_id, user_secret = _get_snaptrade_identity(user_email, user_secret)
    response = _refresh_brokerage_authorization_with_retry(
        client=client,
        authorization_id=authorization_id,
        user_id=user_id,
        user_secret=user_secret,
        **_budget_kwargs(budget_user_id),
    )
    return _extract_snaptrade_body(response)


def remove_snaptrade_connection(
    user_email: str,
    user_secret: str,
    authorization_id: str,
    *,
    on_secret_rotated: Callable[[str], None] | None = None,
    refresh_secret: Callable[[], str | None] | None = None,
    budget_user_id: int | None = None,
) -> None:
    """Remove one SnapTrade brokerage authorization."""
    try:
        _call_with_secret_rotation(
            user_email,
            user_secret,
            lambda user_id, secret: _remove_brokerage_authorization_with_retry(
                _require_snaptrade_client(),
                user_id,
                secret,
                authorization_id,
                **_budget_kwargs(budget_user_id),
            ),
            on_secret_rotated=on_secret_rotated,
            refresh_secret=refresh_secret,
            operation_name="remove_snaptrade_connection",
            user_id=budget_user_id,
            budget_user_id=budget_user_id,
        )
        portfolio_logger.info("✅ Removed SnapTrade connection: %s", authorization_id)
    except Exception as e:
        log_error("snaptrade_connection", "remove_connection", e)
        raise


__all__ = [
    "check_snaptrade_connection_health",
    "create_snaptrade_connection_url",
    "list_snaptrade_connections",
    "list_user_brokerage_authorizations",
    "refresh_brokerage_authorization",
    "remove_snaptrade_connection",
    "upgrade_snaptrade_connection_to_trade",
]
