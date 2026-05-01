"""SnapTrade broker adapter implementation for trade execution workflows.

Called by:
- ``services.trade_execution_service.TradeExecutionService`` via ``BrokerAdapter``.

Calls into:
- ``snaptrade_loader`` wrappers for account lookup, preview, place, list, cancel.
"""

from __future__ import annotations

import math
from datetime import datetime, timezone
from typing import Any, Callable, Dict, List, Optional, Tuple

from brokerage._logging import portfolio_logger
from brokerage.broker_adapter import BrokerAdapter
from brokerage.snaptrade._shared import ApiException, is_snaptrade_secret_error
from brokerage.snaptrade.client import (
    _get_user_account_balance_with_retry,
    _list_user_accounts_with_retry,
    _require_snaptrade_client,
)
from brokerage.snaptrade.connections import (
    list_user_brokerage_authorizations,
    refresh_brokerage_authorization,
)
from brokerage.snaptrade.recovery import _try_rotate_secret
from brokerage.snaptrade.trading import (
    cancel_snaptrade_order,
    get_snaptrade_orders,
    place_snaptrade_checked_order,
    preview_snaptrade_order,
    search_snaptrade_symbol,
)
from brokerage.snaptrade.users import get_snaptrade_user_id_from_email
from brokerage.trade_objects import BrokerAccount, CancelResult, OrderPreview, OrderResult, OrderStatus


class SnapTradeBrokerAdapter(BrokerAdapter):
    """BrokerAdapter implementation for SnapTrade-managed accounts."""

    ACCOUNT_CACHE_SECONDS = 60

    def __init__(
        self,
        user_email: str,
        user_secret: str,
        snaptrade_client: Optional[object] = None,
        region: str = "us-east-1",
        user_id: Optional[int] = None,
        on_secret_rotated: Callable[[str], None] | None = None,
        refresh_secret: Callable[[], str | None] | None = None,
        on_refresh: Callable[[str], None] | None = None,
    ) -> None:
        self._user_email = user_email
        if not user_secret:
            raise ValueError(f"SnapTrade user_secret required for {user_email}")
        self._user_secret = user_secret
        self._region = region
        self._user_id = user_id
        self._snaptrade_client = snaptrade_client
        self._accounts_cache: Optional[List[Dict[str, Any]]] = None
        self._accounts_cache_at: Optional[datetime] = None
        self._on_secret_rotated = on_secret_rotated
        self._refresh_secret_callback = refresh_secret
        self._on_refresh = on_refresh or (lambda _account_id: None)

    @property
    def provider_name(self) -> str:
        return "snaptrade"

    def owns_account(self, account_id: str) -> bool:
        accounts = self._fetch_accounts(force_refresh=False)
        for account in accounts:
            if str(account.get("id")) == str(account_id):
                return True
        return False

    def list_accounts(self) -> List[BrokerAccount]:
        """List SnapTrade accounts that can accept trading orders."""
        accounts = self._fetch_accounts(force_refresh=False)
        out: List[BrokerAccount] = []
        for account in accounts:
            account_id = str(account.get("id"))
            cash_balance = self.get_account_balance(account_id)
            account_type_raw = account.get("account_type") or account.get("type")
            account_type = str(account_type_raw).upper().strip() if account_type_raw is not None else None
            account_name = account.get("name")
            institution_name = account.get("institution_name")
            meta = account.get("meta") if isinstance(account.get("meta"), dict) else {}
            out.append(
                BrokerAccount(
                    account_id=account_id,
                    account_name=str(account_name) if account_name is not None else None,
                    brokerage_name=str(institution_name) if institution_name is not None else "SnapTrade",
                    provider=self.provider_name,
                    cash_balance=cash_balance,
                    available_funds=cash_balance,
                    account_type=account_type,
                    authorization_id=self._extract_authorization_id(account),
                    meta=meta,
                )
            )
        return out

    def search_symbol(self, account_id: str, ticker: str) -> Dict[str, Any]:
        return search_snaptrade_symbol(
            user_email=self._user_email,
            user_secret=self._user_secret,
            account_id=account_id,
            ticker=ticker,
            on_secret_rotated=self._handle_secret_rotated,
            refresh_secret=self._refresh_secret,
            budget_user_id=self._user_id,
        )

    def preview_order(
        self,
        account_id: str,
        ticker: str,
        side: str,
        quantity: float,
        order_type: str,
        time_in_force: str,
        limit_price: Optional[float] = None,
        stop_price: Optional[float] = None,
        symbol_id: Optional[str] = None,
    ) -> OrderPreview:
        """Request broker-native preview/impact estimate from SnapTrade."""
        side_upper = str(side or "").upper().strip()
        if side_upper == "SHORT":
            raise ValueError(
                "SnapTrade does not support SHORT orders. "
                "Use an IBKR account for short selling."
            )
        if side_upper == "COVER":
            side = "BUY"

        preview_raw = preview_snaptrade_order(
            user_email=self._user_email,
            user_secret=self._user_secret,
            account_id=account_id,
            ticker=ticker,
            side=side.upper(),
            quantity=float(quantity),
            order_type=order_type,
            time_in_force=time_in_force,
            limit_price=limit_price,
            stop_price=stop_price,
            universal_symbol_id=symbol_id,
            on_secret_rotated=self._handle_secret_rotated,
            refresh_secret=self._refresh_secret,
            budget_user_id=self._user_id,
        )
        preview = preview_raw if isinstance(preview_raw, dict) else {}
        broker_trade_id = preview.get("snaptrade_trade_id") or preview.get("broker_trade_id")
        trade_impacts = preview.get("trade_impacts")
        impact_response = preview.get("impact_response")
        broker_preview_data = {
            "symbol_info": preview.get("symbol_info"),
            "universal_symbol_id": preview.get("universal_symbol_id"),
        }
        return OrderPreview(
            estimated_price=_to_float(preview.get("estimated_price")),
            estimated_total=_to_float(preview.get("estimated_total")),
            estimated_commission=_to_float(preview.get("estimated_commission")),
            broker_trade_id=str(broker_trade_id) if broker_trade_id is not None else None,
            combined_remaining_balance=preview.get("combined_remaining_balance"),
            trade_impacts=trade_impacts if isinstance(trade_impacts, list) else [],
            impact_response=impact_response if isinstance(impact_response, dict) else None,
            broker_preview_data=broker_preview_data,
        )

    def place_order(
        self,
        account_id: str,
        order_params: Dict[str, Any],
    ) -> OrderResult:
        """Submit previously-previewed SnapTrade order by trade identifier."""
        snaptrade_trade_id = str(order_params.get("snaptrade_trade_id") or "")
        if not snaptrade_trade_id:
            raise ValueError("Missing snaptrade_trade_id for SnapTrade order placement")

        response_raw = place_snaptrade_checked_order(
            user_email=self._user_email,
            user_secret=self._user_secret,
            snaptrade_trade_id=snaptrade_trade_id,
            wait_to_confirm=bool(order_params.get("wait_to_confirm", True)),
            on_secret_rotated=self._handle_secret_rotated,
            refresh_secret=self._refresh_secret,
            budget_user_id=self._user_id,
        )
        response = response_raw if isinstance(response_raw, dict) else {}
        brokerage_order_id = (
            response.get("brokerage_order_id")
            or response.get("order_id")
            or response.get("id")
        )
        return OrderResult(
            brokerage_order_id=str(brokerage_order_id) if brokerage_order_id is not None else None,
            status=str(response.get("status") or "PENDING"),
            filled_quantity=_to_float(response.get("filled_quantity")),
            total_quantity=_to_float(response.get("total_quantity")),
            execution_price=_to_float(response.get("execution_price")),
            total_cost=_to_float(response.get("total_cost")),
            commission=_to_float(response.get("commission")),
            broker_data=response if isinstance(response, dict) else None,
        )

    def get_orders(
        self,
        account_id: str,
        state: str = "all",
        days: int = 30,
    ) -> List[OrderStatus]:
        """Fetch SnapTrade order history and map rows to common order status objects."""
        raw_orders = get_snaptrade_orders(
            user_email=self._user_email,
            user_secret=self._user_secret,
            account_id=account_id,
            state=state,
            days=days,
            on_secret_rotated=self._handle_secret_rotated,
            refresh_secret=self._refresh_secret,
            budget_user_id=self._user_id,
        )
        out: List[OrderStatus] = []
        for row in raw_orders:
            symbol = _extract_symbol_text((row or {}).get("universal_symbol") or {})
            time_placed = (row or {}).get("time_placed")
            time_updated = (row or {}).get("time_updated")
            out.append(
                OrderStatus(
                    brokerage_order_id=(row or {}).get("brokerage_order_id"),
                    ticker=symbol,
                    side=(row or {}).get("action"),
                    quantity=_to_float((row or {}).get("total_quantity")),
                    order_type=(row or {}).get("order_type"),
                    status=(row or {}).get("status") or "PENDING",
                    filled_quantity=_to_float((row or {}).get("filled_quantity")),
                    execution_price=_to_float((row or {}).get("execution_price")),
                    total_quantity=_to_float((row or {}).get("total_quantity")),
                    total_cost=_to_float((row or {}).get("total_cost")),
                    commission=_to_float((row or {}).get("commission")),
                    time_placed=str(time_placed) if time_placed is not None else None,
                    time_updated=str(time_updated) if time_updated is not None else None,
                    broker_data=row if isinstance(row, dict) else None,
                )
            )
        return out

    def cancel_order(
        self,
        account_id: str,
        order_id: str,
    ) -> CancelResult:
        """Submit cancellation request for one SnapTrade order."""
        response_raw = cancel_snaptrade_order(
            user_email=self._user_email,
            user_secret=self._user_secret,
            account_id=account_id,
            order_id=order_id,
            on_secret_rotated=self._handle_secret_rotated,
            refresh_secret=self._refresh_secret,
            budget_user_id=self._user_id,
        )
        response = response_raw if isinstance(response_raw, dict) else {}
        return CancelResult(
            brokerage_order_id=str(order_id),
            status=str(response.get("status") or "CANCEL_PENDING"),
            broker_data=response if isinstance(response, dict) else None,
        )

    def get_account_balance(self, account_id: str) -> Optional[float]:
        try:
            client = self._get_client()
            user_id, user_secret = self._get_identity()
            try:
                response = _get_user_account_balance_with_retry(
                    client,
                    user_id,
                    user_secret,
                    account_id,
                    budget_user_id=self._user_id,
                )
            except ApiException as exc:
                if not is_snaptrade_secret_error(exc):
                    raise
                _try_rotate_secret(
                    self._user_email,
                    user_secret,
                    on_secret_rotated=self._handle_secret_rotated,
                    refresh_secret=self._refresh_secret,
                    budget_user_id=self._user_id,
                )
                user_id, user_secret = self._get_identity()
                response = _get_user_account_balance_with_retry(
                    client,
                    user_id,
                    user_secret,
                    account_id,
                    budget_user_id=self._user_id,
                )
            balances = response.body if hasattr(response, "body") else response
            if not isinstance(balances, list):
                return None

            cash_total = 0.0
            for balance in balances:
                cash_total += _to_float((balance or {}).get("cash")) or 0.0
            return cash_total
        except Exception as e:
            portfolio_logger.warning(f"Failed to get account cash balance for {account_id}: {e}")
            return None

    def refresh_after_trade(self, account_id: str) -> None:
        """Trigger SnapTrade refresh and invalidate position cache after order placement."""
        try:
            authorization_id = self._resolve_authorization_id(account_id)
            if authorization_id:
                _user_id, user_secret = self._get_identity()
                try:
                    refresh_brokerage_authorization(
                        authorization_id,
                        self._user_email,
                        self._user_secret,
                        budget_user_id=self._user_id,
                    )
                except ApiException as exc:
                    if not is_snaptrade_secret_error(exc):
                        raise
                    _try_rotate_secret(
                        self._user_email,
                        user_secret,
                        on_secret_rotated=self._handle_secret_rotated,
                        refresh_secret=self._refresh_secret,
                        budget_user_id=self._user_id,
                    )
                    refresh_brokerage_authorization(
                        authorization_id,
                        self._user_email,
                        self._user_secret,
                        budget_user_id=self._user_id,
                    )
        except Exception as refresh_err:
            portfolio_logger.warning(
                f"Failed to refresh brokerage authorization for account {account_id}: {refresh_err}"
            )

        try:
            self._on_refresh(account_id)
        except Exception as cache_err:
            portfolio_logger.warning(f"on_refresh callback failed for SnapTrade account {account_id}: {cache_err}")

    def get_fractional_share_support(self, account_meta: BrokerAccount) -> Optional[bool]:
        return self._get_fractional_share_support(account_meta)

    def get_account_brokerage_name(self, account_id: str) -> Optional[str]:
        try:
            accounts = self.list_accounts()
            for account in accounts:
                if str(account.account_id) == str(account_id):
                    return account.brokerage_name
        except Exception:
            return None
        return None

    def resolve_authorization_id(self, account_id: str) -> Optional[str]:
        return self._resolve_authorization_id(account_id)

    def _fetch_accounts(self, force_refresh: bool = False) -> List[Dict[str, Any]]:
        if (
            not force_refresh
            and self._accounts_cache is not None
            and self._accounts_cache_at is not None
            and (datetime.now(timezone.utc) - self._accounts_cache_at).total_seconds() < self.ACCOUNT_CACHE_SECONDS
        ):
            return self._accounts_cache

        client = self._get_client()
        user_id, user_secret = self._get_identity()

        try:
            response = _list_user_accounts_with_retry(
                client,
                user_id,
                user_secret,
                budget_user_id=self._user_id,
            )
        except ApiException as exc:
            if not is_snaptrade_secret_error(exc):
                raise
            _try_rotate_secret(
                self._user_email,
                user_secret,
                on_secret_rotated=self._handle_secret_rotated,
                refresh_secret=self._refresh_secret,
                budget_user_id=self._user_id,
            )
            user_id, user_secret = self._get_identity()
            response = _list_user_accounts_with_retry(
                client,
                user_id,
                user_secret,
                budget_user_id=self._user_id,
            )
        accounts = response.body if hasattr(response, "body") else response
        if not isinstance(accounts, list):
            accounts = []

        self._accounts_cache = accounts
        self._accounts_cache_at = datetime.now(timezone.utc)
        return accounts

    def _resolve_authorization_id(self, account_id: str) -> Optional[str]:
        accounts = self._fetch_accounts(force_refresh=False)
        for account in accounts:
            if str(account.get("id")) == str(account_id):
                auth_id = self._extract_authorization_id(account)
                if auth_id:
                    return auth_id

        try:
            auths = list_user_brokerage_authorizations(
                self._user_email,
                self._user_secret,
                on_secret_rotated=self._handle_secret_rotated,
                refresh_secret=self._refresh_secret,
                budget_user_id=self._user_id,
            )
            for auth in auths:
                auth_id = auth.get("id")
                auth_accounts = auth.get("accounts") or []
                for account in auth_accounts:
                    if str(account.get("id")) == str(account_id):
                        return auth_id
        except Exception as e:
            portfolio_logger.warning(f"Failed to resolve authorization for account {account_id}: {e}")

        return None

    def _extract_authorization_id(self, account: Dict[str, Any]) -> Optional[str]:
        auth = account.get("brokerage_authorization")
        if isinstance(auth, dict):
            return str(auth.get("id")) if auth.get("id") else None
        if auth:
            return str(auth)
        return None

    def _get_fractional_share_support(self, account_meta: BrokerAccount) -> Optional[bool]:
        meta = account_meta.meta or {}
        for key in ("supports_fractional_shares", "fractional_shares_supported", "fractional_trading_enabled"):
            if key in meta:
                return bool(meta.get(key))
        return None

    def _get_client(self):
        if self._snaptrade_client is not None:
            return self._snaptrade_client

        del self._region
        self._snaptrade_client = _require_snaptrade_client()
        return self._snaptrade_client

    def _get_identity(self) -> Tuple[str, str]:
        user_id = get_snaptrade_user_id_from_email(self._user_email)
        return user_id, self._user_secret

    def _handle_secret_rotated(self, new_secret: str) -> None:
        self._user_secret = new_secret
        if self._on_secret_rotated is not None:
            self._on_secret_rotated(new_secret)

    def _refresh_secret(self) -> str | None:
        if self._refresh_secret_callback is None:
            return self._user_secret
        refreshed = self._refresh_secret_callback()
        if refreshed:
            self._user_secret = refreshed
        return refreshed


def _to_float(value: Any) -> Optional[float]:
    try:
        if value is None:
            return None
        result = float(value)
        if math.isinf(result):
            return None
        return result
    except (TypeError, ValueError):
        return None


def _extract_symbol_text(value: Dict[str, Any]) -> Optional[str]:
    if not isinstance(value, dict):
        return None
    symbol = value.get("symbol")
    if isinstance(symbol, dict):
        symbol = symbol.get("symbol")
    return str(symbol) if symbol else None
