"""Schwab broker adapter implementing ``BrokerAdapter`` via schwab-py.

Called by:
- ``services.trade_execution_service.TradeExecutionService`` for Schwab accounts.

Calls into:
- ``schwab_client`` wrappers for account/quote/order operations.
"""

from __future__ import annotations

import math
import time
from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING, Any, Callable, Dict, List, Optional

try:
    from app_platform.api_budget import guard_call
except ModuleNotFoundError as e:
    # Only fall back when app_platform itself or its api_budget submodule is unavailable
    # (dist runtime). Re-raise if a transitive import inside app_platform.api_budget fails —
    # those represent monorepo bugs that must surface, not silently disable budget enforcement.
    if e.name not in {"app_platform", "app_platform.api_budget"}:
        raise
    def guard_call(*, fn, args=(), kwargs=None, **_):
        """No-op fallback when app_platform.api_budget isn't installed (dist runtime)."""
        return fn(*args, **(kwargs or {}))

from brokerage._logging import portfolio_logger
from brokerage.broker_adapter import BrokerAdapter
from brokerage.schwab.client import (
    get_account_hashes,
    get_schwab_client,
    invalidate_schwab_caches,
    is_invalid_grant_error,
)
from brokerage.schwab.orders import build_equity_order_spec
from brokerage.trade_objects import BrokerAccount, CancelResult, OrderPreview, OrderResult, OrderStatus
from brokerage._shared.api_budget_costs import COST_PER_CALL

if TYPE_CHECKING:
    import pandas as pd

    from brokerage.options_types import OptionStrategy
    from ibkr.contract_spec import IBKRContractSpec


SCHWAB_STATUS_MAP = {
    "PENDING_ACTIVATION": "PENDING",
    "PENDING": "PENDING",
    "WORKING": "ACCEPTED",
    "QUEUED": "ACCEPTED",
    "FILLED": "EXECUTED",
    "PARTIAL_FILL": "PARTIAL",
    "CANCELED": "CANCELED",
    "REJECTED": "REJECTED",
    "EXPIRED": "EXPIRED",
}

_RETRY_DELAYS_SECONDS = (0.0, 0.5, 1.0, 2.0)


def _schwab_cost_per_call(operation: str) -> Any:
    return COST_PER_CALL.get(("schwab", operation), 0)


def _call_schwab_get_account(client: Any, account_hash: str, fields: list[str] | None = None) -> Any:
    try:
        return client.get_account(account_hash, fields=fields) if fields else client.get_account(account_hash)
    except TypeError:
        return client.get_account(account_hash)


def _call_schwab_get_orders_for_account(
    client: Any,
    account_hash: str,
    start: datetime,
    end: datetime,
) -> Any:
    try:
        return client.get_orders_for_account(account_hash, start, end)
    except TypeError:
        return client.get_orders_for_account(account_hash)


def _call_schwab_cancel_order(client: Any, order_id: str, account_hash: str) -> Any:
    try:
        return client.cancel_order(order_id, account_hash)
    except TypeError:
        return client.cancel_order(account_hash, order_id)


def _to_float(value: Any) -> Optional[float]:
    try:
        if value is None:
            return None
        result = float(value)
        if math.isinf(result):
            return None
        return result
    except Exception:
        return None


def _format_price(value: Optional[float]) -> Optional[str]:
    if value is None:
        return None
    try:
        number = float(value)
    except Exception:
        return None
    text = f"{number:.8f}".rstrip("0").rstrip(".")
    return text or "0"


def _response_payload(response: Any) -> Any:
    if response is None:
        return None
    if isinstance(response, (dict, list)):
        return response
    body = getattr(response, "body", None)
    if body is not None:
        return body
    if hasattr(response, "json"):
        try:
            return response.json()
        except Exception:
            return None
    return None


def _status_from_response(response: Any) -> str:
    status_code = getattr(response, "status_code", None)
    if status_code in {200, 201, 202, 204}:
        return "ACCEPTED"
    return "PENDING"


def _extract_order_id(response: Any, payload: Any = None) -> Optional[str]:
    if isinstance(payload, dict):
        for key in ("orderId", "order_id", "id"):
            value = payload.get(key)
            if value is not None and str(value).strip():
                return str(value).strip()

    headers = getattr(response, "headers", None)
    if isinstance(headers, dict):
        location = headers.get("Location") or headers.get("location")
        if location:
            return str(location).rstrip("/").split("/")[-1]
    return None


def _to_common_status(status: str) -> str:
    normalized = str(status or "").strip().upper()
    if normalized in {
        "PENDING",
        "ACCEPTED",
        "EXECUTED",
        "PARTIAL",
        "CANCELED",
        "REJECTED",
        "FAILED",
        "EXPIRED",
        "CANCEL_PENDING",
    }:
        return normalized
    return SCHWAB_STATUS_MAP.get(normalized, "PENDING")


def _matches_state_filter(status: str, state: str) -> bool:
    state = str(state or "all").lower().strip()
    if state == "all":
        return True
    if state == "open":
        return status in {"PENDING", "ACCEPTED", "PARTIAL", "CANCEL_PENDING"}
    if state == "executed":
        return status == "EXECUTED"
    if state == "cancelled":
        return status in {"CANCELED", "REJECTED", "FAILED", "EXPIRED"}
    return True


class SchwabBrokerAdapter(BrokerAdapter):
    """Direct Schwab trading adapter with retry/backoff behavior."""

    def __init__(
        self,
        user_email: str,
        on_refresh: Callable[[str], None] | None = None,
    ):
        self._user_email = user_email
        self._on_refresh = on_refresh or (lambda _account_id: None)

    @property
    def provider_name(self) -> str:
        return "schwab"

    def _unsupported_ibkr_method(self, method_name: str):
        raise NotImplementedError(f"{self.provider_name} does not support {method_name}")

    def fetch_market_snapshot(
        self,
        contracts: list[IBKRContractSpec | Any],
        *,
        budget_user_id: int | None = None,
        **kwargs,
    ) -> list[dict[str, Any]]:
        return self._unsupported_ibkr_method("fetch_market_snapshot")

    def get_live_positions(
        self,
        account_id: str | None = None,
        *,
        budget_user_id: int | None = None,
    ) -> pd.DataFrame:
        return self._unsupported_ibkr_method("get_live_positions")

    def query_open_orders(
        self,
        account_id: str | None = None,
        *,
        budget_user_id: int | None = None,
    ) -> List[OrderStatus]:
        return self._unsupported_ibkr_method("query_open_orders")

    def query_completed_orders(
        self,
        account_id: str | None = None,
        *,
        budget_user_id: int | None = None,
    ) -> List[OrderStatus]:
        return self._unsupported_ibkr_method("query_completed_orders")

    def preview_roll(
        self,
        account_id: str,
        symbol: str,
        front_month: str,
        back_month: str,
        quantity: float,
        direction: str = "long_roll",
        order_type: str = "Market",
        limit_price: Optional[float] = None,
        time_in_force: str = "Day",
    ) -> OrderPreview:
        return self._unsupported_ibkr_method("preview_roll")

    def place_roll(
        self,
        account_id: str,
        order_params: Dict[str, Any],
    ) -> OrderResult:
        return self._unsupported_ibkr_method("place_roll")

    def preview_multileg_option(
        self,
        account_id: str,
        strategy: OptionStrategy,
        quantity: float,
        order_type: str = "Market",
        limit_price: Optional[float] = None,
        time_in_force: str = "Day",
    ) -> OrderPreview:
        return self._unsupported_ibkr_method("preview_multileg_option")

    def place_multileg_option(
        self,
        account_id: str,
        order_params: Dict[str, Any],
    ) -> OrderResult:
        return self._unsupported_ibkr_method("place_multileg_option")

    def _call_with_backoff(
        self,
        func,
        *args,
        operation: str | None = None,
        budget_user_id: int | None = None,
        **kwargs,
    ):
        last_exception: Exception | None = None
        last_response: Any = None
        operation_name = str(operation or getattr(func, "__name__", "") or "unknown")
        for delay in _RETRY_DELAYS_SECONDS:
            if delay > 0:
                time.sleep(delay)
            try:
                response = guard_call(
                    provider="schwab",
                    operation=operation_name,
                    budget_user_id=budget_user_id,
                    cost_per_call=_schwab_cost_per_call(operation_name),
                    fn=func,
                    args=args,
                    kwargs=kwargs,
                )
                last_response = response
                if getattr(response, "status_code", None) == 429:
                    continue
                return response
            except Exception as exc:
                if is_invalid_grant_error(exc):
                    raise RuntimeError(
                        "Schwab refresh token expired. Run `python3 -m scripts.run_schwab login`."
                    ) from exc
                text = str(exc).lower()
                if "429" in text or "rate limit" in text:
                    last_exception = exc
                    continue
                raise

        if last_exception is not None:
            raise last_exception
        return last_response

    def _account_hashes(self, force_refresh: bool = False) -> dict[str, str]:
        return get_account_hashes(force_refresh=force_refresh)

    def _resolve_account_hash(self, account_id: str) -> str:
        account_id = str(account_id or "").strip()
        if not account_id:
            raise ValueError("account_id is required")

        mapping = self._account_hashes()
        if account_id in mapping.values():
            return account_id
        if account_id in mapping:
            return mapping[account_id]
        raise ValueError(f"Unknown Schwab account: {account_id}")

    def _account_number_for_hash(self, account_hash: str) -> str:
        mapping = self._account_hashes()
        for account_number, mapped_hash in mapping.items():
            if str(mapped_hash) == str(account_hash):
                return str(account_number)
        return str(account_hash)

    def _fetch_account(self, account_hash: str, fields: Optional[list[str]] = None) -> dict[str, Any]:
        client = get_schwab_client()
        response = self._call_with_backoff(
            _call_schwab_get_account,
            client,
            account_hash,
            fields=fields,
            operation="get_account",
        )
        payload = _response_payload(response)
        return payload if isinstance(payload, dict) else {}

    def _quote_price(self, ticker: str) -> Optional[float]:
        client = get_schwab_client()
        symbol = str(ticker).upper().strip()
        response = self._call_with_backoff(client.get_quote, symbol, operation="get_quote")
        payload = _response_payload(response)
        if not isinstance(payload, dict):
            return None
        symbol_data = payload.get(symbol, payload)
        if not isinstance(symbol_data, dict):
            return None
        # Price fields live in the nested "quote" sub-dict; fall back to top-level
        quote = symbol_data.get("quote", symbol_data)
        if not isinstance(quote, dict):
            quote = symbol_data
        for key in ("lastPrice", "mark", "closePrice", "bidPrice", "askPrice"):
            value = _to_float(quote.get(key))
            if value is not None and value > 0:
                return value
        return None

    def _instruction_for_side(self, side: str) -> str:
        side_upper = str(side or "").upper().strip()
        if side_upper == "BUY":
            return "BUY"
        if side_upper == "SELL":
            return "SELL"
        if side_upper == "COVER":
            return "BUY_TO_COVER"
        if side_upper == "SHORT":
            raise ValueError(
                "Schwab SHORT orders require adapter routing changes not yet implemented. "
                "Use an IBKR account for short selling."
            )
        raise ValueError(f"Unsupported side: {side}")

    def _duration_for_tif(self, time_in_force: str) -> str:
        tif = str(time_in_force or "Day").strip().upper()
        mapping = {
            "DAY": "DAY",
            "GTC": "GOOD_TILL_CANCEL",
            "FOK": "FILL_OR_KILL",
            "IOC": "IMMEDIATE_OR_CANCEL",
        }
        return mapping.get(tif, "DAY")

    def _order_type_for_input(self, order_type: str) -> str:
        value = str(order_type or "Market").strip().upper()
        mapping = {
            "MARKET": "MARKET",
            "LIMIT": "LIMIT",
            "STOP": "STOP",
            "STOPLIMIT": "STOP_LIMIT",
            "STOP_LIMIT": "STOP_LIMIT",
        }
        return mapping.get(value, "MARKET")

    def _build_order_spec(
        self,
        *,
        ticker: str,
        side: str,
        quantity: float,
        order_type: str,
        time_in_force: str,
        limit_price: Optional[float],
        stop_price: Optional[float],
    ) -> dict[str, Any]:
        instruction = self._instruction_for_side(side)
        mapped_type = self._order_type_for_input(order_type)
        duration = self._duration_for_tif(time_in_force)

        # Fallback spec works even if schwab.orders builder APIs differ by version.
        spec: dict[str, Any] = {
            "orderType": mapped_type,
            "session": "NORMAL",
            "duration": duration,
            "orderStrategyType": "SINGLE",
            "orderLegCollection": [
                {
                    "instruction": instruction,
                    "quantity": float(quantity),
                    "instrument": {
                        "symbol": str(ticker).upper().strip(),
                        "assetType": "EQUITY",
                    },
                }
            ],
        }
        if mapped_type in {"LIMIT", "STOP_LIMIT"} and limit_price is not None:
            spec["price"] = _format_price(limit_price)
        if mapped_type in {"STOP", "STOP_LIMIT"} and stop_price is not None:
            spec["stopPrice"] = _format_price(stop_price)

        # Prefer schwab.orders builders when available.
        try:
            built = build_equity_order_spec(
                ticker=ticker,
                side=side,
                quantity=quantity,
                mapped_type=mapped_type,
                duration=duration,
                limit_price=_format_price(limit_price),
                stop_price=_format_price(stop_price),
            )
            if built is not None:
                return built
        except Exception:
            pass

        return spec

    def owns_account(self, account_id: str) -> bool:
        mapping = self._account_hashes()
        account_id = str(account_id or "").strip()
        return account_id in mapping or account_id in mapping.values()

    def list_accounts(self) -> List[BrokerAccount]:
        rows: List[BrokerAccount] = []
        for account_number, account_hash in self._account_hashes().items():
            payload = self._fetch_account(account_hash, fields=["positions"])
            sec = payload.get("securitiesAccount") if isinstance(payload, dict) else {}
            balances = sec.get("currentBalances") if isinstance(sec, dict) else {}

            account_type_raw = sec.get("type") if isinstance(sec, dict) else None
            account_type = str(account_type_raw).upper().strip() if account_type_raw is not None else None
            available_funds = _to_float((balances or {}).get("availableFunds"))
            cash_balance_raw = _to_float((balances or {}).get("cashBalance"))
            margin_balance = _to_float((balances or {}).get("marginBalance"))
            buying_power = _to_float((balances or {}).get("buyingPower"))

            if account_type == "MARGIN":
                cash_balance = margin_balance if margin_balance is not None else available_funds
            elif account_type == "CASH" or account_type is None:
                cash_balance = cash_balance_raw if cash_balance_raw is not None else available_funds
                if cash_balance is None and account_type is None:
                    cash_balance = buying_power
            else:
                cash_balance = available_funds if available_funds is not None else cash_balance_raw
                if cash_balance is None:
                    cash_balance = buying_power

            rows.append(
                BrokerAccount(
                    account_id=account_hash,
                    account_name=account_number,
                    brokerage_name="Charles Schwab",
                    provider="schwab",
                    cash_balance=cash_balance,
                    available_funds=available_funds,
                    account_type=account_type,
                    meta={
                        "account_number": account_number,
                        "account_hash": account_hash,
                    },
                )
            )
        return rows

    def search_symbol(self, account_id: str, ticker: str) -> Dict[str, Any]:
        del account_id
        symbol = str(ticker or "").upper().strip()
        if not symbol:
            raise ValueError("ticker is required")

        client = get_schwab_client()
        instrument_data: dict[str, Any] = {}
        try:
            response = self._call_with_backoff(
                client.search_instruments,
                symbol,
                projection="symbol-search",
                operation="search_instruments",
            )
            payload = _response_payload(response)
            if isinstance(payload, dict):
                instrument_data = payload.get(symbol) or {}
        except Exception:
            instrument_data = {}

        quote_price = self._quote_price(symbol)
        description = (
            instrument_data.get("description")
            if isinstance(instrument_data, dict)
            else None
        )

        return {
            "ticker": symbol,
            "name": description or symbol,
            "universal_symbol_id": symbol,
            "broker_symbol_id": symbol,
            "last_price": quote_price,
            "instrument": instrument_data if isinstance(instrument_data, dict) else {},
        }

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
        account_hash = self._resolve_account_hash(account_id)
        symbol = str(symbol_id or ticker or "").upper().strip()
        if not symbol:
            raise ValueError("ticker is required")

        side_upper = str(side or "").upper().strip()
        if side_upper == "SHORT":
            raise ValueError(
                "Schwab SHORT orders require adapter routing changes not yet implemented. "
                "Use an IBKR account for short selling."
            )
        if side_upper == "COVER":
            side = "COVER"

        if limit_price is not None:
            estimated_price = float(limit_price)
        elif stop_price is not None:
            estimated_price = float(stop_price)
        else:
            estimated_price = self._quote_price(symbol)

        estimated_commission = 0.0
        estimated_total = None
        if estimated_price is not None:
            estimated_total = (float(estimated_price) * float(quantity)) + estimated_commission

        return OrderPreview(
            estimated_price=estimated_price,
            estimated_total=estimated_total,
            estimated_commission=estimated_commission,
            broker_trade_id=None,
            combined_remaining_balance=None,
            trade_impacts=[],
            broker_preview_data={
                "order_params": {
                    "account_id": account_hash,
                    "ticker": symbol,
                    "side": str(side or "").upper(),
                    "quantity": float(quantity),
                    "order_type": order_type,
                    "time_in_force": time_in_force,
                    "limit_price": limit_price,
                    "stop_price": stop_price,
                    "symbol_id": symbol,
                }
            },
        )

    def place_order(
        self,
        account_id: str,
        order_params: Dict[str, Any],
    ) -> OrderResult:
        account_hash = self._resolve_account_hash(account_id)
        ticker = str(order_params.get("ticker") or "").upper().strip()
        side = str(order_params.get("side") or "").upper().strip()
        quantity = float(order_params.get("quantity") or 0.0)
        order_type = str(order_params.get("order_type") or "Market")
        time_in_force = str(order_params.get("time_in_force") or "Day")
        limit_price = _to_float(order_params.get("limit_price"))
        stop_price = _to_float(order_params.get("stop_price"))

        if not ticker:
            raise ValueError("Missing ticker for Schwab order placement")
        if quantity <= 0:
            raise ValueError("Quantity must be greater than zero")

        order_spec = self._build_order_spec(
            ticker=ticker,
            side=side,
            quantity=quantity,
            order_type=order_type,
            time_in_force=time_in_force,
            limit_price=limit_price,
            stop_price=stop_price,
        )

        client = get_schwab_client()
        response = self._call_with_backoff(
            client.place_order,
            account_hash,
            order_spec,
            operation="place_order",
        )
        status_code = getattr(response, "status_code", None)
        if status_code is not None and status_code >= 400:
            payload = _response_payload(response)
            raise RuntimeError(
                f"Schwab order placement failed (HTTP {status_code}): {payload}"
            )
        payload = _response_payload(response)
        brokerage_order_id = _extract_order_id(response, payload)
        status = _status_from_response(response)

        execution_price = limit_price or stop_price or self._quote_price(ticker)
        total_cost = (execution_price * quantity) if execution_price is not None else None

        broker_data: Optional[Dict[str, Any]] = None
        if isinstance(payload, dict):
            broker_data = payload
        elif payload is not None:
            broker_data = {"payload": payload}
        return OrderResult(
            brokerage_order_id=brokerage_order_id,
            status=status,
            filled_quantity=0.0,
            total_quantity=quantity,
            execution_price=execution_price,
            total_cost=total_cost,
            commission=0.0,
            broker_data=broker_data,
        )

    def get_orders(
        self,
        account_id: str,
        state: str = "all",
        days: int = 30,
    ) -> List[OrderStatus]:
        account_hash = self._resolve_account_hash(account_id)
        end = datetime.now(timezone.utc)
        start = end - timedelta(days=max(int(days), 1))
        client = get_schwab_client()

        response = self._call_with_backoff(
            _call_schwab_get_orders_for_account,
            client,
            account_hash,
            start,
            end,
            operation="get_orders_for_account",
        )

        payload = _response_payload(response)
        orders = payload if isinstance(payload, list) else []
        mapped: List[OrderStatus] = []
        for row in orders:
            if not isinstance(row, dict):
                continue

            leg = {}
            legs = row.get("orderLegCollection")
            if isinstance(legs, list) and legs:
                leg = legs[0] if isinstance(legs[0], dict) else {}
            instrument = leg.get("instrument") if isinstance(leg, dict) else {}
            ticker = (instrument or {}).get("symbol")
            quantity = _to_float(leg.get("quantity")) or _to_float(row.get("quantity"))
            filled = _to_float(row.get("filledQuantity")) or 0.0
            total_qty = _to_float(row.get("quantity")) or quantity or 0.0

            status = _to_common_status(str(row.get("status") or "PENDING"))
            if total_qty > 0 and 0 < filled < total_qty:
                status = "PARTIAL"
            if not _matches_state_filter(status, state):
                continue

            mapped.append(
                OrderStatus(
                    brokerage_order_id=str(row.get("orderId") or row.get("id") or ""),
                    ticker=ticker,
                    side=leg.get("instruction"),
                    quantity=quantity,
                    order_type=row.get("orderType"),
                    status=status,
                    filled_quantity=filled,
                    total_quantity=total_qty,
                    execution_price=_to_float(row.get("price")),
                    total_cost=(_to_float(row.get("price")) or 0.0) * (filled or 0.0),
                    commission=_to_float(row.get("commission")) or 0.0,
                    time_placed=row.get("enteredTime"),
                    time_updated=row.get("closeTime") or row.get("enteredTime"),
                    broker_data=row,
                )
            )
        return mapped

    def cancel_order(
        self,
        account_id: str,
        order_id: str,
    ) -> CancelResult:
        account_hash = self._resolve_account_hash(account_id)
        client = get_schwab_client()
        response = self._call_with_backoff(
            _call_schwab_cancel_order,
            client,
            order_id,
            account_hash,
            operation="cancel_order",
        )

        status_code = getattr(response, "status_code", None)
        if status_code is not None and status_code >= 400:
            payload = _response_payload(response)
            raise RuntimeError(
                f"Schwab order cancellation failed (HTTP {status_code}): {payload}"
            )
        status = "CANCELED" if status_code in {200, 201, 202, 204} else "CANCEL_PENDING"
        payload = _response_payload(response)
        broker_data: Optional[Dict[str, Any]] = None
        if isinstance(payload, dict):
            broker_data = payload
        elif status_code is not None:
            broker_data = {"status_code": status_code}
        return CancelResult(
            brokerage_order_id=str(order_id),
            status=status,
            broker_data=broker_data,
        )

    def get_account_balance(self, account_id: str) -> Optional[float]:
        account_hash = self._resolve_account_hash(account_id)
        payload = self._fetch_account(account_hash, fields=None)
        sec = payload.get("securitiesAccount") if isinstance(payload, dict) else {}
        balances = sec.get("currentBalances") if isinstance(sec, dict) else {}
        return (
            _to_float((balances or {}).get("availableFunds"))
            or _to_float((balances or {}).get("cashBalance"))
            or _to_float((balances or {}).get("buyingPower"))
        )

    def refresh_after_trade(self, account_id: str) -> None:
        invalidate_schwab_caches()
        try:
            self._on_refresh(account_id)
        except Exception as exc:
            portfolio_logger.warning(
                "on_refresh callback failed for Schwab account %s: %s",
                account_id,
                exc,
            )
