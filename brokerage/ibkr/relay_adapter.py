"""IBKR BrokerAdapter implementation backed by the gateway local relay."""

from __future__ import annotations

import dataclasses
import os
import uuid
from typing import TYPE_CHECKING, Any, Callable, Dict, List, Optional, TypeVar

import httpx
import pandas as pd

from brokerage._vendor import make_json_safe
from brokerage.broker_adapter import BrokerAdapter
from brokerage.relay_config import assert_valid_ibkr_relay_env, normalized_gateway_url
from brokerage.trade_objects import (
    BrokerAccount,
    CancelResult,
    OrderPreview,
    OrderResult,
    OrderStatus,
)

if TYPE_CHECKING:
    from brokerage.options_types import OptionStrategy
    from ibkr.contract_spec import IBKRContractSpec


class BrokerAdapterError(RuntimeError):
    """Transport-level relay adapter failure."""


_T = TypeVar("_T")


def _serialize_value(value: Any) -> Any:
    if dataclasses.is_dataclass(value):
        return make_json_safe(dataclasses.asdict(value))
    if isinstance(value, dict):
        return {str(key): _serialize_value(inner) for key, inner in value.items()}
    if isinstance(value, (list, tuple)):
        return [_serialize_value(item) for item in value]
    return make_json_safe(value)


def _coerce_dataclass(cls: type[_T], payload: Any) -> _T:
    if isinstance(payload, cls):
        return payload
    if not isinstance(payload, dict):
        raise BrokerAdapterError(f"relay_bad_payload:{cls.__name__}")
    return cls(**payload)


def _coerce_dataclass_list(cls: type[_T], payload: Any) -> list[_T]:
    if payload is None:
        return []
    if not isinstance(payload, list):
        raise BrokerAdapterError(f"relay_bad_payload:list[{cls.__name__}]")
    return [_coerce_dataclass(cls, item) for item in payload]


class IBKRRelayAdapter(BrokerAdapter):
    """Dispatch IBKR broker operations through the gateway's local relay."""

    def __init__(
        self,
        user_email: str,
        on_refresh: Callable[[str], None] | None = None,
        *,
        account_map: dict[str, str] | None = None,
    ) -> None:
        self._user_email = user_email
        self._on_refresh = on_refresh or (lambda _account_id: None)
        self._account_map = account_map if account_map is not None else {}
        self._user_id: int | None = None
        self._service_token = os.getenv("IBKR_RELAY_INTERNAL_TOKEN", "").strip()
        transport = os.getenv("IBKR_TRANSPORT", "direct").strip().lower()
        if transport == "relay":
            assert_valid_ibkr_relay_env()
        self._gateway_url = normalized_gateway_url()
        self._timeout = float(os.getenv("IBKR_RELAY_ADAPTER_TIMEOUT", "75"))

    @property
    def provider_name(self) -> str:
        return "ibkr"

    def bind_user_id(self, user_id: int) -> None:
        self._user_id = int(user_id)

    def _dispatch(self, tool_name: str, tool_input: Dict[str, Any], *, no_replay: bool) -> Any:
        if self._user_id is None:
            raise BrokerAdapterError("relay_user_id_unbound")
        if not self._service_token:
            raise BrokerAdapterError("service_auth_failed")

        payload = {
            "user_id": self._user_id,
            "tool_name": tool_name,
            "tool_input": _serialize_value(tool_input),
            "request_id": str(uuid.uuid4()),
            "no_replay": bool(no_replay),
        }
        url = f"{self._gateway_url}/api/ibkr/execute"
        try:
            with httpx.Client(timeout=self._timeout) as client:
                response = client.post(
                    url,
                    json=payload,
                    headers={"X-IBKR-Service-Token": self._service_token},
                )
        except httpx.RequestError as exc:
            raise BrokerAdapterError(f"relay_request_failed:{exc}") from exc

        if response.status_code == 401:
            raise BrokerAdapterError("service_auth_failed")
        if response.status_code == 503:
            raise BrokerAdapterError("relay_disconnected")
        if response.status_code == 504:
            raise BrokerAdapterError("relay_timeout")

        body: Any = None
        try:
            body = response.json()
        except ValueError:
            body = None

        if response.status_code >= 400:
            detail = body.get("error") if isinstance(body, dict) else response.text
            raise BrokerAdapterError(f"relay_http_{response.status_code}:{detail}")

        if not isinstance(body, dict):
            raise BrokerAdapterError("relay_bad_payload:response")
        relay_error = body.get("error")
        if relay_error:
            if isinstance(relay_error, dict):
                code = relay_error.get("code") or relay_error.get("error") or relay_error.get("message")
                raise BrokerAdapterError(str(code or relay_error))
            raise BrokerAdapterError(str(relay_error))
        return body.get("result")

    def _dispatch_preview(self, tool_name: str, tool_input: Dict[str, Any]) -> OrderPreview:
        return _coerce_dataclass(OrderPreview, self._dispatch(tool_name, tool_input, no_replay=False))

    def _dispatch_order_result(self, tool_name: str, tool_input: Dict[str, Any], *, no_replay: bool) -> OrderResult:
        return _coerce_dataclass(OrderResult, self._dispatch(tool_name, tool_input, no_replay=no_replay))

    def owns_account(self, account_id: str) -> bool:
        return bool(
            self._dispatch(
                "ibkr.owns_account",
                {"account_id": account_id},
                no_replay=False,
            )
        )

    def list_accounts(self) -> List[BrokerAccount]:
        result = self._dispatch("ibkr.list_accounts", {}, no_replay=False)
        return _coerce_dataclass_list(BrokerAccount, result)

    def search_symbol(self, account_id: str, ticker: str) -> Dict[str, Any]:
        result = self._dispatch(
            "ibkr.search_symbol",
            {"account_id": account_id, "ticker": ticker},
            no_replay=False,
        )
        return dict(result or {})

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
        return self._dispatch_preview(
            "ibkr.preview_order",
            {
                "account_id": account_id,
                "ticker": ticker,
                "side": side,
                "quantity": quantity,
                "order_type": order_type,
                "time_in_force": time_in_force,
                "limit_price": limit_price,
                "stop_price": stop_price,
                "symbol_id": symbol_id,
            },
        )

    def place_order(
        self,
        account_id: str,
        order_params: Dict[str, Any],
    ) -> OrderResult:
        return self._dispatch_order_result(
            "ibkr.place_order",
            {"account_id": account_id, "order_params": order_params},
            no_replay=True,
        )

    def get_orders(
        self,
        account_id: str,
        state: str = "all",
        days: int = 30,
    ) -> List[OrderStatus]:
        result = self._dispatch(
            "ibkr.get_orders",
            {"account_id": account_id, "state": state, "days": days},
            no_replay=False,
        )
        return _coerce_dataclass_list(OrderStatus, result)

    def cancel_order(
        self,
        account_id: str,
        order_id: str,
    ) -> CancelResult:
        return _coerce_dataclass(
            CancelResult,
            self._dispatch(
                "ibkr.cancel_order",
                {"account_id": account_id, "order_id": order_id},
                no_replay=True,
            ),
        )

    def get_account_balance(self, account_id: str) -> Optional[float]:
        result = self._dispatch(
            "ibkr.get_account_balance",
            {"account_id": account_id},
            no_replay=False,
        )
        return None if result is None else float(result)

    def refresh_after_trade(self, account_id: str) -> None:
        self._dispatch(
            "ibkr.refresh_after_trade",
            {"account_id": account_id},
            no_replay=False,
        )
        self._on_refresh(account_id)

    def fetch_market_snapshot(
        self,
        contracts: list[IBKRContractSpec | Any],
        *,
        budget_user_id: int | None = None,
        **kwargs,
    ) -> list[dict[str, Any]]:
        result = self._dispatch(
            "ibkr.fetch_market_snapshot",
            {"contracts": contracts, "budget_user_id": budget_user_id, **kwargs},
            no_replay=False,
        )
        return list(result or [])

    def get_live_positions(
        self,
        account_id: str | None = None,
        *,
        budget_user_id: int | None = None,
    ) -> pd.DataFrame:
        result = self._dispatch(
            "ibkr.get_live_positions",
            {"account_id": account_id, "budget_user_id": budget_user_id},
            no_replay=False,
        )
        if isinstance(result, pd.DataFrame):
            return result
        return pd.DataFrame(result or [])

    def query_open_orders(
        self,
        account_id: str | None = None,
        *,
        budget_user_id: int | None = None,
    ) -> List[OrderStatus]:
        result = self._dispatch(
            "ibkr.query_open_orders",
            {"account_id": account_id, "budget_user_id": budget_user_id},
            no_replay=False,
        )
        return _coerce_dataclass_list(OrderStatus, result)

    def query_completed_orders(
        self,
        account_id: str | None = None,
        *,
        budget_user_id: int | None = None,
    ) -> List[OrderStatus]:
        result = self._dispatch(
            "ibkr.query_completed_orders",
            {"account_id": account_id, "budget_user_id": budget_user_id},
            no_replay=False,
        )
        return _coerce_dataclass_list(OrderStatus, result)

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
        return self._dispatch_preview(
            "ibkr.preview_roll",
            {
                "account_id": account_id,
                "symbol": symbol,
                "front_month": front_month,
                "back_month": back_month,
                "quantity": quantity,
                "direction": direction,
                "order_type": order_type,
                "limit_price": limit_price,
                "time_in_force": time_in_force,
            },
        )

    def place_roll(
        self,
        account_id: str,
        order_params: Dict[str, Any],
    ) -> OrderResult:
        return self._dispatch_order_result(
            "ibkr.place_roll",
            {"account_id": account_id, "order_params": order_params},
            no_replay=True,
        )

    def preview_multileg_option(
        self,
        account_id: str,
        strategy: OptionStrategy,
        quantity: float,
        order_type: str = "Market",
        limit_price: Optional[float] = None,
        time_in_force: str = "Day",
    ) -> OrderPreview:
        return self._dispatch_preview(
            "ibkr.preview_multileg_option",
            {
                "account_id": account_id,
                "strategy": strategy,
                "quantity": quantity,
                "order_type": order_type,
                "limit_price": limit_price,
                "time_in_force": time_in_force,
            },
        )

    def place_multileg_option(
        self,
        account_id: str,
        order_params: Dict[str, Any],
    ) -> OrderResult:
        return self._dispatch_order_result(
            "ibkr.place_multileg_option",
            {"account_id": account_id, "order_params": order_params},
            no_replay=True,
        )
