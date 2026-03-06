"""Structured trade execution payload contracts.

Called by:
- ``services.trade_execution_service.TradeExecutionService``
- Broker adapter implementations for normalized return objects.

Used by:
- API routes/tools that serialize trade previews, executions, and order lists.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, UTC
from typing import Any, Dict, List, Optional

from brokerage._vendor import make_json_safe


ALLOWED_ORDER_TYPES = ("Market", "Limit", "Stop", "StopLimit")
ALLOWED_TIME_IN_FORCE = ("Day", "GTC", "FOK", "IOC")
ALLOWED_SIDES = ("BUY", "SELL")


def _iso(value: Optional[datetime]) -> Optional[str]:
    if isinstance(value, datetime):
        return value.isoformat()
    return value


@dataclass
class PreTradeValidation:
    """Pre-trade validation outcome."""

    is_valid: bool
    errors: List[str] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)
    buying_power: Optional[float] = None
    estimated_cost: Optional[float] = None
    post_trade_weight: Optional[float] = None

    def to_api_response(self) -> Dict[str, Any]:
        return make_json_safe(
            {
                "is_valid": self.is_valid,
                "errors": self.errors,
                "warnings": self.warnings,
                "buying_power": self.buying_power,
                "estimated_cost": self.estimated_cost,
                "post_trade_weight": self.post_trade_weight,
            }
        )

    def to_formatted_report(self) -> str:
        lines = ["Pre-Trade Validation"]
        lines.append(f"- valid: {self.is_valid}")
        if self.errors:
            lines.append(f"- errors: {'; '.join(self.errors)}")
        if self.warnings:
            lines.append(f"- warnings: {'; '.join(self.warnings)}")
        if self.buying_power is not None:
            lines.append(f"- buying_power: ${self.buying_power:,.2f}")
        if self.estimated_cost is not None:
            lines.append(f"- estimated_cost: ${self.estimated_cost:,.2f}")
        if self.post_trade_weight is not None:
            lines.append(f"- post_trade_weight: {self.post_trade_weight:.2%}")
        return "\n".join(lines)


@dataclass
class TradePreviewResult:
    """Preview envelope returned by pre-trade workflow boundaries."""

    status: str
    user_email: str
    account_id: str
    ticker: str
    side: str
    quantity: float
    order_type: str
    time_in_force: str
    preview_id: Optional[str] = None
    expires_at: Optional[datetime] = None
    limit_price: Optional[float] = None
    stop_price: Optional[float] = None
    universal_symbol_id: Optional[str] = None
    snaptrade_trade_id: Optional[str] = None
    estimated_price: Optional[float] = None
    estimated_total: Optional[float] = None
    estimated_commission: Optional[float] = None
    combined_remaining_balance: Optional[Dict[str, Any]] = None
    trade_impacts: List[Dict[str, Any]] = field(default_factory=list)
    validation: Optional[PreTradeValidation] = None
    pre_trade_weight: Optional[float] = None
    post_trade_weight: Optional[float] = None
    requires_confirmation: bool = True
    error: Optional[str] = None
    broker_provider: Optional[str] = None

    def to_api_response(self) -> Dict[str, Any]:
        payload = {
            "module": "trading",
            "version": "1.0",
            "timestamp": datetime.now(UTC).isoformat(),
            "status": self.status,
            "error": self.error,
            "metadata": {
                "user_email": self.user_email,
                "account_id": self.account_id,
                "expires_at": _iso(self.expires_at),
                "requires_confirmation": self.requires_confirmation,
                "broker_provider": self.broker_provider,
            },
            "data": {
                "preview_id": self.preview_id,
                "ticker": self.ticker,
                "side": self.side,
                "quantity": self.quantity,
                "order_type": self.order_type,
                "time_in_force": self.time_in_force,
                "limit_price": self.limit_price,
                "stop_price": self.stop_price,
                "universal_symbol_id": self.universal_symbol_id,
                "snaptrade_trade_id": self.snaptrade_trade_id,
                "estimated_price": self.estimated_price,
                "estimated_total": self.estimated_total,
                "estimated_commission": self.estimated_commission,
                "combined_remaining_balance": self.combined_remaining_balance,
                "trade_impacts": self.trade_impacts,
                "pre_trade_weight": self.pre_trade_weight,
                "post_trade_weight": self.post_trade_weight,
                "validation": self.validation.to_api_response() if self.validation else None,
            },
        }
        return make_json_safe(payload)

    def to_formatted_report(self) -> str:
        lines = [
            "Trade Preview",
            f"- status: {self.status}",
            f"- account: {self.account_id}",
            f"- order: {self.side} {self.quantity} {self.ticker} ({self.order_type}/{self.time_in_force})",
        ]
        if self.preview_id:
            lines.append(f"- preview_id: {self.preview_id}")
        if self.expires_at:
            lines.append(f"- expires_at: {_iso(self.expires_at)}")
        if self.estimated_price is not None:
            lines.append(f"- estimated_price: ${self.estimated_price:,.4f}")
        if self.estimated_total is not None:
            lines.append(f"- estimated_total: ${self.estimated_total:,.2f}")
        if self.estimated_commission is not None:
            lines.append(f"- estimated_commission: ${self.estimated_commission:,.2f}")
        if self.broker_provider:
            lines.append(f"- broker_provider: {self.broker_provider}")
        if self.pre_trade_weight is not None and self.post_trade_weight is not None:
            lines.append(f"- weight_change: {self.pre_trade_weight:.2%} -> {self.post_trade_weight:.2%}")
        if self.validation:
            lines.append(self.validation.to_formatted_report())
        if self.error:
            lines.append(f"- error: {self.error}")
        return "\n".join(lines)


@dataclass
class TradeExecutionResult:
    """Execution/cancel envelope with normalized order state fields."""

    status: str
    user_email: str
    preview_id: Optional[str] = None
    order_id: Optional[str] = None
    brokerage_order_id: Optional[str] = None
    order_status: Optional[str] = None
    account_id: Optional[str] = None
    ticker: Optional[str] = None
    side: Optional[str] = None
    quantity: Optional[float] = None
    filled_quantity: Optional[float] = None
    average_fill_price: Optional[float] = None
    total_cost: Optional[float] = None
    commission: Optional[float] = None
    executed_at: Optional[datetime] = None
    cancelled_at: Optional[datetime] = None
    message: Optional[str] = None
    error: Optional[str] = None
    broker_provider: Optional[str] = None
    new_preview: Optional[TradePreviewResult] = None

    def to_api_response(self) -> Dict[str, Any]:
        payload = {
            "module": "trading",
            "version": "1.0",
            "timestamp": datetime.now(UTC).isoformat(),
            "status": self.status,
            "error": self.error,
            "message": self.message,
            "metadata": {
                "user_email": self.user_email,
                "preview_id": self.preview_id,
                "order_id": self.order_id,
                "brokerage_order_id": self.brokerage_order_id,
                "broker_provider": self.broker_provider,
            },
            "data": {
                "account_id": self.account_id,
                "ticker": self.ticker,
                "side": self.side,
                "quantity": self.quantity,
                "order_status": self.order_status,
                "filled_quantity": self.filled_quantity,
                "average_fill_price": self.average_fill_price,
                "total_cost": self.total_cost,
                "commission": self.commission,
                "executed_at": _iso(self.executed_at),
                "cancelled_at": _iso(self.cancelled_at),
                "new_preview": self.new_preview.to_api_response() if self.new_preview else None,
            },
        }
        return make_json_safe(payload)

    def to_formatted_report(self) -> str:
        lines = ["Trade Execution", f"- status: {self.status}"]
        if self.order_id:
            lines.append(f"- order_id: {self.order_id}")
        if self.brokerage_order_id:
            lines.append(f"- brokerage_order_id: {self.brokerage_order_id}")
        if self.broker_provider:
            lines.append(f"- broker_provider: {self.broker_provider}")
        if self.account_id and self.ticker and self.side and self.quantity is not None:
            lines.append(f"- order: {self.side} {self.quantity} {self.ticker} @ {self.account_id}")
        if self.order_status:
            lines.append(f"- order_status: {self.order_status}")
        if self.filled_quantity is not None:
            lines.append(f"- filled_quantity: {self.filled_quantity}")
        if self.average_fill_price is not None:
            lines.append(f"- average_fill_price: ${self.average_fill_price:,.4f}")
        if self.total_cost is not None:
            lines.append(f"- total_cost: ${self.total_cost:,.2f}")
        if self.commission is not None:
            lines.append(f"- commission: ${self.commission:,.2f}")
        if self.executed_at:
            lines.append(f"- executed_at: {_iso(self.executed_at)}")
        if self.cancelled_at:
            lines.append(f"- cancelled_at: {_iso(self.cancelled_at)}")
        if self.message:
            lines.append(f"- message: {self.message}")
        if self.error:
            lines.append(f"- error: {self.error}")
        if self.new_preview:
            lines.append("")
            lines.append("new_preview:")
            lines.append(self.new_preview.to_formatted_report())
        return "\n".join(lines)


@dataclass
class OrderListResult:
    """Order-history envelope used by list orders endpoints/tools."""

    status: str
    user_email: str
    account_id: Optional[str]
    orders: List[Dict[str, Any]] = field(default_factory=list)
    state: str = "all"
    days: int = 30
    error: Optional[str] = None

    def to_api_response(self) -> Dict[str, Any]:
        payload = {
            "module": "trading",
            "version": "1.0",
            "timestamp": datetime.now(UTC).isoformat(),
            "status": self.status,
            "error": self.error,
            "metadata": {
                "user_email": self.user_email,
                "account_id": self.account_id,
                "state": self.state,
                "days": self.days,
                "order_count": len(self.orders),
            },
            "data": {
                "orders": self.orders,
            },
        }
        return make_json_safe(payload)

    def to_formatted_report(self) -> str:
        lines = [
            "Order List",
            f"- status: {self.status}",
            f"- account_id: {self.account_id}",
            f"- state: {self.state}",
            f"- days: {self.days}",
            f"- count: {len(self.orders)}",
        ]
        if self.error:
            lines.append(f"- error: {self.error}")
        for order in self.orders[:25]:
            lines.append(
                f"  - {order.get('status') or order.get('order_status')} "
                f"{order.get('action') or order.get('side')} "
                f"{order.get('units') or order.get('quantity')} "
                f"{order.get('ticker') or order.get('symbol')} "
                f"(id={order.get('brokerage_order_id') or order.get('id')})"
            )
        return "\n".join(lines)


@dataclass
class BrokerAccount:
    """Typed broker account representation returned by broker adapters."""

    account_id: str
    brokerage_name: str
    provider: str
    account_name: Optional[str] = None
    cash_balance: Optional[float] = None
    available_funds: Optional[float] = None
    account_type: Optional[str] = None
    authorization_id: Optional[str] = None
    meta: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        payload: Dict[str, Any] = {
            "account_id": self.account_id,
            "account_name": self.account_name,
            "brokerage_name": self.brokerage_name,
            "provider": self.provider,
            "cash_balance": self.cash_balance,
            "available_funds": self.available_funds,
            "account_type": self.account_type,
            "meta": self.meta,
        }
        if self.authorization_id is not None:
            payload["authorization_id"] = self.authorization_id
        return make_json_safe(payload)


@dataclass
class OrderPreview:
    """Typed preview response returned by broker adapters."""

    estimated_price: Optional[float] = None
    estimated_total: Optional[float] = None
    estimated_commission: Optional[float] = None
    broker_trade_id: Optional[str] = None
    combined_remaining_balance: Optional[Dict[str, Any]] = None
    trade_impacts: List[Dict[str, Any]] = field(default_factory=list)
    impact_response: Optional[Dict[str, Any]] = None
    broker_preview_data: Optional[Dict[str, Any]] = None
    warnings: List[str] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        return make_json_safe(
            {
                "estimated_price": self.estimated_price,
                "estimated_total": self.estimated_total,
                "estimated_commission": self.estimated_commission,
                "warnings": self.warnings,
                "broker_trade_id": self.broker_trade_id,
                "combined_remaining_balance": self.combined_remaining_balance,
                "trade_impacts": self.trade_impacts,
                "impact_response": self.impact_response,
                "broker_preview_data": self.broker_preview_data,
            }
        )


@dataclass
class OrderResult:
    """Typed placement response returned by broker adapters."""

    brokerage_order_id: Optional[str] = None
    status: str = "PENDING"
    filled_quantity: Optional[float] = None
    total_quantity: Optional[float] = None
    execution_price: Optional[float] = None
    total_cost: Optional[float] = None
    commission: Optional[float] = None
    broker_data: Optional[Dict[str, Any]] = None

    def to_dict(self) -> Dict[str, Any]:
        return make_json_safe(
            {
                "brokerage_order_id": self.brokerage_order_id,
                "status": self.status,
                "filled_quantity": self.filled_quantity,
                "total_quantity": self.total_quantity,
                "execution_price": self.execution_price,
                "total_cost": self.total_cost,
                "commission": self.commission,
                "broker_data": self.broker_data,
            }
        )


@dataclass
class OrderStatus:
    """Typed order status row returned by broker adapters."""

    brokerage_order_id: Optional[str] = None
    ticker: Optional[str] = None
    side: Optional[str] = None
    quantity: Optional[float] = None
    order_type: Optional[str] = None
    status: str = "PENDING"
    filled_quantity: Optional[float] = None
    total_quantity: Optional[float] = None
    execution_price: Optional[float] = None
    total_cost: Optional[float] = None
    commission: Optional[float] = None
    time_placed: Optional[str] = None
    time_updated: Optional[str] = None
    perm_id: Optional[str] = None
    broker_data: Optional[Dict[str, Any]] = None

    def to_dict(self) -> Dict[str, Any]:
        return make_json_safe(
            {
                "brokerage_order_id": self.brokerage_order_id,
                "ticker": self.ticker,
                "side": self.side,
                "quantity": self.quantity,
                "order_type": self.order_type,
                "status": self.status,
                "filled_quantity": self.filled_quantity,
                "total_quantity": self.total_quantity,
                "execution_price": self.execution_price,
                "total_cost": self.total_cost,
                "commission": self.commission,
                "time_placed": self.time_placed,
                "time_updated": self.time_updated,
                "perm_id": self.perm_id,
                "broker_data": self.broker_data,
            }
        )


@dataclass
class CancelResult:
    """Typed cancellation response returned by broker adapters."""

    brokerage_order_id: str
    status: str = "CANCEL_PENDING"
    broker_data: Optional[Dict[str, Any]] = None

    def to_dict(self) -> Dict[str, Any]:
        return make_json_safe(
            {
                "brokerage_order_id": self.brokerage_order_id,
                "status": self.status,
                "broker_data": self.broker_data,
            }
        )


__all__ = [
    "ALLOWED_ORDER_TYPES",
    "ALLOWED_SIDES",
    "ALLOWED_TIME_IN_FORCE",
    "BrokerAccount",
    "CancelResult",
    "OrderListResult",
    "OrderPreview",
    "OrderResult",
    "OrderStatus",
    "PreTradeValidation",
    "TradeExecutionResult",
    "TradePreviewResult",
    "_iso",
]
