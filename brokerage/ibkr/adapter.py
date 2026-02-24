"""IBKR broker adapter implementing ``BrokerAdapter`` via ``ib_async``.

Called by:
- ``services.trade_execution_service.TradeExecutionService`` for IBKR accounts.

Calls into:
- ``ibkr.connection.IBKRConnectionManager`` and IB Gateway order APIs.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any, Callable, Dict, List, Optional

from brokerage._logging import portfolio_logger
from brokerage.broker_adapter import BrokerAdapter
from brokerage.config import (
    IBKR_AUTHORIZED_ACCOUNTS,
    IBKR_GATEWAY_HOST,
    IBKR_GATEWAY_PORT,
    IBKR_READONLY,
)
from brokerage.trade_objects import (
    BrokerAccount,
    CancelResult,
    OrderPreview,
    OrderResult,
    OrderStatus,
    _iso,
)
from ibkr.connection import IBKRConnectionManager
from ibkr.locks import ibkr_shared_lock


IBKR_STATUS_MAP = {
    "PendingSubmit": "PENDING",
    "ApiPending": "PENDING",
    "PreSubmitted": "PENDING",
    "Submitted": "ACCEPTED",
    "ApiUpdate": "ACCEPTED",
    "Filled": "EXECUTED",
    "Cancelled": "CANCELED",
    "ApiCancelled": "CANCELED",
    "Inactive": "REJECTED",
    "PendingCancel": "CANCEL_PENDING",
}


def ibkr_to_common_status(status: str, filled: float = 0, remaining: float = 0) -> str:
    """Map IBKR status to common status with quantity-based PARTIAL detection."""
    if status == "Submitted" and filled > 0 and remaining > 0:
        return "PARTIAL"
    if status == "ValidationError":
        return "REJECTED"
    return IBKR_STATUS_MAP.get(status, "PENDING")


class IBKRBrokerAdapter(BrokerAdapter):
    """Interactive Brokers adapter with contract qualification safeguards."""

    def __init__(
        self,
        user_email: str,
        on_refresh: Callable[[str], None] | None = None,
    ):
        self._user_email = user_email
        self._conn_manager = IBKRConnectionManager()
        self._on_refresh = on_refresh or (lambda _account_id: None)

    @property
    def provider_name(self) -> str:
        return "ibkr"

    def owns_account(self, account_id: str) -> bool:
        if IBKR_AUTHORIZED_ACCOUNTS and account_id not in IBKR_AUTHORIZED_ACCOUNTS:
            return False

        if self._conn_manager.is_connected:
            return account_id in self._conn_manager.managed_accounts

        if IBKR_AUTHORIZED_ACCOUNTS:
            return account_id in IBKR_AUTHORIZED_ACCOUNTS
        return False

    def list_accounts(self) -> List[BrokerAccount]:
        with ibkr_shared_lock:
            ib = self._ensure_connected()
            accounts = list(ib.managedAccounts() or [])
            if IBKR_AUTHORIZED_ACCOUNTS:
                accounts = [a for a in accounts if a in IBKR_AUTHORIZED_ACCOUNTS]

            result: List[BrokerAccount] = []
            for acct_id in accounts:
                available_funds = self._get_account_balance_internal(ib, acct_id)
                result.append(
                    BrokerAccount(
                        account_id=acct_id,
                        account_name=f"IBKR {acct_id}",
                        brokerage_name="Interactive Brokers",
                        provider="ibkr",
                        cash_balance=available_funds,
                        available_funds=available_funds,
                        account_type=None,
                        meta={},
                    )
                )
            return result

    def search_symbol(self, account_id: str, ticker: str) -> Dict[str, Any]:
        with ibkr_shared_lock:
            ib = self._ensure_connected()
            return self._search_symbol_with_ib(ib, ticker)

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
        """Run IB what-if order and return estimated execution metrics."""
        from ib_async import Contract

        with ibkr_shared_lock:
            ib = self._ensure_connected()

            # Reuse conId from earlier search_symbol() call if available
            if symbol_id:
                try:
                    contract = Contract(conId=int(symbol_id), exchange="SMART")
                    qualified = ib.qualifyContracts(contract)
                    if qualified:
                        contract = qualified[0]
                        symbol_info = {
                            "ticker": (ticker or "").upper().strip(),
                            "con_id": contract.conId,
                            "contract": contract,
                        }
                    else:
                        symbol_info = self._search_symbol_with_ib(ib, ticker)
                        contract = symbol_info["contract"]
                except Exception:
                    symbol_info = self._search_symbol_with_ib(ib, ticker)
                    contract = symbol_info["contract"]
            else:
                symbol_info = self._search_symbol_with_ib(ib, ticker)
                contract = symbol_info["contract"]

            order = self._build_order(
                side=side,
                quantity=quantity,
                order_type=order_type,
                time_in_force=time_in_force,
                limit_price=limit_price,
                stop_price=stop_price,
                account_id=account_id,
            )

            order_state = ib.whatIfOrder(contract, order)

            estimated_commission = 0.0
            try:
                estimated_commission = float(order_state.commission)
            except (TypeError, ValueError):
                estimated_commission = 0.0

            estimated_price = limit_price or stop_price
            if estimated_price is None:
                try:
                    ib.reqMktData(contract, "", False, False)
                    ib.sleep(2)
                    ticker_obj = ib.ticker(contract)
                    if ticker_obj and ticker_obj.last and ticker_obj.last > 0:
                        estimated_price = float(ticker_obj.last)
                    elif ticker_obj and ticker_obj.close and ticker_obj.close > 0:
                        estimated_price = float(ticker_obj.close)
                except Exception:
                    pass
                finally:
                    try:
                        ib.cancelMktData(contract)
                    except Exception:
                        pass

            estimated_total = None
            if estimated_price is not None:
                estimated_total = (estimated_price * float(quantity)) + estimated_commission

            return OrderPreview(
                estimated_price=estimated_price,
                estimated_total=estimated_total,
                estimated_commission=estimated_commission,
                broker_trade_id=None,
                combined_remaining_balance=None,
                trade_impacts=[],
                broker_preview_data={
                    "commission": str(order_state.commission),
                    "commission_currency": order_state.commissionCurrency,
                    "init_margin_before": str(order_state.initMarginBefore),
                    "init_margin_after": str(order_state.initMarginAfter),
                    "init_margin_change": str(order_state.initMarginChange),
                    "maint_margin_before": str(order_state.maintMarginBefore),
                    "maint_margin_after": str(order_state.maintMarginAfter),
                    "maint_margin_change": str(order_state.maintMarginChange),
                    "equity_with_loan_before": str(order_state.equityWithLoanBefore),
                    "equity_with_loan_after": str(order_state.equityWithLoanAfter),
                    "equity_with_loan_change": str(order_state.equityWithLoanChange),
                    "warning_text": order_state.warningText or "",
                    "order_params": {
                        "account_id": account_id,
                        "ticker": (ticker or "").upper().strip(),
                        "side": side,
                        "quantity": float(quantity),
                        "order_type": order_type,
                        "time_in_force": time_in_force,
                        "limit_price": limit_price,
                        "stop_price": stop_price,
                        "con_id": symbol_info["con_id"],
                    },
                },
            )

    def place_order(
        self,
        account_id: str,
        order_params: Dict[str, Any],
    ) -> OrderResult:
        """Place IBKR order after contract re-qualification safety checks."""
        from ib_async import Contract, Stock

        if IBKR_READONLY:
            raise ValueError(
                "IBKR is in read-only mode (IBKR_READONLY=true). "
                "Order placement is disabled."
            )

        with ibkr_shared_lock:
            ib = self._ensure_connected()

            ticker = str(order_params["ticker"])
            stored_con_id = order_params.get("con_id")

            if stored_con_id:
                contract = Contract(conId=int(stored_con_id), exchange="SMART")
                qualified = ib.qualifyContracts(contract)
                if not qualified:
                    portfolio_logger.warning(
                        f"conId {stored_con_id} qualification failed, falling back to ticker"
                    )
                    contract = Stock(ticker, "SMART", "USD")
                    qualified = ib.qualifyContracts(contract)
            else:
                contract = Stock(ticker, "SMART", "USD")
                qualified = ib.qualifyContracts(contract)

            if not qualified:
                raise ValueError(f"Cannot re-qualify contract for {ticker}")

            contract = qualified[0]
            if stored_con_id and contract.conId != int(stored_con_id):
                raise ValueError(
                    f"Contract mismatch: stored conId={stored_con_id}, resolved conId={contract.conId}. "
                    "Aborting for safety."
                )

            order = self._build_order(
                side=str(order_params["side"]),
                quantity=float(order_params["quantity"]),
                order_type=str(order_params["order_type"]),
                time_in_force=str(order_params["time_in_force"]),
                limit_price=_to_float(order_params.get("limit_price")),
                stop_price=_to_float(order_params.get("stop_price")),
                account_id=account_id,
            )

            if order_params.get("preview_id"):
                order.orderRef = str(order_params["preview_id"])

            trade = ib.placeOrder(contract, order)

            max_wait = 5
            waited = 0
            while not trade.isDone() and waited < max_wait:
                ib.sleep(1)
                waited += 1

            ibkr_status = trade.orderStatus.status if trade.orderStatus else "PendingSubmit"
            filled = float(trade.orderStatus.filled) if trade.orderStatus else 0.0
            remaining = float(trade.orderStatus.remaining) if trade.orderStatus else 0.0
            common_status = ibkr_to_common_status(ibkr_status, filled, remaining)

            commission = self._commission_from_trade(trade)
            avg_fill = (
                float(trade.orderStatus.avgFillPrice)
                if trade.orderStatus and trade.orderStatus.avgFillPrice
                else None
            )

            total_cost = None
            if avg_fill is not None and filled > 0:
                total_cost = (filled * avg_fill) + (commission or 0.0)

            return OrderResult(
                brokerage_order_id=str(trade.order.orderId),
                status=common_status,
                filled_quantity=filled,
                execution_price=avg_fill,
                total_quantity=float(order_params["quantity"]),
                total_cost=total_cost,
                commission=commission,
                broker_data={
                    "perm_id": str(trade.order.permId) if trade.order.permId else None,
                    "ibkr_status": ibkr_status,
                },
            )

    def get_orders(
        self,
        account_id: str,
        state: str = "all",
        days: int = 30,
    ) -> List[OrderStatus]:
        with ibkr_shared_lock:
            ib = self._ensure_connected()
            cutoff = datetime.now(timezone.utc) - timedelta(days=int(days))

            seen_perm_ids = set()
            results: List[OrderStatus] = []

            if state in ("all", "open"):
                for trade in ib.openTrades():
                    if account_id and trade.order.account != account_id:
                        continue

                    perm_id = trade.order.permId
                    if perm_id and perm_id in seen_perm_ids:
                        continue
                    if perm_id:
                        seen_perm_ids.add(perm_id)

                    results.append(self._map_trade_to_status(trade))

            if state in ("all", "executed", "cancelled"):
                completed_trades = ib.reqCompletedOrders(apiOnly=False)
                for trade in completed_trades:
                    if account_id and trade.order.account != account_id:
                        continue

                    perm_id = trade.order.permId
                    if perm_id and perm_id in seen_perm_ids:
                        continue
                    if perm_id:
                        seen_perm_ids.add(perm_id)

                    mapped = self._map_trade_to_status(trade)

                    if state == "executed" and mapped.status != "EXECUTED":
                        continue
                    if state == "cancelled" and mapped.status not in ("CANCELED", "REJECTED"):
                        continue

                    placed_at = _as_utc_from_iso(mapped.time_placed)
                    if placed_at and placed_at < cutoff:
                        continue

                    results.append(mapped)

            return results

    def cancel_order(
        self,
        account_id: str,
        order_id: str,
    ) -> CancelResult:
        with ibkr_shared_lock:
            ib = self._ensure_connected()

            target_trade = None
            for trade in ib.openTrades():
                if str(trade.order.orderId) == str(order_id) and trade.order.account == account_id:
                    target_trade = trade
                    break

            if not target_trade:
                raise ValueError(
                    f"Open order {order_id} not found in IB Gateway for account {account_id}"
                )

            ib.cancelOrder(target_trade.order)
            ib.sleep(2)

            return CancelResult(
                status="CANCEL_PENDING",
                brokerage_order_id=str(order_id),
                broker_data={"account_id": account_id},
            )

    def get_account_balance(self, account_id: str) -> Optional[float]:
        with ibkr_shared_lock:
            ib = self._ensure_connected()
            return self._get_account_balance_internal(ib, account_id)

    def refresh_after_trade(self, account_id: str) -> None:
        try:
            self._on_refresh(account_id)
        except Exception as e:
            portfolio_logger.warning(
                f"on_refresh callback failed for IBKR account {account_id}: {e}"
            )

    def _ensure_connected(self):
        """Ensure gateway connection and return user-facing errors when unavailable."""
        try:
            return self._conn_manager.ensure_connected()
        except ConnectionRefusedError as e:
            raise ValueError(
                "IB Gateway is not running. Start IB Gateway on "
                f"{IBKR_GATEWAY_HOST}:{IBKR_GATEWAY_PORT} and try again."
            ) from e
        except Exception as e:
            message = str(e).lower()
            if "2fa" in message or "authentication" in message or "auth" in message:
                raise ValueError(
                    "IB Gateway authentication expired. Approve the 2FA notification on IBKR Mobile."
                ) from e
            raise ValueError(f"Cannot connect to IB Gateway: {e}") from e

    def _build_order(
        self,
        side: str,
        quantity: float,
        order_type: str,
        time_in_force: str,
        limit_price: Optional[float],
        stop_price: Optional[float],
        account_id: str,
    ):
        from ib_async import LimitOrder, MarketOrder, Order, StopOrder

        action = side.upper()
        qty = float(quantity)

        tif_map = {
            "Day": "DAY",
            "GTC": "GTC",
            "FOK": "FOK",
            "IOC": "IOC",
        }
        ib_tif = tif_map.get(time_in_force, "DAY")

        if order_type == "Market":
            order = MarketOrder(action, qty)
        elif order_type == "Limit":
            if limit_price is None:
                raise ValueError("limit_price required for Limit orders")
            order = LimitOrder(action, qty, limit_price)
        elif order_type == "Stop":
            if stop_price is None:
                raise ValueError("stop_price required for Stop orders")
            order = StopOrder(action, qty, stop_price)
        elif order_type == "StopLimit":
            if limit_price is None or stop_price is None:
                raise ValueError("Both limit_price and stop_price required for StopLimit orders")
            order = Order(
                action=action,
                totalQuantity=qty,
                orderType="STP LMT",
                lmtPrice=limit_price,
                auxPrice=stop_price,
            )
        else:
            raise ValueError(f"Unsupported order type: {order_type}")

        order.tif = ib_tif
        order.account = account_id
        return order

    def _search_symbol_with_ib(self, ib, ticker: str) -> Dict[str, Any]:
        from ib_async import Stock

        ticker_upper = (ticker or "").upper().strip()
        contract = Stock(ticker_upper, "SMART", "USD")
        qualified = ib.qualifyContracts(contract)
        if not qualified:
            raise ValueError(
                f"Could not resolve ticker '{ticker_upper}' on IBKR. "
                "Ensure the symbol is valid and tradeable on SMART exchange."
            )

        resolved = qualified[0]
        return {
            "ticker": ticker_upper,
            "symbol": resolved.symbol,
            "universal_symbol_id": None,
            "broker_symbol_id": str(resolved.conId),
            "con_id": resolved.conId,
            "name": getattr(resolved, "description", None) or ticker_upper,
            "currency": resolved.currency,
            "type": resolved.secType,
            "contract": resolved,
        }

    def _get_account_balance_internal(self, ib, account_id: str) -> Optional[float]:
        try:
            account_values = ib.accountValues(account=account_id)
            for av in account_values:
                if av.tag == "AvailableFunds" and av.currency == "USD":
                    return float(av.value)

            if not account_values:
                ib.reqAccountUpdates(account=account_id)
                account_values = ib.accountValues(account=account_id)
                for av in account_values:
                    if av.tag == "AvailableFunds" and av.currency == "USD":
                        return float(av.value)
        except Exception as e:
            portfolio_logger.warning(f"Failed to get IBKR balance for {account_id}: {e}")
        return None

    def _map_trade_to_status(self, trade) -> OrderStatus:
        ibkr_status = trade.orderStatus.status if trade.orderStatus else "Unknown"
        filled = float(trade.orderStatus.filled) if trade.orderStatus else 0.0
        remaining = float(trade.orderStatus.remaining) if trade.orderStatus else 0.0

        time_placed = None
        time_updated = None
        if trade.log:
            time_placed = trade.log[0].time if trade.log[0] else None
            time_updated = trade.log[-1].time if trade.log[-1] else None

        execution_price = (
            float(trade.orderStatus.avgFillPrice)
            if trade.orderStatus and trade.orderStatus.avgFillPrice
            else None
        )
        commission = self._commission_from_trade(trade)
        total_cost = None
        if execution_price is not None and filled > 0:
            total_cost = (filled * execution_price) + (commission or 0.0)

        time_placed_iso = _iso(_as_utc(time_placed))
        time_updated_iso = _iso(_as_utc(time_updated))

        return OrderStatus(
            brokerage_order_id=str(trade.order.orderId),
            perm_id=str(trade.order.permId) if trade.order.permId else None,
            ticker=trade.contract.symbol if trade.contract else None,
            side=trade.order.action,
            quantity=float(trade.order.totalQuantity),
            order_type=trade.order.orderType,
            status=ibkr_to_common_status(ibkr_status, filled, remaining),
            filled_quantity=filled,
            total_quantity=float(trade.order.totalQuantity),
            execution_price=execution_price,
            total_cost=total_cost,
            commission=commission,
            time_placed=time_placed_iso,
            time_updated=time_updated_iso,
            broker_data={"ibkr_status": ibkr_status},
        )

    def _commission_from_trade(self, trade) -> Optional[float]:
        commission_total = 0.0
        seen = False
        for fill in getattr(trade, "fills", []) or []:
            report = getattr(fill, "commissionReport", None)
            commission = _to_float(getattr(report, "commission", None)) if report else None
            if commission is not None:
                commission_total += commission
                seen = True
        return commission_total if seen else None


def _to_float(value: Any) -> Optional[float]:
    try:
        if value is None:
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def _as_utc(value: Any) -> Optional[datetime]:
    if not isinstance(value, datetime):
        return None
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _as_utc_from_iso(value: Optional[str]) -> Optional[datetime]:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError:
        return None
    return _as_utc(parsed)
