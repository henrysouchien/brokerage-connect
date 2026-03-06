"""IBKR broker adapter implementing ``BrokerAdapter`` via ``ib_async``.

Called by:
- ``services.trade_execution_service.TradeExecutionService`` for IBKR accounts.

Calls into:
- ``ibkr.connection.IBKRConnectionManager`` and IB Gateway order APIs.

Related:
- ``ibkr.client.IBKRClient`` — read-only data facade (positions, market data, metadata).
"""

from __future__ import annotations

import math
import os
import sys
import threading
from contextlib import contextmanager
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
from ibkr.config import (
    IBKR_OPTION_SNAPSHOT_TIMEOUT,
    IBKR_SNAPSHOT_POLL_INTERVAL,
    IBKR_TIMEOUT,
    IBKR_TRADE_CLIENT_ID,
)
from ibkr.connection import IBKRConnectionManager
from ibkr.locks import ibkr_shared_lock
from options import OptionLeg, OptionStrategy
from providers.routing_config import TRADE_ACCOUNT_MAP


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


_trading_conn_manager: Optional[IBKRConnectionManager] = None
_trading_conn_lock = threading.Lock()
_trading_conn_manager_factory: Any = None

_IBKR_MAX_FLOAT = sys.float_info.max
_IBKR_COMMISSION_UNAVAILABLE_WARNING = "IBKR could not compute commission for this order"


def _get_trading_conn_manager() -> IBKRConnectionManager:
    global _trading_conn_manager, _trading_conn_manager_factory
    with _trading_conn_lock:
        if _trading_conn_manager is None or _trading_conn_manager_factory is not IBKRConnectionManager:
            _trading_conn_manager = IBKRConnectionManager(client_id=IBKR_TRADE_CLIENT_ID)
            _trading_conn_manager_factory = IBKRConnectionManager
        return _trading_conn_manager


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
        # Use a dedicated connection with IBKR_TRADE_CLIENT_ID to avoid
        # colliding with ibkr-mcp (IBKR_CLIENT_ID) or market data (IBKR_CLIENT_ID+1).
        self._conn_manager = _get_trading_conn_manager()
        self._on_refresh = on_refresh or (lambda _account_id: None)
        self._warned_empty_authorized_accounts = False

    @property
    def provider_name(self) -> str:
        return "ibkr"

    def _resolve_native_account(self, account_id: str) -> str:
        """Translate aggregator account ID to native IBKR account ID if mapped.

        Uses directional TRADE_ACCOUNT_MAP lookup (aggregator -> native), not
        resolve_account_aliases() equivalence classes. This is intentional:
        trade submission needs the specific native ID, not all aliases.
        """
        return TRADE_ACCOUNT_MAP.get(account_id, account_id)

    def owns_account(self, account_id: str) -> bool:
        """Check whether this adapter handles the given account.

        Uses the static IBKR_AUTHORIZED_ACCOUNTS env var. Returns False when
        the list is empty (ephemeral connection mode) to prevent auto-claiming
        accounts without explicit configuration. Production deployments must
        set IBKR_AUTHORIZED_ACCOUNTS.
        """
        native_id = self._resolve_native_account(account_id)
        if IBKR_AUTHORIZED_ACCOUNTS:
            return native_id in IBKR_AUTHORIZED_ACCOUNTS

        if not self._warned_empty_authorized_accounts:
            portfolio_logger.warning(
                "IBKR_AUTHORIZED_ACCOUNTS is empty; IBKR account ownership checks return False "
                "in ephemeral connection mode."
            )
            self._warned_empty_authorized_accounts = True
        return False

    def list_accounts(self) -> List[BrokerAccount]:
        with ibkr_shared_lock, self._connected() as ib:
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
        with ibkr_shared_lock, self._connected() as ib:
            return self._search_symbol_with_ib(ib, ticker)

    def _build_roll_contract(
        self,
        ib,
        symbol: str,
        front_month: str,
        back_month: str,
        direction: str = "long_roll",
    ):
        """Build a qualified IBKR BAG combo contract for a futures calendar roll."""
        from ib_async import ComboLeg, Contract
        from ibkr.contracts import resolve_futures_contract

        sym = str(symbol or "").strip().upper()
        fm = str(front_month or "").strip()
        bm = str(back_month or "").strip()
        roll_direction = str(direction or "long_roll").strip().lower()

        if not sym:
            raise ValueError("symbol is required")
        if not fm or not bm:
            raise ValueError("front_month and back_month are required")
        if roll_direction not in {"long_roll", "short_roll"}:
            raise ValueError("direction must be 'long_roll' or 'short_roll'")

        front_contract = resolve_futures_contract(sym, contract_month=fm)
        back_contract = resolve_futures_contract(sym, contract_month=bm)

        qualified = list(ib.qualifyContracts(front_contract, back_contract) or [])
        if len(qualified) < 2:
            raise ValueError(f"Failed to qualify contracts for {sym} {fm}/{bm}")

        front_qualified = qualified[0]
        back_qualified = qualified[1]

        front_con_id = int(front_qualified.conId) if getattr(front_qualified, "conId", None) else None
        back_con_id = int(back_qualified.conId) if getattr(back_qualified, "conId", None) else None
        exchange = (
            getattr(front_qualified, "exchange", None)
            or getattr(back_qualified, "exchange", None)
            or getattr(front_contract, "exchange", None)
            or getattr(back_contract, "exchange", None)
        )
        currency = (
            getattr(front_qualified, "currency", None)
            or getattr(back_qualified, "currency", None)
            or getattr(front_contract, "currency", None)
            or getattr(back_contract, "currency", None)
            or "USD"
        )

        if not front_con_id or not back_con_id:
            raise ValueError(f"Missing conId for {sym} contracts")
        if not exchange:
            raise ValueError(f"Missing exchange for {sym} contracts")

        if roll_direction == "long_roll":
            front_action, back_action = "SELL", "BUY"
        else:
            front_action, back_action = "BUY", "SELL"

        leg1 = ComboLeg(conId=front_con_id, ratio=1, action=front_action, exchange=exchange)
        leg2 = ComboLeg(conId=back_con_id, ratio=1, action=back_action, exchange=exchange)

        bag = Contract(
            symbol=sym,
            secType="BAG",
            exchange=exchange,
            currency=currency,
            comboLegs=[leg1, leg2],
        )
        return bag, front_con_id, back_con_id

    def _serialize_leg_for_storage(self, leg: OptionLeg) -> Dict[str, Any]:
        """Persist only OptionLeg constructor-compatible fields."""
        expiration = leg.expiry_yyyymmdd if leg.expiration else None
        return {
            "position": leg.position,
            "option_type": leg.option_type,
            "strike": leg.strike,
            "premium": leg.premium,
            "size": leg.size,
            "multiplier": leg.multiplier,
            "expiration": expiration,
            "label": leg.label,
            "con_id": leg.con_id,
        }

    def _reconstruct_legs_from_storage(self, legs_data: list[dict[str, Any]]) -> list[OptionLeg]:
        """Rebuild OptionLeg objects and enforce integer combo ratios."""
        if not isinstance(legs_data, list) or not legs_data:
            raise ValueError("order_params['legs'] must be a non-empty list")

        reconstructed: list[OptionLeg] = []
        for idx, leg_data in enumerate(legs_data, start=1):
            if not isinstance(leg_data, dict):
                raise ValueError(f"legs[{idx}] must be an object")
            leg = OptionLeg(**leg_data)
            if leg.size != int(leg.size):
                raise ValueError("All leg sizes must be integers for combo ratios")
            reconstructed.append(leg)
        return reconstructed

    def _build_option_combo_contract(
        self,
        ib,
        strategy: OptionStrategy,
        quantity: float = 1,
    ):
        """Build a qualified IBKR BAG contract for multi-leg option execution."""
        from ib_async import ComboLeg, Contract, Stock
        from ibkr.contracts import resolve_option_contract

        if quantity <= 0:
            raise ValueError("quantity must be greater than 0")

        if not strategy.legs:
            raise ValueError("strategy must include at least one leg")

        underlying_symbol = str(strategy.underlying_symbol or "").strip().upper()
        if not underlying_symbol:
            raise ValueError("underlying_symbol is required for multi-leg option execution")

        contracts_to_qualify: list[Any] = []
        for idx, leg in enumerate(strategy.legs, start=1):
            if leg.size != int(leg.size):
                raise ValueError(f"leg {idx} size must be an integer for combo ratio")

            if leg.option_type == "stock":
                contracts_to_qualify.append(Stock(underlying_symbol, "SMART", "USD"))
                continue

            right = "C" if leg.option_type == "call" else "P"
            contract_identity: dict[str, Any] = {
                "con_id": leg.con_id,
                "underlying_symbol": underlying_symbol,
                "expiry": leg.expiry_yyyymmdd,
                "strike": leg.strike,
                "right": right,
                "multiplier": leg.multiplier,
            }
            contracts_to_qualify.append(
                resolve_option_contract(underlying_symbol, contract_identity=contract_identity)
            )

        qualified_contracts = list(ib.qualifyContracts(*contracts_to_qualify) or [])
        if len(qualified_contracts) != len(contracts_to_qualify):
            raise ValueError("Failed to qualify one or more combo leg contracts on IBKR")

        derived_exchange = next(
            (str(getattr(c, "exchange", "") or "").strip() for c in qualified_contracts if str(getattr(c, "exchange", "") or "").strip()),
            "",
        )
        derived_currency = next(
            (
                str(getattr(c, "currency", "") or "").strip().upper()
                for c in qualified_contracts
                if str(getattr(c, "currency", "") or "").strip()
            ),
            "",
        )
        if not derived_exchange:
            raise ValueError("Unable to determine combo exchange from qualified contracts")
        if not derived_currency:
            raise ValueError("Unable to determine combo currency from qualified contracts")
        if derived_currency != "USD":
            raise ValueError("only US equity options supported in phase 1")

        combo_legs = []
        for idx, (leg, qualified) in enumerate(zip(strategy.legs, qualified_contracts), start=1):
            con_id = int(getattr(qualified, "conId", 0) or 0)
            if con_id <= 0:
                raise ValueError(f"Qualified contract for leg {idx} is missing conId")

            exchange = str(getattr(qualified, "exchange", "") or "").strip() or derived_exchange

            if leg.option_type in {"call", "put"}:
                sec_type = str(getattr(qualified, "secType", "") or "").strip().upper()
                if sec_type == "FOP":
                    raise ValueError(
                        "Futures options (FOP) are not supported in phase 1. Use equity options only."
                    )
                qualified_symbol = str(getattr(qualified, "symbol", "") or "").strip().upper()
                if qualified_symbol and qualified_symbol != underlying_symbol:
                    raise ValueError(
                        f"leg {idx} qualified symbol '{qualified_symbol}' does not match "
                        f"underlying_symbol '{underlying_symbol}'"
                    )

            combo_legs.append(
                ComboLeg(
                    conId=con_id,
                    ratio=int(leg.size),
                    action="BUY" if leg.position == "long" else "SELL",
                    exchange=exchange,
                )
            )

        bag = Contract(
            symbol=underlying_symbol,
            secType="BAG",
            exchange=derived_exchange,
            currency=derived_currency,
            comboLegs=combo_legs,
        )
        return bag, qualified_contracts

    def preview_multileg_option(
        self,
        account_id: str,
        strategy: OptionStrategy,
        quantity: float,
        order_type: str = "Market",
        limit_price: Optional[float] = None,
        time_in_force: str = "Day",
    ) -> OrderPreview:
        """Preview a multi-leg option BAG order with live per-leg pricing."""
        qty = float(quantity)
        if qty <= 0:
            raise ValueError("quantity must be greater than 0")

        account_id = self._resolve_native_account(account_id)
        with ibkr_shared_lock, self._connected() as ib:
            bag, qualified_contracts = self._build_option_combo_contract(
                ib=ib,
                strategy=strategy,
                quantity=qty,
            )

            tickers: list[Any] = []
            poll_interval = max(0.05, float(IBKR_SNAPSHOT_POLL_INTERVAL))
            timeout = max(0.0, float(IBKR_OPTION_SNAPSHOT_TIMEOUT))
            elapsed = 0.0

            try:
                for contract in qualified_contracts:
                    sec_type = str(getattr(contract, "secType", "") or "").upper()
                    generic_ticks = "100,101,106" if sec_type == "OPT" else ""
                    ticker = ib.reqMktData(
                        contract,
                        genericTickList=generic_ticks,
                        snapshot=False,
                        regulatorySnapshot=False,
                        mktDataOptions=[],
                    )
                    tickers.append(ticker)

                while elapsed < timeout:
                    ib.sleep(poll_interval)
                    elapsed += poll_interval
                    if len(tickers) != len(qualified_contracts):
                        break

                    all_ready = True
                    for contract, ticker in zip(qualified_contracts, tickers):
                        bid = _to_float(getattr(ticker, "bid", None))
                        ask = _to_float(getattr(ticker, "ask", None))
                        last = _to_float(getattr(ticker, "last", None))
                        if bid is None and ask is None and last is None:
                            all_ready = False
                            break

                        sec_type = str(getattr(contract, "secType", "") or "").upper()
                        if sec_type == "OPT" and getattr(ticker, "modelGreeks", None) is None:
                            all_ready = False
                            break

                    if all_ready:
                        break
            finally:
                for ticker in tickers:
                    try:
                        ib.cancelMktData(getattr(ticker, "contract", None) or ticker)
                    except Exception:
                        pass

            leg_prices: list[dict[str, Any]] = []
            all_mids_available = len(tickers) == len(strategy.legs)
            net_debit_credit_mid = 0.0

            for idx, (leg, contract) in enumerate(zip(strategy.legs, qualified_contracts)):
                ticker = tickers[idx] if idx < len(tickers) else None
                raw_bid = _to_float(getattr(ticker, "bid", None)) if ticker is not None else None
                raw_ask = _to_float(getattr(ticker, "ask", None)) if ticker is not None else None
                # IBKR returns -1.0 as sentinel for "no data" (e.g. market closed)
                bid = raw_bid if raw_bid is not None and raw_bid > 0 else None
                ask = raw_ask if raw_ask is not None and raw_ask > 0 else None
                mid = (bid + ask) / 2.0 if bid is not None and ask is not None else None

                implied_vol = None
                if leg.option_type in {"call", "put"} and ticker is not None:
                    model_greeks = getattr(ticker, "modelGreeks", None)
                    implied_vol = _to_float(getattr(model_greeks, "impliedVol", None))
                    if implied_vol is None:
                        implied_vol = _to_float(getattr(ticker, "impliedVolatility", None))

                con_id = int(getattr(contract, "conId", 0) or 0) or leg.con_id
                leg_prices.append(
                    {
                        "con_id": con_id,
                        "bid": bid,
                        "ask": ask,
                        "mid": mid,
                        "implied_vol": implied_vol,
                    }
                )

                if mid is None:
                    all_mids_available = False
                    continue

                net_debit_credit_mid += (
                    mid
                    * float(leg.direction)
                    * float(leg.size)
                    * float(leg.multiplier or 1.0)
                )

            if not all_mids_available:
                net_debit_credit_mid = None

            order = self._build_order(
                side="BUY",
                quantity=qty,
                order_type=order_type,
                time_in_force=time_in_force,
                limit_price=limit_price,
                stop_price=None,
                account_id=account_id,
            )
            order_state = ib.whatIfOrder(bag, order)

            estimated_commission, commission_unavailable = _parse_preview_commission(
                getattr(order_state, "commission", None)
            )
            estimated_total = None
            if net_debit_credit_mid is not None and estimated_commission is not None:
                estimated_total = (net_debit_credit_mid * qty) + estimated_commission

            underlying_symbol = str(strategy.underlying_symbol or "").strip().upper()
            order_params = {
                "legs": [self._serialize_leg_for_storage(leg) for leg in strategy.legs],
                "underlying_symbol": underlying_symbol,
                "underlying_price": strategy.underlying_price,
                "quantity": qty,
                "order_type_str": order_type,
                "limit_price": limit_price,
                "time_in_force": time_in_force,
            }

            return OrderPreview(
                estimated_price=net_debit_credit_mid,
                estimated_total=estimated_total,
                estimated_commission=estimated_commission,
                warnings=[_IBKR_COMMISSION_UNAVAILABLE_WARNING] if commission_unavailable else [],
                broker_trade_id=None,
                combined_remaining_balance=None,
                trade_impacts=[],
                broker_preview_data={
                    "order_category": "multi_leg_option",
                    "order_type": order_type,
                    "underlying_symbol": underlying_symbol,
                    "strategy_description": strategy.description,
                    "leg_prices": leg_prices,
                    "net_debit_credit_mid": net_debit_credit_mid,
                    "estimated_total": estimated_total,
                    "order_params": order_params,
                    "init_margin_change": _to_float(getattr(order_state, "initMarginChange", None)),
                    "maint_margin_change": _to_float(getattr(order_state, "maintMarginChange", None)),
                    "init_margin_before": _to_float(getattr(order_state, "initMarginBefore", None)),
                    "init_margin_after": _to_float(getattr(order_state, "initMarginAfter", None)),
                    "maint_margin_before": _to_float(getattr(order_state, "maintMarginBefore", None)),
                    "maint_margin_after": _to_float(getattr(order_state, "maintMarginAfter", None)),
                    "warning_text": getattr(order_state, "warningText", "") or "",
                    "commission": estimated_commission,
                    "commission_currency": getattr(order_state, "commissionCurrency", None),
                },
            )

    def place_multileg_option(
        self,
        account_id: str,
        order_params: Dict[str, Any],
    ) -> OrderResult:
        """Place a previously previewed multi-leg option BAG order."""
        if os.environ.get("IBKR_READONLY", "").lower() == "true":
            raise ValueError("IBKR is in read-only mode. Cannot place orders.")

        if not isinstance(order_params, dict):
            raise ValueError("order_params must be a dictionary")

        account_id = self._resolve_native_account(account_id)
        with ibkr_shared_lock, self._connected() as ib:
            legs_data = order_params.get("legs")
            legs = self._reconstruct_legs_from_storage(legs_data)

            underlying_symbol = str(order_params.get("underlying_symbol") or "").strip().upper()
            if not underlying_symbol:
                raise ValueError("order_params must include underlying_symbol")

            quantity = float(order_params.get("quantity") or 0)
            if quantity <= 0:
                raise ValueError("order_params quantity must be greater than 0")

            strategy = OptionStrategy(
                legs=legs,
                underlying_symbol=underlying_symbol,
                underlying_price=order_params.get("underlying_price"),
                description=order_params.get("description"),
            )
            bag, _ = self._build_option_combo_contract(ib=ib, strategy=strategy, quantity=quantity)

            order = self._build_order(
                side="BUY",
                quantity=quantity,
                order_type=str(order_params.get("order_type_str") or "Market"),
                time_in_force=str(order_params.get("time_in_force") or "Day"),
                limit_price=_to_float(order_params.get("limit_price")),
                stop_price=None,
                account_id=account_id,
            )
            if order_params.get("preview_id"):
                order.orderRef = str(order_params["preview_id"])

            trade = ib.placeOrder(bag, order)

            max_wait_seconds = 5.0
            waited = 0.0
            while not trade.isDone() and waited < max_wait_seconds:
                ib.sleep(0.5)
                waited += 0.5

            ibkr_status = trade.orderStatus.status if trade.orderStatus else "PendingSubmit"
            filled = float(trade.orderStatus.filled) if trade.orderStatus else 0.0
            remaining = float(trade.orderStatus.remaining) if trade.orderStatus else 0.0
            common_status = ibkr_to_common_status(ibkr_status, filled=filled, remaining=remaining)

            avg_fill = (
                float(trade.orderStatus.avgFillPrice)
                if trade.orderStatus and trade.orderStatus.avgFillPrice
                else None
            )
            commission = self._commission_from_trade(trade)

            total_cost = None
            if avg_fill is not None and filled > 0:
                total_cost = (filled * avg_fill) + (commission or 0.0)

            return OrderResult(
                brokerage_order_id=str(trade.order.orderId) if trade.order else None,
                status=common_status,
                filled_quantity=filled,
                total_quantity=quantity,
                execution_price=avg_fill,
                total_cost=total_cost,
                commission=commission,
                broker_data={
                    "ibkr_status": ibkr_status,
                    "order_category": "multi_leg_option",
                    "perm_id": str(trade.order.permId) if trade.order and trade.order.permId else None,
                },
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
        """Run IB what-if order and return estimated execution metrics."""
        from ib_async import Contract

        account_id = self._resolve_native_account(account_id)
        with ibkr_shared_lock, self._connected() as ib:

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

            estimated_commission, commission_unavailable = _parse_preview_commission(
                getattr(order_state, "commission", None)
            )

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
            if estimated_price is not None and estimated_commission is not None:
                estimated_total = (estimated_price * float(quantity)) + estimated_commission

            return OrderPreview(
                estimated_price=estimated_price,
                estimated_total=estimated_total,
                estimated_commission=estimated_commission,
                warnings=[_IBKR_COMMISSION_UNAVAILABLE_WARNING] if commission_unavailable else [],
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
        """Preview a futures calendar roll as an atomic BAG combo order."""
        qty = float(quantity)
        if qty <= 0:
            raise ValueError("quantity must be greater than 0")

        account_id = self._resolve_native_account(account_id)
        with ibkr_shared_lock, self._connected() as ib:

            bag, front_con_id, back_con_id = self._build_roll_contract(
                ib=ib,
                symbol=symbol,
                front_month=front_month,
                back_month=back_month,
                direction=direction,
            )

            order = self._build_order(
                side="BUY",
                quantity=qty,
                order_type=order_type,
                time_in_force=time_in_force,
                limit_price=limit_price,
                stop_price=None,
                account_id=account_id,
            )

            order_state = ib.whatIfOrder(bag, order)

            estimated_commission, commission_unavailable = _parse_preview_commission(
                getattr(order_state, "commission", None)
            )
            estimated_price = _to_float(limit_price)
            estimated_total = None
            if estimated_price is not None and estimated_commission is not None:
                estimated_total = (estimated_price * float(quantity)) + estimated_commission

            init_margin_change = _to_float(getattr(order_state, "initMarginChange", None)) or 0.0
            maint_margin_change = _to_float(getattr(order_state, "maintMarginChange", None)) or 0.0

            order_params = {
                "symbol": str(symbol or "").strip().upper(),
                "front_month": str(front_month or "").strip(),
                "back_month": str(back_month or "").strip(),
                "direction": str(direction or "long_roll").strip().lower(),
                "quantity": qty,
                "order_type_str": order_type,
                "limit_price": limit_price,
                "time_in_force": time_in_force,
                "front_con_id": front_con_id,
                "back_con_id": back_con_id,
            }

            return OrderPreview(
                estimated_price=estimated_price,
                estimated_total=estimated_total,
                estimated_commission=estimated_commission,
                warnings=[_IBKR_COMMISSION_UNAVAILABLE_WARNING] if commission_unavailable else [],
                broker_preview_data={
                    "order_category": "roll",
                    "order_type": "roll",
                    "symbol": order_params["symbol"],
                    "front_month": order_params["front_month"],
                    "back_month": order_params["back_month"],
                    "direction": order_params["direction"],
                    "quantity": order_params["quantity"],
                    "front_con_id": front_con_id,
                    "back_con_id": back_con_id,
                    "init_margin_change": init_margin_change,
                    "maint_margin_change": maint_margin_change,
                    "order_type_str": order_type,
                    "limit_price": limit_price,
                    "time_in_force": time_in_force,
                    "commission": str(getattr(order_state, "commission", "")),
                    "commission_currency": getattr(order_state, "commissionCurrency", None),
                    "warning_text": getattr(order_state, "warningText", "") or "",
                    "order_params": order_params,
                },
            )

    def place_roll(
        self,
        account_id: str,
        order_params: Dict[str, Any],
    ) -> OrderResult:
        """Place a previously previewed futures calendar roll."""
        if os.environ.get("IBKR_READONLY", "").lower() == "true":
            raise ValueError("IBKR is in read-only mode. Cannot place orders.")

        account_id = self._resolve_native_account(account_id)
        with ibkr_shared_lock, self._connected() as ib:

            symbol = str(order_params.get("symbol") or "").strip().upper()
            front_month = str(order_params.get("front_month") or "").strip()
            back_month = str(order_params.get("back_month") or "").strip()
            direction = str(order_params.get("direction") or "long_roll").strip().lower()

            if not symbol or not front_month or not back_month:
                raise ValueError("order_params must include symbol, front_month, and back_month")

            quantity = float(order_params.get("quantity") or 0)
            if quantity <= 0:
                raise ValueError("order_params quantity must be greater than 0")

            bag, _, _ = self._build_roll_contract(
                ib=ib,
                symbol=symbol,
                front_month=front_month,
                back_month=back_month,
                direction=direction,
            )

            order = self._build_order(
                side="BUY",
                quantity=quantity,
                order_type=str(order_params.get("order_type_str") or "Market"),
                time_in_force=str(order_params.get("time_in_force") or "Day"),
                limit_price=_to_float(order_params.get("limit_price")),
                stop_price=None,
                account_id=account_id,
            )

            if order_params.get("preview_id"):
                order.orderRef = str(order_params["preview_id"])

            trade = ib.placeOrder(bag, order)

            max_wait_seconds = 5.0
            waited = 0.0
            while not trade.isDone() and waited < max_wait_seconds:
                ib.sleep(0.5)
                waited += 0.5

            ibkr_status = trade.orderStatus.status if trade.orderStatus else "PendingSubmit"
            filled = float(trade.orderStatus.filled) if trade.orderStatus else 0.0
            remaining = float(trade.orderStatus.remaining) if trade.orderStatus else 0.0
            common_status = ibkr_to_common_status(ibkr_status, filled=filled, remaining=remaining)

            avg_fill = (
                float(trade.orderStatus.avgFillPrice)
                if trade.orderStatus and trade.orderStatus.avgFillPrice
                else None
            )
            commission = self._commission_from_trade(trade)

            total_cost = None
            if avg_fill is not None and filled > 0:
                total_cost = (filled * avg_fill) + (commission or 0.0)

            return OrderResult(
                brokerage_order_id=str(trade.order.orderId) if trade.order else None,
                status=common_status,
                filled_quantity=filled,
                total_quantity=quantity,
                execution_price=avg_fill,
                total_cost=total_cost,
                commission=commission,
                broker_data={
                    "ibkr_status": ibkr_status,
                    "order_category": "roll",
                    "perm_id": str(trade.order.permId) if trade.order and trade.order.permId else None,
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

        account_id = self._resolve_native_account(account_id)
        with ibkr_shared_lock, self._connected() as ib:

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
        account_id = self._resolve_native_account(account_id)
        with ibkr_shared_lock, self._connected() as ib:
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
        account_id = self._resolve_native_account(account_id)
        with ibkr_shared_lock, self._connected() as ib:

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
        account_id = self._resolve_native_account(account_id)
        with ibkr_shared_lock, self._connected() as ib:
            return self._get_account_balance_internal(ib, account_id)

    def refresh_after_trade(self, account_id: str) -> None:
        try:
            self._on_refresh(account_id)
        except Exception as e:
            portfolio_logger.warning(
                f"on_refresh callback failed for IBKR account {account_id}: {e}"
            )

    @contextmanager
    def _connected(self):
        """Open an ephemeral IBKR connection with user-friendly connection errors."""
        cm = self._conn_manager.connection()
        try:
            ib = cm.__enter__()
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
        try:
            yield ib
        except Exception:
            if not cm.__exit__(*sys.exc_info()):
                raise
        else:
            cm.__exit__(None, None, None)

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

    def _fill_data_from_trade(self, trade) -> tuple[float, Optional[float]]:
        total_filled = 0.0
        weighted_price_total = 0.0
        priced_fill_qty = 0.0

        for fill in getattr(trade, "fills", []) or []:
            execution = getattr(fill, "execution", None)
            shares = _to_float(getattr(execution, "shares", None)) if execution else None
            price = _to_float(getattr(execution, "price", None)) if execution else None
            if shares is None or shares <= 0:
                continue

            total_filled += shares
            if price is None:
                continue
            weighted_price_total += shares * price
            priced_fill_qty += shares

        if total_filled <= 0:
            return 0.0, None

        avg_price = (weighted_price_total / priced_fill_qty) if priced_fill_qty > 0 else None
        return total_filled, avg_price

    def _map_trade_to_status(self, trade) -> OrderStatus:
        ibkr_status = trade.orderStatus.status if trade.orderStatus else "Unknown"
        os_filled = _to_float(getattr(trade.orderStatus, "filled", None)) if trade.orderStatus else None
        os_avg_fill_raw = getattr(trade.orderStatus, "avgFillPrice", None) if trade.orderStatus else None
        os_avg_price = _to_float(os_avg_fill_raw) if os_avg_fill_raw else None
        filled = os_filled or 0.0
        remaining = (
            _to_float(getattr(trade.orderStatus, "remaining", None)) if trade.orderStatus else None
        ) or 0.0

        time_placed = None
        time_updated = None
        if trade.log:
            time_placed = trade.log[0].time if trade.log[0] else None
            time_updated = trade.log[-1].time if trade.log[-1] else None

        execution_price = os_avg_price
        if os_filled is not None and os_filled <= 0 and ibkr_status == "Filled":
            sec_type = getattr(trade.contract, "secType", "")
            if sec_type != "BAG":
                fill_filled, fill_avg_price = self._fill_data_from_trade(trade)
                if fill_filled > 0:
                    filled = fill_filled
                    execution_price = fill_avg_price

        order_total_quantity = _to_float(getattr(trade.order, "totalQuantity", None)) or 0.0
        if order_total_quantity <= 0 and filled > 0:
            order_total_quantity = filled

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
            quantity=order_total_quantity,
            order_type=trade.order.orderType,
            status=ibkr_to_common_status(ibkr_status, filled, remaining),
            filled_quantity=filled,
            total_quantity=order_total_quantity,
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
        result = float(value)
        if math.isinf(result):
            return None
        return result
    except (TypeError, ValueError):
        return None


def _parse_preview_commission(value: Any) -> tuple[Optional[float], bool]:
    if value is None:
        return None, True
    try:
        commission = float(value)
    except (TypeError, ValueError):
        return None, True
    if commission >= _IBKR_MAX_FLOAT or math.isinf(commission):
        return None, True
    return commission, False


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
