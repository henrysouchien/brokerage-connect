"""Abstract broker adapter interface for trade operations."""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Any, Dict, List, Optional

from brokerage.trade_objects import (
    BrokerAccount,
    CancelResult,
    OrderPreview,
    OrderResult,
    OrderStatus,
)

if TYPE_CHECKING:
    import pandas as pd

    from brokerage.options_types import OptionStrategy
    from ibkr.contract_spec import IBKRContractSpec


class BrokerAdapter(ABC):
    """Abstract trade adapter contract used by ``TradeExecutionService``.

    Called by:
    - ``services.trade_execution_service.TradeExecutionService``.

    Implemented by:
    - SnapTrade/IBKR/Schwab adapter classes.

    Contract semantics:
    - Methods should raise clear exceptions on broker-side errors.
    - Returned dataclasses in ``core.trade_objects`` must be populated with
      broker-native details in ``broker_data`` where available.
    """

    @property
    @abstractmethod
    def provider_name(self) -> str:
        """Return the provider identifier (e.g., 'snaptrade', 'ibkr')."""

    @abstractmethod
    def owns_account(self, account_id: str) -> bool:
        """Return True if this adapter manages the given account_id."""

    @abstractmethod
    def list_accounts(self) -> List[BrokerAccount]:
        """List tradeable accounts managed by this broker."""

    @abstractmethod
    def search_symbol(self, account_id: str, ticker: str) -> Dict[str, Any]:
        """Resolve a ticker symbol for the given account."""

    @abstractmethod
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
        """Preview an order and return estimated cost/commission."""

    @abstractmethod
    def place_order(
        self,
        account_id: str,
        order_params: Dict[str, Any],
    ) -> OrderResult:
        """Place an order and return execution details."""

    @abstractmethod
    def get_orders(
        self,
        account_id: str,
        state: str = "all",
        days: int = 30,
    ) -> List[OrderStatus]:
        """Fetch order history from the broker."""

    @abstractmethod
    def cancel_order(
        self,
        account_id: str,
        order_id: str,
    ) -> CancelResult:
        """Cancel an order and return status."""

    @abstractmethod
    def get_account_balance(self, account_id: str) -> Optional[float]:
        """Return available cash balance for the account."""

    @abstractmethod
    def refresh_after_trade(self, account_id: str) -> None:
        """Trigger post-trade position refresh/cache invalidation."""

    @abstractmethod
    def fetch_market_snapshot(
        self,
        contracts: list[IBKRContractSpec | Any],
        *,
        budget_user_id: int | None = None,
        **kwargs,
    ) -> list[dict[str, Any]]:
        """Fetch live market snapshots for broker-native contract specs."""

    @abstractmethod
    def get_live_positions(
        self,
        account_id: str | None = None,
        *,
        budget_user_id: int | None = None,
    ) -> pd.DataFrame:
        """Fetch live broker positions for safety validation."""

    @abstractmethod
    def query_open_orders(
        self,
        account_id: str | None = None,
        *,
        budget_user_id: int | None = None,
    ) -> List[OrderStatus]:
        """Query currently open orders for broker recovery probes.

        ``OrderStatus.broker_data`` must include these keys for recovery
        matching: ``order_ref``, ``con_id``, ``symbol``, ``action``,
        ``quantity``, ``filled``, and ``remaining``.
        """

    @abstractmethod
    def query_completed_orders(
        self,
        account_id: str | None = None,
        *,
        budget_user_id: int | None = None,
    ) -> List[OrderStatus]:
        """Query completed orders for broker recovery probes.

        ``OrderStatus.broker_data`` must include these keys for recovery
        matching: ``order_ref``, ``con_id``, ``symbol``, ``action``,
        ``quantity``, ``filled``, and ``remaining``.
        """

    @abstractmethod
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
        """Preview a futures calendar roll."""

    @abstractmethod
    def place_roll(
        self,
        account_id: str,
        order_params: Dict[str, Any],
    ) -> OrderResult:
        """Place a previously previewed futures calendar roll."""

    @abstractmethod
    def preview_multileg_option(
        self,
        account_id: str,
        strategy: OptionStrategy,
        quantity: float,
        order_type: str = "Market",
        limit_price: Optional[float] = None,
        time_in_force: str = "Day",
    ) -> OrderPreview:
        """Preview a multi-leg option order."""

    @abstractmethod
    def place_multileg_option(
        self,
        account_id: str,
        order_params: Dict[str, Any],
    ) -> OrderResult:
        """Place a previously previewed multi-leg option order."""
