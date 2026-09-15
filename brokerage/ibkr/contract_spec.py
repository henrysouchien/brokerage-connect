"""Vendor-neutral contract specs for boundary-crossing IBKR snapshot calls."""

from __future__ import annotations

from dataclasses import dataclass
import re
from typing import Literal, Optional, Union

SecType = Literal["STK", "FUT", "OPT"]
OptionRight = Literal["C", "P"]


@dataclass(frozen=True)
class IBKRContractSpec:
    """Vendor-neutral description of a contract for boundary-crossing calls."""

    sec_type: SecType
    symbol: str
    currency: str
    exchange: str = "SMART"
    expiry: Optional[str] = None
    strike: Optional[float] = None
    right: Optional[OptionRight] = None
    multiplier: Optional[Union[str, int, float]] = None
    contract_month: Optional[str] = None
    con_id: Optional[int] = None

    def __post_init__(self) -> None:
        currency = str(self.currency or "").strip().upper()
        if re.fullmatch(r"[A-Z]{3}", currency) is None:
            raise ValueError("IBKR contract currency requires an explicit ISO code")
        object.__setattr__(self, "currency", currency)

    @classmethod
    def stock(
        cls,
        symbol: str,
        *,
        currency: str,
        exchange: str = "SMART",
    ) -> "IBKRContractSpec":
        return cls(sec_type="STK", symbol=symbol, exchange=exchange, currency=currency)

    @classmethod
    def option(
        cls,
        symbol: str,
        *,
        expiry: str,
        strike: float,
        right: OptionRight,
        currency: str,
        exchange: str = "SMART",
        multiplier: Optional[Union[str, int, float]] = None,
    ) -> "IBKRContractSpec":
        return cls(
            sec_type="OPT",
            symbol=symbol,
            expiry=expiry,
            strike=strike,
            right=right,
            exchange=exchange,
            currency=currency,
            multiplier=multiplier,
        )

    @classmethod
    def option_by_con_id(
        cls,
        symbol: str,
        *,
        con_id: int,
        currency: str,
        exchange: str = "SMART",
    ) -> "IBKRContractSpec":
        return cls(
            sec_type="OPT",
            symbol=symbol,
            con_id=con_id,
            exchange=exchange,
            currency=currency,
        )

    @classmethod
    def future(
        cls,
        symbol: str,
        *,
        currency: str,
        contract_month: Optional[str] = None,
        exchange: str = "SMART",
    ) -> "IBKRContractSpec":
        return cls(
            sec_type="FUT",
            symbol=symbol,
            contract_month=contract_month,
            exchange=exchange,
            currency=currency,
        )
