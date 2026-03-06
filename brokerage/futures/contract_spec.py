from __future__ import annotations

from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path
from typing import Any, Dict, Literal, Optional, cast

import yaml

FuturesAssetClass = Literal[
    "equity_index",
    "fixed_income",
    "metals",
    "energy",
    "agricultural",
    "fx",
]


_VALID_ASSET_CLASSES = {
    "equity_index",
    "fixed_income",
    "metals",
    "energy",
    "agricultural",
    "fx",
}


@dataclass(frozen=True)
class FuturesContractSpec:
    """Broker-agnostic futures contract specification."""

    symbol: str
    multiplier: float
    tick_size: float
    currency: str
    exchange: str
    asset_class: FuturesAssetClass
    fmp_symbol: Optional[str] = None

    @property
    def tick_value(self) -> float:
        """Dollar value of one tick move."""
        return self.tick_size * self.multiplier

    @property
    def point_value(self) -> float:
        """Dollar value of a one-point move."""
        return self.multiplier

    def notional(self, quantity: float, price: float) -> float:
        """Calculate notional exposure: quantity x multiplier x price."""
        return quantity * self.multiplier * price

    def pnl(self, quantity: float, entry_price: float, exit_price: float) -> float:
        """Calculate P&L: quantity x multiplier x (exit - entry)."""
        return quantity * self.multiplier * (exit_price - entry_price)

    def to_contract_identity(self) -> Dict[str, object]:
        """Export as contract_identity dict for InstrumentMeta threading."""
        return {
            "symbol": self.symbol,
            "multiplier": self.multiplier,
            "tick_size": self.tick_size,
            "currency": self.currency,
            "exchange": self.exchange,
            "asset_class": self.asset_class,
        }


@lru_cache(maxsize=1)
def _load_contracts_yaml() -> Dict[str, Any]:
    """Load the canonical futures contracts catalog."""
    yaml_path = Path(__file__).resolve().with_name("contracts.yaml")
    with yaml_path.open("r", encoding="utf-8") as f:
        payload = yaml.safe_load(f) or {}

    contracts = payload.get("contracts", {})
    if not isinstance(contracts, dict):
        raise ValueError("Invalid contracts catalog: 'contracts' must be a mapping")
    return contracts


def load_contract_specs() -> Dict[str, FuturesContractSpec]:
    """Load all contract specs from the canonical contracts catalog."""
    catalog = _load_contracts_yaml()
    specs: Dict[str, FuturesContractSpec] = {}

    for symbol, meta in catalog.items():
        if not isinstance(meta, dict):
            raise ValueError(f"Invalid contracts catalog entry for symbol: {symbol}")

        key = str(symbol or "").strip().upper()
        if not key:
            continue

        asset_class_raw = str(meta.get("asset_class") or "").strip()
        if not asset_class_raw:
            raise ValueError(f"Missing asset_class in contracts.yaml for symbol: {key}")
        if asset_class_raw not in _VALID_ASSET_CLASSES:
            raise ValueError(
                f"Invalid asset_class '{asset_class_raw}' in contracts.yaml for symbol: {key}"
            )

        fmp_symbol_raw = meta.get("fmp_symbol")
        fmp_symbol = str(fmp_symbol_raw).strip().upper() if fmp_symbol_raw else None

        specs[key] = FuturesContractSpec(
            symbol=key,
            multiplier=float(meta["multiplier"]),
            tick_size=float(meta["tick_size"]),
            currency=str(meta["currency"]),
            exchange=str(meta["exchange"]),
            asset_class=cast(FuturesAssetClass, asset_class_raw),
            fmp_symbol=fmp_symbol,
        )

    return specs


def get_contract_spec(symbol: str) -> Optional[FuturesContractSpec]:
    """Look up a single contract spec by IBKR root symbol."""
    specs = load_contract_specs()
    return specs.get(str(symbol or "").strip().upper())
