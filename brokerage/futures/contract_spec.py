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
    data_symbol: Optional[str] = None
    margin_rate: float = 0.10

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
            "margin_rate": self.margin_rate,
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


def _build_contract_spec(symbol: str, meta: Dict[str, Any], source_name: str) -> FuturesContractSpec:
    if not isinstance(meta, dict):
        raise ValueError(f"Invalid contract spec entry for symbol: {symbol}")

    key = str(symbol or meta.get("symbol") or "").strip().upper()
    if not key:
        raise ValueError(f"Missing symbol in {source_name} contract catalog entry")

    asset_class_raw = str(meta.get("asset_class") or "").strip()
    if not asset_class_raw:
        raise ValueError(f"Missing asset_class in {source_name} for symbol: {key}")
    if asset_class_raw not in _VALID_ASSET_CLASSES:
        raise ValueError(
            f"Invalid asset_class '{asset_class_raw}' in {source_name} for symbol: {key}"
        )

    data_symbol_raw = (
        meta.get("data_symbol")
        if "data_symbol" in meta
        else meta.get("fmp_symbol")
    )
    data_symbol = str(data_symbol_raw).strip().upper() if data_symbol_raw else None

    return FuturesContractSpec(
        symbol=key,
        multiplier=float(meta["multiplier"]),
        tick_size=float(meta["tick_size"]),
        currency=str(meta["currency"]),
        exchange=str(meta["exchange"]),
        asset_class=cast(FuturesAssetClass, asset_class_raw),
        data_symbol=data_symbol,
        margin_rate=float(meta.get("margin_rate", 0.10)),
    )


def _parse_catalog(catalog: Dict[str, Any]) -> Dict[str, FuturesContractSpec]:
    """Parse YAML contract catalog into dataclass instances."""
    specs: Dict[str, FuturesContractSpec] = {}
    for symbol, meta in catalog.items():
        if not isinstance(meta, dict):
            raise ValueError(f"Invalid contracts catalog entry for symbol: {symbol}")

        spec = _build_contract_spec(str(symbol), meta, "contracts.yaml")
        specs[spec.symbol] = spec

    return specs


def _rows_to_specs(rows: Dict[str, Dict[str, Any]]) -> Dict[str, FuturesContractSpec]:
    """Convert DB rows into futures contract specs."""
    specs: Dict[str, FuturesContractSpec] = {}
    for symbol, row in rows.items():
        spec = _build_contract_spec(str(symbol), dict(row), "database")
        specs[spec.symbol] = spec
    return specs


@lru_cache(maxsize=1)
def load_contract_specs() -> Dict[str, FuturesContractSpec]:
    """Load contract specs from DB first, then fall back to YAML."""
    try:
        import logging

        from database import get_db_session
        from inputs.database_client import DatabaseClient

        with get_db_session() as conn:
            db_client = DatabaseClient(conn)
            rows = db_client.get_futures_contracts()
        if rows:
            return _rows_to_specs(rows)
    except Exception as e:
        logging.getLogger(__name__).warning("futures contracts DB read failed: %s", e)

    catalog = _load_contracts_yaml()
    return _parse_catalog(catalog)


def get_contract_spec(symbol: str) -> Optional[FuturesContractSpec]:
    """Look up a single contract spec by IBKR root symbol."""
    specs = load_contract_specs()
    return specs.get(str(symbol or "").strip().upper())
