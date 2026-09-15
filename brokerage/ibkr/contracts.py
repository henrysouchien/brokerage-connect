"""IBKR contract resolution helpers."""

from __future__ import annotations

import math
import re
from functools import lru_cache
from pathlib import Path
from typing import Any

import yaml

from ._types import InstrumentType, coerce_instrument_type

from .exceptions import IBKRContractError

Contract = Any

_FX_PAIR_RE = re.compile(r"^[A-Z]{3}[./]?[A-Z]{3}$")
_CONTRACT_MONTH_RE = re.compile(r"^\d{6}(\d{2})?$")
_CURRENCY_RE = re.compile(r"^[A-Z]{3}$")


def _require_currency_code(value: Any, *, context: str) -> str:
    currency = str(value or "").strip().upper()
    if _CURRENCY_RE.fullmatch(currency) is None:
        raise IBKRContractError(f"{context} requires an explicit ISO currency code")
    return currency


@lru_cache(maxsize=1)
def _load_ibkr_exchange_mappings() -> dict[str, Any]:
    path = Path(__file__).resolve().with_name("exchange_mappings.yaml")
    try:
        with path.open("r", encoding="utf-8") as f:
            payload = yaml.safe_load(f) or {}
    except FileNotFoundError as exc:
        raise IBKRContractError(f"IBKR exchange_mappings.yaml not found at {path}") from exc
    except Exception as exc:
        raise IBKRContractError(f"Failed to load IBKR exchange mappings from {path}: {exc}") from exc
    if not isinstance(payload, dict) or not payload:
        raise IBKRContractError(f"IBKR exchange mappings must be a non-empty mapping at {path}")
    return payload


def _futures_exchange_meta(symbol: str) -> tuple[str, str]:
    sym = str(symbol or "").strip().upper()
    if not sym:
        raise IBKRContractError("Missing futures symbol")

    mappings = _load_ibkr_exchange_mappings().get("ibkr_futures_exchanges", {})
    raw = mappings.get(sym)
    if not isinstance(raw, dict):
        raise IBKRContractError(f"No IBKR futures exchange mapping configured for '{sym}'")

    exchange = str(raw.get("exchange") or "").strip()
    currency = _require_currency_code(
        raw.get("currency"),
        context=f"IBKR futures mapping for {sym!r}",
    )
    if not exchange:
        raise IBKRContractError(f"Invalid IBKR futures exchange mapping for '{sym}'")
    return exchange, currency


def resolve_futures_contract(symbol: str, contract_month: str | None = None) -> Contract:
    """Resolve a futures root symbol into an IBKR continuous futures contract."""
    try:
        from ib_async import ContFuture, Future
    except ImportError:
        from ib_async import ContFuture

        Future = None

    sym = str(symbol or "").strip().upper()
    exchange, currency = _futures_exchange_meta(sym)
    if contract_month is None:
        return ContFuture(symbol=sym, exchange=exchange, currency=currency)

    if Future is None:
        from ib_async import Future

    cm = str(contract_month).strip()
    if not _CONTRACT_MONTH_RE.match(cm):
        raise IBKRContractError(
            f"Invalid futures contract_month '{contract_month}'; expected YYYYMM or YYYYMMDD"
        )
    return Future(
        symbol=sym,
        lastTradeDateOrContractMonth=cm,
        exchange=exchange,
        currency=currency,
    )


def _normalize_fx_pair(symbol: str) -> str:
    raw = str(symbol or "").strip().upper()
    if not raw:
        raise IBKRContractError("Missing FX symbol")

    # Accept canonical forms like GBP.HKD, GBP/HKD, or GBPHKD.
    if not _FX_PAIR_RE.match(raw) and len(re.sub(r"[^A-Z]", "", raw)) != 6:
        raise IBKRContractError(f"Invalid FX symbol '{symbol}'")

    pair = re.sub(r"[^A-Z]", "", raw)
    if len(pair) != 6:
        raise IBKRContractError(f"Invalid FX symbol '{symbol}'")
    return pair


def resolve_fx_contract(symbol: str) -> Contract:
    """Resolve an FX pair symbol into an IBKR Forex contract."""
    from ib_async import Forex

    pair = _normalize_fx_pair(symbol)
    return Forex(pair=pair)


def _coerce_con_id(contract_identity: dict[str, Any] | None) -> int | None:
    if not isinstance(contract_identity, dict):
        return None
    con_id = contract_identity.get("con_id")
    if con_id in (None, ""):
        return None
    # Reject non-finite or non-integer float values (e.g., NaN, inf, 123.9)
    if isinstance(con_id, float):
        if math.isnan(con_id) or math.isinf(con_id):
            raise IBKRContractError(f"Invalid con_id '{con_id}' in contract_identity") from None
        if con_id != int(con_id):
            raise IBKRContractError(f"Invalid con_id '{con_id}' in contract_identity (non-integer float)") from None
    try:
        return int(con_id)
    except (TypeError, ValueError, OverflowError):
        raise IBKRContractError(f"Invalid con_id '{con_id}' in contract_identity") from None


def resolve_bond_contract(
    symbol: str,
    contract_identity: dict[str, Any] | None = None,
) -> Contract:
    """Resolve a bond contract. Tries con_id first, then CUSIP."""
    del symbol
    try:
        con_id = _coerce_con_id(contract_identity)
    except IBKRContractError:
        con_id = None  # allow CUSIP fallback when con_id is invalid

    if con_id is not None:
        try:
            from ib_async import Bond

            return Bond(conId=con_id)
        except Exception:
            from ib_async import Contract

            return Contract(conId=con_id, secType="BOND")

    identity = contract_identity if isinstance(contract_identity, dict) else {}
    cusip = identity.get("cusip")
    if isinstance(cusip, str) and cusip.strip():
        from ib_async import Bond

        bond = Bond()
        bond.secIdType = "CUSIP"
        bond.secId = cusip.strip()
        bond.currency = _require_currency_code(
            identity.get("currency"),
            context="IBKR bond CUSIP identity",
        )
        return bond

    isin = identity.get("isin")
    if isinstance(isin, str) and isin.strip():
        from ib_async import Bond

        bond = Bond()
        bond.secIdType = "ISIN"
        bond.secId = isin.strip()
        bond.currency = _require_currency_code(
            identity.get("currency"),
            context="IBKR bond ISIN identity",
        )
        return bond

    raise IBKRContractError(
        "Bond pricing requires contract_identity with con_id, cusip, or isin"
    )


def resolve_option_contract(symbol: str, contract_identity: dict[str, Any] | None = None) -> Contract:
    """Resolve an option contract from conId or full contract identity."""
    identity = contract_identity if isinstance(contract_identity, dict) else {}
    con_id = _coerce_con_id(identity)

    from ib_async import Contract, Option

    if con_id is not None:
        try:
            return Option(conId=con_id)
        except Exception:
            return Contract(conId=con_id, secType="OPT")

    expiry = identity.get("expiry")
    strike = identity.get("strike")
    right_raw = str(identity.get("right") or "").strip().upper()
    right = {"C": "C", "CALL": "C", "P": "P", "PUT": "P"}.get(right_raw, "")
    if not expiry or strike in (None, "") or right not in {"C", "P"}:
        raise IBKRContractError(
            "Option pricing requires contract_identity with either con_id or (expiry, strike, right)"
        )

    underlying = str(
        identity.get("underlying")
        or identity.get("underlying_symbol")
    ).strip().upper()
    if not underlying:
        raise IBKRContractError("Option pricing requires an underlying symbol in contract_identity")

    exchange = str(identity.get("exchange") or "").strip().upper()
    if not exchange:
        raise IBKRContractError("Option pricing requires exchange in contract_identity")
    kwargs: dict[str, Any] = {
        "symbol": underlying,
        "lastTradeDateOrContractMonth": str(expiry),
        "strike": float(strike),
        "right": right,
        "exchange": exchange,
        "currency": _require_currency_code(
            identity.get("currency"),
            context="IBKR option identity",
        ),
    }
    multiplier = identity.get("multiplier")
    if multiplier not in (None, ""):
        kwargs["multiplier"] = str(multiplier)
    return Option(**kwargs)


def resolve_contract(
    symbol: str,
    instrument_type: str | InstrumentType,
    contract_identity: dict[str, Any] | None = None,
) -> Contract:
    """Resolve an IBKR contract for the requested instrument type."""
    raw_instrument_type = str(instrument_type or "").strip().lower()
    if raw_instrument_type in {"fx", "forex"}:
        return resolve_fx_contract(symbol)

    normalized = coerce_instrument_type(instrument_type)
    if normalized == "futures":
        month = (contract_identity or {}).get("contract_month")
        return resolve_futures_contract(symbol, contract_month=month)
    if normalized == "bond":
        return resolve_bond_contract(symbol, contract_identity=contract_identity)
    if normalized == "option":
        return resolve_option_contract(symbol, contract_identity=contract_identity)
    if normalized in {"equity", "etf", "fund", "cash", "warrant", "derivative"}:
        raise IBKRContractError(
            f"IBKR resolver not yet implemented for instrument type '{normalized}'"
        )
    raise IBKRContractError(f"Unsupported instrument type '{instrument_type}'")
