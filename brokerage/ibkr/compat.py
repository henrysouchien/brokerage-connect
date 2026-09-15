"""Public interface for IBKR provider.

Most external code should import from this module.

Agent orientation:
    Compatibility boundary for callers that should not depend on IBKR internals.
    This module delegates to ``ibkr.market_data`` and ``ibkr.flex`` while
    normalizing failure behavior for higher layers.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping
from datetime import datetime
from functools import lru_cache
import math
import os
from pathlib import Path
from typing import Any, Union

import pandas as pd
import yaml

from brokerage.futures import FuturesContractSpec

from .exceptions import (
    IBKRAccountError,
    IBKRConnectionError,
    IBKRContractError,
    IBKRDataError,
    IBKREntitlementError,
    IBKRNoDataError,
    IBKRTimeoutError,
)
from ._logging import logger
from .config import IBKR_FUTURES_CURVE_TIMEOUT

IBKRMarketDataClient = None


def _ibkr_fx_daily_enabled():
    return os.getenv("IBKR_FX_DAILY_ENABLED", "0").strip() == "1"


def _ibkr_bond_daily_enabled():
    return os.getenv("IBKR_BOND_DAILY_ENABLED", "0").strip() == "1"


def _ibkr_ts_cache_enabled():
    return os.getenv("IBKR_TIMESERIES_CACHE_ENABLED", "1").strip() == "1"


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


def get_ibkr_futures_fmp_map(
    *,
    spec_loader: Callable[[], Mapping[str, FuturesContractSpec]] | None = None,
) -> dict[str, str]:
    """Return IBKR-routable price aliases; default to the bundled catalog."""
    from brokerage.futures import load_contract_specs

    ibkr_routing = get_ibkr_futures_exchanges()
    all_specs = (spec_loader if spec_loader is not None else load_contract_specs)()

    out: dict[str, str] = {}
    for symbol, spec in all_specs.items():
        if symbol not in ibkr_routing:
            continue
        mapped = str(spec.data_symbol or "").strip().upper()
        if mapped:
            out[symbol] = mapped
    return out


def get_ibkr_futures_exchanges() -> dict[str, dict[str, str]]:
    """Load IBKR futures-root exchange metadata."""
    raw_map = _load_ibkr_exchange_mappings().get("ibkr_futures_exchanges")
    if not isinstance(raw_map, dict) or not raw_map:
        raise IBKRContractError("IBKR futures exchange mappings are not configured")
    out: dict[str, dict[str, str]] = {}
    for symbol, meta in raw_map.items():
        if not isinstance(meta, dict):
            continue
        key = str(symbol or "").strip().upper()
        exchange = str(meta.get("exchange") or "").strip().upper()
        currency = str(meta.get("currency") or "").strip().upper()
        if key and exchange and len(currency) == 3 and currency.isalpha():
            out[key] = {"exchange": exchange, "currency": currency}
    if not out:
        raise IBKRContractError("IBKR futures exchange mappings contain no valid rows")
    return out


def get_ibkr_futures_contract_meta(
    *,
    spec_loader: Callable[[], Mapping[str, FuturesContractSpec]] | None = None,
) -> dict[str, dict[str, Any]]:
    """Return IBKR-routable metadata from the supplied or bundled catalog."""
    from brokerage.futures import load_contract_specs

    ibkr_routing = get_ibkr_futures_exchanges()
    all_specs = (spec_loader if spec_loader is not None else load_contract_specs)()

    out: dict[str, dict[str, Any]] = {}
    for symbol, spec in all_specs.items():
        if symbol not in ibkr_routing:
            continue

        identity = spec.to_contract_identity()
        # IBKR routing values are authoritative inside IBKR contexts.
        identity["exchange"] = ibkr_routing[symbol]["exchange"]
        identity["currency"] = ibkr_routing[symbol]["currency"]
        out[symbol] = identity
    return out


def get_futures_currency(
    symbol: str,
    *,
    spec_loader: Callable[[], Mapping[str, FuturesContractSpec]] | None = None,
) -> str:
    """Return settlement currency from the supplied or bundled catalog."""
    key = str(symbol or "").strip().upper()
    if not key:
        raise IBKRContractError("Futures symbol is required")
    from brokerage.futures import load_contract_specs

    specs = (spec_loader if spec_loader is not None else load_contract_specs)()
    spec = specs.get(key)
    if spec is None:
        raise IBKRContractError(
            f"Futures contract metadata is required for {key}"
        )
    return spec.currency


def get_futures_months(symbol: str) -> list[dict[str, Any]]:
    """Discover available contract months for a futures root symbol."""
    from .client import IBKRClient

    client = IBKRClient()
    return client.get_futures_months(symbol)


def get_futures_curve_snapshot(
    symbol: str,
    timeout: float = IBKR_FUTURES_CURVE_TIMEOUT,
) -> list[dict[str, Any]]:
    """Snapshot prices for all active contract months of a futures root symbol."""
    from .client import IBKRClient

    client = IBKRClient()
    return client.get_futures_curve_snapshot(symbol, timeout=timeout)


def fetch_ibkr_monthly_close(
    symbol: str,
    start_date: Union[str, datetime],
    end_date: Union[str, datetime],
) -> pd.Series:
    """Fetch month-end futures close series through the IBKR market data client.

    Upstream references:
    - IBKR historical bars: https://interactivebrokers.github.io/tws-api/historical_bars.html
    """
    try:
        client_cls = IBKRMarketDataClient
        if client_cls is None:
            from .market_data import IBKRMarketDataClient as _IBKRMarketDataClient

            client_cls = _IBKRMarketDataClient
        client = client_cls()
        return client.fetch_monthly_close_futures(symbol, start_date, end_date)
    except Exception as exc:
        logger.warning("IBKR futures fetch failed for %s: %s", symbol, exc)
        return pd.Series(dtype=float)


def _raw_daily_futures(
    symbol: str,
    start_date: Union[str, datetime],
    end_date: Union[str, datetime],
) -> pd.Series:
    """Raw IBKR daily futures fetch (fail-open, existing contract)."""
    try:
        client_cls = IBKRMarketDataClient
        if client_cls is None:
            from .market_data import IBKRMarketDataClient as _IBKRMarketDataClient

            client_cls = _IBKRMarketDataClient
        client = client_cls()
        return client.fetch_daily_close_futures(symbol, start_date, end_date)
    except Exception as exc:
        logger.warning("IBKR daily futures fetch failed for %s: %s", symbol, exc)
        return pd.Series(dtype=float)


def _raw_daily_futures_for_cache(
    symbol: str,
    start_date: Union[str, datetime],
    end_date: Union[str, datetime],
) -> pd.Series:
    """Raw fetch for cache loader with transient failures propagated."""
    client_cls = IBKRMarketDataClient
    if client_cls is None:
        from .market_data import IBKRMarketDataClient as _IBKRMarketDataClient

        client_cls = _IBKRMarketDataClient
    client = client_cls()
    return client.fetch_daily_close_futures(
        symbol,
        start_date,
        end_date,
        raise_on_transient=True,
    )


def fetch_ibkr_daily_close_futures(
    symbol: str,
    start_date: Union[str, datetime],
    end_date: Union[str, datetime],
) -> pd.Series:
    """Public wrapper for daily futures close with optional cache."""
    if _ibkr_ts_cache_enabled():
        try:
            from .timeseries_cache import cached_daily_fetch

            return cached_daily_fetch(
                symbol,
                start_date,
                end_date,
                instrument_type="futures",
                raw_fetcher=_raw_daily_futures_for_cache,
            )
        except Exception as exc:
            logger.warning("IBKR cache-backed daily futures failed for %s, falling back: %s", symbol, exc)
            return _raw_daily_futures(symbol, start_date, end_date)
    return _raw_daily_futures(symbol, start_date, end_date)


def fetch_ibkr_fx_monthly_close(
    symbol: str,
    start_date: Union[str, datetime],
    end_date: Union[str, datetime],
) -> pd.Series:
    """Fetch month-end FX close series through the IBKR market data client."""
    try:
        client_cls = IBKRMarketDataClient
        if client_cls is None:
            from .market_data import IBKRMarketDataClient as _IBKRMarketDataClient

            client_cls = _IBKRMarketDataClient
        client = client_cls()
        return client.fetch_monthly_close_fx(symbol, start_date, end_date)
    except Exception as exc:
        logger.warning("IBKR FX fetch failed for %s: %s", symbol, exc)
        return pd.Series(dtype=float)


def _raw_daily_fx(
    symbol: str,
    start_date: Union[str, datetime],
    end_date: Union[str, datetime],
) -> pd.Series:
    """Raw IBKR daily FX fetch (fail-open, existing contract)."""
    try:
        client_cls = IBKRMarketDataClient
        if client_cls is None:
            from .market_data import IBKRMarketDataClient as _IBKRMarketDataClient

            client_cls = _IBKRMarketDataClient
        client = client_cls()
        return client.fetch_daily_close_fx(symbol, start_date, end_date)
    except Exception as exc:
        logger.warning("IBKR daily FX fetch failed for %s: %s", symbol, exc)
        return pd.Series(dtype=float)


def _raw_daily_fx_for_cache(
    symbol: str,
    start_date: Union[str, datetime],
    end_date: Union[str, datetime],
) -> pd.Series:
    """Raw IBKR daily FX fetch with transient failures propagated."""
    client_cls = IBKRMarketDataClient
    if client_cls is None:
        from .market_data import IBKRMarketDataClient as _IBKRMarketDataClient

        client_cls = _IBKRMarketDataClient
    client = client_cls()
    return client.fetch_daily_close_fx(
        symbol,
        start_date,
        end_date,
        raise_on_transient=True,
    )


def fetch_ibkr_daily_close_fx(
    symbol: str,
    start_date: Union[str, datetime],
    end_date: Union[str, datetime],
) -> pd.Series:
    """Fetch daily FX close series through the IBKR market data client."""
    if not _ibkr_fx_daily_enabled():
        return pd.Series(dtype=float)
    if _ibkr_ts_cache_enabled():
        try:
            from .timeseries_cache import cached_daily_fetch

            return cached_daily_fetch(
                symbol,
                start_date,
                end_date,
                instrument_type="fx",
                raw_fetcher=_raw_daily_fx_for_cache,
            )
        except Exception as exc:
            logger.warning("IBKR cache-backed daily FX failed for %s, falling back: %s", symbol, exc)
            return _raw_daily_fx(symbol, start_date, end_date)
    return _raw_daily_fx(symbol, start_date, end_date)


def fetch_ibkr_bond_monthly_close(
    symbol: str,
    start_date: Union[str, datetime],
    end_date: Union[str, datetime],
    contract_identity: dict | None = None,
) -> pd.Series:
    """Fetch month-end bond close series through the IBKR market data client."""
    try:
        client_cls = IBKRMarketDataClient
        if client_cls is None:
            from .market_data import IBKRMarketDataClient as _IBKRMarketDataClient

            client_cls = _IBKRMarketDataClient
        client = client_cls()
        return client.fetch_monthly_close_bond(
            symbol,
            start_date,
            end_date,
            contract_identity=contract_identity,
        )
    except Exception as exc:
        logger.warning("IBKR bond fetch failed for %s: %s", symbol, exc)
        return pd.Series(dtype=float)


def _raw_daily_bond(
    symbol: str,
    start_date: Union[str, datetime],
    end_date: Union[str, datetime],
    contract_identity: dict | None = None,
) -> pd.Series:
    """Raw IBKR daily bond fetch (fail-open, existing contract)."""
    try:
        client_cls = IBKRMarketDataClient
        if client_cls is None:
            from .market_data import IBKRMarketDataClient as _IBKRMarketDataClient

            client_cls = _IBKRMarketDataClient
        client = client_cls()
        return client.fetch_daily_close_bond(
            symbol,
            start_date,
            end_date,
            contract_identity=contract_identity,
        )
    except Exception as exc:
        logger.warning("IBKR daily bond fetch failed for %s: %s", symbol, exc)
        return pd.Series(dtype=float)


def _raw_daily_bond_for_cache(
    symbol: str,
    start_date: Union[str, datetime],
    end_date: Union[str, datetime],
    contract_identity: dict | None = None,
) -> pd.Series:
    """Raw IBKR daily bond fetch with transient failures propagated."""
    client_cls = IBKRMarketDataClient
    if client_cls is None:
        from .market_data import IBKRMarketDataClient as _IBKRMarketDataClient

        client_cls = _IBKRMarketDataClient
    client = client_cls()
    return client.fetch_daily_close_bond(
        symbol,
        start_date,
        end_date,
        contract_identity=contract_identity,
        raise_on_transient=True,
    )


def fetch_ibkr_daily_close_bond(
    symbol: str,
    start_date: Union[str, datetime],
    end_date: Union[str, datetime],
    contract_identity: dict | None = None,
) -> pd.Series:
    """Fetch daily bond close series through the IBKR market data client."""
    if not _ibkr_bond_daily_enabled():
        return pd.Series(dtype=float)
    if _ibkr_ts_cache_enabled():
        try:
            from .timeseries_cache import cached_daily_fetch

            return cached_daily_fetch(
                symbol,
                start_date,
                end_date,
                instrument_type="bond",
                raw_fetcher=_raw_daily_bond_for_cache,
                contract_identity=contract_identity,
            )
        except Exception as exc:
            logger.warning("IBKR cache-backed daily bond failed for %s, falling back: %s", symbol, exc)
            return _raw_daily_bond(symbol, start_date, end_date, contract_identity=contract_identity)
    return _raw_daily_bond(symbol, start_date, end_date, contract_identity=contract_identity)


def fetch_ibkr_option_monthly_mark(
    symbol: str,
    start_date: Union[str, datetime],
    end_date: Union[str, datetime],
    contract_identity: dict | None = None,
    raise_on_failure: bool = False,
) -> pd.Series:
    """Fetch month-end option marks through the IBKR market data client."""
    try:
        client_cls = IBKRMarketDataClient
        if client_cls is None:
            from .market_data import IBKRMarketDataClient as _IBKRMarketDataClient

            client_cls = _IBKRMarketDataClient
        client = client_cls()
        return client.fetch_monthly_close_option(
            symbol,
            start_date,
            end_date,
            contract_identity=contract_identity,
            raise_on_failure=raise_on_failure,
        )
    except Exception as exc:
        if raise_on_failure:
            raise
        logger.warning("IBKR option fetch failed for %s: %s", symbol, exc)
        return pd.Series(dtype=float)


def _clean_option_mark(value: Any) -> float | None:
    try:
        mark = float(value)
    except (TypeError, ValueError):
        return None
    if not math.isfinite(mark) or mark < 0:
        return None
    return mark


def _select_option_snapshot_mark(snapshot: dict[str, Any]) -> float | None:
    """Select a usable per-share option mark from an IBKR snapshot row."""

    for field in ("mid", "last", "close", "model_price", "market_price", "theoretical_price"):
        mark = _clean_option_mark(snapshot.get(field))
        if mark is not None:
            return mark
    return None


def _build_option_contract_spec(symbol: str, contract_identity: dict[str, Any] | None):
    from .contract_spec import IBKRContractSpec

    identity = contract_identity if isinstance(contract_identity, dict) else {}
    con_id = identity.get("con_id") or identity.get("conId")
    underlying = str(
        identity.get("underlying")
        or identity.get("underlying_symbol")
        or identity.get("symbol")
    ).strip().upper()
    currency = str(identity.get("currency") or "").strip().upper()
    if not currency:
        raise ValueError(
            "Option snapshot pricing requires contract_identity.currency"
        )

    if con_id not in (None, ""):
        return IBKRContractSpec.option_by_con_id(
            underlying or str(symbol or "").strip().upper(),
            con_id=int(con_id),
            exchange=str(identity.get("exchange") or "SMART"),
            currency=currency,
        )

    expiry = identity.get("expiry")
    strike = identity.get("strike")
    right = str(identity.get("right") or "").strip().upper()[:1]
    if not underlying or not expiry or strike in (None, "") or right not in {"C", "P"}:
        raise ValueError(
            "Option snapshot pricing requires contract_identity with either con_id "
            "or (underlying, expiry, strike, right)"
        )

    return IBKRContractSpec.option(
        underlying,
        expiry=str(expiry),
        strike=float(strike),
        right=right,
        exchange=str(identity.get("exchange") or "SMART"),
        currency=currency,
        multiplier=identity.get("multiplier"),
    )


def fetch_ibkr_option_snapshot_mark(
    symbol: str,
    *,
    contract_identity: dict[str, Any] | None = None,
) -> float | None:
    """Fetch a current per-share option mark through the IBKR market data boundary."""

    client_cls = IBKRMarketDataClient
    if client_cls is None:
        from .market_data import IBKRMarketDataClient as _IBKRMarketDataClient

        client_cls = _IBKRMarketDataClient
    spec = _build_option_contract_spec(symbol, contract_identity)
    rows = client_cls().fetch_snapshot([spec])
    row = rows[0] if rows else None
    if not isinstance(row, dict):
        raise ValueError(f"IBKR option snapshot returned no row for {symbol}")
    if row.get("error"):
        raise ValueError(f"IBKR option snapshot failed for {symbol}: {row['error']}")

    mark = _select_option_snapshot_mark(row)
    if mark is None:
        raise ValueError(f"IBKR option snapshot returned no usable mark for {symbol}")
    return mark


def fetch_ibkr_flex_trades(
    token: str = "",
    query_id: str = "",
    path: str | None = None,
):
    """Fetch and normalize IBKR Flex trades.

    Upstream reference:
    - IBKR Flex Web Service: https://www.interactivebrokers.com/en/software/am-api/am/flex-web-service.htm
    """
    from .flex import fetch_ibkr_flex_trades as _fetch_ibkr_flex_trades

    return _fetch_ibkr_flex_trades(token=token, query_id=query_id, path=path)


def fetch_ibkr_flex_payload(
    token: str = "",
    query_id: str = "",
    path: str | None = None,
) -> dict[str, Any]:
    """Fetch IBKR Flex trades/cash rows with fetch diagnostics."""
    from .flex import fetch_ibkr_flex_payload as _fetch_ibkr_flex_payload

    return _fetch_ibkr_flex_payload(token=token, query_id=query_id, path=path)


def __getattr__(name: str):
    if name == "IBKRClient":
        from .client import IBKRClient as _IBKRClient

        return _IBKRClient
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


__all__ = [
    "IBKRClient",
    "fetch_ibkr_monthly_close",
    "fetch_ibkr_daily_close_futures",
    "fetch_ibkr_daily_close_fx",
    "fetch_ibkr_daily_close_bond",
    "fetch_ibkr_fx_monthly_close",
    "fetch_ibkr_bond_monthly_close",
    "fetch_ibkr_option_monthly_mark",
    "fetch_ibkr_option_snapshot_mark",
    "fetch_ibkr_flex_trades",
    "fetch_ibkr_flex_payload",
    "get_ibkr_futures_fmp_map",
    "get_ibkr_futures_exchanges",
    "get_ibkr_futures_contract_meta",
    "get_futures_currency",
    "get_futures_months",
    "get_futures_curve_snapshot",
    "IBKRDataError",
    "IBKRConnectionError",
    "IBKRContractError",
    "IBKRNoDataError",
    "IBKREntitlementError",
    "IBKRAccountError",
    "IBKRTimeoutError",
]
