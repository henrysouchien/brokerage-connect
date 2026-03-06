from __future__ import annotations

from typing import List, Optional, Protocol

import pandas as pd


class FuturesPriceSource(Protocol):
    """Protocol for futures price data sources."""

    @property
    def name(self) -> str:
        """Human-readable source name for logging."""
        ...

    def fetch_latest_price(self, symbol: str, alt_symbol: Optional[str] = None) -> Optional[float]:
        """Fetch latest price. Return None when unavailable."""
        ...

    def fetch_monthly_close(
        self,
        symbol: str,
        start_date: str,
        end_date: str,
        alt_symbol: Optional[str] = None,
    ) -> Optional[pd.Series]:
        """Fetch month-end close prices. Return None when unavailable."""
        ...


class FuturesPricingChain:
    """Ordered futures pricing chain that falls back across sources."""

    def __init__(self, sources: Optional[List[FuturesPriceSource]] = None):
        self._sources: List[FuturesPriceSource] = list(sources or [])

    def add_source(self, source: FuturesPriceSource) -> None:
        self._sources.append(source)

    def fetch_latest_price(self, symbol: str, alt_symbol: Optional[str] = None) -> float:
        """Try all sources in order and return the first non-empty latest price."""
        for source in self._sources:
            try:
                price = source.fetch_latest_price(symbol, alt_symbol=alt_symbol)
                if price is not None:
                    return float(price)
            except Exception:
                continue
        raise ValueError(f"No price available for futures ticker {symbol}")

    def fetch_monthly_close(
        self,
        symbol: str,
        start_date: str,
        end_date: str,
        alt_symbol: Optional[str] = None,
    ) -> pd.Series:
        """Try all sources in order and return the first non-empty monthly close series."""
        for source in self._sources:
            try:
                prices = source.fetch_monthly_close(
                    symbol,
                    start_date,
                    end_date,
                    alt_symbol=alt_symbol,
                )
                if prices is not None and not prices.empty:
                    return prices
            except Exception:
                continue
        raise ValueError(f"No price data for futures ticker {symbol}")


def get_default_pricing_chain() -> FuturesPricingChain:
    """Build the default futures pricing chain (FMP first, IBKR fallback)."""
    from brokerage.futures.sources.fmp import FMPFuturesPriceSource
    from brokerage.futures.sources.ibkr import IBKRFuturesPriceSource

    chain = FuturesPricingChain()
    chain.add_source(FMPFuturesPriceSource())
    chain.add_source(IBKRFuturesPriceSource())
    return chain


__all__ = [
    "FuturesPriceSource",
    "FuturesPricingChain",
    "get_default_pricing_chain",
]
