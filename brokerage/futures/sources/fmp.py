from __future__ import annotations

from collections.abc import Callable
from typing import Optional

import pandas as pd



class FMPFuturesPriceSource:
    """FMP commodity/index prices with explicitly supplied fetch/currency behavior."""

    def __init__(
        self,
        *,
        fetch_monthly_close: Callable[..., Optional[pd.Series]],
        infer_currency: Callable[[str], Optional[str]],
        normalize_minor_currency_price: Callable[
            [Optional[float], Optional[str]], tuple[Optional[float], str]
        ],
    ) -> None:
        self._fetch_monthly_close = fetch_monthly_close
        self._infer_currency = infer_currency
        self._normalize_minor_currency_price = normalize_minor_currency_price

    @property
    def name(self) -> str:
        return "FMP"

    def fetch_latest_price(self, symbol: str, alt_symbol: Optional[str] = None) -> Optional[float]:
        del symbol
        # Never fall back to raw ticker to avoid equity collisions (e.g., Z).
        if not alt_symbol:
            return None

        prices = self._fetch_monthly_close(alt_symbol)
        if prices is None or prices.empty or prices.dropna().empty:
            return None

        raw_price = float(prices.dropna().iloc[-1])
        fmp_currency = self._infer_currency(alt_symbol)
        normalized_price, _ = self._normalize_minor_currency_price(raw_price, fmp_currency)
        if normalized_price is None:
            return raw_price
        return float(normalized_price)

    def fetch_monthly_close(
        self,
        symbol: str,
        start_date: str,
        end_date: str,
        alt_symbol: Optional[str] = None,
    ) -> Optional[pd.Series]:
        del symbol
        # Never fall back to raw ticker to avoid equity collisions (e.g., Z).
        if not alt_symbol:
            return None

        prices = self._fetch_monthly_close(
            alt_symbol,
            start_date=start_date,
            end_date=end_date,
        )
        if prices is None or prices.empty:
            return None

        # No minor-currency normalization needed for returns series: scaling cancels.
        return prices


__all__ = ["FMPFuturesPriceSource"]
