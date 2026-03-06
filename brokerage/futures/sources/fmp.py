from __future__ import annotations

from typing import Optional

import pandas as pd

from portfolio_risk_engine._ticker import fetch_fmp_quote_with_currency, normalize_fmp_price
from portfolio_risk_engine.data_loader import fetch_monthly_close


class FMPFuturesPriceSource:
    """FMP commodity/index symbols as a futures price source."""

    @property
    def name(self) -> str:
        return "FMP"

    def fetch_latest_price(self, symbol: str, alt_symbol: Optional[str] = None) -> Optional[float]:
        del symbol
        # Never fall back to raw ticker to avoid equity collisions (e.g., Z).
        if not alt_symbol:
            return None

        prices = fetch_monthly_close(alt_symbol, fmp_ticker=alt_symbol)
        if prices is None or prices.empty or prices.dropna().empty:
            return None

        raw_price = float(prices.dropna().iloc[-1])
        _, fmp_currency = fetch_fmp_quote_with_currency(alt_symbol)
        normalized_price, _ = normalize_fmp_price(raw_price, fmp_currency)
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

        prices = fetch_monthly_close(
            alt_symbol,
            start_date=start_date,
            end_date=end_date,
            fmp_ticker=alt_symbol,
        )
        if prices is None or prices.empty:
            return None

        # No minor-currency normalization needed for returns series: scaling cancels.
        return prices


__all__ = ["FMPFuturesPriceSource"]
