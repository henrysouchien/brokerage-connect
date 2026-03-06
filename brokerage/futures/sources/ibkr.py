from __future__ import annotations

from typing import Optional

import pandas as pd


class IBKRFuturesPriceSource:
    """IBKR historical futures prices as a source adapter."""

    @property
    def name(self) -> str:
        return "IBKR"

    def fetch_latest_price(self, symbol: str, alt_symbol: Optional[str] = None) -> Optional[float]:
        del alt_symbol
        from ibkr.compat import fetch_ibkr_monthly_close

        prices = fetch_ibkr_monthly_close(symbol, "2020-01-01", "2099-12-31")
        if prices is None or prices.empty or prices.dropna().empty:
            return None
        return float(prices.dropna().iloc[-1])

    def fetch_monthly_close(
        self,
        symbol: str,
        start_date: str,
        end_date: str,
        alt_symbol: Optional[str] = None,
    ) -> Optional[pd.Series]:
        del alt_symbol
        from ibkr.compat import fetch_ibkr_monthly_close

        prices = fetch_ibkr_monthly_close(symbol, start_date, end_date)
        if prices is None or prices.empty:
            return None
        return prices


__all__ = ["IBKRFuturesPriceSource"]
