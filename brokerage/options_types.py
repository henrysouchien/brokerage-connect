"""Minimal option-leg/strategy dataclasses for the brokerage adapter.

Field shapes mirror portfolio_math.options exactly so monorepo callers can pass
portfolio_math instances interchangeably (Python's duck-typing on dataclass
fields). External standalone users construct brokerage.options_types instances.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from typing import Any, Literal

_POSITION_VALUES = {"long", "short"}
_OPTION_TYPE_VALUES = {"call", "put", "stock"}


def _parse_expiration(value: Any) -> date | None:
    """Accept date, datetime, ISO-8601 string, YYYYMMDD string, or any
    str()-coercible value. Body copied exactly from
    portfolio_math/options.py:363-375 to preserve duck-typed
    interchangeability between brokerage.options_types.OptionLeg and
    portfolio_math.options.OptionLeg in monorepo callers. Drift discipline
    enforced by parity tests (see Q-A).
    """
    if value in (None, ""):
        return None
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value

    text = str(value).strip()
    if not text:
        return None
    if len(text) == 8 and text.isdigit():
        return datetime.strptime(text, "%Y%m%d").date()
    return datetime.fromisoformat(text).date()


@dataclass
class OptionLeg:
    position: Literal["long", "short"]
    option_type: Literal["call", "put", "stock"]
    premium: float
    strike: float | None = None
    size: float = 1.0
    multiplier: float | None = None
    expiration: date | None = None
    label: str | None = None
    con_id: int | None = None

    def __post_init__(self) -> None:
        self.position = str(self.position).strip().lower()
        self.option_type = str(self.option_type).strip().lower()

        if self.position not in _POSITION_VALUES:
            raise ValueError("position must be 'long' or 'short'")
        if self.option_type not in _OPTION_TYPE_VALUES:
            raise ValueError("option_type must be 'call', 'put', or 'stock'")

        self.premium = float(self.premium)
        self.size = float(self.size)

        if self.premium < 0:
            raise ValueError("premium must be >= 0")
        if self.size <= 0:
            raise ValueError("size must be > 0")

        if self.option_type == "stock":
            self.strike = None
            self.expiration = None
            if self.multiplier is None:
                self.multiplier = 1.0
        else:
            if self.strike in (None, ""):
                raise ValueError("strike is required for call/put legs")
            self.strike = float(self.strike)
            if self.strike <= 0:
                raise ValueError("strike must be > 0 for call/put legs")

            try:
                self.expiration = _parse_expiration(self.expiration)
            except ValueError as exc:
                raise ValueError(f"invalid expiration: {self.expiration}") from exc
            if self.expiration is None:
                raise ValueError("expiration is required for call/put legs")
            if self.multiplier is None:
                self.multiplier = 100.0

        self.multiplier = float(self.multiplier)
        if self.multiplier <= 0:
            raise ValueError("multiplier must be > 0")

        if self.con_id in (None, ""):
            self.con_id = None
        elif isinstance(self.con_id, bool):
            raise ValueError("con_id must be an integer")
        else:
            try:
                self.con_id = int(self.con_id)
            except (TypeError, ValueError) as exc:
                raise ValueError("con_id must be an integer") from exc

    @property
    def direction(self) -> int:
        """+1 for long, -1 for short. Used by adapter pricing math at adapter.py:508-512."""
        return 1 if self.position == "long" else -1

    @property
    def expiry_yyyymmdd(self) -> str | None:
        if self.expiration is None:
            return None
        return self.expiration.strftime("%Y%m%d")


@dataclass
class OptionStrategy:
    legs: list[OptionLeg]
    underlying_price: float | None = None
    underlying_symbol: str | None = None
    description: str | None = None

    def __post_init__(self) -> None:
        if not self.legs:
            raise ValueError("strategy must include at least one leg")

        if self.underlying_price in (None, ""):
            self.underlying_price = None
        else:
            self.underlying_price = float(self.underlying_price)
            if self.underlying_price <= 0:
                raise ValueError("underlying_price must be > 0 when provided")

        if self.underlying_symbol is not None:
            self.underlying_symbol = str(self.underlying_symbol).strip().upper() or None


__all__ = ["OptionLeg", "OptionStrategy"]
