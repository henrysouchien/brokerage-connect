"""Shared instrument metadata types vendored for ibkr package portability."""

from __future__ import annotations

from typing import Any, Literal

InstrumentType = Literal[
    "equity",
    "etf",
    "fund",
    "option",
    "futures",
    "fx",
    "bond",
    "cash",
    "warrant",
    "derivative",
]

_VALID_INSTRUMENT_TYPES = {
    "equity",
    "etf",
    "fund",
    "option",
    "futures",
    "fx",
    "bond",
    "cash",
    "warrant",
    "derivative",
}


def coerce_instrument_type(value: Any) -> InstrumentType:
    """Decode an exact canonical instrument kind or fail closed."""
    normalized = str(value or "").strip().lower()
    if normalized in _VALID_INSTRUMENT_TYPES:
        return normalized  # type: ignore[return-value]
    raise ValueError(f"unsupported canonical instrument kind: {value!r}")
