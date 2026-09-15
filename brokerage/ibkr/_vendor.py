"""Lightweight helpers vendored from trading_analysis for ibkr portability."""

from __future__ import annotations

from typing import Any

from brokerage._vendor import safe_float


__all__ = ["normalize_strike", "safe_float"]


def normalize_strike(strike: Any) -> str:
    """Canonical strike string: 30.0->"30", 2.5->"2p5", 2.50->"2p5"."""
    val = float(strike)
    if val == int(val):
        return str(int(val))
    return f"{val:g}".replace(".", "p")
