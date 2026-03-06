from typing import Optional

from brokerage.futures.contract_spec import get_contract_spec


def calculate_notional(symbol: str, quantity: float, price: float) -> Optional[float]:
    """Calculate notional exposure for a futures position."""
    spec = get_contract_spec(symbol)
    if spec is None:
        return None
    return spec.notional(quantity, price)


def calculate_point_value(symbol: str) -> Optional[float]:
    """Return the dollar value of a one-point move."""
    spec = get_contract_spec(symbol)
    if spec is None:
        return None
    return spec.point_value


def calculate_tick_value(symbol: str) -> Optional[float]:
    """Return the dollar value of one tick move."""
    spec = get_contract_spec(symbol)
    if spec is None:
        return None
    return spec.tick_value
