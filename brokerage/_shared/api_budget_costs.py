"""Application-supplied API rates; standalone brokerage calls have no billing policy."""

from decimal import Decimal

COST_PER_CALL: dict[tuple[str, str], Decimal] = {}
