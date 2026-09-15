"""Optional host API-budget enforcement, configured by the application's bootstrap.

Standalone calls execute directly. Applications that enforce budgets inject their
own guard before using any provider client; the package never discovers a host.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping
from decimal import Decimal
from typing import Any

from .api_budget_costs import COST_PER_CALL


def _unguarded_call(*, fn: Callable[..., Any], args=(), kwargs=None, **_: Any) -> Any:
    return fn(*args, **(kwargs or {}))


_guard: Callable[..., Any] = _unguarded_call


def configure_budget(
    *,
    guard: Callable[..., Any],
    cost_per_call: Mapping[tuple[str, str], Decimal],
) -> None:
    """Bind the host's budget guard and rates once, before provider use."""
    global _guard
    _guard = guard
    COST_PER_CALL.clear()
    COST_PER_CALL.update(cost_per_call)


def guard_call(**kwargs: Any) -> Any:
    """Execute a provider operation under the configured application policy."""
    return _guard(**kwargs)
