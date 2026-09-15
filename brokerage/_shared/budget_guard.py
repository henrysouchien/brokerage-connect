"""Host-first shim for API budget guard calls.

Preserves monorepo budget enforcement; standalone installs fall back to
executing the wrapped function without tracking.
"""

from __future__ import annotations

from collections.abc import Callable
from decimal import Decimal
from typing import Any

try:
    from app_platform.api_budget import guard_call as _host_guard_call  # type: ignore[import-not-found]

    guard_call = _host_guard_call
except ImportError:

    def guard_call(
        *,
        provider: str,
        operation: str,
        fn: Callable[..., Any],
        args: tuple[Any, ...] = (),
        kwargs: dict[str, Any] | None = None,
        budget_user_id: int | None = None,
        account_id: str | None = None,
        caller: str | None = None,
        cost_fn: Callable[[Any], Any] | None = None,
        cost_per_call: Decimal | float | int | str | None = None,
        item_id: str | None = None,
    ) -> Any:
        del (
            provider,
            operation,
            budget_user_id,
            account_id,
            caller,
            cost_fn,
            cost_per_call,
            item_id,
        )
        return fn(*args, **(kwargs or {}))


__all__ = ["guard_call"]
