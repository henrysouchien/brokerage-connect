"""Shared helpers for SnapTrade extraction modules."""

from __future__ import annotations

from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
import math
import random
import time
from typing import Any, Callable, Optional, TYPE_CHECKING

from brokerage._logging import log_error, portfolio_logger

if TYPE_CHECKING:
    from snaptrade_client import ApiException
else:
    try:
        from snaptrade_client import ApiException
    except Exception:  # pragma: no cover - fallback when sdk is missing
        class ApiException(Exception):
            status: int | None = None


MAX_HONORED_WAIT_SECONDS = 65.0
JITTER = 0.5


def handle_snaptrade_api_exception(e: ApiException, operation: str) -> bool:
    """Handle SnapTrade API exceptions and return whether the request is retryable."""
    try:
        status_code = e.status

        if status_code in [401, 403]:
            log_error(
                "snaptrade_api",
                operation,
                {
                    "error_type": "auth_error",
                    "status_code": status_code,
                    "message": str(e),
                    "retry": False,
                },
            )
            return False

        if status_code == 429:
            log_error(
                "snaptrade_api",
                operation,
                {
                    "error_type": "rate_limit",
                    "status_code": status_code,
                    "message": str(e),
                    "retry": True,
                },
            )
            return True

        if status_code is not None and status_code >= 500:
            log_error(
                "snaptrade_api",
                operation,
                {
                    "error_type": "server_error",
                    "status_code": status_code,
                    "message": str(e),
                    "retry": True,
                },
            )
            return True

        if status_code is not None and status_code >= 400:
            log_error(
                "snaptrade_api",
                operation,
                {
                    "error_type": "client_error",
                    "status_code": status_code,
                    "message": str(e),
                    "retry": False,
                },
            )
            return False

        log_error(
            "snaptrade_api",
            operation,
            {
                "error_type": "unknown_error",
                "status_code": status_code,
                "message": str(e),
                "retry": False,
            },
        )
        return False

    except Exception as parse_error:
        log_error("snaptrade_api", "error_parsing", parse_error)
        return False


def is_snaptrade_secret_error(e: Exception) -> bool:
    """Check if exception is a SnapTrade 401 invalid-secret error."""
    return isinstance(e, ApiException) and getattr(e, "status", None) == 401


def _budget_kwargs(budget_user_id: int | None) -> dict[str, int]:
    if budget_user_id is None:
        return {}
    return {"budget_user_id": budget_user_id}


def _rate_limit_wait_seconds(
    exc: Exception,
    *,
    now_utc: datetime,
) -> float | None:
    """Return the server-requested wait for a rate-limited response."""
    headers = getattr(exc, "headers", None)
    if not headers:
        return None

    try:
        normalized = {
            str(name).strip().lower(): value
            for name, value in headers.items()
        }
    except (AttributeError, TypeError, ValueError):
        return None

    def _integer_seconds(name: str) -> float | None:
        raw = normalized.get(name.lower())
        try:
            value = int(str(raw).strip())
        except (TypeError, ValueError):
            return None
        return float(value) if value >= 0 else None

    retry_after = normalized.get("retry-after")
    if retry_after is not None:
        retry_seconds = _integer_seconds("retry-after")
        if retry_seconds is not None:
            return retry_seconds
        try:
            retry_at = parsedate_to_datetime(str(retry_after).strip())
            if retry_at.tzinfo is None:
                retry_at = retry_at.replace(tzinfo=timezone.utc)
            else:
                retry_at = retry_at.astimezone(timezone.utc)
            current = now_utc
            if current.tzinfo is None:
                current = current.replace(tzinfo=timezone.utc)
            else:
                current = current.astimezone(timezone.utc)
            return max(0.0, (retry_at - current).total_seconds())
        except (TypeError, ValueError, OverflowError):
            pass

    account_reset = _integer_seconds("x-ratelimit-account-reset")
    customer_reset = _integer_seconds("x-ratelimit-reset")
    account_remaining = _integer_seconds("x-ratelimit-account-remaining")
    customer_remaining = _integer_seconds("x-ratelimit-remaining")

    account_exhausted = account_remaining == 0
    customer_exhausted = customer_remaining == 0
    if account_exhausted and customer_exhausted:
        available = [
            reset
            for reset in (account_reset, customer_reset)
            if reset is not None
        ]
        return max(available) if available else None
    if account_exhausted:
        return account_reset if account_reset is not None else customer_reset
    if customer_exhausted:
        return customer_reset if customer_reset is not None else account_reset

    available = [
        reset
        for reset in (account_reset, customer_reset)
        if reset is not None
    ]
    return max(available) if available else None


def with_snaptrade_retry(operation_name: str, max_retries: int = 3) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
    """Retry decorator for SnapTrade SDK calls using shared error classification."""

    def decorator(func: Callable[..., Any]) -> Callable[..., Any]:
        def wrapper(*args, **kwargs):
            last_exception: Exception | None = None
            for attempt in range(max_retries + 1):
                try:
                    return func(*args, **kwargs)
                except ApiException as e:
                    last_exception = e
                    should_retry = handle_snaptrade_api_exception(
                        e,
                        f"{operation_name}_attempt_{attempt + 1}",
                    )
                    if not should_retry or attempt == max_retries:
                        portfolio_logger.error(
                            "❌ %s failed after %s attempts",
                            operation_name,
                            attempt + 1,
                        )
                        raise

                    delay = 2 ** attempt + random.uniform(0, JITTER)
                    if getattr(e, "status", None) == 429:
                        header_wait = _rate_limit_wait_seconds(
                            e,
                            now_utc=datetime.now(timezone.utc),
                        )
                        if header_wait is not None:
                            if header_wait > MAX_HONORED_WAIT_SECONDS:
                                raise
                            delay = header_wait
                    portfolio_logger.warning(
                        "⏳ %s attempt %s failed, retrying in %ss...",
                        operation_name,
                        attempt + 1,
                        delay,
                    )
                    time.sleep(delay)
                except Exception:
                    raise

            if last_exception is not None:
                raise last_exception
            raise RuntimeError(
                f"Unknown error in {operation_name} after {max_retries + 1} attempts"
            )

        return wrapper

    return decorator


def _extract_snaptrade_body(response: Any) -> Any:
    """Unwrap SDK ApiResponse objects and return plain body payload."""
    if hasattr(response, "body"):
        return response.body
    return response


def _get_snaptrade_identity(user_email: str, user_secret: str) -> tuple[str, str]:
    """Resolve SnapTrade user_id/user_secret pair from caller-provided secret."""
    from brokerage.snaptrade.users import get_snaptrade_user_id_from_email

    if not user_secret:
        raise ValueError(f"SnapTrade user_secret required for {user_email}")
    user_id = get_snaptrade_user_id_from_email(user_email)
    return user_id, user_secret


def _to_float(value: Any) -> Optional[float]:
    """Best-effort numeric conversion helper."""
    try:
        if value is None:
            return None
        result = float(value)
        if math.isinf(result):
            return None
        return result
    except (TypeError, ValueError):
        return None


__all__ = [
    "ApiException",
    "JITTER",
    "MAX_HONORED_WAIT_SECONDS",
    "_extract_snaptrade_body",
    "_get_snaptrade_identity",
    "_rate_limit_wait_seconds",
    "_to_float",
    "handle_snaptrade_api_exception",
    "is_snaptrade_secret_error",
    "with_snaptrade_retry",
]
