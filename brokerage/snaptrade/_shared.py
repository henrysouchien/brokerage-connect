"""Shared helpers for SnapTrade extraction modules."""

from __future__ import annotations

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

                    delay = 2 ** attempt
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


def _get_snaptrade_identity(user_email: str) -> tuple[str, str]:
    """Resolve SnapTrade user_id/user_secret pair from user email."""
    from brokerage.snaptrade.secrets import get_snaptrade_user_secret
    from brokerage.snaptrade.users import get_snaptrade_user_id_from_email

    user_id = get_snaptrade_user_id_from_email(user_email)
    user_secret = get_snaptrade_user_secret(user_email)
    if not user_secret:
        raise ValueError(f"No SnapTrade user secret found for {user_email}")
    return user_id, user_secret


def _to_float(value: Any) -> Optional[float]:
    """Best-effort numeric conversion helper."""
    try:
        if value is None:
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


__all__ = [
    "ApiException",
    "_extract_snaptrade_body",
    "_get_snaptrade_identity",
    "_to_float",
    "handle_snaptrade_api_exception",
    "with_snaptrade_retry",
]
