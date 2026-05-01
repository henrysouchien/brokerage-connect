"""SnapTrade user lifecycle helpers."""

from __future__ import annotations

import hashlib

from brokerage._logging import log_error, portfolio_logger
from brokerage.snaptrade._shared import ApiException, _budget_kwargs
from brokerage.snaptrade.client import (
    _delete_snap_trade_user_with_retry,
    _register_snap_trade_user_with_retry,
    _require_snaptrade_client,
)


def get_snaptrade_user_id_from_email(email: str) -> str:
    """Generate stable SnapTrade user ID from email."""
    user_hash = hashlib.sha256(email.encode()).hexdigest()[:16]
    return f"user_{user_hash}"


def register_snaptrade_user(
    user_email: str,
    *,
    budget_user_id: int | None = None,
) -> str:
    """Register user in SnapTrade and return the newly issued user secret."""
    client = _require_snaptrade_client()
    try:
        snaptrade_user_id = get_snaptrade_user_id_from_email(user_email)
        user_hash = hashlib.sha256(snaptrade_user_id.encode()).hexdigest()[:16]

        response = _register_snap_trade_user_with_retry(
            client,
            snaptrade_user_id,
            **_budget_kwargs(budget_user_id),
        )
        user_secret = response.body["userSecret"]

        portfolio_logger.info("✅ Registered SnapTrade user: %s", user_hash)
        return user_secret

    except ApiException as e:
        if "already exist" in str(e).lower():
            portfolio_logger.info("ℹ️ SnapTrade user already exists: %s", user_hash)
            raise RuntimeError(
                "SnapTrade user already exists. Use the stored user_secret to create a "
                "connection URL, or run recovery if the secret is unavailable."
            )

        log_error("snaptrade_user", "register_user", e)
        raise
    except Exception as e:
        log_error("snaptrade_user", "register_user", e)
        raise


def delete_snaptrade_user(
    user_email: str,
    user_secret: str,
    *,
    budget_user_id: int | None = None,
) -> None:
    """Delete user from SnapTrade. Caller owns local secret cleanup."""
    if not user_secret:
        raise ValueError(f"SnapTrade user_secret required for {user_email}")
    client = _require_snaptrade_client()
    try:
        snaptrade_user_id = get_snaptrade_user_id_from_email(user_email)
        user_hash = hashlib.sha256(snaptrade_user_id.encode()).hexdigest()[:16]

        _delete_snap_trade_user_with_retry(
            client,
            snaptrade_user_id,
            **_budget_kwargs(budget_user_id),
        )
        portfolio_logger.info("✅ Deleted SnapTrade user: %s", user_hash)

    except ApiException as e:
        if "not found" in str(e).lower():
            portfolio_logger.info(
                "ℹ️ SnapTrade user not found: %s",
                user_hash,
            )
            return
        log_error("snaptrade_user", "delete_user", e)
        raise
    except Exception as e:
        log_error("snaptrade_user", "delete_user", e)
        raise


__all__ = [
    "delete_snaptrade_user",
    "get_snaptrade_user_id_from_email",
    "register_snaptrade_user",
]
