"""SnapTrade user lifecycle helpers."""

from __future__ import annotations

import hashlib

from brokerage._logging import log_error, portfolio_logger
from brokerage.snaptrade._shared import ApiException
from brokerage.snaptrade.client import (
    _delete_snap_trade_user_with_retry,
    _register_snap_trade_user_with_retry,
)
from brokerage.snaptrade.secrets import (
    delete_snaptrade_user_secret,
    get_snaptrade_user_secret,
    store_snaptrade_user_secret,
)


def get_snaptrade_user_id_from_email(email: str) -> str:
    """Generate stable SnapTrade user ID from email."""
    user_hash = hashlib.sha256(email.encode()).hexdigest()[:16]
    return f"user_{user_hash}"


def register_snaptrade_user(user_email: str, client) -> str:
    """Register user in SnapTrade and persist user secret."""
    try:
        snaptrade_user_id = get_snaptrade_user_id_from_email(user_email)
        user_hash = hashlib.sha256(snaptrade_user_id.encode()).hexdigest()[:16]

        response = _register_snap_trade_user_with_retry(client, snaptrade_user_id)
        user_secret = response.body["userSecret"]
        store_snaptrade_user_secret(user_email, user_secret)

        portfolio_logger.info("✅ Registered SnapTrade user: %s", user_hash)
        return user_secret

    except ApiException as e:
        if "already exist" in str(e).lower():
            portfolio_logger.info("ℹ️ SnapTrade user already exists: %s", user_hash)
            existing_secret = get_snaptrade_user_secret(user_email)
            if existing_secret and not existing_secret.startswith("needs_reconnection_"):
                portfolio_logger.info("✅ Using stored secret for existing user: %s", user_hash)
                return existing_secret
            if existing_secret and existing_secret.startswith("needs_reconnection_"):
                portfolio_logger.warning(
                    "⚠️ Found reconnection marker for existing SnapTrade user: %s",
                    user_hash,
                )
            else:
                portfolio_logger.warning(
                    "⚠️ User exists in SnapTrade but no secret in AWS storage"
                )
            raise RuntimeError(
                "SnapTrade user exists but no valid AWS user secret is available. "
                "Automatic delete/recreate is disabled to protect brokerage connections. "
                "Operator action required: manually re-register this user to restore credentials."
            )

        log_error("snaptrade_user", "register_user", e)
        raise
    except Exception as e:
        log_error("snaptrade_user", "register_user", e)
        raise


def delete_snaptrade_user(user_email: str, client) -> None:
    """Delete user from SnapTrade and clean up local secret storage."""
    try:
        snaptrade_user_id = get_snaptrade_user_id_from_email(user_email)
        user_hash = hashlib.sha256(snaptrade_user_id.encode()).hexdigest()[:16]

        _delete_snap_trade_user_with_retry(client, snaptrade_user_id)
        delete_snaptrade_user_secret(user_email)
        portfolio_logger.info("✅ Deleted SnapTrade user: %s", user_hash)

    except ApiException as e:
        if "not found" in str(e).lower():
            delete_snaptrade_user_secret(user_email)
            portfolio_logger.info(
                "ℹ️ SnapTrade user not found, cleaned up locally: %s",
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
