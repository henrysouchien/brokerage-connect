"""SnapTrade secret recovery helpers."""

from __future__ import annotations

import hashlib
import os
import threading
import time
from typing import Dict

from brokerage._logging import portfolio_logger
from brokerage.snaptrade._shared import ApiException, _budget_kwargs, is_snaptrade_secret_error
from brokerage.snaptrade.client import (
    _delete_snap_trade_user_with_retry,
    _list_user_accounts_with_retry,
    _require_snaptrade_client,
    _register_snap_trade_user_with_retry,
    _reset_snap_trade_user_secret_with_retry,
)
from brokerage.snaptrade.users import get_snaptrade_user_id_from_email

_rotation_locks: Dict[str, threading.Lock] = {}
_rotation_locks_guard = threading.Lock()


def _get_rotation_lock(user_email: str) -> threading.Lock:
    with _rotation_locks_guard:
        if user_email not in _rotation_locks:
            _rotation_locks[user_email] = threading.Lock()
        return _rotation_locks[user_email]


def get_snaptrade_rotation_lock(user_email: str) -> threading.Lock:
    return _get_rotation_lock(user_email)


def _user_hash(user_email: str) -> str:
    snaptrade_user_id = get_snaptrade_user_id_from_email(user_email)
    return hashlib.sha256(snaptrade_user_id.encode()).hexdigest()[:16]


def _extract_secret_from_response(response) -> str:
    payload = response.body if hasattr(response, "body") else response
    if hasattr(payload, "to_dict"):
        payload = payload.to_dict()
    if not isinstance(payload, dict):
        raise RuntimeError("SnapTrade recovery response missing body payload")
    new_secret = payload.get("userSecret")
    if not new_secret:
        raise RuntimeError("SnapTrade recovery response missing userSecret")
    return str(new_secret)


def _write_recovery_file(user_email: str, user_secret: str) -> str:
    recovery_dir = os.path.expanduser("~/.snaptrade_recovery")
    os.makedirs(recovery_dir, mode=0o700, exist_ok=True)
    os.chmod(recovery_dir, 0o700)

    recovery_path = os.path.join(recovery_dir, f"{_user_hash(user_email)}.secret")
    fd = os.open(recovery_path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    with os.fdopen(fd, "w", encoding="utf-8") as handle:
        handle.write(user_secret)
    os.chmod(recovery_path, 0o600)
    return recovery_path


def rotate_snaptrade_user_secret(
    user_email: str,
    current_secret: str,
    *,
    budget_user_id: int | None = None,
) -> str:
    """Rotate a SnapTrade user secret and return the new secret."""
    client = _require_snaptrade_client()
    user_id = get_snaptrade_user_id_from_email(user_email)
    if not current_secret:
        raise ValueError(f"SnapTrade user_secret required for {user_email}")

    response = _reset_snap_trade_user_secret_with_retry(
        client,
        user_id,
        current_secret,
        **_budget_kwargs(budget_user_id),
    )
    new_secret = _extract_secret_from_response(response)

    try:
        _list_user_accounts_with_retry(
            client,
            user_id,
            new_secret,
            **_budget_kwargs(budget_user_id),
        )
    except ApiException as exc:
        if is_snaptrade_secret_error(exc):
            portfolio_logger.error(
                "Rotated SnapTrade secret rejected by probe for user_hash=%s",
                _user_hash(user_email),
            )
            raise RuntimeError("Rotated SnapTrade secret rejected by probe") from exc
        raise

    return new_secret


def _try_rotate_secret(
    user_email: str,
    failed_secret: str,
    *,
    on_secret_rotated=None,
    refresh_secret=None,
    budget_user_id: int | None = None,
) -> str | None:
    """Attempt Tier 1 rotation if the secret hasn't already been rotated."""
    lock = _get_rotation_lock(user_email)
    with lock:
        current_secret = refresh_secret() if refresh_secret is not None else failed_secret
        if current_secret and current_secret != failed_secret:
            return current_secret
        if current_secret == failed_secret:
            new_secret = rotate_snaptrade_user_secret(
                user_email,
                failed_secret,
                **_budget_kwargs(budget_user_id),
            )
            if on_secret_rotated is not None:
                try:
                    on_secret_rotated(new_secret)
                except Exception:
                    try:
                        recovery_path = _write_recovery_file(user_email, new_secret)
                        portfolio_logger.error(
                            "CRITICAL: new SnapTrade secret written to recovery file: %s",
                            recovery_path,
                        )
                    except Exception as recovery_error:
                        portfolio_logger.error(
                            "CRITICAL: failed to write SnapTrade recovery file: %s",
                            recovery_error,
                        )
                    raise
            return new_secret
    return None


def recover_snaptrade_auth(
    user_email: str,
    current_secret: str,
    budget_user_id: int = 0,
) -> tuple[str, str]:
    """Run Tier 1 rotation, then Tier 2 delete/recreate if rotation 401s."""
    client = _require_snaptrade_client()
    try:
        return (
            rotate_snaptrade_user_secret(
                user_email,
                current_secret,
                **_budget_kwargs(budget_user_id),
            ),
            "rotation",
        )
    except ApiException as exc:
        if not is_snaptrade_secret_error(exc):
            raise

    snaptrade_user_id = get_snaptrade_user_id_from_email(user_email)
    user_hash = _user_hash(user_email)

    try:
        _delete_snap_trade_user_with_retry(
            client,
            snaptrade_user_id,
            **_budget_kwargs(budget_user_id),
        )
    except ApiException as exc:
        if "not found" in str(exc).lower():
            portfolio_logger.info(
                "SnapTrade user already absent during Tier 2 recovery for user_hash=%s",
                user_hash,
            )
        else:
            raise

    try:
        response = _register_snap_trade_user_with_retry(
            client,
            snaptrade_user_id,
            **_budget_kwargs(budget_user_id),
        )
    except ApiException as exc:
        if "already exist" not in str(exc).lower():
            raise
        time.sleep(2)
        response = _register_snap_trade_user_with_retry(
            client,
            snaptrade_user_id,
            **_budget_kwargs(budget_user_id),
        )

    new_secret = _extract_secret_from_response(response)

    resolved_secret = new_secret
    try:
        _list_user_accounts_with_retry(
            client,
            snaptrade_user_id,
            new_secret,
            **_budget_kwargs(budget_user_id),
        )
    except Exception:
        raise

    return resolved_secret, "delete_recreate"


__all__ = [
    "_get_rotation_lock",
    "_try_rotate_secret",
    "get_snaptrade_rotation_lock",
    "recover_snaptrade_auth",
    "rotate_snaptrade_user_secret",
]
