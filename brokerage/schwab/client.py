"""Thin Schwab API client wrapper with token/account-hash helpers."""

from __future__ import annotations

import importlib.machinery
import importlib.util
import json
import os
import sys
import types
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

from brokerage._logging import portfolio_logger
from brokerage.config import (
    SCHWAB_APP_KEY,
    SCHWAB_APP_SECRET,
    SCHWAB_CALLBACK_URL,
    SCHWAB_TOKEN_PATH,
)


_client_cache: Any = None
_account_hash_cache: dict[str, str] | None = None


class _NoopLogRedactor:
    def register(self, _string: Any, _label: Any) -> None:
        return None

    def redact(self, msg: Any) -> str:
        return str(msg)


def _load_schwab_auth_module() -> Any:
    """Load schwab.auth without importing schwab.__init__ (which imports streaming)."""
    module_name = "schwab.auth"
    existing = sys.modules.get(module_name)
    if existing is not None:
        return existing

    pkg_name = "schwab"
    pkg_module = sys.modules.get(pkg_name)
    if pkg_module is None:
        pkg_spec = importlib.util.find_spec(pkg_name)
        if pkg_spec is None or not pkg_spec.origin:
            raise ImportError("schwab package not found")

        pkg_dir = Path(pkg_spec.origin).resolve().parent
        pkg_module = types.ModuleType(pkg_name)
        pkg_module.__file__ = str(pkg_dir / "__init__.py")
        pkg_module.__package__ = pkg_name
        pkg_module.__path__ = [str(pkg_dir)]  # type: ignore[attr-defined]
        pkg_module.__spec__ = importlib.machinery.ModuleSpec(
            name=pkg_name,
            loader=None,
            is_package=True,
        )
        pkg_module.LOG_REDACTOR = _NoopLogRedactor()
        sys.modules[pkg_name] = pkg_module

    pkg_paths = getattr(pkg_module, "__path__", None)
    if not pkg_paths:
        raise ImportError("schwab package path unavailable")

    auth_path = Path(pkg_paths[0]) / "auth.py"
    spec = importlib.util.spec_from_file_location(module_name, auth_path)
    if spec is None or spec.loader is None:
        raise ImportError("could not load schwab.auth module spec")

    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    setattr(pkg_module, "auth", module)
    return module


def _token_path() -> str:
    return os.path.expanduser(SCHWAB_TOKEN_PATH)


def _load_json_response(response: Any) -> Any:
    if response is None:
        return None
    if isinstance(response, (dict, list)):
        return response
    body = getattr(response, "body", None)
    if body is not None:
        return body
    if hasattr(response, "json"):
        try:
            return response.json()
        except Exception:
            return None
    return None


def is_invalid_grant_error(exc: Exception) -> bool:
    name = exc.__class__.__name__.lower()
    message = str(exc).lower()
    return (
        "invalidgranterror" in name
        or "invalid_grant" in message
        or "invalid grant" in message
        or "refresh_token_authentication_error" in message
        or "unsupported_token_type" in message
    )


def _raise_relogin_required(exc: Exception) -> None:
    raise RuntimeError(
        "Schwab refresh token appears expired. Re-authenticate with: "
        "`python3 run_schwab.py login`"
    ) from exc


def _client_from_token_file() -> Any:
    if not SCHWAB_APP_KEY or not SCHWAB_APP_SECRET:
        raise ValueError("Missing SCHWAB_APP_KEY or SCHWAB_APP_SECRET in environment")

    token_path = _token_path()
    if not os.path.exists(token_path):
        raise FileNotFoundError(
            f"Schwab token file not found at {token_path}. Run `python3 run_schwab.py login`."
        )

    auth = _load_schwab_auth_module()

    # Keep compatibility with minor signature differences across schwab-py versions.
    try:
        return auth.client_from_token_file(
            token_path=token_path,
            api_key=SCHWAB_APP_KEY,
            app_secret=SCHWAB_APP_SECRET,
            enforce_enums=False,
        )
    except TypeError:
        return auth.client_from_token_file(token_path, SCHWAB_APP_KEY, SCHWAB_APP_SECRET)


def get_schwab_client(force_refresh: bool = False) -> Any:
    """Return an authenticated schwab-py client (auto-refresh handled by schwab-py)."""
    global _client_cache

    if _client_cache is not None and not force_refresh:
        return _client_cache

    try:
        _client_cache = _client_from_token_file()
        return _client_cache
    except Exception as exc:
        if is_invalid_grant_error(exc):
            _raise_relogin_required(exc)
        raise


def schwab_login(manual: bool = False) -> Any:
    """Run one-time OAuth login flow and persist token to local token file.

    Removes any stale token file before starting the flow so the schwab-py
    library doesn't attempt to refresh an expired refresh token.

    Args:
        manual: If True, use manual flow where the user pastes the redirect URL
                instead of relying on the local HTTPS callback server.
    """
    if not SCHWAB_APP_KEY or not SCHWAB_APP_SECRET:
        raise ValueError("Missing SCHWAB_APP_KEY or SCHWAB_APP_SECRET in environment")

    token_path = _token_path()
    token_dir = os.path.dirname(token_path)
    if token_dir:
        os.makedirs(token_dir, exist_ok=True)

    # Remove stale token so schwab-py doesn't try to refresh a dead token.
    if os.path.exists(token_path):
        os.remove(token_path)
        portfolio_logger.info("Removed stale token file before re-login: %s", token_path)

    auth = _load_schwab_auth_module()

    # Clear cached client so next get_schwab_client() loads the fresh token.
    global _client_cache
    _client_cache = None

    if manual:
        try:
            _client_cache = auth.client_from_manual_flow(
                api_key=SCHWAB_APP_KEY,
                app_secret=SCHWAB_APP_SECRET,
                callback_url=SCHWAB_CALLBACK_URL,
                token_path=token_path,
            )
        except TypeError:
            _client_cache = auth.client_from_manual_flow(
                SCHWAB_APP_KEY,
                SCHWAB_APP_SECRET,
                SCHWAB_CALLBACK_URL,
                token_path,
            )
    else:
        try:
            _client_cache = auth.client_from_login_flow(
                api_key=SCHWAB_APP_KEY,
                app_secret=SCHWAB_APP_SECRET,
                callback_url=SCHWAB_CALLBACK_URL,
                token_path=token_path,
            )
        except TypeError:
            _client_cache = auth.client_from_login_flow(
                SCHWAB_APP_KEY,
                SCHWAB_APP_SECRET,
                SCHWAB_CALLBACK_URL,
                token_path,
            )
    return _client_cache


def get_account_hashes(force_refresh: bool = False) -> dict[str, str]:
    """Return cached account_number -> account_hash mapping for this process."""
    global _account_hash_cache

    if _account_hash_cache is not None and not force_refresh:
        return dict(_account_hash_cache)

    client = get_schwab_client()
    try:
        response = client.get_account_numbers()
    except Exception as exc:
        if is_invalid_grant_error(exc):
            _raise_relogin_required(exc)
        raise

    payload = _load_json_response(response)
    rows = payload if isinstance(payload, list) else []

    mapping: dict[str, str] = {}
    for row in rows:
        if not isinstance(row, dict):
            continue
        account_number = str(row.get("accountNumber") or row.get("account_number") or "").strip()
        account_hash = str(row.get("hashValue") or row.get("hash_value") or "").strip()
        if account_number and account_hash:
            mapping[account_number] = account_hash

    _account_hash_cache = mapping
    return dict(mapping)


def check_token_health() -> dict[str, Any]:
    """Inspect token file and client age; warn near 7-day refresh expiry."""
    token_path = _token_path()
    health: dict[str, Any] = {
        "token_path": token_path,
        "token_file_exists": os.path.exists(token_path),
        "token_age_seconds": None,
        "refresh_token_expires_at": None,
        "refresh_token_days_remaining": None,
        "near_refresh_expiry": False,
        "warnings": [],
    }

    if not os.path.exists(token_path):
        health["warnings"].append("Token file missing. Run `python3 run_schwab.py login`.")
        return health

    token_blob: dict[str, Any] = {}
    try:
        with open(token_path, "r", encoding="utf-8") as handle:
            token_blob = json.load(handle)
    except Exception as exc:
        health["warnings"].append(f"Could not parse token file JSON: {exc}")

    try:
        client = get_schwab_client()
        token_age = getattr(client, "token_age", None)
        if token_age is not None:
            # schwab-py exposes token_age as a method in some versions,
            # a property returning timedelta or int in others.
            if callable(token_age):
                token_age = token_age()
            if isinstance(token_age, timedelta):
                health["token_age_seconds"] = token_age.total_seconds()
            else:
                health["token_age_seconds"] = float(token_age)
    except Exception as exc:
        if is_invalid_grant_error(exc):
            health["warnings"].append(
                "Refresh token appears expired. Run `python3 run_schwab.py login`."
            )
            health["near_refresh_expiry"] = True
        else:
            health["warnings"].append(f"Client health check failed: {exc}")

    # Use file mtime as the most reliable indicator of when the token was
    # last written (creation_timestamp is set once and not updated on re-login
    # by some schwab-py versions).
    try:
        mtime = os.path.getmtime(token_path)
        created_dt = datetime.fromtimestamp(mtime, tz=UTC)

        # Also check creation_timestamp if it's newer (in case schwab-py does update it)
        blob_ts = token_blob.get("creation_timestamp")
        if blob_ts is not None:
            blob_dt = datetime.fromtimestamp(float(blob_ts), tz=UTC)
            if blob_dt > created_dt:
                created_dt = blob_dt

        refresh_expiry = created_dt + timedelta(days=7)
        remaining_days = (refresh_expiry - datetime.now(tz=UTC)).total_seconds() / 86400
        health["refresh_token_expires_at"] = refresh_expiry.isoformat()
        health["refresh_token_days_remaining"] = round(remaining_days, 2)
        if remaining_days <= 1.0:
            health["near_refresh_expiry"] = True
            health["warnings"].append(
                "Refresh token near expiry (<=1 day). Re-run `python3 run_schwab.py login` soon."
            )
    except Exception:
        pass

    return health


def invalidate_schwab_caches() -> None:
    """Clear in-memory client/hash caches."""
    global _client_cache, _account_hash_cache
    _client_cache = None
    _account_hash_cache = None
    portfolio_logger.info("Cleared in-memory Schwab client/account-hash cache")
