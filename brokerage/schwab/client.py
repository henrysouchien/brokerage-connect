"""Thin Schwab API client wrapper with token/account-hash helpers."""

from __future__ import annotations

import functools
import importlib.machinery
import importlib.util
import json
import os
import sys
import time
import types
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any

from app_platform.api_budget import guard_call
from brokerage._logging import portfolio_logger
from brokerage.config import (
    SCHWAB_APP_KEY,
    SCHWAB_APP_SECRET,
    SCHWAB_CALLBACK_URL,
    SCHWAB_SSL_CERT_PATH,
    SCHWAB_SSL_KEY_PATH,
    SCHWAB_TOKEN_PATH,
)
from config.api_budget_costs import COST_PER_CALL


_account_hash_cache: dict[str, str] | None = None
_invalid_grant_cache: tuple[float, str] | None = None
_original_server_fn: Any = None
_INVALID_GRANT_TTL_SECONDS = 300.0
_RELOGIN_REQUIRED_MESSAGE = (
    "Schwab refresh token appears expired. Re-authenticate with: "
    "`python3 -m scripts.run_schwab login`"
)
_REFRESH_TOKEN_MAX_AGE = timedelta(days=7)


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
            origin=str(pkg_dir / "__init__.py"),
            is_package=True,
        )
        pkg_module.__spec__.submodule_search_locations = [str(pkg_dir)]
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


def _response_as_dict(response: Any) -> dict[str, Any]:
    payload = _load_json_response(response)
    if isinstance(payload, dict):
        result = dict(payload)
    elif payload is None:
        result = {}
    else:
        result = {"payload": payload}

    status_code = getattr(response, "status_code", None)
    if isinstance(status_code, int) and "status_code" not in result:
        result["status_code"] = status_code

    headers = getattr(response, "headers", None)
    if isinstance(headers, dict) and headers and "headers" not in result:
        result["headers"] = dict(headers)

    return result


def _schwab_cost_per_call(operation: str) -> Any:
    return COST_PER_CALL.get(("schwab", operation), 0)


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
    raise RuntimeError(_RELOGIN_REQUIRED_MESSAGE) from exc


def _get_cached_invalid_grant_message(force_refresh: bool = False) -> str | None:
    global _invalid_grant_cache

    if force_refresh or _invalid_grant_cache is None:
        return None

    cached_at, message = _invalid_grant_cache
    if (time.monotonic() - cached_at) >= _INVALID_GRANT_TTL_SECONDS:
        _invalid_grant_cache = None
        return None

    return message


def _cache_invalid_grant() -> None:
    global _invalid_grant_cache
    _invalid_grant_cache = (time.monotonic(), _RELOGIN_REQUIRED_MESSAGE)


def _token_written_at() -> datetime | None:
    token_path = _token_path()
    if not os.path.exists(token_path):
        return None

    try:
        written_at = datetime.fromtimestamp(os.path.getmtime(token_path), tz=UTC)
    except Exception:
        written_at = None

    try:
        with open(token_path, "r", encoding="utf-8") as handle:
            token_blob = json.load(handle)
    except Exception:
        token_blob = {}

    blob_ts = token_blob.get("creation_timestamp") if isinstance(token_blob, dict) else None
    if blob_ts is not None:
        try:
            blob_dt = datetime.fromtimestamp(float(blob_ts), tz=UTC)
            if written_at is None or blob_dt > written_at:
                written_at = blob_dt
        except Exception:
            pass

    return written_at


def _refresh_token_expired_by_file_age(now: datetime | None = None) -> bool:
    written_at = _token_written_at()
    if written_at is None:
        return False
    if now is None:
        now = datetime.now(tz=UTC)
    return (now - written_at) >= _REFRESH_TOKEN_MAX_AGE


def _client_from_token_file() -> Any:
    if not SCHWAB_APP_KEY or not SCHWAB_APP_SECRET:
        raise ValueError("Missing SCHWAB_APP_KEY or SCHWAB_APP_SECRET in environment")

    token_path = _token_path()
    if not os.path.exists(token_path):
        raise FileNotFoundError(
            f"Schwab token file not found at {token_path}. Run `python3 -m scripts.run_schwab login`."
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


@functools.lru_cache(maxsize=1)
def _get_or_create_client() -> Any:
    return _client_from_token_file()


def get_schwab_client(force_refresh: bool = False) -> Any:
    """Return an authenticated schwab-py client (auto-refresh handled by schwab-py)."""
    global _invalid_grant_cache

    cached_invalid_message = _get_cached_invalid_grant_message(force_refresh=force_refresh)
    if cached_invalid_message is not None:
        raise RuntimeError(cached_invalid_message)

    if not force_refresh and _refresh_token_expired_by_file_age():
        _cache_invalid_grant()
        raise RuntimeError(_RELOGIN_REQUIRED_MESSAGE)

    try:
        if force_refresh:
            _get_or_create_client.cache_clear()
        client = _get_or_create_client()
        _invalid_grant_cache = None
        return client
    except Exception as exc:
        if is_invalid_grant_error(exc):
            _cache_invalid_grant()
            _raise_relogin_required(exc)
        raise


def _call_client_method(func: Any, /, *args: Any, **kwargs: Any) -> Any:
    try:
        return func(*args, **kwargs)
    except Exception as exc:
        if is_invalid_grant_error(exc):
            _cache_invalid_grant()
            _raise_relogin_required(exc)
        raise


def _call_schwab_get_account(client: Any, account_hash: str, fields: list[str] | None = None) -> Any:
    try:
        return (
            _call_client_method(client.get_account, account_hash, fields=fields)
            if fields
            else _call_client_method(client.get_account, account_hash)
        )
    except TypeError:
        return _call_client_method(client.get_account, account_hash)


def _call_schwab_get_transactions(
    client: Any,
    account_hash: str,
    *,
    start_date: date,
    end_date: date,
) -> Any:
    try:
        return _call_client_method(
            client.get_transactions,
            account_hash,
            start_date=start_date,
            end_date=end_date,
        )
    except TypeError:
        return _call_client_method(client.get_transactions, account_hash, start_date, end_date)


def _call_schwab_get_orders_for_account(
    client: Any,
    account_hash: str,
    *,
    start: datetime,
    end: datetime,
) -> Any:
    try:
        return _call_client_method(client.get_orders_for_account, account_hash, start, end)
    except TypeError:
        return _call_client_method(client.get_orders_for_account, account_hash)


def _call_schwab_cancel_order(client: Any, account_hash: str, order_id: str) -> Any:
    try:
        return _call_client_method(client.cancel_order, order_id, account_hash)
    except TypeError:
        return _call_client_method(client.cancel_order, account_hash, order_id)


def _apply_mkcert_ssl_patch(auth_module: Any) -> None:
    global _original_server_fn

    if _original_server_fn is None:
        _original_server_fn = getattr(auth_module, "__run_client_from_login_flow_server")

    cert_path = SCHWAB_SSL_CERT_PATH
    key_path = SCHWAB_SSL_KEY_PATH
    if not os.path.exists(cert_path) or not os.path.exists(key_path):
        setattr(auth_module, "__run_client_from_login_flow_server", _original_server_fn)
        portfolio_logger.warning(
            "Schwab SSL cert/key not found at %s and %s. Falling back to schwab-py's adhoc cert. "
            "Run `python3 -m scripts.run_schwab setup-ssl`.",
            cert_path,
            key_path,
        )
        return

    # This runs in a separate process and is invisible to coverage
    def _patched_server(q: Any, callback_port: int, callback_path: str) -> None:  # pragma: no cover
        """Helper server for intercepting redirects to the callback URL.

        See client_from_login_flow for details.
        """
        import flask

        app = flask.Flask(__name__)

        @app.route(callback_path)
        def handle_token() -> str:
            q.put(flask.request.url)
            return "schwab-py callback received! You may now close this window/tab."

        @app.route("/schwab-py-internal/status")
        def status() -> str:
            return "running"

        if callback_port == 443:
            return

        # Wrap this call in some hackery to suppress the flask startup messages
        with open(os.devnull, "w") as devnull:
            import logging

            log = logging.getLogger("werkzeug")
            log.setLevel(logging.ERROR)

            old_stdout = sys.stdout
            sys.stdout = devnull
            app.run(port=callback_port, ssl_context=(cert_path, key_path))
            sys.stdout = old_stdout

    setattr(auth_module, "__run_client_from_login_flow_server", _patched_server)


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
    _apply_mkcert_ssl_patch(auth)

    # Clear cached client so next get_schwab_client() loads the fresh token.
    global _invalid_grant_cache
    _invalid_grant_cache = None
    _get_or_create_client.cache_clear()

    if manual:
        try:
            client = auth.client_from_manual_flow(
                api_key=SCHWAB_APP_KEY,
                app_secret=SCHWAB_APP_SECRET,
                callback_url=SCHWAB_CALLBACK_URL,
                token_path=token_path,
            )
        except TypeError:
            client = auth.client_from_manual_flow(
                SCHWAB_APP_KEY,
                SCHWAB_APP_SECRET,
                SCHWAB_CALLBACK_URL,
                token_path,
            )
    else:
        try:
            client = auth.client_from_login_flow(
                api_key=SCHWAB_APP_KEY,
                app_secret=SCHWAB_APP_SECRET,
                callback_url=SCHWAB_CALLBACK_URL,
                token_path=token_path,
            )
        except TypeError:
            client = auth.client_from_login_flow(
                SCHWAB_APP_KEY,
                SCHWAB_APP_SECRET,
                SCHWAB_CALLBACK_URL,
                token_path,
            )
    return client


def get_account_hashes(
    force_refresh: bool = False,
    *,
    budget_user_id: int | None = None,
) -> dict[str, str]:
    """Return cached account_number -> account_hash mapping for this process."""
    global _account_hash_cache

    if _account_hash_cache is not None and not force_refresh:
        return dict(_account_hash_cache)

    client = get_schwab_client()
    response = guard_call(
        provider="schwab",
        operation="get_account_numbers",
        budget_user_id=budget_user_id,
        cost_per_call=_schwab_cost_per_call("get_account_numbers"),
        fn=_call_client_method,
        args=(client.get_account_numbers,),
    )

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


def get_account_data(
    account_hash: str,
    *,
    fields: list[str] | None = None,
    budget_user_id: int | None = None,
) -> dict[str, Any]:
    """Return one account payload as a plain dict."""
    client = get_schwab_client()
    response = guard_call(
        provider="schwab",
        operation="get_account",
        budget_user_id=budget_user_id,
        cost_per_call=_schwab_cost_per_call("get_account"),
        fn=_call_schwab_get_account,
        args=(client, account_hash),
        kwargs={"fields": fields},
    )
    payload = _load_json_response(response)
    return payload if isinstance(payload, dict) else {}


def get_transactions(
    account_hash: str,
    *,
    start_date: date,
    end_date: date,
    budget_user_id: int | None = None,
) -> list[dict[str, Any]]:
    """Return account transaction rows as plain dicts."""
    client = get_schwab_client()
    response = guard_call(
        provider="schwab",
        operation="get_transactions",
        budget_user_id=budget_user_id,
        cost_per_call=_schwab_cost_per_call("get_transactions"),
        fn=_call_schwab_get_transactions,
        args=(client, account_hash),
        kwargs={
            "start_date": start_date,
            "end_date": end_date,
        },
    )
    payload = _load_json_response(response)
    return [row for row in payload if isinstance(row, dict)] if isinstance(payload, list) else []


def get_quotes(symbols: list[str], *, budget_user_id: int | None = None) -> dict[str, Any]:
    """Return multi-symbol quote payload keyed by symbol."""
    if not symbols:
        return {}
    client = get_schwab_client()
    response = guard_call(
        provider="schwab",
        operation="get_quotes",
        budget_user_id=budget_user_id,
        cost_per_call=_schwab_cost_per_call("get_quotes"),
        fn=_call_client_method,
        args=(client.get_quotes, symbols),
    )
    payload = _load_json_response(response)
    return payload if isinstance(payload, dict) else {}


def get_quote(symbol: str, *, budget_user_id: int | None = None) -> dict[str, Any] | None:
    """Return one quote row when available."""
    normalized_symbol = str(symbol or "").upper().strip()
    if not normalized_symbol:
        return None
    client = get_schwab_client()
    response = guard_call(
        provider="schwab",
        operation="get_quote",
        budget_user_id=budget_user_id,
        cost_per_call=_schwab_cost_per_call("get_quote"),
        fn=_call_client_method,
        args=(client.get_quote, normalized_symbol),
    )
    payload = _load_json_response(response)
    if not isinstance(payload, dict):
        return None
    if isinstance(payload.get(normalized_symbol), dict):
        return payload[normalized_symbol]
    return payload


def search_instruments(
    symbol: str,
    *,
    projection: str = "symbol-search",
    budget_user_id: int | None = None,
) -> dict[str, Any]:
    """Return search payload for a symbol lookup."""
    normalized_symbol = str(symbol or "").upper().strip()
    if not normalized_symbol:
        return {}
    client = get_schwab_client()
    response = guard_call(
        provider="schwab",
        operation="search_instruments",
        budget_user_id=budget_user_id,
        cost_per_call=_schwab_cost_per_call("search_instruments"),
        fn=_call_client_method,
        args=(client.search_instruments, normalized_symbol),
        kwargs={"projection": projection},
    )
    payload = _load_json_response(response)
    return payload if isinstance(payload, dict) else {}


def get_orders_for_account(
    account_hash: str,
    *,
    start: datetime,
    end: datetime,
    budget_user_id: int | None = None,
) -> list[dict[str, Any]]:
    """Return account order rows as plain dicts."""
    client = get_schwab_client()
    response = guard_call(
        provider="schwab",
        operation="get_orders_for_account",
        budget_user_id=budget_user_id,
        cost_per_call=_schwab_cost_per_call("get_orders_for_account"),
        fn=_call_schwab_get_orders_for_account,
        args=(client, account_hash),
        kwargs={
            "start": start,
            "end": end,
        },
    )
    payload = _load_json_response(response)
    return [row for row in payload if isinstance(row, dict)] if isinstance(payload, list) else []


def cancel_order(
    account_hash: str,
    order_id: str,
    *,
    budget_user_id: int | None = None,
) -> dict[str, Any]:
    """Cancel an order and return the normalized response payload."""
    client = get_schwab_client()
    response = guard_call(
        provider="schwab",
        operation="cancel_order",
        budget_user_id=budget_user_id,
        cost_per_call=_schwab_cost_per_call("cancel_order"),
        fn=_call_schwab_cancel_order,
        args=(client, account_hash, order_id),
    )
    return _response_as_dict(response)


def place_order(
    account_hash: str,
    order_spec: dict[str, Any],
    *,
    budget_user_id: int | None = None,
) -> dict[str, Any]:
    """Submit an order and return the normalized response payload."""
    client = get_schwab_client()
    response = guard_call(
        provider="schwab",
        operation="place_order",
        budget_user_id=budget_user_id,
        cost_per_call=_schwab_cost_per_call("place_order"),
        fn=_call_client_method,
        args=(client.place_order, account_hash, order_spec),
    )
    return _response_as_dict(response)


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
        health["warnings"].append("Token file missing. Run `python3 -m scripts.run_schwab login`.")
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
                "Refresh token appears expired. Run `python3 -m scripts.run_schwab login`."
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
                "Refresh token near expiry (<=1 day). Re-run `python3 -m scripts.run_schwab login` soon."
            )
    except Exception:
        pass

    return health


def invalidate_schwab_caches() -> None:
    """Clear in-memory client/hash caches."""
    global _account_hash_cache, _invalid_grant_cache
    _account_hash_cache = None
    _invalid_grant_cache = None
    _get_or_create_client.cache_clear()
    portfolio_logger.info("Cleared in-memory Schwab client/account-hash cache")
