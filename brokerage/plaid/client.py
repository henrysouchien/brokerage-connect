"""Plaid client bootstrap and pure API helper functions."""

from __future__ import annotations

import datetime as dt
import functools
import time
from typing import Any, Dict, Optional

try:
    from app_platform.api_budget import guard_call
except ModuleNotFoundError as e:
    # Only fall back when app_platform itself or its api_budget submodule is unavailable
    # (dist runtime). Re-raise if a transitive import inside app_platform.api_budget fails —
    # those represent monorepo bugs that must surface, not silently disable budget enforcement.
    if e.name not in {"app_platform", "app_platform.api_budget"}:
        raise
    def guard_call(*, fn, args=(), kwargs=None, **_):
        """No-op fallback when app_platform.api_budget isn't installed (dist runtime)."""
        return fn(*args, **(kwargs or {}))

from brokerage._logging import (
    log_critical_alert,
    log_error,
    log_service_health,
    plaid_logger,
)
from brokerage.config import PLAID_CLIENT_ID, PLAID_ENV, PLAID_SECRET
from brokerage._shared.api_budget_costs import COST_PER_CALL

_PLAID_IMPORT_ERROR: Exception | None = None
_PLAID_AVAILABLE = False

try:
    import certifi
    from plaid import ApiClient, Configuration, Environment
    from plaid.api import plaid_api
    from plaid.model.accounts_balance_get_request import AccountsBalanceGetRequest
    from plaid.model.country_code import CountryCode
    from plaid.model.institutions_get_by_id_request import InstitutionsGetByIdRequest
    from plaid.model.investments_holdings_get_request import InvestmentsHoldingsGetRequest
    from plaid.model.investments_transactions_get_request import InvestmentsTransactionsGetRequest
    from plaid.model.investments_transactions_get_request_options import (
        InvestmentsTransactionsGetRequestOptions,
    )
    from plaid.model.item_public_token_exchange_request import (
        ItemPublicTokenExchangeRequest,
    )
    from plaid.model.item_get_request import ItemGetRequest
    from plaid.model.link_token_create_request import LinkTokenCreateRequest
    from plaid.model.link_token_create_request_user import LinkTokenCreateRequestUser
    from plaid.model.link_token_get_request import LinkTokenGetRequest
    from plaid.model.products import Products

    _PLAID_AVAILABLE = True
except Exception as exc:  # pragma: no cover - plaid sdk optional in some environments
    _PLAID_IMPORT_ERROR = exc


def _require_plaid_sdk() -> None:
    if not _PLAID_AVAILABLE:
        raise RuntimeError(
            "Plaid SDK unavailable. Install optional dependency group 'plaid'."
        ) from _PLAID_IMPORT_ERROR


def create_client() -> Optional["plaid_api.PlaidApi"]:
    """Create and return a Plaid client, or None when config/sdk is unavailable."""
    if not _PLAID_AVAILABLE:
        plaid_logger.warning("⚠️ Plaid SDK not available: %s", _PLAID_IMPORT_ERROR)
        return None

    if not PLAID_CLIENT_ID or not PLAID_SECRET:
        plaid_logger.warning("⚠️ Missing PLAID_CLIENT_ID / PLAID_SECRET; Plaid client not initialized")
        return None

    host = getattr(Environment, PLAID_ENV.capitalize(), None)
    if host is None:
        plaid_logger.error("❌ Invalid PLAID_ENV=%s", PLAID_ENV)
        return None

    try:
        config = Configuration(
            host=host,
            api_key={"clientId": PLAID_CLIENT_ID, "secret": PLAID_SECRET},
            ssl_ca_cert=certifi.where(),
        )
        return plaid_api.PlaidApi(ApiClient(config))
    except Exception as exc:
        log_error("plaid_client", "create_client", exc)
        return None


@functools.lru_cache(maxsize=1)
def _get_or_create_client() -> Optional["plaid_api.PlaidApi"]:
    return create_client()


def _require_plaid_client() -> "plaid_api.PlaidApi":
    client = _get_or_create_client()
    if client is None:
        raise RuntimeError("Plaid client unavailable")
    return client


def _plaid_cost_per_call(operation: str) -> Any:
    return COST_PER_CALL.get(("plaid", operation), 0)


def create_hosted_link_token(
    user_id: str,
    redirect_uri: str = "https://yourapp.com/plaid/complete",
    webhook_uri: str = "https://yourapp.com/plaid/webhook",
    client_name: str = "Risk Analysis App",
    is_mobile_app: bool = False,
    *,
    budget_user_id: int | None = None,
) -> dict:
    """Create a hosted Plaid Link token for an end user."""
    _require_plaid_sdk()
    client = _require_plaid_client()

    req = LinkTokenCreateRequest(
        user=LinkTokenCreateRequestUser(client_user_id=user_id),
        client_name=client_name,
        products=[Products("investments")],
        country_codes=[CountryCode("US")],
        language="en",
        hosted_link={
            "completion_redirect_uri": redirect_uri,
            "is_mobile_app": is_mobile_app,
        },
        webhook=webhook_uri,
    )

    resp = guard_call(
        provider="plaid",
        operation="link_token_create",
        budget_user_id=budget_user_id,
        cost_per_call=_plaid_cost_per_call("link_token_create"),
        fn=client.link_token_create,
        args=(req,),
    )
    return {
        "link_token": resp.link_token,
        "hosted_link_url": resp.hosted_link_url,
    }


def create_update_link_token(
    access_token: str,
    user_id: str,
    redirect_uri: str = "https://yourapp.com/plaid/complete",
    webhook_uri: str = "https://yourapp.com/plaid/webhook",
    client_name: str = "Risk Analysis App",
    is_mobile_app: bool = False,
    *,
    budget_user_id: int | None = None,
) -> dict:
    """Create a hosted Plaid Link token in update mode for re-authentication."""
    _require_plaid_sdk()
    client = _require_plaid_client()

    req = LinkTokenCreateRequest(
        user=LinkTokenCreateRequestUser(client_user_id=user_id),
        client_name=client_name,
        access_token=access_token,
        country_codes=[CountryCode("US")],
        language="en",
        hosted_link={
            "completion_redirect_uri": redirect_uri,
            "is_mobile_app": is_mobile_app,
        },
        webhook=webhook_uri,
    )

    resp = guard_call(
        provider="plaid",
        operation="link_token_create",
        budget_user_id=budget_user_id,
        cost_per_call=_plaid_cost_per_call("link_token_create"),
        fn=client.link_token_create,
        args=(req,),
    )
    return {
        "link_token": resp.link_token,
        "hosted_link_url": resp.hosted_link_url,
    }


def _wait_for_public_token(
    link_token: str,
    *,
    timeout: int,
    poll: int,
    client: "plaid_api.PlaidApi",
    budget_user_id: int | None = None,
) -> str:
    """Poll Plaid for link-session completion and return the resulting public token."""
    deadline = dt.datetime.now().timestamp() + timeout
    while dt.datetime.now().timestamp() < deadline:
        resp = guard_call(
            provider="plaid",
            operation="link_token_get",
            budget_user_id=budget_user_id,
            cost_per_call=_plaid_cost_per_call("link_token_get"),
            fn=client.link_token_get,
            args=(LinkTokenGetRequest(link_token=link_token),),
        )
        sessions = getattr(resp, "link_sessions", None)
        if sessions:
            results = getattr(sessions[0], "results", None)
            add_results = getattr(results, "item_add_results", None) if results else None
            if add_results and len(add_results) > 0:
                return add_results[0].public_token
        time.sleep(poll)

    raise TimeoutError("Timed-out waiting for Plaid to finish.")


def wait_for_public_token(
    link_token: str,
    *,
    timeout: int = 300,
    poll: int = 10,
    budget_user_id: int | None = None,
) -> str:
    """Poll Plaid for link-session completion and return the resulting public token."""
    _require_plaid_sdk()
    return _wait_for_public_token(
        link_token,
        timeout=timeout,
        poll=poll,
        client=_require_plaid_client(),
        budget_user_id=budget_user_id,
    )


def _get_institution_info(
    *,
    access_token: str,
    client: "plaid_api.PlaidApi",
    country: str = "US",
    budget_user_id: int | None = None,
) -> tuple[str, str]:
    """Fetch ``(institution_name, institution_id)`` for a Plaid access token."""
    _require_plaid_sdk()

    item_rsp = guard_call(
        provider="plaid",
        operation="item_get",
        budget_user_id=budget_user_id,
        cost_per_call=_plaid_cost_per_call("item_get"),
        fn=client.item_get,
        args=(ItemGetRequest(access_token=access_token),),
    )
    inst_id = item_rsp.item.institution_id

    inst_rsp = guard_call(
        provider="plaid",
        operation="institutions_get_by_id",
        budget_user_id=budget_user_id,
        cost_per_call=_plaid_cost_per_call("institutions_get_by_id"),
        fn=client.institutions_get_by_id,
        args=(
            InstitutionsGetByIdRequest(
                institution_id=inst_id,
                country_codes=[CountryCode(country)],
            ),
        ),
    )
    inst_name = inst_rsp.institution.name
    return inst_name, inst_id


def get_institution_info(
    *,
    access_token: str,
    country: str = "US",
    budget_user_id: int | None = None,
) -> tuple[str, str]:
    """Fetch ``(institution_name, institution_id)`` for a Plaid access token."""
    _require_plaid_sdk()
    return _get_institution_info(
        access_token=access_token,
        client=_require_plaid_client(),
        country=country,
        budget_user_id=budget_user_id,
    )


def _fetch_plaid_holdings(
    access_token: str,
    *,
    client: "plaid_api.PlaidApi",
    budget_user_id: int | None = None,
    item_id: str | None = None,
) -> Dict[str, Any]:
    """Fetch investment holdings payload from Plaid."""
    _require_plaid_sdk()

    start_time = time.time()
    token_suffix = access_token[-6:] if access_token else "unknown"
    plaid_req_id = f"pld_h_{int(start_time * 1000)}_{token_suffix}"
    plaid_logger.info(
        "🔄 Plaid holdings API call started (plaid_req_id=%s, token_suffix=%s)",
        plaid_req_id,
        token_suffix,
    )

    request = InvestmentsHoldingsGetRequest(access_token=access_token)
    try:
        response = guard_call(
            provider="plaid",
            operation="investments_holdings_get",
            budget_user_id=budget_user_id,
            item_id=item_id,
            cost_per_call=_plaid_cost_per_call("investments_holdings_get"),
            fn=client.investments_holdings_get,
            args=(request,),
        )
        response_data = response.to_dict()
        response_time = time.time() - start_time

        holdings_count = len(response_data.get("holdings", []))
        securities_count = len(response_data.get("securities", []))
        request_id = response_data.get("request_id")

        plaid_logger.info(
            "✅ Plaid holdings API call succeeded (plaid_req_id=%s, token_suffix=%s, holdings=%s, securities=%s, request_id=%s, response_time_ms=%.1f)",
            plaid_req_id,
            token_suffix,
            holdings_count,
            securities_count,
            request_id,
            response_time * 1000,
        )
        return response_data
    except Exception as exc:
        response_time = time.time() - start_time
        log_error("plaid_loader", "investments_holdings_get", exc, correlation_id=plaid_req_id)
        log_service_health(
            "Plaid",
            "down",
            response_time=response_time,
            error_details={
                "endpoint": "investments_holdings_get",
                "plaid_req_id": plaid_req_id,
                "token_suffix": token_suffix,
                "error": str(exc),
            },
        )
        log_critical_alert(
            "plaid_api_failure",
            "high",
            "Plaid investments_holdings_get failed",
            "Retry with backoff and check Plaid API status",
            details={"plaid_req_id": plaid_req_id, "token_suffix": token_suffix, "error": str(exc)},
        )
        plaid_logger.error(
            "❌ Plaid holdings API call failed (plaid_req_id=%s, token_suffix=%s, response_time_ms=%.1f): %s",
            plaid_req_id,
            token_suffix,
            response_time * 1000,
            exc,
        )
        raise


def fetch_plaid_holdings(
    access_token: str,
    *,
    budget_user_id: int | None = None,
    item_id: str | None = None,
) -> Dict[str, Any]:
    """Fetch investment holdings payload from Plaid."""
    return _fetch_plaid_holdings(
        access_token,
        client=_require_plaid_client(),
        budget_user_id=budget_user_id,
        item_id=item_id,
    )


# Intentionally retained for manual/debug use; the Plaid holdings refresh hot path does not call this.
def _fetch_plaid_balances(
    access_token: str,
    *,
    client: "plaid_api.PlaidApi",
    budget_user_id: int | None = None,
    item_id: str | None = None,
) -> Dict[str, Any]:
    """Fetch account-balance payload from Plaid."""
    _require_plaid_sdk()

    start_time = time.time()
    token_suffix = access_token[-6:] if access_token else "unknown"
    plaid_req_id = f"pld_b_{int(start_time * 1000)}_{token_suffix}"
    plaid_logger.info(
        "🔄 Plaid balances API call started (plaid_req_id=%s, token_suffix=%s)",
        plaid_req_id,
        token_suffix,
    )

    request = AccountsBalanceGetRequest(access_token=access_token)
    try:
        response = guard_call(
            provider="plaid",
            operation="accounts_balance_get",
            budget_user_id=budget_user_id,
            item_id=item_id,
            cost_per_call=_plaid_cost_per_call("accounts_balance_get"),
            fn=client.accounts_balance_get,
            args=(request,),
        )
        response_data = response.to_dict()
        response_time = time.time() - start_time

        accounts_count = len(response_data.get("accounts", []))
        request_id = response_data.get("request_id")

        plaid_logger.info(
            "✅ Plaid balances API call succeeded (plaid_req_id=%s, token_suffix=%s, accounts=%s, request_id=%s, response_time_ms=%.1f)",
            plaid_req_id,
            token_suffix,
            accounts_count,
            request_id,
            response_time * 1000,
        )
        return response_data
    except Exception as exc:
        response_time = time.time() - start_time
        log_error("plaid_loader", "accounts_balance_get", exc, correlation_id=plaid_req_id)
        log_service_health(
            "Plaid",
            "down",
            response_time=response_time,
            error_details={
                "endpoint": "accounts_balance_get",
                "plaid_req_id": plaid_req_id,
                "token_suffix": token_suffix,
                "error": str(exc),
            },
        )
        log_critical_alert(
            "plaid_api_failure",
            "high",
            "Plaid accounts_balance_get failed",
            "Retry with backoff and check Plaid API status",
            details={"plaid_req_id": plaid_req_id, "token_suffix": token_suffix, "error": str(exc)},
        )
        plaid_logger.error(
            "❌ Plaid balances API call failed (plaid_req_id=%s, token_suffix=%s, response_time_ms=%.1f): %s",
            plaid_req_id,
            token_suffix,
            response_time * 1000,
            exc,
        )
        raise


def fetch_plaid_balances(
    access_token: str,
    *,
    budget_user_id: int | None = None,
    item_id: str | None = None,
) -> Dict[str, Any]:
    """Fetch account-balance payload from Plaid."""
    return _fetch_plaid_balances(
        access_token,
        client=_require_plaid_client(),
        budget_user_id=budget_user_id,
        item_id=item_id,
    )


def exchange_public_token(
    public_token: str,
    *,
    budget_user_id: int | None = None,
) -> Dict[str, Any]:
    """Exchange a Plaid public token for a persistent access token."""
    _require_plaid_sdk()

    client = _require_plaid_client()
    response = guard_call(
        provider="plaid",
        operation="item_public_token_exchange",
        budget_user_id=budget_user_id,
        cost_per_call=_plaid_cost_per_call("item_public_token_exchange"),
        fn=client.item_public_token_exchange,
        args=(ItemPublicTokenExchangeRequest(public_token=public_token),),
    )
    return response.to_dict()


def _normalize_investments_transaction_options(options: Any) -> Any:
    if options is None or InvestmentsTransactionsGetRequestOptions is None:
        return options
    if isinstance(options, InvestmentsTransactionsGetRequestOptions):
        return options
    if isinstance(options, dict):
        return InvestmentsTransactionsGetRequestOptions(**options)
    raise TypeError("options must be a dict, InvestmentsTransactionsGetRequestOptions, or None")


def get_investments_transactions(
    access_token: str,
    *,
    start_date: Any,
    end_date: Any,
    options: Any = None,
    budget_user_id: int | None = None,
    item_id: str | None = None,
) -> Dict[str, Any]:
    """Fetch investment transactions as a plain dict payload."""
    _require_plaid_sdk()

    client = _require_plaid_client()
    request = InvestmentsTransactionsGetRequest(
        access_token=access_token,
        start_date=start_date,
        end_date=end_date,
        options=_normalize_investments_transaction_options(options),
    )
    response = guard_call(
        provider="plaid",
        operation="investments_transactions_get",
        budget_user_id=budget_user_id,
        item_id=item_id,
        cost_per_call=_plaid_cost_per_call("investments_transactions_get"),
        fn=client.investments_transactions_get,
        args=(request,),
    )
    return response.to_dict()


def get_item(
    access_token: str,
    *,
    budget_user_id: int | None = None,
) -> Dict[str, Any]:
    """Fetch a Plaid item as a plain dict payload."""
    _require_plaid_sdk()

    client = _require_plaid_client()
    response = guard_call(
        provider="plaid",
        operation="item_get",
        budget_user_id=budget_user_id,
        cost_per_call=_plaid_cost_per_call("item_get"),
        fn=client.item_get,
        args=(ItemGetRequest(access_token=access_token),),
    )
    return response.to_dict()


__all__ = [
    "create_client",
    "create_hosted_link_token",
    "create_update_link_token",
    "exchange_public_token",
    "fetch_plaid_balances",
    "fetch_plaid_holdings",
    "get_institution_info",
    "get_investments_transactions",
    "get_item",
    "wait_for_public_token",
]
