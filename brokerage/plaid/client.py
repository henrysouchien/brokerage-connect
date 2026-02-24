"""Plaid client bootstrap and pure API helper functions."""

from __future__ import annotations

import datetime as dt
import time
from typing import Any, Dict, Optional

from brokerage._logging import (
    log_critical_alert,
    log_error,
    log_service_health,
    plaid_logger,
)
from brokerage.config import PLAID_CLIENT_ID, PLAID_ENV, PLAID_SECRET

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


def create_hosted_link_token(
    client: "plaid_api.PlaidApi",
    user_id: str,
    redirect_uri: str = "https://yourapp.com/plaid/complete",
    webhook_uri: str = "https://yourapp.com/plaid/webhook",
    client_name: str = "Risk Analysis App",
    is_mobile_app: bool = False,
) -> dict:
    """Create a hosted Plaid Link token for an end user."""
    _require_plaid_sdk()

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

    resp = client.link_token_create(req)
    return {
        "link_token": resp.link_token,
        "hosted_link_url": resp.hosted_link_url,
    }


def wait_for_public_token(
    link_token: str,
    timeout: int = 300,
    poll: int = 10,
    client: Optional["plaid_api.PlaidApi"] = None,
) -> str:
    """Poll Plaid for link-session completion and return the resulting public token."""
    _require_plaid_sdk()

    if client is None:
        # Fall back to module-level singleton
        client = globals().get("client")
    if client is None:
        raise RuntimeError("Plaid client unavailable")

    deadline = dt.datetime.now().timestamp() + timeout
    while dt.datetime.now().timestamp() < deadline:
        resp = client.link_token_get(LinkTokenGetRequest(link_token=link_token))
        sessions = getattr(resp, "link_sessions", None)
        if sessions:
            return sessions[0].results.item_add_results[0].public_token
        time.sleep(poll)

    raise TimeoutError("Timed-out waiting for Plaid to finish.")


def get_institution_info(
    *,
    access_token: str,
    client: "plaid_api.PlaidApi",
    country: str = "US",
) -> tuple[str, str]:
    """Fetch ``(institution_name, institution_id)`` for a Plaid access token."""
    _require_plaid_sdk()

    item_rsp = client.item_get(ItemGetRequest(access_token=access_token))
    inst_id = item_rsp.item.institution_id

    inst_rsp = client.institutions_get_by_id(
        InstitutionsGetByIdRequest(
            institution_id=inst_id,
            country_codes=[CountryCode(country)],
        )
    )
    inst_name = inst_rsp.institution.name
    return inst_name, inst_id


def fetch_plaid_holdings(access_token: str, client: "plaid_api.PlaidApi") -> Dict[str, Any]:
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
        response = client.investments_holdings_get(request)
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


def fetch_plaid_balances(access_token: str, client: "plaid_api.PlaidApi") -> Dict[str, Any]:
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
        response = client.accounts_balance_get(request)
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


client = create_client()


__all__ = [
    "client",
    "create_client",
    "create_hosted_link_token",
    "fetch_plaid_balances",
    "fetch_plaid_holdings",
    "get_institution_info",
    "wait_for_public_token",
]
