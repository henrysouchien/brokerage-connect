"""Plaid connection teardown helpers."""

from __future__ import annotations

from app_platform.api_budget import guard_call
from brokerage._logging import log_error, portfolio_logger
from brokerage.plaid.client import _require_plaid_client
from config.api_budget_costs import COST_PER_CALL

try:
    from plaid.api import plaid_api
    from plaid.model.item_remove_request import ItemRemoveRequest
except Exception:  # pragma: no cover - plaid sdk optional in some environments
    plaid_api = None  # type: ignore[assignment]
    ItemRemoveRequest = None  # type: ignore[assignment]

def _remove_plaid_connection(
    access_token: str,
    *,
    client: "plaid_api.PlaidApi",
    budget_user_id: int | None = None,
) -> bool:
    """Revoke a Plaid item using its access token."""
    if ItemRemoveRequest is None:
        raise RuntimeError("Plaid SDK unavailable. Install optional dependency group 'plaid'.")

    try:
        request = ItemRemoveRequest(access_token=access_token)
        guard_call(
            provider="plaid",
            operation="item_remove",
            budget_user_id=budget_user_id,
            cost_per_call=COST_PER_CALL.get(("plaid", "item_remove"), 0),
            fn=client.item_remove,
            args=(request,),
        )
        portfolio_logger.info("✅ Successfully removed Plaid item")
        return True
    except Exception as exc:
        log_error("plaid_remove_connection", "item_remove_failed", exc)
        portfolio_logger.error("❌ Failed to remove Plaid item: %s", exc)
        raise


def remove_plaid_connection(
    access_token: str,
    *,
    budget_user_id: int | None = None,
) -> bool:
    """Revoke a Plaid item using its access token."""
    return _remove_plaid_connection(
        access_token,
        client=_require_plaid_client(),
        budget_user_id=budget_user_id,
    )


def remove_plaid_institution(
    access_token: str,
    institution_slug: str,
    dry_run: bool = True,
    *,
    budget_user_id: int | None = None,
) -> dict:
    """Revoke one Plaid institution using a caller-provided access token."""
    if not access_token:
        raise ValueError("access_token is required")

    result = {
        "institution_slug": institution_slug,
        "plaid_removed": False,
        "revoked": 0,
        "dry_run": dry_run,
    }

    if dry_run:
        portfolio_logger.info(
            "🔍 DRY RUN: Would remove Plaid institution '%s'",
            institution_slug,
        )
        return result

    remove_plaid_connection(access_token, budget_user_id=budget_user_id)
    result["plaid_removed"] = True
    result["revoked"] = 1
    portfolio_logger.info("✅ Revoked Plaid connection for %s", institution_slug)

    return result


__all__ = [
    "remove_plaid_connection",
    "remove_plaid_institution",
]
