"""Plaid connection teardown helpers."""

from __future__ import annotations

import json

from brokerage._logging import log_error, portfolio_logger

try:
    import boto3
except Exception:  # pragma: no cover - boto3 optional without [plaid] extras
    boto3 = None  # type: ignore[assignment]
from brokerage.plaid.secrets import list_user_tokens

try:
    from plaid.api import plaid_api
    from plaid.model.item_remove_request import ItemRemoveRequest
except Exception:  # pragma: no cover - plaid sdk optional in some environments
    plaid_api = None  # type: ignore[assignment]
    ItemRemoveRequest = None  # type: ignore[assignment]


def _require_boto3() -> None:
    if boto3 is None:
        raise RuntimeError(
            "boto3 unavailable. Install optional dependency group 'plaid'."
        )


def remove_plaid_connection(access_token: str, client: "plaid_api.PlaidApi") -> bool:
    """Revoke a Plaid item using its access token."""
    if ItemRemoveRequest is None:
        raise RuntimeError("Plaid SDK unavailable. Install optional dependency group 'plaid'.")

    try:
        request = ItemRemoveRequest(access_token=access_token)
        client.item_remove(request)
        portfolio_logger.info("✅ Successfully removed Plaid item")
        return True
    except Exception as exc:
        log_error("plaid_remove_connection", "item_remove_failed", exc)
        portfolio_logger.error("❌ Failed to remove Plaid item: %s", exc)
        raise


def remove_plaid_institution(
    user_id: str,
    institution_slug: str,
    region_name: str,
    client: "plaid_api.PlaidApi",
    dry_run: bool = True,
) -> dict:
    """Remove one institution: revoke Plaid token and delete associated AWS secret."""
    _require_boto3()
    secrets = list_user_tokens(user_id, region_name)
    matches = [secret_name for secret_name in secrets if institution_slug in secret_name]

    if len(matches) == 0:
        raise ValueError(f"No Plaid secret found matching '{institution_slug}' for user {user_id}")
    if len(matches) > 1:
        raise ValueError(
            f"Multiple Plaid secrets match '{institution_slug}': {matches}. "
            "Expected exactly one match — aborting for safety."
        )

    secret_name = matches[0]
    result = {
        "secret_name": secret_name,
        "matched_secrets": matches,
        "plaid_removed": False,
        "secret_deleted": False,
        "revoked": 0,
        "deleted": 0,
        "dry_run": dry_run,
    }

    if dry_run:
        portfolio_logger.info(
            "🔍 DRY RUN: Would remove Plaid institution '%s' (secret: %s)",
            institution_slug,
            secret_name,
        )
        return result

    sm_client = boto3.client("secretsmanager", region_name=region_name)
    secret_response = sm_client.get_secret_value(SecretId=secret_name)
    token_data = json.loads(secret_response["SecretString"])
    access_token = token_data.get("access_token")

    if not access_token:
        raise ValueError(f"No access_token found in secret {secret_name}")

    remove_plaid_connection(access_token, client)
    result["plaid_removed"] = True
    result["revoked"] = 1
    portfolio_logger.info("✅ Revoked Plaid connection for %s", institution_slug)

    sm_client.delete_secret(SecretId=secret_name, RecoveryWindowInDays=7)
    result["secret_deleted"] = True
    result["deleted"] = 1
    portfolio_logger.info("✅ Deleted secret %s (recoverable for 7 days)", secret_name)

    return result


__all__ = [
    "remove_plaid_connection",
    "remove_plaid_institution",
]
