"""Plaid token storage helpers for AWS Secrets Manager."""

from __future__ import annotations

import json

from brokerage._logging import log_error, portfolio_logger

try:
    import boto3
    from botocore.exceptions import ClientError
except Exception:  # pragma: no cover - boto3 optional without [plaid] extras
    boto3 = None  # type: ignore[assignment]
    ClientError = Exception  # type: ignore[assignment,misc]


def _require_boto3() -> None:
    if boto3 is None:
        raise RuntimeError(
            "boto3 unavailable. Install optional dependency group 'plaid'."
        )


def store_plaid_token(
    user_id: str,
    institution: str,
    access_token: str,
    item_id: str,
    region_name: str,
) -> None:
    """Store or update a Plaid access token payload."""
    _require_boto3()
    secret_name = f"plaid/access_token/{user_id}/{institution.lower().replace(' ', '-')}"
    payload = {
        "access_token": access_token,
        "item_id": item_id,
        "institution": institution,
        "user_id": user_id,
    }

    session = boto3.session.Session()
    client = session.client("secretsmanager", region_name=region_name)

    try:
        client.put_secret_value(
            SecretId=secret_name,
            SecretString=json.dumps(payload),
        )
        print(f"🔁 Updated token for {user_id} at {institution}")
    except ClientError as exc:
        if exc.response["Error"]["Code"] == "ResourceNotFoundException":
            client.create_secret(
                Name=secret_name,
                SecretString=json.dumps(payload),
            )
            print(f"✅ Created token for {user_id} at {institution}")
        else:
            raise


def get_plaid_token(user_id: str, institution: str, region_name: str) -> dict:
    """Retrieve Plaid token payload from AWS Secrets Manager."""
    _require_boto3()
    secret_name = f"plaid/access_token/{user_id}/{institution.lower().replace(' ', '-')}"

    session = boto3.session.Session()
    client = session.client("secretsmanager", region_name=region_name)

    try:
        response = client.get_secret_value(SecretId=secret_name)
        return json.loads(response["SecretString"])
    except ClientError as exc:
        portfolio_logger.error(
            "Failed to get Plaid token for user %s at %s: %s",
            user_id,
            institution,
            exc,
        )
        raise


def get_plaid_token_by_item_id(user_id: str, item_id: str, region_name: str) -> dict:
    """Retrieve Plaid token payload by item_id for a user."""
    _require_boto3()
    token_paths = list_user_tokens(user_id, region_name)

    session = boto3.session.Session()
    client = session.client("secretsmanager", region_name=region_name)

    for path in token_paths:
        try:
            response = client.get_secret_value(SecretId=path)
            payload = json.loads(response["SecretString"])
            if payload.get("item_id") == item_id:
                return payload
        except ClientError as exc:
            if exc.response["Error"]["Code"] == "ResourceNotFoundException":
                continue
            raise

    raise KeyError(f"No Plaid token found for item_id={item_id}")


def list_user_tokens(user_id: str, region_name: str) -> list[str]:
    """List Plaid token secret names for a user."""
    _require_boto3()
    prefix = f"plaid/access_token/{user_id}/"

    session = boto3.session.Session()
    client = session.client("secretsmanager", region_name=region_name)
    paginator = client.get_paginator("list_secrets")

    tokens: list[str] = []
    for page in paginator.paginate():
        for secret in page.get("SecretList", []):
            name = secret.get("Name", "")
            if name.startswith(prefix):
                tokens.append(name)
    return tokens


def delete_plaid_user_tokens(user_id: str, region_name: str) -> bool:
    """Delete all Plaid token secrets for a user."""
    _require_boto3()
    try:
        tokens = list_user_tokens(user_id, region_name)

        if not tokens:
            portfolio_logger.info("✅ No Plaid tokens found for user %s", user_id)
            return True

        secrets_client = boto3.client("secretsmanager", region_name=region_name)
        deleted_count = 0

        for secret_name in tokens:
            try:
                secrets_client.delete_secret(
                    SecretId=secret_name,
                    ForceDeleteWithoutRecovery=True,
                )
                deleted_count += 1
                portfolio_logger.info("🗑️ Deleted Plaid token: %s", secret_name)
            except ClientError as exc:
                if exc.response["Error"]["Code"] == "ResourceNotFoundException":
                    portfolio_logger.warning("⚠️ Plaid token already deleted: %s", secret_name)
                else:
                    raise

        portfolio_logger.info("✅ Deleted %s Plaid tokens for user %s", deleted_count, user_id)
        return True
    except Exception as exc:
        log_error("plaid_delete_user", "token_deletion_failed", exc)
        portfolio_logger.error("❌ Failed to delete Plaid tokens for user %s: %s", user_id, exc)
        raise


__all__ = [
    "delete_plaid_user_tokens",
    "get_plaid_token",
    "get_plaid_token_by_item_id",
    "list_user_tokens",
    "store_plaid_token",
]
