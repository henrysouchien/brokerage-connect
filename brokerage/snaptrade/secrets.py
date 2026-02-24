"""SnapTrade secret management helpers."""

from __future__ import annotations

import json
from datetime import datetime
from typing import Dict, Optional

import boto3
from botocore.exceptions import BotoCoreError, ClientError

from brokerage._logging import log_error, portfolio_logger
from brokerage.config import SNAPTRADE_CLIENT_ID, SNAPTRADE_CONSUMER_KEY, SNAPTRADE_ENVIRONMENT


def store_snaptrade_app_credentials(
    client_id: str,
    consumer_key: str,
    environment: str,
    region_name: str = "us-east-1",
) -> None:
    """Store app-level credentials in AWS Secrets Manager."""
    secret_name = f"snaptrade/app_credentials/{environment}"
    secret_value = {
        "client_id": client_id,
        "consumer_key": consumer_key,
        "environment": environment,
    }

    try:
        secrets_client = boto3.client("secretsmanager", region_name=region_name)
        try:
            secrets_client.create_secret(
                Name=secret_name,
                SecretString=json.dumps(secret_value),
                Description=f"SnapTrade app credentials for {environment}",
            )
            portfolio_logger.info("✅ Created new SnapTrade app credentials for %s", environment)
        except ClientError as e:
            if e.response["Error"]["Code"] == "ResourceExistsException":
                secrets_client.put_secret_value(
                    SecretId=secret_name,
                    SecretString=json.dumps(secret_value),
                )
                portfolio_logger.info("✅ Updated SnapTrade app credentials for %s", environment)
            else:
                raise
    except Exception as e:
        log_error("snaptrade_secrets", "store_app_credentials", e)
        raise


def get_snaptrade_app_credentials(region_name: str = "us-east-1") -> Dict[str, str]:
    """Retrieve app-level credentials from env first, then AWS Secrets Manager."""
    if SNAPTRADE_CLIENT_ID and SNAPTRADE_CONSUMER_KEY:
        portfolio_logger.info("✅ Using SnapTrade credentials from environment variables")
        return {
            "client_id": SNAPTRADE_CLIENT_ID,
            "consumer_key": SNAPTRADE_CONSUMER_KEY,
            "environment": SNAPTRADE_ENVIRONMENT,
        }

    portfolio_logger.info("🔍 Environment variables not found, trying AWS Secrets Manager...")
    secret_name = f"snaptrade/app_credentials/{SNAPTRADE_ENVIRONMENT}"
    try:
        secrets_client = boto3.client("secretsmanager", region_name=region_name)
        response = secrets_client.get_secret_value(SecretId=secret_name)
        credentials = json.loads(response["SecretString"])
        portfolio_logger.info("✅ Using SnapTrade credentials from AWS Secrets Manager")
        return credentials
    except ClientError as e:
        if e.response["Error"]["Code"] == "ResourceNotFoundException":
            portfolio_logger.error("❌ AWS Secret '%s' not found", secret_name)
        else:
            portfolio_logger.error("❌ AWS Secrets Manager error: %s", e)
        log_error("snaptrade_secrets", "get_app_credentials", e)
        raise Exception(
            "SnapTrade credentials not found in environment variables or AWS Secrets Manager"
        ) from e


def store_snaptrade_user_secret(
    user_email: str,
    user_secret: str,
    region_name: str = "us-east-1",
) -> None:
    """Store user-level SnapTrade secret in AWS Secrets Manager."""
    secret_name = f"snaptrade/user_secret/{user_email}"
    secret_value = {
        "user_email": user_email,
        "user_secret": user_secret,
        "created_at": datetime.now().isoformat(),
    }

    try:
        secrets_client = boto3.client("secretsmanager", region_name=region_name)
        try:
            secrets_client.create_secret(
                Name=secret_name,
                SecretString=json.dumps(secret_value),
                Description=f"SnapTrade user secret for {user_email}",
            )
            portfolio_logger.info(
                "✅ Created new SnapTrade user secret for user %s in AWS Secrets Manager",
                user_email,
            )
        except ClientError as e:
            if e.response["Error"]["Code"] == "ResourceExistsException":
                secrets_client.put_secret_value(
                    SecretId=secret_name,
                    SecretString=json.dumps(secret_value),
                )
                portfolio_logger.info(
                    "✅ Updated SnapTrade user secret for user %s in AWS Secrets Manager",
                    user_email,
                )
            else:
                raise
    except Exception as e:
        portfolio_logger.warning("⚠️ Could not store user secret in AWS Secrets Manager: %s", e)
        portfolio_logger.warning("💡 For production, ensure AWS credentials are configured")
        log_error("snaptrade_secrets", "store_user_secret", e)
        raise


def get_snaptrade_user_secret(
    user_email: str,
    region_name: str = "us-east-1",
) -> Optional[str]:
    """Retrieve user-level secret; returns None only when not found."""
    secret_name = f"snaptrade/user_secret/{user_email}"
    try:
        secrets_client = boto3.client("secretsmanager", region_name=region_name)
        response = secrets_client.get_secret_value(SecretId=secret_name)
        secret_data = json.loads(response["SecretString"])
        return secret_data.get("user_secret")
    except ClientError as e:
        if e.response["Error"]["Code"] == "ResourceNotFoundException":
            return None
        portfolio_logger.warning("⚠️ AWS Secrets Manager error when retrieving user secret: %s", e)
        log_error("snaptrade_secrets", "get_user_secret", e)
        raise RuntimeError(
            f"Failed to retrieve SnapTrade user secret from AWS Secrets Manager: {e}"
        ) from e
    except BotoCoreError as e:
        portfolio_logger.warning("⚠️ Could not retrieve user secret from AWS: %s", e)
        log_error("snaptrade_secrets", "get_user_secret", e)
        raise RuntimeError(
            f"Failed to retrieve SnapTrade user secret from AWS Secrets Manager: {e}"
        ) from e


def delete_snaptrade_user_secret(user_email: str, region_name: str = "us-east-1") -> None:
    """Delete user-level secret from AWS Secrets Manager."""
    secret_name = f"snaptrade/user_secret/{user_email}"
    try:
        secrets_client = boto3.client("secretsmanager", region_name=region_name)
        secrets_client.delete_secret(SecretId=secret_name, RecoveryWindowInDays=7)
        portfolio_logger.info("✅ Deleted SnapTrade user secret for user %s", user_email)
    except ClientError as e:
        if e.response["Error"]["Code"] == "ResourceNotFoundException":
            portfolio_logger.info("ℹ️ SnapTrade user secret not found for user %s", user_email)
            return
        log_error("snaptrade_secrets", "delete_user_secret", e)
        raise


__all__ = [
    "delete_snaptrade_user_secret",
    "get_snaptrade_app_credentials",
    "get_snaptrade_user_secret",
    "store_snaptrade_app_credentials",
    "store_snaptrade_user_secret",
]
