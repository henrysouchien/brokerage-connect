"""Brokerage package-local configuration loaded from environment variables."""

from __future__ import annotations

import os

try:
    from pathlib import Path
    from dotenv import load_dotenv

    _pkg_dir = Path(__file__).resolve().parent
    load_dotenv(_pkg_dir.parent.parent / ".env", override=False)
except Exception:
    # Keep imports resilient when python-dotenv is unavailable.
    pass


def _int_env(name: str, default: int) -> int:
    raw = os.getenv(name)
    if raw is None:
        return default
    try:
        return int(raw)
    except (TypeError, ValueError):
        return default


# Cross-reference: ibkr/config.py:41-58.
IBKR_GATEWAY_HOST: str = os.getenv("IBKR_GATEWAY_HOST", "127.0.0.1")
IBKR_GATEWAY_PORT: int = _int_env("IBKR_GATEWAY_PORT", 7496)
IBKR_READONLY: bool = os.getenv("IBKR_READONLY", "false").lower() == "true"
IBKR_AUTHORIZED_ACCOUNTS: list[str] = [
    account.strip()
    for account in os.getenv("IBKR_AUTHORIZED_ACCOUNTS", "").split(",")
    if account.strip()
]

SCHWAB_APP_KEY: str = os.getenv("SCHWAB_APP_KEY", "")
SCHWAB_APP_SECRET: str = os.getenv("SCHWAB_APP_SECRET", "")
SCHWAB_TOKEN_PATH: str = os.path.expanduser(os.getenv("SCHWAB_TOKEN_PATH", "~/.schwab_token.json"))
SCHWAB_CALLBACK_URL: str = os.getenv("SCHWAB_CALLBACK_URL", "https://127.0.0.1:8182")
SCHWAB_SSL_CERT_PATH: str = os.path.expanduser("~/.schwab_auth/127.0.0.1.pem")
SCHWAB_SSL_KEY_PATH: str = os.path.expanduser("~/.schwab_auth/127.0.0.1-key.pem")

SNAPTRADE_CLIENT_ID: str = os.getenv("SNAPTRADE_CLIENT_ID", "")
SNAPTRADE_CONSUMER_KEY: str = os.getenv("SNAPTRADE_CONSUMER_KEY", "")
SNAPTRADE_ENVIRONMENT: str = os.getenv("SNAPTRADE_ENVIRONMENT", "production")

PLAID_CLIENT_ID: str = os.getenv("PLAID_CLIENT_ID", "")
PLAID_SECRET: str = os.getenv("PLAID_SECRET", "")
PLAID_ENV: str = os.getenv("PLAID_ENV", "production")
AWS_DEFAULT_REGION: str = os.getenv("AWS_DEFAULT_REGION", "us-east-1")
