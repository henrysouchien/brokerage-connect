"""Brokerage package-local configuration loaded from environment variables."""

from __future__ import annotations

import os
from ibkr.config import (
    IBKR_AUTHORIZED_ACCOUNTS,
    IBKR_GATEWAY_HOST,
    IBKR_GATEWAY_PORT,
    IBKR_READONLY,
)

SCHWAB_APP_KEY: str = os.getenv("SCHWAB_APP_KEY", "")
SCHWAB_APP_SECRET: str = os.getenv("SCHWAB_APP_SECRET", "")
SCHWAB_TOKEN_PATH: str = os.path.expanduser(os.getenv("SCHWAB_TOKEN_PATH", "~/.schwab_token.json"))
SCHWAB_CALLBACK_URL: str = os.getenv("SCHWAB_CALLBACK_URL", "https://127.0.0.1:8182")

SNAPTRADE_CLIENT_ID: str = os.getenv("SNAPTRADE_CLIENT_ID", "")
SNAPTRADE_CONSUMER_KEY: str = os.getenv("SNAPTRADE_CONSUMER_KEY", "")
SNAPTRADE_ENVIRONMENT: str = os.getenv("SNAPTRADE_ENVIRONMENT", "production")

PLAID_CLIENT_ID: str = os.getenv("PLAID_CLIENT_ID", "")
PLAID_SECRET: str = os.getenv("PLAID_SECRET", "")
PLAID_ENV: str = os.getenv("PLAID_ENV", "production")
AWS_DEFAULT_REGION: str = os.getenv("AWS_DEFAULT_REGION", "us-east-1")
