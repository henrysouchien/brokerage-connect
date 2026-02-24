"""Brokerage package-local configuration loaded from environment variables."""

from __future__ import annotations

import os
from pathlib import Path

try:
    from dotenv import load_dotenv

    _pkg_dir = Path(__file__).resolve().parent
    load_dotenv(_pkg_dir / ".env", override=False)
    load_dotenv(_pkg_dir.parent / ".env", override=False)
except Exception:
    pass


def _int_env(name: str, default: int) -> int:
    raw = os.getenv(name)
    if raw is None:
        return default
    try:
        return int(raw)
    except (TypeError, ValueError):
        return default


IBKR_READONLY: bool = os.getenv("IBKR_READONLY", "false").lower() == "true"
IBKR_AUTHORIZED_ACCOUNTS: list[str] = [
    account.strip()
    for account in os.getenv("IBKR_AUTHORIZED_ACCOUNTS", "").split(",")
    if account.strip()
]
IBKR_GATEWAY_HOST: str = os.getenv("IBKR_GATEWAY_HOST", "127.0.0.1")
IBKR_GATEWAY_PORT: int = _int_env("IBKR_GATEWAY_PORT", 7496)

SCHWAB_APP_KEY: str = os.getenv("SCHWAB_APP_KEY", "")
SCHWAB_APP_SECRET: str = os.getenv("SCHWAB_APP_SECRET", "")
SCHWAB_TOKEN_PATH: str = os.path.expanduser(os.getenv("SCHWAB_TOKEN_PATH", "~/.schwab_token.json"))
SCHWAB_CALLBACK_URL: str = os.getenv("SCHWAB_CALLBACK_URL", "https://127.0.0.1:8182")

SNAPTRADE_CLIENT_ID: str = os.getenv("SNAPTRADE_CLIENT_ID", "")
SNAPTRADE_CONSUMER_KEY: str = os.getenv("SNAPTRADE_CONSUMER_KEY", "")
SNAPTRADE_ENVIRONMENT: str = os.getenv("SNAPTRADE_ENVIRONMENT", "production")

FRONTEND_BASE_URL: str = os.getenv("FRONTEND_BASE_URL", "http://localhost:3000")
