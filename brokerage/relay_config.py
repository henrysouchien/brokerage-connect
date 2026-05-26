"""Configuration validation for gateway relay transports."""

from __future__ import annotations

import os
from collections.abc import Mapping
from urllib.parse import urlparse


PROD_LOOPBACK_GATEWAY_URL = "http://127.0.0.1:8001"
_LOOPBACK_HOSTS = {"localhost", "127.0.0.1", "::1"}


def _env(environ: Mapping[str, str] | None, key: str, default: str = "") -> str:
    source = os.environ if environ is None else environ
    return str(source.get(key, default) or "").strip()


def ibkr_transport(environ: Mapping[str, str] | None = None) -> str:
    return _env(environ, "IBKR_TRANSPORT", "direct").lower()


def is_ibkr_relay_enabled(environ: Mapping[str, str] | None = None) -> bool:
    return ibkr_transport(environ) == "relay"


def normalized_gateway_url(environ: Mapping[str, str] | None = None) -> str:
    return _env(environ, "GATEWAY_URL").rstrip("/")


def validate_ibkr_relay_env(environ: Mapping[str, str] | None = None) -> list[str]:
    """Return human-readable relay config errors, or an empty list when valid."""
    if not is_ibkr_relay_enabled(environ):
        return []

    errors: list[str] = []
    environment = _env(environ, "ENVIRONMENT", "development").lower()
    gateway_url = normalized_gateway_url(environ)
    service_token = _env(environ, "IBKR_RELAY_INTERNAL_TOKEN")

    if not service_token:
        errors.append("IBKR_RELAY_INTERNAL_TOKEN is required when IBKR_TRANSPORT=relay")
    if not gateway_url:
        errors.append("GATEWAY_URL is required when IBKR_TRANSPORT=relay")
        return errors

    parsed = urlparse(gateway_url)
    try:
        port = parsed.port
    except ValueError as exc:
        errors.append(f"GATEWAY_URL has an invalid port: {exc}")
        return errors

    scheme = (parsed.scheme or "").lower()
    host = (parsed.hostname or "").lower()
    if scheme not in {"http", "https"} or not host:
        errors.append("GATEWAY_URL must be an absolute http(s) URL when IBKR_TRANSPORT=relay")
        return errors

    if environment == "production" and host in _LOOPBACK_HOSTS:
        if scheme != "http" or port != 8001:
            errors.append(
                "Production IBKR relay must use "
                f"GATEWAY_URL={PROD_LOOPBACK_GATEWAY_URL} for the co-located gateway; "
                f"got {gateway_url!r}"
            )

    return errors


def assert_valid_ibkr_relay_env(environ: Mapping[str, str] | None = None) -> None:
    errors = validate_ibkr_relay_env(environ)
    if errors:
        raise RuntimeError("; ".join(errors))
