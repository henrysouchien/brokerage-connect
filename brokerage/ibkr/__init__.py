from __future__ import annotations

from collections.abc import Callable
from importlib import import_module
import os

from brokerage.broker_adapter import BrokerAdapter


_LAZY_EXPORTS = {
    "IBKRBrokerAdapter": ("brokerage.ibkr.adapter", "IBKRBrokerAdapter"),
    "IBKRRelayAdapter": ("brokerage.ibkr.relay_adapter", "IBKRRelayAdapter"),
    "ibkr_to_common_status": ("brokerage.ibkr.adapter", "ibkr_to_common_status"),
    "IBKRClient": ("brokerage.ibkr.client", "IBKRClient"),
    "IBKRContractSpec": ("brokerage.ibkr.contract_spec", "IBKRContractSpec"),
    "get_ibkr_client": ("brokerage.ibkr.client", "get_ibkr_client"),
    "probe_ibkr_connection": ("brokerage.ibkr.connection", "probe_ibkr_connection"),
    "get_ibkr_connection_status": (
        "brokerage.ibkr.connection",
        "get_ibkr_connection_status",
    ),
    "fetch_flex_report": ("brokerage.ibkr.flex", "fetch_flex_report"),
    "fetch_ibkr_monthly_close": ("brokerage.ibkr.compat", "fetch_ibkr_monthly_close"),
    "fetch_ibkr_daily_close_futures": (
        "brokerage.ibkr.compat",
        "fetch_ibkr_daily_close_futures",
    ),
    "fetch_ibkr_daily_close_fx": ("brokerage.ibkr.compat", "fetch_ibkr_daily_close_fx"),
    "fetch_ibkr_daily_close_bond": (
        "brokerage.ibkr.compat",
        "fetch_ibkr_daily_close_bond",
    ),
    "fetch_ibkr_fx_monthly_close": (
        "brokerage.ibkr.compat",
        "fetch_ibkr_fx_monthly_close",
    ),
    "fetch_ibkr_bond_monthly_close": (
        "brokerage.ibkr.compat",
        "fetch_ibkr_bond_monthly_close",
    ),
    "fetch_ibkr_option_monthly_mark": (
        "brokerage.ibkr.compat",
        "fetch_ibkr_option_monthly_mark",
    ),
    "fetch_ibkr_option_snapshot_mark": (
        "brokerage.ibkr.compat",
        "fetch_ibkr_option_snapshot_mark",
    ),
    "fetch_ibkr_flex_trades": ("brokerage.ibkr.compat", "fetch_ibkr_flex_trades"),
    "get_ibkr_futures_fmp_map": ("brokerage.ibkr.compat", "get_ibkr_futures_fmp_map"),
    "get_ibkr_futures_exchanges": ("brokerage.ibkr.compat", "get_ibkr_futures_exchanges"),
    "get_futures_currency": ("brokerage.ibkr.compat", "get_futures_currency"),
}

__all__ = ["make_ibkr_adapter", *_LAZY_EXPORTS.keys()]


def make_ibkr_adapter(
    *,
    user_email: str,
    on_refresh: Callable[[str], None] | None = None,
    account_map: dict[str, str] | None = None,
    user_id: int | None = None,
) -> BrokerAdapter:
    """Single chokepoint for IBKR adapter construction."""
    transport = os.getenv("IBKR_TRANSPORT", "direct").strip().lower()
    if transport == "relay":
        if user_id is None:
            raise RuntimeError(
                "make_ibkr_adapter: IBKR_TRANSPORT=relay requires user_id parameter."
            )
        from .relay_adapter import IBKRRelayAdapter

        adapter = IBKRRelayAdapter(
            user_email=user_email,
            on_refresh=on_refresh,
            account_map=account_map,
        )
        adapter.bind_user_id(user_id)
        return adapter

    if transport == "oauth":
        # PR 4: surfaces the stub. Instantiation raises NotImplementedError.
        from .oauth_adapter import IBKROAuthAdapter

        return IBKROAuthAdapter(user_email=user_email, on_refresh=on_refresh)

    from .adapter import IBKRBrokerAdapter

    return IBKRBrokerAdapter(
        user_email=user_email,
        on_refresh=on_refresh,
        account_map=account_map,
    )


def __getattr__(name: str):
    if name in _LAZY_EXPORTS:
        module_name, attr_name = _LAZY_EXPORTS[name]
        module = import_module(module_name)
        value = getattr(module, attr_name)
        globals()[name] = value
        return value
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
