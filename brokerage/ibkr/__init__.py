__all__ = ["IBKRBrokerAdapter", "IBKRRelayAdapter", "ibkr_to_common_status"]


def __getattr__(name: str):
    if name in {"IBKRBrokerAdapter", "ibkr_to_common_status"}:
        from brokerage.ibkr.adapter import IBKRBrokerAdapter, ibkr_to_common_status

        return {
            "IBKRBrokerAdapter": IBKRBrokerAdapter,
            "ibkr_to_common_status": ibkr_to_common_status,
        }[name]
    if name == "IBKRRelayAdapter":
        from brokerage.ibkr.relay_adapter import IBKRRelayAdapter

        return IBKRRelayAdapter
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
