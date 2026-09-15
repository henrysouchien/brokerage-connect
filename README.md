# brokerage-connect

**Status:** CURRENT / ACTIVE REFERENCE
**Last reviewed:** 2026-07-29
**Role:** Package entrypoint for brokerage-connect (install, supported integrations, BrokerAdapter sketch).


Unified Python interface for brokerage APIs. One abstract adapter, multiple brokers.

## Supported Integrations

| Integration | Extra | Status |
|--------|-------|--------|
| **Schwab** | `pip install brokerage-connect[schwab]` | Token-based auth via `schwab-py` (`BrokerAdapter`) |
| **SnapTrade** | `pip install brokerage-connect[snaptrade]` | OAuth connection flow (`BrokerAdapter`) |
| **IBKR** | `pip install brokerage-connect[ibkr]` | Trade adapter and IBKR client modules (`BrokerAdapter`) |
| **Plaid** | `pip install brokerage-connect[plaid]` | Investments + account data APIs via `plaid-python` (not a `BrokerAdapter` trade implementation) |

## Install

```bash
pip install brokerage-connect

# With broker-specific dependencies:
pip install brokerage-connect[schwab]
pip install brokerage-connect[schwab,plaid]
pip install brokerage-connect[schwab,ibkr]
pip install brokerage-connect[plaid]
```

## Quick Start

```python
from brokerage import BrokerAdapter

# Every trade broker adapter implements the same interface (sketch; see brokerage/broker_adapter.py):
class MyBroker(BrokerAdapter):
    provider_name = "my_broker"

    def owns_account(self, account_id: str) -> bool: ...
    def list_accounts(self): ...
    def search_symbol(self, account_id, ticker, currency: str): ...
    def preview_order(
        self, account_id, ticker, side, quantity, order_type, time_in_force,
        currency: str, limit_price=None, stop_price=None, symbol_id=None,
    ): ...
    def place_order(self, account_id, order_params): ...
    def get_orders(self, account_id, state="all", days=30): ...
    def cancel_order(self, account_id, order_id): ...
    def get_account_balance(self, account_id): ...
    def refresh_after_trade(self, account_id): ...
    # Full ABC also requires: fetch_market_snapshot, get_live_positions,
    # query_open_orders, query_completed_orders, preview_roll/place_roll,
    # preview_multileg_option/place_multileg_option
```

## Architecture

- **`BrokerAdapter`** — abstract base class defining the trade interface
- **`trade_objects`** — shared dataclasses (`OrderResult`, `OrderPreview`, `OrderStatus`, etc.)
- **`schwab/`**, **`snaptrade/`**, **`ibkr/`**, **`plaid/`** — broker/provider integrations
- **`futures/`** — futures contract specs, notionals, pricing helpers, and source adapters
- **`config.py`** — broker configuration and credential loading via environment variables

## License

PolyForm-Noncommercial-1.0.0
