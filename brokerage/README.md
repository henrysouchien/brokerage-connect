# brokerage-connect

Unified Python interface for brokerage APIs. One abstract adapter, multiple brokers.

## Supported Brokers

| Broker | Extra | Status |
|--------|-------|--------|
| **Schwab** | `pip install brokerage-connect[schwab]` | Token-based auth via `schwab-py` |
| **SnapTrade** | `pip install brokerage-connect[snaptrade]` | OAuth connection flow |
| **IBKR** | `pip install brokerage-connect[ibkr]` | Gateway/TWS via `ib-async` |
| **Plaid** | `pip install brokerage-connect[plaid]` | Investments + account APIs via `plaid-python` |

## Install

```bash
pip install brokerage-connect

# With broker-specific dependencies:
pip install brokerage-connect[schwab]
pip install brokerage-connect[schwab,ibkr]
pip install brokerage-connect[plaid]
```

## Quick Start

```python
from brokerage import BrokerAdapter

# Every broker adapter implements the same interface:
class MyBroker(BrokerAdapter):
    provider_name = "my_broker"

    def owns_account(self, account_id: str) -> bool: ...
    def list_accounts(self): ...
    def search_symbol(self, account_id, ticker): ...
    def preview_order(self, account_id, ticker, side, quantity, order_type, time_in_force, **kw): ...
    def place_order(self, account_id, order_params): ...
    def get_orders(self, account_id, state="all", days=30): ...
    def cancel_order(self, account_id, order_id): ...
    def get_account_balance(self, account_id): ...
    def refresh_after_trade(self, account_id): ...
```

## Architecture

- **`BrokerAdapter`** — abstract base class defining the trade interface
- **`trade_objects`** — shared dataclasses (`OrderResult`, `OrderPreview`, `OrderStatus`, etc.)
- **`schwab/`**, **`snaptrade/`**, **`ibkr/`**, **`plaid/`** — broker/provider integrations
- **`config.py`** — broker configuration and credential loading via environment variables

## License

MIT
