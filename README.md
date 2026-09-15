# brokerage-connect

**Status:** CURRENT / ACTIVE REFERENCE
**Last reviewed:** 2026-09-15
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

## Application integration

The wheel has no dependency on a Risk checkout. Install a provider extra before
using that provider's SDK; importing the Flex parser and connection helpers does
not initialize an SDK or load a checkout `.env`.

- `IBKRClient(authorized_accounts=...)` accepts the application's account policy;
  absent an explicit value, it uses `IBKR_AUTHORIZED_ACCOUNTS` from the process environment.
- SnapTrade connection creation accepts `frontend_base_url=...` (default:
  process `FRONTEND_BASE_URL`, then `http://localhost:3000`). A
  `SnapTradeBrokerAdapter(..., trade_limiter=...)` requires a callable that
  serializes/paces submissions by account. Without coordination, trading raises
  `TradeRateLimitUnavailable`; it does not submit an unpaced order.
- `brokerage.futures.load_contract_specs()` reads the bundled catalog.
  Applications own database-backed reference-data policy. IBKR futures metadata
  helpers accept `spec_loader=...`.
- `FMPFuturesPriceSource` accepts `fetch_monthly_close`, `infer_currency`, and
  `normalize_minor_currency_price` callables. Pass a source to
  `get_default_pricing_chain(fmp_source=...)` or configure an application factory
  with `configure_futures_pricing(factory=...)`. Unconfigured standalone pricing
  has no FMP source and uses IBKR.
- At process bootstrap, applications may call
  `brokerage._shared.budget_guard.configure_budget(guard=..., cost_per_call=...)`.
  The standalone default executes the operation directly and carries no billing
  rates; it does not discover a host budget service.
- `brokerage.ibkr.flex.configure_flex(ticker_alias_resolver=...,
  futures_spec_loader=...)` binds application normalization policy before parsing.
  Without configuration, Flex keeps native equity symbols and reads bundled
  futures roots. `normalize_flex_trades` also accepts a per-call
  `ticker_alias_resolver`.
- `brokerage.config.CACHE_ROOT` accepts an application-owned `Path`. Explicit
  `IBKR_CACHE_DIR` / `IBKR_TIMESERIES_CACHE_DIR` environment values take precedence;
  without application configuration, IBKR uses the user's `.cache/ibkr-mcp` tree.

Risk supplies these dependencies once from its own `brokerage/__init__.py`
composition point. Its database, distributed trade limiter, logging, and FMP
pricing policies remain application code and are not bundled into this wheel.

## License

PolyForm-Noncommercial-1.0.0
