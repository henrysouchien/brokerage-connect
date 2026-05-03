# risk-module-brokerage

`brokerage/` is the extracted brokerage package used by the backend trade-execution and provider-integration flows in this repo.

Package metadata lives in `brokerage/pyproject.toml` under the name `risk-module-brokerage`.

## What It Contains

| Path | Role |
|---|---|
| `broker_adapter.py` | Abstract `BrokerAdapter` contract used by `TradeExecutionService` |
| `trade_objects.py` | Shared order, preview, fill, cancel, and account dataclasses |
| `snaptrade/` | SnapTrade clients, adapters, connection helpers, recovery helpers, trading helpers |
| `schwab/` | Schwab client and broker adapter |
| `ibkr/` | IBKR broker adapter for trade-execution flows |
| `plaid/` | Plaid connection and secret helpers |
| `futures/` | Futures contract specs, notionals, pricing helpers, and source adapters |
| `config.py` | Brokerage configuration and env loading |

`core/trade_objects.py` remains as a compatibility shim that re-exports `brokerage.trade_objects`.

## Supported Integrations

| Integration | Package extra | What it covers |
|---|---|---|
| SnapTrade | `risk-module-brokerage[snaptrade]` | Connection flows, account discovery, trade preview/execute, order status |
| Schwab | `risk-module-brokerage[schwab]` | Direct Schwab client and trade adapter |
| IBKR | `risk-module-brokerage[ibkr]` | Trade adapter that works alongside the separate `ibkr/` package |
| Plaid | `risk-module-brokerage[plaid]` | Connection-oriented helpers and secrets support |

## Install

```bash
pip install risk-module-brokerage
pip install "risk-module-brokerage[snaptrade]"
pip install "risk-module-brokerage[schwab,plaid]"
pip install "risk-module-brokerage[schwab,ibkr]"
```

## Public Exports

The package exports:

- `BrokerAdapter`
- broker/order dataclasses such as `BrokerAccount`, `OrderPreview`, `OrderResult`, `OrderStatus`, `CancelResult`
- trade-preview and trade-execution result objects

## How It Fits The Repo

- `services/trade_execution_service.py` is the main consumer of the `BrokerAdapter` interface.
- The REST and MCP trading surfaces call into the service layer, which then uses these adapters.
- The separate `ibkr/` package covers market-data and account tooling; `brokerage/ibkr/adapter.py` is specifically the trade-execution side.

## Notes

- This package is an extracted subsystem inside the monorepo, not the full application surface by itself.
- For the higher-level trading APIs and MCP tools, see `docs/interfaces/api.md`, `docs/interfaces/mcp.md`, and `mcp_tools/README.md`.
