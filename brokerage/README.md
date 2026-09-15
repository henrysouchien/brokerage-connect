# brokerage-connect

**Status:** CURRENT / ACTIVE REFERENCE
**Last reviewed:** 2026-09-15
**Current source of truth:** brokerage-connect/brokerage/ code + brokerage-connect/pyproject.toml


`brokerage/` is the extracted brokerage package used by the backend trade-execution and provider-integration flows in this repo.

Package metadata lives in `pyproject.toml` (one level up, at the package root) under the name `brokerage-connect`.

## What It Contains

| Path | Role |
|---|---|
| `broker_adapter.py` | Abstract `BrokerAdapter` contract used by `TradeExecutionService` |
| `trade_objects.py` | Shared order, preview, fill, cancel, and account dataclasses |
| `snaptrade/` | SnapTrade clients, adapters, connection helpers, recovery helpers, trading helpers |
| `schwab/` | Schwab client and broker adapter |
| `ibkr/` | IBKR provider modules (trade adapter, client, market data, account, connection, flex) used by trade-execution and the separate MCP shell |
| `plaid/` | Plaid connection and secret helpers |
| `futures/` | Futures contract specs, notionals, pricing helpers, and source adapters |
| `config.py` | Broker environment values and optional application-supplied cache root; launcher owns env loading |

Risk's root `brokerage/__init__.py` selects this package tree and composes its application dependencies. The published wheel imports no Risk root modules. See the package-root [application integration contract](../README.md#application-integration).

## Supported Integrations

| Integration | Package extra | What it covers |
|---|---|---|
| SnapTrade | `brokerage-connect[snaptrade]` | Connection flows, account discovery, trade preview/execute, order status |
| Schwab | `brokerage-connect[schwab]` | Direct Schwab client and trade adapter |
| IBKR | `brokerage-connect[ibkr]` | Trade adapter + IBKR client modules; monorepo `ibkr/` package is the MCP server shell importing `brokerage.ibkr` |
| Plaid | `brokerage-connect[plaid]` | Connection-oriented helpers and secrets support |

## Install

```bash
pip install brokerage-connect
pip install "brokerage-connect[snaptrade]"
pip install "brokerage-connect[schwab,plaid]"
pip install "brokerage-connect[schwab,ibkr]"
```

## Public Exports

The package exports:

- `BrokerAdapter`
- broker/order dataclasses such as `BrokerAccount`, `OrderPreview`, `OrderResult`, `OrderStatus`, `CancelResult`
- trade-preview and trade-execution result objects

## How It Fits The Repo

- `services/trade_execution_service.py` is the main consumer of the `BrokerAdapter` interface.
- The REST and MCP trading surfaces call into the service layer, which then uses these adapters.
- Implementation lives under `brokerage/ibkr/` (including `adapter.py` for trade execution plus client/market_data/account); root `ibkr/` is the published MCP shell (`server.py`) that imports `brokerage.ibkr`.

## Notes

- This package is an extracted subsystem inside the monorepo, not the full application surface by itself.
- For the higher-level trading APIs and MCP tools, see `docs/interfaces/api.md`, `docs/interfaces/mcp.md`, and `mcp_tools/README.md`.
