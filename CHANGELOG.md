# Changelog

## 0.6.0 - 2026-05-03

- IBKR is now standalone-installable via `brokerage-connect[ibkr]`; `from brokerage.ibkr.adapter import IBKRBrokerAdapter` works without the risk_module monorepo on `PYTHONPATH`.
- The `[ibkr]` extra now depends on `interactive-brokers-mcp>=0.2.4`, which brings the sibling `ibkr.*` modules and a heavier opt-in dependency tree.
- Added standalone `brokerage.options_types.OptionLeg` and `OptionStrategy` shapes for adapter option-trade flows.
- `IBKRBrokerAdapter` now accepts `account_map=` for aggregator-to-native account routing and falls back to parsing `TRADE_ACCOUNT_MAP` from the environment.

## 0.5.0 - 2026-05-03

- SnapTrade, Plaid, and Schwab provider clients now support standalone wheel installs via `brokerage-connect[snaptrade]`, `brokerage-connect[plaid]`, and `brokerage-connect[schwab]` without requiring the risk_module monorepo on `PYTHONPATH`.
- The sync workflow vendors the stdlib-only API budget exception and cost-table helpers into `brokerage._shared` for the published package.
- IBKR remains monorepo-only in this release. The `[ibkr]` extra still installs the SDK for monorepo callers, but `brokerage.ibkr.adapter` is not standalone-importable because it still depends on `app_platform`, `options`, `providers.routing_config`, and sibling `ibkr.*` modules. See `docs/planning/BROKERAGE_CONNECT_VENDOR_API_BUDGET_PLAN.md` for the scoped follow-up rationale.
