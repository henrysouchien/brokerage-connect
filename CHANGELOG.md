# Changelog

## 0.6.6 (2026-09-16)

- The IBKR extra admits PyArrow 23–25 (`>=23.0.1,<26`) with unchanged parquet cache behavior.

## 0.6.4 (2026-09-15)

- Removed all Risk-checkout imports, including hidden logger, budget, settings, ticker, reference-data, and cache-root discovery. Applications explicitly supply their policies; the source and published wheel now have the same dependency boundary.
- Standalone budget calls execute directly with no application billing rates. Risk's existing bootstrap supplies its guard, rates, logging, cache root, Flex ticker resolver, and FMP pricing factory; Risk futures lookups retain database-first reference-data behavior through their application adapter.
- SnapTrade connection redirects accept `frontend_base_url`; trading accepts the host's account-keyed distributed `trade_limiter`. Missing standalone trading coordination still refuses submission.
- IBKR account authorization accepts `authorized_accounts`. Flex imports no longer eagerly require provider extras; SDK construction remains worker-thread safe. Futures YAML parsing is a declared base dependency.
- Bundled futures metadata works without Risk. FMP futures sources accept pricing/currency callables; applications configure FMP-first pricing, while standalone pricing uses only its configured sources and IBKR.

## 0.6.3 (2026-09-15)

- Configuration is process-environment only; the package no longer reads a checkout-relative `.env` (the launcher owns dotenv loading).

## 0.6.2 (2026-09-15)

- Published from the source-owned tree: in-package IBKR adapter, endpoints and relay state machine ship in the wheel; the [ibkr] extra no longer depends on interactive-brokers-mcp (supersedes the 0.6.0/0.6.1 public wheels built from the stale dist).
- **Schwab adapter trade-integrity fix** (commit `4a689fd8`, 2026-05-28). `_extract_order_id` and `_response_as_dict` now accept any `collections.abc.Mapping` (including `httpx.Headers` from schwab-py) instead of requiring `isinstance(dict)`, fixing a latent bug where successful Schwab orders silently dropped the `brokerage_order_id` because schwab-py returns `httpx.Headers` (not a dict subclass). Schwab's place-order success only carries the order ID in the `Location` header per the schwab-py SDK contract. `_extract_order_id` now uses the canonical schwab-py regex (`r"https://api\.schwabapi\.com/trader/v1/accounts/(\w+)/orders/(\d+)"`) — no last-path-segment fallback. **Behavior change**: `SchwabBrokerAdapter.place_order` now raises `RuntimeError` when Schwab returns a non-`{200,201,202,204}` status OR a success-shaped response without an extractable `brokerage_order_id`. Callers that previously received `OrderResult(brokerage_order_id=None, status="ACCEPTED")` will now see the RuntimeError; the `services/trade_execution_service` orchestrator already handles this by rolling back the preview to `cancelled` and inserting a FAILED row with `error_message`. Same trade-integrity invariant needs to be applied to IBKR + SnapTrade adapters separately (see `risk_module/docs/TODO.md` row `OtherAdapters-TradeIntegrity-Audit`).

## 0.6.0 - 2026-05-03

- IBKR is now standalone-installable via `brokerage-connect[ibkr]`; `from brokerage.ibkr.adapter import IBKRBrokerAdapter` works without the risk_module monorepo on `PYTHONPATH`.
- The `[ibkr]` extra now carries the IBKR client modules directly under `brokerage.ibkr` instead of depending on `interactive-brokers-mcp`.
- Added standalone `_shared` shims for IBKR budget guarding and time-series caching.
- Added standalone `brokerage.options_types.OptionLeg` and `OptionStrategy` shapes for adapter option-trade flows.
- `IBKRBrokerAdapter` now accepts `account_map=` for aggregator-to-native account routing and falls back to parsing `TRADE_ACCOUNT_MAP` from the environment.

## 0.5.0 - 2026-05-03

- SnapTrade, Plaid, and Schwab provider clients now support standalone wheel installs via `brokerage-connect[snaptrade]`, `brokerage-connect[plaid]`, and `brokerage-connect[schwab]` without requiring the risk_module monorepo on `PYTHONPATH`.
- The sync workflow vendors the stdlib-only API budget exception and cost-table helpers into `brokerage._shared` for the published package.
- Historical 0.5.0 limitation: IBKR required the monorepo in that release. This does not describe the current package; 0.6.4 removes the remaining checkout imports and verifies every shipped module with only declared dependencies.
