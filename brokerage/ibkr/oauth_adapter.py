"""IBKR OAuth 1.0a adapter — STUB. Implementation gated on IBKR vendor approval.

The OAuth 1.0a Web API is the only IBKR API path that is fully cloud-hostable —
no TWS, no local gateway process, no relay daemon. Once IBKR grants third-party
vendor approval, this adapter replaces the relay vertical entirely:

- HTTPS calls signed with HMAC-SHA256 using a Diffie-Hellman-derived 24h session token
- Per-customer OAuth handshake via vendor-registered RSA keypair
- No per-user containers, no daily 2FA reauth (cf. CPAPI/ibeam path), no local
  process requirement

Vendor onboarding (calendar-bound, not engineering-bound):
- Contacts: webapionboarding@interactivebrokers.com (primary) +
  api-solutions@interactivebrokers.com (alternate per IBKR docs);
  check the IBKR Trading Web API onboarding page for the current
  preferred contact at the time of application.
- Process: EDD/compliance review ~3-6 weeks; initial screening, legal
  agreement, and key setup may add time on top.
- Requirements: public RSA key registration + callback URL + IBKR Pro
  customer accounts (not Lite).
- References:
    https://www.interactivebrokers.com/campus/ibkr-api-page/oauth-1-0a-extended/
    https://www.interactivebrokers.com/campus/ibkr-api-page/web-api-trading/
    https://www.interactivebrokers.com/webtradingapi/oauth.pdf

Once approved + implemented, switch prod by setting IBKR_TRANSPORT=oauth and
remove the relay state machine, endpoints, and daemon from the deploy.

This stub exists so the third adapter slot is visible in the directory
structure. Do not import this module in production code paths until the
implementation lands.
"""

from __future__ import annotations

from typing import Callable

from brokerage.broker_adapter import BrokerAdapter


class IBKROAuthAdapter(BrokerAdapter):
    """Placeholder. All methods raise NotImplementedError until vendor approval.

    NOTE: every abstract method on BrokerAdapter MUST be implemented here so
    that ABC instantiation passes and __init__'s NotImplementedError is the
    actual error surfaced (otherwise a TypeError fires before __init__ runs).
    The lock-down test asserts IBKROAuthAdapter.__abstractmethods__ ==
    frozenset() to catch future BrokerAdapter abstract additions.
    """

    _NOT_READY = NotImplementedError(
        "IBKROAuthAdapter is not yet implemented. IBKR vendor approval "
        "required (~3-6 weeks). See module docstring for onboarding details."
    )

    def __init__(
        self,
        user_email: str,
        on_refresh: Callable[[str], None] | None = None,
    ) -> None:
        raise self._NOT_READY

    @property
    def provider_name(self) -> str:
        return "ibkr"

    # Stub every abstract method on BrokerAdapter PLUS the IBKR-adapter
    # public surface aliases from PR 1 (`probe`, `get_portfolio_with_cash`,
    # `fetch_snapshot`, `get_option_chain`). The non-abstract items aren't
    # strictly required for ABC instantiation, but stubbing them gives
    # callers a consistent "vendor approval pending" error across the full
    # adapter API rather than a different (e.g., AttributeError or
    # NotImplementedError from BrokerAdapter's concrete default).
    # Audit list at impl time by running pytest: tests/brokerage/
    # test_ibkr_oauth_stub.py will fail if BrokerAdapter grows new
    # abstract methods this stub doesn't cover.
    def list_accounts(self): raise self._NOT_READY
    def get_live_positions(self, *args, **kwargs): raise self._NOT_READY
    def get_portfolio_with_cash(self, *args, **kwargs): raise self._NOT_READY
    def get_orders(self, *args, **kwargs): raise self._NOT_READY
    def query_open_orders(self, *args, **kwargs): raise self._NOT_READY
    def query_completed_orders(self, *args, **kwargs): raise self._NOT_READY
    def fetch_market_snapshot(self, *args, **kwargs): raise self._NOT_READY
    def fetch_snapshot(self, *args, **kwargs): raise self._NOT_READY  # alias from PR 1
    def get_account_balance(self, *args, **kwargs): raise self._NOT_READY
    def place_order(self, *args, **kwargs): raise self._NOT_READY
    def preview_order(self, *args, **kwargs): raise self._NOT_READY
    def cancel_order(self, *args, **kwargs): raise self._NOT_READY
    def preview_multileg_option(self, *args, **kwargs): raise self._NOT_READY
    def place_multileg_option(self, *args, **kwargs): raise self._NOT_READY
    def preview_roll(self, *args, **kwargs): raise self._NOT_READY
    def place_roll(self, *args, **kwargs): raise self._NOT_READY
    def search_symbol(self, *args, **kwargs): raise self._NOT_READY
    def refresh_after_trade(self, *args, **kwargs): raise self._NOT_READY
    def owns_account(self, *args, **kwargs): raise self._NOT_READY
    def probe(self): raise self._NOT_READY
    def get_option_chain(self, *args, **kwargs): raise self._NOT_READY  # from PR 1
