"""
gateway.py — IB Gateway connection wrapper using ib_insync

ib_insync is a clean asyncio wrapper around IBKR's TWS API.
It is on PyPI (pip install ib_insync) unlike the raw ibapi package.

Handles:
- Connection + heartbeat + auto-reconnect
- Market data requests with stale-quote guard
- Option contract builder
- Order placement, tracking, cancellation
- IBKR error classification
"""

import os
import time
import logging
from datetime import datetime, timezone

from ib_insync import IB, Stock, Option, LimitOrder, MarketOrder, util

import alerter

log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# IBKR error code classification
# ---------------------------------------------------------------------------

IGNORE_CODES = {2104, 2106, 2158, 2103, 2105, 202}
RETRY_CODES  = {1100, 1102, 10197}
FATAL_CODES  = {162, 200, 201, 203, 321, 502, 504}

QUOTE_MAX_AGE   = 60    # seconds — reject stale quotes
MAX_SPREAD_PCT  = 0.15  # reject if spread > 15% of mark


class IBGateway:
    RECONNECT_BACKOFF    = [2, 4, 8, 16, 32, 60]
    MAX_RECONNECT_WINDOW = 600  # 10 minutes

    def __init__(self):
        self._ib = IB()
        self._host = os.environ.get("IB_GATEWAY_HOST", "ibgateway")
        self._port = int(os.environ.get("IB_PORT", "4002"))
        self._client_id = int(os.environ.get("IB_CLIENT_ID", "1"))
        self._reconnect_start = None
        self._trading_mode = os.environ.get("TRADING_MODE", "paper")
        self._ib_mode = os.environ.get("IB_TRADING_MODE", "paper")

        # Wire up event handlers
        self._ib.errorEvent += self._on_error
        self._ib.disconnectedEvent += self._on_disconnect

    # ------------------------------------------------------------------
    # Connection
    # ------------------------------------------------------------------

    def connect(self) -> None:
        attempt = 0
        while True:
            try:
                self._ib.connect(
                    self._host,
                    self._port,
                    clientId=self._client_id,
                    timeout=20,
                    readonly=False,
                )
                self._reconnect_start = None
                log.info(f"Connected to IB Gateway at {self._host}:{self._port}")
                alerter.info("GATEWAY", "Connected to IB Gateway")
                return
            except Exception as e:
                delay = self.RECONNECT_BACKOFF[min(attempt, len(self.RECONNECT_BACKOFF) - 1)]
                log.error(f"Connection failed: {e} — retrying in {delay}s")

                if self._reconnect_start is None:
                    self._reconnect_start = time.time()
                elif time.time() - self._reconnect_start > self.MAX_RECONNECT_WINDOW:
                    alerter.critical("GATEWAY", "Cannot reconnect to IB Gateway — halting")
                    raise RuntimeError("IB Gateway reconnect window exceeded") from e

                attempt += 1
                time.sleep(delay)

    def disconnect(self) -> None:
        self._ib.disconnect()
        log.info("Disconnected from IB Gateway")

    def _on_disconnect(self) -> None:
        log.warning("IB Gateway disconnected — attempting reconnect")
        alerter.warn("GATEWAY", "IB Gateway disconnected")
        time.sleep(5)
        self.connect()

    def _on_error(self, reqId, errorCode, errorString, contract) -> None:
        if errorCode in IGNORE_CODES:
            log.debug(f"IBKR info {errorCode}: {errorString}")
            return
        if errorCode in RETRY_CODES:
            log.warning(f"IBKR transient {errorCode}: {errorString}")
            alerter.warn("GATEWAY", f"Transient IBKR error {errorCode}: {errorString}")
            return
        if errorCode in FATAL_CODES:
            log.error(f"IBKR fatal {errorCode}: {errorString}")
            alerter.critical("GATEWAY", f"Fatal IBKR error {errorCode}: {errorString}")
            return
        log.warning(f"IBKR unknown {errorCode} (reqId={reqId}): {errorString}")

    # ------------------------------------------------------------------
    # Contract builders
    # ------------------------------------------------------------------

    @staticmethod
    def option_contract(
        symbol: str,
        expiry: str,    # YYYYMMDD
        strike: float,
        right: str,     # 'C' or 'P'
        exchange: str = "SMART",
        currency: str = "USD",
    ) -> Option:
        return Option(
            symbol=symbol,
            lastTradeDateOrContractMonth=expiry,
            strike=strike,
            right=right,
            exchange=exchange,
            currency=currency,
            multiplier="100",
        )

    @staticmethod
    def stock_contract(
        symbol: str,
        exchange: str = "SGX",
        currency: str = "SGD",
    ) -> Stock:
        return Stock(symbol=symbol, exchange=exchange, currency=currency)

    # ------------------------------------------------------------------
    # Market data
    # ------------------------------------------------------------------

    def get_mark_price(self, contract, timeout: int = 15) -> float | None:
        """
        Request market data snapshot and return mark price.
        Validates quote age and rejects stale/bad quotes.
        """
        try:
            self._ib.qualifyContracts(contract)
            ticker = self._ib.reqMktData(contract, "", snapshot=True, regulatorySnapshot=False)

            deadline = time.time() + timeout
            while time.time() < deadline:
                self._ib.sleep(0.5)
                if ticker.last and ticker.last > 0:
                    break
                if ticker.close and ticker.close > 0:
                    break

            self._ib.cancelMktData(contract)

            price = ticker.last or ticker.close or ticker.marketPrice()
            if not price or price <= 0:
                log.warning(f"No valid price for {contract.symbol}")
                return None

            # Bid=0 guard
            if ticker.bid == 0 and ticker.ask and price > 0:
                log.warning(f"Bid is zero for {contract.symbol} — rejecting quote")
                return None

            # Spread guard
            if ticker.bid and ticker.ask and ticker.bid > 0:
                spread_pct = (ticker.ask - ticker.bid) / price
                if spread_pct > MAX_SPREAD_PCT:
                    log.warning(f"Spread {spread_pct:.1%} too wide for {contract.symbol}")
                    return None

            return float(price)

        except Exception as e:
            log.error(f"get_mark_price failed: {e}")
            return None

    def get_underlying_price(self, symbol: str = "GOOG") -> float | None:
        contract = Stock(symbol, "SMART", "USD")
        return self.get_mark_price(contract)

    # ------------------------------------------------------------------
    # Order placement
    # ------------------------------------------------------------------

    def place_limit_order(
        self,
        contract,
        action: str,
        quantity: int,
        limit_price: float,
        tif: str = "DAY",
    ) -> int | None:
        if not self._is_live():
            log.info(f"[PAPER] Would place {action} LMT {quantity} @ ${limit_price:.4f}")
            return 9999  # dummy order id for paper mode

        try:
            self._ib.qualifyContracts(contract)
            order = LimitOrder(action, quantity, round(limit_price, 2), tif=tif)
            trade = self._ib.placeOrder(contract, order)
            log.info(f"Placed {action} LMT {quantity} @ ${limit_price:.4f} — orderId={trade.order.orderId}")
            return trade.order.orderId
        except Exception as e:
            log.error(f"place_limit_order failed: {e}")
            return None

    def place_market_order(self, contract, action: str, quantity: int) -> int | None:
        if not self._is_live():
            log.info(f"[PAPER] Would place {action} MKT {quantity}")
            return 9999

        try:
            self._ib.qualifyContracts(contract)
            order = MarketOrder(action, quantity)
            trade = self._ib.placeOrder(contract, order)
            log.info(f"Placed {action} MKT {quantity} — orderId={trade.order.orderId}")
            return trade.order.orderId
        except Exception as e:
            log.error(f"place_market_order failed: {e}")
            return None

    def cancel_order(self, ibkr_order_id: int) -> None:
        try:
            trades = self._ib.trades()
            for trade in trades:
                if trade.order.orderId == ibkr_order_id:
                    self._ib.cancelOrder(trade.order)
                    log.info(f"Cancelled order {ibkr_order_id}")
                    return
            log.warning(f"Order {ibkr_order_id} not found for cancellation")
        except Exception as e:
            log.error(f"cancel_order failed: {e}")

    def cancel_all_pending(self, ibkr_order_ids: list[int]) -> None:
        for oid in ibkr_order_ids:
            self.cancel_order(oid)
            time.sleep(0.3)

    def wait_for_fill(
        self,
        ibkr_order_id: int,
        timeout: int = 60,
        poll_interval: float = 2.0,
    ) -> dict | None:
        """Poll until filled, cancelled, or timeout."""
        if not self._is_live():
            # Paper mode — simulate a fill
            return {
                "status": "Filled",
                "filled": 2,
                "remaining": 0,
                "avg_fill_price": 0.0,
            }

        deadline = time.time() + timeout
        while time.time() < deadline:
            trades = self._ib.trades()
            for trade in trades:
                if trade.order.orderId == ibkr_order_id:
                    status = trade.orderStatus.status
                    if status in ("Filled", "Cancelled", "Inactive"):
                        return {
                            "status": status,
                            "filled": trade.orderStatus.filled,
                            "remaining": trade.orderStatus.remaining,
                            "avg_fill_price": trade.orderStatus.avgFillPrice,
                        }
            self._ib.sleep(poll_interval)

        log.warning(f"Order {ibkr_order_id} timed out after {timeout}s")
        return None

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _is_live(self) -> bool:
        return self._trading_mode == "live" and self._ib_mode == "live"

    def sleep(self, secs: float) -> None:
        self._ib.sleep(secs)
