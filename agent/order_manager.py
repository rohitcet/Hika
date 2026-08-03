"""
order_manager.py — Order lifecycle management

Handles:
- Buy / sell limit orders per leg
- Market close orders
- Partial fill policy (cancel remainder if not filled in time)
- 3:58 PM cleanup sweep cancelling all pending orders
- DB order record linkage
"""

import logging
import time

from ib_insync import Contract

import db
import alerter
from gateway import IBGateway

log = logging.getLogger(__name__)

# After this many seconds waiting for a fill, cancel remainder
FILL_TIMEOUT = 300          # 5 minutes
CLEANUP_TIMEOUT_EST = "15:58"


class OrderManager:
    def __init__(self, gw: IBGateway):
        self._gw = gw
        self._db_order_map: dict[int, int] = {}  # ibkr_order_id → db order id

    # ------------------------------------------------------------------
    # Public order actions
    # ------------------------------------------------------------------

    def buy_leg(
        self,
        cycle_id: int,
        contract: Contract,
        leg: str,
        contracts: int,
        limit_price: float,
    ) -> int | None:
        """Place a limit buy. Returns IBKR order ID or None on failure."""
        try:
            ibkr_id = self._gw.place_limit_order(
                contract, "BUY", contracts, limit_price
            )
            db_id = db.create_order(
                cycle_id=cycle_id,
                ibkr_order_id=ibkr_id,
                leg=leg,
                action="BUY",
                order_type="LIMIT",
                quantity=contracts,
                limit_price=limit_price,
            )
            self._db_order_map[ibkr_id] = db_id
            alerter.info("ORDER_MGR", f"BUY {leg} {contracts}x @ ${limit_price:.4f} placed (ibkr={ibkr_id})")
            return ibkr_id

        except Exception as e:
            alerter.critical("ORDER_MGR", f"Failed to place BUY {leg}: {e}")
            return None

    def sell_leg(
        self,
        cycle_id: int,
        contract: Contract,
        leg: str,
        contracts: int,
        limit_price: float,
    ) -> int | None:
        """Place a limit sell. Returns IBKR order ID or None on failure."""
        try:
            ibkr_id = self._gw.place_limit_order(
                contract, "SELL", contracts, limit_price
            )
            db_id = db.create_order(
                cycle_id=cycle_id,
                ibkr_order_id=ibkr_id,
                leg=leg,
                action="SELL",
                order_type="LIMIT",
                quantity=contracts,
                limit_price=limit_price,
            )
            self._db_order_map[ibkr_id] = db_id
            alerter.info("ORDER_MGR", f"SELL {leg} {contracts}x @ ${limit_price:.4f} placed (ibkr={ibkr_id})")
            return ibkr_id

        except Exception as e:
            alerter.critical("ORDER_MGR", f"Failed to place SELL {leg}: {e}")
            return None

    def close_leg_market(
        self,
        cycle_id: int,
        contract: Contract,
        leg: str,
        contracts: int,
    ) -> int | None:
        """Market close — used for stop loss and hard exit."""
        try:
            ibkr_id = self._gw.place_market_order(contract, "SELL", contracts)
            db_id = db.create_order(
                cycle_id=cycle_id,
                ibkr_order_id=ibkr_id,
                leg=leg,
                action="SELL",
                order_type="MARKET",
                quantity=contracts,
            )
            self._db_order_map[ibkr_id] = db_id
            alerter.warn("ORDER_MGR", f"MARKET CLOSE {leg} {contracts}x placed (ibkr={ibkr_id})")
            return ibkr_id

        except Exception as e:
            alerter.critical("ORDER_MGR", f"Failed to market-close {leg}: {e}")
            return None

    def handle_partial_fill(
        self,
        cycle_id: int,
        ibkr_order_id: int,
        contract: Contract,
        leg: str,
        total_qty: int,
        filled_qty: int,
    ) -> None:
        """
        Partial fill policy:
        - Cancel the remaining portion
        - Log partial fill in DB
        - Alert for manual review
        """
        remaining = total_qty - filled_qty
        if remaining <= 0:
            return

        log.warning(f"Partial fill: order {ibkr_order_id} filled {filled_qty}/{total_qty}")
        self._gw.cancel_order(ibkr_order_id)
        db.update_order_fill(
            ibkr_order_id=ibkr_order_id,
            fill_price=0,
            fill_qty=filled_qty,
            status="PARTIAL",
        )
        alerter.warn(
            "ORDER_MGR",
            f"Partial fill on {leg}: {filled_qty}/{total_qty} contracts — remainder cancelled",
            {"ibkr_order_id": ibkr_order_id, "remaining": remaining}
        )

    def cleanup_sweep(self, cycle_id: int) -> None:
        """
        Cancel all pending limit orders for this cycle.
        Called at 3:58 PM EST to prevent GTC orders carrying overnight.
        """
        pending = db.get_pending_orders(cycle_id)
        if not pending:
            return

        ibkr_ids = [o["ibkr_order_id"] for o in pending if o["ibkr_order_id"]]
        log.info(f"Cleanup sweep: cancelling {len(ibkr_ids)} pending orders")
        self._gw.cancel_all_pending(ibkr_ids)
        db.cancel_pending_orders(cycle_id)
        alerter.warn(
            "ORDER_MGR",
            f"Cleanup sweep: {len(ibkr_ids)} orders cancelled at 3:58 PM",
            {"ibkr_ids": ibkr_ids}
        )

    def get_db_order_id(self, ibkr_order_id: int) -> int | None:
        return self._db_order_map.get(ibkr_order_id)

    def sync_fill_to_db(self, ibkr_order_id: int, fill: dict) -> None:
        """Write fill result from gateway to DB."""
        status_map = {
            "Filled": "FILLED",
            "PartiallyFilled": "PARTIAL",
            "Cancelled": "CANCELLED",
            "Inactive": "CANCELLED",
        }
        db_status = status_map.get(fill.get("status", ""), "PENDING")
        db.update_order_fill(
            ibkr_order_id=ibkr_order_id,
            fill_price=fill.get("avg_fill_price", 0),
            fill_qty=int(fill.get("filled", 0)),
            status=db_status,
        )
