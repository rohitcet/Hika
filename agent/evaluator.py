"""
evaluator.py — Daily evaluation engine

Runs at 3:45 PM EST. For each active cycle:
1. DTE check — hard exit at <= 15 DTE
2. Per-leg: harvest at +15%, stop at -40%, refill when <= entry
3. Serialises call leg before put leg to avoid race conditions
4. Logs every decision to eval_log and audit tables
"""

import os
import logging
from datetime import date, datetime, timezone

import pandas_market_calendars as mcal
import pytz

import db
import alerter
from gateway import IBGateway
from order_manager import OrderManager

log = logging.getLogger(__name__)

EST = pytz.timezone("America/New_York")

PROFIT_TARGET_PCT = float(os.environ.get("PROFIT_TARGET_PCT", "0.15"))  # 15%
STOP_LOSS_PCT     = float(os.environ.get("STOP_LOSS_PCT", "0.40"))      # 40%
HARD_EXIT_DTE     = int(os.environ.get("HARD_EXIT_DTE", "15"))
REFILL_MIN_DTE    = int(os.environ.get("REFILL_MIN_DTE", "15"))         # don't refill if <= this
ABORT_AFTER_EST   = "15:55"                                              # HH:MM — no orders after this


class Evaluator:
    def __init__(self, gw: IBGateway, order_mgr: OrderManager):
        self._gw = gw
        self._om = order_mgr

    # ------------------------------------------------------------------
    # Entry point — called by scheduler
    # ------------------------------------------------------------------

    def run(self) -> None:
        now_est = datetime.now(EST)
        today = now_est.date()

        log.info(f"=== Evaluation run starting: {now_est.strftime('%Y-%m-%d %H:%M:%S %Z')} ===")

        # Guard 1: abort window
        if self._past_abort_window(now_est):
            alerter.warn("EVALUATOR", f"Past abort window {ABORT_AFTER_EST} EST — skipping evaluation")
            return

        # Guard 2: market open check
        if not self._is_market_open(today):
            log.info("Market closed today — skipping evaluation")
            db.log_eval(today, False, None, None, None, None, None, None, "Market closed")
            return

        # Guard 3: get active cycle
        cycle = db.get_active_cycle()
        if cycle is None:
            log.info("No active cycle — nothing to evaluate")
            db.log_eval(today, True, None, None, None, None, None, None, "No active cycle")
            return

        cycle_id = cycle["id"]
        expiry = cycle["expiry_date"]
        dte = (expiry - today).days
        log.info(f"Active cycle {cycle_id} | expiry={expiry} | DTE={dte}")

        # Guard 4: hard exit at DTE threshold
        if dte <= HARD_EXIT_DTE:
            log.info(f"DTE {dte} <= {HARD_EXIT_DTE} — triggering hard exit")
            alerter.warn("EVALUATOR", f"Hard exit triggered: DTE={dte}")
            self._hard_exit(cycle)
            db.log_eval(today, True, cycle_id, None, None, "HARD_EXIT", "HARD_EXIT", dte)
            return

        # Guard 5: reconcile DB status vs live IBKR positions
        if not self._reconcile_positions(cycle):
            alerter.critical("EVALUATOR", "Position reconciliation failed — halting evaluation")
            db.log_eval(today, True, cycle_id, None, None, "RECONCILE_FAIL", "RECONCILE_FAIL", dte)
            return

        # Fetch mark prices for both legs
        call_mark = self._get_mark(cycle, "CALL")
        put_mark  = self._get_mark(cycle, "PUT")

        call_action = "SKIP"
        put_action  = "SKIP"

        # Evaluate call leg first (serialised)
        if cycle["call_status"] in ("ACTIVE", "SOLD"):
            call_action = self._evaluate_leg(cycle, "CALL", call_mark, dte)

        # Reload cycle after call leg may have changed status
        cycle = db.get_active_cycle()

        # Evaluate put leg
        if cycle and cycle["put_status"] in ("ACTIVE", "SOLD"):
            put_action = self._evaluate_leg(cycle, "PUT", put_mark, dte)

        # Daily summary alert
        alerter.daily_summary(
            str(today), dte,
            call_action, put_action,
            call_mark, put_mark,
        )

        db.log_eval(
            today, True, cycle_id,
            call_mark, put_mark,
            call_action, put_action,
            dte,
        )

    # ------------------------------------------------------------------
    # Per-leg evaluation
    # ------------------------------------------------------------------

    def _evaluate_leg(
        self,
        cycle: dict,
        leg: str,         # 'CALL' | 'PUT'
        mark: float | None,
        dte: int,
    ) -> str:
        status_key = f"{leg.lower()}_status"
        entry_px_key = f"{leg.lower()}_current_entry_px"
        status = cycle[status_key]
        entry_px = float(cycle[entry_px_key])
        cycle_id = cycle["id"]
        contracts = cycle["max_legs"] // 2  # split evenly between call and put

        if mark is None:
            log.warning(f"{leg} leg: no valid mark price — holding")
            return "HOLD_NO_MARK"

        pct_change = (mark - entry_px) / entry_px

        if status == "ACTIVE":
            # Check stop loss first
            if pct_change <= -STOP_LOSS_PCT:
                log.info(f"{leg} leg STOP LOSS: mark={mark:.4f} entry={entry_px:.4f} pct={pct_change:.1%}")
                alerter.stop_loss_notice(leg, entry_px, mark, abs(pct_change) * 100)
                self._close_leg(cycle, leg, mark, contracts, reason="STOP")
                return "STOP"

            # Check profit target
            if pct_change >= PROFIT_TARGET_PCT:
                log.info(f"{leg} leg HARVEST: mark={mark:.4f} entry={entry_px:.4f} pct={pct_change:.1%}")
                self._harvest_leg(cycle, leg, mark, contracts)
                return "HARVEST"

            log.info(f"{leg} leg HOLD: mark={mark:.4f} entry={entry_px:.4f} pct={pct_change:.1%}")
            return "HOLD"

        elif status == "SOLD":
            # Check refill condition
            if mark <= entry_px:
                if dte <= REFILL_MIN_DTE:
                    log.info(f"{leg} leg: mark <= entry but DTE={dte} too low to refill")
                    return "REFILL_SKIP_DTE"

                log.info(f"{leg} leg REFILL: mark={mark:.4f} <= entry={entry_px:.4f}")
                self._refill_leg(cycle, leg, mark, contracts)
                return "REFILL"

            log.info(f"{leg} leg SOLD/WAITING: mark={mark:.4f} entry={entry_px:.4f}")
            return "WAIT_REFILL"

        return "UNKNOWN"

    # ------------------------------------------------------------------
    # Actions
    # ------------------------------------------------------------------

    def _harvest_leg(
        self,
        cycle: dict,
        leg: str,
        mark: float,
        contracts: int,
    ) -> None:
        cycle_id = cycle["id"]
        entry_px = float(cycle[f"{leg.lower()}_current_entry_px"])
        expiry = cycle["expiry_date"].strftime("%Y%m%d")
        strike = float(cycle[f"{leg.lower()}_strike"])
        right = "C" if leg == "CALL" else "P"

        contract = self._gw.option_contract("GOOG", expiry, strike, right)

        # Place limit sell at mark price
        order_id = self._om.sell_leg(
            cycle_id=cycle_id,
            contract=contract,
            leg=leg,
            contracts=contracts,
            limit_price=mark,
        )

        if order_id is None:
            alerter.warn("EVALUATOR", f"{leg} harvest order failed — will retry next session")
            return

        # Wait for fill
        fill = self._gw.wait_for_fill(order_id, timeout=120)
        if fill and fill.get("status") == "Filled":
            exit_px = fill["avg_fill_price"]
            pnl = (exit_px - entry_px) * contracts * 100
            harvest_id = db.record_harvest(
                cycle_id=cycle_id,
                order_id=self._om.get_db_order_id(order_id),
                leg=leg,
                contracts=contracts,
                entry_px=entry_px,
                exit_px=exit_px,
            )
            db.update_cycle_leg_status(cycle_id, leg, "SOLD")
            alerter.harvest_notice(leg, contracts, entry_px, exit_px, pnl)

            # Trigger sweep for SGX deployment
            from sgx_deployer import queue_sweep
            queue_sweep(harvest_id, pnl)
        else:
            alerter.warn("EVALUATOR", f"{leg} harvest fill not confirmed — check manually")

    def _close_leg(
        self,
        cycle: dict,
        leg: str,
        mark: float,
        contracts: int,
        reason: str,
    ) -> None:
        """Close a leg at market (stop loss or hard exit)."""
        cycle_id = cycle["id"]
        expiry = cycle["expiry_date"].strftime("%Y%m%d")
        strike = float(cycle[f"{leg.lower()}_strike"])
        right = "C" if leg == "CALL" else "P"

        contract = self._gw.option_contract("GOOG", expiry, strike, right)
        self._om.close_leg_market(cycle_id, contract, leg, contracts)
        db.update_cycle_leg_status(cycle_id, leg, "STOPPED")
        db.audit("WARN", "EVALUATOR", f"{leg} leg closed: {reason}", {
            "mark": mark, "contracts": contracts
        })

    def _refill_leg(
        self,
        cycle: dict,
        leg: str,
        mark: float,
        contracts: int,
    ) -> None:
        """Rebuy a sold leg when mark drops back to entry."""
        cycle_id = cycle["id"]
        expiry = cycle["expiry_date"].strftime("%Y%m%d")
        strike = float(cycle[f"{leg.lower()}_strike"])
        right = "C" if leg == "CALL" else "P"

        contract = self._gw.option_contract("GOOG", expiry, strike, right)
        order_id = self._om.buy_leg(
            cycle_id=cycle_id,
            contract=contract,
            leg=leg,
            contracts=contracts,
            limit_price=mark,
        )
        if order_id:
            fill = self._gw.wait_for_fill(order_id, timeout=120)
            if fill and fill.get("status") == "Filled":
                new_entry = fill["avg_fill_price"]
                db.update_cycle_entry_price(cycle_id, leg, new_entry)
                db.update_cycle_leg_status(cycle_id, leg, "ACTIVE")
                alerter.info("EVALUATOR", f"{leg} refilled at ${new_entry:.4f}")

    def _hard_exit(self, cycle: dict) -> None:
        """Market-close all open legs and mark cycle closed."""
        cycle_id = cycle["id"]
        contracts = cycle["max_legs"] // 2
        expiry = cycle["expiry_date"].strftime("%Y%m%d")

        # Cancel any pending orders first
        pending = db.get_pending_orders(cycle_id)
        ibkr_ids = [o["ibkr_order_id"] for o in pending if o["ibkr_order_id"]]
        self._gw.cancel_all_pending(ibkr_ids)
        db.cancel_pending_orders(cycle_id)

        for leg, right, status_key in [
            ("CALL", "C", "call_status"),
            ("PUT",  "P", "put_status"),
        ]:
            if cycle[status_key] == "ACTIVE":
                strike = float(cycle[f"{leg.lower()}_strike"])
                contract = self._gw.option_contract("GOOG", expiry, strike, right)
                self._om.close_leg_market(cycle_id, contract, leg, contracts)

        db.close_cycle(cycle_id, "DTE_EXIT")
        alerter.info("EVALUATOR", f"Cycle {cycle_id} closed at DTE exit")

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _get_mark(self, cycle: dict, leg: str) -> float | None:
        expiry = cycle["expiry_date"].strftime("%Y%m%d")
        strike = float(cycle[f"{leg.lower()}_strike"])
        right = "C" if leg == "CALL" else "P"
        contract = self._gw.option_contract("GOOG", expiry, strike, right)
        return self._gw.get_mark_price(contract)

    def _reconcile_positions(self, cycle: dict) -> bool:
        """
        Stub: compare DB status with live IBKR positions.
        A full implementation calls reqPositions() and cross-checks.
        Returns False if mismatch detected.
        """
        # TODO: implement full position reconciliation in Phase 2
        # For now, trust DB state and log
        db.audit("INFO", "EVALUATOR", "Position reconciliation: trusted DB state")
        return True

    @staticmethod
    def _is_market_open(check_date: date) -> bool:
        nyse = mcal.get_calendar("NYSE")
        schedule = nyse.schedule(
            start_date=str(check_date),
            end_date=str(check_date),
        )
        return not schedule.empty

    @staticmethod
    def _past_abort_window(now_est: datetime) -> bool:
        abort_h, abort_m = ABORT_AFTER_EST.split(":")
        abort_time = now_est.replace(
            hour=int(abort_h), minute=int(abort_m), second=0, microsecond=0
        )
        return now_est >= abort_time
