"""
sgx_deployer.py — SGX equity deployment

On every harvest event:
1. Convert USD P&L to SGD via IBKR FX
2. Select DBS (D05) or CapitaLand (9CI) by 10-DMA dip
3. Place buy order at SGX open (9:00 AM SGT = 1:00 AM EST)
4. Update equity pool in DB
"""

import os
import logging
import time
from datetime import datetime, timedelta

import requests
import pytz

import db
import alerter

log = logging.getLogger(__name__)

SGT = pytz.timezone("Asia/Singapore")
EST = pytz.timezone("America/New_York")

EQUITY_TICKERS = ["D05", "9CI"]   # DBS, CapitaLand Invest
MAX_SINGLE_NAME_PCT = 0.60         # max 60% of pool in one stock
DMA_PERIOD = 10                    # 10-day moving average for dip selection


# ---------------------------------------------------------------------------
# Public API — called from evaluator after a harvest
# ---------------------------------------------------------------------------

def queue_sweep(harvest_id: int, pnl_usd: float) -> None:
    """Create a pending sweep record. Actual deployment happens at SGX open."""
    if pnl_usd <= 0:
        log.info(f"P&L is ${pnl_usd:.2f} — no sweep queued")
        return
    sweep_id = db.create_sweep(harvest_id, pnl_usd)
    alerter.info("SGX_DEPLOYER", f"Sweep queued: ${pnl_usd:.2f} USD (sweep_id={sweep_id})")


def deploy_pending_sweeps(gw) -> None:
    """
    Called at SGX open (9:05 AM SGT).
    Process all pending sweeps and deploy to equities.
    """
    sweeps = db.get_pending_sweeps()
    if not sweeps:
        log.info("No pending sweeps to deploy")
        return

    log.info(f"Deploying {len(sweeps)} pending sweep(s)")

    for sweep in sweeps:
        try:
            _deploy_one(sweep, gw)
        except Exception as e:
            alerter.critical("SGX_DEPLOYER", f"Sweep {sweep['id']} failed: {e}")


# ---------------------------------------------------------------------------
# Core deployment logic
# ---------------------------------------------------------------------------

def _deploy_one(sweep: dict, gw) -> None:
    sweep_id = sweep["id"]
    usd_amount = float(sweep["usd_amount"])

    # Step 1: FX conversion USD → SGD
    fx_rate = _get_fx_rate()
    if fx_rate is None:
        alerter.warn("SGX_DEPLOYER", f"Cannot get FX rate — sweep {sweep_id} deferred")
        return

    sgd_amount = usd_amount * fx_rate
    log.info(f"Sweep {sweep_id}: ${usd_amount:.2f} USD → SGD {sgd_amount:.2f} @ {fx_rate:.4f}")

    # Step 2: Select stock by dip
    ticker = _select_ticker_by_dip()
    if ticker is None:
        alerter.warn("SGX_DEPLOYER", "Could not determine dip ticker — defaulting to D05")
        ticker = "D05"

    # Step 3: Check concentration limit
    if not _check_concentration(ticker, sgd_amount):
        # Flip to the other ticker
        ticker = "9CI" if ticker == "D05" else "D05"
        alerter.info("SGX_DEPLOYER", f"Concentration limit — switched to {ticker}")

    # Step 4: Get current price and calculate shares
    price_sgd = _get_sgx_price(ticker)
    if price_sgd is None:
        alerter.warn("SGX_DEPLOYER", f"Cannot get price for {ticker} — sweep {sweep_id} deferred")
        return

    # SGX trades in board lots of 100 shares — round down
    shares_float = sgd_amount / price_sgd
    shares = int(shares_float // 100) * 100
    if shares == 0:
        alerter.warn(
            "SGX_DEPLOYER",
            f"Amount too small for 1 board lot of {ticker} "
            f"(need SGD {price_sgd * 100:.2f}, have SGD {sgd_amount:.2f})"
        )
        return

    actual_invested = shares * price_sgd
    log.info(f"Buying {shares} shares of {ticker} @ SGD {price_sgd:.4f} = SGD {actual_invested:.2f}")

    # Step 5: Place SGX buy order via IBKR
    contract = gw.stock_contract(ticker, exchange="SGX", currency="SGD")
    order_id = gw.place_limit_order(
        contract=contract,
        action="BUY",
        quantity=shares,
        limit_price=round(price_sgd * 1.002, 3),  # 0.2% above mid to ensure fill
    )

    if order_id is None:
        alerter.critical("SGX_DEPLOYER", f"Failed to place SGX buy for {ticker}")
        return

    # Wait for fill (SGX is slower — allow 10 minutes)
    fill = gw.wait_for_fill(order_id, timeout=600, poll_interval=5.0)

    if fill and fill.get("status") == "Filled":
        actual_price = fill["avg_fill_price"]
        actual_cost = shares * actual_price

        db.update_sweep_deployed(
            sweep_id=sweep_id,
            sgd_amount=actual_cost,
            fx_rate=fx_rate,
            ticker=ticker,
            shares=shares,
            buy_price=actual_price,
        )
        db.update_equity_position(ticker, shares, actual_price, actual_cost)
        alerter.deploy_notice(ticker, shares, actual_price, actual_cost, fx_rate)

    else:
        alerter.warn(
            "SGX_DEPLOYER",
            f"SGX buy fill not confirmed for {ticker} — order_id={order_id}",
            {"sweep_id": sweep_id}
        )


# ---------------------------------------------------------------------------
# Dip selector — buy whichever is furthest below its 10-DMA
# ---------------------------------------------------------------------------

def _select_ticker_by_dip() -> str | None:
    """
    Fetch 10-day price history for each ticker.
    Return the ticker trading furthest below its 10-DMA.
    Uses Yahoo Finance as a lightweight price source (no auth required).
    """
    dip_scores: dict[str, float] = {}

    for ticker in EQUITY_TICKERS:
        try:
            prices = _fetch_price_history(ticker, days=12)
            if len(prices) < DMA_PERIOD:
                log.warning(f"Insufficient price history for {ticker}")
                continue
            dma = sum(prices[-DMA_PERIOD:]) / DMA_PERIOD
            current = prices[-1]
            # Negative = below DMA (dip), positive = above DMA (premium)
            dip_pct = (current - dma) / dma
            dip_scores[ticker] = dip_pct
            log.info(f"{ticker}: price={current:.4f} 10DMA={dma:.4f} dip={dip_pct:.2%}")
        except Exception as e:
            log.warning(f"Price history fetch failed for {ticker}: {e}")

    if not dip_scores:
        return None

    # Return the ticker with the lowest (most negative) dip score
    return min(dip_scores, key=lambda t: dip_scores[t])


def _fetch_price_history(ticker: str, days: int = 12) -> list[float]:
    """
    Fetch closing prices from Yahoo Finance.
    SGX tickers: D05.SI, 9CI.SI
    """
    yf_ticker = ticker + ".SI"
    end = datetime.now(SGT)
    start = end - timedelta(days=days + 5)  # extra days for weekends

    url = (
        f"https://query1.finance.yahoo.com/v8/finance/chart/{yf_ticker}"
        f"?period1={int(start.timestamp())}&period2={int(end.timestamp())}"
        f"&interval=1d"
    )
    headers = {"User-Agent": "Mozilla/5.0"}
    resp = requests.get(url, headers=headers, timeout=15)
    resp.raise_for_status()
    data = resp.json()

    closes = data["chart"]["result"][0]["indicators"]["quote"][0]["close"]
    # Filter out None values (holidays)
    return [c for c in closes if c is not None][-days:]


def _get_sgx_price(ticker: str) -> float | None:
    """Get current price for an SGX stock."""
    try:
        prices = _fetch_price_history(ticker, days=2)
        return prices[-1] if prices else None
    except Exception as e:
        log.warning(f"Failed to get SGX price for {ticker}: {e}")
        return None


# ---------------------------------------------------------------------------
# FX rate
# ---------------------------------------------------------------------------

def _get_fx_rate() -> float | None:
    """
    Get USD/SGD rate from a free FX API.
    Falls back to a hardcoded conservative rate if unavailable.
    """
    try:
        resp = requests.get(
            "https://api.exchangerate-api.com/v4/latest/USD",
            timeout=10
        )
        resp.raise_for_status()
        rate = resp.json()["rates"]["SGD"]
        log.info(f"FX rate USD/SGD: {rate:.4f}")
        return rate
    except Exception as e:
        log.warning(f"FX rate fetch failed: {e} — using fallback")
        # Fallback: conservative rate (will be slightly undervalued, safe)
        return float(os.environ.get("FX_FALLBACK_RATE", "1.32"))


# ---------------------------------------------------------------------------
# Concentration check
# ---------------------------------------------------------------------------

def _check_concentration(ticker: str, new_invest_sgd: float) -> bool:
    """
    Return True if buying is within the 60% single-name cap.
    """
    pool = db.get_equity_pool()
    total_invested = sum(float(p["total_invested_sgd"]) for p in pool)
    ticker_row = next((p for p in pool if p["ticker"] == ticker), None)
    ticker_invested = float(ticker_row["total_invested_sgd"]) if ticker_row else 0

    new_total = total_invested + new_invest_sgd
    new_ticker_total = ticker_invested + new_invest_sgd

    if new_total == 0:
        return True

    concentration = new_ticker_total / new_total
    if concentration > MAX_SINGLE_NAME_PCT:
        log.warning(
            f"{ticker} concentration would be {concentration:.1%} "
            f"> {MAX_SINGLE_NAME_PCT:.0%} cap"
        )
        return False
    return True
