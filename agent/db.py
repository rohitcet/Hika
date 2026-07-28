"""
db.py — Database layer
All tables live under the 'trading' schema to avoid collisions
with other services on the same Railway Postgres instance.
"""

import os
import logging
from datetime import date, datetime
from contextlib import contextmanager

import psycopg2
from psycopg2 import pool
from psycopg2.extras import RealDictCursor

log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Connection pool (created once at startup)
# ---------------------------------------------------------------------------

_pool: pool.ThreadedConnectionPool | None = None


def init_pool() -> None:
    global _pool
    url = os.environ["DATABASE_URL"]
    # Railway injects postgres:// — psycopg2 needs postgresql://
    url = url.replace("postgres://", "postgresql://", 1)
    _pool = pool.ThreadedConnectionPool(
        minconn=1,
        maxconn=5,
        dsn=url,
        sslmode="require",
        cursor_factory=RealDictCursor,
    )
    log.info("Database connection pool initialised")


@contextmanager
def get_conn():
    """Yield a connection from the pool, return it on exit."""
    if _pool is None:
        raise RuntimeError("Pool not initialised — call init_pool() first")
    conn = _pool.getconn()
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        _pool.putconn(conn)


# ---------------------------------------------------------------------------
# Schema bootstrap — runs once on startup
# ---------------------------------------------------------------------------

SCHEMA_SQL = """
CREATE SCHEMA IF NOT EXISTS trading;

-- Active option cycles
CREATE TABLE IF NOT EXISTS trading.cycles (
    id              SERIAL PRIMARY KEY,
    underlying      TEXT NOT NULL DEFAULT 'GOOG',
    expiry_date     DATE NOT NULL,
    entry_date      DATE NOT NULL,
    entry_dte       INT  NOT NULL,

    call_strike     NUMERIC(10,2) NOT NULL,
    put_strike      NUMERIC(10,2) NOT NULL,

    call_entry_px   NUMERIC(10,4) NOT NULL,
    put_entry_px    NUMERIC(10,4) NOT NULL,

    call_status     TEXT NOT NULL DEFAULT 'ACTIVE',  -- ACTIVE | SOLD | STOPPED
    put_status      TEXT NOT NULL DEFAULT 'ACTIVE',

    -- Track current entry price for refill resets
    call_current_entry_px  NUMERIC(10,4) NOT NULL,
    put_current_entry_px   NUMERIC(10,4) NOT NULL,

    max_legs        INT  NOT NULL DEFAULT 4,
    closed          BOOLEAN NOT NULL DEFAULT FALSE,
    close_reason    TEXT,          -- DTE_EXIT | MANUAL | ERROR
    closed_at       TIMESTAMPTZ,

    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Every order placed with IBKR
CREATE TABLE IF NOT EXISTS trading.orders (
    id              SERIAL PRIMARY KEY,
    cycle_id        INT  REFERENCES trading.cycles(id),
    ibkr_order_id   INT  UNIQUE,
    leg             TEXT NOT NULL,   -- CALL | PUT
    action          TEXT NOT NULL,   -- BUY | SELL
    order_type      TEXT NOT NULL,   -- LIMIT | MARKET
    quantity        INT  NOT NULL,
    limit_price     NUMERIC(10,4),
    fill_price      NUMERIC(10,4),
    fill_qty        INT  DEFAULT 0,
    status          TEXT NOT NULL DEFAULT 'PENDING',
    -- PENDING | PARTIAL | FILLED | CANCELLED | TIMEOUT | ERROR
    placed_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    filled_at       TIMESTAMPTZ,
    cancelled_at    TIMESTAMPTZ,
    notes           TEXT
);

-- Realised P&L per harvest event
CREATE TABLE IF NOT EXISTS trading.harvests (
    id              SERIAL PRIMARY KEY,
    cycle_id        INT  REFERENCES trading.cycles(id),
    order_id        INT  REFERENCES trading.orders(id),
    leg             TEXT NOT NULL,
    contracts       INT  NOT NULL,
    entry_px        NUMERIC(10,4) NOT NULL,
    exit_px         NUMERIC(10,4) NOT NULL,
    pnl_usd         NUMERIC(12,2) NOT NULL,
    harvested_at    TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Cash sweep → SGX deployment log
CREATE TABLE IF NOT EXISTS trading.sweeps (
    id              SERIAL PRIMARY KEY,
    harvest_id      INT  REFERENCES trading.harvests(id),
    usd_amount      NUMERIC(12,2) NOT NULL,
    sgd_amount      NUMERIC(12,2),
    fx_rate         NUMERIC(10,6),
    status          TEXT NOT NULL DEFAULT 'PENDING',
    -- PENDING | CONVERTED | DEPLOYED | FAILED
    deployed_to     TEXT,           -- 'D05' | '9CI'
    shares_bought   NUMERIC(10,4),
    buy_price_sgd   NUMERIC(10,4),
    converted_at    TIMESTAMPTZ,
    deployed_at     TIMESTAMPTZ,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- SGX equity pool — running position per stock
CREATE TABLE IF NOT EXISTS trading.equity_pool (
    id              SERIAL PRIMARY KEY,
    ticker          TEXT NOT NULL UNIQUE,  -- 'D05', '9CI'
    exchange        TEXT NOT NULL DEFAULT 'SGX',
    total_shares    NUMERIC(12,4) NOT NULL DEFAULT 0,
    avg_cost_sgd    NUMERIC(10,4),
    total_invested_sgd NUMERIC(12,2) NOT NULL DEFAULT 0,
    last_updated    TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Seed equity pool rows on first run
INSERT INTO trading.equity_pool (ticker, exchange)
VALUES ('D05', 'SGX'), ('9CI', 'SGX')
ON CONFLICT (ticker) DO NOTHING;

-- Daily evaluation log — one row per evaluation run
CREATE TABLE IF NOT EXISTS trading.eval_log (
    id              SERIAL PRIMARY KEY,
    eval_date       DATE NOT NULL UNIQUE,
    run_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    market_open     BOOLEAN NOT NULL DEFAULT TRUE,
    active_cycle_id INT  REFERENCES trading.cycles(id),
    call_mark       NUMERIC(10,4),
    put_mark        NUMERIC(10,4),
    call_action     TEXT,   -- HOLD | HARVEST | STOP | REFILL | SKIP
    put_action      TEXT,
    dte_at_eval     INT,
    notes           TEXT
);

-- Audit trail — every state change
CREATE TABLE IF NOT EXISTS trading.audit (
    id              SERIAL PRIMARY KEY,
    event_time      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    level           TEXT NOT NULL,  -- INFO | WARN | CRITICAL
    component       TEXT NOT NULL,  -- EVALUATOR | ORDER_MGR | SGX | GATEWAY | SCHEDULER
    message         TEXT NOT NULL,
    payload         JSONB
);
"""


def bootstrap_schema() -> None:
    """Create all tables if they don't exist. Safe to run on every startup."""
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(SCHEMA_SQL)
    log.info("Schema bootstrap complete")


# ---------------------------------------------------------------------------
# Cycle operations
# ---------------------------------------------------------------------------

def get_active_cycle() -> dict | None:
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT * FROM trading.cycles
                WHERE closed = FALSE
                ORDER BY created_at DESC
                LIMIT 1
            """)
            return cur.fetchone()


def create_cycle(
    expiry_date: date,
    entry_dte: int,
    call_strike: float,
    put_strike: float,
    call_entry_px: float,
    put_entry_px: float,
    max_legs: int,
) -> int:
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO trading.cycles (
                    expiry_date, entry_date, entry_dte,
                    call_strike, put_strike,
                    call_entry_px, put_entry_px,
                    call_current_entry_px, put_current_entry_px,
                    max_legs
                ) VALUES (
                    %s, CURRENT_DATE, %s,
                    %s, %s,
                    %s, %s,
                    %s, %s,
                    %s
                ) RETURNING id
            """, (
                expiry_date, entry_dte,
                call_strike, put_strike,
                call_entry_px, put_entry_px,
                call_entry_px, put_entry_px,
                max_legs,
            ))
            return cur.fetchone()["id"]


def update_cycle_leg_status(cycle_id: int, leg: str, status: str) -> None:
    col = "call_status" if leg == "CALL" else "put_status"
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(f"""
                UPDATE trading.cycles
                SET {col} = %s, updated_at = NOW()
                WHERE id = %s
            """, (status, cycle_id))


def update_cycle_entry_price(cycle_id: int, leg: str, new_px: float) -> None:
    col = "call_current_entry_px" if leg == "CALL" else "put_current_entry_px"
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(f"""
                UPDATE trading.cycles
                SET {col} = %s, updated_at = NOW()
                WHERE id = %s
            """, (new_px, cycle_id))


def close_cycle(cycle_id: int, reason: str) -> None:
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE trading.cycles
                SET closed = TRUE,
                    close_reason = %s,
                    closed_at = NOW(),
                    updated_at = NOW()
                WHERE id = %s
            """, (reason, cycle_id))


# ---------------------------------------------------------------------------
# Order operations
# ---------------------------------------------------------------------------

def create_order(
    cycle_id: int,
    ibkr_order_id: int,
    leg: str,
    action: str,
    order_type: str,
    quantity: int,
    limit_price: float | None = None,
) -> int:
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO trading.orders (
                    cycle_id, ibkr_order_id, leg, action,
                    order_type, quantity, limit_price
                ) VALUES (%s, %s, %s, %s, %s, %s, %s)
                RETURNING id
            """, (cycle_id, ibkr_order_id, leg, action, order_type, quantity, limit_price))
            return cur.fetchone()["id"]


def update_order_fill(
    ibkr_order_id: int,
    fill_price: float,
    fill_qty: int,
    status: str,
) -> None:
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE trading.orders
                SET fill_price = %s,
                    fill_qty = %s,
                    status = %s,
                    filled_at = CASE WHEN %s = 'FILLED' THEN NOW() ELSE filled_at END
                WHERE ibkr_order_id = %s
            """, (fill_price, fill_qty, status, status, ibkr_order_id))


def cancel_pending_orders(cycle_id: int) -> list[dict]:
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE trading.orders
                SET status = 'CANCELLED', cancelled_at = NOW()
                WHERE cycle_id = %s AND status IN ('PENDING', 'PARTIAL')
                RETURNING ibkr_order_id
            """, (cycle_id,))
            return cur.fetchall()


def get_pending_orders(cycle_id: int) -> list[dict]:
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT * FROM trading.orders
                WHERE cycle_id = %s AND status IN ('PENDING', 'PARTIAL')
            """, (cycle_id,))
            return cur.fetchall()


# ---------------------------------------------------------------------------
# Harvest operations
# ---------------------------------------------------------------------------

def record_harvest(
    cycle_id: int,
    order_id: int,
    leg: str,
    contracts: int,
    entry_px: float,
    exit_px: float,
) -> int:
    pnl = (exit_px - entry_px) * contracts * 100  # each contract = 100 shares
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO trading.harvests (
                    cycle_id, order_id, leg, contracts,
                    entry_px, exit_px, pnl_usd
                ) VALUES (%s, %s, %s, %s, %s, %s, %s)
                RETURNING id
            """, (cycle_id, order_id, leg, contracts, entry_px, exit_px, pnl))
            return cur.fetchone()["id"]


# ---------------------------------------------------------------------------
# Sweep operations
# ---------------------------------------------------------------------------

def create_sweep(harvest_id: int, usd_amount: float) -> int:
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO trading.sweeps (harvest_id, usd_amount)
                VALUES (%s, %s)
                RETURNING id
            """, (harvest_id, usd_amount))
            return cur.fetchone()["id"]


def update_sweep_deployed(
    sweep_id: int,
    sgd_amount: float,
    fx_rate: float,
    ticker: str,
    shares: float,
    buy_price: float,
) -> None:
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE trading.sweeps
                SET sgd_amount = %s,
                    fx_rate = %s,
                    status = 'DEPLOYED',
                    deployed_to = %s,
                    shares_bought = %s,
                    buy_price_sgd = %s,
                    converted_at = NOW(),
                    deployed_at = NOW()
                WHERE id = %s
            """, (sgd_amount, fx_rate, ticker, shares, buy_price, sweep_id))


def get_pending_sweeps() -> list[dict]:
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT * FROM trading.sweeps
                WHERE status = 'PENDING'
                ORDER BY created_at ASC
            """)
            return cur.fetchall()


# ---------------------------------------------------------------------------
# Equity pool operations
# ---------------------------------------------------------------------------

def get_equity_pool() -> list[dict]:
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT * FROM trading.equity_pool ORDER BY ticker")
            return cur.fetchall()


def update_equity_position(
    ticker: str,
    new_shares: float,
    buy_price: float,
    invested_sgd: float,
) -> None:
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE trading.equity_pool
                SET total_shares = total_shares + %s,
                    total_invested_sgd = total_invested_sgd + %s,
                    avg_cost_sgd = (total_invested_sgd + %s) / (total_shares + %s),
                    last_updated = NOW()
                WHERE ticker = %s
            """, (new_shares, invested_sgd, invested_sgd, new_shares, ticker))


# ---------------------------------------------------------------------------
# Eval log
# ---------------------------------------------------------------------------

def log_eval(
    eval_date: date,
    market_open: bool,
    cycle_id: int | None,
    call_mark: float | None,
    put_mark: float | None,
    call_action: str | None,
    put_action: str | None,
    dte: int | None,
    notes: str | None = None,
) -> None:
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO trading.eval_log (
                    eval_date, market_open, active_cycle_id,
                    call_mark, put_mark, call_action, put_action,
                    dte_at_eval, notes
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (eval_date) DO UPDATE
                SET run_at = NOW(),
                    call_mark = EXCLUDED.call_mark,
                    put_mark = EXCLUDED.put_mark,
                    call_action = EXCLUDED.call_action,
                    put_action = EXCLUDED.put_action,
                    notes = EXCLUDED.notes
            """, (
                eval_date, market_open, cycle_id,
                call_mark, put_mark, call_action, put_action,
                dte, notes,
            ))


# ---------------------------------------------------------------------------
# Audit log
# ---------------------------------------------------------------------------

def audit(level: str, component: str, message: str, payload: dict | None = None) -> None:
    import json
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO trading.audit (level, component, message, payload)
                VALUES (%s, %s, %s, %s)
            """, (level, component, message, json.dumps(payload) if payload else None))
