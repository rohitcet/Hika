"""
main.py — Entry point and scheduler

Scheduled jobs:
  3:45 PM EST  Mon–Fri  →  Daily evaluation (harvest / stop / refill)
  3:58 PM EST  Mon–Fri  →  Cleanup sweep (cancel pending orders)
  9:05 AM SGT  Mon–Fri  →  SGX deployment (deploy pending sweeps)
  2:00 AM EST  Daily    →  Gateway restart (clean reconnect)

HTTP health endpoint on :8080 for Railway uptime checks.
"""

import logging
import os
import sys
import signal
import threading
from datetime import datetime
from http.server import BaseHTTPRequestHandler, HTTPServer

import pytz
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger

import db
import alerter
from gateway import IBGateway
from order_manager import OrderManager
from evaluator import Evaluator
from sgx_deployer import deploy_pending_sweeps

# ---------------------------------------------------------------------------
# Logging setup
# ---------------------------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s %(name)-20s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    stream=sys.stdout,
)
log = logging.getLogger(__name__)

EST = pytz.timezone("America/New_York")
SGT = pytz.timezone("Asia/Singapore")

# ---------------------------------------------------------------------------
# Health check HTTP server
# ---------------------------------------------------------------------------

_healthy = True


class HealthHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path == "/health":
            status = 200 if _healthy else 503
            self.send_response(status)
            self.send_header("Content-Type", "text/plain")
            self.end_headers()
            self.wfile.write(b"OK" if _healthy else b"UNHEALTHY")
        else:
            self.send_response(404)
            self.end_headers()

    def log_message(self, format, *args):
        pass  # suppress access logs


def _start_health_server():
    server = HTTPServer(("0.0.0.0", 8080), HealthHandler)
    t = threading.Thread(target=server.serve_forever, daemon=True, name="health-http")
    t.start()
    log.info("Health server started on :8080")


# ---------------------------------------------------------------------------
# Scheduled jobs
# ---------------------------------------------------------------------------

def job_daily_evaluation():
    """3:45 PM EST — core evaluation engine."""
    log.info("=== JOB: daily_evaluation ===")
    try:
        evaluator.run()
    except Exception as e:
        alerter.critical("SCHEDULER", f"daily_evaluation job failed: {e}")


def job_cleanup_sweep():
    """3:58 PM EST — cancel all pending orders."""
    log.info("=== JOB: cleanup_sweep ===")
    try:
        cycle = db.get_active_cycle()
        if cycle:
            order_mgr.cleanup_sweep(cycle["id"])
    except Exception as e:
        alerter.critical("SCHEDULER", f"cleanup_sweep job failed: {e}")


def job_sgx_deploy():
    """9:05 AM SGT — deploy pending sweeps to SGX equities."""
    log.info("=== JOB: sgx_deploy ===")
    try:
        deploy_pending_sweeps(gw)
    except Exception as e:
        alerter.critical("SCHEDULER", f"sgx_deploy job failed: {e}")


def job_gateway_restart():
    """2:00 AM EST — clean reconnect to IB Gateway."""
    log.info("=== JOB: gateway_restart ===")
    try:
        gw.disconnect()
        import time; time.sleep(5)
        gw.connect()
        alerter.info("SCHEDULER", "Gateway restarted cleanly")
    except Exception as e:
        alerter.critical("SCHEDULER", f"gateway_restart failed: {e}")


# ---------------------------------------------------------------------------
# Startup
# ---------------------------------------------------------------------------

def startup() -> None:
    global _healthy

    log.info("=== Trading agent starting up ===")
    log.info(f"TRADING_MODE: {os.environ.get('TRADING_MODE', 'paper')}")
    log.info(f"IB_TRADING_MODE: {os.environ.get('IB_TRADING_MODE', 'paper')}")
    log.info(f"MAX_LEGS: {os.environ.get('MAX_LEGS', '4')}")
    log.info(f"PROFIT_TARGET_PCT: {os.environ.get('PROFIT_TARGET_PCT', '0.15')}")

    # Verify required env vars
    required = [
        "DATABASE_URL", "TELEGRAM_BOT_TOKEN", "TELEGRAM_CHAT_ID",
        "IB_GATEWAY_HOST",
    ]
    missing = [k for k in required if not os.environ.get(k)]
    if missing:
        log.critical(f"Missing required env vars: {missing}")
        sys.exit(1)

    # Init database
    db.init_pool()
    db.bootstrap_schema()
    log.info("Database ready")

    # Init alerter
    alerter.init()
    log.info("Alerter ready")

    # Connect to IB Gateway
    gw.connect()
    log.info("IB Gateway connected")

    _healthy = True
    alerter.info("SCHEDULER", "Trading agent started successfully")


def shutdown(signum, frame):
    global _healthy
    log.info("Shutdown signal received")
    _healthy = False
    alerter.warn("SCHEDULER", "Trading agent shutting down")
    scheduler.shutdown(wait=False)
    gw.disconnect()
    sys.exit(0)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    # Instantiate singletons
    gw = IBGateway()
    order_mgr = OrderManager(gw)
    evaluator = Evaluator(gw, order_mgr)

    # Signal handlers
    signal.signal(signal.SIGTERM, shutdown)
    signal.signal(signal.SIGINT, shutdown)

    # Health server
    _start_health_server()

    # Run startup sequence
    startup()

    # Configure scheduler
    scheduler = BackgroundScheduler(timezone=EST)

    # 3:45 PM EST Mon–Fri — evaluation
    scheduler.add_job(
        job_daily_evaluation,
        CronTrigger(
            day_of_week="mon-fri",
            hour=15, minute=45,
            timezone=EST,
        ),
        id="daily_evaluation",
        name="Daily evaluation engine",
        max_instances=1,
        coalesce=True,
    )

    # 3:58 PM EST Mon–Fri — order cleanup
    scheduler.add_job(
        job_cleanup_sweep,
        CronTrigger(
            day_of_week="mon-fri",
            hour=15, minute=58,
            timezone=EST,
        ),
        id="cleanup_sweep",
        name="Order cleanup sweep",
        max_instances=1,
        coalesce=True,
    )

    # 9:05 AM SGT Mon–Fri — SGX deployment
    # 9:05 SGT = 1:05 AM EST (UTC+8 vs UTC-5)
    scheduler.add_job(
        job_sgx_deploy,
        CronTrigger(
            day_of_week="mon-fri",
            hour=9, minute=5,
            timezone=SGT,
        ),
        id="sgx_deploy",
        name="SGX equity deployment",
        max_instances=1,
        coalesce=True,
    )

    # 2:00 AM EST daily — gateway clean restart
    scheduler.add_job(
        job_gateway_restart,
        CronTrigger(
            hour=2, minute=0,
            timezone=EST,
        ),
        id="gateway_restart",
        name="Gateway daily restart",
        max_instances=1,
        coalesce=True,
    )

    scheduler.start()
    log.info("Scheduler started — all jobs registered")
    log.info("Next jobs:")
    for job in scheduler.get_jobs():
        log.info(f"  {job.name}: next run = {job.next_run_time}")

    alerter.info("SCHEDULER", "All scheduled jobs active — agent is live")

    # Keep main thread alive
    import time
    while True:
        time.sleep(60)
