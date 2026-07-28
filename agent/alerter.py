"""
alerter.py — Telegram alert dispatcher
Three tiers: INFO (log only), WARN (Telegram), CRITICAL (Telegram + bold header)
"""

import os
import logging
import requests

import db

log = logging.getLogger(__name__)

_TOKEN = None
_CHAT_ID = None


def init() -> None:
    global _TOKEN, _CHAT_ID
    _TOKEN = os.environ["TELEGRAM_BOT_TOKEN"]
    _CHAT_ID = os.environ["TELEGRAM_CHAT_ID"]
    log.info("Alerter initialised")


def _send(text: str) -> None:
    if not _TOKEN or not _CHAT_ID:
        log.warning("Alerter not initialised — skipping Telegram send")
        return
    try:
        url = f"https://api.telegram.org/bot{_TOKEN}/sendMessage"
        resp = requests.post(url, json={
            "chat_id": _CHAT_ID,
            "text": text,
            "parse_mode": "HTML",
        }, timeout=10)
        resp.raise_for_status()
    except Exception as e:
        log.error(f"Telegram send failed: {e}")


def info(component: str, message: str, payload: dict | None = None) -> None:
    log.info(f"[{component}] {message}")
    db.audit("INFO", component, message, payload)


def warn(component: str, message: str, payload: dict | None = None) -> None:
    log.warning(f"[{component}] {message}")
    db.audit("WARN", component, message, payload)
    text = (
        f"⚠️ <b>WARN — {component}</b>\n"
        f"{message}"
    )
    if payload:
        text += f"\n<pre>{payload}</pre>"
    _send(text)


def critical(component: str, message: str, payload: dict | None = None) -> None:
    log.critical(f"[{component}] {message}")
    db.audit("CRITICAL", component, message, payload)
    text = (
        f"🚨 <b>CRITICAL — {component}</b>\n"
        f"<b>{message}</b>"
    )
    if payload:
        text += f"\n<pre>{payload}</pre>"
    _send(text)


def harvest_notice(
    leg: str,
    contracts: int,
    entry_px: float,
    exit_px: float,
    pnl_usd: float,
) -> None:
    pct = ((exit_px - entry_px) / entry_px) * 100
    text = (
        f"✅ <b>HARVEST — {leg} leg</b>\n"
        f"Contracts: {contracts}\n"
        f"Entry: ${entry_px:.2f} → Exit: ${exit_px:.2f} "
        f"(+{pct:.1f}%)\n"
        f"P&amp;L: <b>+${pnl_usd:.2f} USD</b>"
    )
    _send(text)
    db.audit("INFO", "HARVESTER", f"{leg} leg harvested +{pct:.1f}%", {
        "contracts": contracts,
        "entry_px": entry_px,
        "exit_px": exit_px,
        "pnl_usd": pnl_usd,
    })


def deploy_notice(
    ticker: str,
    shares: float,
    price_sgd: float,
    invested_sgd: float,
    fx_rate: float,
) -> None:
    text = (
        f"📈 <b>DEPLOYED → {ticker}</b>\n"
        f"Shares: {shares:.0f} @ SGD {price_sgd:.2f}\n"
        f"Invested: SGD {invested_sgd:.2f} "
        f"(FX: {fx_rate:.4f})"
    )
    _send(text)


def stop_loss_notice(leg: str, entry_px: float, mark_px: float, loss_pct: float) -> None:
    text = (
        f"🛑 <b>STOP LOSS — {leg} leg</b>\n"
        f"Entry: ${entry_px:.2f} → Mark: ${mark_px:.2f}\n"
        f"Loss: <b>-{loss_pct:.1f}%</b> — closing position"
    )
    _send(text)
    db.audit("WARN", "EVALUATOR", f"{leg} stop loss triggered -{loss_pct:.1f}%", {
        "entry_px": entry_px,
        "mark_px": mark_px,
    })


def daily_summary(
    eval_date: str,
    dte: int,
    call_action: str,
    put_action: str,
    call_mark: float | None,
    put_mark: float | None,
) -> None:
    text = (
        f"📊 <b>Daily summary — {eval_date}</b>\n"
        f"DTE: {dte}\n"
        f"Call: {call_mark and f'${call_mark:.2f}' or 'N/A'} → {call_action}\n"
        f"Put:  {put_mark and f'${put_mark:.2f}' or 'N/A'} → {put_action}"
    )
    _send(text)
