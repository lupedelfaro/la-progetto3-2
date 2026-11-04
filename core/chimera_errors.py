# -*- coding: utf-8 -*-
"""
CHIMERA v4.0 — Error Tracking System
Posizione: core/chimera_errors.py

Ogni modulo usa:
    from core.chimera_errors import ErrorTracker
    _err = ErrorTracker("NomeModulo")

    except Exception as e:
        _err.capture(e, "nome_metodo", {"asset": asset})
"""

import os
import sys
import json
import time
import logging
import threading
import traceback
from datetime import datetime
from collections import defaultdict

_CORE_DIR    = os.path.dirname(os.path.abspath(__file__))
_ROOT_DIR    = os.path.dirname(_CORE_DIR)
_ERRORS_FILE = os.path.join(_ROOT_DIR, "chimera_errors.json")
_MAX_ERRORS  = 500
_lock        = threading.Lock()

# ── Soglia per alert Telegram automatico ─────────────────────
# Se lo stesso errore (modulo+categoria) supera questa soglia → alert
_ALERT_THRESHOLD = 3

# ── Categorie automatiche ────────────────────────────────────
_CATEGORY_MAP = {
    "KeyError":             "DATA_MISSING",
    "IndexError":           "DATA_MISSING",
    "TypeError":            "TYPE_ERROR",
    "ValueError":           "VALUE_ERROR",
    "AttributeError":       "ATTRIBUTE_ERROR",
    "ZeroDivisionError":    "MATH_ERROR",
    "TimeoutError":         "NETWORK_TIMEOUT",
    "ConnectionError":      "NETWORK_ERROR",
    "JSONDecodeError":      "JSON_PARSE",
    "OperationalError":     "DB_ERROR",
    "sqlite3":              "DB_ERROR",
    "ccxt.BaseError":       "EXCHANGE_ERROR",
    "ccxt.NetworkError":    "NETWORK_ERROR",
    "ccxt.ExchangeError":   "EXCHANGE_ERROR",
    "NameError":            "CODE_BUG",
    "ImportError":          "IMPORT_ERROR",
}

_CRITICAL_CATEGORIES = {
    "INSUFFICIENT_FUNDS", "DB_ERROR", "AUTH_ERROR",
    "CODE_BUG", "IMPORT_ERROR"
}

def _categorize(exc: Exception) -> str:
    exc_type = type(exc).__name__
    for k, v in _CATEGORY_MAP.items():
        if k in exc_type:
            return v
    msg = str(exc).lower()
    if "insufficient" in msg:  return "INSUFFICIENT_FUNDS"
    if "invalid" in msg:       return "INVALID_ORDER"
    if "timeout" in msg:       return "NETWORK_TIMEOUT"
    if "rate limit" in msg:    return "RATE_LIMIT"
    if "not found" in msg:     return "NOT_FOUND"
    if "permission" in msg:    return "AUTH_ERROR"
    if "not defined" in msg:   return "CODE_BUG"
    return "UNKNOWN"

def _load() -> dict:
    try:
        if os.path.exists(_ERRORS_FILE):
            with open(_ERRORS_FILE, "r") as f:
                return json.load(f)
    except Exception:
        pass
    return {"errors": [], "counts": {}, "alerts_sent": {}, "last_reset": datetime.now().isoformat()}

def _save(data: dict):
    try:
        with open(_ERRORS_FILE, "w") as f:
            json.dump(data, f, indent=2)
    except Exception:
        pass

def _send_telegram(message: str):
    """Invia alert Telegram in modo silenzioso (non blocca mai)."""
    try:
        from core import config_la
        token   = getattr(config_la, 'TELEGRAM_TOKEN', None)
        chat_id = getattr(config_la, 'TELEGRAM_CHAT_ID', None)
        if not token or not chat_id:
            return
        import requests
        requests.post(
            f"https://api.telegram.org/bot{token}/sendMessage",
            data={"chat_id": chat_id, "text": message, "parse_mode": "Markdown"},
            timeout=5
        )
    except Exception:
        pass  # Mai bloccare per un alert fallito


class ErrorTracker:
    """
    Tracker per singolo modulo.

    Uso:
        _err = ErrorTracker("StrategyEngine")

        try:
            ...
        except Exception as e:
            _err.capture(e, "analyze", {"ticker": ticker})
            return None
    """

    def __init__(self, module: str):
        self.module  = module
        self.logger  = logging.getLogger(f"Chimera.{module}")
        self._counts = defaultdict(int)

    def capture(
        self,
        exc: Exception,
        method: str = "",
        context: dict = None,
        level: str = "ERROR"
    ) -> str:
        category = _categorize(exc)
        exc_type = type(exc).__name__
        exc_msg  = str(exc)
        stack    = traceback.format_exc()
        ts       = datetime.now().isoformat()
        error_id = f"{self.module[:4].upper()}-{int(time.time()*1000) % 999999:06d}"

        # ── 1. LOG STANDARD ──────────────────────────────────
        log_msg = (
            f"[{error_id}] {self.module}.{method} | "
            f"{category} | {exc_type}: {exc_msg}"
        )
        if level == "CRITICAL":
            self.logger.critical(log_msg)
        elif level == "WARNING":
            self.logger.warning(log_msg)
        else:
            self.logger.error(log_msg)

        # ── 2. CONTEGGI IN-MEMORY ─────────────────────────────
        self._counts[category] += 1

        # ── 3. SALVATAGGIO FILE ───────────────────────────────
        record = {
            "id":       error_id,
            "ts":       ts,
            "module":   self.module,
            "method":   method,
            "category": category,
            "exc_type": exc_type,
            "message":  exc_msg[:300],
            "context":  {k: str(v)[:100] for k, v in (context or {}).items()},
            "stack":    stack[-800:] if stack else "",
            "level":    level,
        }

        should_alert = False
        alert_key    = f"{self.module}.{category}"

        with _lock:
            data = _load()
            data["errors"].insert(0, record)
            data["errors"] = data["errors"][:_MAX_ERRORS]
            data["counts"][alert_key] = data["counts"].get(alert_key, 0) + 1

            # Auto-elevazione a CRITICAL per categorie critiche
            if category in _CRITICAL_CATEGORIES and level == "ERROR":
                record["level"] = "CRITICAL"
                level = "CRITICAL"

            # Controlla soglia alert Telegram
            alerts_sent = data.get("alerts_sent", {})
            last_alert_count = alerts_sent.get(alert_key, 0)
            current_count    = data["counts"][alert_key]

            if (current_count >= _ALERT_THRESHOLD and
                    current_count % _ALERT_THRESHOLD == 0 and
                    current_count != last_alert_count):
                should_alert = True
                alerts_sent[alert_key] = current_count
                data["alerts_sent"] = alerts_sent

            # Sempre alert per CRITICAL
            if level == "CRITICAL":
                should_alert = True

            _save(data)

        # ── 4. ALERT TELEGRAM (fuori dal lock) ───────────────
        if should_alert:
            icon = "🔴" if level == "CRITICAL" else "🟠"
            msg  = (
                f"{icon} *CHIMERA ERROR ALERT*\n"
                f"Modulo: `{self.module}.{method}`\n"
                f"Categoria: `{category}`\n"
                f"Errore: `{exc_type}: {exc_msg[:150]}`\n"
                f"ID: `{error_id}`\n"
                f"Occorrenze: {data['counts'].get(alert_key, 1)}"
            )
            if context:
                ctx_str = " | ".join(f"{k}={v}" for k, v in list(context.items())[:3])
                msg += f"\nContesto: `{ctx_str}`"
            threading.Thread(target=_send_telegram, args=(msg,), daemon=True).start()

        return error_id

    def get_module_counts(self) -> dict:
        return dict(self._counts)

    @staticmethod
    def get_summary(top_n: int = 10) -> dict:
        data   = _load()
        errors = data.get("errors", [])
        by_module   = defaultdict(int)
        by_category = defaultdict(int)
        critical    = 0
        for e in errors:
            by_module[e.get("module", "?")] += 1
            by_category[e.get("category", "?")] += 1
            if e.get("level") == "CRITICAL":
                critical += 1
        return {
            "total":          len(errors),
            "critical_count": critical,
            "top_modules":    sorted(by_module.items(),   key=lambda x: -x[1])[:top_n],
            "top_categories": sorted(by_category.items(), key=lambda x: -x[1])[:top_n],
            "recent":         errors[:5],
            "counts_raw":     data.get("counts", {}),
        }

    @staticmethod
    def reset():
        with _lock:
            _save({
                "errors":      [],
                "counts":      {},
                "alerts_sent": {},
                "last_reset":  datetime.now().isoformat()
            })
        logging.getLogger("Chimera.ErrorTracker").info("🔄 chimera_errors.json azzerato")
