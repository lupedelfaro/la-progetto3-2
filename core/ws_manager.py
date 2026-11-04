# -*- coding: utf-8 -*-
"""
WebSocket Manager per Kraken v2 — implementazione reale.
Connessione real-time per ticker, trades, orderbook.

Canali pubblici: wss://ws.kraken.com/v2
  - ticker  → prezzo bid/ask/last real-time
  - trade   → ogni trade eseguito (per CVD, velocity, VPIN)
  - book    → orderbook top-10 (per spread e liquidità)

Canali privati: wss://ws-auth.kraken.com/v2
  - executions → ogni fill/cancellazione ordine in tempo reale
  - balances   → cambio saldo/margine in tempo reale
"""

import logging
import json
import threading
import time
import collections
import sys
import hmac
import hashlib
import base64
import urllib.parse
from core.chimera_errors import ErrorTracker

_err = ErrorTracker("WSManager")

logger = logging.getLogger("KrakenWSManager")

_TICKER_MAP = {
    # Asset principali
    "XXBTZUSD": "BTC/USD",
    "XETHZUSD": "ETH/USD",
    "SOLUSD":   "SOL/USD",
    "XXRPZUSD": "XRP/USD",
    "XDGUSD":   "DOGE/USD",
    # Asset secondari monitorati dal bot
    "TAOUSD":   "TAO/USD",
    "FETUSD":   "FET/USD",
    "BONKUSD":  "BONK/USD",
    "AAVEUSD":  "AAVE/USD",
    "XZECZUSD": "ZEC/USD",
    "POLUSD":   "POL/USD",
    "LINKUSD":  "LINK/USD",
    "AVAXUSD":  "AVAX/USD",
    "ADAUSD":   "ADA/USD",
    "DOTUSD":   "DOT/USD",
}

WS_PUBLIC_URL  = "wss://ws.kraken.com/v2"
WS_PRIVATE_URL = "wss://ws-auth.kraken.com/v2"
MAX_TRADES_CACHE = 500


class KrakenWSManager:
    def __init__(self):
        self.logger = logging.getLogger("KrakenWSManager")
        self.api_key    = None
        self.api_secret = None
        self.position_callback  = None
        self.execution_callback = None  # callback(event_type, data) per fill ordini
        self.is_running = False

        self._ticker_cache = {}
        self._trades_cache = {}
        self._book_cache   = {}
        self._ohlc_cache   = {}  # symbol → timeframe → deque di candele
        self._lock = threading.Lock()

        # Cache eventi privati
        self._executions_cache = collections.deque(maxlen=200)
        self._balance_cache    = {}

        # WS pubblico
        self._ws_public_thread = None
        self._ws_public        = None
        self._connected_public = False
        self._reconnect_delay  = 2

        # WS privato
        self._ws_private_thread   = None
        self._ws_private          = None
        self._connected_private   = False
        self._reconnect_delay_prv = 2
        self._ws_token            = None
        self._token_expiry        = 0.0

        self.logger.info("✅ KrakenWSManager inizializzato (WebSocket v2 reale)")

    def set_credentials(self, api_key, api_secret):
        self.api_key    = api_key
        self.api_secret = api_secret
        self.logger.info("🔑 Credenziali WebSocket impostate")

    def set_position_callback(self, callback):
        self.position_callback = callback
        self.logger.info("📞 Position callback registrato")

    def set_execution_callback(self, callback):
        """
        Registra callback per eventi di esecuzione ordini.
        Firma: callback(event_type: str, data: dict)
        event_type: 'filled' | 'canceled' | 'pending' | 'balance'
        """
        self.execution_callback = callback
        self.logger.info("📞 Execution callback registrato")

    def start(self):
        if self.is_running:
            self.logger.warning("⚠️ WebSocket già in esecuzione")
            return
        self.is_running = True

        # WS pubblico
        self._ws_public_thread = threading.Thread(
            target=self._run_public_ws,
            daemon=True,
            name="KrakenWS-Public"
        )
        self._ws_public_thread.start()

        # WS privato (solo se credenziali disponibili)
        if self.api_key and self.api_secret:
            self._ws_private_thread = threading.Thread(
                target=self._run_private_ws,
                daemon=True,
                name="KrakenWS-Private"
            )
            self._ws_private_thread.start()

        self.logger.info("🔌 WebSocket pubblico+privato avviati")

        # Aspetta connessione pubblico max 5s
        for _ in range(25):
            if self._connected_public:
                break
            time.sleep(0.2)
        if self._connected_public:
            self.logger.info("✅ WebSocket pubblico connesso — dati real-time attivi")
        else:
            self.logger.warning("⚠️ WebSocket pubblico non connesso entro 5s")

    def stop(self):
        self.is_running = False
        for ws in [self._ws_public, self._ws_private]:
            if ws:
                try:
                    ws.close()
                except Exception:
                    pass
        self.logger.info("🔌 WebSocket fermato")

    # ── TOKEN AUTENTICAZIONE ──────────────────────────────────────────────────

    def _get_ws_token(self) -> str:
        """
        Ottiene il token WebSocket tramite l'exchange ccxt già autenticato.
        Più affidabile che rifare la firma HMAC manualmente.
        """
        if self._ws_token and time.time() < self._token_expiry - 60:
            return self._ws_token
        try:
            # Usa ccxt se disponibile (già autenticato)
            if hasattr(self, '_ccxt_exchange') and self._ccxt_exchange:
                res = self._ccxt_exchange.private_post_getwebsocketstoken()
                token = res.get('result', {}).get('token')
                expires = int(res.get('result', {}).get('expires', 900))
            else:
                # Fallback: firma manuale
                import requests as _req
                nonce    = str(int(time.time() * 1000))
                post_data = f"nonce={nonce}"
                url_path  = "/0/private/GetWebSocketsToken"
                msg       = (nonce + post_data).encode()
                sha256    = __import__('hashlib').sha256(msg).digest()
                hmac_data = url_path.encode() + sha256
                import hmac as _hmac, base64 as _b64
                sig = _b64.b64encode(
                    _hmac.new(_b64.b64decode(self.api_secret),
                              hmac_data, __import__('hashlib').sha512).digest()
                ).decode()
                resp = _req.post(
                    f"https://api.kraken.com{url_path}",
                    data=post_data,
                    headers={"API-Key": self.api_key, "API-Sign": sig,
                             "Content-Type": "application/x-www-form-urlencoded"},
                    timeout=10
                )
                result = resp.json()
                if result.get("error"):
                    self.logger.error(f"❌ Token WS error: {result['error']}")
                    return None
                token   = result["result"]["token"]
                expires = int(result["result"].get("expires", 900))

            if token:
                self._ws_token    = token
                self._token_expiry = time.time() + expires
                self.logger.info(f"🔑 Token WS ottenuto (valido {expires}s)")
                return token
        except Exception as e:
            _err.capture(e, "_get_ws_token", {"module": "WSManager"})
            self.logger.error(f"❌ Errore recupero token WS: {e}")
        return None

    def set_ccxt_exchange(self, exchange):
        """Collega l'exchange ccxt già autenticato per il token WS."""
        self._ccxt_exchange = exchange

    # ── WS PUBBLICO ───────────────────────────────────────────────────────────

    def _run_public_ws(self):
        try:
            import websocket as ws_lib
        except ImportError as e:
            _err.capture(e, "_run_public_ws", {"module": "WSManager"})
            self.logger.error(f"❌ websocket-client ImportError: {e}")
            import sys, traceback
            self.logger.error(f"   sys.path[0]={sys.path[0] if sys.path else 'EMPTY'}")
            self.logger.error(f"   traceback: {traceback.format_exc()}")
            return
        except Exception as e:
            # Cattura ogni altra eccezione durante l'import (es. AttributeError,
            # RuntimeError, problemi di compatibilità Python 3.14)
            _err.capture(e, "_run_public_ws", {"module": "WSManager"})
            self.logger.error(f"❌ websocket import failed [{type(e).__name__}]: {e}")
            import traceback
            self.logger.error(f"   traceback: {traceback.format_exc()}")
            return

        # Verifica esplicita che WebSocketApp esista (sanity check anti-shadowing)
        if not hasattr(ws_lib, 'WebSocketApp'):
            self.logger.error(
                f"❌ Modulo 'websocket' importato ma manca WebSocketApp! "
                f"File caricato: {getattr(ws_lib, '__file__', 'unknown')}. "
                f"Probabile shadowing da file locale o pacchetto sbagliato."
            )
            return

        delay = self._reconnect_delay
        while self.is_running:
            try:
                self.logger.info(f"🔌 Connessione pubblica a {WS_PUBLIC_URL}")
                self._ws_public = ws_lib.WebSocketApp(
                    WS_PUBLIC_URL,
                    on_open    = self._on_open,
                    on_message = self._on_message,
                    on_error   = self._on_error,
                    on_close   = self._on_close,
                )
                self._ws_public.run_forever(ping_interval=20, ping_timeout=10)
            except Exception as e:
                _err.capture(e, "_run_public_ws", {"module": "WSManager"})
                self.logger.error(f"❌ Errore WS pubblico: {e}")
            if not self.is_running:
                break
            self._connected_public = False
            self.logger.info(f"🔄 Riconnessione pubblica in {delay}s...")
            time.sleep(delay)
            delay = min(delay * 2, 30)

    # ── WS PRIVATO ────────────────────────────────────────────────────────────

    def _run_private_ws(self):
        try:
            import websocket as ws_lib
        except ImportError as e:
            self.logger.error(f"❌ websocket-client ImportError (private WS): {e}")
            return
        except Exception as e:
            _err.capture(e, "_run_private_ws", {"module": "WSManager"})
            self.logger.error(f"❌ websocket import failed private [{type(e).__name__}]: {e}")
            return

        delay = self._reconnect_delay_prv
        while self.is_running:
            try:
                token = self._get_ws_token()
                if not token:
                    self.logger.warning("⚠️ Token WS non disponibile — riprovo in 30s")
                    time.sleep(30)
                    continue

                self.logger.info(f"🔌 Connessione privata a {WS_PRIVATE_URL}")
                self._ws_private = ws_lib.WebSocketApp(
                    WS_PRIVATE_URL,
                    on_open    = lambda ws: self._on_private_open(ws, token),
                    on_message = self._on_private_message,
                    on_error   = lambda ws, e: self.logger.warning(f"⚠️ WS privato error: {e}"),
                    on_close   = lambda ws, c, m: self._on_private_close(ws, c, m),
                )
                self._ws_private.run_forever(ping_interval=20, ping_timeout=10)
            except Exception as e:
                _err.capture(e, "_run_private_ws", {"module": "WSManager"})
                self.logger.error(f"❌ Errore WS privato: {e}")
            if not self.is_running:
                break
            self._connected_private = False
            self.logger.info(f"🔄 Riconnessione privata in {delay}s...")
            time.sleep(delay)
            delay = min(delay * 2, 60)

    def _on_private_open(self, ws, token):
        self._connected_private   = True
        self._reconnect_delay_prv = 2
        self.logger.info("✅ WebSocket privato connesso")

        # Sottoscrivi executions (fill ordini) e balances
        for channel in ("executions", "balances"):
            payload = {
                "method": "subscribe",
                "params": {
                    "channel": channel,
                    "token":   token,
                    "snap_orders": True,   # snapshot ordini aperti all'avvio
                }
            }
            ws.send(json.dumps(payload))
        self.logger.info("📡 WS privato: sottoscritto a executions + balances")

    def _on_private_close(self, ws, code, msg):
        self._connected_private = False
        self.logger.info(f"🔌 WS privato chiuso (code={code})")
        # Rinnova token alla prossima connessione
        self._ws_token = None

    def _on_private_message(self, ws, raw):
        try:
            msg = json.loads(raw)
            if not isinstance(msg, dict):
                return
            mtype   = msg.get("type") or msg.get("method") or ""
            channel = msg.get("channel", "")
            data    = msg.get("data", [])

            if mtype in ("heartbeat", "pong", "subscribe", "subscriptionStatus"):
                return

            if channel == "executions":
                self._handle_executions(data)
            elif channel == "balances":
                self._handle_balances(data)

        except Exception as e:
            _err.capture(e, "_on_private_message", {"module": "WSManager"})
            self.logger.debug(f"WS privato parsing: {e}")

    def _handle_executions(self, data):
        """
        Gestisce eventi di esecuzione ordini in tempo reale.
        Ogni evento contiene lo stato aggiornato dell'ordine.
        """
        for item in data:
            try:
                order_status = str(item.get("order_status", "")).lower()
                order_id     = item.get("order_id", "")
                cl_ord_id    = item.get("cl_ord_id", "")
                symbol       = item.get("symbol", "")
                side         = item.get("side", "")
                filled_qty   = float(item.get("cum_qty", 0) or 0)
                avg_price    = float(item.get("avg_price", 0) or 0)
                fee          = float(item.get("fee_paid", 0) or 0)

                event = {
                    "order_id":    order_id,
                    "cl_ord_id":   cl_ord_id,
                    "symbol":      symbol,
                    "side":        side,
                    "status":      order_status,
                    "filled_qty":  filled_qty,
                    "avg_price":   avg_price,
                    "fee":         fee,
                    "raw":         item,
                    "ts":          time.time(),
                }

                with self._lock:
                    self._executions_cache.append(event)

                # Classifica il tipo di evento
                if order_status in ("filled", "closed"):
                    event_type = "filled"
                    self.logger.info(
                        f"✅ [WS EXEC] FILLED: {symbol} {side} "
                        f"qty={filled_qty} @ {avg_price} "
                        f"(id={order_id or cl_ord_id})"
                    )
                elif order_status == "canceled":
                    event_type = "canceled"
                    self.logger.info(
                        f"🗑️ [WS EXEC] CANCELED: {symbol} {side} "
                        f"(id={order_id or cl_ord_id})"
                    )
                elif order_status in ("pending_new", "new"):
                    event_type = "pending"
                else:
                    event_type = order_status

                # Chiama callback se registrato
                if self.execution_callback:
                    try:
                        self.execution_callback(event_type, event)
                    except Exception as e_cb:
                        _err.capture(e_cb, sys._getframe().f_code.co_name, {"module": "KrakenWSManager"})
                        self.logger.debug(f"Execution callback error: {e_cb}")

                # Chiama anche position_callback per compatibilità
                if self.position_callback and event_type == "filled":
                    try:
                        self.position_callback(symbol, event)
                    except Exception as e_cb:
                        _err.capture(e_cb, sys._getframe().f_code.co_name, {"module": "KrakenWSManager"})
                        self.logger.debug(f"Position callback error: {e_cb}")

            except Exception as e:
                _err.capture(e, "_handle_executions", {"module": "WSManager"})

    def _handle_balances(self, data):
        """Aggiorna cache saldo/margine in tempo reale."""
        for item in data:
            try:
                asset = item.get("asset", "")
                if asset:
                    with self._lock:
                        self._balance_cache[asset] = {
                            "balance":  float(item.get("balance", 0) or 0),
                            "hold":     float(item.get("hold_trade", 0) or 0),
                            "ts":       time.time(),
                        }
            except Exception:
                continue

    # ── PUBLIC READ API ───────────────────────────────────────────────────────

    def get_last_execution(self, order_id: str = None, cl_ord_id: str = None) -> dict:
        """
        Restituisce l'ultimo evento di esecuzione per un ordine specifico.
        Usato da trade_manager per sapere se un LIMIT è stato fillato.
        """
        with self._lock:
            cache = list(self._executions_cache)
        for event in reversed(cache):
            if order_id and event.get("order_id") == order_id:
                return event
            if cl_ord_id and event.get("cl_ord_id") == cl_ord_id:
                return event
        return {}

    def is_order_filled(self, order_id: str = None, cl_ord_id: str = None) -> bool:
        """Verifica se un ordine risulta filled dalla cache WS."""
        event = self.get_last_execution(order_id=order_id, cl_ord_id=cl_ord_id)
        return event.get("status") in ("filled", "closed")

    def is_order_canceled(self, order_id: str = None, cl_ord_id: str = None) -> bool:
        """Verifica se un ordine è stato cancellato."""
        event = self.get_last_execution(order_id=order_id, cl_ord_id=cl_ord_id)
        return event.get("status") == "canceled"

    def get_balance(self, asset: str = "ZUSD") -> float:
        """Saldo real-time da cache WS privato."""
        with self._lock:
            b = self._balance_cache.get(asset, {})
        return float(b.get("balance", 0))

    def is_private_connected(self) -> bool:
        return self._connected_private

    def _on_open(self, ws):
        self._connected_public = True
        self._reconnect_delay  = 2
        self.logger.info("✅ WebSocket v2 connesso")
        symbols = list(_TICKER_MAP.values())

        # Ticker, trade, book — come prima
        for channel in ("ticker", "trade", "book"):
            payload = {"method": "subscribe", "params": {"channel": channel, "symbol": symbols, "snapshot": True}}
            if channel == "book":
                payload["params"]["depth"] = 10
            ws.send(json.dumps(payload))

        # OHLC — candele real-time per i timeframe principali
        # Elimina le chiamate REST fetch_ohlcv per 1m, 5m, 15m, 1h
        for interval in (1, 5, 15, 60):
            ws.send(json.dumps({
                "method": "subscribe",
                "params": {
                    "channel":  "ohlc",
                    "symbol":   symbols,
                    "interval": interval,
                    "snapshot": True,
                }
            }))

        self.logger.info(f"📡 Sottoscritto a ticker+trade+book+ohlc(1m/5m/15m/1h) per {len(symbols)} simboli")

    def _on_message(self, ws, raw):
        try:
            msg = json.loads(raw)
            if not isinstance(msg, dict):
                return
            mtype = msg.get("type") or msg.get("method") or ""
            if mtype in ("heartbeat", "pong", "subscribe"):
                return
            channel = msg.get("channel", "")
            data    = msg.get("data", [])
            if not data:
                return
            if channel == "ticker":
                self._handle_ticker(data)
            elif channel == "trade":
                self._handle_trade(data)
            elif channel == "book":
                self._handle_book(data)
            elif channel == "ohlc":
                self._handle_ohlc(data)
        except Exception as e:
            _err.capture(e, sys._getframe().f_code.co_name, {"module": "WSManager"})
            self.logger.debug(f"Parsing WS: {e}")

    def _handle_ticker(self, data):
        for item in data:
            sym = item.get("symbol", "")
            if not sym:
                continue
            with self._lock:
                self._ticker_cache[sym] = {
                    "bid":  float(item.get("bid",  0) or 0),
                    "ask":  float(item.get("ask",  0) or 0),
                    "last": float(item.get("last", 0) or 0),
                    "ts":   time.time(),
                }

    def _handle_trade(self, data):
        for item in data:
            sym = item.get("symbol", "")
            if not sym:
                continue
            # Usa il timestamp del messaggio WS se disponibile — più preciso
            # del timestamp locale che include latenza di rete (~0-500ms)
            _ts_ws = item.get("timestamp")
            if _ts_ws:
                try:
                    # Kraken v2 manda ISO string "2026-04-11T12:34:56.123456Z"
                    from datetime import datetime, timezone
                    _ts_ms = int(datetime.fromisoformat(
                        str(_ts_ws).replace("Z", "+00:00")
                    ).timestamp() * 1000)
                except Exception:
                    _ts_ms = int(time.time() * 1000)
            else:
                _ts_ms = int(time.time() * 1000)
            with self._lock:
                if sym not in self._trades_cache:
                    self._trades_cache[sym] = collections.deque(maxlen=MAX_TRADES_CACHE)
                self._trades_cache[sym].append({
                    "timestamp": _ts_ms,
                    "price":     float(item.get("price", 0) or 0),
                    "amount":    float(item.get("qty",   0) or 0),
                    "side":      "buy" if str(item.get("side", "")).lower() in ("buy", "b") else "sell",
                })

    def _handle_book(self, data):
        for item in data:
            sym = item.get("symbol", "")
            if not sym:
                continue
            bids = [[float(b["price"]), float(b["qty"])] for b in item.get("bids", []) if b.get("price") and b.get("qty")]
            asks = [[float(a["price"]), float(a["qty"])] for a in item.get("asks", []) if a.get("price") and a.get("qty")]
            if bids or asks:
                with self._lock:
                    if sym not in self._book_cache:
                        self._book_cache[sym] = {"bids": [], "asks": []}
                    if bids:
                        self._book_cache[sym]["bids"] = sorted(bids, key=lambda x: -x[0])
                    if asks:
                        self._book_cache[sym]["asks"] = sorted(asks, key=lambda x:  x[0])

    def _handle_ohlc(self, data):
        """
        Gestisce candele OHLC real-time dal WS.
        Formato Kraken v2: ogni item ha symbol, interval_begin, interval,
        open, high, low, close, vwap, volume, trades.
        """
        for item in data:
            sym      = item.get("symbol", "")
            interval = item.get("interval", 0)  # minuti: 1, 5, 15, 60
            if not sym or not interval:
                continue
            try:
                candle = {
                    "ts":     item.get("interval_begin", item.get("timestamp", "")),
                    "open":   float(item.get("open",   0) or 0),
                    "high":   float(item.get("high",   0) or 0),
                    "low":    float(item.get("low",    0) or 0),
                    "close":  float(item.get("close",  0) or 0),
                    "volume": float(item.get("volume", 0) or 0),
                    "vwap":   float(item.get("vwap",   0) or 0),
                    "trades": int(item.get("trades",   0) or 0),
                }
                with self._lock:
                    if sym not in self._ohlc_cache:
                        self._ohlc_cache[sym] = {}
                    if interval not in self._ohlc_cache[sym]:
                        self._ohlc_cache[sym][interval] = collections.deque(maxlen=500)
                    # Aggiorna l'ultima candela se stesso timestamp, altrimenti appendi
                    cache = self._ohlc_cache[sym][interval]
                    if cache and cache[-1]["ts"] == candle["ts"]:
                        cache[-1] = candle  # aggiorna candela corrente
                    else:
                        cache.append(candle)
            except Exception:
                continue

    def _on_error(self, ws, error):
        self._connected_public = False
        self.logger.warning(f"⚠️ Errore WS: {error}")

    def _on_close(self, ws, code, msg):
        self._connected_public = False
        self.logger.info(f"🔌 WS chiuso (code={code})")

    def subscribe_executions(self):
        self.logger.debug("📡 executions: usa REST (WS privato non attivo)")

    def subscribe_positions(self):
        self.logger.debug("📡 positions: usa REST")

    def subscribe_ticker(self, symbol):
        self.logger.debug(f"📡 {symbol}: già sottoscritto all'avvio")

    def _ws_symbol(self, symbol: str) -> str:
        if "/" in symbol:
            return symbol
        return _TICKER_MAP.get(symbol, symbol)

    def get_ticker(self, symbol: str):
        ws_sym = self._ws_symbol(symbol)
        with self._lock:
            data = self._ticker_cache.get(ws_sym)
        if data and data.get("last", 0) > 0:
            if time.time() - data["ts"] > 10:
                return None  # dati stale → forza REST
            return {"bid": data["bid"], "ask": data["ask"], "last": data["last"], "baseVolume": 0}
        return None

    def get_trades(self, symbol: str, limit: int = 100) -> list:
        ws_sym = self._ws_symbol(symbol)
        with self._lock:
            cache = self._trades_cache.get(ws_sym)
            if not cache:
                return []
            return list(cache)[-limit:]

    def get_ohlcv(self, symbol: str, timeframe: str = "1m", limit: int = 100) -> list:
        """
        Restituisce candele OHLC dalla cache WS se disponibile,
        altrimenti ritorna lista vuota (fallback a REST nell'engine).
        """
        ws_sym = self._ws_symbol(symbol)
        # Mappa timeframe stringa → intervallo in minuti
        _TF_MAP = {"1m": 1, "5m": 5, "15m": 15, "1h": 60, "4h": 240, "1d": 1440}
        interval = _TF_MAP.get(timeframe, 0)
        if not interval:
            return []
        with self._lock:
            cache = self._ohlc_cache.get(ws_sym, {}).get(interval)
            if not cache:
                return []
            candles = list(cache)[-limit:]
        # Formato [timestamp_ms, open, high, low, close, volume]
        result = []
        for c in candles:
            try:
                from datetime import datetime, timezone
                ts_str = c.get("ts", "")
                if ts_str:
                    dt  = datetime.fromisoformat(str(ts_str).replace("Z", "+00:00"))
                    ts  = int(dt.timestamp() * 1000)
                else:
                    ts = 0
                result.append([ts, c["open"], c["high"], c["low"], c["close"], c["volume"]])
            except Exception:
                continue
        return result

    def get_ohlc(self, symbol: str, interval_min: int = 1, limit: int = 100) -> list:
        """Alias per get_ohlcv usando intervallo in minuti direttamente."""
        ws_sym = self._ws_symbol(symbol)
        with self._lock:
            cache = self._ohlc_cache.get(ws_sym, {}).get(interval_min)
            if not cache:
                return []
            return list(cache)[-limit:]

    def get_orderbook(self, symbol: str, limit: int = 10) -> dict:
        ws_sym = self._ws_symbol(symbol)
        with self._lock:
            book = self._book_cache.get(ws_sym, {})
            if book:
                return {"bids": book.get("bids", [])[:limit], "asks": book.get("asks", [])[:limit]}
        return {"bids": [], "asks": []}

    def get_order_book(self, symbol: str, limit: int = 10) -> dict:
        return self.get_orderbook(symbol, limit)

    def is_connected(self) -> bool:
        return self._connected_public

    def get_stats(self) -> dict:
        with self._lock:
            return {
                "connected":   self._connected_public,
                "ticker_syms": list(self._ticker_cache.keys()),
                "trade_counts": {s: len(v) for s, v in self._trades_cache.items()},
                "book_syms":   list(self._book_cache.keys()),
            }
