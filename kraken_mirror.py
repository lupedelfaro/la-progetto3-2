# -*- coding: utf-8 -*-
import time
import hmac
import hashlib
import base64
import requests
import json
import threading
import websocket
import urllib.parse
import logging
from core import asset_list
from core.chimera_errors import ErrorTracker

_err = ErrorTracker("KrakenMirror")

class KrakenMirror:
    """
    Crea uno specchio real-time dell'account Kraken.
    Riduce le chiamate REST e azzera la latenza sui prezzi.
    """
    def __init__(self, api_key, api_secret):
        self.api_key = api_key
        self.api_secret = api_secret
        self.base_url = "https://api.kraken.com"
        self.logger = logging.getLogger("KrakenMirror")
        
        # Specchio dei dati (Wallet, Prezzi)
        self.wallet = {}
        # Inizializziamo i prezzi a 0 per tutti gli asset principali
        self.live_prices = {asset: 0.0 for asset in asset_list.ASSET_PRINCIPALI}
        
        # Stato connessione
        self.is_synced = False

    # --- FIRMA DIGITALE (Standard Python 3) ---
    def _generate_signature(self, urlpath, data):
        postdata = urllib.parse.urlencode(data)
        encoded = (str(data['nonce']) + postdata).encode()
        message = urlpath.encode() + hashlib.sha256(encoded).digest()
        mac = hmac.new(base64.b64decode(self.api_secret), message, hashlib.sha512)
        return base64.b64encode(mac.digest()).decode()

    # --- SINCRONIZZAZIONE PORTAFOGLIO (REST) ---
    def sync_wallet(self):
        """Recupera il saldo attuale via REST API."""
        urlpath = '/0/private/Balance'
        data = {"nonce": int(time.time() * 1000)}
        headers = {
            "API-Key": self.api_key,
            "API-Sign": self._generate_signature(urlpath, data)
        }
        try:
            res = requests.post(self.base_url + urlpath, headers=headers, data=data, timeout=10)
            result = res.json()
            if not result.get('error'):
                self.wallet = result['result']
                self.logger.info("✅ Wallet sincronizzato con successo.")
                return self.wallet
            else:
                self.logger.error(f"❌ Errore API Balance: {result['error']}")
        except Exception as e:
            _err.capture(e, "sync_wallet", {"module": "KrakenMirror"})
            self.logger.error(f"❌ Eccezione sync_wallet: {e}")
        return None

    # --- SINCRONIZZAZIONE PREZZI (WEBSOCKET REAL-TIME) ---
    def _start_price_stream(self):
        def on_message(ws, message):
            msg = json.loads(message)
            # Formato Ticker Kraken WS v1: [channelID, {data}, "ticker", "pair"]
            if isinstance(msg, list) and len(msg) > 3 and msg[-2] == "ticker":
                ws_pair = msg[-1] # es. "BTC/USD"
                # Convertiamo il nome WS nel ticker interno (es. XXBTZUSD)
                kraken_ticker = asset_list.get_ticker(ws_pair)
                # 'c' = last trade closed [price, lot volume]
                price = float(msg[1]['c'][0])
                self.live_prices[kraken_ticker] = price

        def on_open(ws):
            # Iscrizione dinamica basata su asset_list
            pairs_to_subscribe = [asset_list.get_human_name(a) for a in asset_list.ASSET_PRINCIPALI]
            self.logger.info(f"📡 Sottoscrizione WebSocket per: {pairs_to_subscribe}")
            
            subscribe_msg = {
                "event": "subscribe",
                "pair": pairs_to_subscribe,
                "subscription": {"name": "ticker"}
            }
            ws.send(json.dumps(subscribe_msg))

        def on_error(ws, error):
            self.logger.error(f"⚠️ WebSocket Error: {error}")

        def on_close(ws, close_status_code, close_msg):
            self.logger.warning("🔄 WebSocket chiuso. Riconnessione in 5s...")
            # Non chiamare _start_price_stream() ricorsivamente — usa thread separato
            import threading
            def _riconnetti():
                time.sleep(5)
                if self.is_synced:
                    self._start_price_stream()
            threading.Thread(target=_riconnetti, daemon=True).start()

        # Kraken WS v1 URL
        ws = websocket.WebSocketApp(
            "wss://ws.kraken.com",
            on_message=on_message,
            on_open=on_open,
            on_error=on_error,
            on_close=on_close
        )
        ws.run_forever()

    # --- AVVIO SPECCHIO ---
    def start_mirroring(self):
        """Avvia la sincronizzazione totale in background."""
        # Primo sync wallet
        self.sync_wallet()
        # Avvio thread WebSocket
        threading.Thread(target=self._start_price_stream, daemon=True).start()
        self.is_synced = True
        self.logger.info("💎 KRAKEN MIRROR ATTIVO: Wallet e Prezzi in tempo reale.")

    def get_snapshot(self):
        """Ritorna lo stato esatto di Kraken in questo istante."""
        return {
            "wallet": self.wallet,
            "prices": self.live_prices,
            "timestamp": time.time(),
            "is_synced": self.is_synced
        }

    def get_balance(self, asset="ZUSD"):
        """Restituisce il saldo istantaneo per un asset specifico."""
        return float(self.wallet.get(asset, 0.0))

    def get_price(self, asset):
        """Restituisce il prezzo istantaneo (senza chiamate API)."""
        ticker = asset_list.get_ticker(asset)
        return self.live_prices.get(ticker, 0.0)