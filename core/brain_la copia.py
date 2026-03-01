# -*- coding: utf-8 -*-
"""
BrainLA - Enterprise AI Trading Bot (ALL components)
Full real API, OMS, risk manager, ML/ensemble, plugin, dashboard, compliance, error handler, notifications, REST, cloud logging, testing.
Collegato a tutti i moduli: engine_la, feedback_engine, institutional_filters, macro_sentiment, asset_list, performer_la, telegram_alerts_la, trade_manager.
"""
print("🟢🟢 LOADING core/brain_la.py VERSIONE CON chiama_gemini! 🟢🟢")
import logging
import json
import os
import threading
import time
import random
import sqlite3
from typing import Any, Dict, List, Optional

import requests  # Usato quasi sempre, meglio importarlo sempre
import hashlib
import hmac
import base64
import urllib.parse
import google.genai as genai

from core.config_la import GEMINI_API_KEY, KRAKEN_KEY, KRAKEN_SECRET
from core.asset_list import get_ticker

# --- COLLEGAMENTO MODULI ESTERNI ---
from core import engine_la
from core import feedback_engine
from core import institutional_filters
from core import macro_sentiment
from core import asset_list
from core import performer_la
from core import telegram_alerts_la
from core import trade_manager
from core import config_la
# Moduli opzionali per dashboard/API
try:
    import streamlit as st
    import smtplib
    from flask import Flask, request, jsonify
    import plotly.graph_objs as go
except ImportError:
    pass

# --- Funzione HMAC Kraken Private ---
def kraken_private_query(url_path, payload, api_key, api_secret):
    nonce = str(int(time.time() * 1000))
    payload['nonce'] = nonce
    post_data = urllib.parse.urlencode(payload)
    message = (nonce + post_data).encode()
    sha256 = hashlib.sha256(message).digest()
    msg = url_path.encode() + sha256

    sig = hmac.new(base64.b64decode(api_secret), msg, hashlib.sha512)
    signature = base64.b64encode(sig.digest()).decode()

    headers = {'API-Key': api_key, 'API-Sign': signature}
    url = 'https://api.kraken.com' + url_path
    response = requests.post(url, data=payload, headers=headers)
    return response.json()
############################################
# --- Notification Systems ---
############################################

class TelegramAlerts:
    def __init__(self, token, chat_id):
        self.token = token
        self.chat_id = chat_id
    def invia_alert(self, msg):
        try:
            requests.post(f"https://api.telegram.org/bot{self.token}/sendMessage", data={"chat_id": self.chat_id, "text": msg})
        except Exception: pass

class DiscordAlerts:
    def __init__(self, webhook_url):
        self.url = webhook_url
    def invia_alert(self, msg):
        try:
            requests.post(self.url, json={"content": msg})
        except Exception: pass

class EmailAlerts:
    def __init__(self, smtp_server, port, user, pw, to):
        self.smtp_server = smtp_server
        self.port = port
        self.user = user
        self.pw = pw
        self.to = to
    def invia_alert(self, msg):
        try:
            with smtplib.SMTP(self.smtp_server, self.port) as server:
                server.starttls()
                server.login(self.user, self.pw)
                server.sendmail(self.user, self.to, msg)
        except Exception: pass

class WebhookAlerts:
    def __init__(self, webhook_url):
        self.url = webhook_url
    def invia_alert(self, msg):
        try:
            requests.post(self.url, json={"body": msg})
        except Exception: pass

############################################
# --- API/DB/Config ---
############################################

class DBManager:
    def __init__(self, db_path="brainla.db"):
        self.db_path = db_path
        self.conn = sqlite3.connect(self.db_path)
        self.create_table()
    def create_table(self):
        try:
            self.conn.execute("""CREATE TABLE IF NOT EXISTS feedback_log (
                timestamp INTEGER, asset TEXT, strategy TEXT, outcome TEXT, motivation TEXT, warning TEXT)""")
            self.conn.commit()
        except Exception: pass
    def log_feedback(self, asset, strategy, outcome, motivation, warning):
        try:
            ts = int(time.time())
            self.conn.execute("INSERT INTO feedback_log VALUES (?, ?, ?, ?, ?, ?)", (ts, asset, strategy, outcome, motivation, warning))
            self.conn.commit()
        except Exception: pass
    def fetch_all(self):
        try:
            return self.conn.execute("SELECT * FROM feedback_log").fetchall()
        except Exception:
            return []

class ConfigManager:
    def __init__(self, config_path="brainla_config.json"):
        self.config_path = config_path
        self.config = {}
        self.load_config()
    def load_config(self):
        if os.path.exists(self.config_path):
            with open(self.config_path,"r") as f:
                self.config = json.load(f)
    def reload(self):
        self.load_config()
    def get_assets(self):
        return self.config.get("assets", [])
    def get_indicators(self):
        return self.config.get("indicators", [])
    def get_strategies(self):
        return self.config.get("strategies", [])

############################################
# --- OMS (Order Management System) ---
############################################

class OMSDemo:
    def place_order(self, asset, side, qty, api_key, api_secret):
        # Integration stub: add CCXT/binance/IBKR/other real trading API.
        try:
            print(f"[OMS] Placing {side} order on {asset} for qty {qty} (API key, secret hidden)")
            # Real placement logic: integrate with broker API, handle response
            return True
        except Exception: return False

############################################
# --- Risk/Compliance ---
############################################

class RiskManager:
    def check_risk(self, decision, account_limits=None):
        sl = decision.get("sl")
        sizing = decision.get("sizing")
        tp = decision.get("tp")

        # Conversione SICURA sizing (qualsiasi sia la provenienza)
        try:
            sizing_val = float(sizing)
        except (TypeError, ValueError):
            sizing_val = 0.0

        if sl is None or sizing is None or tp is None or sizing_val > 1:
            return False, "Risk check failed"
        # Compliance/KYC demo
        if account_limits and sizing_val > account_limits.get("max_size", 1):
            return False, "Sizing exceeds account limit"
        return True, ""
############################################
# --- ML/Strategy ---
############################################

class StrategyManager:
    def select_strategy(self, asset, dati, storico):
        # ML/ensemble demo: random, best performer, bayes, etc.
        if storico:
            winrates = {k: sum([1 for t in storico if t["asset"]==asset and t["strategy"]==k and t["outcome"]=="WIN"]) for k in ["mean_rev","trend","ia"]}
            best = max(winrates, key=winrates.get)
            return best
        return random.choice(["ia","mean_rev","trend"])

############################################
# --- Gemini Adapter ---
############################################

class GeminiAdapter:
    def __init__(self, gemini_api_key, gemini_model="gemini-2.0-flash"):
        self.gemini_api_key = gemini_api_key
        self.model_name = gemini_model
        
        if genai is not None and gemini_api_key:
            # Nuovo metodo di inizializzazione
            self.client = genai.Client(api_key=self.gemini_api_key)
        else:
            self.client = None

    def call(self, prompt):
        if self.client is None:
            return '{"direzione":"FLAT"}'
        try:
            # Nuova sintassi per generare contenuti
            response = self.client.models.generate_content(
                model=self.model_name,
                contents=prompt
            )
            return response.text.strip()
        except Exception as e:
            print(f"🔴 Errore GeminiAdapter: {e}")
            return '{"direzione":"FLAT","warning":"Gemini error"}'
############################################
# --- Fetch/Data Expansion ---
############################################

class DataFetcher:
    def __init__(self, config=None):
        self.config = config
    def fetch_news(self):
        # Real fetch: config API key, pooling, queue, fallback
        try:
            resp = requests.get("https://cryptopanic.com/api/v1/posts/?auth_token=YOUR_REAL_KEY&public=true")
            news_json = resp.json()
            return [item["title"] for item in news_json.get("results", [])][:3]
        except Exception: return ["No news"]
    def fetch_macro(self):
        # Real macro fetch or provider
        try:
            return {"macro": "Dummy economic indicator"} # Build with QuantLib, FRED, etc.
        except Exception: return {}
    def fetch_asset(self, asset):
        try:
            resp = requests.get(f"https://api.binance.com/api/v3/ticker/price?symbol={asset}")
            return resp.json()["price"]
        except Exception: return None
    def fetch_indicator(self, asset, indicator):
        # Fetch indicator (es: volatility, sentiment, order book, etc.)
        try:
            return {"indicator": "Dummy"}
        except Exception: return {}

############################################
# --- Error Handler ---
############################################

class ErrorHandler:
    def validate_ia_output(self, result):
        required_keys = ["direzione","tp","sl","ts","sizing"]
        for k in required_keys:
            if k not in result or result[k] is None:
                result[k] = "NA"
        # Logic plausibility: TP > Entry > SL, sizing in range, etc.
        try:
            tp = float(result["tp"])
            sl = float(result["sl"])
            if tp <= sl:
                result["warning"] = "TP must be > SL"
        except Exception:
            result["warning"] = "TP/SL not numeric"
        return result

############################################
# --- Testing ---
############################################

class TestRunner:
    def unit_test(self, func, args):
        try:
            res = func(*args)
            return True, res
        except Exception as e:
            return False, str(e)
    def integration_test(self, bot):
        try:
            test_assets = ["BTCUSDT","ETHUSDT"]
            for a in test_assets:
                bot.fetch_asset(a)
            return True, "Integration OK"
        except Exception as e:
            return False, str(e)

############################################
# --- BrainLA MAIN CLASS ---
############################################

class BrainLA:
    def valuta_modifica_posizione(self, dati_engine, posizione):
        """
        FASE 2.1: Gestione Automatica Protezione (Break-Even).
        Aggiorna lo Stop Loss a pareggio se il profitto raggiunge lo 0.6%.
        """
        asset = posizione.get("asset", "Unknown")
        direzione = posizione.get("direzione")
        p_entrata = posizione.get("p_entrata", 0)
        fase_attuale = posizione.get("fase", 0)
        
        # Recuperiamo il P&L attuale calcolato dal bot_la
        pnl_perc = posizione.get("pnl_perc", 0.0)

        # 1. TRIGGER BREAK-EVEN (Sposta SL a prezzo entrata)
        if fase_attuale == 0 and pnl_perc >= 0.6:
            self.logger.info(f"🛡️ {asset}: Target Break-Even raggiunto ({pnl_perc}%). Protezione attiva.")
            # Restituiamo True per dire al bot_la di aggiornare la posizione
            return True 

        # 2. TRIGGER LOCK PROFIT (Sposta SL in attivo se > 1.2%)
        if fase_attuale == 1 and pnl_perc >= 1.2:
            self.logger.info(f"💰 {asset}: Target Lock-Profit raggiunto ({pnl_perc}%).")
            return True

        # 3. FALLBACK: Chiedi all'IA se ci sono altre modifiche suggerite
        tp_new, sl_new, ts_new = self.determina_tp_sl_ts(
            dati_engine, {"voto": posizione.get("voto", 0)}, p_entrata
        )
        
        return (str(sl_new) != str(posizione.get("sl")) or str(tp_new) != str(posizione.get("tp")))

    def get_kraken_balance(self):
        # Esempio: richiesta saldo (balance) Kraken
        return kraken_private_query('/0/private/Balance', {}, KRAKEN_KEY, KRAKEN_SECRET)
    
    def chiama_gemini(self, prompt):
        """Versione Unificata: Invia i dati a Gemini e pulisce l'output JSON."""
        try:
            # Istruzione per forzare il formato ed evitare chiacchiere dall'IA
            system_instruction = "Rispondi esclusivamente in formato JSON puro. Niente blocchi markdown, niente testo extra."
            full_prompt = f"{system_instruction}\n\n{prompt}"

            # Usa il client corretto (google-genai >= 1.0)
            response = self.client.models.generate_content(
                model=self.gemini_model_name, 
                contents=full_prompt
            )
            
            if not response or not response.text:
                return '{"direzione": "FLAT", "warning": "Nessuna risposta da IA"}'
            
            # --- PULIZIA DEL TESTO ---
            # Questo toglie i vari ```json o ``` che Gemini mette spesso e che rompono il codice
            clean_text = response.text.strip()
            if clean_text.startswith("```"):
                clean_text = clean_text.replace("```json", "").replace("```", "").strip()
            
            return clean_text

        except Exception as e:
            self.logger.error(f"🔴 Errore chiamata Gemini: {e}")
            return '{"direzione": "FLAT", "voto": 0, "motivi": ["Errore connessione API"]}'
        
    def __init__(self,
        alerts=None,
        discord_alerts=None,
        email_alerts=None,
        webhook_alerts=None,
        db_path="brainla.db",
        gemini_api_key=None,
        gemini_model_name="gemini-1.5-flash",
        logger=None,
        config_path="brainla_config.json",
        account_limits=None,
        api_key=None,
        api_secret=None
    ):
        # Scegli API Key: da argomento o da config
        self.gemini_api_key = gemini_api_key or GEMINI_API_KEY
        
        # Patch CORRETTA per google-genai (>=1.0)
        self.client = genai.Client(api_key=self.gemini_api_key)
        self.gemini_model_name = gemini_model_name

        self.logger = logger or logging.getLogger("BrainLA")
        self.alerts = alerts
        self.discord_alerts = discord_alerts
        self.email_alerts = email_alerts
        self.webhook_alerts = webhook_alerts
        
        # Inizializzazione componenti
        self.oms = OMSDemo()
        self.risk = RiskManager()
        self.db = DBManager(db_path)
        self.strategy = StrategyManager()
        self.gemini = GeminiAdapter(self.gemini_api_key, gemini_model_name)
        self.fetcher = DataFetcher()
        self.error_handler = ErrorHandler()
        self.config = ConfigManager(config_path)
        self.test_runner = TestRunner()
        
        # Configurazione Account e API Kraken
        self.account_limits = account_limits or {"max_size": 1}
        self.api_key = api_key
        self.api_secret = api_secret
        
        # Buffer e Stati
        self.last_results: List[Dict[str, Any]] = []
        self.expansion_requests: List[Dict[str, Any]] = []
        self.multi_asset_data: Dict[str, Dict[str, Any]] = {}
        self.suggestions_to_apply: List[str] = []
        self.dashboard_buffer: List[str] = []
        self.running = True
        
    def routine_expansion(self, richieste):
        for req in richieste:
            if req.get("request"):
                if "news" in str(req["request"]):
                    news = self.fetcher.fetch_news()
                    self.logger.info(f"[EXPANSION] News aggiunte: {news}")
                if "macro" in str(req["request"]):
                    macro = self.fetcher.fetch_macro()
                    self.logger.info(f"[EXPANSION] Macro aggiunte: {macro}")
                if "asset:" in str(req["request"]):
                    asset_name = str(req["request"]).split("asset:")[1].strip()
                    asset_price = self.fetcher.fetch_asset(asset_name)
                    self.logger.info(f"[EXPANSION] Asset {asset_name} price={asset_price}")
            if req.get("suggestion"):
                self.suggestions_to_apply.append(req["suggestion"])
        self.expansion_requests.clear()
        
    def determina_tp_sl_ts(self, dati_engine, decision, entry_price):
        """
        Recupera i valori di TP, SL e TS dalla decisione IA.
        Se l'IA non li fornisce, usa i livelli istituzionali (POC/VWAP).
        """
        # Se entry_price è 0, proviamo a prenderlo dai dati engine
        if not entry_price or entry_price == 0:
            entry_price = dati_engine.get('close_1h', 0)

        tp = decision.get('tp')
        sl = decision.get('sl')
        ts = decision.get('ts')

        # Fallback istituzionale se l'IA restituisce null o 0
        if not tp or tp == 0:
            # Se siamo in SELL, il TP è il POC (spesso sotto il prezzo)
            tp = dati_engine.get('poc_1h', entry_price * 0.98)
    
        if not sl or sl == 0:
            # Stop loss sopra il VWAP per gli short, sotto per i long
            vwap = dati_engine.get('vwap_1h', entry_price)
            sl = vwap * 1.02 if decision.get('direzione') == 'SELL' else vwap * 0.98

        return float(tp), float(sl), float(ts or 0.0)

    def full_global_strategy(
        self,
        dati_engine: dict,
        macro_sentiment: Optional[str] = None,
        entry_price: Optional[float] = None,
        storico_trade: Optional[List[dict]] = None,
        portafoglio: Optional[dict] = None,
        asset_name: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Analisi Istituzionale Fase 3.0: 
        Integra Volumi (POC/VWAP), Protezione Break-even e Phase 2 Trailing.
        """
        # 1. Preparazione parametri
        ticker_reale = get_ticker(asset_name) if asset_name else None
        prezzo_attuale = dati_engine.get('close_1h', 0)
        
        # 2. PROMPT ISTITUZIONALE (Sincronizzato con Phase 3.0)
        prompt = (
            f"Sei l'AI di un Hedge Fund. Asset: {asset_name} ({ticker_reale}).\n"
            f"DATI TECNICI: {dati_engine}\n"
            f"SENTIMENT: {macro_sentiment}\n"
            f"ENTRY: {entry_price} | LIVE: {prezzo_attuale}\n\n"
            "--- PROTOCOLLO OPERATIVO ---\n"
            "1. BREAK-EVEN: Se profitto >= 0.6%, imposta SL a prezzo di entrata.\n"
            "2. PHASE 2 (TRAILING): Se profitto >= 1.5%, rimuovi TP (null) e imposta SL a 1% di distanza.\n"
            "3. VOLUMI (POC): Se il prezzo rompe il POC con CVD opposto, chiudi posizione.\n"
            "Rispondi in JSON: {'direzione','voto','tp','sl','ts','sizing','motivazione'}"
        )

        # 3. ESECUZIONE IA
        ia_output = self.chiama_gemini(prompt)
        try:
            risultato = json.loads(ia_output)
            # Casting sicuro
            for k in ['tp', 'sl', 'ts', 'sizing']:
                val = risultato.get(k)
                risultato[k] = float(val) if val and val != "NA" else 0.0
        except:
            risultato = {"direzione": "FLAT", "voto": 0, "motivazione": "Errore parsing JSON"}

        # 4. NOTIFICHE E GESTIONE (Sincronizzato con TradeManager)
        pos_attiva = self.trade_manager.posizioni_aperte.get(asset_name)
        if pos_attiva:
            # Calcolo P&L per alert
            pnl_perc = ((prezzo_attuale - entry_price) / entry_price) * 100
            if pos_attiva.get('direzione') == "SELL": pnl_perc *= -1
            
            # Controllo Cambi Fase per Alert Telegram
            if risultato.get('sl') != pos_attiva.get('sl'):
                msg = f"🛡️ *UPDATE PROTEZIONE {asset_name}*\nPNL: {pnl_perc:.2f}%\nNuovo SL: {risultato['sl']}"
                if not risultato.get('tp'): msg = f"🔥 *PHASE 2: {asset_name}*\nTrailing Attivo. TP Rimosso."
                self.alerts.invia_alert(msg)

        return risultato
    def serve_api(self):
        app = Flask("BrainLA_API")
        @app.route("/strategy", methods=["POST"])
        def get_strategy():
            data = request.json
            res = self.full_global_strategy(**data)
            return jsonify(res)
        # Security: add auth, token, rate-limiting!
        threading.Thread(target=app.run, kwargs={"port":5000}).start()

    def run_dashboard(self):
        st.title("BrainLA Dashboard")
        st.write("Ultimi output IA:")
        for r in self.dashboard_buffer[-10:]:
            st.code(r)
        # Plot performance
        try:
            perf = [float(json.loads(r)["tp"]) for r in self.dashboard_buffer if "tp" in r]
            fig = go.Figure(data=[go.Scatter(y=perf)])
            st.plotly_chart(fig)
        except Exception: pass
        st.write("Suggerimenti IA:")
        for s in self.suggestions_to_apply[-5:]:
            st.warning(s)

    def run_backtest(self, storico, asset):
        risultati = []
        for trade in storico:
            decision = self.full_global_strategy(
                dati_engine=trade.get("dati_engine", {}),
                macro_sentiment=trade.get("macro_sentiment"),
                entry_price=trade.get("entry_price"),
                storico_trade=storico,
                portafoglio=trade.get("portafoglio"),
                asset_name=asset
            )
            outcome = trade.get("outcome")
            risultati.append({
                "ia_decision": decision,
                "real_outcome": outcome,
                "win": outcome == "WIN",
                "loss": outcome == "LOSS"
            })
        self.logger.info(f"Backtest results: {risultati}")
        return risultati
    
    def auto_tune(self):
        for sug in self.suggestions_to_apply:
            if "stop-loss" in sug:
                self.logger.info(f"[Auto-Tune] Modifico parametri stop-loss: {sug}")

    def config_asset(self, asset_list):
        for a in asset_list:
            self.multi_asset_data[a] = {}
        self.logger.info(f"Asset configurati: {asset_list}")

    def main_loop(self, asset_list, dati_dict):
        self.config_asset(asset_list)
        while self.running:
            for asset in asset_list:
                dati = dati_dict.get(asset, {})
                res = self.full_global_strategy(
                    dati_engine=dati,
                    macro_sentiment="BULLISH",
                    entry_price=float(self.fetcher.fetch_asset(asset) or 0),
                    storico_trade=[],
                    portafoglio={},
                    asset_name=asset
                )
                self.auto_tune()
            time.sleep(10) # ciclo demo

    def unit_test(self):
        ok, res = self.test_runner.unit_test(self.full_global_strategy, [dict(),None,None,None,None,None])
        self.logger.info(f"Unit test: {ok}, result: {res}")

    def integration_test(self):
        ok, res = self.test_runner.integration_test(self)
        self.logger.info(f"Integration test: {ok}, result: {res}")

    # Compliance/KYC/anti-fraud (demo)
    def compliance_check(self, user_info):
        if not user_info.get("kyc_valid",False):
            self.logger.warning("Non-compliant user (KYC fail)")
            return False
        return True

    # Plugin/Extensibility
    def load_plugin(self, plugin_class):
        self.logger.info(f"Plugin loaded: {plugin_class.__name__}")
        # plugin_class can override methods, add event handler

    # Cloud logging/monitoring
    def cloud_log(self, msg):
        # Add connection to cloud log/monitoring (ex: Datadog, AWS)
        self.logger.info(f"[CLOUD LOG] {msg}")

    def calcola_voto(self, dati_engine, macro_sentiment):
        # Usa la tua logica, ad esempio chiama full_global_strategy oppure genera decisione modello
        return self.full_global_strategy(
            dati_engine=dati_engine,
            macro_sentiment=macro_sentiment,
            entry_price=dati_engine.get("close_1h", 0),
            storico_trade=[],
            portafoglio={},
            asset_name=None
        )