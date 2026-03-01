# -*- coding: utf-8 -*-
"""
BrainLA - Enterprise AI Trading Bot (ALL components)
Versione: prompt compatto + schema validation + fail-safe + prior RF + bonus/malus pattern
"""
print("🟢🟢 LOADING core/brain_la.py - ARCHITETTURA PULITA 🟢🟢")

import logging
import json
import threading
import time
from datetime import datetime

import google.genai as genai
from pydantic import BaseModel, ValidationError, field_validator

from core.config_la import GEMINI_API_KEY, KRAKEN_KEY, KRAKEN_SECRET
from core.asset_list import get_ticker
from core.feedback_engine import FeedbackEngine

# --- COLLEGAMENTO MODULI ESTERNI ---
from core import engine_la
from core import institutional_filters
from core import macro_sentiment
from core import asset_list
from core import performer_la
from core import trade_manager
from core import config_la

try:
    import streamlit as st
    from flask import Flask, request, jsonify
except ImportError:
    pass

############################################
# --- Risk/Compliance ---
############################################

class RiskManager:
    def check_risk(self, decision, account_limits=None):
        sizing = decision.get("sizing", 0)
        try:
            sizing_val = float(sizing)
            if sizing_val <= 0 or sizing_val > 1:
                return False, f"⚠️ SIZING BLOCCATO: {sizing_val} fuori range."
            max_limit = account_limits.get("max_size", 0.1) if account_limits else 0.1
            if sizing_val > max_limit:
                return False, f"⚠️ RISCHIO ECCESSIVO: Sizing {sizing_val} > {max_limit}"
            sl = decision.get("sl")
            tp = decision.get("tp")
            if sl and tp:
                f_sl = float(sl); f_tp = float(tp)
                if abs(f_sl - f_tp) < 0.0000001:
                    return False, f"⚠️ BLOCCO CRITICO: SL ({f_sl}) e TP ({f_tp}) coincidono."
            return True, ""
        except (TypeError, ValueError):
            return False, "❌ ERRORE CRITICO: Calcolo numerico rischio fallito."

############################################
# --- Strategy Manager (placeholder) ---
############################################

class StrategyManager:
    def select_strategy(self, asset, dati, storico):
        return "ia_institutional"

############################################
# --- Error Handler / Schema ---
############################################

class DecisionSchema(BaseModel):
    direzione: str
    voto: int
    sizing: float
    leverage: float | int
    sl: float | int | None = None
    tp: float | int | None = None
    razionale: str = ""

    @field_validator("direzione")
    def direzione_ok(cls, v):
        v = (v or "FLAT").upper()
        if v not in ("BUY", "SELL", "FLAT"):
            v = "FLAT"
        return v

    @field_validator("voto")
    def voto_ok(cls, v):
        try:
            v = int(v)
        except Exception:
            v = 0
        return max(0, min(10, v))

    @field_validator("sizing")
    def sizing_ok(cls, v):
        try:
            v = float(v)
        except Exception:
            v = 0.0
        return 0.0 if v < 0 else min(1.0, v)

    @field_validator("leverage")
    def lev_ok(cls, v):
        try:
            v = float(v)
        except Exception:
            v = 1.0
        return max(1.0, min(50.0, v))

class ErrorHandler:
    def validate_ia_output(self, raw_json_text):
        try:
            decision_dict = json.loads(raw_json_text)
        except Exception:
            return {"direzione": "FLAT", "voto": 0, "sizing": 0, "leverage": 1, "razionale": "JSON invalid"}
        try:
            model = DecisionSchema(**decision_dict)
            return model.model_dump()
        except ValidationError as e:
            return {"direzione": "FLAT", "voto": 0, "sizing": 0, "leverage": 1, "razionale": f"Schema fail: {e.errors()}"}

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

############################################
# --- BrainLA MAIN CLASS ---
############################################

class BrainLA:
    def __init__(self, gemini_api_key=None, gemini_model_name="gemini-2.0-flash", 
                  api_key=None, api_secret=None, logger=None, account_limits=None, feedback_engine=None):
        self.gemini_api_key = gemini_api_key or GEMINI_API_KEY
        self.client = genai.Client(api_key=self.gemini_api_key)
        self.gemini_model_name = gemini_model_name
        self.logger = logger or logging.getLogger("BrainLA")
        self.api_key = api_key or KRAKEN_KEY
        self.api_secret = api_secret or KRAKEN_SECRET
        self.account_limits = account_limits or {"max_size": 0.1}
        self.risk_manager = RiskManager()
        self.error_handler = ErrorHandler()
        self.strategy_manager = StrategyManager()
        self.dashboard_buffer = []
        self.feedback_engine = feedback_engine or FeedbackEngine()
        self.trade_manager = None  # viene collegato da bot_la

    def chiama_gemini(self, prompt):
        from google.genai import types
        max_retries = 3
        system_instruction = (
            "Sei un trader istituzionale. Rispondi SOLO in JSON con campi: direzione (BUY/SELL/FLAT), voto (0-10), sizing (0-1), leverage, sl, tp, razionale. "
            "Se le condizioni sono deboli metti FLAT e voto 0."
        )
        full_prompt = f"{system_instruction}\n\n{prompt}"

        for i in range(max_retries):
            try:
                time.sleep(1.0) 
                response = self.client.models.generate_content(
                    model=self.gemini_model_name, 
                    contents=full_prompt,
                    config=types.GenerateContentConfig(
                        http_options={'timeout': 30000},
                        temperature=0.2,
                        top_p=0.8,
                        candidate_count=1
                    )
                )
                if response and response.text:
                    testo_pulito = response.text.strip().replace("```json", "").replace("```", "").strip()
                    if "{" in testo_pulito:
                        testo_pulito = "{" + testo_pulito.split("{", 1)[1]
                        if "}" in testo_pulito:
                            testo_pulito = testo_pulito.rsplit("}", 1)[0] + "}"
                    return testo_pulito
                return '{"direzione": "FLAT", "voto": 0, "sizing": 0, "leverage": 1, "razionale": "no response"}'
            except Exception as e:
                if "timeout" in str(e).lower():
                    self.logger.error("⏰ TIMEOUT GEMINI")
                    break
                if "429" in str(e) or "RESOURCE_EXHAUSTED" in str(e).upper():
                    self.logger.warning("⚠️ Rate limit, attendo 10s...")
                    time.sleep(10)
                else:
                    self.logger.error(f"🔴 Errore Gemini: {e}")
                    break
        return '{"direzione": "FLAT", "voto": 0, "sizing": 0, "leverage": 1, "razionale": "errore"}'

    def get_kraken_balance(self):
        temp_engine = engine_la.EngineLA(api_key=self.api_key, api_secret=self.api_secret)
        return temp_engine.get_balance_real()

    def valuta_modifica_posizione(self, dati_engine, posizione):
        p_entrata = posizione.get("p_entrata", 0)
        tp_new, sl_new, ts_new = self.determina_tp_sl_ts(
            asset_name=posizione.get('asset'),
            direzione=posizione.get('direzione'),
            prezzo_ingresso=p_entrata,
            dati_engine=dati_engine
        )
        return (str(sl_new) != str(posizione.get("sl")) or str(tp_new) != str(posizione.get("tp")))

    def determina_tp_sl_ts(self, *args, **kwargs):
        asset_name = kwargs.get('asset_name') or (args[0] if len(args) > 0 else "")
        direzione = kwargs.get('direzione') or (args[1] if len(args) > 1 else 'FLAT')
        prezzo_ingresso = float(kwargs.get('prezzo_ingresso') or (args[2] if len(args) > 2 else 0))
        dati_engine = kwargs.get('dati_engine') or (args[3] if len(args) > 3 else {})
        from core.asset_list import ASSET_CONFIG, get_ticker as get_ticker_local
        if prezzo_ingresso <= 0:
            return 0, 0, 0
        try:
            symbol = get_ticker_local(asset_name)
        except Exception:
            symbol = None
        prec = ASSET_CONFIG.get(asset_name, {}).get('precision', 2) if isinstance(asset_name, str) else 2
        if symbol and isinstance(symbol, str) and "/" in symbol:
            try:
                _, quote = symbol.split("/")
                if quote.upper() not in ("USD", "EUR", "USDT", "ZUSD"):
                    prec = max(prec, 5)
            except Exception:
                pass
        atr_val = dati_engine.get('atr') if isinstance(dati_engine, dict) else None
        min_atr = prezzo_ingresso * 0.005
        try:
            atr = max(float(atr_val or 0), min_atr)
        except Exception:
            atr = min_atr
        d = str(direzione).upper()
        if d == 'BUY':
            sl = prezzo_ingresso - (atr * 1.5)
            tp = prezzo_ingresso + (atr * 3.5)
        elif d == 'SELL':
            sl = prezzo_ingresso + (atr * 1.5)
            tp = prezzo_ingresso - (atr * 3.5)
        else:
            return 0, 0, 0
        sl_final = round(float(sl), prec)
        tp_final = round(float(tp), prec)
        ts_final = round(float(atr * 1.1), prec)
        if abs(sl_final - tp_final) < (1 / (10 ** prec)):
            offset = 20 / (10 ** prec)
            if d == 'BUY':
                sl_final = round(prezzo_ingresso - offset, prec)
                tp_final = round(prezzo_ingresso + offset, prec)
            else:
                sl_final = round(prezzo_ingresso + offset, prec)
                tp_final = round(prezzo_ingresso - offset, prec)
        self.logger.info(f"🎯 TARGET {asset_name}: Entry {prezzo_ingresso} | SL {sl_final} | TP {tp_final} (Prec: {prec})")
        return tp_final, sl_final, ts_final
    
    def _policy_adjust(self, asset_name, decision, dati_engine):
        metrics = self.feedback_engine.get_asset_metrics(asset_name, window=50)
        win_rate = metrics.get("win_rate", 0)
        streak_loss = metrics.get("streak_loss", 0)
        z = dati_engine.get('z_score', 0)
        fz = dati_engine.get('funding_z_score', 0)

        if z > 2 and fz > 2 and decision['direzione'] == "BUY":
            decision['voto'] = max(0, decision['voto'] - 1)
        if z < -2 and fz < -2 and decision['direzione'] == "SELL":
            decision['voto'] = max(0, decision['voto'] - 1)

        if win_rate < 40:
            decision['voto'] = max(0, decision.get('voto', 0) - 1)
        elif win_rate > 60:
            decision['voto'] = min(10, decision.get('voto', 0) + 1)

        if streak_loss >= 3:
            decision['direzione'] = "FLAT"
            decision['razionale'] += " | 🔒 cooldown losing streak"

        try:
            edge = (win_rate/100) - (1 - win_rate/100)
            factor = max(0.2, min(1.5, 1 + edge))
            decision['sizing'] = round(float(decision.get('sizing', 0.15)) * factor, 5)
        except Exception:
            pass

        return decision

    def _apply_prior(self, asset_name, decision):
        prior = self.feedback_engine.get_prior_signal(asset_name)
        if not prior:
            return decision
        diff = abs(decision.get('voto', 0) - prior['prior_voto'])
        if diff > 2:
            decision['sizing'] = round(decision.get('sizing', 0) * 0.5, 5)
            decision['razionale'] += f" | prior RF disagrees ({prior['prior_voto']}) sizing 50%"
        elif diff <= 1 and prior['prior_conf'] > 0.55:
            decision['sizing'] = min(1.0, round(decision.get('sizing', 0) * 1.2, 5))
            decision['voto'] = int(round((decision.get('voto', 0) + prior['prior_voto']) / 2))
            decision['razionale'] += f" | prior RF aligned ({prior['prior_voto']}) sizing +20%"
        return decision

    def full_global_strategy(self, dati_engine, asset_name, macro_sentiment, performance_history=None):
        spread = dati_engine.get('spread_perc', 0)
        if spread and spread > 0.5:
            return {"direzione": "FLAT", "voto": 0, "sizing": 0, "razionale": f"Spread alto {spread:.2f}%"}
        if not dati_engine.get('close'):
            return {"direzione": "FLAT", "voto": 0, "sizing": 0, "razionale": "Dati prezzo mancanti"}

        rvol = dati_engine.get('macro_proxy', {}).get('relative_volume_status', 1.0)
        self.penalty_factor = 1.0  
        if rvol < 0.4:
            return {"direzione": "FLAT", "voto": 0, "sizing": 0, "razionale": f"RVOL {rvol} < 0.4 (Illiquido)"}
        elif 0.4 <= rvol < 0.8:
            self.penalty_factor = 0.5

        fe = self.feedback_engine
        memoria_reale = fe.get_recent_summary(limit=5)
        stats_globali = fe.get_feedback_summary()
        
        entry_price = dati_engine.get('close', 0)
        z_score = dati_engine.get('z_score', 0)
        bp = dati_engine.get('book_pressure', 1.0)
        cvd_div = dati_engine.get('cvd_divergence', 0)
        liq = dati_engine.get('liquidazioni_24h', 0)
        vol_shock = dati_engine.get('vol_shock', 1.0)
        poc = dati_engine.get('poc', 0)
        vah = dati_engine.get('vah', 0)
        val = dati_engine.get('val', 0)
        funding_z = dati_engine.get('funding_z_score', 0)

        prompt = (
            "{\n"
            f' "asset": "{asset_name}",\n'
            f' "price": {entry_price},\n'
            f' "z_score": {z_score}, "funding_z": {funding_z}, "book_pressure": {bp}, "cvd_div": {cvd_div},\n'
            f' "vol_shock": {vol_shock}, "liq24h": {liq}, "poc": {poc}, "vah": {vah}, "val": {val},\n'
            f' "rvol": {rvol}, "macro_sentiment": "{macro_sentiment}",\n'
            f' "mem_win_rate": {stats_globali.get("win_rate",0)},\n'
            f' "recente": "{memoria_reale.get("testo","N/A").replace(chr(10)," ")}"\n'
            "}\n"
            "Regole: se macro_sentiment = BEARISH, voto massimo 6 per BUY. Se dati incoerenti -> FLAT. Restituisci solo JSON."
        )

        response_json = self.chiama_gemini(prompt)
        decision = self.error_handler.validate_ia_output(response_json)

        rvol_val = dati_engine.get('macro_proxy', {}).get('relative_volume_status', 1.0) 
        if hasattr(self, 'penalty_factor') and decision.get('direzione') != "FLAT":
            original_sizing = float(decision.get('sizing', 0.15))
            decision['sizing'] = round(original_sizing * self.penalty_factor, 5)
            if self.penalty_factor < 1.0:
                decision['razionale'] += f" [SIZE ridotta per RVOL {rvol_val}]"

        decision = self._policy_adjust(asset_name, decision, dati_engine)
        decision = self._apply_prior(asset_name, decision)

        eth_btc_ratio = dati_engine.get('eth_btc_ratio', 0)
        if "ETH" in asset_name and "XBT" not in asset_name:
            if eth_btc_ratio < 0:
                decision['voto'] = max(1, decision['voto'] - 2)
                decision['razionale'] += " | ETH debole vs BTC"
        elif "XBT" in asset_name:
            if eth_btc_ratio > 2.0:
                decision['voto'] = max(1, decision['voto'] - 1)
                decision['razionale'] += " | Altseason risk"

        voto_ia = int(decision.get("voto", 0))
        direzione_ia = decision.get("direzione", "FLAT")

        if direzione_ia != "FLAT":
            tp_f, sl_f, _ = self.determina_tp_sl_ts(asset_name, direzione_ia, entry_price, dati_engine)
            decision['sl'] = sl_f
            decision['tp'] = tp_f
            self.logger.info(f"🧠 TRADE DECISO: {direzione_ia} (Voto: {voto_ia})")
            self.logger.info(f"🎯 TARGET TECNICI: SL {decision['sl']} | TP {decision['tp']}")
            self.logger.info(f"📝 RAZIONALE: {decision['razionale']}")

        if direzione_ia != "FLAT" or voto_ia >= 6:
            msg_telegram = (
                f"🤖 *IA ANALYSIS: {asset_name}*\n"
                f"⚖️ *Direzione:* {direzione_ia}\n"
                f"📊 *Voto:* {voto_ia}/10\n"
                f"🧠 *Razionale:* {decision.get('razionale')}\n"
            )
            try:
                self.trade_manager.alerts.invia_alert(msg_telegram)
            except Exception as te:
                print(f"⚠️ Errore invio Telegram: {te}")

        ok_risk, msg_risk = self.risk_manager.check_risk(decision, self.account_limits)
        if not ok_risk:
            decision['direzione'] = "FLAT"
            decision['sizing'] = 0
            decision['razionale'] += f" | {msg_risk}"

        return decision

    # --- SERVIZI ACCESSORI (API & DASHBOARD) ---
    def serve_api(self):
        app = Flask("BrainLA_API")
        @app.route("/strategy", methods=["POST"])
        def get_strategy():
            data = request.json
            res = self.full_global_strategy(**data)
            return jsonify(res)
        threading.Thread(target=app.run, kwargs={"port": 5000, "debug": False}, daemon=True).start()

    def run_dashboard(self):
        st.title("L&A Institutional Dashboard")
        st.write("Segnali IA Recenti:")
        for r in self.dashboard_buffer[-10:]:
            st.json(r)

    # --- TESTING & COMPLIANCE ---
    def unit_test(self):
        dati_test = {
            'close': 50000, 
            'z_score': 2.8,
            'book_pressure': 0.4,
            'cvd_divergence': -0.8,
            'liquidazioni_24h': 15000000, 
            'vol_shock': 1.8,
            'poc': 48500, 
            'vah': 49000,
            'val': 47000,
            'funding_z_score': 2.5,
            'macro_proxy': {'relative_volume_status': 1.0},
            'eth_btc_ratio': 0.07,
            'spread_perc': 0.1
        }
        print("\n--- 🧠 AVVIO TEST GEMINI (ISTITUZIONALE) ---")
        res = self.full_global_strategy(dati_test, asset_name="XXBTZUSD", macro_sentiment="BEARISH")
        print(f"\n✅ RISPOSTA IA:\nDirezione: {res.get('direzione')}\nRazionale: {res.get('razionale')}\nSL: {res.get('sl')} | TP: {res.get('tp')}")
        return res

    def compliance_check(self, user_info):
        if not user_info.get("kyc_valid", False):
            self.logger.warning("🚫 Compliance Fail: KYC non valido.")
            return False
        return True

    def cloud_log(self, msg):
        self.logger.info(f"[CLOUD] {msg}")
    
    def genera_report_mattutino(self, macro_sentiment):
        try:
            prompt = (
                f"Sentiment Macro: {macro_sentiment}. "
                "Briefing rapido (≤30 parole) con mood e consiglio operativo secco."
            )
            report = self.chiama_gemini(prompt)
            from core.telegram_alerts_la import TelegramAlerts
            alerts = TelegramAlerts()
            alerts.invia_alert(f"☕ *BUONGIORNO ANDREA - MORNING BIAS*\n\n{report}")
            print("☀️ Report mattutino inviato con successo.")
        except Exception as e:
            print(f"❌ Errore report mattutino: {e}")

    def genera_report_serale(self, stats_giornaliere):
        try:
            from core.trade_manager import TradeManager
            tm = TradeManager()
            posizioni = tm.posizioni_aperte
            dati_reali = f"Posizioni Attive: {len(posizioni)}. Dettagli: {posizioni}. Stats: {stats_giornaliere}"
            prompt = (
                f"Dati: {dati_reali}. "
                "1) Se posizioni aperte, elenca asset e prezzo ingresso. "
                "2) Se SL aggiornati (fase>0), segnala. "
                "Tono secco e istituzionale."
            )
            report = self.chiama_gemini(prompt)
            from core.telegram_alerts_la import TelegramAlerts
            alerts = TelegramAlerts()
            alerts.invia_alert(f"📊 *REPORT TECNICO SERALE*\n\n{report}")
            print("🌙 Report serale istituzionale inviato.")
        except Exception as e:
            print(f"❌ Errore report serale: {e}")
    
    def calcola_voto(self, dati_engine, asset_name, macro_sentiment):
        return self.full_global_strategy(
            dati_engine=dati_engine,
            asset_name=asset_name, 
            macro_sentiment=macro_sentiment
        )