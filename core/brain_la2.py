# -*- coding: utf-8 -*-
"""
BrainLA - Enterprise AI Trading Bot (ALL components)
OTTIMIZZAZIONE: Pulizia doppioni e centralizzazione logica Risk/IA.
"""
print("🟢🟢 LOADING core/brain_la.py - ARCHITETTURA PULITA 🟢🟢")

import logging
import json
import os
import threading
import time
import random
import sqlite3
from typing import Any, Dict, List, Optional
from datetime import datetime

import requests
import google.genai as genai

from core.config_la import GEMINI_API_KEY, KRAKEN_KEY, KRAKEN_SECRET
from core.asset_list import get_ticker
from core.feedback_engine import FeedbackEngine  # <--- import diretto

# --- COLLEGAMENTO MODULI ESTERNI ---
from core import engine_la
from core import institutional_filters
from core import macro_sentiment
from core import asset_list
from core import performer_la
from core import trade_manager
from core import config_la

# Moduli opzionali per dashboard
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
        """
        Versione 2.6.2: VALIDAZIONE SIZING E PROTEZIONE PRECISIONE.
        """
        sizing = decision.get("sizing", 0)
        
        try:
            sizing_val = float(sizing)
            
            # 1. Controllo Sizing
            if sizing_val <= 0 or sizing_val > 1:
                return False, f"⚠️ SIZING BLOCCATO: {sizing_val} fuori range."
            
            # 2. Limite massimo account
            max_limit = account_limits.get("max_size", 0.1) if account_limits else 0.1
            if sizing_val > max_limit:
                 return False, f"⚠️ RISCHIO ECCESSIVO: Sizing {sizing_val} > {max_limit}"
            
            # 3. Controllo coerenza SL/TP
            sl = decision.get("sl")
            tp = decision.get("tp")
            
            if sl and tp:
                f_sl = float(sl)
                f_tp = float(tp)
                
                if abs(f_sl - f_tp) < 0.0000001:
                    return False, f"⚠️ BLOCCO CRITICO: SL ({f_sl}) e TP ({f_tp}) coincidono. Errore decimali asset."
            
            return True, ""
            
        except (TypeError, ValueError):
            return False, "❌ ERRORE CRITICO: Calcolo numerico rischio fallito."

############################################
# --- Strategy Manager (Ensemble) ---
############################################

class StrategyManager:
    def select_strategy(self, asset, dati, storico):
        """ML/Ensemble: decide quale approccio usare (Placeholder per espansione)"""
        if storico:
            return "ia_institutional"
        return "ia_institutional"
        
############################################
# --- Error Handler ---
############################################

class ErrorHandler:
    def validate_ia_output(self, result):
        """Assicura che l'output dell'IA sia integro e numerico."""
        required_keys = ["direzione", "voto", "sizing"]
        for k in required_keys:
            if k not in result or result[k] is None:
                result[k] = 0 if k != "direzione" else "FLAT"
        
        try:
            result["voto"] = int(float(result.get("voto", 0)))
            result["sizing"] = float(result.get("sizing", 0))
        except:
            result["voto"] = 0
            result["sizing"] = 0.0
            
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
        self.dashboard_buffer = []
        self.feedback_engine = feedback_engine or FeedbackEngine()
    
    def chiama_gemini(self, prompt):
        """Gestione resiliente 429 con delay tra asset e output JSON pulito."""
        from google.genai import types  # <--- Necessario per il timeout
        max_retries = 3
        system_instruction = (
            "Sei un High-Frequency Institutional Trader. Analizzi dati di microstruttura Spot in LEVA. "
            "Rispondi SEMPRE spiegando prima il tuo ragionamento e concludi con il JSON puro. "
            "Campi obbligatori: direzione, voto, sizing, leverage, razionale."
        )
        full_prompt = f"{system_instruction}\n\n{prompt}"

        for i in range(max_retries):
            try:
                time.sleep(3) 
                response = self.client.models.generate_content(
                    model=self.gemini_model_name, 
                    contents=full_prompt,
                    config=types.GenerateContentConfig(
                        http_options={'timeout': 30000} # 30 secondi (ms)
                    )
                )
                
                if response and response.text:
                    print(f"\n🧠 [ANALISI GEMINI - {datetime.now().strftime('%H:%M:%S')}]")
                    print("-" * 60)
                    print(response.text)
                    print("-" * 60 + "\n")

                    testo_pulito = response.text.strip().replace("```json", "").replace("```", "").strip()
                    if "{" in testo_pulito:
                        testo_pulito = "{" + testo_pulito.split("{", 1)[1]
                        if "}" in testo_pulito:
                            testo_pulito = testo_pulito.rsplit("}", 1)[0] + "}"

                    return testo_pulito
                
                return '{"direzione": "FLAT", "voto": 0, "sizing": 0, "leverage": 1, "razionale": "Nessuna risposta dal modello"}'

            except Exception as e:
                if "timeout" in str(e).lower():
                    self.logger.error("⏰ TIMEOUT GEMINI: Il server non ha risposto entro 30s. Salto ciclo.")
                    break
                
                if "429" in str(e) or "RESOURCE_EXHAUSTED" in str(e).upper():
                    self.logger.warning("⚠️ Limite API raggiunto. Attesa di sicurezza 20s...")
                    time.sleep(15)
                else:
                    self.logger.error(f"🔴 Errore Gemini: {e}")
                    break
        
        return '{"direzione": "FLAT", "voto": 0, "sizing": 0, "leverage": 1, "razionale": "Errore critico dopo tentativi"}'

    def get_kraken_balance(self):
        from core.engine_la import EngineLA
        temp_engine = EngineLA(api_key=self.api_key, api_secret=self.api_secret)
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
    
    def _policy_adjust(self, asset_name, decision):
        """
        Adatta voto e sizing in base alle metriche rolling dell'asset.
        """
        metrics = self.feedback_engine.get_asset_metrics(asset_name, window=50)
        win_rate = metrics.get("win_rate", 0)
        streak_loss = metrics.get("streak_loss", 0)

        # Regola voto
        if win_rate < 40:
            decision['voto'] = max(0, decision.get('voto', 0) - 1)
        elif win_rate > 60:
            decision['voto'] = decision.get('voto', 0) + 1

        # Blacklist soft su losing streak
        if streak_loss >= 3:
            decision['direzione'] = "FLAT"
            decision['razionale'] = decision.get('razionale', '') + " | 🔒 Asset in cooldown per losing streak"

        # Regola sizing (Kelly ridotto: size *= (win_rate/100 - (1-win_rate/100)))
        try:
            edge = (win_rate/100) - (1 - win_rate/100)
            factor = max(0.2, min(1.5, 1 + edge))
            decision['sizing'] = round(float(decision.get('sizing', 0.15)) * factor, 5)
        except Exception:
            pass

        return decision

    def full_global_strategy(self, dati_engine, asset_name, macro_sentiment, performance_history=None):
        # --- LOGICA RVOL A TRE LIVELLI (Sizing Dinamico) ---
        rvol = dati_engine.get('macro_proxy', {}).get('relative_volume_status', 1.0)
        self.penalty_factor = 1.0  
        
        if rvol < 0.4:
            self.logger.info(f"🚫 [KILL-SWITCH] RVOL {rvol}: Liquidità insufficiente per {asset_name}. Ciclo saltato.")
            return {"direzione": "FLAT", "voto": 0, "sizing": 0, "razionale": f"RVOL {rvol} < 0.4 (Mercato Illiquido)"}
        
        elif 0.4 <= rvol < 0.8:
            self.penalty_factor = 0.5
            self.logger.warning(f"⚠️ [CAUTELA] RVOL {rvol}: Volume basso. Sizing ridotto al 50%.")
        
        else:
            self.penalty_factor = 1.0
            self.logger.info(f"✅ [LIQUIDITÀ OK] RVOL {rvol}: Operatività standard.")

        fe = self.feedback_engine
        memoria_reale = fe.get_recent_summary(limit=5)
        stats_globali = fe.get_feedback_summary()
        
        print(f"\n🧠 [IA PROCESS] Invio dati di {asset_name} a Gemini per analisi...")
        try:
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

            feedback_block = f"""
--- MEMORIA PERFORMANCE (AUTO-LEARNING) ---
Ultimi esiti: {memoria_reale.get('testo', 'Nessun dato')}
Win-Rate Globale: {stats_globali.get('win_rate', 0)}%
"""

            prompt = (
                f"AGISCI COME UN QUANT HEDGE FUND MANAGER SENIOR. Asset: {asset_name} @ {entry_price}\n"
                f"{feedback_block}"
                f"--- CONTESTO MACRO & SENTIMENT ---\n"
                f"Sentiment Globale: {macro_sentiment}\n"
                f"Dati Correlati: RVOL {rvol:.2f} | Ratio ETH/BTC {dati_engine.get('eth_btc_ratio')}\n\n"
                
                f"--- SET DATI TECNICI ---\n"
                f"Statistica: Z-Score {z_score:.2f} | Funding Z {funding_z:.2f}\n"
                f"Order Flow: Book Pressure {bp:.2f} | CVD Divergence {cvd_div:.2f}\n"
                f"Volatilità: Shock {vol_shock:.2f} | Liquidazioni 24h: ${liq:,.0f}\n"
                f"Livelli: POC {poc} | Value Area {val} - {vah}\n"
                f"Muri Liquidità: Supporto {dati_engine.get('muro_supporto')} | Resistenza {dati_engine.get('muro_resistenza')}\n\n"
                
                "GERARCHIA DI ANALISI E FILTRI MACRO:\n"
                "1. FILTRO FEAR & GREED: Se il sentiment è 'EXTREME_GREED', sii spietato sui LONG. Penalizza il voto se il prezzo non è sopra un supporto volumetrico (POC) granitico.\n"
                "2. VOLUME PROFILE: POC/VAH/VAL sono i tuoi binari principali.\n"
                "3. PERSISTENZA MURO: Ignora i muri marcati come 'POSSIBILE_SPOOFING'.\n\n"
                
                "ISTRUZIONI OPERATIVE:\n"
                "1. ASSEGNAZIONE VOTO (1-10): SII SPIETATO. Se il macro è avverso, il voto non può superare 6.\n"
                "2. LOGICA DI USCITA: Calcola SL e TP basandoti sui livelli volumetrici.\n\n"
                "RISPONDI SOLO IN JSON PURO:\n"
                "{\"direzione\": \"BUY/SELL/FLAT\", \"voto\": <int>, \"sizing\": 0.001, \"leverage\": 10, \"sl\": <float>, \"tp\": <float>, \"razionale\": \"...\"}"
            )
            response_json = self.chiama_gemini(prompt)
            
            decision = json.loads(response_json)

            rvol_val = dati_engine.get('macro_proxy', {}).get('relative_volume_status', 1.0) 
            if hasattr(self, 'penalty_factor') and decision.get('direzione') != "FLAT":
                original_sizing = float(decision.get('sizing', 0.15))
                decision['sizing'] = round(original_sizing * self.penalty_factor, 5)
                if self.penalty_factor < 1.0:
                    decision['razionale'] += f" [⚠️ SIZE RIDOTTA DEL 50% PER BASSO RVOL: {rvol_val}]"

            # Adattamento policy locale (win_rate asset, streak, ecc.)
            decision = self._policy_adjust(asset_name, decision)

            eth_btc_ratio = dati_engine.get('eth_btc_ratio', 0)
            if "ETH" in asset_name and "XBT" not in asset_name:
                if eth_btc_ratio < 0:
                    decision['voto'] = max(1, decision['voto'] - 2)
                    decision['razionale'] += " | ⚠️ Alert: ETH debole vs BTC (Voto penalizzato)"
            elif "XBT" in asset_name:
                if eth_btc_ratio > 2.0:
                    decision['voto'] = max(1, decision['voto'] - 1)
                    decision['razionale'] += " | ⚠️ Alert: Altcoins aggressive (BTC in possibile stallo)"

            voto_ia = int(decision.get("voto", 0))
            direzione_ia = decision.get("direzione", "FLAT")

            if direzione_ia != "FLAT" or voto_ia >= 6:
                msg_telegram = (
                    f"🤖 *IA ANALYSIS: {asset_name}*\n"
                    f"⚖️ *Direzione:* {direzione_ia}\n"
                    f"📊 *Voto:* {voto_ia}/10\n\n"
                    f"🔬 *Dati Quant:* \n"
                    f"• Z-Score: {z_score:.2f} | CVD: {cvd_div:.2f}\n"
                    f"• Book: {bp:.2f} | Funding Z: {funding_z:.2f}\n\n"
                    f"🧠 *Razionale:* {decision.get('razionale')}\n"
                    f"---"
                )
                try:
                    self.trade_manager.alerts.invia_alert(msg_telegram)
                except Exception as te:
                    print(f"⚠️ Errore invio Telegram: {te}")

            if direzione_ia != "FLAT":
                tp_f, sl_f, _ = self.determina_tp_sl_ts(asset_name, direzione_ia, entry_price, dati_engine)
                decision['sl'] = sl_f
                decision['tp'] = tp_f
                
                self.logger.info(f"🧠 TRADE DECISO: {direzione_ia} (Voto: {voto_ia})")
                self.logger.info(f"🎯 TARGET TECNICI FORZATI (5 dec): SL {decision['sl']} | TP {decision['tp']}")
                self.logger.info(f"📝 RAZIONALE: {decision['razionale']}")
                    
            return decision

        except Exception as e:
            self.logger.error(f"❌ Errore critico nel Brain Strategy: {e}")
            return {"direzione": "FLAT", "voto": 0, "leverage": 1, "razionale": "Crash logica interna"}

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
            'funding_z_score': 2.5
        }
        print("\n--- 🧠 AVVIO TEST REALE GEMINI (ISTITUZIONALE) ---")
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
                f"Agisci come il consulente senior di Andrea. Sentiment Macro: {macro_sentiment}.\n"
                "Scrivi un briefing rapidissimo (max 30 parole) per Telegram.\n"
                "Indica il 'Mood' del mercato oggi e un consiglio operativo secco."
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
                f"Agisci come un analista quantitativo. Dati: {dati_reali}.\n"
                "1. Se ci sono posizioni aperte, elenca l'asset e il prezzo di ingresso.\n"
                "2. Se sono stati fatti aggiornamenti di Stop Loss (fase > 0), segnalalo.\n"
                "3. Mantieni un tono professionale, secco e istituzionale.\n"
                "NON usare dialetti, NON fare battute. Solo numeri e fatti."
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