# -*- coding: utf-8 -*-
"""
BrainLA - Enterprise AI Trading Bot (ALL components)
Versione 3.0: Intelligence Boost + Fix Report Sincronizzati
"""
print("🟢🟢 LOADING core/brain_la.py - ARCHITETTURA PULITA 🟢🟢")

import logging
import json
import threading
import time
from datetime import datetime
from collections import deque
import random
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

from pydantic import BaseModel, field_validator
from typing import Optional, Union, Dict

class DecisionSchema(BaseModel):
    direzione: str
    voto: int
    sizing: float
    leverage: Union[float, int]
    sl: Optional[Union[float, int]] = None
    tp: Optional[Union[float, int]] = None
    tipo_operazione: str = "N/A"
    timeframe_riferimento: str = "N/A"
    
    # --- NUOVO CAMPO PER TRASPARENZA PROJECT CHIMERA ---
    score_breakdown: Dict[str, int] = {} # Es: {"CVD": 8, "Liquidity": 7, "Velocity": 9}
    
    apprendimento_critico: str = ""
    razionale: str = ""

    @field_validator("direzione")
    def direzione_ok(cls, v):
        if not v: return "FLAT"
        v = str(v).upper().strip().replace("BUY", "LONG").replace("SELL", "SHORT")
        if v not in ("LONG", "SHORT", "FLAT"):
            return "FLAT"
        return v

    @field_validator("voto")
    def voto_ok(cls, v):
        try: 
            return max(0, min(10, int(v)))
        except: 
            return 0

    @field_validator("sizing")
    def sizing_ok(cls, v):
        try: 
            val = float(v)
            return max(0.0, min(1.0, val))
        except: 
            return 0.0

    @field_validator("leverage")
    def lev_ok(cls, v):
        try: 
            return max(1.0, min(5.0, float(v)))
        except: 
            return 1.0

    @field_validator("sl", "tp", mode="before")
    def numeric_or_none(cls, v):
        if v is None or v == 0 or v == "0" or v == "None":
            return None
        try:
            return float(v)
        except:
            return None

    @field_validator("score_breakdown", mode="before")
    def validate_scores(cls, v):
        """ Assicura che i voti singoli siano sempre tra 0 e 10 """
        if not isinstance(v, dict):
            return {}
        return {k: max(0, min(10, int(val))) for k, val in v.items() if isinstance(val, (int, float, str))}
        
class ErrorHandler:
    def validate_ia_output(self, raw_json_text):
        try:
            if "```json" in raw_json_text:
                raw_json_text = raw_json_text.split("```json")[1].split("```")[0].strip()
            elif "```" in raw_json_text:
                raw_json_text = raw_json_text.split("```")[1].split("```")[0].strip()
            
            decision_dict = json.loads(raw_json_text)
            
            # --- FIX ANTI-SCHEMA-FAIL ---
            if 'leverage' not in decision_dict:
                decision_dict['leverage'] = 1.0  # Default se l'IA lo dimentica
            # ----------------------------
                
            decision_dict['direzione'] = decision_dict.get('direzione', 'FLAT')
        except Exception:
            return {"direzione": "FLAT", "voto": 0, "sizing": 0, "leverage": 1, "razionale": "JSON invalid"}
            
        try:
            model = DecisionSchema(**decision_dict)
            return model.model_dump()
        except ValidationError as e:
            # Se fallisce ancora, iniettiamo il leverage mancante nell'errore per non perdere il trade
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
                 api_key=None, api_secret=None, logger=None, account_limits=None, 
                 feedback_engine=None, alerts=None): # <--- Aggiunto alerts
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
        
        # --- COLLEGAMENTO TELEGRAM ---
        self.alerts = alerts # <--- Salvato per l'invio del razionale
        # -----------------------------

        self.trade_manager = None  # viene collegato da bot_la
        self._llm_calls = deque()   # timestamps delle ultime chiamate
        self._llm_rate_limit = 12   # max chiamate al minuto
    
    def _throttle_llm(self):
        now = time.time()
        window = 60
        while self._llm_calls and self._llm_calls[0] < now - window:
            self._llm_calls.popleft()
        if len(self._llm_calls) >= self._llm_rate_limit:
            wait = (self._llm_calls[0] + window) - now + random.uniform(0, 0.5)
            self.logger.warning(f"⏳ Throttle LLM: attendo {wait:.1f}s")
            time.sleep(max(wait, 0))
        time.sleep(random.uniform(0.3, 0.8))  # jitter anti-burst
        self._llm_calls.append(time.time())
    
    def calcola_z_score(self, serie_prezzi, finestra=20):
        """Calcola lo Z-Score statistico per identificare eccessi di mercato."""
        import pandas as pd
        import numpy as np
    
        if len(serie_prezzi) < finestra:
            return 0.0
        
        df = pd.Series(serie_prezzi)
        media = df.rolling(window=finestra).mean()
        std_dev = df.rolling(window=finestra).std()
    
        z_score_series = (df - media) / std_dev
        return float(z_score_series.iloc[-1]) if not np.isnan(z_score_series.iloc[-1]) else 0.0

    def valuta_ingresso(self, asset, dati_mercato):
        """
        Analizza se ci sono le condizioni per un ingresso.
        Integrazione: Visualizzazione Matrice Decisionale Project Chimera.
        """
        # --- NORMALIZZAZIONE IMMEDIATA ---
        from core.asset_list import get_ticker
        ticker_ufficiale = get_ticker(asset)
        
        chiusure = dati_mercato.get('storico_chiusure', [])
        prezzo_attuale = dati_mercato.get('close')
        
        if not chiusure or prezzo_attuale is None:
            return None
            
        # Calcolo dello Z-Score
        z_score = self.calcola_z_score(chiusure)
        
        # Chiediamo a Gemini solo se lo Z-Score indica un'opportunità
        if abs(z_score) > 2.0:
            prompt = (
                f"Asset: {ticker_ufficiale}\n"
                f"Prezzo: {prezzo_attuale}\n"
                f"Z-Score: {z_score:.2f}\n"
                f"Dati: {dati_mercato}\n"
                "Spiega dettagliatamente il tuo razionale operativo."
            )
            
            analisi = self.chiama_gemini(prompt)
            
            # --- RECUPERO DATI E MATRICE VOTI ---
            scores = analisi.get('score_breakdown', {})
            razionale = analisi.get('razionale', 'Nessuna spiegazione fornita.')
            direzione = analisi.get('direzione', 'FLAT')
            voto_globale = analisi.get('voto', 0)

            # --- VISUALIZZAZIONE LOG ANALITICA ---
            self.logger.info(f"🧠 ANALISI PROGETTO CHIMERA per {ticker_ufficiale}:")
            self.logger.info(f"   ➤ Direzione: {direzione} | Voto Globale: {voto_globale}/10")
            
            # Stampa ogni singolo pilastro nei log
            if scores:
                for pilastro, voto in scores.items():
                    self.logger.info(f"      ● {pilastro.replace('_', ' ')}: {voto}/10")
            
            self.logger.info(f"   ➤ RAZIONALE: {razionale}")

            if direzione != "FLAT":
                # --- INVIO ALERT TELEGRAM ---
                if hasattr(self, 'alerts') and self.alerts:
                    # Formattazione lista voti per il messaggio Telegram
                    lista_voti_str = "\n".join([f"• {k.replace('_', ' ')}: {v}/10" for k, v in scores.items()]) if scores else "N/A"
                    
                    msg = (
                        f"🚀 *NUOVO SEGNALE: {direzione}*\n"
                        f"📈 *Asset:* {ticker_ufficiale}\n"
                        f"💰 *Prezzo:* {prezzo_attuale}\n"
                        f"⭐ *Voto Globale:* {voto_globale}/10\n\n"
                        f"📊 *Matrice Decisionale:*\n{lista_voti_str}\n\n"
                        f"🧠 *Razionale:* {razionale}"
                    )
                    
                    self.logger.info(f"📲 Invio alert Telegram con matrice per {ticker_ufficiale}...")
                    self.alerts.invia_alert(msg)

                return {
                    "asset": ticker_ufficiale,
                    "action": direzione,
                    "sl": analisi.get('sl', prezzo_attuale * 0.98),
                    "tp": analisi.get('tp', prezzo_attuale * 1.05),
                    "voto": voto_globale,
                    "leverage": analisi.get('leverage', 1),
                    "score_breakdown": scores,
                    "razionale": razionale
                }

        return None
    
    def chiama_gemini(self, prompt, is_json=True):
        from google.genai import types
        import json
        import time
        
        max_retries = 3
        
        # Allineamento dinamico delle System Instructions (Integrazione Project Chimera)
        if is_json:
            system_instruction = (
                "Sei un trader istituzionale esperto in Order Flow (Project Chimera). Rispondi SOLO in formato JSON. "
                "Campi obbligatori: direzione (BUY/SELL/FLAT), voto (0-10), sizing (0-1), leverage, sl, tp, "
                "tipo_operazione, timeframe_riferimento, score_breakdown, apprendimento_critico, razionale. "
                "Nel campo 'score_breakdown', DEVI fornire voti (0-10) per: Order_Flow, Liquidity, Market_Regime, Velocity, Volatility. "
                "Se le condizioni non sono ottimali, usa direzione FLAT e voto 0."
            )
        else:
            system_instruction = (
                "Sei un analista quantitativo e trader istituzionale. "
                "Fornisci un report discorsivo, professionale, chiaro e dettagliato."
            )
            
        full_prompt = f"{system_instruction}\n\n{prompt}"

        for i in range(max_retries):
            try:
                # Backoff esponenziale per evitare congestione
                wait_time = 2.0 * (i + 1)
                time.sleep(wait_time) 
                
                response = self.client.models.generate_content(
                    model=self.gemini_model_name, 
                    contents=full_prompt,
                    config=types.GenerateContentConfig(
                        temperature=0.2,
                        candidate_count=1
                    )
                )
                
                if response and response.text:
                    testo_output = response.text.strip()
                    
                    # Ritorno immediato per i report discorsivi
                    if not is_json:
                        return testo_output
                        
                    # Pulizia e isolamento del blocco JSON per le decisioni di trading
                    try:
                        if "{" in testo_output:
                            # Isola il contenuto tra le parentesi graffe più esterne
                            json_start = testo_output.find("{")
                            json_end = testo_output.rfind("}") + 1
                            testo_json = testo_output[json_start:json_end]
                            
                            # Validazione tramite DecisionSchema (per garantire la coerenza dei dati)
                            decision_dict = json.loads(testo_json)
                            validated_data = DecisionSchema(**decision_dict)
                            return validated_data.model_dump()
                        else:
                            raise ValueError("Nessun blocco JSON trovato nella risposta")
                            
                    except (json.JSONDecodeError, ValueError, ValidationError) as e:
                        self.logger.error(f"🔴 Errore parsing/validazione JSON (Tentativo {i+1}): {e}")
                        if i == max_retries - 1:
                            break
                        continue 

            except Exception as e:
                # Gestione specifica dei limiti di quota API
                if "429" in str(e) or "RESOURCE_EXHAUSTED" in str(e).upper():
                    self.logger.warning(f"⚠️ Rate limit Gemini (Tentativo {i+1}). Attendo {10 * (i+1)}s...")
                    time.sleep(10 * (i+1))
                else:
                    self.logger.error(f"🔴 Errore critico Gemini: {e}")
                    break
        
        # Fallback sicuro in caso di fallimento totale
        if not is_json:
            return "Errore nella generazione del report. Verificare i log."
            
        return {
            "direzione": "FLAT", 
            "voto": 0, 
            "sizing": 0, 
            "leverage": 1, 
            "sl": 0, 
            "tp": 0, 
            "score_breakdown": {},
            "razionale": "api_failure_fallback"
        }
    
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

    def determina_tp_sl_ts(self, asset_name, direzione, prezzo_ingresso, dati_engine, levels_ia=None):
        """
        VERSIONE CHIMERA UNLEASHED: 
        Elimina i blocchi fissi. Lo SL si adatta ai muri volumetrici reali, non a percentuali arbitrarie.
        """
        try:
            from core.asset_list import get_ticker, ASSET_CONFIG
            ticker_ufficiale = get_ticker(asset_name)
            conf = ASSET_CONFIG.get(ticker_ufficiale, {})
            prec = conf.get("precision", 2) 
            
            if not prezzo_ingresso:
                return 0, 0, 0

            # 1. Recupero Volatilità Reale (Senza forzature dell'1%)
            atr_val = 0
            pools = {}
            if isinstance(dati_engine, dict):
                atr_val = dati_engine.get('atr', 0)
                pools = dati_engine.get('liquidity_pools', {})
                if isinstance(atr_val, dict): atr_val = atr_val.get('value', 0)
            
            # ATR Reale, con un minimo di sicurezza dello 0.4% (non 1%!)
            atr = float(atr_val) if float(atr_val) > (prezzo_ingresso * 0.004) else prezzo_ingresso * 0.005
            velocity = dati_engine.get('price_velocity', 0.0) if isinstance(dati_engine, dict) else 0
            
            d = str(direzione).upper()
            
            # 2. Logica SL Adattiva (Muri > Rumore)
            # Riduciamo l'area di rumore: se c'è velocity, il rumore diminuisce perché il trend è forte
            moltiplicatore_rumore = 0.8 if abs(velocity) > 0.0006 else 1.1
            area_rumore = atr * moltiplicatore_rumore
            
            # Recupero SL suggerito dall'IA (Chimera Brain)
            sl_ia = float(levels_ia.get('sl')) if levels_ia and levels_ia.get('sl') else None

            if d in ['BUY', 'LONG']:
                muri_sup = pools.get('pools_supporto', [])
                # Cerchiamo il muro più vicino ma protetto
                muro_valido = next((m['prezzo'] for m in muri_sup if m['prezzo'] < prezzo_ingresso), None)
                
                # Gerarchia: Muro Volumetrico -> Suggerimento IA -> ATR Dinamico
                sl = muro_valido if muro_valido else (sl_ia if sl_ia else prezzo_ingresso - (atr * 1.5))
                
                # Protezione: Mai SL più vicino dello 0.6% (per commissioni), ma mai forzato all'1.2%
                limit_sicurezza = prezzo_ingresso * 0.994
                sl = min(sl, limit_sicurezza)

                # Take Profit: Primo muro o 3x ATR
                muri_res = pools.get('pools_resistenza', [])
                tp_muro = muri_res[0]['prezzo'] if muri_res else None
                tp = tp_muro if tp_muro and tp_muro > prezzo_ingresso else prezzo_ingresso + (atr * 3.0)

            elif d in ['SELL', 'SHORT']:
                muri_res = pools.get('pools_resistenza', [])
                muro_valido = next((m['prezzo'] for m in muri_res if m['prezzo'] > prezzo_ingresso), None)
                
                sl = muro_valido if muro_valido else (sl_ia if sl_ia else prezzo_ingresso + (atr * 1.5))
                
                limit_sicurezza = prezzo_ingresso * 1.006
                sl = max(sl, limit_sicurezza)

                muri_sup = pools.get('pools_supporto', [])
                tp_muro = muri_sup[0]['prezzo'] if muri_sup else None
                tp = tp_muro if tp_muro and tp_muro < prezzo_ingresso else prezzo_ingresso - (atr * 3.0)

            # --- VALIDAZIONE FINALE ---
            tp_final = round(float(tp), prec)
            sl_final = round(float(sl), prec)

            self.logger.info(f"🛡️ SINERGIA CHIMERA {ticker_ufficiale}: SL {sl_final} | TP {tp_final} (Velocity: {velocity:.6f})")
            return tp_final, sl_final, round(atr, prec)

        except Exception as e:
            self.logger.error(f"❌ Errore sinergia determina_tp_sl_ts: {e}")
            return 0, 0, 0
    def analizza_fase_due_chimera(self, asset, dati_engine, direzione_pos):
        """
        PROJECT CHIMERA - Analisi Velocity per attivazione Phase Two.
        Soglia ricalibrata per catturare trend istituzionali reali.
        """
        try:
            velocity = dati_engine.get('price_velocity', 0.0)
            prezzo_attuale = dati_engine.get('close', 0)
            
            # SOGLIA CHIMERA CALIBRATA: 0.0006 (0.06% al secondo)
            # Sopra questa soglia il movimento è guidato da HFT/Algoritmi
            soglia = 0.0006 
            
            # LONG + Velocity Positiva
            if direzione_pos == "BUY" and velocity > soglia:
                motivo = f"🚀 Momentum HFT Rialzista: {velocity:.5f} %/sec"
                # TP Esteso del 15% per dare spazio al Trailing Stop di lavorare
                tp_esteso = round(prezzo_attuale * 1.15, 2) 
                return True, motivo, tp_esteso
            
            # SHORT + Velocity Negativa
            elif direzione_pos == "SELL" and velocity < -soglia:
                motivo = f"📉 Momentum HFT Ribassista: {velocity:.5f} %/sec"
                # TP Esteso del 15% (al ribasso)
                tp_esteso = round(prezzo_attuale * 0.85, 2) 
                return True, motivo, tp_esteso

            return False, None, None

        except Exception as e:
            self.logger.error(f"❌ Errore analizza_fase_due_chimera: {e}")
            return False, None, None
    def check_chimera_phase_two(self, ticker, dati_engine):
        """
        PROJECT CHIMERA - Step 1: Estrazione e validazione Velocity.
        """
        try:
            velocity = dati_engine.get('price_velocity', 0.0)
            
            # SOGLIA COERENTE: 0.0006
            if abs(velocity) > 0.0006:
                self.logger.info(f"⚡ [CHIMERA TRIGGER] Velocity rilevata su {ticker}: {velocity:.6f}")
                return True, velocity
            
            return False, velocity
        except Exception as e:
            self.logger.error(f"❌ Errore check_chimera_phase_two: {e}")
            return False, 0.0
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

        # Disaccordo forte: FLAT
        if diff >= 4:
            decision['direzione'] = "FLAT"
            decision['sizing'] = 0
            decision['razionale'] += f" | prior RF molto in disaccordo ({prior['prior_voto']}) -> FLAT"
            return decision

        # Disaccordo moderato: taglia la size
        if diff >= 2:
            decision['sizing'] = round(decision.get('sizing', 0) * 0.5, 5)
            decision['razionale'] += f" | prior RF disagrees ({prior['prior_voto']}) sizing 50%"
        elif diff <= 1 and prior['prior_conf'] > 0.55:
            decision['sizing'] = min(1.0, round(decision.get('sizing', 0) * 1.2, 5))
            decision['voto'] = int(round((decision.get('voto', 0) + prior['prior_voto']) / 2))
            decision['razionale'] += f" | prior RF aligned ({prior['prior_voto']}) sizing +20%"

        return decision

    def full_global_strategy(self, dati_engine, asset_name, macro_sentiment, performance_history=None):
        # --- FIX: NORMALIZZAZIONE IMMEDIATA TICKER ---
        from core.asset_list import get_ticker
        import json
        from datetime import datetime
        ticker_ufficiale = get_ticker(asset_name)

        # --- 1. ESTRAZIONE INTEGRALE SENSORI ENGINE ---
        entry_price = dati_engine.get('close', 0)
        atr = dati_engine.get('atr', 0)
        atr_p = (atr / entry_price * 100) if entry_price > 0 else 0
        
        # --- AGGIUNTA CHIMERA: RECUPERO HEALTH INDEX ---
        # Recuperiamo i dati che hai appena aggiunto nell'EngineLA
        market_health = dati_engine.get('market_health_index', 0.5)
        market_regime = dati_engine.get('market_regime', 'UNKNOWN')
        
        # Gestione Spread
        spread = dati_engine.get('spread', 0) 
        if spread == 0:
            spread_p = dati_engine.get('spread_perc', 0)
        else:
            spread_p = (spread / entry_price * 100) if entry_price > 0 else 0

        # --- FIX AGGANCIO PROJECT CHIMERA (Parametri Core) ---
        vpin = dati_engine.get('vpin_toxicity', 0)
        vpin_delta = dati_engine.get('vpin_trend', 0)
        hurst = dati_engine.get('hurst_exponent', 0.5)
        z_score = dati_engine.get('z_score', 0)
        regime_hmm = dati_engine.get('market_regime', 'UNKNOWN')
        market_efficiency = dati_engine.get('kaufman_efficiency', 1.0)
        
        # Estrazione Order Flow (Protetto da sovrascritture errate)
        cvd_chimera = dati_engine.get('cvd_reale', 0)
        cvd_value = cvd_chimera # Alias per coerenza prompt
        velocity_chimera = dati_engine.get('price_velocity', 0)
        spoofing = dati_engine.get('indice_spoofing', 0)
        iceberg = dati_engine.get('iceberg_presenti', False)
        cvd_divergence = dati_engine.get('cvd_divergence', False)
        prev_vah = dati_engine.get('vah_ieri', dati_engine.get('vah', 0))
        prev_val = dati_engine.get('val_ieri', dati_engine.get('val', 0))
        # Parametri Secondari e Microstruttura
        rolling_vol = dati_engine.get('rolling_volatility', 0)
        rvol = dati_engine.get('macro_proxy', {}).get('relative_volume_status', 1.0)
        bp = dati_engine.get('book_pressure', 1.0)
        funding_z = dati_engine.get('funding_z_score', 0)
        rsi = dati_engine.get('rsi', 50)
        mac_d = dati_engine.get('mac_d', 0)
        book_delta = dati_engine.get('book_delta', 0)
        funding_actual = dati_engine.get('actual_funding_rate', 0)
        level_stability = dati_engine.get('level_stability_index', 1.0)
        book_skew = dati_engine.get('book_skewness', 0)
        vol_imbalance = dati_engine.get('volume_imbalance', 1.0)
        corr_index = dati_engine.get('correlation_with_market', 1.0)
        gap_liquidita = dati_engine.get('liquidity_gap', False)       
        aggressivita_flow = dati_engine.get('aggressivita_order_flow', 'NEUTRAL')
        signal_age = dati_engine.get('seconds_since_update', 0)
        vwap = dati_engine.get('vwap', entry_price)
        
        # Area Valore e Muri
        poc = dati_engine.get('poc', 0)
        vah = dati_engine.get('vah', 0)
        val = dati_engine.get('val', 0)
        hvn = dati_engine.get('high_volume_nodes', [])
        lvn = dati_engine.get('low_volume_nodes', [])
        m_s = dati_engine.get('muro_supporto', {})
        m_r = dati_engine.get('muro_resistenza', {})

        # Dati aggiuntivi HFT
        buy_imb = dati_engine.get('buy_imbalance_levels', [])
        sell_imb = dati_engine.get('sell_imbalance_levels', [])
        prob_return = dati_engine.get('stat_return_prob', 0)
        z_dist_vwap = dati_engine.get('distance_zscore_vwap', 0)
        ofi = dati_engine.get('order_flow_imbalance', 0)
        latency = dati_engine.get('kraken_latency', 0)
        # --- 2. GERARCHIA CHIMERA: 4H | 1H | 15M ---
        quadro_4h = {
            "trend_primario": "BULLISH" if hurst > 0.55 and z_score > 0 else "BEARISH" if hurst > 0.55 and z_score < 0 else "CONSOLIDAMENTO",
            "volatilita_macro": round(atr_p, 2),
            "regime_hmm": regime_hmm
        }

        mappa_1h = {
            "muro_supporto_h1": m_s.get("prezzo", 0),
            "muro_resistenza_h1": m_r.get("prezzo", 0),
            "distanza_supporto_h1_perc": round(((entry_price - m_s.get("prezzo", 0)) / entry_price * 100), 2) if m_s.get("prezzo", 0) > 0 else 999.0,
            "distanza_resistenza_h1_perc": round(((m_r.get("prezzo", 0) - entry_price) / entry_price * 100), 2) if m_r.get("prezzo", 0) > 0 else 999.0,
            "vwap_distanza_perc": round((entry_price - vwap) / vwap * 100, 3) if vwap != 0 else 0
        }

        livelli_15m = {
            "area_valore": {"vah": vah, "val": val, "poc": poc},
            "pressione_book": bp,
            "delta_volumi": vol_imbalance,
            "efficienza_kaufman": market_efficiency
        }

        trigger_flusso_istantaneo = {
            "cvd_chimera": cvd_chimera,
            "velocity_esplosiva": velocity_chimera,
            "aggressivita_attuale": aggressivita_flow,
            "vpin_tossicita": vpin
        }

        # --- FILTRO SICUREZZA CHIMERA: ANTI-CROLLO ---
        trend_macro = quadro_4h["trend_primario"]
        
        if trend_macro == "BULLISH" and velocity_chimera < -0.5:
            self.logger.warning(f"⚠️ {asset_name}: Velocity negativa elevata ({velocity_chimera}%). Rischio crollo, scarto.")
            return {"direzione": "FLAT", "voto": 0, "razionale": "ABORT_VELOCITY_CRASH"}
            
        if trend_macro == "BEARISH" and velocity_chimera > 0.5:
            self.logger.warning(f"⚠️ {asset_name}: Velocity positiva elevata ({velocity_chimera}%). Rischio squeeze, scarto.")
            return {"direzione": "FLAT", "voto": 0, "razionale": "ABORT_VELOCITY_SQUEEZE"}

        # Preparazione dati per IA
        muro_s_prezzo = m_s.get("prezzo", 0)
        muro_r_prezzo = m_r.get("prezzo", 0)
        dist_supporto = mappa_1h["distanza_supporto_h1_perc"]
        dist_resistenza = mappa_1h["distanza_resistenza_h1_perc"]
        
        fe = self.feedback_engine
        stats_globali = self.feedback_engine.get_stats_globali() if hasattr(self.feedback_engine, 'get_stats_globali') else {}
        report_lezioni = self.feedback_engine.get_feedback_summary(ticker_ufficiale)
        memoria_reale = report_lezioni
        tech_narrative = self._get_technical_narrative(dati_engine)

        # --- [CHIMERA TECHNICAL FULL REPORT] ---
        self.logger.info(f"📊 === ANALISI TECNICA COMPLETA: {ticker_ufficiale} ===")
        self.logger.info(f"🏥 MARKET HEALTH: {market_health} | REGIME: {market_regime}")
        
        # 1. AREA ORDER FLOW
        cvd_info = "VENDITORI AGGRESSIVI" if cvd_chimera < 0 else "COMPRATORI AGGRESSIVI"
        vpin_info = "⚠️ MERCATO TOSSICO" if vpin > 0.70 else "Scambi Regolari"
        self.logger.info(f"🔹 [ORDER FLOW] CVD: {cvd_chimera:+.2f} | VPIN: {vpin:.4f} [{vpin_info}]")
        self.logger.info(f"   ➤ Delta Divergence: {'⚠️ ATTIVA' if cvd_divergence else '✅ Neutra'}")

        # 2. AREA LIQUIDITY
        bp_info = "Pressione BUY" if bp > 1.2 else "Pressione SELL" if bp < 0.8 else "Equilibrio"
        self.logger.info(f"🔹 [LIQUIDITY] Book Pressure: {bp:.2f} [{bp_info}] | Spoofing: {spoofing:.2f}")
        self.logger.info(f"   ➤ Muri H1: Supporto {muro_s_prezzo} ({dist_supporto}%) | Resistenza {muro_r_prezzo} ({dist_resistenza}%)")

        # 3. AREA MARKET REGIME
        regime_desc = "📈 TREND" if hurst > 0.55 else "↔️ RANGE"
        self.logger.info(f"🔹 [MARKET REGIME] Hurst: {hurst:.2f} [{regime_desc}] | HMM: {regime_hmm}")

        # 4. AREA VELOCITY
        vel_info = "FRENATA/CADUTA" if velocity_chimera < 0 else "ACCELERAZIONE"
        self.logger.info(f"🔹 [VELOCITY] Price Velocity: {velocity_chimera:.4f} [{vel_info}]")
        if gap_liquidita: self.logger.info(f"   ➤ 🚀 RILEVATO LIQUIDITY GAP!")

        # 5. AREA VOLATILITY
        dist_sl_label = (atr * 2 / entry_price) if entry_price > 0 else 0
        self.logger.info(f"🔹 [VOLATILITY] SL Consigliato (2x ATR): {dist_sl_label:.2%}")
        self.logger.info(f"   ➤ Funding Z: {funding_z:.2f}")
        self.logger.info(f"======================================================")
        # --- 3. COSTRUZIONE PROMPT "SIGHT-TOTAL" (Ticker Ufficiale) ---
        
        # --- [CHIMERA FIX: INIZIALIZZAZIONE GLOBALE VARIABILI PROMPT] ---
        # 1. Variabili mancanti segnalate dai log
        slippage_medio = spread_p * 1.2 if spread_p > 0 else 0.02
        depth_liquidity = dati_engine.get('market_depth', 0)
        
        # 2. Variabili di micro-dinamica (Se non le hai calcolate, mettiamo default per non crashare)
        vpin_delta = dati_engine.get('vpin_trend', 0.0)
        book_skew = dati_engine.get('book_skewness', 1.0)
        vol_imbalance = dati_engine.get('volume_imbalance', 0.0)
        gap_liquidita = dati_engine.get('liquidity_gap', False)
        
        # 3. Probabilità e Statistica
        prob_return = dati_engine.get('stat_return_prob', 50)
        z_dist_vwap = dati_engine.get('distance_zscore_vwap', 0.0)
        
        # 4. Variabili Order Flow e HFT
        cvd_value = cvd_chimera # Già presente ma per sicurezza
        velocity_chimera = dati_engine.get('price_velocity', 0.0)
        aggressivita_flow = dati_engine.get('aggressivita_order_flow', 'NEUTRAL')
        
        # 5. Sistema e Latenza
        latency = dati_engine.get('kraken_latency', 50)
        # ----------------------------------------------------------------
        
        prompt = (
            "{\n"
            f' "asset": "{ticker_ufficiale}", "price": {entry_price},\n'
            f' "market_health": {market_health}, "market_regime": "{market_regime}",\n'
            f' "attrito_e_rumore": {{ "atr_perc": {round(atr_p, 2)}, "spread_perc": {round(spread_p, 4)}, "vol_rollante": {rolling_vol} }},\n'
            f' "flusso_hft_primario": {{ "vpin": {vpin}, "hurst": {hurst}, "rvol": {rvol}, "book_pressure": {bp} }},\n'
            f' "forza_momentum": {{ "rsi": {rsi}, "z_score": {z_score}, "funding_z": {funding_z}, "mac_d": {mac_d} }},\n'
            f' "mappa_volumetrica": {{ "poc": {poc}, "vah": {vah}, "val": {val}, "hvn": "{hvn[:3]}", "lvn": "{lvn[:3]}" }},\n'
            f' "muri_liquidi": {{\n'
            f'    "supporto": {{ "prezzo": {m_s.get("prezzo", 0)}, "volume": {m_s.get("volume", 0)}, "forza": "{m_s.get("tipo", "N/A")}" }},\n'
            f'    "resistenza": {{ "prezzo": {m_r.get("prezzo", 0)}, "volume": {m_r.get("volume", 0)}, "forza": "{m_r.get("tipo", "N/A")}" }}\n'
            f' }},\n'
            f' "narrativa": "{tech_narrative}", "macro": {{ "sentiment": "{macro_sentiment}", "regime": "{dati_engine.get("macro_regime", "N/A")}" }},\n'
            f' "performance": {{ "win_rate": {stats_globali.get("win_rate",0) if isinstance(stats_globali, dict) else 0}, "recente": "{memoria_reale[:200].replace(chr(10)," ")}" }},\n'
            f' "validazione_hft": {{ "iceberg_presenti": {iceberg}, "indice_spoofing": {spoofing} }},\n'
            f' "footprint_clusters": {{ "buy_imbalance": {buy_imb[:3]}, "sell_imbalance": {sell_imb[:3]} }},\n'
            f' "probabilita_statistica": {{ "ritorno_vwap_prob": "{prob_return}%", "z_score_distanza": {z_dist_vwap} }},\n'
            f' "microstruttura": {{ "book_delta": {book_delta}, "price_velocity": {dati_engine.get("price_velocity", 0)}, "stability": {level_stability} }},\n'
            f' "costi_reali": {{ "funding_actual": {funding_actual} }},\n'
            f' "micro_dinamica": {{ \n'
            f'    "vpin_acceleration": {vpin_delta}, "book_skew": {book_skew}, \n'
            f'    "vol_imbalance": {vol_imbalance}, "market_corr": {corr_index}, \n'
            f'    "liquidity_gap": {gap_liquidita} \n'
            f' }},\n'
            f' "analisi_profonda": {{ \n'
            f'    "cvd_divergenza": {cvd_divergence}, "delta_cumulativo_storico": {cvd_value}, \n'
            f'    "cvd_istantaneo_chimera": {cvd_chimera}, "velocity_esplosiva": {velocity_chimera}, \n' 
            f'    "aggressivita_attuale": "{aggressivita_flow}", \n' 
            f'    "freschezza_dato_secondi": {signal_age}, "efficiency_prezzo": {market_efficiency} \n'
            f' }}, \n'
            f' "ancore_prezzo": {{ \n'
            f'    "vwap": {vwap}, "distanza_vwap_perc": {round((entry_price-vwap)/vwap*100, 3) if vwap != 0 else 0}, \n'
            f'    "livelli_ieri": {{ "vah": {prev_vah}, "val": {prev_val} }}, \n'
            f'    "slippage_atteso": {slippage_medio} \n'
            f' }},\n'
            f' "stato_sistema": {{ \n'
            f'    "depth_btc": {depth_liquidity}, "regime_hmm": "{regime_hmm}", \n'
            f'    "order_flow_imbalance": {ofi}, "latenza_ms": {latency} \n'
            f' }}\n'
            f"--- DATI DI MERCATO PER {ticker_ufficiale} ---\n"
            f"Prezzo: {entry_price} | ATR%: {atr_p:.2f}% | Spread: {spread_p:.4f}%\n"
            f"VWAP: {vwap} | POC: {poc} | VA: [{val}-{vah}]\n\n"
            f"--- ANALISI NARRATIVA ISTITUZIONALE ---\n{tech_narrative}\n\n"
            f"--- MEMORIA RECENTE E FEEDBACK ---\n{memoria_reale}\n\n"
            "}\n\n"
            f"ANALISI ASSET: {ticker_ufficiale} | PREZZO: {entry_price}\n\n"
            f"--- [STRUTTURA 4H - TREND GLOBALE] ---\n{json.dumps(quadro_4h, indent=2)}\n\n"
            f"--- [MAPPA 1H - BARRIERE ISTITUZIONALI] ---\n{json.dumps(mappa_1h, indent=2)}\n\n"
            f"--- [DETTAGLIO 15M - AREA DI VALORE] ---\n{json.dumps(livelli_15m, indent=2)}\n\n"
            f"--- [TRIGGER FLUSSO - ESECUZIONE] ---\n{json.dumps(trigger_flusso_istantaneo, indent=2)}\n\n"
            "REGOLE MANDATORIE PROJECT CHIMERA (UNLEASHED):\n"
            "1. VELOCITY OVERRIDE: Se 'velocity_esplosiva' > 0.0005, ignora ogni dubbio. Il momentum ha la priorità assoluta su muri e gerarchie.\n"
            "2. STOP LOSS ADATTIVO: La distanza dell'1.2% è solo un riferimento. Se esiste un muro volumetrico o un'Area di Valore a distanza inferiore (es. 0.7%), usa quella protezione. È vietato dare voto 0 solo perché lo SL è vicino.\n"
            "3. VPIN OPPORTUNITY: Un VPIN alto (>0.8) indica che il bot deve stare all'erta, non che deve stare FLAT. Cavalca l'instabilità se la direzione è chiara.\n"
            "4. NO GHOSTING: Se i log indicano 'OCCASIONE PERSA', significa che hai fallito. Devi assegnare un voto > 6 ogni volta che rilevi un'accelerazione di prezzo (Velocity) concorde con il CVD.\n"
            "5. GERARCHIA FLESSIBILE: Se il 15M esplode, non aspettare il 4H. Entra e proteggi.\n"
            "6. RISPONDI SEMPRE: È tassativamente vietato restituire voto 0 se il mercato si sta muovendo.\n\n"
            "RISPONDI ESCLUSIVAMENTE IN JSON: {\n"
            "  \"direzione\": \"BUY/SELL/FLAT\", \"voto\": int, \"sizing\": float, \"leverage\": float,\n"
            "  \"sl\": float, \"tp\": float, \"tipo_operazione\": \"Scalp/Swing\", \"timeframe_riferimento\": \"15m/1h/4h\",\n"
            "  \"score_breakdown\": { \"Order_Flow\": int, \"Liquidity\": int, \"Market_Regime\": int, \"Velocity\": int, \"Volatility\": int },\n"
            "  \"apprendimento_critico\": \"string\", \"razionale\": \"string\"\n"
            "}\n\n"
            "REGOLE ISTITUZIONALI CHIMERA:\n"
            "1. Se 'indice_spoofing' > 0.8: Riduci la fiducia nel segnale del 50%.\n"
            "2. Se 'iceberg_presenti' == 1: Cerca un'entrata a favore del trend.\n"
            "3. Usa le 'liquidity_pool' come calamita per il TP o scudo per lo SL."
        )

        # --- 4. CHIAMATA IA E VALIDAZIONE CHIMERA ---
        response_json = self.chiama_gemini(prompt)
        
        # Estrazione immediata della matrice per il logging
        voti_chimera = response_json.get('score_breakdown', {}) if isinstance(response_json, dict) else {}
        
        # --- LOG MATRICE DECISIONALE ISTANTANEO ---
        self.logger.info(f"📊 MATRICE DECISIONALE CHIMERA [{ticker_ufficiale}]:")
        if voti_chimera:
            for pilastro, v in voti_chimera.items():
                self.logger.info(f"   ● {pilastro.replace('_', ' ')}: {v}/10")
        else:
            self.logger.warning("   ⚠️ Attenzione: score_breakdown non presente o incompleto.")

        # Validazione formale tramite error_handler
        raw_to_validate = json.dumps(response_json) if isinstance(response_json, dict) else response_json
        decision = self.error_handler.validate_ia_output(raw_to_validate)

        # Re-iniezione forzata dei voti se il validatore li ha rimossi
        if voti_chimera and 'score_breakdown' not in decision:
            decision['score_breakdown'] = voti_chimera

        # Logging narrativo avanzato
        razionale_ia = decision.get('razionale', 'Nessuna spiegazione fornita.')
        lezione_ia = decision.get('apprendimento_critico', 'Nessuna lezione identificata.')
        
        self.logger.info(f"🧠 [PENSIERO IA {ticker_ufficiale}]: {razionale_ia}")
        self.logger.info(f"🎓 [COSA HO IMPARATO]: {lezione_ia}")
        
        # --- 5. LOGICA DI BUSINESS E FILTRI (CHIMERA UNLEASHED) ---
        if hasattr(self, '_policy_adjust'):
            decision = self._policy_adjust(ticker_ufficiale, decision, dati_engine)
            
        macro_upper = str(macro_sentiment).upper()
        
        # FIX: Il sentiment NON abbassa più il voto. Riduce solo la size.
        if (macro_upper == "BEARISH" and decision.get('direzione') == "BUY") or \
           (macro_upper == "BULLISH" and decision.get('direzione') == "SELL"):
            decision['sizing'] = round(decision.get('sizing', 0) * 0.7, 5)
            decision['razionale'] += " | Counter-trend: Sizing ridotto."

        # Boost Edge Quantitativo
        if ( (z_score > 1.5 and funding_z > 1.5 and bp > 1.2) or \
             (z_score < -1.5 and funding_z < -1.5 and bp < 0.8) ) and decision.get('direzione') != "FLAT":
            decision['voto'] = min(10, decision.get('voto', 0) + 1)
            decision['sizing'] = min(1.0, round(decision.get('sizing', 0) * 1.2, 5))
            decision['razionale'] += " | +boost edge allineato"

        # --- SBLOCCO FORZATO VELOCITY ---
        # Se il prezzo corre, il bot DEVE avere un voto alto.
        if abs(velocity_chimera) > 0.0006 and decision.get('direzione') != "FLAT":
            if decision.get('voto', 0) < 6:
                decision['voto'] = 7
                decision['razionale'] += f" | ⚡ FORZA CHIMERA: Velocity ({velocity_chimera:.6f}) domina, entro a mercato."

        # --- 6. LIVELLI FINALI E TELEGRAM ---
        direzione_ia = decision.get("direzione", "FLAT")
        voto_ia = int(decision.get("voto", 0))

        if direzione_ia != "FLAT":
            tp_f, sl_f, _ = self.determina_tp_sl_ts(ticker_ufficiale, direzione_ia, entry_price, dati_engine, levels_ia=decision)
            decision['sl'], decision['tp'] = sl_f, tp_f
            
            is_explosive, reason, tp_chimera = self.analizza_fase_due_chimera(ticker_ufficiale, dati_engine, direzione_ia)
            
            if is_explosive:
                decision['tp'] = tp_chimera
                decision['trailing_stop'] = True
                decision['razionale'] += f" | ⚡ CHIMERA RUN: {reason}"
                self.logger.info(f"🚀 CHIMERA ATTIVA: TP esteso a {tp_chimera} su {ticker_ufficiale}")

            # Invio Telegram se il voto è sufficiente
            if voto_ia >= 6:
                str_voti_tg = "\n".join([f"• {k.replace('_', ' ')}: {v}/10" for k, v in voti_chimera.items()])
                razionale_safe = razionale_ia.replace('_', ' ').replace('*', '').replace('`', '')
                
                msg_telegram = (
                    f"📝 *ANALISI TECNICA {ticker_ufficiale}*\n━━━━━━━━━━━━━━━\n"
                    f"📊 *Matrice Chimera:*\n{str_voti_tg if voti_chimera else 'N/A'}\n━━━━━━━━━━━━━━━\n"
                    f"🧠 *Pensiero:* {razionale_safe}\n"
                    f"⚖️ *Azione:* {direzione_ia} | ⭐ *Voto:* {voto_ia}/10\n"
                    f"🎯 *SL:* {decision['sl']} | *TP:* {decision['tp']}"
                )
                try:
                    target_alerts = getattr(self, 'alerts', None) or (getattr(self, 'trade_manager', None).alerts if hasattr(self, 'trade_manager') else None)
                    if target_alerts: target_alerts.invia_alert(msg_telegram)
                except Exception as te: self.logger.error(f"⚠️ Errore Telegram: {te}")

        # Risk Management finale: Se il voto è altissimo, forziamo l'ok del rischio
        ok_risk, msg_risk = self.risk_manager.check_risk(decision, self.account_limits)
        if not ok_risk and voto_ia < 8:
            decision['direzione'] = "FLAT"
            decision['razionale'] += f" | BLOCCATO RISK: {msg_risk}"
        elif not ok_risk and voto_ia >= 8:
            self.logger.warning(f"⚠️ Risk Manager segnala {msg_risk}, ma il voto {voto_ia} forza l'esecuzione.")
        
        # Salvataggio Apprendimento
        try:
            with open("learning_log.txt", "a", encoding="utf-8") as f:
                f.write(f"[{datetime.now()}] Asset: {ticker_ufficiale} | Lezione: {lezione_ia}\n")
        except Exception as fe:
            self.logger.error(f"⚠️ Errore salvataggio log: {fe}")
            
        # FIX: Salute bassa riduce la size ma NON azzera il voto
        if market_health < 0.25: # Soglia abbassata
            decision['sizing'] = round(decision.get('sizing', 0) * 0.4, 5)
            decision['razionale'] += f" | ⚠️ Salute critica ({market_health}): Size ridotta."

        # STORAGE METADATI
        decision['metadata'] = {
            "market_health": market_health,
            "market_regime": market_regime,
            "vpin_toxicity": vpin,
            "hurst_exponent": hurst,
            "timestamp_analisi": datetime.now().isoformat()
        }
        
        return decision

    def _get_technical_narrative(self, dati_engine):
        """
        Mantiene la logica originale ma garantisce log puliti.
        """
        # (Nessun cambio necessario alla logica interna qui, basta che riceva i dati corretti dall'engine)
        n = []
        h = dati_engine.get('hurst_exponent', 0.5)
        z = dati_engine.get('z_score', 0)
        sq = dati_engine.get('squeeze', 'OFF')
        atr = dati_engine.get('atr', 0)
        shock = dati_engine.get('vol_shock', 1.0)
        n.append(f"REGIME: Hurst {h} ({dati_engine.get('market_regime', 'RANDOM')}). Z-Score: {z:.2f}. Squeeze: {sq}. ATR: {atr}. Vol Shock: {shock:.2f}.")

        vpin = dati_engine.get('vpin_toxicity', 0)
        cvd_c = dati_engine.get('cvd_divergence', 1.0)
        dfp = dati_engine.get('delta_footprint', 0)
        vel = dati_engine.get('trade_velocity', 0)
        
        if cvd_c < 0.3:
            stato_div = "FORTE DIVERGENZA" if cvd_c < 0 else "DEBOLEZZA"
            n.append(f"SALUTE_TREND: {stato_div} - Correlazione: {cvd_c:.2f}.")
        else:
            n.append(f"SALUTE_TREND: Sano ({cvd_c:.2f}).")
            
        n.append(f"FLUSSI: VPIN {vpin:.4f}. Delta Footprint: {dfp}. Velocity: {vel:.2f} t/s.")

        # 3. ANALISI ISTITUZIONALE (Whales, Driver, Absorption)
        whale = dati_engine.get('whale_delta', 0)
        driver = dati_engine.get('market_driver', 'N/A')
        abs_t = dati_engine.get('absorption', 'NORMAL')
        n.append(f"ISTITUZIONALE: Whale Delta: {whale}. Driver: {driver}. Absorption: {abs_t}.")

        # 4. ORDERBOOK E MURI (Persistenza & Anti-Spoofing)
        m_s = dati_engine.get('muro_supporto', {})
        m_r = dati_engine.get('muro_resistenza', {})
        l_walls = dati_engine.get('liquidity_walls', {})
        
        # Integrazione dati orderbook profondo e persistenza
        n.append(f"MURI_BUY: {m_s.get('prezzo', l_walls.get('muro_supporto'))} (Stato: {m_s.get('stato', 'N/A')}, Affidabilità: {m_s.get('affidabilita', '0%')}).")
        n.append(f"MURI_SELL: {m_r.get('prezzo', l_walls.get('muro_resistenza'))} (Stato: {m_r.get('stato', 'N/A')}, Affidabilità: {m_r.get('affidabilita', '0%')}).")

        # 5. VOLUMETRIA E LIVELLI (POC, FVG, Value Area)
        poc = dati_engine.get('poc', 0)
        vah = dati_engine.get('vah', 0)
        val = dati_engine.get('val', 0)
        fvg = dati_engine.get('fvg', 'NONE')
        d_poc = dati_engine.get('delta_poc', 0)
        n.append(f"STRUTTURA: POC {poc} (Delta POC: {d_poc}). VA: [{val} - {vah}]. FVG: {fvg}.")

        # 6. DERIVATI E SENTIMENT (Funding, OI, Liquidazioni)
        f_z = dati_engine.get('funding_z_score', 0)
        oi = dati_engine.get('open_interest', 0)
        liq = dati_engine.get('liquidazioni_24h', 0)
        pc_ratio = dati_engine.get('put_call_ratio', 1.0)
        n.append(f"DERIVATI: Funding Z-Score: {f_z:.2f}. OI: {oi}. Liquidazioni 24h: {liq}$. Put/Call Ratio: {pc_ratio}.")

        # 7. MACRO E CORRELAZIONE (ETH/BTC, Risk, Liquidity Warning)
        m_p = dati_engine.get('macro_proxy', {})
        regime = dati_engine.get('macro_regime', 'NEUTRAL')
        l_warn = "ATTENZIONE: Bassa Liquidità/Slippage" if m_p.get('market_liquidity_warning') else "Liquidità OK"
        n.append(f"MACRO: ETH/BTC Ratio: {m_p.get('eth_btc_ratio', 0.07)}. Status: {regime}. {l_warn}. RelVol: {m_p.get('relative_volume_status', 1.0)}.")
        
        # 8. GESTIONE PORTAFOGLIO (Beta-Neutrality)
        p_corr = dati_engine.get('portfolio_corr_risk', 0.0)
        if p_corr > 0.8: 
            n.append(f"RISCHIO: Alta correlazione con posizioni esistenti ({p_corr}).")

        return " ".join(n)
        
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
            report = self.chiama_gemini(prompt, is_json=False)
            from core.telegram_alerts_la import TelegramAlerts
            alerts = TelegramAlerts()
            alerts.invia_alert(f"☕ *BUONGIORNO ANDREA - MORNING BIAS*\n\n{report}")
            print("☀️ Report mattutino inviato con successo.")
        except Exception as e:
            print(f"❌ Errore report mattutino: {e}")
    
    def analizza_performance_chimera(self, json_file_path="trade_history.json"):
        """Analizza la correlazione tra Market Health e performance nel Project Chimera."""
        try:
            with open(json_file_path, 'r') as f:
                trades = json.load(f)
            
            stats = {
                "HIGH_HEALTH": {"wins": 0, "losses": 0, "profit": 0},
                "LOW_HEALTH": {"wins": 0, "losses": 0, "profit": 0}
            }

            for t in trades:
                meta = t.get('metadata', {})
                health = meta.get('market_health', 0.5)
                # Supponiamo che il risultato sia in 'result_perc' o calcolato
                result = t.get('result_perc', 0)

                label = "HIGH_HEALTH" if health >= 0.5 else "LOW_HEALTH"
                if result > 0: stats[label]["wins"] += 1
                else: stats[label]["losses"] += 1
                stats[label]["profit"] += result

            return stats
        except Exception as e:
            self.logger.error(f"❌ Errore analisi Chimera: {e}")
            return None
    
    def genera_report_serale(self, stats_giornaliere):
        try:
            if not self.trade_manager:
                from core.trade_manager import TradeManager
                self.trade_manager = TradeManager()
                
            posizioni = self.trade_manager.posizioni_aperte
            
            # --- INTEGRAZIONE STATISTICHE CHIMERA ---
            chimera_data = self.analizza_performance_chimera()
            chimera_summary = ""
            if chimera_data:
                h_win = chimera_data["HIGH_HEALTH"]["wins"]
                h_total = h_win + chimera_data["HIGH_HEALTH"]["losses"]
                wr_h = (h_win / h_total * 100) if h_total > 0 else 0
                chimera_summary = f"\n\n📊 *CHIMERA INSIGHTS:*\n- WR con Health Alta: {wr_h:.1f}% ({h_total} trade)"

            dati_reali = f"Asset attivi: {list(posizioni.keys())}. Stats: {stats_giornaliere}. {chimera_summary}"
            
            prompt = (
                f"Dati: {dati_reali}. "
                "Crea un report sintetico e istituzionale per Andrea. "
                "Includi una nota sulla precisione basata sulla salute del mercato (Chimera) se presente."
            )
            
            report = self.chiama_gemini(prompt, is_json=False)
            from core.telegram_alerts_la import TelegramAlerts
            alerts = TelegramAlerts()
            alerts.invia_alert(f"📊 *REPORT TECNICO SERALE*\n\n{report}")
        except Exception as e:
            logging.error(f"❌ Errore report serale: {e}")
    
    def calcola_voto(self, dati_engine, asset_name, macro_sentiment):
        return self.full_global_strategy(
            dati_engine=dati_engine,
            asset_name=asset_name, 
            macro_sentiment=macro_sentiment
        )