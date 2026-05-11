# -*- coding: utf-8 -*-
import logging
import json
import threading
import time
from datetime import datetime
from collections import deque
import random
import sys
import os
from core.chimera_errors import ErrorTracker
from google import genai
from google.genai import types
from google.genai.types import ThinkingLevel
from pydantic import BaseModel, ValidationError, field_validator

from core.config_la import GEMINI_API_KEY, KRAKEN_KEY, KRAKEN_SECRET

_err = ErrorTracker("BrainLA")
try:
    from core.config_la import GEMINI_API_KEY_2
except ImportError:
    GEMINI_API_KEY_2 = None
from core.asset_list import get_ticker
from core.feedback_engine import FeedbackEngine

# Line 17 is empty to avoid circular imports
try:
    import streamlit as st
    from flask import Flask, request, jsonify
except ImportError:
    pass

class RiskManager:
    def __init__(self):
        self.logger = logging.getLogger("RiskManager")

    def check_risk(self, decision, posizioni_aperte=None, account_limits=None):
        """
        Analisi del rischio a livello di portafoglio (Hedge Fund style).
        Controlla correlazione, esposizione netta e limiti per asset.
        """
        sizing = decision.get("sizing", 0)
        asset = decision.get("asset", "UNKNOWN")
        direzione = decision.get("direzione", "FLAT")
        
        try:
            sizing_val = float(sizing)
            if sizing_val < 0 or sizing_val > 1:
                return False, f"⚠️ SIZING BLOCCATO: {sizing_val} fuori range."
            
            # 1. Controllo Esposizione Totale
            posizioni_aperte = posizioni_aperte or {}
            n_pos = len(posizioni_aperte)
            if n_pos >= 5: # Limite standard di 5 posizioni simultanee
                return False, f"⚠️ RISCHIO PORTAFOGLIO: Già {n_pos} posizioni aperte. Limite raggiunto."

            # 2. Controllo Correlazione / Esposizione Direzionale
            # Se abbiamo già 3 posizioni nella stessa direzione, blocchiamo la quarta
            direzione_norm = "LONG" if direzione in ["BUY", "LONG"] else ("SHORT" if direzione in ["SELL", "SHORT"] else "FLAT")
            stessa_direzione = [p for p in posizioni_aperte.values() if p.get("direzione") == direzione_norm]
            
            if len(stessa_direzione) >= 3 and direzione_norm != "FLAT": # Limite standard di 3 posizioni per direzione
                return False, f"⚠️ RISCHIO CORRELAZIONE: Già {len(stessa_direzione)} posizioni {direzione_norm}. Diversificazione richiesta."

            # 3. Limiti Account
            max_limit = account_limits.get("max_size", 1.0) if account_limits else 1.0
            if sizing_val > max_limit:
                self.logger.warning(f"⚠️ Sizing IA ({sizing_val}) superiore al limite ({max_limit}). Procedo con cap istituzionale.")
            
            sl = decision.get("sl")
            tp = decision.get("tp")
            if sl and tp:
                f_sl = float(sl); f_tp = float(tp)
                if abs(f_sl - f_tp) < 0.0000001:
                    return False, f"⚠️ BLOCCO CRITICO: SL ({f_sl}) e TP ({f_tp}) coincidono."
            return True, ""
        except (TypeError, ValueError):
            return False, "❌ ERRORE CRITICO: Calcolo numerico rischio fallito."

class StrategyManager:
    def select_strategy(self, asset, dati, storico):
        return "ia_institutional"

from typing import Optional, Union, Dict, Any

class DecisionSchema(BaseModel):
    catena_di_pensiero: str = ""
    direzione: str
    voto: int
    stile_operativo: str
    timeframe_riferimento: str = "15m"
    score_breakdown: Dict[str, int] = {}
    apprendimento_critico: str = ""
    razionale: str = ""
    # ── Nuovi campi v9 (gate rumore + ragionamento narrativo) ───────────
    # Sono opzionali: se Gemini risponde col vecchio schema, default vuoti.
    gate_rumore: Dict[str, Union[str, int, bool]] = {}
    narrativa_mercato: str = ""
    ragionamento_decisione: str = ""
    confronto_decisioni_passate: str = ""
    stop_logico: float = 0.0
    target_logico: float = 0.0
    # ── Nuovi campi v10 (risk-first + calibration + autodiag) ───────────
    # Anch'essi opzionali. Permettono il debug ex-post e l'enforcement
    # deterministico se Gemini ignora le regole hard.
    risk_assessment: Dict[str, Union[float, int, str]] = {}
    calibration_check: Dict[str, Union[str, list]] = {}
    fase_autodiagnosi_rispettata: str = ""
    # ── Nuovi campi v10 — scenari condizionali (Modifica B soft) ────────
    # Gemini formula scenari che il sistema salva e mostrerà al ciclo
    # successivo. Il bot agisce su 'direzione' adesso, ma gli scenari
    # restano pendenti per essere verificati nei cicli seguenti.
    scenario_principale: Dict[str, Union[str, float]] = {}
    scenario_alternativo: Dict[str, Union[str, float]] = {}
    scenario_no_trade: Dict[str, str] = {}
    asset_situation_now: str = ""
    # ── Nuovi campi v11 — flusso/prezzo/terreno + 3 orizzonti + pattern ─
    # F: la causa è il flusso, l'effetto è il prezzo (no nuovi campi schema)
    # G: pattern microstrutturali (absorbing / exhaustion)
    # H: narrativa spezzata in 3 orizzonti temporali
    pattern_microstrutturali: Dict[str, str] = {}
    narrativa_intraday: str = ""
    narrativa_daily: str = ""
    narrativa_strutturale: str = ""
    coerenza_orizzonti: str = ""
    # ── Nuovi campi v12 — Watchdog + scansione (alignment 2026-05-08) ───
    # Aggiunti per coerenza prompt↔schema↔post-processing.
    # Prima venivano richiesti nel prompt e usati nel post-processing
    # (riga 3406 rescue loop, Watchdog v12) ma NON erano nello schema:
    # `model.model_dump()` li scartava silenziosamente.
    # FIX 2026-05-09 (Schema-Strict-Block): cambiato Dict[str, str]/Dict[str, int]
    # → Dict[str, Any]. Causa: Gemini in modalità legacy (post-fallimento
    # response_schema) restituiva spesso strutture annidate o tipi misti
    # (es. valore dict invece di str), che facevano fallire la validazione
    # Pydantic e producevano un FALSO 'direzione=FLAT, voto=0' che bloccava
    # anche segnali Strategy fortissimi (vedi log 2026-05-09 20:28+).
    # Vedi anche prompt aggiornato a riga ~3370 con esempio esplicito.
    condizioni_tesi: Dict[str, Any] = {}
    scansione_dimensioni: Dict[str, Any] = {}
    conteggio_confluenza: Dict[str, Any] = {}

    @field_validator("direzione")
    def direzione_ok(cls, v):
        if not v: return "FLAT"
        v = str(v).upper().strip().replace("BUY", "LONG").replace("SELL", "SHORT")
        if v not in ("LONG", "SHORT", "FLAT"):
            return "FLAT"
        return v

    @field_validator("voto")
    def voto_ok(cls, v):
        try: return max(0, min(10, int(v)))
        except: return 0

    @field_validator("score_breakdown", mode="before")
    def validate_scores(cls, v):
        if not isinstance(v, dict): return {}
        return {k: max(0, min(10, int(val))) for k, val in v.items() if isinstance(val, (int, float, str))}
        
class ThesisSchema(BaseModel):
    valida: bool
    motivo: str
    azione: str

    @field_validator("azione")
    def azione_ok(cls, v):
        v = str(v).upper().strip()
        if v not in ("HOLD", "CLOSE", "REVERSE"):
            return "HOLD"
        return v

class NightReviewSchema(BaseModel):
    sintesi: str
    pattern_errore_principale: str
    condizioni_sfavorevoli: str
    uso_time_stop: str
    consiglio_sizing: str
    regola_domani: str
    voto_performance: Union[int, float]

class AuditorSchema(BaseModel):
    anomalia_rilevata: bool
    gravita: str
    descrizione_problema: str

class ErrorHandler:
    def __init__(self, logger=None):
        self.logger = logger or logging.getLogger("ErrorHandler")

    def validate_ia_output(self, raw_json_text, schema_class=DecisionSchema):
        try:
            if "```json" in raw_json_text:
                raw_json_text = raw_json_text.split("```json")[1].split("```")[0].strip()
            elif "```" in raw_json_text:
                raw_json_text = raw_json_text.split("```")[1].split("```")[0].strip()
            
            decision_dict = json.loads(raw_json_text)
            
            # Fallback per DecisionSchema
            if schema_class == DecisionSchema:
                if 'leverage' not in decision_dict:
                    decision_dict['leverage'] = 1.0
                decision_dict['direzione'] = decision_dict.get('direzione', 'FLAT')
                if 'timeframe_riferimento' not in decision_dict:
                    decision_dict['timeframe_riferimento'] = "15m"
                if 'stile_operativo' not in decision_dict:
                    decision_dict['stile_operativo'] = "SWING"
        except Exception:
            if schema_class == DecisionSchema:
                return {"direzione": "FLAT", "voto": 0, "sizing": 0, "leverage": 1, "stile_operativo": "SWING", "razionale": "JSON invalid"}
            else:
                return {}
            
        try:
            model = schema_class(**decision_dict)
            return model.model_dump()
        except ValidationError as e:
            if schema_class == DecisionSchema:
                return {"direzione": "FLAT", "voto": 0, "sizing": 0, "leverage": 1.0, "stile_operativo": "SWING", "razionale": f"Schema fail: {str(e)}"}
            else:
                self.logger.warning(f"⚠️ Schema validation failed for {schema_class.__name__}: {e}")
                return decision_dict # Ritorna il dict non validato come fallback estremo

class TestRunner:
    def unit_test(self, func, args):
        try:
            res = func(*args)
            return True, res
        except Exception as e:
            return False, str(e)

class BrainLA:
    def __init__(self, gemini_api_key=None, gemini_model_name="gemini-2.5-flash", 
                 api_key=None, api_secret=None, logger=None, account_limits=None, 
                 feedback_engine=None, alerts=None, engine=None):
        self.gemini_api_key = gemini_api_key or GEMINI_API_KEY
        self.logger = logger or logging.getLogger("BrainLA")
        
        if not self.gemini_api_key or self.gemini_api_key == "YOUR_API_KEY_HERE" or len(self.gemini_api_key) < 10:
            self.logger.error("🔴 ERRORE CRITICO: GEMINI_API_KEY mancante, vuota o non valida! Controlla il file .env")
            self.client = None
        else:
            try:
                masked_key = self.gemini_api_key[:5] + "..." + self.gemini_api_key[-4:]
                self.logger.info(f"🔑 Inizializzazione Gemini con API Key: {masked_key}")
                self.client = genai.Client(api_key=self.gemini_api_key)
            except Exception as e:
                _err.capture(e, "__init__", {"module": "BrainLA"})
                self.logger.error(f"🔴 Errore inizializzazione Gemini Client: {e}")
                self.client = None

        # Client backup per failover su 429
        self._client_backup = None
        self._client_primary_cooldown = 0.0
        _key2 = GEMINI_API_KEY_2 if GEMINI_API_KEY_2 else None
        if _key2 and len(_key2) >= 10 and _key2 != self.gemini_api_key:
            try:
                self._client_backup = genai.Client(api_key=_key2)
                self.logger.info(f"🔑 Gemini backup: {_key2[:5]}...{_key2[-4:]} — failover attivo")
            except Exception as e:
                _err.capture(e, "__init__", {"module": "BrainLA"})
                self.logger.warning(f"⚠️ Backup Gemini non inizializzato: {e}")

        self.gemini_model_name = gemini_model_name
        self.api_key = api_key or KRAKEN_KEY
        self.api_secret = api_secret or KRAKEN_SECRET
        self.account_limits = account_limits or {"max_size": 1.0}
        self.risk_manager = RiskManager()
        self.error_handler = ErrorHandler(logger=self.logger)
        self.strategy_manager = StrategyManager()
        self.dashboard_buffer = []
        self.feedback_engine = feedback_engine or FeedbackEngine()
        self.alerts = alerts
        self.engine = engine
        self.trade_manager = None
        self._llm_calls = deque()
        self._llm_rate_limit = 20   # paid tier Gemini Flash
        # ── response_schema kill-switch (2026-05-08) ────────────────────────
        # Se la prima chiamata con response_schema fallisce con errore
        # schema/INVALID_ARGUMENT, questo flag si attiva e disabilita
        # response_schema per tutto il resto della sessione. Il bot
        # ritorna automaticamente al comportamento legacy (solo response_mime_type).
        self._schema_mode_disabled = False
        # Carica cooldown persistito su disco (sopravvive ai riavvii)
        _quota_file = os.path.join(os.path.dirname(__file__), '.gemini_quota_until')
        try:
            if os.path.exists(_quota_file):
                with open(_quota_file) as _qf:
                    _qt = float(_qf.read().strip() or 0)
                self._gemini_quota_until = _qt if _qt > time.time() else 0.0
            else:
                self._gemini_quota_until = 0.0
        except Exception:
            self._gemini_quota_until = 0.0
        self.boot_time = time.time()

        # ── Memoria persistente delle decisioni di Gemini per asset ─────────
        # Permette a Gemini di vedere cosa lui stesso ha deciso sui cicli
        # precedenti su questo asset, con quale tesi e con quale esito.
        # Vedi core/gemini_memory.py.
        try:
            from core.gemini_memory import gemini_memory as _gm
            self.gemini_memory = _gm
            self.logger.info("🧠 GeminiMemory inizializzata — memoria decisionale persistente attiva")
        except Exception as _e_gm:
            self.gemini_memory = None
            self.logger.warning(f"⚠️ GeminiMemory non disponibile: {_e_gm}")

        # ── Tesi macro e scenari condizionali (v10) ─────────────────────────
        # MacroThesisCache: tesi macro condivisa per 4h tra tutte le decisioni
        # PendingScenarios: scenari condizionali formulati ai cicli precedenti
        # Vedi core/macro_thesis.py.
        try:
            from core.macro_thesis import macro_thesis_cache as _mtc, pending_scenarios as _ps
            self.macro_thesis_cache = _mtc
            self.pending_scenarios = _ps
            self.logger.info("🌍 MacroThesisCache + PendingScenarios inizializzati")
        except Exception as _e_mtc:
            self.macro_thesis_cache = None
            self.pending_scenarios = None
            self.logger.warning(f"⚠️ MacroThesisCache/PendingScenarios non disponibili: {_e_mtc}")

        # ── Profili asset (v11 Modifica I) ──────────────────────────────────
        # Memoria istituzionale per asset: natura, orari caldi, pattern tipici,
        # idiosincrasie operative + lezioni dal NightReview. Vedi core/asset_profiles.py.
        try:
            from core.asset_profiles import format_for_prompt as _ap_fmt, has_dedicated_profile as _ap_has
            self._asset_profile_format = _ap_fmt
            self._asset_profile_has = _ap_has
            self.logger.info("📋 AssetProfiles caricati — memoria istituzionale per asset attiva")
        except Exception as _e_ap:
            self._asset_profile_format = None
            self._asset_profile_has = None
            self.logger.warning(f"⚠️ AssetProfiles non disponibili: {_e_ap}")
    
    def _throttle_llm(self):
        now = time.time()
        window = 60
        while self._llm_calls and self._llm_calls[0] < now - window:
            self._llm_calls.popleft()
        if len(self._llm_calls) >= self._llm_rate_limit:
            wait = (self._llm_calls[0] + window) - now + random.uniform(0, 0.5)
            time.sleep(max(wait, 0))
        time.sleep(random.uniform(0.3, 0.8))
        self._llm_calls.append(time.time())
    
    def calcola_z_score(self, serie_prezzi, finestra=20):
        import pandas as pd
        import numpy as np
        if len(serie_prezzi) < finestra: return 0.0
        df = pd.Series(serie_prezzi)
        media = df.rolling(window=finestra).mean()
        std_dev = df.rolling(window=finestra).std()
        std_dev = std_dev.replace(0, np.nan)
        z_score_series = (df - media) / std_dev
        val = float(z_score_series.iloc[-1])
        return val if not (np.isnan(val) or np.isinf(val)) else 0.0

    def valuta_ingresso(self, asset, dati_mercato):
        try:
            from core.asset_list import get_ticker
            ticker_ufficiale = get_ticker(asset)
            chiusure = dati_mercato.get('storico_chiusure', [])
            prezzo_attuale = dati_mercato.get('close')
            
            if not chiusure or prezzo_attuale is None: return None
            z_score = self.calcola_z_score(chiusure)
            
            if abs(z_score) > 2.0:
                narrativa = self._get_technical_narrative(dati_mercato)
                prompt = (f"Asset: {ticker_ufficiale}\nPrezzo: {prezzo_attuale}\nZ-Score: {z_score:.2f}\n"
                          f"Contesto: {narrativa}\nAnalizza e decidi: BUY, SELL o FLAT. Sii telegrafico.")
                
                analisi = self.chiama_gemini(prompt, is_json=True)
                analisi = self._policy_adjust(ticker_ufficiale, analisi, dati_mercato)
                
                fase_due_attiva = False
                if analisi.get('direzione') != "FLAT":
                    fase_due_attiva, motivo, tp_esteso = self.analizza_fase_due_chimera(ticker_ufficiale, dati_mercato, analisi.get('direzione'))
                    if fase_due_attiva:
                        analisi['tp'] = tp_esteso
                        analisi['razionale'] += f" | {motivo}"
                
                scores = analisi.get('score_breakdown', {})
                razionale = analisi.get('razionale', 'Nessuna spiegazione fornita.')
                direzione = analisi.get('direzione', 'FLAT')
                voto_globale = analisi.get('voto', 0)

                if direzione != "FLAT":
                    if hasattr(self, 'alerts') and self.alerts:
                        lista_voti_str = "\n".join([f"• {k.replace('_', ' ')}: {v}/10" for k, v in scores.items()]) if scores else "N/A"
                        f2_str = "🔥 PHASE TWO ATTIVA" if fase_due_attiva else "Standard"
                        
                        safe_razionale = str(razionale).replace('_', ' ').replace('*', ' ').replace('[', '(').replace(']', ')')
                        msg = (f"🚀 *SEGNALE CHIMERA: {direzione}*\n📈 *Asset:* {ticker_ufficiale}\n💰 *Prezzo:* {prezzo_attuale}\n"
                               f"⭐ *Voto Globale:* {voto_globale}/10\n⚡ *Modo:* {f2_str}\n\n📊 *Matrice Decisionale:*\n{lista_voti_str}\n\n🧠 *Razionale:* {safe_razionale}")
                        self.alerts.invia_alert(msg)

                    return {
                        "asset": ticker_ufficiale, "action": direzione, "sl": analisi.get('sl', prezzo_attuale * 0.98),
                        "tp": analisi.get('tp', prezzo_attuale * 1.05), "voto": voto_globale, "leverage": analisi.get('leverage', 1),
                        "score_breakdown": scores, "razionale": razionale, "fase_due": fase_due_attiva
                    }
            return None
        except Exception as e:
            _err.capture(e, "unknown", {"module": "BrainLA"})
            self.logger.error(f"❌ Errore critico valuta_ingresso: {e}")
            return None

    def formulate_macro_thesis(self, contesto_macro: dict = None, force: bool = False):
        """
        Modifica A — formula la tesi macro condivisa, da chiamare ogni ~4h
        (o quando force=True) per dare a Gemini una "view del desk" coerente
        prima di analizzare i singoli asset.

        Se la tesi corrente è ancora valida (entro THESIS_TTL_SECONDS), non
        ne formula una nuova — risparmia chiamate Gemini.

        contesto_macro: dict opzionale con i dati macro disponibili
            (BTC trend, ETH leader, DXY, funding aggregato, fear/greed, regime BTC).
            Se None, prova a costruirlo da self.engine se disponibile.

        Returns: il dict della tesi salvata, oppure None se nulla è cambiato.
        """
        if self.macro_thesis_cache is None:
            return None

        # Se non forziamo e c'è già una tesi valida, skip
        if not force:
            current = self.macro_thesis_cache.get_current_thesis()
            if current:
                self.logger.debug(
                    f"🌍 Tesi macro ancora valida (formulata "
                    f"{int((time.time()-current['ts'])/60)} min fa). Skip."
                )
                return current

        # Costruisco contesto se non passato
        if contesto_macro is None:
            contesto_macro = {}
            try:
                if self.engine is not None:
                    # Snapshot rapido di BTC come proxy regime
                    btc_data = self.engine.get_dati_engine('XXBTZUSD') if hasattr(self.engine, 'get_dati_engine') else {}
                    contesto_macro = {
                        'btc_trend':       btc_data.get('ema_trend_dominante', 'N/A'),
                        'btc_regime':      btc_data.get('market_regime', 'N/A'),
                        'btc_hurst':       btc_data.get('hurst_exponent', 0.5),
                        'btc_funding_z':   btc_data.get('funding_z_score', 0),
                        'btc_atr_pct':     (btc_data.get('atr', 0) / btc_data.get('close', 1) * 100) if btc_data.get('close') else 0,
                        'btc_velocity':    btc_data.get('price_velocity', 0),
                        'fear_greed':      btc_data.get('fear_greed', 50),
                        'liquidations_24h': btc_data.get('liquidazioni_24h', 0),
                    }
            except Exception as _e_ctx:
                self.logger.warning(f"formulate_macro_thesis: errore costruzione contesto: {_e_ctx}")

        # Prompt per la tesi macro
        prompt_tesi = (
            "Sei un capo desk di trading istituzionale crypto. Davanti a te c'è il contesto\n"
            "macro corrente. Il tuo compito: formulare la TESI DEL GIORNO che guiderà\n"
            "tutti i trader del tuo desk per le prossime ore.\n\n"

            "CONTESTO MACRO ATTUALE:\n"
            f"  • BTC trend dominante (multi-TF): {contesto_macro.get('btc_trend','N/A')}\n"
            f"  • BTC market regime: {contesto_macro.get('btc_regime','N/A')}\n"
            f"  • BTC Hurst (>0.55=trend, <0.45=mean-rev): {contesto_macro.get('btc_hurst',0.5):.2f}\n"
            f"  • BTC Funding Z-score (estremi >|2| = squeeze): {contesto_macro.get('btc_funding_z',0):+.2f}\n"
            f"  • BTC volatilità (ATR %): {contesto_macro.get('btc_atr_pct',0):.2f}%\n"
            f"  • BTC velocity: {contesto_macro.get('btc_velocity',0):+.4f}\n"
            f"  • Fear & Greed Index: {contesto_macro.get('fear_greed','N/A')}/100\n"
            f"  • Liquidazioni 24h: {contesto_macro.get('liquidations_24h',0):.0f} USD\n\n"

            "Formula la tesi in 3-5 frasi. Sii concreto e direzionale, non vago.\n"
            "Esempio buono: 'BTC in trend rialzista pulito (Hurst 0.62) con funding ancora\n"
            "neutrale (z=0.3): non c'è ancora over-leverage. Bias LONG sui pullback fino a\n"
            "supporto 73k. SHORT solo se rejection chiara su 78k con CVD negativo.'\n\n"

            "Esempio cattivo: 'Mercato volatile, situazione mista, attenzione ai segnali'.\n\n"

            "Bias: scegli UNO tra LONG_BIAS, SHORT_BIAS, NEUTRAL, RISK_OFF, RISK_ON.\n\n"

            "RISPONDI ESCLUSIVAMENTE IN JSON PURO:\n"
            "{\n"
            '  "tesi": "3-5 frasi di analisi concreta e direzionale",\n'
            '  "bias": "LONG_BIAS|SHORT_BIAS|NEUTRAL|RISK_OFF|RISK_ON",\n'
            '  "scenari_macro": [\n'
            '    {"se_accade": "trigger macro 1", "implicazione": "cosa cambia per i trader"},\n'
            '    {"se_accade": "trigger macro 2", "implicazione": "..."}\n'
            "  ]\n"
            "}\n"
        )

        try:
            self.logger.info("🌍 Formulazione tesi macro in corso (chiamata Gemini)...")
            response = self.chiama_gemini(prompt_tesi, is_json=True)
            if not isinstance(response, dict):
                self.logger.warning("formulate_macro_thesis: risposta non è dict")
                return None
            tesi = response.get('tesi', '')
            bias = response.get('bias', 'NEUTRAL')
            scenari = response.get('scenari_macro', [])
            if not tesi:
                self.logger.warning("formulate_macro_thesis: tesi vuota")
                return None

            # Salva
            self.macro_thesis_cache.save_thesis(
                tesi=tesi, bias=bias, scenari_macro=scenari, contesto=contesto_macro
            )
            self.logger.info(f"🌍 TESI MACRO formulata — bias={bias}")
            self.logger.info(f"   {tesi[:200]}{'...' if len(tesi)>200 else ''}")

            if self.alerts:
                try:
                    self.alerts.invia_alert(
                        f"🌍 *TESI MACRO formulata*\nBias: *{bias}*\n\n{tesi[:400]}"
                    )
                except Exception:
                    pass

            return self.macro_thesis_cache.get_current_thesis()

        except Exception as e:
            _err.capture(e, "formulate_macro_thesis", {"module": "BrainLA"})
            self.logger.error(f"formulate_macro_thesis: errore: {e}")
            return None

    def chiama_gemini(self, prompt, is_json=True, schema_class=None):
        import json
        import time
        import random
        try: from pydantic import ValidationError
        except ImportError: ValidationError = Exception

        max_retries = 2  # 429: 1 tentativo + 1 retry, poi skip 90s
        if is_json:
            if schema_class == ThesisSchema:
                system_instruction = ("Sei un Senior Risk Manager. Rispondi ESCLUSIVAMENTE in formato JSON puro. "
                    "Campi obbligatori: valida (bool), motivo (string, max 12 parole), azione (HOLD/CLOSE/REVERSE).")
            elif schema_class == NightReviewSchema:
                system_instruction = ("Sei un Risk Manager istituzionale. Rispondi ESCLUSIVAMENTE in formato JSON puro. "
                    "Campi obbligatori: sintesi, pattern_errore_principale, condizioni_sfavorevoli, uso_time_stop, consiglio_sizing, regola_domani, voto_performance.")
            elif schema_class == AuditorSchema:
                system_instruction = ("Sei un Auditor Tecnico di Trading. Rispondi ESCLUSIVAMENTE in formato JSON puro. "
                    "Campi obbligatori: anomalia_rilevata (bool), gravita (ALTA/MEDIA/BASSA), descrizione_problema (string).")
            else:
                system_instruction = ("Sei un analista di mercato razionale. Hai accesso a 7 dimensioni di analisi indipendenti "
                    "che osservano il mercato da angolazioni diverse. "
                    "Il tuo compito è OSSERVARE ognuna delle 7 dimensioni, DICHIARARE cosa vedi in ognuna, "
                    "CONTARE quante puntano nella stessa direzione, e DECIDERE solo in base alla confluenza reale. "
                    "FLAT è una scelta valida e neutra. Non c'è penalità per dire FLAT quando la confluenza è bassa. "
                    "Non c'è premio per entrare. Decidi solo basandoti su cosa vedi davvero, non su narrative coerenti. "
                    "La maggior parte del tempo il mercato non offre setup chiari: in quei casi FLAT è la risposta corretta. "
                    "Rispondi ESCLUSIVAMENTE in formato JSON puro. "
                    "Campi obbligatori: scansione_dimensioni (object con 7 chiavi), conteggio_confluenza (object con long/short/neutro/veto), "
                    "direzione (BUY/SELL/FLAT), voto (0-10), sizing (0-1), leverage, sl, tp, "
                    "stile_operativo (SCALPING/SWING/MOMENTUM), timeframe_riferimento (1m/5m/15m/1h/4h/1d), razionale, "
                    "score_breakdown (object con voti 0-10 per: Order_Flow, Liquidity, Market_Regime, Velocity, Volatility). "
                    "Il campo 'razionale' DEVE essere TELEGRAFICO, MAX 10 PAROLE.")
        else:
            system_instruction = "Sei un analista quantitativo e trader istituzionale. Fornisci un report discorsivo, professionale, chiaro e dettagliato."
            
        full_prompt = f"{system_instruction}\n\n{prompt}"

        if getattr(self, '_gemini_quota_until', 0) > time.time():
            if is_json:
                return {"direzione": "FLAT", "voto": 0, "sizing": 0, "leverage": 1, "sl": 0, "tp": 0,
                        "stile_operativo": "SWING", "timeframe_riferimento": "none",
                        "score_breakdown": {"Order_Flow": 0, "Liquidity": 0, "Market_Regime": 0, "Velocity": 0, "Volatility": 0},
                        "apprendimento_critico": "quota_cooldown", "razionale": "quota_cooldown_skip"}
            return "Quota Gemini in cooldown."

        for i in range(max_retries):
            # Default per scope sicuro nel branch except (vedi schema rejection handler)
            use_schema_now = False
            try:
                if i > 0:
                    time.sleep(2.0)

                # Failover: usa backup se primario in cooldown
                _c = self.client
                if time.time() < getattr(self, '_client_primary_cooldown', 0):
                    _c = getattr(self, '_client_backup', None) or self.client

                # ── Build config con response_schema se non disabilitato ────
                # Forza Gemini a rispettare lo schema Pydantic (compliance ai
                # campi richiesti come condizioni_tesi, scansione_dimensioni,
                # ecc.). Se lo schema viene rifiutato dall'API, il kill-switch
                # nel branch except sotto disabilita response_schema per la
                # sessione e si ricade sul comportamento legacy.
                _config_dict = {
                    "temperature": 0.4,
                    "response_mime_type": "application/json" if is_json else "text/plain",
                }
                use_schema_now = is_json and not getattr(self, '_schema_mode_disabled', False)
                if use_schema_now:
                    _target_schema = schema_class if schema_class else DecisionSchema
                    _config_dict["response_schema"] = _target_schema

                response = _c.models.generate_content(
                    model=self.gemini_model_name, contents=full_prompt,
                    config=_config_dict
                )
                if response and response.text:
                    testo_output = response.text.strip()
                    if not is_json: return testo_output
                    
                    # Pulizia estrema per evitare "Extra data" errors
                    if "{" in testo_output:
                        json_start = testo_output.find("{")
                        json_end = testo_output.rfind("}") + 1
                        testo_json = testo_output[json_start:json_end]
                        
                        try:
                            decision_dict = json.loads(testo_json)
                            target_schema = schema_class if schema_class else DecisionSchema
                            validated_data = self.error_handler.validate_ia_output(testo_json, schema_class=target_schema)
                            return validated_data
                        except json.JSONDecodeError as e:
                            self.logger.warning(f"⚠️ Errore decodifica JSON (Tentativo {i+1}/{max_retries}): {e}")
                            continue
                    else:
                        raise ValueError("Nessun blocco JSON trovato nella risposta")
            except Exception as e:
                error_str = str(e)
                _err_upper = error_str.upper()

                # Pre-classifica: errori chiaramente NON di schema
                _is_quota_err = ("429" in _err_upper or "RESOURCE_EXHAUSTED" in _err_upper)
                _is_network_err = any(err in _err_upper for err in [
                    "503", "UNAVAILABLE", "500", "INTERNAL", "502", "BAD_GATEWAY",
                    "504", "TIMEOUT", "DEADLINE_EXCEEDED", "ERRNO 8", "NODENAME",
                    "CONNECTION", "NETWORK"
                ])

                # ── KILL-SWITCH response_schema (2026-05-08, v2 difensivo) ──
                # Approccio "presumption of guilt": se response_schema è attivo
                # E l'errore NON è chiaramente quota/network → presumiamo
                # problema di schema e disabilitiamo per la sessione.
                #
                # Errori noti su schema Pydantic→Gemini:
                #   • "additionalProperties is not supported" (Pydantic strict mode)
                #   • "propertyOrdering" (keyword non supportata)
                #   • 400 INVALID_ARGUMENT
                #   • ValueError dalla validazione client-side della SDK
                #   • RESPONSE_SCHEMA / RESPONSE_JSON_SCHEMA rejection
                #
                # La versione precedente cercava parole chiave specifiche e
                # mancava il caso "additionalProperties" che è il più comune
                # quando si passa un modello Pydantic come response_schema.
                if (use_schema_now
                    and not getattr(self, '_schema_mode_disabled', False)
                    and not _is_quota_err
                    and not _is_network_err):
                    self._schema_mode_disabled = True
                    _err.capture(e, "chiama_gemini.schema_disabled",
                                 {"module": "BrainLA",
                                  "error": error_str[:200],
                                  "exc_type": type(e).__name__},
                                 level="WARNING")
                    self.logger.warning(
                        f"⚠️ response_schema rifiutato — disabilitato per la sessione. "
                        f"Bot continua in modalità legacy. "
                        f"{type(e).__name__}: {error_str[:180]}"
                    )
                    continue  # retry IMMEDIATO senza response_schema

                if _is_quota_err:
                    now = time.time()
                    _backup = getattr(self, '_client_backup', None)
                    if _backup and time.time() >= getattr(self, '_client_primary_cooldown', 0):
                        self._client_primary_cooldown = now + 90
                        self.logger.warning("⚠️ Gemini 429 — switch a backup...")
                        time.sleep(random.uniform(1.0, 2.0))
                        continue
                    # Una sola key: aspetta 15s e riprova una volta sola
                    if i == 0:
                        self.logger.warning("⚠️ Gemini 429 — attendo 15s e riprovo...")
                        time.sleep(15)
                        continue
                    # Secondo tentativo fallito: salta il resto del ciclo (180s)
                    # 180s copre 2 cicli completi evitando loop di 429
                    self._gemini_quota_until = now + 180
                    # Persisti su disco così sopravvive ai riavvii del bot
                    try:
                        _quota_file = os.path.join(os.path.dirname(__file__), '.gemini_quota_until')
                        with open(_quota_file, 'w') as _qf:
                            _qf.write(str(self._gemini_quota_until))
                    except Exception:
                        pass
                    self.logger.warning("⚠️ Gemini 429 persistente — skip ciclo (riprova tra 180s).")
                    break
                elif any(err in error_str.upper() for err in ["503", "UNAVAILABLE", "500", "INTERNAL", "502", "BAD_GATEWAY", "504", "TIMEOUT", "DEADLINE_EXCEEDED", "ERRNO 8", "NODENAME", "CONNECTION", "NETWORK"]):
                    # Backoff esponenziale con jitter: 15s, 30s, 60s, 120s, 240s + jitter
                    wait_time = (15 * (2 ** i)) + random.uniform(1.0, 5.0)
                    self.logger.warning(f"⚠️ Errore temporaneo Gemini (Tentativo {i+1}/{max_retries}): {error_str[:50]}... Attendo {wait_time:.2f}s...")
                    time.sleep(wait_time)
                else: 
                    _err.capture(e, "chiama_gemini", {"module": "BrainLA", "is_json": is_json, "error": str(e)[:120]})
                    self.logger.error(f"🔴 Errore critico Gemini: {e}")
                    break
        
        if not is_json: return "Errore nella generazione del report. Verificare i log."
        return {"direzione": "FLAT", "voto": 0, "sizing": 0, "leverage": 1, "sl": 0, "tp": 0, "stile_operativo": "SWING",
                "timeframe_riferimento": "none", "score_breakdown": {"Order_Flow": 0, "Liquidity": 0, "Market_Regime": 0, "Velocity": 0, "Volatility": 0},
                "apprendimento_critico": "api_failure", "razionale": "api_failure_fallback"}

    def get_kraken_balance(self):
        temp_engine = engine_la.EngineLA(api_key=self.api_key, api_secret=self.api_secret)
        try:
            balances = temp_engine.exchange.fetch_balance()
            return balances.get('total', {})
        except:
            return {}

    def valuta_modifica_posizione(self, dati_engine, posizione):
        p_entrata = posizione.get("p_entrata", 0)
        tp_new, sl_new, ts_new = self.determina_tp_sl_ts(
            asset_name=posizione.get('asset'), direzione=posizione.get('direzione'),
            prezzo_ingresso=p_entrata, dati_engine=dati_engine
        )
        return (str(sl_new) != str(posizione.get("sl")) or str(tp_new) != str(posizione.get("tp")))

    # ════════════════════════════════════════════════════════════════════════
    # VALUTAZIONE POSIZIONE APERTA — consulto Gemini su cosa fare ora
    # ════════════════════════════════════════════════════════════════════════
    def valuta_posizione_aperta(self, asset, dati_engine, posizione):
        """
        Valuta una posizione GIÀ APERTA per decidere se TIENI / CHIUDI / TIGHTEN_SL.
        
        FIX 2026-05-04: prima il bot non chiamava mai Gemini su posizioni aperte
        (faceva continue dopo aver loggato il PnL). Risultato: BTC SHORT aperto e
        ignorato per 16+ ore mentre il prezzo faceva movimenti ±2%.
        
        Filosofia:
        - Gemini riceve i dati live + lo stato della posizione (entry, SL, TP, PnL)
          e dà un giudizio sulla tesi originale: è ancora valida o si è invalidata?
        - Se la sua direzione raccomandata è OPPOSTA a quella della posizione
          → tesi invalidata → CHIUDI_ANTICIPATO se voto >= 8.
        - Se Gemini è incerto ma il PnL è positivo → TIGHTEN_SL al BE per
          proteggere profitti (voto >= 7).
        - Altrimenti TIENI.
        - RINFORZA non è mai un'opzione: con capitale piccolo è suicidio.
        
        Args:
            asset: ticker dell'asset (es. 'XXBTZUSD').
            dati_engine: dict con dati di mercato live (close, atr, cvd, vpin, ecc).
            posizione: dict della posizione aperta (direzione, p_entrata, sl, tp,
                       leverage, data_apertura, voto_ia originale).
        
        Returns:
            dict {azione, voto, motivo, direzione_gemini} dove:
            - azione = 'TIENI' | 'CHIUDI_ANTICIPATO' | 'TIGHTEN_SL'
            - voto = int 0-10 (convinzione di Gemini)
            - motivo = stringa breve
            - direzione_gemini = 'LONG' | 'SHORT' | 'FLAT'
            
            Ritorna {azione: 'TIENI', voto: 0, motivo: 'errore'} su qualsiasi failure.
            È DESIGN: in caso di dubbio, NON chiudere prematuramente.
        """
        try:
            ticker_ufficiale = asset
            direzione_pos = str(posizione.get('direzione', 'LONG')).upper()
            p_entrata = float(posizione.get('p_entrata', 0))
            sl_attuale = float(posizione.get('sl', 0))
            tp_attuale = float(posizione.get('tp', 0))
            voto_originale = int(posizione.get('voto_ia') or 0)
            data_apertura = str(posizione.get('data_apertura', ''))
            prezzo_attuale = float(dati_engine.get('close', 0))
            
            if prezzo_attuale <= 0 or p_entrata <= 0:
                return {'azione': 'TIENI', 'voto': 0, 'motivo': 'dati prezzo non disponibili',
                        'direzione_gemini': 'FLAT'}
            
            # Calcoli derivati
            is_long = direzione_pos in ('LONG', 'BUY')
            if is_long:
                pnl_perc_lordo = (prezzo_attuale - p_entrata) / p_entrata * 100
            else:
                pnl_perc_lordo = (p_entrata - prezzo_attuale) / p_entrata * 100
            
            # Tempo dall'apertura (ore)
            ore_aperta = 0
            try:
                from datetime import datetime as _dt
                t_open = _dt.fromisoformat(data_apertura.replace('Z', '').replace('T', ' '))
                ore_aperta = (_dt.now() - t_open).total_seconds() / 3600
            except Exception:
                pass
            
            # Distanza % al SL e al TP (per capire quanto è "stretto")
            if is_long:
                dist_sl_perc = (prezzo_attuale - sl_attuale) / prezzo_attuale * 100 if sl_attuale > 0 else 999
                dist_tp_perc = (tp_attuale - prezzo_attuale) / prezzo_attuale * 100 if tp_attuale > 0 else 999
            else:
                dist_sl_perc = (sl_attuale - prezzo_attuale) / prezzo_attuale * 100 if sl_attuale > 0 else 999
                dist_tp_perc = (prezzo_attuale - tp_attuale) / prezzo_attuale * 100 if tp_attuale > 0 else 999
            
            # Estraggo dati di mercato chiave (sintesi)
            cvd_ist = dati_engine.get('cvd_istantaneo', 0)
            vpin = dati_engine.get('vpin', 0)
            ofi = dati_engine.get('order_flow_imbalance', 0)
            hurst = dati_engine.get('hurst_exponent', 0.5)
            kaufman = dati_engine.get('kaufman_efficiency', 0.5)
            ha_colore = dati_engine.get('ha_daily_colore', '?')
            ha_streak = dati_engine.get('ha_daily_streak', 0)
            phase = dati_engine.get('entry_phase', '?')
            cvd_30s = dati_engine.get('cvd_delta_30s', 0)
            ciclo = dati_engine.get('ciclo_fase', '?')
            
            prompt = (
                f"VALUTA POSIZIONE APERTA su {ticker_ufficiale}.\n\n"
                f"📊 STATO POSIZIONE:\n"
                f"- Direzione: {direzione_pos}\n"
                f"- Entry: {p_entrata:.6f}\n"
                f"- Prezzo attuale: {prezzo_attuale:.6f}\n"
                f"- PnL lordo: {pnl_perc_lordo:+.2f}%\n"
                f"- SL: {sl_attuale:.6f} (dist {dist_sl_perc:+.2f}%)\n"
                f"- TP: {tp_attuale:.6f} (dist {dist_tp_perc:+.2f}%)\n"
                f"- Voto originale apertura: {voto_originale}\n"
                f"- Aperta da: {ore_aperta:.1f} ore\n\n"
                f"📈 MERCATO ATTUALE:\n"
                f"- CVD istantaneo: {cvd_ist:+.0f}\n"
                f"- CVD delta 30s: {cvd_30s:+.0f}\n"
                f"- VPIN: {vpin:.3f}\n"
                f"- OFI: {ofi:+.3f}\n"
                f"- Hurst: {hurst:.2f}\n"
                f"- Kaufman: {kaufman:.3f}\n"
                f"- HA daily: {ha_colore} streak={ha_streak}\n"
                f"- Fase SSE: {phase}\n"
                f"- Ciclo strutturale: {ciclo}\n\n"
                f"DOMANDA: la TESI originale ({direzione_pos}) è ANCORA VALIDA?\n"
                f"O il mercato si è girato e dovremmo USCIRE prima del SL/TP?\n\n"
                f"Rispondi in JSON con queste chiavi:\n"
                f'  - "direzione_attuale": "LONG" | "SHORT" | "FLAT" (cosa faresti ORA)\n'
                f'  - "voto": 0-10 (convinzione)\n'
                f'  - "azione": "TIENI" | "CHIUDI_ANTICIPATO" | "TIGHTEN_SL"\n'
                f'  - "motivo": stringa max 120 chars\n\n'
                f"REGOLE:\n"
                f"- TIENI se la tesi originale è ancora valida o se sei incerto.\n"
                f"- CHIUDI_ANTICIPATO solo se voto>=8 E direzione_attuale è OPPOSTA "
                f"alla posizione (es. pos SHORT, tu dici LONG voto 8+).\n"
                f"- TIGHTEN_SL se PnL>0 e momentum si sta invertendo (voto>=7).\n"
                f"- Default = TIENI. Non chiudere su sospetti deboli.\n"
                f"- RINFORZARE NON è un'opzione (capitale piccolo).\n"
                f"\nRispondi SOLO con il JSON, niente altro."
            )
            
            risposta_raw = self.chiama_gemini(prompt, is_json=True)
            if not risposta_raw or not isinstance(risposta_raw, dict):
                return {'azione': 'TIENI', 'voto': 0,
                        'motivo': 'risposta Gemini non parsabile',
                        'direzione_gemini': 'FLAT'}
            
            azione = str(risposta_raw.get('azione', 'TIENI')).upper()
            voto = int(risposta_raw.get('voto', 0) or 0)
            motivo = str(risposta_raw.get('motivo', ''))[:120]
            direz_g = str(risposta_raw.get('direzione_attuale', 'FLAT')).upper()
            
            # Sanity check direzioni
            if direz_g not in ('LONG', 'SHORT', 'FLAT'):
                direz_g = 'FLAT'
            if azione not in ('TIENI', 'CHIUDI_ANTICIPATO', 'TIGHTEN_SL'):
                azione = 'TIENI'
            
            # Enforcer regole: blocca azioni con voto sotto soglia
            if azione == 'CHIUDI_ANTICIPATO':
                # Direzione DEVE essere opposta + voto >= 8
                opposta = (is_long and direz_g == 'SHORT') or (not is_long and direz_g == 'LONG')
                if voto < 8 or not opposta:
                    self.logger.info(
                        f"🛡️ [{ticker_ufficiale}] Gemini suggerisce CHIUDI ma voto={voto}<8 "
                        f"o direzione non opposta (pos={direzione_pos}, gem={direz_g}). Mantengo."
                    )
                    azione = 'TIENI'
            elif azione == 'TIGHTEN_SL':
                # PnL deve essere positivo + voto >= 7
                if voto < 7 or pnl_perc_lordo <= 0:
                    azione = 'TIENI'
            
            return {'azione': azione, 'voto': voto, 'motivo': motivo,
                    'direzione_gemini': direz_g, 'pnl_perc': pnl_perc_lordo}
        
        except Exception as e:
            try:
                _err.capture(e, "valuta_posizione_aperta", {"module": "BrainLA"})
            except Exception:
                pass
            self.logger.error(f"❌ valuta_posizione_aperta {asset}: {e}")
            return {'azione': 'TIENI', 'voto': 0,
                    'motivo': f'errore: {e}', 'direzione_gemini': 'FLAT'}

    def determina_tp_sl_ts(self, asset_name, direzione, prezzo_ingresso, dati_engine, levels_ia=None):
        try:
            from core.asset_list import get_ticker, ASSET_CONFIG
            ticker_ufficiale = get_ticker(asset_name)
            conf = ASSET_CONFIG.get(ticker_ufficiale, {})
            prec = conf.get("precision", 2)

            if not prezzo_ingresso or prezzo_ingresso <= 0: return 0, 0, 0

            direzione = str(direzione).upper()

            # Stile operativo — calibra i moltiplicatori ATR
            stile = "SWING"
            if isinstance(levels_ia, dict):
                stile = str(levels_ia.get('stile_operativo', 'SWING')).upper()

            # Moltiplicatori ATR per stile:
            # SCALPING : SL 1.0x ATR,  TP 1.5x ATR  → movimenti stretti, veloce
            # MOMENTUM : SL 1.5x ATR,  TP 2.5x ATR  → trend in corso
            # SWING    : SL 2.2x ATR,  TP 3.5x ATR  → ampio respiro
            if stile == "SCALPING":
                mult_sl = 1.0
                mult_tp = 1.5
                min_rr  = 1.5
            elif stile == "MOMENTUM":
                mult_sl = 1.5
                mult_tp = 2.5
                min_rr  = 1.5
            else:  # SWING default
                mult_sl = 2.2
                mult_tp = 3.5
                min_rr  = 1.5
            
            # --- 1. TAKE PROFIT DINAMICO (Muri + ATR) ---
            # FIX: le chiavi corrette sono 'muro_resistenza' e 'muro_supporto'
            # (non 'muro_resistenza_prezzo' — queste non esistono nel dict Engine)
            muro_res  = float(dati_engine.get('muro_resistenza', 0))
            muro_supp = float(dati_engine.get('muro_supporto',   0))
            atr_val = 0
            if isinstance(dati_engine, dict):
                atr_val = dati_engine.get('atr', 0)
                if isinstance(atr_val, dict): atr_val = atr_val.get('value', 0)
            atr = float(atr_val) if float(atr_val) > 0 else prezzo_ingresso * 0.01

            if direzione in ['BUY', 'LONG']:
                # TP primario: poco sotto il muro di resistenza, o mult_tp x ATR
                if muro_res > prezzo_ingresso:
                    tp_f = muro_res * 0.998
                    tp_type = "MURO"
                    self.logger.info(f"📐 [{ticker_ufficiale}] TP da muro resistenza: {muro_res:.5f} → {tp_f:.5f}")
                else:
                    tp_f = prezzo_ingresso + (atr * mult_tp)
                    tp_type = f"ATR×{mult_tp}"

                tp_f = max(tp_f, prezzo_ingresso * 1.002)  # Min 0.2%
                tp_f = min(tp_f, prezzo_ingresso * 1.25)   # Max 25%

            elif direzione in ['SELL', 'SHORT']:
                # TP primario: poco sopra il muro di supporto, o mult_tp x ATR
                if muro_supp > 0 and muro_supp < prezzo_ingresso:
                    tp_f = muro_supp * 1.002
                    tp_type = "MURO"
                    self.logger.info(f"📐 [{ticker_ufficiale}] TP da muro supporto: {muro_supp:.5f} → {tp_f:.5f}")
                else:
                    tp_f = prezzo_ingresso - (atr * mult_tp)
                    tp_type = f"ATR×{mult_tp}"

                tp_f = min(tp_f, prezzo_ingresso * 0.998)  # Min 0.2%
                tp_f = max(tp_f, prezzo_ingresso * 0.75)   # Max 25%
            else:
                return 0, 0, 0

            # --- 2. STOP LOSS DINAMICO (Muri + ATR) ---
            sl_f = None

            # Cap massimo SL per stile — Gemini a volte suggerisce SL troppo larghi
            cap_sl_perc = {'SCALPING': 0.015, 'MOMENTUM': 0.035, 'SWING': 0.07}
            max_sl_dist = prezzo_ingresso * cap_sl_perc.get(stile, 0.05)

            # Se Brain fornisce uno SL, lo validiamo E lo cappamo per stile
            if isinstance(levels_ia, dict):
                sl_ia = levels_ia.get('sl')
                if sl_ia and float(sl_ia) > 0:
                    temp_sl = float(sl_ia)
                    dist_sl_ia = abs(prezzo_ingresso - temp_sl)
                    if direzione in ['BUY', 'LONG'] and temp_sl < prezzo_ingresso:
                        if dist_sl_ia <= max_sl_dist:
                            sl_f = temp_sl
                        else:
                            self.logger.info(
                                f"📐 [{ticker_ufficiale}] SL Gemini ({temp_sl:.5f}, dist {dist_sl_ia/prezzo_ingresso*100:.2f}%) "
                                f"troppo largo per {stile} (max {cap_sl_perc.get(stile,0.05)*100:.1f}%) — ignoro, uso ATR/muri."
                            )
                    elif direzione in ['SELL', 'SHORT'] and temp_sl > prezzo_ingresso:
                        if dist_sl_ia <= max_sl_dist:
                            sl_f = temp_sl
                        else:
                            self.logger.info(
                                f"📐 [{ticker_ufficiale}] SL Gemini ({temp_sl:.5f}, dist {dist_sl_ia/prezzo_ingresso*100:.2f}%) "
                                f"troppo largo per {stile} (max {cap_sl_perc.get(stile,0.05)*100:.1f}%) — ignoro, uso ATR/muri."
                            )

            # Se non c'è SL valido da Brain, usiamo i muri o ATR
            if sl_f is None:
                velocity = dati_engine.get('price_velocity', 0.0) if isinstance(dati_engine, dict) else 0
                if stile == "SCALPING":
                    moltiplicatore_rumore = mult_sl
                else:
                    moltiplicatore_rumore = 1.8 if abs(velocity) > 0.0006 else mult_sl

                # Soglia minima distanza muro: muri troppo vicini (<0.3%) non sono muri reali
                min_dist_muro = 0.003  # 0.3%

                if direzione in ['BUY', 'LONG']:
                    if (muro_supp > 0 and muro_supp < prezzo_ingresso and
                            (prezzo_ingresso - muro_supp) / prezzo_ingresso >= min_dist_muro):
                        sl_f = muro_supp * 0.995
                    else:
                        sl_f = prezzo_ingresso - (atr * moltiplicatore_rumore)
                    # Cap per stile
                    max_sl_perc_abs = 0.985 if stile == "SCALPING" else (0.965 if stile == "MOMENTUM" else 0.95)
                    sl_f = max(sl_f, prezzo_ingresso * max_sl_perc_abs)
                else:
                    if (muro_res > prezzo_ingresso and
                            (muro_res - prezzo_ingresso) / prezzo_ingresso >= min_dist_muro):
                        sl_f = muro_res * 1.005
                    else:
                        sl_f = prezzo_ingresso + (atr * moltiplicatore_rumore)
                    max_sl_perc_abs = 1.015 if stile == "SCALPING" else (1.035 if stile == "MOMENTUM" else 1.05)
                    sl_f = min(sl_f, prezzo_ingresso * max_sl_perc_abs)

            # --- R:R ENFORCEMENT (minimo 1.5:1 per tutti gli stili) ---
            dist_sl = abs(prezzo_ingresso - sl_f)
            dist_tp = abs(tp_f - prezzo_ingresso)

            if dist_sl > 0 and dist_tp < dist_sl * min_rr:
                if direzione in ['BUY', 'LONG']:
                    tp_f_rr = prezzo_ingresso + dist_sl * min_rr
                    if tp_f_rr > tp_f:
                        tp_f = tp_f_rr
                        tp_type = f"RR{min_rr}x"
                        self.logger.info(
                            f"📐 [{ticker_ufficiale}] TP espanso a R:R {min_rr}:1 "
                            f"(stile:{stile} SL={dist_sl:.4f} → TP min={tp_f:.4f})"
                        )
                else:
                    tp_f_rr = prezzo_ingresso - dist_sl * min_rr
                    if tp_f_rr < tp_f:
                        tp_f = tp_f_rr
                        tp_type = f"RR{min_rr}x"
                        self.logger.info(
                            f"📐 [{ticker_ufficiale}] TP espanso a R:R {min_rr}:1 "
                            f"(stile:{stile} SL={dist_sl:.4f} → TP min={tp_f:.4f})"
                        )
                if direzione in ['BUY', 'LONG']:
                    tp_f = min(tp_f, prezzo_ingresso * 1.25)
                else:
                    tp_f = max(tp_f, prezzo_ingresso * 0.75)

            # --- 4. DISTANZA TRAILING STOP (Basata su ATR) ---
            atr_val = 0
            if isinstance(dati_engine, dict):
                atr_val = dati_engine.get('atr', 0)
                if isinstance(atr_val, dict): atr_val = atr_val.get('value', 0)
            atr = float(atr_val) if float(atr_val) > 0 else prezzo_ingresso * 0.005
            distanza_trailing = round(atr * 1.5, prec)

            self.logger.info(f"🎯 Livelli Finali {ticker_ufficiale}: SL {round(sl_f, prec)} (Brain/Fallback), TP {round(tp_f, prec)} ({tp_type})")
            
            return round(tp_f, prec), round(sl_f, prec), distanza_trailing
        except Exception as e:
            _err.capture(e, "unknown", {"module": "BrainLA"})
            self.logger.error(f"❌ Errore sinergia determina_tp_sl_ts: {e}")
            return 0, 0, 0

    def analizza_fase_due_chimera(self, asset, dati_engine, direzione_pos):
        try:
            velocity = dati_engine.get('price_velocity', 0.0)
            prezzo_attuale = dati_engine.get('close', 0)
            if prezzo_attuale <= 0: return False, None, None
            soglia = 0.0006 
            
            if direzione_pos == "BUY" and velocity > soglia:
                motivo = f"🚀 Momentum HFT Rialzista: {velocity:.5f} %/sec"
                tp_esteso = round(float(prezzo_attuale) * 1.15, 2) 
                return True, motivo, tp_esteso
            elif direzione_pos == "SELL" and velocity < -soglia:
                motivo = f"📉 Momentum HFT Ribassista: {velocity:.5f} %/sec"
                tp_esteso = round(float(prezzo_attuale) * 0.85, 2) 
                return True, motivo, tp_esteso
            return False, None, None
        except Exception as e:
            _err.capture(e, "analizza_fase_due_chimera", {"module": "BrainLA"})
            self.logger.error(f"❌ Errore analizza_fase_due_chimera su {asset}: {e}")
            return False, None, None

    def check_chimera_phase_two(self, ticker, dati_engine):
        try:
            velocity = dati_engine.get('price_velocity', 0.0)
            if abs(velocity) > 0.0006:
                return True, velocity
            return False, velocity
        except Exception as e:
            _err.capture(e, "check_chimera_phase_two", {"module": "BrainLA", "ticker": ticker}, level="WARNING")
            return False, 0.0

    def _policy_adjust(self, asset_name, decision, dati_engine):
        """
        Aggiustamento della policy basato sulla qualità del segnale corrente.

        Filosofia: un trader non riduce la size perché ha perso ieri.
        Riduce la size quando il segnale attuale è debole o il mercato
        non sta rispettando le sue strategie.

        Con XGBoost attivo (AUC > 0.65), il ML è il filtro principale
        sulla qualità del segnale — non i contatori di perdite.
        Lo streak serve come contesto informativo, non come penalità.
        """
        try:
            metrics = self.feedback_engine.get_asset_metrics(asset_name, window=50)
            win_rate = float(metrics.get("win_rate", 50))
            streak_loss = int(metrics.get("streak_loss", 0))

            # ── 1. Alpha Decay ────────────────────────────────────────────────────
            # Scatta solo se PF < 0.5 su 30 trade — mercato che rifiuta sistematicamente.
            health = self.feedback_engine.get_strategy_health(window=30)
            if health["status"] == "DECAY":
                pf_score = health.get('score', 0.5)
                if pf_score < 0.5:
                    decay_factor = max(0.70, 0.5 + pf_score * 0.7)
                    decision['sizing'] = round(decision.get('sizing', 0) * decay_factor, 5)
                    self.logger.warning(
                        f"⚠️ [ALPHA DECAY] {health['msg']}. "
                        f"Sizing ridotto a {decay_factor:.0%} (voto invariato)"
                    )
                    decision['razionale'] += f" | 📉 Alpha Decay (sizing {decay_factor:.0%})"

            # ── 2. Z-Score / Funding protection ──────────────────────────────────
            z = float(dati_engine.get('z_score', 0))
            fz = float(dati_engine.get('funding_z_score', 0))
            hurst = float(dati_engine.get('hurst', 0.5))
            velocity = abs(float(dati_engine.get('price_velocity', 0.0)))
            is_strong_trend = hurst > 0.6 or velocity > 0.0008

            if not is_strong_trend:
                if z > 2.5 and fz > 2.5 and decision['direzione'] == "BUY":
                    decision['voto'] = max(0, decision.get('voto', 0) - 2)
                    decision['razionale'] += " | ⚠️ Protezione: Z-Score/Funding estremi su BUY"
                if z < -2.5 and fz < -2.5 and decision['direzione'] == "SELL":
                    decision['voto'] = max(0, decision.get('voto', 0) - 2)
                    decision['razionale'] += " | ⚠️ Protezione: Z-Score/Funding estremi su SELL"
            else:
                decision['razionale'] += f" | 🚀 TREND DETECTED (Hurst:{hurst:.2f}): Filtri Z-Score bypassati"

            # ── 3. Win rate storico — aggiustamento lieve su lungo periodo ────────
            if win_rate < 35:
                decision['voto'] = max(0, decision.get('voto', 0) - 1)
            elif win_rate > 60:
                decision['voto'] = min(10, decision.get('voto', 0) + 1)

            # ── 4. Streak Loss — informazione, non penalità ───────────────────────
            # Con XGBoost a AUC 0.870, il modello ha già imparato dai pattern
            # delle perdite consecutive. Non serve una penalità meccanica in più.
            # Riduciamo sizing solo per streak molto alto (>= 7) e solo del 20%.
            if streak_loss >= 7:
                decision['sizing'] = round(decision.get('sizing', 0) * 0.80, 5)
                self.logger.info(
                    f"ℹ️ [STREAK] {asset_name}: {streak_loss} perdite consecutive — "
                    f"sizing -20% (ML è il filtro principale)"
                )
                decision['razionale'] += f" | ℹ️ Streak {streak_loss} (sizing 80%)"
            elif streak_loss >= 4:
                self.logger.info(
                    f"ℹ️ [STREAK] {asset_name}: {streak_loss} perdite consecutive "
                    f"(contesto informativo — ML decide)"
                )

            # ── 4-bis. FIX-F (2026-04-26): SHORT controtrend HA daily VERDE ───
            # Analisi DB: SHORT con HA daily VERDE = 57 trade, WR 29.8%, perdente
            # sistematico. È un controtrend forzato che apre quando il momentum
            # daily è ancora rialzista. Non blocco totale (a volte il VERDE è
            # esauriente: streak >=4 con body grosso può essere top), ma alzo
            # la soglia voto e taglio il sizing.
            _ha_col_pol = str(dati_engine.get('ha_daily_colore', '') or '').upper()
            _ha_streak_pol = int(dati_engine.get('ha_daily_streak', 0) or 0)
            if (decision.get('direzione') in ('SELL', 'SHORT')
                    and _ha_col_pol == 'VERDE'
                    and _ha_streak_pol < 4):  # streak<4: trend rialzista ancora fresco
                voto_orig = decision.get('voto', 0)
                if voto_orig < 8:
                    decision['voto'] = 0
                    decision['direzione'] = 'FLAT'
                    decision['razionale'] += (
                        f" | 🚫 SHORT controtrend HA VERDE streak={_ha_streak_pol}: "
                        f"voto {voto_orig}<8 → FLAT (storico WR 29.8%)"
                    )
                    self.logger.warning(
                        f"🚫 [{asset_name}] FILTRO HA: SHORT vs HA VERDE streak={_ha_streak_pol} "
                        f"voto={voto_orig} — trade FLATTATO (storico WR 29.8%)"
                    )
                else:
                    # Voto alto: lascia passare ma con sizing dimezzato
                    decision['sizing'] = round(decision.get('sizing', 1.0) * 0.5, 5)
                    decision['razionale'] += (
                        f" | ⚠️ SHORT controtrend HA VERDE streak={_ha_streak_pol}: "
                        f"sizing dimezzato"
                    )

            if decision.get('voto', 0) <= 0:
                decision['direzione'] = "FLAT"
                decision['razionale'] += " | 🛑 Voto insufficiente dopo filtri policy"

            # ── 5. Volatility-Adjusted Sizing ─────────────────────────────────────
            entry_price = float(dati_engine.get('close', 0))
            atr = float(dati_engine.get('atr', 0))
            vol_perc = (atr / entry_price * 100) if entry_price > 0 else 2.0
            vol_factor = 2.0 / vol_perc if vol_perc > 0 else 1.0
            vol_factor = max(0.3, min(1.2, vol_factor))

            # ── 6. Performance-based Sizing ───────────────────────────────────────
            edge = (win_rate / 100.0) - (1.0 - (win_rate / 100.0))
            perf_factor = max(0.5, min(1.5, 1.0 + edge))

            current_sizing = float(decision.get('sizing', 0.15))
            final_sizing = current_sizing * vol_factor * perf_factor
            decision['sizing'] = round(final_sizing, 5)

            if vol_factor < 0.8:
                decision['razionale'] += f" | 📉 Vol-Adjust: sizing ridotto per alta volatilità ({vol_perc:.1f}%)"

        except Exception as e:
            _err.capture(e, "unknown", {"module": "BrainLA"})
            self.logger.error(f"Errore _policy_adjust: {e}")
            decision['sizing'] = 0.10
        return decision
        
    def _apply_prior(self, asset_name, decision):
        prior = self.feedback_engine.get_prior_signal(asset_name)
        if not prior: return decision
        diff = abs(decision.get('voto', 0) - prior['prior_voto'])

        if diff >= 4:
            self.logger.warning(f"⚠️ [NIGHT REVIEW DISAGREE] Prior RF in forte disaccordo ({prior['prior_voto']}). Forzo FLAT per sicurezza.")
            decision['direzione'] = "FLAT"
            decision['voto'] = 0
            decision['razionale'] += f" | 🛑 Prior RF Conflict ({prior['prior_voto']})"
            return decision

        if diff >= 2:
            decision['sizing'] = round(decision.get('sizing', 0) * 0.5, 5)
            decision['razionale'] += f" | prior RF disagrees ({prior['prior_voto']}) sizing 50%"
        elif diff <= 1 and prior['prior_conf'] > 0.55:
            decision['sizing'] = min(1.0, round(decision.get('sizing', 0) * 1.2, 5))
            decision['voto'] = int(round((decision.get('voto', 0) + prior['prior_voto']) / 2))
            decision['razionale'] += f" | prior RF aligned ({prior['prior_voto']}) sizing +20%"
        return decision

    def _audit_dati_tecnici(self, ticker, d):
        """
        Esegue un controllo di integrità logica sui dati tecnici (Sanity Check).
        Rileva incoerenze tra indicatori che dovrebbero essere correlati.
        """
        anomalie = []
        
        price = d.get('close', 0)
        vwap = d.get('vwap', price)
        z_score = d.get('z_score', 0)
        hurst = d.get('hurst_exponent', 0.5)
        ker = d.get('kaufman_efficiency', 0.5)
        dist_s = d.get('dist_supporto', 999)
        press_s = d.get('pressione_muro_supporto', 0)
        dist_r = d.get('dist_resistenza', 999)
        press_r = d.get('pressione_muro_resistenza', 0)
        vel = d.get('price_velocity', 0)
        vpin = d.get('vpin', 0)
        ofi = d.get('order_flow_imbalance', 0)
        agg_flow = d.get('aggressivita_order_flow', 'NEUTRAL')
        f_rate = d.get('funding_rate', 0)
        f_z = d.get('funding_z_score', 0)

        # 1. Incoerenza Prezzo vs Z-Score (Posizione relativa)
        if vwap > 0:
            if (price > vwap * 1.005 and z_score < -3.0) or (price < vwap * 0.995 and z_score > 3.0):
                anomalie.append(f"Z-Score ({z_score}) estremo ed incoerente con posizione rispetto a VWAP (Prezzo: {price}, VWAP: {vwap:.2f})")

        # 2. Incoerenza Regime (Hurst vs Kaufman)
        if hurst == 0.0:
            anomalie.append("Hurst Exponent a 0.0: Errore di calcolo o dati storici insufficienti per definire il regime")
        elif hurst > 0.75 and ker < 0.15:
            anomalie.append(f"Hurst ({hurst}) indica Trend estremo ma Kaufman ({ker}) indica puro rumore")
        elif hurst < 0.25 and ker > 0.7:
            anomalie.append(f"Hurst ({hurst}) indica Mean Reversion estrema ma Kaufman ({ker}) indica Trend lineare")

        # 3. Incoerenza Liquidità (Distanza vs Pressione)
        if dist_s < 0.5 and press_s == 0 and vel < -0.0005:
            anomalie.append(f"Prezzo vicino a Supporto ({dist_s}%) con velocity negativa ma Pressione Muro è 0.0 (Dato mancante o muro ignorato?)")
        if dist_r < 0.5 and press_r == 0 and vel > 0.0005:
            anomalie.append(f"Prezzo vicino a Resistenza ({dist_r}%) con velocity positiva ma Pressione Muro è 0.0 (Dato mancante o muro ignorato?)")

        # 4. Incoerenza Flusso (Velocity vs VPIN vs OFI)
        if abs(vel) > 0.0015 and vpin < 0.05:
            anomalie.append(f"Alta Velocity ({vel}) con VPIN quasi nullo ({vpin}): possibile errore feed trade/volumi")
        
        if (agg_flow == "BUYERS" and ofi < -0.8) or (agg_flow == "SELLERS" and ofi > 0.8):
            self.logger.info(
                f"⚙️ [{ticker}] Possibile accumulo/distribuzione istituzionale: "
                f"Aggressività {agg_flow} con OFI opposto ({ofi:.3f}) — pattern normale in accumulo. NON bloccante."
            )

        # 5. Incoerenza Statistica (Funding) - Rilassato per evitare blocchi in startup
        if f_rate != 0 and f_rate == f_z and abs(f_rate) > 0.0001:
            anomalie.append("Funding Rate e Funding Z-Score identici: Possibile errore statistico (Buffer non popolato?)")

        if anomalie:
            # Grace period di 10 minuti all'avvio: le anomalie sono solo warning, non bloccano
            is_grace_period = (time.time() - self.boot_time) < 600
            for a in anomalie:
                msg = f"🧠 [AUDIT CERVELLO] {ticker}: {a}"
                self.logger.warning(msg)
                if self.alerts:
                    status_msg = "⚠️ *ANOMALIA DATI (WARNING)*" if is_grace_period else "⚠️ *ANOMALIA DATI RILEVATA*"
                    footer = "_Operazione consentita (Grace Period)_" if is_grace_period else "_Operazione bloccata per sicurezza._"
                    self.alerts.invia_alert(f"{status_msg}\nAsset: {ticker}\n{a}\n\n{footer}")
            
            if is_grace_period:
                return True, [] # In grace period, facciamo passare tutto come warning
            return False, anomalie
        
        return True, []

    def _audit_ragionamento_ia(self, ticker, decision, dati_engine):
        """
        Analizza la coerenza logica della risposta dell'IA.
        Rileva dissonanze tra ragionamento, voti e direzione.
        """
        anomalie = []
        direzione = decision.get("direzione", "FLAT")
        voto = decision.get("voto", 0)
        breakdown = decision.get("score_breakdown", {})
        cot = decision.get("catena_di_pensiero", "").upper()
        razionale = decision.get("razionale", "").upper()

        if direzione == "FLAT":
            return True, []

        # 1. Coerenza Matematica (Voto vs Breakdown)
        if breakdown:
            media_voti = sum(breakdown.values()) / len(breakdown)
            if abs(voto - media_voti) > 3:
                anomalie.append(f"Dissonanza Matematica: Voto globale {voto} incoerente con media breakdown ({media_voti:.1f})")

        # 2. Coerenza Semantica (Direzione vs Sentiment nel testo)
        parole_bearish = ["BEARISH", "CROLLO", "DEBOLE", "RESISTENZA", "DUMP", "DISTRIBUZIONE", "VENDITA"]
        parole_bullish = ["BULLISH", "PUMP", "FORTE", "SUPPORTO", "ACCUMULAZIONE", "ACQUISTO", "BREAKOUT"]
        
        # FIX 2026-05-04: includo anche le 3 narrative orizzonti (intraday/daily/strutturale)
        # nel calcolo del sentiment, oltre a catena_di_pensiero e razionale. Prima erano
        # ignorate quindi un Gemini che metteva tutto in narrativa_intraday "il prezzo crolla"
        # ma diceva direzione=LONG passava il check.
        _narratives_join = " ".join([
            str(decision.get("narrativa_intraday", "") or ""),
            str(decision.get("narrativa_daily", "") or ""),
            str(decision.get("narrativa_strutturale", "") or ""),
            str(decision.get("ragionamento_decisione", "") or ""),
        ]).upper()
        
        count_bear = sum(1 for p in parole_bearish if p in cot or p in razionale or p in _narratives_join)
        count_bull = sum(1 for p in parole_bullish if p in cot or p in razionale or p in _narratives_join)

        if direzione == "LONG" and count_bear > count_bull + 2:
            anomalie.append(f"Dissonanza Semantica: Decisione LONG ma ragionamento prevalentemente BEARISH ({count_bear} segnali negativi)")
        if direzione == "SHORT" and count_bull > count_bear + 2:
            anomalie.append(f"Dissonanza Semantica: Decisione SHORT ma ragionamento prevalentemente BULLISH ({count_bull} segnali positivi)")

        # 3. Anti-Allucinazione (Dati citati vs Dati Reali)
        vpin_reale = dati_engine.get('vpin', 0)
        if "VPIN ALTO" in razionale and vpin_reale < 0.3:
            anomalie.append(f"Allucinazione Dati: IA cita 'VPIN Alto' ma valore reale è {vpin_reale:.2f}")
        if "VPIN BASSO" in razionale and vpin_reale > 0.7:
            anomalie.append(f"Allucinazione Dati: IA cita 'VPIN Basso' ma valore reale è {vpin_reale:.2f}")

        # 4. Controllo Divergenze Critiche (TRAPPOLE)
        # Soglia alzata: CVD negativo moderato è normale in accumulo istituzionale
        cvd = dati_engine.get('cvd_istantaneo', 0)
        velocity = dati_engine.get('price_velocity', 0)
        if direzione == "LONG" and velocity > 0 and cvd < -50000:
            anomalie.append("CRITICAL: LONG suggerito con divergenza CVD negativa estrema (BULL TRAP)")
        elif direzione == "LONG" and velocity > 0 and cvd < -10000:
            anomalie.append(f"Attenzione: CVD negativo ({cvd:.0f}) in contesto LONG — possibile accumulo istituzionale")
        if direzione == "SHORT" and velocity < 0 and cvd > 50000:
            anomalie.append("CRITICAL: SHORT suggerito con divergenza CVD positiva estrema (BEAR TRAP)")
        elif direzione == "SHORT" and velocity < 0 and cvd > 10000:
            anomalie.append(f"Attenzione: CVD positivo ({cvd:.0f}) in contesto SHORT — possibile distribuzione")

        # 5. Coerenza Narrative Multi-Orizzonte (Lavoro 3 — 2026-05-04)
        # Gemini produce 3 narrative su 3 orizzonti (intraday, daily, strutturale)
        # e dichiara `coerenza_orizzonti = ALLINEATE | INTRADAY_DIVERGE | ...`.
        # Verifico empiricamente che le narrative siano coerenti col valore dichiarato:
        # se Gemini scrive "intraday LONG, daily SHORT" ma poi dice ALLINEATE, è un bug.
        # Per voto >= 7 questa anomalia non deve passare.
        try:
            n_intraday = str(decision.get('narrativa_intraday', '') or '').upper()
            n_daily = str(decision.get('narrativa_daily', '') or '').upper()
            n_struct = str(decision.get('narrativa_strutturale', '') or '').upper()
            coerenza_dichiarata = str(decision.get('coerenza_orizzonti', '') or '').upper()

            def _sentiment_score(testo):
                """Ritorna +1 (bullish) / -1 (bearish) / 0 (neutro)
                basato su conteggio semplice di parole-segnale."""
                if not testo or len(testo) < 20:
                    return 0
                bull = sum(1 for p in parole_bullish if p in testo)
                bear = sum(1 for p in parole_bearish if p in testo)
                # Se differenza significativa
                if bull >= bear + 2:
                    return 1
                elif bear >= bull + 2:
                    return -1
                return 0

            # Calcola solo se ci sono almeno 2 narrative non vuote
            narrative_presenti = [n for n in (n_intraday, n_daily, n_struct) if len(n) >= 20]
            if len(narrative_presenti) >= 2:
                s_intraday = _sentiment_score(n_intraday)
                s_daily = _sentiment_score(n_daily)
                s_struct = _sentiment_score(n_struct)

                # Quante narrative concordano con direzione?
                # LONG vuole +1 su tutte, SHORT vuole -1 su tutte
                segno_atteso = 1 if direzione == "LONG" else -1
                divergenze_real = []
                if s_intraday != 0 and s_intraday != segno_atteso:
                    divergenze_real.append("INTRADAY")
                if s_daily != 0 and s_daily != segno_atteso:
                    divergenze_real.append("DAILY")
                if s_struct != 0 and s_struct != segno_atteso:
                    divergenze_real.append("STRUTTURALE")

                # Caso 1: Gemini dice ALLINEATE ma in realtà ci sono divergenze
                if coerenza_dichiarata == "ALLINEATE" and divergenze_real:
                    anomalie.append(
                        f"Divergenza Orizzonti Non Dichiarata: Gemini dichiara ALLINEATE "
                        f"ma narrative {divergenze_real} divergono semanticamente da {direzione}"
                    )
                # Caso 2: voto >= 7 ma in realtà ci sono divergenze multiple
                #         (anche se Gemini le ha dichiarate, voto alto non è giustificato)
                elif voto >= 7 and len(divergenze_real) >= 2:
                    anomalie.append(
                        f"Voto Alto su Multi-Divergenza: voto {voto} ma {len(divergenze_real)} "
                        f"orizzonti su 3 divergono ({divergenze_real}) — voto sopravvalutato"
                    )
        except Exception as _e_narr:
            self.logger.debug(f"audit narrative {ticker}: {_e_narr}")

        if anomalie:
            for a in anomalie:
                msg = f"🧠 [AUDIT LOGICA IA] {ticker}: {a}"
                self.logger.warning(msg)
                if self.alerts:
                    self.alerts.invia_alert(f"🧠 *DISSONANZA LOGICA IA*\nAsset: {ticker}\n{a}\n\n_Voto e Sizing ridotti automaticamente._")
            return False, anomalie
        
        return True, []

    # ════════════════════════════════════════════════════════════════════════
    # CLASSIFIER LOCALE — fallback durante quota Gemini cooldown (Lavoro 4)
    # ════════════════════════════════════════════════════════════════════════
    def _classifier_locale_fallback(self, ticker, dati_engine):
        """
        Classifier deterministico locale che opera quando Gemini è in quota
        cooldown (180s post-429). Restituisce un dict compatibile con lo schema
        di full_global_strategy.
        
        Aggiunto 2026-05-04 — Lavoro 4 della roadmap "voto 10".
        Filosofia:
        - NON sostituisce Gemini. Lo COPRE solo durante il blackout.
        - Voto MAX 7 (mai 8+, riservati a Gemini con calibration check completo).
        - Sizing ridotto del 50% (precauzione).
        - Combina 4 segnali tecnici con peso uguale, voto = media.
        - In dubbio → FLAT, MAI prendere rischi senza Gemini.
        
        Segnali pesati:
        1. Trend Hurst+HA daily (40%): direzione strutturale
        2. Order Flow CVD+OFI (30%): pressione real-time del mercato
        3. Microstruttura VPIN+velocity (20%): qualità del flow
        4. Pressione muri book (10%): livelli S/R con domanda/offerta
        
        Args:
            ticker: identificativo asset
            dati_engine: dict con dati di mercato (close, cvd, vpin, hurst, ecc)
        
        Returns:
            dict compatibile con schema Gemini, marcato `fonte=local_fallback`.
        """
        try:
            # Estraggo dati
            close = float(dati_engine.get('close', 0) or 0)
            atr = float(dati_engine.get('atr', 0) or 0)
            cvd_ist = float(dati_engine.get('cvd_istantaneo', 0) or 0)
            cvd_30s = float(dati_engine.get('cvd_delta_30s', 0) or 0)
            cvd_120s = float(dati_engine.get('cvd_delta_120s', 0) or 0)
            vpin = float(dati_engine.get('vpin', 0.5) or 0.5)
            ofi = float(dati_engine.get('order_flow_imbalance', 0) or 0)
            hurst = float(dati_engine.get('hurst_exponent', 0.5) or 0.5)
            kaufman = float(dati_engine.get('kaufman_efficiency', 0.5) or 0.5)
            ha_col = str(dati_engine.get('ha_daily_colore', '?') or '?').upper()
            ha_streak = int(dati_engine.get('ha_daily_streak', 0) or 0)
            velocity = float(dati_engine.get('price_velocity', 0) or 0)
            muro_supp = float(dati_engine.get('muro_supporto', 0) or 0)
            muro_res = float(dati_engine.get('muro_resistenza', 0) or 0)
            press_supp = float(dati_engine.get('pressione_muro_supporto', 0) or 0)
            press_res = float(dati_engine.get('pressione_muro_resistenza', 0) or 0)

            # Funzione FLAT default per uscita rapida
            def _flat_response(motivo):
                return {
                    "direzione": "FLAT", "voto": 0,
                    "sizing": 0, "leverage": 1, "sl": 0, "tp": 0,
                    "stile_operativo": "SWING", "timeframe_riferimento": "5m",
                    "stop_logico": 0, "target_logico": 0,
                    "score_breakdown": {
                        "Order_Flow": 0, "Liquidity": 0, "Market_Regime": 0,
                        "Velocity": 0, "Volatility": 0
                    },
                    "ragionamento_decisione": f"[LOCAL FALLBACK] FLAT: {motivo}",
                    "razionale": f"local_fallback_flat",
                    "apprendimento_critico": f"[auto-local] {motivo}",
                    "fonte": "local_fallback",
                    "narrativa_intraday": "Gemini non disponibile, attendo recupero quota.",
                    "narrativa_daily": "Gemini non disponibile, attendo recupero quota.",
                    "narrativa_strutturale": "Gemini non disponibile, attendo recupero quota.",
                    "coerenza_orizzonti": "TUTTE_DIVERSE",
                    "condizioni_tesi": {},
                }

            if close <= 0:
                return _flat_response("dati prezzo non disponibili")

            # ──────────────────────────────────────────────────────────────
            # SEGNALE 1: Trend Hurst + HA daily streak (peso 40%)
            # ──────────────────────────────────────────────────────────────
            trend_score = 0  # -10..+10 dove + = LONG, - = SHORT
            if hurst > 0.55:
                # Mercato trending — segui HA daily
                if ha_col in ('VERDE',) and ha_streak >= 3:
                    trend_score = +6 + min(ha_streak - 3, 3)  # max +9 con streak 6+
                elif ha_col in ('ROSSO',) and ha_streak >= 3:
                    trend_score = -6 - min(ha_streak - 3, 3)
                elif ha_streak >= 2:
                    trend_score = +3 if ha_col == 'VERDE' else -3
            elif hurst < 0.45:
                # Mercato mean-reverting — niente trend, score basso
                trend_score = 0

            # ──────────────────────────────────────────────────────────────
            # SEGNALE 2: Order Flow (CVD acceleration + OFI) (peso 30%)
            # ──────────────────────────────────────────────────────────────
            of_score = 0
            # CVD 120s significativo (lungo orizzonte)
            cvd_thr = 1500.0
            if cvd_120s > cvd_thr * 3:    # forte accelerazione bull
                of_score += 6
            elif cvd_120s > cvd_thr:
                of_score += 3
            elif cvd_120s < -cvd_thr * 3:
                of_score -= 6
            elif cvd_120s < -cvd_thr:
                of_score -= 3
            # OFI (book imbalance)
            if ofi > 0.5:
                of_score += 3
            elif ofi > 0.2:
                of_score += 1
            elif ofi < -0.5:
                of_score -= 3
            elif ofi < -0.2:
                of_score -= 1
            of_score = max(-10, min(10, of_score))

            # ──────────────────────────────────────────────────────────────
            # SEGNALE 3: Microstruttura VPIN + velocity (peso 20%)
            # ──────────────────────────────────────────────────────────────
            micro_score = 0
            # Velocity con direzione coerente
            vel_thr = max(0.0001, atr / close * 0.001) if close > 0 else 0.0001
            if velocity > vel_thr * 3:
                micro_score += 4
            elif velocity > vel_thr:
                micro_score += 2
            elif velocity < -vel_thr * 3:
                micro_score -= 4
            elif velocity < -vel_thr:
                micro_score -= 2
            # VPIN: alto VPIN amplifica il segnale di velocity (toxic flow direzionale)
            if vpin > 0.7 and abs(micro_score) > 0:
                micro_score = int(micro_score * 1.3)
            # Kaufman: bassa efficienza → riduce score (rumore puro)
            if kaufman < 0.25:
                micro_score = int(micro_score * 0.5)
            micro_score = max(-10, min(10, micro_score))

            # ──────────────────────────────────────────────────────────────
            # SEGNALE 4: Pressione muri book (peso 10%)
            # ──────────────────────────────────────────────────────────────
            book_score = 0
            if press_supp > 0 and press_res > 0:
                ratio = press_supp / press_res
                if ratio > 2.0:
                    book_score = +3   # supporto domina → bias LONG
                elif ratio > 1.3:
                    book_score = +1
                elif ratio < 0.5:
                    book_score = -3   # resistenza domina → bias SHORT
                elif ratio < 0.77:
                    book_score = -1

            # ──────────────────────────────────────────────────────────────
            # COMBINA: media pesata
            # ──────────────────────────────────────────────────────────────
            score_finale = (
                trend_score * 0.40 +
                of_score    * 0.30 +
                micro_score * 0.20 +
                book_score  * 0.10
            )
            # score_finale è in range [-10, +10]

            # ──────────────────────────────────────────────────────────────
            # VERIFICA COERENZA: i 4 segnali devono concordare almeno parzialmente
            # ──────────────────────────────────────────────────────────────
            segni = [
                1 if trend_score > 1 else (-1 if trend_score < -1 else 0),
                1 if of_score > 1 else (-1 if of_score < -1 else 0),
                1 if micro_score > 1 else (-1 if micro_score < -1 else 0),
                1 if book_score > 1 else (-1 if book_score < -1 else 0),
            ]
            n_long = sum(1 for s in segni if s == 1)
            n_short = sum(1 for s in segni if s == -1)
            n_neut = sum(1 for s in segni if s == 0)

            # Se segni completamente conflitto (es. 2 LONG + 2 SHORT) → FLAT
            if n_long >= 2 and n_short >= 2:
                return _flat_response(
                    f"segnali in conflitto (long={n_long} short={n_short}) — Gemini cooldown"
                )

            # Se troppi neutri (3+) → FLAT (segnale debole)
            if n_neut >= 3:
                return _flat_response(
                    f"segnali deboli ({n_neut} neutri) — Gemini cooldown"
                )

            # ──────────────────────────────────────────────────────────────
            # MAPPA score_finale a direzione + voto
            # ──────────────────────────────────────────────────────────────
            # Soglie conservative: solo se score abbastanza forte do voto >= 6
            if abs(score_finale) < 2.5:
                return _flat_response(
                    f"score finale {score_finale:+.1f} sotto soglia 2.5 — Gemini cooldown"
                )

            direzione = "LONG" if score_finale > 0 else "SHORT"
            # Mappa: |score| 2.5 → voto 5; 4 → voto 6; 5.5 → voto 7 (max)
            voto_local = min(7, int(4 + abs(score_finale) * 0.55))
            voto_local = max(5, voto_local)  # min 5 se decidiamo non-FLAT

            # Verifica anti-trappola: SHORT con velocity positiva forte → trappola, FLAT
            if direzione == "SHORT" and velocity > vel_thr * 5:
                return _flat_response(
                    "SHORT contro velocity positiva estrema — possibile bear trap"
                )
            if direzione == "LONG" and velocity < -vel_thr * 5:
                return _flat_response(
                    "LONG contro velocity negativa estrema — possibile bull trap"
                )

            # ──────────────────────────────────────────────────────────────
            # Calcola SL e TP basici dal close + ATR
            # ──────────────────────────────────────────────────────────────
            atr_eff = atr if atr > 0 else close * 0.01  # 1% se ATR mancante
            if direzione == "LONG":
                sl = round(close - atr_eff * 1.5, 8)
                tp = round(close + atr_eff * 2.5, 8)
            else:
                sl = round(close + atr_eff * 1.5, 8)
                tp = round(close - atr_eff * 2.5, 8)

            # Score breakdown approssimato (per audit a valle)
            score_bd = {
                "Order_Flow":    max(0, min(10, int(5 + of_score * 0.5))),
                "Liquidity":     max(0, min(10, int(5 + book_score * 0.5))),
                "Market_Regime": max(0, min(10, int(5 + trend_score * 0.5))),
                "Velocity":      max(0, min(10, int(5 + micro_score * 0.5))),
                "Volatility":    int(5 + (vpin - 0.5) * 5),
            }

            # Condizioni tesi standard (per Watchdog v12)
            if direzione == "LONG":
                cond = [
                    {"campo": "cvd_delta_120s", "operatore": "<", "valore": -cvd_thr,
                     "descrizione": "CVD si inverte negativo"},
                    {"campo": "price_velocity", "operatore": "<", "valore": -vel_thr,
                     "descrizione": "velocity ribassista"},
                ]
            else:
                cond = [
                    {"campo": "cvd_delta_120s", "operatore": ">", "valore": cvd_thr,
                     "descrizione": "CVD si inverte positivo"},
                    {"campo": "price_velocity", "operatore": ">", "valore": vel_thr,
                     "descrizione": "velocity rialzista"},
                ]

            motivazione = (
                f"[LOCAL FALLBACK] {direzione} score {score_finale:+.1f} | "
                f"trend={trend_score:+d} OF={of_score:+d} micro={micro_score:+d} book={book_score:+d}"
            )
            self.logger.info(
                f"🛟 [{ticker}] Classifier locale (Gemini cooldown): "
                f"{direzione} voto {voto_local} score {score_finale:+.2f}"
            )

            return {
                "direzione": direzione,
                "voto": voto_local,
                "sizing": 0.5,           # ridotto del 50% rispetto a default 1.0
                "leverage": 3,            # leverage moderato (no max)
                "sl": sl, "tp": tp,
                "stop_logico": sl, "target_logico": tp,
                "stile_operativo": "MOMENTUM",
                "timeframe_riferimento": "15m",
                "score_breakdown": score_bd,
                "ragionamento_decisione": motivazione,
                "razionale": f"local_fallback_{direzione.lower()}",
                "apprendimento_critico": (
                    f"[auto-local] Classifier locale: {direzione} voto {voto_local}, "
                    f"trend_hurst={trend_score:+d}, of_cvd={of_score:+d}, "
                    f"micro={micro_score:+d}, book={book_score:+d}"
                ),
                "fonte": "local_fallback",
                "narrativa_intraday": f"Classifier locale: {direzione} per momentum {of_score:+d}.",
                "narrativa_daily": f"Trend Hurst {hurst:.2f}, HA {ha_col} streak {ha_streak}.",
                "narrativa_strutturale": f"Pressione book ratio {book_score:+d}.",
                "coerenza_orizzonti": "ALLINEATE" if (n_long >= 3 or n_short >= 3) else "INTRADAY_DIVERGE",
                "condizioni_tesi": {"invalidata_se_diventa_vero": cond,
                                    "fonte": "fallback_deterministico_v1"},
            }

        except Exception as e:
            _err.capture(e, "_classifier_locale_fallback", {"module": "BrainLA", "ticker": ticker})
            self.logger.error(f"❌ Classifier locale fallito {ticker}: {e}")
            return {
                "direzione": "FLAT", "voto": 0, "sizing": 0, "leverage": 1, "sl": 0, "tp": 0,
                "stile_operativo": "SWING", "timeframe_riferimento": "none",
                "score_breakdown": {"Order_Flow": 0, "Liquidity": 0, "Market_Regime": 0,
                                    "Velocity": 0, "Volatility": 0},
                "ragionamento_decisione": f"local_fallback_error: {e}",
                "razionale": "local_fallback_error",
                "apprendimento_critico": "[auto-local] errore classifier",
                "fonte": "local_fallback_error",
            }

    def _validate_statistical_edge(self, asset, dati, direzione):
        """
        Valida se esiste un vantaggio statistico reale per l'operazione.
        """
        try:
            close = float(dati.get('close', 0))
            vwap = float(dati.get('vwap', 0))
            val = float(dati.get('val', 0))
            vah = float(dati.get('vah', 0))
            hurst = float(dati.get('hurst', 0.5))
            
            # 1. Filtro Trend vs Posizione VWAP
            if hurst > 0.55: # Trend
                if direzione == "LONG" and close < vwap * 0.99:
                    return False, "❌ Trend rialzista ma prezzo troppo sotto VWAP (Rischio debolezza)"
                if direzione == "SHORT" and close > vwap * 1.01:
                    return False, "❌ Trend ribassista ma prezzo troppo sopra VWAP (Rischio forza)"
            
            # 2. Filtro Value Area (Volume Profile)
            if hurst < 0.45: # Mean Reversion
                if direzione == "LONG" and close > vah:
                    return False, "❌ Mean Reversion LONG ma prezzo sopra VAH (Iper-esteso)"
                if direzione == "SHORT" and close < val:
                    return False, "❌ Mean Reversion SHORT ma prezzo sotto VAL (Iper-esteso)"
                    
            return True, "✅ Edge statistico confermato."
        except Exception as e:
            _err.capture(e, "_validate_statistical_edge", {"module": "BrainLA", "asset": asset, "direzione": direzione}, level="WARNING")
            return True, f"⚠️ Errore validazione edge: {e}"

    def full_global_strategy(self, dati_engine, asset_name, macro_sentiment, performance_history=None):
        from core.asset_list import get_ticker
        import json
        ticker_ufficiale = get_ticker(asset_name)

        entry_price = float(dati_engine.get('close', 0))
        atr = float(dati_engine.get('atr', 0))
        atr_p = (atr / entry_price * 100) if entry_price > 0 else 0
        
        # Correzione chiavi per allineamento con EngineLA
        health_data = dati_engine.get('health_data', {})
        market_health = health_data.get('market_health_index', 0.5)
        
        spread = float(dati_engine.get('spread', 0)) 
        spread_p = (spread / entry_price * 100) if entry_price > 0 else float(dati_engine.get('spread_perc', 0))

        vpin = float(dati_engine.get('vpin', 0))
        vpin_delta = float(dati_engine.get('vpin_trend', 0))
        hurst = float(dati_engine.get('hurst_exponent', 0.5))
        z_score = float(dati_engine.get('z_score', 0))
        regime_hmm = dati_engine.get('market_regime', 'UNKNOWN')
        market_regime = regime_hmm
        market_efficiency = float(dati_engine.get('kaufman_efficiency', 1.0))
        
        cvd_istantaneo = float(dati_engine.get('cvd_istantaneo', 0.0))
        prev_vah = float(dati_engine.get('vah_ieri', dati_engine.get('vah', 0)))
        prev_val = float(dati_engine.get("prev_val", 0))
        price_velocity = float(dati_engine.get('price_velocity', 0.0))
        is_explosive = bool(dati_engine.get('is_explosive', False))
        iceberg = bool(dati_engine.get('iceberg_presenti', False))
        spoofing = float(dati_engine.get('indice_spoofing', 0.0))
        cvd_divergence = float(dati_engine.get('cvd_divergence', 1.0))
        ofi = float(dati_engine.get('order_flow_imbalance', 0.0))
        whale_delta = float(dati_engine.get('whale_delta', 0.0))
        delta_footprint = float(dati_engine.get('delta_footprint', 0.0))
        rolling_vol = float(dati_engine.get('rolling_volatility', 0))
        
        macro_proxy = dati_engine.get('macro_proxy', {})
        rvol = float(macro_proxy.get('relative_volume_status', 1.0))
        
        bp = float(dati_engine.get('book_pressure', 1.0))
        funding_z = float(dati_engine.get('funding_z_score', 0))
        rsi = float(dati_engine.get('rsi', 50))
        mac_d = dati_engine.get('mac_d', 0)
        book_delta = float(dati_engine.get('book_delta', 0))
        funding_actual = float(dati_engine.get('actual_funding_rate', 0))
        liquidazioni_24h = float(dati_engine.get('liquidazioni_24h', 0))
        open_interest = float(dati_engine.get('open_interest', 0))
        level_stability = float(dati_engine.get('level_stability_index', 1.0))
        book_skew = float(dati_engine.get('book_skewness', 0))
        vol_imbalance = float(dati_engine.get('volume_imbalance', 1.0))
        corr_index = float(dati_engine.get('correlation_with_market', 1.0))
        gap_liquidita = bool(dati_engine.get('liquidity_gap', False))        
        aggressivita_flow = dati_engine.get('aggressivita_order_flow', 'NEUTRAL')
        signal_age = float(dati_engine.get('seconds_since_update', 0))
        vwap = float(dati_engine.get('vwap', entry_price))
        
        poc = float(dati_engine.get('poc', 0))
        vah = float(dati_engine.get('vah', 0))
        val = float(dati_engine.get('val', 0))
        hvn = dati_engine.get('high_volume_nodes', [])
        lvn = dati_engine.get('low_volume_nodes', [])

        buy_imb = dati_engine.get('buy_imbalance_levels', [])
        sell_imb = dati_engine.get('sell_imbalance_levels', [])
        voids_supp = dati_engine.get('liquidity_voids_supporto', [])
        voids_res = dati_engine.get('liquidity_voids_resistenza', [])
        prob_return = float(dati_engine.get('prob_ritorno_vwap', 50))
        z_dist_vwap = float(dati_engine.get('z_score_dist_vwap', 0))
        muro_supporto_prezzo = float(dati_engine.get('muro_supporto', 0.0))
        muro_resistenza_prezzo = float(dati_engine.get('muro_resistenza', 0.0))
        dist_supporto = float(dati_engine.get('dist_supporto', 999.0))
        dist_resistenza = float(dati_engine.get('dist_resistenza', 999.0))
        latency = float(dati_engine.get('kraken_latency', 0))
        
        corr_driver = float(dati_engine.get('correlazione_driver', 0))
        press_supp = float(dati_engine.get('pressione_muro_supporto', 0))
        press_res = float(dati_engine.get('pressione_muro_resistenza', 0))
        
        quadro_4h = {
            'trend_primario': 'BULLISH' if hurst > 0.55 and z_score > 0 else 'BEARISH' if hurst > 0.55 and z_score < 0 else 'CONSOLIDAMENTO',
            'volatilita_macro': round(atr_p, 2),
            'regime_hmm': regime_hmm
        }

        mappa_1h = {
            'muro_supporto_h1': muro_supporto_prezzo,
            'muro_resistenza_h1': muro_resistenza_prezzo,
            'distanza_supporto_h1_perc': round(dist_supporto, 2),
            'distanza_resistenza_h1_perc': round(dist_resistenza, 2),
            'vwap_distanza_perc': round((entry_price - vwap) / vwap * 100, 3) if vwap != 0 else 0
        }

        livelli_15m = {
            'area_valore': {'vah': vah, 'val': val, 'poc': poc},
            'pressione_book': bp,
            'delta_volumi': vol_imbalance,
            'efficienza_kaufman': market_efficiency
        }

        trigger_flusso_istantaneo = {
            'cvd_istantaneo': cvd_istantaneo,
            'price_velocity': price_velocity,
            'aggressivita_flow': aggressivita_flow,
            'vpin_tossicita': vpin
        }

        trend_macro = quadro_4h['trend_primario']
        
        # --- 1. RILEVAMENTO TOXIC FLOW (VPIN Avanzato) ---
        is_toxic = dati_engine.get('is_toxic', False)
        
        # --- 1.1 DATA INTEGRITY CHECK (Audit Cervello) ---
        is_feed_incomplete = (funding_actual == 0 and liquidazioni_24h == 0 and open_interest == 0)
        
        ok_audit, lista_anomalie = self._audit_dati_tecnici(ticker_ufficiale, dati_engine)
        _penalita_audit = 0
        if not ok_audit:
            self.logger.warning(f"⚠️ [{ticker_ufficiale}] Anomalie dati ({len(lista_anomalie)}) — penalità voto -3. Gemini decide comunque.")
            _penalita_audit = 3
        
        # Controllo incoerenza Z-Score vs VWAP (Audit Fix - Rilassato per asset volatili)
        _penalita_zscore = 0
        if vwap > 0:
            if (entry_price > vwap and z_score < -2.0) or (entry_price < vwap and z_score > 2.0):
                self.logger.warning(f"⚠️ [{ticker_ufficiale}] Incoerenza Z-Score/VWAP — penalità voto -2.")
                _penalita_zscore = 2
        
        # --- 2. ARBITRAGGIO STATISTICO DELLA VELOCITY ---
        # Se la velocity è alta ma il CVD è debole rispetto al volume totale (VPIN basso), è un Fake Breakout
        is_fake_breakout = False
        if abs(price_velocity) > 0.001 and vpin < 0.15: # CVD molto debole (net flow < 15% del volume) con alta velocità
            is_fake_breakout = True
            
        # --- 3. FILTRO CONFLUENZA MULTI-TIMEFRAME — penalità voto, non blocco
        _penalita_confluenza = 0
        _vel_abs = abs(price_velocity)
        _e_momentum = _vel_abs > 0.0003

        if trend_macro == 'BULLISH' and cvd_istantaneo < 0 and vpin > 0.6 and z_score > -2.0 and _e_momentum:
            self.logger.info(f"⚠️ [{ticker_ufficiale}] Confluenza contro macro BULLISH — penalità -2.")
            _penalita_confluenza = 2
        elif trend_macro == 'BEARISH' and cvd_istantaneo > 0 and vpin > 0.6 and z_score < 2.0 and _e_momentum:
            self.logger.info(f"⚠️ [{ticker_ufficiale}] Confluenza contro macro BEARISH — penalità -2.")
            _penalita_confluenza = 2
        if trend_macro == 'BULLISH' and price_velocity < -0.5:
            self.logger.info(f"⚠️ [{ticker_ufficiale}] Velocity crash — penalità -2.")
            _penalita_confluenza = max(_penalita_confluenza, 2)
        if trend_macro == 'BEARISH' and price_velocity > 0.5:
            self.logger.info(f"⚠️ [{ticker_ufficiale}] Velocity squeeze — penalità -2.")
            _penalita_confluenza = max(_penalita_confluenza, 2)

        # Inizializza esaurimento (sarà usato dopo Gemini)
        _penalita_esaurimento = 0
        # Variabile per il calcolo score proporzionale (valorizzata dopo Gemini)
        _extra_score = 0.0


        stats_globali = self.feedback_engine.get_stats_globali() if hasattr(self.feedback_engine, 'get_stats_globali') else {}
        report_lezioni = self.feedback_engine.get_feedback_summary(ticker_ufficiale)
        lezione_asset = self.feedback_engine.get_lezioni_asset(ticker_ufficiale)
        
        # ── Memoria operativa: framing INFORMATIVO, non imperativo ─────────────
        # Filosofia (2026-05-02): NightReview e statistiche storiche forniscono
        # OSSERVAZIONI sul comportamento passato del bot, non REGOLE da seguire.
        # Gemini riceve queste informazioni come contesto e decide autonomamente
        # se applicarle al setup attuale o ignorarle se il contesto attuale è diverso.
        # Prima qui i testi erano "⚠️ REGOLA DA SEGUIRE", "Evita questi pattern" —
        # framing imperativo che trasformava dati in direttive vincolanti.

        # Pattern di perdita ricorrenti (puramente descrittivi)
        error_matrix_str = ""
        if "error_matrix" in stats_globali and ticker_ufficiale in stats_globali["error_matrix"]:
            em = stats_globali["error_matrix"][ticker_ufficiale]
            if em:
                error_matrix_str = (
                    f"📊 PATTERN DI PERDITA OSSERVATI SU {ticker_ufficiale} (per contesto, non vincolanti):\n"
                )
                for cat, count in em.items():
                    error_matrix_str += f"  - {cat}: osservato in {count} trade in perdita\n"
                error_matrix_str += (
                    "  (Valuta se il setup attuale presenta queste condizioni o se è diverso.)\n"
                )

        # Pattern vincenti speculari (simmetria)
        success_matrix_str = ""
        if "success_matrix" in stats_globali and ticker_ufficiale in stats_globali.get("success_matrix", {}):
            sm = stats_globali["success_matrix"][ticker_ufficiale]
            if sm:
                success_matrix_str = (
                    f"✅ PATTERN VINCENTI OSSERVATI SU {ticker_ufficiale} (per contesto):\n"
                )
                for cat, count in sm.items():
                    success_matrix_str += f"  - {cat}: osservato in {count} trade vincenti\n"

        # Sintesi NightReview e regola_domani: presentate come osservazioni
        sintesi_nr = lezione_asset.get('sintesi', '')
        regola_nr  = lezione_asset.get('regola_domani', '')

        lezione_str = ""
        if sintesi_nr:
            lezione_str = f"🧠 OSSERVAZIONE NIGHTREVIEW SU {ticker_ufficiale}: {sintesi_nr}\n"
        if regola_nr:
            lezione_str += (
                f"💡 NOTA dal NightReview (per contesto, non vincolante): {regola_nr}\n"
                f"   (Valuta autonomamente se applicarla al setup attuale.)\n"
            )

        memoria_reale = (
            report_lezioni + "\n"
            + error_matrix_str + "\n"
            + success_matrix_str + "\n"
            + lezione_str
        )
        tech_narrative = self._get_technical_narrative(dati_engine)

        slippage_medio = spread_p * 1.2 if spread_p > 0 else 0.02
        depth_liquidity = dati_engine.get('market_depth', 0)

        from core.asset_list import get_config
        asset_config = get_config(ticker_ufficiale)
        asset_dna = asset_config.get('dna', 'Nessun DNA specifico per questo asset.')

        # Estrazione dati multi-timeframe sicuri per JSON
        # Passiamo ora trend, regime e volume per ogni TF — non solo Hurst
        # ── Analisi strutturale del ciclo ────────────────────────────────────
        _ciclo_fase         = str(dati_engine.get('ciclo_fase', 'SCONOSCIUTO') or 'SCONOSCIUTO')
        _ciclo_recupero     = float(dati_engine.get('ciclo_recupero_pct', 0) or 0)
        _minimo_qualita     = str(dati_engine.get('minimo_qualita', 'NORMALE') or 'NORMALE')
        _minimo_vol_ratio   = float(dati_engine.get('minimo_volume_ratio', 1.0) or 1.0)
        _sr_flip            = bool(dati_engine.get('sr_flip_detected', False))
        _sr_flip_tipo       = str(dati_engine.get('sr_flip_tipo', '') or '')
        _sr_flip_livello    = float(dati_engine.get('sr_flip_livello', 0) or 0)
        _contesto_strutturale = str(dati_engine.get('contesto_strutturale', '') or '')
        # ─────────────────────────────────────────────────────────────────────

        # ── Signal State Engine — contesto di mercato completo ──────────────
        _entry_phase     = str(dati_engine.get('entry_phase', 'FORMAZIONE') or 'FORMAZIONE')
        _phase_subtype   = str(dati_engine.get('phase_subtype', '') or '')
        _exhaustion      = int(dati_engine.get('exhaustion_score', 0) or 0)
        _phase_narrative = str(dati_engine.get('phase_narrative', '') or '')
        _override_ok     = bool(dati_engine.get('phase_override_ok', False))
        _override_cond   = str(dati_engine.get('phase_override_cond', '') or '')
        _cvd_trend       = str(dati_engine.get('cvd_trend', 'PIATTO') or 'PIATTO')
        _cvd_delta_30s   = float(dati_engine.get('cvd_delta_30s', 0) or 0)
        _cvd_delta_120s  = float(dati_engine.get('cvd_delta_120s', 0) or 0)
        _cvd_accel       = float(dati_engine.get('cvd_acceleration', 0) or 0)
        _signal_age      = float(dati_engine.get('signal_age_s', 0) or 0)
        _signal_narrative= _phase_narrative  # usa narrativa SSE
        _short_ok        = bool(dati_engine.get('short_conditions_met', False))
        _short_veto      = str(dati_engine.get('short_veto_motivo', '') or '')
        _contesto_tf     = dati_engine.get('contesto_tf_superiore', {}) or {}
        # ─────────────────────────────────────────────────────────────────────

        multi_tf_raw = dati_engine.get('multi_tf', {})
        multi_tf_safe = {}
        for tf, data in multi_tf_raw.items():
            if isinstance(data, dict):
                multi_tf_safe[tf] = {
                    'hurst':       round(data.get('hurst', 0.5), 3),
                    'trend':       data.get('trend_dir', '?'),        # UP / DOWN
                    'regime':      data.get('regime', '?'),           # TRENDING / MEAN_REVERSION
                    'vol_relativo': round(data.get('vol_relativo', 1.0), 2),
                    'ema20_vs_ema50': 'EMA20>EMA50' if data.get('trend_dir') == 'UP' else 'EMA20<EMA50',
                }

        # --- NARRATIVA STRUTTURALE MULTI-TF ---
        # Riassunto leggibile della posizione dell'asset nella struttura più grande.
        # Gemini riceve testo diretto invece di doverlo dedurre dal JSON.
        _struttura_lines = []
        _tf_labels = {'5m': '5 minuti', '15m': '15 minuti', '1h': '1 ora', '4h': '4 ore', '1d': 'Giornaliero'}

        for tf in ['1d', '4h', '1h', '15m', '5m']:
            if tf not in multi_tf_safe:
                continue
            d_tf = multi_tf_safe[tf]
            trend = d_tf.get('trend','?')
            regime = d_tf.get('regime','?')
            hurst  = d_tf.get('hurst', 0.5)
            vol    = d_tf.get('vol_relativo', 1.0)
            label  = _tf_labels.get(tf, tf)

            trend_emoji  = "📈" if trend == "UP" else "📉"
            regime_short = "trend" if regime == "TRENDING" else "range"
            vol_note     = f", volume {vol:.1f}x media" if vol > 1.5 else (f", volume basso {vol:.1f}x" if vol < 0.7 else "")

            _struttura_lines.append(
                f"  {label}: {trend_emoji} {trend} | {regime_short} | Hurst {hurst:.2f}{vol_note}"
            )

        # Aggiungi struttura H1 da BoS/CHoCH se disponibile
        struttura_h1_bos = str(dati_engine.get('struttura_h1', '')).upper()
        ultimo_choch     = str(dati_engine.get('ultimo_choch', '')).upper()
        ultimo_bos       = str(dati_engine.get('ultimo_bos', '')).upper()
        if struttura_h1_bos:
            _struttura_lines.append(
                f"  Struttura H1 (BoS/CHoCH): {struttura_h1_bos}"
                + (f" | Ultimo CHoCH: {ultimo_choch}" if ultimo_choch and ultimo_choch != 'NESSUNO' else "")
                + (f" | Ultimo BoS: {ultimo_bos}" if ultimo_bos and ultimo_bos != 'NESSUNO' else "")
            )

        # HA daily — fatti grezzi, nessuna interpretazione
        _ha_col    = str(dati_engine.get("ha_daily_colore","")).upper()
        _ha_streak = int(dati_engine.get("ha_daily_streak", 0) or 0)
        _ciclo_fase = str(dati_engine.get("ciclo_fase","") or "")
        _min_qual   = str(dati_engine.get("minimo_qualita","") or "")
        _ciclo_rec  = float(dati_engine.get("ciclo_recupero_pct", 0) or 0)
        _min_90g    = float(dati_engine.get("ciclo_minimo_90g", 0) or 0)
        _max_90g    = float(dati_engine.get("ciclo_massimo_90g", 0) or 0)

        # Sequenza HA ultime 10 candele se disponibile
        _ha_seq = dati_engine.get("ha_daily_sequenza", "") or ""

        _ha_color_emoji = "🟢" if _ha_col == "VERDE" else ("🔴" if _ha_col == "ROSSO" else "⚪")
        _ha_line = (
            f"  HA Daily: {_ha_color_emoji} ultima candela chiusa {_ha_col} streak={_ha_streak}"
            + (f" | sequenza ultime 10: {_ha_seq}" if _ha_seq else "")
        )
        _ciclo_line = (
            f"  Posizione 90g: {_ciclo_rec:.0f}% del range "
            f"(min 90g: {_min_90g:.4f}, max 90g: {_max_90g:.4f}, qualità minimo: {_min_qual or 'n/d'})"
        )

        narrativa_strutturale = (
            "STRUTTURA MULTI-TIMEFRAME (fatti grezzi, nessuna interpretazione):\n"
            + _ha_line + "\n"
            + _ciclo_line + "\n"
            + ("\n".join(_struttura_lines) if _struttura_lines else "  Dati TF non disponibili")
            + "\n"
        )

        # Estrazione nuovi dati arricchiti
        corr_driver = dati_engine.get('correlazione_driver', 1.0)
        press_supp = dati_engine.get('pressione_muro_supporto', 0.0)
        press_res = dati_engine.get('pressione_muro_resistenza', 0.0)

        # --- MAPPA CONTESTUALE DEI LIVELLI GERARCHICI ───────────────────────
        # Risponde alla domanda: dove si trova il prezzo nella struttura?
        # Un muro del 15m che coincide con un livello H4 o weekly è
        # completamente diverso da uno isolato nel vuoto.
        try:
            prezzo_c       = float(dati_engine.get('close', 0))
            poc_w          = float(dati_engine.get('poc', 0))
            vah_w          = float(dati_engine.get('vah', 0))
            val_w          = float(dati_engine.get('val', 0))
            pivot_d        = float(dati_engine.get('pivot_daily', 0))
            pivot_w        = float(dati_engine.get('pivot_weekly', 0))
            r1_d           = float(dati_engine.get('pivot_r1', 0))
            r2_d           = float(dati_engine.get('pivot_r2', 0))
            s1_d           = float(dati_engine.get('pivot_s1', 0))
            s2_d           = float(dati_engine.get('pivot_s2', 0))
            res_strut       = float(dati_engine.get('res_strutturale', 0))
            sup_strut       = float(dati_engine.get('sup_strutturale', 0))
            muro_s_now      = float(dati_engine.get('muro_supporto', 0))
            muro_r_now      = float(dati_engine.get('muro_resistenza', 0))
            vol_muro_s      = float(dati_engine.get('vol_supporto', 0))
            vol_muro_r      = float(dati_engine.get('vol_resistenza', 0))
            swing_h_h4      = dati_engine.get('swing_high_h4', [])
            swing_l_h4      = dati_engine.get('swing_low_h4', [])

            livelli_map = []

            def _prox(livello, label, soglia_perc=0.5):
                """Restituisce True se il prezzo è vicino al livello (soglia %)"""
                if livello <= 0 or prezzo_c <= 0: return False
                return abs(prezzo_c - livello) / prezzo_c * 100 < soglia_perc

            def _lato(livello):
                if prezzo_c > livello: return "sotto il prezzo"
                return "sopra il prezzo"

            # Posizione nella Value Area settimanale (fatti grezzi, no interpretazione)
            if vah_w > 0 and val_w > 0 and poc_w > 0:
                if prezzo_c > vah_w:
                    va_pos = f"SOPRA la Value Area settimanale (prezzo {prezzo_c:.4f} > VAH {vah_w:.2f})"
                elif prezzo_c < val_w:
                    va_pos = f"SOTTO la Value Area settimanale (prezzo {prezzo_c:.4f} < VAL {val_w:.2f})"
                else:
                    dist_poc = (prezzo_c - poc_w) / poc_w * 100
                    va_pos = f"DENTRO la Value Area settimanale (VAL={val_w:.2f} POC={poc_w:.2f} VAH={vah_w:.2f}, distanza POC: {dist_poc:+.2f}%)"
                livelli_map.append(f"📊 Volume Profile 7gg: {va_pos}")

            # Prossimità a livelli strutturali
            livelli_strutturali = [
                (res_strut,  "Resistenza strutturale H1 (swing high)"),
                (sup_strut,  "Supporto strutturale H1 (swing low)"),
                (pivot_w,    "Pivot settimanale"),
                (r2_d,       "R2 daily"),
                (r1_d,       "R1 daily"),
                (pivot_d,    "Pivot daily"),
                (s1_d,       "S1 daily"),
                (s2_d,       "S2 daily"),
            ]
            vicini = []
            for liv, nome in livelli_strutturali:
                if liv > 0:
                    dist = (prezzo_c - liv) / prezzo_c * 100
                    if abs(dist) < 1.5:  # entro 1.5%
                        vicini.append(f"{nome}={liv:.4f} (dist {dist:+.2f}%)")
            if vicini:
                livelli_map.append(f"📍 Livelli strutturali vicini (<1.5%): {' | '.join(vicini)}")
            else:
                livelli_map.append(f"📍 Nessun livello strutturale entro 1.5% — prezzo in spazio libero")

            # Contesto muro book vs livelli superiori
            muro_s_context = ""
            muro_r_context = ""

            if muro_s_now > 0:
                # Il muro di supporto book coincide con un livello superiore?
                coincidenze_s = []
                for liv, nome in [(poc_w,"POC weekly"),(val_w,"VAL weekly"),(pivot_d,"Pivot daily"),(s1_d,"S1 daily"),(sup_strut,"Supporto strutturale H1")]:
                    if liv > 0 and abs(muro_s_now - liv) / max(muro_s_now, liv) < 0.003:  # entro 0.3%
                        coincidenze_s.append(nome)
                # H4 swing lows
                for sl in (swing_l_h4[:3] if isinstance(swing_l_h4, list) else []):
                    sl_p = float(sl) if isinstance(sl, (int, float)) else float(sl.get('price', 0))
                    if sl_p > 0 and abs(muro_s_now - sl_p) / max(muro_s_now, sl_p) < 0.003:
                        coincidenze_s.append(f"Swing Low H4 {sl_p:.4f}")

                if coincidenze_s:
                    muro_s_context = (f"MURO SUPPORTO {muro_s_now:.4f} (vol={vol_muro_s:.0f}) "
                                      f"— CONFERMATO da livelli superiori: {', '.join(coincidenze_s)} ← LIVELLO FORTE")
                else:
                    muro_s_context = (f"MURO SUPPORTO {muro_s_now:.4f} (vol={vol_muro_s:.0f}) "
                                      f"— isolato, non confermato da struttura superiore ← LIVELLO DEBOLE")

            if muro_r_now > 0:
                coincidenze_r = []
                for liv, nome in [(poc_w,"POC weekly"),(vah_w,"VAH weekly"),(pivot_d,"Pivot daily"),(r1_d,"R1 daily"),(res_strut,"Resistenza strutturale H1")]:
                    if liv > 0 and abs(muro_r_now - liv) / max(muro_r_now, liv) < 0.003:
                        coincidenze_r.append(nome)
                for sh in (swing_h_h4[:3] if isinstance(swing_h_h4, list) else []):
                    sh_p = float(sh) if isinstance(sh, (int, float)) else float(sh.get('price', 0))
                    if sh_p > 0 and abs(muro_r_now - sh_p) / max(muro_r_now, sh_p) < 0.003:
                        coincidenze_r.append(f"Swing High H4 {sh_p:.4f}")

                if coincidenze_r:
                    muro_r_context = (f"MURO RESISTENZA {muro_r_now:.4f} (vol={vol_muro_r:.0f}) "
                                      f"— CONFERMATO da livelli superiori: {', '.join(coincidenze_r)} ← LIVELLO FORTE")
                else:
                    muro_r_context = (f"MURO RESISTENZA {muro_r_now:.4f} (vol={vol_muro_r:.0f}) "
                                      f"— isolato, non confermato da struttura superiore ← LIVELLO DEBOLE")

            if muro_s_context: livelli_map.append(f"🧱 {muro_s_context}")
            if muro_r_context: livelli_map.append(f"🧱 {muro_r_context}")

            mappa_livelli = (
                "CONTESTO STRUTTURALE — GERARCHIA DEI LIVELLI:\n"
                + "\n".join(livelli_map)
                + "\n\nREGOLA FONDAMENTALE: un muro del book che coincide con un livello "
                "strutturale superiore (H4 swing, pivot weekly, VAH/VAL) è un ostacolo REALE "
                "— può bloccare il prezzo per ore. Un muro isolato viene spesso attraversato "
                "in pochi minuti. Valuta il tuo target in base a quali livelli forti si trovano "
                "tra il prezzo attuale e il TP proposto."
            )
        except Exception as e_map:
            self.logger.debug(f"Errore mappa livelli: {e_map}")
            mappa_livelli = "Mappa livelli non disponibile."
        # ─────────────────────────────────────────────────────────────────────
        try:
            # Usiamo l'engine già esistente se disponibile, altrimenti fallback
            engine_to_use = self.engine
            if not engine_to_use and self.trade_manager and hasattr(self.trade_manager, 'engine'):
                engine_to_use = self.trade_manager.engine
            
            if engine_to_use and hasattr(engine_to_use, 'get_asset_leverage_info'):
                lev_info = engine_to_use.get_asset_leverage_info(asset_name)
                allowed_levs = lev_info.get("allowed_leverages", [1])
                max_lev_kraken = lev_info.get("max_leverage", 1)
            else:
                # Fallback se l'engine non è pronto o non ha il metodo
                allowed_levs = [1, 2, 3, 5]
                max_lev_kraken = 5
        except Exception as e:
            _err.capture(e, "unknown", {"module": "BrainLA"})
            self.logger.warning(f"⚠️ Errore recupero leva reale per {asset_name}: {e}")
            allowed_levs = [1, 2, 3, 5]
            max_lev_kraken = 5

        # --- NORMALIZZAZIONE E CONTESTO RELATIVO (TRADUZIONE PER IL CERVELLO) ---
        volume_24h = float(dati_engine.get('volume_24h', 1.0))
        cvd_relativo = (cvd_istantaneo / (volume_24h / 1440)) if volume_24h > 0 else 0 # CVD rispetto al volume medio al minuto
        
        # Semafori di Coerenza (Pre-interpretazione)
        semaforo_cvd = "VERDE (Concorde)" if (price_velocity > 0 and cvd_istantaneo > 0) or (price_velocity < 0 and cvd_istantaneo < 0) else "ROSSO (Divergente)"
        semaforo_vpin = "PERICOLO (Tossico)" if vpin > 0.85 else "SICURO"
        semaforo_zscore = "IPER-ESTESO" if abs(z_score) > 2.5 else "NORMALE"

        # --- ALERT TRAPPOLE (LOGICA RIGIDA) ---
        is_trap = False
        trap_reason = ""
        if (price_velocity > 0 and cvd_istantaneo < -10000) or (price_velocity < 0 and cvd_istantaneo > 10000):
            is_trap = True
            trap_reason = "DIVERGENZA PREZZO/CVD MASSICCIA (Trappola Istituzionale)"
        elif vpin > 0.90:
            is_trap = True
            trap_reason = "TOSSICITÀ ESTREMA (VPIN > 0.90)"

        dati_per_gemini = {
            'identità_asset': {
                'ticker': ticker_ufficiale,
                'dna_operativo': asset_dna,
                'prezzo_attuale': entry_price
            },
            'NOTE_TEMPORALI': {
                'cvd_vpin_whale': 'ultimi ~500 trade WebSocket (2-30 min a seconda del volume)',
                'hurst_atr_rsi_vwap': 'ultimi 100 periodi 15m = ~8 ore',
                'book_pressure_ofi': 'snapshot istantaneo del book',
                'multi_tf': 'calcolato separatamente su 15m/1h/4h/1d',
                'ha_daily': 'candele giornaliere ultimi 30 giorni',
                'avviso': (
                    'CVD e VPIN sono i dati PIU FRESCHI — riflettono gli ultimi minuti. '
                    'Hurst e RSI sono su 8 ore — descrivono il regime recente non il momento. '
                    'Peso maggiore ai dati freschi per il TIMING, ai dati lenti per il REGIME.'
                )
            },
            'ALERT_CRITICI_DI_SISTEMA': {
                'IS_TRAP_DETECTED': is_trap,
                'MOTIVAZIONE_TRAPPOLA': trap_reason
            },
            'semafori_coerenza_tecnica': {
                'coerenza_prezzo_volumi': semaforo_cvd,
                'livello_tossicità_hft': semaforo_vpin,
                'estensione_statistica': semaforo_zscore
            },
            'matrice_rischio_kraken': {
                'leve_permesse': allowed_levs,
                'leva_max': max_lev_kraken,
                'slippage_atteso_perc': slippage_medio
            },
            'contesto_macro_e_salute': {
                'market_health_score': market_health,
                'regime_hmm': market_regime,
                'sentiment_globale': macro_sentiment,
                'correlazione_btc': corr_driver
            },
            'analisi_volumetrica_statica': {
                'area_valore': {'poc': poc, 'vah': vah, 'val': val},
                'z_score_distanza_vwap': z_dist_vwap
            },
            'dinamica_order_flow_hft': {
                'vpin_tossicità': vpin,
                'cvd_relativo_al_volume_medio': round(cvd_relativo, 4),
                'cvd_divergenza_correlazione': cvd_divergence,
                'order_flow_imbalance': ofi,
                'whale_delta': round(whale_delta, 3),
                'delta_footprint': round(delta_footprint, 3),
                'aggressività_dominante': aggressivita_flow,
                'NOTA_OFI': (
                    "CRITICO: order_flow_imbalance va da -1 a +1. "
                    "Sopra +0.5 = pressione buy reale. Sotto -0.5 = pressione sell reale. "
                    "Se proponi BUY con ofi < -0.4, devi giustificare esplicitamente. "
                    "Se proponi SELL con ofi > 0.4, devi giustificare esplicitamente."
                ),
                'NOTA_WHALE_DELTA': (
                    "CRITICO: whale_delta va da -1 a +1. "
                    "Positivo = i grandi player comprano. Negativo = i grandi player vendono. "
                    "I whale hanno informazioni migliori del retail. "
                    "Se proponi BUY con whale_delta < -0.5, il segnale è contro l'istituzionale — riduci voto. "
                    "Se proponi SELL con whale_delta > 0.5, stesso principio."
                )
            },
            'candlestick_15m': {
                'patterns':   dati_engine.get('candlestick_patterns', ['NESSUNO']),
                'bias':       dati_engine.get('candlestick_bias', 'NEUTRO'),
                'ultima_bullish': dati_engine.get('candlestick_ultima_bull', False),
                'body_perc':  dati_engine.get('candlestick_body_perc', 0.5),
                'NOTA': (
                    "I pattern candlestick confermano o smentiscono il segnale di order flow. "
                    "ENGULFING_BULL/BEAR e MARUBOZU sono i più forti — momentum reale. "
                    "PIN_BAR su supporto/resistenza = inversione ad alta probabilità. "
                    "DOJI o INSIDE_BAR = indecisione, riduci il voto di 1. "
                    "Se bias candlestick CONCORDA con la tua direzione: +1 punto di confidenza. "
                    "Se bias CONTRARIO alla tua direzione: -1 punto."
                )
            },
            'microstruttura_book': {
                'book_pressure': bp,
                'muro_supporto': {'prezzo': muro_supporto_prezzo, 'distanza': dist_supporto},
                'muro_resistenza': {'prezzo': muro_resistenza_prezzo, 'distanza': dist_resistenza},
                'indice_spoofing': spoofing
            },
            'cinematica_prezzo': {
                'price_velocity': price_velocity,
                'is_fake_breakout': is_fake_breakout,
                'hurst_exponent': hurst,
                'kaufman_efficiency': market_efficiency
            },
            'indicatori_derivati': {
                'funding_z_score': funding_z,
                'rsi_momentum': rsi,
                'liquidazioni_24h': liquidazioni_24h
            },
            'multi_timeframe_hurst': multi_tf_safe
        }

        json_input = json.dumps(dati_per_gemini, indent=2)

        # ── SEGNALE IN TEMPO REALE — Signal State Engine ────────────────────
        # Traiettoria del CVD su finestre brevi — il dato più fresco disponibile.
        # entry_phase dice in quale fase si trova il segnale adesso.
        # exhaustion_score misura se il momentum è esaurito (0=fresco, 10=esaurito).
        try:
            _fase_label = {
                'SILENZIO':   'SILENZIO — nessun segnale attivo',
                'FORMAZIONE': 'FORMAZIONE — segnale sta nascendo',
                'MATURAZIONE':'MATURAZIONE — segnale sviluppato, momentum attivo',
                'BREAKOUT':   'BREAKOUT — movimento esplosivo in corso',
                'BREAKOUT (FAKE)': 'BREAKOUT FALSO — velocity alta ma CVD debole',
                'ESAURIMENTO':'ESAURIMENTO — momentum si sta spegnendo',
            }.get(_entry_phase, _entry_phase)

            if _exhaustion >= 7:
                _exhaust_label = f"ALTO ({_exhaustion}/10) — segnale probabilmente esaurito ⚠️"
            elif _exhaustion >= 4:
                _exhaust_label = f"MEDIO ({_exhaustion}/10) — momentum in calo"
            else:
                _exhaust_label = f"BASSO ({_exhaustion}/10) — segnale fresco ✅"

            if _cvd_accel > 0:
                _accel_label = f"IN ACCELERAZIONE (+{_cvd_accel:.0f}) — flusso crescente ✅"
            elif _cvd_accel < 0:
                _accel_label = f"IN DECELERAZIONE ({_cvd_accel:.0f}) — flusso in calo ⚠️"
            else:
                _accel_label = "STABILE"

            # Contesto TF superiore
            _tf4h_trend = _contesto_tf.get('trend_4h', '?')
            _tf1d_trend = _contesto_tf.get('trend_1d', '?')
            _tf4h_hurst = _contesto_tf.get('hurst_4h', 0.5)
            _tf4h_regime= _contesto_tf.get('regime_4h', '?')

            _tf_ctx = (
                f"  Contesto 4h: trend={_tf4h_trend} | Hurst={_tf4h_hurst:.2f} | regime={_tf4h_regime}\n"
                f"  Contesto 1d: trend={_tf1d_trend}\n"
            )

            # Veto SHORT (sbloccato: solo info, Gemini decide)
            _short_info = ""
            if not _short_ok and _short_veto:
                _short_info = f"  SHORT info SSE: {_short_veto} (osservazione, non blocco — valuta tu se ha peso)\n"
            elif _short_ok:
                _short_info = "  SHORT: condizioni valide secondo SSE\n"

            sse_block = (
                f"CONTESTO DI MERCATO (Signal State Engine):\n"
                f"  Fase: {_fase_label}\n"
                + (f"  Sottotipo: {_phase_subtype}\n" if _phase_subtype else "")
                + f"  Exhaust: {_exhaust_label}\n"
                + (f"  Età segnale: {_signal_age:.0f}s\n" if _signal_age > 0 else "")
                + f"\n"
                + (f"  Narrativa SSE: {_phase_narrative}\n" if _phase_narrative else "")
                + f"\n"
                + f"  CVD [ultimi 120s]: trend={_cvd_trend} | delta_30s={_cvd_delta_30s:+.0f}$ | delta_120s={_cvd_delta_120s:+.0f}$\n"
                + f"  Accelerazione CVD: {_accel_label}\n"
                + _short_info
                + f"\n"
                + _tf_ctx
                + (f"\n  OVERRIDE DISPONIBILE: {_override_cond}\n" if _override_ok and _override_cond else "")
                + f"\nREGOLA: Usa la narrativa SSE come punto di partenza del tuo ragionamento.\n"
                f"La fase di mercato determina il TIPO di opportunità — non ignorarla mai."
            )
            self.logger.info(
                f"⚡ [SSE] {ticker_ufficiale}: fase={_entry_phase} exhaust={_exhaustion}/10 "
                f"cvd_trend={_cvd_trend} delta30s={_cvd_delta_30s:+.0f} | "
                f"ciclo={_ciclo_fase}({_ciclo_recupero:.0f}%) min={_minimo_qualita} "
                f"{'SR_FLIP!' if _sr_flip else ''}"
            )
        except Exception as e_sse:
            self.logger.debug(f"SSE block error: {e_sse}")
            sse_block = ""
        # ─────────────────────────────────────────────────────────────────────

        # ── STORIA DEL MERCATO — ultimi 60 minuti dal sequence_buffer ──────────
        # Analisi dati reali: i trade vincenti hanno volume 3x la media nelle
        # candele precedenti. I perdenti entrano quando il momentum è esaurito.
        # Gemini deve vedere la TRAIETTORIA, non solo lo snapshot istantaneo.
        storia_mercato_block = ""
        try:
            from core.sequence_buffer import seq_buf
            seq = seq_buf.get_sequence(asset_name, n=18)  # ~54 minuti a ciclo 3min
            if seq and len(seq) >= 6:
                cvds    = [s.get("cvd_istantaneo", 0) for s in seq]
                prezzi  = [s.get("close", 0) for s in seq if s.get("close", 0) > 0]
                vol_raw = [abs(s.get("cvd_istantaneo", 0) - (seq[max(0,i-1)].get("cvd_istantaneo", 0)))
                           for i, s in enumerate(seq)]

                mid         = len(cvds) // 2
                cvd_prima   = cvds[mid] - cvds[0]
                cvd_seconda = cvds[-1] - cvds[mid]
                cvd_totale  = cvds[-1] - cvds[0]

                vol_media  = (sum(vol_raw[:-3]) / max(len(vol_raw[:-3]), 1)) if len(vol_raw) > 3 else 1
                vol_ultimi = sum(vol_raw[-3:]) / 3 if len(vol_raw) >= 3 else 0
                vol_ratio  = vol_ultimi / max(vol_media, 0.001)

                price_trend_pct = ((prezzi[-1] - prezzi[0]) / prezzi[0] * 100) if len(prezzi) >= 2 else 0

                if vol_ratio >= 2.0:
                    vol_label = f"ALTO ({vol_ratio:.1f}x media) — volume conferma il movimento ✅"
                elif vol_ratio >= 1.0:
                    vol_label = f"NELLA NORMA ({vol_ratio:.1f}x media) — nessuna accelerazione"
                else:
                    vol_label = f"BASSO ({vol_ratio:.1f}x media) — movimento senza partecipazione ⚠️"

                if cvd_seconda > cvd_prima * 1.5 and cvd_seconda > 0:
                    cvd_acc_label = "IN ACCELERAZIONE (il flusso sta aumentando) ✅"
                elif cvd_seconda < cvd_prima * 0.5 and cvd_prima > 0:
                    cvd_acc_label = "IN ESAURIMENTO (il flusso si sta spegnendo) ⚠️"
                elif cvd_seconda < 0 < cvd_prima:
                    cvd_acc_label = "INVERSIONE NEGATIVA (il flusso ha girato contro) ❌"
                elif cvd_seconda > 0 > cvd_prima:
                    cvd_acc_label = "INVERSIONE POSITIVA (il flusso si e risvegliato) ✅"
                else:
                    cvd_acc_label = "STABILE"

                storia_mercato_block = (
                    f"TENDENZA RECENTE (ultimi ~60 minuti, {len(seq)} snapshot):\n"
                    f"  Prezzo: {price_trend_pct:+.2f}% ({'su' if price_trend_pct > 0 else 'giu'})\n"
                    f"  CVD totale: {cvd_totale:+.0f} (prima meta {cvd_prima:+.0f} / seconda meta {cvd_seconda:+.0f})\n"
                    f"  Momentum CVD: {cvd_acc_label}\n"
                    f"  Volume corrente: {vol_label}\n\n"
                    f"REGOLA CRITICA (dati storici {len(seq)} cicli reali):\n"
                    f"  Trade vincenti: aperti con volume >= 2x media e CVD in accelerazione.\n"
                    f"  Trade perdenti: aperti con volume normale e CVD in esaurimento.\n"
                    f"  Se volume basso e CVD decelera: il movimento sta probabilmente finendo.\n"
                    f"  In quel caso preferisci FLAT e attendi conferma reale."
                )
                self.logger.info(
                    f"📈 [STORIA] {ticker_ufficiale}: prezzo {price_trend_pct:+.2f}% | "
                    f"CVD={cvd_acc_label[:25]} | vol={vol_ratio:.1f}x"
                )
        except Exception as e_storia:
            self.logger.debug(f"Storia mercato non disponibile per {asset_name}: {e_storia}")
            storia_mercato_block = ""

        # --- LOGGING DETTAGLIATO (Ripristinato) ---
        self.logger.debug(f"📊 === ANALISI TECNICA COMPLETA: {ticker_ufficiale} ===")
        self.logger.debug(f"🏥 MARKET HEALTH: {market_health} | REGIME: {market_regime}")
        vpin_status = "☢️ TOSSICO (Assorbimento)" if is_toxic else ("Tossico" if vpin > 0.8 else "Scambi Regolari")
        self.logger.debug(f"🔹 [ORDER FLOW] CVD: {cvd_istantaneo:+.2f} | VPIN: {vpin:.4f} [{vpin_status}]")
        if is_fake_breakout:
            self.logger.warning(f"⚠️ FAKE BREAKOUT RILEVATO: Alta Velocity ({price_velocity:.4f}) ma CVD debole ({cvd_istantaneo:.2f})")
        div_status = "✅ Neutra" if not cvd_divergence else "⚠️ Divergenza"
        self.logger.debug(f"   ➤ Delta Divergence: {div_status}")
        self.logger.debug(f"🔹 [LIQUIDITY] Book Pressure: {bp:.2f} | Spoofing: {spoofing:.2f}")
        self.logger.debug(f"   ➤ Muri H1: Supporto {muro_supporto_prezzo} ({dist_supporto}%) | Resistenza {muro_resistenza_prezzo} ({dist_resistenza}%)")
        hurst_status = "📈 TRENDING" if hurst > 0.55 else "📉 MEAN REVERTING" if hurst < 0.45 else "↔️ RANGE"
        self.logger.debug(f"🔹 [MARKET REGIME] Hurst: {hurst:.2f} [{hurst_status}] | HMM: {regime_hmm}")
        vel_status = "ACCELERAZIONE" if abs(price_velocity) > 0.0005 else "NORMALE"
        self.logger.debug(f"🔹 [VELOCITY] Price Velocity: {price_velocity:.4f} [{vel_status}]")
        self.logger.debug(f"🔹 [VOLATILITY] SL Consigliato (2x ATR): {atr_p * 2:.2f}%")
        self.logger.debug("======================================================")

        # ── CONDIZIONI MICROSTRUTTURALI GREZZE (no etichette interpretative) ────
        # Valutazione condizioni correnti vs pattern storici validati su 395 trade reali.
        # I pattern vengono comunicati esplicitamente a Gemini così non deve dedurli.
        pattern_block = ""
        _pattern_attivi = []
        _sl_mult_pattern = 1.0
        try:
            # Calcoliamo le condizioni grezze (non più "pattern WR 83%")
            # Fatti che Gemini può usare per ragionare, senza etichette interpretative
            _pattern_long  = self._riconosci_pattern(dati_engine, "LONG")
            _pattern_short = self._riconosci_pattern(dati_engine, "SHORT")
            _all_facts = _pattern_long + _pattern_short

            if _all_facts:
                lines = ["CONDIZIONI MICROSTRUTTURALI ATTUALI (fatti grezzi — interpretali tu):"]
                seen = set()
                for p in _all_facts:
                    # Mostra solo il dettaglio (fatto grezzo), niente WR/forza/nome cabalistico
                    det = p.get('dettaglio', '')
                    if det and det not in seen:
                        seen.add(det)
                        lines.append(f"  • {det}")
                pattern_block = "\n".join(lines)

                # Moltiplicatore SL: media dei valori invece che max (riduce bias)
                _sl_mults = [p.get('sl_mult', 1.0) for p in _all_facts]
                _sl_mult_pattern = sum(_sl_mults) / len(_sl_mults) if _sl_mults else 1.0
                self.logger.info(
                    f"📋 [{ticker_ufficiale}] {len(_all_facts)} condizioni grezze rilevate | "
                    f"SL mult={_sl_mult_pattern:.2f}x"
                )
                # Espongo comunque _pattern_attivi per back-compat con il resto del codice
                _pattern_attivi = _all_facts
            else:
                pattern_block = ""
        except Exception as e_pat:
            self.logger.debug(f"Pattern block error: {e_pat}")
            pattern_block = ""
        # ─────────────────────────────────────────────────────────────────────

        # SBLOCCO: rimosso il blocco pre-Gemini su SILENZIO e BREAKOUT_FAKE.
        # Gemini riceve l'informazione della fase nei dati e decide lui.
        if _entry_phase in ('SILENZIO',) or (_entry_phase == 'BREAKOUT' and _phase_subtype == 'FAKE'):
            self.logger.info(
                f"ℹ️ [{ticker_ufficiale}] {_entry_phase} {_phase_subtype} — info, Gemini decide."
            )
        # ─────────────────────────────────────────────────────────────────────

        # ── CONTESTO GLOBALE DI MERCATO ───────────────────────────────────────
        _penalita_ctx = 0
        _prox_evento  = None
        _fg_val       = None
        _fund_pct     = None
        try:
            _ctx_raw = dati_engine.get('market_context', {})
            if _ctx_raw:
                # Usa il build_prompt_block direttamente dal contesto raw
                # senza creare una nuova istanza di MarketContextEngine
                from core.market_context_engine import MarketContextEngine
                contesto_globale_block = MarketContextEngine.build_prompt_block_static(_ctx_raw)
                _prox_evento = _ctx_raw.get('prossimo_evento_minuti')
                _fg_val      = _ctx_raw.get('fear_greed', {}).get('value') if isinstance(_ctx_raw.get('fear_greed'), dict) else _ctx_raw.get('fear_greed_index')
                _fund_pct    = _ctx_raw.get('funding', {}).get('percentile') if isinstance(_ctx_raw.get('funding'), dict) else _ctx_raw.get('funding_btc_percentile')

                # Log news visibili nel log ad ogni analisi
                _news = _ctx_raw.get('news', [])
                if _news:
                    _news_str = " | ".join(n[:60] for n in _news[:3])
                    self.logger.info(f"📰 [{ticker_ufficiale}] News attive: {_news_str}")
                else:
                    self.logger.debug(f"📰 [{ticker_ufficiale}] Nessuna news disponibile dal feed")

                if _prox_evento is not None and _prox_evento < 60:
                    _penalita_ctx += 2
                    self.logger.warning(f"⚠️ [{ticker_ufficiale}] Evento macro tra {_prox_evento} min — penalità ctx -2")
                if _fg_val is not None and int(_fg_val) >= 85:
                    _penalita_ctx += 1
                    self.logger.warning(f"⚠️ [{ticker_ufficiale}] Fear&Greed euforia ({_fg_val}) — penalità ctx -1")
                if _fund_pct is not None and int(_fund_pct) >= 90:
                    _penalita_ctx += 1
                    self.logger.warning(f"⚠️ [{ticker_ufficiale}] Funding squeeze ({_fund_pct}° pct) — penalità ctx -1")
            else:
                contesto_globale_block = "CONTESTO GLOBALE: Non disponibile in questo ciclo."
        except Exception as e_ctx:
            contesto_globale_block = "CONTESTO GLOBALE: Errore caricamento."
            self.logger.debug(f"MarketContext build: {e_ctx}")
        # ─────────────────────────────────────────────────────────────────────

        # ── MEMORIA RECENTE: ultimi trade su QUESTO asset+direzione ──────────
        # Inserita nel prompt come contesto: se sono in una streak di LOSS sullo
        # stesso asset+direzione, Gemini deve esserne consapevole prima di decidere.
        memoria_recente_block = ""
        try:
            from core.database_manager import db_manager
            from datetime import timedelta
            storico_db = db_manager.get_storico()
            # Filtro: stesso asset, ultimi 7 giorni, esiti reali
            now_dt = datetime.now()
            recenti_asset = []
            for _t in storico_db:
                if _t.get('asset') != ticker_ufficiale:
                    continue
                if _t.get('fonte') in ('STORICO_SIMULATO','VIRTUAL_BRAIN','GHOST','GHOST_KRAKEN'):
                    continue
                if _t.get('arricchito_retroattivo'):
                    continue
                if _t.get('esito') not in ('WIN','LOSS'):
                    continue
                if _t.get('motivo_chiusura') == 'ALREADY_CLOSED':
                    continue
                if 'TRASCRITTA' in str(_t.get('nota','')):
                    continue
                try:
                    _ts = datetime.fromisoformat(_t.get('data_chiusura','').replace('Z',''))
                    if (now_dt - _ts) <= timedelta(days=7):
                        recenti_asset.append(_t)
                except Exception:
                    continue
            recenti_asset = sorted(recenti_asset, key=lambda x: x.get('data_chiusura',''))[-10:]
            
            if recenti_asset:
                long_recenti = [_t for _t in recenti_asset if _t.get('direzione')=='LONG']
                short_recenti = [_t for _t in recenti_asset if _t.get('direzione')=='SHORT']
                
                # Streak ultimi LOSS per direzione
                streak_long_loss = 0
                for _t in reversed(long_recenti):
                    if _t.get('esito')=='LOSS':
                        streak_long_loss += 1
                    else:
                        break
                streak_short_loss = 0
                for _t in reversed(short_recenti):
                    if _t.get('esito')=='LOSS':
                        streak_short_loss += 1
                    else:
                        break
                
                _wins_l = sum(1 for _t in long_recenti if _t.get('esito')=='WIN')
                _wins_s = sum(1 for _t in short_recenti if _t.get('esito')=='WIN')
                
                lines_mem = [f"STORIA RECENTE SU {ticker_ufficiale} (ultimi 7 giorni, {len(recenti_asset)} trade reali):"]
                if long_recenti:
                    wr_l = 100*_wins_l/len(long_recenti) if long_recenti else 0
                    lines_mem.append(f"  LONG: {len(long_recenti)} trade, {_wins_l} WIN, WR {wr_l:.0f}%")
                    if streak_long_loss >= 2:
                        lines_mem.append(f"    ⚠️ ATTENZIONE: ultimi {streak_long_loss} LONG sono LOSS consecutivi.")
                        lines_mem.append(f"    Se vuoi entrare LONG, chiediti: cosa è cambiato rispetto agli ultimi tentativi falliti?")
                        lines_mem.append(f"    Se nulla è cambiato in modo materiale, la risposta corretta è probabilmente FLAT.")
                if short_recenti:
                    wr_s = 100*_wins_s/len(short_recenti) if short_recenti else 0
                    lines_mem.append(f"  SHORT: {len(short_recenti)} trade, {_wins_s} WIN, WR {wr_s:.0f}%")
                    if streak_short_loss >= 2:
                        lines_mem.append(f"    ⚠️ ATTENZIONE: ultimi {streak_short_loss} SHORT sono LOSS consecutivi.")
                        lines_mem.append(f"    Se vuoi entrare SHORT, chiediti: cosa è cambiato rispetto agli ultimi tentativi falliti?")
                        lines_mem.append(f"    Se nulla è cambiato in modo materiale, la risposta corretta è probabilmente FLAT.")
                memoria_recente_block = "\n".join(lines_mem)
            else:
                memoria_recente_block = f"STORIA RECENTE SU {ticker_ufficiale}: nessun trade negli ultimi 7 giorni."
        except Exception as _e_mem:
            memoria_recente_block = "STORIA RECENTE: non disponibile in questo ciclo."
            self.logger.debug(f"[MEMORIA RECENTE] {ticker_ufficiale}: {_e_mem}")
        # ─────────────────────────────────────────────────────────────────────

        # ── MEMORIA PERSISTENTE DI GEMINI: ultime decisioni e loro esito ─────
        # Permette a Gemini di vedere cosa lui stesso ha pensato sui cicli
        # precedenti su questo asset. È il singolo cambio cognitivo più grande:
        # prima decideva nel vuoto, ora ha continuità.
        try:
            if self.gemini_memory is not None:
                gemini_self_memory_block = self.gemini_memory.format_for_prompt(ticker_ufficiale, n=3)
            else:
                gemini_self_memory_block = "LE TUE ULTIME DECISIONI: memoria non disponibile."
        except Exception as _e_gsm:
            gemini_self_memory_block = "LE TUE ULTIME DECISIONI: errore lettura memoria."
            self.logger.debug(f"[GEMINI SELF-MEM] {ticker_ufficiale}: {_e_gsm}")
        # ─────────────────────────────────────────────────────────────────────

        # ── AUTODIAGNOSI BIAS: WR delle ultime decisioni concluse ───────────
        # Modifica E — un trader istituzionale sa quando è in fase di errore o
        # euforia e adatta lo standard di evidenza. Mostriamo a Gemini il suo
        # WR recente su questo asset così possa autocalibrarsi.
        try:
            if self.gemini_memory is not None:
                _wr_stats = self.gemini_memory.get_recent_wr(ticker_ufficiale, n=10)
            else:
                _wr_stats = {'fase': 'INSUFFICIENT', 'wr_pct': None,
                             'n_decisioni_concluse': 0, 'ultime_loss_consecutive': 0}
        except Exception as _e_wr:
            _wr_stats = {'fase': 'INSUFFICIENT', 'wr_pct': None,
                         'n_decisioni_concluse': 0, 'ultime_loss_consecutive': 0}

        _fase = _wr_stats.get('fase', 'INSUFFICIENT')
        _streak = _wr_stats.get('ultime_loss_consecutive', 0)

        if _fase == 'TROUBLE':
            _autodiag_intro = (
                f"⚠️ FASE: TROUBLE — Il tuo WR sulle ultime "
                f"{_wr_stats['n_decisioni_concluse']} decisioni concluse è "
                f"{_wr_stats['wr_pct']:.0f}%. Sei in fase di errore."
            )
            _autodiag_regola = (
                "  → Standard di evidenza richiesto: ALTO.\n"
                "  → Voto massimo accettabile in questa fase: 7.\n"
                "  → Stile preferito: solo MOMENTUM o SCALPING (durata corta).\n"
                "  → NON entrare in setup ambigui — la statistica recente dice che stai sbagliando."
            )
        elif _fase == 'HOT':
            _autodiag_intro = (
                f"🔥 FASE: HOT — Il tuo WR sulle ultime "
                f"{_wr_stats['n_decisioni_concluse']} decisioni concluse è "
                f"{_wr_stats['wr_pct']:.0f}%. Sei in fase di accuratezza."
            )
            _autodiag_regola = (
                "  → Bias possibile: euforia. Mantieni la disciplina.\n"
                "  → NON aumentare leva o sizing rispetto al normale.\n"
                "  → Usa la qualità per fare meno trade, non più trade."
            )
        elif _fase == 'NORMAL':
            _autodiag_intro = (
                f"✅ FASE: NORMAL — WR {_wr_stats['wr_pct']:.0f}% "
                f"su {_wr_stats['n_decisioni_concluse']} decisioni concluse. Calibrazione regolare."
            )
            _autodiag_regola = "  → Procedi con il tuo standard normale di evidenza."
        else:
            _autodiag_intro = (
                f"⏳ FASE: INSUFFICIENT — Hai meno di 3 decisioni concluse su questo asset. "
                f"Calibrazione non ancora misurabile."
            )
            _autodiag_regola = "  → Procedi con prudenza. Voto massimo accettabile finché non hai dati: 7."

        if _streak >= 2:
            _autodiag_streak = (
                f"\n⚠️ STREAK: hai appena perso {_streak} decisioni consecutive su questo asset. "
                "Se la tua tesi attuale assomiglia anche solo in parte a quelle perse, FLAT è la risposta corretta."
            )
        else:
            _autodiag_streak = ""

        autodiagnosi_block = (
            "AUTODIAGNOSI:\n"
            f"{_autodiag_intro}\n"
            f"{_autodiag_regola}"
            f"{_autodiag_streak}"
        )
        # ─────────────────────────────────────────────────────────────────────

        # ── TESI MACRO CONDIVISA (Modifica A) ────────────────────────────────
        # Se esiste una tesi macro valida, la mostriamo in cima al prompt come
        # "view del desk". Se non esiste o è scaduta, blocco vuoto (graceful).
        try:
            if self.macro_thesis_cache is not None:
                macro_thesis_block = self.macro_thesis_cache.format_for_prompt()
            else:
                macro_thesis_block = ""
        except Exception as _e_mtc:
            macro_thesis_block = ""
            self.logger.debug(f"[MACRO THESIS] {ticker_ufficiale}: {_e_mtc}")
        # ─────────────────────────────────────────────────────────────────────

        # ── PROFILO ASSET (Modifica I v11) ───────────────────────────────────
        # Memoria istituzionale per quell'asset specifico: natura, orari caldi,
        # pattern tipici, idiosincrasie operative e lezione storica dal bot.
        # Permette a Gemini di non trattare BTC e BONK come la stessa cosa.
        try:
            if self._asset_profile_format is not None:
                asset_profile_block = self._asset_profile_format(ticker_ufficiale)
            else:
                asset_profile_block = ""
        except Exception as _e_ap:
            asset_profile_block = ""
            self.logger.debug(f"[ASSET PROFILE] {ticker_ufficiale}: {_e_ap}")
        # ─────────────────────────────────────────────────────────────────────

        # ── SCENARI PENDENTI (Modifica B soft) ───────────────────────────────
        # Se al ciclo precedente Gemini ha formulato scenari condizionali su
        # questo asset, li mostriamo qui così può verificare se uno è scattato.
        try:
            if self.pending_scenarios is not None:
                self.pending_scenarios.expire_old()
                pending_scenarios_block = self.pending_scenarios.format_for_prompt(ticker_ufficiale)
            else:
                pending_scenarios_block = ""
        except Exception as _e_ps:
            pending_scenarios_block = ""
            self.logger.debug(f"[PENDING SCENARIOS] {ticker_ufficiale}: {_e_ps}")
        # ─────────────────────────────────────────────────────────────────────

        # ── SEGNALI DI RUMORE ESPLICITI per il prompt ────────────────────────
        # Pre-calcoliamo le 3 firme statistiche del rumore così Gemini le vede
        # già pronte invece di doverle dedurre da 130 campi.
        _ke_now    = float(dati_engine.get('kaufman_efficiency', 0.5) or 0.5)
        _rvol_now  = float(macro_proxy.get('relative_volume_status', 1.0) or 1.0)
        _cvd30  = float(dati_engine.get('cvd_delta_30s',  0) or 0)
        _cvd120 = float(dati_engine.get('cvd_delta_120s', 0) or 0)
        # firma 1: efficienza scarsa = movimento senza direzione
        _firma1_rumore = (_ke_now < 0.25)
        # firma 2: flusso che cambia idea = nessun narratore
        _firma2_rumore = (_cvd30 != 0 and _cvd120 != 0
                          and ((_cvd30 > 0) != (_cvd120 > 0))
                          and abs(_cvd30) < 5000 and abs(_cvd120) < 5000)
        # firma 3: volume tiepido durante movimento
        _firma3_rumore = (_rvol_now < 0.8)
        _n_firme_rumore = int(_firma1_rumore) + int(_firma2_rumore) + int(_firma3_rumore)

        # ── BLOCCO CONSULENTE STRATEGY (Strada A v12) ────────────────────────
        # Le 5 strategie deterministiche non aprono più trade da sole. Sono
        # diventate "consulenti specializzati" che osservano un aspetto specifico
        # e riferiscono al cervello (Gemini). Quando una strategy ha trovato un
        # setup, il bot inietta i suoi dati in dati_engine['_strategy_signal'].
        # Qui li formatiamo per il prompt.
        strategy_signal_block = ""
        try:
            _strat_sig = dati_engine.get('_strategy_signal') if isinstance(dati_engine, dict) else None
            if _strat_sig and isinstance(_strat_sig, dict) and _strat_sig.get('signal'):
                _ss_name  = _strat_sig.get('strategy_name','?')
                _ss_sig   = str(_strat_sig.get('signal','?')).upper()
                _ss_score = float(_strat_sig.get('score',0) or 0)
                _ss_conf  = float(_strat_sig.get('confidence',0) or 0)
                _ss_entry = float(_strat_sig.get('entry_price',0) or 0)
                _ss_sl    = float(_strat_sig.get('sl',0) or 0)
                _ss_tp    = float(_strat_sig.get('tp',0) or 0)
                _ss_raz   = str(_strat_sig.get('razionale','') or '')[:200]

                # Etichetta forza in base allo score
                if _ss_score >= 80:
                    _ss_forza = "FORTE"
                elif _ss_score >= 65:
                    _ss_forza = "MEDIO"
                elif _ss_score >= 50:
                    _ss_forza = "DEBOLE"
                else:
                    _ss_forza = "MARGINALE"

                strategy_signal_block = (
                    "═══════════════════════════════════════════════════════════════\n"
                    "SEGNALE DEL CONSULENTE STRATEGY (uno dei 5 osservatori specializzati)\n"
                    "═══════════════════════════════════════════════════════════════\n"
                    f"Una delle 5 strategie deterministiche ha trovato un setup attivo:\n"
                    f"  • Strategia: {_ss_name}\n"
                    f"  • Direzione suggerita: {_ss_sig}\n"
                    f"  • Forza segnale: {_ss_forza} (score {_ss_score:.0f}/100, confidence {_ss_conf:.0%})\n"
                    f"  • Entry suggerito: {_ss_entry}\n"
                    f"  • SL suggerito: {_ss_sl}\n"
                    f"  • TP suggerito: {_ss_tp}\n"
                    f"  • Razionale strategia: \"{_ss_raz}\"\n\n"
                    "IMPORTANTE: questo è il parere di UN consulente specializzato che guarda\n"
                    "solo il suo aspetto. È un INPUT, non una regola. Tu vedi tutto: flusso,\n"
                    "prezzo, terreno, contesto, memoria, autodiagnosi. Decidi tu se il segnale\n"
                    "del consulente ha senso nel quadro più grande:\n"
                    "  • Se il setup è coerente con la tua narrativa → puoi confermarlo.\n"
                    "  • Se la tua narrativa contraddice il consulente → puoi ignorarlo o\n"
                    "    invertire (override deliberato, motivalo in ragionamento_decisione).\n"
                    "  • Se il consulente conferma quello che già vedevi → boost di convinzione.\n"
                    "  • SL/TP del consulente sono % fisse — i tuoi stop_logico/target_logico\n"
                    "    basati sui livelli reali sono superiori.\n"
                )
            else:
                # Nessuna strategia attiva su questo asset in questo ciclo
                strategy_signal_block = (
                    "SEGNALE DEL CONSULENTE STRATEGY: nessuna delle 5 strategie ha trovato un\n"
                    "setup attivo su questo asset. Procedi solo sul tuo giudizio autonomo.\n"
                )
        except Exception as _e_ss:
            strategy_signal_block = ""
            self.logger.debug(f"[STRATEGY SIGNAL BLOCK] {ticker_ufficiale}: {_e_ss}")
        # ─────────────────────────────────────────────────────────────────────

        gate_rumore_block = (
            "GATE DI RUMORE — VERIFICA PRELIMINARE OBBLIGATORIA:\n"
            f"  Firma 1 — Movimento senza direzione: kaufman_efficiency = {_ke_now:.2f}  "
            f"→ {'⚠️ RUMORE (sotto 0.25)' if _firma1_rumore else '✅ OK'}\n"
            f"  Firma 2 — Flusso che cambia idea: cvd_30s={_cvd30:+.0f} / cvd_120s={_cvd120:+.0f}  "
            f"→ {'⚠️ RUMORE (segni opposti, valori piccoli)' if _firma2_rumore else '✅ OK'}\n"
            f"  Firma 3 — Volume tiepido: relative_volume = {_rvol_now:.2f}x  "
            f"→ {'⚠️ RUMORE (sotto 0.8x)' if _firma3_rumore else '✅ OK'}\n"
            f"  TOTALE FIRME DI RUMORE: {_n_firme_rumore}/3\n"
        )
        # ─────────────────────────────────────────────────────────────────────

        # ── DATI GERARCHIZZATI v11: FLUSSO CAUSA, PREZZO EFFETTO ─────────────
        # Modifica F (v11): un trader istituzionale legge prima il flusso (chi sta
        # agendo, in che modo, con che intenzione) e POI guarda il prezzo come
        # conseguenza. Il prezzo che si muove SENZA un flusso coerente è un
        # falso breakout. Il flusso che è coerente SENZA che il prezzo si muova
        # ancora è un'opportunità early-entry.
        #
        # Ordine v11:
        #   Livello 1 — IL FLUSSO (chi sta agendo): la causa
        #   Livello 2 — IL PREZZO (cosa risulta): l'effetto
        #   Livello 3 — IL TERRENO (in che ambiente): il contesto

        _dati_flusso = (
            f"  CVD totale: {cvd_istantaneo:+.0f}  (consenso accumulato: chi ha vinto finora)\n"
            f"  CVD ultimi 30s:  {_cvd30:+.0f}  (chi sta vincendo nel breve)\n"
            f"  CVD ultimi 120s: {_cvd120:+.0f}  (continuità del flusso recente)\n"
            f"  CVD acceleration: {float(dati_engine.get('cvd_acceleration', 0) or 0):+.2f}  "
            f"(il flusso sta accelerando o decelerando?)\n"
            f"  Whale delta: {whale_delta:+.2f}  (i grossi cosa stanno facendo)\n"
            f"  Order Flow Imbalance: {ofi:+.3f}  "
            f"({'forte squeeze in atto' if abs(ofi)>0.4 else 'pressione direzionale' if abs(ofi)>0.2 else 'tiepido' if abs(ofi)>0.05 else 'piatto'})\n"
            f"  Aggressività flow: {aggressivita_flow}\n"
            f"  VPIN: {vpin:.2f}  "
            f"({'tossico (un lato schiaccia)' if vpin>0.65 else 'silenzio (nessuno agisce)' if vpin<0.30 else 'normale'})\n"
            f"  Book pressure: {bp:.2f}  "
            f"({'bid pesante (compratori in attesa sotto)' if bp>0.55 else 'ask pesante (venditori in attesa sopra)' if bp<0.45 else 'bilanciato'})\n"
            f"  Book skew: {book_skew:+.3f}  (asimmetria nelle intenzioni nascoste nel book)\n"
            f"  Spoofing: {spoofing:.2f}  (ordini falsi rilevati)\n"
            f"  Iceberg presenti: {iceberg}  (ordini grossi nascosti dietro piccoli)"
        )

        _dati_prezzo = (
            f"  Velocity prezzo: {price_velocity:+.4f}  "
            f"({'esplosivo' if abs(price_velocity)>0.0015 else 'veloce' if abs(price_velocity)>0.0008 else 'medio' if abs(price_velocity)>0.0003 else 'lento' if abs(price_velocity)>0.0001 else 'piatto'})\n"
            f"  is_explosive: {is_explosive}  (movimento esplosivo in atto)\n"
            f"  ATR: {atr:.4f}  ({atr_p:.2f}% del prezzo — ampiezza media barra)\n"
            f"  Z-score VWAP: {float(dati_engine.get('z_score_dist_vwap', 0) or 0):+.2f}  "
            f"(quanto il prezzo è lontano dalla VWAP, in deviazioni standard)\n"
            f"  Spread: {spread_p:.3f}%  (spread bid/ask attuale)"
        )

        _dati_terreno = (
            f"  Hurst: {hurst:.2f}  "
            f"({'TRENDING (sopra 0.55)' if hurst>0.55 else 'MEAN-REVERTING (sotto 0.45)' if hurst<0.45 else 'RANGE / no trend'})\n"
            f"  Kaufman efficiency: {_ke_now:.2f}  "
            f"({'movimento direzionale' if _ke_now>0.40 else 'misto' if _ke_now>0.25 else 'rumore puro'})\n"
            f"  Market regime (HMM): {regime_hmm}\n"
            f"  Relative volume: {_rvol_now:.2f}x media (quanto è 'caldo' il mercato adesso)\n"
            f"  Funding Z-score: {funding_z:+.2f}  "
            f"({'long affollati (squeeze possibile)' if funding_z>1.5 else 'short affollati (squeeze possibile)' if funding_z<-1.5 else 'sentiment normale'})\n"
            f"  Liquidazioni 24h: {liquidazioni_24h:.0f} USD"
        )
        # ─────────────────────────────────────────────────────────────────────

        # ── BLOCCO PORTFOLIO ATTUALE (Lavoro 2 — 2026-05-04) ─────────────────
        # Gemini deve VEDERE il portfolio prima di decidere, non scoprirlo dopo.
        # Prima questo era un controllo a posteriori (RiskManager.check_risk) che
        # bloccava decisioni a posteriori se max 5 posizioni o >=3 nella stessa
        # direzione. Risultato: Gemini sprecava cicli giustificando tesi che
        # poi venivano cancellate. Ora vede tutto e ragiona già con il vincolo.
        # Inoltre può decidere SHORT come hedge naturale quando ha già 3 LONG, ecc.
        portfolio_block = ""
        try:
            _posizioni = (
                self.trade_manager.posizioni_aperte
                if self.trade_manager and hasattr(self.trade_manager, 'posizioni_aperte')
                else {}
            )
            _now = time.time()
            _capitale_impegnato_usd = 0.0
            _esposizione_long_usd = 0.0
            _esposizione_short_usd = 0.0
            _righe_pos = []
            _stesso_asset_aperto = None  # se l'asset corrente è già aperto

            for _ass, _pos in _posizioni.items():
                if not isinstance(_pos, dict):
                    continue
                _dir_p = str(_pos.get('direzione', '?')).upper()
                _entry_p = float(_pos.get('p_entrata', 0) or 0)
                _size_p = float(_pos.get('size', 0) or 0)
                _lev_p = _pos.get('leverage', 1)
                try:
                    _lev_p = int(_lev_p)
                except Exception:
                    _lev_p = 1
                _ap_data = str(_pos.get('data_apertura', ''))
                _sl_p = float(_pos.get('sl', 0) or 0)
                _tp_p = float(_pos.get('tp', 0) or 0)

                # Notional in USD = size_units * prezzo
                _notional_usd = _size_p * _entry_p if _entry_p > 0 else 0
                # Margine impegnato approssimato = notional / leverage
                _margin_usd = _notional_usd / _lev_p if _lev_p > 0 else _notional_usd
                _capitale_impegnato_usd += _margin_usd

                if _dir_p in ('LONG', 'BUY'):
                    _esposizione_long_usd += _notional_usd
                elif _dir_p in ('SHORT', 'SELL'):
                    _esposizione_short_usd += _notional_usd

                # PnL% live se ho il prezzo attuale corrente sull'asset
                _pnl_perc = None
                if _ass == ticker_ufficiale or _ass == asset_name:
                    _stesso_asset_aperto = _pos
                    if _entry_p > 0 and entry_price > 0:
                        if _dir_p in ('LONG', 'BUY'):
                            _pnl_perc = (entry_price - _entry_p) / _entry_p * 100
                        else:
                            _pnl_perc = (_entry_p - entry_price) / _entry_p * 100

                # Tempo aperta
                _ore_aperta = 0.0
                try:
                    _t_open = datetime.fromisoformat(_ap_data.replace('Z', '').replace('T', ' '))
                    _ore_aperta = (datetime.now() - _t_open).total_seconds() / 3600
                except Exception:
                    pass

                _pnl_str = f"PnL {_pnl_perc:+.2f}%" if _pnl_perc is not None else "PnL n/d"
                _righe_pos.append(
                    f"  • {_ass}: {_dir_p} {_lev_p}x | size {_size_p} | "
                    f"entry {_entry_p:.6f} | SL {_sl_p:.6f} TP {_tp_p:.6f} | "
                    f"notional ~{_notional_usd:.2f}$ | aperta da {_ore_aperta:.1f}h | {_pnl_str}"
                )

            _n_pos = len(_posizioni)
            _esp_netta_usd = _esposizione_long_usd - _esposizione_short_usd
            _bilancio = "BILANCIATO" if abs(_esp_netta_usd) < 5 else (
                f"NET LONG (+{_esp_netta_usd:.0f}$)" if _esp_netta_usd > 0 else f"NET SHORT ({_esp_netta_usd:.0f}$)"
            )

            # Capitale account residuo (se disponibile)
            _kraken_balance_str = ""
            try:
                _bal = float(self.get_kraken_balance() or 0)
                if _bal > 0:
                    _kraken_balance_str = f"  Capitale account totale: {_bal:.2f}$\n"
                    if _capitale_impegnato_usd > 0:
                        _pct_impegnato = _capitale_impegnato_usd / _bal * 100
                        _kraken_balance_str += (
                            f"  Margine impegnato (approssimato): {_capitale_impegnato_usd:.2f}$ "
                            f"({_pct_impegnato:.0f}% del capitale)\n"
                        )
            except Exception:
                pass

            # Compila il blocco
            if _n_pos == 0:
                portfolio_block = (
                    "═══════════════════════════════════════════════════════════════\n"
                    "PORTFOLIO ATTUALE\n"
                    "═══════════════════════════════════════════════════════════════\n"
                    "Nessuna posizione aperta. Capitale interamente disponibile.\n"
                    f"{_kraken_balance_str}"
                )
            else:
                _alert_lines = []
                if _stesso_asset_aperto is not None:
                    _dir_sa = _stesso_asset_aperto.get('direzione', '?')
                    _alert_lines.append(
                        f"⚠️ HAI GIÀ UNA POSIZIONE APERTA SU {ticker_ufficiale} ({_dir_sa}). "
                        f"NON puoi aprirne una seconda — la tua decisione qui è SOLO di valutazione "
                        f"posizione esistente (TIENI/CHIUDI/TIGHTEN_SL gestiti separatamente)."
                    )
                if _n_pos >= 5:
                    _alert_lines.append(
                        f"⚠️ LIMITE PORTAFOGLIO: Già {_n_pos}/5 posizioni aperte. "
                        f"Nuove entry vengono bloccate dal RiskManager. La tua decisione "
                        f"qui dovrebbe essere FLAT salvo motivo eccellente per chiudere "
                        f"un'altra posizione."
                    )
                # Conta posizioni stessa direzione
                _n_long = sum(1 for p in _posizioni.values() if str(p.get('direzione','')).upper() in ('LONG','BUY'))
                _n_short = sum(1 for p in _posizioni.values() if str(p.get('direzione','')).upper() in ('SHORT','SELL'))
                if _n_long >= 3:
                    _alert_lines.append(
                        f"⚠️ ESPOSIZIONE DIREZIONALE: già {_n_long} posizioni LONG aperte. "
                        f"Una nuova LONG aumenterebbe il rischio correlazione. "
                        f"Considera SHORT come hedge naturale o FLAT."
                    )
                if _n_short >= 3:
                    _alert_lines.append(
                        f"⚠️ ESPOSIZIONE DIREZIONALE: già {_n_short} posizioni SHORT aperte. "
                        f"Una nuova SHORT aumenterebbe il rischio correlazione. "
                        f"Considera LONG come hedge naturale o FLAT."
                    )
                _alert_str = ""
                if _alert_lines:
                    _alert_str = "\n".join(_alert_lines) + "\n\n"

                portfolio_block = (
                    "═══════════════════════════════════════════════════════════════\n"
                    "PORTFOLIO ATTUALE — vedi tutto prima di decidere\n"
                    "═══════════════════════════════════════════════════════════════\n"
                    f"Posizioni aperte: {_n_pos}/5 (max sistema)\n"
                    f"Esposizione LONG: {_esposizione_long_usd:.2f}$ ({_n_long} pos)\n"
                    f"Esposizione SHORT: {_esposizione_short_usd:.2f}$ ({_n_short} pos)\n"
                    f"Esposizione netta: {_bilancio}\n"
                    f"{_kraken_balance_str}"
                    f"\nDettaglio posizioni:\n"
                    + "\n".join(_righe_pos) + "\n\n"
                    f"{_alert_str}"
                    "REGOLE PORTAFOGLIO (le sai già, ma rinfresco):\n"
                    "  • Max 5 posizioni aperte simultanee. Limite hard del sistema.\n"
                    "  • Max 3 posizioni nella stessa direzione (rischio correlazione).\n"
                    "  • Una posizione per asset (no doppia entry sullo stesso ticker).\n"
                    "  • Capitale piccolo: se margine impegnato >70%, valuta FLAT anche\n"
                    "    su voto alto — non c'è cushion per gestire drawdown.\n\n"
                )
        except Exception as _e_port:
            self.logger.debug(f"[portfolio_block] {ticker_ufficiale}: {_e_port}")
            portfolio_block = ""
        # ─────────────────────────────────────────────────────────────────────

        prompt = (
            "Sei un trader esperto. Davanti a te hai un asset in un momento specifico del mercato.\n"
            "Il tuo compito NON è compilare una scheda. È RAGIONARE su cosa sta succedendo e DECIDERE.\n"
            "Un trader vero sa che la maggior parte dei minuti, la risposta corretta è: NON FARE NULLA.\n"
            "Solo entra quando hai una tesi chiara, un livello di invalidazione preciso, e una conferma.\n\n"

            f"=== ASSET: {ticker_ufficiale} | PREZZO ATTUALE: {entry_price} ===\n\n"

            f"{macro_thesis_block}\n"

            f"{portfolio_block}"

            f"{asset_profile_block}\n"

            "═══════════════════════════════════════════════════════════════\n"
            "STEP 0 — IL MERCATO È LEGGIBILE? (gate da passare PRIMA di tutto)\n"
            "═══════════════════════════════════════════════════════════════\n"
            f"{gate_rumore_block}\n"
            "Regola del gate:\n"
            "  • 2 o 3 firme di rumore → il mercato NON è leggibile. La risposta è FLAT, voto 0.\n"
            "    Non procedere con l'analisi narrativa. Compila comunque score_breakdown\n"
            "    (sono valori bassi, ma compila per audit).\n"
            "  • 1 firma di rumore → procedi con CAUTELA, voto massimo 7.\n"
            "  • 0 firme di rumore → procedi normalmente.\n\n"

            "═══════════════════════════════════════════════════════════════\n"
            "STEP 1 — DATI IN ORDINE DI CAUSALITÀ (flusso → prezzo → terreno)\n"
            "═══════════════════════════════════════════════════════════════\n\n"

            "🟦 LIVELLO 1 — IL FLUSSO (CHI sta agendo, e con quale intenzione):\n"
            f"{_dati_flusso}\n\n"

            "🟩 LIVELLO 2 — IL PREZZO (cosa risulta dal flusso):\n"
            f"{_dati_prezzo}\n\n"

            "🟨 LIVELLO 3 — IL TERRENO (in quale ambiente avviene tutto):\n"
            f"{_dati_terreno}\n\n"

            "REGOLA D'ORO (cambio di mentalità v11):\n"
            "  Il FLUSSO è la CAUSA. Il PREZZO è l'EFFETTO. Sempre, in questo ordine.\n"
            "  Un trader istituzionale legge prima cosa stanno facendo gli operatori (flusso),\n"
            "  e SOLO DOPO guarda cosa sta facendo il prezzo. Se il prezzo si muove SENZA un\n"
            "  flusso coerente → falso breakout, niente trade. Se il flusso è chiaramente\n"
            "  direzionale MA il prezzo non si è ancora mosso → potenziale early entry.\n"
            "  Il TERRENO inquadra: ti dice se il setup ha senso nel contesto più grande.\n"
            "  Niente movimento di flusso direzionale = niente trade, qualunque sia il prezzo.\n\n"

            "═══════════════════════════════════════════════════════════════\n"
            "STEP 2 — STRUTTURA, LIVELLI, CONTESTO ESTESO\n"
            "═══════════════════════════════════════════════════════════════\n\n"

            f"--- STRUTTURA MULTI-TIMEFRAME ---\n{narrativa_strutturale}\n\n"
            f"--- DOVE SEI NEI LIVELLI ---\n{mappa_livelli}\n\n"
            f"--- CICLO DI 90 GIORNI ---\n{_contesto_strutturale}\n\n"
            f"--- CONTESTO GLOBALE (BTC, ETH, macro) ---\n{contesto_globale_block}\n\n"
            f"--- PATTERN STORICI ATTIVI ---\n{pattern_block}\n\n"
            f"--- SIGNAL ENGINE (decisioni pre-elaborate) ---\n{sse_block}\n\n"
            f"--- STORIA ULTIMI 60 MIN DEL PREZZO ---\n{storia_mercato_block}\n\n"

            f"{strategy_signal_block}\n"

            "═══════════════════════════════════════════════════════════════\n"
            "STEP 3 — LE TUE DECISIONI PRECEDENTI SU QUESTO ASSET (memoria)\n"
            "═══════════════════════════════════════════════════════════════\n"
            f"{gemini_self_memory_block}\n\n"

            f"{pending_scenarios_block}\n"

            f"{autodiagnosi_block}\n\n"

            f"--- OSSERVAZIONI STORICHE SUL COMPORTAMENTO PASSATO (per contesto) ---\n"
            f"{memoria_reale}\n"
            f"(Queste sono osservazioni descrittive, non vincoli. Valuta tu se sono "
            f"applicabili al setup ATTUALE o se il contesto è cambiato.)\n\n"

            f"--- LEVA DISPONIBILE SU KRAKEN ---\n"
            f"Leve permesse: {allowed_levs} | Massimo: {max_lev_kraken}x\n\n"

            "═══════════════════════════════════════════════════════════════\n"
            "STEP 4 — RISK-FIRST: identifica il livello di invalidazione PRIMA della direzione\n"
            "═══════════════════════════════════════════════════════════════\n\n"

            "Un trader istituzionale NON parte mai dall'idea ('voglio entrare LONG'). Parte dal\n"
            "RISCHIO ('quanto posso perdere se entro qui? quel rischio è giustificato dal payoff?').\n\n"

            "Procedura obbligatoria, in QUESTO ordine:\n\n"

            "  1. Guarda la struttura tecnica intorno al prezzo attuale: supporti, resistenze,\n"
            "     FVG, OB, livelli weekly/daily. Identifica il livello tecnico CHIARO più vicino\n"
            "     che, se rotto, invaliderebbe ogni tesi direzionale che potresti formulare.\n"
            "     Quello è il tuo punto di invalidazione naturale.\n\n"

            "  2. Calcola la distanza in % tra prezzo attuale e quel livello = RISCHIO_PCT.\n"
            "     Esempio: prezzo 85.40, supporto chiaro a 84.90 → RISCHIO_PCT = 0.59%.\n\n"

            "  3. Cerca il livello tecnico opposto (dove la tesi sarebbe confermata).\n"
            "     Calcola la distanza in % = PAYOFF_PCT.\n"
            "     Esempio: resistenza chiara a 87.20 → PAYOFF_PCT = 2.11%.\n\n"

            "  4. Calcola asimmetria: ASIMMETRIA_RR = PAYOFF_PCT / RISCHIO_PCT.\n"
            "     Nell'esempio: 2.11 / 0.59 = 3.58. → asimmetria sufficiente.\n\n"

            "  5. INDICAZIONE: se ASIMMETRIA_RR < 1.0, valuta con cautela ma decidi TU\n"
            "     in base al setup completo. Non c'è regola hard di blocco.\n\n"

            "  6. Procedi al ragionamento direzionale (STEP 5) sempre.\n\n"

            "═══════════════════════════════════════════════════════════════\n"
            "STEP 5 — CALIBRATION CHECK: scoperta dei contro-argomenti PRIMA del voto\n"
            "═══════════════════════════════════════════════════════════════\n\n"

            "Le AI tendono a dare voti gonfiati per essere 'utili'. Ti costringo a un test:\n\n"

            "  • Se intendi dare voto >= 8 → DEVI scrivere 3 motivi specifici e plausibili per\n"
            "    cui questa idea potrebbe essere SBAGLIATA. Se non riesci a trovare 3 motivi\n"
            "    seri, il tuo voto è troppo alto: riducilo a 6.\n\n"

            "  • Se intendi dare voto = 7 → DEVI elencare 2 cose che dovrebbero accadere PRIMA\n"
            "    dell'entry per confermare la tesi. Se quelle 2 cose non sono già accadute,\n"
            "    sei in attesa, NON in azione. → direzione = FLAT, motiva con 'attendo conferme'.\n\n"

            "  • Se voto <= 6: la convinzione è insufficiente per assumere rischio direzionale.\n"
            "    → direzione = FLAT, salvo asimmetria R:R > 4.0 (in tal caso voto 6 + entry\n"
            "    accettabile su carta perché il payoff giustifica anche la convinzione media).\n\n"

            "═══════════════════════════════════════════════════════════════\n"
            "STEP 6 — RAGIONA, NARRA, DECIDI E FORMULA SCENARI\n"
            "═══════════════════════════════════════════════════════════════\n\n"

            "Ultima cosa fondamentale da capire — i trader istituzionali NON pensano\n"
            "in 'compro adesso o non compro adesso'. Pensano in SCENARI:\n"
            "  • Scenario principale: il più probabile dato lo stato attuale.\n"
            "    Specifichi il TRIGGER ('se prezzo passa X con volume Y') e l'azione conseguente.\n"
            "  • Scenario alternativo: il secondo più probabile (di solito specchio).\n"
            "  • Scenario no-trade: la condizione in cui aspetti senza agire.\n\n"
            "Quando compili il JSON, DEVI sempre formulare i 3 scenari, anche se la tua\n"
            "decisione attuale è FLAT. Gli scenari restano in memoria e ai cicli successivi\n"
            "li puoi verificare: se uno è scattato, esegui. Se la situazione è cambiata\n"
            "radicalmente, formuli scenari nuovi. Questo è il modo professionale di operare.\n\n"
            "Il campo 'asset_situation_now' ti dice in QUALE scenario sei ORA, e quindi\n"
            "che azione devi prendere subito (o aspettare).\n\n"

            "Devi compilare il JSON nell'ordine sotto. La narrativa viene PRIMA, perché\n"
            "il ragionamento DEVE precedere la conclusione, non giustificarla a posteriori.\n\n"

            "NARRATIVA SU 3 ORIZZONTI TEMPORALI (Modifica v11):\n"
            "Un trader istituzionale non ragiona su un solo orizzonte. Ragiona simultaneamente\n"
            "su intraday, daily, strutturale — e cerca la coerenza tra i tre. Devi quindi\n"
            "produrre TRE narrative separate, ognuna con il suo focus:\n\n"

            "  • narrativa_intraday (orizzonte: prossimi 30-60 minuti)\n"
            "    Guarda: velocity, OFI, CVD ultimi 30s/120s, book pressure, is_explosive.\n"
            "    Domande: 'cosa accadrà nei prossimi 30 minuti? Quale flusso domina ora?'\n\n"

            "  • narrativa_daily (orizzonte: sessione attuale, ~1-8 ore)\n"
            "    Guarda: VWAP e Z-score VWAP, struttura H1, ATR, range della giornata.\n"
            "    Domande: 'qual è il narrativo della giornata? Stiamo trendando, ricavo,\n"
            "    o consolidando? In che fase della sessione siamo?'\n\n"

            "  • narrativa_strutturale (orizzonte: 1-5 giorni)\n"
            "    Guarda: HA daily, ciclo, supporti/resistenze weekly, regime, Hurst.\n"
            "    Domande: 'in che setup più grande siamo? Reazione a un livello daily?\n"
            "    Continuazione di trend? Distribuzione/accumulazione?'\n\n"

            "REGOLA DI COERENZA (importante):\n"
            "  • Le 3 narrative ALLINEATE (tutte LONG o tutte SHORT) → puoi dare voto 8-9.\n"
            "  • Una narrativa contraddice le altre → voto massimo 7.\n"
            "  • Più narrative contraddittorie → FLAT.\n"
            "  • Quando intraday e daily sono allineate ma strutturale è contraria → è uno\n"
            "    SCALPING contro trend (voto max 7, stop stretto, durata corta).\n"
            "  • Quando intraday è opposta ma daily+strutturale allineate → attendere\n"
            "    riallineamento intraday (FLAT, scenario pendente).\n\n"

            "DOMANDE GENERICHE (a cui ogni narrativa deve dare risposta):\n"
            "  1. Cosa sta succedendo a quell'orizzonte temporale?\n"
            "  2. Chi sta dominando il flusso (a quell'orizzonte)?\n"
            "  3. Il movimento può continuare, o è esaurito/in inversione?\n\n"

            "Sii concreto. Non scrivere 'segnali misti'. Scrivi: 'intraday: CVD positivo\n"
            "ma velocity lenta — compratori senza urgenza. Daily: prezzo sopra VWAP, trend\n"
            "verde dalla mattina. Strutturale: HA daily verde streak 3, in zona di breakout\n"
            "su 87.20. Le tre narrative sono allineate verso LONG.'\n\n"

            "Per stop_logico e target_logico: dammi i livelli di PREZZO REALE che\n"
            "invaliderebbero o confermerebbero la tua tesi, NON percentuali generiche.\n"
            "Esempio: se sei LONG a 84.50, stop_logico potrebbe essere 84.10 (sotto il\n"
            "minimo locale) e target_logico 86.00 (resistenza H1). Se direzione=FLAT, metti 0.\n\n"

            "RISPONDI ESCLUSIVAMENTE IN JSON PURO (niente markdown, niente backticks):\n"
            "\n"
            "⚠️ CAMPI CRITICI — sono i PRIMI 2 perché spesso vengono dimenticati:\n"
            "  • condizioni_tesi: OBBLIGATORIO se direzione != FLAT (Watchdog v12)\n"
            "  • apprendimento_critico: SEMPRE compilato, anche se direzione=FLAT\n"
            "\n"
            "{\n"
            '  "condizioni_tesi": {\n'
            '    "invalidata_se_diventa_vero": [\n'
            '      {"campo": "<nome_campo>", "operatore": "<|<=|>|>=|==|!=", "valore": <number>, "descrizione": "1 frase"},\n'
            '      ...altre 1-4 condizioni...\n'
            '    ]\n'
            '  },  // OBBLIGATORIO se direzione != FLAT — usato dal Trade Watchdog (v12)\n'
            '       // per chiudere il trade se la tesi viene invalidata in tempo reale.\n'
            '       // Nomi campi VALIDI: cvd_istantaneo, cvd_delta_30s, cvd_delta_120s,\n'
            '       // cvd_acceleration, price_velocity, z_score_dist_vwap, book_pressure,\n'
            '       // book_skew, order_flow_imbalance, vpin, kaufman_efficiency,\n'
            '       // hurst_exponent, is_explosive, atr, whale_delta, spoofing_score.\n'
            '       // Esempio per LONG: [{"campo":"cvd_delta_120s","operatore":"<","valore":-3000,"descrizione":"CVD lungo si inverte"},\n'
            '       //                    {"campo":"z_score_dist_vwap","operatore":"<","valore":-0.5,"descrizione":"prezzo torna sotto VWAP"}]\n'
            '       // Min 2 condizioni, max 5. Devono essere SPECIFICHE alla tua tesi, non generiche.\n'
            '       // Se direzione=FLAT, metti {} o ometti.\n'
            '  "apprendimento_critico": "OBBLIGATORIO sempre — una frase concreta che ricorda cosa\n'
            '                            è stato osservato in QUESTO setup specifico (es: \\"VPIN tossico con\n'
            '                            CVD positivo è precursore di breakout istituzionale\\"). Non frasi\n'
            '                            generiche tipo \\"attenzione al rischio\\". Se non hai osservazioni\n'
            '                            specifiche, scrivi \\"setup standard, nessun pattern peculiare\\".",\n'
            '  "gate_rumore": {\n'
            '    "n_firme_rumore": 0|1|2|3,\n'
            '    "verdetto": "LEGGIBILE|CAUTELA|RUMORE",\n'
            '    "spiegazione": "una frase su quali firme hai visto"\n'
            "  },\n"
            '  "pattern_microstrutturali": {\n'
            '    "absorbing": "yes|no|unclear — descrizione di 1 frase",\n'
            '    "exhaustion": "yes|no|unclear — descrizione di 1 frase"\n'
            "  },\n"
            '  "risk_assessment": {\n'
            '    "livello_invalidazione": <prezzo del supporto/resistenza chiave>,\n'
            '    "rischio_pct": <distanza % dal prezzo>,\n'
            '    "livello_target": <prezzo del livello opposto>,\n'
            '    "payoff_pct": <distanza % dal prezzo>,\n'
            '    "asimmetria_RR": <payoff_pct / rischio_pct>\n'
            "  },\n"
            '  "calibration_check": {\n'
            '    "motivi_potrei_sbagliare": ["motivo 1", "motivo 2", "motivo 3"],  // se voto>=8, OBBLIGATORI\n'
            '    "conferme_richieste_prima_entry": ["conferma 1", "conferma 2"],  // se voto=7\n'
            '    "verdetto_calibrazione": "voto giustificato | voto ridotto | attendo conferme | convinzione insufficiente"\n'
            "  },\n"
            '  "narrativa_intraday": "2-3 frasi su cosa sta accadendo nei prossimi 30-60 min",\n'
            '  "narrativa_daily": "2-3 frasi su cosa sta accadendo oggi (sessione)",\n'
            '  "narrativa_strutturale": "2-3 frasi sul setup multi-day in cui siamo",\n'
            '  "coerenza_orizzonti": "ALLINEATE | INTRADAY_DIVERGE | DAILY_DIVERGE | STRUTTURALE_DIVERGE | TUTTE_DIVERSE",\n'
            '  "narrativa_mercato": "1-2 frasi di sintesi delle 3 narrative sopra (per back-compat)",\n'
            '  "ragionamento_decisione": "se entri, qual è la tesi precisa. Se non entri, perché",\n'
            '  "confronto_decisioni_passate": "le tue decisioni passate (vedi STEP 3) ti dicono qualcosa? Cosa è simile o diverso? Se non hai decisioni passate rilevanti, scrivi \\"nessun parallelo significativo\\"",\n'
            '  "fase_autodiagnosi_rispettata": "spiega in 1 frase come hai adattato la decisione alla tua FASE attuale (TROUBLE/NORMAL/HOT/INSUFFICIENT)",\n'
            '  "scenario_principale": {\n'
            '    "trigger": "descrizione testuale del trigger es. \\"se prezzo passa 85.30 con vol > 1.5x media\\"",\n'
            '    "azione": "es. \\"LONG voto 8 stile MOMENTUM\\"",\n'
            '    "stop_logico": <prezzo>,\n'
            '    "target_logico": <prezzo>,\n'
            '    "asimmetria_RR": <number>\n'
            "  },\n"
            '  "scenario_alternativo": {\n'
            '    "trigger": "trigger condizionale alternativo es. \\"se rejection 85.30 con CVD negativo\\"",\n'
            '    "azione": "es. \\"SHORT voto 7 stile MOMENTUM\\"",\n'
            '    "stop_logico": <prezzo>,\n'
            '    "target_logico": <prezzo>,\n'
            '    "asimmetria_RR": <number>\n'
            "  },\n"
            '  "scenario_no_trade": {\n'
            '    "condizione": "quando NON fare nulla es. \\"se prezzo resta in range 85.00-85.30 a volume basso\\"",\n'
            '    "motivo": "perché aspettare in quel caso"\n'
            "  },\n"
            '  "asset_situation_now": "in QUALE dei 3 scenari sopra ti trovi ADESSO. Se nessuno scatta ancora, scrivi \\"in scenario_no_trade — attendo trigger\\"",\n'
            '  "direzione": "LONG|SHORT|FLAT",\n'
            '  "voto": 0..10,\n'
            '  "stile_operativo": "SCALPING|MOMENTUM|SWING",\n'
            '  "stop_logico": <prezzo>,\n'
            '  "target_logico": <prezzo>,\n'
            '  "razionale": "max 10 parole — sintesi telegrafica",\n'
            '  "score_breakdown": {\n'
            '    "Order_Flow":   0..10,\n'
            '    "Liquidity":    0..10,\n'
            '    "Market_Regime": 0..10,\n'
            '    "Velocity":     0..10,\n'
            '    "Volatility":   0..10\n'
            "  },\n"
            # FIX 2026-05-09 (Schema-Strict-Block): struttura ESPLICITA dei due
            # campi che prima erano descritti solo come "object con N chiavi".
            # Senza esempio, Gemini in modalità legacy nidificava conteggio_confluenza
            # dentro scansione_dimensioni e metteva dict invece di stringhe →
            # validazione Pydantic falliva → falso FLAT/voto=0 → blocco trade.
            '  "scansione_dimensioni": {\n'
            '    "Order_Flow":    "frase max 30 char su cosa vedi",\n'
            '    "Liquidity":     "frase max 30 char su cosa vedi",\n'
            '    "Market_Regime": "frase max 30 char su cosa vedi",\n'
            '    "Velocity":      "frase max 30 char su cosa vedi",\n'
            '    "Volatility":    "frase max 30 char su cosa vedi",\n'
            '    "Macro":         "frase max 30 char su cosa vedi",\n'
            '    "Pattern":       "frase max 30 char su cosa vedi"\n'
            "  },\n"
            '  "conteggio_confluenza": {\n'
            '    "long":   <int 0-7>,\n'
            '    "short":  <int 0-7>,\n'
            '    "neutro": <int 0-7>,\n'
            '    "veto":   <int 0-7>\n'
            "  }\n"
            "}\n\n"

            "PROMEMORIA FINALE:\n"
            "  • FLAT è una decisione VALIDA e spesso CORRETTA. Niente premio per entrare.\n"
            "  • Se gate_rumore.n_firme_rumore >= 3 → considera FLAT, ma decidi tu.\n"
            "  • Se asimmetria_RR < 1.0 → considera con cautela, ma decidi tu.\n"
            "  • Se sei in fase TROUBLE → voto massimo 7, no SWING.\n"
            "  • Se voto >= 8 senza 3 motivi seri di possibile errore → riduci a 6.\n"
            "  • Se la tua narrativa contiene 'forse', 'potrebbe', 'aspetto conferma' → FLAT.\n"
            "  • Compila SEMPRE score_breakdown con valori reali, anche se direzione=FLAT.\n"
            "  • [v11] Se le 3 narrative (intraday/daily/strutturale) sono TUTTE_DIVERSE → FLAT.\n"
            "  • [v11] Voto 8-9 SOLO se le 3 narrative sono ALLINEATE.\n"
            "  • [v11] Il FLUSSO è la causa, il PREZZO è l'effetto. Niente flusso direzionale = niente trade.\n"
            "  • [v12] Se direzione != FLAT, DEVI compilare condizioni_tesi.invalidata_se_diventa_vero\n"
            "    con 2-5 condizioni numeriche specifiche. Il Watchdog le userà per chiudere il trade\n"
            "    se la tesi viene invalidata in tempo reale. NON essere generico — pensa a cosa,\n"
            "    PER QUESTA tesi specifica, dimostrerebbe che ti sei sbagliato.\n"
        )


        # ── COOLDOWN GEMINI: se in cooldown, uso classifier locale ──────────
        # Lavoro 4 (2026-05-04): prima il bot ritornava FLAT cieco per 180s.
        # Ora il classifier locale dà comunque un parere (max voto 7, conservativo).
        # Niente buchi di analisi durante quota cooldown.
        if getattr(self, '_gemini_quota_until', 0) > time.time():
            _resta_s = self._gemini_quota_until - time.time()
            self.logger.warning(
                f"⏳ [{ticker_ufficiale}] Gemini in cooldown ({_resta_s:.0f}s residui) "
                f"— uso classifier locale di backup"
            )
            response_json = self._classifier_locale_fallback(ticker_ufficiale, dati_engine)
        else:
            response_json = self.chiama_gemini(prompt)
        voti_chimera = response_json.get('score_breakdown', {}) if isinstance(response_json, dict) else {}
        
        if voti_chimera:
            self.logger.info(f"📊 MATRICE DECISIONALE CHIMERA [{ticker_ufficiale}]:")
            for k, v in voti_chimera.items():
                self.logger.info(f"   ● {k.replace('_', ' ')}: {v}/10")

        
        raw_to_validate = json.dumps(response_json) if isinstance(response_json, dict) else response_json
        decision = self.error_handler.validate_ia_output(raw_to_validate)

        # Salva subito score_breakdown e catena_di_pensiero nel decision
        # così fluiscono nel chimera_snapshot quando il trade viene aperto
        if voti_chimera and 'score_breakdown' not in decision:
            decision['score_breakdown'] = voti_chimera
        if isinstance(response_json, dict):
            # Compatibilità retro: catena_di_pensiero vecchio schema
            cot = response_json.get('catena_di_pensiero', '')
            if cot and not decision.get('catena_di_pensiero'):
                decision['catena_di_pensiero'] = cot
            # Compatibilità retro: scansione_dimensioni / conteggio_confluenza
            scansione_dim = response_json.get('scansione_dimensioni', {})
            conteggio = response_json.get('conteggio_confluenza', {})
            if scansione_dim:
                decision['scansione_dimensioni'] = scansione_dim
            if conteggio:
                decision['conteggio_confluenza'] = conteggio
            # Nuovi campi v9 (narrativa-first + gate rumore)
            for _k_new in ('gate_rumore', 'narrativa_mercato',
                           'ragionamento_decisione', 'confronto_decisioni_passate',
                           'stop_logico', 'target_logico',
                           # Nuovi campi v10 (risk-first + calibration + autodiag)
                           'risk_assessment', 'calibration_check',
                           'fase_autodiagnosi_rispettata',
                           # Nuovi campi v10 — scenari condizionali
                           'scenario_principale', 'scenario_alternativo',
                           'scenario_no_trade', 'asset_situation_now',
                           # Nuovi campi v11 — pattern + 3 orizzonti
                           'pattern_microstrutturali',
                           'narrativa_intraday', 'narrativa_daily',
                           'narrativa_strutturale', 'coerenza_orizzonti',
                           # Nuovo campo v12 — condizioni di invalidazione tesi (Watchdog)
                           'condizioni_tesi'):
                _v_new = response_json.get(_k_new)
                if _v_new is not None and _k_new not in decision:
                    decision[_k_new] = _v_new
            # Se Gemini ha scritto narrativa ma non catena_di_pensiero, usa la narrativa
            if not decision.get('catena_di_pensiero') and decision.get('narrativa_mercato'):
                decision['catena_di_pensiero'] = decision['narrativa_mercato']

        # ── GATE DI RUMORE — enforcement deterministico ──────────────────────
        # Se il pre-calcolo ha trovato 2+ firme di rumore E Gemini ha comunque
        # provato a entrare (errore di compliance al gate), forziamo FLAT.
        # Questo è il safety net: il gate è già nel prompt, ma garantiamo
        # comunque che il rumore conclamato non porti mai a un trade.
        # SBLOCCO: solo log informativo, niente forzatura FLAT
        if _n_firme_rumore >= 3 and decision.get('direzione', 'FLAT') in ('LONG','SHORT','BUY','SELL'):
            self.logger.info(
                f"ℹ️ [GATE RUMORE info] {ticker_ufficiale}: {_n_firme_rumore}/3 firme di rumore "
                f"(KE={_ke_now:.2f}, RVol={_rvol_now:.2f}x) — Gemini ha deciso {decision.get('direzione')} voto {decision.get('voto',0)}, lasciato passare."
            )
        # ─────────────────────────────────────────────────────────────────────

        # ── ENFORCEMENT ASIMMETRIA R:R MINIMA (Modifica C deterministica) ────
        # Se Gemini ha calcolato risk_assessment ma con asimmetria<2.0,
        # forziamo FLAT. È la versione hard della regola "risk-first": setup
        # simmetrici sono perdenti nel lungo periodo, qualunque sia il voto.
        # SBLOCCO: solo log informativo, niente forzatura FLAT
        if decision.get('direzione', 'FLAT') in ('LONG','SHORT','BUY','SELL'):
            try:
                _ra = decision.get('risk_assessment') or {}
                _rr = float(_ra.get('asimmetria_RR', 0) or 0)
                if _rr > 0 and _rr < 1.0:
                    self.logger.info(
                        f"ℹ️ [R:R basso info] {ticker_ufficiale}: "
                        f"R:R={_rr:.2f} — Gemini ha deciso {decision.get('direzione')} voto {decision.get('voto',0)}, lasciato passare."
                    )
            except (TypeError, ValueError):
                pass
        # ─────────────────────────────────────────────────────────────────────

        # ── ENFORCEMENT FASE TROUBLE (Modifica E deterministica) ─────────────
        # Se siamo in fase TROUBLE, voto>7 viene capped a 7 e SWING viene
        # convertito in MOMENTUM. È il safety net per i casi in cui Gemini
        # ignora l'autodiagnosi nel prompt.
        if _fase == 'TROUBLE' and decision.get('direzione', 'FLAT') in ('LONG','SHORT','BUY','SELL'):
            _voto_pre = int(decision.get('voto', 0) or 0)
            _stile_pre = str(decision.get('stile_operativo','') or '').upper()
            if _voto_pre > 7:
                self.logger.info(
                    f"🩹 [FASE TROUBLE] {ticker_ufficiale}: voto {_voto_pre}→7 "
                    f"(WR recente {_wr_stats.get('wr_pct',0):.0f}%)"
                )
                decision['voto'] = 7
            if _stile_pre == 'SWING':
                self.logger.info(
                    f"🩹 [FASE TROUBLE] {ticker_ufficiale}: SWING→MOMENTUM (durata corta)"
                )
                decision['stile_operativo'] = 'MOMENTUM'
        # ─────────────────────────────────────────────────────────────────────

        # SBLOCCO: rimosso blocco TUTTE_DIVERSE — cap voto a 6 invece che FLAT.
        if decision.get('direzione', 'FLAT') in ('LONG','SHORT','BUY','SELL'):
            _coer = str(decision.get('coerenza_orizzonti','') or '').upper()
            _voto_pre_coer = int(decision.get('voto', 0) or 0)
            if _coer == 'TUTTE_DIVERSE':
                if _voto_pre_coer > 6:
                    self.logger.info(
                        f"📐 [COERENZA ORIZZONTI] {ticker_ufficiale}: TUTTE_DIVERSE → cap voto {_voto_pre_coer}→6"
                    )
                    decision['voto'] = 6
            elif _coer in ('INTRADAY_DIVERGE', 'DAILY_DIVERGE', 'STRUTTURALE_DIVERGE'):
                if _voto_pre_coer > 7:
                    self.logger.info(
                        f"📐 [COERENZA ORIZZONTI] {ticker_ufficiale}: {_coer} → voto {_voto_pre_coer}→7"
                    )
                    decision['voto'] = 7
        # ─────────────────────────────────────────────────────────────────────

        # ── ENFORCEMENT BACK-COMPAT NARRATIVA UNIFICATA ──────────────────────
        # Se Gemini ha scritto le 3 narrative v11 ma non quella unificata
        # (back-compat con il resto del sistema), la sintetizziamo noi.
        if not decision.get('narrativa_mercato'):
            _ni = decision.get('narrativa_intraday', '') or ''
            _nd = decision.get('narrativa_daily', '') or ''
            _ns = decision.get('narrativa_strutturale', '') or ''
            if _ni or _nd or _ns:
                _parts = []
                if _ni: _parts.append(f"Intraday: {_ni}")
                if _nd: _parts.append(f"Daily: {_nd}")
                if _ns: _parts.append(f"Strutturale: {_ns}")
                decision['narrativa_mercato'] = " | ".join(_parts)[:600]
        # ─────────────────────────────────────────────────────────────────────

        # ── FALLBACK DETERMINISTICI (fuori dal try di save) ──────────────────
        # Gemini ignora frequentemente alcune istruzioni del prompt e non restituisce
        # campi obbligatori (verificato: 0/22 decisioni del 2-3 maggio avevano
        # condizioni_tesi popolato; apprendimento_critico è 0% dal 21 aprile).
        #
        # I fallback DEVONO scattare PRIMA del save e SEMPRE — non possono essere
        # nascosti dentro `if self.gemini_memory is not None`, altrimenti se il save
        # fallisce o se gemini_memory è None, decision['condizioni_tesi'] resta vuoto
        # e tutto il flusso a valle (Watchdog v12, ML training, lezioni asset) si
        # rompe silenziosamente.
        #
        # FIX 2026-05-04: spostati FUORI dal try, log a livello INFO (visibile).

        _direz = decision.get('direzione', 'FLAT') if isinstance(decision, dict) else 'FLAT'

        # ── Fallback 1: condizioni_tesi (Watchdog v12) ──────────────────────
        try:
            _cond_tesi_attuale = decision.get('condizioni_tesi') if isinstance(decision, dict) else None
            _has_tesi = (
                isinstance(_cond_tesi_attuale, dict)
                and isinstance(_cond_tesi_attuale.get('invalidata_se_diventa_vero'), list)
                and len(_cond_tesi_attuale['invalidata_se_diventa_vero']) > 0
            )
            if _direz in ('LONG','SHORT','BUY','SELL') and not _has_tesi:
                _close = float(dati_engine.get('close') or 1.0)
                _atr = float(dati_engine.get('atr') or _close * 0.005)
                _cvd_thr = 1500.0
                _vel_thr = max(0.0001, _atr / _close * 0.001)
                if _direz in ('LONG','BUY'):
                    _conds = [
                        {"campo": "cvd_delta_120s", "operatore": "<",
                         "valore": -_cvd_thr,
                         "descrizione": f"CVD lungo si inverte negativo (sotto -{_cvd_thr:.0f}$)"},
                        {"campo": "price_velocity", "operatore": "<",
                         "valore": -_vel_thr,
                         "descrizione": f"prezzo accelera al ribasso (velocity < -{_vel_thr:.4f})"},
                        {"campo": "vpin", "operatore": ">",
                         "valore": 0.85,
                         "descrizione": "VPIN entra in zona toxic flow opposto (>0.85)"},
                    ]
                else:  # SHORT/SELL
                    _conds = [
                        {"campo": "cvd_delta_120s", "operatore": ">",
                         "valore": _cvd_thr,
                         "descrizione": f"CVD lungo si inverte positivo (sopra +{_cvd_thr:.0f}$)"},
                        {"campo": "price_velocity", "operatore": ">",
                         "valore": _vel_thr,
                         "descrizione": f"prezzo accelera al rialzo (velocity > +{_vel_thr:.4f})"},
                        {"campo": "vpin", "operatore": ">",
                         "valore": 0.85,
                         "descrizione": "VPIN entra in zona toxic flow opposto (>0.85)"},
                    ]
                if isinstance(decision, dict):
                    decision['condizioni_tesi'] = {
                        "invalidata_se_diventa_vero": _conds,
                        "fonte": "fallback_deterministico_v1"
                    }
                    self.logger.info(
                        f"⚙️ [WATCHDOG] {ticker_ufficiale}: condizioni_tesi mancanti da Gemini "
                        f"→ generate automaticamente ({len(_conds)} condizioni)"
                    )
        except Exception as _e_fb_ct:
            self.logger.warning(f"⚠️ [WATCHDOG fallback condizioni_tesi] {ticker_ufficiale}: {_e_fb_ct}")

        # ── Fallback 2: apprendimento_critico (memoria storica) ─────────────
        # Questo campo deve essere SEMPRE popolato (anche su FLAT) perché viene
        # propagato a storico_trades e usato per costruire error_matrix /
        # success_matrix che alimentano il prompt di Gemini ai cicli successivi.
        # Se è vuoto, il sistema perde memoria delle situazioni.
        try:
            _ap_cri = (decision.get('apprendimento_critico') if isinstance(decision, dict) else '') or ''
            _ap_cri = str(_ap_cri).strip()
            # Considera vuoto anche placeholder generici
            _generici = {
                '', 'none', 'null', 'n/a', 'nessuna', 'nessuno',
                'attenzione al rischio', 'standard', 'setup standard',
                'quota_cooldown', 'api_failure',
            }
            _ap_vuoto = _ap_cri.lower() in _generici or len(_ap_cri) < 20
            if _ap_vuoto and isinstance(decision, dict):
                # Genera apprendimento_critico osservativo dai dati di mercato
                _vpin_v = float(dati_engine.get('vpin', 0) or 0)
                _cvd_v = float(dati_engine.get('cvd_istantaneo', 0) or 0)
                _vel_v = float(dati_engine.get('price_velocity', 0) or 0)
                _hurst_v = float(dati_engine.get('hurst_exponent', 0.5) or 0.5)
                _ha_col = str(dati_engine.get('ha_daily_colore', '?'))
                _ha_str = int(dati_engine.get('ha_daily_streak', 0) or 0)
                _ciclo = str(dati_engine.get('ciclo_fase', '?'))
                _phase = str(dati_engine.get('entry_phase', '?'))

                # Costruisco una stringa descrittiva del setup
                _parts = []
                if _vpin_v > 0.7:
                    _parts.append(f"VPIN tossico ({_vpin_v:.2f})")
                elif _vpin_v < 0.3:
                    _parts.append(f"VPIN basso ({_vpin_v:.2f})")
                if abs(_cvd_v) > 5000:
                    _parts.append(f"CVD {'positivo forte' if _cvd_v > 0 else 'negativo forte'} ({_cvd_v:+.0f})")
                if _hurst_v > 0.6:
                    _parts.append(f"Hurst trending ({_hurst_v:.2f})")
                elif _hurst_v < 0.4:
                    _parts.append(f"Hurst mean-reverting ({_hurst_v:.2f})")
                if _ha_col in ('VERDE','ROSSO') and _ha_str >= 2:
                    _parts.append(f"HA daily {_ha_col.lower()} streak {_ha_str}")
                if _phase and _phase != '?':
                    _parts.append(f"fase SSE {_phase}")
                if _ciclo and _ciclo != '?':
                    _parts.append(f"ciclo {_ciclo}")

                if _parts:
                    _ap_generato = (
                        f"[auto] Setup {_direz}: " + ", ".join(_parts[:5])
                        + f". Voto {decision.get('voto', 0)}."
                    )
                else:
                    _ap_generato = (
                        f"[auto] Setup {_direz} voto {decision.get('voto', 0)} — "
                        f"nessun pattern microstrutturale evidente."
                    )
                decision['apprendimento_critico'] = _ap_generato[:300]
                self.logger.info(
                    f"⚙️ [APPRENDIMENTO] {ticker_ufficiale}: apprendimento_critico "
                    f"mancante da Gemini → generato automaticamente"
                )
        except Exception as _e_fb_ac:
            self.logger.warning(f"⚠️ [fallback apprendimento_critico] {ticker_ufficiale}: {_e_fb_ac}")

        # ── SALVATAGGIO IN MEMORIA PERSISTENTE DI GEMINI ─────────────────────
        # Salviamo SEMPRE — anche le FLAT — perché la storia delle decisioni
        # è ciò che permette a Gemini al ciclo successivo di vedere come
        # ragionava prima e di confrontarsi con se stesso.
        try:
            if self.gemini_memory is not None:
                _tesi_salva = (
                    decision.get('ragionamento_decisione')
                    or decision.get('narrativa_mercato')
                    or decision.get('catena_di_pensiero')
                    or decision.get('razionale')
                    or ''
                )
                _stop_l = decision.get('stop_logico') or 0
                _targ_l = decision.get('target_logico') or 0
                # Modifica A v12 — passiamo anche condizioni_tesi al save
                # I fallback sopra hanno già garantito che _cond_tesi sia popolato
                # se direzione != FLAT.
                _cond_tesi = decision.get('condizioni_tesi') if isinstance(decision, dict) else None

                _decision_id = self.gemini_memory.save_decision(
                    asset=ticker_ufficiale,
                    direzione=decision.get('direzione', 'FLAT'),
                    voto=decision.get('voto', 0),
                    tesi=_tesi_salva,
                    stop_logico=float(_stop_l) if _stop_l else None,
                    target_logico=float(_targ_l) if _targ_l else None,
                    prezzo_entry=float(entry_price) if entry_price else None,
                    trade_id=None,  # popolato dopo se il trade viene aperto
                    condizioni_tesi=_cond_tesi,
                )
                if _decision_id:
                    decision['_gemini_memory_id'] = _decision_id
        except Exception as _e_save:
            self.logger.debug(f"[GEMINI MEMORY SAVE] {ticker_ufficiale}: {_e_save}")
        # ─────────────────────────────────────────────────────────────────────

        # ── SALVATAGGIO SCENARI CONDIZIONALI (Modifica B soft) ───────────────
        # Persistiamo gli scenari principale e alternativo formulati da Gemini
        # così al ciclo successivo possa vedere se uno è scattato. Lo scenario
        # 'no_trade' non viene salvato perché non genera azioni — è solo un
        # commento metodologico.
        try:
            if self.pending_scenarios is not None:
                _entry_for_scen = float(entry_price) if entry_price else 0.0
                for _kind in ('principale', 'alternativo'):
                    _scen = decision.get(f'scenario_{_kind}') or {}
                    if not _scen or not isinstance(_scen, dict):
                        continue
                    _trigger = _scen.get('trigger') or ''
                    _azione = _scen.get('azione') or ''
                    if not _trigger or not _azione:
                        continue
                    _sl_s = _scen.get('stop_logico') or 0
                    _tp_s = _scen.get('target_logico') or 0
                    try:
                        _sl_f = float(_sl_s)
                        _tp_f = float(_tp_s)
                    except (TypeError, ValueError):
                        _sl_f, _tp_f = 0.0, 0.0
                    self.pending_scenarios.save_scenario(
                        asset=ticker_ufficiale, kind=_kind,
                        trigger=_trigger, azione=_azione,
                        stop_logico=_sl_f, target_logico=_tp_f,
                        prezzo_ref=_entry_for_scen
                    )
        except Exception as _e_scen:
            self.logger.debug(f"[PENDING SCENARIOS SAVE] {ticker_ufficiale}: {_e_scen}")
        # ─────────────────────────────────────────────────────────────────────

        # ── VETO DETERMINISTICO POST-GEMINI ──────────────────────────────────
        # Il prompt richiede a Gemini di rispettare la confluenza, ma se per qualche
        # motivo apre comunque dove non dovrebbe, qui lo riportiamo in linea.
        # Questi NON sono filtri qualitativi ma decisioni già prese da engine
        # (FORMAZIONE_VUOTA, mercato tossico, mercato silenzioso, segnali rumore).
        _veto_motivi_eng = []
        _direz_pre_veto = decision.get('direzione', 'FLAT')
        if _direz_pre_veto in ('BUY', 'SELL', 'LONG', 'SHORT'):
            # Veti hard di engine
            if dati_engine.get('phase_override_ok') is False:
                _veto_motivi_eng.append('phase_override_ok=False')
            if dati_engine.get('phase_subtype') == 'VUOTA':
                _veto_motivi_eng.append('phase_subtype=VUOTA')
            if dati_engine.get('is_toxic') is True:
                _veto_motivi_eng.append('is_toxic=True')
            if dati_engine.get('entry_phase') == 'SILENZIO':
                _veto_motivi_eng.append('entry_phase=SILENZIO')
            
            _vpin_v = dati_engine.get('vpin')
            if _vpin_v is not None:
                if _vpin_v < 0.30:
                    _veto_motivi_eng.append(f'vpin={_vpin_v:.2f}<0.30 silenzio')
                elif _vpin_v > 0.65:
                    _veto_motivi_eng.append(f'vpin={_vpin_v:.2f}>0.65 tossico')
            
            _ke_v = dati_engine.get('kaufman_efficiency')
            if _ke_v is not None and _ke_v < 0.15:
                _veto_motivi_eng.append(f'kaufman={_ke_v:.2f}<0.15 rumore')
            
            # Veto SHORT specifico
            if _direz_pre_veto in ('SELL', 'SHORT'):
                if dati_engine.get('short_conditions_met') is False:
                    _veto_motivi_eng.append('short_conditions_met=False')
        
        if _veto_motivi_eng:
            self.logger.info(
                f"ℹ️ [veto info] {ticker_ufficiale}: motivi tecnici ({', '.join(_veto_motivi_eng)}) — "
                f"Gemini ha deciso {_direz_pre_veto} voto={decision.get('voto',0)}, lasciato passare."
            )

        # ── VETO AUDIT-DRIVEN (analisi 718 trade reali — 27/04/2026) ─────────
        # Pattern di errore identificati nel DB di produzione su voto≥9:
        #   E1: LONG con z_vwap>+1.5 e cvd_istantaneo>0 → FOMO chase (16 trade, WR 31%)
        #   E2: SHORT con book_pressure>0.60 e OFI>+0.30 → squeeze imminente
        #   E3: voto≥9 con ATR/close < 0.5% → bassa volatilità, falsi positivi
        # Ogni veto: forza FLAT con motivo tracciato. SWING e INTRADAY gestiti
        # nel blocco di conversione subito sotto.
        _veto_audit = []
        _direz_pre_audit = decision.get('direzione', 'FLAT')
        _voto_pre_audit  = int(decision.get('voto', 0) or 0)
        if _direz_pre_audit in ('BUY', 'SELL', 'LONG', 'SHORT'):
            _is_long_dec  = _direz_pre_audit in ('BUY',  'LONG')
            _is_short_dec = _direz_pre_audit in ('SELL', 'SHORT')

            _zvwap_a = dati_engine.get('z_score_dist_vwap')
            _cvd_a   = dati_engine.get('cvd_istantaneo')
            _bp_a    = dati_engine.get('book_pressure')
            _ofi_a   = dati_engine.get('order_flow_imbalance')
            _atr_a   = dati_engine.get('atr')
            _close_a = dati_engine.get('close')

            # E1: LONG FOMO sopra VWAP con compratori già dominanti
            if _is_long_dec and _zvwap_a is not None and _cvd_a is not None:
                try:
                    if float(_zvwap_a) > 1.5 and float(_cvd_a) > 0:
                        _veto_audit.append(
                            f'E1_LONG_FOMO(z_vwap={float(_zvwap_a):.2f}>1.5,'
                            f'cvd={float(_cvd_a):.0f}>0)'
                        )
                except (ValueError, TypeError):
                    pass

            # E2: SHORT con book bid-heavy e flusso d'ordini bullish
            if _is_short_dec and _bp_a is not None and _ofi_a is not None:
                try:
                    if float(_bp_a) > 0.60 and float(_ofi_a) > 0.30:
                        _veto_audit.append(
                            f'E2_SHORT_BID_HEAVY(bp={float(_bp_a):.2f}>0.60,'
                            f'ofi={float(_ofi_a):.2f}>0.30)'
                        )
                except (ValueError, TypeError):
                    pass

            # E3: voto alto in mercato a bassa volatilità (rumore camuffato da segnale)
            if _voto_pre_audit >= 9 and _atr_a is not None and _close_a:
                try:
                    _atr_pct = float(_atr_a) / float(_close_a)
                    if _atr_pct < 0.005:
                        _veto_audit.append(
                            f'E3_LOW_VOL_HIGH_VOTE(atr_pct={_atr_pct:.4f}<0.005,'
                            f'voto={_voto_pre_audit})'
                        )
                except (ValueError, TypeError, ZeroDivisionError):
                    pass

        if _veto_audit:
            self.logger.info(
                f"ℹ️ [veto audit info] {ticker_ufficiale}: "
                f"{_direz_pre_audit} voto={_voto_pre_audit} — motivi storici ({', '.join(_veto_audit)}), lasciato passare."
            )
        # ─────────────────────────────────────────────────────────────────────

        # ── CONVERSIONE STILE OPERATIVO PERDENTE → MOMENTUM ──────────────────
        # Audit DB 27/04/2026 su 718 trade reali:
        #   INTRADAY: 54 trade, WR 29.6%, PnL_USD -29.48 (peggiore)
        #   SWING:    75 trade, WR 25.3%, PnL_USD -29.43 (con voto≥9: WR 6.2%!)
        #   MOMENTUM: 146 trade, WR 42.5%, PnL +30.28 (positivo)
        # Convertiamo entrambi gli stili perdenti in MOMENTUM. La conversione
        # tocca solo il "vestito" del trade (cap SL/TP, durata) — direzione e
        # voto restano quelli decisi a monte.
        if decision.get('direzione', 'FLAT') != 'FLAT':
            _stile_orig = str(decision.get('stile_operativo', '') or '').upper()
            if _stile_orig in ('INTRADAY', 'SWING'):
                self.logger.info(
                    f"🔄 [{ticker_ufficiale}] tipo_op {_stile_orig} convertito in MOMENTUM "
                    f"(audit storico: stile perdente)"
                )
                decision['stile_operativo'] = 'MOMENTUM'
                decision['razionale'] = (decision.get('razionale','') or '')[:80] + f" | {_stile_orig}→MOMENTUM"
        # ─────────────────────────────────────────────────────────────────────

        # --- PENALITÀ CONTESTO GLOBALE (eventi macro, euforia, funding squeeze) ---
        if _penalita_ctx > 0 and decision.get('direzione', 'FLAT') != 'FLAT':
            old_voto = decision.get('voto', 0)
            decision['voto'] = max(0, old_voto - _penalita_ctx)
            decision['razionale'] += (
                f" | 🌍 Ctx -{_penalita_ctx}: "
                f"evento={_prox_evento}min F&G={_fg_val} Funding={_fund_pct}pct"
            )
            self.logger.warning(
                f"🌍 [{ticker_ufficiale}] Penalità contesto: "
                f"voto {old_voto}→{decision['voto']} (-{_penalita_ctx})"
            )
        # ─────────────────────────────────────────────────────────────────────

        # --- ML PROB_WIN SENTINEL (XGBoost feedback loop) ---
        # Il modello ML ha studiato tutti i trade passati e stima la probabilità di vincita.
        # Con abbastanza trade nel dataset non ignorarlo è la cosa più semplice da fare.
        # Confidenza minima 0.05 per evitare penalità quando il modello non ha abbastanza dati.
        ml_prob_win  = float(dati_engine.get('ml_prob_win', 0.5))
        ml_confidenza = float(dati_engine.get('ml_confidenza', 0.0))

        # ── ML SENTINEL — soglie calibrate sui dati reali ────────────────────
        # Analisi su 328 trade con ml_prob_win disponibile:
        #   Correlazione P(WIN) → esito reale: 0.082 (quasi nulla)
        #   L'unico bucket affidabile: P(WIN) 0.35-0.40 → WR reale 20%
        #   P(WIN) > 0.65 → WR reale 52% (solo marginalmente sopra media 41%)
        #
        # Il modello NON è ancora calibrato per usare le probabilità assolute
        # come soglie. Usiamo solo il segnale più forte (0.35-0.40 → WR 20%)
        # e convertiamo il P(WIN) in informazione per Gemini, non in penalità rigide.
        # Le soglie verranno ricalibrate automaticamente ogni 4 settimane
        # quando avremo più ghost trades nel dataset.
        if ml_confidenza >= 0.10 and decision.get('direzione', 'FLAT') != 'FLAT':
            voto_corrente = int(decision.get('voto', 0))
            if ml_prob_win < 0.38 and ml_confidenza >= 0.20:
                # Unico bucket affidabile: WR reale 20% → penalità -1 (non -3)
                decision['voto'] = max(0, voto_corrente - 1)
                decision['razionale'] += (
                    f" | 🤖 ML WARN: prob_win={ml_prob_win:.2f} (conf={ml_confidenza:.2f}) — voto -1."
                )
                self.logger.warning(
                    f"🤖 [ML SENTINEL] {ticker_ufficiale}: prob_win={ml_prob_win:.2f} < 0.38 "
                    f"(conf={ml_confidenza:.2f}) — voto -{voto_corrente - decision['voto']}."
                )
            elif ml_prob_win >= 0.65 and ml_confidenza >= 0.20:
                # P(WIN) alto con alta confidenza → WR reale 52%, solo +1
                decision['voto'] = min(10, voto_corrente + 1)
                decision['razionale'] += (
                    f" | 🤖 ML BOOST: prob_win={ml_prob_win:.2f} (conf={ml_confidenza:.2f}) — voto +1."
                )
                self.logger.info(
                    f"🤖 [ML SENTINEL] {ticker_ufficiale}: prob_win={ml_prob_win:.2f} ≥ 0.65 "
                    f"(conf={ml_confidenza:.2f}) — voto +1."
                )
            else:
                # Zona intermedia: solo log informativo, nessuna modifica al voto
                self.logger.info(
                    f"🤖 [ML] {ticker_ufficiale}: P(WIN)={ml_prob_win:.2f} conf={ml_confidenza:.2f} "
                    f"— nella zona neutro, voto invariato."
                )
        else:
            self.logger.debug(
                f"🤖 [ML] {ticker_ufficiale}: confidenza ML {ml_confidenza:.3f} < 0.10 — solo osservazione."
            )
        
        # --- AUDIT LOGICA IA (Coerenza Ragionamento) ---
        ok_logica, anomalie_logica = self._audit_ragionamento_ia(ticker_ufficiale, decision, dati_engine)
        if not ok_logica:
            is_critical = any("CRITICAL" in a for a in anomalie_logica)
            if is_critical:
                self.logger.error(f"🚫 [AUDIT CRITICO] {ticker_ufficiale}: Interpretazione IA errata (Trappola ignorata). FORZATURA FLAT.")
                decision['direzione'] = 'FLAT'
                decision['voto'] = 0
                decision['razionale'] = f"ABORT_CRITICAL_AUDIT: {anomalie_logica[0]}"
            else:
                self.logger.warning(f"⚠️ [LOGICA DEBOLE] {ticker_ufficiale}: Ragionamento IA incoerente. Riduzione voto e sizing.")
                decision['voto'] = max(0, int(decision.get('voto', 0)) - 2)
                decision['razionale'] += f" | ⚠️ Audit Logica: {anomalie_logica[0][:30]}..."

        # --- POLICY ADJUST (Hedge Fund Feedback Loop) ---
        decision = self._policy_adjust(ticker_ufficiale, decision, dati_engine)

        if voti_chimera and 'score_breakdown' not in decision:
            decision['score_breakdown'] = voti_chimera

        macro_upper = str(macro_sentiment).upper()
        direzione_ia = decision.get("direzione", "FLAT")
        voto_ia = int(decision.get("voto", 0))

        self.logger.info(f"🧠 ANALISI {ticker_ufficiale} COMPLETATA | Direzione: {direzione_ia} | Voto: {voto_ia} | VPIN: {vpin:.4f} | Z-Score: {z_score:.2f}")

        # Log catena di pensiero — mostra il ragionamento reale di Gemini
        cot = decision.get('catena_di_pensiero', '')
        if cot and direzione_ia != 'FLAT':
            # Tronca a 200 char per leggibilità nel log
            cot_short = cot[:200] + ('...' if len(cot) > 200 else '')
            self.logger.info(f"💭 [{ticker_ufficiale}] Ragionamento: {cot_short}")

        market_regime = dati_engine.get('market_regime', 'NEUTRAL')
        raw_health = dati_engine.get('health_data', {})
        market_health_val = raw_health.get('market_health_index', 1.0) if isinstance(raw_health, dict) else dati_engine.get('market_health', 1.0)
        
        from core.asset_list import ASSET_CONFIG
        conf = ASSET_CONFIG.get(ticker_ufficiale, {})
        # La leva massima è il minimo tra quella definita dall'utente (10x) e quella di Kraken
        user_max_lev = conf.get('max_leverage', 10)
        max_lev_consentita = min(user_max_lev, max_lev_kraken)

        if direzione_ia != "FLAT":
            tipo_op = decision.get('stile_operativo', 'SWING').upper()

            # ───────────────────────────────────────────────────────────────
            # FIX-E (2026-04-26): FILTRO DATA-DRIVEN PER SHORT in MEAN_REVERSION
            # ───────────────────────────────────────────────────────────────
            # Analisi DB ultimi 30 giorni:
            #   SHORT in MEAN_REVERSION: 61 trade, WR 24.6%, PnL -$55.61
            #   LONG in MEAN_REVERSION:  23 trade, WR 56.5%, PnL +$14.75
            # Il filtro pre-esistente "leva ridotta in MEAN_REV" non bastava perché
            # riduceva la leva ma non il sizing né il filtro qualitativo.
            # Strategia: in MEAN_REVERSION + SHORT, alziamo la soglia voto a 8 e
            # tagliamo il sizing del 50%. Sotto voto 8, il trade viene FLATTATO.
            if market_regime == "MEAN_REVERSION" and direzione_ia in ("SELL", "SHORT"):
                if voto_ia < 8:
                    self.logger.warning(
                        f"🚫 [{ticker_ufficiale}] BLOCCO MEAN_REVERSION SHORT: "
                        f"voto={voto_ia} < 8 (storico WR 24.6% PnL -$55.61 su 61 trade) — FLAT"
                    )
                    decision['direzione'] = 'FLAT'
                    decision['voto'] = 0
                    decision['razionale'] = (
                        f"🚫 Bloccato: SHORT in MEAN_REVERSION richiede voto>=8 "
                        f"(eri a {voto_ia}). " + decision.get('razionale', '')
                    )
                    direzione_ia = "FLAT"
                else:
                    # Voto sufficiente ma resta una zona pericolosa: sizing dimezzato
                    decision['sizing'] = round(decision.get('sizing', 1.0) * 0.5, 5)
                    decision['razionale'] += (
                        " | ⚠️ MEAN_REV SHORT: sizing dimezzato (zona storicamente perdente)."
                    )
            # ───────────────────────────────────────────────────────────────

        if direzione_ia != "FLAT":
            tipo_op = decision.get('stile_operativo', 'SWING').upper()

            # Leva basata su voto per TUTTI gli stili — SWING non è automaticamente max leva
            if voto_ia >= 9:
                leva_base = max_lev_consentita
            elif voto_ia >= 8 and market_regime == "TRENDING":
                leva_base = max_lev_consentita
            elif voto_ia >= 7:
                leva_base = min(5, max_lev_consentita)
            else:
                leva_base = min(3, max_lev_consentita)

            # SWING: leva ridotta perché il trade dura di più (più esposizione al rischio overnight)
            if tipo_op == "SWING":
                leva_finale = min(leva_base, min(5, max_lev_consentita))
                decision['razionale'] += f" | 📈 Swing: Leva {leva_finale}x."
            elif market_regime == "MEAN_REVERSION":
                leva_finale = max(2, round(leva_base * 0.6))
                decision['razionale'] += f" | 📉 Mean Rev: Leva difesa a {leva_finale}x."
            elif market_regime == "TRENDING" and voto_ia >= 8:
                leva_finale = max_lev_consentita
                decision['razionale'] += f" | 🚀 Trend Boost: Max Leverage {leva_finale}x."
            else:
                leva_finale = leva_base

            # AUDIT 27/04/2026: cap finale leva voto≥9 a 5x indipendentemente dal regime.
            # Storico voto≥9 con leva alta: 3 trade @ 20x = 0/3 win, 2 trade @ 9x = 0/2 win.
            # La leva amplifica gli errori di FOMO/squeeze che il voto alto camuffa.
            if voto_ia >= 9 and leva_finale > 5:
                self.logger.info(
                    f"🔒 [{ticker_ufficiale}] CAP LEVA voto≥9: {leva_finale}x → 5x "
                    f"(audit: voto≥9 lev>=9x WR 0%)"
                )
                leva_finale = 5
            decision['leverage'] = int(leva_finale)

            if (macro_upper == "BEARISH" and direzione_ia == "BUY") or (macro_upper == "BULLISH" and direzione_ia == "SELL"):
                decision['sizing'] = round(decision.get('sizing', 1.0) * 0.7, 5)
                decision['razionale'] += " | Counter-trend: Sizing -30%."

            if market_health_val < 0.25:
                decision['sizing'] = round(decision.get('sizing', 1.0) * 0.4, 5)
                decision['razionale'] += f" | ⚠️ Health Critical ({market_health_val}): Sizing -60%."
            elif market_health_val < 0.50:
                decision['sizing'] = round(decision.get('sizing', 1.0) * 0.8, 5)
                decision['razionale'] += " | 🟡 Health Weak: Sizing -20%."

            # --- VELOCITY SENTINEL (multi-conferma, no forzatura voto) ---
            # La velocity alta da sola NON è un segnale — deve essere confermata da altri strumenti.
            # Conta quante conferme puntano nella stessa direzione della velocity.
            # Se la velocity è contro la direzione del trade → penalità.
            # Mai alzare il voto: la velocity è un filtro, non un boost.
            if abs(price_velocity) > 0.0006:
                vel_long = price_velocity > 0   # True se velocity rialzista
                trade_long = direzione_ia in ("BUY", "LONG")

                # Conferme breakout reale nella direzione del trade
                conferme_breakout = 0
                if trade_long == vel_long:
                    # CVD concorde
                    if (trade_long and cvd_istantaneo > 500) or (not trade_long and cvd_istantaneo < -500):
                        conferme_breakout += 1
                    # VPIN elevato (flusso reale, non manipolato)
                    if vpin > 0.35:
                        conferme_breakout += 1
                    # OFI concorde
                    if (trade_long and ofi > 0.2) or (not trade_long and ofi < -0.2):
                        conferme_breakout += 1
                    # Whale delta concorde
                    if (trade_long and whale_delta > 0.3) or (not trade_long and whale_delta < -0.3):
                        conferme_breakout += 1

                    if conferme_breakout >= 2:
                        # Breakout confermato da almeno 2 strumenti → solo log, voto invariato
                        decision['sentinel_velocity'] = 'BREAKOUT_CONFERMATO'
                        self.logger.info(
                            f"✅ [VELOCITY SENTINEL] {ticker_ufficiale}: Breakout confermato "
                            f"({conferme_breakout}/4 conferme) | vel={price_velocity:.6f} CVD={cvd_istantaneo:.0f} "
                            f"VPIN={vpin:.2f} OFI={ofi:.2f} whale={whale_delta:.2f}"
                        )
                    else:
                        # Velocity alta ma conferme insufficienti → FAKE BREAKOUT probabile
                        decision['voto'] = max(0, int(decision.get('voto', 0)) - 2)
                        decision['sentinel_velocity'] = 'FAKE_BREAKOUT_SOSPETTO'
                        decision['razionale'] += (
                            f" | ⚠️ VELOCITY SENTINEL: alta vel ma solo {conferme_breakout}/4 conferme "
                            f"(CVD={cvd_istantaneo:.0f} VPIN={vpin:.2f} OFI={ofi:.2f}) — voto -2."
                        )
                        self.logger.warning(
                            f"⚠️ [VELOCITY SENTINEL] {ticker_ufficiale}: Fake breakout sospetto "
                            f"({conferme_breakout}/4 conferme) — voto ridotto a {decision.get('voto')}."
                        )
                else:
                    # Velocity CONTRO la direzione del trade → penalità
                    decision['voto'] = max(0, int(decision.get('voto', 0)) - 2)
                    decision['sentinel_velocity'] = 'VELOCITY_CONTRO_TRADE'
                    decision['razionale'] += (
                        f" | 🔴 VELOCITY SENTINEL: vel ({price_velocity:.6f}) CONTRO direzione {direzione_ia} — voto -2."
                    )
                    self.logger.warning(
                        f"🔴 [VELOCITY SENTINEL] {ticker_ufficiale}: Velocity contro il trade "
                        f"({price_velocity:.6f} vs {direzione_ia}) — voto ridotto a {decision.get('voto')}."
                    )

        direzione_ia = decision.get("direzione", "FLAT")
        voto_ia = int(decision.get("voto", 0))

        # Salva il contesto strutturale nel decision — verrà persistito nel record della posizione
        # e confrontato alla chiusura per valutare la qualità della lettura strutturale
        decision['contesto_strutturale'] = {
            'mappa_livelli':      mappa_livelli,
            'va_position':        ('sopra' if float(dati_engine.get('close',0)) > float(dati_engine.get('vah',0)) > 0
                                   else ('sotto' if float(dati_engine.get('close',0)) < float(dati_engine.get('val',0)) > 0
                                   else 'dentro')),
            'poc_weekly':         float(dati_engine.get('poc', 0)),
            'vah_weekly':         float(dati_engine.get('vah', 0)),
            'val_weekly':         float(dati_engine.get('val', 0)),
            'pivot_daily':        float(dati_engine.get('pivot_daily', 0)),
            'pivot_weekly':       float(dati_engine.get('pivot_weekly', 0)),
            'struttura_h1':       str(dati_engine.get('struttura_h1', '')),
            'multi_tf_snapshot':  {tf: {'trend': d.get('trend_dir',''), 'hurst': round(d.get('hurst',0.5),3)}
                                   for tf, d in multi_tf_raw.items() if isinstance(d, dict)},
            'penalita': {'extra_score': round(_extra_score, 2)},
            'bonus':    0,
            'ts':       __import__('datetime').datetime.now().isoformat(),
        }

        if direzione_ia != "FLAT":
            tp_f, sl_f, _ = self.determina_tp_sl_ts(
                ticker_ufficiale, direzione_ia, entry_price, dati_engine, levels_ia=decision
            )
            # ── Applica moltiplicatore SL per pattern forte ───────────────────
            # I pattern ad alta probabilità vengono distrutti dal VSL troppo stretto.
            # Se siamo in un pattern forte (WR >= 75%), allarghiamo il SL del
            # moltiplicatore calcolato — non lo cambia la logica, lo sposta.
            try:
                _pattern_reali = self._riconosci_pattern(dati_engine, direzione_ia)
                if _pattern_reali:
                    _sl_mult = max(p.get('sl_mult', 1.0) for p in _pattern_reali)
                    if _sl_mult > 1.0 and sl_f and sl_f > 0:
                        dist_sl = abs(entry_price - sl_f)
                        if direzione_ia in ('BUY', 'LONG'):
                            sl_f = entry_price - dist_sl * _sl_mult
                        else:
                            sl_f = entry_price + dist_sl * _sl_mult
                        sl_f = round(sl_f, 6)
                        decision['razionale'] += (
                            f" | 🎯 Pattern {_pattern_reali[0]['nome']} "
                            f"WR={_pattern_reali[0]['wr']}% — SL x{_sl_mult:.2f}"
                        )
                        self.logger.info(
                            f"🎯 [PATTERN SL] {ticker_ufficiale}: {_pattern_reali[0]['nome']} "
                            f"WR={_pattern_reali[0]['wr']}% → SL allargato x{_sl_mult:.2f} → {sl_f:.4f}"
                        )
            except Exception as e_psl:
                self.logger.debug(f"Pattern SL adjust error: {e_psl}")
            # ─────────────────────────────────────────────────────────────────
            decision['sl'], decision['tp'] = sl_f, tp_f
            
            self.logger.info(f"🛡️ SINERGIA CHIMERA {ticker_ufficiale}: SL {sl_f} | TP {tp_f} (Atr: {atr:.4f})")
            
            is_explosive, reason, tp_chimera = self.analizza_fase_due_chimera(ticker_ufficiale, dati_engine, direzione_ia)
            if is_explosive:
                decision['tp'] = tp_chimera
                decision['trailing_stop'] = True
                decision['razionale'] += f" | ⚡ CHIMERA RUN: {reason}"

        # --- GESTIONE RISK MANAGER (Portfolio Correlation & Exposure) ---
        posizioni_aperte = self.trade_manager.posizioni_aperte if self.trade_manager else {}
        ok_risk, msg_risk = self.risk_manager.check_risk(decision, posizioni_aperte=posizioni_aperte, account_limits=self.account_limits)
        
        # Se il rischio è bocciato e il voto non è eccellente (>=8), annulliamo
        if not ok_risk and voto_ia < 8:
            decision['direzione'] = "FLAT"
            decision['razionale'] += f" | BLOCCATO RISK: {msg_risk}"
            
        # Nota: L'alert Telegram è stato rimosso da qui per evitare churning informativo.
        # Verrà inviato solo da bot_la.py in caso di esecuzione reale.

        if market_health < 0.25:
            decision['sizing'] = round(decision.get('sizing', 0) * 0.4, 5)

        return decision

    def _get_technical_narrative(self, dati_engine):
        """Report clinico e binario con etichette temporali esplicite."""
        cvd = float(dati_engine.get('cvd_istantaneo', 0))
        vpin = float(dati_engine.get('vpin', 0))
        velocity = float(dati_engine.get('price_velocity', 0))
        
        # Stato Divergenza
        div = "DIVERGENZA_RILEVATA" if (velocity > 0 and cvd < 0) or (velocity < 0 and cvd > 0) else "CONCORDANTE"
        
        return (
            f"1. FLUSSO [ultimi ~500 trade, 2-30min]: CVD {cvd:.0f} | VPIN {vpin:.2f} | STATO: {div}\n"
            f"2. DINAMICA [15m×100=8ore]: Velocity {velocity:.4f} | Hurst {dati_engine.get('hurst_exponent', 0.5):.2f}\n"
            f"3. MICROSTRUTTURA [istantaneo]: Book Pressure {dati_engine.get('book_pressure', 0.5):.2f} | Spoofing {dati_engine.get('indice_spoofing', 0):.2f}"
        )
        n.append(f"    - Funding Rate: {f_rate:.6f} (Z-Score: {f_z:.2f}) - Tassi pagati dai Long agli Short o viceversa.")
        n.append(f"    - Open Interest Trend: {oi_trend:.2f} - (>0 = Nuovi contratti aperti, <0 = Chiusura posizioni).")
        n.append(f"    - Liquidazioni 24h: {liq_24h:.2f}$ (Z-Score: {liq_z:.2f}) - Misura l'entità degli stop loss colpiti.")
        n.append(f"    - Correlazione con BTC: {corr_btc:.2f} - (1.0 = Segue BTC perfettamente, 0.0 = Indipendente).")
        eth_btc_label = asset_list.CROSS_PAIRS[asset_list.CROSS_ETH_BTC]
        n.append(f"    - {eth_btc_label} Ratio: {eth_btc_ratio:.4f} - Misura la propensione al rischio verso le Altcoin.")
        n.append(f"    - Macro Correlation (Risk): {macro_correlation} - Sentiment generale del mercato crypto.")
        n.append(f"    - Relative Volume Status: {relative_volume_status:.2f}x - Volumi attuali rispetto alla media storica.")
        n.append(f"    - Market Liquidity Warning: {'ON (Rischio Slippage/Manipolazione)' if market_liquidity_warning else 'OFF (Liquidità Sana)'}")
        n.append(f"    - Indice Salute Mercato (Health): {health:.2f} - (>0.6 = Sano, <0.4 = Instabile/Pericoloso).")

        return "\n".join(n)

    def _riconosci_pattern(self, dati_engine, direzione):
        """
        Riconosce se le condizioni attuali corrispondono a uno dei pattern
        ad alta probabilità identificati sull'analisi storica di 395 trade reali.

        Pattern validati (condizione non_VSL è implicita — si gestisce con SL più ampio):
          1. HA_ok                                   WR=83% n=24  +4.03$
          2. CVD>100k + TF4h_ok                      WR=79% n=38  +9.60$
          3. Whale>0.3 + TF1h_ok                     WR=77% n=43  +7.43$
          4. Whale>0.3 + TF4h_ok                     WR=76% n=33  +7.76$
          5. Hurst>0.6 + VPIN<0.5                    WR=75% n=36  +7.89$

        Restituisce lista di pattern attivi con nome, WR storico e indicazioni operative.
        """
        try:
            sign = 1 if str(direzione).upper() in ('LONG','BUY') else -1

            cvd    = float(dati_engine.get('cvd_istantaneo', 0) or 0)
            vpin   = float(dati_engine.get('vpin', 0.5) or 0.5)
            hurst  = float(dati_engine.get('hurst_exponent', 0.5) or 0.5)
            whale  = float(dati_engine.get('whale_delta', 0) or 0)
            ofi    = float(dati_engine.get('order_flow_imbalance', 0) or 0)
            bp     = float(dati_engine.get('book_pressure', 0.5) or 0.5)

            # TF allineati alla direzione
            multi_tf = dati_engine.get('multi_tf', {}) or {}
            def tf_ok(tf):
                d = multi_tf.get(tf, {})
                if not isinstance(d, dict): return False
                t = str(d.get('trend_dir', d.get('trend', ''))).upper()
                return (t == 'UP' and sign == 1) or (t == 'DOWN' and sign == -1)

            tf1h_ok = tf_ok('1h')
            tf4h_ok = tf_ok('4h')

            # HA Daily allineato
            ha_col    = str(dati_engine.get('ha_daily_colore', '') or '').upper()
            ha_streak = int(dati_engine.get('ha_daily_streak', 0) or 0)
            ha_ok = (ha_col == 'VERDE' and sign == 1) or (ha_col == 'ROSSO' and sign == -1)

            # CVD allineato alla direzione
            cvd_a = cvd * sign

            pattern_attivi = []

            # Condizione: HA daily allineato direzionalmente con streak >= 2
            if ha_ok and ha_streak >= 2:
                pattern_attivi.append({
                    'nome': 'ha_daily_dir',
                    'dettaglio': f"HA daily {ha_col} streak={ha_streak} candele consecutive",
                    'sl_mult': 1.3,
                })

            # Condizione: CVD direzionale forte > 100k con 4h allineato
            if cvd_a > 100000 and tf4h_ok:
                pattern_attivi.append({
                    'nome': 'cvd_4h',
                    'dettaglio': f"CVD direzionale {cvd_a:+.0f} con trend 4h coerente",
                    'sl_mult': 1.25,
                })

            # Condizione: Whale delta > 0.3 con 1h allineato
            if (whale * sign) > 0.3 and tf1h_ok:
                pattern_attivi.append({
                    'nome': 'whale_1h',
                    'dettaglio': f"Whale delta {whale*sign:+.2f} con trend 1h coerente",
                    'sl_mult': 1.2,
                })

            # Condizione: Whale delta > 0.3 con 4h allineato
            if (whale * sign) > 0.3 and tf4h_ok:
                pattern_attivi.append({
                    'nome': 'whale_4h',
                    'dettaglio': f"Whale delta {whale*sign:+.2f} con trend 4h coerente",
                    'sl_mult': 1.2,
                })

            # Condizione: Hurst trending forte con VPIN basso (flusso pulito)
            if hurst > 0.6 and vpin < 0.5:
                pattern_attivi.append({
                    'nome': 'hurst_vpin',
                    'dettaglio': f"Hurst {hurst:.2f} (>0.6, trending) con VPIN {vpin:.2f} (<0.5, flusso pulito)",
                    'sl_mult': 1.15,
                })

            return pattern_attivi

        except Exception as e:
            self.logger.debug(f"_riconosci_pattern error: {e}")
            return []

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

    def unit_test(self):
        dati_test = {
            'close': 50000, 'z_score': 2.8, 'book_pressure': 0.4, 'cvd_divergence': -0.8,
            'liquidazioni_24h': 15000000, 'vol_shock': 1.8, 'poc': 48500, 'vah': 49000,
            'val': 47000, 'funding_z_score': 2.5, 'macro_proxy': {'relative_volume_status': 1.0},
            'eth_btc_ratio': 0.07, 'spread_perc': 0.1
        }
        res = self.full_global_strategy(dati_test, asset_name="XXBTZUSD", macro_sentiment="BEARISH")
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
            from core.telegram_alerts_la import TelegramAlerts
            prompt = (f"Sentiment Macro: {macro_sentiment}. Agisci come capo desk trading. Fornisci un briefing rapido (max 30 parole) "
                      "con mood di mercato e consiglio operativo secco. No introduzioni, solo sostanza.")
            report = self.chiama_gemini(prompt, is_json=False)
            alerts = TelegramAlerts()
            alerts.invia_alert(f"☕ *BUONGIORNO ANDREA - MORNING BIAS*\n\n{report}")
        except Exception as e:
            _err.capture(e, "genera_report_mattutino", {"module": "BrainLA"})
            self.logger.error(f"❌ Errore report mattutino: {e}")

    def analizza_performance_chimera(self):
        try:
            from core.database_manager import db_manager
            trades = db_manager.get_storico()
            
            if not trades or not isinstance(trades, list): return None
            stats = {"HIGH_HEALTH": {"wins": 0, "losses": 0, "profit": 0.0}, "LOW_HEALTH": {"wins": 0, "losses": 0, "profit": 0.0}}

            for t in trades:
                meta = t.get('metadata', {})
                try:
                    health = float(meta.get('market_health', 0.5))
                    result = float(t.get('result_perc', 0.0))
                except (ValueError, TypeError): continue

                label = "HIGH_HEALTH" if health >= 0.5 else "LOW_HEALTH"
                if result > 0: stats[label]["wins"] += 1
                elif result < 0: stats[label]["losses"] += 1
                stats[label]["profit"] = round(stats[label]["profit"] + result, 4)
            return stats
        except Exception as e:
            _err.capture(e, "analizza_performance_chimera", {"module": "BrainLA"}, level="WARNING")
            return None

    def genera_report_serale(self, stats_giornaliere):
        try:
            if not hasattr(self, 'trade_manager') or self.trade_manager is None:
                try:
                    from core.trade_manager import TradeManager
                    self.trade_manager = TradeManager(engine=self.engine)
                except Exception as e:
                    posizioni = {}
                else: posizioni = self.trade_manager.posizioni_aperte
            else: posizioni = self.trade_manager.posizioni_aperte
            
            chimera_data = self.analizza_performance_chimera()
            chimera_summary = ""
            if chimera_data:
                h_win = chimera_data.get("HIGH_HEALTH", {}).get("wins", 0)
                h_loss = chimera_data.get("HIGH_HEALTH", {}).get("losses", 0)
                h_total = h_win + h_loss
                wr_h = (h_win / h_total * 100) if h_total > 0 else 0
                chimera_summary = f"\n📊 CHIMERA INSIGHTS: WR con Market Health Alta: {wr_h:.1f}% ({h_total} trade)."

            dati_reali = (f"Asset attualmente in portafoglio: {list(posizioni.keys())}. "
                          f"Statistiche della giornata: {stats_giornaliere}. {chimera_summary}")
            
            prompt = (f"Dati operativi: {dati_reali}. Agisci come un analista quantitativo. Crea un report serale sintetico per Andrea. "
                      "Analizza se la strategia ha performato meglio in condizioni di alta salute del mercato e fornisci una nota di outlook per la sessione asiatica.")
            
            report = self.chiama_gemini(prompt, is_json=False)
            from core.telegram_alerts_la import TelegramAlerts
            alerts = TelegramAlerts()
            alerts.invia_alert(f"📊 *REPORT TECNICO SERALE*\n\n{report}")
        except Exception as e:
            _err.capture(e, "genera_report_serale", {"module": "BrainLA"})
            self.logger.error(f"❌ Errore report serale: {e}")

    def calcola_voto(self, dati_engine, asset_name, macro_sentiment):
        entry_price = dati_engine.get('price', 0)
        cvd_istantaneo = dati_engine.get('cvd_istantaneo', dati_engine.get('cvd_istantaneo', dati_engine.get('cvd', 0)))
        price_velocity = dati_engine.get('price_velocity', dati_engine.get('velocity', 0))
        vpin = dati_engine.get('vpin', 0)
        
        dati_per_gemini = {
            "asset": asset_name, "price": entry_price, "market_health": dati_engine.get('market_health', 0.5),
            "flusso_hft": {"vpin": vpin, "cvd_istantaneo": cvd_istantaneo, "velocity": price_velocity, "aggressivita": dati_engine.get('aggressivita_flow', "Neutral")},
            "microstruttura": {"price_velocity": price_velocity, "indice_spoofing": dati_engine.get('indice_spoofing', 0), "iceberg": dati_engine.get('iceberg_presenti', 0)}
        }
        
        dati_engine['json_input'] = json.dumps(dati_per_gemini, indent=2)
        return self.full_global_strategy(dati_engine=dati_engine, asset_name=asset_name, macro_sentiment=macro_sentiment)

    def valuta_validita_tesi(self, asset, dati_mercato, posizione):
        """
        Punto 2: Monitoraggio Tesi di Investimento.
        Chiede all'IA se la tesi originale è ancora valida data la nuova situazione di mercato.
        """
        try:
            tesi_originale = posizione.get('razionale', 'N/A')
            tipo_op = posizione.get('tipo_op', 'N/A')
            p_entrata = posizione.get('p_entrata', 0)
            direzione = posizione.get('direzione', 'LONG')
            timeframe = posizione.get('timeframe', 'N/A')
            
            # Prepariamo un subset di dati per non saturare il prompt
            dati_compatti = {
                "prezzo": dati_mercato.get('close'),
                "regime": dati_mercato.get('market_regime'),
                "hurst": dati_mercato.get('hurst_exponent'),
                "vpin": dati_mercato.get('vpin_toxicity'),
                "cvd": dati_mercato.get('cvd_divergence'),
                "vol_shock": dati_mercato.get('vol_shock'),
                "fvg": dati_mercato.get('fvg')
            }
            
            prompt = (
                f"Analisi Validità Tesi per {asset} ({direzione}).\n"
                f"Tesi Originale: {tesi_originale}\n"
                f"Tipo Operazione: {tipo_op} | Timeframe: {timeframe} | Prezzo Entrata: {p_entrata}\n\n"
                f"Dati Mercato Attuali: {json.dumps(dati_compatti)}\n\n"
                "Agisci come un Senior Risk Manager. La tesi originale è ancora valida o è un FALLIMENTO STRUTTURALE?\n"
                "ISTRUZIONI CRITICHE:\n"
                "1. DISTINGUI RITRACCIAMENTO DA FALLIMENTO: Un ritracciamento con CVD neutro o decrescente (basso volume) è SANO. Un movimento contro-trend con CVD esplosivo, VPIN > 0.8 o Velocity alta è un FALLIMENTO STRUTTURALE.\n"
                "2. ANALISI LIQUIDITÀ: Se il prezzo si avvicina a un muro di liquidità opposto e il CVD conferma l'aggressione, la tesi è invalidata.\n"
                "3. HOLD vs CLOSE: Sii resiliente al rumore. Chiudi solo se la struttura di mercato (Hurst, Regime) è cambiata radicalmente rispetto all'ingresso.\n"
                "Rispondi ESCLUSIVAMENTE in JSON: {\"valida\": bool, \"motivo\": \"string\", \"azione\": \"HOLD/CLOSE/REVERSE\"}.\n"
                "Il motivo deve essere tecnico e conciso (max 12 parole)."
            )
            
            res = self.chiama_gemini(prompt, is_json=True, schema_class=ThesisSchema)
            if not res:
                return True, "Errore API Gemini", "HOLD"
                
            valida = res.get('valida', True)
            motivo = res.get('motivo', 'Tesi confermata')
            azione = res.get('azione', 'HOLD')
            
            return valida, motivo, azione
        except Exception as e:
            _err.capture(e, "unknown", {"module": "BrainLA"})
            self.logger.error(f"❌ Errore valutazione tesi per {asset}: {e}")
            return True, "Errore Interno", "HOLD"

# File updated to sync with UI