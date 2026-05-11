# -*- coding: utf-8 -*-
"""
L&A Institutional Bot - NightReview v1.1
CHIMERA SELF-CRITIQUE — FIX ROBUSTO

Fix rispetto alla v1.0:
  - Invia sempre un messaggio Telegram (anche se non ci sono trade)
  - Non inghiotte silenziosamente le eccezioni
  - Testo Gemini sanitizzato prima dell'invio
  - Funziona anche con storico vuoto (primo giorno operativo)
"""

import json
import logging
import time
from datetime import datetime, timedelta
import sys
from core.chimera_errors import ErrorTracker

_err = ErrorTracker("NightReview")



class NightReview:

    def __init__(self, brain, feedback_engine, trade_manager, alerts=None):
        self.brain          = brain
        self.fe             = feedback_engine
        self.tm             = trade_manager
        self.alerts         = alerts
        self.logger         = logging.getLogger("NightReview")
        self._ultimo_review = time.time()  # evita esecuzione immediata all'avvio

    def esegui_se_necessario(self):
        ora = datetime.now()
        secondi_da_ultimo = time.time() - self._ultimo_review
        # Esegue ogni 2 ore ai primi 10 minuti dell'ora, se è passata almeno 2 ore dall'ultima esecuzione
        if ora.minute < 10 and secondi_da_ultimo > 7000:
            self.logger.info("🌙 [AUTO-CORREZIONE] Avvio analisi periodica...")
            self.esegui_review_completo()
            self._ultimo_review = time.time()

    def esegui_review_completo(self):
        # Controlla cooldown Gemini PRIMA di iniziare qualsiasi analisi
        if hasattr(self.brain, '_gemini_quota_until'):
            if time.time() < self.brain._gemini_quota_until:
                self.logger.warning("🌙 NightReview: quota Gemini in cooldown — analisi rimandata.")
                return
        try:
            trade_recenti = self._get_trade_recenti(ore=48)

            if not trade_recenti:
                msg = (
                    "🔄 *AUTO-CORREZIONE — CHIMERA*\n"
                    "━━━━━━━━━━━━━━━━━━\n"
                    "Nessun trade chiuso nelle ultime 48h.\n"
                    "Il sistema è operativo e in attesa di segnali."
                )
                if self.alerts:
                    self.alerts.invia_alert(msg)
                self.logger.info("🔄 Auto-Correzione: nessun trade. Notifica inviata.")
                return

            per_asset = {}
            for t in trade_recenti:
                asset = t.get("asset", "UNKNOWN")
                per_asset.setdefault(asset, []).append(t)

            report_globale = [
                f"Trade analizzati: {len(trade_recenti)} | Asset: {len(per_asset)}"
            ]

            for asset, trades in per_asset.items():
                # Salta asset con meno di 2 trade — non abbastanza dati per lezioni utili
                if len(trades) < 2:
                    self.logger.debug(f"🌙 Salto {asset}: solo {len(trades)} trade, insufficiente per lezioni.")
                    continue

                # Rispetta il cooldown quota Gemini se attivo
                if hasattr(self.brain, '_gemini_quota_until'):
                    if time.time() < self.brain._gemini_quota_until:
                        self.logger.warning(f"🌙 NightReview interrotto: quota Gemini in cooldown.")
                        break

                # Delay tra analisi asset — evita picco di chiamate che causa 429
                # Il NightReview non è urgente, 5s di pausa tra asset è accettabile
                time.sleep(5)

                self.logger.info(f"🌙 Analisi {asset} ({len(trades)} trade)...")
                try:
                    lezioni = self._analizza_asset(asset, trades)
                    if lezioni:
                        self.fe.aggiorna_lezioni(asset, lezioni)
                        sintesi = lezioni.get("sintesi", "analisi ok")
                        regola  = lezioni.get("regola_domani", "")
                        voto    = lezioni.get("voto_performance", "?")
                        pattern = lezioni.get("pattern_errore_principale", "")

                        # FIX 2026-05-02: NightReview NON deve scrivere in feedback_trades.
                        # Quel campo viene popolato da trade_manager.registra_feedback() su
                        # ogni trade chiuso reale, con outcome reale (WIN/LOSS basato sul PnL).
                        # Prima qui c'era una chiamata che hardcodava outcome="LOSS" se
                        # almeno un trade della giornata era LOSS — questo creava un dataset
                        # feedback_trades distorto (3W/47L invece del 41% WR reale dello storico).
                        # NightReview ora aggiorna SOLO `lezioni_asset` (sopra), niente feedback_trades.
                        # Il pattern_errore_principale è già salvato dentro lezioni_asset.

                        report_globale.append(
                            f"\n*{asset}* (voto {voto}/10)\n"
                            f"  {sintesi}\n"
                            f"  Errore: {pattern}\n"
                            f"  Regola domani: {regola}"
                        )
                    else:
                        report_globale.append(f"\n*{asset}*: analisi non disponibile")
                except Exception as e_a:
                    _err.capture(e_a, sys._getframe().f_code.co_name, {"module": "NightReview"})
                    self.logger.error(f"Errore analisi {asset}: {e_a}")
                    report_globale.append(f"\n*{asset}*: errore analisi")

            if self.alerts:
                msg = (
                    "🔄 *AUTO-CORREZIONE — CHIMERA SELF-CRITIQUE*\n"
                    "━━━━━━━━━━━━━━━━━━\n"
                    + "\n".join(report_globale)
                )
                self.alerts.invia_alert(msg)
                self.logger.info("✅ Auto-Correzione inviata su Telegram.")

        except Exception as e:
            _err.capture(e, sys._getframe().f_code.co_name, {"module": "NightReview"})
            self.logger.error(f"❌ Errore Auto-Correzione: {e}")
            import traceback
            traceback.print_exc()
            if self.alerts:
                self.alerts.invia_alert(
                    f"⚠️ *AUTO-CORREZIONE ERRORE*\nDettaglio: {str(e)[:200]}"
                )

    def _get_trade_recenti(self, ore=48):
        cutoff = datetime.now() - timedelta(hours=ore)
        result = []
        for t in self.tm.storico_trades:
            try:
                data_str = t.get("data_chiusura", "")
                if not data_str:
                    continue
                data_ch = datetime.fromisoformat(data_str.replace("Z", ""))
                if data_ch > cutoff:
                    result.append(t)
            except Exception:
                continue
        return result

    def _analizza_asset(self, asset, trades):
        trade_summary = []
        for t in trades:
            snapshot = t.get("chimera_snapshot", {})
            if isinstance(snapshot, str):
                try: snapshot = json.loads(snapshot)
                except: snapshot = {}
            trade_summary.append({
                "data":             t.get("data_apertura", "")[:16],
                "direzione":        t.get("direzione"),
                "esito":            t.get("esito"),
                "pnl":              round(float(t.get("pnl_finale", 0)), 2),
                "voto_ia":          t.get("voto_ia", 0),
                "motivo_chiusura":  t.get("motivo_chiusura", "TP/SL"),
                "ore_aperto":       self._calcola_ore(t),
                # Contesto strutturale — fondamentale per capire gli errori
                "ciclo_fase":       snapshot.get("ciclo_fase", "?"),
                "minimo_qualita":   snapshot.get("minimo_qualita", "?"),
                "ciclo_recupero":   f"{snapshot.get('ciclo_recupero_pct', 0):.0f}%",
                "sr_flip":          snapshot.get("sr_flip_tipo", "no"),
                "ha_daily":         f"{snapshot.get('ha_daily_colore','?')} streak={snapshot.get('ha_daily_streak',0)}",
                # Contesto tecnico
                "entry_phase":      snapshot.get("entry_phase", "?"),
                "hurst":            snapshot.get("hurst_exponent", 0.5),
                "kaufman":          snapshot.get("kaufman_efficiency", 0.5),
                "vpin":             snapshot.get("vpin", 0),
                "regime":           snapshot.get("market_regime", "?"),
                # Decisione
                "razionale_ia":     str(snapshot.get("razionale", ""))[:100],
                "decision_source":  snapshot.get("decision_source", "?"),
            })

        prompt = (
            f"Sei un analista che osserva le operazioni passate su {asset}. Analizza:\n"
            f"{json.dumps(trade_summary, indent=2)}\n\n"
            f"CONTESTO STRUTTURALE: guarda ciclo_fase, minimo_qualita, sr_flip, ha_daily per capire "
            f"se gli ingressi erano allineati con il ciclo di mercato o contro.\n"
            f"CONTESTO TECNICO: guarda entry_phase SSE, hurst, kaufman per capire se il timing era giusto.\n\n"
            f"Il tuo compito è OSSERVARE e DESCRIVERE, NON prescrivere. Identifica:\n"
            f"1. Il pattern di perdita ricorrente che hai notato in questi trade (descrittivo, "
            f"es: 'molti SHORT aperti durante recuperi da capitolazione hanno perso')\n"
            f"2. Le condizioni di mercato in cui il bot ha performato peggio (descrittivo)\n"
            f"3. Una NOTA OSSERVATIVA per il futuro — cosa è stato osservato in questi trade. "
            f"NON una regola da seguire ciecamente, ma un'informazione di contesto che il bot "
            f"valuterà se rilevante al setup attuale (es: 'osservato che SHORT in recupero da "
            f"capitolazione ha avuto WR basso', NON 'NON aprire mai SHORT in recupero').\n"
            f"Il sistema decisionale a valle (Gemini al momento del trade) deciderà autonomamente "
            f"se queste osservazioni sono applicabili al contesto attuale o se ignorarle.\n\n"
            f"RISPONDI SOLO IN JSON:\n"
            f'{{"sintesi":"str max 80 chars descrittiva",'
            f'"pattern_errore_principale":"str descrittivo con dati osservati",'
            f'"condizioni_sfavorevoli":"str con contesto strutturale osservato",'
            f'"uso_time_stop":"CORRETTO/MIGLIORABILE/NON_USATO",'
            f'"consiglio_sizing":"str descrittivo",'
            f'"regola_domani":"str max 150 chars con OSSERVAZIONE per contesto, non imperativo",'
            f'"voto_performance":0}}'
        )

        from core.brain_la import NightReviewSchema
        risposta = self.brain.chiama_gemini(prompt, is_json=True, schema_class=NightReviewSchema)
        if isinstance(risposta, dict) and "sintesi" in risposta:
            return risposta
        self.logger.warning(f"Auto-Correzione risposta non valida per {asset}: {risposta}")
        return None

    def _calcola_ore(self, trade):
        try:
            a = datetime.fromisoformat(trade["data_apertura"].replace("Z", ""))
            c = datetime.fromisoformat(trade["data_chiusura"].replace("Z", ""))
            return round((c - a).total_seconds() / 3600, 1)
        except Exception:
            return 0.0