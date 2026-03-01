# -*- coding: utf-8 -*-
"""
L&A Institutional Bot - FeedbackEngine
Modulo per gestione feedback, apprendimento, logging e analisi risultati.
Versione robusta e pronta all'uso.
"""

import json
import logging
import os
import tempfile
import threading
from datetime import datetime
from core import asset_list

class FeedbackEngine:
    """
    Gestisce il feedback automatico/integrato del bot: registra l'esito, aggiorna record, genera summary.
    """

    def __init__(self, file_feedback="feedback_history.json"):
        self.file_feedback = file_feedback
        self.logger = logging.getLogger("FeedbackEngine")
        self._lock = threading.Lock()
        self.cache = self._carica_feedback()

    # ---------- Utility di I/O sicuro ----------
    def _atomic_write(self, path, data):
        tmp_fd, tmp_path = tempfile.mkstemp(prefix=".tmp_", dir=os.path.dirname(path) or ".")
        with os.fdopen(tmp_fd, "w") as f:
            json.dump(data, f, indent=4)
        os.replace(tmp_path, path)

    def _carica_feedback(self):
        try:
            with open(self.file_feedback, "r") as f:
                data = json.load(f)
                if isinstance(data, list):
                    return data
                else:
                    self.logger.warning("⚠️ File feedback non è una lista, verrà resettato.")
                    return []
        except Exception as e:
            self.logger.warning(f"⚠️ Errore load feedback: {e}")
            return []

    def registra_feedback(self, asset, score, outcome, motivi, snapshot=None):
        """
        Registra un feedback evoluto: include lo snapshot del mercato per l'auto-apprendimento.
        """
        record = {
            "asset": asset,
            "score": score,
            "outcome": outcome,
            "motivi": motivi,
            "snapshot_mercato": snapshot,  # <--- fotografia del momento
            "timestamp": datetime.now().isoformat()
        }
        
        if not isinstance(self.cache, list):
            self.logger.error("⚠️ Cache feedback non è una lista, la resetto.")
            self.cache = []
            
        self.cache.append(record)
        self._salva_feedback()
        
        # Log dettagliato per capire cosa sta imparando
        self.logger.info(f"🧠 AUTO-APPRENDIMENTO: Registrato {outcome} per {asset} (Voto IA: {score})")
        if snapshot:
            self.logger.info(f"📊 Snapshot salvato: Z:{snapshot.get('z_score')} | Fz:{snapshot.get('funding_z_score')}")

    def _salva_feedback(self):
        try:
            with self._lock:
                self._atomic_write(self.file_feedback, self.cache)
        except Exception as e:
            self.logger.error(f"⚠️ Errore salvataggio feedback: {e}")

    def get_feedback_summary(self):
        """
        Ritorna un summary del feedback (win-rate, asset preferiti, ecc).
        """
        if not isinstance(self.cache, list):
            self.logger.error("⚠️ Cache feedback non è una lista, la resetto.")
            self.cache = []
        outcomes = [r["outcome"] for r in self.cache]
        total = len(outcomes)
        win = sum(1 for o in outcomes if o == "WIN")
        loss = sum(1 for o in outcomes if o == "LOSS")
        return {
            "total": total,
            "wins": win,
            "losses": loss,
            "win_rate": win/total*100 if total else 0
        }

    def get_asset_metrics(self, asset, window=50):
        """
        Ritorna metriche rolling per singolo asset.
        """
        if not self.cache or not isinstance(self.cache, list):
            return {"total": 0, "win_rate": 0, "streak_loss": 0, "streak_win": 0}
        filtered = [r for r in self.cache if r.get("asset") == asset][-window:]
        total = len(filtered)
        wins = sum(1 for r in filtered if r.get("outcome") == "WIN")
        win_rate = wins/total*100 if total else 0
        streak_loss = 0
        streak_win = 0
        for r in reversed(filtered):
            if r.get("outcome") == "LOSS":
                streak_loss += 1
                streak_win = 0
            elif r.get("outcome") == "WIN":
                streak_win += 1
                streak_loss = 0
            else:
                break
        return {
            "total": total,
            "win_rate": win_rate,
            "streak_loss": streak_loss,
            "streak_win": streak_win
        }

    def get_win_rate(self):
        """
        Restituisce il win-rate percentuale dei trade feedback.
        """
        summary = self.get_feedback_summary()
        return summary["win_rate"]

    # --- AGGIUNTA PER AUTO-APPRENDIMENTO: NON TOCCA IL CODICE SOPRA ---
    def get_recent_summary(self, limit=5):
        """
        Ritorna un dizionario con lo storico, così il Brain può fare .get() senza crashare.
        """
        if not self.cache or not isinstance(self.cache, list):
            return {"testo": "Nessun trade precedente.", "lista": []}
        
        recenti = self.cache[-limit:]
        testo = "Ultimi trade per apprendimento:\n"
        
        for r in recenti:
            status = "✅ WIN" if r.get('outcome') == "WIN" else "❌ LOSS"
            testo += f"- {r.get('asset')}: {status} (Voto: {r.get('score')})\n"
        
        return {
            "testo": testo,
            "lista": recenti
        }
    
    # --- AGGIUNTA PER LE OCCASIONI PERSE (GHOST TRADING) ---
    def registra_analisi_scartata(self, asset, score, direzione, prezzo_attuale, snapshot):
        """
        Registra un'analisi che non ha portato a un trade per monitorarne l'esito futuro.
        """
        record = {
            "type": "GHOST_ANALYSIS",
            "asset": asset,
            "score": score,
            "direzione": direzione,
            "prezzo_analisi": prezzo_attuale,
            "snapshot_mercato": snapshot,
            "timestamp": datetime.now().isoformat(),
            "esito_verificato": False
        }
        
        ghost_file = "ghost_history.json"
        try:
            with self._lock:
                try:
                    with open(ghost_file, "r") as f: data = json.load(f)
                except: data = []
                if not isinstance(data, list):
                    data = []
                data.append(record)
                # Teniamo solo le ultime 50 analisi scartate
                self._atomic_write(ghost_file, data[-50:])
            self.logger.info(f"👻 GHOST LOG: Registrata analisi {asset} (Voto {score}) per verifica futura.")
        except Exception as e:
            self.logger.error(f"⚠️ Errore salvataggio ghost log: {e}")
            
    def verifica_esiti_ghost(self, exchange):
        """
        Controlla i prezzi attuali per le analisi scartate e decide se erano occasioni perse.
        """
        ghost_file = "ghost_history.json"
        try:
            with open(ghost_file, "r") as f: data = json.load(f)
        except: 
            return

        if not isinstance(data, list):
            return

        aggiornati = []
        for g in data:
            if g.get("esito_verificato"): 
                aggiornati.append(g)
                continue
            
            try:
                symbol = asset_list.get_ticker(g['asset']) or g['asset']
            except Exception:
                symbol = g['asset']
            try:
                ticker_info = exchange.fetch_ticker(symbol)
                curr_price = float(ticker_info.get('last', ticker_info.get('close', 0)))
            except Exception as e:
                self.logger.debug(f"⚠️ Impossibile fetch_ticker {symbol}: {e}")
                aggiornati.append(g)
                continue

            p_old = g.get('prezzo_analisi', 0)
            if not p_old:
                aggiornati.append(g)
                continue

            diff = ((curr_price - p_old) / p_old) * 100
            if g['direzione'] == "SELL":
                diff *= -1
            
            # Se è passato abbastanza tempo o il prezzo si è mosso dell'1%
            if abs(diff) > 1.0:
                g['esito_reale'] = "OCCASIONE_PERSA" if diff > 0 else "SCELTA_CORRETTA"
                g['pnl_potenziale'] = round(diff, 2)
                g['esito_verificato'] = True
                self.logger.info(f"🔍 VERIFICA GHOST: {g['asset']} era {g['esito_reale']} ({round(diff,2)}%)")
            
            aggiornati.append(g)

        with self._lock:
            self._atomic_write(ghost_file, aggiornati[-50:])