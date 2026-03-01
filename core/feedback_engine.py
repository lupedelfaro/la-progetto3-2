# -*- coding: utf-8 -*-
"""
L&A Institutional Bot - FeedbackEngine
Modulo per gestione feedback, apprendimento, logging e analisi risultati.
Versione robusta con prior statistico (RF) per l'IA.
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
    Gestisce il feedback automatico/integrato del bot: registra l'esito, aggiorna record, genera summary
    e produce un segnale statistico (prior) tramite modello tabellare leggero.
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
            "snapshot_mercato": snapshot,  # fotografia del momento
            "timestamp": datetime.now().isoformat()
        }
        
        if not isinstance(self.cache, list):
            self.logger.error("⚠️ Cache feedback non è una lista, la resetto.")
            self.cache = []
            
        self.cache.append(record)
        self._salva_feedback()
        
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
        summary = self.get_feedback_summary()
        return summary["win_rate"]

    # --- AGGIUNTA PER AUTO-APPRENDIMENTO: NON TOCCA IL CODICE SOPRA ---
    def get_recent_summary(self, limit=5):
        if not self.cache or not isinstance(self.cache, list):
            return {"testo": "Nessun trade precedente.", "lista": []}
        recenti = self.cache[-limit:]
        testo = "Ultimi trade per apprendimento:\n"
        for r in recenti:
            status = "✅ WIN" if r.get('outcome') == "WIN" else "❌ LOSS"
            testo += f"- {r.get('asset')}: {status} (Voto: {r.get('score')})\n"
        return {"testo": testo, "lista": recenti}
    
    # --- GHOST TRADING ---
    def registra_analisi_scartata(self, asset, score, direzione, prezzo_attuale, snapshot):
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
                self._atomic_write(ghost_file, data[-50:])
            self.logger.info(f"👻 GHOST LOG: Registrata analisi {asset} (Voto {score}) per verifica futura.")
        except Exception as e:
            self.logger.error(f"⚠️ Errore salvataggio ghost log: {e}")
            
    def verifica_esiti_ghost(self, exchange):
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
                aggiornati.append(g); continue
            try:
                symbol = asset_list.get_ticker(g['asset']) or g['asset']
            except Exception:
                symbol = g['asset']
            try:
                ticker_info = exchange.fetch_ticker(symbol)
                curr_price = float(ticker_info.get('last', ticker_info.get('close', 0)))
            except Exception as e:
                self.logger.debug(f"⚠️ Impossibile fetch_ticker {symbol}: {e}")
                aggiornati.append(g); continue
            p_old = g.get('prezzo_analisi', 0)
            if not p_old:
                aggiornati.append(g); continue
            diff = ((curr_price - p_old) / p_old) * 100
            if g['direzione'] == "SELL":
                diff *= -1
            if abs(diff) > 1.0:
                g['esito_reale'] = "OCCASIONE_PERSA" if diff > 0 else "SCELTA_CORRETTA"
                g['pnl_potenziale'] = round(diff, 2)
                g['esito_verificato'] = True
                self.logger.info(f"🔍 VERIFICA GHOST: {g['asset']} era {g['esito_reale']} ({round(diff,2)}%)")
            aggiornati.append(g)
        with self._lock:
            self._atomic_write(ghost_file, aggiornati[-50:])

    # --- PRIOR STATISTICO CON RANDOM FOREST ---
    def _extract_features(self, rec):
        snap = rec.get("snapshot_mercato") or {}
        return {
            "z_score": float(snap.get("z_score", 0) or 0),
            "funding_z": float(snap.get("funding_z_score", snap.get("funding_z", 0) or 0)),
            "book_pressure": float(snap.get("book_pressure", 1) or 1),
            "atr": float(snap.get("atr", 0) or 0),
            "cvd_div": float(snap.get("cvd_divergence", 0) or 0),
            "voto": float(rec.get("score", 0) or 0),
            "outcome": 1 if rec.get("outcome") == "WIN" else 0
        }

    def get_prior_signal(self, asset, max_samples=300):
        """
        Restituisce un prior statistico basato su RandomForest:
        - prior_voto (0-10)
        - prior_sizing (0-1)
        - prior_conf (probabilità di WIN)
        Se non ci sono abbastanza dati o sklearn manca, ritorna None.
        """
        try:
            from sklearn.ensemble import RandomForestClassifier
        except Exception:
            self.logger.debug("sklearn non disponibile, prior disattivo.")
            return None

        recs = [r for r in self.cache if r.get("asset") == asset]
        recs = [r for r in recs if r.get("snapshot_mercato")]
        if len(recs) < 30:
            return None

        recs = recs[-max_samples:]
        X = []
        y = []
        for r in recs:
            f = self._extract_features(r)
            X.append([f["z_score"], f["funding_z"], f["book_pressure"], f["atr"], f["cvd_div"], f["voto"]])
            y.append(f["outcome"])

        if not X:
            return None

        try:
            clf = RandomForestClassifier(
                n_estimators=64,
                max_depth=6,
                min_samples_leaf=2,
                random_state=42
            )
            clf.fit(X, y)
            last_f = self._extract_features(recs[-1])
            prob_win = float(clf.predict_proba([[last_f["z_score"], last_f["funding_z"], last_f["book_pressure"], last_f["atr"], last_f["cvd_div"], last_f["voto"]]])[0][1])
        except Exception as e:
            self.logger.debug(f"Prior RF fallito: {e}")
            return None

        prior_voto = round(prob_win * 10, 1)
        prior_sizing = max(0.02, min(0.35, 0.15 * (0.5 + prob_win)))  # tra ~0.075 e 0.35

        return {
            "prior_voto": prior_voto,
            "prior_sizing": prior_sizing,
            "prior_conf": prob_win
        }