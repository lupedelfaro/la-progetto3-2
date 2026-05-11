import json
import os
import time
import logging
from datetime import datetime
import sys
from core.chimera_errors import ErrorTracker

_err = ErrorTracker("FeedbackEngine")


class FeedbackEngine:
    """
    Motore di auto-apprendimento (Ghost Trading e Feedback Loop).
    Registra i segnali scartati (voto basso) e verifica se sarebbero stati profittevoli.
    Registra anche i trade reali per calcolare le metriche di successo.
    """
    def __init__(self, file_ghost="ghost_trades.json", file_stats="stats_globali.json"):
        self.file_ghost = file_ghost
        self.file_stats = file_stats
        self.logger = logging.getLogger("FeedbackEngine")
        self.ghost_trades = self._carica_dati(self.file_ghost)
        self.stats_globali = self._carica_dati(self.file_stats)

    def _carica_dati(self, filepath):
        from core.database_manager import db_manager
        if "ghost" in filepath:
            try:
                ghosts_dict = db_manager.get_ghosts()
                return list(ghosts_dict.values())
            except Exception as e:
                _err.capture(e, sys._getframe().f_code.co_name, {"module": "FeedbackEngine"})
                self.logger.error(f"Errore caricamento ghost da DB: {e}")
                return []
        elif "stats" in filepath:
            try:
                return db_manager.get_stats_globali()
            except Exception as e:
                _err.capture(e, sys._getframe().f_code.co_name, {"module": "FeedbackEngine"})
                self.logger.error(f"Errore caricamento stats da DB: {e}")
                return {}
        else:
            if os.path.exists(filepath):
                try:
                    with open(filepath, 'r') as f:
                        return json.load(f)
                except Exception as e:
                    _err.capture(e, sys._getframe().f_code.co_name, {"module": "FeedbackEngine"})
                    self.logger.error(f"Errore caricamento {filepath}: {e}")
            return {}

    def _salva_dati(self, filepath, dati):
        from core.database_manager import db_manager
        if "ghost" in filepath:
            try:
                ghosts_dict = {g.get("id", f"ghost_{time.time()}"): g for g in dati}
                db_manager.save_ghosts(ghosts_dict)
            except Exception as e:
                _err.capture(e, sys._getframe().f_code.co_name, {"module": "FeedbackEngine"})
                self.logger.error(f"Errore salvataggio ghost su DB: {e}")
        elif "stats" in filepath:
            try:
                db_manager.save_stats_globali(dati)
            except Exception as e:
                _err.capture(e, sys._getframe().f_code.co_name, {"module": "FeedbackEngine"})
                self.logger.error(f"Errore salvataggio stats su DB: {e}")
        else:
            try:
                with open(filepath, 'w') as f:
                    json.dump(dati, f, indent=4)
            except Exception as e:
                _err.capture(e, sys._getframe().f_code.co_name, {"module": "FeedbackEngine"})
                self.logger.error(f"Errore salvataggio {filepath}: {e}")

    def registra_analisi_scartata(self, asset, voto, direzione, prezzo, dati_mercato, sl=0, tp=0):
        """
        Registra un trade virtuale per training XGBoost.
        Accetta TUTTI i segnali con direzione (LONG/SHORT) e dati di base disponibili.
        Il voto non è un filtro — anche voto 1-4 è dato di allenamento valido
        (insegna al modello cosa NON fare).
        Il PnL virtuale non tocca il conto reale.
        """
        # Filtro minimo: solo segnali con direzione reale
        if direzione in ('FLAT', None, ''):
            return

        # Verifica che ci siano almeno i dati core per XGBoost
        # (CVD e VPIN sono il minimo — senza questi il trade è rumore puro)
        _cvd = dati_mercato.get("cvd_istantaneo")
        _vpin = dati_mercato.get("vpin")
        if _cvd is None and _vpin is None:
            return

        # ═══════════════════════════════════════════════════════════════════
        # SNAPSHOT IDENTICO AI TRADE REALI
        # ═══════════════════════════════════════════════════════════════════
        # I ghost trade DEVONO avere lo stesso identico snapshot dei trade reali
        # eseguiti dal bot, altrimenti il dataset di addestramento XGBoost è
        # disomogeneo (ghost mancano feature → modello impara su distribuzioni
        # diverse e non generalizza).
        #
        # Replico ESATTAMENTE la logica di trade_manager.apri_posizione riga 1412:
        # _snap = dict(dati_mercato)
        # _snap_clean = {k:v per k,v in _snap.items() if non è (list/dict)
        #                tranne whitelist specifica}
        #
        # Questo garantisce che ogni nuova feature aggiunta a dati_mercato finisca
        # AUTOMATICAMENTE sia nei trade reali sia nei ghost — niente più
        # disallineamento manuale.
        # ═══════════════════════════════════════════════════════════════════

        _snap = dict(dati_mercato) if isinstance(dati_mercato, dict) else {}

        # Whitelist allineata a trade_manager.py:1413 + multi_tf e score_breakdown
        # (richiesti dal modello ML come dict strutturati)
        _DICT_WHITELIST = (
            'flusso_hft_primario', 'analisi_profonda', 'mappa_volumetrica',
            'multi_tf', 'score_breakdown',
        )

        snapshot = {
            k: v for k, v in _snap.items()
            if not isinstance(v, (list, dict)) or k in _DICT_WHITELIST
        }

        # Aggiunge i campi non-mercato (decisione/contesto) che vengono dai parametri
        # della funzione, non da dati_mercato
        snapshot["voto_ia"]       = int(voto)
        snapshot["data_apertura"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        ghost = {
            "id":               f"virtual_{asset}_{int(time.time())}",
            "asset":            asset,
            "timestamp":        time.time(),
            "data":             datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "voto_ia":          voto,
            "direzione":        direzione,
            "prezzo_ingresso":  prezzo,
            "sl":               float(sl) if sl else prezzo * (1.015 if direzione in ('SELL','SHORT') else 0.985),
            "tp":               float(tp) if tp else prezzo * (0.97 if direzione in ('SELL','SHORT') else 1.03),
            "stato":            "PENDING",
            "fonte":            "VIRTUAL_BRAIN",
            "snapshot":         snapshot,
        }

        # ── Sequenza LSTM — allega gli ultimi 30 snapshot al ghost ───────────
        try:
            from core.sequence_buffer import seq_buf
            seq = seq_buf.get_sequence(asset, n=30)
            if seq:
                ghost["lstm_sequence"] = seq
        except Exception:
            pass
        # ─────────────────────────────────────────────────────────────────────

        self.ghost_trades.append(ghost)
        self._salva_dati(self.file_ghost, self.ghost_trades)
        self.logger.info(f"📋 [VIRTUAL] {asset} {direzione} voto={voto} | SL={ghost['sl']:.4f} TP={ghost['tp']:.4f} — in attesa esito")

    def verifica_esiti_ghost(self, exchange, chimera_ml=None):
        """
        Monitora i trade virtuali finché SL o TP viene colpito.
        Quando chiuso → passa a ChimeraML per training.
        Zero impatto su PnL reale.
        Timeout massimo 24h per non tenere ghost in sospeso per sempre.
        """
        ora = time.time()
        modificati = False
        da_passare_a_ml = []

        for ghost in self.ghost_trades:
            if ghost.get("stato") != "PENDING":
                continue
            # Salta i ghost senza SL/TP (vecchio formato)
            if not ghost.get("sl") or not ghost.get("tp"):
                continue
            # Timeout 24h
            if ora - ghost["timestamp"] > 86400:
                ghost["stato"] = "EXPIRED"
                ghost["esito"] = "EXPIRED"
                modificati = True
                continue

            try:
                from core.asset_list import get_ticker
                ticker_api = get_ticker(ghost["asset"])
                tick = exchange.fetch_ticker(ticker_api)
                prezzo_attuale = float(tick['last'])
                p_ingresso = float(ghost["prezzo_ingresso"])
                sl = float(ghost["sl"])
                tp = float(ghost["tp"])
                direzione = ghost["direzione"]
                is_long = direzione in ('BUY', 'LONG')

                # Verifica se SL o TP colpito
                sl_colpito = prezzo_attuale <= sl if is_long else prezzo_attuale >= sl
                tp_colpito = prezzo_attuale >= tp if is_long else prezzo_attuale <= tp

                if tp_colpito or sl_colpito:
                    esito = "WIN" if tp_colpito else "LOSS"
                    pnl_perc = ((prezzo_attuale - p_ingresso) / p_ingresso * 100) if is_long                                else ((p_ingresso - prezzo_attuale) / p_ingresso * 100)
                    
                    ghost["stato"] = "CLOSED"
                    ghost["esito"] = esito
                    ghost["prezzo_uscita"] = prezzo_attuale
                    ghost["pnl_perc"] = round(pnl_perc, 2)
                    ghost["motivo_chiusura"] = "TP" if tp_colpito else "SL"
                    ghost["data_chiusura"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    modificati = True
                    da_passare_a_ml.append(ghost)
                    self.logger.info(
                        f"📋 [VIRTUAL CLOSED] {ghost['asset']} {direzione} voto={ghost['voto_ia']} "
                        f"→ {esito} ({ghost['motivo_chiusura']}) PnL virtuale: {pnl_perc:+.2f}%"
                    )
            except Exception as e:
                _err.capture(e, sys._getframe().f_code.co_name, {"module": "FeedbackEngine"})
                self.logger.debug(f"Verifica ghost {ghost.get('asset')}: {e}")

        # Passa i trade chiusi a ChimeraML
        if da_passare_a_ml:
            try:
                from core.database_manager import db_manager
                # Usa l'istanza passata dall'esterno — evita di ricaricare modello ogni volta
                ml = chimera_ml
                if ml is None:
                    from core.chimera_ml import ChimeraML
                    ml = ChimeraML()
                for g in da_passare_a_ml:
                    trade_data = {
                        "asset":            g["asset"],
                        "direzione":        g["direzione"],
                        "voto_ia":          g["voto_ia"],
                        "esito":            g["esito"],
                        "pnl_netto_usd":    g.get("pnl_perc", 0),
                        "chimera_snapshot": g.get("snapshot", {}),
                        "fonte":            "VIRTUAL_BRAIN",
                        "data_apertura":    g.get("data", ""),
                        "data_chiusura":    g.get("data_chiusura", ""),
                        "motivo_chiusura":  g.get("motivo_chiusura", ""),
                        # Campi extra per _prepara_dataset
                        "decision_source":  g.get("snapshot", {}).get("decision_source", "GEMINI"),
                        "leverage":         g.get("snapshot", {}).get("leverage", 1),
                        "macro_sentiment":  g.get("snapshot", {}).get("macro_sentiment", "NEUTRAL"),
                        "entry_phase":      g.get("snapshot", {}).get("entry_phase", "FORMAZIONE"),
                    }
                    storico = db_manager.get_storico()
                    storico.append(trade_data)
                    db_manager.save_storico(storico)
                    ml.registra_trade_chiuso(trade_data)
                self.logger.info(f"🤖 [VIRTUAL→ML] {len(da_passare_a_ml)} trade virtuali salvati per XGBoost")
            except Exception as e:
                _err.capture(e, sys._getframe().f_code.co_name, {"module": "FeedbackEngine"})
                self.logger.error(f"Errore invio virtual a ML: {e}")

        if modificati:
            self.ghost_trades = [g for g in self.ghost_trades if g.get("stato") == "PENDING"] +                                 [g for g in self.ghost_trades if g.get("stato") != "PENDING"][-200:]
            self._salva_dati(self.file_ghost, self.ghost_trades)

    def get_feedback_summary(self, asset):
        """Genera un riassunto testuale degli ultimi ghost trades chiusi per il prompt di Gemini."""
        # Cerca i ghost trades chiusi per questo asset
        chiusi = [g for g in self.ghost_trades
                  if g.get("asset") == asset
                  and g.get("stato") == "CLOSED"
                  and g.get("esito") in ("WIN", "LOSS")]

        # Ordina per timestamp di chiusura (più recenti prima)
        chiusi.sort(key=lambda g: g.get("timestamp", 0), reverse=True)

        wins = [g for g in chiusi if g.get("esito") == "WIN"]
        losses = [g for g in chiusi if g.get("esito") == "LOSS"]

        summary = f"Storico decisioni virtuali su {asset}:\n"
        if wins:
            ultimi_wins = wins[:3]
            summary += f"- ✅ Ultimi {len(ultimi_wins)} segnali profittevoli (virtuali):\n"
            for g in ultimi_wins:
                summary += f"  * {g['direzione']} a {g['prezzo_ingresso']:.4f} → "
                summary += f"{g.get('prezzo_uscita', 0):.4f} ({g.get('pnl_perc', 0):+.2f}%)\n"

        if losses:
            ultimi_losses = losses[:3]
            summary += f"- ⚠️ Ultimi {len(ultimi_losses)} segnali perdenti (virtuali):\n"
            for g in ultimi_losses:
                summary += f"  * {g['direzione']} a {g['prezzo_ingresso']:.4f} → "
                summary += f"{g.get('prezzo_uscita', 0):.4f} ({g.get('pnl_perc', 0):+.2f}%)\n"

        if not wins and not losses:
            summary += "- Nessun feedback rilevante recente.\n"

        return summary

    def registra_feedback(self, asset, score, outcome, motivi="", stile_operativo="SWING", apprendimento_critico="", dati_mercato=None):
        """Registra il feedback di un trade concluso per il database delle lezioni."""
        if "feedback_trades" not in self.stats_globali:
            self.stats_globali["feedback_trades"] = []
            
        if "error_matrix" not in self.stats_globali:
            self.stats_globali["error_matrix"] = {}
            
        feedback = {
            "timestamp": time.time(),
            "data": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "asset": asset,
            "voto_ia": score,
            "esito": outcome,
            "motivi": motivi,
            "stile_operativo": stile_operativo,
            "apprendimento_critico": apprendimento_critico,
            "snapshot_mercato": {
                "corr_driver": dati_mercato.get("correlazione_driver", 1.0) if dati_mercato else 1.0,
                "press_supp": dati_mercato.get("pressione_muro_supporto", 0.0) if dati_mercato else 0.0,
                "press_res": dati_mercato.get("pressione_muro_resistenza", 0.0) if dati_mercato else 0.0
            }
        }
        
        self.stats_globali["feedback_trades"].append(feedback)
        # Mantieni gli ultimi 50 feedback
        self.stats_globali["feedback_trades"] = self.stats_globali["feedback_trades"][-50:]

        # Aggiorna la matrice degli errori (LOSS) o dei pattern vincenti (WIN).
        # Simmetria architetturale 2026-05-01: prima si tracciava SOLO LOSS,
        # creando un dataset feedback unilateralmente negativo.
        if outcome in ("LOSS", "WIN") and apprendimento_critico:
            categoria = "ALTRO"
            ac_lower = apprendimento_critico.lower()
            if "trend" in ac_lower or "direzione" in ac_lower:
                categoria = "TREND_FOLLOWING" if outcome == "WIN" else "CONTRO_TREND"
            elif "volatilit" in ac_lower or "spike" in ac_lower:
                categoria = "ALTA_VOLATILITA"
            elif "liquidit" in ac_lower or "vuoto" in ac_lower or "void" in ac_lower:
                categoria = "TRAPPOLA_LIQUIDITA" if outcome == "LOSS" else "LIQUIDITA_FAVOREVOLE"
            elif "timing" in ac_lower or "anticipo" in ac_lower or "ritardo" in ac_lower:
                categoria = "ERRATO_TIMING" if outcome == "LOSS" else "TIMING_OK"

            if outcome == "LOSS":
                if "error_matrix" not in self.stats_globali:
                    self.stats_globali["error_matrix"] = {}
                if asset not in self.stats_globali["error_matrix"]:
                    self.stats_globali["error_matrix"][asset] = {}
                self.stats_globali["error_matrix"][asset][categoria] = self.stats_globali["error_matrix"][asset].get(categoria, 0) + 1
                self.logger.info(f"📊 Aggiornata Error Matrix per {asset}: +1 {categoria}")
            else:  # WIN
                if "success_matrix" not in self.stats_globali:
                    self.stats_globali["success_matrix"] = {}
                if asset not in self.stats_globali["success_matrix"]:
                    self.stats_globali["success_matrix"][asset] = {}
                self.stats_globali["success_matrix"][asset][categoria] = self.stats_globali["success_matrix"][asset].get(categoria, 0) + 1
                self.logger.info(f"✅ Aggiornata Success Matrix per {asset}: +1 {categoria}")

        self._salva_dati(self.file_stats, self.stats_globali)
        self.logger.info(f"📝 Feedback registrato per {asset}: {outcome} (Voto: {score}, Stile: {stile_operativo})")

    def registra_evento(self, asset, evento, dettagli=None):
        """Registra un evento generico nel sistema di feedback."""
        if "eventi_log" not in self.stats_globali:
            self.stats_globali["eventi_log"] = []
            
        log_entry = {
            "timestamp": time.time(),
            "data": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "asset": asset,
            "evento": evento,
            "dettagli": dettagli or {}
        }
        
        self.stats_globali["eventi_log"].append(log_entry)
        # Mantieni gli ultimi 100 eventi
        self.stats_globali["eventi_log"] = self.stats_globali["eventi_log"][-100:]
        
        self._salva_dati(self.file_stats, self.stats_globali)
        self.logger.info(f"🔔 Evento registrato per {asset}: {evento}")

    def get_lezioni_asset(self, asset):
        """Restituisce l'ultima lezione appresa (feedback) per un asset specifico come dizionario."""
        lezioni_salvate = self.stats_globali.get("lezioni_asset", {}).get(asset, [])
        if isinstance(lezioni_salvate, list) and len(lezioni_salvate) > 0:
            ultima = lezioni_salvate[0]
            if isinstance(ultima, dict):
                return ultima
            # Fallback per vecchio formato stringa
            return {"sintesi": ultima, "regola_domani": "—", "condizioni_sfavorevoli": ""}
        return {}

    def aggiorna_lezioni(self, asset, lezioni_dict):
        """Salva le nuove lezioni generate dalla NightReview nel database."""
        if not isinstance(lezioni_dict, dict):
            self.logger.error(f"Errore: lezioni_dict non è un dizionario per {asset}")
            return

        if "lezioni_asset" not in self.stats_globali:
            self.stats_globali["lezioni_asset"] = {}
        
        if asset not in self.stats_globali["lezioni_asset"]:
            self.stats_globali["lezioni_asset"][asset] = []
            
        # Arricchiamo il dict con metadati temporali
        lezioni_dict["timestamp"] = time.time()
        lezioni_dict["data_formattata"] = datetime.now().strftime('%d/%m %H:%M')
        
        # Inseriamo il dizionario completo invece della stringa formattata
        self.stats_globali["lezioni_asset"][asset].insert(0, lezioni_dict)
        
        # Mantieni solo le ultime 5 lezioni per asset
        self.stats_globali["lezioni_asset"][asset] = self.stats_globali["lezioni_asset"][asset][:5]
        
        self._salva_dati(self.file_stats, self.stats_globali)
        self.logger.info(f"🧠 Lezioni aggiornate per {asset} tramite NightReview (formato dict).")

    def get_recent_summary(self):
        """Restituisce un riassunto globale delle performance recenti."""
        # Placeholder per ora, restituisce un dizionario vuoto o una stringa
        return {"win_rate": 0.0, "profit_factor": 0.0, "recent_trades": []}

    def get_stats_globali(self):
        return self.stats_globali

    def get_asset_metrics(self, asset, window=50):
        """Calcola metriche reali basate sullo storico dei trade effettivi dal database."""
        from core.database_manager import db_manager
        storico = db_manager.get_storico()
        
        # Filtriamo i trade per l'asset specifico
        trades = [t for t in storico if t.get("asset") == asset]
        if not trades:
            return {"win_rate": 50.0, "profit_factor": 1.0, "streak_loss": 0, "sharpe_rolling": 0.0}
            
        recenti = trades[-window:]
        wins = [t for t in recenti if t.get("esito") == "WIN"]
        losses = [t for t in recenti if t.get("esito") == "LOSS"]
        
        win_rate = (len(wins) / len(recenti)) * 100 if recenti else 50.0
        
        # Calcolo streak loss (ultime perdite consecutive)
        streak = 0
        for t in reversed(recenti):
            if t.get("esito") == "LOSS":
                streak += 1
            else:
                break
                
        # Calcolo Profit Factor reale (Somma profitti / Somma perdite)
        profitti = sum(float(t.get("pnl_finale", 0)) for t in wins)
        perdite = abs(sum(float(t.get("pnl_finale", 0)) for t in losses))
        
        pf = profitti / perdite if perdite > 0 else (profitti if profitti > 0 else 1.0)
        
        return {
            "win_rate": round(win_rate, 2),
            "profit_factor": round(pf, 2),
            "streak_loss": streak,
            "n_trades": len(recenti)
        }

    def get_strategy_health(self, window=30):
        """
        Rileva l'Alpha Decay calcolando il Profit Factor rolling globale.
        Un Hedge Fund smette di tradare se l'edge strutturale svanisce.

        v2 (2026-05-01): soglie ricalibrate.
        - Servono almeno 20 trade per dichiarare DECAY (era 10) — campioni piccoli
          danno falsi positivi (es. 50 trade con 47 LOSS attivava DECAY permanente
          riducendo sizing -30% su ogni trade futuro, impedendo recupero).
        - DECAY threshold abbassato a PF<0.5 (era <0.8): solo edge davvero sparito.
        """
        trades = self.stats_globali.get("feedback_trades", [])
        if len(trades) < 20:
            return {"status": "HEALTHY", "score": 1.0}

        recenti = trades[-window:]
        wins = len([t for t in recenti if t.get("esito") == "WIN"])
        losses = len([t for t in recenti if t.get("esito") == "LOSS"])

        pf = wins / losses if losses > 0 else float(wins)

        # DECAY solo se PF veramente disastroso
        if pf < 0.5:
            return {"status": "DECAY", "score": pf, "msg": "Alpha Decay rilevato (PF < 0.5)"}
        elif pf < 0.9:
            return {"status": "WARNING", "score": pf, "msg": "Edge in riduzione"}
            
        return {"status": "HEALTHY", "score": pf}

    # ════════════════════════════════════════════════════════════════════════
    # RICOSTRUZIONE FEEDBACK DA STORICO REALE
    # ════════════════════════════════════════════════════════════════════════
    def ricostruisci_feedback_da_storico(self, max_trades=50, dry_run=False):
        """
        Ricostruisce stats_globali['feedback_trades'] e error_matrix/success_matrix
        partendo dai trade veri presenti nello storico_trades del DB.
        
        Necessario perché in passato:
        - night_review hardcodava outcome=LOSS (bug fissato 2026-05-02)
        - feedback_trades era plafonato a 50 entries con FIFO, perdendo storia
        - error_matrix conteneva solo LOSS senza pattern WIN simmetrici
        
        Logica:
        1. Prende i trade chiusi REALI (esito WIN/LOSS, motivo_chiusura presente,
           fonte != STORICO_SIMULATO, fonte != VIRTUAL_BRAIN).
        2. Ordina per data_chiusura (più recenti per ultimi).
        3. Prende gli ultimi `max_trades` (default 50, come slot disponibile).
        4. Costruisce feedback_trades con esiti REALI.
        5. Ricostruisce error_matrix e success_matrix da zero con apprendimento_critico.
        
        Args:
            max_trades: quanti trade tenere (default 50).
            dry_run: se True non scrive nulla, solo ritorna le stats finali.
        
        Returns:
            dict con {n_processati, wins, losses, pf, wr_pct}
        """
        from core.database_manager import db_manager

        try:
            storico = db_manager.get_storico()
        except Exception as e:
            _err.capture(e, "ricostruisci_feedback_da_storico", {"module": "FeedbackEngine"})
            self.logger.error(f"Errore caricamento storico per rebuild: {e}")
            return None

        # Filtra trade VERI
        trade_veri = [
            t for t in storico
            if t.get("esito") in ("WIN", "LOSS")
            and t.get("motivo_chiusura")
            and t.get("fonte") not in ("STORICO_SIMULATO", "VIRTUAL_BRAIN")
        ]

        # Ordina cronologicamente (chiusura)
        trade_veri.sort(
            key=lambda t: str(t.get("data_chiusura", "") or t.get("data_apertura", ""))
        )

        # Prendi gli ultimi max_trades
        ultimi = trade_veri[-max_trades:]

        if not ultimi:
            self.logger.warning("⚠️ Rebuild feedback: nessun trade reale trovato.")
            return None

        # Ricostruzione feedback_trades + matrici
        nuovi_feedback = []
        new_error_matrix = {}
        new_success_matrix = {}

        for t in ultimi:
            asset = t.get("asset", "UNKNOWN")
            esito = t.get("esito")
            voto = t.get("voto_ia") or 0
            try:
                voto = int(voto)
            except (ValueError, TypeError):
                voto = 0
            stile = t.get("tipo_op") or "SWING"
            pattern = t.get("apprendimento_critico", "")
            motivo = t.get("motivo_chiusura", "")
            razionale = t.get("razionale", "")
            pnl = t.get("pnl_finale", 0)
            
            # Timestamp dalla data_chiusura
            ts = 0
            try:
                from datetime import datetime as _dt
                dc = t.get("data_chiusura", "")
                if dc:
                    ts = _dt.fromisoformat(dc.replace("Z", "")).timestamp()
            except Exception:
                ts = time.time()

            snap = t.get("chimera_snapshot", {}) or {}

            feedback = {
                "timestamp": ts,
                "data": t.get("data_chiusura", "")[:19],
                "asset": asset,
                "voto_ia": voto,
                "esito": esito,
                "motivi": f"{motivo}. PNL: {pnl}%. {razionale[:80]}",
                "stile_operativo": stile,
                "apprendimento_critico": pattern,
                "snapshot_mercato": {
                    "corr_driver": snap.get("correlazione_driver", 1.0),
                    "press_supp": snap.get("pressione_muro_supporto", 0.0),
                    "press_res":  snap.get("pressione_muro_resistenza", 0.0),
                },
            }
            nuovi_feedback.append(feedback)

            # Aggiorna matrici (stessa logica di registra_feedback ma simmetrica)
            if pattern:
                ac_lower = pattern.lower()
                categoria = "ALTRO"
                if "trend" in ac_lower or "direzione" in ac_lower:
                    categoria = "TREND_FOLLOWING" if esito == "WIN" else "CONTRO_TREND"
                elif "volatilit" in ac_lower or "spike" in ac_lower:
                    categoria = "ALTA_VOLATILITA"
                elif "liquidit" in ac_lower or "vuoto" in ac_lower or "void" in ac_lower:
                    categoria = "TRAPPOLA_LIQUIDITA" if esito == "LOSS" else "LIQUIDITA_FAVOREVOLE"
                elif "timing" in ac_lower or "anticipo" in ac_lower or "ritardo" in ac_lower:
                    categoria = "ERRATO_TIMING" if esito == "LOSS" else "TIMING_OK"

                target = new_error_matrix if esito == "LOSS" else new_success_matrix
                if asset not in target:
                    target[asset] = {}
                target[asset][categoria] = target[asset].get(categoria, 0) + 1

        # Statistiche finali
        wins = sum(1 for f in nuovi_feedback if f["esito"] == "WIN")
        losses = sum(1 for f in nuovi_feedback if f["esito"] == "LOSS")
        n = len(nuovi_feedback)
        wr = wins * 100 / n if n else 0
        pf = wins / losses if losses else float(wins)

        result = {
            "n_processati": n,
            "wins": wins,
            "losses": losses,
            "wr_pct": round(wr, 2),
            "pf": round(pf, 3),
            "asset_in_error_matrix": len(new_error_matrix),
            "asset_in_success_matrix": len(new_success_matrix),
        }

        if dry_run:
            self.logger.info(f"🔍 [DRY-RUN] Rebuild feedback: {result}")
            return result

        # Scrittura effettiva
        self.stats_globali["feedback_trades"] = nuovi_feedback
        self.stats_globali["error_matrix"] = new_error_matrix
        self.stats_globali["success_matrix"] = new_success_matrix
        self._salva_dati(self.file_stats, self.stats_globali)

        self.logger.info(
            f"✅ Feedback ricostruito da storico: {n} trade, "
            f"{wins}W/{losses}L, WR {wr:.1f}%, PF {pf:.3f}"
        )
        return result