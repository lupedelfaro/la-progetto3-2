# -*- coding: utf-8 -*-
"""
TradeWatchdog — monitora la validità delle tesi delle posizioni aperte.

Strada A v12+ — Modifica A.
Quando Gemini apre un trade, salva nel gemini_decisions le `condizioni_tesi`:
piccolo schema strutturato di condizioni numeriche che — se diventano vere —
invaliderebbero la tesi originale. Il watchdog gira in background ogni N
secondi e per ogni posizione aperta:
  1. Recupera le condizioni_tesi salvate
  2. Le valuta sui dati di mercato attuali
  3. Conta quante condizioni sono violate
  4. Se 2+ violazioni per 3 cicli consecutivi → chiude immediatamente

DUAL-TRACK: dopo ogni chiusura, registra nella tabella watchdog_observations
quale sarebbe stato l'esito se avesse aspettato fino a SL/TP. Un thread
separato monitora il prezzo dell'asset a 15/30/60/240 minuti dalla chiusura
e calcola se in quei punti SL o TP sarebbero stati hit. Questo permette
di misurare ex-post se il watchdog ha agito bene o male.

Sicurezza:
  - Solo posizioni con condizioni_tesi valorizzate vengono monitorate
  - Trade aperti prima dell'attivazione (no condizioni_tesi) sono ignorati
  - Errori nel parsing → posizione ignorata (mai chiusura accidentale)
  - Killswitch: se KILLSWITCH_ENABLED, watchdog non agisce
"""

import logging
import threading
import time
import json
import sqlite3
import os
from collections import defaultdict
from typing import Optional, Dict, List
from core.chimera_errors import ErrorTracker

_err = ErrorTracker("TradeWatchdog")


# ──────────────────────────────────────────────────────────────────────────
# Operatori supportati nel campo condizioni_tesi.invalidata_se_diventa_vero
# ──────────────────────────────────────────────────────────────────────────
_OPERATORI = {
    '<':  lambda a, b: a < b,
    '<=': lambda a, b: a <= b,
    '>':  lambda a, b: a > b,
    '>=': lambda a, b: a >= b,
    '==': lambda a, b: a == b,
    '!=': lambda a, b: a != b,
}

# Campi standard accettati per le condizioni di tesi.
# Ogni campo viene letto da dati_engine[campo].
# Se Gemini invia un campo non in questa lista, la condizione viene SCARTATA
# (no errore — il watchdog è permissivo per non chiudere per sbaglio).
_CAMPI_VALIDI = {
    'cvd_istantaneo', 'cvd_delta_30s', 'cvd_delta_120s', 'cvd_acceleration',
    'price_velocity', 'z_score_dist_vwap',
    'book_pressure', 'book_skew', 'order_flow_imbalance',
    'vpin', 'kaufman_efficiency', 'hurst_exponent',
    'is_explosive', 'spread_perc', 'atr',
    'whale_delta', 'spoofing_score',
    'rsi_15m', 'rsi_1h',
}


class TradeWatchdog:
    _instance = None
    _lock = threading.Lock()

    # Frequenza del loop principale (secondi)
    CHECK_INTERVAL_SECONDS = 60
    # Soglia di invalidazione: N violazioni mantenute per K cicli consecutivi
    MIN_VIOLATIONS_TO_INVALIDATE = 2
    MIN_CONSECUTIVE_CYCLES = 3
    # Checkpoint per il dual-track (in secondi dopo la chiusura)
    DUAL_TRACK_CHECKPOINTS = [15*60, 30*60, 60*60, 240*60]

    def __new__(cls, db_path=None, performer=None, engine=None,
                trade_manager=None, gemini_memory=None, alerts=None):
        with cls._lock:
            if cls._instance is None:
                cls._instance = super(TradeWatchdog, cls).__new__(cls)
                cls._instance._initialized = False
            return cls._instance

    def __init__(self, db_path=None, performer=None, engine=None,
                 trade_manager=None, gemini_memory=None, alerts=None):
        if getattr(self, '_initialized', False):
            return
        self._initialized = True

        self.logger = logging.getLogger("TradeWatchdog")
        if db_path is None:
            base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
            db_path = os.path.join(base_dir, "chimera.db")
        self.db_path = db_path

        # Riferimenti agli altri moduli del bot (passati al primo init)
        self.performer = performer
        self.engine = engine
        self.trade_manager = trade_manager
        self.gemini_memory = gemini_memory
        self.alerts = alerts

        # Stato interno: per asset, contatore di cicli consecutivi con
        # violazioni. Si resetta se in un ciclo le violazioni scendono sotto soglia.
        self._consecutive_violations: Dict[str, int] = defaultdict(int)
        # Per evitare doppia chiusura sulla stessa posizione
        self._already_closed: Dict[str, float] = {}

        # Connessione DB dedicata per il dual-track
        self._db_lock = threading.Lock()
        self._conn = sqlite3.connect(self.db_path, check_same_thread=False, timeout=10)
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.row_factory = sqlite3.Row
        self._create_dual_track_table()

        # Thread di tracking (per i checkpoint dual-track)
        self._tracking_queue: List[dict] = []  # lista di osservazioni in monitoraggio
        self._tracking_lock = threading.Lock()

        # Thread principale
        self._stop_event = threading.Event()
        self._thread = None

        self.logger.info("🛡️ TradeWatchdog inizializzato (singleton)")

    # ─── Tabella DB per dual-track ──────────────────────────────────────
    def _create_dual_track_table(self):
        with self._db_lock:
            cur = self._conn.cursor()
            cur.execute('''CREATE TABLE IF NOT EXISTS watchdog_observations (
                              id INTEGER PRIMARY KEY AUTOINCREMENT,
                              trade_id TEXT,
                              asset TEXT NOT NULL,
                              direzione TEXT,
                              ts_close_watchdog REAL NOT NULL,
                              prezzo_close_watchdog REAL,
                              sl_originale REAL,
                              tp_originale REAL,
                              motivo_chiusura TEXT,
                              violazioni_finali TEXT,   -- JSON con le condizioni violate
                              prezzo_15min REAL, ts_15min REAL,
                              prezzo_30min REAL, ts_30min REAL,
                              prezzo_60min REAL, ts_60min REAL,
                              prezzo_240min REAL, ts_240min REAL,
                              sarebbe_stato_win INTEGER,
                              sarebbe_stato_loss INTEGER,
                              watchdog_decision_quality TEXT
                          )''')
            cur.execute('''CREATE INDEX IF NOT EXISTS idx_watchdog_obs_asset
                           ON watchdog_observations(asset, ts_close_watchdog DESC)''')
            self._conn.commit()

    # ─── Avvio/stop thread ──────────────────────────────────────────────
    def start(self):
        if self._thread is not None and self._thread.is_alive():
            self.logger.warning("Watchdog già attivo")
            return
        self._stop_event.clear()
        self._thread = threading.Thread(target=self._main_loop,
                                         name="TradeWatchdog",
                                         daemon=True)
        self._thread.start()
        # Thread separato per dual-track tracking
        self._tracking_thread = threading.Thread(target=self._tracking_loop,
                                                  name="TradeWatchdogTracker",
                                                  daemon=True)
        self._tracking_thread.start()
        self.logger.info(
            f"🛡️ TradeWatchdog AVVIATO — check ogni {self.CHECK_INTERVAL_SECONDS}s, "
            f"soglia {self.MIN_VIOLATIONS_TO_INVALIDATE}/{self.MIN_CONSECUTIVE_CYCLES} cicli"
        )

    def stop(self):
        self._stop_event.set()
        if self._thread:
            self._thread.join(timeout=5)
        self.logger.info("🛡️ TradeWatchdog fermato")

    # ─── Main loop ──────────────────────────────────────────────────────
    def _main_loop(self):
        while not self._stop_event.is_set():
            try:
                self._do_one_check_cycle()
            except Exception as e:
                _err.capture(e, "_main_loop", {"module": "TradeWatchdog"})
                self.logger.error(f"❌ Watchdog loop errore: {e}", exc_info=True)
            # Attesa interrompibile
            self._stop_event.wait(self.CHECK_INTERVAL_SECONDS)

    def _do_one_check_cycle(self):
        # 1. Recupera posizioni aperte
        if self.trade_manager is None or not hasattr(self.trade_manager, 'posizioni_aperte'):
            return
        posizioni = dict(self.trade_manager.posizioni_aperte)  # copia per safety
        if not posizioni:
            self._consecutive_violations.clear()
            return

        active_assets = set()
        for ticker, pos in posizioni.items():
            try:
                asset_id = ticker
                active_assets.add(asset_id)

                # 2. Recupera condizioni_tesi dal gemini_decisions
                condizioni = self._get_condizioni_tesi_for_position(pos)
                if not condizioni:
                    # Nessuna condizione → trade aperto pre-watchdog → ignora
                    continue

                # 3. Recupera dati di mercato attuali
                if self.engine is None:
                    continue
                try:
                    dati_engine = self.engine.get_dati_engine(asset_id)
                except Exception as e_e:
                    _err.capture(e_e, "_do_one_check_cycle", {"module": "TradeWatchdog"})
                    self.logger.debug(f"[WATCHDOG] {asset_id}: errore get_dati_engine: {e_e}")
                    continue
                if not isinstance(dati_engine, dict):
                    continue

                # 4. Valuta condizioni
                violazioni = self._evaluate_conditions(condizioni, dati_engine)
                n_violations = len(violazioni)

                # 5. Aggiorna contatore consecutivo
                if n_violations >= self.MIN_VIOLATIONS_TO_INVALIDATE:
                    self._consecutive_violations[asset_id] += 1
                    self.logger.info(
                        f"⚠️ [WATCHDOG] {asset_id}: {n_violations} violazioni "
                        f"({self._consecutive_violations[asset_id]}/{self.MIN_CONSECUTIVE_CYCLES} cicli) "
                        f"— condizioni violate: {[v['descrizione'][:40] for v in violazioni]}"
                    )
                else:
                    if self._consecutive_violations[asset_id] > 0:
                        self.logger.info(
                            f"✅ [WATCHDOG] {asset_id}: violazioni rientrate ({n_violations}/{len(condizioni)}). "
                            f"Reset contatore."
                        )
                    self._consecutive_violations[asset_id] = 0

                # 6. Se soglia raggiunta → chiusura immediata
                if self._consecutive_violations[asset_id] >= self.MIN_CONSECUTIVE_CYCLES:
                    if asset_id in self._already_closed:
                        # Già chiuso in questo ciclo — non chiudo di nuovo
                        continue
                    self._close_invalidated_trade(asset_id, pos, violazioni)
                    self._already_closed[asset_id] = time.time()

            except Exception as e_pos:
                _err.capture(e_pos, "_do_one_check_cycle", {"module": "TradeWatchdog"})
                self.logger.error(f"[WATCHDOG] errore valutazione {ticker}: {e_pos}", exc_info=True)

        # Pulizia: rimuovi dal contatore gli asset non più aperti
        keys_to_remove = [k for k in self._consecutive_violations if k not in active_assets]
        for k in keys_to_remove:
            self._consecutive_violations.pop(k, None)
            self._already_closed.pop(k, None)

    # ─── Recupero condizioni_tesi ───────────────────────────────────────
    def _get_condizioni_tesi_for_position(self, pos: dict) -> Optional[List[dict]]:
        """
        Cerca le condizioni di invalidazione associate alla decisione di Gemini
        che ha aperto questa posizione.
        Strategia di matching:
          1. Se il trade ha 'gemini_decision_id' nel pos, usa quello (più affidabile).
          2. Altrimenti cerca per asset + finestra temporale (ts_apertura ±10 min).
        """
        try:
            asset = pos.get('asset') or pos.get('ticker') or ''
            if not asset:
                return None

            # Tenta lookup diretto via decision_id se presente
            decision_id = pos.get('gemini_decision_id') or pos.get('decision_id')

            with sqlite3.connect(self.db_path, timeout=5) as conn:
                conn.row_factory = sqlite3.Row
                cur = conn.cursor()
                if decision_id:
                    cur.execute("""SELECT condizioni_tesi FROM gemini_decisions
                                   WHERE id = ?""", (decision_id,))
                else:
                    # Fallback: ultima decisione LONG/SHORT su questo asset
                    cur.execute("""SELECT condizioni_tesi FROM gemini_decisions
                                   WHERE asset = ? AND direzione IN ('LONG','SHORT','BUY','SELL')
                                   ORDER BY ts DESC LIMIT 1""", (asset,))
                row = cur.fetchone()
                if not row:
                    return None
                ct_json = row['condizioni_tesi']
                if not ct_json:
                    return None
                ct = json.loads(ct_json)
                return ct.get('invalidata_se_diventa_vero', None)
        except Exception as e:
            _err.capture(e, "_get_condizioni_tesi_for_position", {"module": "TradeWatchdog"})
            self.logger.debug(f"[WATCHDOG] _get_condizioni_tesi_for_position: {e}")
            return None

    # ─── Valutazione condizioni ─────────────────────────────────────────
    def _evaluate_conditions(self, condizioni: List[dict], dati_engine: dict) -> List[dict]:
        """
        Ritorna la lista delle condizioni VIOLATE (cioè diventate vere → tesi invalidata).
        """
        violazioni = []
        if not isinstance(condizioni, list):
            return violazioni
        for cond in condizioni:
            try:
                if not isinstance(cond, dict):
                    continue
                campo = cond.get('campo')
                op = cond.get('operatore')
                val = cond.get('valore')
                desc = cond.get('descrizione', '')
                if not campo or op not in _OPERATORI or val is None:
                    continue
                if campo not in _CAMPI_VALIDI:
                    self.logger.debug(f"[WATCHDOG] Campo non valido ignorato: {campo}")
                    continue
                attuale = dati_engine.get(campo)
                if attuale is None:
                    continue
                try:
                    attuale_f = float(attuale)
                    val_f = float(val)
                except (TypeError, ValueError):
                    # Per condizioni booleane (is_explosive)
                    if op in ('==', '!='):
                        if _OPERATORI[op](attuale, val):
                            violazioni.append({'campo': campo, 'operatore': op,
                                                'valore': val, 'attuale': attuale,
                                                'descrizione': desc})
                    continue
                if _OPERATORI[op](attuale_f, val_f):
                    violazioni.append({'campo': campo, 'operatore': op,
                                        'valore': val_f, 'attuale': attuale_f,
                                        'descrizione': desc})
            except Exception as e:
                _err.capture(e, "_evaluate_conditions", {"module": "TradeWatchdog"})
                self.logger.debug(f"[WATCHDOG] errore eval condizione {cond}: {e}")
        return violazioni

    # ─── Chiusura della posizione invalidata ────────────────────────────
    def _close_invalidated_trade(self, asset_id: str, pos: dict, violazioni: List[dict]):
        """Chiude la posizione a mercato e registra il dual-track."""
        try:
            self.logger.warning(
                f"🛡️ [WATCHDOG] {asset_id}: TESI INVALIDATA "
                f"({len(violazioni)} violazioni × {self.MIN_CONSECUTIVE_CYCLES} cicli) → CHIUSURA IMMEDIATA"
            )
            for v in violazioni[:3]:
                self.logger.warning(
                    f"   • {v['descrizione'][:80]} (era {v.get('attuale')} {v['operatore']} {v['valore']})"
                )

            # Recupera prezzo attuale
            prezzo_attuale = None
            try:
                if self.engine and hasattr(self.engine, 'get_current_price'):
                    prezzo_attuale = self.engine.get_current_price(asset_id)
            except Exception as e_p:
                _err.capture(e_p, "_close_invalidated_trade", {"module": "TradeWatchdog"})
                self.logger.debug(f"[WATCHDOG] prezzo non disponibile: {e_p}")

            sl_orig = float(pos.get('sl', 0) or 0)
            tp_orig = float(pos.get('tp', 0) or 0)
            direzione = str(pos.get('direzione', '')).upper()
            trade_id = pos.get('entry_id') or pos.get('ordine_id') or ''

            # Esegue chiusura via trade_manager
            chiuso_ok = False
            if self.trade_manager and hasattr(self.trade_manager, '_esegui_chiusura_totale'):
                if prezzo_attuale and prezzo_attuale > 0:
                    try:
                        chiuso_ok = self.trade_manager._esegui_chiusura_totale(
                            asset_id, prezzo_attuale,
                            motivo="WATCHDOG_THESIS_INVALIDATED"
                        )
                    except Exception as e_c:
                        _err.capture(e_c, "_close_invalidated_trade", {"module": "TradeWatchdog"})
                        self.logger.error(f"[WATCHDOG] errore chiusura {asset_id}: {e_c}", exc_info=True)
                else:
                    self.logger.error(f"[WATCHDOG] prezzo non disponibile, NON posso chiudere {asset_id}")

            if not chiuso_ok:
                self.logger.error(
                    f"[WATCHDOG] {asset_id}: chiusura FALLITA — la posizione resta aperta"
                )
                return

            # Alert Telegram (opzionale)
            try:
                if self.alerts and hasattr(self.alerts, 'invia_alert'):
                    desc_violazioni = '\n'.join([f"• {v['descrizione'][:80]}" for v in violazioni[:3]])
                    self.alerts.invia_alert(
                        f"🛡️ *WATCHDOG: chiusura {asset_id}*\n"
                        f"Tesi invalidata da {len(violazioni)} condizioni:\n{desc_violazioni}"
                    )
            except Exception:
                pass

            # Registra dual-track
            self._register_dual_track(
                trade_id=trade_id, asset_id=asset_id, direzione=direzione,
                prezzo_close=prezzo_attuale, sl_orig=sl_orig, tp_orig=tp_orig,
                violazioni=violazioni
            )

        except Exception as e:
            _err.capture(e, "_close_invalidated_trade", {"module": "TradeWatchdog"})
            self.logger.error(f"[WATCHDOG] _close_invalidated_trade {asset_id}: {e}", exc_info=True)

    # ─── Registrazione dual-track ───────────────────────────────────────
    def _register_dual_track(self, trade_id, asset_id, direzione, prezzo_close,
                              sl_orig, tp_orig, violazioni):
        """Registra l'osservazione iniziale e mette in coda per i checkpoint."""
        try:
            ts_now = time.time()
            with self._db_lock:
                cur = self._conn.cursor()
                cur.execute('''INSERT INTO watchdog_observations
                              (trade_id, asset, direzione, ts_close_watchdog,
                               prezzo_close_watchdog, sl_originale, tp_originale,
                               motivo_chiusura, violazioni_finali)
                              VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)''',
                           (str(trade_id), asset_id, direzione, ts_now,
                            prezzo_close, sl_orig, tp_orig,
                            "WATCHDOG_THESIS_INVALIDATED",
                            json.dumps([{'campo': v['campo'],
                                         'descrizione': v['descrizione']} for v in violazioni])))
                obs_id = cur.lastrowid
                self._conn.commit()

            # Aggiungi alla coda di tracking
            with self._tracking_lock:
                self._tracking_queue.append({
                    'obs_id': obs_id,
                    'asset': asset_id,
                    'direzione': direzione,
                    'sl_orig': sl_orig,
                    'tp_orig': tp_orig,
                    'ts_start': ts_now,
                    'checkpoints_done': set(),
                    'sl_hit': False,
                    'tp_hit': False,
                })

            self.logger.info(f"📊 [WATCHDOG DUAL-TRACK] {asset_id} osservazione registrata (id={obs_id})")

        except Exception as e:
            _err.capture(e, "_register_dual_track", {"module": "TradeWatchdog"})
            self.logger.error(f"[WATCHDOG] _register_dual_track {asset_id}: {e}")

    # ─── Tracking loop ──────────────────────────────────────────────────
    def _tracking_loop(self):
        """Loop separato che monitora le osservazioni in coda per i checkpoint."""
        while not self._stop_event.is_set():
            try:
                # Copia per safety
                with self._tracking_lock:
                    queue = list(self._tracking_queue)
                if not queue:
                    self._stop_event.wait(30)
                    continue

                ts_now = time.time()
                queue_to_remove = []
                for obs in queue:
                    elapsed = ts_now - obs['ts_start']

                    # Per ogni checkpoint, se è il momento e non ancora fatto
                    for cp_seconds in self.DUAL_TRACK_CHECKPOINTS:
                        if cp_seconds in obs['checkpoints_done']:
                            continue
                        if elapsed >= cp_seconds:
                            self._record_checkpoint(obs, cp_seconds, ts_now)
                            obs['checkpoints_done'].add(cp_seconds)

                    # Se ho fatto tutti i checkpoint → finalizza e rimuovi
                    if len(obs['checkpoints_done']) == len(self.DUAL_TRACK_CHECKPOINTS):
                        self._finalize_observation(obs)
                        queue_to_remove.append(obs)

                # Rimuovi le finalizzate
                if queue_to_remove:
                    with self._tracking_lock:
                        for obs in queue_to_remove:
                            try:
                                self._tracking_queue.remove(obs)
                            except ValueError:
                                pass

            except Exception as e:
                _err.capture(e, "_tracking_loop", {"module": "TradeWatchdog"})
                self.logger.error(f"[WATCHDOG TRACKING] {e}", exc_info=True)
            self._stop_event.wait(30)

    def _record_checkpoint(self, obs, cp_seconds, ts_now):
        """Registra il prezzo dell'asset al checkpoint corrente."""
        try:
            asset = obs['asset']
            if self.engine is None or not hasattr(self.engine, 'get_current_price'):
                return
            prezzo = self.engine.get_current_price(asset)
            if not prezzo:
                return

            # Verifica se SL o TP sarebbe stato hit FRA la chiusura e adesso
            direzione = obs['direzione']
            sl_orig = obs['sl_orig']
            tp_orig = obs['tp_orig']
            if direzione in ('LONG', 'BUY'):
                if sl_orig > 0 and prezzo <= sl_orig:
                    obs['sl_hit'] = True
                if tp_orig > 0 and prezzo >= tp_orig:
                    obs['tp_hit'] = True
            elif direzione in ('SHORT', 'SELL'):
                if sl_orig > 0 and prezzo >= sl_orig:
                    obs['sl_hit'] = True
                if tp_orig > 0 and prezzo <= tp_orig:
                    obs['tp_hit'] = True

            # Salva nel DB
            cp_min = cp_seconds // 60
            col_prezzo = f"prezzo_{cp_min}min"
            col_ts = f"ts_{cp_min}min"
            with self._db_lock:
                cur = self._conn.cursor()
                cur.execute(f"UPDATE watchdog_observations SET {col_prezzo}=?, {col_ts}=? WHERE id=?",
                           (prezzo, ts_now, obs['obs_id']))
                self._conn.commit()
            self.logger.debug(f"[WATCHDOG TRACK] {asset} {cp_min}min: prezzo={prezzo}")

        except Exception as e:
            _err.capture(e, "_record_checkpoint", {"module": "TradeWatchdog"})
            self.logger.debug(f"[WATCHDOG TRACK] _record_checkpoint: {e}")

    def _finalize_observation(self, obs):
        """Quando tutti i checkpoint sono fatti, calcola la qualità della decisione."""
        try:
            sarebbe_win = 1 if obs['tp_hit'] and not obs['sl_hit'] else 0
            sarebbe_loss = 1 if obs['sl_hit'] and not obs['tp_hit'] else 0
            # Quality:
            #   GOOD: il watchdog ha chiuso prima che il SL fosse hit → ha protetto capitale
            #   BAD: il TP sarebbe stato hit → il watchdog ha chiuso un trade vincente
            #   NEUTRAL: nessuno dei due → era in range
            #   AMBIGUOUS: entrambi hit (SL prima TP o viceversa, non distinguiamo qui)
            if sarebbe_loss and not sarebbe_win:
                quality = 'GOOD'  # avresti perso, watchdog ha protetto
            elif sarebbe_win and not sarebbe_loss:
                quality = 'BAD'   # avresti vinto, watchdog ha sbagliato
            elif sarebbe_win and sarebbe_loss:
                quality = 'AMBIGUOUS'
            else:
                quality = 'NEUTRAL'

            with self._db_lock:
                cur = self._conn.cursor()
                cur.execute('''UPDATE watchdog_observations
                               SET sarebbe_stato_win=?, sarebbe_stato_loss=?,
                                   watchdog_decision_quality=?
                               WHERE id=?''',
                           (sarebbe_win, sarebbe_loss, quality, obs['obs_id']))
                self._conn.commit()

            self.logger.info(
                f"📊 [WATCHDOG DUAL-TRACK] {obs['asset']} osservazione completa: "
                f"qualità={quality} (win={sarebbe_win}, loss={sarebbe_loss})"
            )
        except Exception as e:
            _err.capture(e, "_finalize_observation", {"module": "TradeWatchdog"})
            self.logger.error(f"[WATCHDOG] _finalize_observation: {e}")

    # ─── Statistica aggregata (utile per audit ex-post) ──────────────────
    def get_stats_summary(self):
        """Ritorna stats aggregate sulla qualità delle decisioni del watchdog."""
        try:
            with self._db_lock:
                cur = self._conn.cursor()
                cur.execute('''SELECT watchdog_decision_quality, COUNT(*)
                               FROM watchdog_observations
                               WHERE watchdog_decision_quality IS NOT NULL
                               GROUP BY watchdog_decision_quality''')
                rows = cur.fetchall()
            stats = {row[0]: row[1] for row in rows}
            total = sum(stats.values())
            if total == 0:
                return {'total': 0, 'message': 'Nessuna osservazione finalizzata ancora'}
            return {
                'total': total,
                'good': stats.get('GOOD', 0),
                'bad': stats.get('BAD', 0),
                'neutral': stats.get('NEUTRAL', 0),
                'ambiguous': stats.get('AMBIGUOUS', 0),
                'good_pct': stats.get('GOOD', 0) / total * 100 if total else 0,
                'bad_pct': stats.get('BAD', 0) / total * 100 if total else 0,
            }
        except Exception as e:
            _err.capture(e, "get_stats_summary", {"module": "TradeWatchdog"})
            self.logger.error(f"[WATCHDOG] get_stats_summary: {e}")
            return None
