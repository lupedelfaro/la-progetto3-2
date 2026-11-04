# -*- coding: utf-8 -*-
"""
MacroThesisCache — tesi macro del giorno, condivisa tra tutte le decisioni.

Scopo (Modifica A v10): un trader istituzionale non parte mai da "cosa fa il
prezzo di SOL?" Parte da "qual è il narrativo macro oggi, e SOL lo conferma o
lo smentisce?". Senza questa coerenza, ogni decisione è un'isola e Gemini
contraddice se stesso tra asset diversi nello stesso ciclo.

Questo modulo gestisce una tesi macro che:
- viene formulata UNA VOLTA ogni N ore (default 4h) da una chiamata a Gemini
  con contesto macro (BTC trend, ETH leader, DXY, funding, fear/greed)
- viene RIUSATA da tutte le decisioni nelle prossime N ore come "view del desk"
- viene mostrata in cima al prompt di analisi asset, così ogni decisione è
  ancorata a una vista coerente del mercato

Tabella DB: macro_thesis
  id          INTEGER PK AUTO
  ts          REAL    — timestamp di formulazione
  expires_at  REAL    — timestamp scadenza (ts + N ore)
  tesi        TEXT    — la tesi narrativa (3-5 frasi)
  bias        TEXT    — bias preferito (LONG_BIAS / SHORT_BIAS / NEUTRAL / RISK_OFF)
  scenari_macro TEXT  — JSON con scenari macro possibili
  contesto    TEXT    — JSON con i dati macro al momento della formulazione

Falsy: se la tabella è vuota o l'ultima tesi è scaduta, il sistema funziona
come prima (nessuna tesi macro nel prompt). Il modulo è non-bloccante.
"""

import sqlite3
import threading
import time
import os
import logging
import json
from core.chimera_errors import ErrorTracker

_err = ErrorTracker("MacroThesis")


class MacroThesisCache:
    _instance = None
    _lock = threading.Lock()

    # Durata di una tesi macro: 4 ore. Dopo va ri-formulata.
    THESIS_TTL_SECONDS = 4 * 3600

    def __new__(cls, db_path=None):
        with cls._lock:
            if cls._instance is None:
                if db_path is None:
                    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
                    db_path = os.path.join(base_dir, "chimera.db")
                cls._instance = super(MacroThesisCache, cls).__new__(cls)
                cls._instance._init_db(db_path)
            return cls._instance

    def _init_db(self, db_path):
        self.db_path = db_path
        self.logger = logging.getLogger("MacroThesisCache")
        self._conn_lock = threading.Lock()
        self._conn = sqlite3.connect(self.db_path, check_same_thread=False, timeout=10)
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA synchronous=NORMAL")
        self._conn.row_factory = sqlite3.Row
        self._create_table()

    def _create_table(self):
        with self._conn_lock:
            cur = self._conn.cursor()
            cur.execute('''CREATE TABLE IF NOT EXISTS macro_thesis (
                              id           INTEGER PRIMARY KEY AUTOINCREMENT,
                              ts           REAL NOT NULL,
                              expires_at   REAL NOT NULL,
                              tesi         TEXT,
                              bias         TEXT,
                              scenari_macro TEXT,
                              contesto     TEXT
                          )''')
            cur.execute('''CREATE INDEX IF NOT EXISTS idx_macro_thesis_expires
                           ON macro_thesis(expires_at DESC)''')
            self._conn.commit()

    # ─── Lettura tesi corrente ─────────────────────────────────────────────
    def get_current_thesis(self):
        """
        Ritorna la tesi macro attualmente valida (non scaduta), oppure None.
        """
        try:
            now = time.time()
            with self._conn_lock:
                cur = self._conn.cursor()
                cur.execute('''SELECT * FROM macro_thesis
                               WHERE expires_at > ?
                               ORDER BY ts DESC LIMIT 1''', (now,))
                row = cur.fetchone()
                return dict(row) if row else None
        except Exception as e:
            _err.capture(e, "get_current_thesis", {"module": "MacroThesis"})
            self.logger.warning(f"MacroThesisCache.get_current_thesis error: {e}")
            return None

    def is_thesis_stale(self):
        """True se non c'è tesi valida (vuoto o scaduto). Usato per decidere
           se è il momento di ri-formulare."""
        return self.get_current_thesis() is None

    # ─── Salvataggio nuova tesi ────────────────────────────────────────────
    def save_thesis(self, tesi, bias='NEUTRAL', scenari_macro=None, contesto=None):
        """
        Salva una nuova tesi macro. Diventa attiva immediatamente per le prossime
        THESIS_TTL_SECONDS secondi.
        """
        try:
            now = time.time()
            expires = now + self.THESIS_TTL_SECONDS
            scenari_str = json.dumps(scenari_macro) if scenari_macro else "[]"
            contesto_str = json.dumps(contesto) if contesto else "{}"
            with self._conn_lock:
                cur = self._conn.cursor()
                cur.execute('''INSERT INTO macro_thesis (ts, expires_at, tesi, bias, scenari_macro, contesto)
                               VALUES (?, ?, ?, ?, ?, ?)''',
                            (now, expires, (tesi or '')[:1500], bias,
                             scenari_str[:2000], contesto_str[:3000]))
                self._conn.commit()
                return cur.lastrowid
        except Exception as e:
            _err.capture(e, "save_thesis", {"module": "MacroThesis"})
            self.logger.warning(f"MacroThesisCache.save_thesis error: {e}")
            return None

    # ─── Formattazione per il prompt ───────────────────────────────────────
    def format_for_prompt(self):
        """
        Produce il blocco testuale che va in cima al prompt di analisi asset.
        Se non c'è tesi valida, ritorna stringa vuota (graceful — il prompt
        funziona come prima senza il blocco).
        """
        t = self.get_current_thesis()
        if not t:
            return ""

        mins_ago = int((time.time() - t['ts']) / 60)
        bias = t.get('bias', 'NEUTRAL')

        bias_label = {
            'LONG_BIAS':  'Bias LONG sui pullback. SHORT solo su confluenza forte.',
            'SHORT_BIAS': 'Bias SHORT sui rebound. LONG solo su capitulation chiara.',
            'NEUTRAL':    'Bias neutrale — nessuna direzione preferita. Trade simmetrici accettabili.',
            'RISK_OFF':   'RISK-OFF: ridurre esposizione, massima cautela su entry direzionali.',
            'RISK_ON':    'RISK-ON: ambiente favorevole a movimenti direzionali in breakout.',
        }.get(bias, bias)

        block = (
            "═══════════════════════════════════════════════════════════════\n"
            f"TESI MACRO ATTIVA (formulata {mins_ago} min fa, valida ancora qualche ora)\n"
            "═══════════════════════════════════════════════════════════════\n"
            f"{t.get('tesi', '(tesi vuota)')}\n\n"
            f"Bias del desk per oggi: {bias_label}\n\n"
            "Quando analizzi un asset, ancora la tua decisione a questa tesi:\n"
            "se questo asset CONFERMA la tesi → procedi normalmente.\n"
            "se questo asset DIVERGE dalla tesi → puoi tradare contro, ma DEVI giustificarlo\n"
            "  esplicitamente in 'ragionamento_decisione' (es. 'tesi macro è long ma SOL\n"
            "  mostra rifiuto chiaro su 87.20 con CVD negativo — short controtrend voto 7').\n"
        )
        return block


class PendingScenarios:
    """
    Modifica B (versione soft) — gestisce gli scenari condizionali che Gemini
    formula su ogni asset. Ogni scenario ha un trigger e un'azione associata.
    Al ciclo successivo, Gemini vede gli scenari ancora pendenti e può
    riconoscere se uno è ora triggerato.

    NOTA: in questa versione "soft" il bot NON esegue automaticamente il
    trigger — è Gemini stesso, al ciclo successivo, che valuta se lo scenario
    è scattato e in quel caso emette la decisione corrispondente. È meno
    performante di un trigger automatico ma molto più semplice da debuggare.
    """
    _instance = None
    _lock = threading.Lock()

    SCENARIO_TTL_SECONDS = 30 * 60  # uno scenario "scade" dopo 30 minuti

    def __new__(cls, db_path=None):
        with cls._lock:
            if cls._instance is None:
                if db_path is None:
                    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
                    db_path = os.path.join(base_dir, "chimera.db")
                cls._instance = super(PendingScenarios, cls).__new__(cls)
                cls._instance._init_db(db_path)
            return cls._instance

    def _init_db(self, db_path):
        self.db_path = db_path
        self.logger = logging.getLogger("PendingScenarios")
        self._conn_lock = threading.Lock()
        self._conn = sqlite3.connect(self.db_path, check_same_thread=False, timeout=10)
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA synchronous=NORMAL")
        self._conn.row_factory = sqlite3.Row
        self._create_table()

    def _create_table(self):
        with self._conn_lock:
            cur = self._conn.cursor()
            cur.execute('''CREATE TABLE IF NOT EXISTS pending_scenarios (
                              id           INTEGER PRIMARY KEY AUTOINCREMENT,
                              asset        TEXT NOT NULL,
                              ts           REAL NOT NULL,
                              expires_at   REAL NOT NULL,
                              kind         TEXT,    -- 'principale' / 'alternativo'
                              trigger      TEXT,    -- descrizione narrativa del trigger
                              azione       TEXT,    -- LONG/SHORT + voto + stile
                              stop_logico  REAL,
                              target_logico REAL,
                              prezzo_ref   REAL,    -- prezzo al momento della formulazione
                              status       TEXT     -- 'PENDING' / 'TRIGGERED' / 'EXPIRED' / 'INVALIDATED'
                          )''')
            cur.execute('''CREATE INDEX IF NOT EXISTS idx_pending_asset_status
                           ON pending_scenarios(asset, status, expires_at DESC)''')
            self._conn.commit()

    def save_scenario(self, asset, kind, trigger, azione, stop_logico, target_logico, prezzo_ref):
        """Salva uno scenario pendente. Sovrascrive eventuali scenari precedenti
           dello stesso asset (un asset ha 1 scenario principale + 1 alternativo)."""
        try:
            # Invalida vecchi scenari dello stesso kind per questo asset
            with self._conn_lock:
                cur = self._conn.cursor()
                cur.execute('''UPDATE pending_scenarios
                               SET status='INVALIDATED'
                               WHERE asset=? AND kind=? AND status='PENDING' ''',
                            (asset, kind))
                now = time.time()
                cur.execute('''INSERT INTO pending_scenarios
                              (asset, ts, expires_at, kind, trigger, azione,
                               stop_logico, target_logico, prezzo_ref, status)
                              VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'PENDING')''',
                            (asset, now, now + self.SCENARIO_TTL_SECONDS,
                             kind, (trigger or '')[:300], (azione or '')[:200],
                             stop_logico, target_logico, prezzo_ref))
                self._conn.commit()
                return cur.lastrowid
        except Exception as e:
            _err.capture(e, "save_scenario", {"module": "MacroThesis"})
            self.logger.warning(f"PendingScenarios.save_scenario error: {e}")
            return None

    def get_active_scenarios(self, asset):
        """Ritorna scenari pendenti non scaduti per l'asset, ordine cronologico."""
        try:
            now = time.time()
            with self._conn_lock:
                cur = self._conn.cursor()
                cur.execute('''SELECT * FROM pending_scenarios
                               WHERE asset=? AND status='PENDING' AND expires_at > ?
                               ORDER BY ts DESC''', (asset, now))
                rows = cur.fetchall()
                return [dict(r) for r in rows]
        except Exception as e:
            _err.capture(e, "get_active_scenarios", {"module": "MacroThesis"})
            self.logger.warning(f"PendingScenarios.get_active_scenarios error: {e}")
            return []

    def expire_old(self):
        """Marca come EXPIRED gli scenari scaduti. Chiamato periodicamente."""
        try:
            now = time.time()
            with self._conn_lock:
                cur = self._conn.cursor()
                cur.execute('''UPDATE pending_scenarios
                               SET status='EXPIRED'
                               WHERE status='PENDING' AND expires_at <= ?''', (now,))
                self._conn.commit()
                return cur.rowcount
        except Exception as e:
            _err.capture(e, "expire_old", {"module": "MacroThesis"})
            self.logger.warning(f"PendingScenarios.expire_old error: {e}")
            return 0

    def format_for_prompt(self, asset):
        """Produce il blocco testuale degli scenari pendenti per un asset."""
        scenarios = self.get_active_scenarios(asset)
        if not scenarios:
            return ""  # nessuno scenario pendente — niente blocco
        now = time.time()
        lines = [f"SCENARI PENDENTI SU {asset} (formulati al ciclo precedente):"]
        for s in scenarios:
            mins = int((now - s['ts']) / 60)
            lines.append(
                f"  [{mins} min fa] Scenario {s['kind']}: "
                f"se {s['trigger']} → {s['azione']} "
                f"(SL {s['stop_logico']}, TP {s['target_logico']}, prezzo_ref {s['prezzo_ref']})"
            )
        lines.append("")
        lines.append(
            "⚡ VERIFICA: uno di questi scenari è ORA triggerato? Se sì, eseguilo "
            "(direzione/voto coerenti con l'azione). Se no, gli scenari restano in attesa "
            "o vengono invalidati se le condizioni di mercato sono cambiate radicalmente."
        )
        return "\n".join(lines)


# Singleton globali
macro_thesis_cache = MacroThesisCache()
pending_scenarios = PendingScenarios()
