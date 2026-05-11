import sqlite3
import json
import os
import threading
import logging
import shutil
from core.chimera_errors import ErrorTracker

_err = ErrorTracker("DatabaseManager")

class DatabaseManager:
    _instance = None
    _lock = threading.Lock()

    def __new__(cls, db_path=None):
        with cls._lock:
            if cls._instance is None:
                if db_path is None:
                    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
                    db_path = os.path.join(base_dir, "chimera.db")
                cls._instance = super(DatabaseManager, cls).__new__(cls)
                cls._instance._init_db(db_path)
            return cls._instance

    def _init_db(self, db_path):
        self.db_path = db_path
        self.logger = logging.getLogger("DatabaseManager")
        # PERF P3: connessione persistente + lock esplicito invece di aprire/chiudere ad ogni operazione
        self._conn_lock = threading.Lock()
        self._conn = sqlite3.connect(self.db_path, check_same_thread=False, timeout=10)
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA synchronous=NORMAL")
        self._conn.row_factory = sqlite3.Row
        self._create_tables()
        self._migrate_from_json()

    def _get_conn(self):
        """Restituisce la connessione persistente. Usa sempre con self._conn_lock."""
        return self._conn

    def _create_tables(self):
        with self._conn_lock:
            cursor = self._conn.cursor()
            cursor.execute('''CREATE TABLE IF NOT EXISTS posizioni_aperte (
                                asset TEXT PRIMARY KEY,
                                data TEXT
                              )''')
            cursor.execute('''CREATE TABLE IF NOT EXISTS storico_trades (
                                id INTEGER PRIMARY KEY AUTOINCREMENT,
                                asset TEXT,
                                data TEXT
                              )''')
            cursor.execute('''CREATE TABLE IF NOT EXISTS feedback_db (
                                asset TEXT PRIMARY KEY,
                                data TEXT
                              )''')
            cursor.execute('''CREATE TABLE IF NOT EXISTS ghost_trades (
                                id TEXT PRIMARY KEY,
                                data TEXT
                              )''')
            cursor.execute('''CREATE TABLE IF NOT EXISTS stats_globali (
                                id TEXT PRIMARY KEY,
                                data TEXT
                              )''')
            # Tabella per sequenze LSTM — buffer rolling per asset
            # Ogni riga è un asset con i suoi ultimi N snapshot serializzati
            cursor.execute('''CREATE TABLE IF NOT EXISTS sequence_buffer (
                                ticker TEXT PRIMARY KEY,
                                data   TEXT,
                                ts     REAL
                              )''')
            self._conn.commit()

    def _migrate_from_json(self):
        """Migra i dati dai vecchi file JSON al database SQLite una tantum."""
        # Migrazione posizioni
        if os.path.exists("posizioni_aperte.json"):
            try:
                with open("posizioni_aperte.json", "r") as f:
                    dati = json.load(f)
                self.save_posizioni(dati)
                shutil.move("posizioni_aperte.json", "posizioni_aperte.json.bak")
                self.logger.info("✅ Migrazione posizioni_aperte.json -> SQLite completata.")
            except Exception as e:
                _err.capture(e, "_migrate_from_json", {"module": "DatabaseManager"})
                self.logger.error(f"Errore migrazione posizioni: {e}")

        # Migrazione storico
        if os.path.exists("storico_trades.json"):
            try:
                with open("storico_trades.json", "r") as f:
                    dati = json.load(f)
                self.save_storico(dati)
                shutil.move("storico_trades.json", "storico_trades.json.bak")
                self.logger.info("✅ Migrazione storico_trades.json -> SQLite completata.")
            except Exception as e:
                _err.capture(e, "_migrate_from_json", {"module": "DatabaseManager"})
                self.logger.error(f"Errore migrazione storico: {e}")

        # Migrazione feedback
        if os.path.exists("feedback_db.json"):
            try:
                with open("feedback_db.json", "r") as f:
                    dati = json.load(f)
                self.save_feedback(dati)
                shutil.move("feedback_db.json", "feedback_db.json.bak")
                self.logger.info("✅ Migrazione feedback_db.json -> SQLite completata.")
            except Exception as e:
                _err.capture(e, "_migrate_from_json", {"module": "DatabaseManager"})
                self.logger.error(f"Errore migrazione feedback: {e}")

        # Migrazione ghost
        if os.path.exists("ghost_trades.json"):
            try:
                with open("ghost_trades.json", "r") as f:
                    dati = json.load(f)
                self.save_ghosts(dati)
                shutil.move("ghost_trades.json", "ghost_trades.json.bak")
                self.logger.info("✅ Migrazione ghost_trades.json -> SQLite completata.")
            except Exception as e:
                _err.capture(e, "_migrate_from_json", {"module": "DatabaseManager"})
                self.logger.error(f"Errore migrazione ghost: {e}")

        # Migrazione stats_globali
        if os.path.exists("stats_globali.json"):
            try:
                with open("stats_globali.json", "r") as f:
                    dati = json.load(f)
                self.save_stats_globali(dati)
                shutil.move("stats_globali.json", "stats_globali.json.bak")
                self.logger.info("✅ Migrazione stats_globali.json -> SQLite completata.")
            except Exception as e:
                _err.capture(e, "_migrate_from_json", {"module": "DatabaseManager"})
                self.logger.error(f"Errore migrazione stats_globali: {e}")

    # --- METODI POSIZIONI ---
    def get_posizioni(self):
        with self._conn_lock:
            cursor = self._conn.cursor()
            cursor.execute("SELECT asset, data FROM posizioni_aperte")
            return {row[0]: json.loads(row[1]) for row in cursor.fetchall()}

    def save_posizioni(self, posizioni_dict):
        """
        Salva l'intero stato delle posizioni.
        USA upsert_posizione / delete_posizione per operazioni su singola posizione —
        questo metodo è solo per la sincronizzazione iniziale/bulk.
        """
        with self._conn_lock:
            cursor = self._conn.cursor()
            cursor.execute("DELETE FROM posizioni_aperte")
            for asset, data in posizioni_dict.items():
                cursor.execute(
                    "INSERT INTO posizioni_aperte (asset, data) VALUES (?, ?)",
                    (asset, json.dumps(data))
                )
            self._conn.commit()

    def upsert_posizione(self, asset: str, data: dict):
        """
        Inserisce o aggiorna UNA singola posizione senza toccare le altre.
        Da usare in apri_posizione e ogni update di posizione singola.
        """
        with self._conn_lock:
            cursor = self._conn.cursor()
            cursor.execute(
                "INSERT OR REPLACE INTO posizioni_aperte (asset, data) VALUES (?, ?)",
                (asset, json.dumps(data))
            )
            self._conn.commit()

    def delete_posizione(self, asset: str):
        """
        Rimuove UNA singola posizione senza toccare le altre.
        Da usare in chiudi_posizione.
        """
        with self._conn_lock:
            cursor = self._conn.cursor()
            cursor.execute(
                "DELETE FROM posizioni_aperte WHERE asset = ?",
                (asset,)
            )
            self._conn.commit()

    # --- METODI STORICO ---
    def get_storico(self):
        with self._conn_lock:
            cursor = self._conn.cursor()
            cursor.execute("SELECT data FROM storico_trades ORDER BY id ASC")
            return [json.loads(row[0]) for row in cursor.fetchall()]

    def save_storico(self, storico_list):
        with self._conn_lock:
            cursor = self._conn.cursor()
            try:
                cursor.execute("BEGIN")
                cursor.execute("DELETE FROM storico_trades")
                for trade in storico_list:
                    asset = trade.get("asset", "UNKNOWN")
                    cursor.execute("INSERT INTO storico_trades (asset, data) VALUES (?, ?)", (asset, json.dumps(trade)))
                self._conn.commit()
            except Exception as e:
                _err.capture(e, "save_storico", {"module": "DatabaseManager"})
                self._conn.rollback()
                self.logger.error(f"❌ save_storico rollback: {e}")
                raise

    # --- METODI FEEDBACK ---
    def get_feedback(self):
        with self._conn_lock:
            cursor = self._conn.cursor()
            cursor.execute("SELECT asset, data FROM feedback_db")
            return {row[0]: json.loads(row[1]) for row in cursor.fetchall()}

    def save_feedback(self, feedback_dict):
        with self._conn_lock:
            cursor = self._conn.cursor()
            cursor.execute("DELETE FROM feedback_db")
            for asset, data in feedback_dict.items():
                cursor.execute("INSERT INTO feedback_db (asset, data) VALUES (?, ?)", (asset, json.dumps(data)))
            self._conn.commit()

    # --- METODI GHOST ---
    def get_ghosts(self):
        with self._conn_lock:
            cursor = self._conn.cursor()
            cursor.execute("SELECT id, data FROM ghost_trades")
            return {row[0]: json.loads(row[1]) for row in cursor.fetchall()}

    def save_ghosts(self, ghosts_dict):
        with self._conn_lock:
            cursor = self._conn.cursor()
            cursor.execute("DELETE FROM ghost_trades")
            for gid, data in ghosts_dict.items():
                cursor.execute("INSERT INTO ghost_trades (id, data) VALUES (?, ?)", (gid, json.dumps(data)))
            self._conn.commit()

    # --- METODI STATS GLOBALI ---
    def get_stats_globali(self):
        with self._conn_lock:
            cursor = self._conn.cursor()
            cursor.execute("SELECT data FROM stats_globali WHERE id = 'main'")
            row = cursor.fetchone()
            return json.loads(row[0]) if row else {"max_drawdown": 0.0, "pnl_realizzato_totale": 0.0, "equity_peak": 0.0}

    def save_stats_globali(self, stats_dict):
        with self._conn_lock:
            cursor = self._conn.cursor()
            cursor.execute("REPLACE INTO stats_globali (id, data) VALUES ('main', ?)", (json.dumps(stats_dict),))
            self._conn.commit()

    # --- METODI SEQUENCE BUFFER (per LSTM futuro) ---

    def save_sequence_buffer(self, ticker: str, snapshots: list):
        """Salva il buffer di sequenze per un asset."""
        import time as _time
        with self._conn_lock:
            cursor = self._conn.cursor()
            cursor.execute(
                "REPLACE INTO sequence_buffer (ticker, data, ts) VALUES (?, ?, ?)",
                (ticker, json.dumps(snapshots), _time.time())
            )
            self._conn.commit()

    def get_sequence_buffer(self, ticker: str) -> list:
        """Carica il buffer di sequenze per un asset."""
        with self._conn_lock:
            cursor = self._conn.cursor()
            cursor.execute("SELECT data FROM sequence_buffer WHERE ticker = ?", (ticker,))
            row = cursor.fetchone()
            if row:
                try:
                    return json.loads(row[0])
                except Exception:
                    return []
            return []

    def get_all_sequence_tickers(self) -> list:
        """Ritorna la lista di tutti gli asset con sequenze salvate."""
        with self._conn_lock:
            cursor = self._conn.cursor()
            cursor.execute("SELECT ticker FROM sequence_buffer")
            return [r[0] for r in cursor.fetchall()]

# Istanza globale Singleton
db_manager = DatabaseManager()