# -*- coding: utf-8 -*-
"""
GeminiMemory — memoria persistente delle ultime decisioni di Gemini per asset.

Scopo: dare a Gemini la capacità di vedere cosa LUI STESSO ha deciso negli
ultimi cicli su questo asset, con quale tesi, e con quale esito (quando il
trade si chiude e l'esito viene retro-aggiornato).

Senza questo, Gemini decide in un vuoto cognitivo: ogni ciclo è la prima volta
e ripete gli stessi errori. Con questo, può scrivere "20 minuti fa ho detto
LONG voto 7 con tesi X. Risultato: LOSS. Ora vedo lo stesso pattern. Devo
giustificare cosa è cambiato o restare FLAT".

Tabella: gemini_decisions
  asset           TEXT     — ticker
  ts              REAL     — timestamp epoch
  direzione       TEXT     — LONG/SHORT/FLAT
  voto            INTEGER  — 0..10
  tesi            TEXT     — narrativa breve della decisione (≤200 char)
  stop_logico     REAL     — prezzo che invaliderebbe la tesi
  target_logico   REAL     — prezzo che confermerebbe la tesi
  prezzo_entry    REAL     — prezzo al momento della decisione
  esito           TEXT     — WIN/LOSS/FLAT_CORRETTO/FLAT_PERSO/PENDING (NULL inizialmente)
  pnl_usd         REAL     — popolato post-chiusura del trade
  trade_id        TEXT     — link al trade reale se aperto

Le decisioni FLAT vengono salvate ma con esito FLAT_CORRETTO o FLAT_PERSO
solo se il NightReview lo retroaggiorna (opzionale, non bloccante).
"""

import sqlite3
import threading
import time
import os
import logging
import json
from core.chimera_errors import ErrorTracker

_err = ErrorTracker("GeminiMemory")


class GeminiMemory:
    _instance = None
    _lock = threading.Lock()

    def __new__(cls, db_path=None):
        with cls._lock:
            if cls._instance is None:
                if db_path is None:
                    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
                    db_path = os.path.join(base_dir, "chimera.db")
                cls._instance = super(GeminiMemory, cls).__new__(cls)
                cls._instance._init_db(db_path)
            return cls._instance

    def _init_db(self, db_path):
        self.db_path = db_path
        self.logger = logging.getLogger("GeminiMemory")
        self._conn_lock = threading.Lock()
        self._conn = sqlite3.connect(self.db_path, check_same_thread=False, timeout=10)
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA synchronous=NORMAL")
        self._conn.row_factory = sqlite3.Row
        self._create_table()

    def _create_table(self):
        with self._conn_lock:
            cur = self._conn.cursor()
            cur.execute('''CREATE TABLE IF NOT EXISTS gemini_decisions (
                              id           INTEGER PRIMARY KEY AUTOINCREMENT,
                              asset        TEXT NOT NULL,
                              ts           REAL NOT NULL,
                              direzione    TEXT NOT NULL,
                              voto         INTEGER NOT NULL,
                              tesi         TEXT,
                              stop_logico  REAL,
                              target_logico REAL,
                              prezzo_entry REAL,
                              esito        TEXT,
                              pnl_usd      REAL,
                              trade_id     TEXT
                          )''')
            cur.execute('''CREATE INDEX IF NOT EXISTS idx_gemini_dec_asset_ts
                           ON gemini_decisions(asset, ts DESC)''')
            self._conn.commit()

    # ─── Salvataggio decisione ─────────────────────────────────────────────
    def save_decision(self, asset, direzione, voto, tesi, stop_logico=None,
                      target_logico=None, prezzo_entry=None, trade_id=None,
                      condizioni_tesi=None):
        """
        Salva la decisione di Gemini al momento del ciclo.
        Il campo esito resta NULL e verrà popolato dopo (se serve) tramite
        update_outcome quando il trade si chiude.

        v12: condizioni_tesi è un dict con 'invalidata_se_diventa_vero' usato
        dal Trade Watchdog per chiudere il trade se la tesi diventa invalida.
        """
        try:
            tesi_short = (tesi or '')[:300]
            # Serializza condizioni_tesi a JSON se presente (dict → string)
            cond_str = None
            if condizioni_tesi:
                try:
                    cond_str = json.dumps(condizioni_tesi)[:2000]
                except Exception:
                    cond_str = None
            with self._conn_lock:
                cur = self._conn.cursor()
                # Verifica colonna condizioni_tesi (auto-migrazione idempotente)
                try:
                    cur.execute("PRAGMA table_info(gemini_decisions)")
                    cols = [c[1] for c in cur.fetchall()]
                    if 'condizioni_tesi' not in cols:
                        cur.execute("ALTER TABLE gemini_decisions ADD COLUMN condizioni_tesi TEXT")
                        self._conn.commit()
                except Exception as _e_alt:
                    _err.capture(_e_alt, "save_decision", {"module": "GeminiMemory"})
                    self.logger.debug(f"alter table check: {_e_alt}")

                cur.execute('''INSERT INTO gemini_decisions
                              (asset, ts, direzione, voto, tesi, stop_logico,
                               target_logico, prezzo_entry, esito, trade_id,
                               condizioni_tesi)
                              VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''',
                           (asset, time.time(), direzione, int(voto), tesi_short,
                            stop_logico, target_logico, prezzo_entry,
                            'PENDING' if direzione in ('LONG','SHORT','BUY','SELL') else 'FLAT',
                            trade_id, cond_str))
                self._conn.commit()
                return cur.lastrowid
        except Exception as e:
            _err.capture(e, "save_decision", {"module": "GeminiMemory"})
            self.logger.warning(f"GeminiMemory.save_decision error: {e}")
            return None

    # ─── Aggiornamento esito ────────────────────────────────────────────────
    def update_outcome(self, decision_id, esito, pnl_usd=None):
        """Chiamato dopo chiusura trade — popola esito e pnl_usd."""
        try:
            with self._conn_lock:
                cur = self._conn.cursor()
                cur.execute('''UPDATE gemini_decisions
                               SET esito = ?, pnl_usd = ?
                               WHERE id = ?''',
                           (esito, pnl_usd, decision_id))
                self._conn.commit()
        except Exception as e:
            _err.capture(e, "update_outcome", {"module": "GeminiMemory"})
            self.logger.warning(f"GeminiMemory.update_outcome error: {e}")

    def update_outcome_by_trade_id(self, trade_id, esito, pnl_usd=None):
        """Aggiorna esito dato un trade_id Kraken."""
        try:
            with self._conn_lock:
                cur = self._conn.cursor()
                cur.execute('''UPDATE gemini_decisions
                               SET esito = ?, pnl_usd = ?
                               WHERE trade_id = ? AND esito = 'PENDING' ''',
                           (esito, pnl_usd, trade_id))
                self._conn.commit()
                return cur.rowcount
        except Exception as e:
            _err.capture(e, "update_outcome_by_trade_id", {"module": "GeminiMemory"})
            self.logger.warning(f"GeminiMemory.update_outcome_by_trade_id error: {e}")
            return 0

    # ─── Lettura ultime decisioni ───────────────────────────────────────────
    def get_recent(self, asset, n=3, only_with_outcome=False):
        """
        Ritorna le ultime n decisioni per asset, in ordine cronologico inverso
        (più recente per prima).
        Se only_with_outcome=True, esclude le PENDING.
        """
        try:
            with self._conn_lock:
                cur = self._conn.cursor()
                if only_with_outcome:
                    cur.execute('''SELECT * FROM gemini_decisions
                                   WHERE asset = ? AND esito IS NOT NULL AND esito != 'PENDING'
                                   ORDER BY ts DESC LIMIT ?''', (asset, n))
                else:
                    cur.execute('''SELECT * FROM gemini_decisions
                                   WHERE asset = ?
                                   ORDER BY ts DESC LIMIT ?''', (asset, n))
                rows = cur.fetchall()
                return [dict(r) for r in rows]
        except Exception as e:
            _err.capture(e, "get_recent", {"module": "GeminiMemory"})
            self.logger.warning(f"GeminiMemory.get_recent error: {e}")
            return []

    # ─── Formattazione blocco per il prompt ────────────────────────────────
    def format_for_prompt(self, asset, n=3):
        """
        Formatta le ultime n decisioni in un blocco testuale leggibile per Gemini.
        Esempio output:

        LE TUE ULTIME 3 DECISIONI SU SOLUSD:
          [12 min fa]  LONG voto 7  | Tesi: "breakout VWAP con CVD positivo"
                       Risultato: LOSS −0.8 USD (la tua tesi è stata invalidata).
          [28 min fa]  FLAT         | Motivo: "regime laterale, segnali misti"
                       Risultato: FLAT corretto (il prezzo è rimasto laterale).
          [45 min fa]  SHORT voto 6 | Tesi: "rifiuto resistenza, OFI negativo"
                       Risultato: WIN +0.4 USD (la tua tesi è stata confermata).

        ⚠️ Se la tua tesi attuale assomiglia a una di quelle perse, devi
           giustificare cosa è materialmente diverso. Se non lo trovi → FLAT.
        """
        decisions = self.get_recent(asset, n=n)
        if not decisions:
            return f"LE TUE ULTIME DECISIONI SU {asset}: nessuna decisione precedente registrata su questo asset."

        now = time.time()
        lines = [f"LE TUE ULTIME {len(decisions)} DECISIONI SU {asset}:"]
        for d in decisions:
            mins = int((now - d['ts']) / 60)
            tempo = f"{mins} min fa" if mins < 90 else f"{mins//60}h {mins%60}m fa"
            direzione = d.get('direzione', 'FLAT')
            voto = d.get('voto', 0)
            tesi = d.get('tesi', '') or '(nessuna tesi salvata)'
            esito = d.get('esito', 'PENDING')
            pnl = d.get('pnl_usd')

            # Riga decisione
            if direzione in ('LONG', 'SHORT', 'BUY', 'SELL'):
                lines.append(f"  [{tempo}]  {direzione} voto {voto} | Tesi: \"{tesi}\"")
            else:
                lines.append(f"  [{tempo}]  FLAT | Motivo: \"{tesi}\"")

            # Riga esito
            if esito == 'WIN':
                pnl_s = f" {pnl:+.2f} USD" if pnl is not None else ""
                lines.append(f"               → Risultato: WIN{pnl_s} (tesi confermata).")
            elif esito == 'LOSS':
                pnl_s = f" {pnl:+.2f} USD" if pnl is not None else ""
                lines.append(f"               → Risultato: LOSS{pnl_s} (tesi invalidata).")
            elif esito == 'FLAT_CORRETTO':
                lines.append(f"               → Risultato: hai fatto bene a stare fuori.")
            elif esito == 'FLAT_PERSO':
                lines.append(f"               → Risultato: avresti potuto entrare (hai mancato un'opportunità).")
            elif esito == 'PENDING':
                lines.append(f"               → Risultato: trade ancora aperto, esito non disponibile.")
            elif esito == 'FLAT':
                lines.append(f"               → (decisione FLAT, nessun esito da misurare)")
            else:
                lines.append(f"               → Risultato: {esito}")

        # Promemoria critico
        loss_recenti = [d for d in decisions if d.get('esito') == 'LOSS']
        if loss_recenti:
            ultimo_loss = loss_recenti[0]
            lines.append("")
            lines.append(
                f"⚠️ La tua decisione più recente perdente è stata "
                f"{ultimo_loss.get('direzione')} con tesi: \"{ultimo_loss.get('tesi','')[:120]}\". "
                f"Se la tua tesi attuale è simile, devi spiegare cosa è MATERIALMENTE cambiato. "
                f"Se non lo trovi, la risposta corretta è FLAT."
            )

        return "\n".join(lines)

    # ─── Statistica WR delle ultime decisioni (per autodiagnosi bias) ─────
    def get_recent_wr(self, asset, n=10):
        """
        Calcola il WR delle ultime n decisioni CONCLUSE (con esito noto)
        di Gemini su questo asset. Serve per la modifica E (autodiagnosi):
        Gemini sa se è in fase di errore, normale o euforia.

        Ritorna dict con:
          - n_decisioni_concluse: int (su quante si basa il WR)
          - n_win: int
          - n_loss: int
          - wr_pct: float (0-100), o None se n_decisioni_concluse < 3
          - fase: str — 'TROUBLE' (WR<40%), 'NORMAL' (40-60%), 'HOT' (>60%), 'INSUFFICIENT'
          - ultime_loss_consecutive: int (streak di LOSS più recente)
        """
        try:
            with self._conn_lock:
                cur = self._conn.cursor()
                # Solo decisioni con esito WIN o LOSS (non FLAT, non PENDING)
                cur.execute('''SELECT esito, pnl_usd FROM gemini_decisions
                               WHERE asset = ? AND esito IN ('WIN', 'LOSS')
                               ORDER BY ts DESC LIMIT ?''', (asset, n))
                rows = cur.fetchall()
        except Exception as e:
            _err.capture(e, "get_recent_wr", {"module": "GeminiMemory"})
            self.logger.warning(f"GeminiMemory.get_recent_wr error: {e}")
            return {
                'n_decisioni_concluse': 0, 'n_win': 0, 'n_loss': 0,
                'wr_pct': None, 'fase': 'INSUFFICIENT',
                'ultime_loss_consecutive': 0
            }

        n_total = len(rows)
        n_win = sum(1 for r in rows if r['esito'] == 'WIN')
        n_loss = sum(1 for r in rows if r['esito'] == 'LOSS')

        # Streak di LOSS più recente (le righe sono ordinate DESC = più recente per prima)
        streak = 0
        for r in rows:
            if r['esito'] == 'LOSS':
                streak += 1
            else:
                break

        if n_total < 3:
            return {
                'n_decisioni_concluse': n_total, 'n_win': n_win, 'n_loss': n_loss,
                'wr_pct': None, 'fase': 'INSUFFICIENT',
                'ultime_loss_consecutive': streak
            }

        wr = (n_win / n_total) * 100.0
        if wr < 40:
            fase = 'TROUBLE'
        elif wr > 60:
            fase = 'HOT'
        else:
            fase = 'NORMAL'

        return {
            'n_decisioni_concluse': n_total, 'n_win': n_win, 'n_loss': n_loss,
            'wr_pct': wr, 'fase': fase,
            'ultime_loss_consecutive': streak
        }

    # ─── Statistica WR-in-rumore vs WR-in-leggibilità ──────────────────────
    def get_noise_vs_clean_wr(self, asset, days=30):
        """
        Calcola separatamente il WR di Gemini su questo asset
        quando ha operato in condizioni di RUMORE vs LEGGIBILE.

        Una decisione è marcata "rumore" se nel momento della decisione
        almeno 2 di queste condizioni erano vere:
          - kaufman_efficiency < 0.25
          - relative_volume_status < 0.8
          - segno opposto tra cvd_delta_30s e cvd_delta_120s

        Per popolare questa info servirebbe salvare anche le condizioni
        di rumore nello save_decision. Per ora la lasciamo come placeholder
        e la riempiamo dal storico_trades + chimera_snapshot direttamente.

        Ritorna dict {wr_rumore: float, wr_leggibile: float, n_rumore: int, n_leggibile: int}
        oppure None se dati insufficienti.
        """
        # Implementazione completa fatta esternamente analizzando storico_trades
        # con i loro chimera_snapshot. Qui ritorniamo None (placeholder).
        return None


# Singleton globale
gemini_memory = GeminiMemory()
