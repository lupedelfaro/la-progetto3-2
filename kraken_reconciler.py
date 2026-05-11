# -*- coding: utf-8 -*-
"""
CHIMERA v4.1 — KrakenReconciler
Sincronizzazione completa account Kraken ↔ DB locale.

Scarica da Kraken:
  - Ledger completo (trade, margin, rollover, fee, deposit)
  - Trade history (ogni posizione chiusa con PnL reale, cprice, cfee)

Salva su DB (nuove tabelle):
  - kraken_ledger  : ogni voce raw del ledger Kraken
  - kraken_trades  : ogni trade chiuso con PnL reale

Incrocia con storico_trades:
  - Aggiorna pnl_netto_usd, result_perc, p_uscita reali
  - Segnala orfani (Kraken senza DB) e discrepanze PnL

Integrazione bot_la.py:
  from core.kraken_reconciler import KrakenReconciler
  reconciler = KrakenReconciler(performer=performer, db_manager=db_manager,
                                alerts=alerts, trade_manager=trade_manager)
  reconciler.esegui_se_necessario()   # nel loop principale
  # Telegram: /reconcile
"""

import logging
import time
import json
import threading
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple

try:
    from core.chimera_errors import ErrorTracker
    _err = ErrorTracker("KrakenReconciler")
except Exception:
    class _ErrStub:
        def capture(self, *a, **kw): pass
    _err = _ErrStub()


class KrakenReconciler:

    INTERVALLO_AUTO    = 600   # secondi tra run automatici
    FINESTRA_GIORNI    = 90    # giorni di storico da sincronizzare
    SOGLIA_DISCREPANZA = 0.50  # USD — sotto non segnala
    BUCKET_SECONDI     = 600   # finestra match temporale (=10min, prima 120s).
                               # Tollera offset timezone, latenze API, sleep bot.

    def __init__(self, performer, db_manager, alerts=None, trade_manager=None):
        self.performer     = performer
        self.db            = db_manager
        self.alerts        = alerts
        self.trade_manager = trade_manager
        self.logger        = logging.getLogger("KrakenReconciler")
        self._ultimo_run   = 0.0
        self._lock         = threading.Lock()
        self._init_tabelle()
        self.logger.info("✅ KrakenReconciler inizializzato")

    # ── INIT TABELLE ─────────────────────────────────────────────────────────

    def _init_tabelle(self):
        try:
            with self.db._conn_lock:
                c = self.db._conn.cursor()
                c.execute("""CREATE TABLE IF NOT EXISTS kraken_ledger (
                    id TEXT PRIMARY KEY, refid TEXT, time REAL,
                    type TEXT, asset TEXT, amount REAL, fee REAL,
                    balance REAL, data TEXT)""")
                c.execute("""CREATE TABLE IF NOT EXISTS kraken_trades (
                    trade_id TEXT PRIMARY KEY, order_id TEXT, pair TEXT,
                    time REAL, type TEXT, price REAL, vol REAL, cost REAL,
                    fee REAL, net REAL, cprice REAL, posstatus TEXT,
                    posid TEXT, reconciled INTEGER DEFAULT 0, data TEXT)""")
                c.execute("CREATE INDEX IF NOT EXISTS idx_kl_time ON kraken_ledger(time)")
                c.execute("CREATE INDEX IF NOT EXISTS idx_kl_refid ON kraken_ledger(refid)")
                c.execute("CREATE INDEX IF NOT EXISTS idx_kt_time ON kraken_trades(time)")
                c.execute("CREATE INDEX IF NOT EXISTS idx_kt_oid  ON kraken_trades(order_id)")
                c.execute("CREATE INDEX IF NOT EXISTS idx_kt_pair ON kraken_trades(pair)")
                self.db._conn.commit()
            self.logger.info("✅ Tabelle kraken_ledger e kraken_trades pronte")
        except Exception as e:
            _err.capture(e, "_init_tabelle", {"module": "KrakenReconciler"})
            self.logger.error(f"❌ Init tabelle: {e}")

    # ── PUBLIC ────────────────────────────────────────────────────────────────

    def esegui_se_necessario(self):
        if time.time() - self._ultimo_run >= self.INTERVALLO_AUTO:
            self.esegui()
            # Ricostruzione storico_trades dalle vere voci Kraken — sempre dopo sync
            try:
                self.ricostruisci_storico_da_kraken(giorni=self.FINESTRA_GIORNI)
            except Exception as e_r:
                _err.capture(e_r, "esegui_se_necessario", {"module": "KrakenReconciler"})
                self.logger.error(f"⚠️ ricostruisci_storico_da_kraken fallito: {e_r}")

    def esegui(self, force: bool = False) -> Dict:
        with self._lock:
            self._ultimo_run = time.time()
            stats = {"ledger_voci": 0, "trades_kraken": 0,
                     "trades_aggiornati": 0, "discrepanze": 0,
                     "orfani_kraken": 0, "fantasmi_db": 0,
                     "timestamp": datetime.now(timezone.utc).isoformat()}
            self.logger.info("🔄 [RECONCILER] Avvio sync Kraken...")
            try:
                ledger = self._sync_ledger()
                stats["ledger_voci"] = len(ledger)
                trades = self._sync_trades_history()
                stats["trades_kraken"] = len(trades)
                a, d, o, f = self._incrocia_con_db(trades, ledger)
                stats.update({"trades_aggiornati": a, "discrepanze": d,
                               "orfani_kraken": o, "fantasmi_db": f})
                self.logger.info(
                    f"✅ [RECONCILER] ledger={stats['ledger_voci']} "
                    f"trades={stats['trades_kraken']} "
                    f"aggiornati={a} disc={d} orfani={o}")
                # FIX spam Telegram (2026-05-07): l'alert parte solo per anomalie
                # reali (discrepanze o orfani). Gli "aggiornati" non sono anomalie:
                # erano la causa del messaggio "RECONCILER — ANOMALIE" ricorrente
                # ogni 10 min con sempre gli stessi 21 trade già riconciliati.
                if d > 0 or o > 0:
                    self._invia_report(stats)
            except Exception as e:
                _err.capture(e, "esegui", {"module": "KrakenReconciler"})
                self.logger.error(f"❌ [RECONCILER] {e}")
            return stats

    def riconcilia_full(self, giorni: int = 90) -> Dict:
        """
        Riconciliazione retroattiva forzata su finestra ampia.
        
        Aggiunto 2026-05-04: utile per ricostruire le statistiche storiche
        quando il reconciler abituale ha solo gli ultimi giorni.
        Espande FINESTRA_GIORNI temporaneamente, esegue sync completo,
        ripristina valore originale.
        
        Da chiamare manualmente o dal bot al primo avvio per riconciliare
        tutto lo storico passato.
        
        Args:
            giorni: quanti giorni indietro andare (default 90)
        
        Returns:
            stats dict come esegui()
        """
        finestra_originale = self.FINESTRA_GIORNI
        ultimo_run_orig = self._ultimo_run
        try:
            # Espandi finestra
            self.FINESTRA_GIORNI = giorni
            # Force run anche se è appena stato eseguito
            self._ultimo_run = 0
            self.logger.info(
                f"🔄 [RECONCILER FULL] Riconciliazione retroattiva su {giorni} giorni..."
            )
            stats = self.esegui(force=True)
            # Dopo riconciliazione classica, ricostruisce lo storico dai trade Kraken veri
            try:
                rebuild_stats = self.ricostruisci_storico_da_kraken(giorni=giorni)
                stats['rebuild_nuovi'] = rebuild_stats.get('nuovi', 0)
                stats['rebuild_aggiornati'] = rebuild_stats.get('aggiornati', 0)
            except Exception as e_r:
                _err.capture(e_r, "riconcilia_full", {"module": "KrakenReconciler"})
                self.logger.error(f"⚠️ ricostruisci_storico_da_kraken fallito: {e_r}")
            self.logger.info(
                f"✅ [RECONCILER FULL] Completato. "
                f"Ledger {stats['ledger_voci']}, trades {stats['trades_kraken']}, "
                f"aggiornati {stats['trades_aggiornati']}, "
                f"orfani {stats['orfani_kraken']}, fantasmi {stats['fantasmi_db']}, "
                f"rebuild: +{stats.get('rebuild_nuovi',0)} nuovi, ↻{stats.get('rebuild_aggiornati',0)} aggiornati"
            )
            return stats
        finally:
            self.FINESTRA_GIORNI = finestra_originale
            self._ultimo_run = time.time()  # impedisci re-run immediato

    # ── SYNC LEDGER ───────────────────────────────────────────────────────────

    def _sync_ledger(self) -> List[Dict]:
        since = int(time.time() - 86400 * self.FINESTRA_GIORNI)
        all_entries = {}
        offset = 0
        while True:
            try:
                res = self.performer.exchange.private_post_ledgers(
                    {'start': since, 'ofs': offset})
                batch = res.get('result', {}).get('ledger', {})
                if not batch:
                    break
                all_entries.update(batch)
                if len(batch) < 50:
                    break
                offset += 50
                time.sleep(0.3)
            except Exception as e:
                _err.capture(e, "_sync_ledger", {"module": "KrakenReconciler"})
                self.logger.warning(f"⚠️ Ledger offset={offset}: {e}")
                break

        if not all_entries:
            return []

        entries = []
        with self.db._conn_lock:
            c = self.db._conn.cursor()
            for eid, e in all_entries.items():
                try:
                    row = {
                        'id':      eid,
                        'refid':   e.get('refid', ''),
                        'time':    float(e.get('time', 0) or 0),
                        'type':    e.get('type', ''),
                        'asset':   e.get('asset', ''),
                        'amount':  float(e.get('amount', 0) or 0),
                        'fee':     float(e.get('fee', 0) or 0),
                        'balance': float(e.get('balance', 0) or 0),
                    }
                    c.execute(
                        "INSERT OR REPLACE INTO kraken_ledger "
                        "(id,refid,time,type,asset,amount,fee,balance,data) "
                        "VALUES (?,?,?,?,?,?,?,?,?)",
                        (row['id'], row['refid'], row['time'], row['type'],
                         row['asset'], row['amount'], row['fee'],
                         row['balance'], json.dumps(e)))
                    entries.append(row)
                except Exception:
                    continue
            self.db._conn.commit()

        self.logger.info(f"📒 Ledger: {len(entries)} voci (ultimi {self.FINESTRA_GIORNI}gg)")
        return entries

    # ── SYNC TRADES HISTORY ───────────────────────────────────────────────────

    def _sync_trades_history(self) -> List[Dict]:
        since = int(time.time() - 86400 * self.FINESTRA_GIORNI)
        all_trades = {}
        offset = 0
        while True:
            try:
                res = self.performer.exchange.private_post_tradeshistory(
                    {'start': since, 'trades': True, 'ofs': offset})
                batch = res.get('result', {}).get('trades', {})
                if not batch:
                    break
                all_trades.update(batch)
                count = res.get('result', {}).get('count', 0)
                if len(all_trades) >= count or len(batch) < 50:
                    break
                offset += 50
                time.sleep(0.3)
            except Exception as e:
                _err.capture(e, "_sync_trades_history", {"module": "KrakenReconciler"})
                self.logger.warning(f"⚠️ TradesHistory offset={offset}: {e}")
                break

        if not all_trades:
            return []

        chiusure = []
        with self.db._conn_lock:
            c = self.db._conn.cursor()
            for tid, t in all_trades.items():
                try:
                    def _f(v):
                        try: return float(v) if v not in (None,'','None') else 0.0
                        except: return 0.0
                    row = {
                        'trade_id':  tid,
                        'order_id':  t.get('ordertxid', ''),
                        'pair':      t.get('pair', ''),
                        'time':      _f(t.get('time', 0)),
                        'type':      t.get('type', ''),
                        'price':     _f(t.get('price', 0)),
                        'vol':       _f(t.get('vol', 0)),
                        'cost':      _f(t.get('cost', 0)),
                        'fee':       _f(t.get('fee', 0)),
                        'net':       _f(t.get('net', 0)),
                        'cprice':    _f(t.get('cprice', 0)),
                        'posstatus': str(t.get('posstatus', '') or ''),
                        'posid':     str(t.get('posid', t.get('postxid','')) or ''),
                    }
                    c.execute(
                        "INSERT OR REPLACE INTO kraken_trades "
                        "(trade_id,order_id,pair,time,type,price,vol,cost,"
                        "fee,net,cprice,posstatus,posid,data) "
                        "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                        (row['trade_id'], row['order_id'], row['pair'],
                         row['time'], row['type'], row['price'], row['vol'],
                         row['cost'], row['fee'], row['net'], row['cprice'],
                         row['posstatus'], row['posid'], json.dumps(t)))
                    if row['posstatus'] == 'closed':
                        chiusure.append(row)
                except Exception:
                    continue
            self.db._conn.commit()

        self.logger.info(
            f"📊 TradesHistory: {len(all_trades)} totali, "
            f"{len(chiusure)} chiusure")
        return chiusure

    # ── INCROCIO DB ───────────────────────────────────────────────────────────

    def _incrocia_con_db(
        self, kraken_trades: List[Dict], ledger: List[Dict]
    ) -> Tuple[int, int, int, int]:

        storico = self.db.get_storico()
        aggiornati = 0
        discrepanze = 0
        modificato  = False

        # Indice DB per order_id e posid
        idx_oid = {}
        idx_pos = {}    # per posid kraken
        idx_ts  = {}
        for i, t in enumerate(storico):
            for f in ['sl_id','tp_id','order_id_chiusura','exit_order_id',
                      'ordine_id','entry_id','order_id_apertura']:
                oid = str(t.get(f,'') or '')
                if oid and not oid.startswith('virtual'):
                    idx_oid.setdefault(oid, []).append(i)
            # posid match (Kraken position ID)
            pos_id = str(t.get('kraken_posid','') or '')
            if pos_id:
                idx_pos.setdefault(pos_id, []).append(i)
            ts = self._parse_ts(t.get('data_chiusura') or t.get('data_apertura'))
            if ts:
                # Bucket di BUCKET_SECONDI per tollerare offset timezone/latenza
                idx_ts.setdefault(int(ts // self.BUCKET_SECONDI), []).append(i)

        # Indice ledger fiat per refid
        FIAT = {'ZUSD','ZEUR','USD','EUR','USDT','USDC'}
        SKIP = {'deposit','withdrawal','transfer','staking','dividend'}
        ledger_pnl = {}
        for e in ledger:
            if e['type'] in SKIP:
                continue
            if e['asset'].upper() in FIAT and e.get('refid'):
                ledger_pnl[e['refid']] = ledger_pnl.get(e['refid'],0) + e['amount']

        matched = set()
        skipped_no_pnl = 0

        for kt in kraken_trades:
            pnl = kt['net'] if kt['net'] != 0 else ledger_pnl.get(kt['trade_id'])
            if pnl is None:
                skipped_no_pnl += 1
                continue

            # Cerca match per order_id
            idx = None
            for oid in [kt['order_id'], kt['trade_id']]:
                if oid in idx_oid:
                    idx = idx_oid[oid][0]
                    break

            # Match per posid Kraken (più robusto del solo order_id)
            if idx is None and kt.get('posid'):
                if kt['posid'] in idx_pos:
                    idx = idx_pos[kt['posid']][0]

            # Fallback per timestamp + pair (tolleranza estesa)
            if idx is None and kt['time'] > 0:
                b = int(kt['time'] // self.BUCKET_SECONDI)
                pn = self._norm_pair(kt['pair'])
                # Cerco nei bucket adiacenti (-2 .. +2 = ±20 minuti totali)
                # In modo da tollerare offset timezone bot↔Kraken anche di 1-2h
                # Ma comunque match più stretto possibile per evitare match errati.
                cand = []
                for bb in range(b-12, b+13):  # ±2h tolerance
                    for i in idx_ts.get(bb, []):
                        if self._norm_pair(storico[i].get('asset','')) == pn:
                            t_db = self._parse_ts(storico[i].get('data_chiusura'))
                            if t_db:
                                delta_s = abs(kt['time'] - t_db)
                                cand.append((delta_s, i))
                # Tieni il match più vicino in tempo
                if cand:
                    cand.sort()
                    idx = cand[0][1]

            if idx is None:
                continue

            matched.add(kt['trade_id'])
            td = storico[idx]

            # FIX spam Telegram (2026-05-07): se il trade è già stato riconciliato
            # in un run precedente e il PnL DB combacia con quello Kraken entro la
            # soglia, skippa senza riscrivere. Risolve "Aggiornati: 21" identico
            # ogni 10 minuti che generava alert inutili.
            _nota_pre = str(td.get('nota','') or '')
            _pnl_db_pre = td.get('pnl_netto_usd')
            if 'RECONCILED' in _nota_pre and _pnl_db_pre is not None:
                try:
                    if abs(float(_pnl_db_pre) - pnl) <= self.SOGLIA_DISCREPANZA:
                        continue
                except (TypeError, ValueError):
                    pass

            # Calcola PnL %
            pe  = float(td.get('p_entrata', 0) or 0)
            pu  = float(kt['cprice'] or kt['price'] or td.get('p_uscita',0) or 0)
            pnl_perc = 0.0
            if pe > 0 and pu > 0:
                d = str(td.get('direzione','LONG')).upper()
                pnl_perc = ((pu-pe)/pe*100) if d in ('LONG','BUY') else ((pe-pu)/pe*100)

            # Controlla discrepanza
            pnl_db = td.get('pnl_netto_usd')
            if pnl_db is not None and float(pnl_db or 0) != 0:
                diff = abs(float(pnl_db) - pnl)
                if diff > self.SOGLIA_DISCREPANZA:
                    discrepanze += 1
                    self.logger.warning(
                        f"⚠️ Discrepanza {td.get('asset','?')} "
                        f"DB={float(pnl_db):.2f}$ Kraken={pnl:.2f}$ Δ={diff:.2f}$")

            # Aggiorna
            td.update({
                'pnl_netto_usd':   round(pnl, 4),
                'pnl_usd':         round(pnl + float(kt.get('fee', 0) or 0), 4),  # lordo = netto + fee
                'fees':            round(float(kt.get('fee', 0) or 0), 4),
                'result_perc':     round(pnl_perc, 4),
                'pnl_finale':      round(pnl_perc, 2),
                'esito':           'WIN' if pnl > 0 else 'LOSS',
                'pnl_stimato':     False,
                'fonte_pnl':       f"kraken/{kt['trade_id']}",
                'kraken_trade_id': kt['trade_id'],
                'kraken_order_id': kt['order_id'],
                'kraken_posid':    kt.get('posid', ''),
                'fee_reale':       round(kt['fee'], 6),
            })
            if pu > 0:
                td['p_uscita'] = pu
            if kt['cprice'] > 0:
                td['cprice_kraken'] = kt['cprice']
            nota = str(td.get('nota','') or '')
            if 'RECONCILED' not in nota:
                td['nota'] = nota + ' | RECONCILED'

            with self.db._conn_lock:
                self.db._conn.execute(
                    "UPDATE kraken_trades SET reconciled=1 WHERE trade_id=?",
                    (kt['trade_id'],))
                self.db._conn.commit()

            storico[idx] = td
            modificato   = True
            aggiornati  += 1
            self.logger.info(
                f"✅ {td.get('asset','?')} "
                f"{datetime.fromtimestamp(kt['time']).strftime('%m-%d %H:%M')} "
                f"pnl={pnl:+.2f}$ ({pnl_perc:+.2f}%) pu={pu}")

        if skipped_no_pnl > 0:
            self.logger.info(f"ℹ️ {skipped_no_pnl} trade Kraken senza net PnL (probabili apertures)")

        # Orfani
        orfani = sum(
            1 for kt in kraken_trades
            if kt['trade_id'] not in matched and kt['net'] != 0)
        for kt in kraken_trades:
            if kt['trade_id'] not in matched and kt['net'] != 0:
                self.logger.warning(
                    f"👻 Orfano: {kt['pair']} "
                    f"{datetime.fromtimestamp(kt['time']).strftime('%Y-%m-%d %H:%M')} "
                    f"net={kt['net']:+.2f}$ order={kt['order_id']}")

        # Fantasmi DB
        kt_buckets = {int(kt['time']//120) for kt in kraken_trades}
        fantasmi = 0
        for t in storico:
            if 'RECONCILED' in str(t.get('nota','') or ''):
                continue
            ts = self._parse_ts(t.get('data_chiusura'))
            if not ts:
                continue
            if (time.time()-ts)/86400 > self.FINESTRA_GIORNI:
                continue
            b = int(ts//120)
            if not {b-1,b,b+1}.intersection(kt_buckets):
                fantasmi += 1

        # Salva
        if modificato:
            try:
                self.db.save_storico(storico)
                if self.trade_manager:
                    self.trade_manager.storico_trades = storico
                self.logger.info(f"💾 Storico aggiornato ({aggiornati} trade)")
            except Exception as e:
                _err.capture(e, "_incrocia_con_db", {"module": "KrakenReconciler"})
                self.logger.error(f"❌ Salvataggio storico: {e}")

        return aggiornati, discrepanze, orfani, fantasmi

    # ── RICOSTRUZIONE STORICO DA KRAKEN ────────────────────────────────────
    def ricostruisci_storico_da_kraken(self, giorni: int = 90) -> Dict:
        """
        Ricostruisce lo `storico_trades` interno DIRETTAMENTE da `kraken_trades`,
        facendo round-trip FIFO buy→sell per ciascun pair.
        
        Questo metodo è la FONTE DI VERITÀ: cancella i record interni potenzialmente
        sbagliati e li ricrea da Kraken. Serve quando:
        - kraken_reconciler._incrocia_con_db() ha sbagliato attribuzioni (PnL su trade altri)
        - lo storico_trades è stato corrotto da bug interni
        - mancano trade chiusi che il bot non ha mai registrato (es. crash mid-trade)
        
        Strategia:
        1. Legge tutti i trade Kraken degli ultimi `giorni` giorni dalla tabella kraken_trades
        2. Per ogni pair, fa FIFO matching: ogni 'sell' chiude il più vecchio 'buy' aperto
           (o viceversa per SHORT: 'buy' chiude il più vecchio 'sell' aperto)
        3. Per ogni round-trip completo, costruisce un record di storico_trades
        4. Cerca nello storico esistente un record con kraken_order_id matchante
           - Se trovato: PRESERVA i metadata (voto_ia, chimera_snapshot, razionale, ecc.)
                        e aggiorna i numeri da Kraken
           - Se non trovato: crea nuovo record marcato con fonte_pnl='kraken_rebuild'
                            (= trade non registrato dal bot, recuperato da Kraken)
        5. Salva lo storico aggiornato nel DB
        
        Returns: dict con statistiche
        """
        self.logger.info(f"🔄 [RECONCILER] Ricostruzione storico da Kraken ({giorni} giorni)...")
        since = time.time() - 86400 * giorni
        
        try:
            # 1. Leggi kraken_trades
            with self.db._conn_lock:
                c = self.db._conn.cursor()
                c.execute(
                    "SELECT trade_id, order_id, pair, time, type, price, vol, "
                    "cost, fee, net, cprice, posstatus, posid, data "
                    "FROM kraken_trades WHERE time >= ? ORDER BY time ASC",
                    (since,)
                )
                rows = c.fetchall()
            
            if not rows:
                self.logger.info("ℹ️ [RECONCILER] Nessun trade Kraken nel periodo")
                return {'totale_round_trip': 0, 'nuovi': 0, 'aggiornati': 0, 'errori': 0, 'ancora_aperte': 0}
            
            # 2. Costruisci posizioni FIFO per pair
            pair_queues = {}  # pair → list of {'open': trade_dict, 'close': trade_dict|None, 'side': 'LONG'|'SHORT'}
            for row in rows:
                trade_id, order_id, pair, t, typ, price, vol, cost, fee, net, cprice, posstatus, posid, data_raw = row
                kt = {
                    'trade_id': trade_id,
                    'order_id': order_id,
                    'pair': pair,
                    'time': float(t or 0),
                    'type': str(typ or '').lower(),
                    'price': float(price or 0),
                    'vol': float(vol or 0),
                    'cost': float(cost or 0),
                    'fee': float(fee or 0),
                    'net': float(net or 0),
                    'cprice': float(cprice or 0) if cprice else 0,
                    'posstatus': posstatus,
                    'posid': posid,
                }
                
                if pair not in pair_queues:
                    pair_queues[pair] = []
                queue = pair_queues[pair]
                
                if kt['type'] == 'buy':
                    # Apertura LONG, oppure chiusura di una SHORT esistente
                    open_short = next((p for p in queue if p['close'] is None and p['side'] == 'SHORT'), None)
                    if open_short:
                        open_short['close'] = kt
                    else:
                        queue.append({'open': kt, 'close': None, 'side': 'LONG'})
                elif kt['type'] == 'sell':
                    # Chiusura LONG esistente, oppure apertura SHORT
                    open_long = next((p for p in queue if p['close'] is None and p['side'] == 'LONG'), None)
                    if open_long:
                        open_long['close'] = kt
                    else:
                        queue.append({'open': kt, 'close': None, 'side': 'SHORT'})
            
            # 3. Carica storico esistente e indicizza per kraken_order_id e ordine_id
            storico = self.db.get_storico() or []
            idx_open_oid = {}     # order_id apertura → indice in storico
            idx_close_oid = {}    # order_id chiusura → indice in storico
            idx_kraken_tid = {}   # kraken_trade_id → indice in storico
            for i, t in enumerate(storico):
                # ordine_id originale del bot (= order_id apertura su Kraken)
                oid = str(t.get('ordine_id', '') or '').strip()
                if oid and not oid.startswith('virtual'):
                    idx_open_oid[oid] = i
                # campo settato da reconciler precedente
                kt_id = str(t.get('kraken_trade_id', '') or '').strip()
                if kt_id:
                    idx_kraken_tid[kt_id] = i
                # exit/close order id
                for f in ('order_id_chiusura', 'exit_order_id', 'sl_id', 'tp_id'):
                    oid_c = str(t.get(f, '') or '').strip()
                    if oid_c and not oid_c.startswith('virtual'):
                        idx_close_oid[oid_c] = i
            
            # 4. Per ogni round-trip completo, costruisci/aggiorna record
            nuovi = 0
            aggiornati = 0
            errori = 0
            ancora_aperte = 0
            
            # Recupera posizioni VERAMENTE aperte su Kraken (per filtrare quelle orfane)
            try:
                kraken_real = self.performer.get_open_positions_real() if self.performer else {}
                pair_open_kraken = set()
                for txid, p_k in (kraken_real or {}).items():
                    pair_open_kraken.add(self._norm_pair(p_k.get('pair', '')))
            except Exception:
                pair_open_kraken = set()
            
            for pair, queue in pair_queues.items():
                for p in queue:
                    if p['close'] is None:
                        # Pos "ancora aperta" nel FIFO: verifica che sia VERAMENTE aperta su Kraken
                        # Se non è in pair_open_kraken, allora è una chiusura orfana di una pos
                        # aperta prima della finestra di osservazione → ignora.
                        if pair_open_kraken and self._norm_pair(pair) not in pair_open_kraken:
                            continue
                        ancora_aperte += 1
                        continue
                    
                    op = p['open']
                    cl = p['close']
                    side = p['side']
                    entry = op['price']
                    uscita = cl['price']
                    vol = op['vol']
                    fee_tot = op['fee'] + cl['fee']
                    
                    # PnL netto Kraken = net del trade di chiusura (Kraken lo registra lì)
                    # Fallback: calcolo manuale da entry/uscita
                    pnl_netto = cl['net']
                    if pnl_netto == 0:
                        pnl_lordo = (uscita - entry) * vol if side == 'LONG' else (entry - uscita) * vol
                        pnl_netto = pnl_lordo - fee_tot
                    
                    pnl_pct = ((uscita - entry) / entry * 100) if side == 'LONG' else ((entry - uscita) / entry * 100)
                    
                    dt_apertura = datetime.fromtimestamp(op['time'])
                    dt_chiusura = datetime.fromtimestamp(cl['time'])
                    durata_min = (cl['time'] - op['time']) / 60.0
                    
                    # Cerca record esistente: priorità per order_id apertura
                    idx = None
                    matched_by = None
                    
                    for oid_test in [op['order_id'], op['trade_id']]:
                        if oid_test and oid_test in idx_open_oid:
                            idx = idx_open_oid[oid_test]
                            matched_by = f"open_oid={oid_test}"
                            break
                        if oid_test and oid_test in idx_kraken_tid:
                            idx = idx_kraken_tid[oid_test]
                            matched_by = f"kraken_tid={oid_test}"
                            break
                    
                    # Fallback: order_id chiusura
                    if idx is None:
                        for oid_test in [cl['order_id'], cl['trade_id']]:
                            if oid_test and oid_test in idx_close_oid:
                                idx = idx_close_oid[oid_test]
                                matched_by = f"close_oid={oid_test}"
                                break
                    
                    # Fallback finale: matching per data_apertura+pair (finestra 90 secondi)
                    if idx is None:
                        pn = self._norm_pair(pair)
                        for i, t in enumerate(storico):
                            if self._norm_pair(t.get('asset', '')) != pn:
                                continue
                            ts_ap = self._parse_ts(t.get('data_apertura'))
                            if ts_ap and abs(ts_ap - op['time']) <= 90:
                                # match per timestamp+pair
                                idx = i
                                matched_by = f"ts_pair (Δ={abs(ts_ap-op['time']):.0f}s)"
                                break
                    
                    record_aggiornato = {
                        # Numeri DEFINITIVI da Kraken (sovrascrivono sempre)
                        'asset':             pair,
                        'direzione':         side,
                        'p_entrata':         entry,
                        'p_uscita':          uscita,
                        'size':              vol,
                        'pnl_netto_usd':     round(pnl_netto, 4),
                        'pnl_usd':           round((uscita - entry) * vol if side == 'LONG' else (entry - uscita) * vol, 4),
                        'pnl_finale':        round(pnl_pct, 2),
                        'result_perc':       round(pnl_pct, 4),
                        'fees':              round(fee_tot, 4),
                        'fee_reale':         round(fee_tot, 6),
                        'esito':             'WIN' if pnl_netto > 0 else 'LOSS',
                        'data_apertura':     dt_apertura.strftime('%Y-%m-%d %H:%M:%S'),
                        'data_chiusura':     dt_chiusura.isoformat(),
                        'durata_minuti':     round(durata_min, 1),
                        'ordine_id':         op['order_id'],
                        'kraken_trade_id':   cl['trade_id'],
                        'kraken_order_id':   cl['order_id'],
                        'fonte_pnl':         'kraken_rebuild',
                        'pnl_stimato':       False,
                    }
                    
                    if idx is not None:
                        # PRESERVA tutti i campi originali, sovrascrivi solo i numeri
                        # (voto_ia, chimera_snapshot, razionale, fonte, motivo_chiusura, fase, ecc.)
                        td = storico[idx]
                        for k, v in record_aggiornato.items():
                            td[k] = v
                        nota = str(td.get('nota', '') or '')
                        if 'KRAKEN_REBUILD' not in nota:
                            td['nota'] = (nota + ' | KRAKEN_REBUILD').strip(' |')
                        storico[idx] = td
                        aggiornati += 1
                        self.logger.debug(
                            f"   ↻ {pair} {dt_apertura.strftime('%m-%d %H:%M')} "
                            f"→ {dt_chiusura.strftime('%H:%M')} "
                            f"pnl={pnl_netto:+.3f}$ matched_by={matched_by}"
                        )
                    else:
                        # Trade Kraken non presente nello storico — CREA NUOVO record
                        record_nuovo = dict(record_aggiornato)
                        record_nuovo.update({
                            'voto_ia':       None,
                            'fase':          0,
                            'leverage':      1,  # placeholder, non sappiamo la leva storica
                            'sl':            0,
                            'tp':            0,
                            'sl_id':         None,
                            'tp_id':         None,
                            'fonte':         'KRAKEN_REBUILD',
                            'tipo_op':       'KRAKEN_ONLY',
                            'razionale':     'Trade ricostruito da Kraken — non era nel diario interno del bot',
                            'motivo_chiusura': None,
                            'nota':          'KRAKEN_REBUILD',
                            'chimera_snapshot': {
                                'fonte_apertura':  'KRAKEN_REBUILD',
                                'market_regime':   'UNKNOWN',
                                'entry_phase':     'REBUILT',
                                'ciclo_fase':      'UNKNOWN',
                                'ha_daily_colore': '?',
                                'macro_sentiment': 'NEUTRAL',
                                'leverage':        1,
                                'decision_source': 'EXTERNAL',
                            },
                        })
                        storico.append(record_nuovo)
                        nuovi += 1
                        self.logger.info(
                            f"   ➕ {pair} {dt_apertura.strftime('%m-%d %H:%M')} "
                            f"→ {dt_chiusura.strftime('%H:%M')} "
                            f"pnl={pnl_netto:+.3f}$ NUOVO (non era nel diario)"
                        )
            
            # 5. Salva lo storico aggiornato
            if nuovi > 0 or aggiornati > 0:
                try:
                    self.db.save_storico(storico)
                    self.logger.info(f"💾 Storico salvato: {nuovi} nuovi, {aggiornati} aggiornati")
                    # Notifica trade_manager se collegato
                    if self.trade_manager:
                        try:
                            self.trade_manager.storico_trades = storico
                        except Exception:
                            pass
                except Exception as e:
                    _err.capture(e, "ricostruisci_storico_da_kraken", {"module": "KrakenReconciler"})
                    self.logger.error(f"❌ Salvataggio storico (rebuild): {e}")
                    errori += 1
            
            stats = {
                'totale_round_trip':  sum(1 for q in pair_queues.values() for p in q if p['close']),
                'nuovi':              nuovi,
                'aggiornati':         aggiornati,
                'ancora_aperte':      ancora_aperte,
                'errori':             errori,
            }
            self.logger.info(
                f"✅ [RECONCILER REBUILD] {stats['totale_round_trip']} round-trip processati: "
                f"{nuovi} NUOVI + {aggiornati} aggiornati ({ancora_aperte} ancora aperte)"
            )
            return stats
            
        except Exception as e:
            _err.capture(e, "ricostruisci_storico_da_kraken", {"module": "KrakenReconciler"})
            self.logger.error(f"❌ [RECONCILER REBUILD] {e}")
            return {'totale_round_trip': 0, 'nuovi': 0, 'aggiornati': 0, 'errori': 1, 'ancora_aperte': 0}

    # ── QUERY ────────────────────────────────────────────────────────────────

    def get_ledger_summary(self, giorni: int = 7) -> Dict:
        """PnL per giorno, fee totali, PnL per pair — da tabelle Kraken nel DB."""
        since = time.time() - 86400 * giorni
        try:
            with self.db._conn_lock:
                c = self.db._conn.cursor()
                c.execute("""
                    SELECT date(time,'unixepoch') g, SUM(amount) pnl, COUNT(*) n
                    FROM kraken_ledger
                    WHERE time>=? AND asset IN ('ZUSD','ZEUR','USD','EUR')
                    AND type NOT IN ('deposit','withdrawal','staking','transfer')
                    GROUP BY g ORDER BY g DESC LIMIT ?
                """, (since, giorni))
                pnl_g = [{"giorno":r[0],"pnl":round(r[1],2),"voci":r[2]}
                         for r in c.fetchall()]

                c.execute("SELECT SUM(fee) FROM kraken_ledger WHERE time>=?", (since,))
                fee = c.fetchone()[0] or 0

                c.execute("""
                    SELECT pair, COUNT(*) n, SUM(net) tot
                    FROM kraken_trades
                    WHERE time>=? AND posstatus='closed'
                    GROUP BY pair ORDER BY tot DESC
                """, (since,))
                per_pair = [{"pair":r[0],"n":r[1],"net":round(r[2] or 0,2)}
                            for r in c.fetchall()]

                return {"pnl_per_giorno": pnl_g,
                        "fee_totali":     round(fee, 4),
                        "per_pair":       per_pair}
        except Exception as e:
            _err.capture(e, "get_ledger_summary", {"module": "KrakenReconciler"})
            self.logger.error(f"Ledger summary: {e}")
            return {}

    def get_trade_reale(self, order_id: str) -> Optional[Dict]:
        try:
            with self.db._conn_lock:
                c = self.db._conn.cursor()
                c.execute(
                    "SELECT data FROM kraken_trades "
                    "WHERE order_id=? OR trade_id=?",
                    (order_id, order_id))
                row = c.fetchone()
                return json.loads(row[0]) if row else None
        except Exception:
            return None

    # ── UTILITY ──────────────────────────────────────────────────────────────

    def _parse_ts(self, dt_str) -> Optional[float]:
        """Converte una stringa datetime in timestamp UNIX UTC.
        
        FIX 2026-05-04: il bot logga `datetime.now().isoformat()` che è ora
        LOCALE senza timezone (es. '2026-03-27T21:46:58' in CET).
        Prima questa veniva interpretata come UTC, generando offset di 1-2h
        e facendo fallire tutti i match per timestamp.
        Ora interpretiamo correttamente:
          - Se la stringa contiene '+', '-', 'Z' al timezone → ha già tz, parse diretto
          - Altrimenti → ora locale di sistema, convertiamo a UTC con tz nativa
        """
        if not dt_str:
            return None
        try:
            s = str(dt_str).replace('Z','+00:00').strip()
            dt = datetime.fromisoformat(s)
            if dt.tzinfo is None:
                # Stringa senza timezone = ora LOCALE del sistema
                # (è quello che fa datetime.now() del bot)
                dt = dt.astimezone()  # interpreta come local, convert a UTC tz-aware
            return dt.timestamp()
        except Exception:
            try:
                return float(dt_str)
            except Exception:
                return None

    def _norm_pair(self, pair: str) -> str:
        if not pair:
            return ''
        p = str(pair).upper().replace('/','').replace('-','').replace(' ','')
        M = {'XXBTZUSD':'BTCUSD','XBTUSD':'BTCUSD','BTCUSD':'BTCUSD',
             'XETHZUSD':'ETHUSD','ETHUSD':'ETHUSD',
             'XXRPZUSD':'XRPUSD','XRPUSD':'XRPUSD',
             'XDGUSD':'DOGEUSD','DOGEUSD':'DOGEUSD',
             'XZECZUSD':'ZECUSD','ZECUSD':'ZECUSD',
             'SOLUSD':'SOLUSD','BONKUSD':'BONKUSD'}
        return M.get(p, p)

    # ── TELEGRAM ─────────────────────────────────────────────────────────────

    def invia_stato_telegram(self):
        try:
            stats   = self.esegui(force=True)
            summary = self.get_ledger_summary(giorni=7)

            pnl_lines = ""
            for g in summary.get("pnl_per_giorno", [])[:5]:
                e = "🟢" if g["pnl"] > 0 else "🔴"
                pnl_lines += f"\n  {e} {g['giorno']}: {g['pnl']:+.2f}$"

            pair_lines = ""
            for p in summary.get("per_pair", [])[:5]:
                e = "🟢" if p["net"] > 0 else "🔴"
                pair_lines += f"\n  {e} {p['pair']}: {p['net']:+.2f}$ ({p['n']} trade)"

            msg = (
                f"🔄 *RECONCILER — REPORT COMPLETO*\n\n"
                f"*Sync Kraken ↔ DB:*\n"
                f"• Ledger voci: {stats['ledger_voci']}\n"
                f"• Trade Kraken chiusi: {stats['trades_kraken']}\n"
                f"• Trade DB aggiornati: {stats['trades_aggiornati']}\n"
                f"• Discrepanze PnL: {stats['discrepanze']}\n"
                f"• Orfani (Kraken no DB): {stats['orfani_kraken']}\n"
                f"• Fee totali 7gg: {summary.get('fee_totali',0):.4f}$"
            )
            if pnl_lines:
                msg += f"\n\n*PnL per giorno:*{pnl_lines}"
            if pair_lines:
                msg += f"\n\n*Per pair:*{pair_lines}"
            msg += f"\n\n🕐 {stats['timestamp'][:16]} UTC"

            if self.alerts:
                self.alerts.invia_alert(msg)
        except Exception as e:
            _err.capture(e, "invia_stato_telegram", {"module": "KrakenReconciler"})
            if self.alerts:
                self.alerts.invia_alert(f"❌ Errore /reconcile: {e}")

    def _invia_report(self, stats: Dict):
        if not self.alerts:
            return
        if stats['discrepanze'] == 0 and stats['orfani_kraken'] == 0:
            return
        try:
            self.alerts.invia_alert(
                f"⚠️ *RECONCILER — ANOMALIE*\n"
                f"• Discrepanze: {stats['discrepanze']}\n"
                f"• Orfani: {stats['orfani_kraken']}\n"
                f"• Aggiornati: {stats['trades_aggiornati']}\n"
                f"Usa /reconcile per dettagli.")
        except Exception:
            pass
