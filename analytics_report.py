"""
CHIMERA — Analytics Report Automatico
========================================
Genera report statistici completi direttamente dal DB del bot
con fee reali recuperate da Kraken.

Viene chiamato automaticamente:
  - Ogni domenica alle 20:00 UTC (report settimanale)
  - Su comando tramite Telegram: /stats o /report
  - Ogni giorno alle 23:00 UTC (report giornaliero)

Può anche essere lanciato standalone:
  python3 analytics_report.py
"""

import sqlite3
import json
import time
import logging
import os
import sys
from datetime import datetime, timedelta, timezone
from collections import defaultdict

logger = logging.getLogger("AnalyticsReport")

# ErrorTracker — opzionale: il file è usato anche standalone senza core/
try:
    from core.chimera_errors import ErrorTracker
    _err = ErrorTracker("AnalyticsReport")
except ImportError:
    class _FallbackErr:
        def capture(self, *a, **k): pass
    _err = _FallbackErr()


# ─────────────────────────────────────────────────────────────────────────────
#  FEE REALI DA KRAKEN
# ─────────────────────────────────────────────────────────────────────────────


# ─────────────────────────────────────────────────────────────────────────────
#  PORTFOLIO REALE DA KRAKEN
# ─────────────────────────────────────────────────────────────────────────────

def recupera_portfolio_kraken(performer, asset='ZUSD'):
    """
    Recupera il valore reale del portfolio da Kraken (TradeBalance).
    Affianca le statistiche locali del DB con i dati di cassa reale.

    Restituisce un dict con:
      equity_totale       — Equity netta (capitale + PnL aperto)
      equivalent_balance  — Valore totale conto (collaterale + crypto valorizzati)
      trade_balance       — Collaterale puro fiat
      pnl_aperto          — PnL non realizzato posizioni aperte
      margine_usato       — Margine attualmente impegnato
      margine_libero      — Margine disponibile per nuove posizioni
      margin_level_pct    — Livello margine % (sotto 100 = margin call)
      costo_posizioni     — Cost basis posizioni aperte
      valore_posizioni    — Valore corrente posizioni aperte (cost + PnL)
      errore              — None se ok, stringa se errore API
    """
    vuoto = {
        'equity_totale': 0.0, 'equivalent_balance': 0.0,
        'trade_balance': 0.0, 'pnl_aperto': 0.0,
        'margine_usato': 0.0, 'margine_libero': 0.0,
        'margin_level_pct': 0.0, 'costo_posizioni': 0.0,
        'valore_posizioni': 0.0, 'errore': 'Performer non disponibile'
    }
    if not performer:
        return vuoto
    if not hasattr(performer, 'get_portfolio_snapshot'):
        vuoto['errore'] = 'get_portfolio_snapshot non disponibile nel performer'
        return vuoto

    snap = performer.get_portfolio_snapshot(asset=asset)
    return {
        'equity_totale':      snap.get('equity_totale',  0.0),
        'equivalent_balance': snap.get('eb',              0.0),
        'trade_balance':      snap.get('tb',              0.0),
        'pnl_aperto':         snap.get('pnl_aperto',      0.0),
        'margine_usato':      snap.get('m',               0.0),
        'margine_libero':     snap.get('mf',              0.0),
        'margin_level_pct':   snap.get('ml',              0.0),
        'costo_posizioni':    snap.get('c',               0.0),
        'valore_posizioni':   snap.get('uv',              0.0),
        'errore':             snap.get('errore',          None),
    }


def recupera_fee_reali_kraken(performer, giorni_indietro=30):
    """
    Recupera le fee reali pagate su Kraken negli ultimi N giorni
    direttamente dal ledger.
    Restituisce: { 'totale_fee': float, 'per_trade': { ordine_id: fee } }
    """
    result = {'totale_fee': 0.0, 'per_trade': {}, 'errore': None}
    if not performer:
        result['errore'] = "Performer non disponibile"
        return result
    try:
        since = int(time.time() - 86400 * giorni_indietro)
        # Recupera ledger tipo 'trade' (contiene fee e PnL reali)
        res = performer.exchange.private_post_ledgers({
            'start': since,
            'type': 'trade'
        })
        ledger = res.get('result', {}).get('ledger', {})
        for entry_id, entry in ledger.items():
            fee = float(entry.get('fee', 0) or 0)
            result['totale_fee'] += fee
            ordine = entry.get('refid', entry_id)
            if ordine not in result['per_trade']:
                result['per_trade'][ordine] = 0.0
            result['per_trade'][ordine] += fee
        return result
    except Exception as e:
        _err.capture(e, sys._getframe().f_code.co_name, {"module": "AnalyticsReport"})
        result['errore'] = str(e)
        logger.error(f"Errore recupero fee Kraken: {e}")
        return result


# ─────────────────────────────────────────────────────────────────────────────
#  CARICAMENTO TRADE DAL DB
# ─────────────────────────────────────────────────────────────────────────────

def carica_trades(db_path='chimera.db', giorni=None):
    """
    Carica i trade chiusi dal DB.
    Se giorni è specificato, filtra agli ultimi N giorni.
    """
    trades = []
    try:
        conn = sqlite3.connect(db_path)
        rows = conn.execute("SELECT data FROM storico_trades").fetchall()
        conn.close()
        cutoff = None
        if giorni:
            cutoff = datetime.now() - timedelta(days=giorni)
        for r in rows:
            try:
                t = json.loads(r[0])
                if not t.get('data_chiusura'):
                    continue
                # Escludi ghost (VIRTUAL_BRAIN) e storico simulato dai report
                # — i numeri reali devono riflettere solo i trade eseguiti su Kraken
                fonte = str(t.get('fonte', '') or '').upper()
                if fonte in ('VIRTUAL_BRAIN', 'GHOST', 'GHOST_KRAKEN', 'STORICO_SIMULATO'):
                    continue
                if cutoff:
                    dt = datetime.fromisoformat(
                        t['data_chiusura'].replace('Z', '').replace('+00:00', '')
                    )
                    if dt < cutoff:
                        continue
                trades.append(t)
            except Exception:
                continue
    except Exception as e:
        _err.capture(e, sys._getframe().f_code.co_name, {"module": "AnalyticsReport"})
        logger.error(f"Errore caricamento trades: {e}")
    return trades


# ─────────────────────────────────────────────────────────────────────────────
#  ANALISI STATISTICA COMPLETA
# ─────────────────────────────────────────────────────────────────────────────

def analizza_trades(trades):
    """
    Calcola statistiche complete su una lista di trade.
    """
    if not trades:
        return {}

    stats = {
        'n_totale': len(trades),
        'n_win': 0, 'n_loss': 0,
        'pnl_lordo': 0.0, 'pnl_netto': 0.0, 'fee_totali': 0.0,
        'win_rate': 0.0, 'profit_factor': 0.0,
        'avg_win': 0.0, 'avg_loss': 0.0,
        'best_trade': 0.0, 'worst_trade': 0.0,
        'per_asset': defaultdict(lambda: {'n':0,'win':0,'pnl':0.0,'fee':0.0}),
        'per_direzione': defaultdict(lambda: {'n':0,'win':0,'pnl':0.0}),
        'per_ora': defaultdict(lambda: {'n':0,'win':0,'pnl':0.0}),
        'per_giorno_settimana': defaultdict(lambda: {'n':0,'win':0,'pnl':0.0}),
        'per_stile': defaultdict(lambda: {'n':0,'win':0,'pnl':0.0}),
        'per_voto': defaultdict(lambda: {'n':0,'win':0,'pnl':0.0}),
        'per_decision_source': defaultdict(lambda: {'n':0,'win':0,'pnl':0.0}),
        'wins_usd': [], 'losses_usd': [],
        'sequenza_corrente': 0,  # positivo=win streak, negativo=loss streak
    }

    giorni_sett = ['Lun','Mar','Mer','Gio','Ven','Sab','Dom']
    seq = 0  # tracking streak

    for t in sorted(trades, key=lambda x: x.get('data_chiusura', '')):
        pnl_n = float(t.get('pnl_netto_usd', t.get('pnl_usd', 0)) or 0)
        pnl_l = float(t.get('pnl_usd', pnl_n) or 0)
        fee   = float(t.get('fees', 0) or 0)
        esito = t.get('esito', 'WIN' if pnl_n > 0 else 'LOSS')
        asset = t.get('asset', '?')
        dire  = t.get('direzione', '?').upper()
        stile = t.get('tipo_op', t.get('tipo_operazione', 'SWING')).upper()
        voto  = int(t.get('voto_ia', 0) or 0)

        # Decision source — normalizza varianti storiche in macro-categorie
        raw_src = (
            t.get('decision_source') or
            (t.get('chimera_snapshot') or {}).get('decision_source') or
            'UNKNOWN'
        )
        src = str(raw_src).upper()
        if 'STRATEGY' in src and 'GEMINI' in src:
            src_bucket = 'STRATEGY+GEMINI'
        elif src.startswith('STRATEGY'):
            src_bucket = 'STRATEGY'
        elif 'GEMINI_FLAT' in src:
            src_bucket = 'GEMINI_FLAT'
        elif 'GEMINI' in src:
            src_bucket = 'GEMINI'
        elif 'ML' in src or 'CHIMERA' in src:
            src_bucket = 'ML_ONLY'
        else:
            src_bucket = 'UNKNOWN'

        # Data e ora chiusura
        try:
            dt = datetime.fromisoformat(
                t['data_chiusura'].replace('Z', '').replace('+00:00', '')
            )
            ora  = dt.hour
            gds  = giorni_sett[dt.weekday()]
        except Exception:
            ora, gds = -1, '?'

        # Totali
        stats['pnl_lordo']  += pnl_l
        stats['pnl_netto']  += pnl_n
        stats['fee_totali'] += fee

        if esito == 'WIN':
            stats['n_win'] += 1
            stats['wins_usd'].append(pnl_n)
            seq = seq + 1 if seq >= 0 else 1
        else:
            stats['n_loss'] += 1
            stats['losses_usd'].append(pnl_n)
            seq = seq - 1 if seq <= 0 else -1

        stats['best_trade']  = max(stats['best_trade'],  pnl_n)
        stats['worst_trade'] = min(stats['worst_trade'], pnl_n)

        # Per asset
        stats['per_asset'][asset]['n']   += 1
        stats['per_asset'][asset]['win'] += (1 if esito == 'WIN' else 0)
        stats['per_asset'][asset]['pnl'] += pnl_n
        stats['per_asset'][asset]['fee'] += fee

        # Per direzione (LONG/SHORT)
        stats['per_direzione'][dire]['n']   += 1
        stats['per_direzione'][dire]['win'] += (1 if esito == 'WIN' else 0)
        stats['per_direzione'][dire]['pnl'] += pnl_n

        # Per ora del giorno
        stats['per_ora'][ora]['n']   += 1
        stats['per_ora'][ora]['win'] += (1 if esito == 'WIN' else 0)
        stats['per_ora'][ora]['pnl'] += pnl_n

        # Per giorno della settimana
        stats['per_giorno_settimana'][gds]['n']   += 1
        stats['per_giorno_settimana'][gds]['win'] += (1 if esito == 'WIN' else 0)
        stats['per_giorno_settimana'][gds]['pnl'] += pnl_n

        # Per stile
        stats['per_stile'][stile]['n']   += 1
        stats['per_stile'][stile]['win'] += (1 if esito == 'WIN' else 0)
        stats['per_stile'][stile]['pnl'] += pnl_n

        # Per voto IA (raggruppato)
        bucket = f"{(voto//2)*2}-{(voto//2)*2+1}" if voto < 10 else "10"
        stats['per_voto'][bucket]['n']   += 1
        stats['per_voto'][bucket]['win'] += (1 if esito == 'WIN' else 0)
        stats['per_voto'][bucket]['pnl'] += pnl_n

        # Per decision source (STRATEGY / STRATEGY+GEMINI / GEMINI / GEMINI_FLAT / ML_ONLY)
        stats['per_decision_source'][src_bucket]['n']   += 1
        stats['per_decision_source'][src_bucket]['win'] += (1 if esito == 'WIN' else 0)
        stats['per_decision_source'][src_bucket]['pnl'] += pnl_n

    stats['sequenza_corrente'] = seq
    n = stats['n_totale']
    stats['win_rate'] = round(stats['n_win'] / n * 100, 1) if n > 0 else 0.0
    stats['avg_win']  = round(sum(stats['wins_usd']) / len(stats['wins_usd']), 3) if stats['wins_usd'] else 0.0
    stats['avg_loss'] = round(sum(stats['losses_usd']) / len(stats['losses_usd']), 3) if stats['losses_usd'] else 0.0
    gp = sum(stats['wins_usd'])
    gl = abs(sum(stats['losses_usd']))
    stats['profit_factor'] = round(gp / gl, 2) if gl > 0 else (round(gp, 2) if gp > 0 else 0.0)

    return stats


# ─────────────────────────────────────────────────────────────────────────────
#  FORMATTAZIONE REPORT TELEGRAM
# ─────────────────────────────────────────────────────────────────────────────

def formatta_report_telegram(stats, titolo="📊 CHIMERA — REPORT STATISTICO", fee_reali=None, portfolio_kraken=None):
    """
    Formatta le statistiche in un messaggio Telegram leggibile.
    """
    if not stats:
        return f"{titolo}\n\nNessun trade disponibile."

    n = stats['n_totale']
    pnl_netto = stats['pnl_netto']
    pnl_lordo = stats['pnl_lordo']
    fee_db    = stats['fee_totali']
    wr        = stats['win_rate']
    pf        = stats['profit_factor']

    seq = stats['sequenza_corrente']
    if seq > 1:
        streak_txt = f"🔥 WIN streak: {seq}"
    elif seq < -1:
        streak_txt = f"🥶 LOSS streak: {abs(seq)}"
    else:
        streak_txt = "➡️ Neutro"

    lines = [
        titolo,
        "━━━━━━━━━━━━━━━━━━",
        f"📈 *RIEPILOGO GENERALE*",
        f"• Trade totali: {n} | WIN: {stats['n_win']} | LOSS: {stats['n_loss']}",
        f"• Win Rate: *{wr}%*",
        f"• Profit Factor: *{pf}*",
        f"• PnL Lordo: *{pnl_lordo:+.3f}$*",
        f"• Fee (stimate DB): {fee_db:.4f}$",
    ]

    if fee_reali is not None:
        fee_diff = fee_reali - fee_db
        lines.append(f"• Fee (reali Kraken): *{fee_reali:.4f}$* (diff: {fee_diff:+.4f}$)")
        pnl_reale_post_fee = pnl_lordo - fee_reali
        lines.append(f"• PnL Netto reale: *{pnl_reale_post_fee:+.3f}$*")
    else:
        lines.append(f"• PnL Netto: *{pnl_netto:+.3f}$*")

    lines += [
        f"• Best trade: +{stats['best_trade']:.3f}$ | Worst: {stats['worst_trade']:.3f}$",
        f"• Avg WIN: +{stats['avg_win']:.3f}$ | Avg LOSS: {stats['avg_loss']:.3f}$",
        f"• {streak_txt}",
        "",
        "━━━━━━━━━━━━━━━━━━",
        "📊 *PER ASSET*",
    ]

    for asset, s in sorted(stats['per_asset'].items(), key=lambda x: x[1]['pnl'], reverse=True):
        wr_a = round(s['win'] / s['n'] * 100, 0) if s['n'] > 0 else 0
        fee_a = s['fee']
        lines.append(
            f"• {asset:<12} {s['n']} trade | WR {wr_a:.0f}% | "
            f"PnL {s['pnl']:+.3f}$ | fee {fee_a:.4f}$"
        )

    lines += ["", "━━━━━━━━━━━━━━━━━━", "📊 *LONG vs SHORT*"]
    for dire, s in sorted(stats['per_direzione'].items()):
        wr_d = round(s['win'] / s['n'] * 100, 0) if s['n'] > 0 else 0
        lines.append(f"• {dire:<6} {s['n']} trade | WR {wr_d:.0f}% | PnL {s['pnl']:+.3f}$")

    lines += ["", "━━━━━━━━━━━━━━━━━━", "📊 *PER STILE*"]
    for stile, s in sorted(stats['per_stile'].items()):
        wr_s = round(s['win'] / s['n'] * 100, 0) if s['n'] > 0 else 0
        lines.append(f"• {stile:<10} {s['n']} trade | WR {wr_s:.0f}% | PnL {s['pnl']:+.3f}$")

    # Migliori/peggiori ore
    ore_dati = {k: v for k, v in stats['per_ora'].items() if k >= 0 and v['n'] >= 2}
    if ore_dati:
        migliore_ora = max(ore_dati.items(), key=lambda x: x[1]['pnl'])
        peggiore_ora = min(ore_dati.items(), key=lambda x: x[1]['pnl'])
        lines += [
            "", "━━━━━━━━━━━━━━━━━━",
            "🕐 *ORA MIGLIORE/PEGGIORE*",
            f"• 🟢 {migliore_ora[0]:02d}:00 UTC — {migliore_ora[1]['n']} trade | "
            f"WR {round(migliore_ora[1]['win']/migliore_ora[1]['n']*100)}% | "
            f"PnL {migliore_ora[1]['pnl']:+.3f}$",
            f"• 🔴 {peggiore_ora[0]:02d}:00 UTC — {peggiore_ora[1]['n']} trade | "
            f"WR {round(peggiore_ora[1]['win']/peggiore_ora[1]['n']*100)}% | "
            f"PnL {peggiore_ora[1]['pnl']:+.3f}$",
        ]

    # Ore complete (solo quelle con almeno 1 trade)
    lines += ["", "━━━━━━━━━━━━━━━━━━", "🕐 *DISTRIBUZIONE ORARIA* (UTC)"]
    for ora in sorted(stats['per_ora'].keys()):
        if ora < 0: continue
        s = stats['per_ora'][ora]
        if s['n'] == 0: continue
        wr_o = round(s['win'] / s['n'] * 100)
        bar = "█" * min(s['n'], 10)
        emoji = "🟢" if s['pnl'] > 0 else "🔴"
        lines.append(f"• {emoji} {ora:02d}h {bar:<10} {s['n']:2}T WR{wr_o:3}% {s['pnl']:+.2f}$")

    # Giorni settimana
    ordine_giorni = ['Lun','Mar','Mer','Gio','Ven','Sab','Dom']
    giorni_con_dati = {k: v for k, v in stats['per_giorno_settimana'].items() if v['n'] > 0}
    if giorni_con_dati:
        lines += ["", "━━━━━━━━━━━━━━━━━━", "📅 *DISTRIBUZIONE SETTIMANALE*"]
        for g in ordine_giorni:
            if g not in giorni_con_dati:
                continue
            s = giorni_con_dati[g]
            wr_g = round(s['win'] / s['n'] * 100)
            emoji = "🟢" if s['pnl'] > 0 else "🔴"
            lines.append(f"• {emoji} {g}  {s['n']:2}T WR{wr_g:3}% PnL {s['pnl']:+.3f}$")

    # Per voto IA
    if stats['per_voto']:
        lines += ["", "━━━━━━━━━━━━━━━━━━", "🎯 *PERFORMANCE PER VOTO IA*"]
        for bucket in sorted(stats['per_voto'].keys()):
            s = stats['per_voto'][bucket]
            if s['n'] == 0: continue
            wr_v = round(s['win'] / s['n'] * 100)
            emoji = "🟢" if s['pnl'] > 0 else "🔴"
            lines.append(f"• {emoji} Voto {bucket}  {s['n']:2}T WR{wr_v:3}% PnL {s['pnl']:+.3f}$")

    # Per decision source — confronto deterministico vs AI vs ML
    src_data = {k: v for k, v in stats.get('per_decision_source', {}).items() if v['n'] > 0}
    if src_data:
        lines += ["", "━━━━━━━━━━━━━━━━━━", "🤖 *STRATEGY vs GEMINI vs ML*"]
        ordine_src = ['STRATEGY', 'STRATEGY+GEMINI', 'GEMINI', 'GEMINI_FLAT', 'ML_ONLY', 'UNKNOWN']
        labels = {
            'STRATEGY':        '🎯 STRATEGY       ',
            'STRATEGY+GEMINI': '🤝 STRAT+GEMINI   ',
            'GEMINI':          '🧠 GEMINI         ',
            'GEMINI_FLAT':     '⏭️  GEMINI_FLAT   ',
            'ML_ONLY':         '⚙️  ML_ONLY       ',
            'UNKNOWN':         '❓ UNKNOWN        ',
        }
        for src in ordine_src:
            if src not in src_data:
                continue
            s = src_data[src]
            wr_s = round(s['win'] / s['n'] * 100)
            emoji = "🟢" if s['pnl'] > 0 else ("🔴" if s['pnl'] < 0 else "⚪")
            lines.append(f"• {emoji} {labels.get(src, src)} {s['n']:2}T WR{wr_s:3}% PnL {s['pnl']:+.3f}$")

    # ── PORTFOLIO REALE KRAKEN ─────────────────────────────────────────────
    if portfolio_kraken and not portfolio_kraken.get('errore'):
        p   = portfolio_kraken
        eq  = p.get('equity_totale',     0.0)
        eb  = p.get('equivalent_balance', 0.0)
        pnl = p.get('pnl_aperto',         0.0)
        mf  = p.get('margine_libero',     0.0)
        m   = p.get('margine_usato',      0.0)
        ml  = p.get('margin_level_pct',   0.0)
        uv  = p.get('valore_posizioni',   0.0)
        c   = p.get('costo_posizioni',    0.0)

        pnl_emoji = "🟢" if pnl >= 0 else "🔴"
        ml_emoji  = ("🟢" if ml > 200 else ("🟡" if ml > 120 else "🔴")) if ml > 0 else "⚪"

        lines += [
            "",
            "━━━━━━━━━━━━━━━━━━",
            "💼 *PORTFOLIO REALE KRAKEN*",
            f"• Equity netta:       *{eq:.2f}$*",
            f"• Equivalent Balance: {eb:.2f}$",
            f"• PnL aperto:         {pnl_emoji} {pnl:+.2f}$",
            "━━━━━━━━━━━━━━━━━━",
            f"• Margine libero:     {mf:.2f}$",
            f"• Margine usato:      {m:.2f}$",
        ]
        if ml > 0:
            lines.append(f"• Margin level:       {ml_emoji} {ml:.1f}%")
        if uv > 0:
            diff = uv - c
            diff_emoji = "🟢" if diff >= 0 else "🔴"
            lines.append(f"• Valore posizioni:   {uv:.2f}$ (cost {c:.2f}$ | {diff_emoji} {diff:+.2f}$)")
    elif portfolio_kraken and portfolio_kraken.get('errore'):
        lines += ["", f"⚠️ Portfolio Kraken non disponibile: {portfolio_kraken['errore']}"]

    lines.append("\n_Dati: DB locale + Kraken Ledger + TradeBalance_")
    return "\n".join(lines)


# ─────────────────────────────────────────────────────────────────────────────
#  ENTRY POINT PRINCIPALE
# ─────────────────────────────────────────────────────────────────────────────

class AnalyticsReporter:
    """
    Classe principale — istanziata una volta sola nel bot.
    Gestisce la schedulazione automatica dei report.
    """
    def __init__(self, db_path, alerts=None, performer=None):
        self.db_path   = db_path
        self.alerts    = alerts
        self.performer = performer
        self._ultimo_report_giornaliero  = 0
        self._ultimo_report_settimanale  = 0
        self.logger = logging.getLogger("AnalyticsReporter")

    def controlla_e_invia(self):
        """
        Chiama questo metodo ogni ciclo del bot.
        Invia automaticamente:
        - Report giornaliero ogni giorno alle 23:00 UTC
        - Report settimanale ogni domenica alle 20:00 UTC
        """
        now = datetime.now(timezone.utc)

        # Report giornaliero: 23:00 UTC ogni giorno
        if now.hour == 23 and now.minute < 5:
            if time.time() - self._ultimo_report_giornaliero > 3600:
                self.invia_report_giornaliero()
                self._ultimo_report_giornaliero = time.time()

        # Report settimanale: domenica alle 20:00 UTC
        if now.weekday() == 6 and now.hour == 20 and now.minute < 5:
            if time.time() - self._ultimo_report_settimanale > 3600 * 6:
                self.invia_report_settimanale()
                self._ultimo_report_settimanale = time.time()

    def invia_report_giornaliero(self):
        """Report degli ultimi 24h con fee reali da Kraken."""
        try:
            trades = carica_trades(self.db_path, giorni=1)
            stats  = analizza_trades(trades)

            fee_reali = None
            if self.performer:
                fr = recupera_fee_reali_kraken(self.performer, giorni_indietro=1)
                if not fr.get('errore'):
                    fee_reali = fr['totale_fee']

            portfolio = recupera_portfolio_kraken(self.performer) if self.performer else None
            msg = formatta_report_telegram(
                stats,
                titolo=f"📊 CHIMERA — REPORT GIORNALIERO ({datetime.now(timezone.utc).strftime('%d/%m/%Y')})",
                fee_reali=fee_reali,
                portfolio_kraken=portfolio
            )
            if self.alerts:
                self.alerts.invia_alert(msg)
            self.logger.info("📊 Report giornaliero inviato.")
        except Exception as e:
            _err.capture(e, "invia_report_giornaliero", {"module": "AnalyticsReporter"})
            self.logger.error(f"Errore report giornaliero: {e}")

    def invia_report_settimanale(self):
        """Report degli ultimi 7 giorni con fee reali da Kraken."""
        try:
            trades = carica_trades(self.db_path, giorni=7)
            stats  = analizza_trades(trades)

            fee_reali = None
            if self.performer:
                fr = recupera_fee_reali_kraken(self.performer, giorni_indietro=7)
                if not fr.get('errore'):
                    fee_reali = fr['totale_fee']

            portfolio = recupera_portfolio_kraken(self.performer) if self.performer else None
            msg = formatta_report_telegram(
                stats,
                titolo="📊 CHIMERA — REPORT SETTIMANALE",
                fee_reali=fee_reali,
                portfolio_kraken=portfolio
            )
            if self.alerts:
                self.alerts.invia_alert(msg)
            self.logger.info("📊 Report settimanale inviato.")
        except Exception as e:
            _err.capture(e, "invia_report_settimanale", {"module": "AnalyticsReporter"})
            self.logger.error(f"Errore report settimanale: {e}")

    def invia_report_su_richiesta(self, giorni=30):
        """Report su richiesta per gli ultimi N giorni."""
        try:
            trades = carica_trades(self.db_path, giorni=giorni)
            stats  = analizza_trades(trades)

            fee_reali = None
            if self.performer:
                fr = recupera_fee_reali_kraken(self.performer, giorni_indietro=giorni)
                if not fr.get('errore'):
                    fee_reali = fr['totale_fee']

            portfolio = recupera_portfolio_kraken(self.performer) if self.performer else None
            msg = formatta_report_telegram(
                stats,
                titolo=f"📊 CHIMERA — ULTIMI {giorni} GIORNI",
                fee_reali=fee_reali,
                portfolio_kraken=portfolio
            )
            if self.alerts:
                self.alerts.invia_alert(msg)
            return msg
        except Exception as e:
            _err.capture(e, "invia_report_su_richiesta", {"module": "AnalyticsReporter", "giorni": giorni})
            self.logger.error(f"Errore report su richiesta: {e}")
            return f"Errore generazione report: {e}"


# ─────────────────────────────────────────────────────────────────────────────
#  STANDALONE — eseguibile direttamente
# ─────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import sys
    logging.basicConfig(level=logging.INFO)

    db = "chimera.db"
    if not os.path.exists(db):
        print(f"DB non trovato: {db}")
        sys.exit(1)

    giorni = int(sys.argv[1]) if len(sys.argv) > 1 else 30
    print(f"Analisi ultimi {giorni} giorni...\n")

    trades = carica_trades(db, giorni=giorni)
    print(f"Trade trovati: {len(trades)}")
    stats  = analizza_trades(trades)
    msg    = formatta_report_telegram(
        stats,
        titolo=f"📊 CHIMERA — ULTIMI {giorni} GIORNI",
        portfolio_kraken=None
    )
    print(msg)
