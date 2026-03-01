"""
L&A Institutional Bot - Report/analisi Unificata

Permette di aggregare performance da log, scatola_nera.json e report_reale.json.
Uso:
    python analysis/report_bot.py --log      # Analisi performance dal log
    python analysis/report_bot.py --scatola  # Analisi avanzata scatola nera
    python analysis/report_bot.py --bilancio # Calcolo bilancio reale
"""

import argparse
import json
from collections import defaultdict
import re

def analizza_scatola_nera(path='scatola_nera.json'):
    """
    Analisi avanzata della scatola nera: aggrega performance per voto e asset.
    """
    voti_stats = {}
    SIZE_PER_TRADE = 100
    profitto_totale_generale = 0

    print("\n🧐 ANALISI AVANZATA SCATOLA NERA")
    print("==============================================")
    try:
        with open(path, "r", encoding="utf-8") as f:
            for line in f:
                try:
                    data = json.loads(line)
                    out = data.get("output_brain", {})
                    engine = data.get("input_engine", {})
                    voto = float(out.get("voto", 0))
                    asset = data.get("asset", "N/D")
                    direzione = out.get("direzione", "FLAT")
                    if direzione == "FLAT": continue
                    prezzo_entrata = float(engine.get("prezzo_real", 0))
                    atr = float(engine.get("atr", 0))
                    if prezzo_entrata == 0 or atr == 0: continue
                    distanza_tp = atr * 2.0
                    percentuale_guadagno = (distanza_tp / prezzo_entrata)
                    profitto_dollari = SIZE_PER_TRADE * percentuale_guadagno

                    if voto not in voti_stats:
                        voti_stats[voto] = {"count": 0, "profitto": 0, "assets": {}}
                    voti_stats[voto]["count"] += 1
                    voti_stats[voto]["profitto"] += profitto_dollari
                    voti_stats[voto]["assets"][asset] = voti_stats[voto]["assets"].get(asset, 0) + 1
                    profitto_totale_generale += profitto_dollari
                except Exception:
                    continue
        print(f"{'VOTO':<8} | {'TRADE':<7} | {'PROFITTO STIMATO':<18} | {'ASSET'}")
        print("-" * 70)
        for v in sorted(voti_stats.keys(), reverse=True):
            stats = voti_stats[v]
            print(f"{v:<8} | {stats['count']:<7} | ${stats['profitto']:>16.2f} | {list(stats['assets'].keys())[:3]}")
        print("-" * 70)
        print(f"💰 PROFITTO TEORICO TOTALE (SULLE VINCITE): ${profitto_totale_generale:.2f}")
        print(f"⚠️ Nota: Il profitto non include le perdite (SL), serve a misurare la potenzialità del voto.")
    except FileNotFoundError:
        print(f"❌ File {path} non trovato.")

def calcola_bilancio_reale(path='report_reale.json'):
    """
    Calcola il bilancio netto reale dal log dei trade chiusi.
    """
    totale_netto = 0
    wins = 0
    losses = 0
    stats_monete = {}

    print("\n💰 BILANCIO DETTAGLIATO KRAKEN (Netto Commissioni)")
    print("==================================================")
    try:
        with open(path, "r") as f:
            for line in f:
                data = json.loads(line)
                asset = data.get("asset", "N/D")
                pnl = data.get("pnl_netto", data.get("pnl_dollari", 0))
                totale_netto += pnl
                if asset not in stats_monete:
                    stats_monete[asset] = {"pnl": 0, "trades": 0}
                stats_monete[asset]["pnl"] += pnl
                stats_monete[asset]["trades"] += 1
                if pnl > 0: wins += 1
                else: losses += 1
        totali = wins + losses
        win_rate = (wins / totali * 100) if totali > 0 else 0
        print(f"{'ASSET':<10} | {'TRADES':<8} | {'PNL NETTO'}")
        print("-" * 40)
        for moneta, info in stats_monete.items():
            colore = "+" if info['pnl'] >= 0 else ""
            print(f"{moneta:<10} | {info['trades']:<8} | {colore}${info['pnl']:.2f}")
        print("=" * 40)
        print(f"Trade Chiusi: {totali} ({wins}✅ / {losses}❌)")
        print(f"Win Rate:     {win_rate:.2f}%")
        print(f"BILANCIO NETTO FINALE: ${totale_netto:.2f}")
        print("==================================================")
    except FileNotFoundError:
        print(f"❌ File '{path}' non trovato.")
    except Exception as e:
        print(f"⚠️ Errore durante il calcolo: {e}")

def analizza_log(path='sniper_history.log'):
    """
    Analisi performance dal log del bot.
    """
    trades = []
    try:
        with open(path, 'r') as f:
            for line in f:
                if "POSIZIONE CHIUSA" in line or "PnL:" in line:
                    match = re.search(r'(\w+)\s*\|\s*PnL:\s*([-\d.]+)%\s*\(\$?([-\d.]+)\)', line)
                    if match:
                        asset, pnl_perc, pnl_usd = match.groups()
                        trades.append({
                            'asset': asset,
                            'pnl_perc': float(pnl_perc),
                            'pnl_usd': float(pnl_usd),
                            'timestamp': line.split('|')[0].strip()
                        })
    except Exception as e:
        print(f"Errore lettura log: {e}")
    if not trades:
        print("❌ Nessun trade trovato nel log!")
        return
    print("\n" + "="*60)
    print("📊 ANALISI PERFORMANCE BOT")
    print("="*60)
    wins = len([t for t in trades if t['pnl_perc'] > 0])
    losses = len([t for t in trades if t['pnl_perc'] < 0])
    total = len(trades)
    win_rate = (wins / total * 100) if total > 0 else 0
    print(f"\n🏆 WIN RATE")
    print(f"   Vinti: {wins}/{total} ({win_rate:.1f}%)")
    print(f"   Persi: {losses}/{total}")
    total_pnl = sum(t['pnl_usd'] for t in trades)
    print(f"\n💰 PnL TOTALE: ${total_pnl:.2f}")
    asset_stats = defaultdict(lambda: {'pnl': 0, 'count': 0})
    for trade in trades:
        asset_stats[trade['asset']]['pnl'] += trade['pnl_usd']
        asset_stats[trade['asset']]['count'] += 1
    print(f"\n📈 MIGLIORI ASSET:")
    for asset, stats in sorted(asset_stats.items(), key=lambda x: x[1]['pnl'], reverse=True)[:3]:
        print(f"   {asset}: ${stats['pnl']:.2f} ({stats['count']} trade)")
    print(f"\n📉 PEGGIORI ASSET:")
    for asset, stats in sorted(asset_stats.items(), key=lambda x: x[1]['pnl'])[:3]:
        print(f"   {asset}: ${stats['pnl']:.2f} ({stats['count']} trade)")

def main():
    parser = argparse.ArgumentParser(description="Strumenti di analisi/report L&A Bot")
    parser.add_argument('--scatola', action='store_true', help="Analisi avanzata scatola_nera.json")
    parser.add_argument('--bilancio', action='store_true', help="Calcolo bilancio reale da report_reale.json")
    parser.add_argument('--log', action='store_true', help="Analisi performance da sniper_history.log")
    args = parser.parse_args()
    if args.scatola:
        analizza_scatola_nera()
    if args.bilancio:
        calcola_bilancio_reale()
    if args.log:
        analizza_log()
    if not (args.scatola or args.bilancio or args.log):
        parser.print_help()

if __name__ == "__main__":
    main()