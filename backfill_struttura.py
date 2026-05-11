#!/usr/bin/env python3
"""
CHIMERA — Backfill feature strutturali del ciclo

Ricalcola retroattivamente le 4 nuove feature strutturali per tutti i trade
nel DB che non le hanno ancora nel chimera_snapshot.

Feature aggiunte:
  - ciclo_recupero_pct   (0-100)
  - minimo_qualita       (CAPITOLAZIONE / SIGNIFICATIVO / DEBOLE)
  - sr_flip_tipo         (RESISTENZA_EX_SUPPORTO / SUPPORTO_EX_RESISTENZA / '')
  - ciclo_fase           (FONDO / RECUPERO_INIZIALE / RECUPERO_MEDIO / VICINO_MASSIMO / MASSIMO)

Uso:
  cd /path/to/bot
  python3 backfill_struttura.py

Requisiti: ccxt, pandas installati (sono già nel venv del bot)
"""

import sqlite3
import json
import time
import sys
import os
from datetime import datetime, timezone
from collections import defaultdict

# ── Configurazione ────────────────────────────────────────────────────────────
# Modifica questo path se necessario
DB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'chimera.db')
DRY_RUN = False   # True = mostra cosa farebbe senza modificare il DB
SLEEP_BETWEEN_ASSETS = 1.5  # secondi tra fetch Kraken (evita rate limit)

# ── Setup exchange ────────────────────────────────────────────────────────────
def get_exchange():
    try:
        import ccxt
        exchange = ccxt.kraken({'enableRateLimit': True})
        exchange.load_markets()
        print("✅ Connesso a Kraken")
        return exchange
    except Exception as e:
        print(f"❌ Errore connessione Kraken: {e}")
        sys.exit(1)

# ── Mapping ticker CHIMERA → simbolo Kraken CCXT ─────────────────────────────
def ticker_to_ccxt(ticker):
    mapping = {
        'XXBTZUSD': 'BTC/USD',
        'XETHZUSD': 'ETH/USD',
        'XXRPZUSD': 'XRP/USD',
        'XDGUSD':   'DOGE/USD',
        'SOLUSD':   'SOL/USD',
        'XZECZUSD': 'ZEC/USD',
        'BONKUSD':  'BONK/USD',
        'RAVEUSD':  'RAVE/USD',
        'FETUSD':   'FET/USD',
        'POLUSD':   'POL/USD',
        'AAVEUSD':  'AAVE/USD',
        'ADAUSD':   'ADA/USD',
        'ATOMUSD':  'ATOM/USD',
        'AVAXUSD':  'AVAX/USD',
        'DOTUSD':   'DOT/USD',
        'HYPEUSD':  'HYPE/USD',
        'LINKUSD':  'LINK/USD',
        'NEARUSD':  'NEAR/USD',
        'PAXGUSD':  'PAXG/USD',
        'SUIUSD':   'SUI/USD',
        'TAOUSD':   'TAO/USD',
    }
    return mapping.get(ticker, ticker.replace('X', '', 1).replace('Z', '', 1))

# ── Scarica OHLCV daily fino alla data del trade ──────────────────────────────
_ohlcv_cache = {}

def get_ohlcv_daily(exchange, ticker, ts_trade, limit=90):
    """
    Scarica le ultime `limit` candele daily PRIMA di ts_trade.
    Usa cache per evitare fetch ripetuti dello stesso asset nello stesso giorno.
    """
    symbol = ticker_to_ccxt(ticker)
    # Arrotonda al giorno per il caching
    day_key = f"{ticker}_{str(ts_trade)[:10]}"
    if day_key in _ohlcv_cache:
        return _ohlcv_cache[day_key]

    try:
        # Kraken usa '1440' per daily
        ohlcv = exchange.fetch_ohlcv(symbol, timeframe='1440', limit=limit + 5)
        if not ohlcv:
            return None

        import pandas as pd
        df = pd.DataFrame(ohlcv, columns=['ts','open','high','low','close','volume'])
        for col in ['open','high','low','close','volume']:
            df[col] = pd.to_numeric(df[col], errors='coerce')
        df = df.dropna().reset_index(drop=True)

        # Filtra solo le candele PRIMA della data del trade
        ts_ms = ts_trade * 1000 if ts_trade < 1e12 else ts_trade
        df = df[df['ts'] < ts_ms].tail(limit)

        if len(df) < 20:
            return None

        _ohlcv_cache[day_key] = df
        return df

    except Exception as e:
        print(f"  ⚠️ Errore fetch {symbol}: {e}")
        return None

# ── Calcola le 4 feature strutturali ─────────────────────────────────────────
def calcola_struttura(df, prezzo, sr_resistenze=None, sr_supporti=None):
    """
    Calcola le 4 feature strutturali del ciclo dalla OHLCV daily.
    Stessa logica di engine_la._analisi_strutturale_ciclo
    """
    import numpy as np

    result = {
        'ciclo_fase':          'SCONOSCIUTO',
        'ciclo_recupero_pct':  50.0,
        'minimo_qualita':      'NORMALE',
        'minimo_volume_ratio': 1.0,
        'sr_flip_detected':    False,
        'sr_flip_tipo':        '',
        'sr_flip_livello':     0.0,
    }

    try:
        min_90g = float(df['low'].min())
        max_90g = float(df['high'].max())
        range_90g = max_90g - min_90g

        if range_90g <= 0:
            return result

        recupero_pct = (prezzo - min_90g) / range_90g * 100

        if recupero_pct < 25:
            ciclo_fase = 'FONDO'
        elif recupero_pct < 45:
            ciclo_fase = 'RECUPERO_INIZIALE'
        elif recupero_pct < 65:
            ciclo_fase = 'RECUPERO_MEDIO'
        elif recupero_pct < 85:
            ciclo_fase = 'VICINO_MASSIMO'
        else:
            ciclo_fase = 'MASSIMO'

        # Qualità del minimo
        idx_min = df['low'].idxmin()
        vol_al_minimo = float(df['volume'].iloc[idx_min])
        vol_medio = float(df['volume'].mean())
        vol_ratio = vol_al_minimo / vol_medio if vol_medio > 0 else 1.0

        if vol_ratio >= 3.0:
            minimo_qualita = 'CAPITOLAZIONE'
        elif vol_ratio >= 1.5:
            minimo_qualita = 'SIGNIFICATIVO'
        else:
            minimo_qualita = 'DEBOLE'

        # S/R flip
        sr_flip = False
        sr_flip_tipo = ''
        sr_flip_livello = 0.0

        for livello_list, flip_tipo in [
            (sr_resistenze or [], 'RESISTENZA_EX_SUPPORTO'),
            (sr_supporti or [], 'SUPPORTO_EX_RESISTENZA')
        ]:
            for item in livello_list:
                livello = item if isinstance(item, float) else item.get('prezzo', 0)
                if livello <= 0:
                    continue
                dist_pct = abs(prezzo - livello) / prezzo * 100
                if dist_pct <= 5.0:
                    # Verifica presenza storica del livello
                    if flip_tipo == 'RESISTENZA_EX_SUPPORTO':
                        vicini = df[(df['low'] >= livello * 0.97) & (df['low'] <= livello * 1.03)]
                    else:
                        vicini = df[(df['high'] >= livello * 0.97) & (df['high'] <= livello * 1.03)]
                    if len(vicini) >= 2:
                        sr_flip = True
                        sr_flip_tipo = flip_tipo
                        sr_flip_livello = livello
                        break
            if sr_flip:
                break

        result.update({
            'ciclo_fase':          ciclo_fase,
            'ciclo_recupero_pct':  round(recupero_pct, 1),
            'minimo_qualita':      minimo_qualita,
            'minimo_volume_ratio': round(vol_ratio, 2),
            'sr_flip_detected':    sr_flip,
            'sr_flip_tipo':        sr_flip_tipo,
            'sr_flip_livello':     round(sr_flip_livello, 4),
        })

    except Exception as e:
        print(f"  ⚠️ Errore calcolo struttura: {e}")

    return result

# ── Main backfill ─────────────────────────────────────────────────────────────
def main():
    print(f"\n{'='*60}")
    print(f"CHIMERA — Backfill feature strutturali")
    print(f"DB: {DB_PATH}")
    print(f"DRY_RUN: {DRY_RUN}")
    print(f"{'='*60}\n")

    conn = sqlite3.connect(DB_PATH)
    rows = conn.execute("SELECT rowid, data FROM storico_trades ORDER BY rowid ASC").fetchall()

    exchange = get_exchange()

    # Raggruppa trade per asset per ottimizzare i fetch
    by_asset = defaultdict(list)
    for rowid, raw in rows:
        t = json.loads(raw)
        esito = t.get('esito') or t.get('risultato') or ''
        if esito not in ('WIN', 'LOSS'):
            continue
        if t.get('fonte') in ('STORICO_SIMULATO', 'VIRTUAL_BRAIN'):
            continue
        # Salta trade che hanno già le feature strutturali
        snap = t.get('chimera_snapshot', {})
        if isinstance(snap, str):
            try:
                snap = json.loads(snap)
            except:
                snap = {}
        if snap.get('ciclo_fase') and snap.get('ciclo_fase') != 'SCONOSCIUTO':
            continue
        by_asset[t.get('asset', '?')].append((rowid, t))

    totale = sum(len(v) for v in by_asset.values())
    print(f"Trade da aggiornare: {totale}\n")

    aggiornati = 0
    errori = 0

    for asset, trade_list in sorted(by_asset.items()):
        print(f"📊 {asset} ({len(trade_list)} trade)...")
        last_fetch_day = None

        for rowid, t in trade_list:
            # Estrai timestamp apertura
            ts_raw = t.get('ts_apertura') or t.get('data_apertura') or ''
            try:
                if isinstance(ts_raw, (int, float)) and ts_raw > 0:
                    ts = float(ts_raw)
                    if ts > 1e12:
                        ts = ts / 1000
                elif isinstance(ts_raw, str) and ts_raw:
                    from datetime import datetime
                    dt = datetime.fromisoformat(ts_raw.replace('Z', '+00:00'))
                    ts = dt.timestamp()
                else:
                    ts = time.time() - 86400 * 30  # fallback 30 giorni fa
            except Exception:
                ts = time.time() - 86400 * 30

            # Fetch OHLCV solo se siamo in un giorno diverso dall'ultimo
            current_day = str(datetime.fromtimestamp(ts, tz=timezone.utc))[:10]
            if current_day != last_fetch_day:
                df = get_ohlcv_daily(exchange, asset, ts)
                last_fetch_day = current_day
                if df is None:
                    print(f"  ⚠️ Nessun dato OHLCV per {asset} @ {current_day}")
                    errori += 1
                    continue
                time.sleep(SLEEP_BETWEEN_ASSETS)

            if df is None:
                errori += 1
                continue

            # Prendi S/R dal snapshot se disponibili
            snap = t.get('chimera_snapshot', {})
            if isinstance(snap, str):
                try:
                    snap = json.loads(snap)
                except:
                    snap = {}

            prezzo = float(t.get('p_entrata', 0) or t.get('prezzo_entrata', 0) or snap.get('close', 0) or 0)
            if prezzo <= 0:
                prezzo = float(df['close'].iloc[-1])

            sr_res = snap.get('sr_resistenze', [])
            sr_sup = snap.get('sr_supporti', [])

            # Calcola le feature strutturali
            struttura = calcola_struttura(df, prezzo, sr_res, sr_sup)

            # Aggiorna snapshot nel trade
            snap.update(struttura)
            t['chimera_snapshot'] = snap

            if not DRY_RUN:
                conn.execute(
                    "UPDATE storico_trades SET data = ? WHERE rowid = ?",
                    (json.dumps(t), rowid)
                )
                aggiornati += 1
            else:
                print(f"  [DRY] {asset} @ {current_day}: ciclo={struttura['ciclo_fase']} "
                      f"min={struttura['minimo_qualita']} flip={struttura['sr_flip_tipo'] or 'no'}")
                aggiornati += 1

        if not DRY_RUN:
            conn.commit()

    conn.close()

    print(f"\n{'='*60}")
    print(f"✅ Completato: {aggiornati} trade aggiornati, {errori} errori")
    print(f"{'='*60}")
    print("\nAdesso puoi fare retrain XGBoost dal bot con:")
    print("  python3 bot_la.py  # il modello si riadatta automaticamente")

if __name__ == '__main__':
    main()
