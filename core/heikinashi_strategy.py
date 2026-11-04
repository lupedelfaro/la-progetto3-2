# -*- coding: utf-8 -*-
"""
CHIMERA v4.0 — HeikinAshiStrategy
Strategia SPOT su BTC basata su candele Heikin Ashi daily.

Filosofia:
  Le candele Heikin Ashi filtrano il noise intraday e mostrano il trend
  di fondo in modo pulito. Un cambio di colore HA su un livello S/R storico
  importante è un segnale di inversione ad alta probabilità.

  Questa strategia opera SOLO su XXBTZUSD in modalità SPOT (nessuna leva).
  Tiene la posizione finché il trailing SL non viene colpito.

Logica completa:

  IDENTIFICAZIONE S/R STORICI:
    1. Recupera il massimo storico di candele daily OHLCV da Kraken
    2. Calcola le candele Heikin Ashi su quelle OHLCV reali
    3. Identifica tutti i pivot (cambi di colore HA) — ogni pivot è un
       potenziale livello S/R
    4. Raggruppa i pivot vicini (±0.5 ATR) in cluster — più pivot nello
       stesso cluster = S/R più forte (forza 1-10)
    5. Aggiunge livelli narrativi storici BTC (ATH, bottom cicli,
       psicologici, Fibonacci, halving) con forza proporzionale
    6. Filtra per forza minima

  CONDIZIONE ENTRY:
    - Serie di N candele HA rosse → candela HA che chiude VERDE = LONG
    - Serie di N candele HA verdi → candela HA che chiude ROSSA = SHORT
    - La chiusura deve essere dentro ±1 ATR da un S/R con forza >= soglia
    - Entry: al prezzo di chiusura della candela HA che ha cambiato colore

  SL INIZIALE (solo al momento dell'entry):
    - Usa la candela GIAPPONESE reale (non HA) del giorno di entry
    - LONG:  SL = minimo reale della candela giapponese (incluso shadow)
    - SHORT: SL = massimo reale della candela giapponese (incluso shadow)

  SL TRAILING (ogni giorno alla chiusura della candela HA):
    - LONG:  SL → minimo della candela HA appena chiusa
    - SHORT: SL → massimo della candela HA appena chiusa
    - Lo SL si muove SOLO in direzione favorevole (trailing, mai indietro)
    - Implementato come Virtual SL

  TP: nessuno — trailing SL gestisce l'uscita

  POSIZIONE: SPOT only (leva=1), solo XXBTZUSD

Integrazione in strategy_engine.py:
    from core.heikinashi_strategy import HeikinAshiStrategy
    # In StrategyEngine.__init__, aggiungere a self.strategies:
    HeikinAshiStrategy(engine),
"""

import logging
import sys
import time
from typing import Dict, List, Optional
from datetime import datetime

from core.chimera_errors import ErrorTracker
from core.strategy_engine import BaseStrategy, StrategySignal

_err   = ErrorTracker("HeikinAshiStrategy")
logger = logging.getLogger("HeikinAshiStrategy")

# ══════════════════════════════════════════════════════════════════════════════
#  ASSET
# ══════════════════════════════════════════════════════════════════════════════

TICKER_BTC = "XXBTZUSD"

# ══════════════════════════════════════════════════════════════════════════════
#  LIVELLI NARRATIVI STORICI BTC
#  Fonte: storia pubblica di BTC — ATH, bottom, psicologici, halving, Fibonacci
#  Forza: 1-10 (10 = livello più importante storicamente)
# ══════════════════════════════════════════════════════════════════════════════

BTC_NARRATIVE_LEVELS: Dict[float, Dict] = {
    # ── Bottom cicli bear market ─────────────────────────────────────────────
    3_122.0:  {"nome": "BOTTOM_2018",        "forza": 9,
               "desc": "Bottom ciclo bear 2018 (-84% da ATH 2017)"},
    3_800.0:  {"nome": "BOTTOM_2020_COVID",  "forza": 8,
               "desc": "Bottom crash Covid marzo 2020 — flash crash storico"},
    15_476.0: {"nome": "BOTTOM_2022_FTX",    "forza": 10,
               "desc": "Bottom FTX collapse nov 2022 — bottom strutturale ciclo 2022"},

    # ── ATH storici ──────────────────────────────────────────────────────────
    19_891.0: {"nome": "ATH_2017",           "forza": 9,
               "desc": "ATH ciclo 2017 — poi supporto chiave 2020-2021"},
    64_895.0: {"nome": "ATH_2021_APR",       "forza": 8,
               "desc": "Primo ATH 2021 (aprile) — poi supporto/resistenza"},
    69_000.0: {"nome": "ATH_2021_NOV",       "forza": 10,
               "desc": "ATH ciclo 2021 (novembre) — resistenza storica primaria"},
    73_835.0: {"nome": "ATH_2024_MAR",       "forza": 9,
               "desc": "ATH marzo 2024 pre-halving — massimo attuale"},

    # ── Livelli psicologici rotondi ──────────────────────────────────────────
    10_000.0: {"nome": "PSY_10K",            "forza": 7,
               "desc": "10k psicologico — supporto/resistenza 2019-2020"},
    20_000.0: {"nome": "PSY_20K",            "forza": 8,
               "desc": "20k = ATH 2017 riconquistato, poi supporto 2022"},
    30_000.0: {"nome": "PSY_30K",            "forza": 7,
               "desc": "30k — supporto/resistenza multiplo 2021-2022"},
    40_000.0: {"nome": "PSY_40K",            "forza": 6,
               "desc": "40k — zona consolidamento metà 2021"},
    50_000.0: {"nome": "PSY_50K",            "forza": 7,
               "desc": "50k — supporto istituzionale 2021 e 2024"},
    60_000.0: {"nome": "PSY_60K",            "forza": 7,
               "desc": "60k — zona resistenza/breakout 2021 e 2024"},
    70_000.0: {"nome": "PSY_70K",            "forza": 6,
               "desc": "70k — zona resistenza 2024"},
    100_000.0:{"nome": "PSY_100K",           "forza": 8,
               "desc": "100k — livello psicologico epocale non ancora raggiunto"},

    # ── Livelli strutturali da eventi chiave ─────────────────────────────────
    14_000.0: {"nome": "SR_2019_HIGH",       "forza": 6,
               "desc": "Massimo ciclo 2019 — poi resistenza a lungo termine"},
    26_500.0: {"nome": "SR_PRE_HALVING_2024","forza": 7,
               "desc": "Consolidamento pre-halving 2024 (oltre 6 mesi di ranging)"},
    29_000.0: {"nome": "SR_POST_HALVING_2020","forza": 7,
               "desc": "Breakout post-halving 2020 — inizio nuovo ciclo rialzista"},
    35_000.0: {"nome": "SR_INSTITUTIONAL",   "forza": 7,
               "desc": "Zona accumulazione istituzionale 2023 (BlackRock, Fidelity)"},
    42_000.0: {"nome": "SR_SPOT_ETF",        "forza": 8,
               "desc": "Livello pre/post approvazione ETF spot gennaio 2024"},
    47_000.0: {"nome": "SR_LUNA_CRASH",      "forza": 7,
               "desc": "Supporto rotto nel crash LUNA/UST maggio 2022"},
    52_000.0: {"nome": "SR_2024_Q1",         "forza": 7,
               "desc": "Supporto chiave Q1 2024 prima del rally verso ATH"},
    58_000.0: {"nome": "SR_2021_BREAKOUT",   "forza": 8,
               "desc": "Breakout strutturale 2021 — poi supporto nel 2024"},
    63_000.0: {"nome": "SR_HALVING_2024",    "forza": 8,
               "desc": "Prezzo al momento esatto del halving aprile 2024"},

    # ── Livelli Fibonacci storici ────────────────────────────────────────────
    33_000.0: {"nome": "FIB_786_CICLO2021",  "forza": 6,
               "desc": "0.786 ritracciamento bull run 2021 (0→69k)"},
    42_000.0: {"nome": "FIB_618_CICLO2021",  "forza": 7,
               "desc": "0.618 ritracciamento bull run 2021 (0→69k)"},
    49_000.0: {"nome": "FIB_618_CICLO2024",  "forza": 6,
               "desc": "0.618 da bottom 2022 (15.4k) ad ATH 2024 (73.8k)"},
}

# ══════════════════════════════════════════════════════════════════════════════
#  PARAMETRI
# ══════════════════════════════════════════════════════════════════════════════

OHLCV_LIMIT       = 1000   # candele daily da recuperare (~2.7 anni)
SR_FORZA_MINIMA   = 4      # forza minima per trigger entry
SR_TOLLERANZA_ATR = 1.0    # ±N ATR intorno al livello S/R
MIN_CANDELE_TREND = 2      # candele minime dello stesso colore prima del cambio
SR_CACHE_ORE      = 24     # ricalcola S/R ogni 24h


# ══════════════════════════════════════════════════════════════════════════════
#  FUNZIONI CALCOLO HA
# ══════════════════════════════════════════════════════════════════════════════

def calcola_heikin_ashi(ohlcv: List) -> List[Dict]:
    """
    Calcola candele Heikin Ashi da lista OHLCV.
    Input:  [[ts, open, high, low, close, vol], ...]
    Output: lista dict con ha_open/high/low/close + colore + valori reali
    """
    if not ohlcv:
        return []

    risultati = []
    ha_open_prev = ha_close_prev = None

    for c in ohlcv:
        ts = float(c[0])
        o, h, l, close = float(c[1]), float(c[2]), float(c[3]), float(c[4])

        ha_close = (o + h + l + close) / 4.0
        ha_open  = (o + close) / 2.0 if ha_open_prev is None else (ha_open_prev + ha_close_prev) / 2.0
        ha_high  = max(h, ha_open, ha_close)
        ha_low   = min(l, ha_open, ha_close)

        risultati.append({
            'ts':         ts,
            'ha_open':    ha_open,
            'ha_high':    ha_high,
            'ha_low':     ha_low,
            'ha_close':   ha_close,
            'colore':     'VERDE' if ha_close >= ha_open else 'ROSSO',
            # Valori reali candela giapponese — usati per SL iniziale
            'real_open':  o,
            'real_high':  h,
            'real_low':   l,
            'real_close': close,
        })

        ha_open_prev  = ha_open
        ha_close_prev = ha_close

    return risultati


def calcola_atr_daily(ohlcv: List, period: int = 14) -> float:
    """ATR semplice su N periodi dalle candele OHLCV."""
    if len(ohlcv) < period + 1:
        return float(ohlcv[-1][4]) * 0.02 if ohlcv else 1000.0
    trs = []
    for i in range(1, len(ohlcv)):
        h, l, prev_c = float(ohlcv[i][2]), float(ohlcv[i][3]), float(ohlcv[i-1][4])
        trs.append(max(h - l, abs(h - prev_c), abs(l - prev_c)))
    return sum(trs[-period:]) / period


def identifica_pivot(ha_candles: List[Dict]) -> List[Dict]:
    """
    Trova tutti i cambi di colore HA — ogni cambio è un pivot S/R potenziale.
    ROSSO→VERDE = supporto (fondo, prezzo di riferimento = real_low)
    VERDE→ROSSO = resistenza (top, prezzo di riferimento = real_high)
    """
    pivot = []
    for i in range(1, len(ha_candles)):
        prev, curr = ha_candles[i-1], ha_candles[i]
        if prev['colore'] == 'ROSSO' and curr['colore'] == 'VERDE':
            pivot.append({'prezzo': curr['real_low'],  'tipo': 'SUPPORTO',    'ts': curr['ts']})
        elif prev['colore'] == 'VERDE' and curr['colore'] == 'ROSSO':
            pivot.append({'prezzo': curr['real_high'], 'tipo': 'RESISTENZA',  'ts': curr['ts']})
    return pivot


def calcola_livelli_sr(ha_candles: List[Dict], atr: float) -> Dict[float, Dict]:
    """
    Aggrega pivot in cluster e combina con livelli narrativi.
    Restituisce {prezzo: {forza, tipo, desc, ...}}
    """
    pivot = identifica_pivot(ha_candles)
    tolleranza = atr * 0.5

    # ── Clustering pivot ──────────────────────────────────────────────────────
    usati = [False] * len(pivot)
    clusters = []

    for i, p in enumerate(pivot):
        if usati[i]:
            continue
        gruppo = [p['prezzo']]
        tipi   = [p['tipo']]
        usati[i] = True
        for j in range(i+1, len(pivot)):
            if not usati[j] and abs(pivot[j]['prezzo'] - p['prezzo']) <= tolleranza:
                gruppo.append(pivot[j]['prezzo'])
                tipi.append(pivot[j]['tipo'])
                usati[j] = True
        prezzo = sum(gruppo) / len(gruppo)
        n_sup  = tipi.count('SUPPORTO')
        tipo   = 'SUPPORTO' if n_sup >= len(tipi) / 2 else 'RESISTENZA'
        clusters.append({
            'prezzo':  prezzo,
            'forza':   min(len(gruppo), 10),
            'tipo':    tipo,
            'n_pivot': len(gruppo),
            'desc':    f"{len(gruppo)} pivot HA {tipo}",
        })

    livelli = {cl['prezzo']: cl for cl in clusters}

    # ── Aggiunge/rinforza con narrativi ───────────────────────────────────────
    for prezzo_n, info_n in BTC_NARRATIVE_LEVELS.items():
        vicino = next(
            (p for p in livelli if abs(p - prezzo_n) <= atr),
            None
        )
        if vicino is not None:
            livelli[vicino]['forza'] = min(livelli[vicino]['forza'] + info_n['forza'] // 2, 10)
            livelli[vicino]['desc'] += f" + {info_n['nome']}"
        else:
            livelli[float(prezzo_n)] = {
                'prezzo':  float(prezzo_n),
                'forza':   info_n['forza'],
                'tipo':    'NARRATIVO',
                'n_pivot': 0,
                'desc':    info_n['desc'],
                'nome':    info_n['nome'],
            }

    # ── Filtra per forza minima ────────────────────────────────────────────────
    return {p: v for p, v in livelli.items() if v['forza'] >= SR_FORZA_MINIMA}


def trova_sr_vicino(prezzo: float, livelli: Dict, atr: float,
                    tolleranza_atr: float = SR_TOLLERANZA_ATR) -> Optional[Dict]:
    """Trova il livello S/R più forte entro ±tolleranza_atr * ATR dal prezzo."""
    tol = atr * tolleranza_atr
    candidati = [(p, v) for p, v in livelli.items() if abs(p - prezzo) <= tol]
    if not candidati:
        return None
    return max(candidati, key=lambda x: x[1]['forza'])[1]


# ══════════════════════════════════════════════════════════════════════════════
#  STRATEGIA
# ══════════════════════════════════════════════════════════════════════════════

class HeikinAshiStrategy(BaseStrategy):
    """
    Strategia 7: Heikin Ashi Daily BTC SPOT.
    Opera SOLO su XXBTZUSD, leva=1 (SPOT).
    """

    def __init__(self, engine):
        super().__init__(engine)
        self.name = "HeikinAshiStrategy"
        self._cache_livelli:    Optional[Dict] = None
        self._cache_ha:         Optional[List] = None
        self._cache_ohlcv:      Optional[List] = None
        self._cache_atr:        float = 0.0
        self._cache_ts:         float = 0.0

    def _aggiorna_cache(self, ticker: str) -> bool:
        """Recupera OHLCV, calcola HA e S/R. Cache 24h."""
        if (time.time() - self._cache_ts) < SR_CACHE_ORE * 3600 and self._cache_livelli:
            return True
        try:
            ohlcv = None

            # Priorità 1: get_ohlcv_daily() — metodo nativo engine con retry/backoff
            if hasattr(self.engine, 'get_ohlcv_daily'):
                ohlcv = self.engine.get_ohlcv_daily(ticker, limit=OHLCV_LIMIT)

            # Priorità 2: _safe_fetch dell'engine (stesso meccanismo, chiamata diretta)
            if not ohlcv and hasattr(self.engine, '_safe_fetch'):
                try:
                    from core.asset_list import get_human_name
                    sym = get_human_name(ticker)
                    ohlcv = self.engine._safe_fetch('fetch_ohlcv', sym, timeframe='1d', limit=OHLCV_LIMIT)
                    if not ohlcv:
                        ohlcv = self.engine._safe_fetch('fetch_ohlcv', sym, timeframe='1440', limit=OHLCV_LIMIT)
                except Exception as e_sf:
                    _err.capture(e_sf, "_aggiorna_cache", {"module": "HeikinAshiStrategy"})
                    self.logger.debug(f"⚠️ HeikinAshi _safe_fetch fallback: {e_sf}")

            # Priorità 3: exchange diretto (ultimo resort)
            if not ohlcv and hasattr(self.engine, 'exchange'):
                try:
                    from core.asset_list import get_human_name
                    sym = get_human_name(ticker)
                    ohlcv = self.engine.exchange.fetch_ohlcv(sym, timeframe='1d', limit=OHLCV_LIMIT)
                except Exception as e_ex:
                    _err.capture(e_ex, "_aggiorna_cache", {"module": "HeikinAshiStrategy"})
                    self.logger.debug(f"⚠️ HeikinAshi exchange.fetch_ohlcv fallback: {e_ex}")

            if not ohlcv or len(ohlcv) < 30:
                self.logger.warning(
                    f"⚠️ HeikinAshi: OHLCV daily insufficienti "
                    f"({len(ohlcv) if ohlcv else 0} candele) per {ticker}"
                )
                return False

            atr        = calcola_atr_daily(ohlcv)
            ha_candles = calcola_heikin_ashi(ohlcv)
            livelli    = calcola_livelli_sr(ha_candles, atr)

            self._cache_ohlcv   = ohlcv
            self._cache_ha      = ha_candles
            self._cache_livelli = livelli
            self._cache_atr     = atr
            self._cache_ts      = time.time()

            self.logger.info(
                f"✅ HeikinAshi cache: {len(ha_candles)} candele HA | "
                f"ATR {atr:.0f}$ | {len(livelli)} livelli S/R "
                f"(forza >= {SR_FORZA_MINIMA})"
            )
            return True

        except Exception as e:
            _err.capture(e, "_aggiorna_cache", {"ticker": ticker})
            self.logger.error(f"❌ HeikinAshi cache: {e}")
            return False

    def _check_cambio_colore(self) -> Optional[Dict]:
        """
        Verifica se l'ultima candela HA ha cambiato colore.
        Richiede almeno MIN_CANDELE_TREND candele dello stesso colore prima.
        """
        ha = self._cache_ha
        if not ha or len(ha) < MIN_CANDELE_TREND + 2:
            return None

        ultima    = ha[-1]
        penultima = ha[-2]
        if ultima['colore'] == penultima['colore']:
            return None

        # Conta streak precedente
        colore_prec = penultima['colore']
        streak = 0
        for i in range(len(ha) - 2, max(0, len(ha) - 15), -1):
            if ha[i]['colore'] == colore_prec:
                streak += 1
            else:
                break

        if streak < MIN_CANDELE_TREND:
            return None

        return {
            'direzione':   'LONG' if ultima['colore'] == 'VERDE' else 'SHORT',
            'colore_nuovo': ultima['colore'],
            'colore_prec':  colore_prec,
            'streak':       streak,
            'ha_close':     ultima['ha_close'],
            'ha_low':       ultima['ha_low'],
            'ha_high':      ultima['ha_high'],
            # Candela giapponese REALE per SL iniziale
            'real_high':    ultima['real_high'],
            'real_low':     ultima['real_low'],
            'real_close':   ultima['real_close'],
        }

    def analyze(self, ticker: str, dati_engine: Dict) -> Optional[StrategySignal]:
        """Entry point chiamato dal StrategyEngine ad ogni ciclo."""
        try:
            # Solo BTC
            if ticker != TICKER_BTC:
                return None

            if not self._aggiorna_cache(ticker):
                return None

            cambio = self._check_cambio_colore()
            if not cambio:
                return None

            prezzo_entry = cambio['ha_close']
            if prezzo_entry <= 0:
                return None

            # Cerca S/R vicino
            sr = trova_sr_vicino(prezzo_entry, self._cache_livelli, self._cache_atr)
            if not sr:
                self.logger.debug(
                    f"ℹ️ HeikinAshi: cambio {cambio['colore_prec']}→{cambio['colore_nuovo']} "
                    f"a {prezzo_entry:.0f}$ — nessun S/R entro {self._cache_atr:.0f}$"
                )
                return None

            # Precisione prezzi BTC
            try:
                from core.asset_list import get_config as _gc
                _prec = int(_gc(ticker).get('precision', 1) or 1)
            except Exception:
                _prec = 1

            # ── SL iniziale su candela GIAPPONESE reale ──────────────────────
            if cambio['direzione'] == 'LONG':
                sl = round(cambio['real_low'], _prec)
                # Piccolo buffer per evitare stop hunt esatti
                sl = round(sl * 0.9995, _prec)
                if sl >= prezzo_entry:
                    sl = round(prezzo_entry * 0.985, _prec)
                # TP simbolico alto — non verrà colpito, gestito da trailing SL
                tp = round(prezzo_entry * 1.20, _prec)
            else:  # SHORT
                sl = round(cambio['real_high'], _prec)
                sl = round(sl * 1.0005, _prec)
                if sl <= prezzo_entry:
                    sl = round(prezzo_entry * 1.015, _prec)
                tp = round(prezzo_entry * 0.80, _prec)

            # ── Score ────────────────────────────────────────────────────────
            score = 50
            score += min(sr['forza'] * 4, 40)      # forza S/R (0-40)
            score += min(cambio['streak'] * 2, 10)  # consistenza trend (0-10)
            score = min(int(score), 100)

            razionale = (
                f"HeikinAshi {cambio['colore_prec']}→{cambio['colore_nuovo']} | "
                f"S/R: {sr.get('nome', sr.get('tipo','?'))} "
                f"@ {sr.get('prezzo',0):.0f}$ (forza {sr['forza']}/10) | "
                f"Streak: {cambio['streak']} candele {cambio['colore_prec']} | "
                f"SL reale: {cambio['real_low']:.0f}$ | "
                f"ATR daily: {self._cache_atr:.0f}$"
            )

            self.logger.info(
                f"🕯️ HeikinAshi SEGNALE {cambio['direzione']} BTC | "
                f"Entry {prezzo_entry:.0f}$ | SL {sl:.0f}$ | "
                f"S/R {sr.get('nome','?')} forza {sr['forza']}/10 | Score {score}"
            )

            return StrategySignal(
                signal        = cambio['direzione'],
                score         = float(score),
                confidence    = round(sr['forza'] / 10.0, 2),
                entry_price   = round(prezzo_entry, _prec),
                sl            = sl,
                tp            = tp,
                sizing        = 1.0,   # SPOT: usa il capitale disponibile
                razionale     = razionale,
                strategy_name = f"HeikinAshi_{cambio['direzione']}",
                components    = {
                    'cambio':             cambio,
                    'sr_vicino':          sr,
                    'atr_daily':          round(self._cache_atr, 0),
                    'n_livelli_sr':       len(self._cache_livelli),
                    # Flag per bot_la e trade_manager
                    'spot_only':          True,
                    'leva':               1,
                    'trailing_ha_daily':  True,   # usa trailing SL su HA daily
                    'sl_real_candle':     True,    # SL iniziale su candela reale
                    'virtual_sl':         True,    # gestito come virtual SL
                }
            )

        except Exception as e:
            _err.capture(e, sys._getframe().f_code.co_name, {"ticker": ticker})
            self.logger.error(f"❌ HeikinAshiStrategy: {e}")
            return None

    # ══════════════════════════════════════════════════════════════════════════
    #  TRAILING SL GIORNALIERO
    #  Chiamare ogni giorno alla chiusura della candela daily (00:00 UTC)
    # ══════════════════════════════════════════════════════════════════════════

    def aggiorna_trailing_sl(self, direzione: str, sl_attuale: float) -> Optional[float]:
        """
        Calcola il nuovo SL trailing basato sull'ultima candela HA chiusa.

        Args:
            direzione:  'LONG' o 'SHORT'
            sl_attuale: SL corrente in memoria

        Returns:
            Nuovo SL se si deve spostare (solo in direzione favorevole),
            None se lo SL rimane invariato.

        Logica:
            LONG:  nuovo_sl = ha_low ultima candela → sposta se > sl_attuale
            SHORT: nuovo_sl = ha_high ultima candela → sposta se < sl_attuale
        """
        try:
            if not self._cache_ha or len(self._cache_ha) < 2:
                return None

            # Aggiorna cache se necessario
            self._aggiorna_cache(TICKER_BTC)

            ultima_ha = self._cache_ha[-1]

            if direzione == 'LONG':
                nuovo_sl = ultima_ha['ha_low']
                if nuovo_sl > sl_attuale:
                    self.logger.info(
                        f"📈 HeikinAshi trailing SL LONG: "
                        f"{sl_attuale:.0f}$ → {nuovo_sl:.0f}$ "
                        f"(HA_low {datetime.fromtimestamp(ultima_ha['ts']).strftime('%d/%m')})"
                    )
                    return round(nuovo_sl, 1)

            else:  # SHORT
                nuovo_sl = ultima_ha['ha_high']
                if sl_attuale == 0 or nuovo_sl < sl_attuale:
                    self.logger.info(
                        f"📉 HeikinAshi trailing SL SHORT: "
                        f"{sl_attuale:.0f}$ → {nuovo_sl:.0f}$ "
                        f"(HA_high {datetime.fromtimestamp(ultima_ha['ts']).strftime('%d/%m')})"
                    )
                    return round(nuovo_sl, 1)

            return None

        except Exception as e:
            _err.capture(e, "aggiorna_trailing_sl", {"direzione": direzione})
            self.logger.error(f"❌ HeikinAshi trailing SL: {e}")
            return None

    # ══════════════════════════════════════════════════════════════════════════
    #  DEBUG / MONITORING
    # ══════════════════════════════════════════════════════════════════════════

    def get_stato(self) -> Dict:
        """Stato corrente per debug e Telegram."""
        if not self._cache_livelli or not self._cache_ha:
            return {"stato": "cache_vuota — attendere prossimo ciclo"}

        ultima    = self._cache_ha[-1]
        penultima = self._cache_ha[-2] if len(self._cache_ha) >= 2 else {}

        top5 = sorted(
            self._cache_livelli.items(),
            key=lambda x: x[1]['forza'],
            reverse=True
        )[:5]

        return {
            "stato":          "attiva",
            "n_ha_candles":   len(self._cache_ha),
            "atr_daily_usd":  round(self._cache_atr, 0),
            "n_livelli_sr":   len(self._cache_livelli),
            "ultima_candela_ha": {
                "colore":   ultima.get('colore'),
                "ha_close": round(ultima.get('ha_close', 0), 0),
                "ha_low":   round(ultima.get('ha_low', 0), 0),
                "ha_high":  round(ultima.get('ha_high', 0), 0),
                "real_low": round(ultima.get('real_low', 0), 0),
                "real_high":round(ultima.get('real_high', 0), 0),
            },
            "penultima_colore":  penultima.get('colore'),
            "cambio_colore_oggi": ultima.get('colore') != penultima.get('colore'),
            "cache_ore_fa":   round((time.time() - self._cache_ts) / 3600, 1),
            "top5_livelli_sr": [
                {
                    "prezzo": round(p, 0),
                    "forza":  v['forza'],
                    "label":  v.get('nome', v.get('tipo', '')),
                    "desc":   v.get('desc', '')[:60],
                }
                for p, v in top5
            ],
        }


# ══════════════════════════════════════════════════════════════════════════════
#  HEIKIN ASHI TREND STRATEGY — MARGINE 1H
#
#  Logica:
#    1. Candela HA daily determina il trend → VERDE = bias LONG, ROSSA = bias SHORT
#    2. Su 1H aspettiamo ritracciamento contro trend daily
#       (almeno MIN_RITRACCIAMENTO candele HA 1H del colore opposto)
#    3. Alla chiusura della prima candela HA 1H che torna nella direzione daily → ENTRY
#    4. SL: Virtual SL 1% dall'entry (nascosto)
#    5. TP: 2% fisso su Kraken (R:R 1:2)
#    6. Gestione BE/scaling: gestisci_protezione_istituzionale() esistente
#    7. Leva: 10x (max Kraken BTC/USD)
#
#  Integrazione in bot_la.py:
#    ha_trend_strategy = HeikinAshiTrendStrategy(engine, ha_strategy)
#    Nel loop principale (ogni ciclo su XXBTZUSD):
#    segnale = ha_trend_strategy.analyze("XXBTZUSD", dati_engine)
# ══════════════════════════════════════════════════════════════════════════════

HA_TREND_LEVA           = 10     # max Kraken BTC/USD
HA_TREND_TP_PCT         = 0.02   # 2% TP fisso
HA_TREND_SL_PCT         = 0.01   # 1% SL virtual
HA_TREND_MIN_RITRAC     = 2      # candele HA 1H minime di ritracciamento
HA_TREND_OHLCV_1H_LIMIT = 200    # candele 1H da recuperare
HA_TREND_CACHE_MIN      = 15     # ricalcola cache 1H ogni 15 minuti


class HeikinAshiTrendStrategy(BaseStrategy):
    """
    Strategia 8: Heikin Ashi In Trend — MARGINE 1H BTC.

    Entra a margine (10x) su ritracciamento 1H nella direzione del trend daily HA.
    SL virtual 1%, TP fisso 2%, gestione BE/scaling tramite trade_manager esistente.
    """

    def __init__(self, engine, ha_daily: 'HeikinAshiStrategy'):
        """
        Args:
            engine:    engine_la istanza
            ha_daily:  HeikinAshiStrategy istanza — fornisce il colore daily HA
        """
        super().__init__(engine)
        self.name     = "HeikinAshiTrendStrategy"
        self.ha_daily = ha_daily   # riferimento alla strategia daily per il colore

        # Cache 1H
        self._cache_ha_1h: Optional[List] = None
        self._cache_ts_1h: float = 0.0

    # ── Cache candele HA 1H ───────────────────────────────────────────────────

    def _aggiorna_cache_1h(self, ticker: str) -> bool:
        """Recupera OHLCV 1H, calcola HA. Cache 15 minuti."""
        if (time.time() - self._cache_ts_1h) < HA_TREND_CACHE_MIN * 60 and self._cache_ha_1h:
            return True
        try:
            ohlcv_1h = None

            # Priorità 1: _safe_fetch con retry
            if hasattr(self.engine, '_safe_fetch'):
                try:
                    from core.asset_list import get_human_name
                    sym = get_human_name(ticker)
                    ohlcv_1h = self.engine._safe_fetch(
                        'fetch_ohlcv', sym, timeframe='1h',
                        limit=HA_TREND_OHLCV_1H_LIMIT
                    )
                except Exception as e_sf:
                    _err.capture(e_sf, "_aggiorna_cache_1h", {"module": "HeikinAshiStrategy"})
                    self.logger.debug(f"⚠️ HA Trend _safe_fetch 1h: {e_sf}")

            # Priorità 2: exchange diretto
            if not ohlcv_1h and hasattr(self.engine, 'exchange'):
                try:
                    from core.asset_list import get_human_name
                    sym = get_human_name(ticker)
                    ohlcv_1h = self.engine.exchange.fetch_ohlcv(
                        sym, timeframe='1h', limit=HA_TREND_OHLCV_1H_LIMIT
                    )
                except Exception as e_ex:
                    _err.capture(e_ex, "_aggiorna_cache_1h", {"module": "HeikinAshiStrategy"})
                    self.logger.debug(f"⚠️ HA Trend exchange 1h: {e_ex}")

            if not ohlcv_1h or len(ohlcv_1h) < 10:
                self.logger.warning(f"⚠️ HA Trend: OHLCV 1H insufficienti per {ticker}")
                return False

            self._cache_ha_1h  = calcola_heikin_ashi(ohlcv_1h)
            self._cache_ts_1h  = time.time()

            self.logger.debug(
                f"🕯️ HA Trend cache 1H: {len(self._cache_ha_1h)} candele HA"
            )
            return True

        except Exception as e:
            _err.capture(e, "_aggiorna_cache_1h", {"ticker": ticker})
            self.logger.error(f"❌ HA Trend cache 1H: {e}")
            return False

    # ── Colore daily da ha_daily ──────────────────────────────────────────────

    def _colore_daily(self) -> Optional[str]:
        """
        Restituisce il colore dell'ultima candela HA daily.
        Usa la cache di HeikinAshiStrategy (condivisa).
        """
        try:
            if not self.ha_daily._cache_ha:
                self.ha_daily._aggiorna_cache(TICKER_BTC)
            if self.ha_daily._cache_ha:
                return self.ha_daily._cache_ha[-1]['colore']
        except Exception as e:
            _err.capture(e, "_colore_daily", {"module": "HeikinAshiStrategy"})
            self.logger.debug(f"⚠️ HA Trend colore daily: {e}")
        return None

    # ── Verifica ritracciamento + cambio 1H ───────────────────────────────────

    def _check_segnale_1h(self, direzione_daily: str) -> Optional[Dict]:
        """
        Verifica il setup su HA 1H:
          1. Almeno MIN_RITRACCIAMENTO candele HA 1H del colore OPPOSTO al daily
          2. Ultima candela HA 1H ha cambiato colore verso la direzione daily

        Args:
            direzione_daily: 'LONG' o 'SHORT'

        Returns:
            Dict con info segnale oppure None
        """
        ha = self._cache_ha_1h
        if not ha or len(ha) < HA_TREND_MIN_RITRAC + 2:
            return None

        ultima    = ha[-1]
        penultima = ha[-2]

        # Colori attesi
        colore_trend    = 'VERDE' if direzione_daily == 'LONG' else 'ROSSO'
        colore_ritrac   = 'ROSSO' if direzione_daily == 'LONG' else 'VERDE'

        # L'ultima candela deve essere del colore del trend daily
        if ultima['colore'] != colore_trend:
            return None

        # La penultima deve essere del colore opposto (ritracciamento)
        if penultima['colore'] != colore_ritrac:
            return None

        # Conta quante candele consecutive di ritracciamento ci sono prima
        n_ritrac = 0
        for i in range(len(ha) - 2, max(0, len(ha) - 20), -1):
            if ha[i]['colore'] == colore_ritrac:
                n_ritrac += 1
            else:
                break

        if n_ritrac < HA_TREND_MIN_RITRAC:
            return None

        return {
            'direzione':      direzione_daily,
            'colore_1h':      ultima['colore'],
            'n_ritrac':       n_ritrac,
            'ha_close':       ultima['ha_close'],
            'ha_low':         ultima['ha_low'],
            'ha_high':        ultima['ha_high'],
            'real_high':      ultima['real_high'],
            'real_low':       ultima['real_low'],
        }

    # ── analyze ──────────────────────────────────────────────────────────────

    def analyze(self, ticker: str, dati_engine: Dict) -> Optional[StrategySignal]:
        """Entry point — chiamato ad ogni ciclo del bot su XXBTZUSD."""
        try:
            if ticker != TICKER_BTC:
                return None

            # AUDIT 27/04/2026: la HeikinAshi BTC ha funzionato in trend (40W/1L
            # cluster 19-20 mar) ma ha fallito in range (cluster 20-23 apr: 2 LOSS
            # consecutivi a leva 5x con SL hit). Skip entry se le ultime 5 daily
            # chiuse hanno >=3 cambi di colore = mercato indeciso/oscillante.
            try:
                ha_daily_cache = self.ha_daily._cache_ha if (self.ha_daily and self.ha_daily._cache_ha) else None
                if ha_daily_cache and len(ha_daily_cache) >= 6:
                    last5_colors = [c.get('colore') for c in ha_daily_cache[-6:-1]]
                    cambi = sum(
                        1 for i in range(1, len(last5_colors))
                        if last5_colors[i] != last5_colors[i-1]
                    )
                    if cambi >= 3:
                        self.logger.debug(
                            f"⏭️ [HA Trend] {ticker} skip: BTC in range "
                            f"(ultime 5 daily: {cambi} cambi colore — {last5_colors})"
                        )
                        return None
            except Exception as _e_range:
                _err.capture(_e_range, "analyze", {"module": "HeikinAshiStrategy"})
                self.logger.debug(f"[HA Trend] range filter error: {_e_range}")

            # Colore daily → determina il bias
            colore_daily = self._colore_daily()
            if not colore_daily:
                return None

            direzione = 'LONG' if colore_daily == 'VERDE' else 'SHORT'

            # Aggiorna cache 1H
            if not self._aggiorna_cache_1h(ticker):
                return None

            # Verifica setup ritracciamento + cambio
            segnale = self._check_segnale_1h(direzione)
            if not segnale:
                return None

            # Entry
            prezzo_entry = segnale['ha_close']
            if prezzo_entry <= 0:
                return None

            # Precisione BTC
            try:
                from core.asset_list import get_config as _gc
                _prec = int(_gc(ticker).get('precision', 1) or 1)
            except Exception:
                _prec = 1

            # SL virtual 1% — nascosto, gestito da trade_manager
            if direzione == 'LONG':
                sl = round(prezzo_entry * (1 - HA_TREND_SL_PCT), _prec)
                tp = round(prezzo_entry * (1 + HA_TREND_TP_PCT), _prec)
            else:
                sl = round(prezzo_entry * (1 + HA_TREND_SL_PCT), _prec)
                tp = round(prezzo_entry * (1 - HA_TREND_TP_PCT), _prec)

            # Score: base 60 + n_ritrac (più lungo il ritracciamento, meglio)
            score = min(60 + segnale['n_ritrac'] * 5, 90)

            # Bonus se l'entry è vicino a un livello narrativo significativo
            livelli = dati_engine.get('livelli_narrativi', {})
            if livelli and prezzo_entry > 0:
                tutti_livelli = (
                    livelli.get('supporti', []) +
                    livelli.get('resistenze', []) +
                    livelli.get('fibonacci', []) +
                    livelli.get('ob_walls', [])
                )
                for lv in tutti_livelli:
                    dist_pct = abs(float(lv.get('dist_pct', 999)))
                    if dist_pct <= 1.0:  # entro 1% da un livello chiave
                        bonus = 10 if lv.get('forza') == 'forte' or lv.get('tipo') in ('FIB', 'ATH', 'ATL') else 5
                        score = min(score + bonus, 95)
                        razionale += f" | 📍 Vicino a {lv.get('tipo','livello')} ${float(lv.get('prezzo',0)):,.4g} (dist:{dist_pct:.1f}%)"
                        break

            razionale = (
                f"HA Trend {direzione} | "
                f"Daily HA {colore_daily} | "
                f"Ritracciamento 1H: {segnale['n_ritrac']} candele {segnale['colore_1h']} opposte | "
                f"Entry {prezzo_entry:.0f}$ | SL {sl:.0f}$ | TP {tp:.0f}$"
            )

            self.logger.info(
                f"🕯️ HA Trend SEGNALE {direzione} | "
                f"Entry {prezzo_entry:.0f}$ | SL {sl:.0f}$ (-1%) | "
                f"TP {tp:.0f}$ (+2%) | Leva {HA_TREND_LEVA}x | "
                f"Ritrac {segnale['n_ritrac']} candele 1H"
            )

            return StrategySignal(
                signal        = direzione,
                score         = float(score),
                confidence    = round(min(segnale['n_ritrac'] / 5.0, 1.0), 2),
                entry_price   = round(prezzo_entry, _prec),
                sl            = sl,
                tp            = tp,
                sizing        = 0.8,   # 80% del capitale disponibile a margine
                razionale     = razionale,
                strategy_name = f"HeikinAshiTrend_{direzione}",
                components    = {
                    'colore_daily':   colore_daily,
                    'n_ritrac_1h':    segnale['n_ritrac'],
                    'leva':           HA_TREND_LEVA,
                    'virtual_sl':     True,   # SL nascosto gestito da trade_manager
                    'tp_fisso':       True,
                    'sl_pct':         HA_TREND_SL_PCT,
                    'tp_pct':         HA_TREND_TP_PCT,
                    # Flag per trade_manager: usa gestione BE/scaling standard
                    'trailing_ha_daily': False,   # no trailing daily — usa protezione standard
                }
            )

        except Exception as e:
            _err.capture(e, sys._getframe().f_code.co_name, {"ticker": ticker})
            self.logger.error(f"❌ HeikinAshiTrendStrategy: {e}")
            return None


# ══════════════════════════════════════════════════════════════════════════════
#  HEIKIN ASHI CONTRO TREND STRATEGY — MARGINE 1H
#
#  Logica:
#    1. Daily HA ha già cambiato colore su S/R (trend in corso da >= 1 giorno)
#       VERDE daily = trend LONG in corso | ROSSA daily = trend SHORT in corso
#    2. Su 1H il prezzo è in trend e raggiunge un nuovo massimo/minimo di giornata
#       - Trend LONG daily → nuovo massimo di giornata su 1H
#       - Trend SHORT daily → nuovo minimo di giornata su 1H
#    3. La candela HA 1H che raggiunge quel max/min chiude del colore OPPOSTO al daily
#       → rigetto confermato dalla chiusura
#    4. Entry contro trend alla chiusura di quella candela
#    5. SL: Virtual SL 1% (nascosto)
#    6. TP: 2% fisso su Kraken (R:R 1:2)
#    7. Gestione BE/scaling: gestisci_protezione_istituzionale() esistente
#    8. Leva: 10x
# ══════════════════════════════════════════════════════════════════════════════

HA_CONTRO_LEVA      = 10     # max Kraken BTC/USD
HA_CONTRO_TP_PCT    = 0.02   # 2% TP fisso
HA_CONTRO_SL_PCT    = 0.01   # 1% SL virtual
HA_CONTRO_MIN_GIORNI = 1     # giorni minimi di trend daily prima di cercare contro


class HeikinAshiControTrendStrategy(BaseStrategy):
    """
    Strategia 9: Heikin Ashi Contro Trend — MARGINE 1H BTC.

    Entra contro il trend daily quando il prezzo raggiunge un nuovo
    massimo/minimo di giornata e la candela HA 1H rigetta (cambio colore opposto).
    SL virtual 1%, TP fisso 2%, gestione BE/scaling tramite trade_manager esistente.
    """

    def __init__(self, engine, ha_daily: 'HeikinAshiStrategy'):
        super().__init__(engine)
        self.name     = "HeikinAshiControTrendStrategy"
        self.ha_daily = ha_daily   # riferimento per colore daily e giorni trend

    # ── Giorni di trend daily in corso ───────────────────────────────────────

    def _giorni_trend_daily(self) -> int:
        """
        Conta quante candele daily consecutive hanno lo stesso colore (trend in corso).
        Usa la cache di ha_daily.
        """
        try:
            ha = self.ha_daily._cache_ha
            if not ha or len(ha) < 2:
                return 0
            colore_attuale = ha[-1]['colore']
            count = 0
            for i in range(len(ha) - 1, max(0, len(ha) - 30), -1):
                if ha[i]['colore'] == colore_attuale:
                    count += 1
                else:
                    break
            return count
        except Exception:
            return 0

    # ── Massimo e minimo di giornata su 1H ───────────────────────────────────

    def _max_min_giornata(self, ha_1h: List[Dict]) -> Dict:
        """
        Calcola il massimo e minimo della sessione giornaliera corrente
        sulle candele HA 1H (dalla mezzanotte UTC ad ora).

        Restituisce {'max': x, 'min': x, 'n_candele': n}
        """
        from datetime import timezone
        ora_utc = datetime.utcnow()
        mezzanotte_ts = datetime(
            ora_utc.year, ora_utc.month, ora_utc.day,
            tzinfo=timezone.utc
        ).timestamp() * 1000  # timestamp in ms come Kraken

        candele_oggi = [
            c for c in ha_1h
            if float(c['ts']) >= mezzanotte_ts
        ]

        if not candele_oggi:
            # Fallback: ultime 24 candele 1H
            candele_oggi = ha_1h[-24:] if len(ha_1h) >= 24 else ha_1h

        if not candele_oggi:
            return {'max': 0.0, 'min': 0.0, 'n_candele': 0}

        max_giornata = max(float(c['real_high']) for c in candele_oggi)
        min_giornata = min(float(c['real_low'])  for c in candele_oggi)

        return {
            'max':       max_giornata,
            'min':       min_giornata,
            'n_candele': len(candele_oggi),
        }

    # ── Verifica setup contro trend ───────────────────────────────────────────

    def _check_segnale_contro(
        self,
        direzione_daily: str,
        ha_1h: List[Dict],
    ) -> Optional[Dict]:
        """
        Verifica il setup contro trend:

        Per trend LONG (daily verde):
          - Ultima candela HA 1H ha raggiunto un nuovo massimo di giornata
            (real_high == max_giornata)
          - E chiude ROSSA (rigetto confermato)
          → SHORT contro trend

        Per trend SHORT (daily rossa):
          - Ultima candela HA 1H ha raggiunto un nuovo minimo di giornata
            (real_low == min_giornata)
          - E chiude VERDE (rigetto confermato)
          → LONG contro trend
        """
        if not ha_1h or len(ha_1h) < 3:
            return None

        ultima = ha_1h[-1]
        mm     = self._max_min_giornata(ha_1h)

        if mm['n_candele'] < 2:
            return None

        colore_rigetto  = 'ROSSO' if direzione_daily == 'LONG' else 'VERDE'
        direzione_entry = 'SHORT' if direzione_daily == 'LONG' else 'LONG'

        # La candela deve aver raggiunto il max/min di giornata
        if direzione_daily == 'LONG':
            # Cerca nuovo massimo: real_high dell'ultima candela == massimo di giornata
            ha_toccato_estremo = abs(float(ultima['real_high']) - mm['max']) < mm['max'] * 0.001
        else:
            # Cerca nuovo minimo: real_low dell'ultima candela == minimo di giornata
            ha_toccato_estremo = abs(float(ultima['real_low']) - mm['min']) < mm['min'] * 0.001

        if not ha_toccato_estremo:
            return None

        # La candela deve chiudere nel colore opposto al daily
        if ultima['colore'] != colore_rigetto:
            return None

        return {
            'direzione':        direzione_entry,
            'colore_1h':        ultima['colore'],
            'max_giornata':     mm['max'],
            'min_giornata':     mm['min'],
            'n_candele_oggi':   mm['n_candele'],
            'ha_close':         ultima['ha_close'],
            'real_high':        ultima['real_high'],
            'real_low':         ultima['real_low'],
            'estremo_toccato':  mm['max'] if direzione_daily == 'LONG' else mm['min'],
        }

    # ── analyze ──────────────────────────────────────────────────────────────

    def analyze(self, ticker: str, dati_engine: Dict) -> Optional[StrategySignal]:
        """Entry point — chiamato ad ogni ciclo del bot su XXBTZUSD."""
        try:
            # ═══════════════════════════════════════════════════════════════════
            # 🔒 KILL SWITCH HA CONTRO-TREND (2026-05-10)
            # ───────────────────────────────────────────────────────────────────
            # Backtest 3 anni candele BTC 1m (1.576.800 candele Binance):
            #   - 2722 segnali HA Contro-Trend (streak≥2, daily opposto)
            #   - WR direzionale: 30.0% (sotto random 50%)
            #   - PnL lordo: -481% in 3 anni
            #   - PnL netto fees (0.1%/trade): -753% — DISTRUGGE CAPITALE
            #
            # Per confronto, HA Trend CONCORDE (stessi filtri ma daily allineato):
            #   - 2559 segnali, WR 34%, PnL netto +235% in 3 anni
            #
            # Differenza: +988% di PnL non prendendo i contro-trend.
            # La strategia è statisticamente perdente su BTC. Disabilitata.
            #
            # Per riabilitare: rimuovere blocco tra "🔒 KILL SWITCH" e
            # "FINE KILL SWITCH". Backup: heikinashi_strategy.py.BAK
            # ═══════════════════════════════════════════════════════════════════
            if not getattr(self, '_kill_switch_logged', False):
                self.logger.info(
                    "🔒 [HA CONTRO-TREND] Strategia DISABILITATA "
                    "(backtest 3y BTC: 2722 trade, WR 30%, PnL netto -753%). "
                    "Per riabilitare: HeikinAshiControTrendStrategy.analyze()"
                )
                self._kill_switch_logged = True
            return None
            # ═══════════════════════════════════════════════════════════════════
            # FINE KILL SWITCH
            # ═══════════════════════════════════════════════════════════════════

            if ticker != TICKER_BTC:
                return None

            # Aggiorna cache daily se necessario
            if not self.ha_daily._cache_ha:
                self.ha_daily._aggiorna_cache(TICKER_BTC)

            colore_daily = self.ha_daily._cache_ha[-1]['colore'] if self.ha_daily._cache_ha else None
            if not colore_daily:
                return None

            direzione_daily = 'LONG' if colore_daily == 'VERDE' else 'SHORT'

            # Verifica che il trend daily sia in corso da abbastanza giorni
            giorni_trend = self._giorni_trend_daily()
            if giorni_trend < HA_CONTRO_MIN_GIORNI:
                return None

            # Cache 1H — riusa quella di ha_trend se disponibile, altrimenti recupera
            ha_1h = None
            if hasattr(self.ha_daily, '_ha_trend_ref') and self.ha_daily._ha_trend_ref:
                ha_1h = self.ha_daily._ha_trend_ref._cache_ha_1h
            if not ha_1h:
                # Recupera direttamente
                try:
                    from core.asset_list import get_human_name
                    sym     = get_human_name(ticker)
                    ohlcv_1h = None
                    if hasattr(self.engine, '_safe_fetch'):
                        ohlcv_1h = self.engine._safe_fetch(
                            'fetch_ohlcv', sym,
                            timeframe='1h', limit=HA_TREND_OHLCV_1H_LIMIT
                        )
                    if not ohlcv_1h and hasattr(self.engine, 'exchange'):
                        ohlcv_1h = self.engine.exchange.fetch_ohlcv(
                            sym, timeframe='1h', limit=HA_TREND_OHLCV_1H_LIMIT
                        )
                    if ohlcv_1h:
                        ha_1h = calcola_heikin_ashi(ohlcv_1h)
                except Exception as e_1h:
                    _err.capture(e_1h, "analyze", {"module": "HeikinAshiStrategy"})
                    self.logger.debug(f"⚠️ HA Contro cache 1H: {e_1h}")
                    return None

            if not ha_1h:
                return None

            # Verifica setup contro trend
            segnale = self._check_segnale_contro(direzione_daily, ha_1h)
            if not segnale:
                return None

            prezzo_entry = segnale['ha_close']
            if prezzo_entry <= 0:
                return None

            # Precisione BTC
            try:
                from core.asset_list import get_config as _gc
                _prec = int(_gc(ticker).get('precision', 1) or 1)
            except Exception:
                _prec = 1

            # SL virtual 1%, TP fisso 2%
            direzione_entry = segnale['direzione']
            if direzione_entry == 'SHORT':
                sl = round(prezzo_entry * (1 + HA_CONTRO_SL_PCT), _prec)
                tp = round(prezzo_entry * (1 - HA_CONTRO_TP_PCT), _prec)
            else:
                sl = round(prezzo_entry * (1 - HA_CONTRO_SL_PCT), _prec)
                tp = round(prezzo_entry * (1 + HA_CONTRO_TP_PCT), _prec)

            score = min(65 + giorni_trend * 3, 88)

            razionale = (
                f"HA Contro Trend {direzione_entry} | "
                f"Daily {colore_daily} da {giorni_trend}gg | "
                f"Estremo giornata: {segnale['estremo_toccato']:.0f}$ | "
                f"HA 1H {segnale['colore_1h']} (rigetto) | "
                f"Entry {prezzo_entry:.0f}$ | SL {sl:.0f}$ | TP {tp:.0f}$"
            )

            self.logger.info(
                f"🔄 HA Contro Trend SEGNALE {direzione_entry} | "
                f"BTC {prezzo_entry:.0f}$ | "
                f"Estremo {segnale['estremo_toccato']:.0f}$ | "
                f"Daily {colore_daily} {giorni_trend}gg | "
                f"SL {sl:.0f}$ | TP {tp:.0f}$ | Leva {HA_CONTRO_LEVA}x"
            )

            return StrategySignal(
                signal        = direzione_entry,
                score         = float(score),
                confidence    = round(min(giorni_trend / 5.0, 1.0), 2),
                entry_price   = round(prezzo_entry, _prec),
                sl            = sl,
                tp            = tp,
                sizing        = 0.7,   # sizing leggermente ridotto — setup contrarian
                razionale     = razionale,
                strategy_name = f"HeikinAshiContro_{direzione_entry}",
                components    = {
                    'colore_daily':       colore_daily,
                    'giorni_trend':       giorni_trend,
                    'estremo_giornata':   segnale['estremo_toccato'],
                    'max_giornata':       segnale['max_giornata'],
                    'min_giornata':       segnale['min_giornata'],
                    'leva':               HA_CONTRO_LEVA,
                    'virtual_sl':         True,
                    'tp_fisso':           True,
                    'sl_pct':             HA_CONTRO_SL_PCT,
                    'tp_pct':             HA_CONTRO_TP_PCT,
                    'trailing_ha_daily':  False,
                }
            )

        except Exception as e:
            _err.capture(e, sys._getframe().f_code.co_name, {"ticker": ticker})
            self.logger.error(f"❌ HeikinAshiControTrendStrategy: {e}")
            return None
