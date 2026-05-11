# -*- coding: utf-8 -*-
"""
L&A Institutional Bot - Strategy Engine
Modulo di strategie deterministiche per pre-filtraggio opportunità.

Filosofia:
  - Analisi deterministica basata su edge reali (no AI)
  - Combina segnali da Engine (walls, flow, funding, multi-tf)
  - Score oggettivo 0-100 per ogni opportunità
  - Pre-filtra prima di chiamare Gemini (riduce AI calls 70%)
  - Complementare a Brain (AI) e ChimeraML (XGBoost)

Strategie implementate:
  1. WallBreakoutStrategy - Breakout muri liquidità
  2. ToxicFlowReversalStrategy - Reversal su toxic flow
  3. MultiTFConfluenceStrategy - Allineamento multi-timeframe
  4. FundingSqueezeStrategy - Squeeze overleveraged positions
  5. MeanReversionStrategy - Z-VWAP mean reversion (Hurst < 0.50)
  6. LiquidationCascadeStrategy - Entry post-liquidazione a cascata

Integrazione:
  from core.strategy_engine import StrategyEngine
  strategy_engine = StrategyEngine(engine)
  result = strategy_engine.analyze(ticker, dati_engine)
  
  if result.score >= 75:
      # Strong signal - usa direttamente
  elif result.score >= 60:
      # Medium - valida con Gemini
  else:
      # Skip o solo Gemini
"""

import logging
import time
from typing import Dict, Optional, List
from dataclasses import dataclass, asdict
import sys
from core.chimera_errors import ErrorTracker


_err = ErrorTracker("StrategyEngine")

@dataclass
class StrategySignal:
    """Output standardizzato di una strategia"""
    signal: str  # LONG/SHORT/FLAT/WATCH
    score: float  # 0-100
    confidence: float  # 0-1 (% strategie d'accordo)
    entry_price: float
    sl: float
    tp: float
    sizing: float  # 0-1
    razionale: str
    strategy_name: str
    components: Dict  # Debug/analytics info
    
    def to_dict(self):
        """Converte a dict per logging/serializzazione"""
        return asdict(self)


class BaseStrategy:
    """Classe base per tutte le strategie"""
    
    def __init__(self, engine):
        self.engine = engine
        self.logger = logging.getLogger(self.__class__.__name__)
        self.name = self.__class__.__name__
    
    def analyze(self, ticker: str, dati_engine: Dict) -> Optional[StrategySignal]:
        """Implementato dalle strategie figlie"""
        raise NotImplementedError
    
    def _get_current_price(self, dati_engine: Dict) -> float:
        """Estrae prezzo corrente da dati engine"""
        price = dati_engine.get('close', 0)
        if not price:
            price = dati_engine.get('current_price', 0)
        return float(price) if price else 0.0


class WallBreakoutStrategy(BaseStrategy):
    """
    Strategia 1: Breakout muri di liquidità con conferma order flow.
    
    Setup LONG:
      - Muro resistenza identificato (vol > 50k, distanza 2-5%)
      - Pressure > 60% (muro consumato)
      - CVD positivo (buyers attivi)
      - Momentum crescente (price_velocity > 0.0002)
      - Volume spike > 2.5x media
    
    Entry: Rottura muro + 0.2%
    SL: -1.5%
    TP: +4% (R:R 1:2.6)
    """
    
    def analyze(self, ticker: str, dati_engine: Dict) -> Optional[StrategySignal]:
        try:
            # 1. OTTIENI DATI
            walls = self.engine.get_liquidity_walls(ticker)
            flow = self.engine.analizza_order_flow(ticker)
            sentinel = self.engine.check_sentinel(ticker)
            
            current_price = self._get_current_price(dati_engine)
            if not current_price or current_price <= 0:
                return None
            
            # 2. SETUP LONG (Breakout Resistance)
            muro_res  = walls.get('muro_resistenza', 0)
            vol_res   = walls.get('vol_resistenza', 0)
            dist_res  = walls.get('dist_resistenza', walls.get('distanza_perc_resistenza', 0))

            cvd      = flow.get('cvd_istantaneo', 0)     if isinstance(flow, dict) else 0
            velocity = flow.get('price_velocity', 0)     if isinstance(flow, dict) else 0
            vpin     = flow.get('vpin', 0)               if isinstance(flow, dict) else 0
            vol_spike = (sentinel.get('vol_spike', 0)
                         if sentinel and isinstance(sentinel, dict) else 0)
            
            # 3. VALIDA CONDIZIONI LONG
            # Soglie calibrate sui valori reali:
            # BTC: vol~100 BTC, CVD~500k; DOGE/XRP: vol~millions, CVD~50-120k
            _is_btc = current_price > 10000
            _is_mid = 10 < current_price <= 10000  # ETH, SOL
            _vol_min  = 80       if _is_btc else (5      if _is_mid else 500000)
            _cvd_min  = 200000   if _is_btc else (10000  if _is_mid else 30000)
            _vel_min  = 0.00008  if _is_btc else (0.00003 if _is_mid else 0.0001)

            muro_sup     = walls.get('muro_supporto', 0)
            vol_sup      = walls.get('vol_supporto', 0)
            dist_sup     = walls.get('dist_supporto', walls.get('distanza_perc_supporto', 0))
            pressure_res = walls.get('pressure_resistenza', 0)
            pressure_sup = walls.get('pressure_supporto', 0)

            conditions_long = {
                'muro_valido':    vol_res > _vol_min and 0.3 <= dist_res <= 4,
                'cvd_forte':      cvd > _cvd_min,
                'momentum_forte': velocity > _vel_min,
                'vpin_attivo':    vpin > 0.4,
                'dist_ok':        0.3 <= dist_res <= 3,
            }

            score_long = 0
            if conditions_long['muro_valido']:    score_long += 20
            if conditions_long['cvd_forte']:      score_long += 30
            if conditions_long['momentum_forte']: score_long += 25
            if conditions_long['vpin_attivo']:    score_long += 15
            if conditions_long['dist_ok']:        score_long += 10
            # Penalità per condizioni contrarie
            if cvd < 0 and not conditions_long['cvd_forte']:    score_long -= 15
            if velocity < 0 and not conditions_long['momentum_forte']: score_long -= 10

            met_conditions_long = sum(conditions_long.values())

            conditions_short = {
                'muro_valido':    vol_sup > _vol_min and 0.3 <= dist_sup <= 4,
                'cvd_forte':      cvd < -_cvd_min,
                'momentum_forte': velocity < -_vel_min,
                'vpin_attivo':    vpin > 0.4,
                'dist_ok':        0.3 <= dist_sup <= 3,
            }

            score_short = 0
            if conditions_short['muro_valido']:    score_short += 20
            if conditions_short['cvd_forte']:      score_short += 30
            if conditions_short['momentum_forte']: score_short += 25
            if conditions_short['vpin_attivo']:    score_short += 15
            if conditions_short['dist_ok']:        score_short += 10
            # Penalità per condizioni contrarie
            if cvd > 0 and not conditions_short['cvd_forte']:    score_short -= 15
            if velocity > 0 and not conditions_short['momentum_forte']: score_short -= 10

            met_conditions_short = sum(conditions_short.values())
            
            # 6. DECISIONE FINALE
            # Veto se SignalStateEngine indica ESAURIMENTO
            _phase      = dati_engine.get('entry_phase', 'FORMAZIONE')
            _exhaustion = float(dati_engine.get('exhaustion_score', 0) or 0)
            _phase_veto = _phase in ('ESAURIMENTO',) and _exhaustion > 60

            # ── FILTRO HA DAILY ───────────────────────────────────────────────
            # Le HA daily definiscono il trend di fondo. Logica concordata:
            #
            # HA VERDE → entra normalmente, nessuna restrizione
            # HA ROSSO → blocca LONG a meno che non ci siano tutte e 3 le conferme:
            #   1. struttura_h4 == UPTREND  (H4 già girato rialzista)
            #   2. CVD positivo E cvd_delta_30s > 0  (flusso reale, non spike)
            #   3. Prezzo su livello significativo  (dist_supporto < 1.5% O su POC/VAL)
            # Se tutte e 3 presenti → il trend sta davvero cambiando → entra normalmente
            # Se anche solo 1 manca → non è un'inversione reale → veto
            #
            # Simmetrico per SHORT: HA VERDE blocca SHORT salvo 3 conferme ribassiste
            _ha_colore  = str(dati_engine.get('ha_daily_colore', '') or '').upper()
            _ha_streak  = int(dati_engine.get('ha_daily_streak', 0) or 0)
            _cvd_delta  = float(dati_engine.get('cvd_delta_30s', 0) or 0)
            _dist_sup   = float(dati_engine.get('dist_supporto', 99) or 99)
            _dist_res   = float(dati_engine.get('dist_resistenza', 99) or 99)
            _poc        = float(dati_engine.get('poc', 0) or 0)
            _val        = float(dati_engine.get('val', 0) or 0)
            _vah        = float(dati_engine.get('vah', 0) or 0)
            _struttura_h4 = str((dati_engine.get('multi_tf', {}) or {})
                                .get('4h', {}).get('trend_dir', '') or '').upper()

            # Condizione 3: prezzo su livello significativo
            _su_livello_long = (
                _dist_sup < 1.5 or
                (_poc > 0 and abs(current_price - _poc) / current_price * 100 < 1.0) or
                (_val > 0 and abs(current_price - _val) / current_price * 100 < 1.0)
            )
            _su_livello_short = (
                _dist_res < 1.5 or
                (_poc > 0 and abs(current_price - _poc) / current_price * 100 < 1.0) or
                (_vah > 0 and abs(current_price - _vah) / current_price * 100 < 1.0)
            )

            # Veto LONG su HA ROSSO — sblocca solo con tutte e 3 le conferme
            _ha_long_veto = False
            if _ha_colore == 'ROSSO' and _ha_streak >= 1:
                _conf1 = _struttura_h4 == 'UP'
                _conf2 = cvd > 0 and _cvd_delta > 0
                _conf3 = _su_livello_long
                _conferme_long = sum([_conf1, _conf2, _conf3])
                if _conferme_long < 3:
                    _ha_long_veto = True
                    self.logger.info(
                        f"🚫 WallBreakout LONG [{ticker}]: HA daily ROSSO streak={_ha_streak} "
                        f"— conferme inversione {_conferme_long}/3 "
                        f"(H4={_conf1} CVD={_conf2} Livello={_conf3})"
                    )

            # Veto SHORT su HA VERDE — sblocca solo con tutte e 3 le conferme
            _ha_short_veto = False
            if _ha_colore == 'VERDE' and _ha_streak >= 2:
                _conf1s = _struttura_h4 == 'DOWN'
                _conf2s = cvd < 0 and _cvd_delta < 0
                _conf3s = _su_livello_short
                _conferme_short = sum([_conf1s, _conf2s, _conf3s])
                if _conferme_short < 3:
                    _ha_short_veto = True
                    self.logger.info(
                        f"🚫 WallBreakout SHORT [{ticker}]: HA daily VERDE streak={_ha_streak} "
                        f"— conferme inversione {_conferme_short}/3 "
                        f"(H4={_conf1s} CVD={_conf2s} Livello={_conf3s})"
                    )

            if score_long >= 75 and met_conditions_long >= 4:
                if _phase_veto:
                    self.logger.info(f"🚫 WallBreakout LONG [{ticker}]: veto exhaustion ({_phase} {_exhaustion:.0f})")
                    return None
                if _ha_long_veto:
                    return None
                entry = muro_res * 1.002  # +0.2% sopra muro
                sl = entry * 0.985  # -1.5%
                tp = entry * 1.04   # +4%
                sizing = 0.8 if met_conditions_long == 5 else 0.6
                # Usa precisione asset — DOGE/XRP hanno 5 decimali, non 2
                try:
                    from core.asset_list import get_config as _gc
                    _prec = int(_gc(ticker).get('precision', 5) or 5)
                except Exception:
                    _prec = 5

                return StrategySignal(
                    signal='LONG',
                    score=score_long,
                    confidence=met_conditions_long / 5.0,
                    entry_price=round(entry, _prec),
                    sl=round(sl, _prec),
                    tp=round(tp, _prec),
                    sizing=sizing,
                    razionale=(
                        f"Wall breakout @ {muro_res:.2f} - "
                        f"pressure {pressure_res:.2f} - "
                        f"CVD ${cvd:,.0f} - "
                        f"{met_conditions_long}/5 conditions"
                    ),
                    strategy_name="WallBreakout_LONG",
                    components={
                        'muro': muro_res,
                        'pressure': pressure_res,
                        'cvd': cvd,
                        'velocity': velocity,
                        'vol_spike': vol_spike,
                        'conditions': conditions_long,
                    }
                )
            
            elif score_short >= 75 and met_conditions_short >= 4:
                if _phase_veto:
                    self.logger.info(f"🚫 WallBreakout SHORT [{ticker}]: veto exhaustion ({_phase} {_exhaustion:.0f})")
                    return None
                if _ha_short_veto:
                    return None
                entry = muro_sup * 0.998  # -0.2% sotto muro
                sl = entry * 1.015  # +1.5% sopra entry
                tp = entry * 0.96   # -4% sotto entry
                if sl <= entry: sl = entry * 1.015
                if tp >= entry: tp = entry * 0.96
                sizing = 0.7
                try:
                    from core.asset_list import get_config as _gc
                    _prec = int(_gc(ticker).get('precision', 5) or 5)
                except Exception:
                    _prec = 5

                return StrategySignal(
                    signal='SHORT',
                    score=score_short,
                    confidence=met_conditions_short / 5.0,
                    entry_price=round(entry, _prec),
                    sl=round(sl, _prec),
                    tp=round(tp, _prec),
                    sizing=sizing,
                    razionale=(
                        f"Wall breakdown @ {muro_sup:.2f} - "
                        f"pressure {pressure_sup:.2f} - "
                        f"CVD ${cvd:,.0f} - "
                        f"{met_conditions_short}/5 conditions"
                    ),
                    strategy_name="WallBreakout_SHORT",
                    components={
                        'muro': muro_sup,
                        'pressure': pressure_sup,
                        'cvd': cvd,
                        'velocity': velocity,
                        'conditions': conditions_short,
                    }
                )
            
            # Nessun setup valido
            return None
            
        except Exception as e:
            _err.capture(e, sys._getframe().f_code.co_name, {"module": "StrategyEngine"})
            self.logger.error(f"Errore WallBreakoutStrategy su {ticker}: {e}")
            return None


class ToxicFlowReversalStrategy(BaseStrategy):
    """
    Strategia 2: Toxic flow reversal - sfrutta VPIN > 0.75 per anticipare reversal.
    
    Setup LONG:
      - VPIN > 0.75 (toxic flow detected)
      - CVD < -30k (strong selling pressure)
      - Prezzo vicino muro supporto (< 2%)
      - Muro supporto intatto (pressure < 0.4)
      - Fear & Greed < 30
      - Attende VPIN < 0.6 per entry (toxic absorbed)
    
    Entry: Dopo VPIN scende
    SL: Sotto supporto -1%
    TP: +3%
    """
    
    def analyze(self, ticker: str, dati_engine: Dict) -> Optional[StrategySignal]:
        try:
            # 1. DATI
            flow = self.engine.analizza_order_flow(ticker)
            walls = self.engine.get_liquidity_walls(ticker)
            sentiment_raw = self.engine.get_fear_greed()
            # get_fear_greed può restituire int o dict — normalizza
            if isinstance(sentiment_raw, dict):
                sentiment = sentiment_raw
            else:
                sentiment = {'fear_greed_value': int(sentiment_raw) if sentiment_raw else 50}
            
            current_price = self._get_current_price(dati_engine)
            if not current_price:
                return None
            
            # 2. PARAMETRI
            flow = flow if isinstance(flow, dict) else {}
            walls = walls if isinstance(walls, dict) else {}
            vpin = flow.get('vpin', 0)
            cvd = flow.get('cvd_istantaneo', 0)
            is_toxic = flow.get('is_toxic', False)
            
            muro_sup = walls.get('muro_supporto', 0)
            pressure_sup = walls.get('pressure_supporto', 0)
            dist_sup_perc = walls.get('distanza_perc_supporto', 0)
            
            fear_value = sentiment.get('fear_greed_value', 50)
            
            # 3. CONDIZIONI LONG
            conditions_long = {
                'toxic_detected': vpin > 0.75 or is_toxic,
                'selling_pressure': cvd < -30000,
                'near_support': abs(dist_sup_perc) < 2,
                'wall_intact': pressure_sup < 0.4,
                'fear_present': fear_value < 40,  # crypto raramente < 20, 40 più realistico
            }
            
            # 4. SCORE
            score = 0
            if conditions_long['toxic_detected']: score += 30
            if conditions_long['selling_pressure']: score += 25
            if conditions_long['near_support']: score += 20
            if conditions_long['wall_intact']: score += 15
            if conditions_long['fear_present']: score += 10
            
            met_conditions = sum(conditions_long.values())
            
            # 5. DECISIONE
            if score >= 70 and met_conditions >= 4:
                # Check se toxic flow absorbed
                if vpin < 0.6:
                    # ENTRY NOW
                    entry = current_price
                    sl = muro_sup * 0.99
                    tp = entry * 1.03
                    sizing = 0.5  # Setup rischioso
                    
                    return StrategySignal(
                        signal='LONG',
                        score=score,
                        confidence=met_conditions / 5.0,
                        entry_price=round(entry, 2),
                        sl=round(sl, 2),
                        tp=round(tp, 2),
                        sizing=sizing,
                        razionale=(
                            f"Toxic absorbed - VPIN {vpin:.2f} - "
                            f"Support @ {muro_sup:.2f} - "
                            f"Fear {fear_value}"
                        ),
                        strategy_name="ToxicReversal_LONG",
                        components={
                            'vpin': vpin,
                            'cvd': cvd,
                            'fear': fear_value,
                            'support': muro_sup,
                            'conditions': conditions_long,
                        }
                    )
                else:
                    # WATCH - aspetta assorbimento
                    return StrategySignal(
                        signal='WATCH',
                        score=score,
                        confidence=met_conditions / 5.0,
                        entry_price=current_price,
                        sl=0,
                        tp=0,
                        sizing=0,
                        razionale=(
                            f"Toxic flow active (VPIN {vpin:.2f}) - "
                            f"WAIT for absorption < 0.6"
                        ),
                        strategy_name="ToxicReversal_WATCH",
                        components={
                            'vpin': vpin,
                            'target_vpin': 0.6,
                            'conditions': conditions_long,
                        }
                    )
            
            # ── SETUP SHORT: VPIN tossico + CVD forte POSITIVO = bear trap ──
            # Istituzionali vendono mentre retail compra → reversal ribassista
            conditions_short = {
                'toxic_detected':   vpin > 0.75 or is_toxic,
                'buying_pressure':  cvd > 30000,   # CVD fortemente positivo ma sarà assorbito
                'near_resistance':  0 < walls.get('distanza_perc_resistenza', 99) < 2,
                'wall_intact':      walls.get('pressure_resistenza', 1) < 0.4,
                'greed_present':    fear_value > 70,  # euforia → distribuzione imminente
            }
            score_short = 0
            if conditions_short['toxic_detected']:   score_short += 30
            if conditions_short['buying_pressure']:  score_short += 25
            if conditions_short['near_resistance']:  score_short += 20
            if conditions_short['wall_intact']:      score_short += 15
            if conditions_short['greed_present']:    score_short += 10

            met_short = sum(conditions_short.values())
            if score_short >= 70 and met_short >= 4 and vpin < 0.6:
                entry = current_price
                sl = current_price * 1.015
                tp = current_price * 0.97
                return StrategySignal(
                    signal='SHORT', score=score_short,
                    confidence=met_short / 5.0,
                    entry_price=round(entry, 5),
                    sl=round(sl, 5), tp=round(tp, 5),
                    sizing=0.5,
                    razionale=(
                        f"Bear trap - VPIN {vpin:.2f} + CVD ${cvd:,.0f} - "
                        f"Resistance @ {walls.get('muro_resistenza', 0):.5f} - Greed {fear_value}"
                    ),
                    strategy_name="ToxicReversal_SHORT",
                    components={'vpin': vpin, 'cvd': cvd, 'fear': fear_value, 'conditions': conditions_short}
                )

            return None
            
        except Exception as e:
            _err.capture(e, sys._getframe().f_code.co_name, {"module": "StrategyEngine"})
            self.logger.error(f"Errore ToxicFlowReversalStrategy su {ticker}: {e}")
            return None


class MultiTFConfluenceStrategy(BaseStrategy):
    """
    Strategia 3: Multi-timeframe confluence - allineamento 3 TF per massima probabilità.
    
    Setup LONG:
      - 15m/1h/4h tutti TRENDING
      - Tutti trend = UP
      - Kaufman > 0.25 su tutti TF
      - Volume relativo > 1.3x su 15m
      - CVD allineato alla direzione
      
    Entry: Pull-back a EMA20 del 15m
    SL: Sotto EMA50 del 1h
    TP: 4x ATR del 1h (trailing)
    """
    
    def analyze(self, ticker: str, dati_engine: Dict) -> Optional[StrategySignal]:
        try:
            # 1. DATI MULTI-TF
            data_tf = self.engine.get_market_data_multi_tf(ticker, timeframes=['15m', '1h', '4h'])
            flow = self.engine.analizza_order_flow(ticker)
            
            if len(data_tf) < 3:
                return None
            
            # 2. ANALISI REGIME
            tf_15m = data_tf.get('15m', {})
            tf_1h = data_tf.get('1h', {})
            tf_4h = data_tf.get('4h', {})
            
            regime_15m = tf_15m.get('regime', 'UNDEFINED')
            regime_1h = tf_1h.get('regime', 'UNDEFINED')
            regime_4h = tf_4h.get('regime', 'UNDEFINED')
            
            trend_15m = tf_15m.get('trend_dir', 'FLAT')
            trend_1h = tf_1h.get('trend_dir', 'FLAT')
            trend_4h = tf_4h.get('trend_dir', 'FLAT')
            
            kaufman_15m = tf_15m.get('kaufman', 0)
            kaufman_1h = tf_1h.get('kaufman', 0)
            kaufman_4h = tf_4h.get('kaufman', 0)
            
            # 3. CONDIZIONI ALLINEAMENTO
            all_trending = all([r == 'TRENDING' for r in [regime_15m, regime_1h, regime_4h]])
            all_same_dir = len(set([trend_15m, trend_1h, trend_4h])) == 1
            # Soglia kaufman adattiva: BTC/ETH più efficienti, DOGE/XRP più rumorosi
            strong_kaufman = all([k > 0.25 for k in [kaufman_15m, kaufman_1h, kaufman_4h]])
            
            vol_rel = tf_15m.get('vol_relativo', 1.0)
            high_volume = vol_rel > 1.3
            
            cvd = flow.get('cvd_istantaneo', 0)
            direction = trend_4h  # Usa TF più alto
            
            # 4. SETUP LONG
            if all_trending and all_same_dir and direction == 'UP' and strong_kaufman:
                cvd_aligned = cvd > 0
                
                # Score base per perfect alignment
                score = 85
                if high_volume: score += 7
                if cvd_aligned: score += 8
                
                current_price = tf_15m.get('close', 0)
                ema20 = tf_15m.get('ema20', current_price)
                ema50_1h = tf_1h.get('ema50', current_price)
                atr_1h = tf_1h.get('atr', current_price * 0.02)
                
                # Check se vicino a EMA20 per entry
                # Soglia kaufman adattiva (calcolata dopo current_price)
                _kauf_thr = 0.20 if current_price < 1 else (0.22 if current_price < 10 else 0.25)
                if not all([k > _kauf_thr for k in [kaufman_15m, kaufman_1h, kaufman_4h]]):
                    return None  # kaufman non abbastanza forte per questo asset

                # Finestra entry allargata a 2% da EMA20.
                # Con 0.5% quasi tutto andava in WATCH — in trend forte
                # il prezzo non torna mai così vicino a EMA20.
                if current_price <= ema20 * 1.02:
                    # Veto exhaustion
                    _ph = dati_engine.get('entry_phase','')
                    _ex = float(dati_engine.get('exhaustion_score', 0) or 0)
                    if _ph == 'ESAURIMENTO' and _ex > 60:
                        return None
                    # ENTRY NOW
                    entry = current_price
                    sl = ema50_1h * 0.98
                    # Validazione: SL LONG deve essere SOTTO entry
                    if sl >= entry:
                        sl = entry * 0.985  # fallback -1.5%
                    tp = entry + (atr_1h * 4)
                    # Validazione: TP LONG deve essere SOPRA entry
                    if tp <= entry:
                        tp = entry * 1.04
                    sizing = 0.9
                    
                    return StrategySignal(
                        signal='LONG',
                        score=score,
                        confidence=1.0,  # 3/3 TF aligned
                        entry_price=round(entry, 2),
                        sl=round(sl, 2),
                        tp=round(tp, 2),
                        sizing=sizing,
                        razionale=(
                            f"3-TF TRENDING UP - "
                            f"K: {kaufman_15m:.2f}/{kaufman_1h:.2f}/{kaufman_4h:.2f} - "
                            f"Vol {vol_rel:.1f}x - CVD ${cvd:,.0f}"
                        ),
                        strategy_name="MultiTF_LONG",
                        components={
                            '15m': f"{regime_15m} {trend_15m} K={kaufman_15m:.2f}",
                            '1h': f"{regime_1h} {trend_1h} K={kaufman_1h:.2f}",
                            '4h': f"{regime_4h} {trend_4h} K={kaufman_4h:.2f}",
                            'vol_rel': vol_rel,
                            'cvd': cvd,
                        }
                    )
                else:
                    # WAIT PULLBACK
                    return StrategySignal(
                        signal='WATCH',
                        score=score,
                        confidence=1.0,
                        entry_price=ema20,
                        sl=0,
                        tp=0,
                        sizing=0,
                        razionale=(
                            f"Perfect setup - WAIT pullback to EMA20 ({ema20:.2f}). "
                            f"Current: {current_price:.2f}"
                        ),
                        strategy_name="MultiTF_WAIT",
                        components={
                            'target_entry': ema20,
                            'current_price': current_price,
                            'regimes': f"{regime_15m}/{regime_1h}/{regime_4h}",
                        }
                    )
            
            # Setup SHORT (simmetrico)
            elif all_trending and all_same_dir and direction == 'DOWN' and strong_kaufman:
                cvd_aligned = cvd < 0
                
                score = 85
                if high_volume: score += 7
                if cvd_aligned: score += 8
                
                current_price = tf_15m.get('close', 0)
                ema20 = tf_15m.get('ema20', current_price)
                ema50_1h = tf_1h.get('ema50', current_price)
                atr_1h = tf_1h.get('atr', current_price * 0.02)
                
                # Soglia kaufman adattiva per SHORT
                _kauf_thr_s = 0.20 if current_price < 1 else (0.22 if current_price < 10 else 0.25)
                if not all([k > _kauf_thr_s for k in [kaufman_15m, kaufman_1h, kaufman_4h]]):
                    return None

                # Finestra entry SHORT simmetrica al LONG: 2% da EMA20.
                # Con 0.5% quasi tutto andava in WATCH in trend ribassista forte.
                if current_price >= ema20 * 0.98:
                    entry = current_price
                    sl = ema50_1h * 1.02
                    # Validazione: SL SHORT deve essere SOPRA entry
                    if sl <= entry:
                        sl = entry * 1.015  # fallback +1.5%
                    tp = entry - (atr_1h * 4)
                    # Validazione: TP SHORT deve essere SOTTO entry
                    if tp >= entry:
                        tp = entry * 0.96
                    sizing = 0.9
                    
                    return StrategySignal(
                        signal='SHORT',
                        score=score,
                        confidence=1.0,
                        entry_price=round(entry, 2),
                        sl=round(sl, 2),
                        tp=round(tp, 2),
                        sizing=sizing,
                        razionale=(
                            f"3-TF TRENDING DOWN - "
                            f"K: {kaufman_15m:.2f}/{kaufman_1h:.2f}/{kaufman_4h:.2f}"
                        ),
                        strategy_name="MultiTF_SHORT",
                        components={
                            'regimes': f"{regime_15m}/{regime_1h}/{regime_4h}",
                            'vol_rel': vol_rel,
                        }
                    )
            
            return None
            
        except Exception as e:
            _err.capture(e, sys._getframe().f_code.co_name, {"module": "StrategyEngine"})
            self.logger.error(f"Errore MultiTFConfluenceStrategy su {ticker}: {e}")
            return None


class FundingSqueezeStrategy(BaseStrategy):
    """
    Strategia 4: Funding rate squeeze - overleveraged positions.
    
    SHORT SQUEEZE (funding negativo estremo — soglie calibrate per Kraken):
      - funding_rate < -0.0003 (-0.03%)
      - funding_zscore < -1.5
      - CVD turning positive
      - VPIN > 0.65 (smart money)
      
    LONG SQUEEZE (funding positivo estremo — soglie calibrate per Kraken):
      - funding_rate > 0.001 (+0.10%)
      - funding_zscore > 1.5
      - CVD turning negative
      - VPIN > 0.65

    NOTA: soglie originali (-0.05% / +0.15%) erano per Binance.
    Su Kraken il funding è compresso — quelle soglie non scattavano mai.
    """
    
    def analyze(self, ticker: str, dati_engine: Dict) -> Optional[StrategySignal]:
        try:
            # 1. DATI FUNDING — usa funding da dati_engine (disponibile per tutti gli asset)
            # Non limitare ai futures: engine_la recupera funding anche per spot via endpoint esterni
            funding_rate = float(dati_engine.get('funding_rate', 0) or 0)
            funding_zscore = float(dati_engine.get('funding_z_score', 0) or 0)
            # Se dati non disponibili nell'engine, prova via get_funding_rate_info
            if funding_rate == 0 and funding_zscore == 0:
                try:
                    _fd = self.engine.get_funding_rate_info(ticker) if hasattr(self.engine, 'get_funding_rate_info') else {}
                    _fd = _fd or {}
                    funding_rate   = float(_fd.get('funding_rate_8h_%', 0) or 0)
                    funding_zscore = float(_fd.get('funding_zscore', 0) or 0)
                except Exception:
                    pass
            if funding_rate == 0 and funding_zscore == 0:
                return None  # Nessun dato funding disponibile
            
            flow = self.engine.analizza_order_flow(ticker)
            walls = self.engine.get_liquidity_walls(ticker)
            

            
            cvd = flow.get('cvd_istantaneo', 0)
            vpin = flow.get('vpin', 0)
            velocity = flow.get('price_velocity', 0)
            
            current_price = self._get_current_price(dati_engine)
            if not current_price:
                return None
            
            # 2. SHORT SQUEEZE SETUP
            # Soglie ricalibrare per Kraken: funding compresso vs Binance.
            # Range reale osservato nel DB: -0.034% / +0.019%
            # Vecchie soglie Binance (-0.05% / +0.15%) → 0 trade in 3 settimane
            # Nuove soglie: z-score come filtro principale, rate come conferma
            if funding_rate < -0.0003 and funding_zscore < -1.5:
                conditions = {
                    'extreme_funding': funding_zscore < -2.5,
                    'cvd_turning': cvd > 0,
                    'smart_money': vpin > 0.65,
                    'momentum_shift': velocity > 0,
                }
                
                score = 0
                if conditions['extreme_funding']: score += 30
                if conditions['cvd_turning']: score += 25
                if conditions['smart_money']: score += 25
                if conditions['momentum_shift']: score += 20
                
                met_conditions = sum(conditions.values())
                
                if score >= 70:
                    entry = current_price
                    sl = current_price * 0.98
                    tp = current_price * 1.05
                    sizing = 0.7
                    
                    return StrategySignal(
                        signal='LONG',
                        score=score,
                        confidence=met_conditions / 4.0,
                        entry_price=round(entry, 2),
                        sl=round(sl, 2),
                        tp=round(tp, 2),
                        sizing=sizing,
                        razionale=(
                            f"SHORT SQUEEZE - "
                            f"Funding {funding_rate:.3f}% (z={funding_zscore:.1f}) - "
                            f"CVD turning ${cvd:,.0f}"
                        ),
                        strategy_name="FundingSqueeze_SHORT",
                        components={
                            'funding_rate': funding_rate,
                            'funding_zscore': funding_zscore,
                            'cvd': cvd,
                            'vpin': vpin,
                            'conditions': conditions,
                        }
                    )
            
            # 3. LONG SQUEEZE SETUP
            # Soglia ricalibrata: +0.001 (+0.10%) invece di +0.15% (mai su Kraken)
            elif funding_rate > 0.001 and funding_zscore > 1.5:
                conditions = {
                    'extreme_funding': funding_zscore > 2.5,
                    'cvd_turning': cvd < 0,
                    'smart_money': vpin > 0.65,
                    'momentum_weakening': velocity < 0.0001,
                }
                
                score = 0
                if conditions['extreme_funding']: score += 30
                if conditions['cvd_turning']: score += 25
                if conditions['smart_money']: score += 25
                if conditions['momentum_weakening']: score += 20
                
                met_conditions = sum(conditions.values())
                
                if score >= 70:
                    entry = current_price
                    sl = current_price * 1.02
                    tp = current_price * 0.95
                    sizing = 0.6
                    
                    return StrategySignal(
                        signal='SHORT',
                        score=score,
                        confidence=met_conditions / 4.0,
                        entry_price=round(entry, 2),
                        sl=round(sl, 2),
                        tp=round(tp, 2),
                        sizing=sizing,
                        razionale=(
                            f"LONG SQUEEZE - "
                            f"Funding {funding_rate:.3f}% (z={funding_zscore:.1f}) - "
                            f"Momentum weakening"
                        ),
                        strategy_name="FundingSqueeze_LONG",
                        components={
                            'funding_rate': funding_rate,
                            'funding_zscore': funding_zscore,
                            'cvd': cvd,
                            'conditions': conditions,
                        }
                    )
            
            return None
            
        except Exception as e:
            _err.capture(e, sys._getframe().f_code.co_name, {"module": "StrategyEngine"})
            self.logger.error(f"Errore FundingSqueezeStrategy su {ticker}: {e}")
            return None


class MeanReversionStrategy(BaseStrategy):
    """
    Strategia 5: Z-VWAP Mean Reversion deterministica.

    Logica: in regime MEAN_REVERSION (Hurst < 0.50), il prezzo si allontana
    dalla VWAP in modo statisticamente non sostenibile e tende a tornare al POC.

    Setup LONG:
      - Hurst < 0.50 (regime mean-reverting confermato)
      - Z-Score VWAP < -1.8 (ipervenduto vs VWAP)
      - Prezzo sotto VAL (sotto Value Area Low)
      - CVD divergente: prezzo scende ma CVD >= 0 (accumulo silenzioso)
      - VPIN < 0.65 (flusso non tossico — esclude distribuzioni istituzionali)
      - Muro supporto intatto (pressure < 0.5)

    Setup SHORT:
      - Hurst < 0.50
      - Z-Score VWAP > +1.8 (ipercomprato vs VWAP)
      - Prezzo sopra VAH
      - CVD divergente: prezzo sale ma CVD <= 0 (distribuzione)
      - VPIN < 0.65
      - Muro resistenza intatto

    Entry: prezzo corrente (reversal già iniziato)
    SL LONG:  VAL * 0.985  |  SL SHORT: VAH * 1.015
    TP LONG:  POC           |  TP SHORT: POC
    Sizing: 0.6 (setup contrarian, rischio moderato)

    NON opera se:
      - Hurst > 0.55 (trend attivo — la mean reversion fallisce in trend)
      - CVD concorde col movimento (il move è reale, non speculativo)
      - BTC velocity > 0.0005 con CVD concorde (macro breakout in corso)
    """

    def analyze(self, ticker: str, dati_engine: Dict) -> Optional[StrategySignal]:
        try:
            current_price = self._get_current_price(dati_engine)
            if not current_price or current_price <= 0:
                return None

            # ── Indicatori regime ──────────────────────────────────────────
            hurst     = float(dati_engine.get('hurst_exponent', 0.5) or 0.5)
            z_vwap    = float(dati_engine.get('z_score_dist_vwap', 0) or 0)
            vpin      = float(dati_engine.get('vpin', 0) or 0)
            cvd       = float(dati_engine.get('cvd_istantaneo', 0) or 0)
            velocity  = float(dati_engine.get('price_velocity', 0) or 0)

            # ── Volume Profile ─────────────────────────────────────────────
            poc = float(dati_engine.get('poc', 0) or 0)
            vah = float(dati_engine.get('vah', 0) or 0)
            val = float(dati_engine.get('val', 0) or 0)

            # ── Walls ──────────────────────────────────────────────────────
            walls = {}
            try:
                walls = self.engine.get_liquidity_walls(ticker) or {}
            except Exception:
                pass
            pressure_sup = float(walls.get('pressure_supporto', 1.0) or 1.0)
            pressure_res = float(walls.get('pressure_resistenza', 1.0) or 1.0)
            muro_sup     = float(walls.get('muro_supporto', 0) or 0)
            muro_res     = float(walls.get('muro_resistenza', 0) or 0)

            # ── Veto immediati ─────────────────────────────────────────────
            # Opera SOLO in regime mean-reverting puro: Hurst < 0.50
            # Hurst >= 0.50 → mercato random walk o trending → strategia non applicabile
            # Nota: non toccare questa soglia senza aggiornare anche _prepara_dataset
            # in chimera_ml.py che usa la stessa logica per il filtro training
            if hurst >= 0.50:
                return None
            # 3. VPIN alto → flusso tossico/istituzionale, non è reversal retail
            if vpin > 0.65:
                return None

            # ── Veto macro BTC breakout ────────────────────────────────────
            # Se il macro (proxy: velocity alta + CVD concorde) indica breakout,
            # la mean reversion non funziona — BTC trascina tutto.
            _btc_breakout = abs(velocity) > 0.0005 and (
                (velocity > 0 and cvd > 0) or (velocity < 0 and cvd < 0)
            )
            if _btc_breakout:
                return None

            # ── Veto exhaustion ────────────────────────────────────────────
            _phase      = dati_engine.get('entry_phase', '')
            _exhaustion = float(dati_engine.get('exhaustion_score', 0) or 0)
            if _phase == 'ESAURIMENTO' and _exhaustion > 70:
                return None

            # ── SETUP LONG ─────────────────────────────────────────────────
            # Prezzo ipervenduto vs VWAP, CVD divergente (accumulo), sotto VAL
            long_z_ok    = z_vwap < -1.8
            long_val_ok  = val > 0 and current_price < val
            long_cvd_div = cvd >= 0 and velocity < 0  # prezzo scende, CVD non segue
            long_wall_ok = pressure_sup < 0.5

            score_long = 0
            if long_z_ok:    score_long += 35   # segnale primario
            if long_val_ok:  score_long += 25   # conferma struttura volume
            if long_cvd_div: score_long += 25   # divergenza — cuore della strategia
            if long_wall_ok: score_long += 15   # muro supporto regge
            met_long = sum([long_z_ok, long_val_ok, long_cvd_div, long_wall_ok])

            # ── SETUP SHORT ────────────────────────────────────────────────
            short_z_ok    = z_vwap > 1.8
            short_vah_ok  = vah > 0 and current_price > vah
            short_cvd_div = cvd <= 0 and velocity > 0   # prezzo sale, CVD non segue
            short_wall_ok = pressure_res < 0.5

            score_short = 0
            if short_z_ok:    score_short += 35
            if short_vah_ok:  score_short += 25
            if short_cvd_div: score_short += 25
            if short_wall_ok: score_short += 15
            met_short = sum([short_z_ok, short_vah_ok, short_cvd_div, short_wall_ok])

            # ── Precisione decimali asset ──────────────────────────────────
            try:
                from core.asset_list import get_config as _gc
                _prec = int(_gc(ticker).get('precision', 5) or 5)
            except Exception:
                _prec = 5

            # ── DECISIONE LONG ─────────────────────────────────────────────
            # Soglia 70 (almeno 3/4 condizioni) — più bassa di Breakout
            # perché il regime Hurst < 0.50 è già un filtro forte
            if score_long >= 70 and met_long >= 3:
                entry = current_price
                # SL appena sotto muro supporto (o VAL - buffer)
                sl_ref = muro_sup if muro_sup > 0 else (val if val > 0 else entry * 0.985)
                sl = round(sl_ref * 0.985, _prec)
                if sl >= entry:
                    sl = round(entry * 0.985, _prec)
                # TP = POC (centro della value area — obiettivo naturale del reversal)
                tp_ref = poc if poc > 0 and poc > entry else (vah if vah > entry else entry * 1.025)
                tp = round(tp_ref, _prec)
                if tp <= entry:
                    tp = round(entry * 1.025, _prec)

                return StrategySignal(
                    signal='LONG',
                    score=score_long,
                    confidence=met_long / 4.0,
                    entry_price=round(entry, _prec),
                    sl=sl,
                    tp=tp,
                    sizing=0.6,
                    razionale=(
                        f"MeanRev LONG — Hurst {hurst:.2f} | "
                        f"Z-VWAP {z_vwap:.2f} | CVD div {cvd:,.0f} | "
                        f"Price<VAL ({current_price:.5f}<{val:.5f}) | "
                        f"Target POC {poc:.5f}"
                    ),
                    strategy_name="MeanReversion_LONG",
                    components={
                        'hurst': hurst, 'z_vwap': z_vwap, 'vpin': vpin,
                        'cvd': cvd, 'poc': poc, 'vah': vah, 'val': val,
                        'conditions': {
                            'z_ok': long_z_ok, 'val_ok': long_val_ok,
                            'cvd_div': long_cvd_div, 'wall_ok': long_wall_ok,
                        }
                    }
                )

            # ── DECISIONE SHORT ────────────────────────────────────────────
            if score_short >= 70 and met_short >= 3:
                entry = current_price
                sl_ref = muro_res if muro_res > 0 else (vah if vah > 0 else entry * 1.015)
                sl = round(sl_ref * 1.015, _prec)
                if sl <= entry:
                    sl = round(entry * 1.015, _prec)
                tp_ref = poc if poc > 0 and poc < entry else (val if val > 0 and val < entry else entry * 0.975)
                tp = round(tp_ref, _prec)
                if tp >= entry:
                    tp = round(entry * 0.975, _prec)

                return StrategySignal(
                    signal='SHORT',
                    score=score_short,
                    confidence=met_short / 4.0,
                    entry_price=round(entry, _prec),
                    sl=sl,
                    tp=tp,
                    sizing=0.6,
                    razionale=(
                        f"MeanRev SHORT — Hurst {hurst:.2f} | "
                        f"Z-VWAP {z_vwap:.2f} | CVD div {cvd:,.0f} | "
                        f"Price>VAH ({current_price:.5f}>{vah:.5f}) | "
                        f"Target POC {poc:.5f}"
                    ),
                    strategy_name="MeanReversion_SHORT",
                    components={
                        'hurst': hurst, 'z_vwap': z_vwap, 'vpin': vpin,
                        'cvd': cvd, 'poc': poc, 'vah': vah, 'val': val,
                        'conditions': {
                            'z_ok': short_z_ok, 'vah_ok': short_vah_ok,
                            'cvd_div': short_cvd_div, 'wall_ok': short_wall_ok,
                        }
                    }
                )

            return None

        except Exception as e:
            _err.capture(e, sys._getframe().f_code.co_name, {"module": "StrategyEngine"})
            self.logger.error(f"Errore MeanReversionStrategy su {ticker}: {e}")
            return None


class LiquidationCascadeStrategy(BaseStrategy):
    """
    Strategia 6: Entry post-liquidazione a cascata.

    Logica: un picco di liquidazioni (liq_z > 2.0) indica stop-loss colpiti
    in massa. Dopo il flush, il mercato si "pulisce" e rimbalza violentemente
    perché le mani deboli sono state eliminate e i buyer/seller istituzionali
    entrano sul vuoto lasciato dalle liquidazioni.

    Setup LONG (flush ribassista):
      - liq_z > 2.0 (liquidazione anomala)
      - Movimento recente negativo (velocity < -0.0001) → il flush è short-side
      - CVD torna positivo dopo il flush (buyers entrano sul vuoto)
      - Prezzo vicino o sotto muro supporto (distanza < 3%)
      - VPIN > 0.55 (smart money presente nel flush)

    Setup SHORT (flush rialzista):
      - liq_z > 2.0
      - Movimento recente positivo (velocity > 0.0001) → flush long-side
      - CVD torna negativo
      - Prezzo vicino muro resistenza

    Entry: prezzo corrente (entra subito dopo il flush)
    SL: -2% (flush può continuare brevemente)
    TP: +5% (rimbalzo violento tipico post-liquidazione)
    Sizing: 0.5 (alta volatilità post-flush)

    NON opera se:
      - liq_z < 2.0 (liquidazione normale, non anomalia)
      - CVD ancora concorde con il flush (cascata non finita)
      - Hurst > 0.65 in trend forte (il trend mangia il rimbalzo)
    """

    def analyze(self, ticker: str, dati_engine: Dict) -> Optional[StrategySignal]:
        try:
            current_price = self._get_current_price(dati_engine)
            if not current_price or current_price <= 0:
                return None

            # ── Indicatori liquidazione ────────────────────────────────────
            liq_z    = float(dati_engine.get('liq_z', 0) or
                             dati_engine.get('liquidazioni_z_score', 0) or 0)
            liq_24h  = float(dati_engine.get('liquidazioni_24h', 0) or 0)
            cvd      = float(dati_engine.get('cvd_istantaneo', 0) or 0)
            velocity = float(dati_engine.get('price_velocity', 0) or 0)
            vpin     = float(dati_engine.get('vpin', 0) or 0)
            hurst    = float(dati_engine.get('hurst_exponent', 0.5) or 0.5)

            # ── Walls ──────────────────────────────────────────────────────
            walls = {}
            try:
                walls = self.engine.get_liquidity_walls(ticker) or {}
            except Exception:
                pass
            dist_sup = float(walls.get('dist_supporto',
                             walls.get('distanza_perc_supporto', 99)) or 99)
            dist_res = float(walls.get('dist_resistenza',
                             walls.get('distanza_perc_resistenza', 99)) or 99)

            # ── Veto immediati ─────────────────────────────────────────────
            # 1. Nessuna liquidazione anomala
            # liq_z si popola progressivamente (buffer rolling, min 5 cicli).
            # Al primo avvio è 0 → nessun segnale. Fallback su liq_24h assoluto:
            # se liquidazioni > $2M in 24h su qualsiasi asset è un evento reale
            # anche senza z-score (BTC > $10M, altcoin > $500k sono soglie realistiche)
            _liq_soglia_abs = (
                10_000_000 if current_price > 10000 else   # BTC
                2_000_000  if current_price > 100   else   # ETH, SOL
                500_000                                     # altcoin
            )
            _liq_ok = liq_z >= 2.0 or (liq_z == 0 and liq_24h >= _liq_soglia_abs)
            if not _liq_ok:
                return None
            # 2. Trend troppo forte — il rimbalzo viene mangiato dal trend
            if hurst > 0.65:
                return None
            # 3. Veto exhaustion
            _phase = dati_engine.get('entry_phase', '')
            _exhaust = float(dati_engine.get('exhaustion_score', 0) or 0)
            if _phase == 'ESAURIMENTO' and _exhaust > 75:
                return None

            # ── SETUP LONG (flush ribassista) ──────────────────────────────
            # Il flush ha spinto i prezzi giù con forza → ora rimbalzo
            flush_down  = velocity < -0.0001           # movimento negativo recente
            cvd_recover = cvd > 0                      # CVD torna positivo (buyers entrano)
            near_sup    = abs(dist_sup) < 3.0          # siamo vicino al muro supporto
            smart_money = vpin > 0.55                  # smart money nel flush

            score_long = 0
            if liq_z > 3.0:      score_long += 30     # liquidazione molto anomala
            elif liq_z > 2.0:    score_long += 20
            if flush_down:        score_long += 25
            if cvd_recover:       score_long += 25
            if near_sup:          score_long += 15
            if smart_money:       score_long += 10
            # Bonus se liquidazione massiccia in USD assoluti
            if liq_24h > 5_000_000:  score_long += 5
            met_long = sum([flush_down, cvd_recover, near_sup, smart_money])

            # ── SETUP SHORT (flush rialzista) ──────────────────────────────
            flush_up     = velocity > 0.0001
            cvd_collapse = cvd < 0
            near_res     = abs(dist_res) < 3.0

            score_short = 0
            if liq_z > 3.0:      score_short += 30
            elif liq_z > 2.0:    score_short += 20
            if flush_up:          score_short += 25
            if cvd_collapse:      score_short += 25
            if near_res:          score_short += 15
            if smart_money:       score_short += 10
            if liq_24h > 5_000_000:  score_short += 5
            met_short = sum([flush_up, cvd_collapse, near_res, smart_money])

            # ── Precisione decimali ────────────────────────────────────────
            try:
                from core.asset_list import get_config as _gc
                _prec = int(_gc(ticker).get('precision', 5) or 5)
            except Exception:
                _prec = 5

            # ── DECISIONE LONG ─────────────────────────────────────────────
            # Soglia 65: post-flush il segnale è "noisy" → soglia leggermente
            # più bassa perché liq_z > 2 è già un filtro raro e forte
            if score_long >= 65 and met_long >= 3:
                entry = current_price
                sl    = round(entry * 0.98, _prec)   # -2% — flush può avere spike
                tp    = round(entry * 1.05, _prec)   # +5% — rimbalzo violento atteso
                if sl >= entry: sl = round(entry * 0.98, _prec)
                if tp <= entry: tp = round(entry * 1.05, _prec)

                return StrategySignal(
                    signal='LONG',
                    score=score_long,
                    confidence=met_long / 4.0,
                    entry_price=round(entry, _prec),
                    sl=sl,
                    tp=tp,
                    sizing=0.5,
                    razionale=(
                        f"LiqCascade LONG — liq_z {liq_z:.1f} | "
                        f"CVD recover {cvd:,.0f} | "
                        f"Flush velocity {velocity:.5f} | "
                        f"VPIN {vpin:.2f}"
                    ),
                    strategy_name="LiquidationCascade_LONG",
                    components={
                        'liq_z': liq_z, 'liq_24h': liq_24h,
                        'cvd': cvd, 'velocity': velocity,
                        'vpin': vpin, 'hurst': hurst,
                        'conditions': {
                            'flush_down': flush_down, 'cvd_recover': cvd_recover,
                            'near_sup': near_sup, 'smart_money': smart_money,
                        }
                    }
                )

            # ── DECISIONE SHORT ────────────────────────────────────────────
            if score_short >= 65 and met_short >= 3:
                entry = current_price
                sl    = round(entry * 1.02, _prec)
                tp    = round(entry * 0.95, _prec)
                if sl <= entry: sl = round(entry * 1.02, _prec)
                if tp >= entry: tp = round(entry * 0.95, _prec)

                return StrategySignal(
                    signal='SHORT',
                    score=score_short,
                    confidence=met_short / 4.0,
                    entry_price=round(entry, _prec),
                    sl=sl,
                    tp=tp,
                    sizing=0.5,
                    razionale=(
                        f"LiqCascade SHORT — liq_z {liq_z:.1f} | "
                        f"CVD collapse {cvd:,.0f} | "
                        f"Flush velocity {velocity:.5f} | "
                        f"VPIN {vpin:.2f}"
                    ),
                    strategy_name="LiquidationCascade_SHORT",
                    components={
                        'liq_z': liq_z, 'liq_24h': liq_24h,
                        'cvd': cvd, 'velocity': velocity,
                        'vpin': vpin, 'hurst': hurst,
                        'conditions': {
                            'flush_up': flush_up, 'cvd_collapse': cvd_collapse,
                            'near_res': near_res, 'smart_money': smart_money,
                        }
                    }
                )

            return None

        except Exception as e:
            _err.capture(e, sys._getframe().f_code.co_name, {"module": "StrategyEngine"})
            self.logger.error(f"Errore LiquidationCascadeStrategy su {ticker}: {e}")
            return None


class StrategyEngine:
    """
    Aggregatore strategie - combina output e genera decisione finale.
    Pre-filtra opportunità prima di chiamare Gemini AI.
    """
    
    def __init__(self, engine):
        self.engine = engine
        self.logger = logging.getLogger("StrategyEngine")
        self._last_signal: Dict[str, float] = {}  # ticker → timestamp ultimo segnale
        self._cooldown_s = 90  # secondi minimo tra segnali dello stesso ticker

        # Inizializza strategie
        # AUDIT 29/04/2026: ToxicFlowReversalStrategy DISABILITATA.
        # Su 29 trade reali ha mostrato WR 13.8% e PnL_USD -1.97 — anti-edge
        # statisticamente significativo. Causa probabile: in crypto il VPIN tossico
        # tende a CONTINUARE invece di invertire (squeeze), e la strategia fa il
        # contrario di quello che il mercato fa. Tenuta come classe per audit
        # storico ma non più registrata. Per riabilitarla in futuro (es. dopo
        # riscrittura "trade with toxic flow"), reinserire la riga.
        self.strategies = [
            WallBreakoutStrategy(engine),
            # ToxicFlowReversalStrategy(engine),  # DISABILITATA — vedi audit 29/04
            MultiTFConfluenceStrategy(engine),
            FundingSqueezeStrategy(engine),
            MeanReversionStrategy(engine),
            LiquidationCascadeStrategy(engine),
        ]

        self.logger.info(f"✅ Strategy Engine inizializzato con {len(self.strategies)} strategie (ToxicFlowReversal disabilitata per audit)")
    
    def analyze(self, ticker: str, dati_engine: Dict) -> StrategySignal:
        """
        Esegue tutte le strategie e aggrega risultati.
        
        Args:
            ticker: Nome ticker
            dati_engine: Dati da engine.get_market_data()
        
        Returns:
            StrategySignal con score aggregato e decisione finale
        """
        results = []

        # ── VETO SILENZIO ─────────────────────────────────────────────────────
        # Fase SILENZIO = SignalStateEngine non ha ancora rilevato movimento.
        # Nessun flusso istituzionale, nessuna direzione confermata.
        # Dati DB reali: 36 trade STRATEGY in SILENZIO → WR 22%, PnL -9.47$
        # Bloccandoli il portafoglio guadagna +9.47$ e WR sale di 3 punti.
        # Il veto è a monte di tutte le strategie: nessuna può aprire in SILENZIO.
        _entry_phase = str(dati_engine.get('entry_phase', '') or '')
        if _entry_phase == 'SILENZIO':
            self.logger.debug(f"⏸️ [{ticker}] Fase SILENZIO — strategy engine in attesa")
            return StrategySignal(
                signal='FLAT', score=0, confidence=0,
                entry_price=0, sl=0, tp=0, sizing=0,
                razionale="Fase SILENZIO — nessun flusso rilevato, attesa",
                strategy_name="None", components={}
            )

        # ── VETO VPIN BASSO ───────────────────────────────────────────────────
        # VPIN < 0.30 = nessun flusso istituzionale, mercato vuoto senza direzione.
        # Dati DB reali: 129 trade con VPIN < 0.30 → WR 36%, PnL -16.28$
        # Con VPIN 0.50-0.65 il WR sale al 61% — la relazione è quasi lineare.
        # Il veto blocca tutte le strategie: non ha senso cercare breakout o
        # mean reversion in un mercato senza partecipanti istituzionali.
        # ECCEZIONE: FundingSqueeze può operare anche con VPIN basso perché
        # sfrutta lo squilibrio del funding, non il flusso ordini spot.
        _vpin = float(dati_engine.get('vpin', 0) or 0)
        if 0 < _vpin < 0.30:
            self.logger.debug(
                f"⏸️ [{ticker}] VPIN {_vpin:.2f} < 0.30 — mercato senza flusso, strategy engine in attesa"
            )
            return StrategySignal(
                signal='FLAT', score=0, confidence=0,
                entry_price=0, sl=0, tp=0, sizing=0,
                razionale=f"VPIN {_vpin:.2f} < 0.30 — nessun flusso istituzionale",
                strategy_name="None", components={}
            )

        # 1. Esegui ogni strategia
        for strategy in self.strategies:
            try:
                signal = strategy.analyze(ticker, dati_engine)
                if signal and signal.score > 0:
                    results.append(signal)
                    self.logger.debug(
                        f"{strategy.name}: {signal.signal} "
                        f"(score {signal.score:.0f}, conf {signal.confidence:.0%})"
                    )
            except Exception as e:
                _err.capture(e, sys._getframe().f_code.co_name, {"module": "StrategyEngine"})
                self.logger.error(f"Errore {strategy.name} su {ticker}: {e}")
        
        # 2. Nessuna strategia triggerate → FLAT
        if not results:
            return StrategySignal(
                signal='FLAT',
                score=0,
                confidence=0,
                entry_price=0,
                sl=0,
                tp=0,
                sizing=0,
                razionale="No strategy triggered",
                strategy_name="None",
                components={}
            )
        
        # 3. Filtra WATCH signals (non sono trade, sono attesa)
        tradeable = [r for r in results if r.signal in ['LONG', 'SHORT']]
        watch = [r for r in results if r.signal == 'WATCH']
        
        if not tradeable:
            # Solo WATCH signals → ritorna migliore
            best_watch = max(watch, key=lambda x: x.score)
            return best_watch
        
        # 4. Aggrega segnali tradeable
        long_signals = [r for r in tradeable if r.signal == 'LONG']
        short_signals = [r for r in tradeable if r.signal == 'SHORT']
        
        # Scegli direzione con più strategie d'accordo
        if len(long_signals) > len(short_signals):
            active = long_signals
            final_signal = 'LONG'
        elif len(short_signals) > len(long_signals):
            active = short_signals
            final_signal = 'SHORT'
        else:
            # Parità → usa strategia con score più alto
            best_long = max(long_signals, key=lambda x: x.score) if long_signals else None
            best_short = max(short_signals, key=lambda x: x.score) if short_signals else None
            
            if not best_long:
                active = [best_short]
                final_signal = 'SHORT'
            elif not best_short:
                active = [best_long]
                final_signal = 'LONG'
            else:
                if best_long.score >= best_short.score:
                    active = [best_long]
                    final_signal = 'LONG'
                else:
                    active = [best_short]
                    final_signal = 'SHORT'
        
        # 5. Calcola metriche aggregate
        avg_score = sum(s.score for s in active) / len(active)
        # Confidence = % strategie con segnale direzionale che concordano.
        # Dividiamo per len(results) — le strategie che hanno effettivamente
        # prodotto un output — non per len(self.strategies) che è sempre 6.
        # Le strategie che restituiscono None non sono "in disaccordo",
        # semplicemente non avevano condizioni nel loro regime specifico.
        # Esempio: WallBreakout LONG score 85, altre 5 → None.
        # Con /6: confidence=0.17 → sizing dimezzato ingiustamente.
        # Con /len(results): confidence=1.0 → segnale forte trattato come tale.
        n_con_segnale = len(results)  # tutti quelli che hanno prodotto LONG/SHORT/WATCH
        confidence = len(active) / n_con_segnale if n_con_segnale > 0 else 0.0
        
        # 6. Prendi parametri dalla strategia con score più alto
        best = max(active, key=lambda x: x.score)
        
        # 7. Adjust sizing basato su confidence
        # Boost sizing se più strategie concordano, ma mai > 0.9 su asset volatili
        _is_volatile = dati_engine.get('close', 0) < 1 or dati_engine.get('hurst_exponent', 0.5) > 0.65
        _max_sizing  = 0.7 if _is_volatile else 0.9
        adjusted_sizing = best.sizing * min(1.0, confidence + 0.25)
        adjusted_sizing = round(min(adjusted_sizing, _max_sizing), 3)
        
        # 8. Combina razionali
        strategy_names = [s.strategy_name.split('_')[0] for s in active]
        combined_razionale = (
            f"{len(active)}/{len(self.strategies)} strategies {final_signal}: "
            f"{', '.join(strategy_names)} | " + best.razionale
        )
        
        # Validazione finale SL/TP coerenti con la direzione
        _entry = best.entry_price
        _sl    = best.sl
        _tp    = best.tp
        if _entry > 0:
            if final_signal == 'SHORT':
                if _sl > 0 and _sl <= _entry:
                    self.logger.warning(f"⚠️ [{ticker}] SL SHORT {_sl} <= entry {_entry} — corretto a entry*1.015")
                    _sl = round(_entry * 1.015, 5)
                if _tp == 0 or (_tp > 0 and _tp >= _entry):
                    self.logger.warning(f"⚠️ [{ticker}] TP SHORT {_tp} >= entry {_entry} — corretto a entry*0.96")
                    _tp = round(_entry * 0.96, 8)
            elif final_signal == 'LONG':
                if _sl > 0 and _sl >= _entry:
                    self.logger.warning(f"⚠️ [{ticker}] SL LONG {_sl} >= entry {_entry} — corretto a entry*0.985")
                    _sl = round(_entry * 0.985, 5)
                if _tp == 0 or (_tp > 0 and _tp <= _entry):
                    self.logger.warning(f"⚠️ [{ticker}] TP LONG {_tp} <= entry {_entry} — corretto a entry*1.04")
                    _tp = round(_entry * 1.04, 8)

        return StrategySignal(
            signal=final_signal,
            score=round(avg_score, 1),
            confidence=round(confidence, 2),
            entry_price=best.entry_price,
            sl=_sl,
            tp=_tp,
            sizing=adjusted_sizing,
            razionale=combined_razionale,
            strategy_name="Aggregated",
            components={
                'signals': [s.to_dict() for s in results],
                'long_count': len(long_signals),
                'short_count': len(short_signals),
                'watch_count': len(watch),
                'best_strategy': best.strategy_name,
            }
        )
    
    def get_all_data_snapshot(self, ticker: str) -> Dict:
        """
        Recupera tutti i dati necessari in un'unica chiamata.
        Ottimizza performance evitando chiamate multiple.
        """
        return {
            'walls': self.engine.get_liquidity_walls(ticker),
            'order_flow': self.engine.analizza_order_flow(ticker),
            'multi_tf': self.engine.get_market_data_multi_tf(ticker, ['15m', '1h', '4h']),
            'funding': self.engine.get_funding_rate_info(ticker) if 'PF_' in ticker else None,
            'sentiment': self.engine.get_fear_greed(),
            'timestamp': time.time()
        }