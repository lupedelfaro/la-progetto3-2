import threading
# -*- coding: utf-8 -*-
"""
L&A Institutional Bot - ChimeraML
Modulo di Machine Learning incrementale basato su XGBoost.

Filosofia:
  - Ogni trade chiuso diventa un dato di addestramento etichettato
  - Il modello si riaddestra automaticamente ogni N nuovi trade
  - Predice la probabilità di successo PRIMA dell'entry
  - Corregge (non sostituisce) il voto di Gemini
  - Impara dagli errori reali: se entra LONG con CVD negativo e perde,
    lo registra e penalizza quel pattern in futuro

Compatibilità:
  - MacBook Pro 2013: usa solo CPU, nessuna GPU richiesta
  - XGBoost: addestramento < 5 secondi su 500 trade, inferenza < 1ms
  - Nessuna dipendenza pesante: scikit-learn + xgboost + numpy
  - Fallback graceful: se XGBoost non è installato, ritorna 0.5 (neutro)

Installazione:
  pip install xgboost scikit-learn numpy

Integrazione in bot_la.py:
  from core.chimera_ml import ChimeraML
  chimera_ml = ChimeraML()
  # Prima dell'apertura posizione:
  prob, conf = chimera_ml.predici(dati_mercato_chimera, asset, direzione, voto_ia)
  voto_corretto = chimera_ml.correggi_voto(voto_ia, prob, conf)
"""

import logging
import os
import json
import time
import numpy as np
from datetime import datetime
from typing import Tuple, Optional, Dict, Any
import sys
from core.chimera_errors import ErrorTracker

logger = logging.getLogger("ChimeraML")

# ── Costanti di configurazione ──────────────────────────────────────────────
# I file del modello vengono salvati nella root del progetto (stessa cartella di chimera.db)
_BASE_DIR       = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))  # un livello su da core/
MODELLO_PATH    = os.path.join(_BASE_DIR, "chimera_ml_model.json")
SCALER_PATH     = os.path.join(_BASE_DIR, "chimera_ml_scaler.json")
MIN_TRADE_PER_TRAIN = 30    # Trade minimi per primo addestramento
RETRAIN_OGNI_N      = 10    # Riaddestra ogni N trade nuovi
FINESTRA_RECENTE    = 3500  # Ampia finestra per includere tutti i dati storici scaricati
SOGLIA_CONFIDENZA   = 0.35  # Abbassata: con AUC 0.52 la soglia 0.65 non viene mai raggiunta
PESO_ML_MAX         = 0.40  # Peso massimo del ML sul voto finale (60% rimane a Gemini)

# ── Feature utilizzate dal modello ──────────────────────────────────────────
# Queste feature vengono estratte dal chimera_snapshot salvato ad ogni trade.
# Tutte numeriche, nessuna stringa (XGBoost non gestisce categoriche natively).
FEATURE_NAMES = [
    # Order Flow
    "cvd_istantaneo",        # Delta cumulativo volumetrico (USD)
    "vpin",                  # Tossicità del flusso (0-1)
    "order_flow_imbalance",  # Sbilanciamento book (-1 a +1)
    "aggressivita_num",      # BUYERS=1, SELLERS=-1, NEUTRAL=0
    "whale_delta",           # Frazione volume grandi player (0-1)
    "delta_footprint",       # Delta footprint normalizzato
    # Regime di mercato
    "hurst_exponent",        # Persistenza trend (0.1-0.9)
    "kaufman_efficiency",    # Efficienza del movimento (0-1)
    "market_regime_num",     # TRENDING=1, MEAN_REV=-1, UNDEFINED=0
    "struttura_h1_num",      # UPTREND=1, DOWNTREND=-1, LATERALE=0
    # Livelli tecnici
    "z_score",               # Deviazioni standard dalla media
    "z_score_dist_vwap",     # Distanza dal VWAP in deviazioni
    "prob_ritorno_vwap",     # Probabilità ritorno VWAP (0-100)
    "dist_supporto",         # Distanza % dal muro di supporto
    "dist_resistenza",       # Distanza % dal muro di resistenza
    "pressione_muro_supporto",
    "pressione_muro_resistenza",
    "va_position_num",       # Prezzo dentro=0, sopra=1, sotto=-1 la VA settimanale
    "dist_poc_perc",         # Distanza % dal POC settimanale
    # Volatilità e momentum
    "atr_perc",
    "price_velocity",
    "is_explosive_num",
    "book_pressure",
    "rsi_norm",              # RSI normalizzato 0-1
    "candlestick_bias_num",  # BULLISH=1, BEARISH=-1, NEUTRO/INDECISO=0
    # Multi-timeframe (trend allineato con direzione del trade)
    "tf_1h_allineato",       # 1h trend nella direzione del trade (1=sì, -1=contro, 0=neutro)
    "tf_4h_allineato",       # 4h trend nella direzione del trade
    "tf_1d_allineato",       # 1d trend nella direzione del trade
    # Derivati e macro
    "funding_rate",
    "funding_z_score",
    "open_interest",
    "correlazione_driver",
    # Contesto
    "voto_ia",
    "leverage",
    "macro_num",
    "decision_source_num",  # STRATEGY=1, GEMINI=0, STRATEGY+GEMINI=0.5
    # ── Feature aggiuntive v2 ─────────────────────────────────────────────────
    # Livelli istituzionali (Order Block, FVG, S/R, Breaker)
    "dist_ob_vicino",        # distanza % dall'Order Block più vicino (allineato)
    "fvg_attivi_count",      # quanti FVG attivi (0-10, clippato)
    "dist_sr_vicino",        # distanza % dal livello S/R strutturale più vicino
    "resistenza_score",      # qualità del livello di resistenza (0-1)
    "supporto_score",        # qualità del livello di supporto (0-1)
    # Manipolazione e qualità del mercato
    "indice_spoofing",       # ordini fake nel book (0-1, alto = manipolazione)
    "spread_perc",           # spread bid/ask % (alto = liquidità bassa)
    "rolling_volatility",    # volatilità realizzata 20 candele
    # Divergenza CVD (il prezzo va in una direzione, il flusso nell'altra)
    "cvd_divergence",        # 1=divergenza segnale contrarian, 0=assente
    # Trend daily HA (bussola di medio termine)
    "ha_daily_streak_norm",  # streak HA daily normalizzata (1-10 → 0.1-1.0, allineata)
    # Feature temporali (l'ora conta: WR 90% alle 04:00 vs 21% alle 22:00)
    "ora_sin",               # sin(ora*2π/24) — cattura ciclicità oraria
    "fascia_sera",           # 1 se 18-23 UTC (WR storicamente basso)
    # ── Feature strutturali del ciclo v3 ─────────────────────────────────────
    # Posizione nel ciclo di mercato — cambiano la lettura di tutti gli altri segnali
    "ciclo_recupero_norm",   # % recuperato dal minimo 90g (0=fondo, 1=massimo)
    "minimo_qualita_num",    # CAPITOLAZIONE=1, SIGNIFICATIVO=0.5, DEBOLE=0
    "sr_flip_num",           # S/R flip rilevato: 1=res_ex_sup, -1=sup_ex_res, 0=no
    "ciclo_fase_num",        # FONDO=-1, RECUPERO=0, VICINO_MAX=0.5, MASSIMO=1
    # ── Feature ad alto edge v4 ──────────────────────────────────────────────
    "sentinel_trigger_num",  # sentinel attivo — edge 0.60
    "sr_res_dist_perc",      # distanza % S/R resistenza più vicino — edge 0.66
    "sr_sup_dist_perc",      # distanza % S/R supporto più vicino — edge 0.65
    "pivot_weekly_dist",     # distanza % dal pivot settimanale — edge 0.58
    "sr_flip_detected_num",  # S/R flip binario — edge 0.48
    "minimo_volume_ratio",   # volume al minimo / media — edge 0.47
    "exhaustion_score_norm", # score esaurimento normalizzato — edge 0.40
    "cvd_delta_30s_norm",    # delta CVD 30s allineato — edge 0.35
    "cvd_delta_120s_norm",   # delta CVD 120s allineato — edge 0.36
    "iceberg_num",           # iceberg presenti — edge 0.18
]


_err = ErrorTracker("ChimeraML")

class ChimeraML:
    """
    Motore di Machine Learning incrementale per il bot CHIMERA.
    
    Flusso di vita:
      1. Primo avvio: nessun modello → usa solo Gemini (fallback)
      2. Dopo 30 trade reali: primo addestramento XGBoost
      3. Ogni 10 nuovi trade: riaddestramente con dati aggiornati
      4. Ad ogni analisi: predice probabilità di successo
      5. Corregge il voto Gemini in base alla predizione
    """

    def __init__(self, db_manager=None):
        self.db_manager   = db_manager
        self.model        = None      # Modello XGBoost
        self.scaler_mean  = None      # Media per normalizzazione
        self.scaler_std   = None      # Std per normalizzazione
        self.n_trade_al_ultimo_train = 0
        self._xgb_disponibile = False
        self._best_score_cached = 0.0  # AUC persistito dal JSON scaler (fallback dopo load)
        # Memoria reversal: {asset: {direzione_perdente, ts, prob, conf}}
        # Dura max 5 minuti. Usata da brain per boost quando Gemini concorda.
        self._reversal_memory: dict = {}

        # Tenta import XGBoost — fallback graceful se non installato
        try:
            import xgboost as xgb
            self._xgb = xgb
            self._xgb_disponibile = True
            logger.info("✅ XGBoost disponibile. ChimeraML attivo.")
        except ImportError:
            logger.warning(
                "⚠️ XGBoost non installato. ChimeraML in modalità passiva.\n"
                "   Installa con: pip install xgboost scikit-learn"
            )

        # Carica modello esistente se disponibile
        self._carica_modello()

        # FIX: se non c'è un modello salvato ma ci sono già abbastanza trade nel DB,
        # addestriamo subito all'avvio invece di aspettare il primo trade chiuso.
        if self._xgb_disponibile and self.model is None:
            self._verifica_e_riaddestra()

    # ═══════════════════════════════════════════════════════════════════════
    #  API PUBBLICA — chiamata da bot_la.py
    # ═══════════════════════════════════════════════════════════════════════

    def predici(
        self,
        dati_mercato: Dict[str, Any],
        asset: str,
        direzione: str,
        voto_gemini: int,
        macro_sentiment: str = "NEUTRAL",
        decision_source: str = "GEMINI",
        _silent: bool = False
    ) -> Tuple[float, float]:
        """
        Predice la probabilità di successo del trade proposto.
        
        Returns:
            (probabilità, confidenza)
            probabilità: 0.0-1.0 (>0.5 = più probabile WIN)
            confidenza:  0.0-1.0 (quanto è sicuro il modello)
            
        Se il modello non è pronto, ritorna (0.5, 0.0) — neutro, nessuna influenza.
        """
        if not self._xgb_disponibile or self.model is None:
            logger.debug(f"[ML] predici() skip — model is None o XGBoost non disponibile ({asset} {direzione})")
            return 0.5, 0.0

        try:
            features = self._estrai_features(
                dati_mercato, asset, direzione, voto_gemini, macro_sentiment, decision_source
            )
            if features is None:
                return 0.5, 0.0

            X = self._normalizza(np.array([features]))

            # Risolve mismatch feature: il modello salvato puo avere un sottoinsieme
            # delle feature attuali (es. addestrato con 34 feature, codice ora ha 36).
            # Soluzione: seleziona da X SOLO le colonne che il modello conosce,
            # nell'ordine esatto in cui il modello le aspetta.
            _model_feat_names = getattr(self.model, 'feature_names', None)
            if _model_feat_names and list(_model_feat_names) != FEATURE_NAMES:
                feat_indices = []
                missing = []
                for fn in _model_feat_names:
                    if fn in FEATURE_NAMES:
                        feat_indices.append(FEATURE_NAMES.index(fn))
                    else:
                        missing.append(fn)
                if missing:
                    # Feature nel modello non piu presenti nel codice — forza retrain
                    logger.warning(
                        f"[ML] Feature nel modello non piu presenti nel codice: {missing}. "
                        f"Forzo retrain."
                    )
                    self.model = None
                    self._verifica_e_riaddestra(force_check=True)
                    return 0.5, 0.0
                # Seleziona solo le colonne giuste da X (es. 34 su 36)
                X = X[:, feat_indices]
                logger.debug(
                    f"[ML] Feature align: modello={len(_model_feat_names)}, "
                    f"codice={len(FEATURE_NAMES)} — selezionate {len(feat_indices)}"
                )
                _model_feat_names = list(_model_feat_names)
            else:
                _model_feat_names = FEATURE_NAMES

            dmatrix = self._xgb.DMatrix(X, feature_names=_model_feat_names)
            prob_win = float(self.model.predict(dmatrix)[0])

            # Confidenza = distanza dalla soglia 0.5 (quanto è deciso il modello)
            confidenza = abs(prob_win - 0.5) * 2  # 0.0 = incerto, 1.0 = certissimo

            if not _silent:
                logger.info(
                    f"🤖 [ML] {asset} {direzione} | P(WIN): {prob_win:.1%} | "
                    f"Confidenza: {confidenza:.1%} | Voto Gemini: {voto_gemini}"
                )
            return prob_win, confidenza

        except Exception as e:
            _err.capture(e, sys._getframe().f_code.co_name, {"module": "ChimeraML"})
            logger.error(f"❌ Errore predizione ML per {asset}: {e}")
            return 0.5, 0.0

    def correggi_voto(
        self,
        voto_gemini: int,
        prob_win: float,
        confidenza: float
    ) -> int:
        """
        Corregge il voto di Gemini usando la predizione ML.
        
        Gate di qualità: il modello interviene SOLO se:
        1. AUC del modello >= 0.52 (meglio del random)
        2. Confidenza >= SOGLIA_CONFIDENZA (0.65)
        
        Se AUC < 0.52, il modello è in modalità osservazione:
        predice e salva i dati per il training futuro, ma non
        modifica nessuna decisione.
        """
        if self.model is None:
            return voto_gemini

        # Gate AUC: verifica che il modello sia meglio del random.
        # model.attributes().get('best_score') puo restituire 0 dopo load_model()
        # in alcune versioni XGBoost (attributi non deserializzati). Fallback su _best_score_cached.
        try:
            best_score = float(
                self.model.attributes().get('best_score', 0) or 0
            )
            if best_score == 0.0:
                # Attributi persi dopo reload — usa valore salvato nel JSON scaler
                best_score = getattr(self, '_best_score_cached', 0.0)
                if best_score == 0.0:
                    # Nessun fallback: se scaler caricato il modello era stato validato
                    best_score = 0.52 if self.scaler_mean is not None else 0.0
                logger.debug(f"[ML] best_score=0 da attributes() — fallback={best_score:.3f}")
            if best_score < 0.51:
                logger.debug(
                    f"[ML PASSIVO] AUC={best_score:.3f} < 0.51. Modello in osservazione, voto invariato."
                )
                return voto_gemini
        except Exception as e:
            _err.capture(e, sys._getframe().f_code.co_name, {"module": "ChimeraML", "context": "gate_auc"})
            logger.warning(f"[ML] Errore lettura best_score: {e} — uso fallback")
            best_score = getattr(self, '_best_score_cached', 0.52 if self.scaler_mean is not None else 0.0)
            if best_score < 0.51:
                return voto_gemini

        # Gate confidenza
        if confidenza < SOGLIA_CONFIDENZA:
            return voto_gemini

        try:
            voto_ml = prob_win * 10.0
            peso_ml = min(
                PESO_ML_MAX,
                (confidenza - SOGLIA_CONFIDENZA) / (1.0 - SOGLIA_CONFIDENZA) * PESO_ML_MAX
            )
            peso_gemini = 1.0 - peso_ml
            voto_finale = (voto_gemini * peso_gemini) + (voto_ml * peso_ml)
            voto_finale = max(0, min(10, round(voto_finale)))

            if voto_finale != voto_gemini:
                diff = voto_finale - voto_gemini
                segno = "+" if diff > 0 else ""
                logger.info(
                    f"🎯 [ML CORREZIONE] Voto: {voto_gemini} → {voto_finale} "
                    f"({segno}{diff}) | P(WIN)={prob_win:.1%} | "
                    f"AUC={best_score:.3f} | Peso ML={peso_ml:.0%}"
                )

            return int(voto_finale)

        except Exception as e:
            _err.capture(e, sys._getframe().f_code.co_name, {"module": "ChimeraML"})
            logger.error(f"❌ Errore correzione voto ML: {e}")
            return voto_gemini

    def registra_trade_chiuso(self, trade_data: Dict[str, Any]) -> None:
        """
        Registra un trade chiuso per l'apprendimento.
        Chiamare da TradeManager dopo ogni chiusura reale o ghost.
        Arricchisce il chimera_snapshot con tutti i campi necessari al modello.
        """
        if not self._xgb_disponibile:
            return

        esito = trade_data.get("esito", "")
        if esito not in ("WIN", "LOSS"):
            return

        snapshot = trade_data.get("chimera_snapshot", {})
        if not snapshot:
            logger.debug("⚠️ Trade senza chimera_snapshot — impossibile imparare.")
            return

        # ── ARRICCHIMENTO SNAPSHOT con campi dal trade record ────────
        # Questi campi sono nel trade ma non nello snapshot — il modello
        # li legge da entrambi i posti, ma meglio averli anche nello snapshot
        # per coerenza e per i ghost trade
        _enriched = False
        for campo, chiave_trade in [
            ('voto_ia',          'voto_ia'),
            ('leverage',         'leverage'),
            ('decision_source',  'decision_source'),
            ('entry_phase',      'entry_phase'),
            ('exhaustion_score', 'exhaustion_score'),
            ('ml_prob_win',      'ml_prob_win'),
            ('ml_confidenza',    'ml_confidenza'),
            ('macro_sentiment',  'macro_sentiment'),
            ('fonte',            'fonte'),
        ]:
            if campo not in snapshot and chiave_trade in trade_data:
                snapshot[campo] = trade_data[chiave_trade]
                _enriched = True

        if _enriched:
            trade_data['chimera_snapshot'] = snapshot
            # Aggiorna nel DB
            try:
                from core.database_manager import db_manager
                storico = db_manager.get_storico()
                trade_id = trade_data.get('ordine_id') or trade_data.get('entry_id')
                if trade_id:
                    for i, t in enumerate(storico):
                        if t.get('ordine_id') == trade_id or t.get('entry_id') == trade_id:
                            storico[i] = trade_data
                            db_manager.save_storico(storico)
                            logger.debug(f"📚 [ML] Snapshot arricchito e aggiornato nel DB per {trade_data.get('asset')}")
                            break
            except Exception as e_enrich:
                _err.capture(e_enrich, "registra_trade_chiuso", {"asset": trade_data.get("asset"), "context": "enrichment_snapshot_db"})
                logger.debug(f"⚠️ Arricchimento snapshot DB: {e_enrich}")

        fonte = trade_data.get('fonte', '')
        is_ghost = str(fonte).upper() in ('VIRTUAL_BRAIN', 'GHOST')
        tipo_log = '👻 GHOST' if is_ghost else '📚 REALE'

        logger.info(
            f"{tipo_log} [ML] Trade registrato per apprendimento: "
            f"{trade_data.get('asset', '?')} {trade_data.get('direzione', '?')} → {esito} "
            f"(voto={trade_data.get('voto_ia','?')} fonte={fonte or 'LIVE'})"
        )

        # Riaddestra immediatamente dopo ogni trade reale chiuso
        # Ghost: riaddestra ogni RETRAIN_OGNI_N come prima
        if not is_ghost:
            self._verifica_e_riaddestra(force_check=True)
        else:
            self._verifica_e_riaddestra()

    def registra_segnale_sfavorevole(
        self,
        asset: str,
        direzione: str,
        prob_win: float,
        confidenza: float
    ) -> None:
        """
        Registra in memoria che ML ha visto un segnale sfavorevole
        per questa direzione su questo asset.
        Chiamato da brain_la dopo il ML sentinel quando prob_win < 0.35.
        La memoria scade dopo 5 minuti (300 secondi).
        """
        import time as _time
        self._reversal_memory[asset] = {
            'direzione_perdente': str(direzione).upper(),
            'ts':                 _time.time(),
            'prob_win':           prob_win,
            'confidenza':         confidenza,
        }
        logger.info(
            f"🔁 [ML REVERSAL MEM] {asset}: direzione {direzione} sfavorevole "
            f"(P(WIN)={prob_win:.1%} conf={confidenza:.1%}) — memoria attiva 5min"
        )

    def controlla_reversal_confirm(
        self,
        asset: str,
        direzione_proposta: str,
        prob_win: float,
        confidenza: float
    ) -> tuple:
        """
        Controlla se la direzione proposta da Gemini è il reversal
        di un segnale precedentemente classificato come sfavorevole.

        Returns:
            (confermato: bool, boost: int, motivo: str)
            boost = +1 se confermato, 0 altrimenti
        """
        import time as _time
        rec = self._reversal_memory.get(asset)
        if not rec:
            return False, 0, ''

        # Scadenza 5 minuti
        if _time.time() - rec['ts'] > 300:
            del self._reversal_memory[asset]
            return False, 0, ''

        dir_perdente  = rec['direzione_perdente']
        dir_proposta  = str(direzione_proposta).upper()

        # Normalizza LONG/BUY e SHORT/SELL
        _norm = lambda d: 'LONG' if d in ('LONG','BUY') else 'SHORT'
        dir_perdente_n = _norm(dir_perdente)
        dir_proposta_n = _norm(dir_proposta)

        # La direzione proposta deve essere l'opposta di quella perdente
        opposte = {('LONG','SHORT'), ('SHORT','LONG')}
        if (dir_perdente_n, dir_proposta_n) not in opposte:
            return False, 0, ''

        # ML deve anche essere abbastanza convinto sulla nuova direzione
        if confidenza < 0.20:
            return False, 0, f'confidenza ML troppo bassa ({confidenza:.1%}) per confermare reversal'

        eta = int(_time.time() - rec['ts'])
        motivo = (
            f"ML REVERSAL CONFIRM: {dir_perdente_n} era sfavorevole "
            f"({eta}s fa, P(WIN)={rec['prob_win']:.1%}) → "
            f"{dir_proposta_n} confermato (P(WIN)={prob_win:.1%} conf={confidenza:.1%})"
        )
        logger.info(f"🎯 [{asset}] {motivo}")

        # Pulisce memoria dopo conferma
        del self._reversal_memory[asset]
        return True, 1, motivo

    def stato(self) -> Dict[str, Any]:
        """Restituisce lo stato attuale del modello ML."""
        # Struttura base con valori di default — stesse chiavi sempre,
        # indipendentemente da se XGBoost è installato o meno.
        base = {
            "attivo":            False,
            "trade_disponibili": 0,
            "trade_per_train":   MIN_TRADE_PER_TRAIN,
            "n_features":        len(FEATURE_NAMES),
            "modello_salvato":   False,
            "prossimo_retrain":  MIN_TRADE_PER_TRAIN,
            "motivo":            "",
        }

        if not self._xgb_disponibile:
            base["motivo"] = "XGBoost non installato (pip install xgboost scikit-learn)"
            return base

        try:
            from core.database_manager import db_manager
            storico = db_manager.get_storico()
            DATA_TRAINING = "2026-03-20"
            n_disponibili = len([
                t for t in storico
                if t.get("chimera_snapshot")
                and t.get("esito") in ("WIN", "LOSS")
                and t.get("direzione")
                and str(t.get('fonte', '')).upper() != 'STORICO_SIMULATO'
                and str(t.get("data_apertura", "") or t.get("data_chiusura", "")) >= DATA_TRAINING
                # FIX-M: allineato al filtro nuovo (no entry_phase, sì >=10 chiavi)
                and len(t.get("chimera_snapshot") or {}) >= 10
            ])
        except Exception:
            n_disponibili = 0

        base.update({
            "attivo":            self.model is not None,
            "trade_disponibili": n_disponibili,
            "modello_salvato":   os.path.exists(MODELLO_PATH),
            "prossimo_retrain":  max(0, RETRAIN_OGNI_N - (n_disponibili - self.n_trade_al_ultimo_train)),
        })

        # Aggiungi AUC e stato attivazione
        if self.model is not None:
            try:
                auc = float(self.model.attributes().get('best_score', 0) or 0)
                best_iter = int(self.model.attributes().get('best_iteration', 0) or 0)
                base['auc'] = round(auc, 4)
                base['best_iteration'] = best_iter
                if auc >= 0.52:
                    base['ml_attivo_decisioni'] = True
                    base['motivo'] = f"Attivo — AUC {auc:.3f} ≥ 0.52"
                else:
                    base['ml_attivo_decisioni'] = False
                    base['motivo'] = f"Osservazione passiva — AUC {auc:.3f} < 0.52 (serve più dati)"
            except Exception:
                base['ml_attivo_decisioni'] = False
                base['motivo'] = "AUC non disponibile"

        return base

    # ═══════════════════════════════════════════════════════════════════════
    #  ADDESTRAMENTO
    # ═══════════════════════════════════════════════════════════════════════

    def addestra(self, force: bool = False) -> bool:
        """
        Addestra o riaddestra il modello XGBoost sul dataset corrente.
        
        Args:
            force: se True, addestra anche se ci sono pochi dati
            
        Returns:
            True se l'addestramento è riuscito
        """
        if not self._xgb_disponibile:
            return False

        try:
            result = self._prepara_dataset()
            X, y, n_totali, w = result if len(result) == 4 else (*result, None)
            if X is None:
                return False

            n_win  = int(y.sum())
            n_loss = n_totali - n_win

            if n_totali < MIN_TRADE_PER_TRAIN and not force:
                logger.info(
                    f"📊 [ML] Dataset troppo piccolo: {n_totali}/{MIN_TRADE_PER_TRAIN} trade. "
                    f"Il modello si attiverà automaticamente."
                )
                return False

            if n_win == 0 or n_loss == 0:
                logger.warning(
                    f"⚠️ [ML] Dataset sbilanciato (WIN={n_win}, LOSS={n_loss}). "
                    f"Serve più variabilità per addestrare."
                )
                return False

            logger.info(
                f"🏋️ [ML] Addestramento su {n_totali} trade "
                f"(WIN={n_win} {n_win/n_totali:.0%}, LOSS={n_loss} {n_loss/n_totali:.0%})..."
            )

            # Normalizzazione (Z-Score per feature continue)
            self.scaler_mean = X.mean(axis=0)
            self.scaler_std  = X.std(axis=0)
            self.scaler_std[self.scaler_std == 0] = 1.0
            X_norm = (X - self.scaler_mean) / self.scaler_std

            # Cross-validation TEMPORALE
            n_train = int(n_totali * 0.8)
            X_train, X_val = X_norm[:n_train], X_norm[n_train:]
            y_train, y_val = y[:n_train], y[n_train:]
            w_train = w[:n_train] if w is not None else None

            dtrain = self._xgb.DMatrix(
                X_train, label=y_train,
                feature_names=FEATURE_NAMES,
                weight=w_train          # trade recenti/strutturati pesano di più
            )
            dval = self._xgb.DMatrix(X_val, label=y_val, feature_names=FEATURE_NAMES)

            scale_pw = n_loss / n_win if n_win > 0 else 1.0

            params = {
                "objective":        "binary:logistic",
                "eval_metric":      "auc",
                "max_depth":        4,
                "learning_rate":    0.05,
                "subsample":        0.8,
                "colsample_bytree": 0.8,
                "min_child_weight": 3,
                "gamma":            0.1,
                "scale_pos_weight": scale_pw,
                "nthread":          2,
                "seed":             42,
                "verbosity":        0,
            }

            t0 = time.time()
            self.model = self._xgb.train(
                params,
                dtrain,
                num_boost_round=300,
                evals=[(dval, "val")],
                early_stopping_rounds=20,
                verbose_eval=False,
            )
            elapsed = time.time() - t0

            # Valutazione sul validation set
            y_pred_val = self.model.predict(dval)
            auc = self._calcola_auc(y_val, y_pred_val)
            self._best_score_cached = auc  # Persiste per fallback in correggi_voto() dopo reload

            # Feature importance: le prime 5 più importanti
            importance = self.model.get_score(importance_type="gain")
            top5 = sorted(importance.items(), key=lambda x: x[1], reverse=True)[:5]
            top5_str = ", ".join(f"{k}({v:.0f})" for k, v in top5)

            logger.info(
                f"✅ [ML] Addestramento completato in {elapsed:.1f}s | "
                f"AUC val: {auc:.3f} | "
                f"Top features: {top5_str}"
            )

            self.n_trade_al_ultimo_train = n_totali  # totale dataset (train+val)
            self._salva_modello()

            return True

        except Exception as e:
            _err.capture(e, sys._getframe().f_code.co_name, {"module": "ChimeraML"})
            logger.error(f"❌ Errore addestramento ML: {e}")
            import traceback
            traceback.print_exc()
            return False

    # ═══════════════════════════════════════════════════════════════════════
    #  FEATURE ENGINEERING
    # ═══════════════════════════════════════════════════════════════════════

    def _estrai_features(
        self,
        dati: Dict[str, Any],
        asset: str,
        direzione: str,
        voto_ia: int,
        macro_sentiment: str,
        decision_source: str = "GEMINI"
    ) -> Optional[list]:
        """
        Estrae e normalizza le feature numeriche dal dati_mercato.
        Gestisce i valori mancanti con default conservativi.
        """
        try:
            close = float(dati.get("close", 0))
            atr   = float(dati.get("atr",   0))

            # Conversioni categoriche → numeriche
            agg = dati.get("aggressivita_order_flow", dati.get("aggressivita_flow", "NEUTRAL"))
            agg_num = 1.0 if agg == "BUYERS" else (-1.0 if agg == "SELLERS" else 0.0)

            regime = dati.get("market_regime", "UNDEFINED")
            regime_num = 1.0 if regime == "TRENDING" else (-1.0 if regime == "MEAN_REVERSION" else 0.0)

            macro_upper = str(macro_sentiment).upper()
            macro_num = 1.0 if macro_upper == "BULLISH" else (-1.0 if macro_upper == "BEARISH" else 0.0)

            # Se SHORT, invertiamo il segno di CVD e OFI per rendere il modello
            # direzione-agnostico (impara "flusso concorde" non "flusso rialzista")
            direzione_norm = 1.0 if direzione.upper() in ("BUY", "LONG") else -1.0
            cvd_raw = float(dati.get("cvd_istantaneo", 0))
            ofi_raw = float(dati.get("order_flow_imbalance", 0))
            vel_raw = float(dati.get("price_velocity", 0))

            cvd_allineato = cvd_raw * direzione_norm
            ofi_allineato = ofi_raw * direzione_norm
            vel_allineato = vel_raw * direzione_norm

            # Leverage: None (SPOT) → 1
            lev_raw = dati.get("leverage", 1)
            leverage = float(lev_raw) if lev_raw is not None else 1.0

            # OI normalizzato (evita valori astronomici)
            oi_raw = float(dati.get("open_interest", 0))
            oi_norm = min(oi_raw / 1e8, 10.0) if oi_raw > 0 else 0.0

            # ATR % del prezzo
            atr_perc = (atr / close * 100) if close > 0 else 0.0

            # ── RSI: zero = dato mancante, non RSI=0 ─────────────────────────
            # Quando RSI non è stato calcolato viene salvato 0.
            # Usiamo 50 come neutro (nessuna informazione) invece di 0
            # per evitare che il modello apprenda "RSI=0 → LOSS"
            rsi_raw = float(dati.get("rsi", 0) or 0)
            rsi_val = rsi_raw if rsi_raw > 0 else 50.0  # 50 = neutro se mancante

            # ── z_score: zero può essere reale O mancante ────────────────────
            # Usiamo il campo z_score_dist_vwap (sempre presente) come proxy
            # quando z_score è 0. Distingue il caso "prezzo esattamente sulla media"
            # dal caso "dato non calcolato"
            z_score_raw = float(dati.get("z_score", 0) or 0)

            # ── Nuove feature v2 ─────────────────────────────────────────────

            # Order Block più vicino (allineato alla direzione)
            ob_bull = float(dati.get("ob_bull_piu_vicino", 0) or 0)
            ob_bear = float(dati.get("ob_bear_piu_vicino", 0) or 0)
            if direzione_norm > 0:  # LONG: interessa OB bullish (supporto)
                ob_ref = ob_bull
            else:  # SHORT: interessa OB bearish (resistenza)
                ob_ref = ob_bear
            dist_ob = abs(close - ob_ref) / close * 100 if (ob_ref > 0 and close > 0) else 5.0
            dist_ob_vicino = np.clip(dist_ob, 0, 10)

            # FVG attivi (Fair Value Gap — livelli istituzionali da riempire)
            fvg_count = float(dati.get("fvg_attivi_count", 0) or 0)

            # S/R strutturale più vicino
            res_str = float(dati.get("res_strutturale", dati.get("sr_res_piu_vicina", 0)) or 0)
            sup_str = float(dati.get("sup_strutturale", dati.get("sr_sup_piu_vicina", 0)) or 0)
            if direzione_norm > 0:  # LONG: interessa supporto strutturale
                sr_ref = sup_str
            else:  # SHORT: interessa resistenza strutturale
                sr_ref = res_str
            dist_sr = abs(close - sr_ref) / close * 100 if (sr_ref > 0 and close > 0) else 5.0
            dist_sr_vicino = np.clip(dist_sr, 0, 10)

            # Score qualità livelli
            res_score = float(dati.get("resistenza_score", 0) or 0)
            sup_score = float(dati.get("supporto_score", 0) or 0)

            # Manipolazione e qualità mercato
            spoofing = float(dati.get("indice_spoofing", 0) or 0)
            spread_p = float(dati.get("spread_perc", 0) or 0)
            roll_vol = float(dati.get("rolling_volatility", 0) or 0)

            # Divergenza CVD
            cvd_div_raw = dati.get("cvd_divergence", 0)
            cvd_div = 1.0 if cvd_div_raw else 0.0

            # HA daily streak (allineata alla direzione)
            ha_colore = str(dati.get("ha_daily_colore", "") or "").upper()
            ha_streak = float(dati.get("ha_daily_streak", 0) or 0)
            # Verde + streak alta = trend rialzista forte → favorevole per LONG
            if ha_colore == "VERDE":
                ha_streak_allineata = ha_streak / 10.0  # normalizza 0-1
            elif ha_colore == "ROSSO":
                ha_streak_allineata = -ha_streak / 10.0  # negativo = ribassista
            else:
                ha_streak_allineata = 0.0
            ha_streak_norm = np.clip(ha_streak_allineata * direzione_norm, -1, 1)

            # Feature temporali
            import math as _math
            data_str = dati.get("data_apertura", "") or dati.get("ts_apertura", "") or ""
            try:
                from datetime import datetime as _dt, timezone as _tz
                if data_str:
                    _ora = _dt.fromisoformat(str(data_str)[:19]).hour
                else:
                    _ora = _dt.now(_tz.utc).hour
            except Exception:
                _ora = 12  # neutro se non disponibile
            ora_sin = _math.sin(_ora * 2 * _math.pi / 24)  # ciclico: 04:00 ≈ 1, 16:00 ≈ -1
            fascia_sera = 1.0 if 18 <= _ora <= 23 else 0.0

            features = [
                # Order Flow
                np.clip(cvd_allineato / 1e6, -10, 10),
                float(dati.get("vpin", 0.5)),
                np.clip(ofi_allineato, -1, 1),
                agg_num * direzione_norm,
                float(dati.get("whale_delta", 0)),
                np.clip(float(dati.get("delta_footprint", 0)) * direzione_norm, -1, 1),
                # Regime
                float(dati.get("hurst_exponent", 0.5)),
                float(dati.get("kaufman_efficiency", 0.5)),
                regime_num,
                # struttura H1 numerica
                (1.0 if str(dati.get("struttura_h1","")).upper() == "UPTREND"
                 else -1.0 if str(dati.get("struttura_h1","")).upper() == "DOWNTREND"
                 else 0.0) * direzione_norm,  # allineata alla direzione
                # Livelli
                np.clip(float(dati.get("z_score", 0)), -5, 5),
                np.clip(float(dati.get("z_score_dist_vwap", 0)), -5, 5),
                float(dati.get("prob_ritorno_vwap", 50)) / 100.0,
                np.clip(float(dati.get("dist_supporto", 5)), 0, 10),
                np.clip(float(dati.get("dist_resistenza", 5)), 0, 10),
                float(dati.get("pressione_muro_supporto", 0)),
                float(dati.get("pressione_muro_resistenza", 0)),
                # Posizione nella VA settimanale
                (lambda va, vah, val, close: (
                    1.0 if close > vah > 0 else
                   -1.0 if close < val > 0 else
                    0.0
                ))(
                    dati.get("va_position"),
                    float(dati.get("vah", 0)),
                    float(dati.get("val", 0)),
                    float(dati.get("close", 0))
                ) * direzione_norm,
                # Distanza % dal POC settimanale — allineata alla direzione
                np.clip(
                    (float(dati.get("close",0)) - float(dati.get("poc",0)))
                    / max(float(dati.get("poc",1)), 1) * 100 * direzione_norm,
                    -10, 10
                ),
                # Volatilità
                np.clip(atr_perc, 0, 10),
                np.clip(vel_allineato * 1000, -5, 5),
                1.0 if dati.get("is_explosive") else 0.0,
                float(dati.get("book_pressure", 0.5)),
                # RSI normalizzato e allineato (usa 50 se mancante, non 0)
                (rsi_val / 100.0 - 0.5) * direzione_norm * 2,
                # Candlestick bias
                (1.0 if str(dati.get("candlestick_bias","")).upper() == "BULLISH"
                 else -1.0 if str(dati.get("candlestick_bias","")).upper() == "BEARISH"
                 else 0.0) * direzione_norm,
                # Multi-timeframe — ogni TF: +1 allineato, -1 contro, 0 assente/neutro
                # IMPORTANTE: se multi_tf non c'è per questo asset, usa 0 (neutro)
                # non -1 (che sembrerebbe contro-trend). Il modello deve imparare
                # che "dato assente" ≠ "trend contro".
                (lambda tf: (
                    1.0 if (direzione_norm > 0 and tf.get("trend_dir","") == "UP")
                    else -1.0 if (direzione_norm > 0 and tf.get("trend_dir","") == "DOWN")
                    else 1.0 if (direzione_norm < 0 and tf.get("trend_dir","") == "DOWN")
                    else -1.0 if (direzione_norm < 0 and tf.get("trend_dir","") == "UP")
                    else 0.0
                ))(dati.get("multi_tf", {}).get("1h", {}) if dati.get("multi_tf") else {}),
                (lambda tf: (
                    1.0 if (direzione_norm > 0 and tf.get("trend_dir","") == "UP")
                    else -1.0 if (direzione_norm > 0 and tf.get("trend_dir","") == "DOWN")
                    else 1.0 if (direzione_norm < 0 and tf.get("trend_dir","") == "DOWN")
                    else -1.0 if (direzione_norm < 0 and tf.get("trend_dir","") == "UP")
                    else 0.0
                ))(dati.get("multi_tf", {}).get("4h", {}) if dati.get("multi_tf") else {}),
                (lambda tf: (
                    1.0 if (direzione_norm > 0 and tf.get("trend_dir","") == "UP")
                    else -1.0 if (direzione_norm > 0 and tf.get("trend_dir","") == "DOWN")
                    else 1.0 if (direzione_norm < 0 and tf.get("trend_dir","") == "DOWN")
                    else -1.0 if (direzione_norm < 0 and tf.get("trend_dir","") == "UP")
                    else 0.0
                ))(dati.get("multi_tf", {}).get("1d", {}) if dati.get("multi_tf") else {}),
                # Derivati
                np.clip(float(dati.get("funding_rate", 0)) * 10000, -5, 5),
                np.clip(float(dati.get("funding_z_score", 0)), -5, 5),
                oi_norm,
                np.clip(float(dati.get("correlazione_driver", 1.0)), -1, 1),
                # Contesto
                float(voto_ia) / 10.0,
                np.clip(leverage, 1, 10) / 10.0,
                macro_num,
                # Decision Source
                (1.0 if decision_source == "STRATEGY"
                 else 0.5 if decision_source == "STRATEGY+GEMINI"
                 else 0.0),
                # ── Feature aggiuntive v2 ─────────────────────────────────────
                dist_ob_vicino,
                np.clip(fvg_count, 0, 10),
                dist_sr_vicino,
                np.clip(res_score, 0, 1),
                np.clip(sup_score, 0, 1),
                np.clip(spoofing, 0, 1),
                np.clip(spread_p, 0, 2),
                np.clip(roll_vol * 100, 0, 5),
                cvd_div,
                ha_streak_norm,
                ora_sin,
                fascia_sera,
                # ── Feature strutturali del ciclo v3 ─────────────────────────
                # Normalizzano il contesto di ciclo per XGBoost
                np.clip(float(dati.get("ciclo_recupero_pct", 50) or 50) / 100.0, 0, 1),
                (1.0 if str(dati.get("minimo_qualita", "") or "").upper() == "CAPITOLAZIONE"
                 else 0.5 if str(dati.get("minimo_qualita", "") or "").upper() == "SIGNIFICATIVO"
                 else 0.0),
                (1.0 if str(dati.get("sr_flip_tipo", "") or "") == "RESISTENZA_EX_SUPPORTO"
                 else -1.0 if str(dati.get("sr_flip_tipo", "") or "") == "SUPPORTO_EX_RESISTENZA"
                 else 0.0),
                ({"FONDO": -1.0, "RECUPERO_INIZIALE": -0.5, "RECUPERO_MEDIO": 0.0,
                  "VICINO_MASSIMO": 0.5, "MASSIMO": 1.0}.get(
                    str(dati.get("ciclo_fase", "") or ""), 0.0)),
                # ── Feature ad alto edge v4 ───────────────────────────────────
                1.0 if dati.get("sentinel_trigger") else 0.0,
                np.clip(abs(close - float(dati.get("sr_res_piu_vicina", 0) or 0)) / max(close, 1) * 100, 0, 10),
                np.clip(abs(close - float(dati.get("sr_sup_piu_vicina", 0) or 0)) / max(close, 1) * 100, 0, 10),
                np.clip(abs(close - float(dati.get("pivot_weekly", 0) or 0)) / max(close, 1) * 100, 0, 10),
                1.0 if dati.get("sr_flip_detected") else 0.0,
                np.clip(float(dati.get("minimo_volume_ratio", 1) or 1), 0, 5),
                np.clip(float(dati.get("exhaustion_score", 0) or 0), 0, 10),
                np.clip(float(dati.get("cvd_delta_30s", 0) or 0) * direzione_norm / 1e5, -5, 5),
                np.clip(float(dati.get("cvd_delta_120s", 0) or 0) * direzione_norm / 1e5, -5, 5),
                1.0 if dati.get("iceberg_presenti") else 0.0,
            ]

            # Sanity check: lunghezza deve corrispondere a FEATURE_NAMES
            assert len(features) == len(FEATURE_NAMES), \
                f"Feature count mismatch: {len(features)} vs {len(FEATURE_NAMES)}"

            # Sostituisce NaN/Inf con 0 (dati mancanti = neutro)
            features = [0.0 if (np.isnan(f) or np.isinf(f)) else float(f) for f in features]

            return features

        except Exception as e:
            _err.capture(e, sys._getframe().f_code.co_name, {"module": "ChimeraML"})
            logger.error(f"❌ Errore estrazione feature: {e}")
            return None

    def _prepara_dataset(self):
        """
        Prepara X (feature matrix) e y (label) dal database storico.
        Legge sia storico_trades che ghost_trades (CLOSED) — nessun dato viene perso.
        """
        try:
            from core.database_manager import db_manager
            storico = db_manager.get_storico()
        except Exception as e:
            _err.capture(e, sys._getframe().f_code.co_name, {"module": "ChimeraML"})
            logger.error(f"❌ Impossibile caricare storico dal DB: {e}")
            return None, None, 0

        # ── Aggiungi ghost CLOSED dalla tabella ghost_trades ──────────────────
        # I ghost CLOSED non vengono copiati in storico_trades per design —
        # XGBoost li legge direttamente qui così non si perdono mai.
        try:
            ghosts_dict = db_manager.get_ghosts()
            ghost_closed = []
            for g in ghosts_dict.values():
                if g.get('stato') != 'CLOSED':
                    continue
                if g.get('esito') not in ('WIN', 'LOSS'):
                    continue
                snap = g.get('snapshot', {})
                if not snap:
                    continue
                # Costruisce un record compatibile con lo storico
                ghost_closed.append({
                    "asset":            g.get("asset", ""),
                    "direzione":        g.get("direzione", ""),
                    "voto_ia":          g.get("voto_ia", 5),
                    "esito":            g.get("esito"),
                    "pnl_netto_usd":    g.get("pnl_perc", 0),
                    "chimera_snapshot": snap,
                    "fonte":            "VIRTUAL_BRAIN",
                    "data_apertura":    g.get("data", ""),
                    "data_chiusura":    g.get("data_chiusura", ""),
                    "motivo_chiusura":  g.get("motivo_chiusura", ""),
                    "decision_source":  snap.get("decision_source", "GEMINI"),
                    "leverage":         snap.get("leverage", 1),
                    "macro_sentiment":  snap.get("macro_sentiment", "NEUTRAL"),
                    "entry_phase":      snap.get("entry_phase", "FORMAZIONE"),
                })
            if ghost_closed:
                logger.info(f"📊 [ML] Ghost CLOSED dal DB: {len(ghost_closed)} (aggiunti al dataset)")
                storico = storico + ghost_closed
        except Exception as e_gh:
            logger.debug(f"Ghost load per ML: {e_gh}")
        # ─────────────────────────────────────────────────────────────────────

        DATA_TRAINING = "2026-03-20"
        trade_reali = [
            t for t in storico
            if t.get("chimera_snapshot")
            and t.get("esito") in ("WIN", "LOSS")
            and t.get("direzione")
            and t.get("fonte") != "STORICO_SIMULATO"
            and str(t.get("data_apertura", "") or t.get("data_chiusura", "")) >= DATA_TRAINING
            # FIX-M: rimosso filtro entry_phase (escludeva 295 trade reali da HA strategy)
            and len(t.get("chimera_snapshot") or {}) >= 10
        ]
        # ── Ordinamento cronologico CRITICO ─────────────────────────────────
        # Senza questo, i ghost appesi in coda finiscono nel validation set
        # con distribuzione diversa → AUC crolla (0.861 → 0.014 senza sort).
        # Il sort garantisce che train (80%) e val (20%) abbiano la stessa
        # distribuzione temporale di trade reali e ghost.
        trade_reali.sort(
            key=lambda t: str(t.get("data_apertura", "") or t.get("data_chiusura", "") or "")
        )
        trade_reali = trade_reali[-FINESTRA_RECENTE:]
        trade_ml = trade_reali
        logger.info(f"📊 [ML] Dataset totale: {len(trade_ml)} trade (storico + ghost, dal {DATA_TRAINING})")

        if not trade_ml:
            return None, None, 0

        X_list  = []
        y_list  = []
        w_list  = []   # sample weights

        # Data pivot: i trade dopo questa data hanno feature di qualità alta
        # (SL corretti, muri reali, struttura multi-tf)
        DATA_PIVOT_QUALITA = "2026-03-20"

        for i, t in enumerate(trade_ml):
            snap      = dict(t.get("chimera_snapshot", {}))  # copia per non modificare l'originale
            direzione = t.get("direzione", "LONG")
            voto_ia   = int(t.get("voto_ia", snap.get('voto_ia', 5)) or 5)
            macro = (
                t.get("macro_sentiment")
                or snap.get("macro_sentiment")
                or "NEUTRAL"
            )
            decision_source = (
                t.get("decision_source")
                or snap.get("decision_source")
                or "GEMINI"
            )
            # Arricchisce lo snap con campi del trade per _estrai_features
            for _campo in ['voto_ia','leverage','decision_source','entry_phase',
                           'exhaustion_score','ml_prob_win','ml_confidenza','macro_sentiment',
                           'ha_daily_colore','ha_daily_streak','score_breakdown']:
                if _campo not in snap and _campo in t:
                    snap[_campo] = t[_campo]

            # Inietta data_apertura nello snap per le feature temporali
            if 'data_apertura' not in snap and t.get('data_apertura'):
                snap['data_apertura'] = t['data_apertura']
            features = self._estrai_features(snap, t.get("asset", ""), direzione, voto_ia, macro, decision_source)
            if features is None:
                continue

            label = 1.0 if t.get("esito") == "WIN" else 0.0
            X_list.append(features)
            y_list.append(label)

            # Sample weight — sistema originale stabile
            # Tutti i trade recenti (dal DATA_PIVOT) pesano uguale.
            # Differenziamo solo: storici simulati (esclusi) e trade vecchi (peso ridotto).
            # NON applichiamo decadimento temporale: cambia la distribuzione WIN/LOSS
            # tra training e validation causando AUC invertita.
            data_trade = str(t.get("data_apertura", "") or t.get("data_chiusura", ""))

            if t.get("fonte") == "STORICO_SIMULATO":
                peso = 0.0
            elif data_trade >= DATA_PIVOT_QUALITA:
                # Premio qualità dati senza penalizzare i recenti
                ha_contesto = bool(t.get("contesto_strutturale")) or bool(snap.get("contesto_strutturale"))
                ha_score_bd = bool(snap.get("score_breakdown"))
                ha_multi_tf = bool(snap.get("multi_tf"))
                if ha_contesto or (ha_score_bd and ha_multi_tf):
                    peso = 5.0   # qualità completa
                elif ha_multi_tf:
                    peso = 3.0   # buona qualità
                else:
                    peso = 3.0   # base uguale — non penalizzare trade recenti
            else:
                peso = 1.0   # trade reali precedenti al pivot

            w_list.append(peso)

        if len(X_list) < 2:
            return None, None, 0

        X = np.array(X_list, dtype=np.float32)
        y = np.array(y_list, dtype=np.float32)
        w = np.array(w_list, dtype=np.float32)

        logger.info(
            f"📊 [ML] Dataset: {len(X_list)} trade | "
            f"peso medio: {w.mean():.2f} | "
            f"trade alta qualità (w≥3): {(w>=3).sum()}"
        )

        return X, y, len(X_list), w

    # ═══════════════════════════════════════════════════════════════════════
    #  NORMALIZZAZIONE
    # ═══════════════════════════════════════════════════════════════════════

    def _normalizza(self, X: np.ndarray) -> np.ndarray:
        """Normalizza X usando i parametri fit durante l'addestramento."""
        if self.scaler_mean is None or self.scaler_std is None:
            return X
        return (X - self.scaler_mean) / self.scaler_std

    # ═══════════════════════════════════════════════════════════════════════
    #  RETRAIN AUTOMATICO
    # ═══════════════════════════════════════════════════════════════════════

    def _verifica_e_riaddestra(self, force_check: bool = False) -> None:
        """
        Controlla se riaddestramento necessario.
        force_check=True: riaddestra subito se c'è almeno 1 nuovo trade reale.

        IMPORTANTE: usa lo stesso filtro di _prepara_dataset così
        n_trade_al_ultimo_train e nuovi_da_ultimo_train sono coerenti.
        """
        try:
            # ── FIX-N: RETRAIN FORZATO OGNI 24H ──────────────────────────────
            # Se il modello su disco è più vecchio di 24h, riaddestra a prescindere
            # dal counter (che può rimanere bloccato se i nuovi trade non popolano
            # entry_phase o se si addestrano modifiche al codice non riflesse).
            try:
                if os.path.exists(MODELLO_PATH) and self.model is not None:
                    age_hours = (time.time() - os.path.getmtime(MODELLO_PATH)) / 3600
                    if age_hours >= 24:
                        # Verifica se sta già girando un retrain (evita doppi)
                        _retrain_in_corso = any(
                            th.name.startswith('Thread-') and th.is_alive() and 'addestra' in str(th._target)
                            for th in threading.enumerate()
                            if hasattr(th, '_target') and th._target is not None
                        )
                        if not _retrain_in_corso:
                            logger.info(
                                f"🕐 [ML] FIX-N: retrain forzato — modello vecchio di {age_hours:.1f}h"
                            )
                            threading.Thread(target=self.addestra, daemon=True).start()
                            return
            except Exception as _e_age:
                logger.debug(f"[ML] check età modello: {_e_age}")
            # ─────────────────────────────────────────────────────────────────

            from core.database_manager import db_manager
            storico = db_manager.get_storico()

            DATA_TRAINING = "2026-03-20"
            trade_filtrati = [
                t for t in storico
                if t.get("chimera_snapshot")
                and t.get("esito") in ("WIN", "LOSS")
                and t.get("direzione")
                and str(t.get('fonte', '')).upper() != 'STORICO_SIMULATO'
                and str(t.get("data_apertura", "") or t.get("data_chiusura", "")) >= DATA_TRAINING
                # FIX-M: rimosso filtro entry_phase (escludeva 295 trade reali da HA strategy)
                # Ora basta che lo snapshot sia ragionevolmente popolato (>=10 chiavi).
                and len(t.get("chimera_snapshot") or {}) >= 10
            ]
            n_ghost  = len([t for t in trade_filtrati
                            if str(t.get('fonte','')).upper() in ('VIRTUAL_BRAIN','GHOST','GHOST_KRAKEN')])
            n_reali  = len(trade_filtrati) - n_ghost
            n_validi = len(trade_filtrati)
            nuovi_da_ultimo_train = n_validi - self.n_trade_al_ultimo_train

            logger.debug(
                f"📊 [ML] Dataset filtrato: {n_reali} reali + {n_ghost} ghost = {n_validi} totali "
                f"| Nuovi dall'ultimo train: {nuovi_da_ultimo_train}"
            )

            # Primo addestramento
            if self.model is None and n_reali >= MIN_TRADE_PER_TRAIN:
                logger.info(
                    f"🎓 [ML] Primo addestramento: {n_reali} trade reali disponibili "
                    f"(+ {n_ghost} ghost). Avvio..."
                )
                threading.Thread(target=self.addestra, daemon=True).start()
                return

            # Trade reale nuovo → riaddestra subito (in background)
            if force_check and self.model is not None and nuovi_da_ultimo_train >= 1:
                logger.info(
                    f"🔄 [ML] Nuovo trade reale chiuso — retrain immediato "
                    f"({n_reali} reali + {n_ghost} ghost)"
                )
                threading.Thread(target=self.addestra, daemon=True).start()
                return

            # Ghost o periodico: ogni RETRAIN_OGNI_N
            if self.model is not None and nuovi_da_ultimo_train >= RETRAIN_OGNI_N:
                logger.info(
                    f"🔄 [ML] {nuovi_da_ultimo_train} nuovi trade → retrain periodico"
                )
                threading.Thread(target=self.addestra, daemon=True).start()

        except Exception as e:
            _err.capture(e, sys._getframe().f_code.co_name, {"module": "ChimeraML"})
            logger.error(f"❌ Errore verifica retrain: {e}")

    # ═══════════════════════════════════════════════════════════════════════
    #  PERSISTENZA MODELLO
    # ═══════════════════════════════════════════════════════════════════════

    def _salva_modello(self) -> None:
        """Salva modello e parametri di normalizzazione su disco."""
        try:
            if self.model:
                self.model.save_model(MODELLO_PATH)

            if self.scaler_mean is not None:
                try:
                    _bs = float(self.model.attributes().get('best_score', 0) or 0) if self.model else 0.0
                    if _bs == 0.0:
                        _bs = getattr(self, '_best_score_cached', 0.0)
                except Exception:
                    _bs = getattr(self, '_best_score_cached', 0.0)
                scaler_data = {
                    "mean": self.scaler_mean.tolist(),
                    "std":  self.scaler_std.tolist(),
                    "n_trade_train": self.n_trade_al_ultimo_train,
                    "feature_names": FEATURE_NAMES,
                    "best_score": _bs,
                    "timestamp": datetime.now().isoformat(),
                }
                with open(SCALER_PATH, "w") as f:
                    json.dump(scaler_data, f, indent=2)

            logger.info(f"💾 [ML] Modello salvato → {MODELLO_PATH}")
        except Exception as e:
            _err.capture(e, sys._getframe().f_code.co_name, {"module": "ChimeraML"})
            logger.error(f"❌ Errore salvataggio modello: {e}")

    def _carica_modello(self) -> None:
        """Carica modello e normalizzazione da disco se disponibili."""
        if not self._xgb_disponibile:
            return

        try:
            if os.path.exists(MODELLO_PATH) and os.path.exists(SCALER_PATH):
                self.model = self._xgb.Booster()
                self.model.load_model(MODELLO_PATH)

                with open(SCALER_PATH) as f:
                    scaler_data = json.load(f)

                # Verifica compatibilità feature names
                saved_features = scaler_data.get("feature_names", [])
                if saved_features != FEATURE_NAMES:
                    logger.warning(
                        "⚠️ [ML] Feature names cambiate — modello salvato non compatibile. "
                        "Riaddestramento necessario."
                    )
                    self.model = None
                    return

                self.scaler_mean = np.array(scaler_data["mean"], dtype=np.float32)
                self.scaler_std  = np.array(scaler_data["std"],  dtype=np.float32)
                self._best_score_cached = float(scaler_data.get("best_score", 0.0) or 0.0)
                ts = scaler_data.get("timestamp", "?")

                # Ricalcola n_trade_al_ultimo_train dal dataset reale attuale
                # invece di usare il valore salvato nel JSON.
                # Il valore nel JSON è quello del momento del training precedente;
                # se il filtro cambia tra sessioni (entry_phase, DATA_TRAINING, ecc.)
                # il contatore diverge → nuovi_da_ultimo_train negativo o gonfiato
                # → retrain mai oppure retrain infinito sugli stessi dati.
                try:
                    from core.database_manager import db_manager as _db
                    _storico = _db.get_storico()
                    DATA_TRAINING = "2026-03-20"
                    self.n_trade_al_ultimo_train = len([
                        t for t in _storico
                        if t.get("chimera_snapshot")
                        and t.get("esito") in ("WIN", "LOSS")
                        and t.get("direzione")
                        and str(t.get('fonte', '')).upper() != 'STORICO_SIMULATO'
                        and str(t.get("data_apertura", "") or t.get("data_chiusura", "")) >= DATA_TRAINING
                        # FIX-M: rimosso filtro entry_phase (deve allinearsi a _verifica_e_riaddestra e _prepara_dataset)
                        and len(t.get("chimera_snapshot") or {}) >= 10
                    ])
                except Exception:
                    # Fallback al valore nel JSON se il DB non è accessibile
                    self.n_trade_al_ultimo_train = scaler_data.get("n_trade_train", 0)

                logger.info(
                    f"📂 [ML] Modello caricato (addestrato il {ts[:16]}, "
                    f"{self.n_trade_al_ultimo_train} trade nel dataset attuale, "
                    f"AUC cached={self._best_score_cached:.3f})"
                )
            else:
                logger.info(
                    f"📂 [ML] Nessun modello salvato trovato. "
                    f"Si attiverà dopo {MIN_TRADE_PER_TRAIN} trade reali."
                )
        except Exception as e:
            _err.capture(e, sys._getframe().f_code.co_name, {"module": "ChimeraML"})
            logger.warning(f"⚠️ [ML] Impossibile caricare modello: {e}. Ripartenza da zero.")
            self.model = None

    # ═══════════════════════════════════════════════════════════════════════
    #  UTILITÀ
    # ═══════════════════════════════════════════════════════════════════════

    def log_status(self) -> None:
        """
        FIX-O: Log status ML per visibilità diagnostica.
        Da chiamare periodicamente (es. ogni ora dal main loop) per vedere
        a colpo d'occhio se il modello è vivo, fresco e operativo.
        """
        try:
            stato_dict = self.stato()
            # Età modello
            age_str = "modello_assente"
            if os.path.exists(MODELLO_PATH):
                age_h = (time.time() - os.path.getmtime(MODELLO_PATH)) / 3600
                age_str = f"età={age_h:.1f}h"
            
            logger.info(
                f"📊 [ML STATUS] attivo={stato_dict.get('attivo')} | "
                f"dataset={stato_dict.get('trade_disponibili')} trade | "
                f"AUC={stato_dict.get('auc_validation', getattr(self, '_best_score_cached', 0)):.3f} | "
                f"prossimo_retrain_in={stato_dict.get('prossimo_retrain', '?')} trade | "
                f"{age_str}"
            )
        except Exception as e:
            logger.debug(f"[ML STATUS] errore log: {e}")

    def _calcola_auc(self, y_true: np.ndarray, y_pred: np.ndarray) -> float:
        """
        Calcola AUC-ROC senza sklearn (implementazione minimalista).
        AUC=0.5 → casuale, AUC=0.7+ → decente, AUC=0.8+ → buono.
        """
        try:
            # Ordina per predizione decrescente
            sorted_idx = np.argsort(y_pred)[::-1]
            y_sorted   = y_true[sorted_idx]

            n_pos = y_true.sum()
            n_neg = len(y_true) - n_pos

            if n_pos == 0 or n_neg == 0:
                return 0.5

            tp = 0
            fp = 0
            auc = 0.0
            prev_tp = 0

            for label in y_sorted:
                if label == 1:
                    tp += 1
                else:
                    fp += 1
                    auc += (tp - prev_tp)
                    prev_tp = tp

            return float(auc / (n_pos * n_neg)) if (n_pos * n_neg) > 0 else 0.5
        except Exception:
            return 0.5

    def report_feature_importance(self) -> str:
        """Restituisce le feature più importanti come stringa leggibile."""
        if not self._xgb_disponibile or self.model is None:
            return "Modello non disponibile"

        try:
            importance = self.model.get_score(importance_type="gain")
            totale = sum(importance.values()) or 1
            righe = []
            for feat, val in sorted(importance.items(), key=lambda x: x[1], reverse=True)[:10]:
                perc = val / totale * 100
                righe.append(f"  {feat:<35} {perc:5.1f}%")
            return "\n".join(righe)
        except Exception as e:
            return f"Errore: {e}"