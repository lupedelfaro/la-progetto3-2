# -*- coding: utf-8 -*-
"""
CHIMERA v4.0 — SignalStateEngine
Motore di classificazione della fase del segnale in tempo reale.

Logica a 5 fasi con rilevamento accumulo istituzionale:

  SILENZIO           → nessun segnale, mercato morto
  FORMAZIONE_VUOTA   → indicatori si muovono ma flusso assente
  FORMAZIONE_ISTIT   → volume basso MA istituzionali attivi (VPIN alto + CVD accelera)
  ESTENSIONE         → segnale confermato e in corso
  BREAKOUT           → rottura decisa con volume (reale o fake)
  ESAURIMENTO        → segnale stanco, inversione probabile

Ogni fase produce:
  entry_phase         → stringa fase corrente
  phase_subtype       → sottotipo (es. ISTITUZIONALE, FAKE, REVERSAL)
  exhaustion_score    → 0-100
  phase_override_ok   → True se Brain può fare override del blocco
  phase_override_cond → condizioni necessarie per override
  phase_narrative     → testo clinico per Gemini
  cvd_trend           → CRESCENTE / DECRESCENTE / PIATTO / ACCELERAZIONE
  cvd_delta_30s       → variazione CVD ultimi 30s
  cvd_delta_120s      → variazione CVD ultimi 120s
  cvd_acceleration    → accelerazione del delta
  signal_age_s        → età del segnale in secondi
  short_conditions_met → bool
  short_veto_motivo   → motivo veto short se presente
"""

import time
import logging
from collections import deque
from typing import Dict, Any

logger = logging.getLogger("SignalStateEngine")


class SignalStateEngine:
    """
    Classifica la fase del segnale basandosi sulla storia temporale
    degli indicatori chiave: CVD, VPIN, velocity, volume, exhaustion.

    Mantiene uno storico per asset degli ultimi 120 secondi di campioni.
    """

    # ── Soglie operative ────────────────────────────────────────────────────
    VPIN_ISTIT_SOGLIA      = 0.70   # sopra → istituzionali attivi
    VPIN_TOSSICO_SOGLIA    = 0.80   # sopra → flusso tossico
    CVD_DELTA_30S_MIN      = 15000  # USD, fallback per asset ad alto volume (BTC/ETH)
    CVD_ACCEL_MIN          = 0.15   # accelerazione minima normalizzata
    VELOCITY_BREAKOUT      = 0.0005 # soglia velocity per breakout
    VELOCITY_ESPLOSIVA     = 0.0010 # velocity esplosiva — SCALPING
    EXHAUST_ESAURITO       = 55     # exhaustion oltre → fase ESAURIMENTO
    EXHAUST_REVERSAL       = 75     # exhaustion oltre → possibile reversal
    STORIA_MAX_S           = 180    # secondi di storia da mantenere
    CAMPIONI_MAX           = 36     # max campioni in buffer (ogni ~5s)

    # Fattore soglia SILENZIO
    SILENZIO_CVD_FACTOR    = 0.005
    CVD_DELTA_FLOOR        = 200

    # Sweep detection
    SWEEP_CVD_REVERSAL_RATIO = 0.30  # CVD deve invertire almeno 30% del delta precedente
    SWEEP_VELOCITY_MIN       = 0.0003

    # Distribuzione istituzionale
    # Soglia allineata a VPIN_ISTIT_SOGLIA (0.70) per simmetria con FORMAZIONE_ISTIT.
    # Vecchia soglia 0.65 faceva scattare DISTRIBUZIONE (=SHORT bias) più facilmente
    # di FORMAZIONE_ISTIT (=opportunità LONG/setup bilaterale): asimmetria architetturale.
    DISTRIB_VPIN_MIN         = 0.70
    DISTRIB_CVD_NEG_RATIO    = 0.3   # CVD negativo mentre prezzo stabile o sale

    # Range attivo
    RANGE_HURST_MAX          = 0.48
    RANGE_VELOCITY_MAX       = 0.0002

    # Compressione pre-breakout
    COMPRESS_ACCEL_MAX       = 0.05  # accelerazione quasi nulla
    COMPRESS_VPIN_MIN        = 0.40  # VPIN che sale mentre volatilità scende

    # Divergenza prezzo/CVD
    DIVERG_VELOCITY_MIN      = 0.0002
    DIVERG_CVD_OPPOSITE_RATIO= 0.25  # CVD va in direzione opposta al prezzo di almeno 25%

    # Correlazione rotta
    CORR_ROTTA_SOGLIA        = 0.20  # sotto questa correlazione con BTC → narrativa indipendente

    def _cvd_soglia_dinamica(self, cvd_assoluto: float) -> float:
        """
        Calcola la soglia CVD_DELTA_30S dinamicamente in base al CVD corrente dell'asset.

        Logica:
          - Soglia = max(CVD_assoluto * SILENZIO_CVD_FACTOR, CVD_DELTA_FLOOR)
          - BTC con CVD 200k → soglia = max(1000, 200) = 1000$
          - ZEC con CVD 80k  → soglia = max(400, 200)  = 400$
          - BONK con CVD 10k → soglia = max(50, 200)   = 200$ (floor)
          - Cap superiore a CVD_DELTA_30S_MIN (15000) per non essere mai più permissivi
            degli asset ad alto volume
        """
        soglia = abs(cvd_assoluto) * self.SILENZIO_CVD_FACTOR
        soglia = max(soglia, self.CVD_DELTA_FLOOR)
        soglia = min(soglia, self.CVD_DELTA_30S_MIN)  # cap al valore originale BTC
        return soglia

    def __init__(self):
        # Storico per ticker: deque di dict {ts, cvd, vpin, velocity, exhaust, volume}
        self._storia: Dict[str, deque] = {}
        # Timestamp inizio fase corrente per asset
        self._fase_inizio: Dict[str, float] = {}
        # Fase precedente per rilevare transizioni
        self._fase_prec: Dict[str, str] = {}

    # ═══════════════════════════════════════════════════════════════════════
    #  API PUBBLICA
    # ═══════════════════════════════════════════════════════════════════════

    def aggiorna(self, ticker: str, dati: Dict[str, Any]) -> Dict[str, Any]:
        """
        Chiamato da engine_la ad ogni ciclo di dati.
        Aggiorna lo storico e calcola la fase corrente.

        Dati extra usati per i nuovi contesti:
          - supporto_sweepato, resistenza_sweepata (sweep detection)
          - hurst_exponent (range detection)
          - correlazione_driver (correlazione rotta)
          - multi_tf (contesto timeframe superiore)

        Returns:
            dict con tutti i campi di fase da aggiungere a res.
        """
        ts = time.time()

        # Inizializza buffer se primo campione
        if ticker not in self._storia:
            self._storia[ticker]    = deque(maxlen=self.CAMPIONI_MAX)
            self._fase_inizio[ticker] = ts
            self._fase_prec[ticker]   = 'SILENZIO'

        # Estrai valori correnti
        cvd      = float(dati.get('cvd_istantaneo', 0) or 0)
        vpin     = float(dati.get('vpin', 0) or 0)
        velocity = float(dati.get('price_velocity', 0) or 0)
        exhaust  = float(dati.get('exhaustion_score', 0) or 0)
        volume   = float(dati.get('volume', 0) or 0)

        # Dati extra per contesti avanzati
        sweepato_s   = bool(dati.get('supporto_sweepato', False))
        sweepato_r   = bool(dati.get('resistenza_sweepata', False))
        hurst        = float(dati.get('hurst_exponent', 0.5) or 0.5)
        corr_driver  = float(dati.get('correlazione_driver', 1.0) or 1.0)
        multi_tf     = dati.get('multi_tf', {}) or {}
        close        = float(dati.get('close', 0) or 0)
        atr          = float(dati.get('atr', 0) or 0)

        # Aggiunge campione allo storico
        self._storia[ticker].append({
            'ts':       ts,
            'cvd':      cvd,
            'vpin':     vpin,
            'velocity': velocity,
            'exhaust':  exhaust,
            'volume':   volume,
        })

        # Pulisce campioni troppo vecchi
        while (self._storia[ticker] and
               ts - self._storia[ticker][0]['ts'] > self.STORIA_MAX_S):
            self._storia[ticker].popleft()

        # Calcola derivate temporali
        delta_30s, delta_120s, accel = self._calcola_deltas(ticker, ts)

        # Classifica fase
        fase, sottotipo, override_ok, override_cond = self._classifica_fase(
            vpin, velocity, exhaust, volume,
            delta_30s, delta_120s, accel, cvd, dati,
            sweepato_s, sweepato_r, hurst, corr_driver, multi_tf, close, atr
        )

        # Calcola cvd_trend
        cvd_trend = self._cvd_trend_label(delta_30s, accel, cvd)

        # Età del segnale
        signal_age = ts - self._fase_inizio.get(ticker, ts)

        # Transizione di fase → resetta timer
        if fase != self._fase_prec.get(ticker, fase):
            self._fase_inizio[ticker] = ts
            signal_age = 0
            logger.info(
                f"⚙️ [{ticker}] Fase segnale: "
                f"{self._fase_prec[ticker]} → {fase}"
                f"{' (' + sottotipo + ')' if sottotipo else ''}"
                f" (exhaust={exhaust:.0f})"
            )
            self._fase_prec[ticker] = fase

        # Veto SHORT
        short_ok, short_veto = self._valuta_short(fase, sottotipo, vpin, cvd, velocity, delta_30s)
        long_ok, long_veto = self._valuta_long(fase, sottotipo, vpin, cvd, velocity, delta_30s)

        # Narrativa clinica per Gemini
        narrative = self._genera_narrative(
            fase, sottotipo, vpin, velocity, exhaust,
            delta_30s, delta_120s, accel, cvd_trend, override_ok, override_cond
        )

        # Contesto TF superiore — 4h e 1d per orientare Gemini
        _tf4h = multi_tf.get('4h', {})
        _tf1d = multi_tf.get('1d', {})
        contesto_tf = {
            'trend_4h':    _tf4h.get('trend_dir', '?'),
            'hurst_4h':    round(float(_tf4h.get('hurst', 0.5) or 0.5), 3),
            'regime_4h':   _tf4h.get('regime', '?'),
            'trend_1d':    _tf1d.get('trend_dir', '?'),
            'hurst_1d':    round(float(_tf1d.get('hurst', 0.5) or 0.5), 3),
        }

        return {
            'entry_phase':          fase,
            'phase_subtype':        sottotipo,
            'exhaustion_score':     exhaust,
            'phase_override_ok':    override_ok,
            'phase_override_cond':  override_cond,
            'phase_narrative':      narrative,
            'cvd_trend':            cvd_trend,
            'cvd_delta_30s':        round(delta_30s, 2),
            'cvd_delta_120s':       round(delta_120s, 2),
            'cvd_acceleration':     round(accel, 4),
            'signal_age_s':         round(signal_age, 1),
            'short_conditions_met': short_ok,
            'short_veto_motivo':    short_veto,
            'long_conditions_met':  long_ok,
            'long_veto_motivo':     long_veto,
            'contesto_tf_superiore': contesto_tf,
            'correlazione_driver':  round(corr_driver, 3),
        }

    # ═══════════════════════════════════════════════════════════════════════
    #  CLASSIFICAZIONE FASE
    # ═══════════════════════════════════════════════════════════════════════

    def _classifica_fase(
        self, vpin, velocity, exhaust, volume,
        delta_30s, delta_120s, accel, cvd, dati,
        sweepato_s=False, sweepato_r=False, hurst=0.5,
        corr_driver=1.0, multi_tf=None, close=0.0, atr=0.0
    ):
        """
        Classifica il contesto di mercato in una delle fasi operative.
        Restituisce: (fase, sottotipo, override_ok, override_cond)

        Fasi disponibili:
          SILENZIO, FORMAZIONE, ESTENSIONE, BREAKOUT, ESAURIMENTO (originali)
          SWEEP, DISTRIBUZIONE, ACCUMULAZIONE, RANGE, COMPRESSIONE, DIVERGENZA, CORRELAZIONE_ROTTA (nuove)
        """
        abs_vel = abs(velocity)
        abs_d30 = abs(delta_30s)
        multi_tf = multi_tf or {}
        _soglia  = self._cvd_soglia_dinamica(cvd)

        # ── ESAURIMENTO — ha priorità assoluta ──────────────────────────
        if exhaust >= self.EXHAUST_ESAURITO:
            if (exhaust >= self.EXHAUST_REVERSAL
                    and vpin > self.VPIN_ISTIT_SOGLIA
                    and abs_d30 > _soglia):
                return (
                    'ESAURIMENTO', 'REVERSAL', True,
                    "exhaustion>75 + VPIN istituzionale + CVD che inverte: possibile reversal. "
                    "Richiede voto≥7 + ML conf≥35%."
                )
            return ('ESAURIMENTO', 'NORMALE', False,
                    "Segnale esaurito. Nessun override possibile.")

        # ── SWEEP — liquidity grab prima del vero movimento ─────────────
        # Prezzo è andato oltre un livello (sweep) e il CVD sta già invertendo.
        # Questo è spesso il setup migliore — il movimento reale parte DOPO lo sweep.
        if sweepato_s or sweepato_r:
            # CVD che inverte dopo lo sweep = confermato
            cvd_inverte = (sweepato_s and delta_30s > _soglia * self.SWEEP_CVD_REVERSAL_RATIO) or                           (sweepato_r and delta_30s < -_soglia * self.SWEEP_CVD_REVERSAL_RATIO)
            if cvd_inverte and abs_vel >= self.SWEEP_VELOCITY_MIN:
                return (
                    'SWEEP', 'CONFERMATO', True,
                    "Liquidity sweep confermato: prezzo oltre il livello e CVD già invertito. "
                    "Setup ad alta probabilità — il movimento reale parte adesso. "
                    "Override permesso con voto≥6."
                )
            elif sweepato_s or sweepato_r:
                return (
                    'SWEEP', 'IN_CORSO', True,
                    "Sweep rilevato ma CVD non ha ancora invertito. "
                    "Attendere conferma inversione CVD prima di entrare. "
                    "Override con voto≥7 + whale concorde."
                )

        # ── SILENZIO ────────────────────────────────────────────────────
        if (vpin < 0.30 and abs_vel < 0.0001 and abs_d30 < _soglia * 0.3):
            return ('SILENZIO', '', False, "Mercato inattivo.")

        # ── CORRELAZIONE ROTTA — asset con narrativa indipendente ───────
        # L'asset si muove diversamente da BTC — può essere manipolazione o
        # notizia specifica. Richiede più cautela o più opportunità.
        if abs(corr_driver) < self.CORR_ROTTA_SOGLIA and abs_vel > self.SWEEP_VELOCITY_MIN:
            return (
                'CORRELAZIONE_ROTTA', '', True,
                f"Asset si muove indipendentemente da BTC (corr={corr_driver:.2f}). "
                "Narrativa propria — potenziale opportunità o manipulation. "
                "Verifica notizie specifiche. Override con voto≥7."
            )

        # ── BREAKOUT ────────────────────────────────────────────────────
        if abs_vel >= self.VELOCITY_BREAKOUT:
            cvd_concorde = (velocity > 0 and delta_30s > 0) or                            (velocity < 0 and delta_30s < 0)
            vpin_alto = vpin > self.VPIN_ISTIT_SOGLIA
            if cvd_concorde and (vpin_alto or abs_d30 > _soglia * 2):
                sottotipo = 'ESPLOSIVO' if abs_vel >= self.VELOCITY_ESPLOSIVA else 'REALE'
                return ('BREAKOUT', sottotipo, False, "")
            else:
                return ('BREAKOUT', 'FAKE', True,
                        "Velocity alta ma CVD a 30s non concorde. Il breakout intraday non ha "
                        "ancora conferma di flusso. In trend strutturale chiaro (HA daily, multi-TF) "
                        "il prezzo può continuare per inerzia. Override con voto≥7 se setup strutturale chiaro.")

        # ── DIVERGENZA — prezzo e CVD in direzioni opposte ───────────────
        # Il prezzo si muove ma il CVD non conferma — può essere debolezza
        # del segnale o solo rumore intraday in trend forte di fondo.
        if abs_vel >= self.DIVERG_VELOCITY_MIN:
            prezzo_sale  = velocity > 0
            cvd_scende   = delta_30s < -abs_d30 * self.DIVERG_CVD_OPPOSITE_RATIO
            cvd_sale     = delta_30s > abs_d30 * self.DIVERG_CVD_OPPOSITE_RATIO
            if (prezzo_sale and cvd_scende) or (not prezzo_sale and cvd_sale):
                return (
                    'DIVERGENZA', '', True,
                    "Prezzo e CVD a 30s in direzioni opposte. Può essere debolezza del segnale "
                    "intraday o solo rumore in trend forte di fondo. Verifica TF superiori "
                    "(4h, 1d) prima di interpretare come inversione. Override con voto≥7."
                )

        # ── DISTRIBUZIONE — venditori attivi a flusso istituzionale ──────
        # CVD negativo a breve con VPIN alto e prezzo stabile = pressione di vendita
        # nel breve. NON implica direzione: in trend rialzista può essere profit-taking
        # temporaneo, in trend ribassista o vicino resistenza può precedere il calo.
        if (vpin >= self.DISTRIB_VPIN_MIN
                and delta_30s < -_soglia * self.DISTRIB_CVD_NEG_RATIO
                and abs_vel < self.VELOCITY_BREAKOUT):
            return (
                'DISTRIBUZIONE', '', True,
                f"VPIN={vpin:.2f} (istituzionali attivi) + CVD negativo a 30s (delta_30s={delta_30s:+.0f}$). "
                "Pressione di vendita nel breve. NON è direzionale di per sé: in trend rialzista forte "
                "può essere profit-take temporaneo (continuazione UP); in zona resistenza o trend ribassista "
                "precede spesso ribasso. Verifica struttura TF superiori. Override con voto≥7."
            )

        # ── ACCUMULAZIONE — compratori attivi a flusso istituzionale ─────
        # Simmetrica a DISTRIBUZIONE: CVD positivo + VPIN alto + velocity bassa
        # = whale comprano silenziosamente prima del movimento visibile.
        # Aggiunta 2026-05-01 per simmetria (prima esisteva solo DISTRIBUZIONE/SHORT-bias).
        if (vpin >= self.DISTRIB_VPIN_MIN
                and delta_30s > _soglia * self.DISTRIB_CVD_NEG_RATIO
                and abs_vel < self.VELOCITY_BREAKOUT):
            return (
                'ACCUMULAZIONE', '', True,
                f"VPIN={vpin:.2f} (istituzionali attivi) + CVD positivo a 30s (delta_30s={delta_30s:+.0f}$). "
                "Pressione di acquisto nel breve. NON è direzionale di per sé: in trend rialzista "
                "rinforza la continuazione UP; in zona supporto chiave o post-capitulation può precedere "
                "rimbalzo. Verifica struttura TF superiori. Override con voto≥7."
            )

        # ── COMPRESSIONE — volatilità in calo prima dell'esplosione ─────
        # ATR in calo, CVD piatto, VPIN che sale = accumulo di tensione.
        # Non si sa la direzione ma il breakout è imminente.
        atr_perc = (atr / close * 100) if close > 0 else 0
        if (atr_perc > 0 and atr_perc < 0.5
                and abs_d30 < _soglia * 0.5
                and vpin >= self.COMPRESS_VPIN_MIN
                and abs(accel) < self.COMPRESS_ACCEL_MAX):
            return (
                'COMPRESSIONE', '', True,
                f"Volatilità compressa (ATR={atr_perc:.2f}%) con VPIN={vpin:.2f} in crescita. "
                "Tensione accumulata — breakout imminente in direzione sconosciuta. "
                "Aspetta la rottura con conferma CVD. Override solo su breakout confermato."
            )

        # ── RANGE ATTIVO — mercato laterale con volume normale ───────────
        # Non è silenzioso ma non ha direzione. Setup per mean reversion.
        if (hurst < self.RANGE_HURST_MAX
                and abs_vel < self.RANGE_VELOCITY_MAX
                and vpin < 0.50):
            return (
                'RANGE', '', True,
                f"Mercato in range (Hurst={hurst:.2f}, velocity bassa). "
                "Nessun trend direzionale. Setup per mean reversion verso VWAP/POC. "
                "Evita trend following. Override con voto≥7 su rimbalzo da S/R forte."
            )

        # ── ESTENSIONE ──────────────────────────────────────────────────
        if (abs_d30 >= _soglia and exhaust < self.EXHAUST_ESAURITO and vpin >= 0.35):
            return ('ESTENSIONE', '', False, "")

        # ── FORMAZIONE ──────────────────────────────────────────────────
        if vpin >= self.VPIN_ISTIT_SOGLIA:
            if abs_d30 > _soglia * 0.5 or accel > self.CVD_ACCEL_MIN:
                return (
                    'FORMAZIONE', 'ISTITUZIONALE', True,
                    "VPIN istituzionale (>{:.0f}%) + CVD in accelerazione: possibile accumulo "
                    "pre-breakout. Override se: voto≥7 + ML conf≥35% + almeno 1 tra "
                    "[candlestick bullish/bearish forte, prezzo su S/R storico, "
                    "whale_delta concorde].".format(self.VPIN_ISTIT_SOGLIA * 100)
                )

        return (
            'FORMAZIONE', 'VUOTA', False,
            "Segnale in formazione senza flusso istituzionale. Attendere ESTENSIONE o BREAKOUT."
        )
    def _calcola_deltas(self, ticker, ts_now):
        """
        Calcola delta CVD a 30s e 120s e accelerazione.
        """
        storia = self._storia[ticker]
        if len(storia) < 2:
            return 0.0, 0.0, 0.0

        campioni = list(storia)
        cvd_now  = campioni[-1]['cvd']

        # Delta 30s
        soglia_30  = ts_now - 30
        campioni_30 = [c for c in campioni if c['ts'] >= soglia_30]
        delta_30s = cvd_now - campioni_30[0]['cvd'] if campioni_30 else 0.0

        # Delta 120s
        soglia_120  = ts_now - 120
        campioni_120 = [c for c in campioni if c['ts'] >= soglia_120]
        delta_120s = cvd_now - campioni_120[0]['cvd'] if campioni_120 else 0.0

        # Accelerazione: confronta velocità 30s vs 60s-30s
        soglia_60 = ts_now - 60
        campioni_60 = [c for c in campioni if c['ts'] >= soglia_60]
        if len(campioni_60) >= 2:
            cvd_60ago = campioni_60[0]['cvd']
            delta_prec = cvd_now - cvd_60ago  # delta 60s
            # accelerazione = differenza tra le due velocità, normalizzata
            norm = max(abs(delta_120s), 1)
            accel = (delta_30s - (delta_prec - delta_30s)) / norm
        else:
            accel = 0.0

        return delta_30s, delta_120s, accel

    # ═══════════════════════════════════════════════════════════════════════
    #  LABEL CVD TREND
    # ═══════════════════════════════════════════════════════════════════════

    def _cvd_trend_label(self, delta_30s, accel, cvd=0):
        _soglia = self._cvd_soglia_dinamica(cvd)
        if abs(delta_30s) < _soglia * 0.3:
            return 'PIATTO'
        if abs(accel) > self.CVD_ACCEL_MIN * 2:
            return 'ACCELERAZIONE'
        if delta_30s > 0:
            return 'CRESCENTE'
        return 'DECRESCENTE'

    # ═══════════════════════════════════════════════════════════════════════
    #  VETO SHORT
    # ═══════════════════════════════════════════════════════════════════════

    def _valuta_short(self, fase, sottotipo, vpin, cvd, velocity, delta_30s):
        """
        Valuta se le condizioni SHORT sono valide nella fase corrente.
        """
        if fase == 'SILENZIO':
            return False, "Mercato silenzioso — nessun short"
        if fase == 'FORMAZIONE' and sottotipo == 'VUOTA':
            return False, "FORMAZIONE vuota — attendere conferma"
        if fase == 'BREAKOUT' and sottotipo == 'FAKE':
            return False, "FAKE BREAKOUT — non entrare in nessuna direzione"
        if fase == 'ESAURIMENTO' and sottotipo == 'NORMALE':
            return False, "Segnale esaurito — attendere nuovo ciclo"
        if fase == 'COMPRESSIONE':
            return False, "COMPRESSIONE — direzione sconosciuta, attendere breakout"
        if fase == 'RANGE':
            return False, "RANGE — solo mean reversion, no trend following short"

        # Short valido se CVD negativo e velocity negativa
        if delta_30s < -self._cvd_soglia_dinamica(cvd) and velocity < 0:
            return True, ""
        if delta_30s < 0 and vpin > self.VPIN_ISTIT_SOGLIA:
            return True, ""

        return False, f"CVD delta_30s={delta_30s:.0f} non sufficiente per short"

    def _valuta_long(self, fase, sottotipo, vpin, cvd, velocity, delta_30s):
        """
        Valuta se le condizioni LONG sono valide nella fase corrente.
        Simmetrica a _valuta_short — aggiunta 2026-05-01 per simmetria architetturale.
        """
        if fase == 'SILENZIO':
            return False, "Mercato silenzioso — nessun long"
        if fase == 'FORMAZIONE' and sottotipo == 'VUOTA':
            return False, "FORMAZIONE vuota — attendere conferma"
        if fase == 'BREAKOUT' and sottotipo == 'FAKE':
            return False, "BREAKOUT non confermato — verifica struttura"
        if fase == 'ESAURIMENTO' and sottotipo == 'NORMALE':
            return False, "Segnale esaurito — attendere nuovo ciclo"
        if fase == 'COMPRESSIONE':
            return False, "COMPRESSIONE — direzione sconosciuta, attendere breakout"
        if fase == 'RANGE':
            return False, "RANGE — solo mean reversion, no trend following long"

        # Long valido se CVD positivo e velocity positiva
        if delta_30s > self._cvd_soglia_dinamica(cvd) and velocity > 0:
            return True, ""
        if delta_30s > 0 and vpin > self.VPIN_ISTIT_SOGLIA:
            return True, ""

        return False, f"CVD delta_30s={delta_30s:.0f} non sufficiente per long"

    # ═══════════════════════════════════════════════════════════════════════
    #  NARRATIVA PER GEMINI
    # ═══════════════════════════════════════════════════════════════════════

    def _genera_narrative(
        self, fase, sottotipo, vpin, velocity, exhaust,
        delta_30s, delta_120s, accel, cvd_trend, override_ok, override_cond
    ):
        """
        Genera una narrativa clinica leggibile da Gemini sul contesto di fase.
        """
        lines = []

        # Fase principale
        if fase == 'SILENZIO':
            lines.append("🔇 SILENZIO: Mercato inattivo. Nessun flusso rilevante intraday. Valuta se il setup strutturale (HA daily, multi-TF, livelli) giustifica entry comunque.")

        elif fase == 'FORMAZIONE':
            if sottotipo == 'ISTITUZIONALE':
                lines.append(
                    f"🏛️ FORMAZIONE ISTITUZIONALE: VPIN={vpin:.2f} indica istituzionali attivi. "
                    f"CVD accelera ({cvd_trend}, delta_30s={delta_30s:+.0f}$). "
                    f"Accumulo silenzioso in corso PRIMA del movimento visibile. "
                    f"Questo è il setup pre-breakout classico — il volume esploderà dopo. "
                    f"Override possibile se confluenze forti."
                )
            else:
                lines.append(
                    f"🌱 FORMAZIONE VUOTA: VPIN={vpin:.2f}, CVD={cvd_trend}. "
                    f"Nessun flusso istituzionale. Attendere ESTENSIONE o BREAKOUT."
                )

        elif fase == 'ESTENSIONE':
            lines.append(
                f"📈 ESTENSIONE: Segnale confermato. CVD {cvd_trend} (delta_30s={delta_30s:+.0f}$, "
                f"delta_120s={delta_120s:+.0f}$). Exhaustion={exhaust:.0f}/100. "
                f"Stile preferito: MOMENTUM. Entry permessa."
            )

        elif fase == 'BREAKOUT':
            if sottotipo == 'FAKE':
                lines.append(
                    f"⚠️ BREAKOUT NON CONFERMATO: Velocity={velocity:.5f} alta ma CVD={cvd_trend} "
                    f"non concorde. Movimento intraday senza CVD a supporto. "
                    f"Il prezzo può continuare per inerzia; valuta se il trend di fondo (HA daily, multi-TF) "
                    f"giustifica comunque entry o richiede prudenza."
                )
            elif sottotipo == 'ESPLOSIVO':
                lines.append(
                    f"🚀 BREAKOUT ESPLOSIVO: Velocity={velocity:.5f} + CVD {cvd_trend}. "
                    f"Movimento istituzionale reale. Stile: SCALPING. Entry immediata."
                )
            else:
                lines.append(
                    f"💥 BREAKOUT REALE: Velocity={velocity:.5f}, VPIN={vpin:.2f}. "
                    f"CVD {cvd_trend} (delta_30s={delta_30s:+.0f}$). "
                    f"Stile: SCALPING o MOMENTUM. Entry permessa."
                )

        elif fase == 'ESAURIMENTO':
            if sottotipo == 'REVERSAL':
                lines.append(
                    f"🔄 ESAURIMENTO + REVERSAL: Exhaustion={exhaust:.0f}/100 + "
                    f"VPIN={vpin:.2f} istituzionale + CVD {cvd_trend}. "
                    f"Possibile inversione del segnale corrente. "
                    f"Setup contrarian valutabile SOLO con livello tecnico chiaro e R:R favorevole."
                )
            else:
                lines.append(
                    f"😮‍💨 ESAURIMENTO: Exhaustion={exhaust:.0f}/100. Il segnale corrente è maturo. "
                    f"CVD {cvd_trend}. Trade di continuazione ammissibile se trend strutturale (HA daily, "
                    f"multi-TF) ancora intatto; trade di inversione richiede livello tecnico chiaro."
                )

        elif fase == 'SWEEP':
            if sottotipo == 'CONFERMATO':
                lines.append(
                    f"🎯 SWEEP CONFERMATO: Liquidity grab rilevato e CVD già invertito. "
                    f"delta_30s={delta_30s:+.0f}$ | VPIN={vpin:.2f}. "
                    f"Setup ad alta probabilità — il movimento reale parte ora. Entry permessa."
                )
            else:
                lines.append(
                    f"⚠️ SWEEP IN CORSO: Livello violato ma CVD non ha ancora invertito. "
                    f"Attendere conferma CVD prima di entrare."
                )

        elif fase == 'DISTRIBUZIONE':
            lines.append(
                f"📊 DISTRIBUZIONE INTRADAY: VPIN={vpin:.2f} alto + CVD negativo "
                f"(delta_30s={delta_30s:+.0f}$). I venditori sono attivi nel breve. "
                f"Questo NON implica direzione automatica: in trend rialzista forte può essere "
                f"profit-taking temporaneo (continuazione UP probabile). In trend ribassista o "
                f"vicino a resistenza chiave può precedere ribasso. Decidi in base alla struttura."
            )

        elif fase == 'ACCUMULAZIONE':
            lines.append(
                f"📊 ACCUMULAZIONE INTRADAY: VPIN={vpin:.2f} alto + CVD positivo "
                f"(delta_30s={delta_30s:+.0f}$). I compratori sono attivi nel breve. "
                f"In trend rialzista rinforza la continuazione UP. Vicino a supporto chiave o "
                f"post-capitulation può precedere rimbalzo. In trend ribassista forte può essere "
                f"solo dip-buying retail destinato a fallire. Decidi in base alla struttura."
            )

        elif fase == 'RANGE':
            lines.append(
                f"↔️ RANGE ATTIVO: Mercato laterale (Hurst basso, velocity bassa). "
                f"Setup tipico: mean reversion verso VWAP/POC, oppure attesa di rottura. "
                f"Trend following sconsigliato in pieno range, ma se il prezzo è ai bordi del range "
                f"con livello strutturale superiore (HA daily, multi-TF), il breakout può essere imminente."
            )

        elif fase == 'COMPRESSIONE':
            lines.append(
                f"🗜️ COMPRESSIONE INTRADAY: Volatilità in calo, VPIN={vpin:.2f}. "
                f"Il flusso a breve è piatto. Se il trend di fondo (multi-TF, HA daily) è chiaro, "
                f"questo è spesso consolidamento DENTRO il trend — il movimento riprende nella "
                f"stessa direzione. Solo se la struttura è confusa, attendere conferma di breakout."
            )

        elif fase == 'DIVERGENZA':
            lines.append(
                f"⚡ DIVERGENZA INTRADAY: Prezzo e CVD a 30s in direzioni opposte (delta_30s={delta_30s:+.0f}$). "
                f"Il movimento di breve non è confermato dal flusso. Può essere divergenza tecnica "
                f"reale o solo rumore intraday in trend forte. Verifica la coerenza con TF superiori "
                f"prima di interpretare come segnale di inversione."
            )

        elif fase == 'CORRELAZIONE_ROTTA':
            lines.append(
                f"🔌 CORRELAZIONE ROTTA: Asset si muove indipendentemente da BTC. "
                f"Narrativa propria — verifica se c'è una notizia specifica. "
                f"Può essere opportunità o manipulation."
            )

        # Override disponibile
        if override_ok and override_cond:
            lines.append(f"⚡ OVERRIDE DISPONIBILE: {override_cond}")

        return " | ".join(lines)
