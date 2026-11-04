# -*- coding: utf-8 -*-
"""
SequenceBuffer — Infrastruttura dati per LSTM futuro.

Raccoglie sequenze di snapshot di mercato per ogni asset.
Ogni volta che engine_la calcola i dati, chiama push_snapshot().
Quando un trade o ghost viene registrato, chiama get_sequence()
per ottenere gli ultimi N snapshot da allegare al record.

I dati vengono salvati nel DB per persistenza tra riavvii.
La raccolta è leggera — solo i campi che LSTM userà (12 core features).

Uso:
    from core.sequence_buffer import seq_buf
    seq_buf.push_snapshot("XXBTZUSD", dati_mercato)   # in engine_la
    seq = seq_buf.get_sequence("XXBTZUSD", n=30)       # in feedback_engine
"""

import time
import logging
import json
from collections import deque
from typing import Dict, List, Optional
from core.chimera_errors import ErrorTracker

_err = ErrorTracker("SequenceBuffer")

logger = logging.getLogger("SequenceBuffer")

# Campi core che LSTM userà — bilanciati tra completezza e dimensionalità
# Troppi campi → overfitting, troppo pochi → perde informazione
LSTM_FEATURES = [
    "cvd_istantaneo",       # flusso ordini cumulativo
    "vpin",                 # tossicità flusso
    "price_velocity",       # velocità prezzo
    "order_flow_imbalance", # squilibrio book
    "whale_delta",          # flusso grossi player
    "hurst_exponent",       # regime mercato
    "z_score",              # posizione statistica
    "book_pressure",        # pressione book
    "funding_rate",         # costo carry
    "rsi",                  # momentum
    "rolling_volatility",   # volatilità
    "market_regime_num",    # regime numerico (TRENDING=1, MR=-1, LATERAL=0)
]

# Lunghezza sequenza: 30 snapshot ≈ 90 minuti di storia (ciclo ~3min)
SEQ_LEN = 30

# Max sequenze salvate in memoria per asset (rolling window)
MAX_IN_MEMORY = 200


class SequenceBuffer:
    """
    Buffer rotante di snapshot per asset.
    Thread-safe tramite strutture atomiche Python (deque).
    """

    def __init__(self):
        # {ticker: deque(maxlen=MAX_IN_MEMORY)}
        self._buffers: Dict[str, deque] = {}
        self._last_save: Dict[str, float] = {}
        self._save_interval = 300  # salva su DB ogni 5 minuti

    def push_snapshot(self, ticker: str, dati_mercato: dict) -> None:
        """
        Aggiunge uno snapshot alla sequenza dell'asset.
        Chiamare ad ogni ciclo di get_full_market_data.
        """
        if not dati_mercato or not dati_mercato.get("close"):
            return

        if ticker not in self._buffers:
            self._buffers[ticker] = deque(maxlen=MAX_IN_MEMORY)
            self._load_from_db(ticker)

        # Estrae solo i campi LSTM — compatto e veloce
        regime = str(dati_mercato.get("market_regime", "")).upper()
        regime_num = (1.0 if "TRENDING" in regime
                      else -1.0 if "MEAN" in regime
                      else 0.0)

        snap = {
            "ts":                  time.time(),
            "close":               float(dati_mercato.get("close", 0)),
            "cvd_istantaneo":      float(dati_mercato.get("cvd_istantaneo", 0)),
            "vpin":                float(dati_mercato.get("vpin", 0.5)),
            "price_velocity":      float(dati_mercato.get("price_velocity", 0)),
            "order_flow_imbalance":float(dati_mercato.get("order_flow_imbalance", 0)),
            "whale_delta":         float(dati_mercato.get("whale_delta", 0)),
            "hurst_exponent":      float(dati_mercato.get("hurst_exponent", 0.5)),
            "z_score":             float(dati_mercato.get("z_score", 0)),
            "book_pressure":       float(dati_mercato.get("book_pressure", 0.5)),
            "funding_rate":        float(dati_mercato.get("funding_rate", 0)),
            "rsi":                 float(dati_mercato.get("rsi", 50)),
            "rolling_volatility":  float(dati_mercato.get("rolling_volatility", 0)),
            "market_regime_num":   regime_num,
        }

        self._buffers[ticker].append(snap)

        # Salva periodicamente su DB
        now = time.time()
        if now - self._last_save.get(ticker, 0) > self._save_interval:
            self._save_to_db(ticker)
            self._last_save[ticker] = now

    def get_sequence(self, ticker: str, n: int = SEQ_LEN) -> Optional[List[dict]]:
        """
        Restituisce gli ultimi N snapshot per l'asset.
        Restituisce None se non ci sono abbastanza dati (< n/2).
        """
        buf = self._buffers.get(ticker)
        if not buf:
            self._load_from_db(ticker)
            buf = self._buffers.get(ticker)

        if not buf or len(buf) < max(5, n // 2):
            return None  # troppo pochi dati — non utile per LSTM

        snaps = list(buf)[-n:]  # ultimi N

        # Padding se necessario (meno di N snapshot disponibili)
        if len(snaps) < n:
            pad = [snaps[0].copy() for _ in range(n - len(snaps))]
            snaps = pad + snaps

        return snaps

    def get_sequence_as_features(self, ticker: str, n: int = SEQ_LEN) -> Optional[List[List[float]]]:
        """
        Restituisce la sequenza come lista di vettori float.
        Formato: [[feat1, feat2, ...], ...] shape (n, len(LSTM_FEATURES))
        Pronto per essere passato a un modello LSTM.
        """
        seq = self.get_sequence(ticker, n)
        if seq is None:
            return None

        result = []
        for snap in seq:
            row = [float(snap.get(f, 0.0) or 0.0) for f in LSTM_FEATURES]
            result.append(row)
        return result

    def get_stats(self) -> dict:
        """Stats per logging/monitoring."""
        return {
            ticker: len(buf)
            for ticker, buf in self._buffers.items()
        }

    # ── Persistenza DB ────────────────────────────────────────────────────────

    def _save_to_db(self, ticker: str) -> None:
        """Salva il buffer corrente nel DB."""
        try:
            from core.database_manager import db_manager
            buf = self._buffers.get(ticker)
            if not buf:
                return
            # Salva solo gli ultimi SEQ_LEN*3 per non crescere infinito
            data = list(buf)[-SEQ_LEN * 3:]
            db_manager.save_sequence_buffer(ticker, data)
        except Exception as e:
            _err.capture(e, "_save_to_db", {"module": "SequenceBuffer"})
            logger.debug(f"Sequence save {ticker}: {e}")

    def _load_from_db(self, ticker: str) -> None:
        """Carica il buffer dal DB all'avvio."""
        try:
            from core.database_manager import db_manager
            data = db_manager.get_sequence_buffer(ticker)
            if data:
                self._buffers[ticker] = deque(data, maxlen=MAX_IN_MEMORY)
                logger.debug(f"SequenceBuffer [{ticker}]: caricati {len(data)} snapshot dal DB")
        except Exception as e:
            _err.capture(e, "_load_from_db", {"module": "SequenceBuffer"})
            logger.debug(f"Sequence load {ticker}: {e}")
            self._buffers[ticker] = deque(maxlen=MAX_IN_MEMORY)


# Singleton globale — importato da tutti i moduli
seq_buf = SequenceBuffer()
