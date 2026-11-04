# -*- coding: utf-8 -*-
"""
CHIMERA v4.0 — MarketShield
Protezione capitale: converte crypto spot in USD quando il mercato scende,
redistribuisce quando il trend torna rialzista.

Logica:
  SHIELD ON  → mercato BEARISH confermato → converti crypto spot in USD
  SHIELD OFF → mercato BULLISH confermato → redistribuisci USD in crypto

Criteri SHIELD ON (servono almeno 3 su 5):
  1. BTC Hurst < 0.45 (trend morto o ribassista)
  2. Macro BEARISH (DXY su + NASDAQ giù)
  3. BTC sotto EMA20 su 4h
  4. CVD BTC negativo nelle ultime 2h
  5. Fear & Greed < 35

Criteri SHIELD OFF (servono almeno 3 su 4):
  1. BTC Hurst > 0.55 (trend rialzista)
  2. Macro BULLISH o NEUTRAL
  3. BTC sopra EMA20 su 4h
  4. Fear & Greed > 45

Cooldown: minimo 4h tra shield ON e OFF (evita oscillazioni)
Solo posizioni SPOT (leva=1) vengono convertite — le posizioni a margine
vengono lasciate aperte perché gestiscono il rischio da sole con SL/TP.
"""

import logging
import time
from datetime import datetime

from core.chimera_errors import ErrorTracker
import sys

_err = ErrorTracker("MarketShield")

logger = logging.getLogger("MarketShield")


class MarketShield:
    """
    Monitora il trend macro e protegge il capitale spot convertendo in USD.

    Uso in bot_la.py:
        shield = MarketShield(engine, macro, performer, alerts)

        # Nel loop ogni quarto d'ora:
        if e_quarto_d_ora:
            shield.valuta_e_agisci(trade_manager)
    """

    def __init__(self, engine, macro_sentiment, performer, alerts=None):
        self.engine  = engine
        self.macro   = macro_sentiment
        self.perf    = performer
        self.alerts  = alerts
        self.logger  = logging.getLogger("MarketShield")

        self._shield_attivo     = False   # True = siamo in USD
        self._ultimo_cambio     = 0.0     # timestamp ultimo ON/OFF
        self._cooldown_s        = 4 * 3600  # 4h minimo tra switch
        self._ultimo_check      = 0.0
        self._check_interval_s  = 900     # controlla ogni 15 min

    # ─────────────────────────────────────────────────────────────
    def valuta_e_agisci(self, trade_manager):
        """
        Punto di ingresso principale. Chiama ogni quarto d'ora.
        Valuta il trend e converte/redistribuisce se necessario.
        """
        now = time.time()
        if now - self._ultimo_check < self._check_interval_s:
            return
        self._ultimo_check = now

        try:
            segnali = self._analizza_trend()
            score_bearish = segnali["score_bearish"]
            score_bullish = segnali["score_bullish"]

            self.logger.info(
                f"🛡️ [SHIELD] Bearish={score_bearish}/5 Bullish={score_bullish}/4 | "
                f"Stato={'ON (USD)' if self._shield_attivo else 'OFF (CRYPTO)'}"
            )

            cooldown_ok = (now - self._ultimo_cambio) > self._cooldown_s

            # ── SHIELD ON: converti in USD ──────────────────────
            if not self._shield_attivo and score_bearish >= 3 and cooldown_ok:
                self._attiva_shield(trade_manager, segnali)

            # ── SHIELD OFF: redistribuisci in crypto ────────────
            elif self._shield_attivo and score_bullish >= 3 and cooldown_ok:
                self._disattiva_shield(trade_manager, segnali)

        except Exception as e:
            _err.capture(e, "valuta_e_agisci", {"shield": self._shield_attivo})

    # ─────────────────────────────────────────────────────────────
    def _analizza_trend(self) -> dict:
        """
        Calcola score bearish e bullish basati su 5 indicatori.
        """
        segnali = {
            "score_bearish": 0,
            "score_bullish": 0,
            "dettagli": []
        }

        # ── 1. BTC Hurst ──────────────────────────────────────
        try:
            dati_btc = self.engine.get_full_market_data("XXBTZUSD")
            dati_btc = dati_btc[0] if isinstance(dati_btc, tuple) else dati_btc
            hurst = float(dati_btc.get("hurst_exponent", 0.5) or 0.5)
            btc_price = float(dati_btc.get("close", 0) or 0)
            cvd_btc   = float(dati_btc.get("cvd_istantaneo", 0) or 0)

            if hurst < 0.45:
                segnali["score_bearish"] += 1
                segnali["dettagli"].append(f"Hurst BTC {hurst:.2f} < 0.45 (trend ribassista)")
            elif hurst > 0.55:
                segnali["score_bullish"] += 1
                segnali["dettagli"].append(f"Hurst BTC {hurst:.2f} > 0.55 (trend rialzista)")

            # ── 2. BTC vs EMA20 4h ────────────────────────────
            try:
                multi_tf = dati_btc.get("multi_tf", {})
                tf_4h    = multi_tf.get("4h", {}) if isinstance(multi_tf, dict) else {}
                ema20_4h = float(tf_4h.get("ema20", 0) or 0)
                if ema20_4h > 0 and btc_price > 0:
                    if btc_price < ema20_4h * 0.998:
                        segnali["score_bearish"] += 1
                        segnali["dettagli"].append(f"BTC {btc_price:.0f} sotto EMA20 4h {ema20_4h:.0f}")
                    elif btc_price > ema20_4h * 1.002:
                        segnali["score_bullish"] += 1
                        segnali["dettagli"].append(f"BTC {btc_price:.0f} sopra EMA20 4h {ema20_4h:.0f}")
            except Exception as e:
                _err.capture(e, "_analizza_trend.ema20", {"module": "MarketShield"})

            # ── 3. CVD BTC ────────────────────────────────────
            if cvd_btc < -50000:
                segnali["score_bearish"] += 1
                segnali["dettagli"].append(f"CVD BTC {cvd_btc:,.0f} (flusso vendite)")
            elif cvd_btc > 50000:
                segnali["score_bullish"] += 1
                segnali["dettagli"].append(f"CVD BTC {cvd_btc:,.0f} (flusso acquisti)")

        except Exception as e:
            _err.capture(e, "_analizza_trend.btc", {"module": "MarketShield"})

        # ── 4. Macro sentiment ────────────────────────────────
        try:
            _, macro_sent = self.macro.get_macro_data()
            if macro_sent == "BEARISH":
                segnali["score_bearish"] += 1
                segnali["dettagli"].append("Macro BEARISH (DXY↑ NASDAQ↓)")
            elif macro_sent in ("BULLISH", "NEUTRAL"):
                segnali["score_bullish"] += 1
                segnali["dettagli"].append(f"Macro {macro_sent}")
        except Exception as e:
            _err.capture(e, "_analizza_trend.macro", {"module": "MarketShield"})

        # ── 5. Fear & Greed ───────────────────────────────────
        try:
            fg = self.engine.get_fear_greed()
            fg_val = fg.get("fear_greed_value", 50) if isinstance(fg, dict) else int(fg or 50)
            if fg_val < 35:
                segnali["score_bearish"] += 1
                segnali["dettagli"].append(f"Fear&Greed {fg_val} (paura)")
            elif fg_val > 45:
                segnali["score_bullish"] += 1
                segnali["dettagli"].append(f"Fear&Greed {fg_val} (neutro/greed)")
        except Exception as e:
            _err.capture(e, "_analizza_trend.fg", {"module": "MarketShield"})

        return segnali

    # ─────────────────────────────────────────────────────────────
    def _attiva_shield(self, trade_manager, segnali):
        """
        SHIELD ON: converte posizioni SPOT crypto in USD.
        Le posizioni a margine (leva > 1) vengono lasciate — gestiscono il rischio da sole.
        """
        try:
            self.logger.warning("🛡️ [SHIELD ON] Mercato BEARISH — conversione crypto spot in USD")

            convertiti = []
            for asset, pos in list(trade_manager.posizioni_aperte.items()):
                leva = int(pos.get("leverage", 1) or 1)
                if leva > 1:
                    self.logger.info(f"⏭️ [SHIELD] {asset} leva {leva}x — lasciata aperta (margine)")
                    continue

                # Posizione SPOT — converti in USD vendendo
                try:
                    size   = float(pos.get("size", 0) or 0)
                    prezzo = float(pos.get("prezzo_corrente") or pos.get("p_entrata") or 0)
                    if size <= 0 or prezzo <= 0:
                        continue

                    # Chiudi tramite trade_manager (usa settle-position)
                    ok = trade_manager._esegui_chiusura_totale(
                        asset, prezzo, motivo="MARKET_SHIELD_BEARISH"
                    )
                    if ok:
                        convertiti.append(asset)
                        self.logger.info(f"✅ [SHIELD] {asset} convertito in USD")
                    else:
                        self.logger.warning(f"⚠️ [SHIELD] {asset} conversione fallita")

                except Exception as e:
                    _err.capture(e, "_attiva_shield.asset", {"asset": asset})

            self._shield_attivo = True
            self._ultimo_cambio = time.time()

            # Notifica
            motivi = "\n".join(f"  • {d}" for d in segnali["dettagli"])
            msg = (
                f"🛡️ *MARKET SHIELD ATTIVATO*\n"
                f"Mercato BEARISH — {segnali['score_bearish']}/5 segnali\n\n"
                f"*Segnali rilevati:*\n{motivi}\n\n"
                f"*Convertiti in USD:* {', '.join(convertiti) if convertiti else 'Nessuno (solo posizioni margin)'}\n"
                f"💰 Capitale protetto in USD fino a segnale rialzista."
            )
            if self.alerts:
                self.alerts.invia_alert(msg)

        except Exception as e:
            _err.capture(e, "_attiva_shield", {"module": "MarketShield"})

    # ─────────────────────────────────────────────────────────────
    def _disattiva_shield(self, trade_manager, segnali):
        """
        SHIELD OFF: il mercato torna rialzista.
        Non riacquista automaticamente — sblocca semplicemente il bot
        per aprire nuove posizioni normalmente.
        """
        try:
            self.logger.info("🟢 [SHIELD OFF] Mercato BULLISH — bot torna operativo")

            self._shield_attivo = False
            self._ultimo_cambio = time.time()

            motivi = "\n".join(f"  • {d}" for d in segnali["dettagli"])
            msg = (
                f"🟢 *MARKET SHIELD DISATTIVATO*\n"
                f"Trend BULLISH — {segnali['score_bullish']}/4 segnali\n\n"
                f"*Segnali rilevati:*\n{motivi}\n\n"
                f"Bot torna operativo — nuove posizioni saranno aperte normalmente."
            )
            if self.alerts:
                self.alerts.invia_alert(msg)

        except Exception as e:
            _err.capture(e, "_disattiva_shield", {"module": "MarketShield"})

    # ─────────────────────────────────────────────────────────────
    @property
    def is_attivo(self) -> bool:
        """True se il mercato è in protezione USD."""
        return self._shield_attivo

    def stato(self) -> dict:
        return {
            "shield_attivo":  self._shield_attivo,
            "ultimo_cambio":  datetime.fromtimestamp(self._ultimo_cambio).strftime("%H:%M") if self._ultimo_cambio else "mai",
            "prossimo_check": max(0, int(self._check_interval_s - (time.time() - self._ultimo_check))),
        }
