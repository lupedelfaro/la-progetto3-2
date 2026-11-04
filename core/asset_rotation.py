# -*- coding: utf-8 -*-
"""
CHIMERA v4.0 — Asset Rotation & Market Shield
Logica unificata:

  DOWNTREND confermato (≥3/5 segnali bearish):
    → Converti tutto il capitale SPOT da crypto a USD
    → Blocca nuove entry crypto
    → Mantieni posizioni a margine aperte (hanno SL/TP)

  UPTREND confermato (≥3/4 segnali bullish):
    → Distribuisci capitale USD tra gli asset in ASSET_LIST
    → Quota proporzionale al momentum di ogni asset (CVD + Hurst + velocity)
    → Sblocca nuove entry

  LATERALE / INCERTO:
    → Mantieni lo stato attuale senza cambiamenti
"""

import json
import logging
import time
import sys
from datetime import datetime, timedelta
from core.chimera_errors import ErrorTracker

_err = ErrorTracker("AssetRotation")


# ══════════════════════════════════════════════════════════════════════════════
#  ASSET ROTATION + MARKET SHIELD (modulo unificato)
# ══════════════════════════════════════════════════════════════════════════════

class AssetRotation:
    """
    Gestisce la rotazione del capitale tra crypto e USD in base al trend macro.

    Stato:
      - IN_CRYPTO : capitale distribuito tra gli asset (default)
      - IN_USD    : capitale convertito in USD — mercato bearish
    """

    def __init__(self, asset_list, feedback_engine=None, alerts=None,
                 engine=None, macro_sentiment=None):
        self.asset_list      = asset_list      # lista asset attivi
        self.feedback_engine = feedback_engine
        self.alerts          = alerts
        self.engine          = engine          # per leggere dati mercato
        self.macro           = macro_sentiment
        self.logger          = logging.getLogger("AssetRotation")

        # Stato corrente
        self._stato            = "IN_CRYPTO"   # "IN_CRYPTO" | "IN_USD"
        self._ultimo_cambio    = 0.0
        self._cooldown_s       = 4 * 3600      # 4h minimo tra switch
        self._ultimo_check     = 0.0
        self._check_interval   = 900           # controlla ogni 15 min

        self._ultima_rotazione = {}            # cooldown per asset singolo

    # ─────────────────────────────────────────────────────────────────────────
    #  PUNTO DI INGRESSO PRINCIPALE
    # ─────────────────────────────────────────────────────────────────────────

    def valuta_e_agisci(self, trade_manager, dati_mercato_tutti: dict = None):
        """
        Chiamare ogni quarto d'ora nel loop principale.
        Analizza il trend macro e converte/redistribuisce se necessario.
        """
        now = time.time()
        if now - self._ultimo_check < self._check_interval:
            return
        self._ultimo_check = now

        try:
            segnali = self._analizza_trend_macro(dati_mercato_tutti or {})
            score_b = segnali["score_bearish"]
            score_u = segnali["score_bullish"]
            cooldown_ok = (now - self._ultimo_cambio) > self._cooldown_s

            self.logger.info(
                f"🔄 [ROTATION] Bearish={score_b}/5 Bullish={score_u}/4 | "
                f"Stato={self._stato} | Cooldown={'OK' if cooldown_ok else 'WAIT'}"
            )

            # ── DOWNTREND: converti in USD ───────────────────────────────
            if self._stato == "IN_CRYPTO" and score_b >= 3 and cooldown_ok:
                self._converti_in_usd(trade_manager, segnali)

            # ── UPTREND: redistribuisci in crypto ────────────────────────
            elif self._stato == "IN_USD" and score_u >= 3 and cooldown_ok:
                self._distribuisci_in_crypto(trade_manager, dati_mercato_tutti or {}, segnali)

            elif self._stato == "IN_USD" and score_u >= 3 and not cooldown_ok:
                ore_rimanenti = round((self._cooldown_s - (time.time() - self._ultimo_cambio)) / 3600, 1)
                self.logger.info(f"⏳ [ROTATION] Segnale bullish ({score_u}/4) ma cooldown attivo ancora {ore_rimanenti}h")

            elif self._stato == "IN_USD" and score_b == 0 and score_u == 0:
                self.logger.info("⏳ [ROTATION] IN_USD — nessun segnale chiaro, attendo conferma bullish")

        except Exception as e:
            _err.capture(e, "valuta_e_agisci", {"stato": self._stato})

    # ─────────────────────────────────────────────────────────────────────────
    #  ANALISI TREND MACRO
    # ─────────────────────────────────────────────────────────────────────────

    def _analizza_trend_macro(self, dati_mercato_tutti: dict) -> dict:
        """
        5 segnali bearish / 4 segnali bullish basati su BTC come proxy del mercato.
        """
        segnali = {"score_bearish": 0, "score_bullish": 0, "dettagli": []}

        try:
            # Prendi dati BTC dal dizionario già calcolato
            dati_btc = dati_mercato_tutti.get("XXBTZUSD", {})
            if not dati_btc and self.engine:
                try:
                    res = self.engine.get_full_market_data("XXBTZUSD")
                    dati_btc = res[0] if isinstance(res, tuple) else res
                except Exception:
                    dati_btc = {}

            hurst    = float(dati_btc.get("hurst_exponent", 0.5) or 0.5)
            cvd      = float(dati_btc.get("cvd_istantaneo", 0) or 0)
            btc_price= float(dati_btc.get("close", 0) or 0)
            tema     = str(dati_btc.get("ema_trend_dominante", "") or "")
            multi_tf = dati_btc.get("multi_tf", {}) if isinstance(dati_btc, dict) else {}
            tf_4h    = multi_tf.get("4h", {}) if isinstance(multi_tf, dict) else {}
            ema20_4h = float(tf_4h.get("ema20", 0) or 0)

            # 1. BTC Hurst
            if hurst < 0.45:
                segnali["score_bearish"] += 1
                segnali["dettagli"].append(f"Hurst {hurst:.2f} < 0.45 (trend ribassista)")
            elif hurst > 0.55:
                segnali["score_bullish"] += 1
                segnali["dettagli"].append(f"Hurst {hurst:.2f} > 0.55 (trend rialzista)")

            # 2. BTC vs EMA20 4h
            if ema20_4h > 0 and btc_price > 0:
                if btc_price < ema20_4h * 0.998:
                    segnali["score_bearish"] += 1
                    segnali["dettagli"].append(f"BTC {btc_price:.0f} sotto EMA20 4h {ema20_4h:.0f}")
                elif btc_price > ema20_4h * 1.002:
                    segnali["score_bullish"] += 1
                    segnali["dettagli"].append(f"BTC {btc_price:.0f} sopra EMA20 4h {ema20_4h:.0f}")

            # 3. EMA trend dominante
            if tema == "RIBASSISTA":
                segnali["score_bearish"] += 1
                segnali["dettagli"].append("EMA trend dominante RIBASSISTA")
            elif tema == "RIALZISTA":
                segnali["score_bullish"] += 1
                segnali["dettagli"].append("EMA trend dominante RIALZISTA")

            # 4. CVD BTC
            if cvd < -50000:
                segnali["score_bearish"] += 1
                segnali["dettagli"].append(f"CVD BTC {cvd:,.0f} (vendite istituzionali)")
            elif cvd > 50000:
                segnali["score_bullish"] += 1
                segnali["dettagli"].append(f"CVD BTC {cvd:,.0f} (acquisti istituzionali)")

            # 5. Macro sentiment
            if self.macro:
                try:
                    _, macro_sent = self.macro.get_macro_data()
                    if macro_sent == "BEARISH":
                        segnali["score_bearish"] += 1
                        segnali["dettagli"].append("Macro BEARISH (DXY↑ NASDAQ↓)")
                    elif macro_sent in ("BULLISH", "NEUTRAL"):
                        segnali["score_bullish"] += 1
                        segnali["dettagli"].append(f"Macro {macro_sent}")
                except Exception as e:
                    _err.capture(e, "_analizza_trend_macro.macro", {"module": "AssetRotation"})

        except Exception as e:
            _err.capture(e, "_analizza_trend_macro", {"module": "AssetRotation"})

        return segnali

    # ─────────────────────────────────────────────────────────────────────────
    #  DOWNTREND: CONVERTI IN USD
    # ─────────────────────────────────────────────────────────────────────────

    def _converti_in_usd(self, trade_manager, segnali: dict):
        """
        Mercato BEARISH — chiude le posizioni SPOT e converte in USD.
        Le posizioni a margine (leva > 1) vengono lasciate aperte.
        """
        try:
            self.logger.warning("🔴 [ROTATION] Mercato BEARISH — conversione crypto SPOT → USD")

            convertiti = []
            for asset, pos in list(trade_manager.posizioni_aperte.items()):
                leva = int(pos.get("leverage", 1) or 1)
                if leva > 1:
                    self.logger.info(f"⏭️ [{asset}] leva {leva}x — lasciata aperta (margine)")
                    continue

                try:
                    prezzo = float(
                        pos.get("prezzo_corrente") or
                        pos.get("p_entrata") or 0
                    )
                    if prezzo <= 0:
                        continue
                    ok = trade_manager._esegui_chiusura_totale(
                        asset, prezzo, motivo="ROTATION_BEARISH_TO_USD"
                    )
                    if ok:
                        convertiti.append(asset)
                        self.logger.info(f"✅ [{asset}] convertito in USD")
                except Exception as e:
                    _err.capture(e, "_converti_in_usd.asset", {"asset": asset})

            self._stato         = "IN_USD"
            self._ultimo_cambio = time.time()

            motivi = "\n".join(f"  • {d}" for d in segnali["dettagli"])
            msg = (
                f"🔴 *ROTATION — MERCATO BEARISH*\n"
                f"Segnali bearish: {segnali['score_bearish']}/5\n\n"
                f"*Segnali:*\n{motivi}\n\n"
                f"*Convertiti in USD:* {', '.join(convertiti) if convertiti else 'nessuno (solo margine)'}\n"
                f"💰 Capitale in USD fino a segnale rialzista (min 4h)"
            )
            if self.alerts:
                self.alerts.invia_alert(msg)

        except Exception as e:
            _err.capture(e, "_converti_in_usd", {"module": "AssetRotation"})

    # ─────────────────────────────────────────────────────────────────────────
    #  UPTREND: DISTRIBUISCI IN CRYPTO
    # ─────────────────────────────────────────────────────────────────────────

    def _distribuisci_in_crypto(self, trade_manager, dati_mercato_tutti: dict, segnali: dict):
        """
        Mercato BULLISH — il bot torna operativo e può aprire posizioni.
        Non compra direttamente: sblocca il bot che troverà i suoi setup.
        Calcola e logga la distribuzione ottimale per informazione.
        """
        try:
            self.logger.info("🟢 [ROTATION] Mercato BULLISH — bot torna operativo")

            # Calcola momentum per ogni asset (per info)
            scores = {}
            for asset in self.asset_list:
                dati = dati_mercato_tutti.get(asset, {})
                if not dati:
                    continue
                hurst = float(dati.get("hurst_exponent", 0.5) or 0.5)
                cvd   = float(dati.get("cvd_istantaneo", 0) or 0)
                vel   = abs(float(dati.get("price_velocity", 0) or 0))
                # Score momentum: Hurst trend + CVD positivo + velocity
                score = (hurst * 40) + (min(abs(cvd), 500000) / 500000 * 40) + (vel * 1000 * 20)
                if cvd < 0:
                    score *= 0.5  # penalizza asset con CVD negativo
                scores[asset] = round(score, 1)

            # Ordina per momentum
            ranking = sorted(scores.items(), key=lambda x: -x[1])
            tot_score = sum(v for _, v in ranking) or 1
            distribuzione = {a: round(v/tot_score*100, 1) for a, v in ranking}

            self._stato         = "IN_CRYPTO"
            self._ultimo_cambio = time.time()

            motivi = "\n".join(f"  • {d}" for d in segnali["dettagli"])
            dist_str = "\n".join(f"  {a}: {p}%" for a, p in distribuzione.items() if p > 0)
            msg = (
                f"🟢 *ROTATION — MERCATO BULLISH*\n"
                f"Segnali bullish: {segnali['score_bullish']}/4\n\n"
                f"*Segnali:*\n{motivi}\n\n"
                f"*Distribuzione ottimale per momentum:*\n{dist_str}\n\n"
                f"Bot operativo — nuove posizioni aperte sui setup migliori."
            )
            if self.alerts:
                self.alerts.invia_alert(msg)

        except Exception as e:
            _err.capture(e, "_distribuisci_in_crypto", {"module": "AssetRotation"})

    # ─────────────────────────────────────────────────────────────────────────
    #  VALUTA SOSPENSIONE SINGOLO ASSET
    # ─────────────────────────────────────────────────────────────────────────

    def valuta_sospensione_asset(self, asset, dati_mercato) -> tuple:
        """
        Controlla se un asset specifico deve essere sospeso a livello di analisi.
        Criteri: WR < 45% ultimi 10 trade OPPURE MACD 4h negativo + trend RIBASSISTA.

        IMPORTANTE (modifica 30/04/2026): in stato IN_USD NON sospendiamo più
        l'analisi a livello globale. La rotation gestisce SOLO la conversione
        spot↔USD del capitale. Le operazioni a margine (LONG e SHORT) e gli
        SPOT SHORT devono poter essere analizzati normalmente — il blocco
        specifico per SPOT LONG nuovi avviene al gate di esecuzione in
        bot_la.py (riga ~1843), che è il punto giusto perché lì la decisione
        di Gemini è già stata presa e si conosce la leva richiesta.

        Returns: (sospeso: bool, motivo: str)
        """
        try:
            # NB: l'ex blocco "if IN_USD: return True" è stato RIMOSSO.
            # Vedi commento sopra. Il blocco SPOT LONG avviene al gate
            # di esecuzione, non qui.

            from core.database_manager import db_manager
            storico = db_manager.get_storico()
            trade_asset = [t for t in storico
                          if t.get('asset') == asset
                          and t.get('esito') in ('WIN', 'LOSS')][-10:]

            if len(trade_asset) >= 8:
                wr = sum(1 for t in trade_asset if t.get('esito') == 'WIN') / len(trade_asset)
                if wr < 0.45:
                    return True, f"WR {wr:.0%} < 45% (ultimi {len(trade_asset)} trade)"

            # MACD 4h negativo + trend ribassista
            multi_tf = dati_mercato.get('multi_tf', {}) if isinstance(dati_mercato, dict) else {}
            tf_4h    = multi_tf.get('4h', {}) if isinstance(multi_tf, dict) else {}
            macd_4h  = float(tf_4h.get('macd', tf_4h.get('mac_d', 0)) or 0)
            tema     = str(dati_mercato.get('ema_trend_dominante', '') or '')

            if macd_4h < -5 and tema == 'RIBASSISTA':
                return True, f"MACD 4h {macd_4h:.1f} + trend RIBASSISTA"

            return False, ""

        except Exception as e:
            _err.capture(e, "valuta_sospensione_asset", {"asset": asset})
            return False, ""

    # ─────────────────────────────────────────────────────────────────────────
    #  ROTAZIONE TRA ASSET (logica originale — stagnazione)
    # ─────────────────────────────────────────────────────────────────────────

    def valuta_rotazione(self, posizioni_aperte, dati_mercato_tutti):
        """
        Suggerisce rotazione da asset stagnante a asset con momentum migliore.
        Usato solo quando siamo IN_CRYPTO e non c'è un segnale macro forte.
        """
        if self._stato == "IN_USD":
            return []  # in USD — nessuna rotazione tra asset

        raccomandazioni = []
        now = time.time()

        for asset_aperto, pos in posizioni_aperte.items():
            if now - self._ultima_rotazione.get(asset_aperto, 0) < 14400:
                continue

            ore_aperto = self._calcola_ore(pos)
            tipo_op    = str(pos.get("tipo_op", "Swing")).lower()
            soglia     = 2.0 if "scalp" in tipo_op else 24.0

            if ore_aperto < soglia * 0.8:
                continue

            dati_correnti = dati_mercato_tutti.get(asset_aperto, {})
            vel_corrente  = abs(float(dati_correnti.get("price_velocity", 0)))

            if vel_corrente > 0.0001:
                continue  # ancora in movimento

            score_corrente = vel_corrente * 1000 + float(dati_correnti.get("vpin", 0.5)) * 10

            for asset_alt in self.asset_list:
                if asset_alt == asset_aperto or asset_alt in posizioni_aperte:
                    continue
                dati_alt = dati_mercato_tutti.get(asset_alt, {})
                if not dati_alt or not dati_alt.get("close"):
                    continue

                vel_alt   = abs(float(dati_alt.get("price_velocity", 0)))
                vpin_alt  = float(dati_alt.get("vpin", 0.5))
                hurst_alt = float(dati_alt.get("hurst_exponent", 0.5))
                score_alt = (vel_alt * 1000) + (vpin_alt * 10)

                if score_alt > score_corrente * 3 and score_alt > 0.5:
                    motivo = (
                        f"{asset_aperto} stagnante da {ore_aperto:.1f}h | "
                        f"{asset_alt} momentum {score_alt:.2f} (vel={vel_alt:.6f}, "
                        f"VPIN={vpin_alt:.2f}, Hurst={hurst_alt:.3f})"
                    )
                    raccomandazioni.append({
                        "da": asset_aperto, "a": asset_alt,
                        "score_delta": round(score_alt - score_corrente, 3),
                        "motivo": motivo, "ore_aperto": round(ore_aperto, 1),
                    })
                    self.logger.info(f"🔄 ROTAZIONE SUGGERITA: {motivo}")
                    break

        return sorted(raccomandazioni, key=lambda x: x["score_delta"], reverse=True)

    def notifica_rotazione(self, raccomandazioni):
        if not raccomandazioni or not self.alerts:
            return
        msg = "🔄 *ROTAZIONE CAPITALE SUGGERITA*\n"
        for r in raccomandazioni[:3]:
            msg += f"\n• Da {r['da']} → {r['a']}\n  {r['motivo']}\n"
        self.alerts.invia_alert(msg)

    def esegui_rotazione(self, raccomandazioni, trade_manager, brain,
                         dati_mercato_tutti, macro_sentiment="NEUTRAL",
                         soglia_score_delta=1.5):
        """
        Micro-rotazione tra asset specifici (diverso dalla rotazione macro USD↔crypto).

        La rotazione macro (DOWNTREND → USD, UPTREND → crypto) è gestita da
        valuta_e_agisci() → _converti_in_usd() / _distribuisci_in_crypto().

        Questo metodo gestisce la rotazione tra singoli asset dentro IN_CRYPTO:
        se un asset è stagnante e un altro ha più momentum, suggerisce lo switch.

        NOTA: attualmente registra le rotazioni suggerite ma NON esegue
        aperture/chiusure reali su Kraken. L'implementazione completa richiede:
          1. Conferma IA sull'asset target (brain.full_global_strategy)
          2. Chiusura parziale 50% dell'asset stagnante (trade_manager._chiudi_parzialmente)
          3. Apertura sulla nuova posizione (trade_manager.apri_posizione)
        """
        if not raccomandazioni:
            return []
        eseguite = []
        try:
            for r in raccomandazioni:
                if r.get("score_delta", 0) < soglia_score_delta:
                    continue
                self._ultima_rotazione[r["da"]] = time.time()
                eseguite.append(r)
                self.logger.info(f"📋 Rotazione suggerita registrata: {r['da']} → {r['a']} (non eseguita su Kraken)")
        except Exception as e:
            _err.capture(e, "esegui_rotazione", {"module": "AssetRotation"})
        return eseguite

    @property
    def stato(self) -> str:
        return self._stato

    @property
    def is_in_usd(self) -> bool:
        return self._stato == "IN_USD"

    def _calcola_ore(self, pos: dict) -> float:
        try:
            dt = datetime.fromisoformat(
                pos.get("data_apertura", "").replace("Z", ""))
            return (datetime.now() - dt).total_seconds() / 3600
        except Exception as e:
            _err.capture(e, "_calcola_ore", {"module": "AssetRotation"})
            return 0.0


# ══════════════════════════════════════════════════════════════════════════════
#  ADVANCED REPORTER (invariato)
# ══════════════════════════════════════════════════════════════════════════════

class AdvancedReporter:
    def __init__(self, brain, trade_manager, feedback_engine, macro_sentiment, alerts=None):
        self.brain   = brain
        self.tm      = trade_manager
        self.fe      = feedback_engine
        self.macro   = macro_sentiment
        self.alerts  = alerts
        self.logger  = logging.getLogger("AdvancedReporter")
        self._ultimo_report_giornaliero  = 0
        self._ultimo_report_settimanale  = 0

    def controlla_e_invia(self):
        from datetime import timezone
        now = datetime.now(timezone.utc)
        try:
            if now.hour == 23 and now.minute < 5:
                if time.time() - self._ultimo_report_giornaliero > 3600:
                    self.invia_report_su_richiesta(giorni=1)
                    self._ultimo_report_giornaliero = time.time()
            if now.weekday() == 6 and now.hour == 20 and now.minute < 5:
                if time.time() - self._ultimo_report_settimanale > 3600:
                    self.invia_report_su_richiesta(giorni=7)
                    self._ultimo_report_settimanale = time.time()
        except Exception as e:
            _err.capture(e, "controlla_e_invia", {"module": "AdvancedReporter"})

    def invia_report_su_richiesta(self, giorni=30):
        try:
            from analytics_report import carica_trades, analizza_trades, formatta_report_telegram
            trades = carica_trades(self.tm.db_path if hasattr(self.tm, 'db_path') else 'chimera.db', giorni=giorni)
            stats  = analizza_trades(trades)
            msg    = formatta_report_telegram(stats, titolo=f"📊 CHIMERA — ULTIMI {giorni} GIORNI")
            if self.alerts:
                self.alerts.invia_alert(msg)
            return msg
        except Exception as e:
            _err.capture(e, "invia_report_su_richiesta", {"giorni": giorni})
            return f"Errore report: {e}"

    def genera_report_serale(self):
        """
        Report serale avanzato chiamato da bot_la alle 20:00.
        Aggrega statistiche del giorno, metriche per asset e invia su Telegram.
        """
        try:
            from analytics_report import carica_trades, analizza_trades, formatta_report_telegram

            # Dati base del giorno
            trades_oggi = carica_trades(
                self.tm.db_path if hasattr(self.tm, 'db_path') else 'chimera.db',
                giorni=1
            )
            stats = analizza_trades(trades_oggi)

            # Metriche per asset (ultimi 20 trade ciascuno)
            per_asset_lines = []
            try:
                from core import asset_list as _al
                for asset in _al.ASSET_PRINCIPALI:
                    m = self.fe.get_asset_metrics(asset, window=20)
                    wr = m.get('win_rate', 0)
                    pf = m.get('profit_factor', 1.0)
                    streak = m.get('streak_loss', 0)
                    if wr < 40 or streak >= 3:
                        per_asset_lines.append(
                            f"  ⚠️ {asset}: WR={wr:.0f}% PF={pf:.2f}"
                            + (f" streak={streak}" if streak >= 3 else "")
                        )
            except Exception as _e_a:
                _err.capture(_e_a, sys._getframe().f_code.co_name, {"module": "AssetRotation"})
                self.logger.debug(f"metriche asset: {_e_a}")

            # Breakdown per stile operativo
            per_tipo_lines = []
            by_tipo = {}
            for t in trades_oggi:
                tp = t.get('tipo_op', 'Swing')
                by_tipo.setdefault(tp, {'n': 0, 'pnl': 0.0, 'w': 0})
                by_tipo[tp]['n'] += 1
                pnl = float(t.get('pnl_finale', 0) or 0)
                by_tipo[tp]['pnl'] += pnl
                if pnl > 0:
                    by_tipo[tp]['w'] += 1
            for tp, d in by_tipo.items():
                wr_tp = d['w'] / d['n'] * 100 if d['n'] else 0
                per_tipo_lines.append(
                    f"  {tp}: {d['w']}/{d['n']} WR={wr_tp:.0f}% PnL={d['pnl']:+.2f}$"
                )

            # Macro sentiment
            macro_str = ""
            try:
                _, macro_sentiment = self.macro.get_macro_data()
                macro_str = f"\n📡 *Macro:* {str(macro_sentiment)[:80]}"
            except Exception:
                pass

            # Costruzione messaggio
            titolo = f"📊 *CHIMERA — REPORT SERALE {datetime.now().strftime('%d/%m')}*"
            msg = formatta_report_telegram(stats, titolo=titolo)

            if per_asset_lines:
                msg += "\n\n⚠️ *Asset sotto soglia:*\n" + "\n".join(per_asset_lines)
            if per_tipo_lines:
                msg += "\n\n📋 *Per stile:*\n" + "\n".join(per_tipo_lines)
            if macro_str:
                msg += macro_str

            if self.alerts:
                self.alerts.invia_alert(msg)
            self.logger.info("✅ Report serale avanzato inviato.")

        except Exception as e:
            _err.capture(e, "genera_report_serale", {"module": "AdvancedReporter"})
            self.logger.error(f"❌ genera_report_serale: {e}")
            raise  # bot_la gestisce il fallback

