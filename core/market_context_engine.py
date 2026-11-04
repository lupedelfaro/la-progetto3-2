# -*- coding: utf-8 -*-
"""
MarketContextEngine — Visione globale del mercato per CHIMERA v4.
Aggiornato ogni 5 minuti. Iniettato nel prompt Gemini come contesto macro.

Fonti (tutte gratuite, no API key aggiuntive):
  - Fear & Greed Index (Alternative.me)
  - BTC Dominance % + delta 24h (CoinGecko public)
  - Funding BTC percentile storico (Binance Futures public)
  - News crypto (CryptoPanic se key in config, fallback RSS Messari)
  - Livelli narrativi per asset (Binance OHLCV + orderbook + CoinGecko ATH/ATL)
"""

import logging
import time
import threading
import json
import math
import urllib.request
import urllib.error
from datetime import datetime
from typing import Dict, List, Optional
from core.chimera_errors import ErrorTracker

_err = ErrorTracker("MarketContextEngine")

logger = logging.getLogger("MarketContextEngine")

# ── MAPPINGS ASSET ────────────────────────────────────────────────────────────

# Kraken ticker → Binance symbol (per OHLCV e orderbook)
BINANCE_SYMBOL_MAP: Dict[str, Optional[str]] = {
    "XXBTZUSD": "BTCUSDT",
    "XETHZUSD": "ETHUSDT",
    "SOLUSD":   "SOLUSDT",
    "XXRPZUSD": "XRPUSDT",
    "XDGUSD":   "DOGEUSDT",
    "XZECZUSD": "ZECUSDT",
    "BONKUSD":  "BONKUSDT",
    "HYPEUSD":  "HYPEUSDT",
    "RAVEUSD":  None,   # non presente su Binance
}

# Kraken ticker → CoinGecko ID (per ATH/ATL)
COINGECKO_ID_MAP: Dict[str, str] = {
    "XXBTZUSD": "bitcoin",
    "XETHZUSD": "ethereum",
    "SOLUSD":   "solana",
    "XXRPZUSD": "ripple",
    "XDGUSD":   "dogecoin",
    "XZECZUSD": "zcash",
    "BONKUSD":  "bonk",
    "HYPEUSD":  "hyperliquid",
    "RAVEUSD":  "rave-coin",
}


class MarketContextEngine:
    """
    Recupera e aggrega il contesto globale del mercato crypto.
    Thread-safe — aggiornato in background ogni UPDATE_INTERVAL secondi.
    """

    UPDATE_INTERVAL        = 300   # 5 minuti — contesto globale
    LEVELS_UPDATE_INTERVAL = 3600  # 1 ora — livelli narrativi (meno volatili)

    def __init__(self, cryptopanic_key: str = None):
        self._lock              = threading.Lock()
        self._cryptopanic_key   = cryptopanic_key
        self._context           = {}
        self._livelli_asset     = {}   # {ticker: {livelli...}}
        self._last_update       = 0.0
        self._last_levels_update = 0.0
        self._thread            = None
        self._running           = False
        logger.info("🌍 MarketContextEngine inizializzato")

    def start(self):
        """Avvia il thread di aggiornamento in background."""
        if self._running:
            return
        self._running = True
        try:
            self._aggiorna()
        except Exception as e:
            _err.capture(e, "start", {"module": "MarketContextEngine"})
            logger.warning(f"⚠️ Primo aggiornamento contesto fallito: {e}")
        # Livelli narrativi al primo avvio (in background per non bloccare)
        threading.Thread(
            target=self._aggiorna_livelli_tutti,
            daemon=True, name="MCE-Levels-Init"
        ).start()
        self._thread = threading.Thread(
            target=self._loop, daemon=True, name="MarketContext"
        )
        self._thread.start()

    def stop(self):
        self._running = False

    def _loop(self):
        while self._running:
            time.sleep(self.UPDATE_INTERVAL)
            try:
                self._aggiorna()
            except Exception as e:
                _err.capture(e, "_loop", {"module": "MarketContextEngine"})
                logger.debug(f"MarketContext update: {e}")
            # Aggiorna livelli ogni ora
            if time.time() - self._last_levels_update > self.LEVELS_UPDATE_INTERVAL:
                try:
                    self._aggiorna_livelli_tutti()
                except Exception as e:
                    _err.capture(e, "_loop", {"module": "MarketContextEngine"})
                    logger.debug(f"Levels update: {e}")

    # ── FETCH HELPERS ─────────────────────────────────────────────────────────

    def _fetch_json(self, url: str, timeout: int = 8) -> dict:
        req = urllib.request.Request(
            url,
            headers={"User-Agent": "CHIMERA/4.0 (trading bot)"}
        )
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return json.loads(resp.read().decode())

    # ── LIVELLI NARRATIVI ─────────────────────────────────────────────────────

    def _round_numbers(self, price: float) -> List[dict]:
        """
        Genera livelli psicologici round intorno al prezzo corrente.
        Scala adattiva: BTC usa step $5k, ETH $100, SOL $5, ecc.
        """
        if price <= 0:
            return []

        # Determina step in base alla magnitude del prezzo
        magnitude = 10 ** math.floor(math.log10(price))
        if price >= 1000:
            step = magnitude / 2      # BTC: step $5k, ETH: step $100
        elif price >= 10:
            step = magnitude / 2      # SOL $85 → step $5
        elif price >= 0.1:
            step = magnitude          # XRP $1.39 → step $0.1
        else:
            step = magnitude          # DOGE, BONK → step adattivo

        # 5 livelli sopra e 5 sotto
        base = round(price / step) * step
        livelli = []
        for i in range(-5, 6):
            lv = round(base + i * step, 8)
            if lv <= 0:
                continue
            dist_pct = (lv - price) / price * 100
            if abs(dist_pct) < 0.1:
                continue  # troppo vicino al prezzo attuale
            livelli.append({
                "prezzo":    lv,
                "tipo":      "PSY_ROUND",
                "dist_pct":  round(dist_pct, 2),
                "forza":     "media",
                "nota":      f"Livello psicologico ${lv:,.4g}"
            })
        return livelli

    def _fibonacci_levels(self, binance_symbol: str, current_price: float) -> List[dict]:
        """
        Calcola livelli Fibonacci da swing H/L degli ultimi 30 giorni (Binance OHLCV daily).
        Ritorna solo i livelli entro ±25% dal prezzo corrente.
        """
        if not binance_symbol:
            return []
        try:
            url = (f"https://api.binance.com/api/v3/klines"
                   f"?symbol={binance_symbol}&interval=1d&limit=30")
            data = self._fetch_json(url, timeout=10)
            if not data or len(data) < 10:
                return []

            highs  = [float(c[2]) for c in data]
            lows   = [float(c[3]) for c in data]
            swing_h = max(highs)
            swing_l = min(lows)
            rng     = swing_h - swing_l
            if rng <= 0:
                return []

            # Livelli Fibonacci standard
            ratios = [
                (0.0,   "0%",    "estremo"),
                (0.236, "23.6%", "debole"),
                (0.382, "38.2%", "forte"),
                (0.500, "50%",   "medio"),
                (0.618, "61.8%", "forte"),
                (0.786, "78.6%", "medio"),
                (1.0,   "100%",  "estremo"),
            ]

            livelli = []
            for ratio, label, forza in ratios:
                # Ritracciamento dal massimo
                lv_retr = round(swing_h - ratio * rng, 8)
                # Estensione dal minimo
                lv_ext  = round(swing_l + ratio * rng, 8)

                for lv in set([lv_retr, lv_ext]):
                    if lv <= 0:
                        continue
                    dist_pct = (lv - current_price) / current_price * 100
                    if abs(dist_pct) > 25:
                        continue  # fuori range utile
                    if abs(dist_pct) < 0.1:
                        continue
                    livelli.append({
                        "prezzo":   lv,
                        "tipo":     "FIB",
                        "dist_pct": round(dist_pct, 2),
                        "forza":    forza,
                        "nota":     f"Fibonacci {label} (H:{swing_h:.4g} L:{swing_l:.4g})"
                    })

            # Deduplicazione livelli molto vicini (entro 0.3%)
            livelli.sort(key=lambda x: x["prezzo"])
            dedup = []
            for lv in livelli:
                if not dedup or abs(lv["prezzo"] - dedup[-1]["prezzo"]) / dedup[-1]["prezzo"] > 0.003:
                    dedup.append(lv)
            return dedup[:8]

        except Exception as e:
            _err.capture(e, "_fibonacci_levels", {"module": "MarketContextEngine"})
            logger.warning(f"⚠️ Fibonacci {binance_symbol}: {e}")
            return []

    def _ob_walls(self, binance_symbol: str, current_price: float) -> List[dict]:
        """
        Identifica muri di liquidità (walls) dall'orderbook Binance.
        Un wall è un ordine il cui volume è >= 3x la media degli ordini visibili.
        """
        if not binance_symbol or current_price <= 0:
            return []
        try:
            url = (f"https://api.binance.com/api/v3/depth"
                   f"?symbol={binance_symbol}&limit=50")
            data = self._fetch_json(url, timeout=8)

            walls = []
            for side, tipo in [("bids", "SUPPORTO_OB"), ("asks", "RESISTENZA_OB")]:
                orders = [[float(p), float(q)] for p, q in data.get(side, [])]
                if len(orders) < 5:
                    continue
                # Filtra entro ±8% dal prezzo
                orders = [[p, q] for p, q in orders
                           if abs(p - current_price) / current_price <= 0.08]
                if not orders:
                    continue
                avg_q = sum(q for _, q in orders) / len(orders)
                threshold = avg_q * 3.0  # 3x la media = wall

                for price_lv, qty in orders:
                    if qty >= threshold:
                        dist_pct = (price_lv - current_price) / current_price * 100
                        walls.append({
                            "prezzo":   round(price_lv, 8),
                            "tipo":     tipo,
                            "dist_pct": round(dist_pct, 2),
                            "forza":    "forte" if qty >= avg_q * 5 else "media",
                            "nota":     f"Wall orderbook {tipo} (qty {qty:,.0f})"
                        })

            # Top 3 per lato
            sup_walls = sorted([w for w in walls if w["tipo"] == "SUPPORTO_OB"],
                                key=lambda x: x["dist_pct"], reverse=True)[:3]
            res_walls = sorted([w for w in walls if w["tipo"] == "RESISTENZA_OB"],
                                key=lambda x: x["dist_pct"])[:3]
            return sup_walls + res_walls

        except Exception as e:
            _err.capture(e, "_ob_walls", {"module": "MarketContextEngine"})
            logger.warning(f"⚠️ OB walls {binance_symbol}: {e}")
            return []

    def _ath_atl(self, coingecko_id: str, current_price: float) -> List[dict]:
        """
        ATH e ATL da CoinGecko. Solo se entro ±30% dal prezzo corrente (rilevanti).
        """
        if not coingecko_id or current_price <= 0:
            return []
        try:
            url = (f"https://api.coingecko.com/api/v3/coins/{coingecko_id}"
                   f"?localization=false&tickers=false&market_data=true"
                   f"&community_data=false&developer_data=false")
            data = self._fetch_json(url, timeout=10)
            mkt  = data.get("market_data", {})

            livelli = []
            for campo, tipo, nota in [
                ("ath",  "ATH", "All-Time High"),
                ("atl",  "ATL", "All-Time Low"),
            ]:
                val = mkt.get(campo, {})
                price_lv = float(val.get("usd", 0) if isinstance(val, dict) else val or 0)
                if price_lv <= 0:
                    continue
                dist_pct = (price_lv - current_price) / current_price * 100
                if abs(dist_pct) > 60:
                    continue  # troppo lontano per essere utile
                livelli.append({
                    "prezzo":   round(price_lv, 8),
                    "tipo":     tipo,
                    "dist_pct": round(dist_pct, 2),
                    "forza":    "estrema",
                    "nota":     f"{nota}: ${price_lv:,.4g}"
                })
            return livelli

        except Exception as e:
            _err.capture(e, "_ath_atl", {"module": "MarketContextEngine"})
            logger.debug(f"ATH/ATL {coingecko_id}: {e}")
            return []

    def _get_narrative_levels(self, ticker: str) -> dict:
        """
        Aggrega tutti i livelli narrativi per un asset.
        Ritorna dict con livelli separati per tipo e lista unificata ordinata.
        """
        binance_sym  = BINANCE_SYMBOL_MAP.get(ticker)
        coingecko_id = COINGECKO_ID_MAP.get(ticker)

        # Prezzo corrente da Binance (o fallback 0)
        current_price = 0.0
        if binance_sym:
            try:
                url  = f"https://api.binance.com/api/v3/ticker/price?symbol={binance_sym}"
                data = self._fetch_json(url, timeout=6)
                current_price = float(data.get("price", 0))
            except Exception:
                pass

        if current_price <= 0:
            return {}

        # Raccogli tutti i livelli
        round_lvs = self._round_numbers(current_price)
        fib_lvs   = self._fibonacci_levels(binance_sym, current_price)
        wall_lvs  = self._ob_walls(binance_sym, current_price)
        ath_lvs   = self._ath_atl(coingecko_id, current_price)

        # Lista unificata ordinata per distanza assoluta
        tutti = round_lvs + fib_lvs + wall_lvs + ath_lvs
        tutti.sort(key=lambda x: abs(x["dist_pct"]))

        # Separati per direzione
        supporti   = sorted([l for l in tutti if l["dist_pct"] < 0],
                             key=lambda x: x["dist_pct"], reverse=True)[:6]
        resistenze = sorted([l for l in tutti if l["dist_pct"] > 0],
                             key=lambda x: x["dist_pct"])[:6]

        result = {
            "ticker":        ticker,
            "prezzo":        current_price,
            "ts":            datetime.utcnow().isoformat(),
            "supporti":      supporti,
            "resistenze":    resistenze,
            "round_numbers": round_lvs,
            "fibonacci":     fib_lvs,
            "ob_walls":      wall_lvs,
            "ath_atl":       ath_lvs,
        }

        # Log sintetico
        res_str = " | ".join(
            f"${r['prezzo']:,.4g}({r['tipo']})" for r in resistenze[:3]
        )
        sup_str = " | ".join(
            f"${s['prezzo']:,.4g}({s['tipo']})" for s in supporti[:3]
        )
        logger.info(
            f"📍 [{ticker}] Livelli narrativi | "
            f"R: {res_str or 'nessuno'} | "
            f"S: {sup_str or 'nessuno'}"
        )
        return result

    def _aggiorna_livelli_tutti(self):
        """Aggiorna i livelli narrativi per tutti gli asset mappati."""
        aggiornati = 0
        for ticker in BINANCE_SYMBOL_MAP:
            try:
                lvl = self._get_narrative_levels(ticker)
                if lvl:
                    with self._lock:
                        self._livelli_asset[ticker] = lvl
                    aggiornati += 1
                else:
                    logger.warning(f"⚠️ Livelli narrativi vuoti per {ticker} (asset non su Binance?)")
                time.sleep(0.5)  # evita rate limit
            except Exception as e:
                _err.capture(e, "_aggiorna_livelli_tutti", {"module": "MarketContextEngine"})
                logger.warning(f"⚠️ Livelli narrativi {ticker}: {e}")

        self._last_levels_update = time.time()
        logger.info(f"📍 Livelli narrativi aggiornati per {aggiornati}/{len(BINANCE_SYMBOL_MAP)} asset")

    def aggiorna_livelli_asset(self, ticker: str):
        """Aggiorna i livelli di un singolo asset (chiamabile da bot_la)."""
        try:
            lvl = self._get_narrative_levels(ticker)
            if lvl:
                with self._lock:
                    self._livelli_asset[ticker] = lvl
        except Exception as e:
            _err.capture(e, "aggiorna_livelli_asset", {"module": "MarketContextEngine"})
            logger.debug(f"Livelli singolo {ticker}: {e}")

    def get_livelli_asset(self, ticker: str) -> dict:
        """Ritorna i livelli narrativi per un asset (thread-safe)."""
        with self._lock:
            return dict(self._livelli_asset.get(ticker, {}))

    @staticmethod
    def format_livelli_per_prompt(livelli: dict) -> str:
        """
        Formatta i livelli narrativi per il prompt Gemini.
        Conciso e leggibile — solo i livelli più vicini e significativi.
        """
        if not livelli:
            return ""

        ticker  = livelli.get("ticker", "?")
        prezzo  = livelli.get("prezzo", 0)
        res     = livelli.get("resistenze", [])[:4]
        sup     = livelli.get("supporti", [])[:4]

        lines = [f"LIVELLI CHIAVE {ticker} (prezzo corrente: ${prezzo:,.4g})"]

        if res:
            lines.append("  Resistenze:")
            for r in res:
                lines.append(
                    f"    ${r['prezzo']:,.4g} [{r['tipo']}] "
                    f"dist:{r['dist_pct']:+.1f}% — {r['nota']}"
                )
        if sup:
            lines.append("  Supporti:")
            for s in sup:
                lines.append(
                    f"    ${s['prezzo']:,.4g} [{s['tipo']}] "
                    f"dist:{s['dist_pct']:+.1f}% — {s['nota']}"
                )
        return "\n".join(lines)

    # ── SORGENTI CONTESTO GLOBALE ─────────────────────────────────────────────

    def _get_fear_greed(self) -> dict:
        try:
            data = self._fetch_json("https://api.alternative.me/fng/?limit=2")
            current = data["data"][0]
            prev    = data["data"][1] if len(data["data"]) > 1 else current
            val     = int(current["value"])
            prev_v  = int(prev["value"])
            label   = current["value_classification"]
            delta   = val - prev_v
            return {
                "value": val,
                "label": label,
                "delta_24h": delta,
                "extreme": val >= 80 or val <= 20,
            }
        except Exception as e:
            _err.capture(e, "_get_fear_greed", {"module": "MarketContextEngine"})
            logger.debug(f"Fear&Greed fetch: {e}")
            return {"value": 50, "label": "Neutral", "delta_24h": 0, "extreme": False}

    def _get_btc_dominance(self) -> dict:
        try:
            data = self._fetch_json(
                "https://api.coingecko.com/api/v3/global"
            )
            mkt = data.get("data", {})
            dom = float(mkt.get("market_cap_percentage", {}).get("btc", 50))
            return {
                "dominance": round(dom, 2),
                "altseason": dom < 42,
                "btc_heavy": dom > 58,
            }
        except Exception as e:
            _err.capture(e, "_get_btc_dominance", {"module": "MarketContextEngine"})
            logger.debug(f"BTC dominance fetch: {e}")
            return {"dominance": 50.0, "altseason": False, "btc_heavy": False}

    def _get_funding_percentile(self) -> dict:
        """Funding rate BTC da Binance Futures (endpoint pubblico)."""
        try:
            data = self._fetch_json(
                "https://fapi.binance.com/fapi/v1/fundingRate"
                "?symbol=BTCUSDT&limit=100"
            )
            rates = [float(r["fundingRate"]) for r in data]
            if not rates:
                return {"rate": 0.0, "percentile": 50, "extreme": False}
            current = rates[-1]
            sorted_r = sorted(rates)
            n = len(sorted_r)
            pct = sorted_r.index(
                min(sorted_r, key=lambda x: abs(x - current))
            ) / n * 100
            return {
                "rate": round(current * 100, 4),
                "percentile": round(pct, 0),
                "extreme_positive": pct >= 85,
                "extreme_negative": pct <= 15,
            }
        except Exception as e:
            _err.capture(e, "_get_funding_percentile", {"module": "MarketContextEngine"})
            logger.debug(f"Funding fetch: {e}")
            return {"rate": 0.0, "percentile": 50,
                    "extreme_positive": False, "extreme_negative": False}

    def _get_news(self) -> list:
        """Top 3 news crypto recenti."""
        headlines = []
        try:
            if self._cryptopanic_key:
                url = (
                    f"https://cryptopanic.com/api/v1/posts/"
                    f"?auth_token={self._cryptopanic_key}"
                    f"&kind=news&public=true&filter=hot&currencies=BTC"
                )
                data = self._fetch_json(url)
                for item in data.get("results", [])[:3]:
                    headlines.append(item.get("title", ""))
            else:
                import xml.etree.ElementTree as ET
                req = urllib.request.Request(
                    "https://messari.io/rss/all-news.xml",
                    headers={"User-Agent": "CHIMERA/4.0"}
                )
                with urllib.request.urlopen(req, timeout=8) as resp:
                    tree = ET.parse(resp)
                root = tree.getroot()
                items = root.findall(".//item")[:3]
                for item in items:
                    title = item.find("title")
                    if title is not None and title.text:
                        headlines.append(title.text[:100])
        except Exception as e:
            _err.capture(e, "_get_news", {"module": "MarketContextEngine"})
            logger.debug(f"News fetch: {e}")
        return headlines

    # ── AGGIORNAMENTO CONTESTO GLOBALE ────────────────────────────────────────

    def _aggiorna(self):
        fg   = self._get_fear_greed()
        dom  = self._get_btc_dominance()
        fund = self._get_funding_percentile()
        news = self._get_news()

        penalita = 0
        if fg.get("value", 50) >= 80:
            penalita -= 1
        if fg.get("value", 50) <= 20:
            penalita += 1
        if fund.get("extreme_positive"):
            penalita -= 1
        if fund.get("extreme_negative"):
            penalita += 1

        bias = "NEUTRO"
        if penalita >= 1:
            bias = "OPPORTUNITA"
        elif penalita <= -1:
            bias = "CAUTELA"

        ctx = {
            "ts":            datetime.utcnow().isoformat(),
            "fear_greed":    fg,
            "btc_dominance": dom,
            "funding":       fund,
            "news":          news,
            "bias":          bias,
            "penalita_voto": penalita,
        }

        with self._lock:
            self._context = ctx
            self._last_update = time.time()

        logger.info(
            f"🌍 Contesto aggiornato — "
            f"F&G: {fg['value']} ({fg['label']}) | "
            f"BTC Dom: {dom['dominance']}% | "
            f"Funding: {fund['rate']:.3f}% ({fund['percentile']:.0f}°pct) | "
            f"Bias: {bias}"
        )

    # ── API PUBBLICA ──────────────────────────────────────────────────────────

    def get_context(self) -> dict:
        """Restituisce il contesto corrente (thread-safe)."""
        with self._lock:
            return dict(self._context)

    def get_penalita_voto(self) -> int:
        with self._lock:
            return int(self._context.get("penalita_voto", 0))

    def build_prompt_block(self) -> str:
        ctx = self.get_context()
        return self.build_prompt_block_static(ctx)

    @staticmethod
    def build_prompt_block_static(ctx: dict) -> str:
        if not ctx:
            return ""

        fg   = ctx.get("fear_greed", {})
        dom  = ctx.get("btc_dominance", {})
        fund = ctx.get("funding", {})
        news = ctx.get("news", [])
        bias = ctx.get("bias", "NEUTRO")

        lines = [
            "=== CONTESTO GLOBALE MERCATO ===",
            f"Fear & Greed: {fg.get('value', '?')}/100 ({fg.get('label', '?')}) "
            f"[Δ24h: {fg.get('delta_24h', 0):+d}]",
            f"BTC Dominance: {dom.get('dominance', '?')}%"
            + (" [ALTSEASON]" if dom.get("altseason") else "")
            + (" [BTC HEAVY]" if dom.get("btc_heavy") else ""),
            f"Funding BTC: {fund.get('rate', 0):+.3f}% "
            f"({fund.get('percentile', 50):.0f}° percentile storico)"
            + (" ⚠️ LONGS SATURI" if fund.get("extreme_positive") else "")
            + (" ⚠️ SHORTS SATURI" if fund.get("extreme_negative") else ""),
            f"Bias globale: {bias}"
            + (f" (penalità voto: {ctx.get('penalita_voto', 0):+d})" if ctx.get("penalita_voto") else ""),
        ]

        if news:
            lines.append("News recenti:")
            for n in news[:3]:
                lines.append(f"  • {n[:80]}")

        lines.append("=================================")
        return "\n".join(lines)

    def get_status_telegram(self) -> str:
        ctx = self.get_context()
        if not ctx:
            return "🌍 Contesto non ancora disponibile"

        fg   = ctx.get("fear_greed", {})
        dom  = ctx.get("btc_dominance", {})
        fund = ctx.get("funding", {})
        news = ctx.get("news", [])
        bias = ctx.get("bias", "NEUTRO")
        age  = int((time.time() - self._last_update) / 60)

        emoji_bias = {"NEUTRO": "➡️", "OPPORTUNITA": "🟢", "CAUTELA": "🔴"}.get(bias, "➡️")

        lines = [
            "🌍 *CONTESTO GLOBALE MERCATO*",
            f"_(aggiornato {age} min fa)_\n",
            f"😨 *Fear & Greed:* {fg.get('value', '?')}/100 — {fg.get('label', '?')} "
            f"({fg.get('delta_24h', 0):+d} rispetto ieri)",
            f"₿ *BTC Dominance:* {dom.get('dominance', '?')}%"
            + (" 🌊 Altseason" if dom.get("altseason") else ""),
            f"💰 *Funding BTC:* {fund.get('rate', 0):+.3f}% "
            f"({fund.get('percentile', 50):.0f}° percentile)"
            + (" ⚠️ Longs saturi" if fund.get("extreme_positive") else "")
            + (" ⚠️ Shorts saturi" if fund.get("extreme_negative") else ""),
            f"\n{emoji_bias} *Bias:* {bias}",
        ]

        # Livelli BTC se disponibili
        with self._lock:
            lvl_btc = self._livelli_asset.get("XXBTZUSD", {})
        if lvl_btc:
            res3 = lvl_btc.get("resistenze", [])[:3]
            sup3 = lvl_btc.get("supporti", [])[:3]
            if res3 or sup3:
                lines.append("\n📍 *Livelli chiave BTC:*")
                if res3:
                    lines.append("  Resistenze: " + " | ".join(
                        f"${r['prezzo']:,.0f}({r['tipo']})" for r in res3))
                if sup3:
                    lines.append("  Supporti: " + " | ".join(
                        f"${s['prezzo']:,.0f}({s['tipo']})" for s in sup3))

        if news:
            lines.append("\n📰 *News recenti:*")
            for n in news[:3]:
                lines.append(f"  • {n[:80]}")

        return "\n".join(lines)
    """
    Recupera e aggrega il contesto globale del mercato crypto.
    Thread-safe — aggiornato in background ogni UPDATE_INTERVAL secondi.
    """

    UPDATE_INTERVAL = 300  # 5 minuti

    def __init__(self, cryptopanic_key: str = None):
        self._lock = threading.Lock()
        self._cryptopanic_key = cryptopanic_key
        self._context = {}
        self._last_update = 0.0
        self._thread = None
        self._running = False
        logger.info("🌍 MarketContextEngine inizializzato")

    def start(self):
        """Avvia il thread di aggiornamento in background."""
        if self._running:
            return
        self._running = True
        # Primo aggiornamento sincrono per avere dati subito
        try:
            self._aggiorna()
        except Exception as e:
            _err.capture(e, "start", {"module": "MarketContextEngine"})
            logger.warning(f"⚠️ Primo aggiornamento contesto fallito: {e}")
        # Thread background per aggiornamenti successivi
        self._thread = threading.Thread(
            target=self._loop, daemon=True, name="MarketContext"
        )
        self._thread.start()

    def stop(self):
        self._running = False

    def _loop(self):
        while self._running:
            time.sleep(self.UPDATE_INTERVAL)
            try:
                self._aggiorna()
            except Exception as e:
                _err.capture(e, "_loop", {"module": "MarketContextEngine"})
                logger.debug(f"MarketContext update: {e}")

    # ── FETCH HELPERS ─────────────────────────────────────────────────────────

    def _fetch_json(self, url: str, timeout: int = 8) -> dict:
        req = urllib.request.Request(
            url,
            headers={"User-Agent": "CHIMERA/4.0 (trading bot)"}
        )
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return json.loads(resp.read().decode())

    def _get_fear_greed(self) -> dict:
        try:
            data = self._fetch_json("https://api.alternative.me/fng/?limit=2")
            current = data["data"][0]
            prev    = data["data"][1] if len(data["data"]) > 1 else current
            val     = int(current["value"])
            prev_v  = int(prev["value"])
            label   = current["value_classification"]
            delta   = val - prev_v
            return {
                "value": val,
                "label": label,
                "delta_24h": delta,
                "extreme": val >= 80 or val <= 20,
            }
        except Exception as e:
            _err.capture(e, "_get_fear_greed", {"module": "MarketContextEngine"})
            logger.debug(f"Fear&Greed fetch: {e}")
            return {"value": 50, "label": "Neutral", "delta_24h": 0, "extreme": False}

    def _get_btc_dominance(self) -> dict:
        try:
            data = self._fetch_json(
                "https://api.coingecko.com/api/v3/global"
            )
            mkt = data.get("data", {})
            dom = float(mkt.get("market_cap_percentage", {}).get("btc", 50))
            # Delta rispetto alla sessione precedente non disponibile in questo endpoint
            # Usiamo confronto con soglie storiche
            return {
                "dominance": round(dom, 2),
                "altseason": dom < 42,   # dominance bassa = altseason
                "btc_heavy": dom > 58,   # dominance alta = risk-off su alt
            }
        except Exception as e:
            _err.capture(e, "_get_btc_dominance", {"module": "MarketContextEngine"})
            logger.debug(f"BTC dominance fetch: {e}")
            return {"dominance": 50.0, "altseason": False, "btc_heavy": False}

    def _get_funding_percentile(self) -> dict:
        """Funding rate BTC da Binance Futures (endpoint pubblico)."""
        try:
            data = self._fetch_json(
                "https://fapi.binance.com/fapi/v1/fundingRate"
                "?symbol=BTCUSDT&limit=100"
            )
            rates = [float(r["fundingRate"]) for r in data]
            if not rates:
                return {"rate": 0.0, "percentile": 50, "extreme": False}
            current = rates[-1]
            sorted_r = sorted(rates)
            n = len(sorted_r)
            pct = sorted_r.index(
                min(sorted_r, key=lambda x: abs(x - current))
            ) / n * 100
            return {
                "rate": round(current * 100, 4),  # in %
                "percentile": round(pct, 0),
                "extreme_positive": pct >= 85,   # longs pagano molto
                "extreme_negative": pct <= 15,   # shorts pagano molto
            }
        except Exception as e:
            _err.capture(e, "_get_funding_percentile", {"module": "MarketContextEngine"})
            logger.debug(f"Funding fetch: {e}")
            return {"rate": 0.0, "percentile": 50,
                    "extreme_positive": False, "extreme_negative": False}

    def _get_news(self) -> list:
        """Top 3 news crypto recenti."""
        headlines = []
        try:
            if self._cryptopanic_key:
                url = (
                    f"https://cryptopanic.com/api/v1/posts/"
                    f"?auth_token={self._cryptopanic_key}"
                    f"&kind=news&public=true&filter=hot&currencies=BTC"
                )
                data = self._fetch_json(url)
                for item in data.get("results", [])[:3]:
                    headlines.append(item.get("title", ""))
            else:
                # Fallback: Messari RSS (no key needed)
                import xml.etree.ElementTree as ET
                req = urllib.request.Request(
                    "https://messari.io/rss/all-news.xml",
                    headers={"User-Agent": "CHIMERA/4.0"}
                )
                with urllib.request.urlopen(req, timeout=8) as resp:
                    tree = ET.parse(resp)
                root = tree.getroot()
                items = root.findall(".//item")[:3]
                for item in items:
                    title = item.find("title")
                    if title is not None and title.text:
                        headlines.append(title.text[:100])
        except Exception as e:
            _err.capture(e, "_get_news", {"module": "MarketContextEngine"})
            logger.debug(f"News fetch: {e}")
        return headlines

    # ── AGGIORNAMENTO ─────────────────────────────────────────────────────────

    def _aggiorna(self):
        fg   = self._get_fear_greed()
        dom  = self._get_btc_dominance()
        fund = self._get_funding_percentile()
        news = self._get_news()

        # Calcola bias globale
        penalita = 0
        if fg.get("value", 50) >= 80:
            penalita -= 1   # greed estremo → cautela
        if fg.get("value", 50) <= 20:
            penalita += 1   # fear estremo → opportunità
        if fund.get("extreme_positive"):
            penalita -= 1   # longs saturi
        if fund.get("extreme_negative"):
            penalita += 1   # shorts saturi

        bias = "NEUTRO"
        if penalita >= 1:
            bias = "OPPORTUNITA"
        elif penalita <= -1:
            bias = "CAUTELA"

        ctx = {
            "ts":         datetime.utcnow().isoformat(),
            "fear_greed": fg,
            "btc_dominance": dom,
            "funding":    fund,
            "news":       news,
            "bias":       bias,
            "penalita_voto": penalita,  # applicabile al voto Gemini
        }

        with self._lock:
            self._context = ctx
            self._last_update = time.time()

        logger.info(
            f"🌍 Contesto aggiornato — "
            f"F&G: {fg['value']} ({fg['label']}) | "
            f"BTC Dom: {dom['dominance']}% | "
            f"Funding: {fund['rate']:.3f}% ({fund['percentile']:.0f}°pct) | "
            f"Bias: {bias}"
        )

    # ── API PUBBLICA ──────────────────────────────────────────────────────────

    def get_context(self) -> dict:
        """Restituisce il contesto corrente (thread-safe)."""
        with self._lock:
            return dict(self._context)

    def get_penalita_voto(self) -> int:
        """
        Penalità sul voto Gemini basata sul contesto globale.
        Range: -2 (mercato in euforia/surriscaldato) → +1 (fear estremo)
        """
        with self._lock:
            return int(self._context.get("penalita_voto", 0))

    def build_prompt_block(self) -> str:
        """Genera il blocco testo da iniettare nel prompt Gemini."""
        ctx = self.get_context()
        return self.build_prompt_block_static(ctx)

    @staticmethod
    def build_prompt_block_static(ctx: dict) -> str:
        """
        Versione statica — non richiede istanza attiva.
        Usata da Brain che riceve il contesto già pronto nel dati_engine.
        """
        if not ctx:
            return ""

        fg   = ctx.get("fear_greed", {})
        dom  = ctx.get("btc_dominance", {})
        fund = ctx.get("funding", {})
        news = ctx.get("news", [])
        bias = ctx.get("bias", "NEUTRO")

        lines = [
            "=== CONTESTO GLOBALE MERCATO ===",
            f"Fear & Greed: {fg.get('value', '?')}/100 ({fg.get('label', '?')}) "
            f"[Δ24h: {fg.get('delta_24h', 0):+d}]",
            f"BTC Dominance: {dom.get('dominance', '?')}%"
            + (" [ALTSEASON]" if dom.get("altseason") else "")
            + (" [BTC HEAVY]" if dom.get("btc_heavy") else ""),
            f"Funding BTC: {fund.get('rate', 0):+.3f}% "
            f"({fund.get('percentile', 50):.0f}° percentile storico)"
            + (" ⚠️ LONGS SATURI" if fund.get("extreme_positive") else "")
            + (" ⚠️ SHORTS SATURI" if fund.get("extreme_negative") else ""),
            f"Bias globale: {bias}"
            + (f" (penalità voto: {ctx.get('penalita_voto', 0):+d})" if ctx.get("penalita_voto") else ""),
        ]

        if news:
            lines.append("News recenti:")
            for n in news[:3]:
                lines.append(f"  • {n[:80]}")

        lines.append("=================================")
        return "\n".join(lines)

    def get_status_telegram(self) -> str:
        """Testo per il comando /context su Telegram."""
        ctx = self.get_context()
        if not ctx:
            return "🌍 Contesto non ancora disponibile"

        fg   = ctx.get("fear_greed", {})
        dom  = ctx.get("btc_dominance", {})
        fund = ctx.get("funding", {})
        news = ctx.get("news", [])
        bias = ctx.get("bias", "NEUTRO")
        age  = int((time.time() - self._last_update) / 60)

        emoji_bias = {"NEUTRO": "➡️", "OPPORTUNITA": "🟢", "CAUTELA": "🔴"}.get(bias, "➡️")

        lines = [
            "🌍 *CONTESTO GLOBALE MERCATO*",
            f"_(aggiornato {age} min fa)_\n",
            f"😨 *Fear & Greed:* {fg.get('value', '?')}/100 — {fg.get('label', '?')} "
            f"({fg.get('delta_24h', 0):+d} rispetto ieri)",
            f"₿ *BTC Dominance:* {dom.get('dominance', '?')}%"
            + (" 🌊 Altseason" if dom.get("altseason") else ""),
            f"💰 *Funding BTC:* {fund.get('rate', 0):+.3f}% "
            f"({fund.get('percentile', 50):.0f}° percentile)"
            + (" ⚠️ Longs saturi" if fund.get("extreme_positive") else "")
            + (" ⚠️ Shorts saturi" if fund.get("extreme_negative") else ""),
            f"\n{emoji_bias} *Bias:* {bias}",
        ]

        if news:
            lines.append("\n📰 *News recenti:*")
            for n in news[:3]:
                lines.append(f"  • {n[:80]}")

        return "\n".join(lines)
