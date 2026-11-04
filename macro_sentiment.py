# -*- coding: utf-8 -*-
"""
L&A Institutional Bot - MacroSentiment
CHIMERA v4 — DXY/NASDAQ + On-Chain Glassnode (free tier)

Segnali on-chain aggiunti:
  - NUPL (Net Unrealized Profit/Loss)
  - SOPR (Spent Output Profit Ratio)
  - Exchange Net Flow

Glassnode free tier: non richiede API key per endpoint pubblici.
Se hai una key, impostala in config_la.py come GLASSNODE_API_KEY.
"""

import yfinance as yf
import time
import logging
import requests
from core import asset_list
from core import config_la
from core.chimera_errors import ErrorTracker

_err = ErrorTracker("MacroSentiment")

GLASSNODE_BASE = "https://api.glassnode.com/v1/metrics"


class MacroSentiment:
    def __init__(self, cache_timeout=300):
        self.cache_macro    = {'timestamp': 0, 'data': {}, 'sentiment': 'NEUTRAL'}
        self.cache_onchain  = {'timestamp': 0, 'data': {}}
        self.cache_timeout  = cache_timeout
        self.onchain_timeout = 900
        self.logger         = logging.getLogger("MacroSentiment")
        self.glassnode_key  = getattr(config_la, 'GLASSNODE_API_KEY', None)

    def get_macro_data(self):
        try:
            if time.time() - self.cache_macro['timestamp'] < self.cache_timeout:
                return self.cache_macro['data'], self.cache_macro['sentiment']
            import concurrent.futures
            with concurrent.futures.ThreadPoolExecutor(max_workers=2) as ex:
                fut_dxy = ex.submit(lambda: yf.Ticker("DX-Y.NYB").history(period="2d", timeout=8))
                fut_ndx = ex.submit(lambda: yf.Ticker("^IXIC").history(period="2d", timeout=8))
                try:
                    dxy = fut_dxy.result(timeout=10)
                    ndx = fut_ndx.result(timeout=10)
                except concurrent.futures.TimeoutError:
                    self.logger.warning("⚠️ MacroSentiment: yfinance timeout — uso cache o NEUTRAL")
                    return self.cache_macro.get('data', {}), self.cache_macro.get('sentiment', 'NEUTRAL')
            # yfinance può restituire None invece di DataFrame vuoto
            if dxy is None or ndx is None:
                return self.cache_macro.get('data', {}), self.cache_macro.get('sentiment', 'NEUTRAL')
            if dxy.empty or ndx.empty or len(dxy) < 2 or len(ndx) < 2:
                return {}, "NEUTRAL"
            # yfinance nuove versioni restituisce colonne multi-livello — appiattisci
            if hasattr(dxy.columns, 'levels'):
                dxy.columns = [c[0] if isinstance(c, tuple) else c for c in dxy.columns]
            if hasattr(ndx.columns, 'levels'):
                ndx.columns = [c[0] if isinstance(c, tuple) else c for c in ndx.columns]
            if 'Close' not in dxy.columns or 'Close' not in ndx.columns:
                return {}, "NEUTRAL"
            dxy_c1 = dxy['Close'].iloc[-1]
            dxy_c2 = dxy['Close'].iloc[-2]
            ndx_c1 = ndx['Close'].iloc[-1]
            ndx_c2 = ndx['Close'].iloc[-2]
            # Valori None o NaN (yfinance restituisce None su mercati chiusi)
            if any(v is None for v in [dxy_c1, dxy_c2, ndx_c1, ndx_c2]):
                return {}, "NEUTRAL"
            import math
            if any(isinstance(v, float) and math.isnan(v) for v in [dxy_c1, dxy_c2, ndx_c1, ndx_c2]):
                return {}, "NEUTRAL"
            dxy_price, dxy_prev = float(dxy_c1), float(dxy_c2)
            ndx_price, ndx_prev = float(ndx_c1), float(ndx_c2)
            if dxy_prev == 0 or ndx_prev == 0:
                return {}, "NEUTRAL"
            res = {
                "DXY":    {"price": dxy_price, "change": (dxy_price / dxy_prev) - 1},
                "NASDAQ": {"price": ndx_price, "change": (ndx_price / ndx_prev) - 1}
            }
            if res["NASDAQ"]["change"] > 0 and res["DXY"]["change"] < 0:
                sentiment = "BULLISH"
            elif res["NASDAQ"]["change"] < 0 and res["DXY"]["change"] > 0:
                sentiment = "BEARISH"
            else:
                sentiment = "NEUTRAL"
            self.cache_macro = {'timestamp': time.time(), 'data': res, 'sentiment': sentiment}
            return res, sentiment
        except Exception as e:
            _err.capture(e, "get_macro_data", {"module": "MacroSentiment"})
            self.logger.error(f"Errore macro TradFi: {e}")
            return {}, "NEUTRAL"

    def _glassnode_get(self, endpoint, asset="BTC"):
        try:
            params = {"a": asset, "f": "JSON", "i": "24h"}
            if self.glassnode_key:
                params["api_key"] = self.glassnode_key
            r = requests.get(f"{GLASSNODE_BASE}/{endpoint}", params=params, timeout=10)
            if r.status_code == 200:
                data = r.json()
                if data and isinstance(data, list):
                    return data[-1].get("v")
            elif r.status_code != 403:
                self.logger.debug(f"Glassnode {endpoint}: status {r.status_code}")
        except Exception as e:
            _err.capture(e, "_glassnode_get", {"module": "MacroSentiment"})
            self.logger.debug(f"Glassnode {endpoint} error: {e}")
        return None

    def get_onchain_data(self):
        if time.time() - self.cache_onchain['timestamp'] < self.onchain_timeout:
            return self.cache_onchain['data']
        result = {}
        nupl = self._glassnode_get("indicators/nupl")
        if nupl is not None:
            result['nupl'] = round(float(nupl), 4)
        sopr = self._glassnode_get("sopr/sopr")
        if sopr is not None:
            result['sopr'] = round(float(sopr), 4)
        net_flow = self._glassnode_get("transactions/transfers_volume_exchanges_net")
        if net_flow is not None:
            result['exchange_net_flow'] = round(float(net_flow), 2)
        if result:
            self.logger.info(f"📡 On-chain: {result}")
        self.cache_onchain = {'timestamp': time.time(), 'data': result}
        return result

    def get_onchain_sentiment(self):
        data = self.get_onchain_data()
        if not data:
            return data, "NEUTRAL"
        score = 0
        nupl = data.get('nupl')
        if nupl is not None:
            if nupl > 0.6:   score -= 1   # euforia -> rischio inversione
            elif nupl < 0.1: score += 1   # paura -> opportunità
        sopr = data.get('sopr')
        if sopr is not None:
            if sopr < 1.0:    score += 1  # venditori in perdita -> fondo
            elif sopr > 1.05: score -= 1  # distribuzione -> pressione
        net_flow = data.get('exchange_net_flow')
        if net_flow is not None:
            if net_flow < -1000:  score += 1  # outflow -> accumulo
            elif net_flow > 1000: score -= 1  # inflow -> vendita
        if score >= 2:   bias = "BULLISH"
        elif score <= -2: bias = "BEARISH"
        else:            bias = "NEUTRAL"
        return data, bias

    def get_full_macro(self):
        tradfi_data, tradfi_sent   = self.get_macro_data()
        onchain_data, onchain_sent = self.get_onchain_sentiment()
        if tradfi_sent == onchain_sent and tradfi_sent != "NEUTRAL":
            sentiment_finale = tradfi_sent
        elif onchain_sent != "NEUTRAL" and tradfi_sent == "NEUTRAL":
            sentiment_finale = onchain_sent
        else:
            sentiment_finale = tradfi_sent
        return {
            'tradfi':              tradfi_data,
            'onchain':             onchain_data,
            'tradfi_sentiment':    tradfi_sent,
            'onchain_sentiment':   onchain_sent,
            'sentiment_finale':    sentiment_finale,
            'onchain_disponibile': bool(onchain_data),
        }

    def get_macro_prompt_block(self):
        full = self.get_full_macro()
        oc   = full.get('onchain', {})
        lines = [
            f"MACRO TradFi: {full['tradfi_sentiment']} "
            f"(DXY {full['tradfi'].get('DXY', {}).get('change', 0)*100:.2f}% | "
            f"NASDAQ {full['tradfi'].get('NASDAQ', {}).get('change', 0)*100:.2f}%)",
        ]
        if full['onchain_disponibile']:
            lines.append(
                f"ON-CHAIN BTC: {full['onchain_sentiment']} | "
                f"NUPL={oc.get('nupl','N/A')} | "
                f"SOPR={oc.get('sopr','N/A')} | "
                f"ExchFlow={oc.get('exchange_net_flow','N/A')} BTC"
            )
            lines.append(f"SENTIMENT COMPOSITO: {full['sentiment_finale']}")
        else:
            lines.append("ON-CHAIN: non disponibile (free tier)")
            lines.append(f"SENTIMENT: {full['sentiment_finale']} (solo TradFi)")
        return "\n".join(lines)