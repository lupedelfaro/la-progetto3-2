# -*- coding: utf-8 -*-
"""
L&A Institutional Bot - MacroSentiment
Modulo per la raccolta e analisi del sentiment macro (DXY, NASDAQ).
Versione razionalizzata e pronta all'uso.
"""

import yfinance as yf
import time
import logging
from core import asset_list
from core import config_la
class MacroSentiment:
    """
    Gestore del sentiment macro (BULLISH/BEARISH/NEUTRAL) basato su dati DXY/NASDAQ.
    """

    def __init__(self, cache_timeout=300):
        self.cache_macro = {'timestamp': 0, 'data': {}, 'sentiment': 'NEUTRAL'}
        self.cache_timeout = cache_timeout
        self.logger = logging.getLogger("MacroSentiment")

    def get_macro_data(self):
        """
        Recupera dati macro (DXY, NASDAQ) con cache.
        Returns: (dict, str): dati macro, sentiment
        """
        try:
            if time.time() - self.cache_macro['timestamp'] < self.cache_timeout:
                return self.cache_macro['data'], self.cache_macro['sentiment']
            dxy = yf.Ticker("DX-Y.NYB").history(period="2d", timeout=15)
            ndx = yf.Ticker("^IXIC").history(period="2d", timeout=15)
            if dxy.empty or ndx.empty or len(dxy) < 2 or len(ndx) < 2:
                return {}, "NEUTRAL"
            dxy_price, dxy_prev = float(dxy['Close'].iloc[-1]), float(dxy['Close'].iloc[-2])
            ndx_price, ndx_prev = float(ndx['Close'].iloc[-1]), float(ndx['Close'].iloc[-2])
            if dxy_prev == 0 or ndx_prev == 0:
                return {}, "NEUTRAL"
            res = {
                "DXY": {"price": dxy_price, "change": (dxy_price / dxy_prev) - 1},
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
            self.logger.error(f"Errore recupero macro data: {e}")
            return {}, "NEUTRAL"