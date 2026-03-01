# -*- coding: utf-8 -*-
"""
L&A Institutional Bot - EngineLA
Versione 2.3: Fix "Truth value of a Series" e ottimizzazione confronti.
"""
import ccxt
import pandas as pd
import numpy as np
import logging
import time
import requests
from core import asset_list
from core import config_la

class EngineLA:
    def __init__(self, api_key=None, api_secret=None):
        self.api_key = api_key or config_la.KRAKEN_KEY
        self.api_secret = api_secret or config_la.KRAKEN_SECRET
        self.exchange = ccxt.kraken({
            'apiKey': self.api_key,
            'secret': self.api_secret,
            'enableRateLimit': True
        })
        self.logger = logging.getLogger("EngineLA")
        self._wall_history = {} # Memorizza {ticker: {prezzo: timestamp_inizio}}
        
    def check_sentinel(self, ticker):
        """
        SENTINELLA ISTITUZIONALE (Lightweight)
        Monitora anomalie di Prezzo, Volume e Open Interest ogni minuto.
        """
        try:
            asset_id = asset_list.get_ticker(ticker)
            # Chiamata rapida per ticker (Last Price e Volume 24h)
            ticker_data = self.exchange.fetch_ticker(asset_id)
            current_price = float(ticker_data['last'])
            current_vol = float(ticker_data['baseVolume'])
            
            # Recupero Open Interest (Dato fondamentale per Hedge Funds)
            current_oi = self._get_open_interest(ticker)
            
            # Inizializzazione memoria se non esiste
            if not hasattr(self, '_last_sentinel_data'):
                self._last_sentinel_data = {}

            if ticker not in self._last_sentinel_data:
                self._last_sentinel_data[ticker] = {'price': current_price, 'vol': current_vol, 'oi': current_oi}
                return False

            prev = self._last_sentinel_data[ticker]
            
            # CALCOLO VARIAZIONI
            price_change = abs((current_price - prev['price']) / prev['price']) * 100
            # Se il volume totale 24h cresce di colpo (>1%), c'è attività anomala nel minuto
            vol_increase = prev['vol'] > 0 and (current_vol / prev['vol']) > 1.01
            oi_change = abs((current_oi - prev['oi']) / prev['oi']) * 100 if prev['oi'] > 0 else 0

            # Aggiornamento memoria per il prossimo giro
            self._last_sentinel_data[ticker] = {'price': current_price, 'vol': current_vol, 'oi': current_oi}

            # TRIGGER DI SVEGLIA: 
            # 0.3% prezzo OR 1% Open Interest OR Volume Spike
            if price_change >= 0.3 or oi_change >= 1.0 or vol_increase:
                self.logger.info(f"🚀 SENTINELLA {ticker}: Movimento rilevato! P:{price_change:.2f}% | OI:{oi_change:.2f}%")
                return True
            
            return False
        except Exception as e:
            self.logger.warning(f"⚠️ Sentinella momentaneamente cieca per {ticker}: {e}")
            return False
    
    def get_market_data(self, ticker):
        """Analisi Quantitativa Istituzionale: Microstruttura + Volume Profile."""
        res = {}
        try:
            asset_id = asset_list.get_ticker(ticker)
            
            # 1. ANALISI TRADE & VELOCITY (Micro-flussi)
            try:
                trades = self.exchange.fetch_trades(asset_id, limit=200)
            except Exception as e:
                self.logger.warning(f"⚠️ Kraken trades timeout per {ticker}: {e}")
                trades = []

            delta_tot, whale_delta = 0, 0
            whale_threshold = 50000 
            if trades:
                res['trade_velocity'] = len(trades) / (time.time() - (trades[0]['timestamp']/1000))
                for t in trades:
                    val = float(t['amount']) * float(t['price'])
                    amount = float(t['amount'])
                    if t['side'] == 'buy':
                        delta_tot += amount
                        if val > whale_threshold: whale_delta += amount
                    else:
                        delta_tot -= amount
                        if val > whale_threshold: whale_delta -= amount
            else:
                res['trade_velocity'] = 0

            res['cvd_reale'] = delta_tot
            res['whale_delta'] = whale_delta

            # 2. OHLCV & VOLUME PROFILE (La "Ciccia" Istituzionale)
            ohlcv = self.exchange.fetch_ohlcv(asset_id, timeframe='15m', limit=100)
            df = pd.DataFrame(ohlcv, columns=['ts', 'open', 'high', 'low', 'close', 'volume'])
            
            # --- CALCOLO POC E VALUE AREA ---
            # Dividiamo il range di prezzo in 20 zone (bins)
            bins = 20
            df['price_bin'] = pd.cut(df['close'], bins=bins)
            volume_per_bin = df.groupby('price_bin', observed=True)['volume'].sum()
            poc_bin = volume_per_bin.idxmax()
            
            res['poc'] = float((poc_bin.left + poc_bin.right) / 2) # Point of Control
            res['high_24h'] = float(df['high'].max())
            res['low_24h'] = float(df['low'].min())
            res['close'] = float(df['close'].iloc[-1])
            
            # Identificazione Supporti/Resistenze volumetriche (Value Area High/Low)
            sorted_vols = volume_per_bin.sort_values(ascending=False)
            total_vol = df['volume'].sum()
            current_vol = 0
            va_bins = []
            for bin_idx, vol in sorted_vols.items():
                current_vol += vol
                va_bins.append(bin_idx)
                if current_vol >= total_vol * 0.70: break # Prendiamo il 70% del volume
            
            res['vah'] = float(max([b.right for b in va_bins])) # Value Area High
            res['val'] = float(min([b.left for b in va_bins]))  # Value Area Low

            # 3. INDICATORI TECNICI AVANZATI
            res['vwap'] = float((df['close'] * df['volume']).sum() / df['volume'].sum())
            res['atr'] = self._calcola_atr(df)
            res['squeeze'] = self._calcola_squeeze(df)
            res['fvg'] = self._check_fvg(df)

            # 4. ORDERBOOK IMBALANCE (OTTIMIZZATO: Unica chiamata per 50 e 100 livelli)
            ob = self.exchange.fetch_order_book(asset_id, limit=100)
            
            # Calcolo imbalance sui primi 50 livelli (per tua logica originale)
            bid_vol_50 = sum([b[1] for b in ob['bids'][:50]])
            ask_vol_50 = sum([a[1] for a in ob['asks'][:50]])
            imbalance = bid_vol_50 / ask_vol_50 if ask_vol_50 > 0 else 1
            
            # Assorbimento istituzionale
            res['absorption'] = "BULL_ABS" if (imbalance > 2.5 and delta_tot < 0) else ("BEAR_ABS" if (imbalance < 0.4 and delta_tot > 0) else "NORMAL")
            res['spread_perc'] = (ob['asks'][0][0] - ob['bids'][0][0]) / ob['bids'][0][0] * 100
            
            # 5. FUNDING RATE (Sentiment Future)
            res['funding_rate_ext'] = self._get_external_funding(ticker)
            
            # --- AGGIUNTE ISTITUZIONALI ANDREA ---

            # A1. Z-SCORE PREZZO
            mean_p = df['close'].mean()
            std_p = df['close'].std()
            res['z_score'] = (res['close'] - mean_p) / std_p if std_p > 0 else 0

            # A2. DEEP ORDERBOOK IMBALANCE (Usa l'orderbook già scaricato sopra, senza nuova chiamata)
            deep_bids = sum([b[1] for b in ob['bids']])
            deep_asks = sum([a[1] for a in ob['asks']])
            res['book_pressure'] = deep_bids / deep_asks if deep_asks > 0 else 1
            
            # A3. VOLATILITY SHOCK (IV vs RV)
            vol_storia = df['close'].pct_change().std()
            vol_recente = df['close'].pct_change().rolling(20).std().iloc[-1]
            res['vol_shock'] = vol_recente / vol_storia if vol_storia > 0 else 1

            # A4. LIQUIDAZIONI E OPEN INTEREST (Dati Futures)
            res['liquidazioni_24h'] = self._get_liquidations(ticker)
            res['open_interest'] = self._get_open_interest(ticker)

            # A5. Z-SCORE FUNDING (Affollamento Trade)
            # Usiamo 0.0001 (0.01%) come media base del mercato
            res['funding_z_score'] = (res['funding_rate_ext'] - 0.0001) / 0.00005 

            # A6. DELTA POC (Aggressività al Point of Control)
            # Vediamo se al prezzo più scambiato prevale l'acquisto o la vendita
            res['delta_poc'] = df[df['price_bin'] == poc_bin]['volume'].sum() * (1 if delta_tot > 0 else -1)
            
            # Identifichiamo il 'muro' più grande in acquisto e vendita nei 100 livelli
            # --- AGGIUNTA LIQUIDITY WALLS + PERSISTENZA (ANTI-SPOOFING) ---
            try:
                max_bid = max(ob['bids'], key=lambda x: x[1])
                max_ask = max(ob['asks'], key=lambda x: x[1])
                
                # Calcolo persistenza numerica
                p_bid = self._get_wall_persistence(ticker, float(max_bid[0]), "BID")
                p_ask = self._get_wall_persistence(ticker, float(max_ask[0]), "ASK")

                # --- TRADUZIONE PER L'IA (MURI VERI O FALSI) ---
                def traduci_muro(score):
                    if score >= 0.7: return "VERO_MURO_ISTITUZIONALE"
                    if score >= 0.3: return "IN_ACCUMULO_STABILE"
                    return "POSSIBILE_SPOOFING_FAKE"

                res['muro_supporto'] = {
                    "prezzo": float(max_bid[0]), 
                    "volume": float(max_bid[1]),
                    "stato": traduci_muro(p_bid),  # <--- NUOVO: Ora l'IA legge il testo
                    "affidabilita": f"{int(p_bid * 100)}%"
                }
                res['muro_resistenza'] = {
                    "prezzo": float(max_ask[0]), 
                    "volume": float(max_ask[1]),
                    "stato": traduci_muro(p_ask),  # <--- NUOVO: Ora l'IA legge il testo
                    "affidabilita": f"{int(p_ask * 100)}%"
                }
            except Exception as e:
                self.logger.warning(f"⚠️ Errore calcolo persistenza: {e}")
                res['muro_supporto'] = {"prezzo": 0, "volume": 0, "persistence": 0}
                res['muro_resistenza'] = {"prezzo": 0, "volume": 0, "persistence": 0}

            # 1. Calcolo Divergenza
            prezzi = df['close'].tail(20).values
            res['cvd_divergence'] = float(np.corrcoef(prezzi, np.linspace(0, res['cvd_reale'], 20))[0,1])

            # 2. Dati Macro e Sentiment
            res['macro_proxy'] = self._get_intermarket_data()
            res['put_call_ratio'] = self._get_put_call_ratio(ticker)
            
            # 3. LIQUIDITY WALLS (Magneti di Prezzo)
            # Usiamo il metodo dedicato che hai aggiunto sotto
            res['liquidity_walls'] = self.get_liquidity_walls(asset_id)
            
            # 4. Parametro finale di qualità del segnale
            z = res.get('z_score', 0)
            bp = res.get('book_pressure', 1.0)
            res['signal_quality'] = float(abs(z) * bp)
            
            return res
        except Exception as e:
            self.logger.error(f"⚠️ Errore critico Engine: {e}")
            return {"close": 0, "atr": 0, "poc": 0, "vah": 0, "val": 0}
    
    def get_full_market_data(self, ticker):
        """
        Recupera dati completi di mercato: prezzo e ATR (e alcuni campi ausiliari).
        - Prima prova a ottenere la full analysis tramite get_market_data (dict).
        - In fallback esegue una fetch_ticker per recuperare almeno il prezzo.
        Ritorna sempre un dict con almeno le chiavi: 'price' e 'atr'.
        """
        try:
            # 1) Proviamo a ottenere i dati completi dall'engine (get_market_data)
            data = {}
            try:
                data = self.get_market_data(ticker) or {}
            except Exception as e_gm:
                self.logger.debug(f"ℹ️ get_market_data fallito per {ticker}: {e_gm}")
                data = {}

            # Se data è valida e contiene i campi attesi, normalizziamo il return
            if isinstance(data, dict) and data.get('close') is not None:
                return {
                    'price': float(data.get('close', 0)),
                    'atr': float(data.get('atr', 0) or 0),
                    'volume': float(data.get('volume', 0) or data.get('baseVolume', 0) or 0),
                    'fng_proxy': data.get('z_score', data.get('fng_proxy', 0))
                }

            # 2) Fallback: fetch_ticker per ottenere almeno il prezzo corrente
            try:
                symbol_kraken = asset_list.get_ticker(ticker)
                ticker_info = self.exchange.fetch_ticker(symbol_kraken)
                return {
                    'price': float(ticker_info.get('last', ticker_info.get('close', 0) or 0)),
                    'atr': 0.0,
                    'volume': float(ticker_info.get('baseVolume', 0) or 0),
                    'fng_proxy': 0
                }
            except Exception as e_ft:
                self.logger.debug(f"⚠️ fetch_ticker fallback fallito per {ticker}: {e_ft}")

            # 3) Fallback finale: return valori neutri (coerenti)
            return {'price': 0.0, 'atr': 0.0, 'volume': 0.0, 'fng_proxy': 0.0}

        except Exception as e:
            self.logger.error(f"❌ Errore get_full_market_data per {ticker}: {e}")
            return {'price': 0.0, 'atr': 0.0, 'volume': 0.0, 'fng_proxy': 0.0}
    
    def get_liquidity_walls(self, asset_id):
        """
        Analizza l'Orderbook profondo per trovare muri di ordini (Liquidity Clusters).
        """
        try:
            # Scarichiamo l'orderbook profondo (100 livelli)
            ob = self.exchange.fetch_order_book(asset_id, limit=100)
            bids = ob['bids']  # Ordini in acquisto (Supporto)
            asks = ob['asks']  # Ordini in vendita (Resistenza)

            # Funzione interna per trovare il muro più grosso
            def find_wall(orders):
                if not orders: return 0, 0
                # Troviamo l'ordine con il volume massimo
                max_order = max(orders, key=lambda x: x[1])
                return float(max_order[0]), float(max_order[1])

            wall_bid_price, wall_bid_vol = find_wall(bids)
            wall_ask_price, wall_ask_vol = find_wall(asks)

            return {
                "muro_supporto": wall_bid_price,
                "vol_supporto": wall_bid_vol,
                "muro_resistenza": wall_ask_price,
                "vol_resistenza": wall_ask_vol
            }
        except Exception as e:
            self.logger.warning(f"⚠️ Impossibile recuperare Liquidity Walls: {e}")
            return {}
    
    # ... (resto del file invariato) ...
    
    def _calcola_squeeze(self, df):
        """Rileva se il mercato è in fase di compressione (Squeeze)."""
        std = df['close'].rolling(20).std()
        sma = df['close'].rolling(20).mean()
        
        # Bollinger
        upper_bb = sma + (2 * std)
        lower_bb = sma - (2 * std)
        
        # Keltner (ATR)
        tr = pd.concat([df['high']-df['low'], abs(df['high']-df['close'].shift()), abs(df['low']-df['close'].shift())], axis=1).max(axis=1)
        atr_20 = tr.rolling(20).mean()
        upper_kc = sma + (1.5 * atr_20)
        lower_kc = sma - (1.5 * atr_20)
        
        # FIX: Confronto solo dell'ultimo valore scalare
        try:
            is_squeeze = (lower_bb.iloc[-1] > lower_kc.iloc[-1]) and (upper_bb.iloc[-1] < upper_kc.iloc[-1])
            return "ON" if is_squeeze else "OFF"
        except:
            return "OFF"

    def _check_fvg(self, df):
        """Identifica Fair Value Gaps istituzionali senza errori di Series."""
        try:
            if len(df) < 3: return "NONE"
            
            c1_high = float(df['high'].iloc[-3])
            c1_low = float(df['low'].iloc[-3])
            c3_high = float(df['high'].iloc[-1])
            c3_low = float(df['low'].iloc[-1])

            if c1_high < c3_low: return "BULL_GAP"
            if c1_low > c3_high: return "BEAR_GAP"
        except:
            pass
        return "NONE"

    def _get_external_funding(self, ticker):
        try:
            target = asset_list.get_futures_ticker(ticker)
            if not target: return 0.0
            url = "https://futures.kraken.com/derivatives/api/v3/funding_rates"
            r = requests.get(url, timeout=5).json()
            for rate in r.get('rates', []):
                if rate['symbol'] == target: return float(rate['fundingRate'])
            return 0.0
        except: return 0.0
    
    def _get_liquidations(self, ticker):
        """Recupera le liquidazioni reali per identificare i bottom/top."""
        try:
            target = asset_list.get_futures_ticker(ticker)
            url = f"https://futures.kraken.com/derivatives/api/v3/recent_liquidations?symbol={target}"
            r = requests.get(url, timeout=5).json()
            if r.get('result') == 'success':
                return sum([float(l['amount']) for l in r.get('liquidations', [])])
            return 0
        except: return 0

    def _get_open_interest(self, ticker):
        """Monitora se sta entrando denaro fresco istituzionale."""
        try:
            target = asset_list.get_futures_ticker(ticker)
            url = f"https://futures.kraken.com/derivatives/api/v3/tickers?symbol={target}"
            r = requests.get(url, timeout=5).json()
            
            # PROTEZIONE: Verifichiamo che la struttura dati sia quella attesa
            tickers = r.get('tickers', [])
            if tickers and len(tickers) > 0:
                return float(tickers[0].get('openInterest', 0))
            return 0
        except Exception as e: 
            self.logger.debug(f"ℹ️ Open Interest non disponibile per {ticker}: {e}")
            return 0
    
    def _get_intermarket_data(self):
        """
        Versione 2.5: Analisi Forza Relativa e Liquidity Warning.
        Non limita il trading ma fornisce il contesto di 'salute' del mercato.
        """
        try:
            # Recuperiamo il cross ETH/BTC per capire chi guida il mercato
            cross_data = self.exchange.fetch_ticker('ETH/BTC')
            eth_btc_strength = float(cross_data['last'])
            
            # Analisi ATR vs Volume per il Warning di Liquidità (Slippage)
            # Recuperiamo ultimi dati per calcolare la media del volume
            ohlcv = self.exchange.fetch_ohlcv('BTC/USDT', timeframe='1h', limit=24)
            df = pd.DataFrame(ohlcv, columns=['t', 'o', 'h', 'l', 'c', 'v'])
            avg_vol = df['v'].mean()
            current_vol = df['v'].iloc[-1]
            
            # Se la volatilità (ATR) sale ma il volume è sotto la media del 30%
            atr = self._calcola_atr(df)
            low_liquidity = False
            if current_vol < (avg_vol * 0.7) and atr > df['c'].std():
                low_liquidity = True

            return {
                "eth_btc_ratio": eth_btc_strength,
                "market_liquidity_warning": low_liquidity,
                "relative_volume_status": round(current_vol / avg_vol, 2)
            }
        except:
            return {"eth_btc_ratio": 0.07, "market_liquidity_warning": False}

    def _get_put_call_ratio(self, ticker):
        """Analizza se le istituzioni si stanno assicurando contro un crollo."""
        try:
            target = asset_list.get_futures_ticker(ticker)
            url = f"https://futures.kraken.com/derivatives/api/v3/open_positions?symbol={target}"
            # Analisi semplificata del rapporto posizioni short/long aperte nei futures
            r = requests.get(url, timeout=5).json()
            # Rapporto tra posizioni aperte (finto skew)
            return 1.0 # Placeholder: Kraken non fornisce il ratio secco via API pubblica senza auth
        except: return 1.0

    def _calcola_atr(self, df):
        tr = pd.concat([df['high']-df['low'], abs(df['high']-df['close'].shift()), abs(df['low']-df['close'].shift())], axis=1).max(axis=1)
        return float(tr.rolling(14).mean().iloc[-1])
        
    def _get_wall_persistence(self, ticker, price, side):
        """
        Calcola la persistenza di un muro di liquidità (Anti-Spoofing).
        Ritorna un valore da 0.0 (finto/nuovo) a 1.0 (reale/storico).
        """
        now = time.time()
        key = f"{ticker}_{side}"
        
        if key not in self._wall_history:
            self._wall_history[key] = {"price": price, "start_time": now}
            return 0.1 # Muro appena apparso

        hist = self._wall_history[key]
        
        # Se il muro è allo stesso prezzo (tolleranza 0.05%)
        price_diff = abs(price - hist["price"]) / hist["price"]
        if price_diff < 0.0005:
            duration = now - hist["start_time"]
            # 1.0 (massima fiducia) se il muro resiste da più di 10 minuti (600s)
            score = min(duration / 600, 1.0)
            return round(score, 2)
        else:
            # Il muro si è spostato o è nuovo: reset timer
            self._wall_history[key] = {"price": price, "start_time": now}
            return 0.1
            
    def get_open_positions_real(self):
        """Recupera le posizioni a margine realmente aperte su Kraken."""
        try:
            # Recupera le posizioni aperte (Margin) tramite CCXT
            pos = self.exchange.private_post_openpositions()
            return pos.get('result', {})
        except Exception as e:
            self.logger.error(f"❌ Errore critico recupero posizioni Kraken: {e}")
            return {}