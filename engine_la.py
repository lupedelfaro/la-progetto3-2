# -*- coding: utf-8 -*-
"""
L&A Institutional Bot - EngineLA
Versione 2.3: Fix "Truth value of a Series" e ottimizzazione confronti.
"""
import logging
import time
import requests
import pandas as pd
import ccxt
import numpy as np
# IMPORTAZIONE CORRETTA: Prendiamo la funzione specifica dal file asset_list
from core.asset_list import get_ticker, get_config 

class EngineLA:
    def __init__(self, api_key=None, api_secret=None):
        # ... (il tuo codice init rimane uguale)
        self.api_key = api_key or config_la.KRAKEN_KEY
        self.api_secret = api_secret or config_la.KRAKEN_SECRET
        self.exchange = ccxt.kraken({
            'apiKey': self.api_key,
            'secret': self.api_secret,
            'enableRateLimit': True
        })
        self.logger = logging.getLogger("EngineLA")
        self._wall_history = {} 
        self._last_hurst = 0.5
    
    
    def check_sentinel(self, ticker):
        """
        SENTINELLA ISTITUZIONALE (Lightweight)
        Monitora anomalie di Prezzo, Volume e Open Interest ogni minuto.
        """
        try:
            asset_id = get_ticker(ticker)
            # Chiamata rapida per ticker (Last Price e Volume 24h)
            ticker_data = self.exchange.fetch_ticker(asset_id)
            current_price = float(ticker_data['last'])
            current_vol = float(ticker_data['baseVolume'])
            
            # Recupero Open Interest (DISATTIVATO - Spot Mode Only)
            # current_oi = self._get_open_interest(ticker)
            current_oi = 0
            
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
            
            # Calcolo OI commentato per evitare divisioni per zero o variazioni fittizie
            # oi_change = abs((current_oi - prev['oi']) / prev['oi']) * 100 if prev['oi'] > 0 else 0
            oi_change = 0

            # Aggiornamento memoria per il prossimo giro
            self._last_sentinel_data[ticker] = {'price': current_price, 'vol': current_vol, 'oi': current_oi}

            # TRIGGER DI SVEGLIA: 
            # 0.3% prezzo OR Volume Spike (OI rimosso dai trigger attivi)
            if price_change >= 0.3 or vol_increase:
                self.logger.info(f"🚀 SENTINELLA {ticker}: Movimento rilevato! P:{price_change:.2f}% | Vol Spike: {vol_increase}")
                return True
            
            return False
        except Exception as e:
            self.logger.warning(f"⚠️ Sentinella momentaneamente cieca per {ticker}: {e}")
            return False
    
    def get_market_data(self, ticker):
        """
        Analisi Quantitativa Istituzionale - Versione Unificata Project Chimera.
        Sincronizzata per evitare azzeramento CVD/VPIN.
        """
        res = {}
        try:
            asset_id = get_ticker(ticker) 
            self.logger.info(f"🔍 [ENGINE] Analisi avviata per: {ticker} (Kraken ID: {asset_id})")
            
            # 1. ANALISI TRADE & VELOCITY (Project Chimera)
            # Qui otteniamo i dati REALI (es. -2.87)
            flow_data = self.get_detailed_order_flow(ticker)
            res['cvd'] = flow_data['cvd']
            res['cvd_reale'] = flow_data['cvd']
            res['price_velocity'] = flow_data['price_velocity']
            res['is_explosive'] = flow_data['is_explosive']
            res['aggressivita'] = flow_data['aggressività']
            # Recuperiamo il VPIN se presente in flow_data, altrimenti usiamo 0
            res['vpin'] = flow_data.get('vpin', 0.0)
            
            # 2. OHLCV & VOLUME PROFILE
            ohlcv = self.exchange.fetch_ohlcv(asset_id, timeframe='15m', limit=100)
            df = pd.DataFrame(ohlcv, columns=['ts', 'open', 'high', 'low', 'close', 'volume'])
            
            if df.empty: return {"close": 0, "atr": 0.5, "market_regime": "MEAN_REVERSION"}

            for col in ['open', 'high', 'low', 'close', 'volume']:
                df[col] = pd.to_numeric(df[col], errors='coerce')
            
            df = df.dropna()

            res['close'] = float(df['close'].iloc[-1])
            res['atr'] = self._calcola_atr(df)
            res['squeeze'] = self._calcola_squeeze(df)

            # 3. LIQUIDITY WALLS
            walls = self.get_liquidity_walls(ticker)
            res['muro_supporto'] = walls['muro_supporto']
            res['muro_resistenza'] = walls['muro_resistenza']
            res['dist_supporto'] = walls['dist_supporto']
            res['dist_resistenza'] = walls['dist_resistenza']
            res['liquidity_pools'] = walls 

            # 4. REGIME & HFT
            res['hurst_exponent'] = self._get_hurst_exponent(df['close'].values)
            res['market_regime'] = "TRENDING" if res['hurst_exponent'] > 0.52 else "MEAN_REVERSION"
            
            # Salviamo l'hurst per l'health score
            self._last_hurst = res['hurst_exponent']
            
            ob = self.exchange.fetch_order_book(asset_id, limit=50)
            iceberg, spoofing = self._detect_hft_anomalies(ticker, ob)
            res['indice_spoofing'] = float(spoofing)
            
            # --- FIX CRITICO: NON RICALCOLARE, USA FLOW_DATA ---
            # Passiamo l'intero dizionario flow_data alla salute del mercato
            res['health_data'] = self.get_market_health_score(ticker, flow_data)
            # Aggiorniamo il vpin finale con quello validato dalla salute
            res['vpin'] = res['health_data']['vpin_value']

            # --- LOG ISTITUZIONALE PER SBLOCCO GEMINI ---
            self.logger.info(
                f"📊 [DATA_DUMP] {ticker} | CVD: {res['cvd']} | Vel: {res['price_velocity']} | "
                f"Muro_S: {res['muro_supporto']} ({res['dist_supporto']:.2f}%) | "
                f"Muro_R: {res['muro_resistenza']} ({res['dist_resistenza']:.2f}%) | "
                f"VPIN: {res['vpin']:.4f} | Spoofing: {res['indice_spoofing']:.2f}"
            )
            
            self.logger.info(f"🔹 [ORDER FLOW] CVD: {res['cvd']:+.2f} | VPIN: {res['vpin']:.4f}")
            self.logger.info(f"🏥 MARKET HEALTH: {res['health_data']['market_health_index']} | REGIME: {res['market_regime']}")
            
            return res
        except Exception as e:
            self.logger.error(f"🔴 Errore fatale get_market_data: {e}")
            return {"close": 0, "atr": 0.5, "market_regime": "MEAN_REVERSION"}
    
    def get_price_velocity(self, ticker, trades_freschi=None):
        """Calcola la velocità vettoriale del prezzo (%/secondo)."""
        try:
            trades = trades_freschi if trades_freschi is not None else self.exchange.fetch_trades(get_ticker(ticker), limit=50)
            if not trades or len(trades) < 2: return 0.0
            
            p_start, p_end = float(trades[0]['price']), float(trades[-1]['price'])
            t_start, t_end = trades[0]['timestamp'] / 1000, trades[-1]['timestamp'] / 1000
            
            duration = max(t_end - t_start, 1)
            velocity = ((p_end - p_start) / p_start * 100) / duration
            return round(velocity, 6)
        except: return 0.0      
    
    def _calcola_divergenza_cvd_reale(self, prezzi, trades):
        """
        Analisi avanzata della divergenza: confronta l'andamento del prezzo
        con l'accumulo reale dei contratti (CVD) durante gli ultimi trade.
        """
        if not trades or len(prezzi) < 10:
            return 1.0 # Default: nessuna divergenza rilevata
            
        try:
            # Creiamo un array CVD dai trade scaricati
            deltas = []
            cumulative = 0
            # Dividiamo i trade in "fette" per allinearli ai 20 campioni di prezzo
            chunk_size = max(1, len(trades) // len(prezzi))
            
            for i in range(0, len(trades), chunk_size):
                chunk = trades[i : i + chunk_size]
                d = sum([float(t['amount']) if t['side'] == 'buy' else -float(t['amount']) for t in chunk])
                cumulative += d
                deltas.append(cumulative)
            
            # Allineamento lunghezze
            min_len = min(len(prezzi), len(deltas))
            if min_len < 5: return 1.0
            
            # Calcolo correlazione di Pearson
            # 1.0 = Prezzo e CVD vanno insieme (Sano)
            # < 0.3 = Debolezza / Divergenza
            # < 0 = Divergenza Forte (Inversione probabile)
            corr = np.corrcoef(prezzi[-min_len:], deltas[-min_len:])[0, 1]
            return float(corr) if not np.isnan(corr) else 1.0
        except Exception:
            return 1.0

    def _calcola_delta_footprint_veloce(self, trades):
        """Versione ottimizzata: calcola l'aggressività senza nuove chiamate API."""
        if not trades: return 0
        buys = sum([float(t['amount']) for t in trades if t['side'] == 'buy'])
        sells = sum([float(t['amount']) for t in trades if t['side'] == 'sell'])
        total = buys + sells
        return (buys - sells) / total if total > 0 else 0
    
    def _calcola_zscore(self, serie, window=20):
        """Normalizza i dati per rilevare anomalie istituzionali (>2.0 o <-2.0)."""
        try:
            if isinstance(serie, list): serie = pd.Series(serie)
            if len(serie) < window: return 0.0
            rolling_mean = serie.rolling(window=window).mean()
            rolling_std = serie.rolling(window=window).std()
            last_std = rolling_std.iloc[-1]
            if last_std == 0 or np.isnan(last_std): return 0.0
            z = (serie.iloc[-1] - rolling_mean.iloc[-1]) / last_std
            return round(float(z), 2)
        except: return 0.0
    
    def _get_vpin_toxicity_veloce(self, trades):
        """
        VPIN (Volume-synchronized Probability of Informed Trading).
        Rileva se il flusso è 'tossico' (troppi ordini aggressivi istituzionali).
        """
        if len(trades) < 50: return 0.5
        
        try:
            # Dividiamo i trade in 5 bucket di volume uguale per sincronizzare il tempo al volume
            total_vol = sum([float(t['amount']) for t in trades])
            if total_vol <= 0: return 0.5
            
            vol_per_bucket = total_vol / 5
            vpin_buckets = []
            current_buy, current_sell, current_vol = 0, 0, 0
            
            for t in trades:
                amt = float(t['amount'])
                if t['side'] == 'buy': 
                    current_buy += amt
                else: 
                    current_sell += amt
                current_vol += amt
                
                if current_vol >= vol_per_bucket:
                    vpin_buckets.append(abs(current_buy - current_sell))
                    current_buy, current_sell, current_vol = 0, 0, 0
            
            if not vpin_buckets: 
                return 0.5
                
            # Normalizzazione: uno sbilanciamento totale in tutti i bucket porterebbe a 1.0
            vpin_score = float(np.mean(vpin_buckets) / vol_per_bucket)
            return round(min(vpin_score, 1.0), 4)
            
        except Exception as e:
            self.logger.debug(f"⚠️ Errore calcolo VPIN: {e}")
            return 0.5

    def get_market_health_score(self, ticker, order_flow_data):
        """
        CHIMERA: Genera un punteggio di 'salute' (0.0 - 1.0).
        FIX: Riceve order_flow_data (già calcolato) invece di ricalcolare i trades.
        """
        try:
            # Recuperiamo l'Hurst appena calcolato
            if not hasattr(self, '_last_hurst'):
                self._last_hurst = 0.5 
            
            # --- FIX CHIMERA: Prendiamo il VPIN già calcolato nel DATA_DUMP ---
            # Se order_flow_data non ha il vpin, usiamo 0.0 come fallback sicuro
            vpin = float(order_flow_data.get('vpin', 0.0))
            hurst = self._last_hurst
        
            # Logica di confidenza: più ci allontaniamo dal rumore (0.5), più il mercato è sano
            regime_conf = 1.0 if (hurst > 0.55 or hurst < 0.45) else 0.5
        
            # Salute: (Struttura Mercato + Assenza Tossicità) / 2
            health_score = (regime_conf + (1.0 - vpin)) / 2
        
            return {
                "market_health_index": round(health_score, 4),
                "vpin_value": round(vpin, 4),
                "hurst_used": hurst,
                "status": "HEALTHY" if health_score > 0.6 else "UNSTABLE"
            }
        except Exception as e:
            self.logger.error(f"❌ Errore Health Score su {ticker}: {e}")
            return {"market_health_index": 0.5, "vpin_value": 0.0, "status": "UNKNOWN"}
    
    def get_full_market_data(self, ticker):
        """
        Recupera dati completi di mercato senza filtri distruttivi.
        Passa l'intero dizionario generato da get_market_data per alimentare il Brain.
        """
        try:
            # 1) Proviamo a ottenere i dati completi dall'engine
            data = {}
            try:
                data = self.get_market_data(ticker) or {}
            except Exception as e_gm:
                self.logger.debug(f"ℹ️ get_market_data fallito per {ticker}: {e_gm}")
                data = {}

            # Se data contiene il prezzo, passiamo TUTTO il dizionario
            if isinstance(data, dict) and data.get('close') is not None:
                # 1. Normalizziamo il prezzo
                data['price'] = float(data.get('close', 0))
                
                # 2. FIX ATR: Se è 0 o mancante, calcoliamo un valore di emergenza (0.5% del prezzo)
                raw_atr = data.get('atr', 0)
                if not raw_atr or float(raw_atr) == 0:
                    data['atr'] = data['price'] * 0.005  # Emergenza: 0.5% del prezzo attuale
                else:
                    data['atr'] = float(raw_atr)

                # 3. Pulizia Muri
                if 'liquidity_pools' not in data or not data['liquidity_pools']:
                    data['liquidity_pools'] = []
                
                return data

            # 2) Fallback: fetch_ticker se l'analisi tecnica fallisce
            try:
                from core import asset_list
                symbol_kraken = get_ticker(ticker)
                ticker_info = self.exchange.fetch_ticker(symbol_kraken)
                return {
                    'price': float(ticker_info.get('last', 0)),
                    'close': float(ticker_info.get('last', 0)),
                    'atr': float(ticker_info.get('last', 0)) * 0.01, # Default 1% invece di 0
                    'volume': float(ticker_info.get('baseVolume', 0)),
                    'market_regime': 'Noise',
                    'liquidity_pools': [],
                    'price_velocity': 0.0,
                    'cvd_reale': 0.0
                }
            except Exception as e_ft:
                self.logger.debug(f"⚠️ fetch_ticker fallback fallito per {ticker}: {e_ft}")

            return {'price': 0.0, 'close': 0.0, 'atr': 0.0, 'liquidity_pools': []}

        except Exception as e:
            self.logger.error(f"❌ Errore critico get_full_market_data per {ticker}: {e}")
            return {'price': 0.0, 'close': 0.0, 'atr': 0.0, 'liquidity_pools': []}
    
    def analizza_asset(self, ticker):
        """Metodo ponte per la compatibilità con il bot principale."""
        return self.get_full_market_data(ticker)
    
    def get_liquidity_walls(self, asset_id):
        """
        ANALISI CHIMERA: Trova muri aggregati e calcola la distanza reale.
        FIX: Conversione numerica esplicita per evitare muri a 0.
        """
        try:
            # FIX: Converte XXBTZUSD in BTC/USD per CCXT
            ticker_ccxt = get_ticker(asset_id)
            ob = self.exchange.fetch_order_book(ticker_ccxt, limit=500)
            
            if not ob.get('bids') or not ob.get('asks'):
                return {"muro_supporto": 0, "dist_supporto": 0.0, "muro_resistenza": 0, "dist_resistenza": 0.0}
            
            last_price = float(ob['bids'][0][0])
            
            def get_best_zone(orders):
                if not orders: return 0.0, 0.0, 0.0
                # Specifichiamo le colonne e forziamo il tipo float
                df_ob = pd.DataFrame(orders).iloc[:, :2]
                df_ob.columns = ['price', 'vol']
                df_ob['price'] = pd.to_numeric(df_ob['price'], errors='coerce')
                df_ob['vol'] = pd.to_numeric(df_ob['vol'], errors='coerce')
                df_ob = df_ob.dropna()

                if df_ob.empty: return 0.0, 0.0, 0.0

                # Clustering istituzionale 0.1%
                bin_size = last_price * 0.001 
                df_ob['zone'] = (df_ob['price'] / bin_size).round() * bin_size
                clusters = df_ob.groupby('zone')['vol'].sum().sort_values(ascending=False)
                
                if clusters.empty: return 0.0, 0.0, 0.0
                
                best_price = float(clusters.index[0])
                distanza = abs(best_price - last_price) / last_price * 100
                return best_price, float(clusters.iloc[0]), round(distanza, 2)

            w_bid_p, w_bid_v, d_bid = get_best_zone(ob['bids'])
            w_ask_p, w_ask_v, d_ask = get_best_zone(ob['asks'])

            return {
                "muro_supporto": w_bid_p, "vol_supporto": w_bid_v, "dist_supporto": d_bid,
                "muro_resistenza": w_ask_p, "vol_resistenza": w_ask_v, "dist_resistenza": d_ask
            }
        except Exception as e:
            self.logger.warning(f"⚠️ Errore Liquidity Walls: {e}")
            return {"muro_supporto": 0, "dist_supporto": 0.0, "muro_resistenza": 0, "dist_resistenza": 0.0}
    def get_liquidity_pools(self, ticker):
        """
        Identifica le aree di liquidità più dense (Calamite per il TP).
        FIX: Gestione robusta dei tipi per Kraken.
        """
        try:
            asset_id = get_ticker(ticker)
            ob = self.exchange.fetch_order_book(asset_id, limit=500)
            
            def find_top_zones(orders):
                if not orders: return []
                df = pd.DataFrame(orders).iloc[:, :2]
                df.columns = ['prezzo', 'volume']
                # Forza conversione per evitare errori di ordinamento stringa
                df['prezzo'] = pd.to_numeric(df['prezzo'], errors='coerce')
                df['volume'] = pd.to_numeric(df['volume'], errors='coerce')
                return df.dropna().sort_values('volume', ascending=False).head(3).to_dict('records')

            return {
                "pools_supporto": find_top_zones(ob.get('bids', [])),
                "pools_resistenza": find_top_zones(ob.get('asks', []))
            }
        except Exception as e:
            self.logger.error(f"🔴 Errore Liquidity Mapping {ticker}: {e}")
            return {"pools_supporto": [], "pools_resistenza": []}
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
            df = pd.DataFrame(ohlcv, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
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
        """Calcola l'ATR adattivo per evitare lo 0.00% iniziale."""
        if df is None or len(df) < 2:
            return 0.0
        
        # Calcolo True Range
        high_low = df['high'] - df['low']
        high_close = abs(df['high'] - df['close'].shift())
        low_close = abs(df['low'] - df['close'].shift())
        
        tr = pd.concat([high_low, high_close, low_close], axis=1).max(axis=1)
        
        # --- FIX ADATTIVO ---
        # Se abbiamo meno di 14 candele, usiamo quelle disponibili (min_periods=1)
        # Questo impedisce al bot di restituire 0 nei primi minuti di vita
        atr_series = tr.rolling(window=14, min_periods=1).mean()
        
        valore_atr = atr_series.iloc[-1]
        
        # Protezione finale contro i NaN
        import math
        if math.isnan(valore_atr):
            return 0.0
            
        return float(valore_atr)
    def _get_wall_persistence(self, ticker, price, side):
        """
        Calcola la persistenza di un muro di liquidità (Anti-Spoofing).
        Versione corretta: Gestisce più livelli e non resetta se il muro "shifta".
        """
        now = time.time()
        # Usiamo il prezzo nella chiave per non sovrascrivere altri muri
        key = f"{ticker}_{side}_{price:.8f}"
        
        # 1. Pulizia rapida per non ingolfare la RAM (ogni 100 muri registrati)
        if len(self._wall_history) > 100:
            self._wall_history = {k: v for k, v in self._wall_history.items() 
                                  if now - v['start_time'] < 3600} # Teniamo traccia per 1h

        # 2. Se non esiste esattamente questo prezzo, cerchiamo un muro "vicino" (0.05%)
        if key not in self._wall_history:
            for old_key, data in list(self._wall_history.items()):
                if old_key.startswith(f"{ticker}_{side}"):
                    diff = abs(price - data["price"]) / data["price"]
                    if diff < 0.0005: # Il muro si è solo spostato di un tick
                        # Aggiorniamo il prezzo ma manteniamo lo start_time originale
                        self._wall_history[key] = {"price": price, "start_time": data["start_time"]}
                        if old_key != key: del self._wall_history[old_key]
                        
                        duration = now - data["start_time"]
                        return round(min(duration / 600, 1.0), 2)

            # 3. Se è proprio un muro nuovo
            self._wall_history[key] = {"price": price, "start_time": now}
            return 0.1

        # 4. Muro esistente e stabile
        duration = now - self._wall_history[key]["start_time"]
        return round(min(duration / 600, 1.0), 2)
    
    def get_open_positions_real(self):
        """Recupera le posizioni a margine realmente aperte su Kraken."""
        try:
            # Recupera le posizioni aperte (Margin) tramite CCXT
            pos = self.exchange.private_post_openpositions()
            return pos.get('result', {})
        except Exception as e:
            self.logger.error(f"❌ Errore critico recupero posizioni Kraken: {e}")
            return {}
            
    def _calcola_delta_footprint(self, ticker):
        """Analisi Aggressività: chi sta colpendo il book ora?"""
        try:
            asset_id = get_ticker(ticker)
            trades = self.exchange.fetch_trades(asset_id, limit=300)
            if not trades: return 0
            buy_v = sum([float(t['amount']) for t in trades if t['side'] == 'buy'])
            sell_v = sum([float(t['amount']) for t in trades if t['side'] == 'sell'])
            return round((buy_v - sell_v) / (buy_v + sell_v), 3) if (buy_v + sell_v) > 0 else 0
        except: return 0

    def _get_macro_correlation(self):
        """Monitora se il contesto generale è favorevole (Risk-On/Off)."""
        try:
            cross = self.exchange.fetch_ticker('ETH/BTC')
            val = float(cross['last'])
            if val > 0.04: return "RISK_ON"   # Mercato sano, altcoin forti
            if val < 0.035: return "RISK_OFF" # Mercato timoroso, tutti su BTC o cash
            return "NEUTRAL"
        except: return "NEUTRAL"
        
    def _get_hurst_exponent(self, prices, lags=range(2, 20)):
        """
        CLASSIFICATORE DI REGIME (Hurst) - Versione Protetta Project Chimera.
        H < 0.5: Mean Reverting (Laterale/Box)
        H > 0.5: Trending (Trend Forte/Breakout)
        """
        try:
            # Protezione 1: Numero minimo di dati
            if len(prices) < 20: 
                return 0.5
            
            # Calcolo tau con protezione per prezzi piatti (std=0)
            tau = []
            for lag in lags:
                diffs = np.subtract(prices[lag:], prices[:-lag])
                std_val = np.std(diffs)
                # Se la std è 0 (prezzi identici), aggiungiamo un epsilon per evitare log(0)
                tau.append(np.sqrt(std_val) if std_val > 0 else 1e-10)
            
            # Protezione 2: Regressione lineare (np.polyfit)
            # np.log(tau) e np.log(lags) devono avere valori validi
            poly = np.polyfit(np.log(lags), np.log(tau), 1)
            
            # Scalatura Hurst: il coefficiente va raddoppiato per il range 0-1
            h = round(poly[0] * 2.0, 2)
            
            # Normalizzazione forzata nel range teorico [0, 1]
            h = max(0.0, min(1.0, h))
            
            # Salvataggio per uso interno (Market Health Score)
            self._last_hurst = h
            return h
            
        except Exception as e:
            # Se il mercato è troppo illiquido o i dati sono corrotti, default neutro
            if hasattr(self, 'logger'):
                self.logger.debug(f"ℹ️ Hurst calc info: mercato troppo statico per {len(prices)} sample.")
            return 0.5

    def _get_vpin_toxicity(self, ticker):
        """
        ORDER FLOW TOXICITY (VPIN)
        Rileva se gli scambi sono dominati da 'Informed Traders' (Alta frequenza/Istituzionali).
        """
        try:
            asset_id = get_ticker(ticker)
            trades = self.exchange.fetch_trades(asset_id, limit=100)
            if not trades: return 0
            v_buy = sum([float(t['amount']) for t in trades if t['side'] == 'buy'])
            v_sell = sum([float(t['amount']) for t in trades if t['side'] == 'sell'])
            v_total = v_buy + v_sell
            return round(abs(v_buy - v_sell) / v_total, 3) if v_total > 0 else 0
        except: return 0

    def _check_portfolio_correlation(self, ticker, posizioni_aperte):
        """
        BETA-NEUTRALITY
        Monitora se il nuovo asset è troppo correlato a quelli già aperti.
        """
        if not posizioni_aperte: return 0.0
        # Matrice di correlazione pre-impostata per velocità (può essere resa dinamica)
        correlazioni = {
            ('XXBTZUSD', 'XETHZUSD'): 0.92,
            ('XETHZUSD', 'XETHXXBT'): 0.75,
            ('XXBTZUSD', 'XETHXXBT'): -0.40 # Spesso inversamente proporzionali
        }
        max_corr = 0
        for aperta in posizioni_aperte:
            pair = tuple(sorted((ticker, aperta)))
            corr = correlazioni.get(pair, 0.5)
            if corr > max_corr: max_corr = corr
        return max_corr
        
    def get_market_driver_logic(self, delta_f, whale_d, threshold):
        """Identifica chi sta muovendo il prezzo senza escludere nessuno."""
        if abs(delta_f) > 0.4 and abs(whale_d) < threshold:
            return "RETAIL_MOMENTUM" # I piccoli spingono (non ignorare!)
        elif abs(whale_d) > threshold:
            return "INSTITUTIONAL_PUSH" # Ci sono i grossi
        return "ORGANIC_FLOW" # Movimento naturale
        
    def get_detailed_order_flow(self, ticker):
        """
        PROJECT CHIMERA - Step 1 & 2:
        Analisi avanzata Order Flow: calcola CVD, Momentum, PRICE VELOCITY e VPIN.
        """
        try:
            # 1. Recupero ticker corretto
            symbol = get_ticker(ticker)
            
            # 2. Fetch trade con protezione timeout - Alzato a 500 per avere dati reali
            trades = self.exchange.fetch_trades(symbol, limit=500)
            
            # --- PROTEZIONE CHIMERA ---
            if not trades or len(trades) < 2:
                return {
                    'cvd': 0.0, 'vpin': 0.0, 'momentum_perc': 0.0, 'price_velocity': 0.0, 
                    'is_explosive': False, 'aggressività': "NEUTRAL"
                }
            
            # 3. Creazione DataFrame
            df = pd.DataFrame(trades)
            
            if df.empty or 'side' not in df.columns:
                return {'cvd': 0.0, 'vpin': 0.0, 'momentum_perc': 0.0, 'price_velocity': 0.0, 'is_explosive': False, 'aggressività': "NEUTRAL"}

            # --- [LOGICA CHIMERA: DATA CLEANING] ---
            df['price'] = pd.to_numeric(df['price'], errors='coerce')
            df['amount'] = pd.to_numeric(df['amount'], errors='coerce')
            df['timestamp'] = pd.to_numeric(df['timestamp'], errors='coerce')
            df = df.dropna(subset=['price', 'amount', 'timestamp', 'side'])

            # --- [LOGICA CHIMERA: PRICE VELOCITY] ---
            start_price = float(df['price'].iloc[0])
            end_price = float(df['price'].iloc[-1])
            start_time = df['timestamp'].iloc[0] / 1000
            end_time = df['timestamp'].iloc[-1] / 1000
            duration = max(end_time - start_time, 1) 
            
            momentum = ((end_price - start_price) / start_price) * 100
            velocity = momentum / duration
            is_explosive = abs(velocity) > 0.0005

            # 5. Calcolo Delta (CVD) e VPIN
            df['delta'] = np.where(df['side'] == 'buy', df['amount'], -df['amount'])
            cvd = float(df['delta'].sum())
            
            # --- AGGIUNTA CHIMERA: Calcolo VPIN ---
            vol_totale = df['amount'].sum()
            vpin = abs(cvd) / vol_totale if vol_totale > 0 else 0.0
            
            self.logger.info(f"⚡ CHIMERA: Vel {velocity:.6f} %/s | CVD: {cvd:.4f} | VPIN: {vpin:.4f}")

            return {
                'cvd': round(cvd, 4),
                'vpin': round(vpin, 4), # <--- ORA È PRESENTE!
                'momentum_perc': round(momentum, 4),
                'price_velocity': round(velocity, 6),
                'is_explosive': is_explosive,
                'aggressività': "BUYERS" if cvd > 0 else "SELLERS"
            }
        except Exception as e:
            self.logger.error(f"🔴 Errore calcolo Order Flow su {ticker}: {e}")
            return {'cvd': 0.0, 'vpin': 0.0, 'momentum_perc': 0.0, 'price_velocity': 0.0, 'is_explosive': False, 'aggressività': "NEUTRAL"}
    
    def _detect_hft_anomalies(self, ticker, ob):
        """Rileva tracce di algoritmi HFT e manipolazioni (Spoofing/Iceberg)."""
        try:
            bids = ob['bids'][:20]
            asks = ob['asks'][:20]
            
            # Calcolo Spoofing
            bid_vol = sum([float(b[1]) for b in bids])
            ask_vol = sum([float(a[1]) for a in asks])
            spoofing_idx = round(abs(bid_vol - ask_vol) / (bid_vol + ask_vol), 2) if (bid_vol + ask_vol) > 0 else 0
            
            # Iceberg: Se lo spoofing è altissimo (>0.85), rileviamo assorbimento
            iceberg_detected = 1 if (spoofing_idx > 0.85) else 0
            
            return iceberg_detected, spoofing_idx
        except Exception:
            return 0, 0.0