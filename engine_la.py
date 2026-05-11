# -*- coding: utf-8 -*-
"""
L&A Institutional Bot - EngineLA
Versione 2.3: Fix "Truth value of a Series" e ottimizzazione confronti.
"""
import logging
import time
import os
import json
import requests
import pandas as pd
import ccxt
import numpy as np
from core import config_la
from core.asset_list import (
    get_ticker, get_config, get_cross_ticker, 
    CROSS_ETH_BTC, CROSS_BTC_USDT
)
from core.ws_manager import KrakenWSManager
from core.chimera_errors import ErrorTracker

_err = ErrorTracker("EngineLA")

class EngineLA:
    def __init__(self, api_key=None, api_secret=None):
        self.api_key = api_key or config_la.KRAKEN_KEY
        self.api_secret = api_secret or config_la.KRAKEN_SECRET
        self.exchange = ccxt.kraken({
            'apiKey': self.api_key,
            'secret': self.api_secret,
            'enableRateLimit': True,
            'timeout': 30000, # 30 secondi di timeout
        })
        # FIX 2026-05-02: ccxt blocca fetch_trades su Kraken per problema di
        # paginazione (issue #5698). Il dict 'options' nell'init non viene
        # mergiato — il pattern corretto è settare DOPO la creazione (come
        # documentato per adjustForTimeDifference). Setto entrambi i possibili
        # nomi del flag (con e senza camelCase) perché la versione esatta
        # cambia da release a release di ccxt.
        try:
            if not hasattr(self.exchange, 'options') or self.exchange.options is None:
                self.exchange.options = {}
            self.exchange.options['fetchTradesWarning'] = True
            self.exchange.options['fetch_trades_warning'] = True
        except Exception:
            pass
        self.logger = logging.getLogger("EngineLA")
        self._last_rate_limit_ts: float = 0.0
        self._rate_limit_cooldown: float = 20.0
        self._wall_history = {} 
        self._last_hurst = 0.5
        self.ws_manager = KrakenWSManager()
        self._futures_tickers_cache = {}
        self._last_futures_fetch = 0
        self._futures_cache_ttl = 10 # secondi
        self._funding_history = {} # Ticker -> list of funding rates
        self._funding_history_limit = 100
        self._funding_state_file = os.path.join(
            os.path.dirname(os.path.abspath(__file__)), '..', 'chimera_funding_state.json'
        )
        self._carica_funding_history()

    def _safe_fetch(self, method_name, *args, **kwargs):
        """
        CCXT con retry e backoff differenziato per rate limit.
        Rate limit: attese 15s/30s/45s + cooldown globale 20s.
        Errori generici: attese standard 2s/4s.
        """
        max_retries = 3
        _now = time.time()
        _last_rl = getattr(self, '_last_rate_limit_ts', 0)
        _rl_cd   = getattr(self, '_rate_limit_cooldown', 20)
        if _now - _last_rl < _rl_cd:
            _rimasto = _rl_cd - (_now - _last_rl)
            self.logger.debug(f"⏸️  Rate-limit cooldown: attendo {_rimasto:.0f}s prima di {method_name}")
            time.sleep(_rimasto)
        for attempt in range(max_retries):
            try:
                method = getattr(self.exchange, method_name)
                return method(*args, **kwargs)
            except ccxt.RateLimitExceeded as e:
                _err.capture(e, "_safe_fetch", {"module": "EngineLA"})
                self._last_rate_limit_ts = time.time()
                wait = 15 * (attempt + 1)
                if attempt == max_retries - 1:
                    self.logger.error(f"❌ Rate limit definitivo su {method_name} dopo {max_retries} tentativi.")
                    raise e
                self.logger.warning(f"⏱️  Rate limit Kraken su {method_name} (Tentativo {attempt+1}/{max_retries}). Attendo {wait}s...")
                time.sleep(wait)
            except (ccxt.NetworkError, ccxt.ExchangeError) as e:
                _err.capture(e, "_safe_fetch", {"module": "EngineLA"})
                err_str = str(e).lower()
                if 'too many' in err_str or 'rate' in err_str:
                    self._last_rate_limit_ts = time.time()
                    wait = 15 * (attempt + 1)
                    if attempt == max_retries - 1:
                        self.logger.error(f"❌ Errore definitivo API {method_name}: {e}")
                        raise e
                    self.logger.warning(f"⏱️  Rate limit (ExchangeError) su {method_name} (Tentativo {attempt+1}/{max_retries}). Attendo {wait}s...")
                    time.sleep(wait)
                else:
                    if attempt == max_retries - 1:
                        self.logger.error(f"❌ Errore definitivo API {method_name} dopo {max_retries} tentativi: {e}")
                        raise e
                    wait = (attempt + 1) * 2
                    self.logger.warning(f"⚠️ Errore API {method_name} (Tentativo {attempt+1}/{max_retries}): {e}. Attendo {wait}s...")
                    time.sleep(wait)
            except Exception as e:
                _err.capture(e, "_safe_fetch", {"module": "EngineLA"})
                self.logger.error(f"❌ Errore imprevisto API {method_name}: {e}")
                raise e
        return None

    def _carica_funding_history(self):
        """Ricarica la history del funding rate dal disco al riavvio."""
        try:
            if os.path.exists(self._funding_state_file):
                with open(self._funding_state_file, 'r') as f:
                    self._funding_history = json.load(f)
                total = sum(len(v) for v in self._funding_history.values())
                self.logger.info(
                    f"⚙️ Funding history ricaricata: {len(self._funding_history)} asset, "
                    f"{total} campioni totali."
                )
        except Exception as e:
            _err.capture(e, "_carica_funding_history", {"module": "EngineLA"})
            self.logger.debug(f"Impossibile caricare funding history: {e}")

    def _salva_funding_history(self):
        """Persiste la history del funding rate su disco."""
        try:
            tmp = self._funding_state_file + '.tmp'
            with open(tmp, 'w') as f:
                json.dump(self._funding_history, f)
            os.replace(tmp, self._funding_state_file)
        except Exception as e:
            _err.capture(e, "_salva_funding_history", {"module": "EngineLA"})
            self.logger.debug(f"Impossibile salvare funding history: {e}")

    def _safe_request(self, url, timeout=10):
        """
        Esegue una richiesta HTTP GET con retry automatici.

        FIX-B (2026-04-26): un 404 è una risposta deterministica dell'endpoint, NON
        un errore di rete o di codice — il retry non serve e il log non deve
        riempirsi di ERROR. I chiamanti di questa funzione (_get_liquidations,
        _get_open_interest) hanno già fallback in cascata progettati per gestire
        endpoint non esistenti su Kraken Futures.
        """
        max_retries = 3
        for attempt in range(max_retries):
            try:
                r = requests.get(url, timeout=timeout)
                # 404: endpoint non disponibile per quell'asset → fallback gestito a monte
                if r.status_code == 404:
                    self.logger.debug(f"ℹ️ Endpoint non disponibile (404): {url}")
                    return {}
                r.raise_for_status()
                return r.json()
            except Exception as e:
                _err.capture(e, "_safe_request", {"module": "EngineLA"})
                if attempt == max_retries - 1:
                    self.logger.debug(f"ℹ️ Request {url} fallita dopo {max_retries} tentativi: {e}")
                    return {}
                wait = (attempt + 1) * 2
                time.sleep(wait)
        return {}
    
    def check_sentinel(self, ticker):
        """
        Sentinella real-time. Rileva QUALSIASI movimento significativo
        e passa i dettagli a Brain per l'analisi immediata.

        LOGICA:
        - Campiona prezzo, CVD e volume dai trade WS ogni 2s (ciclo bot)
        - Calcola variazione su finestre: 10s, 30s, 60s
        - Triggera su UNA SOLA condizione forte — non richiede 2/3
        - Cooldown 30s (era 45s) per non perdere movimenti rapidi
        - Aggiunge 'sentinel_data' al buffer per passare contesto a Brain
        """
        try:
            now = time.time()

            if not hasattr(self, '_sentinel_buffer'):
                self._sentinel_buffer = {}

            if ticker not in self._sentinel_buffer:
                self._sentinel_buffer[ticker] = {
                    'samples':         [],
                    'last_trigger':    0,
                    'trigger_cooldown': 30,
                }

            buf = self._sentinel_buffer[ticker]

            # ── Prezzo dal WebSocket ───────────────────────────────────────────
            ws_ticker = self.ws_manager.get_ticker(ticker)
            ws_trades = self.ws_manager.get_trades(ticker)

            if ws_ticker and ws_ticker.get('last'):
                current_price = float(ws_ticker['last'])
            else:
                from core.asset_list import get_human_name
                raw = self._safe_fetch('fetch_ticker', get_human_name(ticker))
                current_price = float(raw['last'])

            if current_price <= 0:
                return False

            # ── CVD dai trade WS (ultimi 30s e 60s) ───────────────────────────
            cvd_30s = 0.0
            cvd_60s = 0.0
            n_trades_30s = 0
            vol_30s = 0.0
            if ws_trades:
                for t in ws_trades:
                    ts = (t.get('timestamp') or 0) / 1000
                    age = now - ts
                    amt_usd = float(t.get('amount', 0)) * float(t.get('price', current_price))
                    delta = amt_usd if t.get('side') == 'buy' else -amt_usd
                    if age <= 60:
                        cvd_60s += delta
                    if age <= 30:
                        cvd_30s += delta
                        n_trades_30s += 1
                        vol_30s += amt_usd

            # ── Aggiungi campione ─────────────────────────────────────────────
            buf['samples'].append({
                'ts':    now,
                'price': current_price,
                'cvd':   cvd_30s,
                'vol':   vol_30s,
                'n':     n_trades_30s,
            })
            # Mantieni 90s di storia
            buf['samples'] = [s for s in buf['samples'] if now - s['ts'] <= 90]

            if len(buf['samples']) < 3:
                return False

            # ── Cooldown ──────────────────────────────────────────────────────
            if now - buf['last_trigger'] < buf['trigger_cooldown']:
                return False

            # ── Calcola variazioni su finestre multiple ────────────────────────
            def ref_at(seconds):
                """Campione più vecchio entro N secondi."""
                cutoff = now - seconds
                cands = [s for s in buf['samples'] if s['ts'] <= cutoff]
                return cands[-1] if cands else buf['samples'][0]

            ref10 = ref_at(10)
            ref30 = ref_at(30)
            ref60 = ref_at(60)

            chg10 = abs(current_price - ref10['price']) / ref10['price'] * 100
            chg30 = abs(current_price - ref30['price']) / ref30['price'] * 100
            chg60 = abs(current_price - ref60['price']) / ref60['price'] * 100

            # Direzione del movimento
            direzione_mov = 'UP' if current_price > ref30['price'] else 'DOWN'

            # CVD acceleration — CVD degli ultimi 30s vs ultimi 60s
            cvd_accel = abs(cvd_30s) > abs(cvd_60s) * 0.6 and abs(cvd_30s) > 10_000

            # Volume spike — confronto con media campioni precedenti
            vol_media = sum(s['vol'] for s in buf['samples'][:-1]) / max(len(buf['samples']) - 1, 1)
            vol_spike = vol_30s > vol_media * 1.8 if vol_media > 0 else False

            # ── Condizioni di trigger (basta UNA) ─────────────────────────────
            trigger_motivo = None

            if chg10 >= 0.15:
                trigger_motivo = f"Δ10s={chg10:.3f}% ({direzione_mov})"
            elif chg30 >= 0.25:
                trigger_motivo = f"Δ30s={chg30:.3f}% ({direzione_mov})"
            elif chg60 >= 0.40:
                trigger_motivo = f"Δ60s={chg60:.3f}% ({direzione_mov})"
            elif cvd_accel and chg30 >= 0.10:
                trigger_motivo = f"CVD_accel={cvd_30s:+.0f} + Δ30s={chg30:.3f}% ({direzione_mov})"
            elif vol_spike and chg30 >= 0.10:
                trigger_motivo = f"VolSpike + Δ30s={chg30:.3f}% ({direzione_mov})"

            if trigger_motivo:
                buf['last_trigger'] = now

                # Salva contesto sentinella nel buffer per passarlo a Brain
                buf['last_sentinel_data'] = {
                    'trigger_motivo':  trigger_motivo,
                    'direzione_mov':   direzione_mov,
                    'chg10s':          round(chg10, 4),
                    'chg30s':          round(chg30, 4),
                    'chg60s':          round(chg60, 4),
                    'cvd_30s':         round(cvd_30s, 2),
                    'cvd_60s':         round(cvd_60s, 2),
                    'vol_spike':       vol_spike,
                    'n_trades_30s':    n_trades_30s,
                    'ts':              now,
                }

                self.logger.info(
                    f"⚡ SENTINELLA [{ticker}] {trigger_motivo} | "
                    f"CVD30s={cvd_30s:+.0f} | "
                    f"trades30s={n_trades_30s}"
                )
                return True

            return False

        except ccxt.RateLimitExceeded:
            self.logger.warning(f"⏳ Rate limit su Sentinella [{ticker}]. Pausa 2s.")
            time.sleep(2)
            return False
        except Exception as e:
            _err.capture(e, "check_sentinel", {"module": "EngineLA"})
            self.logger.debug(f"Sentinella [{ticker}] errore: {e}")
            return False

    def get_sentinel_data(self, ticker):
        """Restituisce i dati dell'ultimo trigger sentinella per un ticker."""
        try:
            buf = self._sentinel_buffer.get(ticker, {})
            return buf.get('last_sentinel_data', {})
        except Exception:
            return {}
    
    def get_current_price(self, ticker):
        """ Recupera l'ultimo prezzo di mercato per un asset. """
        try:
            from core.asset_list import get_human_name
            symbol = get_human_name(ticker)
            ticker_info = self._safe_fetch('fetch_ticker', symbol)
            return float(ticker_info['last'])
        except Exception as e:
            _err.capture(e, "get_current_price", {"module": "EngineLA"})
            self.logger.error(f"❌ Errore recupero prezzo {ticker} in Engine: {e}")
            return None

    def get_asset_leverage_info(self, ticker, side=None):
        """
        Recupera le informazioni sulla leva (allowed_leverages e max_leverage) 
        direttamente da Kraken per un dato ticker.
        Se side è specificato ('buy' o 'sell'), restituisce solo le leve per quel lato.
        """
        try:
            from core.asset_list import get_human_name
            symbol = get_human_name(ticker)
            
            # Carica i mercati se non sono già caricati
            if not self.exchange.markets or symbol not in self.exchange.markets:
                self.exchange.load_markets()
            
            if symbol not in self.exchange.markets:
                self.logger.debug(f"ℹ️ {symbol} non trovato nei mercati margin Kraken (asset spot-only).")
                return {"allowed_leverages": [1], "max_leverage": 1}
                
            market = self.exchange.market(symbol)
            
            # Kraken fornisce leverage_buy e leverage_sell nelle info del mercato
            info = market.get('info', {})
            leverage_buy = info.get('leverage_buy', [])
            leverage_sell = info.get('leverage_sell', [])
            
            # Assicuriamoci che siano liste di numeri
            def clean_lev(lev_list):
                if not lev_list: return []
                if isinstance(lev_list, (int, float)): return [int(lev_list)]
                if isinstance(lev_list, str): 
                    try: return [int(lev_list)]
                    except: return []
                if isinstance(lev_list, list):
                    return [int(l) for l in lev_list if str(l).replace('.','').isdigit()]
                return []

            l_buy = clean_lev(leverage_buy)
            l_sell = clean_lev(leverage_sell)
            
            # Filtriamo in base al lato se richiesto
            if side:
                if side.lower() == 'buy':
                    all_levs = sorted(list(set(l_buy)))
                elif side.lower() == 'sell':
                    all_levs = sorted(list(set(l_sell)))
                else:
                    all_levs = sorted(list(set(l_buy + l_sell)))
            else:
                all_levs = sorted(list(set(l_buy + l_sell)))

            if not all_levs:
                all_levs = [1]
                
            max_lev = max(all_levs)
            
            self.logger.debug(f"🔍 Leverage Info {ticker} ({side if side else 'both'}): {all_levs} (Max: {max_lev})")
            
            return {
                "allowed_leverages": all_levs,
                "max_leverage": max_lev
            }
        except Exception as e:
            _err.capture(e, "get_asset_leverage_info", {"module": "EngineLA"})
            self.logger.error(f"❌ Errore recupero leva Kraken per {ticker}: {e}")
            return {"allowed_leverages": [1], "max_leverage": 1}

    def get_total_balance(self, currency="USD"):
        """Recupera il bilancio totale (equity) dell'account."""
        try:
            balance = self.exchange.fetch_balance()
            # Kraken 'total' include margin equity se configurato correttamente in CCXT
            if 'info' in balance and 'eb' in balance['info']:
                return float(balance['info']['eb']) # Equity Balance
            return float(balance.get('total', {}).get(currency, 0))
        except Exception as e:
            _err.capture(e, "get_total_balance", {"module": "EngineLA"})
            self.logger.error(f"❌ Errore recupero bilancio totale: {e}")
            return 0.0

    def get_market_data(self, ticker):
        res = {}
        try:
            # Usiamo il mapping umano (es. SOL/USD) per le chiamate CCXT pubbliche
            # Questo evita errori "market symbol not found" su Kraken
            from core.asset_list import get_human_name
            asset_id = get_human_name(ticker) 
            self.logger.debug(f"🔍 [ENGINE] Analisi avviata per: {ticker} (CCXT Symbol: {asset_id})")
            
            flow_data = self.get_detailed_order_flow(ticker)
            res['cvd_istantaneo'] = float(flow_data.get('cvd_istantaneo', 0.0))
            res['cvd_reale'] = float(flow_data.get('cvd_istantaneo', 0.0))
            res['price_velocity'] = float(flow_data.get('price_velocity', 0.0))
            res['is_explosive'] = flow_data.get('is_explosive', False)
            res['is_toxic'] = flow_data.get('is_toxic', False)
            res['aggressivita_order_flow'] = flow_data.get('aggressivita_flow', 'NEUTRAL')
            res['vpin'] = flow_data.get('vpin', 0.0)
            
            ws_ohlcv = self.ws_manager.get_ohlcv(ticker)
            if ws_ohlcv and len(ws_ohlcv) >= 100:
                # WS buffer completo — usalo direttamente
                ohlcv = ws_ohlcv
            elif ws_ohlcv and len(ws_ohlcv) > 10:
                # WS buffer parziale — integra con REST per avere 100 candele
                # senza WS sarebbe un unico fetch, con WS usiamo il più recente
                ohlcv_rest = self._safe_fetch('fetch_ohlcv', asset_id, timeframe='15m', limit=100)
                if ohlcv_rest and len(ohlcv_rest) >= 100:
                    ohlcv = ohlcv_rest
                else:
                    ohlcv = ws_ohlcv
            else:
                ohlcv = self._safe_fetch('fetch_ohlcv', asset_id, timeframe='15m', limit=100)
                
            df = pd.DataFrame(ohlcv, columns=['ts', 'open', 'high', 'low', 'close', 'volume'])
            
            if df.empty: return {"close": 0, "atr": 0.5, "market_regime": "MEAN_REVERSION", "funding_rate": 0.0}

            for col in ['open', 'high', 'low', 'close', 'volume']:
                df[col] = pd.to_numeric(df[col], errors='coerce')
            
            df = df.dropna()

            res['close'] = float(df['close'].iloc[-1])
            res['atr'] = self._calcola_atr(df)
            res['squeeze'] = self._calcola_squeeze(df)

            # ── ROLLING VOLATILITY (deviazione standard rendimenti su 20 candele) ──
            # Misura la volatilità realizzata recente — diversa dall'ATR che usa
            # high/low. Utile per sizing adattivo e rilevamento regime.
            try:
                returns = df['close'].pct_change().dropna()
                if len(returns) >= 20:
                    res['rolling_volatility'] = float(round(
                        returns.rolling(20).std().iloc[-1] * 100, 6
                    ))
                else:
                    res['rolling_volatility'] = float(round(returns.std() * 100, 6))
            except Exception:
                res['rolling_volatility'] = 0.0

            # ── RSI (14 periodi) ──────────────────────────────────────────────
            try:
                delta_c = df['close'].diff()
                gain = delta_c.clip(lower=0).rolling(14).mean()
                loss = (-delta_c.clip(upper=0)).rolling(14).mean()
                rs   = gain / loss.replace(0, float('nan'))
                rsi_series = 100 - (100 / (1 + rs))
                res['rsi'] = float(round(rsi_series.iloc[-1], 2)) if not rsi_series.iloc[-1] != rsi_series.iloc[-1] else 50.0
            except Exception:
                res['rsi'] = 50.0

            # ── MACD (12/26/9) ────────────────────────────────────────────────
            try:
                ema12 = df['close'].ewm(span=12, adjust=False).mean()
                ema26 = df['close'].ewm(span=26, adjust=False).mean()
                macd_line   = ema12 - ema26
                signal_line = macd_line.ewm(span=9, adjust=False).mean()
                macd_hist   = macd_line - signal_line
                res['mac_d']      = float(round(macd_line.iloc[-1],   6))
                res['macd_signal']= float(round(signal_line.iloc[-1], 6))
                res['macd_hist']  = float(round(macd_hist.iloc[-1],   6))
            except Exception:
                res['mac_d'] = res['macd_signal'] = res['macd_hist'] = 0.0

            # ── Spread e volume 24h — WS con fallback REST ──────────────────
            try:
                ws_tick = self.ws_manager.get_ticker(ticker)
                bid = ask = 0.0
                if ws_tick:
                    ask = float(ws_tick.get('ask', 0) or 0)
                    bid = float(ws_tick.get('bid', 0) or 0)
                    vol_24h = float(ws_tick.get('baseVolume') or ws_tick.get('quoteVolume') or 0)
                    if vol_24h > 0:
                        res['volume_24h'] = vol_24h
                    else:
                        res['volume_24h'] = float(df['volume'].sum())
                else:
                    res['volume_24h'] = float(df['volume'].sum())

                # Se WS non ha bid/ask validi → fallback su orderbook REST
                if not (ask > 0 and bid > 0):
                    try:
                        from core.asset_list import get_human_name as _ghn
                        ob = self._safe_fetch('fetch_order_book', _ghn(ticker), limit=5)
                        if ob and ob.get('bids') and ob.get('asks'):
                            bid = float(ob['bids'][0][0])
                            ask = float(ob['asks'][0][0])
                    except Exception:
                        pass

                if ask > 0 and bid > 0:
                    res['spread']      = round(ask - bid, 8)
                    res['spread_perc'] = round((ask - bid) / bid * 100, 4)
                else:
                    # Fallback: ATR/1000 come proxy spread minimo
                    _atr = float(res.get('atr', 0) or 0)
                    res['spread']      = round(_atr * 0.1, 8) if _atr > 0 else 0.0
                    res['spread_perc'] = round(res['spread'] / res['close'] * 100, 4) if res.get('close', 0) > 0 else 0.0

            except Exception:
                res['spread'] = 0.0
                res['spread_perc'] = 0.0
                res['volume_24h'] = float(df['volume'].sum()) if not df.empty else 1.0
            
            # --- VWAP (Volume Weighted Average Price) ---
            try:
                typical_price = (df['high'] + df['low'] + df['close']) / 3
                res['vwap'] = float((typical_price * df['volume']).sum() / df['volume'].sum())
                
                # Calcolo Z-Score reale rispetto alla VWAP (distanza / deviazione standard)
                std_dev = df['close'].std()
                if std_dev > 0:
                    res['z_score_dist_vwap'] = round((res['close'] - res['vwap']) / std_dev, 3)
                else:
                    res['z_score_dist_vwap'] = 0.0
                
                # Probabilità di ritorno al VWAP (Mean Reversion)
                res['prob_ritorno_vwap'] = min(max(abs(res['z_score_dist_vwap']) * 25, 10), 90)
            except Exception:
                res['vwap'] = res['close']
                res['z_score_dist_vwap'] = 0.0
                res['prob_ritorno_vwap'] = 50.0

            walls = self.get_liquidity_walls(ticker)
            res['muro_supporto'] = walls['muro_supporto']
            res['muro_resistenza'] = walls['muro_resistenza']
            res['dist_supporto'] = walls['dist_supporto']
            res['dist_resistenza'] = walls['dist_resistenza']
            
            pools = self.get_liquidity_pools(ticker)
            res['liquidity_pools'] = pools 
            
            try:
                res['buy_imbalance_levels'] = [p['prezzo'] for p in pools.get('pools_supporto', [])]
                res['sell_imbalance_levels'] = [p['prezzo'] for p in pools.get('pools_resistenza', [])]
                res['liquidity_voids_supporto'] = pools.get('voids_supporto', [])
                res['liquidity_voids_resistenza'] = pools.get('voids_resistenza', [])
            except Exception:
                res['buy_imbalance_levels'] = []
                res['sell_imbalance_levels'] = []
                res['liquidity_voids_supporto'] = []
                res['liquidity_voids_resistenza'] = []

            res['hurst_exponent'] = self._get_hurst_exponent(df['close'].values)
            res['market_regime'] = "TRENDING" if res['hurst_exponent'] > 0.52 else "MEAN_REVERSION"
            
            # --- KAUFMAN EFFICIENCY RATIO (KER) ---
            try:
                period = min(10, len(df) - 1)
                if period > 0:
                    change = abs(df['close'].iloc[-1] - df['close'].iloc[-(period+1)])
                    volatility = df['close'].diff().abs().tail(period).sum()
                    res['kaufman_efficiency'] = round(change / volatility, 3) if volatility > 0 else 0.0
                else:
                    res['kaufman_efficiency'] = 0.0
            except:
                res['kaufman_efficiency'] = 0.0
            
            self._last_hurst = res['hurst_exponent']

            # Recupera order book — usato sia per analisi candlestick context che per spoofing/pressure
            ws_ob = self.ws_manager.get_order_book(ticker)

            # --- ANALISI PATTERN CANDLESTICK (ultime 3 candele) ────────────
            # Calcola i pattern più rilevanti per il trading istituzionale.
            # Risultati salvati come stringa leggibile + flag booleani.
            try:
                c  = df.iloc[-1]   # candela corrente
                c1 = df.iloc[-2]   # candela precedente
                c2 = df.iloc[-3]   # due candele fa

                body    = abs(c['close'] - c['open'])
                rng     = c['high'] - c['low']
                body1   = abs(c1['close'] - c1['open'])
                rng1    = c1['high'] - c1['low']

                # Evita divisioni per zero
                rng     = rng  if rng  > 0 else 0.0001
                rng1    = rng1 if rng1 > 0 else 0.0001

                is_bull = c['close'] > c['open']
                is_bull1= c1['close'] > c1['open']

                upper_wick = c['high'] - max(c['close'], c['open'])
                lower_wick = min(c['close'], c['open']) - c['low']
                upper_wick1= c1['high'] - max(c1['close'], c1['open'])
                lower_wick1= min(c1['close'], c1['open']) - c1['low']

                body_perc  = body  / rng
                body_perc1 = body1 / rng1

                patterns = []

                # --- DOJI: corpo < 10% del range, incertezza ---
                if body_perc < 0.10:
                    patterns.append("DOJI")

                # --- HAMMER / HANGING MAN: long lower wick, small body top ---
                if lower_wick > body * 2 and upper_wick < body * 0.5 and body_perc > 0.05:
                    patterns.append("HAMMER_BULL" if is_bull else "HANGING_MAN_BEAR")

                # --- SHOOTING STAR: long upper wick, small body bottom ---
                if upper_wick > body * 2 and lower_wick < body * 0.5 and body_perc > 0.05:
                    patterns.append("SHOOTING_STAR_BEAR" if not is_bull else "INVERTED_HAMMER")

                # --- PIN BAR: wick > 2/3 del range, corpo piccolo ---
                if lower_wick > rng * 0.60 and body_perc < 0.25:
                    patterns.append("PIN_BAR_BULL")
                if upper_wick > rng * 0.60 and body_perc < 0.25:
                    patterns.append("PIN_BAR_BEAR")

                # --- MARUBOZU: corpo > 85% del range, momentum forte ---
                if body_perc > 0.85:
                    patterns.append("MARUBOZU_BULL" if is_bull else "MARUBOZU_BEAR")

                # --- ENGULFING: candela attuale ingloba la precedente ---
                if (is_bull and not is_bull1 and
                        c['close'] > c1['open'] and c['open'] < c1['close']):
                    patterns.append("ENGULFING_BULL")
                if (not is_bull and is_bull1 and
                        c['close'] < c1['open'] and c['open'] > c1['close']):
                    patterns.append("ENGULFING_BEAR")

                # --- INSIDE BAR: corpo contenuto nella candela precedente ---
                if c['high'] < c1['high'] and c['low'] > c1['low']:
                    patterns.append("INSIDE_BAR_COMPRESSIONE")

                # --- OUTSIDE BAR (engulf completo di high+low) ---
                if c['high'] > c1['high'] and c['low'] < c1['low']:
                    patterns.append("OUTSIDE_BAR_ESPANSIONE")

                # --- MORNING/EVENING STAR (3 candele) ---
                body2 = abs(c2['close'] - c2['open'])
                if (not (c2['close'] > c2['open']) and  # c2 bearish
                        body1 < body2 * 0.3 and         # c1 piccola (stella)
                        is_bull and body > body2 * 0.5): # c corrente bullish forte
                    patterns.append("MORNING_STAR_BULL")
                if ((c2['close'] > c2['open']) and       # c2 bullish
                        body1 < body2 * 0.3 and          # c1 piccola (stella)
                        not is_bull and body > body2 * 0.5): # c corrente bearish forte
                    patterns.append("EVENING_STAR_BEAR")

                # Bias complessivo del pattern
                bull_patterns = [p for p in patterns if 'BULL' in p or 'HAMMER' in p or 'MORNING' in p]
                bear_patterns = [p for p in patterns if 'BEAR' in p or 'SHOOTING' in p or 'EVENING' in p or 'HANGING' in p]

                if len(bull_patterns) > len(bear_patterns):
                    bias = "BULLISH"
                elif len(bear_patterns) > len(bull_patterns):
                    bias = "BEARISH"
                elif "DOJI" in patterns or "INSIDE_BAR_COMPRESSIONE" in patterns:
                    bias = "INDECISO"
                else:
                    bias = "NEUTRO"

                res['candlestick_patterns']     = patterns if patterns else ["NESSUNO"]
                res['candlestick_bias']         = bias
                res['candlestick_ultima_bull']  = bool(is_bull)
                res['candlestick_body_perc']    = round(body_perc, 2)

            except Exception as e_candle:
                _err.capture(e_candle, "get_market_data", {"module": "EngineLA"})
                self.logger.debug(f"Errore analisi candele: {e_candle}")
                res['candlestick_patterns'] = ["NESSUNO"]
                res['candlestick_bias']     = "NEUTRO"
                res['candlestick_ultima_bull'] = False
                res['candlestick_body_perc'] = 0.5
            # ─────────────────────────────────────────────────────────────────
            if ws_ob and ws_ob.get('bids') and ws_ob.get('asks'):
                ob = ws_ob
            else:
                ob = self._safe_fetch('fetch_order_book', asset_id, limit=50)
                
            iceberg, spoofing = self._detect_hft_anomalies(ticker, ob)
            res['indice_spoofing'] = float(spoofing)
            res['iceberg_presenti'] = bool(iceberg)
            
            # --- BOOK PRESSURE E OFI PROFONDO (Weighted) ---
            try:
                bids = ob.get('bids', [])[:20]
                asks = ob.get('asks', [])[:20]
                
                # Calcolo pesato: i livelli più vicini al prezzo valgono di più
                bids_weighted = sum([float(b[1]) * (1 / (i + 1)) for i, b in enumerate(bids)])
                asks_weighted = sum([float(a[1]) * (1 / (i + 1)) for i, a in enumerate(asks)])
                
                res['book_pressure'] = round(bids_weighted / (bids_weighted + asks_weighted), 3) if (bids_weighted + asks_weighted) > 0 else 0.5
                res['order_flow_imbalance'] = round((bids_weighted - asks_weighted) / (bids_weighted + asks_weighted), 3) if (bids_weighted + asks_weighted) > 0 else 0.0
            except:
                res['book_pressure'] = 0.5
                res['order_flow_imbalance'] = 0.0
            
            res['health_data'] = self.get_market_health_score(ticker, flow_data)
            res['vpin'] = res['health_data']['vpin_value']

            # --- CORRELAZIONE REAL-TIME COL DRIVER ---
            res['correlazione_driver'] = self._calcola_correlazione_driver(ticker, df)

            # --- PRESSIONE SUI MURI (Wall Pressure) ---
            try:
                ws_trades = self.ws_manager.get_trades(ticker)
                # WS-first: usa WS anche con pochi trades (>=2). fetch_trades è
                # fallback rischioso (blocco paginazione ccxt issue #5698).
                if ws_trades and len(ws_trades) >= 2:
                    trades_recenti = ws_trades
                else:
                    trades_recenti = self._safe_fetch('fetch_trades', asset_id, limit=50) or ws_trades or []
                res['pressione_muro_supporto'] = self._calcola_wall_pressure(ticker, res['muro_supporto'], trades_recenti)
                res['pressione_muro_resistenza'] = self._calcola_wall_pressure(ticker, res['muro_resistenza'], trades_recenti)
            except:
                res['pressione_muro_supporto'] = 0.0
                res['pressione_muro_resistenza'] = 0.0

            # --- INTEGRAZIONE STRUMENTI AVANZATI ISTITUZIONALI ---
            res['z_score'] = self._calcola_zscore(df['close'], window=100) # Allineato a VWAP
            
            # --- VOLUME PROFILE (POC, VAH, VAL, HVN, LVN) ---
            try:
                vp = self._calcola_volume_profile(df, ticker=ticker)
                res['poc']               = vp['poc']
                res['vah']               = vp['vah']
                res['val']               = vp['val']
                res['vwap_anchored']     = vp.get('vwap_anchored', res.get('vwap', res['close']))
                res['high_volume_nodes'] = vp.get('high_volume_nodes', [])
                res['low_volume_nodes']  = vp.get('low_volume_nodes',  [])
                res['delta_poc']    = round(
                    (res['close'] - vp['poc']) / vp['poc'] * 100, 3
                ) if vp['poc'] > 0 else 0.0
            except Exception as e_vp:
                _err.capture(e_vp, "get_market_data", {"module": "EngineLA"})
                self.logger.debug(f"Errore calcolo Volume Profile: {e_vp}")
                res['poc'] = res['vah'] = res['val'] = res['delta_poc'] = 0.0
                res['high_volume_nodes'] = []
                res['low_volume_nodes']  = []
                res['vwap_anchored'] = res.get('vwap', res.get('close', 0.0))
            
            try:
                ws_trades = self.ws_manager.get_trades(ticker)
                # WS-first: usa WS anche con pochi trades (>=2)
                if ws_trades and len(ws_trades) >= 2:
                    trades_per_cvd = ws_trades
                else:
                    trades_per_cvd = self._safe_fetch('fetch_trades', asset_id, limit=50) or ws_trades or []
                res['cvd_divergence'] = self._calcola_divergenza_cvd_reale(trades_per_cvd)
            except Exception as e_cvd:
                _err.capture(e_cvd, "get_market_data", {"module": "EngineLA"})
                self.logger.debug(f"Errore calcolo CVD divergence: {e_cvd}")
                res['cvd_divergence'] = 1.0

            res['fvg'] = self._check_fvg(df)

            # --- STEP 2: LIVELLI STRUTTURALI (Swing, Pivot, EMA) ---
            # Questi vengono calcolati in background con gestione errori
            # indipendente — un fallimento non blocca il resto dell'analisi.

            # Swing High / Low strutturali su H1 e H4
            try:
                swing = self._calcola_swing_levels(ticker)
                res['swing_high_h1']   = swing.get('swing_high_h1',   [])
                res['swing_low_h1']    = swing.get('swing_low_h1',    [])
                res['swing_high_h4']   = swing.get('swing_high_h4',   [])
                res['swing_low_h4']    = swing.get('swing_low_h4',    [])
                res['res_strutturale'] = swing.get('res_strutturale', 0.0)
                res['sup_strutturale'] = swing.get('sup_strutturale', 0.0)
                # Distanza % dalla resistenza/supporto strutturale
                if res['res_strutturale'] > 0 and res['close'] > 0:
                    res['dist_res_strutturale'] = round(
                        (res['res_strutturale'] - res['close']) / res['close'] * 100, 2
                    )
                else:
                    res['dist_res_strutturale'] = 0.0
                if res['sup_strutturale'] > 0 and res['close'] > 0:
                    res['dist_sup_strutturale'] = round(
                        (res['close'] - res['sup_strutturale']) / res['close'] * 100, 2
                    )
                else:
                    res['dist_sup_strutturale'] = 0.0
            except Exception as e_sw:
                _err.capture(e_sw, "get_market_data", {"module": "EngineLA"})
                self.logger.debug(f"Swing levels skip {ticker}: {e_sw}")
                res['swing_high_h1'] = res['swing_low_h1'] = []
                res['swing_high_h4'] = res['swing_low_h4'] = []
                res['res_strutturale'] = res['sup_strutturale'] = 0.0
                res['dist_res_strutturale'] = res['dist_sup_strutturale'] = 0.0

            # Pivot Points daily e weekly
            try:
                pivot = self._calcola_pivot_points(ticker)
                res['pivot_daily']  = pivot.get('daily_pivot', 0.0)
                res['pivot_r1']     = pivot.get('daily_r1',    0.0)
                res['pivot_r2']     = pivot.get('daily_r2',    0.0)
                res['pivot_r3']     = pivot.get('daily_r3',    0.0)
                res['pivot_s1']     = pivot.get('daily_s1',    0.0)
                res['pivot_s2']     = pivot.get('daily_s2',    0.0)
                res['pivot_s3']     = pivot.get('daily_s3',    0.0)
                res['pivot_weekly'] = pivot.get('weekly_pivot',0.0)
                res['pivot_trend']  = pivot.get('pivot_trend', 'NEUTRO')

                # S/R storici con cache 1h
                _cache_key = f'sr_storici_{ticker}'
                _cache_val = getattr(self, '_sr_cache', {}).get(_cache_key)
                _cache_ts  = getattr(self, '_sr_cache_ts', {}).get(_cache_key, 0)
                if _cache_val and (time.time() - _cache_ts) < 3600:
                    sr = _cache_val
                else:
                    sr = self._calcola_livelli_storici(ticker, float(res.get('close', 0)))
                # ── Analisi strutturale del ciclo ────────────────────────────────
                try:
                    struttura = self._analisi_strutturale_ciclo(
                        ticker,
                        prezzo=float(res.get('close', 0)),
                        sr_data=sr
                    )
                    res.update(struttura)
                except Exception as _e_str:
                    _err.capture(_e_str, "get_market_data", {"module": "EngineLA"})
                    self.logger.debug(f"Struttura ciclo skip {ticker}: {_e_str}")
                    res['ciclo_fase'] = 'SCONOSCIUTO'
                    res['contesto_strutturale'] = ''
                    res['sr_flip_detected'] = False
                # ─────────────────────────────────────────────────────────────────

                # ── Heikin-Ashi daily bias ────────────────────────────────────────
                try:
                    ha_data = self._calcola_ha_daily(ticker, sr_data=sr)
                    res.update(ha_data)
                except Exception as _e_ha:
                    _err.capture(_e_ha, "get_market_data", {"module": "EngineLA"})
                    self.logger.debug(f"HA daily skip {ticker}: {_e_ha}")
                    res.update({'ha_daily_bias': 'NEUTRO', 'ha_daily_colore': 'NEUTRO',
                                'ha_daily_streak': 0, 'ha_daily_cambio': False,
                                'ha_daily_su_sr': False, 'ha_pullback_warn': False})
                # ─────────────────────────────────────────────────────────────────
                    if not hasattr(self, '_sr_cache'):    self._sr_cache    = {}
                    if not hasattr(self, '_sr_cache_ts'): self._sr_cache_ts = {}
                    self._sr_cache[_cache_key]    = sr
                    self._sr_cache_ts[_cache_key] = time.time()
                res['sr_resistenze']     = sr.get('sr_resistenze',     [])
                res['sr_supporti']       = sr.get('sr_supporti',       [])
                res['sr_res_piu_vicina'] = sr.get('sr_res_piu_vicina', 0.0)
                res['sr_sup_piu_vicina'] = sr.get('sr_sup_piu_vicina', 0.0)
            except Exception as e_pv:
                _err.capture(e_pv, "get_market_data", {"module": "EngineLA"})
                self.logger.debug(f"Pivot points skip {ticker}: {e_pv}")
                for k in ['pivot_daily','pivot_r1','pivot_r2','pivot_r3',
                          'pivot_s1','pivot_s2','pivot_s3','pivot_weekly']:
                    res[k] = 0.0
                res['pivot_trend'] = 'NEUTRO'

            # EMA struttura multi-TF (15m, 1h, 4h)
            try:
                ema_str = self._calcola_ema_struttura(ticker)
                res['ema_15m']               = ema_str.get('ema_15m',  {})
                res['ema_1h']                = ema_str.get('ema_1h',   {})
                res['ema_4h']                = ema_str.get('ema_4h',   {})
                res['ema_confluence_score']  = ema_str.get('ema_confluence_score',  0)
                res['ema_trend_dominante']   = ema_str.get('ema_trend_dominante',  'NEUTRO')
            except Exception as e_em:
                _err.capture(e_em, "get_market_data", {"module": "EngineLA"})
                self.logger.debug(f"EMA struttura skip {ticker}: {e_em}")
                res['ema_15m'] = res['ema_1h'] = res['ema_4h'] = {}
                res['ema_confluence_score'] = 0
                res['ema_trend_dominante']  = 'NEUTRO'

            # --- STEP 3: FVG STORICI, ORDER BLOCKS, BREAKER BLOCKS ---

            # FVG storici non colmati su H1 e H4
            try:
                fvg_st = self._calcola_fvg_storici(ticker)
                res['fvg_bull_h1']          = fvg_st.get('fvg_bull_h1', [])
                res['fvg_bear_h1']          = fvg_st.get('fvg_bear_h1', [])
                res['fvg_bull_h4']          = fvg_st.get('fvg_bull_h4', [])
                res['fvg_bear_h4']          = fvg_st.get('fvg_bear_h4', [])
                res['fvg_attivi_count']     = fvg_st.get('fvg_attivi_count', 0)
                res['fvg_bull_piu_vicino']  = fvg_st.get('fvg_bull_piu_vicino', 0.0)
                res['fvg_bear_piu_vicino']  = fvg_st.get('fvg_bear_piu_vicino', 0.0)
            except Exception as e_fvg:
                _err.capture(e_fvg, "get_market_data", {"module": "EngineLA"})
                self.logger.debug(f"FVG storici skip {ticker}: {e_fvg}")
                res['fvg_bull_h1'] = res['fvg_bear_h1'] = []
                res['fvg_bull_h4'] = res['fvg_bear_h4'] = []
                res['fvg_attivi_count']    = 0
                res['fvg_bull_piu_vicino'] = res['fvg_bear_piu_vicino'] = 0.0

            # Order Blocks istituzionali su H1 e H4
            try:
                ob = self._calcola_order_blocks(ticker)
                res['ob_bull_h1']          = ob.get('ob_bull_h1', [])
                res['ob_bear_h1']          = ob.get('ob_bear_h1', [])
                res['ob_bull_h4']          = ob.get('ob_bull_h4', [])
                res['ob_bear_h4']          = ob.get('ob_bear_h4', [])
                res['ob_bull_piu_vicino']  = ob.get('ob_bull_piu_vicino', 0.0)
                res['ob_bear_piu_vicino']  = ob.get('ob_bear_piu_vicino', 0.0)

                # Breaker Blocks — calcolati subito dopo, riusano gli OB già trovati
                # Passiamo tutti gli OB (H1 + H4 combinati) per non ricalcolarli
                tutti_ob_bull = ob.get('ob_bull_h1', []) + ob.get('ob_bull_h4', [])
                tutti_ob_bear = ob.get('ob_bear_h1', []) + ob.get('ob_bear_h4', [])
                bb = self._calcola_breaker_blocks(ticker, tutti_ob_bull, tutti_ob_bear)
                res['breaker_bull']               = bb.get('breaker_bull', [])
                res['breaker_bear']               = bb.get('breaker_bear', [])
                res['breaker_piu_vicino_sopra']   = bb.get('breaker_piu_vicino_sopra', 0.0)
                res['breaker_piu_vicino_sotto']   = bb.get('breaker_piu_vicino_sotto', 0.0)
            except Exception as e_ob:
                _err.capture(e_ob, "get_market_data", {"module": "EngineLA"})
                self.logger.debug(f"Order/Breaker blocks skip {ticker}: {e_ob}")
                res['ob_bull_h1'] = res['ob_bear_h1'] = []
                res['ob_bull_h4'] = res['ob_bear_h4'] = []
                res['ob_bull_piu_vicino'] = res['ob_bear_piu_vicino'] = 0.0
                res['breaker_bull'] = res['breaker_bear'] = []
                res['breaker_piu_vicino_sopra'] = res['breaker_piu_vicino_sotto'] = 0.0

            # --- STEP 4: BOS/CHoCH + SWEEP HISTORY ---

            # Break of Structure / Change of Character su H1
            try:
                bos_data = self._calcola_bos_choch(ticker)
                res['struttura_h1']   = bos_data.get('struttura_h1',   'LATERALE')
                res['ultimo_bos']     = bos_data.get('ultimo_bos',     'NESSUNO')
                res['ultimo_choch']   = bos_data.get('ultimo_choch',   'NESSUNO')
                res['bos_level']      = bos_data.get('bos_level',      0.0)
                res['choch_level']    = bos_data.get('choch_level',    0.0)
                res['swing_sequence'] = bos_data.get('swing_sequence', [])
            except Exception as e_bos:
                _err.capture(e_bos, "get_market_data", {"module": "EngineLA"})
                self.logger.debug(f"BOS/CHoCH skip {ticker}: {e_bos}")
                res['struttura_h1']   = 'LATERALE'
                res['ultimo_bos']     = 'NESSUNO'
                res['ultimo_choch']   = 'NESSUNO'
                res['bos_level']      = 0.0
                res['choch_level']    = 0.0
                res['swing_sequence'] = []

            # Sweep history: traccia se i muri correnti sono stati già testati
            try:
                sweep = self._aggiorna_sweep_history(
                    ticker,
                    muro_sup=res.get('muro_supporto', 0.0),
                    muro_res=res.get('muro_resistenza', 0.0),
                    prezzo=res.get('close', 0.0),
                )
                res['supporto_sweepato']     = sweep.get('supporto_sweepato',    False)
                res['resistenza_sweepata']   = sweep.get('resistenza_sweepata',  False)
                res['supporto_test_count']   = sweep.get('supporto_test_count',  0)
                res['resistenza_test_count'] = sweep.get('resistenza_test_count', 0)
                res['supporto_score']        = sweep.get('supporto_score',       0.0)
                res['resistenza_score']      = sweep.get('resistenza_score',     0.0)
            except Exception as e_sw2:
                _err.capture(e_sw2, "get_market_data", {"module": "EngineLA"})
                self.logger.debug(f"Sweep history skip {ticker}: {e_sw2}")
                res['supporto_sweepato']     = False
                res['resistenza_sweepata']   = False
                res['supporto_test_count']   = 0
                res['resistenza_test_count'] = 0
                res['supporto_score']        = 0.0
                res['resistenza_score']      = 0.0

            # --- FUNDING RATE & FUTURES DATA ---
            res['funding_rate'] = self._get_external_funding(ticker)
            res['funding_z_score'] = self._calcola_funding_zscore(ticker, res['funding_rate'])
            res['liquidazioni_24h'] = self._get_liquidations(ticker)
            res['open_interest'] = self._get_open_interest(ticker)
            res['macro_proxy'] = self._get_intermarket_data()
            res['put_call_ratio'] = self._get_put_call_ratio(ticker)
            res['macro_correlation'] = self._get_macro_correlation()
            
            # Gestione sicura per _check_portfolio_correlation
            try:
                posizioni_aperte = self.get_open_positions_real()
                res['portfolio_corr_risk'] = self._check_portfolio_correlation(ticker, posizioni_aperte)
            except Exception as e_corr:
                _err.capture(e_corr, "get_market_data", {"module": "EngineLA"})
                self.logger.debug(f"Errore calcolo correlazione portafoglio: {e_corr}")
                res['portfolio_corr_risk'] = 0.0
                
            res['delta_footprint'] = self._calcola_delta_footprint(ticker)
            res['whale_delta'] = flow_data.get('whale_delta', 0.0)
            res['market_driver'] = self.get_market_driver_logic(res['delta_footprint'], res['whale_delta'], 0.5)

            # ── SIGNAL STATE ENGINE: derivate temporali ──────────────────────
            # Calcola traiettoria, fase e exhaustion score basandosi sulla
            # storia degli ultimi 120s di campioni reali.
            # I risultati vengono aggiunti a res e poi passati a Brain/Gemini.
            try:
                if hasattr(self, 'signal_state') and self.signal_state is not None:
                    ss_output = self.signal_state.aggiorna(ticker, res)
                    res.update(ss_output)
                else:
                    # Fallback con valori neutri se il modulo non è inizializzato
                    res['entry_phase']          = 'FORMAZIONE'
                    res['exhaustion_score']     = 0
                    res['signal_age_s']         = 0
                    res['short_conditions_met'] = False
                    res['short_veto_motivo']    = 'SignalStateEngine non inizializzato'
                    res['signal_narrative']     = ''
                    res['cvd_trend']            = 'PIATTO'
                    res['cvd_delta_30s']        = 0.0
                    res['cvd_delta_120s']       = 0.0
                    res['cvd_acceleration']     = 0.0
            except Exception as e_ss:
                _err.capture(e_ss, "get_market_data", {"module": "EngineLA"})
                self.logger.debug(f"SignalStateEngine skip [{ticker}]: {e_ss}")
                res['entry_phase']      = 'FORMAZIONE'
                res['exhaustion_score'] = 0
                res['short_conditions_met'] = False
                res['signal_narrative'] = ''
            # ─────────────────────────────────────────────────────────────────

            self.logger.debug(
                f"📊 [DATA_DUMP] {ticker} | CVD: {flow_data.get('cvd_istantaneo', 0.0)} | Vel: {res['price_velocity']} | "
                f"Muro_S: {res['muro_supporto']} ({res['dist_supporto']:.2f}%) | "
                f"Muro_R: {res['muro_resistenza']} ({res['dist_resistenza']:.2f}%) | "
                f"VPIN: {res['vpin']:.4f} | Funding: {res['funding_rate']:.6f} | "
                f"Fase: {res.get('entry_phase','?')} | Exhaust: {res.get('exhaustion_score',0)}"
            )
            
            self.logger.debug(f"🔹 [ORDER FLOW] CVD: {flow_data.get('cvd_istantaneo', 0.0):+.2f} | VPIN: {res['vpin']:.4f}")
            self.logger.debug(f"🏥 MARKET HEALTH: {res['health_data']['market_health_index']} | REGIME: {res['market_regime']}")

            # ── Raccolta sequenza per LSTM futuro ────────────────────────────
            # Leggero — solo 14 campi, nessun blocco, nessun I/O sincrono
            try:
                from core.sequence_buffer import seq_buf
                seq_buf.push_snapshot(ticker, res)
            except Exception:
                pass
            # ─────────────────────────────────────────────────────────────────

            return res
        except Exception as e:
            _err.capture(e, "get_market_data", {"module": "EngineLA"})
            self.logger.error(f"🔴 Errore fatale get_market_data: {e}")
            return {"close": 0, "atr": 0.5, "market_regime": "MEAN_REVERSION", "funding_rate": 0.0}
    
    def get_price_velocity(self, ticker, trades_freschi=None):
        try:
            if trades_freschi is not None:
                trades = trades_freschi
            else:
                ws_trades = self.ws_manager.get_trades(ticker)
                # WS-first: usa WS anche con pochi trades (>=2)
                if ws_trades and len(ws_trades) >= 2:
                    trades = ws_trades
                else:
                    from core.asset_list import get_human_name
                    try:
                        trades = self.exchange.fetch_trades(get_human_name(ticker), limit=50)
                    except Exception:
                        trades = ws_trades or []
                    
            if not trades or len(trades) < 2: return 0.0
            
            p_start, p_end = float(trades[0]['price']), float(trades[-1]['price'])
            t_start, t_end = trades[0]['timestamp'] / 1000, trades[-1]['timestamp'] / 1000
            
            duration = max(t_end - t_start, 1)
            velocity = ((p_end - p_start) / p_start * 100) / duration
            return round(velocity, 6)
        except: return 0.0      
    
    def _calcola_divergenza_cvd_reale(self, trades):
        """Calcola la correlazione tra prezzo e CVD esclusivamente sui trade recenti (allineamento temporale)."""
        if not trades or len(trades) < 20:
            return 1.0 
            
        try:
            prezzi = []
            deltas = []
            cumulative = 0
            
            for t in trades:
                p = float(t['price'])
                d = float(t['amount']) * p if t['side'] == 'buy' else -float(t['amount']) * p
                cumulative += d
                prezzi.append(p)
                deltas.append(cumulative)
            
            if len(prezzi) < 10: return 1.0
            
            corr = np.corrcoef(prezzi, deltas)[0, 1]
            return float(corr) if not np.isnan(corr) else 1.0
        except Exception:
            return 1.0

    def _calcola_delta_footprint_veloce(self, trades):
        if not trades: return 0
        buys = sum([float(t['amount']) for t in trades if t['side'] == 'buy'])
        sells = sum([float(t['amount']) for t in trades if t['side'] == 'sell'])
        total = buys + sells
        return (buys - sells) / total if total > 0 else 0
    
    def _calcola_zscore(self, serie, window=20):
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
        if len(trades) < 50: return 0.5
        
        try:
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
                
            vpin_score = float(np.mean(vpin_buckets) / vol_per_bucket)
            return round(min(vpin_score, 1.0), 4)
            
        except Exception as e:
            _err.capture(e, "_get_vpin_toxicity_veloce", {"module": "EngineLA"})
            self.logger.debug(f" ️ Errore calcolo VPIN: {e}")
            return 0.5

    def get_market_health_score(self, ticker, order_flow_data):
        try:
            if not hasattr(self, '_last_hurst'):
                self._last_hurst = 0.5 
            
            vpin = float(order_flow_data.get('vpin', 0.0))
            hurst = self._last_hurst
        
            regime_conf = 1.0 if (hurst > 0.55 or hurst < 0.45) else 0.5
        
            health_score = (regime_conf + (1.0 - vpin)) / 2
        
            return {
                "market_health_index": round(health_score, 4),
                "vpin_value": round(vpin, 4),
                "hurst_used": hurst,
                "status": "HEALTHY" if health_score > 0.6 else "UNSTABLE"
            }
        except Exception as e:
            _err.capture(e, "get_market_health_score", {"module": "EngineLA"})
            self.logger.error(f"  Errore Health Score su {ticker}: {e}")
            return {"market_health_index": 0.5, "vpin_value": 0.0, "status": "UNKNOWN"}
    
    def get_full_market_data(self, ticker):
        try:
            data = {}
            try:
                data = self.get_market_data(ticker) or {}
            except Exception as e_gm:
                _err.capture(e_gm, "get_full_market_data", {"module": "EngineLA"})
                self.logger.debug(f"ℹ️ get_market_data fallito per {ticker}: {e_gm}")
                data = {}

            if isinstance(data, dict) and data.get('close') is not None:
                data['price'] = float(data.get('close', 0))
                
                raw_atr = data.get('atr', 0)
                if not raw_atr or float(raw_atr) == 0:
                    data['atr'] = data['price'] * 0.005
                else:
                    data['atr'] = float(raw_atr)

                if 'liquidity_pools' not in data or not data['liquidity_pools']:
                    data['liquidity_pools'] = []
                
                return data

            try:
                from core import asset_list
                symbol_kraken = get_ticker(ticker)
                ticker_info = self.exchange.fetch_ticker(symbol_kraken)
                return {
                    'price': float(ticker_info.get('last', 0)),
                    'close': float(ticker_info.get('last', 0)),
                    'atr': float(ticker_info.get('last', 0)) * 0.01,
                    'volume': float(ticker_info.get('baseVolume', 0)),
                    'market_regime': 'Noise',
                    'liquidity_pools': [],
                    'price_velocity': 0.0,
                    'cvd_reale': 0.0
                }
            except Exception as e_ft:
                _err.capture(e_ft, "get_full_market_data", {"module": "EngineLA"})
                self.logger.debug(f" ️ fetch_ticker fallback fallito per {ticker}: {e_ft}")

            return {'price': 0.0, 'close': 0.0, 'atr': 0.0, 'liquidity_pools': []}

        except Exception as e:
            _err.capture(e, "get_full_market_data", {"module": "EngineLA"})
            self.logger.error(f"  Errore critico get_full_market_data per {ticker}: {e}")
            return {'price': 0.0, 'close': 0.0, 'atr': 0.0, 'liquidity_pools': []}
    
    def analizza_asset(self, ticker):
        return self.get_full_market_data(ticker)
    
    def get_liquidity_walls(self, asset_id):
        try:
            ws_ob = self.ws_manager.get_order_book(asset_id)
            if ws_ob and ws_ob.get('bids') and ws_ob.get('asks'):
                ob = ws_ob
            else:
                from core.asset_list import get_human_name
                ticker_ccxt = get_human_name(asset_id)
                ob = self._safe_fetch('fetch_order_book', ticker_ccxt, limit=500)
            
            if not ob.get('bids') or not ob.get('asks'):
                return {"muro_supporto": 0, "dist_supporto": 0.0, "muro_resistenza": 0, "dist_resistenza": 0.0}
            
            last_price = float(ob['bids'][0][0])
            
            def get_best_zone(orders, side):
                if not orders: return 0.0, 0.0, 0.0
                df_ob = pd.DataFrame(orders).iloc[:, :2]
                df_ob.columns = ['price', 'vol']
                df_ob['price'] = pd.to_numeric(df_ob['price'], errors='coerce')
                df_ob['vol']   = pd.to_numeric(df_ob['vol'],   errors='coerce')
                df_ob = df_ob.dropna()
                if df_ob.empty: return 0.0, 0.0, 0.0

                # ── FILTRO DUST ──────────────────────────────────────────────
                # Market maker automatici piazzano bids quasi-zero che
                # collassano in zona 0.0 e diventano il cluster dominante.
                # Finestra ±80%/×5 elimina dust mantenendo tutto l'OB significativo.
                if last_price > 0:
                    if side == 'bid':
                        df_ob = df_ob[df_ob['price'] >= last_price * 0.20]
                    else:
                        df_ob = df_ob[df_ob['price'] <= last_price * 5.00]
                if df_ob.empty: return 0.0, 0.0, 0.0

                # Bin size adattivo per trovare muri REALI:
                # BTC  (>10000):  0.2% | ETH/SOL (>100): 0.3%
                # Altcoin (>0.01): 0.5% | Micro-price:   2.0%
                if last_price > 10000:
                    bin_pct = 0.002
                elif last_price > 100:
                    bin_pct = 0.003
                elif last_price > 0.01:
                    bin_pct = 0.005
                else:
                    bin_pct = 0.020   # BONK/SHIB/PEPE: bin 2%
                bin_size = last_price * bin_pct

                if side == 'bid':
                    df_ob['zone'] = np.floor(df_ob['price'] / bin_size) * bin_size
                else:
                    df_ob['zone'] = np.ceil(df_ob['price'] / bin_size) * bin_size

                clusters = df_ob.groupby('zone')['vol'].sum().sort_values(ascending=False)
                if clusters.empty: return 0.0, 0.0, 0.0

                # Scegli il cluster più grande che non sia il bin immediato del prezzo
                # (quello sarebbe solo spread normale, non un muro)
                bin_corrente = (np.floor(last_price / bin_size) * bin_size
                                if side == 'bid'
                                else np.ceil(last_price / bin_size) * bin_size)

                for zone_price in clusters.index:
                    if abs(zone_price - bin_corrente) > bin_size * 0.5:
                        best_price = float(zone_price)
                        best_vol   = float(clusters[zone_price])
                        distanza   = abs(best_price - last_price) / last_price * 100 if last_price > 0 else 0.0
                        return best_price, best_vol, round(distanza, 2)

                # Fallback: prendi il cluster più grande comunque
                best_price = float(clusters.index[0])
                distanza   = abs(best_price - last_price) / last_price * 100 if last_price > 0 else 0.0
                return best_price, float(clusters.iloc[0]), round(distanza, 2)

            w_bid_p, w_bid_v, d_bid = get_best_zone(ob['bids'], 'bid')
            w_ask_p, w_ask_v, d_ask = get_best_zone(ob['asks'], 'ask')

            if w_bid_p == 0 or w_ask_p == 0:
                self.logger.warning(f"⚠️ get_liquidity_walls per {asset_id} ha restituito 0. bids len: {len(ob.get('bids', []))}, asks len: {len(ob.get('asks', []))}")

            return {
                "muro_supporto": w_bid_p, "vol_supporto": w_bid_v, "dist_supporto": d_bid,
                "muro_resistenza": w_ask_p, "vol_resistenza": w_ask_v, "dist_resistenza": d_ask
            }
        except Exception as e:
            _err.capture(e, "get_liquidity_walls", {"module": "EngineLA"})
            self.logger.warning(f" ️ Errore Liquidity Walls: {e}")
            return {"muro_supporto": 0, "dist_supporto": 0.0, "muro_resistenza": 0, "dist_resistenza": 0.0}
            
    def get_liquidity_pools(self, ticker):
        try:
            ws_ob = self.ws_manager.get_order_book(ticker)
            if ws_ob and ws_ob.get('bids') and ws_ob.get('asks'):
                ob = ws_ob
            else:
                from core.asset_list import get_human_name
                asset_id = get_human_name(ticker)
                ob = self._safe_fetch('fetch_order_book', asset_id, limit=500)
            
            def find_top_zones(orders):
                if not orders: return []
                df = pd.DataFrame(orders).iloc[:, :2]
                df.columns = ['prezzo', 'volume']
                df['prezzo'] = pd.to_numeric(df['prezzo'], errors='coerce')
                df['volume'] = pd.to_numeric(df['volume'], errors='coerce')
                return df.dropna().sort_values('volume', ascending=False).head(3).to_dict('records')

            def find_liquidity_voids(orders):
                if not orders or len(orders) < 10: return []
                df = pd.DataFrame(orders).iloc[:, :2]
                df.columns = ['prezzo', 'volume']
                df['prezzo'] = pd.to_numeric(df['prezzo'], errors='coerce')
                df['volume'] = pd.to_numeric(df['volume'], errors='coerce')
                df = df.dropna()
                if df.empty: return []
                
                mean_vol = df['volume'].mean()
                voids = df[df['volume'] < mean_vol * 0.1]
                
                if voids.empty: return []
                
                void_zones = []
                current_void = []
                
                for idx, row in voids.iterrows():
                    if not current_void:
                        current_void.append(row['prezzo'])
                    else:
                        prev_price = current_void[-1]
                        if abs(row['prezzo'] - prev_price) / prev_price < 0.005:
                            current_void.append(row['prezzo'])
                        else:
                            if len(current_void) > 2:
                                void_zones.append((min(current_void), max(current_void)))
                            current_void = [row['prezzo']]
                
                if len(current_void) > 2:
                    void_zones.append((min(current_void), max(current_void)))
                    
                return void_zones

            return {
                "pools_supporto": find_top_zones(ob.get('bids', [])),
                "pools_resistenza": find_top_zones(ob.get('asks', [])),
                "voids_supporto": find_liquidity_voids(ob.get('bids', [])),
                "voids_resistenza": find_liquidity_voids(ob.get('asks', []))
            }
        except Exception as e:
            _err.capture(e, "get_liquidity_pools", {"module": "EngineLA"})
            self.logger.error(f"🔴 Errore Liquidity Mapping {ticker}: {e}")
            return {"pools_supporto": [], "pools_resistenza": []}
            
    def _calcola_squeeze(self, df):
        std = df['close'].rolling(20).std()
        sma = df['close'].rolling(20).mean()
        
        upper_bb = sma + (2 * std)
        lower_bb = sma - (2 * std)
        
        tr = pd.concat([df['high']-df['low'], abs(df['high']-df['close'].shift()), abs(df['low']-df['close'].shift())], axis=1).max(axis=1)
        atr_20 = tr.rolling(20).mean()
        upper_kc = sma + (1.5 * atr_20)
        lower_kc = sma - (1.5 * atr_20)
        
        try:
            is_squeeze = (lower_bb.iloc[-1] > lower_kc.iloc[-1]) and (upper_bb.iloc[-1] < upper_kc.iloc[-1])
            return "ON" if is_squeeze else "OFF"
        except:
            return "OFF"

    def _check_fvg(self, df):
        try:
            if len(df) < 3: return "NONE"
            
            c1_high = float(df['high'].iloc[-3])
            c1_low = float(df['low'].iloc[-3])
            c3_high = float(df['high'].iloc[-1])
            c3_low = float(df['low'].iloc[-1])

            if c1_high < c3_low: 
                return f"BULL_GAP ({c1_high:.4f} - {c3_low:.4f})"
            if c1_low > c3_high: 
                return f"BEAR_GAP ({c3_high:.4f} - {c1_low:.4f})"
        except:
            pass
        return "NONE"

    # ══════════════════════════════════════════════════════════════════════
    #  STEP 2 — LIVELLI STRUTTURALI: SWING, PIVOT POINTS, EMA
    # ══════════════════════════════════════════════════════════════════════

    def _calcola_swing_levels(self, ticker: str) -> dict:
        """
        Calcola i Swing High e Swing Low strutturali su candele H1 e H4.

        Un swing high è un massimo locale dove le N candele a sinistra e a destra
        hanno high inferiori. Stessa logica speculare per i swing low.
        Questi livelli rappresentano dove il mercato ha già respinto una volta —
        alta probabilità di reazione anche al prossimo passaggio.

        Restituisce:
            swing_high_h1  : list[float] — massimi strutturali su 1h (max 5)
            swing_low_h1   : list[float] — minimi strutturali su 1h (max 5)
            swing_high_h4  : list[float] — massimi strutturali su 4h (max 3)
            swing_low_h4   : list[float] — minimi strutturali su 4h (max 3)
            res_strutturale: float — resistenza strutturale più vicina sopra
            sup_strutturale: float — supporto strutturale più vicino sotto
        """
        default = {
            'swing_high_h1':   [],
            'swing_low_h1':    [],
            'swing_high_h4':   [],
            'swing_low_h4':    [],
            'res_strutturale': 0.0,
            'sup_strutturale': 0.0,
        }
        try:
            from core.asset_list import get_human_name
            symbol_ccxt = get_human_name(ticker)

            def find_swings(df: pd.DataFrame, n: int = 3) -> tuple:
                """
                Trova swing high/low con finestra di n candele per lato.
                n=3 su H1 cattura strutture di breve (scalp/swing).
                n=3 su H4 cattura strutture di medio termine.
                """
                highs, lows = [], []
                closes = df['close'].values
                high_v = df['high'].values
                low_v  = df['low'].values

                for i in range(n, len(df) - n):
                    # Swing High: high[i] è il massimo nei 2n+1 candle
                    if all(high_v[i] > high_v[i - j] for j in range(1, n + 1)) and \
                       all(high_v[i] > high_v[i + j] for j in range(1, n + 1)):
                        highs.append(float(high_v[i]))

                    # Swing Low: low[i] è il minimo nei 2n+1 candle
                    if all(low_v[i] < low_v[i - j] for j in range(1, n + 1)) and \
                       all(low_v[i] < low_v[i + j] for j in range(1, n + 1)):
                        lows.append(float(low_v[i]))

                return highs, lows

            result = {}

            for tf_label, tf_kraken, limit, n_swing, max_levels in [
                ('h1', '60',  100, 3, 5),
                ('h4', '240',  60, 3, 3),
            ]:
                try:
                    ohlcv = self._safe_fetch(
                        'fetch_ohlcv', symbol_ccxt,
                        timeframe=tf_kraken, limit=limit
                    )
                    if not ohlcv or len(ohlcv) < 20:
                        result[f'swing_high_{tf_label}'] = []
                        result[f'swing_low_{tf_label}']  = []
                        continue

                    df_tf = pd.DataFrame(
                        ohlcv,
                        columns=['ts', 'open', 'high', 'low', 'close', 'volume']
                    )
                    for col in ['open', 'high', 'low', 'close', 'volume']:
                        df_tf[col] = pd.to_numeric(df_tf[col], errors='coerce')
                    df_tf = df_tf.dropna()

                    sh, sl = find_swings(df_tf, n=n_swing)

                    # Prendiamo i più recenti (coda del DataFrame = più recenti)
                    result[f'swing_high_{tf_label}'] = sorted(set(sh[-max_levels:]), reverse=True)
                    result[f'swing_low_{tf_label}']  = sorted(set(sl[-max_levels:]), reverse=True)

                except Exception as e_tf:
                    _err.capture(e_tf, "_calcola_swing_levels", {"module": "EngineLA"})
                    self.logger.debug(f"Errore swing {tf_label} per {ticker}: {e_tf}")
                    result[f'swing_high_{tf_label}'] = []
                    result[f'swing_low_{tf_label}']  = []

            # Resistenza e supporto strutturale più vicini al prezzo corrente
            try:
                prezzo = float(self.get_current_price(ticker) or 0)
                if prezzo > 0:
                    tutti_sh = result.get('swing_high_h1', []) + result.get('swing_high_h4', [])
                    tutti_sl = result.get('swing_low_h1',  []) + result.get('swing_low_h4',  [])

                    res_sopra = [p for p in tutti_sh if p > prezzo * 1.001]
                    sup_sotto = [p for p in tutti_sl if p < prezzo * 0.999]

                    result['res_strutturale'] = round(min(res_sopra), 6) if res_sopra else 0.0
                    result['sup_strutturale'] = round(max(sup_sotto), 6) if sup_sotto else 0.0
                else:
                    result['res_strutturale'] = 0.0
                    result['sup_strutturale'] = 0.0
            except Exception:
                result['res_strutturale'] = 0.0
                result['sup_strutturale'] = 0.0

            return result

        except Exception as e:
            _err.capture(e, "_calcola_swing_levels", {"module": "EngineLA"})
            self.logger.debug(f"Errore _calcola_swing_levels {ticker}: {e}")
            return default

    def _calcola_pivot_points(self, ticker: str) -> dict:
        """
        Calcola i Pivot Points classici (Floor Pivots) su base daily e weekly.

        Formula standard:
            Pivot = (H + L + C) / 3
            R1 = 2*P - L      S1 = 2*P - H
            R2 = P + (H - L)  S2 = P - (H - L)
            R3 = H + 2*(P-L)  S3 = L - 2*(H-P)

        I pivot daily si calcolano sulla candela del giorno precedente.
        I pivot weekly si calcolano sulla settimana precedente.
        Sono livelli statici, cambiano solo quando cambia la sessione.

        Restituisce dizionario con:
            daily_pivot, daily_r1/r2/r3, daily_s1/s2/s3
            weekly_pivot, weekly_r1/r2, weekly_s1/s2
            pivot_trend: "SOPRA_PIVOT" | "SOTTO_PIVOT"
        """
        default = {
            'daily_pivot': 0.0,
            'daily_r1': 0.0, 'daily_r2': 0.0, 'daily_r3': 0.0,
            'daily_s1': 0.0, 'daily_s2': 0.0, 'daily_s3': 0.0,
            'weekly_pivot': 0.0,
            'weekly_r1': 0.0, 'weekly_r2': 0.0,
            'weekly_s1': 0.0, 'weekly_s2': 0.0,
            'pivot_trend': 'NEUTRO',
        }
        try:
            from core.asset_list import get_human_name
            symbol_ccxt = get_human_name(ticker)

            def calcola_livelli(h: float, l: float, c: float) -> dict:
                p  = (h + l + c) / 3
                r1 = 2 * p - l
                r2 = p + (h - l)
                r3 = h + 2 * (p - l)
                s1 = 2 * p - h
                s2 = p - (h - l)
                s3 = l - 2 * (h - p)
                return {
                    'pivot': round(p,  6),
                    'r1':    round(r1, 6), 'r2': round(r2, 6), 'r3': round(r3, 6),
                    's1':    round(s1, 6), 's2': round(s2, 6), 's3': round(s3, 6),
                }

            result = dict(default)

            # ── Daily pivot: usa la candela del giorno precedente (1d, limit=3) ──
            try:
                ohlcv_d = self._safe_fetch(
                    'fetch_ohlcv', symbol_ccxt,
                    timeframe='1440', limit=3
                )
                if ohlcv_d and len(ohlcv_d) >= 2:
                    # indice -2: giorno precedente completo
                    _, o, h, l, c, _ = ohlcv_d[-2]
                    lv = calcola_livelli(float(h), float(l), float(c))
                    result['daily_pivot'] = lv['pivot']
                    result['daily_r1']    = lv['r1']
                    result['daily_r2']    = lv['r2']
                    result['daily_r3']    = lv['r3']
                    result['daily_s1']    = lv['s1']
                    result['daily_s2']    = lv['s2']
                    result['daily_s3']    = lv['s3']
            except Exception as e_d:
                _err.capture(e_d, "_calcola_pivot_points", {"module": "EngineLA"})
                self.logger.debug(f"Pivot daily error {ticker}: {e_d}")

            # ── Weekly pivot: aggrega le ultime 7 candele daily ──
            try:
                ohlcv_w = self._safe_fetch(
                    'fetch_ohlcv', symbol_ccxt,
                    timeframe='1440', limit=14
                )
                if ohlcv_w and len(ohlcv_w) >= 7:
                    settimana = ohlcv_w[-8:-1]  # 7 giorni scorsi (escluso oggi)
                    h_w = max(float(row[2]) for row in settimana)
                    l_w = min(float(row[3]) for row in settimana)
                    c_w = float(settimana[-1][4])  # close dell'ultimo giorno
                    lv_w = calcola_livelli(h_w, l_w, c_w)
                    result['weekly_pivot'] = lv_w['pivot']
                    result['weekly_r1']    = lv_w['r1']
                    result['weekly_r2']    = lv_w['r2']
                    result['weekly_s1']    = lv_w['s1']
                    result['weekly_s2']    = lv_w['s2']
            except Exception as e_w:
                _err.capture(e_w, "_calcola_pivot_points", {"module": "EngineLA"})
                self.logger.debug(f"Pivot weekly error {ticker}: {e_w}")

            # ── Pivot trend: il prezzo è sopra o sotto il daily pivot? ──
            try:
                prezzo = float(self.get_current_price(ticker) or 0)
                if prezzo > 0 and result['daily_pivot'] > 0:
                    result['pivot_trend'] = (
                        'SOPRA_PIVOT' if prezzo > result['daily_pivot'] else 'SOTTO_PIVOT'
                    )
            except Exception:
                pass

            return result

        except Exception as e:
            _err.capture(e, "_calcola_pivot_points", {"module": "EngineLA"})
            self.logger.debug(f"Errore _calcola_pivot_points {ticker}: {e}")
            return default


    def _calcola_livelli_storici(self, ticker: str, prezzo: float) -> dict:
        """
        Calcola supporti e resistenze storici su candele daily (ultimi 6 mesi).
        Cluster di swing high/low con indice di forza (quante volte testato).
        """
        default = {'sr_resistenze': [], 'sr_supporti': [], 'sr_res_piu_vicina': 0.0, 'sr_sup_piu_vicina': 0.0}
        try:
            from core.asset_list import get_human_name
            symbol_ccxt = get_human_name(ticker)
            ohlcv = self._safe_fetch('fetch_ohlcv', symbol_ccxt, timeframe='1440', limit=180)
            if not ohlcv or len(ohlcv) < 30:
                return default

            import pandas as pd
            df = pd.DataFrame(ohlcv, columns=['ts','open','high','low','close','volume'])
            for col in ['open','high','low','close','volume']:
                df[col] = pd.to_numeric(df[col], errors='coerce')
            df = df.dropna().reset_index(drop=True)

            n = 3
            swing_highs, swing_lows = [], []
            for i in range(n, len(df) - n):
                if all(df['high'].iloc[i] > df['high'].iloc[i-j] for j in range(1, n+1)) and                    all(df['high'].iloc[i] > df['high'].iloc[i+j] for j in range(1, n+1)):
                    swing_highs.append(float(df['high'].iloc[i]))
                if all(df['low'].iloc[i] < df['low'].iloc[i-j] for j in range(1, n+1)) and                    all(df['low'].iloc[i] < df['low'].iloc[i+j] for j in range(1, n+1)):
                    swing_lows.append(float(df['low'].iloc[i]))

            def cluster_levels(levels, tol=0.005):
                if not levels: return []
                sorted_lv = sorted(levels)
                clusters, current = [], [sorted_lv[0]]
                for lv in sorted_lv[1:]:
                    if (lv - current[-1]) / current[-1] < tol:
                        current.append(lv)
                    else:
                        clusters.append(current); current = [lv]
                clusters.append(current)
                return sorted([{'prezzo': round(sum(c)/len(c), 6), 'forza': len(c)} for c in clusters], key=lambda x: -x['forza'])

            cl_res = cluster_levels(swing_highs)
            cl_sup = cluster_levels(swing_lows)
            if prezzo <= 0: return default

            resistenze = sorted([{**c, 'dist_perc': round((c['prezzo']-prezzo)/prezzo*100, 2)} for c in cl_res if c['prezzo'] > prezzo * 1.002], key=lambda x: x['dist_perc'])[:3]
            supporti   = sorted([{**c, 'dist_perc': round((prezzo-c['prezzo'])/prezzo*100, 2)} for c in cl_sup if c['prezzo'] < prezzo * 0.998], key=lambda x: x['dist_perc'])[:3]

            # Precisione adattiva per prezzo: sempre int per evitare TypeError in f-string
            _p0 = (resistenze[0]['prezzo'] if resistenze else
                   supporti[0]['prezzo']   if supporti   else 1.0)
            _prec_log = (8 if _p0 < 0.001    # BONK/SHIB/PEPE: $0.000015
                         else 6 if _p0 < 0.1  # asset sub-centesimo
                         else 4 if _p0 < 10   # DOGE, XRP, MATIC
                         else 2 if _p0 < 1000 # SOL, ETH, TAO
                         else 1)              # BTC
            _prec_log = int(_prec_log)         # garanzia: sempre int (no TypeError)
            self.logger.info(
                f"📊 [{ticker}] S/R storici daily | "
                f"Resistenze: {[f"{r['prezzo']:.{_prec_log}f}({int(r['forza'])}x)" for r in resistenze]} | "
                f"Supporti: {[f"{s['prezzo']:.{_prec_log}f}({int(s['forza'])}x)" for s in supporti]}"
            )
            return {
                'sr_resistenze': resistenze, 'sr_supporti': supporti,
                'sr_res_piu_vicina': resistenze[0]['prezzo'] if resistenze else 0.0,
                'sr_sup_piu_vicina': supporti[0]['prezzo']   if supporti   else 0.0,
            }
        except Exception as e:
            _err.capture(e, "_calcola_livelli_storici", {"module": "EngineLA"})
            self.logger.debug(f"Errore _calcola_livelli_storici {ticker}: {e}")
            return default


    def _analisi_strutturale_ciclo(self, ticker: str, prezzo: float, sr_data: dict = None) -> dict:
        """
        Analisi strutturale del ciclo di mercato su candele daily.

        Calcola 3 elementi che cambiano la lettura di qualsiasi segnale a breve termine:

        1. POSIZIONE NEL CICLO — dove si trova il prezzo rispetto al minimo/massimo
           significativo degli ultimi 90 giorni. Distingue "recupero dal minimo",
           "vicino al massimo", "nel mezzo del range".

        2. QUALITÀ DEL MINIMO — il minimo è una capitolazione reale (volume 3-5x la
           media) o un minimo debole? Questo determina la solidità del supporto strutturale.

        3. S/R FLIP — il prezzo attuale si trova su un livello che era supporto prima
           di un crollo e ora è resistenza (o viceversa)? Contesto critico per decidere
           se un pullback è buy o sell.
        """
        default = {
            'ciclo_fase':           'SCONOSCIUTO',
            'ciclo_dist_minimo_pct': 0.0,
            'ciclo_dist_massimo_pct': 0.0,
            'ciclo_minimo_90g':     0.0,
            'ciclo_massimo_90g':    0.0,
            'ciclo_recupero_pct':   0.0,
            'minimo_volume_ratio':  1.0,
            'minimo_qualita':       'NORMALE',
            'sr_flip_detected':     False,
            'sr_flip_livello':      0.0,
            'sr_flip_tipo':         '',
            'contesto_strutturale': '',
        }
        try:
            from core.asset_list import get_human_name
            import pandas as pd, numpy as np
            symbol_ccxt = get_human_name(ticker)

            # Usa i dati già scaricati se disponibili, altrimenti fetch
            ohlcv = self._safe_fetch('fetch_ohlcv', symbol_ccxt, timeframe='1440', limit=90)
            if not ohlcv or len(ohlcv) < 20:
                return default

            df = pd.DataFrame(ohlcv, columns=['ts','open','high','low','close','volume'])
            for col in ['open','high','low','close','volume']:
                df[col] = pd.to_numeric(df[col], errors='coerce')
            df = df.dropna().reset_index(drop=True)

            close_now = prezzo if prezzo > 0 else float(df['close'].iloc[-1])

            # ── 1. POSIZIONE NEL CICLO ────────────────────────────────────────
            min_90g = float(df['low'].min())
            max_90g = float(df['high'].max())
            range_90g = max_90g - min_90g

            dist_minimo_pct  = (close_now - min_90g) / min_90g * 100 if min_90g > 0 else 0
            dist_massimo_pct = (max_90g - close_now) / max_90g * 100 if max_90g > 0 else 0
            recupero_pct = (close_now - min_90g) / range_90g * 100 if range_90g > 0 else 50

            # Identifica la fase del ciclo
            if recupero_pct < 25:
                ciclo_fase = 'FONDO'           # vicino al minimo
            elif recupero_pct < 45:
                ciclo_fase = 'RECUPERO_INIZIALE'  # 0-45% di recupero
            elif recupero_pct < 65:
                ciclo_fase = 'RECUPERO_MEDIO'     # 45-65% — zona centrale
            elif recupero_pct < 85:
                ciclo_fase = 'VICINO_MASSIMO'     # 65-85%
            else:
                ciclo_fase = 'MASSIMO'           # vicino al massimo

            # ── 2. QUALITÀ DEL MINIMO (volume capitolazione) ─────────────────
            idx_min = df['low'].idxmin()
            vol_al_minimo = float(df['volume'].iloc[idx_min])
            vol_medio_90g = float(df['volume'].mean())
            minimo_vol_ratio = vol_al_minimo / vol_medio_90g if vol_medio_90g > 0 else 1.0

            if minimo_vol_ratio >= 3.0:
                minimo_qualita = 'CAPITOLAZIONE'    # volume 3x+ = vendita panica = fondo solido
            elif minimo_vol_ratio >= 1.5:
                minimo_qualita = 'SIGNIFICATIVO'    # volume elevato ma non estremo
            else:
                minimo_qualita = 'DEBOLE'          # volume normale = fondo instabile

            # ── 3. S/R FLIP ───────────────────────────────────────────────────
            # Cerca livelli dove il prezzo era supporto prima di un crollo
            # e adesso è resistenza (o viceversa)
            sr_flip = False
            sr_flip_livello = 0.0
            sr_flip_tipo = ''

            if sr_data:
                resistenze = sr_data.get('sr_resistenze', [])
                supporti   = sr_data.get('sr_supporti', [])

                # Cerca resistenze che erano supporti prima del crollo
                for res in resistenze:
                    livello = res.get('prezzo', 0)
                    if livello <= 0: continue
                    dist_pct = abs(close_now - livello) / close_now * 100
                    if dist_pct <= 5.0:  # entro 5% dal prezzo attuale
                        # Verifica se questo livello era supporto prima
                        # (presenza di swing lows storici vicini a questo livello)
                        sw_lows_vicini = df[(df['low'] >= livello * 0.97) &
                                            (df['low'] <= livello * 1.03)]
                        if len(sw_lows_vicini) >= 2:
                            sr_flip = True
                            sr_flip_livello = livello
                            sr_flip_tipo = 'RESISTENZA_EX_SUPPORTO'
                            break

                # Cerca supporti che erano resistenze prima
                if not sr_flip:
                    for sup in supporti:
                        livello = sup.get('prezzo', 0)
                        if livello <= 0: continue
                        dist_pct = abs(close_now - livello) / close_now * 100
                        if dist_pct <= 5.0:
                            sw_highs_vicini = df[(df['high'] >= livello * 0.97) &
                                                 (df['high'] <= livello * 1.03)]
                            if len(sw_highs_vicini) >= 2:
                                sr_flip = True
                                sr_flip_livello = livello
                                sr_flip_tipo = 'SUPPORTO_EX_RESISTENZA'
                                break

            # ── Costruisci narrativa testuale per Gemini ──────────────────────
            lines = []

            lines.append(f"POSIZIONE NEL CICLO (90 giorni):")
            lines.append(f"  Fase: {ciclo_fase} — recuperato {recupero_pct:.0f}% dal minimo")
            lines.append(f"  Minimo 90g: {min_90g:.2f} (+{dist_minimo_pct:.1f}% dal fondo)")
            lines.append(f"  Massimo 90g: {max_90g:.2f} (-{dist_massimo_pct:.1f}% dal picco)")

            lines.append(f"QUALITÀ DEL MINIMO:")
            if minimo_qualita == 'CAPITOLAZIONE':
                lines.append(
                    f"  CAPITOLAZIONE confermata — volume al minimo {minimo_vol_ratio:.1f}x la media. "
                    f"Fondo strutturale solido. Il supporto regge finché non ci sono catalizzatori macro negativi."
                )
            elif minimo_qualita == 'SIGNIFICATIVO':
                lines.append(
                    f"  Minimo con volume elevato ({minimo_vol_ratio:.1f}x media) — supporto decente "
                    f"ma non definitivo. Possibile secondo test del minimo."
                )
            else:
                lines.append(
                    f"  Minimo debole (volume {minimo_vol_ratio:.1f}x media) — fondo instabile. "
                    f"Alto rischio di nuovo test del minimo o rottura."
                )

            if sr_flip:
                if sr_flip_tipo == 'RESISTENZA_EX_SUPPORTO':
                    # Distingui S/R flip nel contesto del ciclo strutturale
                    if ciclo_fase in ('RECUPERO_INIZIALE', 'RECUPERO_MEDIO') and minimo_qualita == 'CAPITOLAZIONE':
                        lines.append(
                            f"S/R FLIP: prezzo su {sr_flip_livello:.2f} (ex supporto ora resistenza). "
                            f"CONTESTO: siamo in recupero da capitolazione — questo flip è un ostacolo "
                            f"temporaneo, non un segnale di short. "
                            f"Breakout sopra con volume = continuazione del recupero. "
                            f"Respingimento = pullback su supporto intraday, poi LONG."
                        )
                    else:
                        lines.append(
                            f"S/R FLIP RILEVATO: Il prezzo è su {sr_flip_livello:.2f} che era SUPPORTO "
                            f"prima del crollo, ora è RESISTENZA. "
                            f"Primo test da sotto — alta probabilità di respingimento. "
                            f"LONG qui richiede breakout confermato con volume. "
                            f"SHORT su respingimento valido solo se ciclo strutturale è ribassista."
                        )
                else:
                    lines.append(
                        f"S/R FLIP RILEVATO: Il prezzo è su {sr_flip_livello:.2f} che era RESISTENZA, "
                        f"ora diventa SUPPORTO. "
                        f"Zona di accumulo istituzionale probabile. LONG favorito se regge."
                    )
            else:
                lines.append("S/R FLIP: nessun flip rilevato nella zona corrente.")

            # Sintesi operativa
            if ciclo_fase in ('FONDO', 'RECUPERO_INIZIALE') and minimo_qualita == 'CAPITOLAZIONE':
                lines.append(
                    "SINTESI: Recupero da capitolazione — bias LONG strutturale. "
                    "HA rosso di 1 giorno è pullback fisiologico, non inversione. "
                    "Cerca entrate long su supporti intraday."
                )
            elif ciclo_fase == 'VICINO_MASSIMO' and sr_flip and sr_flip_tipo == 'RESISTENZA_EX_SUPPORTO':
                lines.append(
                    "SINTESI: Vicino al massimo con resistenza strutturale — zona pericolosa. "
                    "Breakout sopra il S/R flip apre spazio. Respingimento = SHORT valido."
                )
            elif ciclo_fase in ('RECUPERO_MEDIO',):
                lines.append(
                    "SINTESI: Fase di recupero intermedia — nessun edge direzionale forte. "
                    "Seguire il momentum di breve termine con stops stretti."
                )
            elif ciclo_fase in ('VICINO_MASSIMO', 'MASSIMO') and not sr_flip:
                # Aggiunto 2026-05-01 per simmetria: massimi senza S/R flip negativo
                # sono tipici di trend rialzisti chiari — non automaticamente "pericolosi"
                lines.append(
                    "SINTESI: Vicino o sui massimi del ciclo — il trend rialzista è in corso. "
                    "Senza S/R flip negativo nelle vicinanze, il setup è continuazione: "
                    "LONG validi su pullback intraday verso supporti, stop stretti. "
                    "SHORT solo su pattern di esaurimento chiari (esempio: rejection forte con volume "
                    "su nuovo massimo + CVD divergente sostenuto)."
                )

            contesto = "\n".join(lines)

            self.logger.info(
                f"🏗️ [STRUTTURA] {ticker}: ciclo={ciclo_fase} "
                f"recupero={recupero_pct:.0f}% "
                f"minimo={minimo_qualita} (vol {minimo_vol_ratio:.1f}x) "
                f"sr_flip={sr_flip_tipo or 'no'}"
            )

            return {
                'ciclo_fase':            ciclo_fase,
                'ciclo_dist_minimo_pct': round(dist_minimo_pct, 1),
                'ciclo_dist_massimo_pct': round(dist_massimo_pct, 1),
                'ciclo_minimo_90g':      round(min_90g, 4),
                'ciclo_massimo_90g':     round(max_90g, 4),
                'ciclo_recupero_pct':    round(recupero_pct, 1),
                'minimo_volume_ratio':   round(minimo_vol_ratio, 2),
                'minimo_qualita':        minimo_qualita,
                'sr_flip_detected':      sr_flip,
                'sr_flip_livello':       round(sr_flip_livello, 4),
                'sr_flip_tipo':          sr_flip_tipo,
                'contesto_strutturale':  contesto,
            }

        except Exception as e:
            _err.capture(e, "_analisi_strutturale_ciclo", {"module": "EngineLA"})
            self.logger.debug(f"_analisi_strutturale_ciclo {ticker}: {e}")
            return default

    def _calcola_ha_daily(self, ticker: str, sr_data: dict = None) -> dict:
        """
        Calcola Heikin-Ashi su timeframe daily e determina il bias di trend.

        Logica:
        - Il cambio di trend arriva con la chiusura di colore opposto al precedente
          che avviene su supporto o resistenza storica giornaliera.
        - Il bias operativo segue il colore dell'ultima HA daily CHIUSA (non quella
          in formazione, che può ingannare).
        - Pullback warning: HA verde ma prezzo tocca supporto → rischio rottura.

        Returns:
            ha_daily_bias:    'LONG' | 'SHORT' | 'NEUTRO'
            ha_daily_colore:  'VERDE' | 'ROSSO'
            ha_daily_streak:  N candele consecutive stesso colore
            ha_daily_cambio:  True se l'ultima candela chiusa ha cambiato colore
            ha_daily_su_sr:   True se il cambio avviene vicino a S/R (±1.5%)
            ha_daily_sr_level: prezzo S/R più vicino al cambio
            ha_pullback_warn: True se HA verde ma siamo su supporto (rischio inversione)
            ha_daily_body_pct: % del corpo HA sull'high/low (forza della candela)
        """
        default = {
            'ha_daily_bias': 'NEUTRO', 'ha_daily_colore': 'NEUTRO',
            'ha_daily_streak': 0, 'ha_daily_cambio': False,
            'ha_daily_su_sr': False, 'ha_daily_sr_level': 0.0,
            'ha_pullback_warn': False, 'ha_daily_body_pct': 0.0,
        }
        try:
            import pandas as pd
            from core.asset_list import get_human_name

            symbol_ccxt = get_human_name(ticker)
            # Scarica 30 candele daily — sufficiente per HA stabile
            ohlcv = self._safe_fetch('fetch_ohlcv', symbol_ccxt, timeframe='1440', limit=30)
            if not ohlcv or len(ohlcv) < 5:
                return default

            df = pd.DataFrame(ohlcv, columns=['ts', 'open', 'high', 'low', 'close', 'volume'])
            for col in ['open', 'high', 'low', 'close']:
                df[col] = pd.to_numeric(df[col], errors='coerce')
            df = df.dropna().reset_index(drop=True)

            # ── Calcolo Heikin-Ashi ──────────────────────────────────────────
            ha_close = (df['open'] + df['high'] + df['low'] + df['close']) / 4
            ha_open  = ha_close.copy()
            ha_open.iloc[0] = (df['open'].iloc[0] + df['close'].iloc[0]) / 2
            for i in range(1, len(df)):
                ha_open.iloc[i] = (ha_open.iloc[i-1] + ha_close.iloc[i-1]) / 2
            ha_high = pd.concat([df['high'], ha_open, ha_close], axis=1).max(axis=1)
            ha_low  = pd.concat([df['low'],  ha_open, ha_close], axis=1).min(axis=1)

            # Colori: VERDE se ha_close > ha_open
            colori = ['VERDE' if ha_close.iloc[i] > ha_open.iloc[i] else 'ROSSO'
                      for i in range(len(df))]

            # Ultima candela CHIUSA = penultima del df
            # (l'ultima è quella in formazione — non conta)
            idx_chiusa   = len(colori) - 2   # ultima chiusa
            idx_precedente = len(colori) - 3  # quella prima

            if idx_chiusa < 1:
                return default

            colore_chiuso    = colori[idx_chiusa]
            colore_precedente = colori[idx_precedente]

            # ── Streak: candele consecutive stesso colore ────────────────────
            streak = 1
            for i in range(idx_chiusa - 1, -1, -1):
                if colori[i] == colore_chiuso:
                    streak += 1
                else:
                    break

            # ── Cambio di colore ─────────────────────────────────────────────
            cambio = (colore_chiuso != colore_precedente)

            # ── Forza della candela (body / range) ───────────────────────────
            _body = abs(ha_close.iloc[idx_chiusa] - ha_open.iloc[idx_chiusa])
            _range = ha_high.iloc[idx_chiusa] - ha_low.iloc[idx_chiusa]
            body_pct = round(_body / _range * 100, 1) if _range > 0 else 0.0

            # ── Cambio su S/R daily ──────────────────────────────────────────
            prezzo_cambio = float(ha_close.iloc[idx_chiusa])
            su_sr    = False
            sr_level = 0.0
            soglia   = 0.015  # ±1.5%

            if cambio and sr_data:
                tutti_livelli = (
                    [r['prezzo'] for r in sr_data.get('sr_resistenze', [])] +
                    [s['prezzo'] for s in sr_data.get('sr_supporti', [])]
                )
                for lvl in tutti_livelli:
                    if lvl > 0 and abs(prezzo_cambio - lvl) / lvl <= soglia:
                        su_sr    = True
                        sr_level = lvl
                        break

            # ── Pullback warning ─────────────────────────────────────────────
            # HA verde (trend up) ma prezzo vicino a supporto → rischio rottura
            prezzo_attuale = float(df['close'].iloc[-1])
            pullback_warn  = False
            if colore_chiuso == 'VERDE' and sr_data:
                supporti = sr_data.get('sr_supporti', [])
                for s in supporti:
                    dist = (prezzo_attuale - s['prezzo']) / prezzo_attuale
                    if 0 <= dist <= 0.008:  # entro 0.8% dal supporto
                        pullback_warn = True
                        break

            # ── Bias operativo ───────────────────────────────────────────────
            bias = 'LONG' if colore_chiuso == 'VERDE' else 'SHORT'

            result = {
                'ha_daily_bias':     bias,
                'ha_daily_colore':   colore_chiuso,
                'ha_daily_streak':   streak,
                'ha_daily_cambio':   cambio,
                'ha_daily_su_sr':    su_sr,
                'ha_daily_sr_level': sr_level,
                'ha_pullback_warn':  pullback_warn,
                'ha_daily_body_pct': body_pct,
            }

            # Log sintetico
            cambio_tag  = f" 🔄 CAMBIO TREND" if cambio else ""
            sr_tag      = f" su S/R {sr_level:.2f}" if su_sr else ""
            warn_tag    = f" ⚠️ PULLBACK WARN" if pullback_warn else ""
            self.logger.info(
                f"🕯️ [{ticker}] HA Daily: {colore_chiuso} "
                f"(streak={streak} body={body_pct:.0f}%)"
                f"{cambio_tag}{sr_tag}{warn_tag}"
            )
            return result

        except Exception as e:
            _err.capture(e, "_calcola_ha_daily", {"module": "EngineLA"})
            self.logger.debug(f"_calcola_ha_daily {ticker}: {e}")
            return default


    def _calcola_ema_struttura(self, ticker: str) -> dict:
        """
        Calcola la struttura EMA su 3 timeframe (15m, 1h, 4h).

        Per ogni TF calcola EMA 20, 50, 200 e determina:
        - Allineamento rialzista: EMA20 > EMA50 > EMA200
        - Allineamento ribassista: EMA20 < EMA50 < EMA200
        - Compressione: EMA20 ≈ EMA50 ≈ EMA200 (mercato laterale)

        Fornisce anche un punteggio di confluenza multi-TF:
        - Se tutti e 3 i TF sono rialzisti → confluence_score = +3
        - Se tutti e 3 i TF sono ribassisti → confluence_score = -3
        - Misto → valore intermedio

        Questo punteggio è molto più affidabile del solo Hurst per
        determinare se vale la pena seguire il trend.
        """
        default = {
            'ema_15m': {'ema20': 0.0, 'ema50': 0.0, 'ema200': 0.0, 'allineamento': 'NEUTRO'},
            'ema_1h':  {'ema20': 0.0, 'ema50': 0.0, 'ema200': 0.0, 'allineamento': 'NEUTRO'},
            'ema_4h':  {'ema20': 0.0, 'ema50': 0.0, 'ema200': 0.0, 'allineamento': 'NEUTRO'},
            'ema_confluence_score': 0,
            'ema_trend_dominante': 'NEUTRO',
        }
        try:
            from core.asset_list import get_human_name
            symbol_ccxt = get_human_name(ticker)

            tf_configs = [
                ('ema_15m', '15',  220),   # 220 candele per avere EMA200 significativa
                ('ema_1h',  '60',  220),
                ('ema_4h',  '240', 220),
            ]

            result      = {}
            score_totale = 0

            for key, tf_kraken, limit in tf_configs:
                try:
                    ohlcv = self._safe_fetch(
                        'fetch_ohlcv', symbol_ccxt,
                        timeframe=tf_kraken, limit=limit
                    )
                    if not ohlcv or len(ohlcv) < 50:
                        result[key] = default[key]
                        continue

                    df_tf = pd.DataFrame(
                        ohlcv,
                        columns=['ts', 'open', 'high', 'low', 'close', 'volume']
                    )
                    df_tf['close'] = pd.to_numeric(df_tf['close'], errors='coerce')
                    df_tf = df_tf.dropna(subset=['close'])

                    ema20  = float(df_tf['close'].ewm(span=20,  adjust=False).mean().iloc[-1])
                    ema50  = float(df_tf['close'].ewm(span=50,  adjust=False).mean().iloc[-1])
                    ema200 = float(df_tf['close'].ewm(span=200, adjust=False).mean().iloc[-1]) \
                             if len(df_tf) >= 200 else 0.0

                    # Determina allineamento
                    if ema200 > 0:
                        if ema20 > ema50 > ema200:
                            allineamento = 'RIALZISTA'
                            score_totale += 1
                        elif ema20 < ema50 < ema200:
                            allineamento = 'RIBASSISTA'
                            score_totale -= 1
                        else:
                            # Compressione: tutte e 3 entro 0.5% l'una dall'altra
                            spread = (max(ema20, ema50, ema200) - min(ema20, ema50, ema200))
                            if spread / ema50 < 0.005:
                                allineamento = 'COMPRESSO'
                            else:
                                allineamento = 'MISTO'
                    else:
                        # Senza EMA200 usiamo solo 20 vs 50
                        if ema20 > ema50:
                            allineamento = 'RIALZISTA'
                            score_totale += 1
                        elif ema20 < ema50:
                            allineamento = 'RIBASSISTA'
                            score_totale -= 1
                        else:
                            allineamento = 'NEUTRO'

                    result[key] = {
                        'ema20':        round(ema20,  6),
                        'ema50':        round(ema50,  6),
                        'ema200':       round(ema200, 6),
                        'allineamento': allineamento,
                    }

                except Exception as e_tf:
                    _err.capture(e_tf, "_calcola_ema_struttura", {"module": "EngineLA"})
                    self.logger.debug(f"Errore EMA {key} per {ticker}: {e_tf}")
                    result[key] = default[key]

            # Punteggio confluenza: -3 (ribassista forte) → +3 (rialzista forte)
            result['ema_confluence_score'] = score_totale
            if score_totale >= 2:
                result['ema_trend_dominante'] = 'RIALZISTA'
            elif score_totale <= -2:
                result['ema_trend_dominante'] = 'RIBASSISTA'
            else:
                result['ema_trend_dominante'] = 'NEUTRO'

            return result

        except Exception as e:
            _err.capture(e, "_calcola_ema_struttura", {"module": "EngineLA"})
            self.logger.debug(f"Errore _calcola_ema_struttura {ticker}: {e}")
            return default

    # ══════════════════════════════════════════════════════════════════════
    #  (fine Step 2)
    # ══════════════════════════════════════════════════════════════════════

    # ══════════════════════════════════════════════════════════════════════
    #  STEP 3 — METODOLOGIA ISTITUZIONALE: FVG STORICI, ORDER BLOCKS,
    #            BREAKER BLOCKS
    # ══════════════════════════════════════════════════════════════════════

    def _calcola_fvg_storici(self, ticker: str) -> dict:
        """
        Identifica tutti i Fair Value Gap (FVG) non ancora colmati su H1 e H4.

        Un FVG bullish esiste tra candela[i-1].high e candela[i+1].low,
        quando c'è un gap verso l'alto (candela[i] è una candela esplosiva rialzista).
        Un FVG bearish è il contrario.

        Il prezzo tende a tornare a "colmare" questi gap — sono zone di inefficienza
        che il mercato non ha prezzato correttamente durante il movimento rapido.
        Un FVG si considera "colmato" quando il prezzo entra nella zona del gap.

        Restituisce:
            fvg_bull_h1: list[dict] con {top, bottom, size_perc, colmato}
            fvg_bear_h1: list[dict]
            fvg_bull_h4: list[dict]
            fvg_bear_h4: list[dict]
            fvg_attivi_count: int  — totale FVG non colmati
            fvg_bull_piu_vicino: float — bottom del FVG bull più vicino sotto
            fvg_bear_piu_vicino: float — top del FVG bear più vicino sopra
        """
        default = {
            'fvg_bull_h1': [], 'fvg_bear_h1': [],
            'fvg_bull_h4': [], 'fvg_bear_h4': [],
            'fvg_attivi_count': 0,
            'fvg_bull_piu_vicino': 0.0,
            'fvg_bear_piu_vicino': 0.0,
        }
        try:
            from core.asset_list import get_human_name
            symbol_ccxt = get_human_name(ticker)

            def trova_fvg(df: pd.DataFrame, prezzo_attuale: float) -> tuple:
                """Trova tutti i FVG non colmati nel DataFrame."""
                bull_gaps = []
                bear_gaps = []

                for i in range(1, len(df) - 1):
                    prev_high = float(df['high'].iloc[i - 1])
                    prev_low  = float(df['low'].iloc[i - 1])
                    next_high = float(df['high'].iloc[i + 1])
                    next_low  = float(df['low'].iloc[i + 1])
                    mid_close = float(df['close'].iloc[i])
                    mid_open  = float(df['open'].iloc[i])

                    # FVG Bullish: gap tra high[i-1] e low[i+1]
                    if prev_high < next_low:
                        gap_top    = next_low
                        gap_bottom = prev_high
                        size_perc  = (gap_top - gap_bottom) / gap_bottom * 100 if gap_bottom > 0 else 0

                        # Minimo 0.1% per essere significativo (filtra rumore)
                        if size_perc >= 0.1:
                            # Controlla se già colmato (prezzo è sceso dentro il gap)
                            colmato = prezzo_attuale <= gap_top and prezzo_attuale >= gap_bottom
                            bull_gaps.append({
                                'top':        round(gap_top,    6),
                                'bottom':     round(gap_bottom, 6),
                                'size_perc':  round(size_perc,  3),
                                'colmato':    colmato,
                                'candle_idx': i,
                            })

                    # FVG Bearish: gap tra low[i-1] e high[i+1]
                    if prev_low > next_high:
                        gap_top    = prev_low
                        gap_bottom = next_high
                        size_perc  = (gap_top - gap_bottom) / gap_bottom * 100 if gap_bottom > 0 else 0

                        if size_perc >= 0.1:
                            colmato = prezzo_attuale >= gap_bottom and prezzo_attuale <= gap_top
                            bear_gaps.append({
                                'top':        round(gap_top,    6),
                                'bottom':     round(gap_bottom, 6),
                                'size_perc':  round(size_perc,  3),
                                'colmato':    colmato,
                                'candle_idx': i,
                            })

                # Restituisce solo quelli non colmati, più recenti prima
                bull_attivi = [g for g in bull_gaps if not g['colmato']][-5:]
                bear_attivi = [g for g in bear_gaps if not g['colmato']][-5:]
                return bull_attivi, bear_attivi

            result      = dict(default)
            prezzo_live = float(self.get_current_price(ticker) or 0)

            for tf_label, tf_kraken, limit in [('h1', '60', 100), ('h4', '240', 60)]:
                try:
                    ohlcv = self._safe_fetch(
                        'fetch_ohlcv', symbol_ccxt,
                        timeframe=tf_kraken, limit=limit
                    )
                    if not ohlcv or len(ohlcv) < 10:
                        continue

                    df_tf = pd.DataFrame(
                        ohlcv,
                        columns=['ts', 'open', 'high', 'low', 'close', 'volume']
                    )
                    for col in ['open', 'high', 'low', 'close']:
                        df_tf[col] = pd.to_numeric(df_tf[col], errors='coerce')
                    df_tf = df_tf.dropna()

                    bull_g, bear_g = trova_fvg(df_tf, prezzo_live)
                    result[f'fvg_bull_{tf_label}'] = bull_g
                    result[f'fvg_bear_{tf_label}'] = bear_g

                except Exception as e_tf:
                    _err.capture(e_tf, "_calcola_fvg_storici", {"module": "EngineLA"})
                    self.logger.debug(f"FVG {tf_label} skip {ticker}: {e_tf}")

            # Conteggio totale FVG attivi
            result['fvg_attivi_count'] = sum(
                len(result[k]) for k in ['fvg_bull_h1','fvg_bear_h1','fvg_bull_h4','fvg_bear_h4']
            )

            # FVG più vicino al prezzo attuale
            if prezzo_live > 0:
                tutti_bull = result['fvg_bull_h1'] + result['fvg_bull_h4']
                tutti_bear = result['fvg_bear_h1'] + result['fvg_bear_h4']

                # FVG bull più vicino sotto il prezzo (zona di supporto FVG)
                bull_sotto = [g for g in tutti_bull if g['top'] < prezzo_live]
                result['fvg_bull_piu_vicino'] = (
                    round(max(bull_sotto, key=lambda g: g['top'])['top'], 6)
                    if bull_sotto else 0.0
                )

                # FVG bear più vicino sopra il prezzo (zona di resistenza FVG)
                bear_sopra = [g for g in tutti_bear if g['bottom'] > prezzo_live]
                result['fvg_bear_piu_vicino'] = (
                    round(min(bear_sopra, key=lambda g: g['bottom'])['bottom'], 6)
                    if bear_sopra else 0.0
                )

            return result

        except Exception as e:
            _err.capture(e, "_calcola_fvg_storici", {"module": "EngineLA"})
            self.logger.debug(f"Errore _calcola_fvg_storici {ticker}: {e}")
            return default

    def _calcola_order_blocks(self, ticker: str) -> dict:
        """
        Identifica gli Order Blocks (OB) istituzionali su H1 e H4.

        Un Order Block è l'ultima candela prima di un movimento impulsivo significativo.
        Logica:
          - OB Bullish: ultima candela bearish prima di un rally rialzista forte
            (il movimento successivo supera il precedente swing high)
          - OB Bearish: ultima candela bullish prima di un crollo ribassista forte

        Gli istituzionali tornano a riempire gli ordini lasciati in queste zone —
        per questo il prezzo le rispetta come supporto/resistenza.
        La "forza" del movimento successivo determina la qualità dell'OB.

        Restituisce:
            ob_bull_h1: list[dict] con {top, bottom, forza, usato}
            ob_bear_h1: list[dict]
            ob_bull_h4: list[dict]
            ob_bear_h4: list[dict]
            ob_bull_piu_vicino: float — OB bull più vicino sotto il prezzo
            ob_bear_piu_vicino: float — OB bear più vicino sopra il prezzo
        """
        default = {
            'ob_bull_h1': [], 'ob_bear_h1': [],
            'ob_bull_h4': [], 'ob_bear_h4': [],
            'ob_bull_piu_vicino': 0.0,
            'ob_bear_piu_vicino': 0.0,
        }
        try:
            from core.asset_list import get_human_name
            symbol_ccxt = get_human_name(ticker)

            def trova_ob(df: pd.DataFrame, prezzo_attuale: float,
                        soglia_impulso: float = 0.008) -> tuple:
                """
                soglia_impulso: il movimento successivo deve essere almeno
                questo % per qualificare come impulso istituzionale (default 0.8%).
                """
                bull_obs = []
                bear_obs = []

                for i in range(1, len(df) - 3):
                    o  = float(df['open'].iloc[i])
                    h  = float(df['high'].iloc[i])
                    l  = float(df['low'].iloc[i])
                    c  = float(df['close'].iloc[i])

                    # Guarda il movimento nelle 3 candele successive
                    future_high = df['high'].iloc[i+1:i+4].max()
                    future_low  = df['low'].iloc[i+1:i+4].min()

                    # OB Bullish: candela bearish (c < o) seguita da rally
                    if c < o:
                        rally = (float(future_high) - h) / h if h > 0 else 0
                        if rally >= soglia_impulso:
                            # L'OB è la zona tra open e low della candela bearish
                            ob_top    = max(o, h)
                            ob_bottom = l
                            # "usato": il prezzo è già rientrato nella zona
                            usato = prezzo_attuale <= ob_top and prezzo_attuale >= ob_bottom
                            bull_obs.append({
                                'top':    round(ob_top,    6),
                                'bottom': round(ob_bottom, 6),
                                'forza':  round(rally * 100, 2),
                                'usato':  usato,
                            })

                    # OB Bearish: candela bullish (c > o) seguita da crollo
                    if c > o:
                        crollo = (l - float(future_low)) / l if l > 0 else 0
                        if crollo >= soglia_impulso:
                            ob_top    = h
                            ob_bottom = min(o, l)
                            usato = prezzo_attuale >= ob_bottom and prezzo_attuale <= ob_top
                            bear_obs.append({
                                'top':    round(ob_top,    6),
                                'bottom': round(ob_bottom, 6),
                                'forza':  round(crollo * 100, 2),
                                'usato':  usato,
                            })

                # Solo OB non ancora "usati", ordinati per forza decrescente
                bull_validi = sorted(
                    [ob for ob in bull_obs if not ob['usato']],
                    key=lambda x: x['forza'], reverse=True
                )[:4]
                bear_validi = sorted(
                    [ob for ob in bear_obs if not ob['usato']],
                    key=lambda x: x['forza'], reverse=True
                )[:4]

                return bull_validi, bear_validi

            result      = dict(default)
            prezzo_live = float(self.get_current_price(ticker) or 0)

            for tf_label, tf_kraken, limit, soglia in [
                ('h1', '60',  100, 0.006),   # H1: soglia 0.6%
                ('h4', '240',  60, 0.012),   # H4: soglia 1.2% (movimenti più ampi)
            ]:
                try:
                    ohlcv = self._safe_fetch(
                        'fetch_ohlcv', symbol_ccxt,
                        timeframe=tf_kraken, limit=limit
                    )
                    if not ohlcv or len(ohlcv) < 10:
                        continue

                    df_tf = pd.DataFrame(
                        ohlcv,
                        columns=['ts', 'open', 'high', 'low', 'close', 'volume']
                    )
                    for col in ['open', 'high', 'low', 'close']:
                        df_tf[col] = pd.to_numeric(df_tf[col], errors='coerce')
                    df_tf = df_tf.dropna()

                    bull_ob, bear_ob = trova_ob(df_tf, prezzo_live, soglia)
                    result[f'ob_bull_{tf_label}'] = bull_ob
                    result[f'ob_bear_{tf_label}'] = bear_ob

                except Exception as e_tf:
                    _err.capture(e_tf, "_calcola_order_blocks", {"module": "EngineLA"})
                    self.logger.debug(f"OB {tf_label} skip {ticker}: {e_tf}")

            # OB più vicino al prezzo
            if prezzo_live > 0:
                tutti_bull_ob = result['ob_bull_h1'] + result['ob_bull_h4']
                tutti_bear_ob = result['ob_bear_h1'] + result['ob_bear_h4']

                bull_sotto = [ob for ob in tutti_bull_ob if ob['top'] < prezzo_live]
                result['ob_bull_piu_vicino'] = (
                    round(max(bull_sotto, key=lambda x: x['top'])['top'], 6)
                    if bull_sotto else 0.0
                )

                bear_sopra = [ob for ob in tutti_bear_ob if ob['bottom'] > prezzo_live]
                result['ob_bear_piu_vicino'] = (
                    round(min(bear_sopra, key=lambda x: x['bottom'])['bottom'], 6)
                    if bear_sopra else 0.0
                )

            return result

        except Exception as e:
            _err.capture(e, "_calcola_order_blocks", {"module": "EngineLA"})
            self.logger.debug(f"Errore _calcola_order_blocks {ticker}: {e}")
            return default

    def _calcola_breaker_blocks(self, ticker: str,
                                 ob_bull: list, ob_bear: list) -> dict:
        """
        Identifica i Breaker Blocks — Order Block che il prezzo ha violato.

        Quando il prezzo rompe un Order Block bullish verso il basso,
        quell'OB si "inverte" e diventa resistenza (breaker bearish).
        Viceversa per gli OB bearish rotti verso l'alto.

        I Breaker Blocks sono tra i segnali più affidabili della metodologia ICT
        perché confermano un cambio strutturale nel bias istituzionale.

        Accetta le liste di OB già calcolate da _calcola_order_blocks per
        evitare chiamate API duplicate.

        Restituisce:
            breaker_bull: list[dict] — ex-OB bearish rotto = ora supporto
            breaker_bear: list[dict] — ex-OB bullish rotto = ora resistenza
            breaker_piu_vicino_sopra: float
            breaker_piu_vicino_sotto: float
        """
        default = {
            'breaker_bull': [],
            'breaker_bear': [],
            'breaker_piu_vicino_sopra': 0.0,
            'breaker_piu_vicino_sotto': 0.0,
        }
        try:
            prezzo_live = float(self.get_current_price(ticker) or 0)
            if prezzo_live <= 0:
                return default

            breaker_bull = []
            breaker_bear = []

            # Un OB bullish diventa breaker bearish se il prezzo è sceso
            # sotto il suo bottom — l'istituzionale ha liquidato i long
            for ob in ob_bull:
                if prezzo_live < ob.get('bottom', 0):
                    breaker_bear.append({
                        'top':    ob['top'],
                        'bottom': ob['bottom'],
                        'tipo':   'EX_BULL_OB',
                        'forza':  ob.get('forza', 0),
                    })

            # Un OB bearish diventa breaker bullish se il prezzo è salito
            # sopra il suo top — l'istituzionale ha liquidato gli short
            for ob in ob_bear:
                if prezzo_live > ob.get('top', float('inf')):
                    breaker_bull.append({
                        'top':    ob['top'],
                        'bottom': ob['bottom'],
                        'tipo':   'EX_BEAR_OB',
                        'forza':  ob.get('forza', 0),
                    })

            # Breaker più vicino sopra e sotto
            bb_sopra = [b for b in breaker_bear if b['bottom'] > prezzo_live]
            bb_sotto = [b for b in breaker_bull if b['top']    < prezzo_live]

            return {
                'breaker_bull':               breaker_bull[:3],
                'breaker_bear':               breaker_bear[:3],
                'breaker_piu_vicino_sopra':   round(
                    min(bb_sopra, key=lambda x: x['bottom'])['bottom'], 6
                ) if bb_sopra else 0.0,
                'breaker_piu_vicino_sotto':   round(
                    max(bb_sotto, key=lambda x: x['top'])['top'], 6
                ) if bb_sotto else 0.0,
            }

        except Exception as e:
            _err.capture(e, "_calcola_breaker_blocks", {"module": "EngineLA"})
            self.logger.debug(f"Errore _calcola_breaker_blocks {ticker}: {e}")
            return default

    # ══════════════════════════════════════════════════════════════════════
    #  (fine Step 3)
    # ══════════════════════════════════════════════════════════════════════

    # ══════════════════════════════════════════════════════════════════════
    #  STEP 4 — BOS/CHoCH + SWEEP HISTORY
    # ══════════════════════════════════════════════════════════════════════

    def _calcola_bos_choch(self, ticker: str) -> dict:
        """
        Identifica Break of Structure (BOS) e Change of Character (CHoCH) su H1.

        Struttura del mercato:
          - Uptrend: HH (Higher High) + HL (Higher Low) in sequenza
          - Downtrend: LH (Lower High) + LL (Lower Low) in sequenza

        BOS (Break of Structure):
          - In uptrend: il prezzo rompe l'ultimo HH → conferma continuazione
          - In downtrend: il prezzo rompe l'ultimo LL → conferma continuazione

        CHoCH (Change of Character):
          - In uptrend: il prezzo rompe l'ultimo HL verso il basso → ATTENZIONE,
            possibile inversione (prima rottura strutturale)
          - In downtrend: il prezzo rompe l'ultimo LH verso l'alto → possibile
            inversione rialzista

        La differenza chiave:
          BOS = movimento CON il trend (conferma)
          CHoCH = movimento CONTRO il trend (warning inversione)

        Restituisce:
            struttura_h1:   "UPTREND" | "DOWNTREND" | "LATERALE"
            ultimo_bos:     "BULLISH" | "BEARISH" | "NESSUNO"
            ultimo_choch:   "BULLISH" | "BEARISH" | "NESSUNO"
            bos_level:      float — livello dove è avvenuto l'ultimo BOS
            choch_level:    float — livello dove è avvenuto l'ultimo CHoCH
            swing_sequence: list[str] — es. ["HH","HL","HH","HL"] ultimi 6
        """
        default = {
            'struttura_h1':  'LATERALE',
            'ultimo_bos':    'NESSUNO',
            'ultimo_choch':  'NESSUNO',
            'bos_level':     0.0,
            'choch_level':   0.0,
            'swing_sequence': [],
        }
        try:
            from core.asset_list import get_human_name
            symbol_ccxt = get_human_name(ticker)

            ohlcv = self._safe_fetch(
                'fetch_ohlcv', symbol_ccxt,
                timeframe='60', limit=100
            )
            if not ohlcv or len(ohlcv) < 20:
                return default

            df = pd.DataFrame(
                ohlcv,
                columns=['ts', 'open', 'high', 'low', 'close', 'volume']
            )
            for col in ['high', 'low', 'close']:
                df[col] = pd.to_numeric(df[col], errors='coerce')
            df = df.dropna()

            # ── Trova swing high/low con finestra 3 ──────────────────────
            n = 3
            swing_highs = []   # (idx, price)
            swing_lows  = []   # (idx, price)

            for i in range(n, len(df) - n):
                h = float(df['high'].iloc[i])
                l = float(df['low'].iloc[i])

                if all(h > float(df['high'].iloc[i - j]) for j in range(1, n + 1)) and \
                   all(h > float(df['high'].iloc[i + j]) for j in range(1, n + 1)):
                    swing_highs.append((i, h))

                if all(l < float(df['low'].iloc[i - j]) for j in range(1, n + 1)) and \
                   all(l < float(df['low'].iloc[i + j]) for j in range(1, n + 1)):
                    swing_lows.append((i, l))

            if len(swing_highs) < 2 or len(swing_lows) < 2:
                return default

            # ── Classifica la struttura ───────────────────────────────────
            # Guarda gli ultimi 4 swing high e 4 swing low
            sh_recenti = swing_highs[-4:]
            sl_recenti = swing_lows[-4:]

            hh_count = sum(
                1 for i in range(1, len(sh_recenti))
                if sh_recenti[i][1] > sh_recenti[i - 1][1]
            )
            hl_count = sum(
                1 for i in range(1, len(sl_recenti))
                if sl_recenti[i][1] > sl_recenti[i - 1][1]
            )
            lh_count = sum(
                1 for i in range(1, len(sh_recenti))
                if sh_recenti[i][1] < sh_recenti[i - 1][1]
            )
            ll_count = sum(
                1 for i in range(1, len(sl_recenti))
                if sl_recenti[i][1] < sl_recenti[i - 1][1]
            )

            if hh_count >= 2 and hl_count >= 2:
                struttura = 'UPTREND'
            elif lh_count >= 2 and ll_count >= 2:
                struttura = 'DOWNTREND'
            else:
                struttura = 'LATERALE'

            # ── Sequenza swing (etichette) ────────────────────────────────
            sequenza = []
            for i in range(1, min(4, len(sh_recenti))):
                sequenza.append('HH' if sh_recenti[i][1] > sh_recenti[i-1][1] else 'LH')
            for i in range(1, min(4, len(sl_recenti))):
                sequenza.append('HL' if sl_recenti[i][1] > sl_recenti[i-1][1] else 'LL')

            # ── Identifica BOS e CHoCH ────────────────────────────────────
            prezzo_attuale = float(df['close'].iloc[-1])
            ultimo_sh = sh_recenti[-1][1] if sh_recenti else 0
            ultimo_sl = sl_recenti[-1][1] if sl_recenti else 0
            prev_sh   = sh_recenti[-2][1] if len(sh_recenti) >= 2 else 0
            prev_sl   = sl_recenti[-2][1] if len(sl_recenti) >= 2 else 0

            bos      = 'NESSUNO'
            choch    = 'NESSUNO'
            bos_lv   = 0.0
            choch_lv = 0.0

            if struttura == 'UPTREND':
                # BOS bullish: prezzo rompe sopra l'ultimo HH
                if prezzo_attuale > ultimo_sh and prev_sh > 0:
                    bos    = 'BULLISH'
                    bos_lv = round(ultimo_sh, 6)
                # CHoCH: prezzo rompe sotto l'ultimo HL (warning inversione)
                if prezzo_attuale < ultimo_sl and prev_sl > 0:
                    choch    = 'BEARISH'
                    choch_lv = round(ultimo_sl, 6)

            elif struttura == 'DOWNTREND':
                # BOS bearish: prezzo rompe sotto l'ultimo LL
                if prezzo_attuale < ultimo_sl and prev_sl > 0:
                    bos    = 'BEARISH'
                    bos_lv = round(ultimo_sl, 6)
                # CHoCH: prezzo rompe sopra l'ultimo LH (warning inversione)
                if prezzo_attuale > ultimo_sh and prev_sh > 0:
                    choch    = 'BULLISH'
                    choch_lv = round(ultimo_sh, 6)

            return {
                'struttura_h1':  struttura,
                'ultimo_bos':    bos,
                'ultimo_choch':  choch,
                'bos_level':     bos_lv,
                'choch_level':   choch_lv,
                'swing_sequence': sequenza[:6],
            }

        except Exception as e:
            _err.capture(e, "_calcola_bos_choch", {"module": "EngineLA"})
            self.logger.debug(f"Errore _calcola_bos_choch {ticker}: {e}")
            return default

    def _aggiorna_sweep_history(self, ticker: str,
                                 muro_sup: float, muro_res: float,
                                 prezzo: float) -> dict:
        """
        Traccia se i muri di supporto/resistenza correnti sono stati già
        "sweepati" (violati e poi recuperati) in passato recente.

        Un livello sweepato è un segnale molto forte:
          - Se il supporto è stato sweepato e poi recuperato → strong support
            (gli istituzionali hanno cacciato gli stop e poi comprato)
          - Se la resistenza è stata sweepata e poi rigettata → strong resistance

        Usa la wall_history già presente in Engine per tracciare la persistenza
        e aggiunge un contatore di quante volte il livello è stato testato.

        Restituisce:
            supporto_sweepato:   bool — supporto corrente già violato e recuperato
            resistenza_sweepata: bool — resistenza corrente già violata e recuperata
            supporto_test_count: int  — quante volte il supporto è stato testato
            resistenza_test_count: int
            supporto_score:      float — 0.0-1.0, quanto è forte il supporto
            resistenza_score:    float — 0.0-1.0, quanto è forte la resistenza
        """
        default = {
            'supporto_sweepato':    False,
            'resistenza_sweepata':  False,
            'supporto_test_count':  0,
            'resistenza_test_count': 0,
            'supporto_score':       0.0,
            'resistenza_score':     0.0,
        }
        try:
            if not hasattr(self, '_sweep_history'):
                self._sweep_history = {}   # {ticker: {'sup': [], 'res': []}}

            if ticker not in self._sweep_history:
                self._sweep_history[ticker] = {'sup': [], 'res': []}

            now   = time.time()
            hist  = self._sweep_history[ticker]
            soglia = 0.003   # 0.3% di tolleranza per "stesso livello"

            def stesso_livello(a: float, b: float) -> bool:
                if a <= 0 or b <= 0:
                    return False
                return abs(a - b) / max(a, b) < soglia

            # Aggiorna il tracciamento del supporto
            if muro_sup > 0:
                # Trova se abbiamo già visto questo supporto
                esistente_s = next(
                    (e for e in hist['sup'] if stesso_livello(e['price'], muro_sup)),
                    None
                )
                if esistente_s is None:
                    hist['sup'].append({
                        'price': muro_sup, 'first_seen': now,
                        'test_count': 1, 'sweep_count': 0,
                        'last_price_at_test': prezzo,
                    })
                else:
                    esistente_s['test_count'] += 1
                    # Sweep: prezzo ha violato il supporto ma è rimasto vicino
                    if prezzo < muro_sup * (1 - soglia):
                        esistente_s['sweep_count'] += 1
                    esistente_s['last_price_at_test'] = prezzo

            # Aggiorna il tracciamento della resistenza
            if muro_res > 0:
                esistente_r = next(
                    (e for e in hist['res'] if stesso_livello(e['price'], muro_res)),
                    None
                )
                if esistente_r is None:
                    hist['res'].append({
                        'price': muro_res, 'first_seen': now,
                        'test_count': 1, 'sweep_count': 0,
                        'last_price_at_test': prezzo,
                    })
                else:
                    esistente_r['test_count'] += 1
                    if prezzo > muro_res * (1 + soglia):
                        esistente_r['sweep_count'] += 1
                    esistente_r['last_price_at_test'] = prezzo

            # Pulizia: mantieni solo gli ultimi 20 livelli per tipo
            hist['sup'] = hist['sup'][-20:]
            hist['res'] = hist['res'][-20:]

            # Recupera stats per i livelli correnti
            info_sup = next(
                (e for e in hist['sup'] if stesso_livello(e['price'], muro_sup)),
                None
            )
            info_res = next(
                (e for e in hist['res'] if stesso_livello(e['price'], muro_res)),
                None
            )

            def calcola_score(info: dict) -> float:
                """Score 0-1 basato su test count e sweep count."""
                if info is None:
                    return 0.0
                tc = info.get('test_count', 0)
                sc = info.get('sweep_count', 0)
                # Più test = livello più forte; sweep aggiunge forza (liquidità cacciata)
                base  = min(tc / 5.0, 1.0)       # normalizza su 5 test
                bonus = min(sc * 0.2, 0.4)        # max +0.4 per sweep
                return round(min(base + bonus, 1.0), 2)

            return {
                'supporto_sweepato':    bool(info_sup and info_sup.get('sweep_count', 0) > 0),
                'resistenza_sweepata':  bool(info_res and info_res.get('sweep_count', 0) > 0),
                'supporto_test_count':  int(info_sup['test_count']) if info_sup else 0,
                'resistenza_test_count': int(info_res['test_count']) if info_res else 0,
                'supporto_score':       calcola_score(info_sup),
                'resistenza_score':     calcola_score(info_res),
            }

        except Exception as e:
            _err.capture(e, "_aggiorna_sweep_history", {"module": "EngineLA"})
            self.logger.debug(f"Errore _aggiorna_sweep_history {ticker}: {e}")
            return default

    # ══════════════════════════════════════════════════════════════════════
    #  (fine Step 4)
    # ══════════════════════════════════════════════════════════════════════

    def _calcola_volume_profile(self, df, bins=50, ticker=None):
        """
        Calcola POC, VAH, VAL sul Volume Profile.
        FIX: se viene passato il ticker, scarica 7 giorni di candele 1h per
        un profilo molto più significativo (vs le 25h precedenti su 15m).
        Aggiunge anche VWAP ancorato all'inizio della finestra.
        """
        try:
            # Se abbiamo il ticker, proviamo a usare dati 1h per 7 giorni
            df_vp = df  # default: usa df passato
            if ticker is not None:
                try:
                    from core.asset_list import get_human_name
                    symbol_ccxt = get_human_name(ticker)
                    ohlcv_1h = self._safe_fetch(
                        'fetch_ohlcv', symbol_ccxt,
                        timeframe='60', limit=168  # 7 giorni × 24h
                    )
                    if ohlcv_1h and len(ohlcv_1h) > 20:
                        df_vp = pd.DataFrame(
                            ohlcv_1h,
                            columns=['ts', 'open', 'high', 'low', 'close', 'volume']
                        )
                        for col in ['open', 'high', 'low', 'close', 'volume']:
                            df_vp[col] = pd.to_numeric(df_vp[col], errors='coerce')
                        df_vp = df_vp.dropna()
                except Exception:
                    df_vp = df  # fallback al df originale

            if df_vp.empty or 'close' not in df_vp.columns or 'volume' not in df_vp.columns:
                return {"poc": 0.0, "vah": 0.0, "val": 0.0, "vwap_anchored": 0.0}

            min_price = df_vp['low'].min()
            max_price = df_vp['high'].max()

            if min_price == max_price:
                return {
                    "poc":          min_price,
                    "vah":          min_price,
                    "val":          min_price,
                    "vwap_anchored": min_price
                }

            bin_size = (max_price - min_price) / bins

            df_vp2        = df_vp.copy()
            df_vp2['price_bin'] = (
                ((df_vp2['close'] - min_price) / bin_size).round() * bin_size + min_price
            )

            vp = df_vp2.groupby('price_bin')['volume'].sum().sort_index()

            if vp.empty:
                return {"poc": 0.0, "vah": 0.0, "val": 0.0, "vwap_anchored": 0.0}

            poc       = float(vp.idxmax())
            total_vol = vp.sum()
            va_vol    = total_vol * 0.70

            current_vol = vp[poc]
            poc_idx     = vp.index.get_loc(poc)
            upper_idx   = poc_idx + 1
            lower_idx   = poc_idx - 1

            while current_vol < va_vol and (upper_idx < len(vp) or lower_idx >= 0):
                upper_vol = vp.iloc[upper_idx] if upper_idx < len(vp) else 0
                lower_vol = vp.iloc[lower_idx] if lower_idx >= 0 else 0

                if upper_vol >= lower_vol and upper_idx < len(vp):
                    current_vol += upper_vol
                    upper_idx   += 1
                elif lower_idx >= 0:
                    current_vol += lower_vol
                    lower_idx   -= 1
                else:
                    break

            vah = float(vp.index[min(upper_idx - 1, len(vp) - 1)])
            val = float(vp.index[max(lower_idx + 1, 0)])

            # VWAP ancorato all'inizio della finestra
            typical_price = (df_vp['high'] + df_vp['low'] + df_vp['close']) / 3
            vol_sum       = df_vp['volume'].sum()
            vwap_anchored = float(
                (typical_price * df_vp['volume']).sum() / vol_sum
            ) if vol_sum > 0 else float(df_vp['close'].iloc[-1])

            # HVN e LVN — nodi ad alto e basso volume
            # HVN: bin con volume > 75° percentile (zone di equilibrio/attrazione)
            # LVN: bin con volume < 25° percentile (zone di transizione rapida)
            vol_75 = vp.quantile(0.75)
            vol_25 = vp.quantile(0.25)
            hvn_prices = [float(p) for p in vp[vp >= vol_75].index.tolist()]
            lvn_prices = [float(p) for p in vp[vp <= vol_25].index.tolist()]

            return {
                "poc":           round(poc,           4),
                "vah":           round(vah,           4),
                "val":           round(val,           4),
                "vwap_anchored": round(vwap_anchored, 4),
                "high_volume_nodes": hvn_prices[:10],  # top 10 HVN più significativi
                "low_volume_nodes":  lvn_prices[:10],  # top 10 LVN
            }

        except Exception as e:
            _err.capture(e, "_calcola_volume_profile", {"module": "EngineLA"})
            self.logger.debug(f"Errore _calcola_volume_profile: {e}")
            return {"poc": 0.0, "vah": 0.0, "val": 0.0, "vwap_anchored": 0.0}

    def _get_futures_tickers(self):
        """Recupera e cachea i tickers dei futures da Kraken."""
        now = time.time()
        if now - self._last_futures_fetch < self._futures_cache_ttl and self._futures_tickers_cache:
            return self._futures_tickers_cache
            
        url = "https://futures.kraken.com/derivatives/api/v3/tickers"
        r = self._safe_request(url)
        if r and r.get('result') == 'success':
            tickers = r.get('tickers', [])
            self._futures_tickers_cache = {t.get('symbol'): t for t in tickers if 'symbol' in t}
            self._last_futures_fetch = now
            return self._futures_tickers_cache
        return {}

    def _get_external_funding(self, ticker):
        """ Recupera il funding rate esterno (Kraken Futures). """
        try:
            from core import asset_list
            target = asset_list.get_futures_ticker(ticker)
            if not target: return 0.0
            
            tickers = self._get_futures_tickers()
            t_data = tickers.get(target)
            if t_data:
                return float(t_data.get('fundingRate', 0.0))
            
            return 0.0
        except Exception as e:
            _err.capture(e, "_get_external_funding", {"module": "EngineLA"})
            self.logger.debug(f"Errore _get_external_funding per {ticker}: {e}")
            return 0.0

    def _get_liquidations(self, ticker):
        """
        Stima le liquidazioni recenti con 3 fonti in cascata:
        1. Endpoint Kraken Futures recent-liquidations (spesso funziona)
        2. Endpoint account-summary per liquidated positions
        3. Approssimazione da trade anomali (spike volume + movimento brusco)
        """
        try:
            from core import asset_list
            target = asset_list.get_futures_ticker(ticker)
            if not target:
                return 0.0

            # Fonte 1: endpoint ufficiale
            url = f"https://futures.kraken.com/derivatives/api/v3/recentliquidations?symbol={target}"
            r = self._safe_request(url)
            if r and r.get('result') == 'success':
                liquidations = r.get('liquidations', [])
                if liquidations:
                    total = sum(float(l.get('amount', l.get('quantity', 0))) for l in liquidations)
                    if total > 0:
                        return float(round(total, 2))

            # Fonte 2: endpoint alternativo
            url2 = f"https://futures.kraken.com/derivatives/api/v3/liquidations?symbol={target}"
            r2 = self._safe_request(url2)
            if r2 and r2.get('result') == 'success':
                liquidations2 = r2.get('liquidations', r2.get('elements', []))
                if liquidations2:
                    total2 = sum(float(l.get('amount', l.get('quantity', 0))) for l in liquidations2)
                    if total2 > 0:
                        return float(round(total2, 2))

            # Fonte 3: approssimazione da Open Interest change × prezzo
            # Se l'OI scende mentre il volume sale → probabili liquidazioni
            try:
                tickers = self._get_futures_tickers()
                t_data = tickers.get(target, {})
                oi = float(t_data.get('openInterest', 0))
                last = float(t_data.get('last', 0))
                oi_change = float(t_data.get('openInterestChange', 0))
                if oi > 0 and last > 0 and oi_change < 0:
                    # OI in calo = posizioni chiuse forzatamente (stima conservativa)
                    estimated_liq = abs(oi_change) * last * 0.3
                    return float(round(estimated_liq, 2))
            except Exception:
                pass

            return 0.0

        except Exception as e:
            _err.capture(e, "_get_liquidations", {"module": "EngineLA"})
            self.logger.debug(f"Errore _get_liquidations per {ticker}: {e}")
            return 0.0

    def _get_open_interest(self, ticker):
        """ Recupera l'Open Interest (Kraken Futures). """
        try:
            from core import asset_list
            target = asset_list.get_futures_ticker(ticker)
            if not target: return 0.0
            
            tickers = self._get_futures_tickers()
            t_data = tickers.get(target)
            if t_data:
                return float(t_data.get('openInterest', 0.0))
                
            return 0.0
        except Exception as e:
            _err.capture(e, "_get_open_interest", {"module": "EngineLA"})
            self.logger.debug(f"Errore _get_open_interest per {ticker}: {e}")
            return 0.0
    
    def _get_intermarket_data(self):
        try:
            cross_data = self._safe_fetch('fetch_ticker', get_cross_ticker(CROSS_ETH_BTC))
            eth_btc_strength = float(cross_data['last'])
            
            ohlcv = self._safe_fetch('fetch_ohlcv', get_cross_ticker(CROSS_BTC_USDT), timeframe='1h', limit=24)
            df = pd.DataFrame(ohlcv, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
            avg_vol = df['volume'].mean()
            current_vol = df['volume'].iloc[-1]
            
            atr = self._calcola_atr(df)
            low_liquidity = False
            if current_vol < (avg_vol * 0.7) and atr > df['close'].std():
                low_liquidity = True

            return {
                "eth_btc_ratio": eth_btc_strength,
                "market_liquidity_warning": bool(low_liquidity),
                "relative_volume_status": float(round(current_vol / avg_vol, 2))
            }
        except:
            return {"eth_btc_ratio": 0.07, "market_liquidity_warning": False}

    def _get_put_call_ratio(self, ticker):
        """
        Sentiment long/short tramite Kraken Futures.
        Prova 3 fonti in ordine:
        1. openInterestLong / openInterestShort dal ticker futures
        2. Endpoint dedicato /api/v3/openinterest
        3. Basis (mark vs last) come proxy direzionale
        """
        try:
            from core import asset_list
            target = asset_list.get_futures_ticker(ticker)
            if not target:
                return 1.0

            tickers = self._get_futures_tickers()
            t_data  = tickers.get(target, {})

            # Fonte 1: long/short direttamente dal ticker
            oi_long  = float(t_data.get('openInterestLong',  0.0))
            oi_short = float(t_data.get('openInterestShort', 0.0))
            if oi_long > 0 and oi_short > 0:
                return float(np.clip(round(oi_long / oi_short, 3), 0.1, 10.0))

            # Fonte 2: endpoint dedicato Kraken Futures open interest
            try:
                url = f"https://futures.kraken.com/derivatives/api/v3/openinterest"
                r = self._safe_request(url)
                if r and r.get('result') == 'success':
                    for item in r.get('openInterests', []):
                        if item.get('symbol') == target:
                            long_v  = float(item.get('longNotional',  item.get('long',  0)))
                            short_v = float(item.get('shortNotional', item.get('short', 0)))
                            if long_v > 0 and short_v > 0:
                                return float(np.clip(round(long_v / short_v, 3), 0.1, 10.0))
            except Exception:
                pass

            # Fonte 3: basis mark vs last come proxy direzionale
            last = float(t_data.get('last', 0.0))
            mark = float(t_data.get('markPrice', t_data.get('mark', last)))
            if last > 0 and mark > 0:
                basis = (mark - last) / last
                if   basis >  0.002: return 1.20
                elif basis >  0.001: return 1.10
                elif basis < -0.002: return 0.80
                elif basis < -0.001: return 0.90

            return 1.0

        except Exception as e:
            _err.capture(e, "_get_put_call_ratio", {"module": "EngineLA"})
            self.logger.debug(f"Errore _get_put_call_ratio per {ticker}: {e}")
            return 1.0

    def _calcola_atr(self, df):
        if df is None or len(df) < 2:
            return 0.0
        
        high_low = df['high'] - df['low']
        high_close = abs(df['high'] - df['close'].shift())
        low_close = abs(df['low'] - df['close'].shift())
        
        tr = pd.concat([high_low, high_close, low_close], axis=1).max(axis=1)
        
        atr_series = tr.rolling(window=14, min_periods=1).mean()
        
        valore_atr = atr_series.iloc[-1]
        
        import math
        if math.isnan(valore_atr):
            return 0.0
            
        return float(valore_atr)
        
    def _get_wall_persistence(self, ticker, price, side):
        now = time.time()
        key = f"{ticker}_{side}_{price:.8f}"
        
        if len(self._wall_history) > 100:
            self._wall_history = {k: v for k, v in self._wall_history.items() 
                                  if now - v['start_time'] < 3600} 

        if key not in self._wall_history:
            for old_key, data in list(self._wall_history.items()):
                if old_key.startswith(f"{ticker}_{side}"):
                    if data["price"] > 0:
                        diff = abs(price - data["price"]) / data["price"]
                    else:
                        diff = 0.0
                    if diff < 0.0005: 
                        self._wall_history[key] = {"price": price, "start_time": data["start_time"]}
                        if old_key != key: del self._wall_history[old_key]
                        
                        duration = now - data["start_time"]
                        return round(min(duration / 600, 1.0), 2)

            self._wall_history[key] = {"price": price, "start_time": now}
            return 0.1

        duration = now - self._wall_history[key]["start_time"]
        return round(min(duration / 600, 1.0), 2)
    
    def get_open_positions_real(self):
        try:
            pos = self._safe_fetch('private_post_openpositions')
            return pos.get('result', {})
        except Exception as e:
            _err.capture(e, "get_open_positions_real", {"module": "EngineLA"})
            self.logger.error(f"  Errore critico recupero posizioni Kraken: {e}")
            return {}
            
    def _calcola_delta_footprint(self, ticker):
        try:
            ws_trades = self.ws_manager.get_trades(ticker)
            # WS-first: usa WS anche con pochi trades
            if ws_trades and len(ws_trades) >= 2:
                trades = ws_trades
            else:
                from core.asset_list import get_human_name
                asset_id = get_human_name(ticker)
                trades = self._safe_fetch('fetch_trades', asset_id, limit=50) or ws_trades or []
                
            if not trades: return 0
            buy_v = sum([float(t['amount']) for t in trades if t['side'] == 'buy'])
            sell_v = sum([float(t['amount']) for t in trades if t['side'] == 'sell'])
            return round((buy_v - sell_v) / (buy_v + sell_v), 3) if (buy_v + sell_v) > 0 else 0
        except: return 0

    def _get_macro_correlation(self):
        try:
            cross = self._safe_fetch('fetch_ticker', get_cross_ticker(CROSS_ETH_BTC))
            val = float(cross['last'])
            if val > 0.04: return "RISK_ON"   
            if val < 0.035: return "RISK_OFF" 
            return "NEUTRAL"
        except: return "NEUTRAL"
        
    def _get_hurst_exponent(self, prices):
        """
        Calcola l'esponente di Hurst usando il metodo R/S (Rescaled Range) semplificato.
        H > 0.5: Trending (Persistente)
        H < 0.5: Mean Reverting (Anti-persistente)
        H = 0.5: Random Walk
        """
        try:
            if len(prices) < 50: 
                return 0.5
            
            # Usiamo i log-returns per la stazionarietà
            returns = np.diff(np.log(prices))
            if len(returns) < 20: return 0.5

            def get_rs(data):
                m = np.mean(data)
                y = np.cumsum(data - m)
                r = np.max(y) - np.min(y)
                s = np.std(data)
                return r / s if s > 0 else 0

            # Calcoliamo R/S per diverse scale temporali
            lags = [5, 10, 20, 40, 80]
            lags = [l for l in lags if l < len(returns)]
            if len(lags) < 3: return 0.5

            rs_values = []
            for lag in lags:
                # Dividiamo in blocchi e mediamo l'R/S
                num_blocks = len(returns) // lag
                block_rs = []
                for i in range(num_blocks):
                    block = returns[i*lag : (i+1)*lag]
                    rs = get_rs(block)
                    if rs > 0: block_rs.append(rs)
                
                if block_rs:
                    rs_values.append(np.mean(block_rs))
                else:
                    rs_values.append(1e-10)

            # Regressione lineare log(R/S) vs log(lag)
            poly = np.polyfit(np.log(lags[:len(rs_values)]), np.log(rs_values), 1)
            h = float(round(poly[0], 2))
            
            # Clamp di sicurezza
            return max(0.0, min(1.0, h))
            
        except Exception as e:
            _err.capture(e, "_get_hurst_exponent", {"module": "EngineLA"})
            self.logger.debug(f"Errore calcolo Hurst: {e}")
            return 0.5

    def _calcola_funding_zscore(self, ticker, current_funding):
        """
        Calcola lo Z-Score del funding rate rispetto allo storico recente.
        FIX warmup: se il buffer è vuoto, lo precarica con gli ultimi
        funding rates storici via API invece di aspettare 20 cicli.
        """
        try:
            if ticker not in self._funding_history:
                self._funding_history[ticker] = []

            history = self._funding_history[ticker]

            # Precarichiamo lo storico se il buffer è sotto la soglia warmup
            if len(history) < 20:
                try:
                    from core.asset_list import get_futures_ticker
                    target = get_futures_ticker(ticker)
                    if target:
                        url = (
                            f"https://futures.kraken.com/derivatives/api/v3/"
                            f"historicalfundingrates?symbol={target}"
                        )
                        r = self._safe_request(url)
                        if r and r.get('result') == 'success':
                            rates = r.get('rates', [])
                            # Prendi gli ultimi 50 rates (da più vecchio a più recente)
                            for entry in rates[-50:]:
                                rate = float(entry.get('fundingRate', entry.get('rate', 0)))
                                if rate not in history:
                                    history.append(rate)
                            self.logger.debug(
                                f"Funding history precaricata per {ticker}: "
                                f"{len(history)} campioni"
                            )
                except Exception:
                    pass  # Fallback silenzioso al vecchio comportamento

            history.append(current_funding)
            if len(history) > self._funding_history_limit:
                history.pop(0)

            # Salva su disco ogni volta che aggiungiamo un campione
            # (il costo è minimo — è solo un piccolo JSON)
            self._salva_funding_history()

            if len(history) < 20:
                return 0.0  # warmup ancora insufficiente

            mean = np.mean(history)
            std  = np.std(history)

            if std == 0:
                return 0.0

            z = (current_funding - mean) / std
            return float(round(np.clip(z, -5.0, 5.0), 4))
        except Exception:
            return 0.0

    def _get_vpin_toxicity(self, ticker):
        try:
            ws_trades = self.ws_manager.get_trades(ticker)
            # WS-first: usa WS anche con pochi trades
            if ws_trades and len(ws_trades) >= 2:
                trades = ws_trades
            else:
                from core.asset_list import get_human_name
                asset_id = get_human_name(ticker)
                trades = self._safe_fetch('fetch_trades', asset_id, limit=50) or ws_trades or []
                
            if not trades: return 0
            v_buy = sum([float(t['amount']) for t in trades if t['side'] == 'buy'])
            v_sell = sum([float(t['amount']) for t in trades if t['side'] == 'sell'])
            v_total = v_buy + v_sell
            return round(abs(v_buy - v_sell) / v_total, 3) if v_total > 0 else 0
        except: return 0

    def _check_portfolio_correlation(self, ticker, posizioni_aperte):
        """
        Matrice di correlazione estesa a tutti i 5 asset del bot.
        FIX: la versione originale copriva solo 3 coppie (default 0.5 per tutti gli altri).
        Correlazioni empiriche crypto (media 30gg rolling 2023-2025).
        """
        if not posizioni_aperte:
            return 0.0

        correlazioni = {
            # BTC come driver principale
            ('AAVEUSD',  'XXBTZUSD'): 0.78,
            ('ATOMUSD',  'XXBTZUSD'): 0.80,
            ('AVAXUSD',  'XXBTZUSD'): 0.82,
            ('DOTUSD',   'XXBTZUSD'): 0.79,
            ('FETUSD',   'XXBTZUSD'): 0.71,
            ('LINKUSD',  'XXBTZUSD'): 0.83,
            ('NEARUSD',  'XXBTZUSD'): 0.77,
            ('POLUSD',   'XXBTZUSD'): 0.81,
            ('SOLUSD',   'XXBTZUSD'): 0.85,
            ('XDGUSD',   'XXBTZUSD'): 0.79,
            ('XETHZUSD', 'XXBTZUSD'): 0.92,
            ('XXRPZUSD', 'XXBTZUSD'): 0.76,
            # ETH come driver secondario
            ('AAVEUSD',  'XETHZUSD'): 0.88,
            ('ATOMUSD',  'XETHZUSD'): 0.82,
            ('AVAXUSD',  'XETHZUSD'): 0.86,
            ('FETUSD',   'XETHZUSD'): 0.74,
            ('LINKUSD',  'XETHZUSD'): 0.85,
            ('NEARUSD',  'XETHZUSD'): 0.80,
            ('POLUSD',   'XETHZUSD'): 0.84,
            ('SOLUSD',   'XETHZUSD'): 0.87,
            ('XDGUSD',   'XETHZUSD'): 0.81,
            ('XXRPZUSD', 'XETHZUSD'): 0.78,
            # Cross altcoin
            ('AVAXUSD',  'SOLUSD'):   0.88,
            ('AVAXUSD',  'NEARUSD'):  0.81,
            ('FETUSD',   'NEARUSD'):  0.72,
            ('LINKUSD',  'AAVEUSD'):  0.77,
            ('SOLUSD',   'NEARUSD'):  0.83,
            ('XDGUSD',   'XXRPZUSD'): 0.74,
        }

        max_corr = 0.0
        for aperta in posizioni_aperte:
            pair = tuple(sorted((ticker, aperta)))
            corr = correlazioni.get(pair, 0.65)  # default 0.65: crypto sempre correlate
            if corr > max_corr:
                max_corr = corr

        return round(max_corr, 3)
        
    def get_market_driver_logic(self, delta_f, whale_d, threshold):
        if abs(delta_f) > 0.4 and abs(whale_d) < threshold:
            return "RETAIL_MOMENTUM" 
        elif abs(whale_d) > threshold:
            return "INSTITUTIONAL_PUSH" 
        return "ORGANIC_FLOW" 
        
    def get_detailed_order_flow(self, ticker):
        try:
            # Fallback robusto WS-first: il WebSocket Kraken accumula trades
            # in buffer. Se >=2 trades ci sono già, USA quelli e NON chiamare
            # fetch_trades (che fallisce per blocco paginazione ccxt).
            # Solo se WS è completamente vuoto, prova fetch_trades come ultima risorsa.
            ws_trades = self.ws_manager.get_trades(ticker)
            if ws_trades and len(ws_trades) >= 2:
                trades = ws_trades
            else:
                from core.asset_list import get_human_name
                symbol = get_human_name(ticker)
                try:
                    trades = self.exchange.fetch_trades(symbol, limit=50)
                except Exception as e_ft:
                    _err.capture(e_ft, "get_detailed_order_flow", {"module": "EngineLA"})
                    self.logger.debug(f"fetch_trades fallito per {ticker} ({e_ft}) — uso WS={len(ws_trades) if ws_trades else 0} trades")
                    trades = ws_trades or []
            
            if not trades or len(trades) < 2:
                return {
                    'cvd_istantaneo': 0.0, 'vpin': 0.0, 'momentum_perc': 0.0, 'price_velocity': 0.0, 
                    'is_explosive': False, 'aggressivita_flow': "NEUTRAL"
                }
            
            df = pd.DataFrame(trades)
            
            if df.empty or 'side' not in df.columns:
                return {'cvd_istantaneo': 0.0, 'vpin': 0.0, 'momentum_perc': 0.0, 'price_velocity': 0.0, 'is_explosive': False, 'aggressivita_flow': "NEUTRAL"}

            df['price'] = pd.to_numeric(df['price'], errors='coerce')
            df['amount'] = pd.to_numeric(df['amount'], errors='coerce')
            df['timestamp'] = pd.to_numeric(df['timestamp'], errors='coerce')
            df = df.dropna(subset=['price', 'amount', 'timestamp', 'side'])

            start_price = float(df['price'].iloc[0])
            end_price = float(df['price'].iloc[-1])
            start_time = df['timestamp'].iloc[0] / 1000
            end_time = df['timestamp'].iloc[-1] / 1000
            duration = max(end_time - start_time, 1) 
            
            if start_price > 0:
                momentum = ((end_price - start_price) / start_price) * 100
            else:
                momentum = 0.0
            velocity = momentum / duration
            is_explosive = bool(abs(velocity) > 0.0005)

            df['delta_usd'] = np.where(df['side'] == 'buy', df['amount'] * df['price'], -df['amount'] * df['price'])
            cvd_usd = float(df['delta_usd'].sum())
            
            vol_totale_usd = float((df['amount'] * df['price']).sum())
            vpin = float(abs(cvd_usd) / vol_totale_usd) if vol_totale_usd > 0 else 0.0

            # ── WHALE DELTA ───────────────────────────────────────────────────
            # Isola i trade sopra il 95° percentile per dimensione (in USD).
            # Whale delta positivo = le whale stanno comprando.
            # Whale delta negativo = le whale stanno vendendo.
            # Normalizzato -1/+1 rispetto al volume whale totale.
            try:
                df['vol_usd'] = df['amount'] * df['price']
                soglia_whale = df['vol_usd'].quantile(0.95)
                df_whale = df[df['vol_usd'] >= soglia_whale]
                if not df_whale.empty:
                    whale_buy  = float(df_whale[df_whale['side'] == 'buy']['vol_usd'].sum())
                    whale_sell = float(df_whale[df_whale['side'] == 'sell']['vol_usd'].sum())
                    whale_tot  = whale_buy + whale_sell
                    whale_delta = (whale_buy - whale_sell) / whale_tot if whale_tot > 0 else 0.0
                else:
                    whale_delta = 0.0
            except Exception:
                whale_delta = 0.0
            
            # --- TOXIC FLOW DETECTION (VPIN Avanzato) ---
            is_toxic = bool(vpin > 0.75)
            if is_toxic:
                self.logger.warning(f"☢️ TOXIC FLOW RILEVATO su {ticker}: VPIN {vpin:.4f}. Possibile assorbimento istituzionale.")
            
            self.logger.debug(f"  CHIMERA: Vel {velocity:.6f} %/s | CVD (USD): {cvd_usd:.2f} | VPIN: {vpin:.4f}")

            return {
                'cvd_istantaneo': float(round(cvd_usd, 2)),
                'vpin': float(round(vpin, 4)),
                'momentum_perc': float(round(momentum, 4)),
                'price_velocity': float(round(velocity, 6)),
                'is_explosive': is_explosive,
                'is_toxic': is_toxic,
                'aggressivita_flow': "BUYERS" if cvd_usd > 0 else "SELLERS",
                'whale_delta': float(round(whale_delta, 4)),
            }
        except Exception as e:
            _err.capture(e, "get_detailed_order_flow", {"module": "EngineLA"})
            self.logger.error(f"🔴 Errore calcolo Order Flow su {ticker}: {e}")
            return {'cvd_istantaneo': 0.0, 'vpin': 0.0, 'momentum_perc': 0.0, 'price_velocity': 0.0, 'is_explosive': False, 'aggressivita_flow': "NEUTRAL"}
    
    def analizza_order_flow(self, ticker):
        """
        Alias per get_detailed_order_flow.
        Usato da strategy_engine.py per compatibilità.
        """
        return self.get_detailed_order_flow(ticker)
    
    def get_fear_greed(self):
        """
        Ritorna Fear & Greed Index dal Crypto Fear & Greed Index (alternative.me API).
        Fallback a 50 (neutro) se API fallisce o cache è vuota.
        Cache locale 1h per evitare hammering API.
        """
        try:
            # Cache check
            now = time.time()
            if not hasattr(self, '_fg_cache'):
                self._fg_cache = {'value': 50, 'ts': 0}

            # Cache valida 1 ora
            if now - self._fg_cache['ts'] < 3600:
                return self._fg_cache['value']

            # Chiamata API
            r = self._safe_request("https://api.alternative.me/fng/?limit=1", timeout=5)
            if r and r.get('data') and len(r['data']) > 0:
                value = int(r['data'][0].get('value', 50))
                self._fg_cache = {'value': value, 'ts': now}
                self.logger.debug(f"📊 Fear&Greed Index: {value}")
                return value

            # Fallback se API fallisce: usa cache vecchia o 50
            return self._fg_cache['value']
        except Exception as e:
            _err.capture(e, "get_fear_greed", {"module": "EngineLA"})
            self.logger.debug(f"Errore get_fear_greed: {e}")
            return getattr(self, '_fg_cache', {'value': 50})['value']
    
    def get_funding_rate_info(self, ticker):
        """
        Ritorna informazioni sul funding rate per strategy_engine.
        """
        try:
            funding_rate = self._get_external_funding(ticker)
            
            # Calcola z-score dalla history
            funding_zscore = 0.0
            if ticker in self._funding_history and len(self._funding_history[ticker]) > 10:
                import numpy as np
                history = self._funding_history[ticker]
                mean = np.mean(history)
                std = np.std(history)
                if std > 0:
                    funding_zscore = (funding_rate - mean) / std
            
            return {
                'funding_rate_8h_%': funding_rate * 100,  # Converti a %
                'funding_zscore': funding_zscore
            }
        except Exception as e:
            _err.capture(e, "get_funding_rate_info", {"module": "EngineLA"})
            self.logger.debug(f"Errore get_funding_rate_info per {ticker}: {e}")
            return {
                'funding_rate_8h_%': 0.0,
                'funding_zscore': 0.0
            }
    
    def _detect_hft_anomalies(self, ticker, ob):
        try:
            bids = ob['bids'][:20]
            asks = ob['asks'][:20]
            
            bid_vol = sum([float(b[1]) for b in bids])
            ask_vol = sum([float(a[1]) for a in asks])
            spoofing_idx = round(abs(bid_vol - ask_vol) / (bid_vol + ask_vol), 2) if (bid_vol + ask_vol) > 0 else 0
            
            iceberg_detected = 1 if (spoofing_idx > 0.85) else 0
            
            return iceberg_detected, spoofing_idx
        except Exception:
            return 0, 0.0

    def get_market_data_multi_tf(self, ticker, timeframes=None):
        """
        Recupera dati OHLCV e calcola indicatori strutturali per diversi timeframe.
        FIX: tf_map corretto per Kraken CCXT ('15m' non '15').
        Ora restituisce struttura completa invece del solo Hurst.
        """
        if timeframes is None:
            timeframes = ['15m', '1h', '4h', '1d']

        data_tf  = {}
        # Usa il ticker CCXT corretto (es. SOL/USD non SOLUSD)
        from core.asset_list import get_human_name
        symbol_ccxt = get_human_name(ticker)

        # Kraken CCXT accetta questi formati per i timeframe
        tf_map = {
            '1m':  '1',
            '5m':  '5',
            '15m': '15',
            '30m': '30',
            '1h':  '60',
            '4h':  '240',
            '1d':  '1440',
        }

        for tf in timeframes:
            try:
                tf_kraken = tf_map.get(tf, tf)
                # 1d: 60 candele = 2 mesi di storia, sufficiente per trend e struttura
                _limit = 60 if tf == '1d' else 100
                ohlcv = self._safe_fetch(
                    'fetch_ohlcv', symbol_ccxt,
                    timeframe=tf_kraken, limit=_limit
                )
                if not ohlcv:
                    continue

                df = pd.DataFrame(
                    ohlcv,
                    columns=['timestamp', 'open', 'high', 'low', 'close', 'volume']
                )
                for col in ['open', 'high', 'low', 'close', 'volume']:
                    df[col] = pd.to_numeric(df[col], errors='coerce')
                df = df.dropna()

                if len(df) < 10:
                    continue

                hurst = self._get_hurst_exponent(df['close'].values)
                atr   = self._calcola_atr(df)
                close = float(df['close'].iloc[-1])

                # Kaufman Efficiency Ratio
                period = min(10, len(df) - 1)
                if period > 0:
                    change    = abs(df['close'].iloc[-1] - df['close'].iloc[-(period + 1)])
                    volatility = df['close'].diff().abs().tail(period).sum()
                    ker = round(change / volatility, 3) if volatility > 0 else 0.0
                else:
                    ker = 0.0

                # Trend direction: confronto EMA20 vs EMA50
                ema20 = float(df['close'].ewm(span=20, adjust=False).mean().iloc[-1])
                ema50 = float(df['close'].ewm(span=50, adjust=False).mean().iloc[-1]) if len(df) >= 50 else ema20
                trend_dir = "UP" if ema20 > ema50 else "DOWN"

                # Volume relativo (ultimo vs media)
                vol_medio   = float(df['volume'].mean())
                vol_corrente = float(df['volume'].iloc[-1])
                vol_rel = round(vol_corrente / vol_medio, 2) if vol_medio > 0 else 1.0

                # MACD su daily per visione macro
                _macd_fast = df['close'].ewm(span=12, adjust=False).mean()
                _macd_slow = df['close'].ewm(span=26, adjust=False).mean()
                _macd_line = _macd_fast - _macd_slow
                _macd_signal = _macd_line.ewm(span=9, adjust=False).mean()
                macd_val    = round(float(_macd_line.iloc[-1]), 6)
                macd_hist   = round(float((_macd_line - _macd_signal).iloc[-1]), 6)

                # RSI su daily
                _delta = df['close'].diff()
                _gain = _delta.clip(lower=0).rolling(14).mean()
                _loss = (-_delta.clip(upper=0)).rolling(14).mean()
                _rs = _gain / _loss.replace(0, 1e-10)
                rsi_val = round(float((100 - 100/(1+_rs)).iloc[-1]), 1)

                data_tf[tf] = {
                    'hurst':        hurst,
                    'kaufman':      ker,
                    'atr':          round(atr, 6),
                    'atr_perc':     round(atr / close * 100, 3) if close > 0 else 0.0,
                    'close':        close,
                    'ema20':        round(ema20, 6),
                    'ema50':        round(ema50, 6),
                    'trend_dir':    trend_dir,
                    'vol_relativo': vol_rel,
                    'macd':         macd_val,
                    'macd_hist':    macd_hist,
                    'rsi':          rsi_val,
                    'regime':       (
                        'TRENDING'       if hurst > 0.55 and ker > 0.20 else
                        'MEAN_REVERSION' if hurst < 0.45 and ker < 0.30 else
                        'TRENDING'       if ker > 0.60 else
                        'MEAN_REVERSION' if ker < 0.10 else
                        'UNDEFINED'
                    ),
                }

            except Exception as e:
                _err.capture(e, "get_market_data_multi_tf", {"module": "EngineLA"})
                self.logger.debug(f"⚠️ Errore dati {tf} per {ticker}: {e}")

        return data_tf
    
    def _calcola_correlazione_driver(self, ticker, df_asset):
        """Calcola la correlazione real-time tra l'asset e il suo Market Driver (BTC)."""
        try:
            # Default driver: Bitcoin
            driver_ticker = "XXBTZUSD"
            
            # Recupero dati driver (stesso timeframe dell'asset)
            ohlcv_driver = self._safe_fetch('fetch_ohlcv', driver_ticker, timeframe='15m', limit=len(df_asset))
            if not ohlcv_driver: return 1.0
            
            df_driver = pd.DataFrame(ohlcv_driver, columns=['ts', 'open', 'high', 'low', 'close', 'volume'])
            
            # Allineamento e calcolo correlazione sui rendimenti logaritmici
            returns_asset = np.log(df_asset['close'] / df_asset['close'].shift(1)).dropna()
            returns_driver = np.log(df_driver['close'] / df_driver['close'].shift(1)).dropna()
            
            common_len = min(len(returns_asset), len(returns_driver))
            if common_len < 10: return 1.0
            
            corr = np.corrcoef(returns_asset.values[-common_len:], returns_driver.values[-common_len:])[0, 1]
            return float(round(corr, 3)) if not np.isnan(corr) else 1.0
        except Exception as e:
            _err.capture(e, "_calcola_correlazione_driver", {"module": "EngineLA"})
            self.logger.debug(f"⚠️ Errore calcolo correlazione driver: {e}")
            return 1.0

    def _calcola_wall_pressure(self, ticker, wall_price, trades):
        """Calcola la pressione (volume consumato) vicino a un muro di liquidità negli ultimi scambi, normalizzata (0.0-1.0)."""
        if not trades or wall_price <= 0: return 0.0
        try:
            # 1. Recupero il volume totale del muro per normalizzare (usa dati WS/Cache)
            walls = self.get_liquidity_walls(ticker)
            vol_muro = 0
            if abs(wall_price - walls.get('muro_supporto', 0)) < (wall_price * 0.001):
                vol_muro = walls.get('vol_supporto', 0)
            elif abs(wall_price - walls.get('muro_resistenza', 0)) < (wall_price * 0.001):
                vol_muro = walls.get('vol_resistenza', 0)
            
            if vol_muro <= 0: return 0.0

            # 2. Calcolo volume scambiato vicino al muro (soglia 0.5% per asset volatili)
            soglia = wall_price * 0.005
            vol_vicino = sum([float(t['amount']) for t in trades if abs(float(t['price']) - wall_price) <= soglia])
            
            # 3. Restituisco la percentuale del muro consumata (max 1.0)
            pressione_norm = min(vol_vicino / vol_muro, 1.0)
            return float(round(pressione_norm, 4))
        except Exception as e:
            _err.capture(e, "_calcola_wall_pressure", {"module": "EngineLA"})
            self.logger.debug(f"Errore normalizzazione pressione muro: {e}")
            return 0.0

# File updated to sync with UI