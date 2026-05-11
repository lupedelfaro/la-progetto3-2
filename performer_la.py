# -*- coding: utf-8 -*-
"""
L&A Institutional Bot - PerformerLA
Versione 3.0: CHIMERA INTEGRATION & DYNAMIC PRECISION
"""

import logging
import ccxt
import time
import uuid
import threading
from datetime import datetime
from core import asset_list
from core import config_la
import sys
from .chimera_errors import ErrorTracker

_err = ErrorTracker("PerformerLA")

class PerformerLA:
    def __init__(self, exchange_id=None):
        exchange_id = exchange_id or 'kraken'
        # FIX InvalidNonce (2026-05-07): nonceWindow tollera nonce out-of-order
        # entro 5000ms se più thread chiamano private_post_* in parallelo.
        # Risolve i 27 errori EAPI:Invalid nonce su pulizia_totale_ordini.
        self.exchange = getattr(ccxt, exchange_id)({
            'apiKey': config_la.KRAKEN_KEY,
            'secret': config_la.KRAKEN_SECRET,
            'enableRateLimit': True,
            'timeout': 45000,
            'options': {'defaultType': 'spot', 'fetchTradesWarning': False, 'nonceWindow': 5000}
        })
        self.logger = logging.getLogger("PerformerLA")
        # Lock per chiamate private batch (pulizia_totale_ordini, cancellazioni
        # multiple). Serializza burst di chiamate sullo stesso thread API.
        self._private_lock = threading.Lock()
        # Questo dizionario manterrà gli ID finché il bot è acceso
        self.ordini_attivi = {}
        # Cache per get_open_positions_real e get_open_orders_real
        # TTL 25s — Kraken rate limit ~15 chiamate private/min
        self._cache_positions      = {}
        self._cache_positions_ts   = 0
        self._cache_orders         = {}
        self._cache_orders_ts      = 0
        self._cache_ttl            = 25  # secondi

    def invalidate_cache(self):
        """Invalida la cache di posizioni e ordini — chiamare dopo apertura/chiusura."""
        self._cache_positions_ts = 0
        self._cache_orders_ts    = 0
        
    def get_available_margin(self, asset='ZUSD'):
        """Recupera il margine disponibile (in EUR/USD) dall'exchange."""
        try:
            res = self.exchange.private_post_tradebalance({'asset': asset})
            if res and 'result' in res:
                # 'mf' è il margine libero (margin free)
                mf = float(res['result'].get('mf', 0))
                if mf > 0:
                    return mf
                # Fallback: se mf è 0, proviamo 'eb' (equivalent balance)
                eb = float(res['result'].get('eb', 0))
                if eb > 0:
                    self.logger.info(f"💰 Margine 'mf' a 0, uso Equivalent Balance 'eb': {eb:.2f} {asset}")
                    return eb
            return 0
        except Exception as e:
            _err.capture(e, sys._getframe().f_code.co_name, {"module": "PerformerLA"})
            self.logger.error(f"❌ Errore recupero margine per {asset}: {e}")
            return 0

    def get_total_equity(self, asset='ZUSD'):
        """Recupera l'equity totale (e) dall'exchange Kraken. Retry su errori rete."""
        for attempt in range(3):
            try:
                res = self.exchange.private_post_tradebalance({'asset': asset})
                if res and 'result' in res:
                    e = float(res['result'].get('e', 0))
                    if e > 0:
                        return e
                    eb = float(res['result'].get('eb', 0))
                    if eb > 0:
                        return eb
                return 0
            except (ccxt.NetworkError, ccxt.RequestTimeout) as e:
                _err.capture(e, "get_total_equity", {"module": "PerformerLA"})
                wait = 10 * (attempt + 1)
                if attempt < 2:
                    self.logger.warning(f"⚠️ TradeBalance network error (tentativo {attempt+1}/3): attendo {wait}s...")
                    time.sleep(wait)
                else:
                    self.logger.warning(f"⚠️ TradeBalance non disponibile dopo 3 tentativi — uso 0")
                    return 0
            except Exception as e:
                _err.capture(e, sys._getframe().f_code.co_name, {"module": "PerformerLA"})
                self.logger.error(f"❌ Errore recupero equity per {asset}: {e}")
                return 0
        return 0

    def get_portfolio_snapshot(self, asset='ZUSD'):
        """
        Recupera lo snapshot completo del portfolio da Kraken (una sola chiamata TradeBalance).

        Campi Kraken:
          eb  — Equivalent Balance: valore totale conto (collaterale + crypto valorizzati)
          tb  — Trade Balance: collaterale puro fiat
          m   — Margin Used: margine impegnato nelle posizioni aperte
          mf  — Margin Free: margine disponibile per nuove posizioni
          ml  — Margin Level %: (equity/margin)*100. Sotto 100 = margin call
          n   — Unrealized PnL: profitto/perdita non realizzato posizioni aperte
          uv  — Float: valore corrente posizioni aperte (cost + n)
          e   — Equity: tb + n (capitale netto incluso PnL aperto)
          c   — Cost Basis: costo apertura posizioni correnti

        Alias leggibili aggiunti:
          disponibile_usd → mf
          equity_totale   → e (o eb come fallback)
          pnl_aperto      → n
        """
        snapshot = {
            'eb': 0.0, 'tb': 0.0, 'm': 0.0, 'mf': 0.0,
            'ml': 0.0, 'n': 0.0, 'uv': 0.0, 'e': 0.0, 'c': 0.0,
            'disponibile_usd': 0.0,
            'equity_totale': 0.0,
            'pnl_aperto': 0.0,
            'asset': asset,
            'errore': None,
            'ts': 0.0,
        }
        for attempt in range(3):
            try:
                res = self.exchange.private_post_tradebalance({'asset': asset})
                if not res or 'result' not in res:
                    snapshot['errore'] = f"Risposta vuota da TradeBalance (asset={asset})"
                    return snapshot

                r = res['result']
                for campo in ('eb', 'tb', 'm', 'mf', 'ml', 'n', 'uv', 'e', 'c'):
                    try:
                        snapshot[campo] = float(r.get(campo, '0') or '0')
                    except (ValueError, TypeError):
                        snapshot[campo] = 0.0

                snapshot['disponibile_usd'] = snapshot['mf']
                snapshot['equity_totale']   = snapshot['e'] if snapshot['e'] > 0 else snapshot['eb']
                snapshot['pnl_aperto']      = snapshot['n']
                snapshot['ts']              = time.time()

                self.logger.debug(
                    f"💼 Portfolio ({asset}): "
                    f"Equity={snapshot['equity_totale']:.2f} | "
                    f"PnL_aperto={snapshot['pnl_aperto']:+.2f} | "
                    f"Margine_libero={snapshot['disponibile_usd']:.2f} | "
                    f"Margin_level={snapshot['ml']:.1f}%"
                )
                return snapshot

            except (ccxt.NetworkError, ccxt.RequestTimeout) as e:
                _err.capture(e, "get_portfolio_snapshot", {"module": "PerformerLA"})
                wait = 10 * (attempt + 1)
                if attempt < 2:
                    self.logger.warning(
                        f"⚠️ TradeBalance network error (tentativo {attempt+1}/3): attendo {wait}s..."
                    )
                    time.sleep(wait)
                else:
                    snapshot['errore'] = f"Network error dopo 3 tentativi: {e}"
                    self.logger.warning("⚠️ get_portfolio_snapshot non disponibile dopo 3 tentativi")
                    return snapshot
            except Exception as e:
                _err.capture(e, sys._getframe().f_code.co_name, {"module": "PerformerLA"})
                snapshot['errore'] = str(e)
                self.logger.error(f"❌ Errore get_portfolio_snapshot: {e}")
                return snapshot
        return snapshot

    def get_order_ids_from_memory(self, asset):
        """ Recupera gli ID salvati senza interrogare Kraken inutilmente """
        dati = self.ordini_attivi.get(asset, {})
        id_sl = dati.get('sl_id') 
        id_tp = dati.get('tp_id')
        self.logger.info(f"🔍 Recuperato ID SL REALE per {asset}: {id_sl}")
        self.logger.info(f"🔍 Recuperato ID TP REALE per {asset}: {id_tp}")
        return id_sl, id_tp
    
    def cancella_ordine_specifico(self, order_id):
        """ 
        VERSIONE MASTER CHIMERA:
        Risolto errore str vs int e aggiunta resilienza per ordini già chiusi o pending.
        """
        if not order_id: return False
        if str(order_id).startswith("virtual_sl"):
            self.logger.info(f"✅ Ordine virtuale {order_id} rimosso con successo.")
            return True
        try:
            # 1. Chiamata diretta all'endpoint di Kraken
            res = self.exchange.private_post_cancelorder({'txid': order_id})
            
            # 2. Verifica reale con cast a intero
            if res and 'result' in res:
                # Se lo stato è pending, la richiesta è stata accettata
                if res['result'].get('status') == 'pending':
                    self.logger.info(f"⏳ Ordine {order_id} in cancellazione (pending)...")
                    return True

                count = int(res['result'].get('count', 0))
                if count > 0:
                    self.logger.info(f"✅ Ordine {order_id} rimosso con successo.")
                    return True
            
            self.logger.warning(f"⚠️ Ordine {order_id} non rimosso (count 0).")
            return False
            
        except Exception as e:
            _err.capture(e, sys._getframe().f_code.co_name, {"module": "PerformerLA"})
            # --- LOGICA DI RESILIENZA CHIMERA ---
            # Se l'ordine è già stato eseguito o cancellato, il risultato per noi è SUCCESS
            msg_errore = str(e).lower()
            if "invalid order" in msg_errore or "already closed" in msg_errore:
                self.logger.info(f"ℹ️ Ordine {order_id} già chiuso o inesistente. Procedo.")
                return True

            self.logger.warning(f"⚠️ Impossibile cancellare l'ordine {order_id}: {e}")
            return False
            
    def qprice(self, symbol, p):
        """ 
        Formattatore prezzi dinamico Chimera. 
        Recupera la precisione reale dal mercato per evitare 'Invalid Price'.
        """
        if not p: return None
        try:
            # Carichiamo i mercati se non presenti per avere le precisioni aggiornate
            if not self.exchange.markets:
                try:
                    self.exchange.load_markets()
                except Exception as e:
                    _err.capture(e, sys._getframe().f_code.co_name, {"module": "PerformerLA"})
                    self.logger.warning(f"⚠️ Errore load_markets (Assets): {e}. Riprovo tra 2s...")
                    time.sleep(2)
                    self.exchange.load_markets()
            
            # Se l'asset è in asset_list, usiamo il ticker ufficiale
            symbol_ufficiale = asset_list.get_ticker(symbol)
            return self.exchange.price_to_precision(symbol_ufficiale, float(p))
        except:
            # Fallback manuale se il caricamento mercati fallisce
            if "XBT" in symbol or "BTC" in symbol: return f"{float(p):.1f}"
            if "ETH" in symbol: return f"{float(p):.2f}"
            return str(p)

    def _safe_fetch(self, method_name, *args, **kwargs):
        """Esegue una chiamata CCXT con retry automatici in caso di errore di rete o rate limit."""
        import time
        import ccxt
        max_retries = 3
        for attempt in range(max_retries):
            try:
                method = getattr(self.exchange, method_name)
                return method(*args, **kwargs)
            except (ccxt.NetworkError, ccxt.ExchangeError, ccxt.RateLimitExceeded) as e:
                _err.capture(e, "_safe_fetch", {"module": "PerformerLA"})
                if attempt == max_retries - 1:
                    self.logger.error(f"❌ Errore definitivo API {method_name} dopo {max_retries} tentativi: {e}")
                    raise e
                wait = (attempt + 1) * 2
                self.logger.warning(f"⚠️ Errore API {method_name} (Tentativo {attempt+1}/{max_retries}): {e}. Attendo {wait}s...")
                time.sleep(wait)
            except Exception as e:
                _err.capture(e, sys._getframe().f_code.co_name, {"module": "PerformerLA"})
                self.logger.error(f"❌ Errore imprevisto API {method_name}: {e}")
                raise e
        return None

    def get_current_price(self, asset):
        """ Recupera l'ultimo prezzo di mercato per un asset. """
        try:
            symbol = asset_list.get_ticker(asset)
            # Use safe fetch if available, otherwise fallback
            if hasattr(self, '_safe_fetch'):
                ticker = self._safe_fetch('fetch_ticker', symbol)
            else:
                ticker = self.exchange.fetch_ticker(symbol)
            return ticker['last']
        except Exception as e:
            _err.capture(e, sys._getframe().f_code.co_name, {"module": "PerformerLA"})
            self.logger.error(f"❌ Errore recupero prezzo {asset}: {e}")
            return None

    def fetch_order_details(self, order_id):
        """Recupera i dettagli di un ordine specifico, incluse le commissioni (fee)."""
        try:
            order = self.exchange.fetch_order(order_id)
            return {
                'id': order.get('id'),
                'status': order.get('status'),
                'filled': order.get('filled', 0),
                'fee': order.get('fee', {}).get('cost', 0) if order.get('fee') else 0,
                'fee_currency': order.get('fee', {}).get('currency', 'USD') if order.get('fee') else 'USD',
                'price': order.get('price', 0),
                'average': order.get('average', 0),
                'cost': order.get('cost', 0)
            }
        except Exception as e:
            _err.capture(e, sys._getframe().f_code.co_name, {"module": "PerformerLA"})
            self.logger.error(f"❌ Errore recupero dettagli ordine {order_id}: {e}")
            return None

    def get_open_orders_real(self, force=False):
        """Recupera gli ordini aperti reali da Kraken. Cache 25s."""
        if not force and (time.time() - self._cache_orders_ts) < self._cache_ttl:
            return self._cache_orders
        try:
            res = self.exchange.fetch_open_orders()
            ordini_dict = {}
            for o in res:
                oid = o.get('id')
                if oid:
                    ordini_dict[oid] = o
            self._cache_orders    = ordini_dict
            self._cache_orders_ts = time.time()
            return ordini_dict
        except Exception as e:
            _err.capture(e, sys._getframe().f_code.co_name, {"module": "PerformerLA"})
            self.logger.error(f"❌ Errore recupero ordini aperti: {e}")
            return self._cache_orders  # restituisce cache vecchia se disponibile

    def get_open_positions(self, force=False):
        """Alias per compatibilità con bot_la."""
        return self.get_open_positions_real(force=force)

    def get_open_positions_real(self, force=False):
        """ 
        Recupera posizioni e ordini usando esclusivamente i codici Kraken originali.
        Cache 25s — evita rate limit su chiamate ravvicinate.
        Usa force=True per forzare un aggiornamento immediato (es. dopo apertura/chiusura).
        """
        if not force and (time.time() - self._cache_positions_ts) < self._cache_ttl:
            return self._cache_positions
        import ccxt
        max_retries = 3
        last_error = None
        for attempt in range(max_retries):
            try:
                # 1. Recupero posizioni aperte a margine
                pos_res = self.exchange.private_post_openpositions()
                raw_positions = pos_res.get('result', {})
                
                # Se il risultato è None o non è un dizionario, potrebbe essere un errore API silente
                if raw_positions is None:
                    self.logger.warning(f"⚠️ Kraken ha restituito result=None per OpenPositions (Tentativo {attempt+1}/{max_retries})")
                    time.sleep(2)
                    continue

                # 2. Recupero ordini aperti per SL/TP
                orders_res = self.exchange.private_post_openorders()
                open_orders = orders_res.get('result', {}).get('open', {})

                # Dizionario finale: Chiave = Ticker (es. XXBTZUSD)
                mapped_positions = {}

                for p_id, p_data in raw_positions.items():
                    # Inizializzazione campi protezione
                    p_data['has_sl'] = False
                    p_data['has_tp'] = False
                    p_data['sl_id_kraken'] = None
                    p_data['tp_id_kraken'] = None
                    p_data['pos_txid'] = p_id  # Salviamo l'ID reale della posizione
                    
                    ticker_k = p_data.get('pair', '')
                    if not ticker_k: continue

                    # 3. Matching chirurgico degli ordini aperti
                    sl_orders = []
                    tp_orders = []
                    
                    for o_id, o_data in open_orders.items():
                        descr = o_data.get('descr', {})
                        o_ticker = descr.get('pair', '')
                        
                        norm_o = self._normalize_ticker(o_ticker)
                        norm_p = self._normalize_ticker(ticker_k)
                        
                        if norm_o == norm_p:
                            self.logger.debug(f"🔍 Match Ordine: {o_id} ({o_ticker}) per posizione {ticker_k}")
                            tipo = str(descr.get('ordertype', '')).lower()
                            o_side = str(descr.get('type', '')).lower()
                            p_side = str(p_data.get('type', '')).lower()
                            
                            # L'ordine di protezione deve essere di segno opposto alla posizione
                            if o_side != p_side:
                                # Cattura ID e Prezzo Stop Loss
                                if 'stop' in tipo: 
                                    sl_orders.append((o_id, float(descr.get('price', 0))))
                                    
                                # Cattura ID e Prezzo Take Profit (limit o take-profit)
                                elif 'profit' in tipo or 'limit' in tipo: 
                                    tp_orders.append((o_id, float(descr.get('price', 0))))
                                
                    # Pulizia duplicati: teniamo solo l'ultimo ordine inserito
                    if len(sl_orders) > 1:
                        self.logger.warning(f"🧹 Trovati {len(sl_orders)} SL per {ticker_k}. Cancello i duplicati.")
                        for o_id, _ in sl_orders[:-1]:
                            self.cancella_ordine_specifico(o_id)
                            
                    if len(tp_orders) > 1:
                        self.logger.warning(f"🧹 Trovati {len(tp_orders)} TP per {ticker_k}. Cancello i duplicati.")
                        for o_id, _ in tp_orders[:-1]:
                            self.cancella_ordine_specifico(o_id)
                            
                    if sl_orders:
                        p_data['has_sl'] = True
                        p_data['sl_id_kraken'] = sl_orders[-1][0]
                        p_data['sl_price_kraken'] = sl_orders[-1][1]
                        
                    if tp_orders:
                        p_data['has_tp'] = True
                        p_data['tp_id_kraken'] = tp_orders[-1][0]
                        p_data['tp_price_kraken'] = tp_orders[-1][1]
                    
                    # Inseriamo nel dizionario usando sia il TICKER originale che quello normalizzato come chiave
                    norm_ticker = self._normalize_ticker(ticker_k)
                    mapped_positions[ticker_k] = p_data
                    if norm_ticker != ticker_k:
                        mapped_positions[norm_ticker] = p_data
                
                self._cache_positions    = mapped_positions
                self._cache_positions_ts = time.time()
                return mapped_positions
                
            except (ccxt.NetworkError, ccxt.RateLimitExceeded) as e:
                last_error = e
                self.logger.warning(f"⚠️ Kraken API limit/network (Tentativo {attempt+1}/{max_retries}): {e}. Attendo 3s...")
                time.sleep(3)
            except Exception as e:
                _err.capture(e, sys._getframe().f_code.co_name, {"module": "PerformerLA"})
                last_error = e
                self.logger.error(f"❌ Errore nel matching ordini Kraken (Tentativo {attempt+1}/{max_retries}): {e}")
                time.sleep(2)
        
        # Tutti i tentativi falliti — restituisce cache se disponibile, altrimenti errore
        if self._cache_positions:
            self.logger.warning(f"⚠️ get_open_positions_real fallita, uso cache ({int(time.time()-self._cache_positions_ts)}s fa)")
            return self._cache_positions
        if last_error:
            raise last_error
        return {}
            
    def get_realized_pnl_24h(self):
        """
        Recupera il PnL realizzato nelle ultime 24 ore dal Ledger Kraken.

        Kraken registra ogni chiusura margin come voci di tipo 'trade' o 'margin'.
        Il PnL netto in fiat si trova nelle voci con asset ZUSD/ZEUR o simili.
        Logghiamo tutte le voci trovate per poter fare debug in caso di 0 voci.
        """
        # Tutti i nomi fiat che Kraken può usare nel ledger
        FIAT_ASSETS = {
            'ZUSD', 'ZEUR', 'USD', 'EUR', 'USDT', 'USDC',
            'ZGBP', 'ZCAD', 'ZJPY', 'ZAUD', 'CHF',
        }
        # Tipi da escludere esplicitamente (non sono PnL trading)
        SKIP_TYPES = {'deposit', 'withdrawal', 'transfer', 'rollover', 'staking', 'dividend'}

        for attempt in range(3):
            try:
                since = int(time.time() - 86400)
                # Non filtriamo per type qui — prendiamo tutto e filtriamo dopo
                # perché Kraken usa tipi diversi (trade, margin, settled) per il PnL
                res = self.exchange.private_post_ledgers({'start': since})
                ledger = res.get('result', {}).get('ledger', {})

                total_pnl = 0.0
                n_entries = 0
                all_types = set()
                all_assets = set()

                for entry_id, entry in ledger.items():
                    asset_k = str(entry.get('asset', '') or '').upper()
                    entry_type = str(entry.get('type', '') or '').lower()
                    amount = float(entry.get('amount', 0) or 0)

                    all_types.add(entry_type)
                    all_assets.add(asset_k)

                    # Salta tipi non-trading
                    if entry_type in SKIP_TYPES:
                        continue
                    # Considera solo movimenti fiat
                    if asset_k not in FIAT_ASSETS:
                        continue

                    total_pnl += amount
                    n_entries += 1

                self.logger.info(
                    f"📊 PnL Realizzato (24h) da Kraken Ledger: {total_pnl:.2f} USD "
                    f"({n_entries} voci fiat) | "
                    f"Tipi trovati: {all_types} | Asset trovati: {all_assets}"
                )

                # Se 0 voci fiat, logga un warning esplicito con tutti gli asset
                if n_entries == 0 and ledger:
                    self.logger.warning(
                        f"⚠️ Ledger ha {len(ledger)} voci ma 0 in fiat. "
                        f"Asset presenti: {all_assets} | Tipi: {all_types}. "
                        f"Controlla se il nome fiat è in FIAT_ASSETS."
                    )
                    # Fallback: prova a sommare tutte le voci non-crypto e non-skip
                    # come ultima risorsa
                    crypto_keywords = {
                        'BTC','ETH','SOL','XRP','DOGE','ADA','DOT','AVAX',
                        'LINK','UNI','AAVE','MATIC','POL','ZEC','TAO','FET',
                        'BONK','XXBT','XETH','XLTC','XXRP','XDGE','XZEC',
                    }
                    for entry_id, entry in ledger.items():
                        asset_k = str(entry.get('asset', '') or '').upper()
                        entry_type = str(entry.get('type', '') or '').lower()
                        if entry_type in SKIP_TYPES:
                            continue
                        # Se non è chiaramente una crypto, probabilmente è fiat
                        is_crypto = any(kw in asset_k for kw in crypto_keywords)
                        if not is_crypto:
                            amount = float(entry.get('amount', 0) or 0)
                            total_pnl += amount
                            n_entries += 1
                    if n_entries > 0:
                        self.logger.info(
                            f"📊 PnL fallback (asset non-crypto): {total_pnl:.2f} USD "
                            f"({n_entries} voci)"
                        )

                return total_pnl

            except (ccxt.NetworkError, ccxt.RequestTimeout) as e:
                _err.capture(e, "get_realized_pnl_24h", {"module": "PerformerLA"})
                wait = 15 * (attempt + 1)
                if attempt < 2:
                    self.logger.warning(f"⚠️ Ledger network error (tentativo {attempt+1}/3): attendo {wait}s...")
                    time.sleep(wait)
                else:
                    self.logger.warning(f"⚠️ Ledger non disponibile dopo 3 tentativi — PnL 24h non aggiornato")
                    return None
            except Exception as e:
                _err.capture(e, sys._getframe().f_code.co_name, {"module": "PerformerLA"})
                self.logger.error(f"❌ Errore recupero PnL 24h da Ledger: {e}")
                return None
        return None

    def get_trade_pnl_real(self, order_id):
        """
        Recupera il PnL reale di un trade specifico tramite l'ID ordine di chiusura.
        Usa fetch_my_trades con ordertxid per ottenere il PnL in USD dalla trade history.
        """
        try:
            trades = self.exchange.fetch_my_trades(
                symbol=None, since=None, limit=None,
                params={'ordertxid': order_id}
            )

            total_pnl_usd = 0.0
            total_fee_usd = 0.0
            found = False

            for t in trades:
                info = t.get('info', {})
                fee = float(t.get('fee', {}).get('cost', 0) or 0)
                total_fee_usd += fee

                # Kraken restituisce 'net' in USD per le posizioni margin chiuse
                net_raw = info.get('net')
                if net_raw is not None and net_raw != '':
                    try:
                        total_pnl_usd += float(net_raw)
                        found = True
                        continue
                    except (ValueError, TypeError):
                        pass

                # Fallback: calcola PnL da prezzo esecuzione se 'net' non disponibile
                price   = float(t.get('price', 0) or 0)
                amount  = float(t.get('amount', 0) or 0)
                cost    = float(t.get('cost', 0) or 0)
                side    = str(t.get('side', '')).lower()
                # 'posstatus' == 'closed' indica chiusura posizione margin
                pos_status = str(info.get('posstatus', '')).lower()
                if pos_status == 'closed' and cost > 0:
                    # PnL approssimato dal costo di chiusura - costo di apertura non disponibile qui
                    # Lasciamo al chiamante il fallback statistico
                    pass

            if found and total_pnl_usd != 0:
                return {
                    'pnl_netto': total_pnl_usd,
                    'fee': total_fee_usd
                }

            # Nessun dato 'net' trovato — il chiamante userà il fallback statistico
            return {'pnl_netto': 0, 'fee': total_fee_usd}

        except Exception as e:
            _err.capture(e, sys._getframe().f_code.co_name, {"module": "PerformerLA"})
            self.logger.error(f"❌ Errore recupero PnL reale per ordine {order_id}: {e}")
            return None

    def _normalize_ticker(self, ticker):
        if not ticker: return ""
        ticker = ticker.upper().replace("/", "").replace(" ", "")
        
        # Alias noti di Kraken per openorders vs openpositions
        if ticker in ["XBTUSD", "XXBTZUSD"]: return "BTCUSD"
        if ticker in ["ETHUSD", "XETHZUSD"]: return "ETHUSD"
        if ticker in ["XRPUSD", "XXRPZUSD"]: return "XRPUSD"
        if ticker in ["LTCUSD", "XLTCZUSD"]: return "LTCUSD"
        if ticker in ["XDGUSD", "XXDGUSD"]: return "DOGEUSD"
        
        from core.asset_list import get_human_name
        # get_human_name returns format like 'BTC/USD', we remove the slash
        human_name = get_human_name(ticker)
        return human_name.replace("/", "").replace(" ", "")

    def esegui_ordine(self, asset, direzione, size, leverage, voto, sl=None, tp=None, tipo_op="Swing"):
        try:
            # 1. IL FIX DEFINITIVO PER I NOMI UMANI
            # Non importa cosa riceve (asset), noi usiamo SOLO il ticker ufficiale di Kraken
            symbol = asset_list.get_ticker(asset)
            
            # Se per qualche motivo asset_list non restituisce nulla, fermiamo tutto prima dell'errore
            if not symbol:
                self.logger.error(f"❌ Asset {asset} non trovato in ASSET_LIST. Operazione annullata.")
                return {'success': False, 'error': 'Invalid Ticker'}

            # 1. DETERMINAZIONE TIPO ESECUZIONE (Smart Execution)
            # Se il voto è altissimo (>=9), usiamo Market per non perdere l'occasione.
            # Altrimenti usiamo Limit per risparmiare commissioni (Maker fee).
            tipo_ordine_side = 'buy' if direzione.upper() in ["BUY", "LONG"] else 'sell'
            
            if voto and int(voto) >= 9:
                tipo_esecuzione = 'market'
                self.logger.info(f"🚀 Convinzione Alta (Voto {voto}) -> Esecuzione MARKET per {asset}")
            else:
                tipo_esecuzione = 'limit'
                self.logger.info(f"⚖️ Convinzione Media (Voto {voto}) -> Esecuzione LIMIT per {asset}")

            # 2. FORMATTAZIONE DINAMICA PRECISIONE E VOLUME (100% Kraken)
            try:
                self.exchange.load_markets()
                market = self.exchange.market(symbol)
                size = float(self.exchange.amount_to_precision(symbol, size))
            except Exception as e:
                _err.capture(e, sys._getframe().f_code.co_name, {"module": "PerformerLA"})
                self.logger.warning(f"⚠️ Impossibile ottenere precisione dinamica per {symbol}: {e}. Uso size originale.")

            # 3. FIX LEVA E PARAMS (per evitare 'Invalid arguments')
            params = {}
            if leverage and float(leverage) > 1:
                params['leverage'] = str(int(float(leverage)))
                
            # Aggiungiamo cl_ord_id per tracciamento Kraken
            params['cl_ord_id'] = str(uuid.uuid4())
            
            # Se è un ordine Limit, dobbiamo passare il prezzo
            prezzo_ordine = None
            if tipo_esecuzione == 'limit':
                prezzo_ordine = self.get_current_price(asset)
                if not prezzo_ordine:
                    self.logger.warning(f"⚠️ Impossibile ottenere prezzo per LIMIT {asset}. Fallback a MARKET.")
                    tipo_esecuzione = 'market'
                else:
                    try:
                        prezzo_ordine = float(self.exchange.price_to_precision(symbol, prezzo_ordine))
                    except Exception as e:
                        _err.capture(e, sys._getframe().f_code.co_name, {"module": "PerformerLA"})
                        self.logger.warning(f"⚠️ Impossibile formattare prezzo per {symbol}: {e}")

            self.logger.info(f"📤 INVIO ORDINE KRAKEN: {tipo_ordine_side} {size} {symbol} @ {prezzo_ordine if prezzo_ordine else 'MARKET'} | Leva: {leverage} | Params: {params}")

            # Log di controllo per vedere la trasformazione (es. ETH/BTC -> XETHXXBT)
            self.logger.info(f"🚀 CHIMERA EXEC | {asset} mappato in {symbol} | Side: {tipo_ordine_side} | Type: {tipo_esecuzione}")

            # 3. ESECUZIONE (usando symbol, non asset)
            order = self.exchange.create_order(
                symbol=symbol, 
                type=tipo_esecuzione, 
                side=tipo_ordine_side, 
                amount=size, 
                price=prezzo_ordine, # Sarà None per Market
                params=params
            )

            if order and ('id' in order or 'order_id' in order):
                main_id = order.get('id') or order.get('order_id')
                sl_id = None
                tp_id = None
                
                # Piccola pausa per far digerire il margine a Kraken
                time.sleep(2.0)
                
                # --- PROTEZIONE STOP LOSS ---
                if sl and float(sl) > 0:
                    self.logger.info(f"🛡️ Project Chimera: Inserimento SL a {sl}...")
                    # Passiamo 'symbol' (il nome Kraken) invece di 'asset' (il nome umano)
                    res_sl = self.gestisci_ordine_protezione(
                        asset=symbol, tipo_protezione='stop-loss', prezzo=sl,
                        direzione_aperta=direzione.upper(), size_fallback=size, leverage=leverage
                    )
                    
                    if res_sl and res_sl.get('success'):
                        sl_id = res_sl.get('id')
                        self.logger.info(f"✅ SL inserito: {sl_id}")
                    else:
                        errore_sl = res_sl.get('error', 'Errore sconosciuto')
                        self.logger.critical(f"🚨 FALLIMENTO SL {symbol}: {errore_sl}. EMERGENZA CHIUSURA!")
                        
                        # Panic Sell/Cover: usiamo 'symbol' e la logica della leva filtrata
                        panic_params = {}
                        if leverage and float(leverage) > 1:
                            panic_params['leverage'] = str(int(float(leverage)))
                            
                        panic_params['cl_ord_id'] = str(uuid.uuid4())
                            
                        self.exchange.create_order(
                            symbol=symbol, 
                            type='market', 
                            side='sell' if direzione.upper() in ['BUY', 'LONG'] else 'buy', 
                            amount=size, 
                            params=panic_params
                        )
                        return {'success': False, 'error': f'SL_FAILED: {errore_sl}'}

                # --- PROTEZIONE TAKE PROFIT ---
                if tp and float(tp) > 0:
                    # Anche qui, usiamo 'symbol'
                    res_tp = self.gestisci_ordine_protezione(
                        asset=symbol, tipo_protezione='take-profit', prezzo=tp,
                        direzione_aperta=direzione.upper(), size_fallback=size, leverage=leverage
                    )
                    if res_tp and res_tp.get('success'):
                        tp_id = res_tp.get('id')
                        self.logger.info(f"✅ TP inserito: {tp_id}")

                # Memoria Persistente Chimera: usiamo 'symbol' come chiave per coerenza con l'exchange
                self.ordini_attivi[symbol] = {
                    'order_id': main_id,
                    'sl_id': sl_id,
                    'tp_id': tp_id,
                    'direzione': direzione.upper(),
                    'timestamp': time.time()
                }

                return {
                    'success': True, 
                    'order_id': main_id, 
                    'sl_id': sl_id, 
                    'tp_id': tp_id,
                    'timestamp_apertura': time.time()
                }
            
            return {'success': False, 'error': 'No order ID'}
        except Exception as e:
            _err.capture(e, sys._getframe().f_code.co_name, {"module": "PerformerLA"})
            self.logger.error(f"❌ Errore esecuzione Performer: {e}")
            return {'success': False, 'error': str(e)}
            
    def gestisci_ordine_protezione(self, asset, tipo_protezione, prezzo, direzione_aperta, size_fallback, leverage=1):
        """ 
        VERSIONE CHIMERA: Fix condizionale per reduce_only e leva.
        Risolve l'errore "reduce_only only valid for leveraged orders".
        """
        try:
            # 1. MAPPA DI EMERGENZA (Converte nomi umani in nomi Kraken)
            mappa_tipi = {
                'SL': 'stop-loss',
                'STOP-LOSS': 'stop-loss',
                'TP': 'take-profit',
                'TAKE-PROFIT': 'take-profit'
            }
            
            # Applichiamo la traduzione
            tipo_kraken = mappa_tipi.get(tipo_protezione.upper(), tipo_protezione.lower())
            
            from core import config_la
            if tipo_kraken == 'stop-loss' and getattr(config_la, 'VIRTUAL_STOP_LOSS', False):
                self.logger.info(f"🛡️ Virtual Stop Loss abilitato. Non invio ordine SL a Kraken per {asset} a {prezzo}.")
                return {'success': True, 'id': f"virtual_sl_{int(time.time())}"}
            
            side_chiusura = "sell" if direzione_aperta.upper() in ["BUY", "LONG"] else "buy"
            
            try:
                self.exchange.load_markets()
                prezzo_fmt = self.exchange.price_to_precision(asset, prezzo)
                volume_fmt = self.exchange.amount_to_precision(asset, size_fallback)
            except Exception as e:
                _err.capture(e, sys._getframe().f_code.co_name, {"module": "PerformerLA"})
                self.logger.warning(f"⚠️ Impossibile ottenere precisione dinamica per {asset}: {e}. Uso fallback.")
                prezzo_fmt = self.qprice(asset, prezzo)
                prec_vol = asset_list.get_config(asset).get('vol_precision', 8)
                volume_fmt = "{:.{}f}".format(float(size_fallback), prec_vol).rstrip('0').rstrip('.')
                if not volume_fmt: volume_fmt = "0"
            
            # Prepariamo i parametri base
            params = {
                'pair': asset,
                'type': side_chiusura,
                'ordertype': tipo_kraken, 
                'price': prezzo_fmt,
                'volume': volume_fmt,
                'cl_ord_id': str(uuid.uuid4())
            }

            # --- GESTIONE LEVA PER ORDINI DI PROTEZIONE ---
            # Con private_post_addorder, Kraken NON supporta reduce_only.
            # Per ordini margin (leva > 1) si passa la leva nell'ordine —
            # Kraken capisce da solo che è una chiusura perché è sul lato opposto.
            # NON passare reduce_only qui: causa EGeneral:Invalid arguments.
            current_lev = float(leverage) if leverage else 1

            if current_lev > 1:
                params['leverage'] = str(int(current_lev))
                self.logger.info(f"🛡️ Ordine Margin protezione (Leva {int(current_lev)}). Leva inviata, senza reduce_only.")
            else:
                self.logger.info(f"🛒 Ordine Spot protezione (Leva 1).")

            # --- LOG DI CONTROLLO ---
            self.logger.info(f"DEBUG CHIMERA | Asset: {asset} | Params: {params}")
            
            try:
                res = self.exchange.private_post_addorder(params)
                
                if res and 'result' in res and 'txid' in res['result']:
                    return {'success': True, 'id': res['result']['txid'][0]}
                
                error_msg = str(res.get('error', ''))
                self.logger.error(f"❌ Errore Kraken {asset}: {error_msg}")
                return {'success': False, 'error': error_msg}
                
            except Exception as e:
                _err.capture(e, sys._getframe().f_code.co_name, {"module": "PerformerLA"})
                self.logger.error(f"❌ Eccezione critica {asset}: {e}")
                return {'success': False, 'error': str(e)}
            
        except Exception as e:
            _err.capture(e, sys._getframe().f_code.co_name, {"module": "PerformerLA"})
            self.logger.error(f"❌ Eccezione critica {asset}: {e}")
            return {'success': False, 'error': str(e)}


    def gestisci_take_profit(self, asset, direzione_aperta, nuovo_tp, size, leverage, tp_id=None):
        """
        Versione migliorata GESTIONE DINAMICA TAKE PROFIT.
        - Se nuovo_tp is None: cancella il TP esistente (fase 2).
        - Se nuovo_tp ha valore: aggiorna il TP cancellando il precedente e creando un nuovo limit.
        Restituisce il nuovo tp_id (string) se creato, None altrimenti.
        """
        try:
            from core.asset_list import get_ticker as get_ticker_local
            symbol = get_ticker_local(asset)
            asset_cfg = asset_list.get_config(asset)
            prec = asset_cfg.get('precision', 2)

            # Forza più decimali per coppie crypto-crypto (quote non fiat)
            if symbol and "/" in symbol:
                try:
                    _, quote = symbol.split("/")
                    if quote.upper() not in ("USD", "EUR", "USDT", "ZUSD"):
                        prec = max(prec, 5)
                except Exception:
                    self.logger.debug(f"[PERFORMER] except silenzioso riga 682")

            prec_vol = asset_list.get_config(asset).get('vol_precision', 8)
            size_str = "{:.{}f}".format(float(size), prec_vol).rstrip('0').rstrip('.')
            if not size_str: size_str = "0"

            # --- 1. CANCELLAZIONE TP ESISTENTE ---
            if tp_id:
                try:
                    # Prima prova cancel_order standard (richiede symbol)
                    self.exchange.cancel_order(tp_id, symbol)
                    self.logger.info(f"🗑️ Take Profit precedente {tp_id} rimosso.")
                except Exception as e_cancel:
                    _err.capture(e_cancel, "gestisci_take_profit", {"module": "PerformerLA"})
                    self.logger.warning(f"⚠️ Impossibile cancellare TP {tp_id} tramite cancel_order: {e_cancel}. Provo fallback raw...")
                    # Fallback Kraken raw (se disponibile)
                    try:
                        if hasattr(self.exchange, 'private_post_cancel_order'):
                            payload = {'txid': [tp_id]}
                            self.exchange.private_post_cancel_order(payload)
                            self.logger.info(f"🗑️ Take Profit precedente {tp_id} rimosso via private_post_cancel_order.")
                    except Exception as e_fb:
                        _err.capture(e_fb, "unknown", {"module": "PerformerLA"})
                        self.logger.warning(f"⚠️ Fallback cancel TP fallito: {e_fb}")

            # --- 2. LOGICA FASE 2 (Lascia correre) ---
            if nuovo_tp is None:
                self.logger.info(f"🚀 FASE 2 ATTIVATA per {asset}: Take Profit rimosso, il trade corre con solo SL.")
                return None

            # --- 3. AGGIORNAMENTO TP (Spostamento) ---
            try:
                self.exchange.load_markets()
                prezzo_pulito = float(self.exchange.price_to_precision(symbol, nuovo_tp))
                size_str = self.exchange.amount_to_precision(symbol, size)
            except Exception as e:
                _err.capture(e, sys._getframe().f_code.co_name, {"module": "PerformerLA"})
                self.logger.warning(f"⚠️ Impossibile ottenere precisione dinamica per {symbol}: {e}. Uso fallback.")
                prezzo_pulito = self.qprice(asset, nuovo_tp)
                if not prezzo_pulito:
                    prezzo_pulito = "{:.{}f}".format(float(nuovo_tp), prec)
            
            # FIX 2026-05-09 (Bug #5): includere anche "LONG" nel match.
            # Il sistema usa "LONG"/"SHORT" come convenzione standard (vedi
            # trade_manager.py:343,618,3223). Il vecchio check "== 'BUY'" falliva
            # per direzione_aperta="LONG" → side_chiusura='buy' invece di 'sell'
            # → invece di chiudere il long, lo raddoppiava.
            side_chiusura = 'sell' if direzione_aperta.upper() in ('BUY', 'LONG') else 'buy'

            self.logger.info(f"🎯 Aggiornamento Take Profit per {asset} a {prezzo_pulito} (symbol={symbol}, prec={prec})...")

            try:
                params_tp = {'leverage': leverage}
                params_tp['cl_ord_id'] = str(uuid.uuid4())
                    
                nuovo_ordine = self.exchange.create_order(
                    symbol=symbol,
                    type='limit',
                    side=side_chiusura,
                    amount=float(size_str),
                    price=prezzo_pulito,
                    params=params_tp
                )
                
                if nuovo_ordine and ('id' in nuovo_ordine or 'txid' in nuovo_ordine):
                    new_id = nuovo_ordine.get('id') or nuovo_ordine.get('txid')
                    if isinstance(new_id, list):
                        new_id = new_id[0]
                    self.logger.info(f"✅ Nuovo Take Profit ATTIVO: {new_id}")
                    return new_id

                if isinstance(nuovo_ordine, dict) and 'result' in nuovo_ordine and 'txid' in nuovo_ordine['result']:
                    tx = nuovo_ordine['result']['txid'][0]
                    self.logger.info(f"✅ Nuovo Take Profit ATTIVO (raw): {tx}")
                    return tx
                    
                self.logger.warning(f"⚠️ Creazione TP non ha restituito ID: {nuovo_ordine}")
                return None
                
            except Exception as e_order:
                _err.capture(e_order, sys._getframe().f_code.co_name, {"module": "PerformerLA"})
                self.logger.error(f"🔴 ERRORE AGGIORNAMENTO TAKE PROFIT {asset}: {e_order}")
                return None

            except Exception as e:
                _err.capture(e, sys._getframe().f_code.co_name, {"module": "PerformerLA"})
                self.logger.error(f"🔴 ERRORE AGGIORNAMENTO TAKE PROFIT {asset}: {e}")
                return None

        except Exception as e:
            _err.capture(e, sys._getframe().f_code.co_name, {"module": "PerformerLA"})
            self.logger.error(f"🔴 ERRORE GESTISCI_TP PREPARAZIONE {asset}: {e}")
            return None

    def sposta_stop_loss(self, asset, direzione_aperta, nuovo_sl, size, leverage=1, sl_id=None):
        """
        Versione migliorata sposta_stop_loss:
        - Cancella l'ordine precedente (se possibile) e crea uno nuovo.
        - Retry automatico decrescendo la precisione se Kraken rifiuta 'Invalid price'.
        - Ritorna txid (string) se disponibile, True se ok senza txid, False su errore.
        """
        try:
            from core import config_la
            is_virtual_sl = getattr(config_la, 'VIRTUAL_STOP_LOSS', False)
            
            # --- 1. PULIZIA PREVENTIVA (cancellazione SL precedente) ---
            try:
                if sl_id:
                    self.cancella_ordine_specifico(sl_id)
                else:
                    # Se non abbiamo l'ID, pulizia generica degli ordini aperti per il symbol
                    try:
                        self.pulizia_totale_ordini(asset)
                        self.logger.info(f"🧹 Pulizia totale ordini eseguita per {asset}.")
                    except Exception as e_pul:
                        _err.capture(e_pul, "sposta_stop_loss", {"module": "PerformerLA"})
                        self.logger.warning(f"⚠️ Pulizia totale ordini fallita: {e_pul}")
            except Exception as e_cancel_gen:
                _err.capture(e_cancel_gen, "sposta_stop_loss", {"module": "PerformerLA"})
                self.logger.warning(f"⚠️ Nota: Cancellazione preventiva ha sollevato errore: {e_cancel_gen}")

            if is_virtual_sl:
                self.logger.info(f"🛡️ Virtual Stop Loss abilitato. Spostamento SL virtuale per {asset} a {nuovo_sl}.")
                return f"virtual_sl_{int(time.time())}"

            from core import asset_list as al_config
            from core.asset_list import ASSET_CONFIG, get_ticker as get_ticker_local

            symbol = al_config.get_ticker(asset)
            asset_cfg = asset_list.get_config(asset)

            # Calcolo precisione: priorità ASSET_CONFIG ma forziamo >=5 per crypto-crypto non fiat
            prec = asset_cfg.get('precision', 2)
            if symbol and "/" in symbol:
                _, quote = symbol.split("/")
                if quote.upper() not in ("USD", "EUR", "USDT", "ZUSD"):
                    prec = max(prec, 5)

            prec_vol = asset_list.get_config(asset).get('vol_precision', 8)
            size_str = "{:.{}f}".format(float(size), prec_vol).rstrip('0').rstrip('.')
            if not size_str: size_str = "0"

            self.logger.info(f"🛡️ Tentativo spostamento SL per {asset} ({symbol}) a {nuovo_sl} (prec iniziale: {prec})...")

            try:
                self.exchange.load_markets()
                prezzo_pulito = float(self.exchange.price_to_precision(symbol, nuovo_sl))
                size_str = self.exchange.amount_to_precision(symbol, size)
            except Exception as e:
                _err.capture(e, sys._getframe().f_code.co_name, {"module": "PerformerLA"})
                self.logger.warning(f"⚠️ Impossibile ottenere precisione dinamica per {symbol}: {e}. Uso fallback.")
                prezzo_pulito = self.qprice(asset, nuovo_sl)
                if not prezzo_pulito:
                    prezzo_pulito = "{:.{}f}".format(float(nuovo_sl), prec)

            # --- 2. CREAZIONE NUOVO SL ---
            # FIX 2026-05-09 (Bug #6): includere anche "LONG" nel match.
            # Stessa root cause del Bug #5 in gestisci_take_profit (riga ~915).
            # Il sistema usa "LONG"/"SHORT" come convenzione standard.
            side_chiusura = 'sell' if direzione_aperta.upper() in ('BUY', 'LONG') else 'buy'
            params_nuovo = {
                'pair': symbol.replace('/', ''),
                'type': side_chiusura,
                'ordertype': 'stop-loss',
                'volume': size_str,
                'price': prezzo_pulito,
                'cl_ord_id': str(uuid.uuid4())
            }
            
            if leverage and float(leverage) > 1:
                params_nuovo['leverage'] = str(leverage)

            try:
                self.logger.info(f"📡 Invio SL a Kraken: price={params_nuovo['price']}, volume={params_nuovo['volume']}")
                res = self.exchange.private_post_addorder(params_nuovo)
                if res and isinstance(res, dict) and 'result' in res and 'txid' in res['result']:
                    nuovo_txid = res['result']['txid'][0]
                    self.logger.info(f"✅ Nuovo Stop Loss ATTIVO per {asset}: {nuovo_txid}")
                    return nuovo_txid
                if res and ('error' in res and res['error']):
                    self.logger.error(f"❌ Errore API Kraken: {res.get('error')}")
                    return False
                return True
            except Exception as e_retry:
                _err.capture(e_retry, sys._getframe().f_code.co_name, {"module": "PerformerLA"})
                self.logger.error(f"❌ Errore spostamento SL {asset}: {e_retry}")
                return False

        except Exception as e:
            _err.capture(e, sys._getframe().f_code.co_name, {"module": "PerformerLA"})
            self.logger.error(f"🔴 ERRORE GESTISCI_TP PREPARAZIONE {asset}: {e}")
            return False

    def aggiorna_stop_loss(self, asset, direzione, nuovo_sl, size, leverage=1, sl_id=None):
        """
        Wrapper compatibile con TradeManager: aggiorna (cancella+ricrea) lo SL su Kraken.
        Restituisce True se l'operazione è riuscita.
        """
        try:
            res = self.sposta_stop_loss(
                asset=asset,
                direzione_aperta=direzione,
                nuovo_sl=nuovo_sl,
                size=size,
                leverage=leverage,
                sl_id=sl_id
            )
            # sposta_stop_loss ritorna txid oppure True/False; normalizziamo a bool
            return bool(res)
        except Exception as e:
            _err.capture(e, sys._getframe().f_code.co_name, {"module": "PerformerLA"})
            self.logger.error(f"🔴 ERRORE aggiorna_stop_loss per {asset}: {e}")
            return False
    
    def pulizia_totale_ordini(self, asset):
        """ Rimuove ogni ordine orfano per l'asset specificato e attende la cancellazione """
        import time
        # FIX InvalidNonce (2026-05-07): lock serializza il burst di chiamate
        # private_post_openorders + cancella_ordine_specifico*N + polling 5x.
        # Senza lock il polling si scontra con _get_ws_token / sincronizza_con_exchange.
        with self._private_lock:
            try:
                symbol = asset_list.get_ticker(asset)
                self.logger.debug(f"🧹 Pulizia orfani per {asset}...")

                # Sostituiamo fetch_open_orders con chiamata diretta per evitare load_markets bloccanti
                res = self.exchange.private_post_openorders()
                open_orders = res.get('result', {}).get('open', {})

                if not open_orders:
                    return True

                count_cancellati = 0
                for order_id, o_data in open_orders.items():
                    descr = o_data.get('descr', {})
                    o_ticker = descr.get('pair', '')

                    # Normalizziamo entrambi per il confronto
                    if self._normalize_ticker(o_ticker) == self._normalize_ticker(symbol):
                        if self.cancella_ordine_specifico(order_id):
                            count_cancellati += 1

                if count_cancellati > 0:
                    self.logger.info(f"⏳ Attesa cancellazione effettiva di {count_cancellati} ordini...")
                    # Attendiamo fino a 5 secondi che gli ordini spariscano
                    for _ in range(5):
                        time.sleep(1.0)
                        res_check = self.exchange.private_post_openorders()
                        open_orders_check = res_check.get('result', {}).get('open', {})
                        ancora_aperti = 0
                        for o_id, o_data in open_orders_check.items():
                            descr = o_data.get('descr', {})
                            o_ticker = descr.get('pair', '')
                            if self._normalize_ticker(o_ticker) == self._normalize_ticker(symbol):
                                ancora_aperti += 1

                        if ancora_aperti == 0:
                            self.logger.info(f"✅ Tutti gli ordini per {asset} sono stati cancellati con successo.")
                            break
                        else:
                            self.logger.info(f"⏳ Ancora {ancora_aperti} ordini pendenti per {asset}...")

                if count_cancellati > 0:
                    self.logger.info(f"✨ Pulizia: {count_cancellati} ordini rimossi per {asset}.")
                else:
                    self.logger.debug(f"✨ Pulizia {asset}: 0 ordini da rimuovere.")
                return True
            except Exception as e:
                _err.capture(e, sys._getframe().f_code.co_name, {"module": "PerformerLA"})
                self.logger.error(f"🔴 Errore pulizia orfani: {e}")
                return False