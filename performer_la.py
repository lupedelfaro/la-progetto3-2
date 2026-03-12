# -*- coding: utf-8 -*-
"""
L&A Institutional Bot - PerformerLA
Versione 3.0: CHIMERA INTEGRATION & DYNAMIC PRECISION
"""

import logging
import ccxt
import time
from datetime import datetime
from core import asset_list
from core import config_la

class PerformerLA:
    def __init__(self, exchange_id=None):
        exchange_id = exchange_id or 'kraken'
        self.exchange = getattr(ccxt, exchange_id)({
            'apiKey': config_la.KRAKEN_KEY,
            'secret': config_la.KRAKEN_SECRET,
            'enableRateLimit': True,
            'timeout': 30000,
            'options': {'defaultType': 'spot'}
        })
        self.logger = logging.getLogger("PerformerLA")
        # Questo dizionario manterrà gli ID finché il bot è acceso
        self.ordini_attivi = {}
        
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
                self.exchange.load_markets()
            
            # Se l'asset è in asset_list, usiamo il ticker ufficiale
            symbol_ufficiale = asset_list.get_ticker(symbol)
            return self.exchange.price_to_precision(symbol_ufficiale, float(p))
        except:
            # Fallback manuale se il caricamento mercati fallisce
            if "XBT" in symbol or "BTC" in symbol: return f"{float(p):.1f}"
            if "ETH" in symbol: return f"{float(p):.2f}"
            return str(p)

    def get_current_price(self, asset):
        """ Recupera l'ultimo prezzo di mercato per un asset. """
        try:
            symbol = asset_list.get_ticker(asset)
            ticker = self.exchange.fetch_ticker(symbol)
            return ticker['last']
        except Exception as e:
            self.logger.error(f"❌ Errore recupero prezzo {asset}: {e}")
            return None

    def get_open_positions_real(self):
        """ 
        Recupera posizioni e ordini usando esclusivamente i codici Kraken originali. 
        Restituisce un dizionario mappato sui TICKER per sincronizzazione immediata.
        """
        try:
            # 1. Recupero posizioni aperte a margine
            pos_res = self.exchange.private_post_openpositions()
            raw_positions = pos_res.get('result', {})
            if not raw_positions: return {}

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
                for o_id, o_data in open_orders.items():
                    descr = o_data.get('descr', {})
                    o_ticker = descr.get('pair', '')
                    
                    if o_ticker == ticker_k:
                        tipo = str(descr.get('ordertype', '')).lower()
                        
                        # Cattura ID Stop Loss
                        if 'stop' in tipo: 
                            p_data['has_sl'] = True
                            p_data['sl_id_kraken'] = o_id
                            
                        # Cattura ID Take Profit (limit o take-profit)
                        elif 'profit' in tipo or 'limit' in tipo: 
                            p_data['has_tp'] = True
                            p_data['tp_id_kraken'] = o_id
                
                # Inseriamo nel dizionario usando il TICKER come chiave
                mapped_positions[ticker_k] = p_data
            
            return mapped_positions
            
        except Exception as e:
            self.logger.error(f"❌ Errore critico nel matching ordini Kraken: {e}")
            return {}
            
    def _normalize_ticker(self, ticker):
        if not ticker: return ""
        t = str(ticker).upper().replace("/", "").replace(" ", "")
        if "XBT" in t or "BTC" in t: return "BTC"
        if "ETH" in t: return "ETH"
        return t

    def esegui_ordine(self, asset, direzione, size, leverage, voto, sl=None, tp=None, tipo_op="Swing"):
        try:
            # 1. IL FIX DEFINITIVO PER I NOMI UMANI
            # Non importa cosa riceve (asset), noi usiamo SOLO il ticker ufficiale di Kraken
            symbol = asset_list.get_ticker(asset)
            
            # Se per qualche motivo asset_list non restituisce nulla, fermiamo tutto prima dell'errore
            if not symbol:
                self.logger.error(f"❌ Asset {asset} non trovato in ASSET_LIST. Operazione annullata.")
                return {'success': False, 'error': 'Invalid Ticker'}

            # Da qui in poi, il bot userà SEMPRE 'symbol' (es. XETHXXBT) e mai più 'asset'
            tipo_ordine_side = 'buy' if direzione.upper() in ["BUY", "LONG"] else 'sell'
            tipo_esecuzione = 'market'

            # 2. FIX LEVA (per evitare 'Invalid arguments')
            params = {'trading_agreement': 'agree'}
            if leverage and float(leverage) > 1:
                params['leverage'] = str(leverage)

            # Log di controllo per vedere la trasformazione (es. ETH/BTC -> XETHXXBT)
            self.logger.info(f"🚀 CHIMERA EXEC | {asset} mappato in {symbol} | Side: {tipo_ordine_side}")

            # 3. ESECUZIONE (usando symbol, non asset)
            order = self.exchange.create_order(
                symbol=symbol, 
                type=tipo_esecuzione, 
                side=tipo_ordine_side, 
                amount=size, 
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
                        panic_params = {'trading_agreement': 'agree'}
                        if leverage and float(leverage) > 1:
                            panic_params['leverage'] = str(leverage)
                            
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
            
            side_chiusura = "sell" if direzione_aperta.upper() in ["BUY", "LONG"] else "buy"
            prezzo_fmt = self.qprice(asset, prezzo)
            
            # Prepariamo i parametri base
            params = {
                'pair': asset,
                'type': side_chiusura,
                'ordertype': tipo_kraken, 
                'price': prezzo_fmt,
                'volume': str(size_fallback),
                'trading_agreement': 'agree'
            }

            # --- FIX CRUCIALE CHIMERA: GESTIONE LEVA E REDUCE_ONLY ---
            # Kraken accetta reduce_only SOLO se la leva è > 1
            current_lev = float(leverage) if leverage else 1
            
            if current_lev > 1:
                params['leverage'] = str(int(current_lev))
                params['reduce_only'] = True
                self.logger.info(f"🛡️ Ordine Margin rilevato (Leva {int(current_lev)}). Attivazione reduce_only.")
            else:
                # Per ordini SPOT (leverage 1), reduce_only deve essere ASSENTE
                # Non mettiamo params['reduce_only'] = False, lo escludiamo proprio.
                self.logger.info(f"🛒 Ordine Spot rilevato (Leva 1). Disattivazione reduce_only.")

            # --- LOG DI CONTROLLO ---
            self.logger.info(f"DEBUG CHIMERA | Asset: {asset} | Params: {params}")
            
            res = self.exchange.private_post_addorder(params)
            
            if res and 'result' in res and 'txid' in res['result']:
                return {'success': True, 'id': res['result']['txid'][0]}
            
            error_msg = str(res.get('error'))
            self.logger.error(f"❌ Errore Kraken {asset}: {error_msg}")
            return {'success': False, 'error': error_msg}
            
        except Exception as e:
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
            from core.asset_list import ASSET_CONFIG, get_ticker as get_ticker_local
            symbol = get_ticker_local(asset)
            asset_cfg = ASSET_CONFIG.get(asset, {})
            prec = asset_cfg.get('precision', 2)

            # Forza più decimali per coppie crypto-crypto (quote non fiat)
            if symbol and "/" in symbol:
                try:
                    _, quote = symbol.split("/")
                    if quote.upper() not in ("USD", "EUR", "USDT", "ZUSD"):
                        prec = max(prec, 5)
                except Exception:
                    pass

            size_str = "{:.8f}".format(float(size)).rstrip('0').rstrip('.')

            # --- 1. CANCELLAZIONE TP ESISTENTE ---
            if tp_id:
                try:
                    # Prima prova cancel_order standard (richiede symbol)
                    self.exchange.cancel_order(tp_id, symbol)
                    self.logger.info(f"🗑️ Take Profit precedente {tp_id} rimosso.")
                except Exception as e_cancel:
                    self.logger.warning(f"⚠️ Impossibile cancellare TP {tp_id} tramite cancel_order: {e_cancel}. Provo fallback raw...")
                    # Fallback Kraken raw (se disponibile)
                    try:
                        if hasattr(self.exchange, 'private_post_cancel_order'):
                            payload = {'txid': [tp_id]}
                            self.exchange.private_post_cancel_order(payload)
                            self.logger.info(f"🗑️ Take Profit precedente {tp_id} rimosso via private_post_cancel_order.")
                    except Exception as e_fb:
                        self.logger.warning(f"⚠️ Fallback cancel TP fallito: {e_fb}")

            # --- 2. LOGICA FASE 2 (Lascia correre) ---
            if nuovo_tp is None:
                self.logger.info(f"🚀 FASE 2 ATTIVATA per {asset}: Take Profit rimosso, il trade corre con solo SL.")
                return None

            # --- 3. AGGIORNAMENTO TP (Spostamento) ---
            prezzo_pulito = "{:.{}f}".format(float(nuovo_tp), prec)
            side_chiusura = 'sell' if direzione_aperta.upper() == 'BUY' else 'buy'

            self.logger.info(f"🎯 Aggiornamento Take Profit per {asset} a {prezzo_pulito} (symbol={symbol}, prec={prec})...")

            try:
                # Creazione nuovo ordine Limit per il Take Profit
                nuovo_ordine = self.exchange.create_order(
                    symbol=symbol,
                    type='limit',
                    side=side_chiusura,
                    amount=float(size_str),
                    price=prezzo_pulito,
                    params={'leverage': leverage}
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

            except Exception as e:
                self.logger.error(f"🔴 ERRORE AGGIORNAMENTO TAKE PROFIT {asset}: {e}")
                return None

        except Exception as e:
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
            from core import asset_list as al_config
            from core.asset_list import ASSET_CONFIG, get_ticker as get_ticker_local

            symbol = al_config.get_ticker(asset)
            asset_cfg = ASSET_CONFIG.get(asset, {})

            # Calcolo precisione: priorità ASSET_CONFIG ma forziamo >=5 per crypto-crypto non fiat
            prec = asset_cfg.get('precision', 2)
            if symbol and "/" in symbol:
                _, quote = symbol.split("/")
                if quote.upper() not in ("USD", "EUR", "USDT", "ZUSD"):
                    prec = max(prec, 5)

            size_str = "{:.8f}".format(float(size)).rstrip('0').rstrip('.')

            self.logger.info(f"🛡️ Tentativo spostamento SL per {asset} ({symbol}) a {nuovo_sl} (prec iniziale: {prec})...")

            # --- 1. PULIZIA PREVENTIVA (cancellazione SL precedente) ---
            try:
                if sl_id:
                    try:
                        # Primo tentativo con CCXT standard
                        self.exchange.cancel_order(sl_id, symbol)
                        self.logger.info(f"🗑️ Vecchio SL {sl_id} cancellato tramite cancel_order.")
                    except Exception as e_cancel_ccxt:
                        self.logger.warning(f"⚠️ cancel_order fallito per {sl_id}: {e_cancel_ccxt}. Provo fallback Kraken TXID...")
                        # Fallback: tentativo con endpoint privato Kraken (se presente)
                        try:
                            if hasattr(self.exchange, 'private_post_cancel_order'):
                                payload = {'txid': [sl_id]}
                                self.exchange.private_post_cancel_order(payload)
                                self.logger.info(f"🗑️ Vecchio SL {sl_id} cancellato tramite private_post_cancel_order.")
                            else:
                                raise Exception("Nessun endpoint private_post_cancel_order disponibile")
                        except Exception as e_cancel_kraken:
                            self.logger.warning(f"⚠️ Fallback cancelazione TXID fallito: {e_cancel_kraken}")
                else:
                    # Se non abbiamo l'ID, pulizia generica degli ordini aperti per il symbol
                    try:
                        self.pulizia_totale_ordini(asset)
                        self.logger.info(f"🧹 Pulizia totale ordini eseguita per {symbol}.")
                    except Exception as e_pul:
                        self.logger.warning(f"⚠️ Pulizia totale ordini fallita: {e_pul}")
            except Exception as e_cancel_gen:
                self.logger.warning(f"⚠️ Nota: Cancellazione preventiva ha sollevato errore: {e_cancel_gen}")

            # --- 2. CREAZIONE NUOVO SL (con retry decrescente precisione) ---
            side_chiusura = 'sell' if direzione_aperta.upper() == 'BUY' else 'buy'
            params_nuovo = {
                'pair': symbol.replace('/', ''),
                'type': side_chiusura,
                'ordertype': 'stop-loss',
                'volume': size_str,
                'price': "{:.{}f}".format(float(nuovo_sl), prec),
                'leverage': str(leverage)
            }

            last_exception = None
            attempt = 0
            max_attempts = prec + 2 if prec >= 0 else 3  # numero tentativi basato sulla precisione

            while attempt < max_attempts:
                attempt += 1
                try:
                    self.logger.info(f"📡 Invio SL (attempt {attempt}/{max_attempts}) a Kraken: price={params_nuovo['price']}, volume={params_nuovo['volume']}")
                    res = self.exchange.private_post_addorder(params_nuovo)
                    # Se non solleva eccezione, prosegui
                    if res and isinstance(res, dict) and 'result' in res and 'txid' in res['result']:
                        nuovo_txid = res['result']['txid'][0]
                        self.logger.info(f"✅ Nuovo Stop Loss ATTIVO per {asset}: {nuovo_txid}")
                        return nuovo_txid
                    # Se la risposta non contiene txid, ritorniamo True come segnale di successo generico
                    if res and ('error' in res and res['error']):
                        last_exception = Exception(f"Errore API Kraken: {res.get('error')}")
                        raise last_exception
                    return True
                except Exception as e_retry:
                    last_exception = e_retry
                    msg = str(e_retry).lower()
                    # Se sembra un problema di price/precisione, scala la precisione
                    if ("invalid price" in msg or "precision" in msg or "price" in msg) and params_nuovo.get('price'):
                        try:
                            # Riduci la precisione di 1 e ricostruisci il campo price
                            prec = max(0, prec - 1)
                            params_nuovo['price'] = "{:.{}f}".format(float(nuovo_sl), prec)
                            self.logger.warning(f"🔄 Errore price/precisione rilevato. Riprovo con precisione ridotta: {prec} decimali -> price {params_nuovo['price']}")
                            time.sleep(0.5)
                            continue
                        except Exception as e_prec:
                            self.logger.warning(f"⚠️ Errore ricalcolo precisione: {e_prec}")
                            break
                    # Se l'errore è di margine o altro non recuperabile, rilancia
                    self.logger.error(f"🔴 Tentativo SL fallito (attempt {attempt}): {e_retry}")
                    # breve sleep prima del prossimo tentativo non-precisione
                    time.sleep(0.5)
                    continue

            # Se siamo qui, tutti i tentativi sono falliti
            if last_exception:
                self.logger.error(f"🔴 ERRORE GLOBALE creazione SL per {asset}: {last_exception}")
            return False

        except Exception as e:
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
            self.logger.error(f"🔴 ERRORE aggiorna_stop_loss per {asset}: {e}")
            return False
    
    def pulizia_totale_ordini(self, asset):
        """ Rimuove ogni ordine orfano per l'asset specificato """
        try:
            symbol = asset_list.get_ticker(asset)
            self.logger.info(f"🧹 Pulizia orfani per {asset}...")
            
            ordini_aperti = self.exchange.fetch_open_orders(symbol)
            if not ordini_aperti:
                return True

            count_cancellati = 0
            for ordine in ordini_aperti:
                order_id = ordine.get('id')
                if order_id:
                    if self.cancella_ordine_specifico(order_id):
                        count_cancellati += 1
            
            self.logger.info(f"✨ Pulizia completata: {count_cancellati} ordini rimossi.")
            return True
        except Exception as e:
            self.logger.error(f"🔴 Errore pulizia orfani: {e}")
            return False
