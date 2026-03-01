# -*- coding: utf-8 -*-
"""
L&A Institutional Bot - PerformerLA
Versione 2.0: FIX SINTASSI KRAKEN & STOP LOSS REAL-TIME
"""

import logging
import ccxt
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

    def esegui_ordine(self, asset, direzione, size, voto, modalita='MARGIN', leverage=1, sl=None, tp=None, razionale=None):
        """
        Esegue ordine Market su Kraken con VERIFICA REALE post-esecuzione.
        Assicura che posizione e protezioni siano attive prima di confermare il trade.
        """
        import time
        if razionale:
            self.logger.info(f"🧠 MOTIVAZIONE IA: {razionale}")

        try:
            symbol = asset_list.get_ticker(asset)
            from core.asset_list import ASSET_CONFIG
            asset_cfg = ASSET_CONFIG.get(asset, {})

            if "XBT" in symbol and "ZUSD" in symbol:
                prec = 1
            elif "ETH" in symbol and "ZUSD" in symbol:
                prec = 2
            elif "ETH" in symbol and "XBT" in symbol:
                prec = 6
            else:
                prec = asset_cfg.get('precision', 2)

            self.logger.info(f"📍 Precisione applicata per {symbol}: {prec} decimali")

            # --- FIX SICUREZZA AGGIUNTIVO ---
            size_val = float(size)
            size_str = "{:.8f}".format(size_val).rstrip('0').rstrip('.')

            order_side = 'buy' if direzione.upper() == 'BUY' else 'sell'
            market_params = {'leverage': leverage} if modalita.upper() == 'MARGIN' else {}

            self.logger.info(f"🚀 INVIO ORDINE MARKET KRAKEN: {order_side.upper()} {size_str} {symbol}")

            # ESECUZIONE MARKET
            order = self.exchange.create_order(
                symbol=symbol,
                type='market',
                side=order_side,
                amount=float(size_str),  # Qui va bene float per la size
                params=market_params
            )

            if order and 'id' in order:
                self.logger.info(f"✅ ORDINE MARKET ESEGUITO. ID: {order['id']}")

                # --- PAUSA TECNICA PER AGGIORNAMENTO MARGIN ENGINE ---
                time.sleep(2.5)

                # --- 1. VERIFICA POSIZIONE REALE ---
                pos_reale = self.get_open_positions(asset)
                if not pos_reale:
                    self.logger.error(f"🚨 FALLIMENTO CRITICO: Posizione {asset} NON rilevata su Kraken dopo Market Order!")
                    return {'success': False, 'error': 'POSIZIONE_NON_TROVATA', 'order_id': order['id']}

                side_chiusura = 'sell' if direzione.upper() == 'BUY' else 'buy'
                sl_txid = None
                tp_txid = None

                # A. Stop Loss
                if sl:
                    try:
                        prezzo_sl = "{:.{}f}".format(float(sl), prec)
                        params_sl = {
                            'pair': symbol.replace('/', ''),
                            'type': side_chiusura,
                            'ordertype': 'stop-loss',
                            'price': prezzo_sl,
                            'volume': size_str,
                            'leverage': str(leverage)
                        }
                        res_sl = self.exchange.private_post_addorder(params_sl)
                        if res_sl and 'result' in res_sl:
                            sl_txid = res_sl['result']['txid'][0]
                            self.logger.info(f"✅ SL confermato (ID: {sl_txid})")
                    except Exception as e_sl:
                        self.logger.error(f"⚠️ Errore SL: {e_sl}")

                # B. Take Profit
                if tp:
                    try:
                        tp_price = "{:.{}f}".format(float(tp), prec)
                        res_tp = self.exchange.create_order(
                            symbol=symbol,
                            type='limit',
                            side=side_chiusura,
                            amount=float(size_str),
                            price=tp_price,
                            params={'leverage': leverage}
                        )
                        if res_tp and 'id' in res_tp:
                            tp_txid = res_tp['id']
                            self.logger.info(f"✅ TP confermato (ID: {tp_txid})")
                    except Exception as e_tp:
                        self.logger.error(f"⚠️ Errore TP: {e_tp}")

                # --- 2. VERIFICA FINALE PROTEZIONI ---
                time.sleep(1.0)
                if sl and not sl_txid:
                    self.logger.warning(f"🚨 ATTENZIONE: Posizione aperta per {asset} ma STOP LOSS MANCANTE!")

                self.logger.info(f"🏁 ESECUZIONE COMPLETATA E VERIFICATA PER {asset}")
                return {
                    'success': True,
                    'order_id': order['id'],
                    'sl_id': sl_txid,
                    'tp_id': tp_txid
                }

            return {'success': False, 'error': 'No order ID from Kraken'}

        except Exception as e:
            self.logger.error(f"🔴 FALLIMENTO TOTALE ESECUZIONE: {e}")
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

    def chiudi_posizione_totale(self, asset, direzione_aperta, size, leverage=1):
        """ Chiude la posizione immediatamente a mercato """
        direzione_chiusura = 'SELL' if direzione_aperta.upper() == 'BUY' else 'BUY'
        self.logger.info(f"⚖️ TENTATIVO CHIUSURA TOTALE POSIZIONE {asset}")

        # Per chiudere una posizione margin, serve un ordine market con lo stesso leverage
        return self.esegui_ordine(
            asset=asset,
            direzione=direzione_chiusura,
            size=size,
            voto=0,
            modalita='MARGIN',
            leverage=leverage
        )

    def pulizia_totale_ordini(self, asset):
        """Cancella TUTTI gli ordini aperti per un determinato asset (usa il ticker Kraken)."""
        try:
            # Ricava il ticker Kraken (es. 'ETH/XBT' o 'XBT/ZUSD')
            symbol = asset_list.get_ticker(asset)
            if not symbol:
                self.logger.warning(f"⚠️ Impossibile determinare ticker per {asset}. Nessuna azione di pulizia.")
                return False

            ordini_aperti = []
            try:
                ordini_aperti = self.exchange.fetch_open_orders(symbol)
            except Exception as e_fetch:
                # fallback: prova a chiamare senza argomento (alcune impl. CCXT lo supportano)
                self.logger.warning(f"⚠️ fetch_open_orders({symbol}) fallito: {e_fetch}. Provo fetch_open_orders() generico...")
                try:
                    ordini_aperti = self.exchange.fetch_open_orders()
                except Exception as e_fetch2:
                    self.logger.error(f"❌ Impossibile recuperare ordini aperti: {e_fetch2}")
                    return False

            for ordine in ordini_aperti:
                ordine_id = ordine.get('id') or ordine.get('orderId') or ordine.get('txid')
                try:
                    self.logger.info(f"🧹 Cancellazione ordine orfano {ordine_id} per {symbol}")
                    # cancel_order richiede il symbol in molte implementazioni
                    self.exchange.cancel_order(ordine_id, symbol)
                except Exception as e_cancel:
                    self.logger.warning(f"⚠️ Impossibile cancellare ordine {ordine_id} ({symbol}): {e_cancel}")
                    # prova fallback Kraken raw (se disponibile)
                    try:
                        if hasattr(self.exchange, 'private_post_cancel_order'):
                            payload = {'txid': [ordine_id]}
                            self.exchange.private_post_cancel_order(payload)
                            self.logger.info(f"🗑️ Ordine {ordine_id} cancellato via private_post_cancel_order.")
                    except Exception:
                        pass
            return True
        except Exception as e:
            self.logger.error(f"❌ Errore durante la pulizia ordini per {asset}: {e}")
            return False

    def get_open_positions(self, asset):
        """
        Verifica se esiste una posizione margin aperta su Kraken per l'asset specificato.
        Ritorna i dati della posizione o None se chiusa.
        """
        try:
            # Recuperiamo le posizioni aperte (margin)
            positions = self.exchange.private_post_openpositions()

            if not positions or 'result' not in positions:
                return None

            # Kraken usa nomi come 'ETHUSD' o 'XBTUSD' nei risultati margin
            ticker_base = asset_list.get_ticker(asset).replace('/', '')

            for pos_id, pos_data in positions['result'].items():
                pair_kraken = pos_data['pair']
                # Controlla se il ticker combacia o se ha la variante XBT invece di BTC
                if pair_kraken == ticker_base or pair_kraken == ticker_base.replace('BTC', 'XBT'):
                    return pos_data

            return None

        except Exception as e:
            self.logger.error(f"⚠️ Errore recupero posizioni aperte per {asset}: {e}")
            return None