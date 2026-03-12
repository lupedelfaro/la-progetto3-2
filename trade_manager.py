# -*- coding: utf-8 -*-
import logging
import json
import os
import time
from datetime import datetime, timedelta
from core import asset_list

class TradeManager:
    def __init__(self, file_posizioni="posizioni_aperte.json", alerts=None, performer=None, feedback_engine=None):
        self.file_posizioni = file_posizioni
        self.file_storico = "storico_trades.json"
        self.file_stats = "stats_globali.json"
        self.logger = logging.getLogger("TradeManager")
        self.alerts = alerts
        self.performer = performer
        
        self.posizioni_aperte = self._carica_posizioni()
        self.storico_trades = self._carica_storico()
        self.stats_globali = self._carica_stats_globali()
        self.feedback_engine = feedback_engine 
  
    # Esempio di fix da applicare dentro trade_manager.py
    def formatta_prezzo(self, asset, prezzo):
        """Normalizza i decimali usando la precisione definita in asset_list.py."""
        try:
            # Recupera la precisione da ASSET_CONFIG dentro asset_list.py
            precision = asset_list.ASSET_CONFIG.get(asset, {}).get('precision', 2)
            return f"{float(prezzo):.{precision}f}"
        except Exception as e:
            self.logger.warning(f"⚠️ Errore formattazione prezzo per {asset}: {e}")
            return str(prezzo)
            
    def set_take_profit(self, asset, tp_price, size, direzione, leverage):
        """Invia TP usando la funzione unificata."""
        try:
            self.logger.info(f"🎯 Invio TP Istituzionale: {asset} a {tp_price}")
            return self.performer.gestisci_ordine_protezione(
                asset=asset, 
                tipo_protezione='take-profit', 
                prezzo=tp_price, 
                direzione_aperta=direzione, 
                size_fallback=size, 
                leverage=leverage
            )
        except Exception as e:
            self.logger.error(f"❌ Errore critico set_take_profit {asset}: {e}")

    def set_stop_loss(self, asset, sl_price, size, direzione, leverage):
        """Invia SL usando la funzione unificata."""
        try:
            self.logger.info(f"🛡️ Invio SL Istituzionale: {asset} a {sl_price}")
            return self.performer.gestisci_ordine_protezione(
                asset=asset, 
                tipo_protezione='stop-loss', 
                prezzo=sl_price, 
                direzione_aperta=direzione, 
                size_fallback=size, 
                leverage=leverage
            )
        except Exception as e:
            self.logger.error(f"❌ Errore critico set_stop_loss {asset}: {e}")
    
    def get_balance_margin(self, currency="USD"):
        """Recupera il saldo disponibile per operazioni in leva (Spot Margin)."""
        try:
            balances = self.exchange.fetch_balance()
            # Su Kraken 'free' indica il capitale non impegnato in ordini, usabile come margine
            return float(balances.get('free', {}).get(currency, 0))
        except Exception as e:
            self.logger.error(f"🔴 Errore recupero balance margin {currency}: {e}")
            return 0.0

    def get_current_price(self, asset):
        """Recupera l'ultimo prezzo battuto (Last) per il calcolo della size."""
        try:
            symbol = asset_list.get_ticker(asset)
            ticker = self.exchange.fetch_ticker(symbol)
            return float(ticker['last'])
        except Exception as e:
            self.logger.error(f"🔴 Errore recupero prezzo per {asset}: {e}")
            return None
    
    def _carica_posizioni(self):
        if os.path.exists(self.file_posizioni):
            try:
                with open(self.file_posizioni, "r") as f:
                    content = f.read().strip()
                    return json.loads(content) if content else {}
            except Exception as e:
                self.logger.error(f"⚠️ Errore caricamento posizioni: {e}")
                return {}
        return {}

    def salva_posizioni(self):
        try:
            with open(self.file_posizioni, "w") as f:
                json.dump(self.posizioni_aperte, f, indent=4)
        except Exception as e:
            self.logger.error(f"⚠️ Errore salvataggio posizioni: {e}")

    def _normalizza(self, s):
        return "".join(c for c in s.upper() if c.isalnum())

    def sincronizza_con_exchange(self, engine=None):
        """ 
        Sincronizzazione Istituzionale: Kraken scrive il Diario.
        Usa SOLO ticker reali (es. XXBTZUSD) per evitare duplicati e conflitti di margine.
        """
        self.logger.info("🔄 Avvio sincronizzazione istituzionale JSON <-> Kraken...")
        try:
            # 1. Recupero posizioni reali da Kraken
            posizioni_real = self.performer.get_open_positions_real()
            # ticker_reali conterrà solo codici come ['XXBTZUSD', 'XETHZUSD']
            ticker_reali = [p.get('pair') for p in posizioni_real.values() if p.get('pair')]
            
            # --- 2. TRASCRIZIONE: Se Kraken ha posizioni, il JSON si adegua ---
            for txid, p_kraken in posizioni_real.items():
                symbol_kraken = p_kraken.get('pair') # Es: XXBTZUSD
                
                # USIAMO SOLO IL SYMBOL KRAKEN COME CHIAVE
                if symbol_kraken not in self.posizioni_aperte:
                    self.logger.warning(f"🔧 Trascrizione: Posizione {symbol_kraken} rilevata su Kraken (ID: {txid}).")
                    
                    costo = float(p_kraken.get('cost', 0))
                    volume = float(p_kraken.get('vol', 0))
                    p_entry = costo / volume if volume > 0 else 0
                    direzione = 'LONG' if p_kraken.get('type') == 'buy' else 'SHORT'
                    
                    leverage_reale = int(float(p_kraken.get('margin', 0)) / costo) if costo > 0 else 1
                    if leverage_reale < 1: leverage_reale = 1

                    # --- RECUPERO DATI ENGINE (Ticker Reale) ---
                    sl_da_applicare = 0
                    tp_da_applicare = 0
                    
                    if engine:
                        try:
                            # Chiediamo all'engine usando il ticker che capisce
                            analisi = engine.analizza_asset(symbol_kraken)
                            sl_da_applicare = float(analisi.get('sl', 0))
                            tp_da_applicare = float(analisi.get('tp', 0))
                        except Exception as e:
                            self.logger.error(f"⚠️ Errore Engine per {symbol_kraken}: {e}")

                    # --- CHECK DATI A ZERO (Paracadute su Ticker Reale) ---
                    if sl_da_applicare == 0:
                        molt_sl = 0.98 if direzione == 'LONG' else 1.02
                        sl_da_applicare = float(self.formatta_prezzo(symbol_kraken, p_entry * molt_sl))
                        self.logger.warning(f"🚨 Engine a 0. SL emergenza (2%) per {symbol_kraken}: {sl_da_applicare}")

                    if tp_da_applicare == 0:
                        molt_tp = 1.05 if direzione == 'LONG' else 0.95
                        tp_da_applicare = float(self.formatta_prezzo(symbol_kraken, p_entry * molt_tp))
                        self.logger.warning(f"🚨 Engine a 0. TP emergenza (5%) per {symbol_kraken}: {tp_da_applicare}")

                    # Il Diario viene scritto solo con i dati di Kraken
                    self.posizioni_aperte[symbol_kraken] = {
                        'asset': symbol_kraken,
                        'ordine_id': txid, 
                        'direzione': direzione,
                        'p_entrata': p_entry,
                        'size': volume,
                        'leverage': leverage_reale,
                        'sl': sl_da_applicare,
                        'tp': tp_da_applicare,
                        'sl_id': None, # Verranno popolati da sincronizza_e_ripara leggendo Kraken
                        'tp_id': None,
                        'fase': 0,
                        'data_apertura': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                        'nota': "TRASCRITTA_DA_KRAKEN"
                    }
                    self.salva_posizioni()
                    
                    # Ora che abbiamo l'entry corretta, sincronizziamo gli ordini
                    self.sincronizza_e_ripara(symbol_kraken)

            # 3. Pulizia Database Locale (Rimuove doppioni o chiusi)
            for asset_in_json in list(self.posizioni_aperte.keys()):
                # Se l'asset nel JSON è un nome umano (BTC/USD) o non è più su Kraken
                if asset_in_json not in ticker_reali:
                    self.logger.warning(f"🧹 Pulizia: {asset_in_json} rimosso (Nome errato o posizione chiusa).")
                    self.posizioni_aperte.pop(asset_in_json, None)
            
            self.salva_posizioni()

            # 4. Pulizia Ordini Orfani (Solo per ticker reali non impegnati)
            for asset_config in asset_list.ASSET_CONFIG.keys():
                if asset_config not in ticker_reali and asset_config not in self.posizioni_aperte:
                    self.performer.pulizia_totale_ordini(asset_config)
            
        except Exception as e:
            self.logger.error(f"❌ Errore critico sincronizzazione: {e}")
            import traceback
            traceback.print_exc()
    
    def is_posizione_aperta_su_kraken(self, asset):
        """ 
        Verifica l'esistenza reale su Kraken usando SOLO il ticker reale.
        Se il JSON ha un'entry che Kraken non ha, la chiude immediatamente.
        """
        try:
            ticker_reale = asset_list.get_ticker(asset)
            posizioni_reali = self.performer.get_open_positions_real()
            
            # La verità è solo nel ticker reale (es. XXBTZUSD)
            is_reale = ticker_reale in posizioni_reali
            
            # Se il diario (usando qualsiasi chiave) crede di essere aperto ma Kraken no
            if (asset in self.posizioni_aperte or ticker_reale in self.posizioni_aperte) and not is_reale:
                self.logger.warning(f"🔄 Discrepanza: {ticker_reale} chiuso su Kraken. Sincronizzo diario...")
                # Puliamo sia l'eventuale nome umano che il ticker reale per sicurezza
                self._chiudi_statisticamente(asset)
                if asset != ticker_reale:
                    self.posizioni_aperte.pop(ticker_reale, None)
                return False
                
            return is_reale
        except Exception as e:
            self.logger.error(f"❌ Errore verifica reale {asset}: {e}")
            return asset in self.posizioni_aperte
            
    def sincronizza_e_ripara(self, asset):
        """
        VERSIONE CHIMERA: Solo MARGINE.
        Sincronizza i dati di Kraken, preserva la strategia e impedisce ordini Spot o duplicati.
        """
        try:
            # 1. TRADUZIONE E RECUPERO DATI REALI
            ticker_reale = asset_list.get_ticker(asset)
            posizioni_reali = self.performer.get_open_positions_real()
            
            dati_kraken = posizioni_reali.get(ticker_reale)
            is_reale = dati_kraken is not None
            ora_attuale = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

            # --- FILTRO SOLO MARGINE ---
            # Se la posizione su Kraken non ha leva (o leva 1), la ignoriamo del tutto
            if is_reale:
                leva_raw = dati_kraken.get('leverage', '1').split(':')[0] if isinstance(dati_kraken.get('leverage'), str) else 1
                leva_k = int(float(leva_raw))
                if leva_k <= 1:
                    self.logger.debug(f"ℹ️ {ticker_reale} è una posizione SPOT. Ignoro come da impostazioni solo Margin.")
                    is_reale = False # La trattiamo come se non esistesse per il bot Margin
            else:
                leva_k = 1

            # 2. COERENZA CHIAVI JSON (Migrazione asset)
            if asset != ticker_reale and asset in self.posizioni_aperte:
                self.posizioni_aperte[ticker_reale] = self.posizioni_aperte.pop(asset)

            chiave_json = ticker_reale

            # A. Se il bot ha il trade nel JSON ma Kraken non lo ha (o non è Margin) -> Pulizia
            if chiave_json in self.posizioni_aperte and not is_reale:
                self.logger.warning(f"🧹 Posizione Margin {ticker_reale} non trovata. Rimozione dal diario.")
                self._chiudi_statisticamente(chiave_json)
                return False

            # B. ADOZIONE (Se è su Kraken Margin ma non nel JSON)
            if is_reale and chiave_json not in self.posizioni_aperte:
                self.logger.warning(f"🛡️ Scudo Chimera: Adottata posizione MARGINE su {ticker_reale}")
                self.posizioni_aperte[ticker_reale] = {
                    'asset': ticker_reale,
                    'ordine_id': dati_kraken.get('pos_txid'),
                    'p_entrata': float(dati_kraken.get('price', 0)),
                    'size': float(dati_kraken.get('vol', 0)),
                    'direzione': 'LONG' if dati_kraken.get('type') == 'buy' else 'SHORT',
                    'leverage': leva_k,
                    'sl': 0, 'tp': 0, 'voto': 5, 'tipo_op': "Swing", 'fase': 0,
                    'data_apertura': ora_attuale,
                    'nota': "RECUPERATA_DA_KRAKEN"
                }

            # 3. VERIFICA ORDINI ESISTENTI (Il blocco anti-duplicati)
            if is_reale:
                pos_stat = self.posizioni_aperte[chiave_json]
                cambiamento = False

                # Chiediamo a Kraken gli ordini aperti per EVITARE di inviarne di nuovi
                ordini_k = self.performer.exchange.fetch_open_orders(ticker_reale)

                # --- Sincronizzazione STOP LOSS ---
                sl_reale = next((o for o in ordini_k if o['info'].get('ordertype') == 'stop-loss'), None)
                if sl_reale:
                    # AGGIUNGI QUESTE RIGHE (devono essere spostate a destra):
                    if pos_stat.get('sl_id') != sl_reale['id']:
                        pos_stat['sl_id'] = sl_reale['id']
                        pos_stat['sl'] = float(sl_reale.get('price', 0))
                        cambiamento = True
                        self.logger.info(f"✅ SL collegato: {sl_reale['id']}")
                
                elif float(pos_stat.get('sl', 0)) > 0:
                    self.logger.warning(f"⚠️ SL mancante su Kraken per {ticker_reale}. Ripristino...")
                    
                    # FIX DECIMALI QUI (come abbiamo visto prima)
                    prezzo_sl = self.performer.qprice(ticker_reale, pos_stat['sl'])
                    
                    res = self.performer.gestisci_ordine_protezione(
                        ticker_reale, 'stop-loss', prezzo_sl, 
                        pos_stat['direzione'], pos_stat['size'], pos_stat.get('leverage', leva_k)
                    )
                    if res.get('success'):
                        pos_stat['sl_id'] = res.get('id')
                        cambiamento = True

                # --- Sincronizzazione TAKE PROFIT ---
                tp_reale = next((o for o in ordini_k if o['info'].get('ordertype') == 'take-profit'), None)
                if tp_reale:
                    # AGGIUNGI QUESTE RIGHE:
                    if pos_stat.get('tp_id') != tp_reale['id']:
                        pos_stat['tp_id'] = tp_reale['id']
                        pos_stat['tp'] = float(tp_reale.get('price', 0))
                        cambiamento = True
                        self.logger.info(f"✅ TP collegato: {tp_reale['id']}")
                
                elif float(pos_stat.get('tp', 0)) > 0:
                    self.logger.warning(f"⚠️ TP mancante su Kraken per {ticker_reale}. Ripristino...")
                    
                    # FIX DECIMALI QUI
                    prezzo_tp = self.performer.qprice(ticker_reale, pos_stat['tp'])
                    
                    res = self.performer.gestisci_ordine_protezione(
                        ticker_reale, 'take-profit', prezzo_tp, 
                        pos_stat['direzione'], pos_stat['size'], pos_stat.get('leverage', leva_k)
                    )
                    if res.get('success'):
                        pos_stat['tp_id'] = res.get('id')
                        cambiamento = True

                if cambiamento:
                    self.salva_posizioni()
                
                return True

            return False

        except Exception as e:
            self.logger.error(f"❌ Errore critico riparazione {asset}: {e}")
            return asset in self.posizioni_aperte
    
    def _chiudi_statisticamente(self, asset):
        """ 
        VERSIONE CHIMERA: Sposta la posizione dalle aperte allo storico calcolando il PNL finale.
        FIX: Pulisce doppie chiavi (BTC/USD e XXBTZUSD) per evitare posizioni fantasma.
        """
        try:
            # --- 1: IDENTIFICAZIONE SICURA ---
            ticker_reale = asset_list.get_ticker(asset)
            # Proviamo a estrarre la posizione usando il ticker reale, poi l'asset originale
            # Usiamo .pop() così la rimuoviamo immediatamente dalla memoria
            pos = self.posizioni_aperte.pop(ticker_reale, None) or self.posizioni_aperte.pop(asset, None)
            
            if pos:
                # Recuperiamo il prezzo attuale per il calcolo PNL statistico
                # Usiamo ticker_reale per la chiamata al performer (più affidabile)
                p_uscita = self.performer.get_current_price(ticker_reale) or float(pos.get('p_entrata', 0))
                p_entrata = float(pos.get('p_entrata', 0))
                
                # Calcolo PNL
                if p_entrata > 0:
                    pnl = ((p_uscita - p_entrata) / p_entrata) * 100
                    # Se la direzione è SHORT (o SELL), invertiamo il PNL
                    if pos.get('direzione') in ["SELL", "SHORT"]: 
                        pnl *= -1
                else:
                    pnl = 0
                
                esito = "WIN" if pnl > 0 else "LOSS"
                
                # Aggiorniamo il record per lo storico
                pos.update({
                    'data_chiusura': datetime.now().isoformat(),
                    'p_uscita': p_uscita,
                    'pnl_finale': round(pnl, 2),
                    'esito': esito,
                    'ticker_chiusura': ticker_reale # Tracciamo quale ticker è stato usato
                })
                
                # --- FIX 2: SALVATAGGIO ATOMICO ---
                self.storico_trades.append(pos)
                self._salva_storico()  # Salva il file storico_trades.json
                self.salva_posizioni() # Salva il file posizioni_aperte.json (ora pulito)
                
                self.logger.info(f"🏁 Diario aggiornato: {ticker_reale} chiuso con {pnl:.2f}% ({esito})")
                
                # Alert
                if self.alerts:
                    # Usiamo il ticker reale per l'alert così è coerente con Kraken
                    self.alerts.invia_alert(f"🏁 *TRADE CONCLUSO {ticker_reale}*\nEsito: {esito}\nPNL: {pnl:.2f}%")
            else:
                self.logger.debug(f"ℹ️ Nessuna posizione attiva trovata nel diario per {asset} (già rimossa o inesistente).")

        except Exception as e:
            self.logger.error(f"❌ Errore critico durante la chiusura statistica di {asset}: {e}")
            
    def apri_posizione(self, asset, direzione, entry_price, size, sl, tp, voto, leverage, dati_mercato):
        """ 
        VERSIONE CHIMERA: Solo MARGINE.
        Apertura posizione reale sincronizzata con la Gerarchia di Comando.
        FIX: Forza leva minima 2 per evitare errori Insufficient Funds su Spot.
        """
        try:
            # 0. TRADUZIONE TICKER IMMEDIATA
            ticker_reale = asset_list.get_ticker(asset)

            # 1. Check reale su Kraken (Fonte della Verità)
            if self.is_posizione_aperta_su_kraken(ticker_reale):
                self.logger.info(f"⏩ {ticker_reale} già aperta su Kraken. Salto l'apertura.")
                return False

            # 2. SINCRONIZZAZIONE SL
            if not sl or sl == 0:
                distanza_emergenza = entry_price * 0.02
                sl = entry_price - distanza_emergenza if direzione.upper() == "BUY" else entry_price + distanza_emergenza
                self.logger.warning(f"⚠️ SL mancante per {ticker_reale}! Impostato 2%: {sl}")

            # 3. Recupero Configurazione Specifica
            conf = asset_list.ASSET_CONFIG.get(ticker_reale, {})
            if not conf:
                self.logger.error(f"❌ Asset {ticker_reale} NON TROVATO in ASSET_CONFIG!")
                return False
                
            # 4. Sizing Istituzionale
            valore_nominale_target = 100.0 
            
            if conf.get('is_cross'):
                quote_asset = conf.get('quote_asset', 'XXBTZUSD')
                prezzo_btc_usd = self.performer.get_current_price(quote_asset)
                
                if prezzo_btc_usd:
                    budget_in_btc = valore_nominale_target / float(prezzo_btc_usd)
                    size_istituzionale = round(budget_in_btc / entry_price, conf.get('vol_precision', 4))
                    self.logger.info(f"🔄 Cross detected: 100$ -> {budget_in_btc:.6f} BTC")
                else:
                    self.logger.error(f"❌ Impossibile ottenere prezzo {quote_asset}. Aborto.")
                    return False
            else:
                size_istituzionale = round(valore_nominale_target / entry_price, conf.get('vol_precision', 2))

            # 5. --- FIX CRUCIALE LEVA (SOLO MARGINE) ---
            min_size_consentita = conf.get('min_size', 0.0001)
            if size_istituzionale < min_size_consentita:
                size_istituzionale = min_size_consentita
            
            # Se il Brain manda leva 1 o nulla, forziamo leva 2 per attivare il Margine su Kraken
            leva_richiesta = int(leverage) if leverage else 2
            if leva_richiesta < 2:
                self.logger.warning(f"🛡️ Forza Margine: Leva {leva_richiesta} non ammessa. Imposto Leva 2.")
                leva_richiesta = 2

            max_leverage_consentita = conf.get('max_leverage', 5)
            leva_da_usare = min(leva_richiesta, max_leverage_consentita)

            self.logger.info(f"💰 Sizing: {valore_nominale_target}$ | SL: {sl} | Voto: {voto} | Leva: {leva_da_usare}x")

            # 6. Esecuzione REALE
            # Passiamo il ticker_reale per evitare errori di mappatura su Kraken
            risultato = self.performer.esegui_ordine(
                asset=ticker_reale, direzione=direzione, size=size_istituzionale,
                leverage=leva_da_usare, voto=voto, sl=sl, tp=tp
            )

            if risultato and risultato.get('success'):
                time.sleep(1.0) # Attesa tecnica Kraken
                
                posizioni_reali = self.performer.get_open_positions_real()
                dati_kraken = next((p for p in posizioni_reali.values() if p.get('pair') == ticker_reale), None)

                p_entrata_finale = float(dati_kraken.get('price', entry_price)) if dati_kraken else entry_price
                size_finale = float(dati_kraken.get('vol', size_istituzionale)) if dati_kraken else size_istituzionale
                
                oid = risultato.get('order_id')
                sid = risultato.get('sl_id')
                tid = risultato.get('tp_id')

                # Registrazione nel JSON con ticker_reale
                self.posizioni_aperte[ticker_reale] = {
                    'asset': ticker_reale,
                    'direzione': direzione.upper(),
                    'ordine_id': oid,
                    'p_entrata': p_entrata_finale,
                    'size': size_finale,
                    'leverage': leva_da_usare,
                    'sl': sl,
                    'tp': tp,
                    'voto_ia': voto,
                    'data_apertura': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    'fase': 0, 
                    'sl_id': sid,
                    'tp_id': tid,
                    'chimera_snapshot': dati_mercato
                }
                
                self.salva_posizioni()
                self.logger.info(f"✅ REGISTRAZIONE REALE COMPLETATA {ticker_reale}: Entry={oid}")
                return True
                
            return False

        except Exception as e:
            self.logger.error(f"❌ Errore critico in apri_posizione per {asset}: {e}")
            return False
            
    def gestisci_protezione_istituzionale(self, asset, prezzo_attuale, atr_attuale=0):
        """ 
        Gestione Dinamica Avanzata (FIXED):
        Inversione dei comandi per evitare il 'buco' di protezione che causa chiusure anticipate.
        Utilizzo chirurgico degli ID per garantire la sincronizzazione con Kraken.
        """
        pos = self.posizioni_aperte.get(asset)
        if not pos: return

        # Recupero dati essenziali
        p_entrata = float(pos['p_entrata'])
        tp_target = float(pos['tp'])
        direzione = pos['direzione'].upper()
        fase_attuale = pos.get('fase', 0)
        symbol_kraken = asset_list.get_ticker(asset)
        
        # 1. Calcolo progresso verso il TP (0% a 100%)
        distanza_totale = abs(tp_target - p_entrata)
        if distanza_totale == 0: return 
        
        distanza_attuale = (prezzo_attuale - p_entrata) if direzione == "BUY" else (p_entrata - prezzo_attuale)
        progresso_percentuale = (distanza_attuale / distanza_totale) * 100

        # --- FASE 1: 50% PROFITTO + FILTRO VOLATILITÀ -> BREAKEVEN ---
        # Spostiamo a BE solo se il profitto attuale è almeno 2.5 volte l'ATR attuale
        distanza_sicurezza_atr = atr_attuale * 2.5 if atr_attuale > 0 else 0
        
        if 50 <= progresso_percentuale < 80 and fase_attuale < 1 and distanza_attuale > distanza_sicurezza_atr:
            nuovo_sl = p_entrata
            self.logger.info(f"🛡️ {asset} al 50% e sopra soglia sicurezza ATR. Spostamento SL a PAREGGIO...")
            # [CHIRURGICO] CANCELLIAMO IL VECCHIO TRAMITE ID PRIMA DI CREARE IL NUOVO
            vecchio_sl_id = pos.get('sl_id')
            cancellazione_confermata = False
            
            if vecchio_sl_id:
                # Se la cancellazione per ID ha successo, procediamo
                if self.performer.cancella_ordine_specifico(vecchio_sl_id):
                    cancellazione_confermata = True
                    # Delay necessario per liberare il margine su Kraken
                    time.sleep(1.5)
            else:
                # Se non c'è l'ID, usiamo la pulizia per non bloccare il margine
                self.performer.pulizia_totale_ordini(asset)
                time.sleep(1.5)
                cancellazione_confermata = True

            if cancellazione_confermata:
                risultato = self.performer.gestisci_ordine_protezione(
                    asset=asset, 
                    tipo_protezione='stop-loss', 
                    prezzo=nuovo_sl, 
                    direzione_aperta=direzione, 
                    size_fallback=pos['size'], 
                    leverage=pos['leverage']
                )
                
                if risultato and risultato.get('success'):
                    pos['fase'] = 1
                    pos['sl'] = nuovo_sl
                    pos['sl_id'] = risultato.get('id')
                    
                    self.salva_posizioni()
                    if self.alerts: self.alerts.invia_alert(f"🛡️ *SAFE MODE {asset}*\n50% raggiunto: SL a Pareggio.")

        # --- FASE 2: 85% PROFITTO -> RIMOZIONE TP + TRAILING STOP ---
        elif progresso_percentuale >= 80:
            if fase_attuale < 2:
                self.logger.warning(f"🚀 {asset} all'85%! Transizione a Phase Two...")
                
                # [CHIRURGICO] CANCELLAZIONE TP E SL VECCHIO
                tp_id_f2 = pos.get('tp_id')
                sl_id_f2 = pos.get('sl_id')
                
                # Cancelliamo tutto prima di inviare il nuovo
                tp_deleted = self.performer.cancella_ordine_specifico(tp_id_f2) if tp_id_f2 else True
                sl_deleted = self.performer.cancella_ordine_specifico(sl_id_f2) if sl_id_f2 else True
                
                if tp_deleted and sl_deleted:
                    # Delay per pulizia engine Kraken
                    time.sleep(1.2) 
                    
                    nuovo_sl_trail = p_entrata + (distanza_totale * 0.5) if direzione == "BUY" else p_entrata - (distanza_totale * 0.5)
                    nuovo_sl_trail = float(self.performer.qprice(symbol_kraken, nuovo_sl_trail))

                    risultato_f2 = self.performer.gestisci_ordine_protezione(
                        asset=asset, 
                        tipo_protezione='stop-loss', 
                        prezzo=nuovo_sl_trail, 
                        direzione_aperta=direzione, 
                        size_fallback=pos['size'], 
                        leverage=pos['leverage']
                    )

                    if risultato_f2 and risultato_f2.get('success'):
                        pos['fase'] = 2
                        pos['tp'] = 0 
                        pos['sl'] = nuovo_sl_trail
                        pos['tp_id'] = None
                        pos['sl_id'] = risultato_f2.get('id')
                        self.salva_posizioni()
                    
                    self.salva_posizioni()
                    if self.alerts: self.alerts.invia_alert(f"🚀 *DYNAMIC MODE {asset}*\n85% raggiunto: TP rimosso, SL al 50% profitto.")

            # --- GESTIONE TRAILING ATTIVO (Inseguimento dopo l'85%) ---
            else:
                distanza_trailing = max(prezzo_attuale * 0.015, atr_attuale * 2) if atr_attuale > 0 else prezzo_attuale * 0.015
                nuovo_sl_dinamico = prezzo_attuale - distanza_trailing if direzione == "BUY" else prezzo_attuale + distanza_trailing
                
                # Formattazione corretta
                nuovo_sl_dinamico = float(self.performer.qprice(symbol_kraken, nuovo_sl_dinamico))
                sl_in_memoria = float(pos.get('sl', 0))
                
                is_migliore = nuovo_sl_dinamico > sl_in_memoria if direzione == "BUY" else nuovo_sl_dinamico < sl_in_memoria
                
                if is_migliore:
                    vecchio_id_trail = pos.get('sl_id')
                    if vecchio_id_trail:
                        if self.performer.cancella_ordine_specifico(vecchio_id_trail):
                            time.sleep(1.0)
                            
                            res_upd = self.performer.gestisci_ordine_protezione(
                                asset, 'stop-loss', nuovo_sl_dinamico, direzione, pos['size'], pos['leverage']
                            )

                            if res_upd and res_upd.get('success'):
                                pos['sl'] = nuovo_sl_dinamico
                                pos['sl_id'] = res_upd.get('id')
                                self.salva_posizioni()
                                self.logger.info(f"📈 {asset}: Trailing aggiornato a {nuovo_sl_dinamico}")
    
    def rimuovi_tp_fase_due(self, asset, motivo):
        """
        PROJECT CHIMERA - Phase Two.
        Rimuove il Take Profit esistente su Kraken per lasciar correre il profitto
        quando la price velocity è esplosiva.
        """
        try:
            pos = self.posizioni_aperte.get(asset)
            if not pos: return False

            # Se Kraken ha già chiuso l'ordine TP (magari per un micro-touch), l'ID non sarà più valido.
            tp_id = pos.get('tp_id')
            if not tp_id:
                self.logger.info(f"ℹ️ {asset}: TP non presente nel JSON. Possibile Phase Two già attiva o TP eseguito.")
                return False

            tp_id = pos.get('tp_id')
            self.logger.info(f"⚡ [CHIMERA] Tentativo rimozione TP ({tp_id}) per {asset} - Motivo: {motivo}")

            # Usiamo il metodo che già utilizzi nel resto della classe
            success = self.performer.cancella_ordine_specifico(tp_id)

            if success:
                # Aggiorniamo lo stato interno per coerenza con la tua logica 'fase'
                pos['fase'] = 2 
                pos['tp_id'] = None
                pos['tp'] = 0  # Indica che non c'è più un target fisso
                
                self.salva_posizioni()
                
                # Messaggio Telegram per Andrea
                if self.alerts:
                    msg = (f"🚀 *CHIMERA PHASE TWO* su {asset}\n\n"
                           f"✅ *Take Profit rimosso*\n"
                           f"📈 Il trade ora è in 'Free Run'.\n"
                           f"🏃 *Motivo:* {motivo}")
                    self.alerts.invia_alert(msg)
                
                # Feedback per il feedback_engine (già presente nel tuo __init__)
                if self.feedback_engine:
                    self.feedback_engine.registra_evento(asset, "PHASE_TWO_ACTIVATED", {"motivo": motivo})
                
                return True
            else:
                self.logger.error(f"❌ Fallita cancellazione ordine TP {tp_id} su Kraken.")
                return False

        except Exception as e:
            self.logger.error(f"❌ Errore in rimuovi_tp_fase_due: {e}")
            return False
    
    def registra_conclusione_trade(self, asset, esito, pnl_finale):
        if asset in self.posizioni_aperte:
            pos = self.posizioni_aperte.pop(asset)
            pos.update({'data_chiusura': datetime.now().isoformat(), 'esito': esito, 'pnl_finale': pnl_finale})
            if self.feedback_engine:
                self.feedback_engine.registra_feedback(asset=asset, score=pos.get('voto_ia', 0), outcome=esito, motivi=f"PNL: {pnl_finale}%")
            self.storico_trades.append(pos)
            self._salva_storico()
            self.salva_posizioni()
            self.logger.info(f"🏁 TRADE CONCLUSO: {asset} | PNL: {pnl_finale}%")

    def aggiorna_posizione(self, asset, dati):
        if asset in self.posizioni_aperte:
            self.posizioni_aperte[asset].update(dati)
            self.salva_posizioni()

    def _carica_storico(self):
        if os.path.exists(self.file_storico):
            try:
                with open(self.file_storico, "r") as f: return json.load(f)
            except: return []
        return []

    def _salva_storico(self):
        try:
            with open(self.file_storico, "w") as f: json.dump(self.storico_trades, f, indent=4)
        except Exception as e: self.logger.error(f"⚠️ Errore storico: {e}")

    def _carica_stats_globali(self):
        if os.path.exists(self.file_stats):
            try:
                with open(self.file_stats, "r") as f: return json.load(f)
            except: pass
        return {"max_drawdown": 0.0, "pnl_realizzato_totale": 0.0, "equity_peak": 0.0}
        
    def genera_dati_report_giornaliero(self):
        """
        Versione Chimera 3.0: Utilizza ticker reali Kraken e tracciamento fasi avanzato.
        """
        ieri = datetime.now() - timedelta(days=1)
        trades_oggi = []
        
        for t in self.storico_trades:
            try:
                # Gestione flessibile del formato data
                dt_str = t['data_chiusura'].replace('Z', '+00:00')
                dt_chiusura = datetime.fromisoformat(dt_str)
                if dt_chiusura.replace(tzinfo=None) > ieri:
                    trades_oggi.append(t)
            except: 
                continue
        
        # Calcolo PNL e Win Rate
        pnl_giorno = sum(float(t.get('pnl_finale', 0)) for t in trades_oggi)
        win = len([t for t in trades_oggi if t.get('esito') == "WIN"])
        
        # CONTEGGIO MOONSHOTS (Fase 2 attiva)
        moonshots = len([t for t in trades_oggi if t.get('fase') == 2])
        
        # --- FIX NOMI UMANI E MAPPATURA TICKER ---
        dettaglio_chiusi = []
        for t in trades_oggi:
            # Usiamo asset_list per essere certi di mostrare il ticker reale nel report
            ticker_reale = asset_list.get_ticker(t['asset']) if 'asset' in t else "N/A"
            icona_fase = "🚀 (Moonshot)" if t.get('fase') == 2 else "✅"
            dettaglio_chiusi.append(
                f"{ticker_reale} ({t['direzione']}): {float(t['pnl_finale']):.2f}% {icona_fase}"
            )

        report = {
            "pnl_totale_24h": round(pnl_giorno, 2),
            "trades_chiusi": len(trades_oggi),
            "win_rate": round((win / len(trades_oggi) * 100), 2) if len(trades_oggi) > 0 else 0.0,
            "moonshots_attivati": moonshots,
            "dettaglio": dettaglio_chiusi,
            # Mostriamo le chiavi delle posizioni aperte (che ora sono ticker Kraken)
            "posizioni_ancora_aperte": list(self.posizioni_aperte.keys())
        }
        
        # Log del report per debug immediato
        self.logger.info(f"📊 Report 24h generato: PNL {report['pnl_totale_24h']}% su {report['trades_chiusi']} operazioni.")
        
        return report