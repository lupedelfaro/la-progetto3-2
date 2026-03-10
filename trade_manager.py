# -*- coding: utf-8 -*-
import logging
import json
import os
import time
from datetime import datetime, timedelta
from core import asset_list as al_config

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
            precision = al_config.ASSET_CONFIG.get(asset, {}).get('precision', 2)
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
        Sincronizza JSON con Kraken e pulisce ordini orfani. 
        Se l'Engine fornisce dati a 0, applica protezioni statistiche di emergenza.
        """
        self.logger.info("🔄 Avvio sincronizzazione istituzionale JSON <-> Kraken...")
        try:
            # 1. Recupero posizioni reali da Kraken
            posizioni_real = self.performer.get_open_positions_real()
            ticker_reali = [p.get('pair') for p in posizioni_real.values() if p.get('pair')]
            
            # --- 2. RIPARAZIONE: Se Kraken ha posizioni che il JSON non ha ---
            for txid, p_kraken in posizioni_real.items():
                symbol_kraken = p_kraken.get('pair')
                asset_interno = symbol_kraken 
                
                if asset_interno not in self.posizioni_aperte:
                    self.logger.warning(f"🔧 Riparazione: Posizione {asset_interno} trovata su Kraken ma assente nel JSON.")
                    
                    costo = float(p_kraken.get('cost', 0))
                    volume = float(p_kraken.get('vol', 0))
                    p_entry = costo / volume if volume > 0 else 0
                    direzione = 'LONG' if p_kraken.get('type') == 'buy' else 'SHORT'
                    
                    # Recupero leva reale
                    leverage_reale = int(float(p_kraken.get('margin', 0)) / costo) if costo > 0 else 1
                    if leverage_reale < 1: leverage_reale = 1

                    # --- RECUPERO DATI ENGINE ---
                    sl_da_applicare = 0
                    tp_da_applicare = 0
                    
                    if engine:
                        try:
                            # Chiediamo all'Engine i livelli (CVD, Price Velocity, etc.)
                            analisi = engine.analizza_asset(asset_interno)
                            sl_da_applicare = float(analisi.get('sl', 0))
                            tp_da_applicare = float(analisi.get('tp', 0))
                        except Exception as e:
                            self.logger.error(f"⚠️ Errore Engine per {asset_interno}: {e}")

                    # --- CHECK DATI A ZERO (PARACADUTE) ---
                    # Se l'engine non dà dati, usiamo lo 2% SL e 5% TP come rete di sicurezza
                    if sl_da_applicare == 0:
                        molt_sl = 0.98 if direzione == 'LONG' else 1.02
                        sl_da_applicare = float(self.formatta_prezzo(asset_interno, p_entry * molt_sl))
                        self.logger.warning(f"🚨 Engine a 0. SL emergenza (2%) per {asset_interno}: {sl_da_applicare}")

                    if tp_da_applicare == 0:
                        molt_tp = 1.05 if direzione == 'LONG' else 0.95
                        tp_da_applicare = float(self.formatta_prezzo(asset_interno, p_entry * molt_tp))
                        self.logger.warning(f"🚨 Engine a 0. TP emergenza (5%) per {asset_interno}: {tp_da_applicare}")

                    # Creiamo l'entry nel JSON con dati validi
                    self.posizioni_aperte[asset_interno] = {
                        'asset': asset_interno,
                        'direzione': direzione,
                        'p_entrata': p_entry,
                        'size': volume,
                        'leverage': leverage_reale,
                        'sl': sl_da_applicare,
                        'tp': tp_da_applicare,
                        'fase': 0,
                        'data_apertura': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                        'nota': "RECUPERATA_DA_KRAKEN"
                    }
                    self.salva_posizioni()
                    
                    # Ora sincronizza_e_ripara troverà dati > 0 e invierà gli ordini mancanti
                    self.logger.info(f"🛡️ Trigger sincronizza_e_ripara per {asset_interno}...")
                    self.sincronizza_e_ripara(asset_interno)

            # 3. Pulizia Database Locale
            orfane_json = []
            for asset_json in list(self.posizioni_aperte.keys()):
                symbol_kraken = al_config.get_ticker(asset_json)
                if symbol_kraken not in ticker_reali:
                    orfane_json.append(asset_json)
            
            for asset in orfane_json:
                self.logger.warning(f"🧹 Asset {asset} non più su Kraken. Pulizia locale...")
                self._chiudi_statisticamente(asset)
            
            if orfane_json or (len(posizioni_real) > 0):
                self.salva_posizioni()

            # 4. Pulizia Ordini Orfani
            for asset in al_config.ASSET_CONFIG.keys():
                symbol_kraken = al_config.get_ticker(asset)
                if symbol_kraken not in ticker_reali and asset not in self.posizioni_aperte:
                    self.performer.pulizia_totale_ordini(asset)
            
        except Exception as e:
            self.logger.error(f"❌ Errore durante sincronizzazione: {e}")
            import traceback
            traceback.print_exc()
    def is_posizione_aperta_su_kraken(self, asset):
        """ Verifica l'esistenza reale su Kraken e sincronizza se chiusa fuori dal bot. """
        try:
            posizioni_reali = self.performer.get_open_positions_real()
            symbol_kraken = al_config.get_ticker(asset)
            is_reale = any(p.get('pair') == symbol_kraken for p in posizioni_reali.values())
            
            if asset in self.posizioni_aperte and not is_reale:
                self.logger.warning(f"🔄 Chiusura rilevata su Kraken per {asset}. Sincronizzazione...")
                self._chiudi_statisticamente(asset)
                return False
            return is_reale
        except Exception as e:
            self.logger.error(f"❌ Errore verifica esistenza reale {asset}: {e}")
            return asset in self.posizioni_aperte
    
    def sincronizza_e_ripara(self, asset):
        """
        Allinea il JSON ai dati reali di Kraken e ripristina SL/TP.
        Se i valori sono a 0 (recupero), imposta protezioni di emergenza.
        """
        try:
            posizioni_reali = self.performer.get_open_positions_real()
            symbol_kraken = al_config.get_ticker(asset)
            dati_kraken = next((p for p in posizioni_reali.values() if p.get('pair') == symbol_kraken), None)
            is_reale = dati_kraken is not None

            if asset in self.posizioni_aperte and not is_reale:
                self._chiudi_statisticamente(asset)
                return False

            if is_reale and asset in self.posizioni_aperte:
                pos_stat = self.posizioni_aperte[asset]
                cambiamento = False

                # 1. Allineamento Prezzo/Size
                p_reale = float(dati_kraken.get('price', pos_stat['p_entrata']))
                s_reale = float(dati_kraken.get('vol', pos_stat['size']))
                if pos_stat['p_entrata'] != p_reale or pos_stat['size'] != s_reale:
                    pos_stat['p_entrata'], pos_stat['size'] = p_reale, s_reale
                    cambiamento = True

                # 2. GESTIONE PROTEZIONI (Emergenza se a 0)
                # Se nel JSON è 0, calcoliamo valori di default per non restare scoperti
                if pos_stat.get('sl', 0) == 0:
                    moltiplicatore = 0.98 if pos_stat['direzione'] == 'LONG' else 1.02
                    pos_stat['sl'] = float(self.formatta_prezzo(asset, p_reale * moltiplicatore))
                    self.logger.warning(f"🚨 SL emergenza (2%) impostato per {asset}: {pos_stat['sl']}")
                    cambiamento = True

                if pos_stat.get('tp', 0) == 0:
                    moltiplicatore = 1.05 if pos_stat['direzione'] == 'LONG' else 0.95
                    pos_stat['tp'] = float(self.formatta_prezzo(asset, p_reale * moltiplicatore))
                    self.logger.warning(f"🚨 TP emergenza (5%) impostato per {asset}: {pos_stat['tp']}")
                    cambiamento = True

                # 3. Allineamento Take Profit su Kraken
                if dati_kraken.get('has_tp'):
                    id_tp = dati_kraken.get('tp_id_kraken')
                    if pos_stat.get('tp_id') != id_tp:
                        pos_stat['tp_id'] = id_tp
                        cambiamento = True
                else:
                    tp_f = self.formatta_prezzo(asset, pos_stat['tp'])
                    self.logger.info(f"🛰️ Invio ordine TP per {asset}: {tp_f}")
                    res_tp = self.performer.gestisci_ordine_protezione(asset, 'take-profit', tp_f, pos_stat['direzione'], pos_stat['size'], pos_stat['leverage'])
                    if res_tp and isinstance(res_tp, dict):
                        pos_stat['tp_id'] = res_tp.get('id')
                        cambiamento = True

                # 4. Allineamento Stop Loss su Kraken
                if dati_kraken.get('has_sl'):
                    id_sl = dati_kraken.get('sl_id_kraken')
                    if pos_stat.get('sl_id') != id_sl:
                        pos_stat['sl_id'] = id_sl
                        cambiamento = True
                else:
                    sl_f = self.formatta_prezzo(asset, pos_stat['sl'])
                    self.logger.info(f"🛰️ Invio ordine SL per {asset}: {sl_f}")
                    res_sl = self.performer.gestisci_ordine_protezione(asset, 'stop-loss', sl_f, pos_stat['direzione'], pos_stat['size'], pos_stat['leverage'])
                    if res_sl and isinstance(res_sl, dict):
                        pos_stat['sl_id'] = res_sl.get('id')
                        cambiamento = True

                if cambiamento:
                    self.salva_posizioni()
                return True
            return is_reale
        except Exception as e:
            self.logger.error(f"❌ Errore critico riparazione {asset}: {e}")
            return asset in self.posizioni_aperte
    def _chiudi_statisticamente(self, asset):
        """ Sposta la posizione dalle aperte allo storico calcolando il PNL finale. """
        try:
            if asset in self.posizioni_aperte:
                pos = self.posizioni_aperte.pop(asset)
                
                # Recuperiamo il prezzo attuale per il calcolo PNL statistico
                p_uscita = self.performer.get_current_price(asset) or float(pos['p_entrata'])
                p_entrata = float(pos['p_entrata'])
                
                pnl = ((p_uscita - p_entrata) / p_entrata) * 100
                if pos['direzione'] == "SELL": pnl *= -1
                
                esito = "WIN" if pnl > 0 else "LOSS"
                
                # Aggiorniamo il record per lo storico
                pos.update({
                    'data_chiusura': datetime.now().isoformat(),
                    'p_uscita': p_uscita,
                    'pnl_finale': round(pnl, 2),
                    'esito': esito
                })
                
                self.storico_trades.append(pos)
                self._salva_storico()
                self.salva_posizioni()
                
                self.logger.info(f"🏁 Diario aggiornato: {asset} chiuso con {pnl:.2f}% ({esito})")
                
                if self.alerts:
                    self.alerts.invia_alert(f"🏁 *TRADE CONCLUSO {asset}*\nEsito: {esito}\nPNL: {pnl:.2f}%")
        except Exception as e:
            self.logger.error(f"❌ Errore durante la chiusura statistica di {asset}: {e}")
    
    def apri_posizione(self, asset, direzione, entry_price, size, sl, tp, voto, leverage, dati_mercato):
        """ 
        Apertura posizione reale sincronizzata con la Gerarchia di Comando.
        INTERROGAZIONE KRAKEN POST-ORDINE: Popola il JSON solo con dati reali eseguiti.
        """
        try:
            # 1. Check reale su Kraken (Fonte della Verità)
            if self.is_posizione_aperta_su_kraken(asset):
                self.logger.info(f"⏩ {asset} già aperta su Kraken. Salto l'apertura.")
                return False

            # 2. SINCRONIZZAZIONE SL (Logica originale mantenuta)
            if not sl or sl == 0:
                distanza_emergenza = entry_price * 0.02
                sl = entry_price - distanza_emergenza if direzione.upper() == "BUY" else entry_price + distanza_emergenza
                self.logger.warning(f"⚠️ SL mancante dal Brain per {asset}! Impostato SL emergenza 2%: {sl}")

            # 3. Recupero Configurazione Specifica (al_config)
            conf = al_config.ASSET_CONFIG.get(asset, {})
            if not conf:
                self.logger.error(f"❌ Asset {asset} NON TROVATO in ASSET_CONFIG!")
                return False

            # 4. Sizing Istituzionale (Logica originale mantenuta)
            valore_nominale_target = 100.0 
            
            if conf.get('is_cross'):
                quote_asset = conf.get('quote_asset', 'XXBTZUSD')
                prezzo_btc_usd = self.performer.get_current_price(quote_asset)
                
                if prezzo_btc_usd:
                    budget_in_btc = valore_nominale_target / float(prezzo_btc_usd)
                    size_istituzionale = round(budget_in_btc / entry_price, conf.get('vol_precision', 4))
                    self.logger.info(f"🔄 Cross detected: 100$ converted to {budget_in_btc:.6f} BTC")
                else:
                    self.logger.error(f"❌ Impossibile ottenere prezzo {quote_asset} per conversione. Aborto.")
                    return False
            else:
                size_istituzionale = round(valore_nominale_target / entry_price, conf.get('vol_precision', 2))

            # 5. Check size minima e Leva
            min_size_consentita = conf.get('min_size', 0.0001)
            if size_istituzionale < min_size_consentita:
                size_istituzionale = min_size_consentita
            
            max_leverage_consentita = conf.get('max_leverage', 5)
            leva_da_usare = min(leverage, max_leverage_consentita) if leverage else max_leverage_consentita

            self.logger.info(f"💰 Sizing: {valore_nominale_target}$ | SL: {sl} | Voto: {voto} | Leva: {leva_da_usare}x")

            # 6. Esecuzione REALE
            risultato = self.performer.esegui_ordine(
                asset=asset, direzione=direzione, size=size_istituzionale,
                leverage=leva_da_usare, voto=voto, sl=sl, tp=tp
            )

            if risultato and risultato.get('success'):
                # --- SINCRONIZZAZIONE DATI REALI POST-ESECUZIONE ---
                # Attendiamo un istante per permettere a Kraken di registrare la posizione
                time.sleep(1.0)
                
                posizioni_reali = self.performer.get_open_positions_real()
                symbol_kraken = al_config.get_ticker(asset)
                dati_kraken = next((p for p in posizioni_reali.values() if p.get('pair') == symbol_kraken), None)

                # Se troviamo i dati reali su Kraken, usiamo quelli. Altrimenti fallback sui dati stimati.
                p_entrata_finale = float(dati_kraken.get('price', entry_price)) if dati_kraken else entry_price
                size_finale = float(dati_kraken.get('vol', size_istituzionale)) if dati_kraken else size_istituzionale
                
                oid = risultato.get('order_id')
                sid = risultato.get('sl_id')
                tid = risultato.get('tp_id')

                # Registrazione nel JSON con i dati confermati da Kraken
                self.posizioni_aperte[asset] = {
                    'asset': asset,
                    'direzione': direzione.upper(),
                    'ordine_id': oid,
                    'p_entrata': p_entrata_finale, # DATO REALE KRAKEN
                    'size': size_finale,           # DATO REALE KRAKEN
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
                self.logger.info(f"✅ REGISTRAZIONE REALE COMPLETATA {asset}: Entry={oid}, Price={p_entrata_finale}")
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
        symbol_kraken = al_config.get_ticker(asset)
        
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
        """Analizza i trade chiusi nelle ultime 24 ore includendo le performance del Trailing."""
        ieri = datetime.now() - timedelta(days=1)
        
        trades_oggi = []
        for t in self.storico_trades:
            try:
                # Gestione flessibile del formato data
                dt_str = t['data_chiusura'].replace('Z', '+00:00')
                dt_chiusura = datetime.fromisoformat(dt_str)
                if dt_chiusura.replace(tzinfo=None) > ieri:
                    trades_oggi.append(t)
            except: continue
        
        # Statistiche avanzate
        pnl_giorno = sum(float(t.get('pnl_finale', 0)) for t in trades_oggi)
        win = len([t for t in trades_oggi if t.get('esito') == "WIN"])
        
        # CONTEGGIO MOONSHOTS: Trade che hanno superato l'85% del target e attivato il Trailing
        moonshots = len([t for t in trades_oggi if t.get('fase') == 2])
        
        report = {
            "pnl_totale_24h": round(pnl_giorno, 2),
            "trades_chiusi": len(trades_oggi),
            "win_rate": round((win / len(trades_oggi) * 100), 2) if len(trades_oggi) > 0 else 0.0,
            "moonshots_attivati": moonshots,  # <--- NUOVO DATO
            "dettaglio": [
                f"{t['asset']} ({t['direzione']}): {t['pnl_finale']:.2f}% {'🚀 (Fase 2)' if t.get('fase') == 2 else ''}" 
                for t in trades_oggi
            ],
            "posizioni_ancora_aperte": list(self.posizioni_aperte.keys())
        }
        return report