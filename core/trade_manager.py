# -*- coding: utf-8 -*-
"""
L&A Institutional Bot - TradeManager Unified
Versione 2.4: AUTO-LEARNING INTEGRATED - Nessun dato andrà perduto
"""

import logging
import json
import os
from datetime import datetime
from core import asset_list as al_config  # necessario per normalizzare i ticker

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
        """Rimuove slash e caratteri non alfanumerici per confronti robusti."""
        return "".join(c for c in s.upper() if c.isalnum())

    def sincronizza_con_exchange(self, engine):
        """Versione FIX: Confronta JSON e Kraken isolando rigorosamente gli asset."""
        self.logger.info("🔄 Avvio sincronizzazione JSON <-> Kraken...")
        posizioni_real = engine.get_open_positions_real()
        
        # Estraiamo i ticker reali (es: 'XXBTZUSD', 'XETHXXBT')
        ticker_reali = []
        if posizioni_real:
            for _, p_data in posizioni_real.items():
                pair = p_data.get('pair')
                if pair:
                    ticker_reali.append(pair)

        orfane = []
        for asset_json in list(self.posizioni_aperte.keys()):
            match_trovato = False
            try:
                symbol_cfg = al_config.get_ticker(asset_json) or asset_json
            except Exception:
                symbol_cfg = asset_json
            norm_cfg = self._normalizza(symbol_cfg)
            norm_cfg_btc = self._normalizza(symbol_cfg.replace("XBT", "BTC"))
            for tr in ticker_reali:
                norm_tr = self._normalizza(tr)
                if norm_tr in (norm_cfg, norm_cfg_btc):
                    match_trovato = True
                    break
            
            if not match_trovato:
                orfane.append(asset_json)

        if orfane:
            for asset in orfane:
                self.logger.warning(f"🧹 Pulizia posizione orfana: {asset} (non trovata su Kraken)")
                if asset in self.posizioni_aperte:
                    self.posizioni_aperte.pop(asset)
            
            self.salva_posizioni()
            self.logger.info(f"✅ Sincronizzazione conclusa. Rimosse {len(orfane)} posizioni.")
        else:
            self.logger.info("✅ Sincronizzazione conclusa. Il JSON è coerente.")

    def apri_posizione(self, asset, direzione, entry_price, size, sl, tp, voto, leverage=3, dati_mercato=None):
        ts = None
        if not sl or float(sl) == 0 or not tp or float(tp) == 0:
            self.logger.info(f"⚖️ Calcolo livelli d'emergenza per {asset}...")
            
            # --- PRECISIONE DINAMICA (ASSET_CONFIG + controllo ticker Kraken) ---
            from core import asset_list as al_config_local
            from core.asset_list import ASSET_CONFIG

            symbol = al_config_local.get_ticker(asset)
            prec = ASSET_CONFIG.get(asset, {}).get('precision', 2)

            # Se la valuta di quotazione NON è fiat, forziamo almeno 5 decimali (crypto-crypto)
            if symbol and "/" in symbol:
                try:
                    _, quote = symbol.split("/")
                    if quote.upper() not in ("USD", "EUR", "USDT", "ZUSD"):
                        prec = max(prec, 5)
                except Exception:
                    pass

            # ATR di fallback
            atr_val = 0
            if dati_mercato:
                atr_val = dati_mercato.get('atr') or dati_mercato.get('atr_14') or 0

            atr = float(atr_val) if atr_val and float(atr_val) > 0 else (float(entry_price) * 0.02)
            p_ingresso = float(entry_price)

            if direzione.upper() == "BUY":
                sl = round(p_ingresso - (atr * 1.5), prec)
                tp = round(p_ingresso + (atr * 3.5), prec)
            else:
                sl = round(p_ingresso + (atr * 1.5), prec)
                tp = round(p_ingresso - (atr * 3.5), prec)

            # Trailing step calcolato e salvato per uso futuro
            ts = round(atr * 1.1, prec)

            self.logger.info(f"✅ Livelli EMERGENZA generati: Entry {p_ingresso} | SL {sl} | TP {tp} (Prec: {prec})")

        res_performer = None
        if self.performer:
            self.logger.info(f"📡 Invio ordine reale a Kraken per {asset} | SL: {sl} TP: {tp}")
            res_performer = self.performer.esegui_ordine(
                asset=asset, direzione=direzione, size=size, voto=voto,
                modalita='MARGIN', leverage=leverage, sl=sl, tp=tp
            )

        if res_performer and res_performer.get('success'):
            dati_tecnici = {}
            if dati_mercato:
                dati_tecnici = {
                    "z_score": dati_mercato.get('z_score'),
                    "cvd_divergence": dati_mercato.get('cvd_divergence'),
                    "funding_z_score": dati_mercato.get('funding_z_score'),
                    "book_pressure": dati_mercato.get('book_pressure'),
                    "atr": dati_mercato.get('atr') or dati_mercato.get('atr_14')
                }

            nuova_pos = {
                'asset': asset,
                'direzione': direzione,
                'p_entrata': entry_price,
                'size': size,
                'leverage': leverage,
                'sl': sl,
                'sl_id': res_performer.get('sl_id'),
                'tp': tp,
                'tp_id': res_performer.get('tp_id'),
                'fase': 0,
                'data_apertura': datetime.now().isoformat(),
                'voto_ia': voto,
                'snapshot_mercato': dati_tecnici,
                'ts': ts
            }
            
            self.posizioni_aperte[asset] = nuova_pos
            self.salva_posizioni()
            
            emoji_dir = "🚀 LONG" if direzione.upper() == "BUY" else "📉 SHORT"
            msg_telegram = (
                f"{emoji_dir} *ESEGUITO: {asset}*\n\n"
                f"💵 *Entry Price:* {entry_price} USD\n"
                f"⚙️ *Setup:* Leva {leverage}x | Voto IA: {voto}/10\n"
                f"🛡️ *Stop Loss:* {sl}\n"
                f"🎯 *Take Profit:* {tp}\n"
            )
            
            if self.alerts:
                self.alerts.invia_alert(msg_telegram)
            
            return nuova_pos
        return None

    def gestisci_protezione_istituzionale(self, asset, prezzo_attuale, atr_attuale=None):
        if asset not in self.posizioni_aperte:
            return False

        pos = self.posizioni_aperte[asset]
        p_entrata = float(pos['p_entrata'])
        direzione = pos['direzione']
        pnl = ((prezzo_attuale - p_entrata) / p_entrata) * 100
        if direzione == "SELL": pnl *= -1
        
        # --- FIX PRECISIONE DINAMICA ---
        prec = 5 if "XBT" in asset and "ZUSD" not in asset else 2
        
        nuovo_sl = None
        nuova_fase = pos.get('fase', 0)
        messaggio = ""

        noise_buffer = (atr_attuale * 1.5) if atr_attuale else (prezzo_attuale * 0.01)
        tp_target = float(pos['tp']) if pos.get('tp') else 0
        distanza_totale = abs(tp_target - p_entrata) if tp_target > 0 else (noise_buffer * 5)
        progresso_tp = (abs(prezzo_attuale - p_entrata) / distanza_totale) if distanza_totale > 0 else 0

        if nuova_fase < 1 and progresso_tp >= 0.50 and pnl >= 1.2:
            nuovo_sl = round(p_entrata * (1.0005 if direzione == "BUY" else 0.9995), prec)
            nuova_fase = 1
            messaggio = f"🛡️ *GOD MODE: BREAK-EVEN* per {asset}\n📈 Progresso: {round(progresso_tp*100, 1)}%"

        elif pnl >= 2.0:
            moltiplicatore_volatilita = 2.0 if pnl < 4.0 else 1.2
            distanza_trailing = (atr_attuale * moltiplicatore_volatilita) if atr_attuale else (prezzo_attuale * 0.012)
            temp_sl = round(prezzo_attuale - distanza_trailing, prec) if direzione == "BUY" else round(prezzo_attuale + distanza_trailing, prec)
            
            if direzione == "BUY":
                nuovo_sl_valido = max(temp_sl, p_entrata, pos.get('sl', 0))
                migliorato = nuovo_sl_valido > (pos.get('sl', 0) + (prezzo_attuale * 0.001))
            else:
                nuovo_sl_valido = min(temp_sl, p_entrata, pos.get('sl', 999999))
                migliorato = nuovo_sl_valido < (pos.get('sl', 999999) - (prezzo_attuale * 0.001))

            if migliorato:
                nuovo_sl = nuovo_sl_valido
                nuova_fase = 2
                messaggio = f"🔄 *UPDATE TRAILING* {asset}: {nuovo_sl}"

        if nuovo_sl is not None:
            id_esistente = pos.get('sl_id')
            risposta_api = None
            if self.performer:
                risposta_api = self.performer.sposta_stop_loss(
                    asset=asset, direzione_aperta=direzione, nuovo_sl=nuovo_sl, 
                    size=pos['size'], leverage=pos['leverage'], sl_id=id_esistente
                )

            if risposta_api:
                if isinstance(risposta_api, str):
                    pos['sl_id'] = risposta_api
                pos['sl'] = nuovo_sl
                pos['fase'] = nuova_fase
                
                # --- MODIFICA: INTEGRAZIONE GESTORE TAKE PROFIT FASE 2 ---
                if nuova_fase == 2 and pnl > 3.0: 
                    if pos.get('tp_id'):
                        try:
                            # Usiamo il nuovo metodo del Performer per pulizia professionale
                            self.performer.gestisci_take_profit(
                                asset=asset, direzione_aperta=direzione, nuovo_tp=None, 
                                size=pos['size'], leverage=pos['leverage'], tp_id=pos['tp_id']
                            )
                            self.logger.info(f"🚀 FASE 2: TP rimosso correttamente per {asset}")
                        except: 
                            pass
                    pos['tp'] = None 
                    pos['tp_id'] = None
                
                self.aggiorna_posizione(asset, pos)
                if self.alerts:
                    self.alerts.invia_alert(messaggio)
                return True
        return False
    
    def gestisci_protezione_dinamica(self, asset, prezzo_attuale, atr_attuale):
        # --- 1. CHECK SICUREZZA & RECUPERO DATI ---
        if not atr_attuale or atr_attuale <= 0 or not prezzo_attuale or prezzo_attuale <= 0:
            return

        pos = self.posizioni_aperte.get(asset)
        if not pos:
            return

        # Compatibilità: preferiamo p_entrata ma manteniamo fallback a entry_price
        entry_price = float(pos.get('p_entrata', pos.get('entry_price', 0)))
        direzione = pos.get('direzione')
        sl_attuale = float(pos.get('sl', 0)) if pos.get('sl') is not None else 0

        if entry_price <= 0:
            return

        # --- 2. CALCOLO PNL % ATTUALE ---
        if direzione == "BUY":
            pnl_perc = ((prezzo_attuale - entry_price) / entry_price) * 100
        else:
            pnl_perc = ((entry_price - prezzo_attuale) / entry_price) * 100

        # --- 3. LOGICA BREAKEVEN (Scatta quando il profitto copre 1.5x ATR) ---
        soglia_be_perc = ((atr_attuale * 1.5) / entry_price) * 100 if entry_price > 0 else float('inf')

        if pnl_perc > soglia_be_perc and pos.get('fase_protezione') != "BREAKEVEN":
            self.logger.info(f"🛡️ SOGLIA BE RAGGIUNTA per {asset}. Sposto SL a pareggio ({entry_price})")
            try:
                if self.performer and self.performer.aggiorna_stop_loss(
                        asset=asset,
                        direzione=direzione,
                        nuovo_sl=entry_price,
                        size=pos.get('size'),
                        leverage=pos.get('leverage', 1),
                        sl_id=pos.get('sl_id')):
                    pos['fase_protezione'] = "BREAKEVEN"
                    pos['sl'] = entry_price
                    self.salva_posizioni()
                    if self.alerts:
                        self.alerts.invia_alert(f"🛡️ *SAFE MODE*: SL a pareggio per {asset}")
            except Exception as e:
                self.logger.error(f"⚠️ Errore durante aggiornamento SL a pareggio per {asset}: {e}")

        # --- 4. TRAILING STOP (Attivo dopo il 50% del percorso verso il TP) ---
        tp_price = float(pos.get('tp', 0)) if pos.get('tp') else 0
        if tp_price > 0:
            distanza_tp = abs(tp_price - entry_price)
            progresso = abs(prezzo_attuale - entry_price) / distanza_tp if distanza_tp > 0 else 0

            if progresso > 0.50:
                # Preferiamo usare ts salvato in posizione se presente, altrimenti fallback all'ATR
                ts_saved = pos.get('ts')
                try:
                    offset = float(ts_saved) if ts_saved and float(ts_saved) > 0 else (atr_attuale * 2)
                except Exception:
                    offset = (atr_attuale * 2)

                nuovo_sl = (prezzo_attuale - offset) if direzione == "BUY" else (prezzo_attuale + offset)

                # Applichiamo solo se migliorativo (Lo SL non torna mai indietro)
                migliorato = False
                if direzione == "BUY" and nuovo_sl > sl_attuale:
                    migliorato = True
                if direzione == "SELL" and (sl_attuale == 0 or nuovo_sl < sl_attuale):
                    migliorato = True

                if migliorato:
                    try:
                        if self.performer and self.performer.aggiorna_stop_loss(
                                asset=asset,
                                direzione=direzione,
                                nuovo_sl=nuovo_sl,
                                size=pos.get('size'),
                                leverage=pos.get('leverage', 1),
                                sl_id=pos.get('sl_id')):
                            pos['sl'] = nuovo_sl
                            # Aggiorniamo anche lo stato protezione
                            pos['fase_protezione'] = "TRAILING"
                            # Log con precisione ragionevole dal ticker
                            try:
                                from core.asset_list import ASSET_CONFIG
                                symbol = al_config.get_ticker(asset)
                                prec_log = ASSET_CONFIG.get(asset, {}).get('precision', 2)
                                if symbol and "/" in symbol:
                                    _, quote = symbol.split("/")
                                    if quote.upper() not in ("USD", "EUR", "USDT", "ZUSD"):
                                        prec_log = max(prec_log, 5)
                            except Exception:
                                prec_log = 2
                            self.logger.info(f"📈 TRAILING: Nuovo SL per {asset} a {round(nuovo_sl, prec_log)}")
                            self.salva_posizioni()
                            if self.alerts:
                                self.alerts.invia_alert(f"🔄 *TRAILING UPDATE* {asset}: SL aggiornato a {round(nuovo_sl, prec_log)}")
                    except Exception as e:
                        self.logger.error(f"⚠️ Errore durante aggiornamento SL trailing per {asset}: {e}")

    def registra_conclusione_trade(self, asset, esito, pnl_finale):
        """
        LOGICA DI CHIUSURA: Salva nello storico e istruisce il FeedbackEngine.
        """
        if asset in self.posizioni_aperte:
            pos = self.posizioni_aperte.pop(asset)
            pos['data_chiusura'] = datetime.now().isoformat()
            pos['esito'] = esito
            pos['pnl_finale'] = pnl_finale
            
            # 1. Feedback per Gemini (Auto-Learning)
            if self.feedback_engine:
                self.feedback_engine.registra_feedback(
                    asset=asset,
                    score=pos.get('voto_ia', 0),
                    outcome=esito,
                    motivi=f"Chiusura con PNL: {round(pnl_finale, 2)}%",
                    snapshot=pos.get('snapshot_mercato')
                )
            
            # 2. Archiviazione Storica
            self.storico_trades.append(pos)
            self._salva_storico()
            self.salva_posizioni()
            self.logger.info(f"🏁 TRADE CONCLUSO: {asset} | Esito: {esito} | PNL: {pnl_finale}%")

    def aggiorna_posizione(self, asset, dati):
        if asset in self.posizioni_aperte:
            self.posizioni_aperte[asset].update(dati)
            self.salva_posizioni()

    def _carica_storico(self):
        if os.path.exists(self.file_storico):
            try:
                with open(self.file_storico, "r") as f: 
                    return json.load(f)
            except: 
                return []
        return []

    def _salva_storico(self):
        try:
            with open(self.file_storico, "w") as f:
                json.dump(self.storico_trades, f, indent=4)
        except Exception as e:
            self.logger.error(f"⚠️ Errore salvataggio storico: {e}")

    def _carica_stats_globali(self):
        if os.path.exists(self.file_stats):
            try:
                with open(self.file_stats, "r") as f: 
                    return json.load(f)
            except: 
                pass
        return {"max_drawdown": 0.0, "pnl_realizzato_totale": 0.0, "equity_peak": 0.0}