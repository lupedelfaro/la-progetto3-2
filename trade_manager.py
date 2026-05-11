# -*- coding: utf-8 -*-
import logging
import json
import os
import time
import uuid
from datetime import datetime, timedelta
from core import asset_list
from core.chimera_errors import ErrorTracker

_err = ErrorTracker("TradeManager")

class TradeManager:
    def __init__(self, file_posizioni="posizioni_aperte.json", alerts=None, performer=None, feedback_engine=None, engine=None):
        self.file_posizioni = file_posizioni
        self.file_storico = "storico_trades.json"
        self.file_stats = "stats_globali.json"
        self.logger = logging.getLogger("TradeManager")
        self.alerts = alerts
        self.performer = performer
        self.engine = engine
        
        self.posizioni_aperte = self._carica_posizioni()
        self.storico_trades = self._carica_storico()
        self.stats_globali = self._carica_stats_globali()
        self.feedback_engine = feedback_engine 
        self.cooldown_assets = {} # Ticker -> timestamp scadenza cooldown
        
        # HeikinAshiStrategy — collegata da bot_la dopo l'inizializzazione
        # (stesso pattern di chimera_ml)
        # trade_manager.ha_strategy = ha_strategy
        self.ha_strategy = None
        self._ha_ultimo_trailing = 0.0  # timestamp ultimo trailing SL HA

        # Parametri Killswitch (configurabili)
        self.max_daily_loss_pct = 0.03 # 3% di perdita massima giornaliera
        self.daily_pnl = 0.0
        self.last_pnl_reset = datetime.now().date()
        
        # SPECCHIO KRAKEN ALL'AVVIO
        if self.performer:
            try:
                self._sincronizza_da_kraken_avvio()
            except Exception as e_avvio:
                _err.capture(e_avvio, "__init__", {"module": "TradeManager"})
                self.logger.warning(f"Sincronizzazione Kraken all'avvio fallita: {e_avvio}. Uso stato DB.")

        # Ricalcolo PnL odierno all'avvio
        self._ricalcola_pnl_odierno()

    def _ricalcola_pnl_odierno(self):
        """Ricalcola il PnL in USD delle operazioni chiuse oggi, sincronizzando con Kraken."""
        oggi = datetime.now().date()
        self.daily_pnl = 0.0
        
        # 1. Calcolo interno dallo storico locale
        pnl_locale = 0.0
        for t in self.storico_trades:
            try:
                dt_str = t.get('data_chiusura', '').replace('Z', '+00:00')
                if not dt_str: continue
                dt_chiusura = datetime.fromisoformat(dt_str).date()
                if dt_chiusura == oggi:
                    pnl_locale += float(t.get('pnl_netto_usd', t.get('pnl_usd', 0)))
            except Exception:
                continue
        
        # 2. Sincronizzazione con Kraken Ledger (Fonte di Verità)
        pnl_reale = self.sincronizza_pnl_con_kraken()
        
        if pnl_reale is not None:
            self.daily_pnl = pnl_reale
            self.logger.info(f"📊 PnL odierno SINCRONIZZATO con Kraken: {self.daily_pnl:.2f}$ (Locale era: {pnl_locale:.2f}$)")
        else:
            self.daily_pnl = pnl_locale
            self.logger.info(f"📊 PnL odierno ricalcolato (solo locale): {self.daily_pnl:.2f}$")

    def sincronizza_pnl_con_kraken(self):
        """Recupera il PnL reale dalle ultime 24 ore di Kraken."""
        if not self.performer:
            return None
        try:
            return self.performer.get_realized_pnl_24h()
        except Exception as e:
            _err.capture(e, "sincronizza_pnl_con_kraken", {"module": "TradeManager"})
            self.logger.error(f"❌ Errore sincronizzazione PnL con Kraken: {e}")
            return None
  
    def formatta_prezzo(self, asset, prezzo):
        """
        Formatta un prezzo con la precisione corretta per l'asset.

        Fix dynamic assets (RLSUSD, ALGOUSD, ecc.):
          - ccxt price_to_precision richiede il simbolo ccxt ("RLS/USD")
            non l'ID Kraken ("RLSUSD"). Usiamo get_human_name() che ora
            legge anche _DYNAMIC_ASSETS.
          - Fallback adattivo sul valore del prezzo: indipendente dalla
            config, funziona correttamente per asset micro-price.
        """
        try:
            ticker     = asset_list.get_ticker(asset)
            ccxt_sym   = asset_list.get_human_name(ticker)   # "RLS/USD" per RLSUSD
            p          = float(prezzo)
            try:
                if not self.performer.exchange.markets:
                    self.performer.exchange.load_markets()
                return self.performer.exchange.price_to_precision(ccxt_sym, p)
            except Exception as e:
                _err.capture(e, "formatta_prezzo", {"module": "TradeManager"})
                self.logger.debug(
                    f"ℹ️ price_to_precision {ccxt_sym}: {e}. Uso fallback adattivo."
                )
                # Fallback adattivo — non dipende dalla config
                # Evita il bug precision=2 che converte 0.0055 → "0.01"
                if p < 0.001:   decimali = 8   # BONK, SHIB, PEPE
                elif p < 0.01:  decimali = 7   # FLOKI, RLS
                elif p < 0.1:   decimali = 6   # asset sub-centesimo
                elif p < 1:     decimali = 5   # asset < $1
                elif p < 10:    decimali = 4   # DOGE, XRP
                elif p < 1000:  decimali = 2   # SOL, ETH, TAO, WIF
                else:           decimali = 1   # BTC
                return f"{p:.{decimali}f}"
        except Exception as e:
            _err.capture(e, "formatta_prezzo", {"module": "TradeManager"})
            self.logger.warning(f"⚠️ Errore formattazione prezzo per {asset}: {e}")
            return str(prezzo)
            
    def set_take_profit(self, asset, tp_price, size, direzione, leverage):
        try:
            ticker = asset_list.get_ticker(asset)
            self.logger.info(f"🎯 Invio TP Istituzionale: {ticker} a {tp_price}")
            return self.performer.gestisci_ordine_protezione(
                asset=ticker, 
                tipo_protezione='take-profit', 
                prezzo=tp_price, 
                direzione_aperta=direzione, 
                size_fallback=size, 
                leverage=leverage
            )
        except Exception as e:
            _err.capture(e, "set_take_profit", {"module": "TradeManager"})
            self.logger.error(f"❌ Errore critico set_take_profit {asset}: {e}")

    def set_stop_loss(self, asset, sl_price, size, direzione, leverage):
        try:
            ticker = asset_list.get_ticker(asset)
            self.logger.info(f"🛡️ Invio SL Istituzionale: {ticker} a {sl_price}")
            return self.performer.gestisci_ordine_protezione(
                asset=ticker, 
                tipo_protezione='stop-loss', 
                prezzo=sl_price, 
                direzione_aperta=direzione, 
                size_fallback=size, 
                leverage=leverage
            )
        except Exception as e:
            _err.capture(e, "set_stop_loss", {"module": "TradeManager"})
            self.logger.error(f"❌ Errore critico set_stop_loss {asset}: {e}")
    
    def get_balance_margin(self, currency="USD", mode="margin"):
        """
        Recupera il bilancio da Kraken.
        mode="margin": restituisce il margine libero (mf)
        mode="equity": restituisce l'equity totale (e)
        """
        try:
            # Valute di fallback da controllare se la principale è a 0
            currencies_to_check = [currency]
            if currency == "USD":
                currencies_to_check.extend(["EUR", "USDT", "USDC"])
            elif currency == "EUR":
                currencies_to_check.extend(["USD", "USDT", "USDC"])

            # 1. Proviamo prima a usare il metodo specifico di Kraken (TradeBalance)
            if hasattr(self.performer, 'get_available_margin'):
                for curr in currencies_to_check:
                    asset_to_check = f"Z{curr}" if curr in ["USD", "EUR", "CAD", "JPY", "GBP"] else curr
                    try:
                        # get_available_margin restituisce il margine libero (mf)
                        # Dobbiamo estendere il performer per supportare l'equity
                        if mode == "equity" and hasattr(self.performer, 'get_total_equity'):
                            val = self.performer.get_total_equity(asset=asset_to_check)
                        else:
                            val = self.performer.get_available_margin(asset=asset_to_check)
                            
                        if val > 0:
                            self.logger.info(f"💰 {mode.capitalize()} disponibile ({curr}): {val:.2f}")
                            return val
                    except Exception as e:
                        _err.capture(e, "get_balance_margin", {"module": "TradeManager"})
                        self.logger.debug(f"Errore recupero {mode} per {curr}: {e}")
                        continue

            # 2. Fallback su fetch_balance()
            balances = self.performer.exchange.fetch_balance()
            
            # Se cerchiamo l'equity, fetch_balance di CCXT spesso la mette in 'total'
            if mode == "equity":
                total_balances = balances.get('total', {})
                for curr in currencies_to_check:
                    if curr in total_balances and total_balances[curr] > 0:
                        return total_balances[curr]
            
            free_balances = balances.get('free', {})
            
            # Fallback per Kraken se 'free' è vuoto ma 'info' -> 'result' contiene i dati
            if not free_balances and 'info' in balances and 'result' in balances['info']:
                result = balances['info']['result']
                for k, v in result.items():
                    if isinstance(v, dict) and 'balance' in v:
                        try:
                            bal = float(v.get('balance', 0))
                            hold = float(v.get('hold_trade', 0))
                            free_balances[k] = bal - hold
                        except (ValueError, TypeError):
                            pass
                    elif isinstance(v, str):
                        try:
                            free_balances[k] = float(v)
                        except ValueError:
                            pass
            
            # Cerchiamo nelle valute di fallback
            for curr in currencies_to_check:
                possible_keys = [curr, f"Z{curr}", f"X{curr}", curr.upper()]
                for key in possible_keys:
                    if key in free_balances:
                        val = float(free_balances[key])
                        if val > 0:
                            self.logger.info(f"💰 Margine disponibile (da fetch_balance): {val:.2f} {curr}")
                            return val
            
            self.logger.warning(f"⚠️ Nessuna valuta trovata con saldo > 0 tra {currencies_to_check}. Chiavi disponibili: {list(free_balances.keys())}")
            
            # Se proprio non troviamo nulla, restituiamo il saldo maggiore tra tutte le chiavi disponibili?
            # Meglio di no, potremmo usare una crypto volatile come margine. Restituiamo 0.0.
            return 0.0
        except Exception as e:
            _err.capture(e, "get_balance_margin", {"module": "TradeManager"})
            self.logger.error(f"🔴 Errore recupero balance margin {currency}: {e}")
            return 0.0

    def get_current_price(self, asset):
        try:
            symbol = asset_list.get_ticker(asset)
            # Use performer's get_current_price which we will update to be safer
            return float(self.performer.get_current_price(symbol))
        except Exception as e:
            _err.capture(e, "get_current_price", {"module": "TradeManager"})
            self.logger.error(f"🔴 Errore recupero prezzo per {asset}: {e}")
            return None
    
    def check_killswitch(self, capitale_totale):
        """
        Controlla se abbiamo raggiunto il limite di perdita giornaliera.
        Include il reset automatico del PnL al cambio del giorno.
        Ritorna (True, messaggio) se il bot deve fermarsi, (False, "") altrimenti.
        """
        # Reset automatico se è cambiato il giorno
        oggi = datetime.now().date()
        if oggi != self.last_pnl_reset:
            self.logger.info(f"📅 Cambio giorno rilevato ({self.last_pnl_reset} -> {oggi}). Reset PnL giornaliero.")
            self.daily_pnl = 0.0
            self.last_pnl_reset = oggi
            
        from core import config_la
        if not getattr(config_la, 'KILLSWITCH_ENABLED', True):
            return False, ""
            
        kill_limit = config_la.KILLSWITCH_GIORNALIERO
        
        if self.daily_pnl <= -(capitale_totale * kill_limit):
            msg = f"🛑 KILLSWITCH ATTIVO: PnL Odierno {self.daily_pnl:.2f}$ < -{kill_limit*100}% del capitale."
            return True, msg
            
        return False, ""

    def registra_chiusura_trade(self, trade_data):
        pnl = float(trade_data.get('pnl_netto_usd', 
                    trade_data.get('pnl_finale', 
                    trade_data.get('pnl', 0))))
        self.daily_pnl += pnl
        
        # ... (resto della logica di salvataggio storico esistente) ...
        self.storico_trades.append(trade_data)
        self._salva_storico()
        self.logger.info(f"✅ Trade registrato. PnL odierno: {self.daily_pnl:.2f}$")

    def _carica_posizioni(self):
        from core.database_manager import db_manager
        try:
            return db_manager.get_posizioni()
        except Exception as e:
            _err.capture(e, "_carica_posizioni", {"module": "TradeManager"})
            self.logger.error(f"⚠️ Errore caricamento posizioni da DB: {e}")
            return {}


    def _sincronizza_da_kraken_avvio(self):
        """
        All'avvio, legge le posizioni reali da Kraken e le rispecchia nello stato interno.
        Kraken è la fonte di verità — il DB è solo una cache.
        
        Logica:
        1. Posizioni su Kraken non nel DB → adottate automaticamente
        2. Posizioni nel DB non su Kraken → rimosse (già chiuse)
        3. Posizioni in entrambi → aggiornate con size/leva reali da Kraken
        """
        self.logger.info("🔄 [AVVIO] Lettura posizioni reali da Kraken...")
        try:
            posizioni_reali = self.performer.get_open_positions_real(force=True)
        except Exception as e:
            _err.capture(e, "_sincronizza_da_kraken_avvio", {"module": "TradeManager"})
            self.logger.error(f"❌ [AVVIO] Impossibile leggere posizioni Kraken: {e}")
            return

        from core.asset_list import ASSET_PRINCIPALI
        kraken_norm = {}
        # Kraken restituisce ogni posizione due volte: ticker ufficiale + alias
        # (es. XETHZUSD + ETHUSD, XXBTZUSD + BTCUSD)
        # Prima passata: prendi solo i ticker ufficiali (quelli in ASSET_PRINCIPALI)
        for txid, p in posizioni_reali.items():
            pair = p.get('pair', '')
            if not pair: continue
            if pair in ASSET_PRINCIPALI:
                norm = self.performer._normalize_ticker(pair)
                kraken_norm[norm] = (pair, txid, p)
        # Seconda passata: aggiungi solo quelli non ancora visti (alias non in ASSET_PRINCIPALI)
        # Questo cattura asset non standard eventualmente presenti
        for txid, p in posizioni_reali.items():
            pair = p.get('pair', '')
            if not pair: continue
            norm = self.performer._normalize_ticker(pair)
            if norm not in kraken_norm:
                kraken_norm[norm] = (pair, txid, p)

        # PASSO 1: aggiorna/adotta posizioni da Kraken
        for norm, (pair, txid, p_k) in kraken_norm.items():
            costo  = float(p_k.get('cost', 0))
            volume = float(p_k.get('vol', 0))
            margin = float(p_k.get('margin', 1) or 1)
            p_entry = costo / volume if volume > 0 else 0
            direzione = 'LONG' if p_k.get('type') == 'buy' else 'SHORT'
            leva = max(1, int(round(costo / margin))) if margin > 0 else 1

            # Cerca pos già nel diario (per chiave normalizzata, non solo esatta)
            chiave = None
            for k in list(self.posizioni_aperte.keys()):
                if self.performer._normalize_ticker(k) == norm:
                    chiave = k
                    break

            if chiave:
                # ── PRESERVA METADATA STORICO ──────────────────────────────
                # La pos era già nel diario (es. aperta dal bot prima del crash).
                # Aggiorniamo SOLO i campi che possono essere realmente cambiati su Kraken
                # (size, leverage, sl_id/tp_id se sono ordini reali, eventuali sl/tp price
                # da ordini Kraken). Tutto il resto (data_apertura, voto_ia, chimera_snapshot,
                # razionale, fase, fonte, sl/tp originali se non presenti su Kraken)
                # viene lasciato intatto.
                pos_old = self.posizioni_aperte[chiave]
                pos_old['size'] = volume
                pos_old['leverage'] = leva
                
                # SL/TP da Kraken hanno priorità SOLO se presenti come ordini reali su Kraken
                # (non virtual_sl). In caso contrario manteniamo quelli originali in memoria.
                _sl_kraken = p_k.get('sl_price_kraken')
                _tp_kraken = p_k.get('tp_price_kraken')
                _sl_id_kraken = p_k.get('sl_id_kraken')
                _tp_id_kraken = p_k.get('tp_id_kraken')
                
                if _sl_kraken and _sl_id_kraken:
                    pos_old['sl'] = _sl_kraken
                    pos_old['sl_id'] = _sl_id_kraken
                if _tp_kraken and _tp_id_kraken:
                    pos_old['tp'] = _tp_kraken
                    pos_old['tp_id'] = _tp_id_kraken
                
                # Aggiorna ordine_id Kraken (utile per attribution)
                if p_k.get('pos_txid'):
                    pos_old['ordine_id'] = p_k.get('pos_txid')
                
                # pending_limit non ha più senso se la pos esiste su Kraken: l'ordine è eseguito
                if pos_old.get('pending_limit'):
                    self.logger.info(f"🔓 [AVVIO] {pair}: pending_limit=False (LIMIT eseguito)")
                    pos_old['pending_limit'] = False
                
                # Sposta a chiave ufficiale Kraken se diversa
                if chiave != pair:
                    self.posizioni_aperte[pair] = self.posizioni_aperte.pop(chiave)
                
                self.logger.info(
                    f"🔄 [AVVIO] {pair}: PRESERVATA dal diario "
                    f"(aperta {pos_old.get('data_apertura','?')} v{pos_old.get('voto_ia','?')}, "
                    f"fase={pos_old.get('fase',0)}, sl={pos_old.get('sl')}, tp={pos_old.get('tp')}) — "
                    f"size/leva aggiornati da Kraken: size={volume} leva={leva}x"
                )
            else:
                # Posizione su Kraken NON nel diario — adozione vera (es. aperta manualmente)
                sl_emerg = p_entry * (0.97 if direzione == 'LONG' else 1.03)
                tp_emerg = p_entry * (1.05 if direzione == 'LONG' else 0.95)
                self.posizioni_aperte[pair] = {
                    'asset':         pair,
                    'ordine_id':     p_k.get('pos_txid'),
                    'direzione':     direzione,
                    'p_entrata':     p_entry,
                    'size':          volume,
                    'leverage':      leva,
                    'sl':            p_k.get('sl_price_kraken') or sl_emerg,
                    'tp':            p_k.get('tp_price_kraken') or tp_emerg,
                    'sl_id':         p_k.get('sl_id_kraken'),
                    'tp_id':         p_k.get('tp_id_kraken'),
                    'fase':          0,
                    'pending_limit': False,
                    'data_apertura': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    'nota':          'ADOTTATA_DA_KRAKEN_AVVIO',
                    'voto_ia':       None,
                    # FIX-C (2026-04-26): chimera_snapshot deve esistere anche per posizioni
                    # adottate, altrimenti chimera_ml.registra_trade_chiuso scarta il trade
                    # ("⚠️ Trade senza chimera_snapshot — impossibile imparare").
                    # Lo popolo con i campi che il modello sa gestire come "default conservativi".
                    # Marcarlo con fonte_apertura=ADOZIONE permette al filtro ML di gestirlo
                    # come categoria a parte invece di trattarlo come trade reale completo.
                    'chimera_snapshot': {
                        'fonte_apertura':  'ADOZIONE_KRAKEN',
                        'market_regime':   'UNKNOWN',
                        'entry_phase':     'ADOPTED',
                        'ciclo_fase':      'UNKNOWN',
                        'ha_daily_colore': '?',
                        'macro_sentiment': 'NEUTRAL',
                        'leverage':        leva,
                        'decision_source': 'EXTERNAL',
                    },
                    'fonte':         'ADOZIONE_KRAKEN',
                    'tipo_op':       'ADOPTED',
                    'razionale':     'Posizione adottata da Kraken all\'avvio del bot (non presente nel diario)',
                }
                self.logger.info(
                    f"✅ [AVVIO] {pair}: NUOVA ADOZIONE da Kraken "
                    f"(non era nel diario — {direzione}, leva={leva}x, size={volume}, entry~{p_entry:.4f})"
                )
                if self.alerts:
                    self.alerts.invia_alert(
                        f"📋 *Posizione adottata all'avvio*\n"
                        f"Asset: *{pair}* | {direzione} | Leva: {leva}x\n"
                        f"SL emergenza: {sl_emerg:.4f} | TP emergenza: {tp_emerg:.4f}\n"
                        f"_Non presente nel diario — adozione vera_"
                    )

        # PASSO 2: rimuovi posizioni nel DB non più su Kraken
        for asset in list(self.posizioni_aperte.keys()):
            norm_int = self.performer._normalize_ticker(asset)
            if norm_int not in kraken_norm:
                self.logger.info(f"🧹 [AVVIO] {asset}: non su Kraken, rimosso dallo stato interno")
                self.posizioni_aperte.pop(asset)

        self.salva_posizioni()
        n = len(self.posizioni_aperte)
        self.logger.info(f"✅ [AVVIO] Specchio Kraken completato — {n} posizioni attive")

    def salva_posizioni(self):
        """
        Salva l'intero dict posizioni_aperte sul DB.
        Da usare SOLO per sincronizzazioni bulk (avvio, sync periodico).
        Per singola posizione usa _salva_posizione(asset) o _rimuovi_posizione(asset).
        """
        from core.database_manager import db_manager
        try:
            db_manager.save_posizioni(self.posizioni_aperte)
        except Exception as e:
            _err.capture(e, "salva_posizioni", {"module": "TradeManager"})
            self.logger.error(f"⚠️ Errore salvataggio posizioni su DB: {e}")

    def _salva_posizione(self, asset: str):
        """
        Upsert atomico di UNA singola posizione — non tocca le altre.
        Usa questo per ogni modifica a una singola posizione (apertura, update SL/TP, fase).
        """
        from core.database_manager import db_manager
        try:
            pos = self.posizioni_aperte.get(asset)
            if pos:
                db_manager.upsert_posizione(asset, pos)
        except Exception as e:
            _err.capture(e, "_salva_posizione", {"module": "TradeManager"})
            self.logger.error(f"⚠️ Errore upsert posizione {asset}: {e}")

    def _rimuovi_posizione(self, asset: str):
        """
        Delete atomico di UNA singola posizione — non tocca le altre.
        Usa questo alla chiusura di una singola posizione.
        """
        from core.database_manager import db_manager
        try:
            db_manager.delete_posizione(asset)
        except Exception as e:
            _err.capture(e, "_rimuovi_posizione", {"module": "TradeManager"})
            self.logger.error(f"⚠️ Errore delete posizione {asset}: {e}")

    def _normalizza(self, s):
        return "".join(c for c in s.upper() if c.isalnum())

    def sincronizza_con_exchange(self, engine=None):
        self.logger.info("🔄 Avvio sincronizzazione istituzionale JSON <-> Kraken...")
        try:
            posizioni_real = self.performer.get_open_positions_real(force=True)
            ticker_reali = [p.get('pair') for p in posizioni_real.values() if p.get('pair')]
            # Costruisce set di normalizzazioni per evitare false rimozioni
            from core.asset_list import get_ticker as _gt
            norm_reali_set = set()
            for t in ticker_reali:
                norm_reali_set.add(self.performer._normalize_ticker(t))
                norm_reali_set.add(self.performer._normalize_ticker(_gt(t)))
                norm_reali_set.add(t)  # aggiungi anche il ticker raw
                norm_reali_set.add(_gt(t))  # e il ticker ufficiale
            norm_reali = list(norm_reali_set)
            
            for txid, p_kraken in posizioni_real.items():
                symbol_kraken = p_kraken.get('pair')
                norm_k = self.performer._normalize_ticker(symbol_kraken)
                
                # Cerchiamo se esiste già nel JSON (usando normalizzazione)
                trovato = False
                for k in list(self.posizioni_aperte.keys()):
                    if self.performer._normalize_ticker(k) == norm_k:
                        # Se la chiave non è quella ufficiale di Kraken, la aggiorniamo
                        if k != symbol_kraken:
                            self.logger.info(f"🔄 Aggiornamento ticker: {k} -> {symbol_kraken}")
                            self.posizioni_aperte[symbol_kraken] = self.posizioni_aperte.pop(k)
                        trovato = True
                        break
                
                if not trovato:
                    # ── PRIMA: cerca nel DIARIO (storico_trades + posizioni_aperte) per ordine_id Kraken ──
                    # Se la pos era già stata aperta dal bot (e poi persa per crash o chiave 
                    # normalizzata diversa), il record con ordine_id corrispondente esiste.
                    # In quel caso PRESERVA TUTTI I DATI ORIGINALI invece di buttarli via.
                    pos_recuperata = None
                    chiave_recuperata = None
                    
                    # 1. Cerca per pos_txid in self.posizioni_aperte
                    pos_txid_kraken = p_kraken.get('pos_txid')
                    if pos_txid_kraken:
                        for k_int, p_int in list(self.posizioni_aperte.items()):
                            if p_int.get('ordine_id') == pos_txid_kraken:
                                pos_recuperata = p_int
                                chiave_recuperata = k_int
                                self.logger.info(
                                    f"📋 [{symbol_kraken}] RECUPERATA dal diario per ordine_id={pos_txid_kraken} "
                                    f"(era con chiave {k_int})"
                                )
                                break
                    
                    # 2. Se non trovata, cerca per chiave normalizzata (caso non coperto dal loop sopra)
                    if not pos_recuperata:
                        for k_int, p_int in list(self.posizioni_aperte.items()):
                            if self.performer._normalize_ticker(k_int) == norm_k:
                                pos_recuperata = p_int
                                chiave_recuperata = k_int
                                self.logger.info(
                                    f"📋 [{symbol_kraken}] RECUPERATA dal diario per ticker normalizzato "
                                    f"(era con chiave {k_int})"
                                )
                                break
                    
                    if pos_recuperata:
                        # ── PRESERVA TUTTI I METADATA ORIGINALI ─────────────────
                        # data_apertura, voto_ia, chimera_snapshot, razionale, fase, fonte ecc.
                        # Aggiorna SOLO size, leverage, ordine_id, e SL/TP se ordini Kraken reali.
                        costo = float(p_kraken.get('cost', 0))
                        volume = float(p_kraken.get('vol', 0))
                        leverage_reale = int(costo / float(p_kraken.get('margin', 1))) if float(p_kraken.get('margin', 0)) > 0 else 1
                        if leverage_reale < 1: leverage_reale = 1
                        
                        pos_recuperata['size'] = volume
                        pos_recuperata['leverage'] = leverage_reale
                        if p_kraken.get('pos_txid'):
                            pos_recuperata['ordine_id'] = p_kraken.get('pos_txid')
                        
                        # SL/TP da Kraken: priorità SOLO se ordini reali registrati su Kraken
                        _sl_k = p_kraken.get('sl_price_kraken')
                        _tp_k = p_kraken.get('tp_price_kraken')
                        _sl_id_k = p_kraken.get('sl_id_kraken')
                        _tp_id_k = p_kraken.get('tp_id_kraken')
                        if _sl_k and _sl_id_k:
                            pos_recuperata['sl'] = _sl_k
                            pos_recuperata['sl_id'] = _sl_id_k
                        if _tp_k and _tp_id_k:
                            pos_recuperata['tp'] = _tp_k
                            pos_recuperata['tp_id'] = _tp_id_k
                        
                        # pending_limit a False (la pos esiste su Kraken = ordine eseguito)
                        if pos_recuperata.get('pending_limit'):
                            pos_recuperata['pending_limit'] = False
                            self.logger.info(f"🔓 [{symbol_kraken}] pending_limit=False (LIMIT eseguito su Kraken)")
                        
                        # Sposta a chiave ufficiale Kraken se diversa
                        if chiave_recuperata != symbol_kraken:
                            self.posizioni_aperte[symbol_kraken] = self.posizioni_aperte.pop(chiave_recuperata)
                        
                        self._salva_posizione(symbol_kraken)
                        self.logger.info(
                            f"✅ [{symbol_kraken}] PRESERVATA dal diario "
                            f"(aperta {pos_recuperata.get('data_apertura','?')} v{pos_recuperata.get('voto_ia','?')}, "
                            f"fase={pos_recuperata.get('fase',0)}, sl={pos_recuperata.get('sl')}, tp={pos_recuperata.get('tp')})"
                        )
                        # Salta la trascrizione "buttando via i dati" — abbiamo recuperato l'originale
                        continue
                    
                    # ── ALTRIMENTI: trascrizione vera (pos non era nel diario) ─────
                    self.logger.warning(f"🔧 Trascrizione: Posizione {symbol_kraken} rilevata su Kraken (ID: {txid}) — NON era nel diario.")
                    
                    self.logger.debug(f"DEBUG SINCRO | SL_ID: {p_kraken.get('sl_id_kraken')} | TP_ID: {p_kraken.get('tp_id_kraken')}")
                    
                    costo = float(p_kraken.get('cost', 0))
                    volume = float(p_kraken.get('vol', 0))
                    p_entry = costo / volume if volume > 0 else 0
                    direzione = 'LONG' if p_kraken.get('type') == 'buy' else 'SHORT'
                    
                    leverage_reale = int(costo / float(p_kraken.get('margin', 1))) if float(p_kraken.get('margin', 0)) > 0 else 1
                    if leverage_reale < 1: leverage_reale = 1

                    sl_da_applicare = 0
                    tp_da_applicare = 0
                    
                    # 1. Prova a recuperare SL/TP dal DB se la posizione era già tracciata
                    _pos_esistente = self.posizioni_aperte.get(symbol_kraken, {})
                    if _pos_esistente.get('sl') and float(_pos_esistente.get('sl', 0)) > 0:
                        sl_da_applicare = float(_pos_esistente['sl'])
                        tp_da_applicare = float(_pos_esistente.get('tp', 0))
                        self.logger.info(f"📋 [{symbol_kraken}] SL/TP recuperati dal DB: SL={sl_da_applicare} TP={tp_da_applicare}")

                    # 2. Se non nel DB, prova con brain.determina_tp_sl_ts + dati engine
                    if sl_da_applicare == 0 and engine:
                        try:
                            dati_eng = engine.analizza_asset(symbol_kraken) or {}
                            if dati_eng and float(dati_eng.get('close', 0)) > 0:
                                from core.brain_la import BrainLA as _BrainLA
                                _b = getattr(self, 'brain', None)
                                if _b:
                                    _tp_s, _sl_s, _ = _b.determina_tp_sl_ts(
                                        asset_name=symbol_kraken,
                                        direzione=direzione,
                                        prezzo_ingresso=p_entry,
                                        dati_engine=dati_eng,
                                        levels_ia={'stile_operativo': 'SWING'}
                                    )
                                    if _sl_s > 0:
                                        sl_da_applicare = _sl_s
                                        tp_da_applicare = _tp_s
                                        self.logger.info(
                                            f"🛡️ [{symbol_kraken}] SL/TP da sinergia: SL={_sl_s} TP={_tp_s}"
                                        )
                            # Fallback legacy
                            if sl_da_applicare == 0:
                                sl_da_applicare = float(dati_eng.get('sl', 0))
                                tp_da_applicare = float(dati_eng.get('tp', 0))
                        except Exception as e:
                            _err.capture(e, "sincronizza_con_exchange", {"module": "TradeManager"})
                            self.logger.debug(f"⚠️ Engine/brain per {symbol_kraken}: {e}")

                    # 3. Ultimo fallback: ±2%/5% (evitabile ma meglio di niente)
                    if sl_da_applicare == 0:
                        molt_sl = 0.98 if direzione == 'LONG' else 1.02
                        sl_da_applicare = float(self.formatta_prezzo(symbol_kraken, p_entry * molt_sl))
                        self.logger.warning(f"⚠️ [{symbol_kraken}] Nessun livello disponibile — SL emergenza 2%: {sl_da_applicare}")

                    if tp_da_applicare == 0:
                        molt_tp = 1.05 if direzione == 'LONG' else 0.95
                        tp_da_applicare = float(self.formatta_prezzo(symbol_kraken, p_entry * molt_tp))
                        self.logger.warning(f"⚠️ [{symbol_kraken}] Nessun livello disponibile — TP emergenza 5%: {tp_da_applicare}")

                    self.posizioni_aperte[symbol_kraken] = {
                        'asset': symbol_kraken,
                        'ordine_id': p_kraken.get('pos_txid'), 
                        'direzione': direzione,
                        'p_entrata': p_entry,
                        'size': volume,
                        'leverage': leverage_reale,
                        'sl': p_kraken.get('sl_price_kraken') or sl_da_applicare,
                        'tp': p_kraken.get('tp_price_kraken') or tp_da_applicare,
                        'sl_id': p_kraken.get('sl_id_kraken'),
                        'tp_id': p_kraken.get('tp_id_kraken'),
                        'fase': 0,
                        'pending_limit': False,
                        'data_apertura': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                        'nota': "TRASCRITTA_DA_KRAKEN",
                        'voto_ia': None,
                        # Chimera snapshot per non far scartare il trade da ML
                        'chimera_snapshot': {
                            'fonte_apertura':  'TRASCRIZIONE_KRAKEN',
                            'market_regime':   'UNKNOWN',
                            'entry_phase':     'TRANSCRIBED',
                            'ciclo_fase':      'UNKNOWN',
                            'ha_daily_colore': '?',
                            'macro_sentiment': 'NEUTRAL',
                            'leverage':        leverage_reale,
                            'decision_source': 'EXTERNAL',
                        },
                        'fonte':     'TRASCRIZIONE_KRAKEN',
                        'tipo_op':   'ADOPTED',
                        'razionale': 'Posizione rilevata su Kraken non presente nel diario',
                    }
                    self._salva_posizione(symbol_kraken)
                    self.sincronizza_e_ripara(symbol_kraken)

            for asset_in_json in list(self.posizioni_aperte.keys()):
                from core.asset_list import get_ticker as _gt2
                _n1 = self.performer._normalize_ticker(asset_in_json)
                _n2 = self.performer._normalize_ticker(_gt2(asset_in_json))
                _n3 = asset_in_json
                _n4 = _gt2(asset_in_json)
                if _n1 not in norm_reali_set and _n2 not in norm_reali_set and _n3 not in norm_reali_set and _n4 not in norm_reali_set:
                    self.logger.warning(f"🔄 Rilevata chiusura esterna per {asset_in_json}. Sincronizzazione storico...")
                    self._chiudi_statisticamente(asset_in_json)
            
            self.salva_posizioni()

            # Pulizia ordini orfani: include asset statici + dinamici
            _tutti_asset_noti = set(asset_list.ASSET_CONFIG.keys())
            try:
                from core.asset_list import _DYNAMIC_ASSETS
                _tutti_asset_noti.update(_DYNAMIC_ASSETS.keys())
            except Exception:
                pass
            for asset_config in _tutti_asset_noti:
                if asset_config not in ticker_reali and asset_config not in self.posizioni_aperte:
                    self.performer.pulizia_totale_ordini(asset_config)
            
        except Exception as e:
            _err.capture(e, "sincronizza_con_exchange", {"module": "TradeManager"})
            self.logger.error(f"❌ Errore critico sincronizzazione: {e}")
            import traceback
            traceback.print_exc()
    
    def is_posizione_aperta_su_kraken(self, asset):
        try:
            ticker_reale = asset_list.get_ticker(asset)
            
            # Forziamo un aggiornamento degli ordini aperti
            ordini_aperti = self.performer.get_open_orders_real()
            
            # Un ordine pendente conta come "posizione occupata" SOLO se è un ordine di ENTRY (limit/market)
            # Gli ordini di protezione (stop-loss/take-profit) non devono bloccare nuove analisi se sono orfani
            has_pending_entry = False
            for o in ordini_aperti.values():
                descr = o.get('descr', {})
                if descr.get('pair') == ticker_reale:
                    o_type = descr.get('ordertype', '').lower()
                    # Se è un ordine di entry (limit/market)
                    if o_type in ['limit', 'market']:
                        has_pending_entry = True
                        break
            
            posizioni_reali = self.performer.get_open_positions_real(force=True)
            # Cerca sia con ticker originale che normalizzato
            from core.asset_list import get_ticker as _gt3
            is_reale = (
                ticker_reale in posizioni_reali or
                any(p.get('pair') == ticker_reale for p in posizioni_reali.values())
            )
            
            if has_pending_entry or is_reale:
                return True

            # Se non c'è posizione reale e non c'è entry pendente, ma abbiamo ordini di protezione orfani, puliamoli
            has_orphaned_protection = any(o.get('descr', {}).get('pair') == ticker_reale for o in ordini_aperti.values())
            if has_orphaned_protection and not is_reale and not has_pending_entry:
                self.logger.warning(f"🧹 [PULIZIA] Rilevati ordini di protezione orfani per {ticker_reale}. Eseguo pulizia...")
                self.performer.pulizia_totale_ordini(ticker_reale)

            if (asset in self.posizioni_aperte or ticker_reale in self.posizioni_aperte) and not is_reale:
                self.logger.warning(f"🔄 Discrepanza: {ticker_reale} chiuso su Kraken. Sincronizzo diario...")
                self._chiudi_statisticamente(asset)
                if asset != ticker_reale:
                    self.posizioni_aperte.pop(ticker_reale, None)
                return False
                
            return False
        except Exception as e:
            _err.capture(e, "is_posizione_aperta_su_kraken", {"module": "TradeManager"})
            self.logger.error(f"❌ Errore verifica reale {asset}: {e}")
            return asset in self.posizioni_aperte
            
    def on_order_filled(self, symbol: str, event: dict):
        """
        Callback dal WS privato — ordine LIMIT eseguito in tempo reale.
        Aggiorna la posizione PENDING → attiva con prezzo e size reali.
        """
        try:
            ticker = asset_list.get_ticker(symbol) or symbol
            pos    = self.posizioni_aperte.get(ticker)
            if not pos:
                self.logger.debug(f"[WS FILL] {ticker}: nessuna posizione nel diario")
                return

            if not pos.get('pending_limit'):
                self.logger.debug(f"[WS FILL] {ticker}: posizione non in stato PENDING")
                return

            avg_price  = float(event.get("avg_price", 0) or 0)
            filled_qty = float(event.get("filled_qty", 0) or 0)
            order_id   = event.get("order_id", "") or event.get("cl_ord_id", "")

            if avg_price > 0:
                pos["p_entrata"]    = avg_price
            if filled_qty > 0:
                pos["size"]         = filled_qty
            pos["pending_limit"]    = False
            pos["ordine_id"]        = order_id or pos.get("ordine_id")
            pos["data_apertura"]    = datetime.now().isoformat()

            self._salva_posizione(ticker)
            self.logger.info(
                f"⚡ [WS FILL] {ticker} PENDING → ATTIVA: "
                f"p_entrata={avg_price} size={filled_qty} id={order_id}"
            )

            if self.alerts:
                self.alerts.invia_alert(
                    f"✅ *ORDINE ESEGUITO: {ticker}*\n"
                    f"Prezzo fill: {avg_price} | Size: {filled_qty}\n"
                    f"Posizione ora ATTIVA"
                )
        except Exception as e:
            _err.capture(e, "on_order_filled", {"module": "TradeManager", "symbol": symbol})
            self.logger.error(f"❌ on_order_filled {symbol}: {e}")

    def on_order_canceled(self, symbol: str, event: dict):
        """
        Callback dal WS privato — ordine LIMIT cancellato.
        Rimuove la posizione PENDING dal diario.
        """
        try:
            ticker = asset_list.get_ticker(symbol) or symbol
            pos    = self.posizioni_aperte.get(ticker)
            if not pos or not pos.get('pending_limit'):
                return

            order_id = event.get("order_id", "") or event.get("cl_ord_id", "")
            self.logger.info(
                f"🗑️ [WS CANCEL] {ticker} ordine {order_id} cancellato "
                f"— rimozione dal diario"
            )
            # Annulla TP se piazzato
            tp_id = pos.get("tp_id")
            if tp_id and not str(tp_id).startswith("virtual"):
                try:
                    self.performer.exchange.cancel_order(tp_id)
                except Exception:
                    pass

            self._rimuovi_posizione(ticker)
            if ticker in self.posizioni_aperte:
                del self.posizioni_aperte[ticker]

            if self.alerts:
                self.alerts.invia_alert(
                    f"🗑️ *ORDINE CANCELLATO: {ticker}*\n"
                    f"L'ordine LIMIT non è stato eseguito."
                )
        except Exception as e:
            _err.capture(e, "on_order_canceled", {"module": "TradeManager", "symbol": symbol})
            self.logger.error(f"❌ on_order_canceled {symbol}: {e}")

    def sincronizza_e_ripara(self, asset, engine=None, dati_kraken_esterni=None):
        try:
            ticker_reale = asset_list.get_ticker(asset)
            
            if dati_kraken_esterni:
                dati_kraken = dati_kraken_esterni
            else:
                posizioni_reali = self.performer.get_open_positions_real()
                dati_kraken = posizioni_reali.get(ticker_reale)
            
            is_reale = dati_kraken is not None
            ora_attuale = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

            if is_reale:
                leva_raw = dati_kraken.get('leverage', '1').split(':')[0] if isinstance(dati_kraken.get('leverage'), str) else 1
                leva_k = int(float(leva_raw))
                if leva_k <= 1:
                    # Calcolo leva reale se possibile
                    cost_p = float(dati_kraken.get('cost', 0))
                    margin_p = float(dati_kraken.get('margin', 0))
                    if margin_p > 0 and cost_p > 0:
                        leva_k = round(cost_p / margin_p)
                        from core.asset_list import get_config
                        conf = get_config(ticker_reale)
                        max_lev = conf.get("max_leverage", 10)
                        if leva_k > max_lev:
                            leva_k = max_lev
                    
                    if leva_k <= 1:
                        self.logger.debug(f"ℹ️ {ticker_reale} è una posizione a margine con leva 1x.")
            else:
                leva_k = 1

            if asset != ticker_reale and asset in self.posizioni_aperte:
                self.posizioni_aperte[ticker_reale] = self.posizioni_aperte.pop(asset)

            chiave_json = ticker_reale

            # Verifica posizione SPOT nel saldo (solo se non trovata come margine)
            if not is_reale and chiave_json in self.posizioni_aperte and self.posizioni_aperte[chiave_json].get('leverage', 1) == 1:
                try:
                    self.performer.exchange.load_markets()
                    market = self.performer.exchange.market(ticker_reale)
                    base_asset = market['base']
                    balance = self.performer.exchange.fetch_balance()
                    free_balance = balance.get(base_asset, {}).get('free', 0)
                    size_in_mem = float(self.posizioni_aperte[chiave_json].get('size', 0))
                    if free_balance >= size_in_mem * 0.9:
                        self.logger.debug(f"ℹ️ {ticker_reale} trovata come posizione SPOT nel saldo.")
                        is_reale = True
                except Exception as e_spot:
                    _err.capture(e_spot, "sincronizza_e_ripara", {"module": "TradeManager"})
                    self.logger.debug(f"⚠️ Errore verifica saldo SPOT per {ticker_reale}: {e_spot}")

            if chiave_json in self.posizioni_aperte and not is_reale:
                pos_mem = self.posizioni_aperte[chiave_json]

                # ── PENDING LIMIT: ordine inviato ma non ancora eseguito ──────
                # Non rimuovere finché l'ordine è ancora aperto su Kraken.
                # Timeout: 10 minuti — dopo annulla e rimuove.
                if pos_mem.get('pending_limit'):
                    entry_oid = str(pos_mem.get('ordine_id', '') or '')
                    try:
                        ordini_aperti = self.performer.get_open_orders_real()
                        # Cerca per order_id esatto
                        if entry_oid and entry_oid in ordini_aperti:
                            self.logger.info(
                                f"⏳ [{ticker_reale}] Ordine LIMIT {entry_oid} "
                                f"ancora aperto — mantengo nel diario"
                            )
                            return True
                        # Cerca per pair
                        found_pair = any(
                            self.performer._normalize_ticker(
                                o.get('descr', {}).get('pair', '')
                            ) == self.performer._normalize_ticker(ticker_reale)
                            for o in ordini_aperti.values()
                        )
                        if found_pair:
                            self.logger.info(
                                f"⏳ [{ticker_reale}] Ordine LIMIT trovato per pair "
                                f"— mantengo nel diario"
                            )
                            return True

                        # Ordine non trovato — controlla timeout (10 min)
                        data_ap = pos_mem.get('data_apertura', '')
                        try:
                            dt_ap = datetime.fromisoformat(str(data_ap))
                            minuti_attesa = (datetime.now() - dt_ap).total_seconds() / 60
                        except Exception:
                            minuti_attesa = 999

                        if minuti_attesa < 10:
                            self.logger.info(
                                f"⏳ [{ticker_reale}] Ordine LIMIT non trovato "
                                f"({minuti_attesa:.1f}min < 10min) — aspetto ancora"
                            )
                            return True
                        else:
                            self.logger.warning(
                                f"⏰ [{ticker_reale}] Ordine LIMIT scaduto dopo "
                                f"{minuti_attesa:.1f}min senza esecuzione — rimozione"
                            )
                            # Annulla TP se era stato piazzato
                            tp_id = pos_mem.get('tp_id')
                            if tp_id and not str(tp_id).startswith('virtual'):
                                try:
                                    self.performer.exchange.cancel_order(tp_id)
                                    self.logger.info(f"🗑️ TP {tp_id} annullato")
                                except Exception:
                                    pass
                            self._rimuovi_posizione(ticker_reale)
                            if ticker_reale in self.posizioni_aperte:
                                del self.posizioni_aperte[ticker_reale]
                            return False
                    except Exception as e_pend:
                        _err.capture(e_pend, "sincronizza_e_ripara", {"module": "TradeManager"})
                        self.logger.debug(f"⚠️ Check ordine pending {ticker_reale}: {e_pend}")
                        # In caso di errore API, non rimuovere — aspetta prossimo ciclo
                        return True
                # ─────────────────────────────────────────────────────────────

                # Prima di rimuovere, verifichiamo se c'è un ordine di entry pendente
                try:
                    ordini_aperti = self.performer.get_open_orders_real()
                    has_pending = any(o.get('descr', {}).get('pair') == ticker_reale for o in ordini_aperti.values())
                    if has_pending:
                        self.logger.info(f"⏳ {ticker_reale} non ancora aperta ma ordine pendente trovato. Mantengo nel diario.")
                        return True
                except Exception:
                    pass

                self.logger.warning(f"🧹 Posizione {ticker_reale} non trovata su Kraken (né Margin né Spot). Rimozione dal diario.")
                self._chiudi_statisticamente(chiave_json)
                return False

            if is_reale and chiave_json not in self.posizioni_aperte:
                # MODIFICA: Non adottiamo più posizioni manuali per non falsare le statistiche.
                # Lo Scudo Chimera deve agire solo su posizioni che il bot SA di aver aperto.
                self.logger.debug(f"ℹ️ Trovata posizione reale su {ticker_reale} non presente nel diario del bot. Ignoro per non falsare statistiche (Acquisto manuale).")
                return False

            if is_reale:
                pos_stat = self.posizioni_aperte[chiave_json]
                cambiamento = False

                sl_id_k = dati_kraken.get('sl_id_kraken')
                tp_id_k = dati_kraken.get('tp_id_kraken')

                # --- LOGICA PARACADUTE (SCUDO CHIMERA INTEGRATO) ---
                fase_attuale = pos_stat.get('fase', 0)
                
                # Aggiornamento ID se presenti su Kraken
                from core import config_la
                is_virtual_sl = getattr(config_la, 'VIRTUAL_STOP_LOSS', False)

                if sl_id_k:
                    if is_virtual_sl:
                        self.logger.info(f"🛡️ Virtual Stop Loss abilitato. Cancello SL reale {sl_id_k} per {ticker_reale}.")
                        self.performer.cancella_ordine_specifico(sl_id_k)
                        pos_stat['sl_id'] = f"virtual_sl_{int(time.time())}"
                        cambiamento = True
                    elif pos_stat.get('sl_id') != sl_id_k:
                        pos_stat['sl_id'] = sl_id_k
                        pos_stat['sl'] = dati_kraken.get('sl_price_kraken', pos_stat.get('sl'))
                        cambiamento = True
                
                if tp_id_k and pos_stat.get('tp_id') != tp_id_k:
                    pos_stat['tp_id'] = tp_id_k
                    pos_stat['tp'] = dati_kraken.get('tp_price_kraken', pos_stat.get('tp'))
                    cambiamento = True

                # Ripristino SL se mancante
                if not sl_id_k and not is_virtual_sl:
                    self.logger.warning(f"🛡️ Scudo Chimera (TM): Rilevata mancanza SL su {ticker_reale}!")
                    
                    atr_p = 0
                    prezzo_p = float(dati_kraken.get('price', 0))
                    vol_p = float(dati_kraken.get('vol', 0))
                    
                    if engine:
                        raw_p = engine.get_full_market_data(ticker_reale)
                        dati_p = raw_p[0] if isinstance(raw_p, tuple) else raw_p
                        atr_p = dati_p.get('atr', 0) if dati_p else 0
                        if prezzo_p == 0 and dati_p: prezzo_p = dati_p.get('close', 0)
                    
                    if prezzo_p == 0:
                        cost_p = float(dati_kraken.get('cost', 0))
                        if vol_p > 0: prezzo_p = cost_p / vol_p

                    direzione_p = "buy" if dati_kraken.get('type') == 'buy' else "sell"

                    prezzo_sl = float(pos_stat.get('sl', 0))
                    if prezzo_sl == 0:
                        dist_sl = atr_p * 2 if atr_p > 0 else prezzo_p * 0.015
                        dist_sl = max(dist_sl, prezzo_p * 0.015)
                        prezzo_sl = prezzo_p - dist_sl if direzione_p == "buy" else prezzo_p + dist_sl
                    
                    prezzo_sl_fmt = self.performer.qprice(ticker_reale, prezzo_sl)
                    res = self.performer.gestisci_ordine_protezione(ticker_reale, 'SL', prezzo_sl_fmt, direzione_p, vol_p, leva_k)
                    if res.get('success'):
                        pos_stat['sl_id'] = res.get('id')
                        pos_stat['sl'] = float(prezzo_sl_fmt)
                        cambiamento = True

                # Ripristino TP se mancante e non in Phase 2
                if not tp_id_k and fase_attuale < 2:
                    self.logger.warning(f"🛡️ Scudo Chimera (TM): Rilevata mancanza TP su {ticker_reale}!")
                    
                    atr_p = 0
                    prezzo_p = float(dati_kraken.get('price', 0))
                    vol_p = float(dati_kraken.get('vol', 0))
                    
                    if engine:
                        raw_p = engine.get_full_market_data(ticker_reale)
                        dati_p = raw_p[0] if isinstance(raw_p, tuple) else raw_p
                        atr_p = dati_p.get('atr', 0) if dati_p else 0
                        if prezzo_p == 0 and dati_p: prezzo_p = dati_p.get('close', 0)
                    
                    if prezzo_p == 0:
                        cost_p = float(dati_kraken.get('cost', 0))
                        if vol_p > 0: prezzo_p = cost_p / vol_p

                    direzione_p = "buy" if dati_kraken.get('type') == 'buy' else "sell"

                    prezzo_tp = float(pos_stat.get('tp', 0))
                    if prezzo_tp == 0:
                        dist_tp = atr_p * 3 if atr_p > 0 else prezzo_p * 0.03
                        dist_tp = max(dist_tp, prezzo_p * 0.03)
                        prezzo_tp = prezzo_p + dist_tp if direzione_p == "buy" else prezzo_p - dist_tp
                    
                    prezzo_tp_fmt = self.performer.qprice(ticker_reale, prezzo_tp)
                    res = self.performer.gestisci_ordine_protezione(ticker_reale, 'TP', prezzo_tp_fmt, direzione_p, vol_p, leva_k)
                    if res.get('success'):
                        pos_stat['tp_id'] = res.get('id')
                        pos_stat['tp'] = float(prezzo_tp_fmt)
                        cambiamento = True

                if cambiamento:
                    self._salva_posizione(ticker_reale)
                
                return True

            return False

        except Exception as e:
            _err.capture(e, "sincronizza_e_ripara", {"module": "TradeManager"})
            self.logger.error(f"❌ Errore critico riparazione {asset}: {e}")
            return asset in self.posizioni_aperte
    
    def _chiudi_statisticamente(self, asset):
        """Chiude una posizione solo nel diario (es. se chiusa esternamente su Kraken)."""
        try:
            ticker_reale = asset_list.get_ticker(asset)
            pos = self.posizioni_aperte.pop(ticker_reale, None)
            
            if not pos:
                # Fallback per chiavi vecchie o umane (non dovrebbe accadere dopo il fix)
                pos = self.posizioni_aperte.pop(asset, None)
            
            if pos:
                p_uscita = self.performer.get_current_price(ticker_reale) or float(pos.get('p_entrata', 0))
                p_entrata = float(pos.get('p_entrata', 0))
                size = float(pos.get('size', 0))
                direzione = pos.get('direzione', 'LONG').upper()
                
                # --- VERIFICA SE È UNA POSIZIONE FANTASMA ---
                # Se non troviamo tracce dell'ordine di entrata, non registriamo PnL
                entry_id = pos.get('entry_id')
                if entry_id and not str(entry_id).startswith("virtual"):
                    try:
                        # Verifichiamo se l'ordine di entrata ha effettivamente generato dei trade
                        entry_trades = self.performer.exchange.fetch_my_trades(symbol=ticker_reale, params={'ordertxid': entry_id})
                        if not entry_trades:
                            self.logger.warning(f"👻 Rilevata POSIZIONE FANTASMA per {ticker_reale} (Entry {entry_id} mai eseguito). Rimozione silenziosa.")
                            self._rimuovi_posizione(ticker_reale)
                            return
                    except Exception as e_check:
                        _err.capture(e_check, "_chiudi_statisticamente", {"module": "TradeManager"})
                        self.logger.debug(f"⚠️ Impossibile verificare entry_id {entry_id}: {e_check}")

                # Tentativo di recupero PnL REALE se abbiamo ID ordini di protezione
                pnl_netto_usd = None
                fees_reali = 0.0
                
                for oid in [pos.get('sl_id'), pos.get('tp_id')]:
                    if oid and not str(oid).startswith("virtual"):
                        real_data = self.performer.get_trade_pnl_real(oid)
                        if real_data and real_data.get('pnl_netto') != 0:
                            pnl_netto_usd = real_data['pnl_netto']
                            fees_reali = real_data['fee']
                            self.logger.info(f"🎯 PnL REALE recuperato per {asset} da ordine {oid}: {pnl_netto_usd}$")
                            break
                
                if pnl_netto_usd is not None:
                    pnl_usd = pnl_netto_usd + fees_reali
                    leverage_val = int(pos.get('leverage', 1) or 1)
                    margine = (p_entrata * size / leverage_val) if (p_entrata > 0 and leverage_val > 0) else 0
                    pnl_perc = (pnl_netto_usd / margine * 100) if margine > 0 else 0
                else:
                    # Fallback statistico se non troviamo l'ordine reale
                    leverage_val = int(pos.get('leverage', 1) or 1)
                    margine = (p_entrata * size / leverage_val) if (p_entrata > 0 and leverage_val > 0) else 0
                    if p_entrata > 0:
                        if direzione in ["SELL", "SHORT"]:
                            price_chg = (p_entrata - p_uscita) / p_entrata
                        else:
                            price_chg = (p_uscita - p_entrata) / p_entrata
                        pnl_perc = price_chg * leverage_val * 100
                        pnl_usd  = size * abs(p_uscita - p_entrata) * (1 if price_chg >= 0 else -1)
                    else:
                        pnl_perc = 0
                        pnl_usd  = 0
                    pnl_netto_usd = pnl_usd * 0.9974
                
                esito = "WIN" if pnl_netto_usd > 0 else "LOSS"
                
                pos.update({
                    'data_chiusura': datetime.now().isoformat(),
                    'p_uscita': p_uscita,
                    'pnl_finale': round(pnl_perc, 2),
                    'pnl_usd': round(pnl_usd, 2),
                    'pnl_netto_usd': round(pnl_netto_usd, 2),
                    'esito': esito,
                    'ticker_chiusura': ticker_reale,
                    'nota': pos.get('nota', '') + " | CHIUSURA_ESTERNA_SINCRONIZZATA"
                })
                
                self.storico_trades.append(pos)
                self._salva_storico()
                self.salva_posizioni()
                
                # Notifica ChimeraML — anche le posizioni adottate da Kraken alimentano XGBoost
                if hasattr(self, 'chimera_ml') and self.chimera_ml:
                    try:
                        voto_ghost = pos.get('voto_ia') or 5
                        snapshot_ghost = pos.get('chimera_snapshot', {})
                        # FIX: metodo corretto è registra_trade_chiuso
                        trade_data_ml = {
                            'asset':            ticker_reale,
                            'direzione':        direzione,
                            'voto_ia':          pos.get('voto_ia') or 5,
                            'esito':            esito,
                            'pnl_netto_usd':    pnl_netto_usd,
                            'chimera_snapshot': pos.get('chimera_snapshot', {}),
                            'fonte':            'GHOST_KRAKEN',
                            'data_apertura':    pos.get('data_apertura', ''),
                            'data_chiusura':    datetime.now().isoformat(),
                        }
                        self.chimera_ml.registra_trade_chiuso(trade_data_ml)
                    except Exception as e_ml:
                        _err.capture(e_ml, "_chiudi_statisticamente", {"module": "TradeManager"})
                        self.logger.debug(f"ChimeraML ghost skip: {e_ml}")

                # Ricalcoliamo il PnL odierno per riflettere la chiusura
                self._ricalcola_pnl_odierno()
                
                self.logger.info(f"🏁 Diario sincronizzato: {ticker_reale} chiuso con {pnl_netto_usd:.2f}$ ({esito})")
                
                if self.alerts:
                    self.alerts.invia_alert(f"🏁 *TRADE SINCRONIZZATO {ticker_reale}*\nEsito: {esito}\nPNL Netto: {pnl_netto_usd:.2f}$\n💰 *PNL Odierno:* {self.daily_pnl:.2f}$")
            else:
                self.logger.debug(f"ℹ️ Nessuna posizione attiva trovata nel diario per {asset} (già rimossa o inesistente).")

        except Exception as e:
            _err.capture(e, "_chiudi_statisticamente", {"module": "TradeManager"})
            self.logger.error(f"❌ Errore critico durante la chiusura statistica di {asset}: {e}")
            
    def apri_posizione(self, asset, direzione, entry_price, size, sl, tp, voto, leverage, dati_mercato, tipo_operazione="Swing", apprendimento_critico="", razionale=""):
        try:
            ticker_reale = asset_list.get_ticker(asset)
            tipo_op_upper = str(tipo_operazione).upper()

            if self.is_posizione_aperta_su_kraken(ticker_reale):
                self.logger.info(f"⏩ {ticker_reale} già aperta su Kraken. Salto l'apertura.")
                return False

            # Recuperiamo la configurazione dell'asset (limiti leva, decimali, ecc.)
            # get_config() cerca prima in ASSET_CONFIG (statici) poi in _DYNAMIC_ASSETS
            # (asset scoperti da auto_populate come ALGOUSD, TAOUSD, ecc.)
            conf = asset_list.get_config(ticker_reale)
            if not conf:
                self.logger.warning(
                    f"⚠️ Config mancante per {ticker_reale} — uso fallback conservativo "
                    f"(spot, min_size 1, precision 4)"
                )
                conf = {
                    "precision": 4,
                    "vol_precision": 2,
                    "min_size": 1.0,
                    "max_leverage": 1,
                    "is_cross": False,
                    "dna": f"Asset dinamico {ticker_reale} — config non disponibile."
                }

            # --- REGOLA LEVA ISTITUZIONALE ---
            # Se l'operazione è MULTIDAY, usiamo SPOT (1x)
            # Altrimenti usiamo la leva suggerita dall'IA (Gemini)
            from core import config_la
            
            # Recupero leva reale da Kraken se possibile (specifica per lato)
            max_lev_kraken = 3 # Fallback più realistico per asset comuni
            allowed_levs = [1, 2, 3]
            if self.engine and hasattr(self.engine, 'get_asset_leverage_info'):
                # Passiamo la direzione per avere la leva corretta per buy/sell
                lev_info = self.engine.get_asset_leverage_info(ticker_reale, side=direzione)
                max_lev_kraken = lev_info.get("max_leverage", 3)
                allowed_levs = lev_info.get("allowed_leverages", [1, 2, 3])
            elif self.engine:
                self.logger.warning(f"⚠️ Engine presente ma privo di get_asset_leverage_info per {ticker_reale}. Uso fallback 3x.")

            # FIX 2026-05-09 (Bug #1): _is_short definito qui (prima del branch MULTIDAY)
            # così è sempre in scope per il fallback SL/TP più sotto.
            # Era definito solo nel ramo else (linea sotto), causando NameError potenziale
            # nel ramo MULTIDAY se SL/TP arrivano a 0.
            _is_short = direzione.upper() in ('SHORT', 'SELL')

            if "MULTIDAY" in tipo_op_upper:
                leverage_f = None
                self.logger.info(f"🏦 Operazione MULTIDAY rilevata: Esecuzione SPOT (1x leverage).")
            else:
                user_max_lev = conf.get("max_leverage", 10)
                leva_suggerita_ia = float(leverage) if leverage else (2.0 if _is_short else 1.0)

                # ── SPOT vs MARGINE ────────────────────────────────────────
                # LONG SPOT:  leva=1 → compri con USD che possiedi. Valido.
                # LONG MARG:  leva>=2 → compri con margine. Valido.
                # SHORT SPOT: leva=1 → vendi crypto che non hai → Kraken rifiuta.
                # SHORT MARG: leva>=2 → vendi a margine (Kraken presta). Unico modo valido.
                if _is_short:
                    # SHORT richiede sempre leva >= 2
                    leva_minima = 2
                    leva_suggerita_ia = max(leva_suggerita_ia, leva_minima)
                    leverage_f = min(leva_suggerita_ia, user_max_lev, max_lev_kraken)
                    # Arrotonda alla leva permessa più vicina >= leva_minima
                    valid_levs = [l for l in allowed_levs if l >= leva_minima]
                    if not valid_levs:
                        valid_levs = [l for l in allowed_levs if l >= 1]
                    if valid_levs:
                        # Prende la più vicina al target senza superarla
                        candidates = [l for l in valid_levs if l <= leverage_f]
                        leverage_f = max(candidates) if candidates else min(valid_levs)
                    else:
                        leverage_f = leva_minima
                    self.logger.info(
                        f"📐 [{ticker_reale}] SHORT: leva suggerita {leva_suggerita_ia}x → "                        f"applicata {leverage_f}x (min 2x, Kraken max {max_lev_kraken}x)"
                    )
                else:
                    # LONG: può essere SPOT (leva=1) o a margine
                    leverage_f = min(leva_suggerita_ia, user_max_lev, max_lev_kraken)
                    if leverage_f > 1 and int(leverage_f) not in allowed_levs:
                        valid_levs = [l for l in allowed_levs if l <= leverage_f]
                        leverage_f = max(valid_levs) if valid_levs else 1
                    self.logger.info(
                        f"🧠 [{ticker_reale}] LONG: leva {leva_suggerita_ia}x → {leverage_f}x "                        f"(Kraken max {max_lev_kraken}x)"
                    )
            
            if not sl or sl == 0:
                distanza_emergenza = entry_price * 0.02
                # FIX 2026-05-09 (Bug #1): _is_short coerente con riga 1282.
                # Il check precedente "== BUY" non scattava per direzione="LONG"
                # (lo standard nel sistema), mettendo SL SOPRA l'entry → liquidazione.
                sl = entry_price + distanza_emergenza if _is_short else entry_price - distanza_emergenza
                self.logger.warning(f"⚠️ SL mancante per {ticker_reale}! Impostato 2%: {sl}")

            if not tp or tp == 0:
                distanza_emergenza_tp = entry_price * 0.05
                # FIX 2026-05-09 (Bug #1): vedi sopra.
                tp = entry_price - distanza_emergenza_tp if _is_short else entry_price + distanza_emergenza_tp
                self.logger.warning(f"⚠️ TP mancante per {ticker_reale}! Impostato 5%: {tp}")

            # --- CALCOLO SIZE DINAMICA (Formula Istituzionale) ---
            # Formula: size = (capitale_totale * rischio_perc_target) / distanza_SL_assoluta
            
            # 1. Recuperiamo il capitale totale
            capitale_totale = self.get_balance_margin(currency="USD")
            if capitale_totale <= 0:
                self.logger.error("❌ Capitale totale <= 0. Impossibile calcolare size.")
                return False
                
            # 2. Definiamo il rischio target per trade
            # Il parametro 'size' (es. 0.5) rappresenta la percentuale del rischio massimo (es. 1.5%)
            # Se l'IA dice 1.0, rischiamo l'1.5%. Se dice 0.5, rischiamo lo 0.75%.
            from core import config_la
            rischio_max_consentito = config_la.RISK_PER_TRADE # Ora letto dal Pannello di Controllo
            
            # Assicuriamoci che size sia un float valido
            try:
                size_factor = float(size)
            except (ValueError, TypeError):
                size_factor = 0.5 # Default 50% del rischio massimo
                
            # Limitiamo il size factor tra 0.01 e 1.0 per sicurezza
            size_factor = max(0.01, min(1.0, size_factor))
            
            rischio_perc_target = rischio_max_consentito * size_factor 
            
            # 3. Calcoliamo la distanza SL assoluta
            distanza_sl_assoluta = abs(entry_price - sl)
            if distanza_sl_assoluta == 0:
                self.logger.error("❌ Distanza SL = 0. Impossibile calcolare size.")
                return False
                
            # 4. Calcoliamo la size in USD
            valore_nominale_target = (capitale_totale * rischio_perc_target) / (distanza_sl_assoluta / entry_price)
            
            # Cap notional: mai più del 50% del (capitale × leva suggerita)
            # Evita che SL stretto produca notionali enormi che forzano leva massima
            _leva_suggerita_cap = float(leverage_f) if leverage_f else 3.0
            _cap_notional = capitale_totale * _leva_suggerita_cap * 0.50
            if valore_nominale_target > _cap_notional:
                self.logger.info(f"⚖️ Notional cappato: {valore_nominale_target:.1f}$ → {_cap_notional:.1f}$ (max 50% capitale×leva)")
                valore_nominale_target = _cap_notional
            valore_nominale_target = max(10.0, valore_nominale_target)
            
            self.logger.info(f"⚖️ Size Dinamica (Istituzionale): {valore_nominale_target:.2f}$ (Capitale: {capitale_totale:.2f}$, Rischio: {rischio_perc_target*100}%, Distanza SL: {distanza_sl_assoluta:.4f})")
            
            # --- GESTIONE MARGINE DINAMICO (FIX INSUFFICIENT MARGIN) ---
            # Usiamo il capitale_totale che abbiamo già recuperato, che rappresenta il margine libero
            margine_libero = capitale_totale
            
            # Filtro di Sopravvivenza: se il margine è inferiore a 5$, non possiamo operare in sicurezza
            if margine_libero < 5.0:
                self.logger.warning(f"🛑 Margine insufficiente ({margine_libero:.2f} < 5.00). Operazione annullata per evitare errori exchange.")
                if self.alerts:
                    self.alerts.invia_alert(f"🛑 *MARGINE INSUFFICIENTE {ticker_reale}*\nDisponibile: {margine_libero:.2f} (Minimo richiesto: 5.00)")
                return False

            self.logger.info(f"💳 Margine disponibile rilevato: {margine_libero:.2f}")

            # Se leverage_f è None, significa che l'operazione è SPOT (1x)
            if leverage_f is None:
                leva_richiesta = None
                margine_necessario = valore_nominale_target
            else:
                leva_richiesta = int(leverage_f)
                # Calcoliamo il margine necessario per il valore nominale target alla leva suggerita
                margine_necessario = valore_nominale_target / leva_richiesta
                
                max_leva_consentita = conf.get('max_leverage', 10)
                
                # Se il margine non basta, aumentiamo la leva fino al massimo consentito
                if margine_necessario > margine_libero:
                    self.logger.warning(f"⚠️ Margine insufficiente ({margine_libero:.2f} < {margine_necessario:.2f}) per leva suggerita {leva_richiesta}x.")
                    
                    if margine_libero <= 0:
                        self.logger.error("❌ Margine libero <= 0. Impossibile calcolare leva minima.")
                        return False
                    
                    # Calcoliamo la leva minima necessaria per far stare il valore nominale nel margine disponibile
                    leva_minima_necessaria = valore_nominale_target / margine_libero
                    
                    # Cerchiamo la leva minima permessa che soddisfi il requisito
                    nuova_leva_target = int(leva_minima_necessaria) + 1
                    
                    user_max_lev_limit = conf.get('max_leverage', 10)
                    abs_max_lev = min(max_lev_kraken, user_max_lev_limit)

                    # Troviamo la leva permessa più vicina (superiore o uguale a quella minima necessaria)
                    valid_levs = [l for l in allowed_levs if l >= nuova_leva_target and l <= abs_max_lev]
                    if valid_levs:
                        nuova_leva = min(valid_levs)
                    else:
                        # Se non c'è una leva permessa che soddisfi il requisito, usiamo la massima possibile
                        nuova_leva = abs_max_lev
                    
                    if nuova_leva > leva_richiesta:
                        self.logger.info(f"⚡ Aumento leva a {nuova_leva}x (max consentito) per coprire il valore nominale di {valore_nominale_target:.2f}$.")
                        leva_richiesta = nuova_leva
                        margine_necessario = valore_nominale_target / leva_richiesta

            # Se anche alla leva massima il margine non basta, riduciamo il valore nominale per adattarlo al residuo
            if margine_necessario > margine_libero:
                leva_effettiva = leva_richiesta if leva_richiesta is not None else 1
                valore_nominale_target = margine_libero * leva_effettiva * 0.80 # Usiamo l'80% per sicurezza (Kraken è rigido)
                self.logger.warning(f"📉 Margine ancora insufficiente a leva {leva_effettiva}x. Adatto la size al residuo: {valore_nominale_target:.2f}$.")
                
                # Se il valore nominale risultante è troppo piccolo per l'exchange (es. < 10$), annulliamo
                if valore_nominale_target < 10.0:
                    self.logger.warning(f"🛑 Valore nominale troppo basso ({valore_nominale_target:.2f}$) anche adattando la size. Annullato.")
                    return False

            if conf.get('is_cross'):
                quote_asset = conf.get('quote_asset', 'XXBTZUSD')
                prezzo_btc_usd = self.performer.get_current_price(quote_asset)
                
                if prezzo_btc_usd and float(prezzo_btc_usd) > 0 and entry_price > 0:
                    budget_in_btc = valore_nominale_target / float(prezzo_btc_usd)
                    size_istituzionale = budget_in_btc / entry_price
                    self.logger.info(f"🔄 Cross detected: {valore_nominale_target}$ -> {budget_in_btc:.6f} BTC")
                else:
                    self.logger.error(f"❌ Impossibile ottenere prezzo {quote_asset} o entry_price non valido. Aborto.")
                    return False
            else:
                if entry_price <= 0:
                    self.logger.error("❌ entry_price <= 0. Aborto.")
                    return False
                size_istituzionale = valore_nominale_target / entry_price

            try:
                self.performer.exchange.load_markets()
                size_istituzionale = float(self.performer.exchange.amount_to_precision(ticker_reale, size_istituzionale))
            except Exception as e:
                _err.capture(e, "apri_posizione", {"module": "TradeManager"})
                self.logger.warning(f"⚠️ Impossibile ottenere precisione del volume dinamica per {ticker_reale}: {e}. Uso fallback.")
                vol_prec = conf.get('vol_precision', 4) if conf.get('is_cross') else conf.get('vol_precision', 2)
                size_istituzionale = round(size_istituzionale, vol_prec)

            # Fallback a conf se non riusciamo a recuperare da Kraken
            min_size_consentita = conf.get('min_size', 0.0001)
            try:
                self.performer.exchange.load_markets()
                market = self.performer.exchange.market(ticker_reale)
                if market and 'limits' in market and 'amount' in market['limits'] and 'min' in market['limits']['amount']:
                    min_size_consentita = market['limits']['amount']['min']
            except Exception as e:
                _err.capture(e, "apri_posizione", {"module": "TradeManager"})
                self.logger.warning(f"⚠️ Impossibile recuperare min_size da Kraken per {ticker_reale}: {e}. Uso fallback: {min_size_consentita}")

            if size_istituzionale < min_size_consentita:
                size_istituzionale = min_size_consentita

            # --- FIX DEFINITIVO LEVA: RISPETTIAMO SEMPRE I LIMITI DI KRAKEN ---
            # Non usiamo solo il valore in conf, ma lo incrociamo con quello reale dell'exchange
            user_max_lev = conf.get('max_leverage', 10)
            abs_max_lev = min(max_lev_kraken, user_max_lev)
            
            leva_da_usare = min(leva_richiesta, abs_max_lev)
            
            # Assicuriamoci che sia una leva permessa (Kraken rifiuta se non è nella lista)
            if leva_da_usare > 1 and int(leva_da_usare) not in allowed_levs:
                valid_levs = [l for l in allowed_levs if l <= leva_da_usare]
                if valid_levs:
                    leva_da_usare = max(valid_levs)
                else:
                    leva_da_usare = 1 # Fallback spot

            # Regola critica: SHORT non può mai essere SPOT su Kraken
            # Vendere crypto senza possederle richiede il margine (leva >= 2)
            if direzione.upper() in ('SHORT', 'SELL') and leva_da_usare is not None and leva_da_usare <= 1:
                leva_short_min = 2
                # Cerca la leva minima permessa >= 2
                valid_short_levs = [l for l in allowed_levs if l >= leva_short_min]
                leva_da_usare = min(valid_short_levs) if valid_short_levs else leva_short_min
                self.logger.info(
                    f"📐 [{ticker_reale}] SHORT richiede leva — forzato a {leva_da_usare}x (no SPOT sell)"
                )

            # Se la leva finale è 1, la trattiamo come operazione SPOT (None per Kraken)
            if leva_da_usare is not None and leva_da_usare <= 1:
                leva_da_usare = None
                self.logger.info(f"🏦 Sizing Finale: {valore_nominale_target}$ | SL: {sl} | Voto: {voto} | Leva: SPOT (1x)")
            else:
                self.logger.info(f"💰 Sizing Finale: {valore_nominale_target}$ | SL: {sl} | Voto: {voto} | Leva: {leva_da_usare}x")

            # --- ESECUZIONE ORDINE ---
            risultato = self.performer.esegui_ordine(
                asset=ticker_reale, direzione=direzione, size=size_istituzionale,
                leverage=leva_da_usare, voto=voto, sl=sl, tp=tp
            )
            
            if not risultato or not risultato.get('success'):
                errore = str(risultato.get('error', '')).lower()
                self.logger.error(f"❌ Errore esecuzione ordine {ticker_reale}: {errore}")

            if risultato and risultato.get('success'):
                # Pausa per permettere a Kraken di registrare la posizione
                time.sleep(3.0)
                
                # force=True bypassa la cache — necessario subito dopo apertura ordine
                posizioni_reali = self.performer.get_open_positions_real(force=True)
                dati_kraken = next(
                    (p for p in posizioni_reali.values()
                     if p.get('pair') == ticker_reale or
                     self.performer._normalize_ticker(p.get('pair','')) == self.performer._normalize_ticker(ticker_reale)),
                    None
                )

                # Se non troviamo la posizione reale, verifichiamo se è un ordine limit ancora aperto
                if not dati_kraken:
                    self.logger.warning(f"⚠️ Ordine {ticker_reale} inviato ma posizione non trovata su Kraken. Verifico ordini aperti...")
                    # Aspetta un secondo in più per Kraken
                    time.sleep(2.0)
                    ordini_aperti = self.performer.exchange.private_post_openorders().get('result', {}).get('open', {})
                    # Prova sia order_id che id (Kraken può restituire entrambi)
                    oid = risultato.get('order_id') or risultato.get('id')
                    if oid and oid in ordini_aperti:
                        self.logger.info(f"⏳ Ordine {oid} per {ticker_reale} è ancora APERTO (LIMIT). Lo aggiungo al diario come PENDING.")
                        # Registra come PENDING — non ritornare False
                    else:
                        # Cerca per pair — l'ordine potrebbe essere lì con ID diverso
                        found_by_pair = any(
                            o.get('descr', {}).get('pair', '') == ticker_reale
                            for o in ordini_aperti.values()
                        )
                        if found_by_pair:
                            self.logger.info(f"⏳ Ordine per {ticker_reale} trovato per pair. Lo aggiungo al diario come PENDING.")
                        else:
                            self.logger.error(f"❌ Ordine {oid} per {ticker_reale} non trovato. Potrebbe essere stato cancellato o rifiutato silenziosamente.")
                            return False

                p_entrata_finale = float(dati_kraken.get('price', entry_price)) if dati_kraken else entry_price
                size_finale = float(dati_kraken.get('vol', size_istituzionale)) if dati_kraken else size_istituzionale
                _is_pending = (dati_kraken is None)
                
                oid = risultato.get('order_id')
                sid = risultato.get('sl_id')
                tid = risultato.get('tp_id')

                # Snapshot arricchito per AdvancedReporter e Time-Stop
                _snap = dict(dati_mercato) if isinstance(dati_mercato, dict) else {}
                _snap_clean = {
                    k: v for k, v in _snap.items()
                    if not isinstance(v, (list, dict)) or k in (
                        'flusso_hft_primario', 'analisi_profonda', 'mappa_volumetrica'
                    )
                }

                self.posizioni_aperte[ticker_reale] = {
                    'asset':            ticker_reale,
                    'direzione':        direzione.upper(),
                    'ordine_id':        oid,
                    'p_entrata':        p_entrata_finale,
                    'size':             size_finale,
                    'leverage':         leva_da_usare,
                    'sl':               sl if not _is_pending else (entry_price * 1.03 if direzione.upper() in ('SELL','SHORT') else entry_price * 0.97),
                    'tp':               tp,
                    'voto_ia':          voto,
                    'data_apertura':    datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    'fase':             0,
                    'sl_id':            sid,
                    'tp_id':            tid,
                    'pending_limit':    _is_pending,
                    # ── CHIMERA v4: campi critici per Time-Stop e Reporter ──
                    'tipo_op':          str(tipo_operazione),
                    'timeframe':        str(dati_mercato.get('timeframe_riferimento', 'N/A')),
                    'razionale':        razionale or str(dati_mercato.get('razionale', 'N/A')),
                    'apprendimento_critico': apprendimento_critico,
                    'market_regime':    str(_snap.get('market_regime', 'UNKNOWN')),
                    'hurst_entry':      float(_snap.get('hurst_exponent', 0.5)),
                    'chimera_snapshot': _snap_clean,
                }
                
                self._salva_posizione(ticker_reale)
                self.logger.info(f"✅ REGISTRAZIONE REALE COMPLETATA {ticker_reale}: Entry={oid}")
                
                if self.alerts:
                    self.alerts.invia_alert(
                        f"🟢 *NUOVO TRADE APERTO: {ticker_reale}*\n"
                        f"Direzione: {direzione.upper()} | Tipo: {tipo_operazione}\n"
                        f"Prezzo: {p_entrata_finale} | Size: {size_finale} | Leva: {leva_da_usare}x\n"
                        f"SL: {sl} | TP: {tp}\n"
                        f"Razionale: {str(dati_mercato.get('razionale', 'N/A'))}"
                    )
                
                return True
                
            return False

        except Exception as e:
            _err.capture(e, "apri_posizione", {"module": "TradeManager"})
            self.logger.error(f"❌ Errore critico in apri_posizione per {asset}: {e}")
            return False
            
    def check_invalidazione_tesi(self, asset, dati_mercato):
        """
        Rileva deterministicamente i 5 pattern di invalidazione tesi:
        1. CHoCH confermato (Change of Character H1)
        2. CVD divergenza massiccia e prolungata
        3. Liquidity Sweep + assorbimento (VPIN spike su rottura)
        4. Whale delta inversione brusca
        5. Funding squeeze estremo + OI in calo

        Restituisce:
          (trigger: bool, n_pattern: int, motivo: str, direzione_suggerita: str)
        """
        ticker = asset_list.get_ticker(asset)
        pos = self.posizioni_aperte.get(ticker)
        if not pos:
            return False, 0, "", ""

        direzione     = pos.get('direzione', 'LONG').upper()
        is_long       = direzione in ('LONG', 'BUY')
        p_entrata     = float(pos.get('p_entrata', 0))
        prezzo        = float(dati_mercato.get('close', 0))
        pnl_perc      = ((prezzo - p_entrata) / p_entrata * 100) if is_long else ((p_entrata - prezzo) / p_entrata * 100)

        # Non triggerare se la posizione è appena entrata (<5 minuti)
        try:
            from datetime import datetime
            data_ap = datetime.fromisoformat(pos.get('data_apertura', '').replace('Z', ''))
            minuti_apertura = (datetime.now() - data_ap).total_seconds() / 60
        except Exception:
            minuti_apertura = 999

        if minuti_apertura < 5:
            return False, 0, "posizione troppo recente", ""

        # Non triggerare se in profit > 2% (rischio whipsawing su trade buoni)
        if pnl_perc > 2.0:
            return False, 0, "in profit solido", ""

        pattern_rilevati = []
        dir_suggerita = "SHORT" if is_long else "LONG"

        cvd      = float(dati_mercato.get('cvd_istantaneo', 0))
        vpin     = float(dati_mercato.get('vpin', 0))
        velocity = float(dati_mercato.get('price_velocity', 0))
        whale    = float(dati_mercato.get('whale_delta', 0))
        ofi      = float(dati_mercato.get('order_flow_imbalance', 0))
        hurst    = float(dati_mercato.get('hurst_exponent', 0.5))
        funding_z = float(dati_mercato.get('funding_z_score', 0))
        struttura = str(dati_mercato.get('struttura_h1', ''))
        ultimo_choch = str(dati_mercato.get('ultimo_choch', ''))
        bos = str(dati_mercato.get('ultimo_bos', ''))

        # ── PATTERN 1: CHoCH confermato nella direzione opposta ──────────────
        choch_contro = (
            is_long and ultimo_choch in ('BEARISH', 'SELL')
        ) or (
            not is_long and ultimo_choch in ('BULLISH', 'BUY')
        )
        struttura_contro = (
            is_long and struttura == 'DOWNTREND'
        ) or (
            not is_long and struttura == 'UPTREND'
        )
        if choch_contro and struttura_contro:
            pattern_rilevati.append("CHoCH+struttura confermata contro posizione")

        # ── PATTERN 2: CVD divergenza massiccia ──────────────────────────────
        # Prezzo si muove a favore ma CVD fortemente contro (distribuzione/accumulo)
        cvd_soglia = 80000  # >80k USD netto contro
        cvd_contro_long  = is_long     and velocity > 0 and cvd < -cvd_soglia
        cvd_contro_short = (not is_long) and velocity < 0 and cvd > cvd_soglia
        if cvd_contro_long or cvd_contro_short:
            pattern_rilevati.append(f"CVD divergenza massiccia ({cvd:.0f}) contro {direzione}")

        # ── PATTERN 3: Liquidity Sweep + assorbimento istituzionale ──────────
        # VPIN alto su rottura di un livello + prezzo torna indietro
        dist_sup = float(dati_mercato.get('dist_supporto', 999))
        dist_res = float(dati_mercato.get('dist_resistenza', 999))
        sup_sweepato = bool(dati_mercato.get('supporto_sweepato', False))
        res_sweepata = bool(dati_mercato.get('resistenza_sweepata', False))
        sweep_contro_long  = is_long     and sup_sweepato and vpin > 0.70
        sweep_contro_short = (not is_long) and res_sweepata and vpin > 0.70
        if sweep_contro_long or sweep_contro_short:
            pattern_rilevati.append(f"Liquidity sweep + VPIN alto ({vpin:.2f}) — assorbimento contro {direzione}")

        # ── PATTERN 4: Whale delta inversione brusca ─────────────────────────
        # Whale e OFI entrambi forti contro la direzione
        whale_contro_long  = is_long     and whale < -0.5 and ofi < -0.4
        whale_contro_short = (not is_long) and whale > +0.5 and ofi > +0.4
        if whale_contro_long or whale_contro_short:
            pattern_rilevati.append(f"Whale ({whale:.2f}) + OFI ({ofi:.2f}) entrambi contro {direzione}")

        # ── PATTERN 5: Funding squeeze estremo ───────────────────────────────
        # Funding estremo nella direzione del trade = troppi retail dalla stessa parte
        funding_contro_long  = is_long     and funding_z > 2.5  # troppi long = squeeze
        funding_contro_short = (not is_long) and funding_z < -2.5  # troppi short = squeeze
        if funding_contro_long or funding_contro_short:
            pattern_rilevati.append(f"Funding squeeze estremo (z={funding_z:.1f}) contro {direzione}")

        n = len(pattern_rilevati)
        trigger = n >= 2  # almeno 2 pattern concordi

        if trigger:
            motivo = f"{n}/5 pattern invalidazione: " + " | ".join(pattern_rilevati)
            self.logger.warning(f"⚠️ [{ticker}] TESI POTENZIALMENTE INVALIDATA — {motivo}")
        else:
            motivo = f"{n}/5 pattern (soglia 2)" + (f": {pattern_rilevati[0]}" if pattern_rilevati else "")

        return trigger, n, motivo, dir_suggerita

    def gestisci_protezione_istituzionale(self, asset, prezzo_attuale, atr_attuale=0, dati_mercato=None):
        if dati_mercato is None:
            dati_mercato = {'atr': atr_attuale, 'close': prezzo_attuale}
        else:
            dati_mercato['close'] = prezzo_attuale
            
        ticker = asset_list.get_ticker(asset)
        pos = self.posizioni_aperte.get(ticker)
        if not pos: return

        # ── TRAILING SL HEIKIN ASHI DAILY ────────────────────────────────────
        # Se la posizione è gestita dalla HeikinAshiStrategy (flag trailing_ha_daily),
        # lo SL si sposta una volta al giorno alla chiusura della candela (00:00 UTC).
        # Non usiamo il trailing standard Hurst/VPIN per queste posizioni.
        _componenti = pos.get('chimera_snapshot') or pos.get('components') or {}
        if _componenti.get('trailing_ha_daily') and self.ha_strategy:
            from datetime import timezone
            _ora_utc = datetime.utcnow()
            # Finestra 00:00-00:05 UTC = candela appena chiusa
            _e_chiusura_daily = (_ora_utc.hour == 0 and _ora_utc.minute < 5)
            # Guard: esegue una sola volta per notte
            _ore_da_ultimo = (time.time() - self._ha_ultimo_trailing) / 3600
            if _e_chiusura_daily and _ore_da_ultimo > 12:
                direzione_pos = pos.get('direzione', 'LONG').upper()
                sl_attuale_ha = float(pos.get('sl', 0))
                try:
                    # Forza aggiornamento cache HA (candela di ieri appena chiusa)
                    self.ha_strategy._cache_ts = 0.0
                    self.ha_strategy._aggiorna_cache(ticker)
                    nuovo_sl = self.ha_strategy.aggiorna_trailing_sl(direzione_pos, sl_attuale_ha)
                    if nuovo_sl:
                        pos['sl'] = nuovo_sl
                        self._salva_posizione(ticker)
                        self._ha_ultimo_trailing = time.time()
                        self.logger.info(
                            f"🕯️ HeikinAshi trailing SL [{ticker}]: "
                            f"{sl_attuale_ha:.0f}$ → {nuovo_sl:.0f}$ "
                            f"({direzione_pos})"
                        )
                        if self.alerts:
                            self.alerts.invia_alert(
                                f"🕯️ *HeikinAshi — Trailing SL*\n"
                                f"Asset: *{ticker}* | {direzione_pos}\n"
                                f"SL: {sl_attuale_ha:.0f}$ → *{nuovo_sl:.0f}$*"
                            )
                    else:
                        self._ha_ultimo_trailing = time.time()
                        self.logger.info(
                            f"🕯️ HeikinAshi [{ticker}]: SL invariato "
                            f"({sl_attuale_ha:.0f}$ già ottimale)"
                        )
                except Exception as e_ha:
                    _err.capture(e_ha, "gestisci_protezione_istituzionale", {"module": "TradeManager"})
                    self.logger.error(f"❌ HeikinAshi trailing SL [{ticker}]: {e_ha}")
            # Per posizioni HA non entra nella logica standard — return anticipato
            # Il Virtual SL rimane attivo e gestisce l'uscita se il prezzo colpisce lo SL
            return

        # ── GUARD POSIZIONI HA MARGINE (Trend / Contro Trend) ────────────────
        # Posizioni con virtual_sl=True e tp_fisso=True sono gestite dalle
        # strategie HeikinAshiTrendStrategy e HeikinAshiControTrendStrategy.
        # Il loro SL è fisso al 1% dall'entry — NON deve essere toccato dal
        # trailing Hurst/VPIN che lo sposterebbe in posizioni sbagliate.
        # La gestione BE al 50% e scaling all'80% rimane attiva normalmente.
        if _componenti.get('virtual_sl') and _componenti.get('tp_fisso'):
            # Salta trailing intelligente e chiusura parziale strategica
            # ma continua con la logica BE/scaling standard sotto
            pass
        else:
            # --- Trailing Stop Intelligente (Hurst/VPIN) ---
            self.gestisci_trailing_stop_intelligente(ticker, dati_mercato)
            # --- Chiusura Parziale Strategica (Muri/Pressione) ---
            self._gestisci_chiusura_parziale_strategica(ticker, dati_mercato)

        p_entrata    = float(pos['p_entrata'])
        tp_target    = float(pos['tp'])
        sl_attuale   = float(pos.get('sl', 0))
        direzione    = pos['direzione'].upper()
        fase_attuale = pos.get('fase', 0)
        symbol_kraken = ticker

        # Normalizza direzione — il record può contenere LONG/SHORT o BUY/SELL
        _is_long = direzione in ("BUY", "LONG")
        distanza_attuale = (prezzo_attuale - p_entrata) if _is_long else (p_entrata - prezzo_attuale)
        pnl_perc = distanza_attuale / p_entrata * 100 if p_entrata > 0 else 0

        # Log protezione ad ogni ciclo per monitoraggio
        self.logger.debug(
            f"🛡️ PROTEZIONE [{ticker}] Fase:{fase_attuale} | "
            f"PnL: {pnl_perc:+.2f}% | "
            f"Entry:{p_entrata:.2f} | Prezzo:{prezzo_attuale:.2f} | "
            f"TP:{tp_target:.2f} | SL:{sl_attuale:.2f}"
        )

        from core import config_la
        if getattr(config_la, 'VIRTUAL_STOP_LOSS', False) and sl_attuale > 0:
            sl_hit = (prezzo_attuale <= sl_attuale) if _is_long else (prezzo_attuale >= sl_attuale)
            if sl_hit:
                # Grace period 90s — non scattare su ordini LIMIT non ancora eseguiti
                try:
                    data_ap = datetime.fromisoformat(pos.get('data_apertura','').replace('Z',''))
                    secondi_apertura = (datetime.now() - data_ap).total_seconds()
                except Exception:
                    secondi_apertura = 9999
                if secondi_apertura < 90:
                    self.logger.info(f"⏳ [{ticker}] aperta {secondi_apertura:.0f}s fa — grace period VSL, attendo.")
                else:
                    self.logger.warning(f"🚨 VIRTUAL STOP LOSS COLPITO per {ticker} a {prezzo_attuale} (SL: {sl_attuale}). Eseguo chiusura a mercato!")
                    if self._esegui_chiusura_totale(ticker, prezzo_attuale, motivo="VIRTUAL_SL"):
                        return
                    else:
                        self.logger.error(f"❌ Fallita chiusura totale per {ticker} dopo VIRTUAL SL. Riproverò al prossimo ciclo.")

        if fase_attuale >= 2:
            # Trailing stop continuo (molto più stretto in Fase 2)
            distanza_trailing = max(prezzo_attuale * 0.005, atr_attuale * 1.5) if atr_attuale > 0 else prezzo_attuale * 0.005
            nuovo_sl_dinamico = prezzo_attuale - distanza_trailing if _is_long else prezzo_attuale + distanza_trailing
            
            nuovo_sl_dinamico = float(self.performer.qprice(symbol_kraken, nuovo_sl_dinamico))
            sl_in_memoria = float(pos.get('sl', 0))
            
            self.logger.debug(f"  → [FASE 2] Trailing Stop calcolato: {nuovo_sl_dinamico:.2f} (Distanza: {distanza_trailing:.2f})")
            
            is_migliore = nuovo_sl_dinamico > sl_in_memoria if _is_long else nuovo_sl_dinamico < sl_in_memoria
            
            if is_migliore:
                vecchio_id_trail = pos.get('sl_id')
                if vecchio_id_trail:
                    if self.performer.cancella_ordine_specifico(vecchio_id_trail):
                        time.sleep(1.0)
                        
                        res_upd = self.performer.gestisci_ordine_protezione(
                            ticker, 'stop-loss', nuovo_sl_dinamico, direzione, pos['size'], pos['leverage']
                        )

                        if res_upd and res_upd.get('success'):
                            pos['sl'] = nuovo_sl_dinamico
                            pos['sl_id'] = res_upd.get('id')
                            self._salva_posizione(asset)
                            self.logger.debug(f"📈 {ticker}: Trailing aggiornato a {nuovo_sl_dinamico}")
            return

        distanza_totale = abs(tp_target - p_entrata)
        if distanza_totale == 0: return

        progresso_percentuale  = (distanza_attuale / distanza_totale) * 100 if distanza_totale > 0 else 0
        distanza_sicurezza_atr = atr_attuale * 1.2 if atr_attuale > 0 else 0

        self.logger.debug(
            f"  → Progresso: {progresso_percentuale:.1f}% verso TP | "
            f"ATR soglia: {distanza_sicurezza_atr:.4f} | "
            f"Dist attuale: {distanza_attuale:.4f}"
        )

        if 50 <= progresso_percentuale < 80 and fase_attuale < 1 and distanza_attuale > distanza_sicurezza_atr:
            nuovo_sl = p_entrata
            self.logger.debug(f"🛡️ {ticker} al 50% e sopra soglia sicurezza ATR. Spostamento SL a PAREGGIO...")
            vecchio_sl_id = pos.get('sl_id')
            cancellazione_confermata = False
            
            if vecchio_sl_id:
                if self.performer.cancella_ordine_specifico(vecchio_sl_id):
                    cancellazione_confermata = True
                    time.sleep(1.5)
            else:
                self.performer.pulizia_totale_ordini(ticker)
                time.sleep(1.5)
                cancellazione_confermata = True

            if cancellazione_confermata:
                risultato = self.performer.gestisci_ordine_protezione(
                    asset=ticker, 
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
                    
                    self._salva_posizione(symbol_kraken)
                    if self.alerts: self.alerts.invia_alert(f"🛡️ *SAFE MODE {ticker}*\n50% raggiunto: SL a Pareggio.")

        elif progresso_percentuale >= 80 and fase_attuale < 2:
            self.logger.warning(f"🚀 {ticker} all'80%! Transizione a Phase Two (Scaling Out 25%)...")
            
            _is_long_f2 = direzione in ("BUY", "LONG")
            nuovo_sl_trail = p_entrata + (distanza_totale * 0.5) if _is_long_f2 else p_entrata - (distanza_totale * 0.5)
            nuovo_sl_trail = float(self.performer.qprice(symbol_kraken, nuovo_sl_trail))

            success = self._chiudi_parzialmente(
                asset=ticker,
                prezzo_attuale=prezzo_attuale,
                dati_mercato=dati_mercato,
                percentuale=0.25,
                motivo="SCALING_OUT_80_PERCENT",
                rimuovi_tp=True,
                nuovo_sl_override=nuovo_sl_trail
            )

            if success:
                self._salva_posizione(symbol_kraken)
            if success and self.alerts:
                self.alerts.invia_alert(f"🚀 *DYNAMIC MODE {ticker}*\n80% raggiunto: Incassato 25% profitto, TP rimosso, SL al 50% profitto.")
    
    def rimuovi_tp_fase_due(self, asset, motivo):
        try:
            ticker = asset_list.get_ticker(asset)
            pos = self.posizioni_aperte.get(ticker)
            if not pos: return False

            tp_id = pos.get('tp_id')
            if not tp_id:
                self.logger.debug(f"ℹ️ {ticker}: TP non presente nel JSON. Possibile Phase Two già attiva o TP eseguito.")
                return False

            self.logger.info(f"⚡ [CHIMERA] Tentativo rimozione TP ({tp_id}) per {ticker} - Motivo: {motivo}")

            success = self.performer.cancella_ordine_specifico(tp_id)

            if success:
                pos['fase'] = 2 
                pos['tp_id'] = None
                pos['tp'] = 0  
                
                self._salva_posizione(symbol_kraken)
                
                if self.alerts:
                    msg = (f"🚀 *CHIMERA PHASE TWO* su {ticker}\n\n"
                           f"✅ *Take Profit rimosso*\n"
                           f"📈 Il trade ora è in 'Free Run'.\n"
                           f"🏃 *Motivo:* {motivo}")
                    self.alerts.invia_alert(msg)
                
                if self.feedback_engine:
                    self.feedback_engine.registra_evento(ticker, "PHASE_TWO_ACTIVATED", {"motivo": motivo})
                
                return True
            else:
                self.logger.error(f"❌ Fallita cancellazione ordine TP {tp_id} su Kraken.")
                return False

        except Exception as e:
            _err.capture(e, "rimuovi_tp_fase_due", {"module": "TradeManager"})
            self.logger.error(f"❌ Errore in rimuovi_tp_fase_due: {e}")
            return False
    
    def registra_conclusione_trade(self, asset, esito, pnl_finale, dati_mercato=None):
        ticker = asset_list.get_ticker(asset)
        if ticker in self.posizioni_aperte:
            pos = self.posizioni_aperte.pop(ticker)
            pos.update({'data_chiusura': datetime.now().isoformat(), 'esito': esito, 'pnl_finale': pnl_finale})
            
            # Calcolo PnL USD se non presente (fallback)
            pnl_usd = pos.get('pnl_netto_usd', 0)
            
            if self.feedback_engine:
                self.feedback_engine.registra_feedback(
                    asset=ticker, 
                    score=pos.get('voto_ia', 0), 
                    outcome=esito, 
                    motivi=f"PNL: {pnl_finale}%. Razionale: {pos.get('razionale', '')}",
                    stile_operativo=pos.get('tipo_op', 'SWING'),
                    apprendimento_critico=pos.get('apprendimento_critico', ''),
                    dati_mercato=dati_mercato
                )
            self.storico_trades.append(pos)
            self._salva_storico()
            self.salva_posizioni()
            
            # Aggiorna metriche globali (Peak, Drawdown)
            self._aggiorna_metriche_performance(pnl_usd)
            
            self.logger.info(f"🏁 TRADE CONCLUSO: {ticker} | PNL: {pnl_finale}%")
            
            # Genera e invia statistiche aggiornate
            stats = self.genera_report_completo()
            if self.alerts:
                self.alerts.invia_stats_complete(stats)

    def _aggiorna_metriche_performance(self, pnl_netto_usd):
        """Aggiorna Peak, Drawdown e PnL Totale basandosi sul saldo reale."""
        try:
            current_balance = 0.0
            if self.engine and hasattr(self.engine, 'get_total_balance'):
                current_balance = self.engine.get_total_balance()
            
            if current_balance <= 0:
                return

            # Carica stats attuali
            stats = self.stats_globali
            
            # 1. Aggiorna PnL Realizzato Totale
            stats['pnl_realizzato_totale'] = round(stats.get('pnl_realizzato_totale', 0) + pnl_netto_usd, 2)
            
            # 2. Aggiorna Equity Peak
            old_peak = stats.get('equity_peak', 0)
            if current_balance > old_peak:
                stats['equity_peak'] = round(current_balance, 2)
                self.logger.info(f"🚀 NUOVO EQUITY PEAK: {stats['equity_peak']}$")
            
            # 3. Calcola Max Drawdown (rispetto al picco massimo mai raggiunto)
            peak = stats.get('equity_peak', current_balance)
            if peak > 0:
                current_dd = ((peak - current_balance) / peak) * 100
                if current_dd > stats.get('max_drawdown', 0):
                    stats['max_drawdown'] = round(current_dd, 2)
                    self.logger.warning(f"⚠️ NUOVO MAX DRAWDOWN: {stats['max_drawdown']}%")
            
            # Salva e aggiorna in memoria
            self.stats_globali = stats
            self._salva_stats_globali()
            
        except Exception as e:
            _err.capture(e, "_aggiorna_metriche_performance", {"module": "TradeManager"})
            self.logger.error(f"❌ Errore aggiornamento metriche performance: {e}")

    def aggiorna_posizione(self, asset, dati):
        ticker = asset_list.get_ticker(asset)
        if ticker in self.posizioni_aperte:
            self.posizioni_aperte[ticker].update(dati)
            self._salva_posizione(ticker)

    def _carica_storico(self):
        from core.database_manager import db_manager
        try:
            return db_manager.get_storico()
        except Exception as e:
            _err.capture(e, "_carica_storico", {"module": "TradeManager"})
            self.logger.error(f"⚠️ Errore caricamento storico da DB: {e}")
            return []

    def _salva_storico(self):
        from core.database_manager import db_manager
        try:
            db_manager.save_storico(self.storico_trades)
        except Exception as e:
            _err.capture(e, "_salva_storico", {"module": "TradeManager"})
            self.logger.error(f"⚠️ Errore salvataggio storico su DB: {e}")

    def _carica_stats_globali(self):
        from core.database_manager import db_manager
        try:
            return db_manager.get_stats_globali()
        except Exception as e:
            _err.capture(e, "_carica_stats_globali", {"module": "TradeManager"})
            self.logger.error(f"⚠️ Errore caricamento stats_globali da DB: {e}")
            return {"max_drawdown": 0.0, "pnl_realizzato_totale": 0.0, "equity_peak": 0.0}

    def _salva_stats_globali(self):
        from core.database_manager import db_manager
        try:
            db_manager.save_stats_globali(self.stats_globali)
        except Exception as e:
            _err.capture(e, "_salva_stats_globali", {"module": "TradeManager"})
            self.logger.error(f"⚠️ Errore salvataggio stats_globali su DB: {e}")
        
    # ══════════════════════════════════════════════════════════════════════════
    #  CHIMERA v4 — TIME-STOP ISTITUZIONALE (3 livelli)
    #  Portato da trade_manager_patch.py — non modificare la logica
    # ══════════════════════════════════════════════════════════════════════════

    def gestisci_trailing_stop_intelligente(self, asset, dati_mercato):
        """
        Sposta lo SL in base alla forza del trend.
        Se Hurst > 0.6 e VPIN è buono, seguiamo il prezzo più da vicino.
        """
        ticker = asset_list.get_ticker(asset)
        pos = self.posizioni_aperte.get(ticker)
        if not pos or not dati_mercato:
            return False

        prezzo_attuale = float(dati_mercato.get("close", 0))
        hurst = float(dati_mercato.get("hurst_exponent", 0.5))
        vpin = float(dati_mercato.get("vpin", 0.3))
        atr = float(dati_mercato.get("atr", prezzo_attuale * 0.01))
        
        direzione = pos.get("direzione", "LONG").upper()
        sl_attuale = float(pos.get("sl", 0))
        p_entrata = float(pos.get("p_entrata", 0))
        
        if prezzo_attuale <= 0 or sl_attuale <= 0:
            return False

        # Calcoliamo la distanza ideale dello SL
        # Se il trend è forte (Hurst alto), stringiamo lo stop per proteggere il profitto
        moltiplicatore = 1.5 if hurst > 0.6 else 2.5
        distanza = atr * moltiplicatore
        
        nuovo_sl = 0
        if direzione in ["BUY", "LONG"]:
            # Solo se siamo in profitto e il nuovo SL è più alto del precedente
            if prezzo_attuale > p_entrata:
                proposta_sl = prezzo_attuale - distanza
                if proposta_sl > sl_attuale:
                    nuovo_sl = proposta_sl
        else:
            # Solo se siamo in profitto e il nuovo SL è più basso del precedente
            if prezzo_attuale < p_entrata:
                proposta_sl = prezzo_attuale + distanza
                if proposta_sl < sl_attuale and sl_attuale > 0:
                    nuovo_sl = proposta_sl

        if nuovo_sl > 0:
            nuovo_sl_fmt = float(self.performer.qprice(ticker, nuovo_sl))
            if abs(nuovo_sl_fmt - sl_attuale) / sl_attuale > 0.002: # Almeno 0.2% di differenza per evitare churning
                self.logger.info(f"📈 TRAILING STOP [{ticker}]: Sposto SL da {sl_attuale} a {nuovo_sl_fmt} (Hurst: {hurst:.2f})")
                
                # Cancelliamo il vecchio SL e mettiamo il nuovo
                sl_id = pos.get("sl_id")
                if sl_id:
                    self.performer.cancella_ordine_specifico(sl_id)
                
                res = self.performer.gestisci_ordine_protezione(
                    asset=ticker, tipo_protezione="stop-loss",
                    prezzo=nuovo_sl_fmt, direzione_aperta=direzione,
                    size_fallback=pos.get("size"), leverage=pos.get("leverage")
                )
                
                if res and res.get("success"):
                    pos["sl"] = nuovo_sl_fmt
                    pos["sl_id"] = res.get("id")
                    self._salva_posizione(asset)
                    return True
        
        return False

    def gestisci_time_stop_istituzionale(self, asset, dati_mercato, engine=None):
        """
        Time Decay del vantaggio statistico. 3 livelli:
          L1 — Stagnazione: SL a Break-Even + TP al POC (Mean Reversion)
          L2 — Velocity morta + VPIN tossico: chiusura frazionata 50%
          L3 — Hurst < 0.40 in perdita: chiusura totale immediata
        """
        import time as _time
        ticker = asset_list.get_ticker(asset)
        pos = self.posizioni_aperte.get(ticker)
        if not pos:
            return "SKIP"

        try:
            data_apertura = datetime.fromisoformat(pos["data_apertura"].replace("Z", ""))
            ore_aperto = (datetime.now() - data_apertura).total_seconds() / 3600
        except Exception:
            ore_aperto = 0.0

        tipo_op   = str(pos.get("tipo_op", pos.get("tipo_operazione", "Swing"))).lower()
        direzione = pos.get("direzione", "LONG").upper()
        p_entrata = float(pos.get("p_entrata", 0))
        size      = float(pos.get("size", 0))
        leverage_raw = pos.get("leverage")
        leverage = int(leverage_raw) if leverage_raw is not None else 1
        fase      = pos.get("fase", 0)

        if p_entrata <= 0 or size <= 0:
            return "SKIP"

        if "scalp" in tipo_op or "momentum" in tipo_op or "sniper" in tipo_op or "breakout" in tipo_op:
            soglia_be   = 2.0
            soglia_exit = 3.0
        elif "multiday" in tipo_op:
            soglia_be   = 72.0
            soglia_exit = 168.0
        else:
            soglia_be   = 24.0
            soglia_exit = 36.0

        if not dati_mercato:
            return "HOLD"

        prezzo_attuale = float(dati_mercato.get("close", p_entrata))
        price_velocity = float(dati_mercato.get("price_velocity", 0.0))
        vpin           = float(dati_mercato.get("vpin", 0.3))
        hurst          = float(dati_mercato.get("hurst_exponent", 0.5))
        poc            = float(dati_mercato.get("poc", 0.0))
        regime         = str(dati_mercato.get("market_regime", "UNKNOWN"))
        atr            = float(dati_mercato.get("atr", prezzo_attuale * 0.015))

        if p_entrata == 0:
            pnl_perc = 0.0
        elif direzione in ["BUY", "LONG"]:
            pnl_perc = (prezzo_attuale - p_entrata) / p_entrata * 100
        else:
            pnl_perc = (p_entrata - prezzo_attuale) / p_entrata * 100

        velocity_morta = abs(price_velocity) < 0.00008
        vpin_tossico   = vpin > 0.72
        hurst_crollato = hurst < 0.40
        in_profitto    = pnl_perc > 0.2

        self.logger.debug(
            f"⏱️ TIME-STOP [{ticker}] | {tipo_op} | {ore_aperto:.1f}h "
            f"(BE={soglia_be}h/exit={soglia_exit}h) | "
            f"PnL: {pnl_perc:.2f}% | Vel: {price_velocity:.6f} | VPIN: {vpin:.3f} | Hurst: {hurst:.3f}"
        )

        # L3 — Hurst crolla + in perdita → chiusura totale
        if hurst_crollato and not in_profitto and ore_aperto > soglia_be * 0.5:
            self.logger.warning(f"🔴 TIME-STOP L3 [{ticker}]: Hurst {hurst:.3f} < 0.40, PnL {pnl_perc:.2f}%. Chiusura totale.")
            if self._esegui_chiusura_totale(ticker, prezzo_attuale, "TIME_STOP_HURST_COLLAPSE"):
                if self.alerts:
                    self.alerts.invia_alert(
                        f"🔴 *TIME-STOP L3 — CHIUSURA TOTALE*\n"
                        f"Asset: {ticker} | PnL: {pnl_perc:.2f}%\n"
                        f"Motivo: Hurst {hurst:.3f} (trend morto)"
                    )
                return "CHIUSO_TOTALE"

        # L2 — Velocity morta + VPIN tossico → riduzione 50%
        if ore_aperto > soglia_be and velocity_morta and vpin_tossico and fase < 2:
            self.logger.warning(f"🟠 TIME-STOP L2 [{ticker}]: Velocity piatta + VPIN {vpin:.3f}. Riduzione 50%.")
            if self._chiudi_parzialmente(ticker, prezzo_attuale, dati_mercato):
                if self.alerts:
                    self.alerts.invia_alert(
                        f"🟠 *TIME-STOP L2 — LIQUIDAZIONE 50%*\n"
                        f"Asset: {ticker} | PnL: {pnl_perc:.2f}%\n"
                        f"Motivo: Velocity morta + VPIN tossico ({vpin:.3f})"
                    )
                return "RIDOTTO_50"

        # L1 — Break-Even: tempo scaduto, velocity piatta, in profitto
        if ore_aperto > soglia_be and velocity_morta and in_profitto and fase < 1:
            self.logger.debug(f"🟡 TIME-STOP L1 [{ticker}]: {ore_aperto:.1f}h, velocity piatta. SL → Break-Even.")
            commissioni = p_entrata * 0.001
            nuovo_sl = (p_entrata + commissioni) if direzione in ["BUY", "LONG"] else (p_entrata - commissioni)
            nuovo_sl_fmt = float(self.performer.qprice(ticker, nuovo_sl))

            nuovo_tp = None
            if regime == "MEAN_REVERSION" and poc > 0 and prezzo_attuale > 0 and abs(poc - prezzo_attuale) / prezzo_attuale < 0.05:
                if direzione in ["BUY", "LONG"] and poc > prezzo_attuale:
                    nuovo_tp = poc
                elif direzione in ["SELL", "SHORT"] and poc < prezzo_attuale:
                    nuovo_tp = poc

            vecchio_sl_id = pos.get("sl_id")
            cancellato = False
            if vecchio_sl_id:
                cancellato = self.performer.cancella_ordine_specifico(vecchio_sl_id)
                if cancellato: _time.sleep(1.0)
            else:
                cancellato = True

            if cancellato:
                res_sl = self.performer.gestisci_ordine_protezione(
                    asset=ticker, tipo_protezione="stop-loss",
                    prezzo=nuovo_sl_fmt, direzione_aperta=direzione,
                    size_fallback=size, leverage=leverage
                )
                if res_sl and res_sl.get("success"):
                    pos["sl"]    = nuovo_sl_fmt
                    pos["sl_id"] = res_sl.get("id")
                    pos["fase"]  = 1
                    pos["nota_time_stop"] = f"BE a {ore_aperto:.1f}h"

                    if nuovo_tp:
                        vecchio_tp_id = pos.get("tp_id")
                        if vecchio_tp_id:
                            self.performer.cancella_ordine_specifico(vecchio_tp_id)
                            _time.sleep(0.8)
                        res_tp = self.performer.gestisci_ordine_protezione(
                            asset=ticker, tipo_protezione="take-profit",
                            prezzo=float(self.performer.qprice(ticker, nuovo_tp)),
                            direzione_aperta=direzione, size_fallback=size, leverage=leverage
                        )
                        if res_tp and res_tp.get("success"):
                            pos["tp"]    = nuovo_tp
                            pos["tp_id"] = res_tp.get("id")

                    self._salva_posizione(ticker)
                    if self.feedback_engine:
                        self.feedback_engine.registra_evento(ticker, "TIME_STOP_BREAKEVEN", {
                            "ore_aperto": ore_aperto, "tipo_op": tipo_op, "pnl_al_momento": pnl_perc
                        })
                    if self.alerts:
                        self.alerts.invia_alert(
                            f"🟡 *TIME-STOP L1 — BREAK EVEN*\n"
                            f"Asset: {ticker} ({tipo_op}) | {ore_aperto:.1f}h\n"
                            f"SL → pareggio ({nuovo_sl_fmt})\n"
                            f"{'TP → POC ' + str(round(nuovo_tp, 2)) if nuovo_tp else 'TP invariato'}"
                        )
                    return "BE_SPOSTATO"

        # Uscita forzata: oltre soglia_exit + stagnante + in perdita
        if ore_aperto > soglia_exit and velocity_morta and not in_profitto:
            self.logger.warning(f"🔴 TIME-STOP FORCE EXIT [{ticker}]: {ore_aperto:.1f}h, alpha svanito.")
            if self._esegui_chiusura_totale(ticker, prezzo_attuale, "TIME_STOP_FORCE_EXIT"):
                if self.alerts:
                    self.alerts.invia_alert(
                        f"🔴 *TIME-STOP — USCITA FORZATA*\n"
                        f"Asset: {ticker} | {ore_aperto:.1f}h | PnL: {pnl_perc:.2f}%\n"
                        f"Alpha degradato oltre soglia temporale"
                    )
                return "CHIUSO_TOTALE"

        return "HOLD"

    def _gestisci_chiusura_parziale_strategica(self, asset, dati_mercato):
        """
        Chiude il 50% se rileva una pressione eccessiva contro la posizione.
        """
        ticker = asset_list.get_ticker(asset)
        pos = self.posizioni_aperte.get(ticker)
        if not pos or pos.get('fase', 0) >= 2 or pos.get('chiusura_strategica_effettuata'): 
            return False

        prezzo_attuale = float(dati_mercato.get("close", 0))
        direzione = pos.get("direzione", "LONG").upper()
        p_entrata = float(pos.get("p_entrata", 0))
        
        # Calcolo PnL attuale
        if direzione in ["BUY", "LONG"]:
            pnl_perc = (prezzo_attuale - p_entrata) / p_entrata * 100 if p_entrata > 0 else 0
            pressione_muro = float(dati_mercato.get("pressione_muro_resistenza", 0))
        else:
            pnl_perc = (p_entrata - prezzo_attuale) / p_entrata * 100 if p_entrata > 0 else 0
            pressione_muro = float(dati_mercato.get("pressione_muro_supporto", 0))

        # Se siamo in profitto (>1%) e la pressione sul muro opposto è alta (>80%)
        if pnl_perc > 1.0 and pressione_muro > 0.8:
            self.logger.warning(f"⚠️ CHIUSURA PARZIALE STRATEGICA [{asset}]: Pressione muro {pressione_muro*100:.1f}% rilevata. PnL: {pnl_perc:.2f}%.")
            if self._chiudi_parzialmente(asset, prezzo_attuale, dati_mercato, motivo="STRATEGIC_WALL_PRESSURE"):
                pos['chiusura_strategica_effettuata'] = True
                self._salva_posizione(asset)
                if self.alerts:
                    self.alerts.invia_alert(
                        f"⚠️ *CHIUSURA PARZIALE STRATEGICA*\n"
                        f"Asset: {asset} | PnL: {pnl_perc:.2f}%\n"
                        f"Motivo: Pressione muro eccessiva ({pressione_muro*100:.1f}%)"
                    )
                return True
        return False

    def _chiudi_parzialmente(self, asset, prezzo_attuale, dati_mercato, percentuale=0.5, motivo="TIME_STOP_PARTIAL", rimuovi_tp=False, nuovo_sl_override=None):
        """Chiude una percentuale della posizione (default 50%). Ricrea SL/TP sulla size residua."""
        import time as _time
        ticker = asset_list.get_ticker(asset)
        pos = self.posizioni_aperte.get(ticker)
        if not pos:
            return False

        direzione    = pos.get("direzione", "LONG").upper()
        size_totale  = float(pos.get("size", 0))
        leverage_raw = pos.get("leverage")
        leverage = int(leverage_raw) if leverage_raw is not None else 1
        sl_attuale   = float(pos.get("sl", 0))
        tp_attuale   = float(pos.get("tp", 0))
        atr          = float(dati_mercato.get("atr", prezzo_attuale * 0.015))

        size_da_chiudere = round(size_totale * percentuale, 8)
        size_residua     = round(size_totale - size_da_chiudere, 8)

        from core.asset_list import get_config
        conf     = get_config(asset)
        
        min_size = conf.get("min_size", 0.0001)
        try:
            self.performer.exchange.load_markets()
            market = self.performer.exchange.market(ticker)
            if market and 'limits' in market and 'amount' in market['limits'] and 'min' in market['limits']['amount']:
                min_size = market['limits']['amount']['min']
        except Exception as e:
            _err.capture(e, "_chiudi_parzialmente", {"module": "TradeManager"})
            self.logger.warning(f"⚠️ Impossibile recuperare min_size da Kraken per {ticker}: {e}. Uso fallback: {min_size}")

        if size_da_chiudere <= 0:
            self.logger.warning(f"⚠️ Size da chiudere ({size_da_chiudere}) <= 0. Ignoro chiusura parziale.")
            return False

        # FIX: Se la parte da chiudere è inferiore al minimo, chiudiamo tutto per sicurezza
        if size_da_chiudere < min_size:
            self.logger.warning(f"⚠️ Size da chiudere {size_da_chiudere} < min_size ({min_size}). Chiusura totale per sicurezza.")
            return self._esegui_chiusura_totale(ticker, prezzo_attuale, "SIZE_DA_CHIUDERE_TROPPO_PICCOLA")

        if size_residua < min_size:
            self.logger.warning(f"⚠️ Size residua {size_residua} < min_size. Chiusura totale.")
            return self._esegui_chiusura_totale(ticker, prezzo_attuale, "SIZE_TROPPO_PICCOLA_POST_SPLIT")

        try:
            # --- VERIFICA SIZE REALE PRIMA DI PROCEDERE ---
            # Evitiamo chiusure parziali "fantasma" se il bot è disallineato
            self.logger.info(f"🔍 Verifica size reale su Kraken per {ticker} prima di chiusura parziale...")
            posizioni_real = self.performer.get_open_positions_real()
            symbol_kraken = ticker
            norm_symbol = self.performer._normalize_ticker(symbol_kraken)
            
            # Matching robusto
            p_real = posizioni_real.get(symbol_kraken)
            if not p_real:
                for k, v in posizioni_real.items():
                    if self.performer._normalize_ticker(k) == norm_symbol:
                        p_real = v
                        symbol_kraken = k
                        break

            if not p_real:
                if leverage == 1:
                    try:
                        self.performer.exchange.load_markets()
                        market = self.performer.exchange.market(symbol)
                        base_asset = market['base']
                        
                        balance = self.performer.exchange.fetch_balance()
                        free_balance = balance.get(base_asset, {}).get('free', 0)
                        
                        if free_balance >= size_totale * 0.95:
                            self.logger.info(f"🛒 Posizione SPOT {ticker} trovata nel saldo ({free_balance} {base_asset}). Procedo con chiusura parziale.")
                            p_real = {'vol': size_totale, 'terms': 'Spot'}
                        else:
                            self.logger.warning(f"⚠️ Saldo SPOT insufficiente per {ticker} ({free_balance} < {size_totale}).")
                    except Exception as e_bal:
                        _err.capture(e_bal, "_chiudi_parzialmente", {"module": "TradeManager"})
                        self.logger.warning(f"⚠️ Errore verifica saldo SPOT per {ticker}: {e_bal}")

            if p_real:
                size_real = float(p_real['vol'])
                
                # Estraiamo la leva reale dall'ordine primario
                entry_id = pos.get('entry_id')
                if entry_id and not str(entry_id).startswith("virtual"):
                    try:
                        ordine_primario = self.performer.exchange.fetch_order(entry_id)
                        leverage_reale = ordine_primario.get('leverage')
                        if leverage_reale is not None:
                            leverage_reale = int(leverage_reale)
                            # Non sovrascrivere leva > 1 con leva = 1
                            if leverage_reale >= 2 and leverage_reale != leverage:
                                self.logger.warning(f"⚖️ Disallineamento leva per {ticker}: Memoria {leverage}x vs Real {leverage_reale}x. Sincronizzo.")
                                leverage = leverage_reale
                                pos['leverage'] = leverage_reale
                            elif leverage_reale <= 1 and leverage > 1:
                                self.logger.debug(f"ℹ️ [{ticker}] Kraken riporta leva=1 su ordine chiuso — tengo memoria {leverage}x")
                    except Exception as e_lev:
                        _err.capture(e_lev, "_chiudi_parzialmente", {"module": "TradeManager"})
                        self.logger.debug(f"⚠️ Impossibile recuperare leva da ordine {entry_id}: {e_lev}")
                
                if abs(size_real - size_totale) > 0.000001:
                    self.logger.warning(f"⚖️ Disallineamento size rilevato per {ticker}: Memoria {size_totale} vs Real {size_real}. Sincronizzo.")
                    size_totale = size_real
                    pos['size'] = size_real
                    # Ricalcoliamo le size con il dato reale
                    size_da_chiudere = round(size_totale * percentuale, 8)
                    size_residua     = round(size_totale - size_da_chiudere, 8)
                    
                    # Rivalidiamo i minimi dopo la sincronizzazione
                    if size_da_chiudere < min_size:
                        self.logger.warning(f"⚠️ Dopo sync, size da chiudere {size_da_chiudere} < min_size. Chiusura totale.")
                        return self._esegui_chiusura_totale(ticker, prezzo_attuale, "SIZE_DA_CHIUDERE_TROPPO_PICCOLA_POST_SYNC")
                    if size_residua < min_size:
                        self.logger.warning(f"⚠️ Dopo sync, size residua {size_residua} < min_size. Chiusura totale.")
                        return self._esegui_chiusura_totale(ticker, prezzo_attuale, "SIZE_TROPPO_PICCOLA_POST_SPLIT_POST_SYNC")
            else:
                self.logger.error(f"❌ Impossibile trovare posizione reale per {ticker} su Kraken. Abort chiusura parziale.")
                if ticker in self.posizioni_aperte:
                    self._chiudi_statisticamente(ticker)
                return False

            self.logger.info(f"🧹 Pulizia totale ordini per {ticker} prima della chiusura parziale...")
            self.performer.pulizia_totale_ordini(ticker)
            _time.sleep(1.5)

            side_chiusura = "sell" if direzione in ["BUY", "LONG"] else "buy"
            from core.asset_list import get_ticker
            symbol = get_ticker(asset)

            try:
                try:
                    vol_fmt = self.performer.exchange.amount_to_precision(symbol, size_da_chiudere)
                except Exception:
                    vol_fmt = str(size_da_chiudere)
                params_parz = {
                    'pair':      symbol,
                    'type':      side_chiusura,
                    'ordertype': 'market',
                    'volume':    vol_fmt,
                    'cl_ord_id': str(uuid.uuid4()),
                }
                if leverage > 1:
                    params_parz['leverage'] = str(int(leverage))
                res = self.performer.exchange.private_post_addorder(params_parz)
                if res and 'result' in res and 'txid' in res['result']:
                    ordine = {'id': res['result']['txid'][0]}
                else:
                    err = str(res.get('error', ''))
                    self.logger.error(f"❌ Errore chiusura parziale {ticker}: {err}")
                    return False
            except Exception as e_order:
                _err.capture(e_order, "_chiudi_parzialmente", {"module": "TradeManager"})
                self.logger.error(f"❌ Errore chiusura parziale {ticker}: {e_order}")
                return False

            if not ordine or not ordine.get("id"):
                self.logger.error(f"❌ Ordine parziale fallito per {ticker}")
                return False

            p_entrata = float(pos.get("p_entrata", prezzo_attuale))
            if p_entrata == 0 or p_entrata == 0.0:
                pnl_parziale_perc = 0.0
                pnl_parziale_usd = 0.0
            else:
                pnl_parziale_perc = ((prezzo_attuale - p_entrata) / p_entrata * 100
                                if direzione in ["BUY", "LONG"]
                                else (p_entrata - prezzo_attuale) / p_entrata * 100)
                
                # Calcolo PnL USD parziale
                if direzione in ["BUY", "LONG"]:
                    pnl_parziale_usd = size_da_chiudere * (prezzo_attuale - p_entrata)
                else:
                    pnl_parziale_usd = size_da_chiudere * (p_entrata - prezzo_attuale)

            # Stima commissioni per la parte chiusa
            fees_parziale = size_da_chiudere * prezzo_attuale * 0.0026 # 0.26% Taker fee stimata
            pnl_netto_usd = pnl_parziale_usd - fees_parziale
            pnl_netto_perc = (pnl_netto_usd / (p_entrata * size_da_chiudere / leverage)) * 100 if p_entrata > 0 else 0

            # Aggiornamento PnL Giornaliero (Netto)
            self.daily_pnl += pnl_netto_usd

            trade_parziale = dict(pos)
            trade_parziale.update({
                "data_chiusura": datetime.now().isoformat(),
                "p_uscita": prezzo_attuale,
                "pnl_finale": round(pnl_parziale_perc, 2),
                "pnl_netto_perc": round(pnl_netto_perc, 2),
                "pnl_usd": round(pnl_parziale_usd, 2),
                "pnl_netto_usd": round(pnl_netto_usd, 2),
                "fees": round(fees_parziale, 4),
                "esito": "WIN" if pnl_netto_usd > 0 else "LOSS",
                "size": size_da_chiudere,
                "motivo_chiusura": motivo,
                "tipo": "PARZIALE"
            })
            self.storico_trades.append(trade_parziale)
            self._salva_storico()
            self.logger.info(f"✅ Chiusura parziale {ticker}: PnL={pnl_parziale_perc:.2f}% ({pnl_netto_usd:.2f}$ NET) | {motivo} | PnL Odierno: {self.daily_pnl:.2f}$")
            _time.sleep(1.5)

            if nuovo_sl_override:
                nuovo_sl = nuovo_sl_override
            else:
                nuovo_sl = sl_attuale if sl_attuale > 0 else (
                    p_entrata - atr * 2 if direzione in ["BUY", "LONG"] else p_entrata + atr * 2
                )
                
            nuovo_sl_fmt = float(self.performer.qprice(ticker, nuovo_sl))
            res_sl = self.performer.gestisci_ordine_protezione(
                asset=ticker, tipo_protezione="stop-loss", prezzo=nuovo_sl_fmt,
                direzione_aperta=direzione, size_fallback=size_residua, leverage=leverage
            )

            nuovo_tp_id = None
            if tp_attuale > 0 and not rimuovi_tp:
                res_tp = self.performer.gestisci_ordine_protezione(
                    asset=ticker, tipo_protezione="take-profit",
                    prezzo=float(self.performer.qprice(ticker, tp_attuale)),
                    direzione_aperta=direzione, size_fallback=size_residua, leverage=leverage
                )
                if res_tp and res_tp.get("success"):
                    nuovo_tp_id = res_tp.get("id")

            pos["size"]  = size_residua
            pos["fase"]  = 2
            pos["sl"]    = nuovo_sl_fmt
            pos["sl_id"] = res_sl.get("id") if res_sl and res_sl.get("success") else None
            
            if rimuovi_tp:
                pos["tp"] = 0
                pos["tp_id"] = None
            else:
                pos["tp_id"] = nuovo_tp_id
                
            pos["nota_time_stop"] = f"RIDOTTA_{int(percentuale*100)}_PERCENT"
            self._salva_posizione(asset)

            if self.feedback_engine:
                self.feedback_engine.registra_evento(ticker, f"LIQUIDAZIONE_FRAZIONATA_{int(percentuale*100)}", {
                    "pnl_parziale": pnl_parziale_perc, "size_residua": size_residua
                })
            
            # Genera e invia statistiche aggiornate
            stats = self.genera_report_completo()
            if self.alerts:
                self.alerts.invia_stats_complete(stats)
                
            return True

        except Exception as e:
            _err.capture(e, "_chiudi_parzialmente", {"module": "TradeManager"})
            self.logger.error(f"❌ Errore chiusura parziale {ticker}: {e}")
            return False

    def _esegui_chiusura_totale(self, asset, prezzo_attuale, motivo="TIME_STOP", dati_mercato=None):
        """Chiude l'intera posizione a mercato dopo aver cancellato SL/TP."""
        import time as _time
        ticker = asset_list.get_ticker(asset)
        pos = self.posizioni_aperte.get(ticker)
        if not pos:
            return False

        direzione = pos.get("direzione", "LONG").upper()
        size      = float(pos.get("size", 0))
        if size <= 0:
            self.logger.warning(f"⚠️ Size <= 0 per {ticker}. Rimuovo posizione senza ordine.")
            self.posizioni_aperte.pop(ticker, None)
            return True
            
        leverage_raw = pos.get("leverage")
        leverage = int(leverage_raw) if leverage_raw is not None else 1

        try:
            # --- VERIFICA ESISTENZA REALE PRIMA DI CHIUDERE ---
            from core.asset_list import get_ticker
            symbol = get_ticker(asset)
            norm_symbol = self.performer._normalize_ticker(symbol)
            
            self.logger.info(f"🔍 Verifica esistenza reale per {asset} prima della chiusura totale...")
            try:
                posizioni_real = self.performer.get_open_positions_real()
            except Exception as e_pos:
                _err.capture(e_pos, "_esegui_chiusura_totale", {"module": "TradeManager"})
                self.logger.error(f"❌ Impossibile recuperare posizioni reali per {asset}: {e_pos}. Aborto chiusura per sicurezza.")
                return False
            
            # Matching robusto per evitare problemi POLUSD vs XPOLZUSD
            p_real = posizioni_real.get(symbol)
            if not p_real:
                for k, v in posizioni_real.items():
                    if self.performer._normalize_ticker(k) == norm_symbol:
                        p_real = v
                        symbol = k # Usiamo il ticker esatto di Kraken
                        self.logger.info(f"🎯 Match trovato tramite normalizzazione: {asset} -> {symbol}")
                        break
            
            if not p_real:
                # Se non la troviamo, logghiamo cosa abbiamo trovato per debug
                self.logger.warning(f"⚠️ Posizione {asset} ({symbol}) non trovata tra le {len(posizioni_real)} posizioni a margine.")
                if posizioni_real:
                    self.logger.debug(f"📋 Posizioni reali presenti: {list(posizioni_real.keys())}")
                
                if leverage == 1:
                    # Per le posizioni SPOT, verifichiamo il saldo
                    try:
                        self.performer.exchange.load_markets()
                        market = self.performer.exchange.market(symbol)
                        base_asset = market['base']
                        
                        balance = self.performer.exchange.fetch_balance()
                        free_balance = balance.get(base_asset, {}).get('free', 0)
                        
                        if free_balance >= size * 0.95: # Tolleranza 5% per fee
                            self.logger.info(f"🛒 Posizione SPOT {asset} trovata nel saldo ({free_balance} {base_asset}). Procedo con la chiusura.")
                            p_real = {'vol': size, 'terms': 'Spot'} # Pseudo-posizione per bypassare il blocco
                        else:
                            self.logger.warning(f"⚠️ Saldo SPOT insufficiente per {asset} ({free_balance} < {size}).")
                    except Exception as e_bal:
                        _err.capture(e_bal, "_esegui_chiusura_totale", {"module": "TradeManager"})
                        self.logger.warning(f"⚠️ Errore verifica saldo SPOT per {asset}: {e_bal}")

            if not p_real:
                self.logger.warning(f"⚠️ Posizione {asset} non trovata su Kraken. Verifico se è una posizione fantasma...")
                # Verifichiamo se l'ordine di entrata ha mai generato trade
                entry_id = pos.get('entry_id')
                if entry_id and not str(entry_id).startswith("virtual"):
                    try:
                        entry_trades = self.performer.exchange.fetch_my_trades(symbol=symbol, params={'ordertxid': entry_id})
                        if not entry_trades:
                            self.logger.error(f"👻 Rilevata POSIZIONE FANTASMA per {asset} (Entry {entry_id} mai eseguito). Rimozione dal diario.")
                            self.posizioni_aperte.pop(asset, None)
                            self._rimuovi_posizione(asset)
                            return True
                    except Exception as e_check:
                        _err.capture(e_check, "_esegui_chiusura_totale", {"module": "TradeManager"})
                        self.logger.debug(f"⚠️ Impossibile verificare entry_id {entry_id}: {e_check}")
                
                self.logger.info(f"ℹ️ Posizione {asset} probabilmente già chiusa esternamente. Procedo con sincronizzazione.")
                ordine = {"id": "already_closed_by_exchange"}
            else:
                # Estraiamo la leva e la size reale dalle posizioni aperte su Kraken (Fonte di Verità)
                try:
                    posizioni_real = self.performer.get_open_positions_real()
                    norm_asset = self.performer._normalize_ticker(asset)
                    
                    found_real = False
                    for p_kraken in posizioni_real.values():
                        if self.performer._normalize_ticker(p_kraken.get('pair', '')) == norm_asset:
                            cost_p = float(p_kraken.get('cost', 0))
                            margin_p = float(p_kraken.get('margin', 1))
                            vol_p = float(p_kraken.get('vol', 0))
                            
                            if margin_p > 0 and cost_p > 0:
                                leverage_reale = int(cost_p / margin_p)
                                # Clampa al valore valido Kraken più vicino per difetto
                                # (cost/margin può dare valori non standard es. 13x)
                                leverage_reale = asset_list.clampa_leva(asset, leverage_reale)
                                if leverage_reale != leverage:
                                    if leverage_reale <= 1 and leverage > 1:
                                        # Kraken riporta leva=1 su posizioni margin aperte —
                                        # è un problema di calcolo (cost/margin arrotondato).
                                        # Non fare downgrade a spot: useremmo leva dalla memoria.
                                        self.logger.info(
                                            f"ℹ️ [{asset}] Kraken riporta leva=1 ma memoria dice "
                                            f"{leverage}x — tengo memoria (posizione margin)"
                                        )
                                    else:
                                        self.logger.warning(
                                            f"⚖️ Disallineamento leva per {asset}: "
                                            f"Memoria {leverage}x vs Real {leverage_reale}x. Sincronizzo."
                                        )
                                        leverage = leverage_reale
                                        pos['leverage'] = leverage_reale
                            
                            if vol_p > 0 and abs(vol_p - size) > 0.00000001:
                                self.logger.warning(f"📏 Disallineamento size per {asset}: Memoria {size} vs Real {vol_p}. Sincronizzo.")
                                size = vol_p
                                pos['size'] = vol_p
                            
                            found_real = True
                            break
                    
                    if not found_real:
                        # Se non la troviamo nelle posizioni a margine, proviamo a vedere se è un ordine primario
                        entry_id = pos.get('entry_id')
                        if entry_id and not str(entry_id).startswith("virtual"):
                            try:
                                ordine_primario = self.performer.exchange.fetch_order(entry_id)
                                leverage_reale = ordine_primario.get('leverage')
                                if leverage_reale is not None:
                                    leverage_reale = int(leverage_reale)
                                    # Non sovrascrivere leva > 1 con leva = 1
                                    # Kraken non restituisce la leva nell'ordine SPOT/margin
                                    if leverage_reale >= 2 and leverage_reale != leverage:
                                        self.logger.warning(f"⚖️ Disallineamento leva (da ordine) per {asset}: Memoria {leverage}x vs Real {leverage_reale}x.")
                                        leverage = leverage_reale
                                        pos['leverage'] = leverage_reale
                                    elif leverage_reale <= 1 and leverage > 1:
                                        self.logger.info(f"ℹ️ [{asset}] Ordine riporta leva=1 ma memoria dice {leverage}x — tengo memoria (Kraken non espone leva su ordini chiusi)")
                            except Exception as e_lev:
                                _err.capture(e_lev, "_esegui_chiusura_totale", {"module": "TradeManager"})
                                self.logger.debug(f"⚠️ Impossibile recuperare leva da ordine {entry_id}: {e_lev}")
                except Exception as e_sincro:
                    _err.capture(e_sincro, "_esegui_chiusura_totale", {"module": "TradeManager"})
                    self.logger.debug(f"⚠️ Errore sincronizzazione pre-chiusura per {asset}: {e_sincro}")

                self.logger.info(f"🧹 Pulizia totale ordini per {asset} prima della chiusura totale...")
                self.performer.pulizia_totale_ordini(asset)
                _time.sleep(1.5)

                side_chiusura = "sell" if direzione in ["BUY", "LONG"] else "buy"

                # Chiusura via private_post_addorder — stesso metodo usato dal performer
                # per SL/TP. Funziona sempre su Kraken margin senza errori leverage/reduce_only.
                try:
                    try:
                        vol_fmt = self.performer.exchange.amount_to_precision(symbol, size)
                    except Exception:
                        vol_fmt = str(size)

                    params_chiusura = {
                        'pair':      symbol,
                        'type':      side_chiusura,
                        'ordertype': 'market',
                        'volume':    vol_fmt,
                        'cl_ord_id': str(uuid.uuid4()),
                        'close':     'true',  # chiude TUTTA la posizione su Kraken
                    }
                    if leverage > 1:
                        # Clampa finale: garantisce sempre un valore accettato da Kraken
                        # (es. leva=13 → 10 per BTC, leva=7 → 5 per SOL)
                        leverage_ok = asset_list.clampa_leva(asset, leverage)
                        if leverage_ok != leverage:
                            self.logger.warning(
                                f"⚠️ [{asset}] Leva {leverage}x non valida per Kraken — "
                                f"clampata a {leverage_ok}x prima della chiusura"
                            )
                            leverage = leverage_ok
                        params_chiusura['leverage'] = str(int(leverage))

                    self.logger.info(f"📤 CHIUSURA {asset} | side={side_chiusura} | vol={vol_fmt} | leva={leverage}")
                    res = self.performer.exchange.private_post_addorder(params_chiusura)

                    if res and 'result' in res and 'txid' in res['result']:
                        ordine = {'id': res['result']['txid'][0]}
                    else:
                        err = str(res.get('error', ''))
                        self.logger.error(f"❌ Errore chiusura totale {asset}: {err}")
                        return False
                except Exception as e_order:
                    _err.capture(e_order, "_esegui_chiusura_totale", {"module": "TradeManager"})
                    self.logger.error(f"❌ Errore chiusura totale {asset}: {e_order}")
                    return False

            if ordine and ordine.get("id"):
                exit_order_id = ordine.get("id")
                p_entrata = float(pos.get("p_entrata", prezzo_attuale))
                leverage_val = int(pos.get('leverage', 1) or 1)
                margine = (p_entrata * size / leverage_val) if (p_entrata > 0 and leverage_val > 0) else 0

                # ── PnL da Kraken (fonte di verità) ──────────────────────────
                # Kraken restituisce 'net' = PnL netto reale sul margine, già
                # comprensivo di leva e fee. È il valore corretto da usare sempre.
                pnl_netto_usd = None
                total_fees    = 0.0

                if exit_order_id != "already_closed_by_exchange":
                    # Attendiamo che Kraken registri il trade (di solito < 2s)
                    _time.sleep(2.5)
                    real_data = self.performer.get_trade_pnl_real(exit_order_id)
                    if real_data and real_data.get('pnl_netto') != 0:
                        pnl_netto_usd = real_data['pnl_netto']
                        total_fees    = real_data.get('fee', 0.0)
                        self.logger.info(
                            f"🎯 PnL REALE Kraken per {asset} "
                            f"(ordine {exit_order_id}): {pnl_netto_usd:+.4f}$"
                        )

                if pnl_netto_usd is not None:
                    # Net Kraken è già netto sul margine → pnl_usd lordo = net + fee
                    pnl_usd   = pnl_netto_usd + total_fees
                    # pnl_perc rispetto al margine impiegato (non alla posizione lorda)
                    pnl_perc  = (pnl_netto_usd / margine * 100) if margine > 0 else 0
                else:
                    # ── Fallback statistico (Kraken non ha ancora registrato) ──
                    # Calcola il PnL corretto applicando la leva sul margine
                    self.logger.warning(
                        f"⚠️ PnL Kraken non disponibile per {asset} "
                        f"(ordine {exit_order_id}) — uso fallback statistico con leva"
                    )
                    if p_entrata == 0:
                        pnl_perc = 0.0
                        pnl_usd  = 0.0
                    else:
                        # Variazione % del prezzo
                        if direzione in ["BUY", "LONG"]:
                            price_chg_perc = (prezzo_attuale - p_entrata) / p_entrata
                        else:
                            price_chg_perc = (p_entrata - prezzo_attuale) / p_entrata

                        # PnL lordo sulla posizione intera (size × Δprezzo)
                        pnl_usd  = size * abs(prezzo_attuale - p_entrata) * (
                            1 if price_chg_perc >= 0 else -1
                        )
                        # PnL % rispetto al margine impiegato
                        pnl_perc = (price_chg_perc * leverage_val * 100)

                    # Stima fee
                    total_fees    = p_entrata * size * 0.0026
                    pnl_netto_usd = pnl_usd - total_fees

                pnl_netto_perc = (pnl_netto_usd / margine * 100) if margine > 0 else pnl_perc

                # Aggiornamento PnL Giornaliero (Netto)
                self.daily_pnl += pnl_netto_usd

                pos.update({
                    "data_chiusura": datetime.now().isoformat(),
                    "p_uscita": prezzo_attuale,
                    "pnl_finale": round(pnl_perc, 2),
                    "pnl_netto_perc": round(pnl_netto_perc, 2),
                    "pnl_usd": round(pnl_usd, 2),
                    "pnl_netto_usd": round(pnl_netto_usd, 2),
                    "fees": round(total_fees, 4),
                    "esito": "WIN" if pnl_netto_usd > 0 else "LOSS",
                    "motivo_chiusura": motivo
                })
                self.storico_trades.append(pos)
                self.posizioni_aperte.pop(asset, None)
                self._salva_storico()
                self._rimuovi_posizione(asset)
                
                # Aggiorna metriche globali (Peak, Drawdown)
                self._aggiorna_metriche_performance(pnl_netto_usd)
                
                self.logger.info(f"✅ CHIUSURA {asset} COMPLETATA:")
                self.logger.info(f"   💰 PnL Lordo: {pnl_usd:+.2f}$ ({pnl_perc:+.2f}%)")
                self.logger.info(f"   💸 Commissioni: {total_fees:.4f}$")
                self.logger.info(f"   🏆 PnL Netto: {pnl_netto_usd:+.2f}$ ({pnl_netto_perc:+.2f}%)")
                self.logger.info(f"   📊 PnL Odierno (Netto): {self.daily_pnl:.2f}$")

                # Cooldown differenziato per tipo di chiusura:
                # WIN reale:          nessun cooldown (il setup era buono)
                # Breakeven SL:       10 min (abbiamo raggiunto il 50%, asset può riformarsi)
                # LOSS reale <-1%:    30 min  
                # LOSS grave >-2%:    60 min
                # Time-stop:          15 min (alpha svanito, aspetta setup nuovo)
                _is_win        = pnl_netto_usd > 0
                _is_breakeven  = _is_win and pnl_netto_perc < 0.15   # WIN ma quasi zero = breakeven SL
                _is_timestop   = 'TIME_STOP' in str(motivo).upper()
                _is_loss_grave = pnl_netto_perc < -2.0

                if _is_win and not _is_breakeven:
                    _cooldown_sec = 0          # WIN reale → nessun cooldown
                    _cd_label = "nessuno (WIN)"
                elif _is_breakeven:
                    _cooldown_sec = 600        # Breakeven SL → 10 min
                    _cd_label = "10 min (breakeven SL)"
                elif _is_timestop:
                    _cooldown_sec = 900        # Time-stop → 15 min
                    _cd_label = "15 min (time-stop)"
                elif _is_loss_grave:
                    _cooldown_sec = 3600       # Loss grave → 60 min
                    _cd_label = "60 min (loss grave)"
                else:
                    _cooldown_sec = 1800       # Loss normale → 30 min
                    _cd_label = "30 min (loss)"

                if _cooldown_sec > 0:
                    self.cooldown_assets[asset] = time.time() + _cooldown_sec
                    self.logger.info(
                        f"⏳ Cooldown {asset}: {_cd_label} "
                        f"(fino a {datetime.fromtimestamp(self.cooldown_assets[asset]).strftime('%H:%M:%S')})"
                    )
                else:
                    # Rimuovi eventuale cooldown precedente
                    self.cooldown_assets.pop(asset, None)
                    self.logger.info(f"✅ [{asset}] Nessun cooldown — WIN reale, rientro libero")

                if self.feedback_engine:
                    self.feedback_engine.registra_feedback(
                        asset=asset, score=pos.get("voto_ia", 0),
                        outcome="WIN" if pnl_perc > 0 else "LOSS", motivi=f"{motivo}. Razionale: {pos.get('razionale', '')}",
                        stile_operativo=pos.get('tipo_op', 'SWING'),
                        apprendimento_critico=pos.get('apprendimento_critico', ''),
                        dati_mercato=dati_mercato
                    )
                
                # Genera e invia statistiche aggiornate
                stats = self.genera_report_completo()
                if self.alerts:
                    self.alerts.invia_stats_complete(stats)
                    
                return True

            self.logger.error(f"❌ Ordine chiusura totale {asset} non confermato")
            return False

        except Exception as e:
            _err.capture(e, "_esegui_chiusura_totale", {"module": "TradeManager"})
            self.logger.error(f"❌ Errore chiusura totale {asset}: {e}")
            return False

    def get_asset_win_rate(self, asset, window=20):
        """Restituisce il Win Rate storico per un asset specifico (ultimi N trade)."""
        ticker = asset_list.get_ticker(asset)
        asset_trades = [t for t in self.storico_trades if t.get('asset') == ticker]
        if not asset_trades:
            return 0.5 # Default 50% se non ci sono trade
        
        recent_trades = asset_trades[-window:]
        wins = len([t for t in recent_trades if t.get('esito') == "WIN"])
        return wins / len(recent_trades)

    def genera_report_completo(self):
        """
        Genera statistiche LEGGENDO ESCLUSIVAMENTE DAL KRAKEN LEDGER.
        Il diario interno (storico_trades) è inattendibile: contiene errori di
        attribuzione PnL (es. PnL SOL assegnato a XDG dal reconciler), trade
        duplicati, trade persi, p_entrata sbagliato per slippage non gestito.
        Kraken è l'unica fonte di verità.
        
        Calcolo:
        - PnL = SUM(margin amount) - SUM(margin fee) - SUM(rollover fee) sui movimenti ZUSD
        - WR/Trades = conteggio kraken_trades con net != 0 (= round-trip chiusi)
        - Periodo "oggi" = da mezzanotte ora locale
        - Periodo "totale" = tutto il ledger disponibile (tipicamente ultimi 90gg)
        """
        ora_now = datetime.now()
        mezzanotte = ora_now.replace(hour=0, minute=0, second=0, microsecond=0)
        mezzanotte_ts = mezzanotte.timestamp()
        
        # Sincronizzazione preventiva con Kraken Ledger (assicura che kraken_ledger 
        # e kraken_trades siano aggiornati con gli ultimi movimenti)
        self.sincronizza_pnl_con_kraken()
        
        # ── LETTURA DIRETTA DA KRAKEN LEDGER (fonte di verità unica) ─────────
        from core.database_manager import db_manager as _dbm
        
        margin_amt_oggi = 0.0
        margin_fee_oggi = 0.0
        rollover_fee_oggi = 0.0
        margin_amt_tot = 0.0
        margin_fee_tot = 0.0
        rollover_fee_tot = 0.0
        n_voci_oggi = 0
        n_voci_tot = 0
        
        try:
            with _dbm._conn_lock:
                _cur = _dbm._conn.cursor()
                _cur.execute("SELECT type, asset, amount, fee, time FROM kraken_ledger")
                for typ, asset_l, amt_l, fee_l, t_l in _cur.fetchall():
                    if asset_l != 'ZUSD':
                        continue
                    amt_f = float(amt_l or 0)
                    fee_f = float(fee_l or 0)
                    t_f = float(t_l or 0)
                    is_oggi = t_f >= mezzanotte_ts
                    if typ == 'margin':
                        margin_amt_tot += amt_f
                        margin_fee_tot += fee_f
                        n_voci_tot += 1
                        if is_oggi:
                            margin_amt_oggi += amt_f
                            margin_fee_oggi += fee_f
                            n_voci_oggi += 1
                    elif typ == 'rollover':
                        rollover_fee_tot += fee_f
                        if is_oggi:
                            rollover_fee_oggi += fee_f
        except Exception as _e:
            _err.capture(_e, "genera_report_completo", {"module": "TradeManager"})
            self.logger.error(f"❌ Errore lettura kraken_ledger: {_e}")
        
        pnl_usd_oggi = margin_amt_oggi - margin_fee_oggi - rollover_fee_oggi
        pnl_usd_totale = margin_amt_tot - margin_fee_tot - rollover_fee_tot
        
        # ── WR e numero trade reali da kraken_trades (round-trip con net != 0) ──
        n_trade_oggi = 0
        n_win_oggi = 0
        n_trade_tot = 0
        n_win_tot = 0
        wins = []
        losses = []
        
        try:
            with _dbm._conn_lock:
                _cur = _dbm._conn.cursor()
                _cur.execute("SELECT time, net FROM kraken_trades WHERE net IS NOT NULL")
                for t_t, net_t in _cur.fetchall():
                    try:
                        net_f = float(net_t or 0)
                    except Exception:
                        continue
                    if net_f == 0:
                        continue  # apertura, non chiusura
                    t_f = float(t_t or 0)
                    is_oggi = t_f >= mezzanotte_ts
                    n_trade_tot += 1
                    if net_f > 0:
                        n_win_tot += 1
                        wins.append(net_f)
                    else:
                        losses.append(net_f)
                    if is_oggi:
                        n_trade_oggi += 1
                        if net_f > 0:
                            n_win_oggi += 1
        except Exception as _e:
            _err.capture(_e, "genera_report_completo", {"module": "TradeManager"})
            self.logger.error(f"❌ Errore lettura kraken_trades: {_e}")
        
        wr_oggi = round(n_win_oggi / n_trade_oggi * 100, 2) if n_trade_oggi > 0 else 0.0
        wr_totale = round(n_win_tot / n_trade_tot * 100, 2) if n_trade_tot > 0 else 0.0
        
        # Profit Factor, Avg Win, Avg Loss (da Kraken net)
        gross_profit = sum(wins)
        gross_loss = abs(sum(losses))
        profit_factor = round(gross_profit / gross_loss, 2) if gross_loss > 0 else (round(gross_profit, 2) if gross_profit > 0 else 0.0)
        avg_win = round(sum(wins) / len(wins), 2) if wins else 0.0
        avg_loss = round(sum(losses) / len(losses), 2) if losses else 0.0

        # Recupero bilancio attuale (EQUITY) per calcolo % reale
        current_balance = 0.0
        try:
            current_balance = self.get_balance_margin(currency="USD", mode="equity")
        except Exception as e:
            _err.capture(e, "genera_report_completo", {"module": "TradeManager"})
            self.logger.debug(f"Errore recupero equity in report: {e}")

        if current_balance <= 0:
            if self.engine and hasattr(self.engine, 'get_total_balance'):
                current_balance = self.engine.get_total_balance()

        daily_pct_real = 0.0
        if current_balance > 0:
            base_daily = current_balance - pnl_usd_oggi
            if base_daily <= 0:
                base_daily = current_balance
            daily_pct_real = (pnl_usd_oggi / base_daily) * 100

        total_pct_real = 0.0
        if current_balance > 0:
            base_total = current_balance - pnl_usd_totale
            if base_total <= 0:
                base_total = current_balance
            total_pct_real = (pnl_usd_totale / base_total) * 100
            
        # --- LOG DI DEBUG: confronta con quello che pensava il bot ---
        self.logger.info(
            f"📊 STATS DA KRAKEN LEDGER | "
            f"Bal:{current_balance:.2f}$ | Daily PnL:{pnl_usd_oggi:+.2f}$ ({daily_pct_real:+.2f}%) | "
            f"Total PnL:{pnl_usd_totale:+.2f}$ ({total_pct_real:+.2f}%) | "
            f"Trades oggi:{n_trade_oggi} (WR {wr_oggi}%) | Trades tot:{n_trade_tot} (WR {wr_totale}%)"
        )
        self.logger.info(
            f"📊 BREAKDOWN OGGI | margin amt:{margin_amt_oggi:+.4f}$ | fees:{margin_fee_oggi:.4f}$ | rollover:{rollover_fee_oggi:.4f}$ | voci:{n_voci_oggi}"
        )

        report = {
            "daily": {
                "pnl": round(pnl_usd_oggi, 2),
                "pnl_pct_real": round(daily_pct_real, 2),
                "trades": n_trade_oggi,
                "win_rate": wr_oggi
            },
            "total": {
                "pnl": round(pnl_usd_totale, 2),
                "pnl_pct_real": round(total_pct_real, 2),
                "trades": n_trade_tot,
                "win_rate": wr_totale,
                "profit_factor": profit_factor,
                "avg_win": avg_win,
                "avg_loss": avg_loss,
                "max_drawdown": self.stats_globali.get('max_drawdown', 0.0),
                "equity_peak": self.stats_globali.get('equity_peak', 0.0)
            },
            "posizioni_aperte": list(self.posizioni_aperte.keys()),
            "current_balance": round(current_balance, 2)
        }
        
        self.logger.info(
            f"📊 STATS AGGIORNATE | "
            f"DAILY: {report['daily']['pnl']}$ ({report['daily']['pnl_pct_real']}%) | "
            f"TOTAL: {report['total']['pnl']}$ | "
            f"PF: {profit_factor} | DD: {report['total']['max_drawdown']}% | "
            f"BALANCE: {report['current_balance']}$"
        )
        
        return report

    def genera_dati_report_giornaliero(self):
        """
        Report giornaliero (Telegram) basato ESCLUSIVAMENTE su kraken_ledger e kraken_trades.
        Niente più storico_trades interno (inattendibile per attribuzione PnL).
        """
        ora_now = datetime.now()
        mezzanotte = ora_now.replace(hour=0, minute=0, second=0, microsecond=0)
        mezzanotte_ts = mezzanotte.timestamp()
        
        from core.database_manager import db_manager as _dbm
        
        # PnL aggregato dalla tabella ledger (margin amount - fees - rollover)
        margin_amt = 0.0
        margin_fee = 0.0
        rollover_fee = 0.0
        try:
            with _dbm._conn_lock:
                _cur = _dbm._conn.cursor()
                _cur.execute(
                    "SELECT type, amount, fee FROM kraken_ledger WHERE asset='ZUSD' AND time >= ?",
                    (mezzanotte_ts,)
                )
                for typ, amt_l, fee_l in _cur.fetchall():
                    amt_f = float(amt_l or 0)
                    fee_f = float(fee_l or 0)
                    if typ == 'margin':
                        margin_amt += amt_f
                        margin_fee += fee_f
                    elif typ == 'rollover':
                        rollover_fee += fee_f
        except Exception as _e:
            _err.capture(_e, "genera_dati_report_giornaliero", {"module": "TradeManager"})
            self.logger.error(f"❌ Errore lettura kraken_ledger (report giornaliero): {_e}")
        
        pnl_usd_aggregato = margin_amt - margin_fee - rollover_fee
        
        # Dettaglio trade chiusi oggi da kraken_trades (con net != 0)
        dettaglio_chiusi = []
        n_trade_oggi = 0
        n_win_oggi = 0
        try:
            with _dbm._conn_lock:
                _cur = _dbm._conn.cursor()
                _cur.execute(
                    "SELECT pair, time, type, price, vol, net FROM kraken_trades "
                    "WHERE time >= ? AND net IS NOT NULL ORDER BY time ASC",
                    (mezzanotte_ts,)
                )
                for pair, t_t, typ_t, price_t, vol_t, net_t in _cur.fetchall():
                    try:
                        net_f = float(net_t or 0)
                    except Exception:
                        continue
                    if net_f == 0:
                        continue  # apertura, non chiusura
                    n_trade_oggi += 1
                    if net_f > 0:
                        n_win_oggi += 1
                    direzione = 'LONG' if str(typ_t).lower() == 'sell' else 'SHORT'
                    # type='sell' chiude una LONG, type='buy' chiude una SHORT
                    dettaglio_chiusi.append(f"{pair} ({direzione}): {net_f:+.2f}$")
        except Exception as _e:
            _err.capture(_e, "genera_dati_report_giornaliero", {"module": "TradeManager"})
            self.logger.error(f"❌ Errore lettura kraken_trades (report giornaliero): {_e}")
        
        # Moonshots: pos in fase 2 attualmente in self.posizioni_aperte (= ancora aperte e già scalate)
        moonshots = sum(1 for p in self.posizioni_aperte.values() if (p or {}).get('fase') == 2)
        
        report = {
            "pnl_totale_24h": round(pnl_usd_aggregato, 2),
            "trades_chiusi": n_trade_oggi,
            "win_rate": round(n_win_oggi / n_trade_oggi * 100, 2) if n_trade_oggi > 0 else 0.0,
            "moonshots_attivati": moonshots,
            "dettaglio": dettaglio_chiusi,
            "posizioni_ancora_aperte": list(self.posizioni_aperte.keys()),
            "breakdown": {
                "margin_amount": round(margin_amt, 4),
                "margin_fee": round(margin_fee, 4),
                "rollover_fee": round(rollover_fee, 4),
            }
        }
        
        self.logger.info(
            f"📊 Report 24h da Kraken | PnL netto: {report['pnl_totale_24h']}$ | "
            f"{n_trade_oggi} trade ({n_win_oggi}W) | "
            f"breakdown: margin={margin_amt:+.2f}$ fees={margin_fee:.2f}$ rollover={rollover_fee:.2f}$"
        )
        
        return report

    def reset_history(self):
        """Pulisce lo storico dei trade dal database e resetta il PnL giornaliero."""
        from core.database_manager import db_manager
        try:
            self.storico_trades = []
            self.daily_pnl = 0.0
            db_manager.save_storico([])
            self.logger.info("🧹 Storico trades e PnL giornaliero ripuliti con successo.")
            return True
        except Exception as e:
            _err.capture(e, "reset_history", {"module": "TradeManager"})
            self.logger.error(f"❌ Errore durante il reset dello storico: {e}")
            return False

# File updated to sync with UI