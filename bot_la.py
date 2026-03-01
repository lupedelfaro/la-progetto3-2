# -*- coding: utf-8 -*-
"""
L&A Institutional Bot - bot_la.py
Versione 2.8: FIX RILEVAMENTO CHIUSURA POSIZIONE E LOGICA IA
"""
import traceback
import logging
import time
from datetime import datetime

from core.engine_la import EngineLA
from core.brain_la import BrainLA
from core.trade_manager import TradeManager
from core.performer_la import PerformerLA
from core.telegram_alerts_la import TelegramAlerts
from core.macro_sentiment import MacroSentiment
from core import config_la
from core import asset_list as al_config
from core.feedback_engine import FeedbackEngine 

def main():
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    logger = logging.getLogger("MainBot")
    
    # 1. Inizializzazione Componenti Reali
    alerts = TelegramAlerts()
    performer = PerformerLA() 
    fe = FeedbackEngine()
    
    # TradeManager 
    trade_manager = TradeManager(
        alerts=alerts,
        performer=performer,
        feedback_engine=fe
    )
    
    brain = BrainLA(
        api_key=config_la.KRAKEN_KEY,
        api_secret=config_la.KRAKEN_SECRET,
        gemini_api_key=config_la.GEMINI_API_KEY,
        gemini_model_name="gemini-2.0-flash"
    )
    
    brain.trade_manager = trade_manager

    engine = EngineLA(api_key=config_la.KRAKEN_KEY, api_secret=config_la.KRAKEN_SECRET)
    try:
        trade_manager.sincronizza_con_exchange(engine)
    except Exception as e:
        logger.error(f"⚠️ Impossibile sincronizzare le posizioni all'avvio: {e}")
    
    macro = MacroSentiment()
    
    # --- CONFIGURAZIONE TIMING ---
    WAIT_PROTEZIONE = 2    
    # Inizializza a -1 per forzare l'analisi immediata al primo avvio
    ultimo_check_ia = {asset: -1 for asset in al_config.ASSET_PRINCIPALI}
    
    ultimo_report_morning = 0
    ultimo_report_night = 0
    logger.info("⚡ MONITORAGGIO REAL-TIME ATTIVO - AUTO-LEARNING FULLY SYNCED")

    try:
        while True:
            tempo_attuale = time.time()
            ora_now = datetime.now()
            minuto_attuale = ora_now.minute
            e_quarto_d_ora = minuto_attuale in [0, 15, 30, 45]

            # --- A. REPORT CRONOMETRATI (07:00 e 23:00) ---
            if ora_now.hour == 7 and ora_now.minute == 0 and (tempo_attuale - ultimo_report_morning > 120):
                _, macro_val = macro.get_macro_data()
                brain.genera_report_mattutino(macro_val)
                ultimo_report_morning = tempo_attuale

            if ora_now.hour == 23 and ora_now.minute == 0 and (tempo_attuale - ultimo_report_night > 120):
                brain.genera_report_serale("Stats Giornaliere in elaborazione...")
                ultimo_report_night = tempo_attuale

            for asset in al_config.ASSET_PRINCIPALI:
                try:
                    # --- 1. SINCRONIZZAZIONE REALE CON KRAKEN (RILEVAMENTO CHIUSURE) ---
                    try:
                        posizione_reale = performer.get_open_positions(asset)
                        
                        # MODIFICA: Controllo volume reale per rilevare chiusura
                        is_aperta = False
                        if isinstance(posizione_reale, dict) and float(posizione_reale.get('vol', 0)) > 0:
                            is_aperta = True

                        if asset in trade_manager.posizioni_aperte and not is_aperta:
                            logger.warning(f"⚠️ Chiusura rilevata per {asset}. Pulizia ordini e feedback...")
                            performer.pulizia_totale_ordini(asset)
                            
                            pos_data = trade_manager.posizioni_aperte[asset]
                            p_entrata = float(pos_data['p_entrata'])
                            direzione = pos_data['direzione']
                            
                            symbol_ticker = al_config.get_ticker(asset)
                            ticker_veloce = engine.exchange.fetch_ticker(symbol_ticker)
                            p_uscita = float(ticker_veloce['last'])
                            
                            pnl_finale = ((p_uscita - p_entrata) / p_entrata) * 100
                            if direzione == "SELL": pnl_finale *= -1
                            esito = "WIN" if pnl_finale > 0 else "LOSS"

                            trade_manager.registra_conclusione_trade(asset, esito, pnl_finale)
                            alerts.invia_alert(f"📉 *CHIUSURA {asset}*\nEsito: {esito} | PNL: {round(pnl_finale, 2)}%\nIl sistema ha archiviato i dati per migliorare.")
                            continue 
                            
                    except Exception as e:
                        logger.error(f"📡 Errore sincronizzazione Kraken per {asset}: {e}")

                    # --- 2. RECUPERO PREZZO ATTIVO ---
                    try:
                        symbol_ticker = al_config.get_ticker(asset)
                        ticker_veloce = engine.exchange.fetch_ticker(symbol_ticker)
                        prezzo_monitor = float(ticker_veloce['last'])
                    except Exception as e:
                        logger.warning(f"❌ Errore Ticker per {asset}: {e}")
                        continue 

                    # --- 3. LOGICA SENTINELLA / IA SINCRONIZZATA ---
                    trigger_sentinella = engine.check_sentinel(asset) 
                    
                    e_primo_avvio = (ultimo_check_ia[asset] == -1)
                    gia_analizzato_ora = (ultimo_check_ia[asset] == minuto_attuale)

                    if e_primo_avvio or trigger_sentinella or (e_quarto_d_ora and not gia_analizzato_ora):
                        
                        if e_primo_avvio:
                            logger.info(f"🚀 [RIAVVIO MANUALE] Analisi iniziale immediata per {asset}...")
                        elif trigger_sentinella:
                            logger.info(f"🔥 [SENTINELLA] Anomalia volumi su {asset}!")
                        else:
                            logger.info(f"⏰ [OROLOGIO] Analisi ciclica delle {ora_now.strftime('%H:%M')} per {asset}")

                        # BLOCCA IL MINUTO IMMEDIATAMENTE
                        ultimo_check_ia[asset] = minuto_attuale

                        dati_mercato = engine.get_market_data(asset)
                        _, macro_val = macro.get_macro_data()
                        
                        if not dati_mercato:
                            logger.warning(f"⚠️ Dati incompleti per {asset}, rinvio analisi...")
                            ultimo_check_ia[asset] = -1
                            continue

                        history = fe.get_recent_summary() or {} 
                        decision = brain.full_global_strategy(
                            dati_mercato, 
                            asset, 
                            macro_val if macro_val is not None else "Neutrale", 
                            performance_history=history
                        )

                        # --- 4. FILTRO VOTO E APERTURA (CORRETTAMENTE INDENTATO) ---
                        VOTO_MINIMO = 7 
                        
                        if asset not in trade_manager.posizioni_aperte:
                            if decision and isinstance(decision, dict):
                                direzione_ia = decision.get('direzione', 'FLAT')
                                voto_ia = decision.get('voto', 0)
                                
                                if direzione_ia != "FLAT" and voto_ia >= VOTO_MINIMO:
                                    config_asset = al_config.ASSET_CONFIG.get(asset, {})
                                    leva = config_asset.get('leverage', 5)
                                    size = config_asset.get('min_size', 0.01)
                                    
                                    logger.info(f"📡 INVIO ORDINE REALE: {asset} {direzione_ia} (Voto {voto_ia})")
                                    
                                    success_pos = trade_manager.apri_posizione(
                                        asset=asset, direzione=direzione_ia,
                                        entry_price=prezzo_monitor, size=size,
                                        sl=0, tp=0, voto=voto_ia, 
                                        leverage=leva, dati_mercato=dati_mercato
                                    )
                                    
                                    if success_pos:
                                        alerts.invia_alert(f"🚀 *ENTRY {asset}* (Voto: {voto_ia})\nRazionale: {decision.get('razionale')}")
                                    else:
                                        logger.error(f"❌ FALLIMENTO INVIO ORDINE KRAKEN per {asset}")
                                
                                else:
                                    # Ghost Trading: Registrato solo una volta per ciclo IA
                                    logger.info(f"⚠️ Analisi archiviata (Ghost): {asset} {direzione_ia} con voto {voto_ia}")
                                    
                                    direzione_ghost = direzione_ia
                                    if direzione_ghost == "FLAT":
                                        direzione_ghost = "BUY" if dati_mercato.get('z_score', 0) < 0 else "SELL"
                                        
                                    fe.registra_analisi_scartata(asset, voto_ia, direzione_ghost, prezzo_monitor, dati_mercato)

                    # --- 5. PROTEZIONE DINAMICA & TRAILING ---
                    if asset in trade_manager.posizioni_aperte:
                        try:
                            # Tentativo di recupero ATR fresco dall'engine
                            dati_freschi = engine.get_full_market_data(asset)
                            atr_fresco = dati_freschi.get('atr', 0)
                            
                            # Se l'engine fallisce (atr=0), usiamo l'ATR salvato all'apertura
                            if atr_fresco == 0:
                                pos_data = trade_manager.posizioni_aperte.get(asset)
                                snapshot = pos_data.get('snapshot_mercato', {})
                                atr_fresco = snapshot.get('atr', 0) if isinstance(snapshot, dict) else 0

                            if atr_fresco > 0:
                                trade_manager.gestisci_protezione_dinamica(
                                    asset=asset, 
                                    prezzo_attuale=prezzo_monitor, 
                                    atr_attuale=atr_fresco
                                )
                            else:
                                # Fallback estremo: protezione base senza ATR
                                trade_manager.gestisci_protezione_istituzionale(asset, prezzo_monitor)
                                
                        except Exception as e_prot:
                            logger.error(f"⚠️ Errore critico ciclo protezione per {asset}: {e_prot}")

                except Exception as e:
                    logger.error(f"❌ ERRORE ASSET {asset}: {e}")

            # --- VERIFICA GHOST TRADES ---
            try:
                fe.verifica_esiti_ghost(engine.exchange)
            except Exception as e:
                logger.error(f"⚠️ Errore verifica Ghost: {e}")

            time.sleep(WAIT_PROTEZIONE)

    except KeyboardInterrupt:
        logger.info("🛑 Bot fermato manualmente.")
    except Exception as e:
        logger.critical(f"💀 CRASH TOTALE: {e}")
        traceback.print_exc()

if __name__ == "__main__":
    main()