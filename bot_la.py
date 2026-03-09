# -*- coding: utf-8 -*-
"""
L&A Institutional Bot - bot_la.py
Versione 2.9.4: FULL CODE - FIX LOOP RIPARAZIONE E ANTI-429
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
    
    # 1. Inizializzazione Componenti
    alerts = TelegramAlerts()
    performer = PerformerLA() 
    fe = FeedbackEngine()
    
    trade_manager = TradeManager(
        alerts=alerts,
        performer=performer,
        feedback_engine=fe
    )
    
    brain = BrainLA(
        api_key=config_la.KRAKEN_KEY,
        api_secret=config_la.KRAKEN_SECRET,
        gemini_api_key=config_la.GEMINI_API_KEY,
        gemini_model_name="gemini-2.0-flash",
        alerts=alerts,        
        feedback_engine=fe
    
    )
    
    brain.trade_manager = trade_manager
    engine = EngineLA(api_key=config_la.KRAKEN_KEY, api_secret=config_la.KRAKEN_SECRET)
    
    # Sincronizzazione iniziale
    try:
        trade_manager.sincronizza_con_exchange(engine)
    except Exception as e:
        logger.error(f"⚠️ Impossibile sincronizzare le posizioni all'avvio: {e}")
    
    macro = MacroSentiment()
    
    report_inviato_oggi = False
    
    # --- CONFIGURAZIONE TIMING ---
    WAIT_PROTEZIONE = 2    
    ultimo_check_ia = {asset: -1 for asset in al_config.ASSET_PRINCIPALI}
    ultimo_report_morning = 0
    ultimo_report_night = 0
    ultima_sincro_globale = -1
    
    logger.info("⚡ MONITORAGGIO REAL-TIME ATTIVO - PROTEZIONE ORFANI E COOLDOWN ATTIVI")

    try:
        while True:
            # Usiamo 'momento_ciclo' per non oscurare la libreria time
            momento_ciclo = time.time()
            ora_now = datetime.now()
            minuto_attuale = ora_now.minute
            e_quarto_d_ora = minuto_attuale in [0, 15, 30, 45]

            # --- A. REPORT CRONOMETRATI ---
            # Report Mattutino (Ore 07:00)
            if ora_now.hour == 7 and ora_now.minute == 0 and (momento_ciclo - ultimo_report_morning > 120):
                _, macro_val = macro.get_macro_data()
                brain.genera_report_mattutino(macro_val)
                ultimo_report_morning = momento_ciclo

            # Report Operativo Serale (Ore 20:00)
            if ora_now.hour == 20 and not report_inviato_oggi:
                logger.info("📡 Generazione report serale operativo...")
                dati_report = trade_manager.genera_dati_report_giornaliero()
                alerts.invia_report_serale(dati_report)
                report_inviato_oggi = True

            # Reset flag di sicurezza (Ore 00:00)
            if ora_now.hour == 0 and report_inviato_oggi:
                report_inviato_oggi = False
                
            # --- B. SINCRONIZZAZIONE GLOBALE PERIODICA ---
            if minuto_attuale % 10 == 0 and ultima_sincro_globale != minuto_attuale:
                logger.info("🔄 Sincronizzazione globale anti-orfani...")
                trade_manager.sincronizza_con_exchange(engine)
                ultima_sincro_globale = minuto_attuale

            # --- C. CICLO ASSET ---
            for asset in al_config.ASSET_PRINCIPALI:
                logger.info(f"🧪 [DEBUG] Entrato nel ciclo per {asset}")
                try:
                    is_aperta = trade_manager.sincronizza_e_ripara(asset)
                    logger.info(f"🧪 [DEBUG] Riparazione completata per {asset}")

                    prezzo_monitor = performer.get_current_price(asset)
                    logger.info(f"🧪 [DEBUG] Prezzo recuperato: {prezzo_monitor}")
                    if not prezzo_monitor: 
                        continue

                    if is_aperta:
                        logger.info(f"🛡️ [DEBUG] Posizione APERTA rilevata per {asset}. Entro in Protezione...")
                        
                        # 1. Tentativo recupero dati profondi
                        dati_freschi = engine.get_full_market_data(asset)
                        prezzo_corrente = dati_freschi.get('close') if dati_freschi else None
                        
                        # --- FIX CRUCIALE: Fallback se l'Engine è lento o incompleto ---
                        if prezzo_corrente is None:
                            logger.warning(f"⚠️ Dati Chimera incompleti per {asset}. Uso fallback prezzo base...")
                            prezzo_corrente = performer.get_current_price(asset)
                        
                        if prezzo_corrente is None:
                            logger.error(f"❌ Impossibile recuperare alcun prezzo per {asset}. Salto protezione.")
                            continue

                        atr_corrente = dati_freschi.get('atr', 0) if dati_freschi else 0
                        pos_info = trade_manager.posizioni_aperte.get(asset, {})
                        direzione_pos = pos_info.get('direzione', 'LONG')
                        
                        if pos_info.get('p_entrata') is None:
                            logger.error(f"❌ DATI CORROTTI PER {asset}. Forzo risincronizzazione immediata...")
                            trade_manager.sincronizza_con_exchange(engine)
                            continue

                        # 2. LOGICA CHIMERA (Solo se i dati Engine sono presenti)
                        if dati_freschi and dati_freschi.get('price_velocity') is not None:
                            try:
                                # Analisi se attivare la Fase Due
                                res_chimera = brain.analizza_fase_due_chimera(asset, dati_freschi, direzione_pos)
                                if attiva_fase_due:
                                    logger.info(f"⚡ [CHIMERA TRIGGER] {motivo_chimera} su {asset}.")
                                    trade_manager.rimuovi_tp_fase_due(asset, motivo_chimera)
                            except Exception as e_chim:
                                logger.warning(f"⚠️ Fallimento analisi Chimera per {asset}: {e_chim}")

                        # 3. PROTEZIONE SEMPRE ATTIVA (Usa il prezzo di fallback se necessario)
                        try:
                            trade_manager.gestisci_protezione_istituzionale(
                                asset=asset, 
                                prezzo_attuale=prezzo_corrente, 
                                atr_attuale=atr_corrente
                            )
                            logger.info(f"🛡️ [DEBUG] Protezione aggiornata per {asset}.")
                        except Exception as e_prot:
                            logger.error(f"❌ Errore critico protezione {asset}: {e_prot}")
                        
                        logger.info(f"✅ Ciclo protezione terminato per {asset}.")
                        continue

                    # --- 3. ANALISI IA (Solo se FLAT) ---
                    logger.info(f"🔭 [DEBUG] Avvio check Sentinella per {asset}...")
                    trigger_sentinella = engine.check_sentinel(asset) 
                    logger.info(f"✅ [DEBUG] Sentinella completata. Trigger: {trigger_sentinella}")
                    
                    if (ultimo_check_ia[asset] == -1) or trigger_sentinella or (e_quarto_d_ora and ultimo_check_ia[asset] != minuto_attuale):
                        
                        # Anti-429 Delay
                        idx = al_config.ASSET_PRINCIPALI.index(asset)
                        if idx > 0: 
                            logger.info(f"⏳ Delay sicurezza anti-429 per {asset}...")
                            import time as t_lib; t_lib.sleep(5)

                        ultimo_check_ia[asset] = minuto_attuale

                        # --- 1. ESTRAZIONE INTEGRALE ENGINE (PROJECT CHIMERA) ---
                        logger.info(f"📊 Analisi Profonda Chimera per {asset}...")
                        res = engine.get_market_data(asset)
                        _, macro_val = macro.get_macro_data()
                        
                        if not res or res.get('close', 0) == 0:
                            logger.warning(f"⚠️ Dati Engine incompleti per {asset}. Salto analisi.")
                            ultimo_check_ia[asset] = -1
                            continue

                        # --- 2. MAPPATURA DICT PER IL BRAIN (IL PACCHETTO DATI) ---
                        # Blindiamo ogni valore con "or 0" per evitare crash di formattazione NoneType
                        dati_mercato_chimera = {
                            "ticker": asset,
                            "prezzo": res.get('close') or 0.0,
                            "close": res.get('close') or 0.0,
                            "atr": res.get('atr') or 0.0,
                            "spread_perc": res.get('spread_perc') or 0.0,
                            "vpin_toxicity": res.get('vpin_toxicity') or 0.0,
                            "hurst_exponent": res.get('hurst_exponent') or 0.0,
                            "z_score": res.get('z_score') or 0.0,
                            "market_regime": res.get('market_regime') or "Noise",
                            "price_velocity": res.get('price_velocity') or 0.0,
                            "cvd_reale": res.get('cvd_reale') or 0.0,
                            "cvd_divergence": res.get('cvd_divergence') or 0.0,
                            "indice_spoofing": res.get('indice_spoofing') or 0.0,
                            "iceberg_presenti": res.get('iceberg_presenti') or 0.0,
                            "funding_z_score": res.get('funding_z_score') or 0.0,
                            "actual_funding_rate": res.get('funding_rate_ext') or 0.0,
                            "vwap": res.get('vwap') or 0.0,
                            "poc": res.get('poc') or 0.0,
                            "vah": res.get('vah') or 0.0,
                            "val": res.get('val') or 0.0,
                            "volume_profile": {
                                "poc": res.get('poc') or 0.0, 
                                "vah": res.get('vah') or 0.0, 
                                "val": res.get('val') or 0.0, 
                                "delta_poc": res.get('delta_poc') or 0.0
                            },
                            "order_flow_advanced": {
                                "cvd": res.get('cvd_reale') or 0.0, 
                                "divergenza_cvd": res.get('cvd_divergence') or 0.0,
                                "velocity_prezzo": res.get('price_velocity') or 0.0, 
                                "trade_velocity": res.get('trade_velocity') or 0.0,
                                "vpin_toxicity": res.get('vpin_toxicity') or 0.0, 
                                "delta_footprint": res.get('delta_footprint') or 0.0,
                                "market_driver": res.get('market_driver') or "Unknown"
                            },
                            "microstruttura_hft": {
                                "indice_spoofing": res.get('indice_spoofing') or 0.0, 
                                "iceberg_detected": res.get('iceberg_presenti') or 0.0,
                                "absorption": res.get('absorption') or 0.0, 
                                "muri_liquidita": res.get('liquidity_pools') or [],
                                "persistenza_muri": res.get('wall_persistence') or 0.0
                            },
                            "regime_e_volatilita": {
                                "hurst_exponent": res.get('hurst_exponent') or 0.0, 
                                "market_regime": res.get('market_regime') or "Noise",
                                "z_score": res.get('z_score') or 0.0, 
                                "squeeze": res.get('squeeze') or False,
                                "atr": res.get('atr') or 0.0, 
                                "vol_shock": res.get('vol_shock') or 0.0
                            },
                            "derivati_sentiment": {
                                "funding_rate": res.get('funding_rate_ext') or 0.0, 
                                "funding_z_score": res.get('funding_z_score') or 0.0,
                                "open_interest": res.get('open_interest') or 0.0, 
                                "liquidazioni_24h": res.get('liquidazioni_24h') or 0.0,
                                "put_call_ratio": res.get('put_call_ratio') or 0.0
                            },
                            "macro_context": {
                                "eth_btc_ratio": res.get('macro_proxy', {}).get('eth_btc_ratio') or 0.0,
                                "liquidity_warning": res.get('macro_proxy', {}).get('market_liquidity_warning') or False,
                                "relative_volume": res.get('macro_proxy', {}).get('relative_volume_status') or "Normal",
                                "macro_regime": res.get('macro_regime') or "Neutral"
                            },
                            "spread_attuale": res.get('spread_perc') or 0.0,
                            "signal_quality": res.get('signal_quality') or 0.0
                        }
                        # --- 3. LOGICA DI DECISIONE BRAIN ---
                        history = fe.get_recent_summary() or {} 
                        decision = brain.full_global_strategy(
                            dati_mercato_chimera, 
                            asset, 
                            macro_val if macro_val is not None else "Neutrale", 
                            performance_history=history
                        )

                        if decision and isinstance(decision, dict):
                            direzione_ia = decision.get('direzione', 'FLAT')
                            voto_ia = decision.get('voto') if decision.get('voto') is not None else 0
                            
                            # Messa in sicurezza dello spread
                            spread_val = dati_mercato_chimera.get('spread_attuale')
                            spread_sicuro = float(spread_val) if spread_val is not None else 0.0
                            
                            voto_minimo = 6 if spread_sicuro <= 0.35 else 7

                            # LOG BLINDATO
                            logger.info(f"⚖️ VALUTAZIONE {asset}: Voto {voto_ia} | Soglia {voto_minimo} | Spread {spread_sicuro:.3f}%")

                            if direzione_ia != "FLAT" and voto_ia >= voto_minimo:
                                logger.info(f"🚀 SEGNALE VALIDATO! Esecuzione ordine per {asset}...")
                                
                                # Recupero prezzo sicuro per l'ordine
                                p_entry = prezzo_monitor if prezzo_monitor is not None else 0
                                
                                success_pos = trade_manager.apri_posizione(
                                    asset=asset, 
                                    direzione=direzione_ia,
                                    entry_price=p_entry,
                                    size=0, 
                                    sl=decision.get('sl', 0), 
                                    tp=decision.get('tp', 0), 
                                    voto=voto_ia, 
                                    leverage=None, 
                                    dati_mercato=dati_mercato_chimera
                                )
                                
                                if success_pos:
                                    alerts.invia_alert(f"🚀 *ENTRY {asset}* (Voto: {voto_ia})\n\n🎯 TP: {decision.get('tp')}\n🛡️ SL: {decision.get('sl')}\n\n🧠 *Razionale:* {decision.get('razionale')}")
                                else:
                                    logger.warning(f"❌ Apertura fallita per {asset} (Rifiuto TradeManager).")
                            else:
                                motivo = "VOTO_BASSO" if direzione_ia != "FLAT" else "DIREZIONE_FLAT"
                                logger.info(f"🚫 ANALISI SCARTATA {asset}: {motivo} (Voto {voto_ia})")
                                fe.registra_analisi_scartata(asset, voto_ia, direzione_ia, res.get('close'), dati_mercato_chimera)
                except Exception as e:
                    logger.error(f"❌ ERRORE ASSET {asset}: {e}")

            # --- VERIFICA GHOST TRADES E ATTESA ---
            try:
                fe.verifica_esiti_ghost(engine.exchange)
            except Exception as e:
                logger.error(f"⚠️ Errore verifica Ghost: {e}")

            # Attesa finale del ciclo - Fix per evitare NameError
            import time as t_lib; t_lib.sleep(WAIT_PROTEZIONE)

    except KeyboardInterrupt:
        logger.info("🛑 Bot fermato manualmente.")
    except Exception as e:
        logger.critical(f"💀 CRASH TOTALE: {e}")
        traceback.print_exc()

if __name__ == "__main__":
    main()