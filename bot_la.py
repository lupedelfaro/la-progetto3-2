# -*- coding: utf-8 -*-
"""
L&A Institutional Bot - bot_la.py
Versione 2.9.6: CHIMERA FLOW RESTORED (No more ghosting)
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
    ultima_sincro_globale = -1
    
    logger.info("⚡ MONITORAGGIO REAL-TIME ATTIVO - PROJECT CHIMERA READY")

    try:
        while True:
            momento_ciclo = time.time()
            ora_now = datetime.now()
            minuto_attuale = ora_now.minute
            e_quarto_d_ora = minuto_attuale in [0, 15, 30, 45]

            # --- A. REPORT CRONOMETRATI ---
            if ora_now.hour == 7 and ora_now.minute == 0 and (momento_ciclo - ultimo_report_morning > 120):
                _, macro_val = macro.get_macro_data()
                brain.genera_report_mattutino(macro_val)
                ultimo_report_morning = momento_ciclo

            if ora_now.hour == 20 and not report_inviato_oggi:
                logger.info("📡 Generazione report serale operativo...")
                dati_report = trade_manager.genera_dati_report_giornaliero()
                alerts.invia_report_serale(dati_report)
                report_inviato_oggi = True

            if ora_now.hour == 0 and report_inviato_oggi:
                report_inviato_oggi = False
                
            # --- B. SINCRONIZZAZIONE GLOBALE PERIODICA ---
            if minuto_attuale % 10 == 0 and ultima_sincro_globale != minuto_attuale:
                logger.info("🔄 Sincronizzazione globale anti-orfani...")
                trade_manager.sincronizza_con_exchange(engine)
                ultima_sincro_globale = minuto_attuale

            # --- C. CICLO ASSET PRINCIPALI ---
            for asset in al_config.ASSET_PRINCIPALI:
                try:
                    # 1. Recupero lo stato REALE da Kraken per questo asset
                    posizioni_reali = performer.get_open_positions_real()
                    # Cerco se l'asset attuale è tra le posizioni aperte (usando il match del performer)
                    pos_real_data = next((v for k, v in posizioni_reali.items() if performer._normalize_ticker(v.get('pair')) == performer._normalize_ticker(asset)), None)

                    # --- SCUDO CHIMERA: PROTEZIONE ORFANI ---
                    if pos_real_data and (not pos_real_data.get('has_sl') or not pos_real_data.get('has_tp')):
                        logger.warning(f"🛡️ Scudo Chimera: Rilevata mancanza protezione su {asset}!")
                        
                        # Recupero dati per calcolo paracadute
                        raw_p = engine.get_full_market_data(asset)
                        dati_p = raw_p[0] if isinstance(raw_p, tuple) else raw_p
                        atr_p = dati_p.get('atr', 0) if dati_p else 0
                        prezzo_p = float(pos_real_data.get('price', 0))
                        direzione_p = "buy" if pos_real_data.get('type') == 'buy' else "sell"
                        vol_p = pos_real_data.get('vol')

                        if not pos_real_data.get('has_sl'):
                            dist_sl = atr_p * 2 if atr_p > 0 else prezzo_p * 0.015
                            prezzo_sl = prezzo_p - dist_sl if direzione_p == "buy" else prezzo_p + dist_sl
                            performer.gestisci_ordine_protezione(asset, "SL", prezzo_sl, direzione_p, vol_p)

                        if not pos_real_data.get('has_tp'):
                            dist_tp = atr_p * 3 if atr_p > 0 else prezzo_p * 0.03
                            prezzo_tp = prezzo_p + dist_tp if direzione_p == "buy" else prezzo_p - dist_tp
                            performer.gestisci_ordine_protezione(asset, "TP", prezzo_tp, direzione_p, vol_p)

                    # 2. Gestione Posizioni Aperte (Protezione & Fase Due)
                    is_aperta = trade_manager.sincronizza_e_ripara(asset)
                    
                    if is_aperta:
                        risultato_raw = engine.get_full_market_data(asset)
                        dati_freschi = risultato_raw[0] if isinstance(risultato_raw, tuple) else risultato_raw
                        
                        prezzo_corrente = dati_freschi.get('close') if dati_freschi else performer.get_current_price(asset)
                        if prezzo_corrente:
                            atr_corrente = dati_freschi.get('atr', 0) if dati_freschi else 0
                            direzione_pos = trade_manager.posizioni_aperte.get(asset, {}).get('direzione', 'LONG')

                            # Trigger Fase Due Chimera
                            if dati_freschi and dati_freschi.get('price_velocity') is not None:
                                res_chimera = brain.analizza_fase_due_chimera(asset, dati_freschi, direzione_pos)
                                if isinstance(res_chimera, dict) and res_chimera.get('attiva_fase_due'):
                                    trade_manager.rimuovi_tp_fase_due(asset, res_chimera.get('motivo'))

                            # Protezione Istituzionale
                            trade_manager.gestisci_protezione_istituzionale(
                                asset=asset, 
                                prezzo_attuale=prezzo_corrente, 
                                atr_attuale=atr_corrente
                            )

                    # --- 2. ANALISI IA (ENTRY LOGIC) ---
                    # PROTEZIONE CHIMERA: Se la posizione è già aperta, non cerchiamo nuove entry per questo asset
                    if is_aperta:
                        continue

                    trigger_sentinella = engine.check_sentinel(asset) 
                    is_primo_giro = (ultimo_check_ia[asset] == -1)

                    if is_primo_giro or trigger_sentinella or (e_quarto_d_ora and ultimo_check_ia[asset] != minuto_attuale):
                        logger.info(f"🔍 [CHIMERA ANALYSIS] Controllo opportunità su {asset}...")
                        ultimo_check_ia[asset] = minuto_attuale

                        # Recupero dati profondi
                        res_raw = engine.get_full_market_data(asset)
                        res = res_raw[0] if isinstance(res_raw, tuple) else res_raw
                        import json
                        logger.info(f"DEBUG CHIAVI ENGINE: {json.dumps(list(res.keys()), indent=2)}")
                        _, macro_val = macro.get_macro_data()
    
                        if not res or res.get('close', 0) == 0:
                            logger.warning(f"⚠️ Dati Engine totalmente assenti per {asset}. Salto.")
                            continue

                        # --- FIX DATI PER BRAIN ---
                        prezzo_ref = float(res.get('close', 0))
                        
                        # ATR Sicuro: evita lo zero che rompe il calcolo SL del Brain
                        atr_sicuro = float(res.get('atr', 0))
                        if atr_sicuro <= 0:
                            atr_sicuro = prezzo_ref * 0.002 

                        # Pulizia Muri (Anti-999%)
                        muri = res.get('liquidity_pools', [])
                        if not muri or len(muri) == 0:
                            muri = [
                                {"price": prezzo_ref * 0.985, "type": "support", "volume": 1.0},
                                {"price": prezzo_ref * 1.015, "type": "resistance", "volume": 1.0}
                            ]

                        # Costruzione Dizionario CHIMERA (Allineamento 1:1 con il tuo DEBUG)
                        dati_mercato_chimera = {
                            'close': res.get('close', res.get('price', 0)), # Prova entrambi
                            'atr': res.get('atr', 0),
                            'health_data': res.get('health_data', {'score': 0.5}),
                            'market_regime': res.get('market_regime', 'UNKNOWN'),
                            'cvd_istantaneo': res.get('cvd_istantaneo', 0),
                            'vpin': res.get('vpin', 0),
                            'price_velocity': res.get('price_velocity', 0),
                            'muro_supporto': res.get('muro_supporto', 0),
                            'muro_resistenza': res.get('muro_resistenza', 0),
                            'dist_supporto': res.get('dist_supporto', 999),
                            'dist_resistenza': res.get('dist_resistenza', 999),
                            'hurst_exponent': res.get('hurst_exponent', 0.5),
                            'indice_spoofing': res.get('indice_spoofing', 0),
                            'microstruttura_hft': {
                                'muri_liquidita': res.get('liquidity_pools', []),
                                'aggressivita': res.get('aggressivita', 0)
                            }
                        }
                        
                        # LOG CORRETTO (Prende i dati dal nuovo dizionario)
                        cvd_log = dati_mercato_chimera.get('cvd_istantaneo', 0.0)
                        logger.info(f"🧠 DATI INVIATI A GEMINI [{asset}]: CVD={cvd_log:.2f}, ATR={atr_sicuro:.2f}")    
                        history = fe.get_recent_summary()
                        decision = brain.full_global_strategy(
                            dati_engine=dati_mercato_chimera, 
                            asset_name=asset, 
                            macro_sentiment=macro_val or "Neutrale", 
                            performance_history=history
                        )

                        if decision and decision.get('direzione') != "FLAT":
                            voto_ia = decision.get('voto', 0)
                            spread_sicuro = float(dati_mercato_chimera.get('spread_perc', 0))
                            voto_minimo = 6 if spread_sicuro <= 0.35 else 7

                            if voto_ia >= voto_minimo:
                                logger.info(f"🚀 SEGNALE VALIDATO {asset}: Voto {voto_ia} - ESEGUO.")
            
                                # --- GESTIONE LEVA ISTITUZIONALE KRAKEN ---
                                lev_ia = decision.get('leverage', 1)
                                
                                if asset in ['XXBTZUSD', 'XETHZUSD']:
                                    leverage_f = min(int(lev_ia), 10)
                                elif asset == 'XETHXXBT':
                                    leverage_f = min(int(lev_ia), 5)
                                else:
                                    leverage_f = None 

                                if leverage_f and leverage_f <= 1:
                                    leverage_f = None

                                success_pos = trade_manager.apri_posizione(
                                    asset=asset, 
                                    direzione=decision['direzione'],
                                    entry_price=prezzo_ref,
                                    size=decision.get('sizing', 0.01),
                                    leverage=leverage_f, 
                                    sl=decision.get('sl', 0), 
                                    tp=decision.get('tp', 0), 
                                    voto=voto_ia, 
                                    dati_mercato=dati_mercato_chimera
                                )
            
                                if success_pos:
                                    alerts.invia_alert(f"🚀 *ENTRY {asset}* (Voto: {voto_ia})\n🧠 {decision.get('razionale')}")
                            else:
                                fe.registra_analisi_scartata(asset, voto_ia, decision['direzione'], prezzo_ref, dati_mercato_chimera)
                                logger.info(f"⚖️ Segnale {asset} scartato: Voto {voto_ia} < {voto_minimo}")
                except Exception as e_asset:
                    logger.error(f"❌ ERRORE CRITICO SU {asset}: {e_asset}")
                    traceback.print_exc()

            # --- VERIFICA GHOST TRADES E ATTESA ---
            try:
                fe.verifica_esiti_ghost(engine.exchange)
            except: pass

            time.sleep(WAIT_PROTEZIONE)

    except KeyboardInterrupt:
        logger.info("🛑 Bot fermato manualmente.")
    except Exception as e:
        logger.critical(f"💀 CRASH TOTALE: {e}")
        traceback.print_exc()

if __name__ == "__main__":
    main()