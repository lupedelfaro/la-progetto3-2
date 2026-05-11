# -*- coding: utf-8 -*-
"""
L&A Institutional Bot - bot_la.py
CHIMERA v4.0 — Full Institutional Stack
- Time-Stop istituzionale a 3 livelli
- Circuit Breaker PnL -8%
- NightReview (apprendimento notturno alle 02:00)
- AssetRotation (rotazione capitale ogni 15min)
- AdvancedReporter (report serale con loop chiuso)
"""
import traceback
import logging
import time
import sys
from datetime import datetime, timezone

from core.engine_la import EngineLA
from core.brain_la import BrainLA
from core.trade_manager import TradeManager
from core.performer_la import PerformerLA
from core.telegram_alerts_la import TelegramAlerts
from core.macro_sentiment import MacroSentiment
from core import config_la
from core import asset_list as al_config
from core.feedback_engine import FeedbackEngine
from core.night_review import NightReview
from core.asset_rotation import AssetRotation, AdvancedReporter
from core.chimera_auditor import ChimeraAuditor
from core.chimera_ml import ChimeraML
from core.strategy_engine import StrategyEngine
from core.heikinashi_strategy import (
    HeikinAshiStrategy,
    HeikinAshiTrendStrategy,
    HeikinAshiControTrendStrategy,
    HA_TREND_LEVA,
    HA_CONTRO_LEVA,
)
from core.signal_state_engine import SignalStateEngine
from analytics_report import AnalyticsReporter
from core.chimera_errors import ErrorTracker
from core.kraken_integration import KrakenIntegration
_err = ErrorTracker("BotLA")

# ── SOGLIE CVD ADATTIVE PER ASSET ──────────────────────────────────────────
# Calibrate sui volumi reali di ogni asset.
# BTC muove centinaia di k USD/min. SOL/XRP/DOGE raramente superano 50-60k.
# Usate come contesto (non blocco puro) — se CVD è fortemente contro
# la direzione E siamo in FORMAZIONE VUOTA, il trade viene scartato.
# In BREAKOUT ISTITUZIONALE il CVD contro è ignorato (il movimento
# può essere un liquidity sweep prima del vero breakout).
_CVD_SOGLIE = {
    'XXBTZUSD': 150000,   # BTC: volumi enormi
    'XETHZUSD':  80000,   # ETH: liquido ma meno di BTC
    'XXRPZUSD':  40000,   # XRP: volumi medi
    'SOLUSD':    40000,   # SOL: volumi medi
    'XDGUSD':    30000,   # DOGE: volumi bassi in USD
}
_CVD_SOGLIA_DEFAULT = 50000  # fallback per asset non mappati

def _controlla_salute_engine(engine, alerts, logger) -> dict:
    """
    Verifica che Engine stia restituendo valori reali e non fallback.
    
    Controlla i 15 indicatori più critici su BTC (l'asset più liquido
    e quello con meno probabilità di dati mancanti).
    
    Returns:
        dict con score (0-100), ok, problemi, total, dettagli
    """
    try:
        res = engine.get_market_data('XXBTZUSD')
        if isinstance(res, tuple): res = res[0]
    except Exception as e:
        _err.capture(e, sys._getframe().f_code.co_name, {"module": "BotLA", "method": "main"})
        logger.error(f"❌ Engine health: impossibile recuperare dati BTC: {e}")
        if alerts:
            alerts.invia_alert(
                "🚨 *ENGINE HEALTH FAIL*\n"
                "Impossibile recuperare dati da Engine su BTC.\n"
                f"Errore: `{str(e)[:100]}`"
            )
        return {'score': 0, 'ok': 0, 'problemi': 15, 'total': 15, 'dettagli': []}

    close = float(res.get('close', 0))
    if close == 0:
        if alerts:
            alerts.invia_alert("🚨 *ENGINE HEALTH FAIL*\nPrezzo BTC = 0. Engine non sta ricevendo dati.")
        return {'score': 0, 'ok': 0, 'problemi': 15, 'total': 15, 'dettagli': []}

    # Hurst: usa _last_hurst cachato per distinguere 0.5 reale da fallback
    _hurst_cached = getattr(engine, '_last_hurst', None)
    _hurst_check  = _hurst_cached if (_hurst_cached is not None and _hurst_cached != 0.5) else res.get('hurst_exponent', 0.5)

    # Definizione controlli: (nome, valore, default_atteso, critico)
    controlli = [
        # Critici — se sono al default il bot è compromesso
        ("z_score_dist_vwap",      res.get('z_score_dist_vwap', 0.0),    0.0,   True),
        ("z_score",                res.get('z_score', 0.0),               0.0,   True),
        ("hurst_exponent",         _hurst_check,                          0.5,   True),
        ("kaufman_efficiency",     res.get('kaufman_efficiency', 1.0),    1.0,   True),
        ("cvd_istantaneo",         res.get('cvd_istantaneo', 0.0),        0.0,   True),
        ("vpin",                   res.get('vpin', 0.0),                  0.0,   True),
        ("poc",                    res.get('poc', 0.0),                   0.0,   True),
        ("vwap",                   res.get('vwap', close),                close, True),
        ("funding_rate",           res.get('funding_rate', 0.0),          0.0,   False),
        ("open_interest",          res.get('open_interest', 0.0),         0.0,   False),
        ("muro_supporto",          res.get('muro_supporto', 0.0),         0.0,   False),
        ("rsi",                    res.get('rsi', 50.0),                  50.0,  False),
        ("entry_phase",            0 if not res.get('entry_phase') else 1, -1,   False),
        ("pivot_daily",            res.get('pivot_daily', 0.0),           0.0,   False),
        ("market_regime",          0 if res.get('market_regime','UNKNOWN')=='UNKNOWN' else 1, -1, True),
    ]

    problemi_critici = []
    problemi_minori  = []
    ok_count = 0

    for nome, valore, default, critico in controlli:
        # Confronto robusto — funding_rate può essere -0.0000x che arrotonda
        # a zero ma è un valore reale. Usa tolleranza 1e-12 per funding
        # (BTC funding reale può essere -2.2e-10 che è minuscolo ma valido).
        try:
            val_f = float(valore)
            def_f = float(default)
            tol = 1e-12 if 'funding' in nome else 1e-6
            at_default = abs(val_f - def_f) < tol
        except (TypeError, ValueError):
            at_default = (valore == default)

        if at_default:
            if critico:
                problemi_critici.append(nome)
            else:
                problemi_minori.append(nome)
        else:
            ok_count += 1

    total = len(controlli)
    n_problemi = len(problemi_critici) + len(problemi_minori)
    score = ok_count / total * 100

    # Manda alert solo se ci sono problemi
    if problemi_critici:
        msg_lines = [
            "🚨 *ENGINE HEALTH — PROBLEMI CRITICI*",
            f"Score: {score:.0f}% ({ok_count}/{total} indicatori OK)\n",
            "❌ *Indicatori critici al default* (valori sbagliati):",
        ]
        for p in problemi_critici:
            msg_lines.append(f"  • `{p}`")
        if problemi_minori:
            msg_lines.append("\n⚠️ *Indicatori minori al default*:")
            for p in problemi_minori:
                msg_lines.append(f"  • `{p}`")
        msg_lines.append("\n_Gemini sta ricevendo dati non corretti. Verificare i log._")
        if alerts:
            alerts.invia_alert('\n'.join(msg_lines))

    elif problemi_minori:
        msg_lines = [
            f"⚠️ *ENGINE HEALTH — ATTENZIONE*",
            f"Score: {score:.0f}% ({ok_count}/{total} OK)\n",
            "Indicatori minori al default:",
        ]
        for p in problemi_minori:
            msg_lines.append(f"  • `{p}`")
        if alerts:
            alerts.invia_alert('\n'.join(msg_lines))

    return {
        'score':     score,
        'ok':        ok_count,
        'problemi':  n_problemi,
        'total':     total,
        'critici':   problemi_critici,
        'minori':    problemi_minori,
    }


def main():
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("google").setLevel(logging.WARNING)
    logging.getLogger("google_genai").setLevel(logging.WARNING)
    logging.getLogger("google.genai").setLevel(logging.WARNING)
    logging.getLogger("google.ai.generativelanguage").setLevel(logging.WARNING)
    logger = logging.getLogger("MainBot")
    
    # 1. Inizializzazione Componenti
    alerts = TelegramAlerts()
    performer = PerformerLA() 
    fe = FeedbackEngine()

    # FIX 2026-05-02: rebuild feedback_trades dallo storico reale al riavvio.
    # Necessario perché in passato:
    # - night_review hardcodava outcome=LOSS su pattern_errore (bug fissato 2026-05-02)
    # - feedback_trades è stato corrotto: 3W/47L invece del WR reale ~32%
    # Il rebuild ricostruisce gli ultimi 50 feedback dai trade veri (WIN/LOSS reali).
    # Sicuro: idempotente, può essere chiamato a ogni avvio senza accumulare duplicati.
    try:
        rebuild_result = fe.ricostruisci_feedback_da_storico(max_trades=50)
        if rebuild_result:
            logger.info(
                f"♻️ [REBUILD] feedback_trades ricostruito: "
                f"{rebuild_result['n_processati']} trade, "
                f"{rebuild_result['wins']}W/{rebuild_result['losses']}L "
                f"(WR {rebuild_result['wr_pct']:.1f}%, PF {rebuild_result['pf']:.3f})"
            )
    except Exception as _e_rebuild:
        _err.capture(_e_rebuild, "main", {"module": "BotLA"})
        logger.warning(f"⚠️ Rebuild feedback fallito (non bloccante): {_e_rebuild}")

    engine = EngineLA(api_key=config_la.KRAKEN_KEY, api_secret=config_la.KRAKEN_SECRET)
    
    trade_manager = TradeManager(
        alerts=alerts,
        performer=performer,
        feedback_engine=fe,
        engine=engine
    )
    
    # FIX 1 — legge il modello Gemini da config invece del valore hardcoded
    # "gemini-3-flash-preview" non esiste → tutte le analisi davano FLAT/voto 0.
    # Aggiungere GEMINI_MODEL=gemini-2.0-flash nel file .env
    _gemini_model = getattr(config_la, 'GEMINI_MODEL', 'gemini-2.0-flash')
    brain = BrainLA(
        api_key=config_la.KRAKEN_KEY,
        api_secret=config_la.KRAKEN_SECRET,
        gemini_api_key=config_la.GEMINI_API_KEY,
        gemini_model_name=_gemini_model,
        alerts=alerts,        
        feedback_engine=fe,
        engine=engine
    )
    
    brain.trade_manager = trade_manager
    
    # Collega ws_manager al trade_manager per sincronizzazione real-time
    try:
        ws_mgr = engine.ws_manager
        ws_mgr.set_credentials(config_la.KRAKEN_KEY, config_la.KRAKEN_SECRET)
        ws_mgr.set_position_callback(
            lambda symbol, ev: trade_manager.sincronizza_con_exchange(engine)
        )
        # FIX 2026-05-02: avvia il WebSocket. Senza .start() il WS non riceve
        # mai trade/ticker/book — la cache resta vuota e fetch_trades è l'unica
        # fonte. Ora che fetch_trades è bloccato da ccxt (issue #5698), il WS
        # diventa essenziale per dati di order flow.
        ws_mgr.start()
        logger.info("✅ WS executions collegato al trade_manager + WebSocket avviato")
    except Exception as e_ws:
        _err.capture(e_ws, sys._getframe().f_code.co_name, {"module": "BotLA", "method": "main"})
        logger.warning(f"⚠️ WS executions non collegato: {e_ws}")
    
    # Sincronizzazione iniziale
    try:
        trade_manager.sincronizza_con_exchange(engine)
    except Exception as e:
        _err.capture(e, sys._getframe().f_code.co_name, {"module": "BotLA", "method": "main"})
        logger.error(f"⚠️ Impossibile sincronizzare le posizioni all'avvio: {e}")
    
    # ── KRAKEN RECONCILER ─────────────────────────────────────────────────────
    # Inizializza il riconciliatore PnL Kraken↔DB.
    # FIX 2026-05-04: prima del fix, il bot non istanziava mai KrakenReconciler.
    # Questo era un bug grave: il PnL nel DB usava il fallback statistico per il
    # 46% dei trade (chiusi server-side da Kraken), generando una discrepanza
    # di ~60$ tra "PnL bot" (-6$) e "PnL Kraken reale" (-64$).
    # Ora reconciler gira ogni 600s per sync incrementale, e al primo avvio
    # fa una riconciliazione retroattiva su 90 giorni di storico.
    try:
        from core.kraken_reconciler import KrakenReconciler
        from core.database_manager import db_manager as _db_mgr
        kraken_reconciler = KrakenReconciler(
            performer=performer,
            db_manager=_db_mgr,
            alerts=alerts,
            trade_manager=trade_manager,
        )
        # Riconciliazione retroattiva su 90 giorni (solo al primo avvio).
        # Operazione costosa ma una tantum: aggiorna i pnl_netto_usd di tutti i
        # trade storici col valore vero da Kraken ledger.
        try:
            _stats_recon = kraken_reconciler.riconcilia_full(giorni=90)
            logger.info(
                f"♻️ [RECONCILER FULL] Avvio: ledger={_stats_recon['ledger_voci']} "
                f"trades={_stats_recon['trades_kraken']} "
                f"aggiornati={_stats_recon['trades_aggiornati']} "
                f"discrepanze={_stats_recon['discrepanze']} "
                f"orfani={_stats_recon['orfani_kraken']}"
            )
            # Dopo riconciliazione, ricarica trade_manager.storico_trades dal DB
            # (è già fatto da _incrocia_con_db ma per sicurezza)
            try:
                trade_manager.storico_trades = _db_mgr.get_storico()
            except Exception:
                pass
        except Exception as _e_full:
            _err.capture(_e_full, "main", {"module": "BotLA"})
            logger.warning(f"⚠️ Riconciliazione retroattiva fallita (non bloccante): {_e_full}")
    except Exception as _e_rec:
        _err.capture(_e_rec, "main", {"module": "BotLA"})
        logger.warning(f"⚠️ KrakenReconciler non disponibile: {_e_rec}")
        kraken_reconciler = None
    # ──────────────────────────────────────────────────────────────────────────
    
    macro = MacroSentiment()

    # ── CHIMERA v4: moduli avanzati ──────────────────────────────────────────
    night_review   = NightReview(brain, fe, trade_manager, alerts)
    asset_rotation = AssetRotation(al_config.ASSET_PRINCIPALI, fe, alerts)
    reporter       = AdvancedReporter(brain, trade_manager, fe, macro, alerts)
    auditor        = ChimeraAuditor(brain, alerts)
    # ── CHIMERA ML: modulo di apprendimento incrementale ─────────────────────
    chimera_ml = ChimeraML()
    stato_ml = chimera_ml.stato()
    if stato_ml["attivo"]:
        logger.info(f"🤖 ChimeraML ATTIVO — modello caricato, {stato_ml['trade_disponibili']} trade nel dataset.")
    else:
        logger.info(
            f"🤖 ChimeraML in ATTESA — serve {stato_ml['trade_per_train']} trade reali "
            f"(disponibili: {stato_ml['trade_disponibili']}). Usa solo Gemini per ora."
        )
    # Collega ChimeraML al TradeManager per ricevere notifiche di chiusura automaticamente
    trade_manager.chimera_ml = chimera_ml
    # ─────────────────────────────────────────────────────────────────────────

    # ── STRATEGY ENGINE: analisi deterministica pre-Gemini ───────────────────
    strategy_engine = StrategyEngine(engine)
    # ─────────────────────────────────────────────────────────────────────────

    # ── HEIKIN ASHI STRATEGY: BTC SPOT daily ─────────────────────────────────
    ha_strategy = HeikinAshiStrategy(engine)
    trade_manager.ha_strategy = ha_strategy  # stesso pattern di chimera_ml
    _ha_ultimo_check = 0.0  # guard: esegue analyze una sola volta per notte

    # HeikinAshi Trend: margine 1H in trend
    ha_trend_strategy = HeikinAshiTrendStrategy(engine, ha_strategy)
    # HeikinAshi Contro Trend: margine 1H contro trend su max/min giornata
    ha_contro_strategy = HeikinAshiControTrendStrategy(engine, ha_strategy)
    # Condivide la cache 1H dalla trend per evitare doppia chiamata a Kraken
    ha_strategy._ha_trend_ref = ha_trend_strategy
    logger.info(
        "🕯️ HeikinAshi: SPOT daily + Trend 1H + Contro Trend 1H inizializzate"
    )

    # ── ANALYTICS REPORTER: statistiche automatiche + fee reali Kraken ───────
    analytics_reporter = AnalyticsReporter(
        db_path=getattr(config_la, 'DB_PATH', 'chimera.db'),
        alerts=alerts,
        performer=performer
    )
    logger.info("📊 AnalyticsReporter inizializzato. Report auto: giornaliero 23:00 UTC, settimanale Dom 20:00 UTC.")
    # ─────────────────────────────────────────────────────────────────────────

    # ── SIGNAL STATE ENGINE: traiettoria temporale degli indicatori ──────────
    # Calcola derivate (delta 30s/120s, accelerazione, fase segnale, exhaustion)
    # e le aggiunge a ogni snapshot che va a Brain/Gemini.
    # Risolve: timing sbagliato, voto 9 peggiore del 7, short aperti contro flusso.
    signal_state = SignalStateEngine()
    engine.signal_state = signal_state   # Engine chiama signal_state.aggiorna() internamente
    logger.info("⚙️ SignalStateEngine inizializzato.")
    # ─────────────────────────────────────────────────────────────────────────

    # ── KRAKEN INTEGRATION: discovery mercati, screener, ordini Telegram ─────
    # Riusa l'exchange di PerformerLA (no connessioni doppie a Kraken).
    # Watchlist dinamica persistente in chimera_watchlist.json.
    # Se vuota, il get_watchlist() fa fallback su al_config.ASSET_PRINCIPALI.
    kraken_int = KrakenIntegration(performer=performer, alerts=alerts)
    logger.info("✅ KrakenIntegration inizializzato — watchlist dinamica attiva.")
    # Auto-popola con i top 20 asset Kraken per volume all'avvio.
    # Non richiede /add manuale — il bot scopre autonomamente i mercati più liquidi.
    # Refresh automatico ogni 6 ore (gestito da necessita_refresh_auto() nel loop).
    try:
        _auto_assets = kraken_int.auto_populate(top_n=7, min_volume_usd=5_000_000)  # FIX QPD: ridotto da 10 a 5
        logger.info(f"🔭 Auto-populate: {len(_auto_assets)} asset nel ciclo di analisi.")
    except Exception as e_auto:
        _err.capture(e_auto, "main", {"module": "BotLA"})
        logger.warning(f"⚠️ Auto-populate fallito all'avvio (uso lista statica): {e_auto}")
    # ─────────────────────────────────────────────────────────────────────────

    report_inviato_oggi   = False
    circuit_breaker_attivo = False
    pnl_24h_snapshot      = 0.0
    
    # --- CONFIGURAZIONE TIMING ---
    WAIT_PROTEZIONE = 2    
    ultimo_check_ia = {asset: 0.0 for asset in al_config.ASSET_PRINCIPALI}  # timestamp ultimo check
    _INTERVALLO_ANALISI = 90  # default — sovrascritto dinamicamente per asset/fase
    # Intervalli per fase: BREAKOUT = veloce, FORMAZIONE = normale, SILENZIO = lento
    _INTERVALLI_FASE = {
        'BREAKOUT':   25,   # BREAKOUT istituzionale — ogni 25s
        'ESTENSIONE': 60,   # Segnale in corso — ogni 60s
        'FORMAZIONE': 90,   # Default — ogni 90s
        'ESAURIMENTO': 90,  # Non urgente — ogni 90s
        'SILENZIO':   300,  # Mercato morto — ogni 300s (FIX QPD: riduce chiamate Gemini)
    }
    # Memorizza la fase precedente per calcolare l'intervallo corretto
    _fase_precedente = {asset: 'FORMAZIONE' for asset in al_config.ASSET_PRINCIPALI}
    ultimo_report_morning = 0
    ultima_sincro_globale = -1
    ultimo_audit = 0
    ultimo_report_stats = 0
    ultimo_log_stats = 0
    ultimo_engine_health = 0  # Health check Engine ogni 4 ore
    
    logger.info("⚡ CHIMERA v4.0 ATTIVO — Time-Stop | Circuit Breaker | NightReview | AssetRotation")

    # ── All'avvio: forza analisi su tutti gli asset al primo ciclo ──────────
    # Nessun thread separato — il primo ciclo del loop analizza tutto
    _forza_analisi_avvio = True
    logger.info("🔭 Primo ciclo: analisi forzata su tutti gli asset in arrivo...")
    # ────────────────────────────────────────────────────────────────────────

    _pnl_cache: dict = {}   # cache PnL — log posizioni solo se PnL cambia >0.3%
    try:
        while True:
            momento_ciclo = time.time()
            ora_now = datetime.now()
            minuto_attuale = ora_now.minute
            e_quarto_d_ora = minuto_attuale in [0, 15, 30, 45]

            # ── A. REPORT E TASK CRONOMETRATI ────────────────────────────────
            if ora_now.hour == 7 and ora_now.minute == 0 and (momento_ciclo - ultimo_report_morning > 120):
                _, macro_val = macro.get_macro_data()
                brain.genera_report_mattutino(macro_val)
                ultimo_report_morning = momento_ciclo

            # Reconciler Kraken: sync incrementale ogni 600s (gestione interna).
            # Aggiunto 2026-05-04 per allineare PnL DB↔Kraken in continuo.
            if kraken_reconciler is not None:
                try:
                    kraken_reconciler.esegui_se_necessario()
                except Exception as _e_recon:
                    _err.capture(_e_recon, "main", {"module": "BotLA"})
                    logger.debug(f"reconciler periodico: {_e_recon}")

            # Report serale avanzato (AdvancedReporter con loop chiuso)
            if ora_now.hour == 20 and not report_inviato_oggi:
                logger.info("📡 Generazione report serale avanzato CHIMERA v4...")
                try:
                    reporter.genera_report_serale()
                except Exception as e_rep:
                    _err.capture(e_rep, sys._getframe().f_code.co_name, {"module": "BotLA", "method": "main"})
                    logger.error(f"❌ Reporter avanzato fallito, fallback base: {e_rep}")
                    dati_report = trade_manager.genera_dati_report_giornaliero()
                    alerts.invia_report_serale(dati_report)
                report_inviato_oggi = True

            if ora_now.hour == 0:
                if report_inviato_oggi:
                    report_inviato_oggi = False
                # Reset circuit breaker a mezzanotte
                if circuit_breaker_attivo:
                    circuit_breaker_attivo = False
                    pnl_24h_snapshot = 0.0
                    logger.info("🔓 Circuit Breaker RESET — nuovo giorno operativo.")
                    alerts.invia_alert("🔓 *Circuit Breaker RESET* — bot operativo per la nuova sessione.")

                # ── HEIKIN ASHI: cerca entry BTC SPOT a chiusura candela daily ──
                # Il trailing SL delle posizioni aperte è gestito dal TradeManager
                # in gestisci_protezione_istituzionale() — qui facciamo solo l'entry.
                if (momento_ciclo - _ha_ultimo_check) > 3600:
                    _ha_ultimo_check = momento_ciclo
                    try:
                        if not trade_manager.is_posizione_aperta_su_kraken("XXBTZUSD"):
                            dati_btc = engine.get_full_market_data("XXBTZUSD")
                            dati_btc_dict = dati_btc[0] if isinstance(dati_btc, tuple) else (dati_btc or {})
                            segnale_ha = ha_strategy.analyze("XXBTZUSD", dati_btc_dict)
                            if segnale_ha and segnale_ha.score >= 60:
                                trade_manager.apri_posizione(
                                    asset           = "XXBTZUSD",
                                    direzione       = segnale_ha.signal,
                                    entry_price     = segnale_ha.entry_price,
                                    size            = segnale_ha.sizing,
                                    sl              = segnale_ha.sl,
                                    tp              = segnale_ha.tp,
                                    voto            = int(segnale_ha.score / 10),
                                    leverage        = 1,
                                    dati_mercato    = segnale_ha.components,
                                    tipo_operazione = "MULTIDAY",
                                    razionale       = segnale_ha.razionale,
                                )
                            else:
                                stato_ha = ha_strategy.get_stato()
                                logger.info(
                                    f"🕯️ HeikinAshi: nessun segnale | "
                                    f"BTC HA: {stato_ha.get('ultima_candela_ha',{}).get('colore','?')} | "
                                    f"Cambio: {stato_ha.get('cambio_colore_oggi', False)}"
                                )
                    except Exception as e_ha:
                        _err.capture(e_ha, sys._getframe().f_code.co_name, {"module": "BotLA", "method": "ha_entry"})
                        logger.error(f"❌ HeikinAshi entry check: {e_ha}")

            # Auto-Correzione (NightReview) ogni ora
            try:
                night_review.esegui_se_necessario()
            except Exception as e_nr:
                _err.capture(e_nr, sys._getframe().f_code.co_name, {"module": "BotLA", "method": "main"})
                logger.error(f"❌ NightReview error: {e_nr}")

            # ── KRAKEN AUTO-POPULATE: refresh watchlist ogni 6 ore ──────────────
            try:
                if kraken_int.necessita_refresh_auto():
                    logger.info("🔭 Refresh auto-populate watchlist Kraken...")
                    kraken_int.auto_populate(top_n=7, min_volume_usd=5_000_000)  # FIX QPD: ridotto da 10 a 5
            except Exception as e_ap:
                _err.capture(e_ap, "main", {"module": "BotLA"})
                logger.debug(f"Auto-populate refresh: {e_ap}")

            # ── ANALYTICS REPORTER: schedulazione automatica ─────────────────
            try:
                analytics_reporter.controlla_e_invia()
            except Exception as e_ar:
                _err.capture(e_ar, sys._getframe().f_code.co_name, {"module": "BotLA", "method": "main"})
                logger.debug(f"AnalyticsReporter: {e_ar}")

            # ── COMANDI TELEGRAM: /stats /report7 /report30 /ml ──────────────
            try:
                comandi = alerts.controlla_comandi()
                for cmd in comandi:
                    cmd = cmd.strip().lower()
                    if cmd in ('/stats', '/report'):
                        logger.info(f"📩 Comando {cmd} ricevuto — genero report 7 giorni")
                        analytics_reporter.invia_report_su_richiesta(giorni=7)
                    elif cmd == '/report30':
                        logger.info("📩 /report30 ricevuto — genero report 30 giorni")
                        analytics_reporter.invia_report_su_richiesta(giorni=30)
                    elif cmd == '/report7':
                        analytics_reporter.invia_report_su_richiesta(giorni=7)
                    elif cmd == '/report1':
                        analytics_reporter.invia_report_su_richiesta(giorni=1)
                    elif cmd == '/ml':
                        stato = chimera_ml.stato()
                        attivo = "✅ ATTIVO" if stato['attivo'] else "⏳ IN ATTESA"
                        msg_ml = (
                            f"🤖 *CHIMERA ML — STATO*\n"
                            f"• Status: {attivo}\n"
                            f"• Trade nel dataset: {stato['trade_disponibili']}\n"
                            f"• Trade per training: {stato['trade_per_train']}\n"
                            f"• Prossimo retrain tra: {stato.get('prossimo_retrain', '?')} trade\n"
                            f"• Feature usate: {stato['n_features']}\n"
                            f"• Modello salvato: {'✅' if stato['modello_salvato'] else '❌'}\n"
                            f"• Peso max sul voto: 40% (60% resta a Gemini)"
                        )
                        if not stato['attivo']:
                            msg_ml += f"\n• Motivo: {stato.get('motivo', '?')}"
                        alerts.invia_alert(msg_ml)
                    elif cmd == '/ha':
                        try:
                            stato_ha = ha_strategy.get_stato()
                            ultima = stato_ha.get('ultima_candela_ha', {})
                            top5  = stato_ha.get('top5_livelli_sr', [])
                            pos_btc = trade_manager.posizioni_aperte.get("XXBTZUSD")
                            if pos_btc and (pos_btc.get('chimera_snapshot') or {}).get('trailing_ha_daily'):
                                pos_str = (
                                    f"✅ {pos_btc.get('direzione','?')} aperta\n"
                                    f"  Entry: {float(pos_btc.get('p_entrata',0)):.0f}$ | "
                                    f"SL: {float(pos_btc.get('sl',0)):.0f}$"
                                )
                            else:
                                pos_str = "Nessuna posizione BTC SPOT attiva"
                            top5_str = "\n".join(
                                f"  {l['prezzo']:.0f}$ — {l['label']} (forza {l['forza']}/10)"
                                for l in top5
                            ) or "  —"
                            alerts.invia_alert(
                                f"🕯️ *HeikinAshi BTC SPOT — STATO*\n\n"
                                f"Ultima HA: {ultima.get('colore','?')} | "
                                f"Close {ultima.get('ha_close',0):.0f}$\n"
                                f"HA Low: {ultima.get('ha_low',0):.0f}$ | "
                                f"Real Low: {ultima.get('real_low',0):.0f}$\n"
                                f"Cambio colore: {'🔄 SÌ' if stato_ha.get('cambio_colore_oggi') else '➡️ No'}\n"
                                f"ATR daily: {stato_ha.get('atr_daily_usd',0):.0f}$ | "
                                f"S/R attivi: {stato_ha.get('n_livelli_sr',0)}\n\n"
                                f"*Top S/R:*\n{top5_str}\n\n"
                                f"*Posizione:* {pos_str}"
                            )
                        except Exception as e_ha_cmd:
                            _err.capture(e_ha_cmd, "main", {"module": "BotLA"})
                            alerts.invia_alert(f"❌ Errore /ha: {e_ha_cmd}")
                    elif cmd == '/help':
                        alerts.invia_alert(
                            "📋 *COMANDI DISPONIBILI*\n\n"
                            "*📊 Report & Stats*\n"
                            "/stats — Report ultimi 7 giorni\n"
                            "/report1 — Report oggi\n"
                            "/report7 — Report 7 giorni\n"
                            "/report30 — Report 30 giorni\n"
                            "/ml — Stato XGBoost\n\n"
                            "*🔧 Kraken & Mercati*\n"
                            "/kraken — Comandi Kraken (help)\n"
                            "/autoscan [N] — Auto-scopre top N asset\n"
                            "/markets — Top mercati per volume\n"
                            "/screener — Screener multi-criterio\n"
                            "/watchlist — Watchlist corrente\n"
                            "/add TICKER — Aggiunge asset\n"
                            "/remove TICKER — Rimuove asset\n"
                            "/ticker SYMBOL — Dati live\n"
                            "/balance — Saldo account\n"
                            "/positions — Posizioni aperte\n"
                            "/orders — Ordini aperti\n"
                            "/buy SYMBOL QTY [PREZZO] [LEVx]\n"
                            "/sell SYMBOL QTY [PREZZO] [LEVx]\n"
                            "/help — Questo messaggio"
                        )
                    else:
                        # ── COMANDI KRAKEN INTEGRATION ─────────────────────
                        # Gestisce: /markets /screener /watchlist /add /remove
                        #           /ticker /orderbook /balance /positions
                        #           /orders /buy /sell /cancel /cancelall /kraken
                        try:
                            _args = cmd.split()[1:] if ' ' in cmd else []
                            _base_cmd = cmd.split()[0]
                            kraken_int.gestisci_comando_telegram(_base_cmd, _args)
                        except Exception as e_ki:
                            _err.capture(e_ki, sys._getframe().f_code.co_name, {"module": "BotLA", "cmd": cmd})
                            logger.debug(f"KrakenIntegration cmd error: {e_ki}")
            except Exception as e_cmd:
                _err.capture(e_cmd, sys._getframe().f_code.co_name, {"module": "BotLA", "method": "main"})
                logger.debug(f"Gestione comandi Telegram: {e_cmd}")

            # ── B. CIRCUIT BREAKER ────────────────────────────────────────────
            # DISABILITATO SU RICHIESTA DELL'UTENTE
            # Se PnL 24h < -8%, blocca nuove entry e avvisa
            # try:
            #     dati_report_cb = trade_manager.genera_dati_report_giornaliero()
            #     pnl_24h_snapshot = float(dati_report_cb.get("pnl_totale_24h", 0))
            #     if pnl_24h_snapshot < -8.0 and not circuit_breaker_attivo:
            #         circuit_breaker_attivo = True
            #         logger.warning(f"🛑 CIRCUIT BREAKER ATTIVATO: PnL 24h = {pnl_24h_snapshot:.2f}%")
            #         alerts.invia_alert(
            #             f"🛑 *CIRCUIT BREAKER ATTIVATO*\n"
            #             f"PnL 24h: {pnl_24h_snapshot:.2f}% < -8%\n"
            #             f"Nessuna nuova entry fino a mezzanotte."
            #         )
            # except Exception:
            #     pass

            # ── C. SINCRONIZZAZIONE GLOBALE PERIODICA ────────────────────────
            if minuto_attuale % 10 == 0 and ultima_sincro_globale != minuto_attuale:
                logger.debug("🔄 Sincronizzazione globale anti-orfani...")
                trade_manager.sincronizza_con_exchange(engine)
                ultima_sincro_globale = minuto_attuale

            # ── D. CICLO ASSET PRINCIPALI ─────────────────────────────────────
            _, macro_val_loop = macro.get_macro_data()
            # FIX 4 — normalizza a inglese: Brain si aspetta BULLISH/BEARISH/NEUTRAL,
            # ma get_macro_data() può restituire "Neutrale" o None.
            _macro_raw = macro_val_loop or "NEUTRAL"
            _macro_map = {"Neutrale": "NEUTRAL", "Rialzista": "BULLISH", "Ribassista": "BEARISH"}
            macro_sentiment_loop = _macro_map.get(_macro_raw, _macro_raw.upper())

            try:
                posizioni_reali_globali = performer.get_open_positions_real()
            except Exception as e_pos:
                _err.capture(e_pos, sys._getframe().f_code.co_name, {"module": "BotLA", "method": "main"})
                logger.error(f"⚠️ Errore recupero posizioni globali: {e_pos}")
                posizioni_reali_globali = {}

            assets_da_riprovare = []

            def is_data_corrupted(res):
                if not res or res.get('close', 0) == 0:
                    return True
                if res.get('muro_supporto') == res.get('muro_resistenza'):
                    return True
                return False

            # --- PREPARAZIONE CICLO ---
            capitale_per_killswitch = trade_manager.get_balance_margin()
            
            # Se il capitale è <= 0, potrebbe essere un errore API temporaneo
            if capitale_per_killswitch <= 0 and config_la.KILLSWITCH_ENABLED:
                logger.warning("⚠️ Capitale non rilevato o <= 0. Attendo 10s per recupero API...")
                time.sleep(10)
                continue
            elif capitale_per_killswitch <= 0:
                logger.debug("⚠️ Capitale <= 0 ma Killswitch disabilitato. Procedo con l'analisi (TEST MODE).")
                capitale_per_killswitch = 1000.0 # Valore fittizio per calcoli sizing in test

            is_killed, kill_msg = trade_manager.check_killswitch(capitale_per_killswitch)
            if is_killed:
                logger.warning(kill_msg)
                # Se il killswitch è attivo, non apriamo nuove posizioni, ma continuiamo 
                # il loop per gestire quelle esistenti (Time-Stop, SL, TP).

            # Rotazione macro — usa dati del ciclo precedente (più completi)
            # _dati_cache_prev viene costruito alla fine del loop precedente
            try:
                _dati_rotation = getattr(main, '_dati_cache_prev', {}) or {}
                asset_rotation.valuta_e_agisci(trade_manager, _dati_rotation)
                if asset_rotation.is_in_usd:
                    logger.info("🔴 [ROTATION IN_USD] Entry crypto sospese — in attesa segnale bullish")
            except Exception as e_rot:
                _err.capture(e_rot, sys._getframe().f_code.co_name, {"module": "BotLA", "method": "is_data_corrupted"})
                logger.debug(f"AssetRotation: {e_rot}")

            _dati_cache = {}  # reset cache corrente — riempita durante il loop

            # ── Watchlist dinamica: usa chimera_watchlist.json se presente,
            # altrimenti fallback su al_config.ASSET_PRINCIPALI.
            # Aggiorna anche AssetRotation con la lista corrente.
            asset_rotation.asset_list = kraken_int.get_watchlist()
            # I dict di timing vengono aggiornati on-the-fly per nuovi asset.
            _assets_attivi = kraken_int.get_watchlist()

            # ═══════════════════════════════════════════════════════════════
            # 🔒 WHITELIST HARD — TEST BTC-ONLY (2026-05-09)
            # ───────────────────────────────────────────────────────────────
            # Filtro di sicurezza: anche se asset_list / chimera_watchlist.json
            # contengono altri asset, vengono ignorati durante questo test.
            # Per ripristinare watchlist completa: rimuovere questo blocco
            # (delimitato da "WHITELIST HARD" e "FINE WHITELIST HARD") e
            # ripristinare asset_list.py dal backup .BAK.
            # ═══════════════════════════════════════════════════════════════
            ASSETS_TEST_WHITELIST = {"XXBTZUSD"}
            _assets_attivi_pre = list(_assets_attivi)
            _assets_attivi = [a for a in _assets_attivi if a in ASSETS_TEST_WHITELIST]
            asset_rotation.asset_list = list(_assets_attivi)
            _filtrati = [a for a in _assets_attivi_pre if a not in ASSETS_TEST_WHITELIST]
            if _filtrati:
                logger.info(
                    f"🔒 [WHITELIST] Filtrati {len(_filtrati)} asset non whitelist: "
                    f"{', '.join(_filtrati)}. Attivi: {list(_assets_attivi)}"
                )
            # ═══════════════════════════════════════════════════════════════
            # FINE WHITELIST HARD
            # ═══════════════════════════════════════════════════════════════

            for asset in _assets_attivi:
                # Inizializza timing per asset aggiunti dinamicamente
                if asset not in ultimo_check_ia:
                    ultimo_check_ia[asset] = 0.0
                if asset not in _fase_precedente:
                    _fase_precedente[asset] = 'FORMAZIONE'
                dati_mercato_chimera = None  # reset per ogni asset — evita UnboundLocalError
                try:
                    pos_real_data = next(
                        (v for k, v in posizioni_reali_globali.items()
                         if performer._normalize_ticker(v.get('pair')) == performer._normalize_ticker(asset)),
                        None
                    )

                    is_aperta = trade_manager.sincronizza_e_ripara(
                        asset, engine=engine, dati_kraken_esterni=pos_real_data
                    )
                    
                    if is_aperta:
                        risultato_raw = engine.get_full_market_data(asset)
                        dati_freschi  = risultato_raw[0] if isinstance(risultato_raw, tuple) else risultato_raw
                        
                        prezzo_corrente = dati_freschi.get('close') if dati_freschi else performer.get_current_price(asset)
                        if prezzo_corrente:
                            atr_corrente  = dati_freschi.get('atr', 0) if dati_freschi else 0
                            direzione_pos = trade_manager.posizioni_aperte.get(asset, {}).get('direzione', 'LONG')

                            # Trigger Fase Due Chimera
                            if dati_freschi and dati_freschi.get('price_velocity') is not None:
                                fase_due_attiva, motivo, tp_esteso = brain.analizza_fase_due_chimera(
                                    asset, dati_freschi, direzione_pos
                                )
                                if fase_due_attiva:
                                    trade_manager.rimuovi_tp_fase_due(asset, motivo)

                            # Protezione istituzionale standard
                            trade_manager.gestisci_protezione_istituzionale(
                                asset=asset,
                                prezzo_attuale=prezzo_corrente,
                                atr_attuale=atr_corrente,
                                dati_mercato=dati_freschi
                            )

                            # ── PUNTO 2: MONITORAGGIO TESI ISTITUZIONALE ──────
                            # DISABILITATO: La logica di REVERSE/CLOSE anticipato causava troppi falsi positivi
                            # e svuotava il conto (whipsawing). Ora ci si affida solo a SL, TP e Time-Stop.
                            # ──────────────────────────────────────────────────

                            # ── CONTROLLO UPGRADE TRADE (SCALP -> SWING -> MULTIDAY) ──
                            if dati_freschi:
                                pos = trade_manager.posizioni_aperte.get(asset)
                                if pos:
                                    try:
                                        data_apertura = datetime.fromisoformat(pos["data_apertura"].replace("Z", ""))
                                        ore_aperto = (datetime.now() - data_apertura).total_seconds() / 3600
                                        tipo_op = str(pos.get("tipo_op", pos.get("tipo_operazione", "Swing"))).upper()
                                        
                                        # Soglia per controllare l'upgrade: 80% del tempo massimo
                                        soglia_upgrade = 2.4 if "SCALP" in tipo_op else (28.8 if "SWING" in tipo_op else 9999.0)
                                        
                                        if ore_aperto >= soglia_upgrade and not pos.get("upgrade_checked", False):
                                            logger.info(f"🔄 [{asset}] Trade aperto da {ore_aperto:.1f}h. Verifico upgrade stile operativo...")
                                            
                                            # FIX 3 — evita doppia chiamata Gemini.
                                            # calcola_voto() chiamava full_global_strategy() per intero,
                                            # sprecando budget API. Usiamo i dati freschi già presenti
                                            # e la funzione diretta che non riesegue tutto il pipeline.
                                            try:
                                                voto_attuale_dict = brain.full_global_strategy(
                                                    dati_engine=dati_freschi,
                                                    asset_name=asset,
                                                    macro_sentiment=macro_sentiment_loop
                                                )
                                            except Exception as e_voto:
                                                _err.capture(e_voto, sys._getframe().f_code.co_name, {"module": "BotLA", "method": "main"})
                                                logger.error(f"❌ Errore calcolo voto upgrade [{asset}]: {e_voto}")
                                                pos["upgrade_checked"] = True
                                                trade_manager.salva_posizioni()
                                                continue
                                            voto_attuale = voto_attuale_dict.get('voto', 0)
                                            dir_attuale = voto_attuale_dict.get('direzione', 'FLAT')
                                            dir_pos = pos.get('direzione', 'LONG')
                                            
                                            # Normalizziamo le direzioni
                                            dir_attuale_norm = "LONG" if dir_attuale in ["BUY", "LONG"] else ("SHORT" if dir_attuale in ["SELL", "SHORT"] else "FLAT")
                                            dir_pos_norm = "LONG" if dir_pos in ["BUY", "LONG"] else "SHORT"
                                            
                                            if voto_attuale >= 6 and dir_attuale_norm == dir_pos_norm:
                                                if "SCALP" in tipo_op:
                                                    nuovo_tipo = "SWING"
                                                    logger.info(f"📈 [{asset}] Setup ancora valido (Voto {voto_attuale}). Promuovo SCALPING -> SWING per evitare fees.")
                                                    pos["tipo_op"] = nuovo_tipo
                                                    pos["tipo_operazione"] = nuovo_tipo
                                                    pos["upgrade_checked"] = True
                                                    trade_manager.salva_posizioni()
                                                    if trade_manager.alerts:
                                                        trade_manager.alerts.invia_alert(f"📈 *UPGRADE TRADE {asset}*\nDa SCALPING a SWING\nIl setup è ancora valido (Voto {voto_attuale}), mantengo la posizione.")
                                                elif "SWING" in tipo_op:
                                                    nuovo_tipo = "MULTIDAY"
                                                    leva = int(pos.get("leverage", 1))
                                                    msg_costi = f"Attenzione ai costi di funding (Leva {leva}x)." if leva > 1 else "Posizione in SPOT."
                                                    logger.info(f"📈 [{asset}] Setup ancora valido (Voto {voto_attuale}). Promuovo SWING -> MULTIDAY. {msg_costi}")
                                                    pos["tipo_op"] = nuovo_tipo
                                                    pos["tipo_operazione"] = nuovo_tipo
                                                    pos["upgrade_checked"] = True
                                                    trade_manager.salva_posizioni()
                                                    if trade_manager.alerts:
                                                        trade_manager.alerts.invia_alert(f"📈 *UPGRADE TRADE {asset}*\nDa SWING a MULTIDAY\nIl setup è ancora valido (Voto {voto_attuale}). {msg_costi}")
                                            else:
                                                logger.info(f"📉 [{asset}] Condizioni non sufficienti per upgrade (Voto {voto_attuale}, Dir {dir_attuale}). Il Time-Stop farà il suo corso.")
                                                pos["upgrade_checked"] = True
                                                trade_manager.salva_posizioni()
                                    except Exception as e_upg:
                                        _err.capture(e_upg, sys._getframe().f_code.co_name, {"module": "BotLA", "method": "main"})
                                        logger.error(f"❌ Errore durante controllo upgrade trade [{asset}]: {e_upg}")

                            # ── TIME-STOP ISTITUZIONALE (CHIMERA v4) ──────────
                            if dati_freschi:
                                try:
                                    risultato_ts = trade_manager.gestisci_time_stop_istituzionale(
                                        asset=asset,
                                        dati_mercato=dati_freschi,
                                        engine=engine
                                    )
                                    if risultato_ts in ("CHIUSO_TOTALE", "RIDOTTO_50"):
                                        logger.debug(f"⏱️ Time-Stop [{asset}]: {risultato_ts} — passo al prossimo.")
                                        continue
                                except Exception as e_ts:
                                    _err.capture(e_ts, sys._getframe().f_code.co_name, {"module": "BotLA", "method": "main"})
                                    logger.error(f"❌ Time-Stop error [{asset}]: {e_ts}")

                    # ── ANALISI IA ───────────────────────────────────────────
                    # 1. Killswitch Check (Anti-Rovina)
                    if is_killed:
                        logger.warning(f"🛑 Killswitch attivo su {asset}. Salto nuove entry.")
                        continue

                    # 2. Cooldown Check (Anti-Churning) - REINSERITO MINIMO DI SICUREZZA
                    if asset in trade_manager.cooldown_assets:
                        if time.time() < trade_manager.cooldown_assets[asset]:
                            tempo_rimasto = trade_manager.cooldown_assets[asset] - time.time()
                            logger.info(f"⏱️ [{asset}] BLOCCATO: Cooldown attivo (ancora {tempo_rimasto:.0f}s)")
                            continue
                        else:
                            del trade_manager.cooldown_assets[asset]

                    # 3a. IN_USD blocca solo nuovi SPOT LONG (capital rotation).
                    # Le operazioni a MARGINE (leverage >= 2, long o short) proseguono
                    # indipendentemente dallo stato rotation — usano la leva Kraken,
                    # non il saldo spot. Il blocco SPOT LONG avviene al gate di esecuzione.
                    # NON bloccare qui l'analisi — altrimenti margine non opera mai.

                # 3. Check sospensione asset (WR < 45% o MACD ribassista strutturale)
                    if locals().get('dati_mercato_chimera') and not is_aperta:
                        try:
                            sospeso, motivo_sosp = asset_rotation.valuta_sospensione_asset(
                                asset, dati_mercato_chimera
                            )
                            if sospeso:
                                logger.info(f"⏸️ [{asset}] SOSPESO: {motivo_sosp}")
                                continue
                        except Exception as e_sosp:
                            _err.capture(e_sosp, sys._getframe().f_code.co_name, {"module": "BotLA", "method": "_night_cb"})
                            logger.debug(f"check sospensione [{asset}]: {e_sosp}")

                    # 3. Protezione Doppia Entry (CRITICO)
                    # FIX 2026-05-04: prima qui c'era `continue` che SALTAVA tutto
                    # il ciclo, incluso Gemini. Risultato: quando BTC era aperto, il
                    # bot smetteva COMPLETAMENTE di analizzare BTC. Conseguenze:
                    #  - Non vedeva movimenti grafico significativi (+2% / -2.5%)
                    #  - Non poteva valutare uscita anticipata o rinforzo
                    #  - Non poteva aprire posizione contraria su nuovo segnale
                    # La protezione DOPPIA ENTRY si basa già sul check successivo
                    # a riga ~1526 dove `salta_entry=True` impedisce l'apertura ma
                    # l'analisi viene completata e loggata.
                    # Adesso solo LOG dello stato + flag per non aprire, niente continue.
                    if is_aperta:
                        # Log stato posizione ghost (adottata da Kraken senza analisi)
                        pos_ghost = trade_manager.posizioni_aperte.get(asset) or trade_manager.posizioni_aperte.get(al_config.get_ticker(asset))
                        if pos_ghost:
                            p_e = float(pos_ghost.get('p_entrata', 0))
                            sl_g = float(pos_ghost.get('sl', 0))
                            tp_g = float(pos_ghost.get('tp', 0))
                            dir_g = pos_ghost.get('direzione', '?')
                            leva_g = pos_ghost.get('leverage', '?')
                            nota_g = pos_ghost.get('nota', '')
                            prezzo_live = float(dati_freschi.get('close', 0)) if dati_freschi else 0
                            if p_e > 0 and prezzo_live > 0:
                                pnl_live = ((prezzo_live - p_e) / p_e * 100) if dir_g in ('LONG','BUY') else ((p_e - prezzo_live) / p_e * 100)
                                is_adottata = 'KRAKEN' in str(nota_g).upper()
                                tag = '📋' if is_adottata else '📊'
                                label = 'ADOTTATA ' if is_adottata else ''
                                # Mostra solo se PnL è cambiato >0.3% rispetto all'ultimo log
                                _pnl_key = f"_pnl_last_{asset}"
                                _pnl_prev = _pnl_cache.get(_pnl_key, None)
                                _pnl_changed = _pnl_prev is None or abs(pnl_live - _pnl_prev) >= 0.3
                                if _pnl_changed:
                                    _pnl_cache[_pnl_key] = pnl_live
                                    logger.info(
                                        f"{tag} [{asset}] {label}PnL {pnl_live:+.2f}% | "
                                        f"{dir_g} {leva_g}x | "
                                        f"entry {p_e:.4f} → {prezzo_live:.4f} | "
                                        f"SL {sl_g:.4f} TP {tp_g:.4f}"
                                    )
                                else:
                                    logger.debug(
                                        f"{tag} [{asset}] {label}PnL {pnl_live:+.2f}% (invariato)"
                                    )

                                # ────────────────────────────────────────────
                                # VALUTAZIONE GEMINI POSIZIONE APERTA
                                # ────────────────────────────────────────────
                                # FIX 2026-05-04: prima il bot non chiedeva mai a Gemini
                                # cosa pensare delle posizioni aperte. Ora consulta Gemini
                                # quando il PnL si è mosso ≥0.5% dall'ultima valutazione.
                                # Gemini può raccomandare TIENI / CHIUDI_ANTICIPATO / TIGHTEN_SL.
                                _val_key = f"_pnl_lastval_{asset}"
                                _pnl_lastval = _pnl_cache.get(_val_key, None)
                                _delta_pnl = abs(pnl_live - _pnl_lastval) if _pnl_lastval is not None else 999
                                # Trigger: prima volta, oppure movimento PnL >= 0.5%
                                if _pnl_lastval is None or _delta_pnl >= 0.5:
                                    _pnl_cache[_val_key] = pnl_live
                                    try:
                                        ticker_v = al_config.get_ticker(asset)
                                        valutazione = brain.valuta_posizione_aperta(
                                            asset=ticker_v,
                                            dati_engine=dati_freschi or {},
                                            posizione=pos_ghost,
                                        )
                                        azione_v = valutazione.get('azione', 'TIENI')
                                        voto_v = valutazione.get('voto', 0)
                                        motivo_v = valutazione.get('motivo', '')
                                        dir_v = valutazione.get('direzione_gemini', 'FLAT')
                                        
                                        if azione_v == 'TIENI':
                                            logger.info(
                                                f"🤝 [{asset}] Valutazione Gemini posizione: TIENI "
                                                f"(voto={voto_v}, dir_gem={dir_v}) — {motivo_v}"
                                            )
                                        elif azione_v == 'CHIUDI_ANTICIPATO':
                                            logger.warning(
                                                f"🚪 [{asset}] Gemini raccomanda CHIUDI ANTICIPATO "
                                                f"(voto={voto_v}, dir_gem={dir_v} opposta a {dir_g}) — {motivo_v}"
                                            )
                                            # Cooldown anti-flip-flop: blocca riaperture per 30 min
                                            _cooldown_key = f"_cooldown_{asset}"
                                            _pnl_cache[_cooldown_key] = time.time() + 1800
                                            # Esegui chiusura
                                            try:
                                                ok = trade_manager._esegui_chiusura_totale(
                                                    asset=ticker_v,
                                                    prezzo_attuale=prezzo_live,
                                                    motivo=f"GEMINI_EXIT_voto{voto_v}",
                                                    dati_mercato=dati_freschi,
                                                )
                                                if ok:
                                                    logger.info(
                                                        f"✅ [{asset}] Chiusura anticipata da Gemini eseguita. "
                                                        f"Cooldown 30min attivo."
                                                    )
                                                    # Alert
                                                    if alerts:
                                                        try:
                                                            alerts.invia_alert(
                                                                f"🚪 *Chiusura Anticipata Gemini* {ticker_v}\n"
                                                                f"Direzione era: {dir_g}\n"
                                                                f"PnL chiusura: {pnl_live:+.2f}%\n"
                                                                f"Voto Gemini opposto: {voto_v} ({dir_v})\n"
                                                                f"Motivo: {motivo_v}"
                                                            )
                                                        except Exception:
                                                            pass
                                                    continue  # esci dal ciclo asset (posizione chiusa)
                                                else:
                                                    logger.warning(
                                                        f"⚠️ [{asset}] Chiusura anticipata Gemini fallita — riproverò."
                                                    )
                                            except Exception as e_close:
                                                _err.capture(e_close, "main", {"module": "BotLA"})
                                                logger.error(f"❌ [{asset}] Errore chiusura Gemini: {e_close}")
                                        elif azione_v == 'TIGHTEN_SL':
                                            logger.info(
                                                f"🛡️ [{asset}] Gemini raccomanda TIGHTEN_SL "
                                                f"(voto={voto_v}) — {motivo_v}"
                                            )
                                            try:
                                                trade_manager.tighten_sl_a_breakeven(
                                                    asset=ticker_v,
                                                    prezzo_attuale=prezzo_live,
                                                    motivo=f"GEMINI_TIGHTEN_voto{voto_v}",
                                                )
                                            except Exception as e_t:
                                                _err.capture(e_t, "main", {"module": "BotLA"})
                                                logger.error(f"❌ [{asset}] Errore tighten SL: {e_t}")
                                    except Exception as e_val:
                                        _err.capture(e_val, "main", {"module": "BotLA"})
                                        logger.debug(f"valuta_posizione_aperta {asset}: {e_val}")
                            else:
                                logger.info(f"🛡️ [{asset}] Posizione attiva — analizzo per monitor (no nuove entry)")
                        else:
                            logger.info(f"🛡️ [{asset}] Posizione attiva — analizzo per monitor (no nuove entry)")
                        # NB: NIENTE continue qui. L'analisi prosegue.
                        # Il blocco aperture nuove è gestito a riga ~1526 via salta_entry.

                    # 2. Circuit Breaker: blocca nuove entry ma non il monitoraggio
                    if circuit_breaker_attivo and is_aperta:
                        continue

                    trigger_sentinella = engine.check_sentinel(asset)
                    forza_questo_ciclo = _forza_analisi_avvio

                    # Se la sentinella ha triggerato, recupera i dettagli
                    # del movimento per passarli a Brain come contesto aggiuntivo
                    sentinel_data = engine.get_sentinel_data(asset) if trigger_sentinella else {}

                    # ── FILTRO ORE BASSA LIQUIDITÀ ────────────────────────────────
                    # 01:00-06:00 UTC: volumi bassi, spread alti, molti fake breakout.
                    # ECCEZIONE: se la sentinella rileva un movimento reale (≥0.3% o
                    # volume spike), lasciamo passare anche in orario bassa liquidità —
                    # un movimento esplosivo reale va catturato sempre.
                    # BUG FIX: il vecchio codice sovrascriveva salta_entry subito dopo,
                    # rendendo il filtro completamente inutile. Ora la logica è unificata.
                    _ora_utc = datetime.now(timezone.utc).hour
                    # Fascia oraria a bassa qualità (dati storici):
                    # 01-05 UTC: bassa liquidità asiatica — già filtrata
                    # 22-23 UTC: fine sessione americana, spread alti, fake moves
                    #   → ore 22 WR 27%, ore 23 WR 10% su storico (28 trade, -$5.32)
                    # Eccezione: sentinella bypassa sempre (movimento reale in corso)
                    _bassa_liquidita = (1 <= _ora_utc < 6) or (_ora_utc >= 22)

                    # is_primo_giro deve essere calcolato PRIMA di salta_entry
                    # perché salta_entry lo usa nel proprio calcolo
                    is_primo_giro = (ultimo_check_ia.get(asset, 0.0) == 0.0)

                    # FIX 2026-05-04: cooldown 30 min dopo chiusura anticipata Gemini.
                    # Evita pattern flip-flop: chiudo SHORT, prezzo continua a salire,
                    # apro LONG, prezzo gira, chiudo, riapro SHORT... perderei a ogni flip.
                    _cooldown_key = f"_cooldown_{asset}"
                    _cooldown_until = _pnl_cache.get(_cooldown_key, 0)
                    _in_cooldown = time.time() < _cooldown_until
                    if _in_cooldown:
                        _resta_min = (_cooldown_until - time.time()) / 60
                        logger.info(
                            f"🧊 [{asset}] In cooldown post-chiusura Gemini per altri "
                            f"{_resta_min:.1f} min. Skip nuove entry."
                        )

                    # Calcola salta_entry combinando TUTTI i motivi di blocco
                    salta_entry = (
                        (is_aperta and not forza_questo_ciclo)
                        or (_bassa_liquidita
                            and not trigger_sentinella
                            and not is_primo_giro
                            and not forza_questo_ciclo)
                        or _in_cooldown
                    )
                    
                    if _bassa_liquidita and not trigger_sentinella and not is_aperta:
                        logger.debug(
                            f"🌙 [{asset}] Ora UTC {_ora_utc}:xx — fascia bassa qualità, "
                            f"nuove entry bloccate (sentinella: {trigger_sentinella})."
                        )
                    elif _bassa_liquidita and trigger_sentinella:
                        logger.info(
                            f"⚡ [{asset}] Sentinella attiva in fascia bassa qualità "
                            f"(ora UTC {_ora_utc}:xx) — entry consentita."
                        )
                    # ─────────────────────────────────────────────────────────────

                    # Analisi con intervallo adattivo per fase del segnale
                    # BREAKOUT: ogni 25s — ESTENSIONE: 60s — FORMAZIONE/altro: 90s — SILENZIO: 120s
                    # Asset auto-scoperti (non nella lista statica): intervallo raddoppiato
                    # per evitare rate limit Kraken quando si monitorano 10-20 asset.
                    _fase_att = _fase_precedente.get(asset, 'FORMAZIONE')
                    _intervallo_att = _INTERVALLI_FASE.get(_fase_att, _INTERVALLO_ANALISI)
                    _is_auto_asset = asset not in al_config.ASSET_PRINCIPALI
                    if _is_auto_asset:
                        _intervallo_att = max(_intervallo_att * 2, 180)  # min 180s per auto-asset
                    _tempo_da_ultimo = time.time() - ultimo_check_ia.get(asset, 0)
                    if is_primo_giro or forza_questo_ciclo or trigger_sentinella or _tempo_da_ultimo >= _intervallo_att:
                        logger.debug(f"🔍 [CHIMERA v4] Analisi opportunità su {asset}...")
                        ultimo_check_ia[asset] = time.time()

                        res_raw = engine.get_full_market_data(asset)
                        res     = res_raw[0] if isinstance(res_raw, tuple) else res_raw
    
                        if is_data_corrupted(res):
                            logger.warning(f"⚠️ Dati corrotti o instabili per {asset}. Salto analisi per sicurezza.")
                            continue

                        prezzo_ref  = float(res.get('close', 0))
                        atr_sicuro  = float(res.get('atr', 0))
                        if atr_sicuro <= 0:
                            atr_sicuro = prezzo_ref * 0.002

                        muri = res.get('liquidity_pools', [])
                        if not muri or len(muri) == 0:
                            muri = [
                                {"price": prezzo_ref * 0.985, "type": "support",    "volume": 1.0},
                                {"price": prezzo_ref * 1.015, "type": "resistance", "volume": 1.0}
                            ]

                        dati_mercato_chimera = res.copy()
                        dati_mercato_chimera['close'] = prezzo_ref
                        dati_mercato_chimera['atr']   = atr_sicuro
                        # Cache dati per AssetRotation (evita doppie chiamate API)
                        _dati_cache[asset] = dati_mercato_chimera
                        dati_mercato_chimera['microstruttura_hft'] = {
                            'muri_liquidita': muri,
                            'aggressivita': res.get('aggressivita', 0)
                        }
                        
                        # Dati sentinella a Brain
                        if sentinel_data:
                            dati_mercato_chimera['sentinel_trigger']   = True
                            dati_mercato_chimera['sentinel_motivo']    = sentinel_data.get('trigger_motivo', '')
                            dati_mercato_chimera['sentinel_direzione'] = sentinel_data.get('direzione_mov', '')
                            dati_mercato_chimera['sentinel_chg10s']    = sentinel_data.get('chg10s', 0.0)
                            dati_mercato_chimera['sentinel_chg30s']    = sentinel_data.get('chg30s', 0.0)
                            dati_mercato_chimera['sentinel_cvd30s']    = sentinel_data.get('cvd_30s', 0.0)
                        else:
                            dati_mercato_chimera['sentinel_trigger'] = False
                        
                        # Recupero dati multi-timeframe — TF adattivi in base alla fase
                        # SCALPING e BREAKOUT: aggiunge 5m per microstruttura veloce
                        # SWING e MOMENTUM: aggiunge 1d per contesto macro
                        # Default: 15m + 1h + 4h
                        try:
                            _fase_pre = str(res.get('entry_phase', 'FORMAZIONE') or 'FORMAZIONE')
                            _sub_pre  = str(res.get('phase_subtype', '') or '')
                            _is_fast  = _fase_pre == 'BREAKOUT' or _sub_pre == 'ISTITUZIONALE'
                            if _is_fast:
                                # Aggiunge 5m per catturare microstruttura veloce
                                _tfs = ['5m', '15m', '1h', '4h']
                            else:
                                # Standard + 1d per contesto macro
                                _tfs = ['15m', '1h', '4h', '1d']
                            mtf_data = engine.get_market_data_multi_tf(asset, timeframes=_tfs)
                            dati_mercato_chimera['multi_tf'] = mtf_data

                            # ── TREND_SCORE aggregato (-4 → +4) ──────────────
                            # +1 per ogni TF con trend UP, -1 per ogni TF DOWN.
                            # Pesi: HA_daily=3 (cambio su S/R vale doppio), 1d=2, 4h=1.5, 1h=1, 15m=0.5
                            _tf_pesi = {'1d': 2.0, '4h': 1.5, '1h': 1.0, '15m': 0.5, '5m': 0.3}
                            _ts = 0.0
                            _ts_max = sum(_tf_pesi.get(tf, 0.5) for tf in mtf_data.keys())
                            for _tf_n, _tf_d in mtf_data.items():
                                _td = str(_tf_d.get('trend_dir', '') or '')
                                _w  = _tf_pesi.get(_tf_n, 0.5)
                                if _td == 'UP':   _ts += _w
                                elif _td == 'DOWN': _ts -= _w
                            # HA daily: peso 3 (il più importante per cambio trend)
                            # Cambio confermato su S/R vale doppio
                            _ha_bias   = str(dati_mercato_chimera.get('ha_daily_bias', 'NEUTRO') or 'NEUTRO')
                            _ha_cambio = bool(dati_mercato_chimera.get('ha_daily_cambio', False))
                            _ha_su_sr  = bool(dati_mercato_chimera.get('ha_daily_su_sr', False))
                            _ha_streak = int(dati_mercato_chimera.get('ha_daily_streak', 0) or 0)
                            _ha_w = 3.0 * (2.0 if (_ha_cambio and _ha_su_sr) else 1.0)
                            _ts_max += _ha_w
                            if _ha_bias == 'LONG':  _ts += _ha_w
                            elif _ha_bias == 'SHORT': _ts -= _ha_w
                            # Normalizza a -4/+4
                            _ts_norm = round(_ts / max(_ts_max, 1) * 4, 2) if _ts_max > 0 else 0.0
                            dati_mercato_chimera['trend_score'] = _ts_norm
                            # Log HA daily se rilevante
                            if _ha_cambio:
                                _sr_tag = f" su S/R {dati_mercato_chimera.get('ha_daily_sr_level',0):.4f}" if _ha_su_sr else ""
                                logger.info(
                                    f"🕯️ [{asset}] HA Daily CAMBIO TREND → {_ha_bias}"
                                    f"{_sr_tag} (streak={_ha_streak}) "
                                    f"trend_score={_ts_norm:+.1f}"
                                )
                            logger.debug(
                                f"📊 [{asset}] Multi-TF: {list(mtf_data.keys())} "
                                f"trend_score={_ts_norm:+.1f} (fase={_fase_pre})"
                            )
                        except Exception as e:
                            _err.capture(e, sys._getframe().f_code.co_name, {"module": "BotLA", "method": "main"})
                            logger.error(f"❌ Errore recupero dati multi-timeframe per {asset}: {e}")
                            dati_mercato_chimera['multi_tf'] = {}
                            dati_mercato_chimera['trend_score'] = 0.0
                        
                        cvd_log = dati_mercato_chimera.get('cvd_istantaneo', 0.0)

                        # Aggiorna fase precedente per calcolo intervallo prossimo ciclo
                        _fase_now = str(res.get('entry_phase', 'FORMAZIONE') or 'FORMAZIONE')
                        _sub_now  = str(res.get('phase_subtype', '') or '')
                        # BREAKOUT REALE/ESPLOSIVO → intervallo veloce
                        # BREAKOUT FAKE → non urgente, mantieni intervallo normale
                        if _fase_now == 'BREAKOUT' and _sub_now == 'FAKE':
                            _fase_precedente[asset] = 'FORMAZIONE'
                        else:
                            _fase_precedente[asset] = _fase_now
                        if _fase_now != _fase_att and _fase_now != 'FORMAZIONE':
                            logger.debug(
                                f"⏱️ [{asset}] Intervallo analisi: "
                                f"{_INTERVALLI_FASE.get(_fase_now, _INTERVALLO_ANALISI)}s "
                                f"(fase={_fase_now})"
                            )

                        # --- FILTRO TECNICO PRE-IA (RIDUZIONE COSTI) ---
                        volatilta_perc = (atr_sicuro / prezzo_ref) * 100 if prezzo_ref > 0 else 0
                        volume_recente = res.get('volume', 0)
                        vpin = res.get('vpin', 0)
                        
                        # Filtro Toxic Flow (VPIN)
                        # FIX 2: VPIN > 0.90 non blocca più l'analisi qui.
                        # La logica is_trap in Brain (soglia coerente 0.90) già gestisce questo.
                        # Bloccare qui impedisce a Brain di usare il VPIN alto come conferma
                        # di breakout reale (istituzionale). Ora solo log di warning.
                        if vpin > 0.90:
                            logger.warning(f"☢️ TOXIC FLOW su {asset}: VPIN {vpin:.4f} — Brain decide se entrare o restare FLAT.")

                        if volatilta_perc < 0.05 and not trigger_sentinella and not forza_questo_ciclo:
                            logger.debug(f"💤 [{asset}] Volatilità {volatilta_perc:.3f}% molto bassa — passa a Gemini con contesto bassa volatilità.")

                        # ── FILTRO PRE-GEMINI: confluenza minima sui dati tecnici ──────
                        # Calcola un pre-score sui dati grezzi prima di chiamare Gemini.
                        # Se il mercato è strutturalmente piatto su tutti i fronti,
                        # Gemini darà comunque FLAT — evitiamo la chiamata API.
                        _cvd    = abs(float(res.get('cvd_istantaneo', 0)))
                        _vel    = abs(float(res.get('price_velocity', 0)))
                        _hurst  = float(res.get('hurst_exponent', 0.5))
                        _ker    = float(res.get('kaufman_efficiency', 0.5))
                        _z      = abs(float(res.get('z_score', 0)))
                        _vpin_f = float(res.get('vpin', 0.5))

                        # Punteggio grezzo: ogni condizione vale 1 punto
                        _pre_score = sum([
                            _cvd > 50000,           # CVD significativo
                            _vel > 0.0003,           # velocity presente
                            _hurst > 0.55 or _hurst < 0.45,  # regime definito
                            _ker > 0.35,             # efficienza del movimento
                            _z > 1.0,                # prezzo fuori dal VWAP
                            _vpin_f > 0.3,           # qualche flusso
                        ])

                        if _pre_score < 3 and not trigger_sentinella and not forza_questo_ciclo:
                            logger.debug(
                                f"🔕 [{asset}] Pre-score {_pre_score}/6 basso — "
                                f"mercato piatto, passo a Gemini con contesto."
                            )
                            # Non blocca — Gemini decide con penalità voto applicata da Brain
                        # ─────────────────────────────────────────────────────────────

                        # ── FILTRO MACRO + ZSCORE + VPIN COMBINATO ──────────────────
                        # Se macro è BEARISH e tutti gli indicatori puntano a debolezza,
                        # metti l'asset in cooldown 2h invece di sprecare chiamate Gemini.
                        _z_signed = float(res.get('z_score', 0))
                        if (macro_sentiment_loop == "BEARISH"
                                and _z_signed < -1.0
                                and _vpin_f > 0.65
                                and not trigger_sentinella):
                            logger.info(
                                f"⚠️ [{asset}] Macro BEARISH + Z-Score {_z_signed:.2f} + "
                                f"VPIN {_vpin_f:.2f} — contesto passato a Gemini come penalità."
                            )
                            # Non blocca — Brain applica la penalità nel voto
                        # ─────────────────────────────────────────────────────────────

                        # ══════════════════════════════════════════════════════════
                        # VETO GLOBALE: SILENZIO + VPIN BASSO
                        # A monte di TUTTI i path: Strategy, Gemini, GEMINI_ONLY.
                        # Strategy Engine ha già questi veti ma coprono solo il path
                        # STRATEGY. Il path GEMINI_ONLY li bypassa → li mettiamo qui.
                        # ECCEZIONE: trigger_sentinella bypassa (movimento reale).
                        # ══════════════════════════════════════════════════════════
                        _phase_veto_global = str(dati_mercato_chimera.get('entry_phase', '') or '')
                        _vpin_veto_global  = float(dati_mercato_chimera.get('vpin', 1.0) or 1.0)

                        if _phase_veto_global == 'SILENZIO' and not trigger_sentinella:
                            logger.debug(f"🔇 [{asset}] Veto SILENZIO globale — tutti i path bloccati")
                            continue

                        if 0 < _vpin_veto_global < 0.30 and not trigger_sentinella:
                            logger.debug(f"🚫 [{asset}] Veto VPIN {_vpin_veto_global:.2f} < 0.30 — tutti i path bloccati")
                            continue
                        # ──────────────────────────────────────────────────────────

                        # ══════════════════════════════════════════════════════════
                        # STRATEGY ENGINE: Pre-analisi deterministica
                        # ══════════════════════════════════════════════════════════
                        try:
                            strategy_result = strategy_engine.analyze(asset, dati_mercato_chimera)
                        except Exception as e:
                            _err.capture(e, sys._getframe().f_code.co_name, {"module": "BotLA", "method": "main"})
                            logger.error(f"❌ Strategy Engine fallito su {asset}: {e}")
                            strategy_result = None
                        
                        decision_source = "UNKNOWN"
                        voto_base_pre_ml = 0
                        decision = None
                        
                        # ══════════════════════════════════════════════════════════
                        # ROUTING DECISIONALE: Strategy → Gemini → XGBoost
                        # ══════════════════════════════════════════════════════════

                        # ── SOGLIA SCORE ADATTIVA PER ORA ─────────────────────────
                        # Dati DB: 18:00-23:00 → STRATEGY WR 19% -6.54$
                        #          04:00-12:00 → STRATEGY WR ~55% positivo
                        # In fascia serale il volume è basso, i movimenti rumorosi.
                        # Alziamo la soglia STRONG da 75 a 82 nelle ore 18-23:
                        # solo segnali molto forti passano, gli altri vanno a Gemini.
                        _ora_corrente = datetime.now().hour
                        _fascia_sera = 18 <= _ora_corrente <= 23
                        _soglia_strong = 82 if _fascia_sera else 75
                        _soglia_medium = 65 if _fascia_sera else 60
                        if _fascia_sera and strategy_result and 0 < strategy_result.score < 82:
                            logger.debug(
                                f"🌙 [{asset}] Fascia serale: soglia alzata a 82 "
                                f"(score {strategy_result.score:.0f} → Gemini)"
                            )
                        # ──────────────────────────────────────────────────────────

                        if strategy_result and strategy_result.score >= _soglia_strong:
                            # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
                            # STRONG SIGNAL → PRE-VALIDA CON FILTRI ANALISI PROFONDA
                            # WR Strategy era 19% perché bypassa tutti i filtri
                            # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
                            _sig  = str(strategy_result.signal).upper()
                            _snap = dati_mercato_chimera or {}
                            _tema_s  = str(_snap.get('ema_trend_dominante','') or '')
                            _hurst_s = float(_snap.get('hurst_exponent',0.5) or 0.5)
                            _cvd_s   = float(_snap.get('cvd_istantaneo',0) or 0)
                            _phase_s = str(_snap.get('entry_phase','') or '')
                            _vel_s   = float(_snap.get('price_velocity',0) or 0)
                            _mtf_s   = _snap.get('multi_tf',{})
                            _kauf_15 = float((_mtf_s.get('15m',{}) or {}).get('kaufman',1) or 1)
                            _kauf_1h = float((_mtf_s.get('1h', {}) or {}).get('kaufman',1) or 1)

                            _strategy_veto = None
                            _strategy_context = []  # contesti non bloccanti per Gemini

                            # Contesto 1: LONG con EMA ribassista — NON è un veto.
                            # Gemini riceve il warning e decide se ci sono conferme
                            # sufficienti (CVD, whale delta, S/R, volume spike...).
                            # Se Gemini dice LONG voto≥6 → il trade viene eseguito.
                            if _sig in ('LONG','BUY') and _tema_s == 'RIBASSISTA':
                                _strategy_context.append(
                                    "⚠️ EMA RIBASSISTA: LONG contro trend dominante. "
                                    "Cerca conferme: CVD divergente, supporto testato 3x, "
                                    "whale delta positivo, volume spike. Altrimenti monitora."
                                )
                                logger.info(
                                    f"⚠️ [{asset}] EMA RIBASSISTA — contesto per Gemini "
                                    f"(non bloccante). Gemini verifica le conferme."
                                )

                            # Veto 2: entry_phase FORMAZIONE (tutte le lezioni lo indicano)
                            elif _phase_s == 'FORMAZIONE':
                                _strategy_veto = f"fase FORMAZIONE — attendere ESTENSIONE"

                            # Hurst > 0.65 non è più un veto — è contesto.
                            # Un Hurst alto durante un BREAKOUT istituzionale
                            # indica trend forte e persistente, non inversione.
                            # Gemini riceve il valore e lo valuta nel contesto.
                            # (rimosso veto Hurst)

                            # Veto 4: CVD fortemente contro — soglia adattiva per asset
                            elif _sig in ('SHORT','SELL') and _cvd_s > _CVD_SOGLIE.get(asset, _CVD_SOGLIA_DEFAULT) * 1.5:
                                _strategy_veto = f"CVD {_cvd_s:,.0f} bullish vs SHORT (soglia {_CVD_SOGLIE.get(asset, _CVD_SOGLIA_DEFAULT)*1.5:,.0f})"
                            elif _sig in ('LONG','BUY') and _cvd_s < -_CVD_SOGLIE.get(asset, _CVD_SOGLIA_DEFAULT) * 1.5:
                                _strategy_veto = f"CVD {_cvd_s:,.0f} bearish vs LONG (soglia {_CVD_SOGLIE.get(asset, _CVD_SOGLIA_DEFAULT)*1.5:,.0f})"

                            # Veto 5: Kaufman troppo basso (mercato rumoroso)
                            elif (_kauf_15 + _kauf_1h) / 2 < 0.15 and (_kauf_15 + _kauf_1h) > 0:
                                _strategy_veto = f"Kaufman {(_kauf_15+_kauf_1h)/2:.2f} — nessun momentum"

                            # Inietta contesti non bloccanti nel snapshot per Gemini
                            if _strategy_context and dati_mercato_chimera:
                                _ctx_existing = dati_mercato_chimera.get('strategy_context', '')
                                dati_mercato_chimera['strategy_context'] = (
                                    (_ctx_existing + ' | ' if _ctx_existing else '') +
                                    ' | '.join(_strategy_context)
                                )

                            if _strategy_veto:
                                # Declassa a MEDIUM per validazione Gemini invece di bloccare
                                logger.info(
                                    f"⚠️ [{asset}] STRATEGY STRONG declassato → GEMINI: {_strategy_veto}"
                                )
                                history = fe.get_recent_summary()
                                decision = brain.full_global_strategy(
                                    dati_engine=dati_mercato_chimera,
                                    asset_name=asset,
                                    macro_sentiment=macro_sentiment_loop,
                                    performance_history=history
                                )
                                if decision and decision.get('direzione') not in ('FLAT', None):
                                    decision_source = "STRATEGY+GEMINI_VALIDATED"
                                    voto_base_pre_ml = decision.get('voto', 0)
                                else:
                                    decision_source = "STRATEGY_VETOED"
                                    _raz_sv = str(decision.get('razionale', '') or '') if decision else ''
                                    _voto_sv = decision.get('voto', 0) if decision else 0
                                    logger.info(f"⏭️ [{asset}] Strategy vetoed + Gemini FLAT (voto={_voto_sv}) — skip")
                                    if _raz_sv:
                                        logger.info(f"   📋 Razionale Gemini: {_raz_sv[:150]}")
                            else:
                                # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
                                # FIX 2026-05-07: VALIDAZIONE GEMINI OBBLIGATORIA
                                # 
                                # Strategy STRONG NON apre più da solo. Anche quando
                                # i 5 mini-veti pre-Gemini non scattano, Gemini deve
                                # confermare la direzione.
                                # 
                                # Motivo: i dati storici (859 trade) mostrano:
                                #   STRATEGY STRONG bypass Gemini → 27% WR, -10$ PnL
                                #   GEMINI puro                   → 42% WR, -2$ PnL
                                # Il problema sono i top di esaurimento (es. ZEC 6/5
                                # aperto sul doji a 587 dopo trend rialzista, -3.2%
                                # in 18 minuti). WallBreakout vede "muro rotto" ma
                                # non vede contesto (ciclo 95% recupero, doji 15m,
                                # divergenza prezzo/CVD).
                                # 
                                # Gemini ha potere di VETO + override direzione.
                                # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
                                
                                history = fe.get_recent_summary()
                                decision_gemini = brain.full_global_strategy(
                                    dati_engine=dati_mercato_chimera,
                                    asset_name=asset,
                                    macro_sentiment=macro_sentiment_loop,
                                    performance_history=history
                                )
                                
                                _strategy_signal_up = str(strategy_result.signal).upper()
                                _gemini_dir = str((decision_gemini or {}).get('direzione', 'FLAT')).upper()
                                _gemini_voto = (decision_gemini or {}).get('voto', 0)
                                
                                if not decision_gemini or _gemini_dir == 'FLAT':
                                    # ─────────────────────────────────────────
                                    # CASO 1: Gemini FLAT → STRATEGY VETOED
                                    # Skip totale del trade nonostante score alto.
                                    # ─────────────────────────────────────────
                                    decision_source = "STRATEGY_VETOED_BY_GEMINI"
                                    decision = None
                                    _raz_g = str((decision_gemini or {}).get('razionale', '') or '')[:250]
                                    logger.info(
                                        f"🛑 [{asset}] STRATEGY STRONG bloccata da Gemini FLAT "
                                        f"(strategy {strategy_result.score:.0f} {_strategy_signal_up}, "
                                        f"gemini voto {_gemini_voto})"
                                    )
                                    if _raz_g:
                                        logger.info(f"   📋 Veto Gemini: {_raz_g}")
                                
                                elif _gemini_dir != _strategy_signal_up:
                                    # ─────────────────────────────────────────
                                    # CASO 2: Direzioni discordanti → priorità GEMINI
                                    # Strategy diceva LONG ma Gemini dice SHORT (o viceversa).
                                    # Si segue Gemini, Strategy ignorato.
                                    # ─────────────────────────────────────────
                                    decision = decision_gemini
                                    decision['razionale'] = (
                                        str(decision.get('razionale', '') or '') +
                                        f" | ⚠️ Strategy {strategy_result.strategy_name} "
                                        f"suggeriva {_strategy_signal_up} "
                                        f"(score {strategy_result.score:.0f}), Gemini override"
                                    )
                                    decision_source = "STRATEGY_OVERRIDDEN_BY_GEMINI"
                                    voto_base_pre_ml = _gemini_voto
                                    logger.warning(
                                        f"⚠️ [{asset}] MISMATCH STRONG: Strategy={_strategy_signal_up} "
                                        f"({strategy_result.score:.0f}) vs Gemini={_gemini_dir} "
                                        f"(voto {_gemini_voto}). Using Gemini."
                                    )
                                
                                else:
                                    # ─────────────────────────────────────────
                                    # CASO 3: Strategy + Gemini CONCORDI → STRATEGY+GEMINI_CONFIRMED
                                    # Voto = max(strategy_voto, gemini_voto). 
                                    # SL/TP dalla sinergia (muri/OB/FVG).
                                    # ─────────────────────────────────────────
                                    _snap_st  = dati_mercato_chimera or {}
                                    _phase_st = str(_snap_st.get('entry_phase', '') or '')
                                    _hurst_st = float(_snap_st.get('hurst_exponent', 0.5) or 0.5)
                                    _regime_st = str(_snap_st.get('market_regime', '') or '')
                                    _ts_st     = float(_snap_st.get('trend_score', 0) or 0)
                                    _struttura = str(_snap_st.get('struttura_h1', '') or '')

                                    if _regime_st == 'MEAN_REVERSION' or _hurst_st < 0.45:
                                        _stile_inferred = 'SCALPING'
                                    elif _phase_st in ('BREAKOUT',) and _hurst_st > 0.55:
                                        _stile_inferred = 'MOMENTUM'
                                    elif (abs(_ts_st) >= 1.5
                                          and _struttura in ('UPTREND', 'DOWNTREND')
                                          and _hurst_st > 0.55):
                                        _stile_inferred = 'SWING'
                                    else:
                                        _stile_inferred = 'INTRADAY'

                                    logger.info(
                                        f"🎨 [{asset}] Stile inferito: {_stile_inferred} "
                                        f"(phase={_phase_st} hurst={_hurst_st:.2f} "
                                        f"regime={_regime_st} ts={_ts_st:+.1f})"
                                    )

                                    # Voto: max tra Strategy (score/10) e Gemini
                                    _voto_strategy = min(10, int(strategy_result.score / 10))
                                    try:
                                        _voto_gemini_int = int(_gemini_voto or 0)
                                    except Exception:
                                        _voto_gemini_int = 0
                                    _voto_finale = max(_voto_strategy, _voto_gemini_int)
                                    
                                    decision = {
                                        'direzione': strategy_result.signal,
                                        'voto': _voto_finale,
                                        'sizing': strategy_result.sizing,
                                        'leverage': decision_gemini.get('leverage') or (
                                            engine.get_asset_leverage_info(asset).get('max_leverage', 3)
                                            if hasattr(engine, 'get_asset_leverage_info') else 3
                                        ),
                                        'sl': strategy_result.sl,
                                        'tp': strategy_result.tp,
                                        'razionale': (
                                            f"{strategy_result.razionale} | "
                                            f"✅ Gemini AGREE voto {_voto_gemini_int}: "
                                            f"{str(decision_gemini.get('razionale', '') or '')[:200]}"
                                        ),
                                        'stile_operativo': _stile_inferred,
                                        'timeframe_riferimento': '15m',
                                        'apprendimento_critico': decision_gemini.get('apprendimento_critico', ''),
                                        'score_breakdown': {
                                            'strategy_score': int(strategy_result.score),
                                            'gemini_voto': _voto_gemini_int,
                                            'confidence': int(strategy_result.confidence * 100),
                                        },
                                        'soglia_override': 0,
                                    }
                                    decision_source = "STRATEGY+GEMINI_CONFIRMED"
                                    voto_base_pre_ml = _voto_finale

                                    # Sinergia SL/TP (muri/OB/FVG dal brain)
                                    try:
                                        _tp_s, _sl_s, _ = brain.determina_tp_sl_ts(
                                            asset_name=asset,
                                            direzione=strategy_result.signal,
                                            prezzo_ingresso=strategy_result.entry_price or prezzo_ref,
                                            dati_engine=dati_mercato_chimera or {},
                                            levels_ia=decision
                                        )
                                        if _sl_s > 0 and _tp_s > 0:
                                            decision['sl'] = _sl_s
                                            decision['tp'] = _tp_s
                                            logger.info(
                                                f"🛡️ SINERGIA CHIMERA {asset}: SL {_sl_s} | TP {_tp_s}"
                                            )
                                    except Exception as _e_syn:
                                        _err.capture(_e_syn, "main", {"module": "BotLA"})
                                        logger.debug(f"Sinergia Strategy {asset}: {_e_syn}")

                                    logger.info(f"── {asset} {'─' * max(0, 45 - len(asset))} ")
                                    logger.info(
                                        f"🎯 {asset} STRATEGY+GEMINI CONFIRMED "
                                        f"(strategy {strategy_result.score:.0f} {_strategy_signal_up} "
                                        f"+ gemini voto {_voto_gemini_int}): "
                                        f"{strategy_result.signal} @ {strategy_result.entry_price:.2f} "
                                        f"[{strategy_result.strategy_name}]"
                                    )
                        
                        elif strategy_result and strategy_result.score >= _soglia_medium:
                            # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
                            # MEDIUM SIGNAL → VALIDA CON GEMINI
                            # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
                            logger.info(
                                f"🔍 {asset} Strategy score {strategy_result.score:.0f} - "
                                f"validazione Gemini richiesta"
                            )
                            
                            history = fe.get_recent_summary()
                            decision = brain.full_global_strategy(
                                dati_engine=dati_mercato_chimera,
                                asset_name=asset,
                                macro_sentiment=macro_sentiment_loop,
                                performance_history=history
                            )
                            
                            if decision and decision.get('direzione') != 'FLAT':
                                decision_source = "STRATEGY+GEMINI"
                                voto_base_pre_ml = decision.get('voto', 0)
                                
                                # Confronta Strategy vs Gemini
                                strategy_signal = strategy_result.signal
                                gemini_signal = decision['direzione'].upper()
                                
                                if strategy_signal == gemini_signal:
                                    # CONCORDANO → boost confidence
                                    decision['razionale'] = decision.get('razionale', '') + (
                                        f" | ✅ Strategy confirms (score {strategy_result.score:.0f}, "
                                        f"conf {strategy_result.confidence:.0%})"
                                    )
                                    logger.info(
                                        f"🤝 {asset} STRATEGY+GEMINI AGREE: {gemini_signal} "
                                        f"(Strategy {strategy_result.score:.0f}, Gemini voto {voto_base_pre_ml})"
                                    )
                                else:
                                    # DISCORDANO → usa Gemini ma logga
                                    decision['razionale'] = decision.get('razionale', '') + (
                                        f" | ⚠️ Strategy suggeriva {strategy_signal} "
                                        f"(score {strategy_result.score:.0f}), Gemini override"
                                    )
                                    logger.warning(
                                        f"⚠️ {asset} MISMATCH: "
                                        f"Strategy={strategy_signal} ({strategy_result.score:.0f}) vs "
                                        f"Gemini={gemini_signal} (voto {voto_base_pre_ml}). Using Gemini."
                                    )
                            else:
                                decision_source = "GEMINI_FLAT"
                                logger.debug(
                                    f"⏭️ {asset} Gemini override to FLAT "
                                    f"(strategy score {strategy_result.score:.0f})"
                                )
                        
                        else:
                            # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
                            # LOW SCORE o Strategy fallito → SOLO GEMINI
                            # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
                            score_str = f"{strategy_result.score:.0f}" if strategy_result else "N/A"
                            logger.info(f"── {asset} {'─' * max(0, 45 - len(asset))} ")
                            logger.info(
                                f"🧠 {asset} GEMINI ONLY (strategy score {score_str} < 60)"
                            )
                            
                            history = fe.get_recent_summary()
                            decision = brain.full_global_strategy(
                                dati_engine=dati_mercato_chimera,
                                asset_name=asset,
                                macro_sentiment=macro_sentiment_loop,
                                performance_history=history
                            )
                            
                            if decision and decision.get('direzione') not in ('FLAT', None):
                                decision_source = "GEMINI"
                                voto_base_pre_ml = decision.get('voto', 0)
                            else:
                                decision_source = "GEMINI_FLAT"
                                if decision:
                                    _voto_f = decision.get('voto', 0)
                                    _raz_f  = str(decision.get('razionale','') or '')
                                    # Estrai tutti i tag penalità presenti
                                    _tags_trovati = []
                                    for _tag in ['Floor voto','Prior RF','Voto insufficiente',
                                                 'ML VETO','ML WARN','AUDIT CRITICO','Streak Loss',
                                                 'Alpha Decay','policy','penalità','FLAT']:
                                        if _tag.lower() in _raz_f.lower():
                                            _tags_trovati.append(_tag)
                                    # Se nessun tag trovato nel razionale, Gemini ha deciso FLAT autonomamente
                                    if not _tags_trovati:
                                        _motivo_str = "Gemini autonomo (nessuna penalità nel razionale)"
                                    else:
                                        _motivo_str = " | ".join(_tags_trovati[:3])
                                    # Log compatto con ragionamento
                                    # Separa razionale Gemini dai tag policy aggiunti dal bot
                                    # I tag policy iniziano con | 📉 o | ⚠️ o | 🚀 o | 🚫
                                    _raz_parts = _raz_f.split(' | ') if _raz_f else []
                                    _raz_gemini = _raz_parts[0][:150] if _raz_parts else 'nessun razionale'
                                    _raz_policy = ' | '.join(p for p in _raz_parts[1:] if p.strip())
                                    logger.info(
                                        f"⏭️ [{asset}] Gemini→FLAT | voto={_voto_f} | {_motivo_str}"
                                    )
                                    logger.info(
                                        f"   📋 Razionale: {_raz_gemini}"
                                        + (f" | {_raz_policy}" if _raz_policy else "")
                                    )
                                else:
                                    logger.info(f"⏭️ [{asset}] Gemini→FLAT | nessuna decision")

                        # Reset flag avvio dopo aver analizzato tutti gli asset
                        ultimo_check_ia[asset] = time.time()

                        logger.debug(
                            f"🔎 [{asset}] Decision post-routing: "
                            f"dir={decision.get('direzione') if decision else 'None'}, "
                            f"voto={decision.get('voto') if decision else 'None'}, "
                            f"source={decision_source}, "
                            f"salta_entry={salta_entry}"
                        )
                        
                        # ══ ML SEMPRE VISIBILE — anche se direzione FLAT ══════════
                        if decision:
                            try:
                                _dir_ml  = decision.get('direzione', 'FLAT')
                                _voto_ml = int(decision.get('voto', 0) or 0)
                                _prob_ml, _conf_ml = chimera_ml.predici(
                                    dati_mercato=dati_mercato_chimera,
                                    asset=asset,
                                    direzione=_dir_ml if _dir_ml not in ('FLAT','WATCH') else 'LONG',
                                    voto_gemini=_voto_ml,
                                    macro_sentiment=macro_sentiment_loop,
                                    _silent=True  # log dalla chiamata principale sotto
                                )
                                _ml_status = (
                                    f"P(WIN)={_prob_ml:.1%} conf={_conf_ml:.1%}"
                                    if _conf_ml > 0 else
                                    f"P(WIN)={_prob_ml:.1%} conf=bassa (modello incerto)"
                                )
                                logger.debug(
                                    f"🤖 [ML] {asset} {_dir_ml} voto={_voto_ml} | {_ml_status}"
                                )
                                if dati_mercato_chimera:
                                    dati_mercato_chimera['ml_prob_win']   = _prob_ml
                                    dati_mercato_chimera['ml_confidenza'] = _conf_ml
                                    dati_mercato_chimera['decision_source'] = decision_source
                            except Exception as _e_ml_pre:
                                _err.capture(_e_ml_pre, sys._getframe().f_code.co_name, {"module": "BotLA", "method": "main"})
                                logger.debug(f"ML pre-check {asset}: {_e_ml_pre}")
                        # ══════════════════════════════════════════════════════

                        if decision and decision.get('direzione') not in ("FLAT", "WATCH"):
                            # --- LOGICA VOTO DAL PANNELLO DI CONTROLLO ---
                            voto_ia      = decision.get('voto', 0)
                            voto_minimo  = config_la.BRAIN_SOGGLIA
                            # Sentinella: soglia -1 (il movimento è reale)
                            if trigger_sentinella:
                                voto_minimo = max(5, voto_minimo - 1)
                                logger.info(
                                    f"⚡ [{asset}] SENTINEL PRIORITY: "
                                    f"soglia {config_la.BRAIN_SOGGLIA}→{voto_minimo}"
                                )

                            # [OFF] # ── CONFLUENZA BREAKDOWN: penalità voto ──────────────────
                            # [OFF] _bd = decision.get("score_breakdown", {})
                            # [OFF] if _bd:
                                # [OFF] _deboli = [k for k, v in _bd.items() if int(v) < 4]
                                # [OFF] if len(_deboli) >= 2:
                                    # [OFF] _pen = len(_deboli) - 1
                                    # [OFF] old_v = decision.get('voto', 0)
                                    # [OFF] decision['voto'] = max(0, old_v - _pen)
                                    # [OFF] logger.info(
                                        # [OFF] f"⚠️ [{asset}] Confluenza debole "
                                        # [OFF] f"({', '.join(_deboli)} sotto 4/10) — "
                                        # [OFF] f"voto {old_v}→{decision['voto']}"
                                    # [OFF] )
                            # Sincronizza voto_ia dopo penalità confluenza
                            voto_ia = decision.get('voto', 0)
                            _voto_pre_penalty = voto_ia  # snapshot per cap penalità
                            # ─────────────────────────────────────────────────────────

                            # ══════════════════════════════════════════════════════════
                            # CHIMERA ML: correzione voto (TUTTE le fonti)
                            # ══════════════════════════════════════════════════════════
                            try:
                                prob_ml, conf_ml = chimera_ml.predici(
                                    dati_mercato=dati_mercato_chimera,
                                    asset=asset,
                                    direzione=decision['direzione'],
                                    voto_gemini=voto_ia,
                                    macro_sentiment=macro_sentiment_loop
                                )
                                voto_ia_corretto = chimera_ml.correggi_voto(voto_ia, prob_ml, conf_ml)
                                
                                # Salva metadata per analytics
                                dati_mercato_chimera['ml_prob_win']   = prob_ml
                                dati_mercato_chimera['ml_confidenza'] = conf_ml
                                dati_mercato_chimera['decision_source'] = decision_source
                                
                                # Log correzione se significativa
                                if abs(voto_ia_corretto - voto_ia) >= 1:
                                    logger.info(
                                        f"🤖 {asset} XGBoost correction: "
                                        f"voto {voto_ia} → {voto_ia_corretto} "
                                        f"(prob {prob_ml:.2%}, conf {conf_ml:.2%}) [{decision_source}]"
                                    )
                                
                                voto_ia = voto_ia_corretto
                            except Exception as e_ml:
                                _err.capture(e_ml, sys._getframe().f_code.co_name, {"module": "BotLA", "method": "main"})
                                logger.debug(f"⚠️ ChimeraML skip [{asset}]: {e_ml}")
                            # ────────────────────────────────────────────────────────────────

                            # Se posizione aperta o CB attivo: mostra analisi ma non aprire
                            if salta_entry or (circuit_breaker_attivo and not is_aperta):
                                if salta_entry:
                                    motivo = 'posizione già aperta su Kraken' if is_aperta else 'orario bassa liquidità'
                                else:
                                    motivo = 'circuit breaker attivo (PnL -8%)'
                                logger.info(f"🚫 [{asset}] Analisi completata ma NON APRO: {motivo} (Voto: {voto_ia})")
                                continue

                            # ══ FILTRI ANALISI PROFONDA ══════════════════════
                            # Regole ricavate dall'analisi statistica di 300+ trade
                            # Applicabili a tutti gli asset — migliorano WR di 5-13%

                            _dire_f  = str(decision.get('direzione','')).upper()
                            _snap_f  = dati_mercato_chimera or {}
                            _hurst_f = float(_snap_f.get('hurst_exponent', 0.5) or 0.5)
                            _cvd_f   = float(_snap_f.get('cvd_istantaneo', 0) or 0)
                            _ofi_f   = float(_snap_f.get('order_flow_imbalance', 0) or 0)
                            _tema_f  = str(_snap_f.get('ema_trend_dominante', '') or '')

                            # 1. Cap voto 9-10 a 8
                            # Storico: voto 9-10 da Gemini ha WR peggiore → abbasso sempre a 8.
                            # Eccezione: STRATEGY STRONG/VALIDATED → cap a 8 senza blocco,
                            # il segnale è deterministico (score 100 = tutte le condizioni).
                            if voto_ia >= 9:
                                _snap_ml = dati_mercato_chimera or {}
                                _ml_prob = float(_snap_ml.get('ml_prob_win', 0) or 0)
                                _ml_conf = float(_snap_ml.get('ml_confidenza', 0) or 0)
                                _is_strategy = 'STRATEGY' in str(decision_source).upper()
                                if _is_strategy:
                                    # Strategy Strong/Validated: cap a 8, mai blocco
                                    decision['voto'] = 8
                                    voto_ia = 8
                                    logger.info(f"⚠️ [{asset}] Voto {voto_ia+1}→8 (Strategy strong, cap istituzionale)")
                                elif _ml_prob >= 0.65 and _ml_conf >= 0.30:
                                    # Gemini voto alto + ML forte: cap a 8
                                    decision['voto'] = 8
                                    voto_ia = 8
                                    logger.info(f"⚠️ [{asset}] Voto {voto_ia+1}→8 (ML forte: {_ml_prob:.1%} conf={_ml_conf:.1%})")
                                else:
                                    # Gemini ottimista senza conferma ML: blocca
                                    logger.info(f"⏭️ [{asset}] Voto {voto_ia} cappato→SKIP (Gemini over-conf, ML={_ml_prob:.1%})")
                                    continue

                            # [OFF] # 2. LONG contro trend dominante RIBASSISTA → penalità -1
                            # [OFF] if _dire_f in ('LONG','BUY') and _tema_f == 'RIBASSISTA':
                                # [OFF] _pen_ema = 1
                                # [OFF] voto_ia = max(0, voto_ia - _pen_ema)
                                # [OFF] decision['voto'] = voto_ia
                                # [OFF] decision['razionale'] = decision.get('razionale','') + f" | 📉 EMA ribassista: voto -{_pen_ema}"
                                # [OFF] logger.info(f"⚠️ [{asset}] LONG contro EMA RIBASSISTA — penalità -{_pen_ema} (voto={voto_ia})")

                            # 3. Hurst come contesto — non blocca, informa
                            # Hurst > 0.65 = trend forte e persistente.
                            # In BREAKOUT REALE è un segnale positivo.
                            # In ESAURIMENTO è un warning. Gemini decide.
                            if _hurst_f > 0.65:
                                logger.info(
                                    f"ℹ️ [{asset}] Hurst {_hurst_f:.2f} — trend forte "
                                    f"(contesto per Gemini, non blocco)"
                                )
                            # Hurst < 0.35 = mercato in mean reversion estrema
                            elif _hurst_f < 0.35:
                                logger.info(
                                    f"ℹ️ [{asset}] Hurst {_hurst_f:.2f} — mean reversion estrema "
                                    f"(contesto per Gemini, non blocco)"
                                )

                            # 3b. Logica 5 fasi con override istituzionale
                            _phase_f   = str(_snap_f.get('entry_phase', '') or '')
                            _subtype_f = str(_snap_f.get('phase_subtype', '') or '')
                            _override_f = bool(_snap_f.get('phase_override_ok', False))
                            _override_cond = str(_snap_f.get('phase_override_cond', '') or '')
                            _exhaust_f  = float(_snap_f.get('exhaustion_score', 0) or 0)
                            _cvd_d30    = float(_snap_f.get('cvd_delta_30s', 0) or 0)
                            _phase_narr = str(_snap_f.get('phase_narrative', '') or '')

                            # [OFF] # SILENZIO — penalità voto -2
                            # [OFF] # Bypassato se sentinella ha triggerato (il movimento È il segnale).
                            # [OFF] if _phase_f == 'SILENZIO':
                                # [OFF] if trigger_sentinella:
                                    # [OFF] logger.info(f"⚡ [{asset}] SILENZIO — penalità BYPASSATA (sentinella attiva)")
                                # [OFF] else:
                                    # [OFF] _pen_sil = 2
                                    # [OFF] voto_ia = max(0, voto_ia - _pen_sil)
                                    # [OFF] decision['voto'] = voto_ia
                                    # [OFF] decision['razionale'] = decision.get('razionale','') + f" | 🔇 SILENZIO: voto -{_pen_sil}"
                                    # [OFF] logger.info(f"⚠️ [{asset}] Fase SILENZIO — penalità voto -{_pen_sil} (voto ora {voto_ia})")

                            # FAKE BREAKOUT — info, non blocco se voto alto (Gemini decide)
                            # Versione 2026-05-01: prima era blocco totale → impediva LONG
                            # in trend rialzista normale dove velocity alta è sana ma CVD a 30s
                            # oscillava temporaneamente.
                            if _phase_f == 'BREAKOUT' and _subtype_f == 'FAKE':
                                if voto_ia < 8:
                                    logger.info(f"⏭️ [{asset}] FAKE BREAKOUT + voto {voto_ia}<8 — skip")
                                    continue
                                else:
                                    logger.info(f"⚠️ [{asset}] FAKE BREAKOUT info, voto {voto_ia}>=8 — Gemini decide, lascio passare")

                            # [OFF] # ESAURIMENTO NORMALE — penalità voto -2
                            # [OFF] if _phase_f == 'ESAURIMENTO' and _subtype_f == 'NORMALE':
                                # [OFF] _pen_esau = 2
                                # [OFF] voto_ia = max(0, voto_ia - _pen_esau)
                                # [OFF] decision['voto'] = voto_ia
                                # [OFF] decision['razionale'] = decision.get('razionale','') + f" | 📉 ESAURIMENTO: voto -{_pen_esau}"
                                # [OFF] logger.info(f"⚠️ [{asset}] Segnale in esaurimento — penalità voto -{_pen_esau} (voto ora {voto_ia})")

                            # [OFF] # FORMAZIONE — distingue VUOTA da ISTITUZIONALE
                            # [OFF] # Sentinella bypassa penalità VUOTA: il movimento è già reale.
                            # [OFF] if _phase_f == 'FORMAZIONE':
                                # [OFF] if _subtype_f != 'ISTITUZIONALE' or not _override_f:
                                    # [OFF] if trigger_sentinella:
                                        # [OFF] logger.info(f"⚡ [{asset}] FORMAZIONE VUOTA — penalità BYPASSATA (sentinella attiva)")
                                    # [OFF] else:
                                        # [OFF] _pen_form = 1
                                        # [OFF] voto_ia = max(0, voto_ia - _pen_form)
                                        # [OFF] decision['voto'] = voto_ia
                                        # [OFF] decision['razionale'] = decision.get('razionale','') + f" | 📐 FORMAZIONE: voto -{_pen_form}"
                                        # [OFF] logger.info(
                                            # [OFF] f"⚠️ [{asset}] Fase FORMAZIONE VUOTA — "
                                            # [OFF] f"penalità voto -{_pen_form} (voto ora {voto_ia})"
                                        # [OFF] )
                                    # [OFF] # Non fa continue — prosegue con threshold
                                # [OFF] else:
                                    # [OFF] # ISTITUZIONALE → verifica condizioni override
                                    # [OFF] _snap_ml2  = dati_mercato_chimera or {}
                                    # [OFF] _ml_prob2  = float(_snap_ml2.get('ml_prob_win', 0) or 0)
                                    # [OFF] _ml_conf2  = float(_snap_ml2.get('ml_confidenza', 0) or 0)
                                    # [OFF] _whale2    = float(_snap_ml2.get('whale_delta', 0) or 0)
                                    # [OFF] _ofi2      = float(_snap_ml2.get('order_flow_imbalance', 0) or 0)
                                    # [OFF] _cb2       = str(_snap_ml2.get('candlestick_bias', '') or '')
                                    # [OFF] _sr_dist2  = min(
                                        # [OFF] float(_snap_ml2.get('dist_supporto', 99) or 99),
                                        # [OFF] float(_snap_ml2.get('dist_resistenza', 99) or 99)
                                    # [OFF] )
                                    # [OFF] _dire2     = _dire_f

                                    # [OFF] # Conta confluenze override
                                    # [OFF] _conf_override = 0
                                    # [OFF] _conf_motivi   = []
                                    # [OFF] if voto_ia >= 7:
                                        # [OFF] _conf_override += 1; _conf_motivi.append(f"voto={voto_ia}")
                                    # [OFF] if _ml_conf2 >= 0.35:
                                        # [OFF] _conf_override += 1; _conf_motivi.append(f"ML conf={_ml_conf2:.0%}")
                                    # [OFF] if _cb2.upper() in ('BULLISH','BEARISH'):
                                        # [OFF] _conf_override += 1; _conf_motivi.append(f"candlestick={_cb2}")
                                    # [OFF] if _sr_dist2 < 1.5:
                                        # [OFF] _conf_override += 1; _conf_motivi.append(f"vicino S/R ({_sr_dist2:.1f}%)")
                                    # [OFF] if ((_dire2 in ('LONG','BUY') and _whale2 > 0.3) or
                                        # [OFF] (_dire2 in ('SHORT','SELL') and _whale2 < -0.3)):
                                        # [OFF] _conf_override += 1; _conf_motivi.append(f"whale_delta={_whale2:.2f}")

                                    # [OFF] if _conf_override >= 3:
                                        # [OFF] logger.info(
                                            # [OFF] f"⚡ [{asset}] PHASE OVERRIDE: FORMAZIONE ISTITUZIONALE — "
                                            # [OFF] f"{_conf_override}/5 confluenze: {', '.join(_conf_motivi)}"
                                        # [OFF] )
                                        # [OFF] # Prosegue — non fa continue
                                    # [OFF] else:
                                        # [OFF] logger.info(
                                            # [OFF] f"⏭️ [{asset}] FORMAZIONE ISTITUZIONALE ma confluenze "
                                            # [OFF] f"insufficienti ({_conf_override}/5, servono 3) — "
                                            # [OFF] f"{', '.join(_conf_motivi) if _conf_motivi else 'nessuna'}"
                                        # [OFF] )
                                        # [OFF] continue

                            # ESAURIMENTO REVERSAL — permesso con cautela
                            if _phase_f == 'ESAURIMENTO' and _subtype_f == 'REVERSAL':
                                _ml_prob_r = float((dati_mercato_chimera or {}).get('ml_prob_win', 0) or 0)
                                _ml_conf_r = float((dati_mercato_chimera or {}).get('ml_confidenza', 0) or 0)
                                if voto_ia >= 7 and _ml_conf_r >= 0.35:
                                    logger.info(
                                        f"⚡ [{asset}] ESAURIMENTO REVERSAL override: "
                                        f"voto={voto_ia}, ML conf={_ml_conf_r:.0%}"
                                    )
                                else:
                                    # Confluenze insufficienti → penalità -1 (non blocco totale)
                                    _pen_rev = 1
                                    voto_ia = max(0, voto_ia - _pen_rev)
                                    decision['voto'] = voto_ia
                                    decision['razionale'] = decision.get('razionale','') + f" | 🔄 REVERSAL: voto -{_pen_rev}"
                                    logger.info(
                                        f"⚠️ [{asset}] ESAURIMENTO REVERSAL: confluenze deboli "
                                        f"(voto={voto_ia}, ML conf={_ml_conf_r:.0%}) — penalità -{_pen_rev}"
                                    )

                            # BREAKOUT REALE/ESPLOSIVO ed ESTENSIONE → sempre permessi (nessun blocco)

                            # [OFF] # 4. CVD fortemente contro la direzione — soglia adattiva per asset
                            # [OFF] # In BREAKOUT ISTITUZIONALE il CVD contro può essere un liquidity
                            # [OFF] # sweep prima del vero movimento — non blocchiamo.
                            # [OFF] _cvd_soglia = _CVD_SOGLIE.get(asset, _CVD_SOGLIA_DEFAULT)
                            # [OFF] _is_breakout_istit = (
                                # [OFF] _phase_f == 'BREAKOUT' and _subtype_f in ('REALE','ESPLOSIVO')
                            # [OFF] ) or (
                                # [OFF] _phase_f == 'FORMAZIONE' and _subtype_f == 'ISTITUZIONALE'
                            # [OFF] )
                            # [OFF] if not _is_breakout_istit:
                                # [OFF] if _dire_f in ('SHORT','SELL') and _cvd_f > _cvd_soglia:
                                    # [OFF] _pen_cvd = 1
                                    # [OFF] voto_ia = max(0, voto_ia - _pen_cvd)
                                    # [OFF] decision['voto'] = voto_ia
                                    # [OFF] decision['razionale'] = decision.get('razionale','') + f" | 📊 CVD contro SHORT: voto -{_pen_cvd}"
                                    # [OFF] logger.info(
                                        # [OFF] f"⚠️ [{asset}] SHORT: CVD {_cvd_f:,.0f} > soglia {_cvd_soglia:,.0f} "
                                        # [OFF] f"— penalità voto -{_pen_cvd} (voto ora {voto_ia})"
                                    # [OFF] )
                                # [OFF] if _dire_f in ('LONG','BUY') and _cvd_f < -_cvd_soglia:
                                    # [OFF] _pen_cvd = 1
                                    # [OFF] voto_ia = max(0, voto_ia - _pen_cvd)
                                    # [OFF] decision['voto'] = voto_ia
                                    # [OFF] decision['razionale'] = decision.get('razionale','') + f" | 📊 CVD contro LONG: voto -{_pen_cvd}"
                                    # [OFF] logger.info(
                                        # [OFF] f"⚠️ [{asset}] LONG: CVD {_cvd_f:,.0f} < -{_cvd_soglia:,.0f} "
                                        # [OFF] f"— penalità voto -{_pen_cvd} (voto ora {voto_ia})"
                                    # [OFF] )
                            # [OFF] else:
                                # [OFF] logger.info(
                                    # [OFF] f"ℹ️ [{asset}] CVD contro ({_cvd_f:,.0f}) ignorato: "
                                    # [OFF] f"fase {_phase_f} {_subtype_f} — possibile liquidity sweep"
                                # [OFF] )

                            # [OFF] # 5. Filtro OFI per DOGE → penalità -1
                            # [OFF] if 'DG' in asset or 'DOGE' in asset:
                                # [OFF] if _dire_f in ('LONG','BUY') and _ofi_f < -0.15:
                                    # [OFF] _pen_ofi = 1
                                    # [OFF] voto_ia = max(0, voto_ia - _pen_ofi)
                                    # [OFF] decision['voto'] = voto_ia
                                    # [OFF] logger.info(f"⚠️ [DOGE] LONG: OFI {_ofi_f:.3f} contro — penalità -{_pen_ofi} (voto={voto_ia})")
                                # [OFF] if _dire_f in ('SHORT','SELL') and _ofi_f > 0.15:
                                    # [OFF] _pen_ofi = 1
                                    # [OFF] voto_ia = max(0, voto_ia - _pen_ofi)
                                    # [OFF] decision['voto'] = voto_ia
                                    # [OFF] logger.info(f"⚠️ [DOGE] SHORT: OFI {_ofi_f:.3f} contro — penalità -{_pen_ofi} (voto={voto_ia})")

                            # 6 & 7. Kaufman e Velocity — filtri differenziati per stile operativo
                            # SCALPING: opera su rimbalzi veloci — Kaufman basso è normale,
                            #           velocity alta nella direzione è un segnale POSITIVO.
                            # MOMENTUM: mercato in movimento — Kaufman medio, velocity concorde.
                            # SWING:    movimento efficiente richiesto — Kaufman alto, velocity
                            #           contro è un warning ma non blocco se struttura solida.
                            _stile_op = str(decision.get('stile_operativo', 'MOMENTUM') or 'MOMENTUM').upper()
                            _mtf = _snap_f.get('multi_tf', {})
                            _kauf_15m = float((_mtf.get('15m', {}) or {}).get('kaufman', 1.0) or 1.0)
                            _kauf_1h  = float((_mtf.get('1h',  {}) or {}).get('kaufman', 1.0) or 1.0)
                            _kauf_avg = (_kauf_15m + _kauf_1h) / 2
                            _vel_raw  = float(_snap_f.get('price_velocity', 0) or 0)

                            if 'SCALP' in _stile_op:
                                # SCALPING: Kaufman basso OK (mercato laterale con rimbalzi)
                                # Velocity alta nella direzione = segnale positivo, non blocco
                                # Blocca solo se Kaufman praticamente zero (dati assenti)
                                if _kauf_avg < 0.05 and _kauf_avg > 0:
                                    logger.info(f"⏭️ [{asset}] SCALPING: Kaufman {_kauf_avg:.2f} praticamente zero — dati assenti")
                                    continue
                                # Velocity alta in direzione opposta → warning ma non blocco
                                if abs(_vel_raw) > 0.0008:
                                    _vel_contro = (
                                        (_vel_raw < 0 and _dire_f in ('LONG','BUY')) or
                                        (_vel_raw > 0 and _dire_f in ('SHORT','SELL'))
                                    )
                                    if _vel_contro:
                                        logger.info(
                                            f"ℹ️ [{asset}] SCALPING: velocity {_vel_raw:.5f} contro "
                                            f"{_dire_f} — contesto (non blocco in SCALPING)"
                                        )

                            # [OFF] elif 'MOMENTUM' in _stile_op:
                                # [OFF] # MOMENTUM: Kaufman medio richiesto
                                # [OFF] _kauf_min_mom = {'XXBTZUSD': 0.18, 'XETHZUSD': 0.15,
                                                 # [OFF] 'XXRPZUSD': 0.15, 'XDGUSD': 0.15, 'SOLUSD': 0.15}
                                # [OFF] _kauf_min = _kauf_min_mom.get(asset, 0.15)
                                # [OFF] if _kauf_avg < _kauf_min and _kauf_avg > 0:
                                    # [OFF] logger.info(
                                        # [OFF] f"⏭️ [{asset}] MOMENTUM: Kaufman {_kauf_avg:.2f} < {_kauf_min} "
                                        # [OFF] f"— nessun momentum direzionale"
                                    # [OFF] )
                                    # [OFF] continue
                                # [OFF] # Velocity fortemente contro → blocco
                                # [OFF] if abs(_vel_raw) > 0.0003:
                                    # [OFF] _vel_contro = (
                                        # [OFF] (_vel_raw < -0.0003 and _dire_f in ('LONG','BUY')) or
                                        # [OFF] (_vel_raw > 0.0003  and _dire_f in ('SHORT','SELL'))
                                    # [OFF] )
                                    # [OFF] if _vel_contro:
                                        # [OFF] logger.info(
                                            # [OFF] f"⏭️ [{asset}] MOMENTUM: velocity {_vel_raw:.5f} contro "
                                            # [OFF] f"{_dire_f} — attendere allineamento"
                                        # [OFF] )
                                        # [OFF] continue

                            # [OFF] else:  # SWING / MULTIDAY
                                # [OFF] # SWING: Kaufman alto richiesto — movimento deve essere efficiente
                                # [OFF] _kauf_min_sw = {'XXBTZUSD': 0.20, 'XETHZUSD': 0.18,
                                                # [OFF] 'XXRPZUSD': 0.18, 'XDGUSD': 0.18, 'SOLUSD': 0.18}
                                # [OFF] _kauf_min = _kauf_min_sw.get(asset, 0.18)
                                # [OFF] if _kauf_avg < _kauf_min and _kauf_avg > 0:
                                    # [OFF] logger.info(
                                        # [OFF] f"⏭️ [{asset}] SWING: Kaufman {_kauf_avg:.2f} < {_kauf_min} "
                                        # [OFF] f"— movimento non efficiente per swing"
                                    # [OFF] )
                                    # [OFF] continue
                                # [OFF] # Velocity contro in SWING: solo warning, la struttura H1/4H conta di più
                                # [OFF] if abs(_vel_raw) > 0.0003:
                                    # [OFF] _vel_contro = (
                                        # [OFF] (_vel_raw < -0.0003 and _dire_f in ('LONG','BUY')) or
                                        # [OFF] (_vel_raw > 0.0003  and _dire_f in ('SHORT','SELL'))
                                    # [OFF] )
                                    # [OFF] if _vel_contro:
                                        # [OFF] logger.info(
                                            # [OFF] f"ℹ️ [{asset}] SWING: velocity {_vel_raw:.5f} contro "
                                            # [OFF] f"{_dire_f} — contesto (struttura H1/4H prioritaria)"
                                        # [OFF] )

                            # [OFF] # ══ FINE FILTRI ══════════════════════════════════════
                            # [OFF] # ── CAP PENALITÀ CUMULATIVE (max -2 dal bot) ──────────
                            # [OFF] # Evita death spiral: voto=6 → -1EMA -1CVD -2SILENZIO = 2
                            # [OFF] # Streak/Alpha agiscono solo sul sizing, non inclusi.
                            # [OFF] _pen_applicata = _voto_pre_penalty - voto_ia
                            # [OFF] if _pen_applicata > 2:
                                # [OFF] _voto_cappato = _voto_pre_penalty - 2
                                # [OFF] logger.info(
                                    # [OFF] f"🛡️ [{asset}] Cap penalità: "
                                    # [OFF] f"{_voto_pre_penalty}→{voto_ia} "
                                    # [OFF] f"cappato a {_voto_cappato} (max -2)"
                                # [OFF] )
                                # [OFF] voto_ia = _voto_cappato
                                # [OFF] decision['voto'] = voto_ia
                            # [OFF] # ──────────────────────────────────────────────────────

                            # [OFF] # Riduzione sizing fascia 20-24h UTC
                            # [OFF] _ora_utc = datetime.now(timezone.utc).hour
                            # [OFF] if 20 <= _ora_utc < 24:
                                # [OFF] _stile_now = str(decision.get('stile_operativo','')).upper()
                                # [OFF] if _stile_now in ('SWING', 'MOMENTUM') and voto_ia < 8:
                                    # [OFF] logger.info(f"⏭️ [{asset}] {_stile_now} bloccato 20-24h UTC (WR storico 36%) — solo SCALPING voto≥8")
                                    # [OFF] continue
                                # [OFF] # Per trade che passano: sizing ridotto del 50%
                                # [OFF] _size_old = decision.get('sizing', 0.1)
                                # [OFF] decision['sizing'] = round(_size_old * 0.5, 5)
                                # [OFF] logger.info(f"⚠️ [{asset}] Fascia 20-24h UTC: sizing ridotto {_size_old:.3f} → {decision['sizing']:.3f}")

                            if voto_ia >= voto_minimo + decision.get('soglia_override', 0):
                                # Gate IN_USD: blocca solo SPOT LONG (leva=1 + direzione LONG)
                                # Margin trades (leva>=2) e SHORT sempre permessi
                                _leverage_dec = decision.get('leverage', 1) or 1
                                _dir_dec = str(decision.get('direzione','')).upper()
                                if (asset_rotation.is_in_usd
                                        and int(_leverage_dec) <= 1
                                        and _dir_dec in ('LONG', 'BUY')):
                                    logger.info(
                                        f"⏭️ [{asset}] SPOT LONG bloccato: rotation IN_USD "
                                        f"(capitale in valuta tradizionale). Margine libero."
                                    )
                                    continue
                                # ── SIZING per tipo trade ──────────────────
                                # WITH_TREND (score allineato):   sizing pieno
                                # NEUTRAL:                        sizing invariato
                                # COUNTER_TREND (score opposto):  sizing 55%
                                # Dati: WITH=64% WR, COUNTER=32% WR su storico
                                _ts_now  = float(dati_mercato_chimera.get('trend_score', 0) or 0)
                                _dir_now = str(decision.get('direzione', '') or '').upper()
                                _is_long_now = _dir_now in ('BUY', 'LONG')
                                _is_counter = (
                                    (_is_long_now and _ts_now <= -1.5) or
                                    (not _is_long_now and _ts_now >= 1.5)
                                )
                                if _is_counter and decision.get('sizing', 0) > 0:
                                    _sz_old = decision['sizing']
                                    decision['sizing'] = round(_sz_old * 0.55, 5)
                                    logger.info(
                                        f"⚠️ [{asset}] COUNTER_TREND "
                                        f"(trend_score={_ts_now:+.1f}): "
                                        f"sizing {_sz_old:.3f}→{decision['sizing']:.3f} (-45%)"
                                    )
                                    decision['razionale'] += f" | ⚠️ COUNTER_TREND score={_ts_now:+.1f}"
                                elif _ts_now >= 1.5 or _ts_now <= -1.5:
                                    logger.debug(f"✅ [{asset}] WITH_TREND (score={_ts_now:+.1f}): sizing pieno")
                                # ───────────────────────────────────────────────
                                logger.info(f"🚀 SEGNALE VALIDATO {asset}: Voto {voto_ia} — ESEGUO.")
                
                                # La leva viene ora gestita dinamicamente dal TradeManager 
                                # rispettando i limiti di asset_list.py e il Pannello di Controllo
                                leverage_f = decision.get('leverage', None)

                                success_pos = trade_manager.apri_posizione(
                                    asset=asset,
                                    direzione=decision['direzione'],
                                    entry_price=prezzo_ref,
                                    size=decision.get('sizing', 0.01),
                                    leverage=leverage_f,
                                    sl=decision.get('sl', 0),
                                    tp=decision.get('tp', 0),
                                    voto=voto_ia,
                                    dati_mercato=dati_mercato_chimera,
                                    tipo_operazione=decision.get('stile_operativo', 'SWING'),
                                    apprendimento_critico=decision.get('apprendimento_critico', ''),
                                    razionale=decision.get('razionale', '')
                                )
            
                                if success_pos:
                                    safe_razionale = (str(decision.get('razionale', ''))
                                                      .replace('_', ' ').replace('*', ' ')\
                                                      .replace('[', '(').replace(']', ')'))
                                    
                                    voti_chimera = decision.get('score_breakdown', {})
                                    str_voti = "\n".join([f"• {k.replace('_', ' ')}: {v}/10" for k, v in voti_chimera.items()])
                                    
                                    msg_alert = (
                                        f"🚀 *ENTRY {asset}* (Voto: {voto_ia}/10)\n"
                                        f"⏱️ *TF:* {decision.get('timeframe_riferimento','N/A')} | 🎯 *Tipo:* {decision.get('stile_operativo', decision.get('tipo_operazione', 'N/A'))}\n"
                                        f"━━━━━━━━━━━━━━━\n"
                                        f"📊 *Matrice Chimera:*\n{str_voti if str_voti else 'N/A'}\n"
                                        f"━━━━━━━━━━━━━━━\n"
                                        f"🧠 *Razionale:* {safe_razionale}"
                                    )
                                    appr = decision.get('apprendimento_critico', '')
                                    if appr:
                                        msg_alert += f"\n📚 *Insight:* {appr[:120]}"
                                        
                                    alerts.invia_alert(msg_alert)
                            else:
                                fe.registra_analisi_scartata(asset, voto_ia, decision['direzione'], prezzo_ref, dati_mercato_chimera, sl=decision.get('sl',0), tp=decision.get('tp',0))
                                logger.info(f"⚖️ [{asset}] SCARTATO: Voto {voto_ia} < soglia minima {voto_minimo} — trade virtuale avviato")

                except Exception as e_asset:
                    _err.capture(e_asset, sys._getframe().f_code.co_name, {"module": "BotLA", "method": "main"})
                    logger.error(f"❌ ERRORE CRITICO SU {asset}: {e_asset}")
                    traceback.print_exc()

            # ── D.2 RETRY ASSET CON DATI CORROTTI ──────────────────────────────
            if assets_da_riprovare:
                logger.info(f"🔄 Ritento il fetch dei dati per {len(assets_da_riprovare)} asset con dati inaffidabili...")
                time.sleep(2) # Breve pausa prima del retry
                for asset, salta_entry, forza_questo_ciclo, trigger_sentinella in assets_da_riprovare:
                    try:
                        res_raw = engine.get_full_market_data(asset)
                        res     = res_raw[0] if isinstance(res_raw, tuple) else res_raw

                        if is_data_corrupted(res):
                            logger.error(f"❌ Dati ancora corrotti per {asset} dopo il retry. Salto definitivamente per questo ciclo.")
                            continue

                        logger.info(f"✅ Dati recuperati con successo per {asset} al secondo tentativo.")
                        
                        prezzo_ref  = float(res.get('close', 0))
                        atr_sicuro  = float(res.get('atr', 0))
                        if atr_sicuro <= 0:
                            atr_sicuro = prezzo_ref * 0.002

                        muri = res.get('liquidity_pools', [])
                        if not muri or len(muri) == 0:
                            muri = [
                                {"price": prezzo_ref * 0.985, "type": "support",    "volume": 1.0},
                                {"price": prezzo_ref * 1.015, "type": "resistance", "volume": 1.0}
                            ]

                        dati_mercato_chimera = res.copy()
                        dati_mercato_chimera['close'] = prezzo_ref
                        dati_mercato_chimera['atr']   = atr_sicuro
                        # Cache dati per AssetRotation (evita doppie chiamate API)
                        _dati_cache[asset] = dati_mercato_chimera
                        dati_mercato_chimera['microstruttura_hft'] = {
                            'muri_liquidita': muri,
                            'aggressivita': res.get('aggressivita', 0)
                        }
                        
                        cvd_log = dati_mercato_chimera.get('cvd_istantaneo', 0.0)
                        logger.debug(f"🧠 DATI → GEMINI [{asset}] (RETRY): CVD={cvd_log:.2f}, ATR={atr_sicuro:.2f}")
                        history  = fe.get_recent_summary()
                        decision = brain.full_global_strategy(
                            dati_engine=dati_mercato_chimera,
                            asset_name=asset,
                            macro_sentiment=macro_sentiment_loop,
                            performance_history=history
                        )

                        logger.info(f"🔎 [{asset}] Decision post-brain: dir={decision.get('direzione') if decision else 'None'}, voto={decision.get('voto') if decision else 'None'}, salta_entry={salta_entry}")
                        # ══ ML SEMPRE VISIBILE — anche se direzione FLAT ══════════
                        if decision:
                            try:
                                _dir_ml  = decision.get('direzione', 'FLAT')
                                _voto_ml = int(decision.get('voto', 0) or 0)
                                _prob_ml, _conf_ml = chimera_ml.predici(
                                    dati_mercato=dati_mercato_chimera,
                                    asset=asset,
                                    direzione=_dir_ml if _dir_ml not in ('FLAT','WATCH') else 'LONG',
                                    voto_gemini=_voto_ml,
                                    macro_sentiment=macro_sentiment_loop,
                                    _silent=True  # log dalla chiamata principale sotto
                                )
                                _ml_status = (
                                    f"P(WIN)={_prob_ml:.1%} conf={_conf_ml:.1%}"
                                    if _conf_ml > 0 else
                                    f"P(WIN)={_prob_ml:.1%} conf=bassa (modello incerto)"
                                )
                                logger.debug(
                                    f"🤖 [ML] {asset} {_dir_ml} voto={_voto_ml} | {_ml_status}"
                                )
                                if dati_mercato_chimera:
                                    dati_mercato_chimera['ml_prob_win']   = _prob_ml
                                    dati_mercato_chimera['ml_confidenza'] = _conf_ml
                                    dati_mercato_chimera['decision_source'] = decision_source
                            except Exception as _e_ml_pre:
                                _err.capture(_e_ml_pre, sys._getframe().f_code.co_name, {"module": "BotLA", "method": "main"})
                                logger.debug(f"ML pre-check {asset}: {_e_ml_pre}")
                        # ══════════════════════════════════════════════════════

                        if decision and decision.get('direzione') not in ("FLAT", "WATCH"):
                            # --- LOGICA VOTO DAL PANNELLO DI CONTROLLO ---
                            voto_ia      = decision.get('voto', 0)
                            voto_minimo  = config_la.BRAIN_SOGGLIA
                            
                            if salta_entry or (circuit_breaker_attivo and not is_aperta):
                                if salta_entry:
                                    motivo = 'posizione già aperta su Kraken' if is_aperta else 'orario bassa liquidità'
                                else:
                                    motivo = 'circuit breaker attivo (PnL -8%)'
                                logger.info(f"🚫 [{asset}] RETRY - Analisi completata ma NON APRO: {motivo} (Voto: {voto_ia})")
                                continue

                            # ══ FILTRI ANALISI PROFONDA ══════════════════════
                            # Regole ricavate dall'analisi statistica di 300+ trade
                            # Applicabili a tutti gli asset — migliorano WR di 5-13%

                            _dire_f  = str(decision.get('direzione','')).upper()
                            _snap_f  = dati_mercato_chimera or {}
                            _hurst_f = float(_snap_f.get('hurst_exponent', 0.5) or 0.5)
                            _cvd_f   = float(_snap_f.get('cvd_istantaneo', 0) or 0)
                            _ofi_f   = float(_snap_f.get('order_flow_imbalance', 0) or 0)
                            _tema_f  = str(_snap_f.get('ema_trend_dominante', '') or '')

                            # 1. Cap voto 9-10 a 8
                            # Storico: voto 9-10 da Gemini ha WR peggiore → abbasso sempre a 8.
                            # Eccezione: STRATEGY STRONG/VALIDATED → cap a 8 senza blocco,
                            # il segnale è deterministico (score 100 = tutte le condizioni).
                            if voto_ia >= 9:
                                _snap_ml = dati_mercato_chimera or {}
                                _ml_prob = float(_snap_ml.get('ml_prob_win', 0) or 0)
                                _ml_conf = float(_snap_ml.get('ml_confidenza', 0) or 0)
                                _is_strategy = 'STRATEGY' in str(decision_source).upper()
                                if _is_strategy:
                                    # Strategy Strong/Validated: cap a 8, mai blocco
                                    decision['voto'] = 8
                                    voto_ia = 8
                                    logger.info(f"⚠️ [{asset}] Voto {voto_ia+1}→8 (Strategy strong, cap istituzionale)")
                                elif _ml_prob >= 0.65 and _ml_conf >= 0.30:
                                    # Gemini voto alto + ML forte: cap a 8
                                    decision['voto'] = 8
                                    voto_ia = 8
                                    logger.info(f"⚠️ [{asset}] Voto {voto_ia+1}→8 (ML forte: {_ml_prob:.1%} conf={_ml_conf:.1%})")
                                else:
                                    # Gemini ottimista senza conferma ML: blocca
                                    logger.info(f"⏭️ [{asset}] Voto {voto_ia} cappato→SKIP (Gemini over-conf, ML={_ml_prob:.1%})")
                                    continue

                            # [OFF] # 2. LONG contro trend dominante RIBASSISTA → penalità -1
                            # [OFF] if _dire_f in ('LONG','BUY') and _tema_f == 'RIBASSISTA':
                                # [OFF] _pen_ema = 1
                                # [OFF] voto_ia = max(0, voto_ia - _pen_ema)
                                # [OFF] decision['voto'] = voto_ia
                                # [OFF] decision['razionale'] = decision.get('razionale','') + f" | 📉 EMA ribassista: voto -{_pen_ema}"
                                # [OFF] logger.info(f"⚠️ [{asset}] LONG contro EMA RIBASSISTA — penalità -{_pen_ema} (voto={voto_ia})")

                            # 3. Hurst — contesto, non blocco (vedi logica principale)
                            if _hurst_f > 0.65:
                                logger.info(f"ℹ️ [{asset}] Hurst {_hurst_f:.2f} — trend forte (contesto)")
                            elif _hurst_f < 0.35:
                                logger.info(f"ℹ️ [{asset}] Hurst {_hurst_f:.2f} — mean reversion estrema (contesto)")

                            # [OFF] # 3b. FORMAZIONE → penalità -1 (allineato al loop principale)
                            # [OFF] _phase_f = str(_snap_f.get('entry_phase', '') or '')
                            # [OFF] if _phase_f == 'FORMAZIONE':
                                # [OFF] _pen_form_r = 1
                                # [OFF] voto_ia = max(0, voto_ia - _pen_form_r)
                                # [OFF] decision['voto'] = voto_ia
                                # [OFF] decision['razionale'] = decision.get('razionale','') + f" | 📐 FORMAZIONE: voto -{_pen_form_r}"
                                # [OFF] logger.info(f"⚠️ [{asset}] Fase FORMAZIONE — penalità -{_pen_form_r} (voto={voto_ia})")

                            # [OFF] # 4. CVD fortemente contro la direzione → penalità -1 (allineato al loop principale)
                            # [OFF] _cvd_soglia_r = _CVD_SOGLIE.get(asset, _CVD_SOGLIA_DEFAULT)
                            # [OFF] if _dire_f in ('SHORT','SELL') and _cvd_f > _cvd_soglia_r:
                                # [OFF] _pen_cvd_r = 1
                                # [OFF] voto_ia = max(0, voto_ia - _pen_cvd_r)
                                # [OFF] decision['voto'] = voto_ia
                                # [OFF] logger.info(
                                    # [OFF] f"⚠️ [{asset}] SHORT: CVD {_cvd_f:,.0f} > soglia {_cvd_soglia_r:,.0f} "
                                    # [OFF] f"— penalità -{_pen_cvd_r} (voto={voto_ia})"
                                # [OFF] )
                            # [OFF] if _dire_f in ('LONG','BUY') and _cvd_f < -_cvd_soglia_r:
                                # [OFF] _pen_cvd_r = 1
                                # [OFF] voto_ia = max(0, voto_ia - _pen_cvd_r)
                                # [OFF] decision['voto'] = voto_ia
                                # [OFF] logger.info(
                                    # [OFF] f"⚠️ [{asset}] LONG: CVD {_cvd_f:,.0f} < -{_cvd_soglia_r:,.0f} "
                                    # [OFF] f"— penalità -{_pen_cvd_r} (voto={voto_ia})"
                                # [OFF] )

                            # [OFF] # 5. Filtro OFI per DOGE → penalità -1 (retry loop)
                            # [OFF] if 'DG' in asset or 'DOGE' in asset:
                                # [OFF] if _dire_f in ('LONG','BUY') and _ofi_f < -0.15:
                                    # [OFF] _pen_ofi_r = 1
                                    # [OFF] voto_ia = max(0, voto_ia - _pen_ofi_r)
                                    # [OFF] decision['voto'] = voto_ia
                                    # [OFF] logger.info(f"⚠️ [DOGE] LONG: OFI {_ofi_f:.3f} contro — penalità -{_pen_ofi_r} (voto={voto_ia})")
                                # [OFF] if _dire_f in ('SHORT','SELL') and _ofi_f > 0.15:
                                    # [OFF] _pen_ofi_r = 1
                                    # [OFF] voto_ia = max(0, voto_ia - _pen_ofi_r)
                                    # [OFF] decision['voto'] = voto_ia
                                    # [OFF] logger.info(f"⚠️ [DOGE] SHORT: OFI {_ofi_f:.3f} contro — penalità -{_pen_ofi_r} (voto={voto_ia})")

                            # [OFF] # 6. Kaufman Efficiency — soglie per asset calibrate sui dati reali
                            # [OFF] # WIN medio >> LOSS medio: BTC 0.42 vs 0.29, ETH 0.37 vs 0.28, SOL 0.33 vs 0.32
                            # [OFF] # Non è un blocco duro — soglie conservative, solo i casi chiaramente inefficienti
                            # [OFF] _kauf_soglie = {
                                # [OFF] 'XXBTZUSD': 0.20,  # BTC: WIN=0.42, LOSS=0.29 — soglia conservativa
                                # [OFF] 'XETHZUSD': 0.18,  # ETH: mercato più rumoroso
                                # [OFF] 'XXRPZUSD': 0.18,
                                # [OFF] 'XDGUSD':   0.18,
                                # [OFF] 'SOLUSD':   0.18,
                            # [OFF] }
                            # [OFF] _mtf = _snap_f.get('multi_tf', {})
                            # [OFF] _kauf_15m = float((_mtf.get('15m', {}) or {}).get('kaufman', 1.0) or 1.0)
                            # [OFF] _kauf_1h  = float((_mtf.get('1h',  {}) or {}).get('kaufman', 1.0) or 1.0)
                            # [OFF] _kauf_avg = (_kauf_15m + _kauf_1h) / 2
                            # [OFF] _kauf_min = _kauf_soglie.get(asset, 0.18)
                            # [OFF] if _kauf_avg < _kauf_min and _kauf_avg > 0:
                                # [OFF] logger.info(
                                    # [OFF] f"⏭️ [{asset}] Kaufman basso ({_kauf_avg:.2f} < {_kauf_min}) "
                                    # [OFF] f"— mercato rumoroso, nessun momentum direzionale"
                                # [OFF] )
                                # [OFF] continue

                            # [OFF] # 7. Velocity allineata alla direzione (soft — solo se fortemente contro)
                            # [OFF] # Dai dati: vel contro = LOSS+. Solo blocco se vel molto forte e chiaramente contro
                            # [OFF] _vel_raw = float(_snap_f.get('price_velocity', 0) or 0)
                            # [OFF] if abs(_vel_raw) > 0.0003:  # velocity significativa
                                # [OFF] _vel_contro = (
                                    # [OFF] (_vel_raw < -0.0003 and _dire_f in ('LONG','BUY')) or
                                    # [OFF] (_vel_raw > 0.0003  and _dire_f in ('SHORT','SELL'))
                                # [OFF] )
                                # [OFF] if _vel_contro:
                                    # [OFF] logger.info(
                                        # [OFF] f"⏭️ [{asset}] Velocity {_vel_raw:.5f} fortemente contro {_dire_f} "
                                        # [OFF] f"— attendere inversione momentum"
                                    # [OFF] )
                                    # [OFF] continue

                            # ══ FINE FILTRI ══════════════════════════════════════
                            if voto_ia >= voto_minimo:
                                # ── SIZING per tipo trade ──────────────────
                                # WITH_TREND (score allineato):   sizing pieno
                                # NEUTRAL:                        sizing invariato
                                # COUNTER_TREND (score opposto):  sizing 55%
                                # Dati: WITH=64% WR, COUNTER=32% WR su storico
                                _ts_now  = float(dati_mercato_chimera.get('trend_score', 0) or 0)
                                _dir_now = str(decision.get('direzione', '') or '').upper()
                                _is_long_now = _dir_now in ('BUY', 'LONG')
                                _is_counter = (
                                    (_is_long_now and _ts_now <= -1.5) or
                                    (not _is_long_now and _ts_now >= 1.5)
                                )
                                if _is_counter and decision.get('sizing', 0) > 0:
                                    _sz_old = decision['sizing']
                                    decision['sizing'] = round(_sz_old * 0.55, 5)
                                    logger.info(
                                        f"⚠️ [{asset}] COUNTER_TREND "
                                        f"(trend_score={_ts_now:+.1f}): "
                                        f"sizing {_sz_old:.3f}→{decision['sizing']:.3f} (-45%)"
                                    )
                                    decision['razionale'] += f" | ⚠️ COUNTER_TREND score={_ts_now:+.1f}"
                                elif _ts_now >= 1.5 or _ts_now <= -1.5:
                                    logger.debug(f"✅ [{asset}] WITH_TREND (score={_ts_now:+.1f}): sizing pieno")
                                # ───────────────────────────────────────────────
                                logger.info(f"🚀 SEGNALE VALIDATO {asset} (RETRY): Voto {voto_ia} — ESEGUO.")
                                # La leva viene ora gestita dinamicamente dal TradeManager 
                                # rispettando i limiti di asset_list.py e il Pannello di Controllo
                                leverage_f = decision.get('leverage', None)

                                success_pos = trade_manager.apri_posizione(
                                    asset=asset,
                                    direzione=decision['direzione'],
                                    entry_price=prezzo_ref,
                                    size=decision.get('sizing', 0.01),
                                    leverage=leverage_f,
                                    sl=decision.get('sl', 0),
                                    tp=decision.get('tp', 0),
                                    voto=voto_ia,
                                    dati_mercato=dati_mercato_chimera,
                                    tipo_operazione=decision.get('stile_operativo', 'SWING'),
                                    apprendimento_critico=decision.get('apprendimento_critico', ''),
                                    razionale=decision.get('razionale', '')
                                )
            
                                if success_pos:
                                    safe_razionale = (str(decision.get('razionale', ''))
                                                      .replace('_', ' ').replace('*', ' ')\
                                                      .replace('[', '(').replace(']', ')'))
                                    
                                    voti_chimera = decision.get('score_breakdown', {})
                                    str_voti = "\n".join([f"• {k.replace('_', ' ')}: {v}/10" for k, v in voti_chimera.items()])
                                    
                                    msg_alert = (
                                        f"🚀 *ENTRY {asset}* (Voto: {voto_ia}/10)\n"
                                        f"⏱️ *TF:* {decision.get('timeframe_riferimento','N/A')} | 🎯 *Tipo:* {decision.get('stile_operativo', decision.get('tipo_operazione', 'N/A'))}\n"
                                        f"━━━━━━━━━━━━━━━\n"
                                        f"📊 *Matrice Chimera:*\n{str_voti if str_voti else 'N/A'}\n"
                                        f"━━━━━━━━━━━━━━━\n"
                                        f"🧠 *Razionale:* {safe_razionale}"
                                    )
                                    appr = decision.get('apprendimento_critico', '')
                                    if appr:
                                        msg_alert += f"\n📚 *Insight:* {appr[:120]}"
                                        
                                    alerts.invia_alert(msg_alert)
                            else:
                                fe.registra_analisi_scartata(asset, voto_ia, decision['direzione'], prezzo_ref, dati_mercato_chimera, sl=decision.get('sl',0), tp=decision.get('tp',0))
                                logger.info(f"⚖️ [{asset}] RETRY - SCARTATO: Voto {voto_ia} < soglia minima {voto_minimo} — trade virtuale avviato")

                    except Exception as e_retry:
                        _err.capture(e_retry, sys._getframe().f_code.co_name, {"module": "BotLA", "method": "main"})
                        logger.error(f"❌ ERRORE CRITICO SU {asset} DURANTE RETRY: {e_retry}")
                        traceback.print_exc()

            # Reset flag avvio dopo il primo ciclo completo
            if _forza_analisi_avvio:
                _forza_analisi_avvio = False
                # Salva cache per la rotation del ciclo successivo
            try:
                main._dati_cache_prev = dict(_dati_cache)
            except Exception:
                pass

            logger.debug("✅ Analisi avvio completata su tutti gli asset.")

            # ── E. ASSET ROTATION (ogni quarto d'ora) ────────────────────────
            if e_quarto_d_ora:
                try:
                    if trade_manager.posizioni_aperte:
                        # Recupera dati per TUTTI gli asset (servono sia per valutare stagnanti che target)
                        dati_tutti = {}
                        for a in kraken_int.get_watchlist():
                            try:
                                r = engine.get_full_market_data(a)
                                dati_tutti[a] = r[0] if isinstance(r, tuple) else r
                            except Exception:
                                pass
                        rotazioni = asset_rotation.valuta_rotazione(
                            trade_manager.posizioni_aperte, dati_tutti
                        )
                        if rotazioni:
                            asset_rotation.notifica_rotazione(rotazioni)
                            _, macro_val_rot = macro.get_macro_data()
                            asset_rotation.esegui_rotazione(
                                raccomandazioni=rotazioni,
                                trade_manager=trade_manager,
                                brain=brain,
                                dati_mercato_tutti=dati_tutti,
                                macro_sentiment=macro_val_rot,
                                soglia_score_delta=1.5
                            )
                except Exception as e_rot:
                    _err.capture(e_rot, sys._getframe().f_code.co_name, {"module": "BotLA", "method": "main"})
                    logger.error(f"❌ AssetRotation error: {e_rot}")

            # ── F. GHOST TRADES E ATTESA ──────────────────────────────────────
            try:
                fe.verifica_esiti_ghost(engine.exchange, chimera_ml=chimera_ml)
            except Exception:
                pass

            # ── G. CHIMERA AUDITOR (Ogni 4 ore) ───────────────────────────────
            if momento_ciclo - ultimo_audit > 14400:
                logger.info("🕵️‍♂️ Avvio Chimera Auditor (Controllo asincrono trade)...")
                try:
                    auditor.esegui_audit(ore_indietro=4)
                except Exception as e_aud:
                    _err.capture(e_aud, sys._getframe().f_code.co_name, {"module": "BotLA", "method": "main"})
                    logger.error(f"❌ Errore Auditor: {e_aud}")
                ultimo_audit = momento_ciclo

            # ── G2. ENGINE HEALTH CHECK (All'avvio + ogni 4 ore) ──────────────
            # Verifica che tutti gli indicatori Engine restituiscano valori reali
            # e non i valori di fallback (che segnalano un errore silenzioso).
            # Manda alert Telegram solo se trova problemi — silenzioso se tutto ok.
            if momento_ciclo - ultimo_engine_health > 14400 or ultimo_engine_health == 0:
                try:
                    _health = _controlla_salute_engine(engine, alerts, logger)
                    if _health['score'] >= 80:
                        logger.info(
                            f"🟢 Engine Health OK — {_health['score']:.0f}% indicatori reali "
                            f"({_health['ok']}/{_health['total']})"
                        )
                    else:
                        logger.warning(
                            f"🟡 Engine Health {_health['score']:.0f}% — "
                            f"{_health['problemi']} indicatori al default"
                        )
                except Exception as e_health:
                    _err.capture(e_health, sys._getframe().f_code.co_name, {"module": "BotLA", "method": "main"})
                    logger.error(f"❌ Engine health check fallito: {e_health}")
                ultimo_engine_health = momento_ciclo

            # ── G2. HEIKIN ASHI TREND: segnale margine 1H BTC ────────────────
            # Gira ad ogni ciclo — la cache 1H si aggiorna ogni 15 minuti.
            # Non apre se c'è già una posizione BTC aperta da questa strategia.
            try:
                _btc_aperto = trade_manager.is_posizione_aperta_su_kraken("XXBTZUSD")
                if not _btc_aperto and not is_killed:
                    _dati_btc_trend = engine.get_full_market_data("XXBTZUSD")
                    _dati_btc_dict  = _dati_btc_trend[0] if isinstance(_dati_btc_trend, tuple) else (_dati_btc_trend or {})

                    # Trend in direzione daily
                    _segnale_trend = ha_trend_strategy.analyze("XXBTZUSD", _dati_btc_dict)
                    if _segnale_trend and _segnale_trend.score >= 65:
                        trade_manager.apri_posizione(
                            asset           = "XXBTZUSD",
                            direzione       = _segnale_trend.signal,
                            entry_price     = _segnale_trend.entry_price,
                            size            = _segnale_trend.sizing,
                            sl              = _segnale_trend.sl,
                            tp              = _segnale_trend.tp,
                            voto            = int(_segnale_trend.score / 10),
                            leverage        = HA_TREND_LEVA,
                            dati_mercato    = _segnale_trend.components,
                            tipo_operazione = "INTRADAY",
                            razionale       = _segnale_trend.razionale,
                        )
                    else:
                        # Contro trend su max/min di giornata
                        _segnale_contro = ha_contro_strategy.analyze("XXBTZUSD", _dati_btc_dict)
                        if _segnale_contro and _segnale_contro.score >= 65:
                            trade_manager.apri_posizione(
                                asset           = "XXBTZUSD",
                                direzione       = _segnale_contro.signal,
                                entry_price     = _segnale_contro.entry_price,
                                size            = _segnale_contro.sizing,
                                sl              = _segnale_contro.sl,
                                tp              = _segnale_contro.tp,
                                voto            = int(_segnale_contro.score / 10),
                                leverage        = HA_CONTRO_LEVA,
                                dati_mercato    = _segnale_contro.components,
                                tipo_operazione = "INTRADAY",
                                razionale       = _segnale_contro.razionale,
                            )
            except Exception as e_ha_trend:
                _err.capture(e_ha_trend, sys._getframe().f_code.co_name,
                             {"module": "BotLA", "method": "ha_trend"})
                logger.debug(f"HA Trend/Contro: {e_ha_trend}")

            # ── H. REPORT STATISTICHE PERIODICO (Ogni ora) ────────────────────
            if momento_ciclo - ultimo_report_stats > 3600:
                stats_report = trade_manager.genera_report_completo()
                if alerts:
                    alerts.invia_stats_complete(stats_report)
                ultimo_report_stats = momento_ciclo
                ultimo_log_stats = momento_ciclo # Evita doppio log ravvicinato

            # ── I. LOG STATISTICHE PERIODICO (Ogni 5 minuti) ──────────────────
            if momento_ciclo - ultimo_log_stats > 300:
                trade_manager.genera_report_completo()
                ultimo_log_stats = momento_ciclo

            time.sleep(WAIT_PROTEZIONE)

    except KeyboardInterrupt:
        logger.info("🛑 Bot fermato manualmente.")
    except Exception as e:
        _err.capture(e, sys._getframe().f_code.co_name, {"module": "BotLA", "method": "main"})
        logger.critical(f"💀 CRASH TOTALE: {e}")
        traceback.print_exc()

if __name__ == "__main__":
    main()

# File updated to sync with UI