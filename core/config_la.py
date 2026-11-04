 # -*- coding: utf-8 -*-
"""
CHIMERA v4.0 - Configuration Module
Configurazione centralizzata per il bot di trading

IMPORTANTE:
- Le API keys vengono caricate da file .env (non committare mai .env!)
- I parametri del Pannello di Controllo sono modificabili qui
- Tutti i valori hanno fallback sicuri
"""
import os
from dotenv import load_dotenv
from core.chimera_errors import ErrorTracker

_err = ErrorTracker("ConfigLa")

# Carica variabili da .env
load_dotenv()

# ═══════════════════════════════════════════════════════════════
# API CREDENTIALS (caricate da .env)
# ═══════════════════════════════════════════════════════════════

# Kraken API
KRAKEN_KEY = os.environ.get("KRAKEN_KEY", "la mia chiave")
KRAKEN_SECRET = os.environ.get("KRAKEN_SECRET", "la mia chiave")

# Kraken Futures API
KRAKEN_FUTURES_KEY = os.environ.get("KRAKEN_FUTURES_KEY", "la mia chiave")
KRAKEN_FUTURES_SECRET = os.environ.get("KRAKEN_FUTURES_SECRET", "la mia chiave")

# Google Gemini AI
GEMINI_API_KEY = os.environ.get("GEMINI_API_KEY", "la mia chiave")

# FIX 1: Modello configurabile (NON più hardcoded "gemini-3-flash-preview")
GEMINI_MODEL = os.environ.get("GEMINI_MODEL", "gemini-2.5-flash")
# Coinglass API (per dati derivati/funding)
COINGLASS_API_KEY = os.environ.get("COINGLASS_API_KEY", "la mia chiave")

# Telegram Bot
TELEGRAM_TOKEN = os.environ.get("TELEGRAM_TOKEN", "la mia chiave")
TELEGRAM_BOT_TOKEN = TELEGRAM_TOKEN  # Alias per compatibilità
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "la mia chiave")

# ═══════════════════════════════════════════════════════════════
# PANNELLO DI CONTROLLO (Parametri Tradabili)
# ═══════════════════════════════════════════════════════════════

# Risk Management
RISK_PER_TRADE = float(os.environ.get("RISK_PER_TRADE", "0.10"))  # 10% default
KILLSWITCH_GIORNALIERO = float(os.environ.get("KILLSWITCH_GIORNALIERO", "0.10"))  # 10% perdita max
KILLSWITCH_ENABLED = os.environ.get("KILLSWITCH_ENABLED", "False").lower() == "true"
KILLSWITCH_THRESHOLD = float(os.environ.get("KILLSWITCH_THRESHOLD", "0.20"))  # 20% perdita capitale totale

# Brain AI Threshold
BRAIN_SOGGLIA = float(os.environ.get("BRAIN_SOGGLIA", "6.0"))  # Voto minimo per entry (0-10)

# Stop Loss Virtuale
VIRTUAL_STOP_LOSS = os.environ.get("VIRTUAL_STOP_LOSS", "True").lower() == "true"

# Trading Limits
MIN_TRADE_SIZE = float(os.environ.get("MIN_TRADE_SIZE", "0.001"))
MAX_TRADE_SIZE = float(os.environ.get("MAX_TRADE_SIZE", "100"))
MAX_POSITION_SIZE = float(os.environ.get("MAX_POSITION_SIZE", "0.10"))  # 10% capitale per trade
MAX_OPEN_POSITIONS = int(os.environ.get("MAX_OPEN_POSITIONS", "3"))

# Leverage Management
DEFAULT_LEVERAGE = int(os.environ.get("DEFAULT_LEVERAGE", "1"))
MAX_LEVERAGE = int(os.environ.get("MAX_LEVERAGE", "5"))

# ═══════════════════════════════════════════════════════════════
# EXCHANGE CONFIGURATION
# ═══════════════════════════════════════════════════════════════

DEFAULT_EXCHANGE = os.environ.get("DEFAULT_EXCHANGE", "kraken")

# ═══════════════════════════════════════════════════════════════
# TIMING & INTERVALS
# ═══════════════════════════════════════════════════════════════

SLEEP_SECONDS = int(os.environ.get("SLEEP_SECONDS", "30"))
ANALISI_INTERVAL = int(os.environ.get("ANALISI_INTERVAL", "90"))  # Secondi tra analisi stesso asset
COOLDOWN_DOPO_CHIUSURA = int(os.environ.get("COOLDOWN_DOPO_CHIUSURA", "300"))  # 5 minuti

# ═══════════════════════════════════════════════════════════════
# FILE PATHS
# ═══════════════════════════════════════════════════════════════

POSITION_FILE = os.environ.get("POSITION_FILE", "posizioni_aperte.json")
FEEDBACK_FILE = os.environ.get("FEEDBACK_FILE", "feedback_history.json")
REPORT_FILE = os.environ.get("REPORT_FILE", "report_reale.json")
SCATOLA_NERA_FILE = os.environ.get("SCATOLA_NERA_FILE", "scatola_nera.json")
DB_PATH = os.environ.get("DB_PATH", "chimera.db")

# ═══════════════════════════════════════════════════════════════
# CIRCUIT BREAKER (Opzionale, disabilitato su richiesta utente)
# ═══════════════════════════════════════════════════════════════

CIRCUIT_BREAKER_ENABLED = os.environ.get("CIRCUIT_BREAKER_ENABLED", "false").lower() == "true"
CIRCUIT_BREAKER_THRESHOLD = float(os.environ.get("CIRCUIT_BREAKER_THRESHOLD", "-8.0"))  # -8% PnL 24h

# ═══════════════════════════════════════════════════════════════
# ASSET ROTATION
# ═══════════════════════════════════════════════════════════════

ROTATION_ENABLED = os.environ.get("ROTATION_ENABLED", "true").lower() == "true"
ROTATION_INTERVAL = int(os.environ.get("ROTATION_INTERVAL", "900"))  # 15 minuti
ROTATION_SCORE_DELTA = float(os.environ.get("ROTATION_SCORE_DELTA", "1.5"))

# ═══════════════════════════════════════════════════════════════
# REPORT SCHEDULING
# ═══════════════════════════════════════════════════════════════

REPORT_MATTUTINO_ORA = int(os.environ.get("REPORT_MATTUTINO_ORA", "7"))  # UTC
REPORT_SERALE_ORA = int(os.environ.get("REPORT_SERALE_ORA", "20"))  # UTC
REPORT_SETTIMANALE_GIORNO = int(os.environ.get("REPORT_SETTIMANALE_GIORNO", "6"))  # 0=Lun, 6=Dom
REPORT_SETTIMANALE_ORA = int(os.environ.get("REPORT_SETTIMANALE_ORA", "20"))  # UTC

# ═══════════════════════════════════════════════════════════════
# CHIMERA ML (XGBoost)
# ═══════════════════════════════════════════════════════════════

ML_MIN_TRADES = int(os.environ.get("ML_MIN_TRADES", "50"))  # Minimo trade per attivare ML
ML_RETRAIN_INTERVAL = int(os.environ.get("ML_RETRAIN_INTERVAL", "10"))  # Retrain ogni N trade
ML_MAX_WEIGHT = float(os.environ.get("ML_MAX_WEIGHT", "0.40"))  # Peso massimo ML sul voto (40%)

# ═══════════════════════════════════════════════════════════════
# WEBSOCKET CONFIGURATION
# ═══════════════════════════════════════════════════════════════

WS_RECONNECT = os.environ.get("WS_RECONNECT", "true").lower() == "true"
WS_RECONNECT_DELAY = int(os.environ.get("WS_RECONNECT_DELAY", "5"))  # Secondi

# ═══════════════════════════════════════════════════════════════
# DEVELOPMENT / TESTING
# ═══════════════════════════════════════════════════════════════

TEST_MODE = os.environ.get("TEST_MODE", "false").lower() == "true"
DEBUG_MODE = os.environ.get("DEBUG_MODE", "false").lower() == "true"
LOG_LEVEL = os.environ.get("LOG_LEVEL", "INFO")
LOG_FILE = os.environ.get("LOG_FILE", "logs/chimera.log")

# Capitale iniziale (per calcoli percentuali)
CAPITALE_INIZIALE = float(os.environ.get("CAPITALE_INIZIALE", "1000.0"))

# ═══════════════════════════════════════════════════════════════
# HELPER FUNCTIONS
# ═══════════════════════════════════════════════════════════════

def get_risk_settings():
    """
    Ritorna i parametri di rischio configurati.
    
    Returns:
        dict: Dizionario con parametri di rischio
    """
    return {
        "risk_per_trade": RISK_PER_TRADE,
        "min_trade_size": MIN_TRADE_SIZE,
        "max_trade_size": MAX_TRADE_SIZE,
        "max_position_size": MAX_POSITION_SIZE,
        "max_open_positions": MAX_OPEN_POSITIONS,
        "killswitch_enabled": KILLSWITCH_ENABLED,
        "killswitch_threshold": KILLSWITCH_THRESHOLD,
        "virtual_stop_loss": VIRTUAL_STOP_LOSS
    }


def get_brain_settings():
    """
    Ritorna i parametri Brain AI.
    
    Returns:
        dict: Dizionario con configurazione Brain
    """
    return {
        "gemini_model": GEMINI_MODEL,
        "brain_sogglia": BRAIN_SOGGLIA,
        "ml_enabled": not TEST_MODE,
        "ml_min_trades": ML_MIN_TRADES
    }


def get_timing_settings():
    """
    Ritorna i parametri temporali.
    
    Returns:
        dict: Dizionario con timing configuration
    """
    return {
        "sleep_seconds": SLEEP_SECONDS,
        "analisi_interval": ANALISI_INTERVAL,
        "cooldown_dopo_chiusura": COOLDOWN_DOPO_CHIUSURA,
        "rotation_interval": ROTATION_INTERVAL
    }


def print_settings():
    """
    Stampa le impostazioni principali del bot (senza credenziali sensibili).
    """
    print("\n" + "="*60)
    print("🤖 CHIMERA v4.0 - CONFIGURAZIONE ATTIVA")
    print("="*60)
    print(f"Exchange default:        {DEFAULT_EXCHANGE}")
    print(f"Gemini Model:            {GEMINI_MODEL}")
    print("-"*60)
    print("📊 RISK MANAGEMENT:")
    print(f"  Risk per trade:        {RISK_PER_TRADE*100:.1f}%")
    print(f"  Min trade size:        {MIN_TRADE_SIZE}")
    print(f"  Max trade size:        {MAX_TRADE_SIZE}")
    print(f"  Max position size:     {MAX_POSITION_SIZE*100:.1f}%")
    print(f"  Max open positions:    {MAX_OPEN_POSITIONS}")
    print(f"  Killswitch enabled:    {'✅ YES' if KILLSWITCH_ENABLED else '❌ NO'}")
    if KILLSWITCH_ENABLED:
        print(f"  Killswitch threshold:  {KILLSWITCH_THRESHOLD*100:.0f}%")
    print("-"*60)
    print("🧠 BRAIN AI:")
    print(f"  Voto minimo entry:     {BRAIN_SOGGLIA}/10")
    print(f"  ML min trades:         {ML_MIN_TRADES}")
    print(f"  ML max weight:         {ML_MAX_WEIGHT*100:.0f}%")
    print("-"*60)
    print("⏱️  TIMING:")
    print(f"  Sleep seconds:         {SLEEP_SECONDS}s")
    print(f"  Analisi interval:      {ANALISI_INTERVAL}s")
    print(f"  Cooldown chiusura:     {COOLDOWN_DOPO_CHIUSURA}s")
    print(f"  Rotation interval:     {ROTATION_INTERVAL}s (ogni {ROTATION_INTERVAL//60}min)")
    print("-"*60)
    print("🔄 FEATURES:")
    print(f"  Asset Rotation:        {'✅' if ROTATION_ENABLED else '❌'}")
    print(f"  Circuit Breaker:       {'✅' if CIRCUIT_BREAKER_ENABLED else '❌'}")
    print(f"  WebSocket Auto-Recon:  {'✅' if WS_RECONNECT else '❌'}")
    print(f"  Virtual Stop Loss:     {'✅' if VIRTUAL_STOP_LOSS else '❌'}")
    print(f"  Test Mode:             {'⚠️  YES' if TEST_MODE else '✅ NO (LIVE)'}")
    print("="*60 + "\n")


def validate_config():
    """
    Valida che tutte le configurazioni obbligatorie siano presenti.
    
    Raises:
        ValueError: Se mancano configurazioni critiche
    """
    errors = []
    
    # Check API Keys (solo se non in TEST_MODE)
    if not TEST_MODE:
        if not KRAKEN_KEY or KRAKEN_KEY == "":
            errors.append("KRAKEN_KEY mancante in .env")
        if not KRAKEN_SECRET or KRAKEN_SECRET == "":
            errors.append("KRAKEN_SECRET mancante in .env")
        if not GEMINI_API_KEY or GEMINI_API_KEY == "":
            errors.append("GEMINI_API_KEY mancante in .env")
        if not TELEGRAM_BOT_TOKEN or TELEGRAM_BOT_TOKEN == "":
            errors.append("TELEGRAM_BOT_TOKEN mancante in .env")
        if not TELEGRAM_CHAT_ID or TELEGRAM_CHAT_ID == "":
            errors.append("TELEGRAM_CHAT_ID mancante in .env")
    
    # Check Parametri Logici
    if BRAIN_SOGGLIA < 0 or BRAIN_SOGGLIA > 10:
        errors.append(f"BRAIN_SOGGLIA deve essere tra 0 e 10 (attuale: {BRAIN_SOGGLIA})")
    
    if RISK_PER_TRADE <= 0 or RISK_PER_TRADE > 1:
        errors.append(f"RISK_PER_TRADE deve essere tra 0 e 1 (attuale: {RISK_PER_TRADE})")
    
    if MIN_TRADE_SIZE <= 0:
        errors.append(f"MIN_TRADE_SIZE deve essere > 0 (attuale: {MIN_TRADE_SIZE})")
    
    if MAX_TRADE_SIZE < MIN_TRADE_SIZE:
        errors.append(f"MAX_TRADE_SIZE ({MAX_TRADE_SIZE}) < MIN_TRADE_SIZE ({MIN_TRADE_SIZE})")
    
    if errors:
        error_msg = "❌ ERRORI DI CONFIGURAZIONE:\n  - " + "\n  - ".join(errors)
        raise ValueError(error_msg)
    
    return True


# ═══════════════════════════════════════════════════════════════
# AUTO-VALIDATION ON IMPORT
# ═══════════════════════════════════════════════════════════════

if __name__ != "__main__":
    # Valida automaticamente quando il modulo viene importato
    # (solo se non eseguito direttamente)
    try:
        validate_config()
    except ValueError as e:
        _err.capture(e, "unknown", {"module": "ConfigLa"})
        print(f"\n⚠️  WARNING: {e}\n")


# ═══════════════════════════════════════════════════════════════
# TEST CONFIGURATION (se eseguito direttamente)
# ═══════════════════════════════════════════════════════════════

if __name__ == "__main__":
    print("\n🔍 Testing Configuration Module...\n")
    
    try:
        validate_config()
        print("✅ Configurazione valida!\n")
    except ValueError as e:
        _err.capture(e, "unknown", {"module": "ConfigLa"})
        print(f"❌ Errori trovati:\n{e}\n")
    
    print_settings()
    
    print("\n📊 Risk Settings:")
    print(get_risk_settings())
    
    print("\n🧠 Brain Settings:")
    print(get_brain_settings())
    
    print("\n⏱️  Timing Settings:")
    print(get_timing_settings())
