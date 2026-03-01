# -*- coding: utf-8 -*-
import google.generativeai as genai
"""
L&A Institutional Bot - ConfigLA
Modulo centrale per la gestione delle chiavi API, parametri bot, e impostazioni generali.
Versione razionalizzata e pronta all'uso.
"""

# -- Chiavi API e secret (placeholders, sostituisci con le tue credenziali reali) --
KRAKEN_KEY = "xq/ndt21DJjfIdNts8HePaMz4SO/merfMpVulCBfGx0qZAF6HyNRT6Fo"
KRAKEN_SECRET = "rb01L3fEg4so3E7iFVSnwXzfIq4gfoXH1+xzDXMQcyUFbtGYKWHTTSRYE2eUP7nDaHyMbF5AyTLAB1uJedornA=="
KRAKEN_FUTURES_API_KEY = "WA6jkAbwSCSlucWrYr6g7HS+KprhsK2ORgu5BH41/G+XW/nnYUseLrSR"
KRAKEN_FUTURES_API_SECRET = "vJCSPahURRG1zY6yb+F6IQymj3RKBgJXQ9DmwaIWsYTedUzFZ99TJrEq5hh1SVbGwQb6LEbiZKm8hFUFchRcVOMh"

TELEGRAM_TOKEN = "8288745485:AAG895uZ799msuiMV6-_3QTMkCYovB3ooSo"
TELEGRAM_CHAT_ID = "6705393127"
COINGLASS_API_KEY = "cfbdef695c3242898fb9407de5662e2c"
GEMINI_API_KEY = "AIzaSyAOIz3PGkDWAER3as3mfQiUcjqyhcj47k8"
# -- Parametri tradabili --
RISK_PER_TRADE = 0.03        # percentuale rischio trade
MIN_TRADE_SIZE = 0.001
MAX_TRADE_SIZE = 100
DEFAULT_EXCHANGE = "kraken"

# -- Parametri tecnici / bot --
SLEEP_SECONDS = 30
POSITION_FILE = "posizioni_aperte.json"
FEEDBACK_FILE = "feedback_history.json"
REPORT_FILE = "report_reale.json"
SCATOLA_NERA_FILE = "scatola_nera.json"

# -- Parametri learning --
BRAIN_SOGGLIA = 5.5

def get_risk_settings():
    """
    Ritorna i parametri di rischio configurati.
    """
    return {
        "risk_per_trade": RISK_PER_TRADE,
        "min_trade_size": MIN_TRADE_SIZE,
        "max_trade_size": MAX_TRADE_SIZE
    }

def print_settings():
    """
    Stampa le impostazioni principali del bot.
    """
    print(f"Exchange default: {DEFAULT_EXCHANGE}")
    print(f"Risk per trade: {RISK_PER_TRADE}")
    print(f"Min trade size: {MIN_TRADE_SIZE}")
    print(f"Max trade size: {MAX_TRADE_SIZE}")
    print(f"Sleep seconds: {SLEEP_SECONDS}")