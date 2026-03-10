# -*- coding: utf-8 -*-
"""
L&A Institutional Bot - AssetList
FIX: Implementazione mapping Futures e correzione Ticker per EngineLA.
"""

ASSET_PRINCIPALI = ["XXBTZUSD", "XETHZUSD", "XETHXXBT"]

# Mapping per CCXT e chiamate API dirette
ASSET_MAPPING = {
    "XXBTZUSD": "BTC/USD",
    "XETHZUSD": "ETH/USD",
    "XETHXXBT": "ETH/BTC"
}

# Mapping specifico per i Futures di Kraken (necessario per Funding/OI)
FUTURES_MAPPING = {
    "XXBTZUSD": "PI_XBTUSD",
    "XETHZUSD": "PI_ETHUSD",
    "XETHXXBT": "FI_ETHUSD" # Il cross non ha un future diretto, usiamo ETH come proxy
}

ASSET_CONFIG = {
    "XXBTZUSD": {
        "precision": 1,
        "vol_precision": 8,
        "min_size": 0.0001,
        "max_leverage": 5,
        "is_cross": False
    },
    "XETHZUSD": {
        "precision": 2,
        "vol_precision": 2,
        "min_size": 0.01,
        "max_leverage": 5,
        "is_cross": False
    },
    "XETHXXBT": {
        "ticker": "ETH/BTC",
        "precision": 5,
        "vol_precision": 4,
        "min_size": 0.01,
        "max_leverage": 1, # Spesso leva non disponibile su cross crypto-crypto
        "is_cross": True,
        "quote_asset": "XXBTZUSD"
    }
}

def is_asset_supported(asset_name):
    return asset_name.upper() in ASSET_MAPPING or asset_name.upper() in ASSET_PRINCIPALI

def get_ticker(asset_name):
    """
    Trasforma nomi umani in codici Kraken (es. BTC/USD -> XXBTZUSD)
    o viceversa, assicurando compatibilità totale con l'Engine.
    """
    name_upper = asset_name.upper()
    
    # Caso 1: È già una chiave (es. XXBTZUSD), la restituiamo
    if name_upper in ASSET_MAPPING:
        return name_upper
        
    # Caso 2: È un valore (es. BTC/USD), cerchiamo la chiave corrispondente
    for kraken_code, human_name in ASSET_MAPPING.items():
        if human_name.upper() == name_upper:
            return kraken_code
            
    # Caso 3: Non è nel mapping, restituiamo l'originale
    return name_upper
def get_futures_ticker(asset_name):
    """Restituisce il ticker per le API Futures di Kraken."""
    return FUTURES_MAPPING.get(asset_name.upper())

def get_config(asset_name):
    """Ritorna la configurazione completa dell'asset."""
    return ASSET_CONFIG.get(asset_name.upper(), {})

def filtra_asset_istituzionali(lista_asset):
    return [a for a in lista_asset if a in ASSET_PRINCIPALI]