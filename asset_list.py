# -*- coding: utf-8 -*-
"""
L&A Institutional Bot - AssetList
Definisce il mapping dei ticker e gli asset principali.
"""

ASSET_PRINCIPALI = ["XXBTZUSD", "XETHZUSD", "XETHXXBT"]

ASSET_MAPPING = {
    "XXBTZUSD": "XXBTZUSD",
    "XETHZUSD": "XETHZUSD",
    "XETHXXBT": "XETHXXBT"
}

ASSET_CONFIG = {
    "XXBTZUSD": {
        "precision": 1,
        "vol_precision": 8,
        "min_size": 0.0001,
        "max_leverage": 10,
        "is_cross": False  # Asset standard (USD)
    },
    "XETHZUSD": {
        "precision": 2,
        "vol_precision": 2,
        "min_size": 0.01,
        "max_leverage": 10,
        "is_cross": False  # Asset standard (USD)
    },
    "XETHXXBT": {
        "ticker": "XETHXXBT",
        "precision": 5,
        "vol_precision": 4,
        "min_size": 0.01,
        "max_leverage": 5,
        "is_cross": True,           # <--- Identifica che è un cross crypto-crypto
        "quote_asset": "XXBTZUSD"    # <--- Specifica l'asset da usare per convertire il budget
    }
}

def is_asset_supported(asset_name):
    return asset_name.upper() in ASSET_MAPPING

def get_ticker(asset_name):
    return ASSET_MAPPING.get(asset_name.upper(), asset_name)

def get_config(asset_name):
    """Ritorna la configurazione completa dell'asset."""
    return ASSET_CONFIG.get(asset_name.upper(), {})

def filtra_asset_istituzionali(lista_asset):
    return [a for a in lista_asset if a in ASSET_PRINCIPALI]