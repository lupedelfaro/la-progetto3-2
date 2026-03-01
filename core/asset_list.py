# -*- coding: utf-8 -*-
"""
L&A Institutional Bot - AssetList
Definisce il mapping dei ticker, asset principali, e filtri istituzionali.
Versione razionalizzata, pronta per uso.
"""
# -*- coding: utf-8 -*-

# Aggiunto il cross alla lista principale
ASSET_PRINCIPALI = ["XXBTZUSD", "XETHZUSD", "XETHXXBT"]

ASSET_MAPPING = {
    "XXBTZUSD": "XXBTZUSD",
    "XETHZUSD": "XETHZUSD",
    "XETHXXBT": "ETH/BTC"  # Mappatura corretta per Kraken
}

ASSET_CONFIG = {
    "XXBTZUSD": {"precision": 1, "leverage": 5, "min_size": 0.0001},
    "XETHZUSD": {"precision": 2, "leverage": 5, "min_size": 0.01},
    "XETHXXBT": {
        "ticker": "ETH/BTC",
        "precision": 5,       # Fondamentale per il cross
        "leverage": 3,        # Leva ridotta come da specifiche Kraken
        "min_size": 0.01
    }
}

def is_asset_supported(asset_name):
    """ Verifica se l'asset è supportato dal bot. """
    return asset_name.upper() in ASSET_MAPPING

def get_ticker(asset_name):
    """ Restituisce il ticker exchange dell'asset. """
    return ASSET_MAPPING.get(asset_name.upper(), asset_name)

def filtra_asset_istituzionali(lista_asset):
    """ Filtra la lista per asset considerati 'istituzionali'. """
    return [a for a in lista_asset if a in ASSET_PRINCIPALI]