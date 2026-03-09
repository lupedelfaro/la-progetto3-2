# -*- coding: utf-8 -*-
"""
L&A Institutional Bot - InstitutionalFilters
Filtri quantitativi di qualità asset, rischio, volatilità, volume.
Versione razionalizzata e pronta all'uso.
"""

import numpy as np

def filtro_volatilita(asset_data, soglia=0.01):
    """
    Filtra asset per volatilità minima richiesta.
    Args:
        asset_data (dict): dati con chiave asset e valori dict ('volatility', ...)
        soglia (float): soglia minima richiesta
    Returns: list - asset che superano la soglia
    """
    return [a for a, d in asset_data.items() if d.get("volatility", 0) >= soglia]

def filtro_volume(asset_data, soglia=1000000):
    """
    Filtra asset per volume minimo richiesto.
    Args:
        asset_data (dict): dati con chiave asset e valori dict ('volume', ...)
        soglia (float): soglia minima
    Returns: list - asset che superano la soglia
    """
    return [a for a, d in asset_data.items() if d.get("volume", 0) >= soglia]

def filtro_rischio(asset_data, rischi_minimi):
    """
    Filtra asset per rischio: elimina asset troppo rischiosi.
    Args:
        asset_data (dict): dati con chiave asset e valori dict ('risk', ...)
        rischi_minimi (float): rischio massimo accettato
    Returns: list - asset che rispettano i limiti
    """
    return [a for a, d in asset_data.items() if d.get("risk", 0) <= rischi_minimi]

def filtro_istituzionali(lista_asset):
    """
    Filtra asset secondo la lista istituzionali (integrata con asset_list).
    """
    from asset_list import ASSET_PRINCIPALI
    return [a for a in lista_asset if a in ASSET_PRINCIPALI]