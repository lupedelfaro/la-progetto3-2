# -*- coding: utf-8 -*-
"""
L&A Institutional Bot - AssetList
FIX: Implementazione mapping Futures e correzione Ticker per EngineLA.

CHIMERA v4.0 - Asset Configuration & Mapping
- Ticker normalization per compatibilità Kraken API
- DNA asset per Brain AI context
- Configurazione leva dinamica
- Futures mapping per funding rate e open interest
"""

# ═══════════════════════════════════════════════════════════════
# LISTA ASSET PRINCIPALI
# ═══════════════════════════════════════════════════════════════

# 🔒 RIDUZIONE A SOLO BTC (2026-05-09)
# Motivo: WR 28% sugli ultimi 50 trade su asset misti (BTC/ETH/SOL/XRP/DOGE/ZEC/BONK).
# Test pulito su singolo asset più liquido per isolare problemi di logica decisionale
# vs problemi di asset selection. ETH e SOL recenti hanno WR catastrofico (1/8 e 3/9).
# BTC ultimi 50 trade: 1W/0L (campione piccolo, ma il più liquido del mercato).
# Per ripristinare lista completa: backup in asset_list.py.BAK
ASSET_PRINCIPALI = ["XXBTZUSD"]
HIGH_BETA_ASSETS = []  # BTC non è high-beta. Nessun asset attivo high-beta in test.

# ═══════════════════════════════════════════════════════════════
# MAPPING TICKER
# ═══════════════════════════════════════════════════════════════

# Mapping per CCXT e chiamate API dirette
ASSET_MAPPING = {
    "XXBTZUSD": "BTC/USD",
    "XETHZUSD": "ETH/USD",
    "SOLUSD":   "SOL/USD",
    "XXRPZUSD": "XRP/USD",
    "XDGUSD":   "DOGE/USD",
}

# Cross-asset per analisi intermarket
CROSS_ETH_BTC = "ETH_BTC"
CROSS_BTC_USD = "BTC_USD"
CROSS_ETH_USD = "ETH_USD"
CROSS_BTC_USDT = "BTC_USDT"

CROSS_PAIRS = {
    CROSS_ETH_BTC: "ETH/BTC",
    CROSS_BTC_USD: "BTC/USD",
    CROSS_ETH_USD: "ETH/USD",
    CROSS_BTC_USDT: "BTC/USDT"
}

# Leve valide per asset su Kraken (margine)
# Usato da brain_la e trade_manager per clampare leve invalide
ALLOWED_LEVERAGES = {
    "XXBTZUSD": [2, 3, 5, 10],
    "XETHZUSD": [2, 3, 5, 10],
    "SOLUSD":   [2, 3, 5],
    "XXRPZUSD": [2, 3, 5],
    "XDGUSD":   [2, 3, 5],
    "SHIBUSD":  [2],
    "PEPEUSD":  [2],
    "BONKUSD":  [2],
    "FLOKIUSD": [2],
    "WIFUSD":   [2, 3],
}

# Mapping specifico per i Futures di Kraken (necessario per Funding/OI)
FUTURES_MAPPING = {
    "XXBTZUSD": "PI_XBTUSD",
    "XETHZUSD": "PI_ETHUSD",
    "SOLUSD":   "PF_SOLUSD",
    "XXRPZUSD": "PF_XRPUSD",
    "XDGUSD":   "PF_DOGEUSD",
    "XZECZUSD": "PF_ZECUSD",
}

# ═══════════════════════════════════════════════════════════════
# CONFIGURAZIONE ASSET (DNA + Parametri Tecnici)
# ═══════════════════════════════════════════════════════════════

ASSET_CONFIG = {
    "XXBTZUSD": {
        "precision": 1,
        "vol_precision": 8,
        "min_size": 0.0001,
        "max_leverage": 10,  # Utilizzato da TradeManager per leva dinamica
        "is_cross": False,
        "dna": "Il re del mercato. Movimenti più lenti ma direzionali. Rispetta molto bene i livelli istituzionali (FVG, Liquidity Pools). Ottimo per Swing trading. Basso rumore."
    },
    "XETHZUSD": {
        "precision": 2,
        "vol_precision": 2,
        "min_size": 0.01,
        "max_leverage": 10,
        "is_cross": False,
        "dna": "Segue BTC ma con beta maggiore (più volatile). Spesso anticipa o ritarda i movimenti delle altcoin. Buono sia per Scalping che per Swing."
    },
    "SOLUSD": {
        "precision": 2,
        "vol_precision": 2,
        "min_size": 0.1,
        "max_leverage": 5,
        "is_cross": False,
        "dna": "Altamente volatile e reattivo. Tendenza a fare 'fake breakout' (caccia agli stop) prima di partire. Ottimo per momentum e Scalping aggressivo. Il pair più scambiato su Kraken nel 2026."
    },
    "XXRPZUSD": {
        "precision": 5,
        "vol_precision": 1,
        "min_size": 10,
        "max_leverage": 5,
        "is_cross": False,
        "dna": "Spesso decorrelato dal resto del mercato. Movimenti laterali estenuanti seguiti da esplosioni improvvise di volatilità. Richiede pazienza (Swing) o reattività estrema sui breakout. Ottimo per XGBoost grazie ai pattern ripetibili."
    },
    "XDGUSD": {
        "precision": 5,
        "vol_precision": 1,
        "min_size": 50,
        "max_leverage": 5,
        "is_cross": False,
        "dna": "Meme coin con liquidità istituzionale. Movimenti da 5-15% rapidi guidati da sentiment social. Unico meme coin su Kraken con margine disponibile. Alta volatilità, richiede SL larghi e attenzione al timing di entrata. Segue BTC nei trend macro ma amplifica i movimenti."
    },
}


# ═══════════════════════════════════════════════════════════════
# MEME COINS — Asset aggiuntivi pre-configurati
# ═══════════════════════════════════════════════════════════════
# Non inclusi in ASSET_PRINCIPALI (per evitare rate limit),
# ma pre-configurati per funzionare perfettamente con auto_populate
# e con /add via Telegram.
#
# Per abilitarli nell'analisi automatica:
#   Option A — auto: imposta min_volume_usd=$500k in auto_populate()
#   Option B — manuale: /add SHIBUSD  /add PEPEUSD  ecc.
#   Option C — statico: aggiungi il ticker a ASSET_PRINCIPALI (max 3-4 tot meme)

MEME_COINS = ["SHIBUSD", "PEPEUSD", "BONKUSD", "FLOKIUSD", "WIFUSD"]

# Aggiunti al mapping ticker → ccxt symbol
ASSET_MAPPING.update({
    "SHIBUSD":  "SHIB/USD",
    "PEPEUSD":  "PEPE/USD",
    "BONKUSD":  "BONK/USD",
    "FLOKIUSD": "FLOKI/USD",
    "WIFUSD":   "WIF/USD",
})

# Aggiunti al mapping futures (solo quelli con perp su Kraken Futures)
FUTURES_MAPPING.update({
    "SHIBUSD":  "PF_SHIBUSD",
    "PEPEUSD":  "PF_PEPEUSD",
    "BONKUSD":  "PF_BONKUSD",
    "WIFUSD":   "PF_WIFUSD",
})

# Configurazione completa con DNA per Gemini
ASSET_CONFIG.update({
    "SHIBUSD": {
        "precision":     8,   # prezzo ~$0.000012 → 8 decimali
        "vol_precision": 0,   # quantità in unità intere (milioni)
        "min_size":      10_000,
        "max_leverage":  2,
        "is_cross":      False,
        "dna": (
            "Meme coin storica (Shiba Inu). Movimenti lenti in laterale interrotti da "
            "spike improvvisi di volume. Segue DOGE con lag di 30-60 min. "
            "Prezzo micro ($0.00001x) richiede 8 decimali e bin_pct 2%. "
            "Ottima per momentum breakout su volume, pessima per swing senza volume. "
            "Massima attenzione alla manipolazione social (Twitter/X)."
        ),
    },
    "PEPEUSD": {
        "precision":     8,   # prezzo ~$0.000007
        "vol_precision": 0,
        "min_size":      100_000,
        "max_leverage":  2,
        "is_cross":      False,
        "dna": (
            "Meme coin di seconda generazione (PEPE the Frog). Alta volatilità intraday "
            "con movimenti del 20-50% in poche ore. Decorrelata da BTC nel breve, "
            "segue il sentiment meme generale. Prezzo micro ($0.000007x). "
            "Meglio su timeframe 5-15 min con volume confermato. "
            "Evitare posizioni overnight — spread può ampliarsi notevolmente."
        ),
    },
    "BONKUSD": {
        "precision":     8,   # prezzo ~$0.000015
        "vol_precision": 0,
        "min_size":      100_000,
        "max_leverage":  2,
        "is_cross":      False,
        "dna": (
            "Meme coin ecosistema Solana (BONK). Movimenti legati al ciclo di SOL "
            "ma con amplificazione 3-5x. Alta liquidità nelle fasi di hype. "
            "Prezzo micro ($0.000015x) → bin_pct 2%, filtro dust attivo. "
            "Pattern tipico: accumulo silenzioso → spike +30% → correzione -20%. "
            "Correlata a SOL: se SOL pumpa, BONK tende a seguire con 15-30 min di lag."
        ),
    },
    "FLOKIUSD": {
        "precision":     8,   # prezzo ~$0.00010-0.00020
        "vol_precision": 0,
        "min_size":      10_000,
        "max_leverage":  2,
        "is_cross":      False,
        "dna": (
            "Meme coin (Floki Inu). Volatilità estrema, liquidity più bassa degli altri meme. "
            "Movimenti guidati da marketing aggressivo e listing su exchange. "
            "Prezzo sub-centesimo → 8 decimali. Spread bid/ask più alto della media. "
            "Richiede SL larghi (3-5% minimo) per evitare stop hunting. "
            "Preferire volumi >$5M/24h prima di entrare."
        ),
    },
    "WIFUSD": {
        "precision":     4,   # prezzo ~$0.50-2.00 → 4 decimali
        "vol_precision": 2,
        "min_size":      5,
        "max_leverage":  3,
        "is_cross":      False,
        "dna": (
            "Dogwifhat (WIF) — meme coin Solana di nuova generazione. "
            "Prezzo più alto degli altri meme ($0.5-2), spread più stretto. "
            "Alta correlazione con SOL in fase rialzista. "
            "Movimenti rapidi e direzionali — ottima per scalping e momentum. "
            "Rispetta meglio i livelli tecnici rispetto ai meme micro-price. "
            "Volume >$50M/24h nelle fasi attive — liquidity istituzionale presente."
        ),
    },
})

# HIGH_BETA_ASSETS include i meme con leva > 1
# 🔒 DISABILITATO durante test BTC-only (2026-05-09): commento la extend.
# Per ripristinare: rimuovere il commento dalla riga sotto.
# HIGH_BETA_ASSETS.extend(["BONKUSD", "WIFUSD"])

# ═══════════════════════════════════════════════════════════════
# LEGACY MAPPING (Asset Migrati)
# ═══════════════════════════════════════════════════════════════

LEGACY_MAPPING = {
    "MATICUSD": "POLUSD",
    "MATIC/USD": "POL/USD"
}


# ═══════════════════════════════════════════════════════════════
# REGISTRO DINAMICO (Asset aggiunti a runtime via KrakenIntegration)
# ═══════════════════════════════════════════════════════════════
# Popolato da core.kraken_integration → aggiungi_asset() / rimuovi_asset()
# Non modificare manualmente.
#
# Struttura valore:
#   { "ccxt_symbol": "BONK/USD", "precision": 8, "vol_precision": 0,
#     "min_size": 1000.0, "max_leverage": 1, "is_cross": False, "dna": "..." }

_DYNAMIC_ASSETS: dict = {}


def register_dynamic_asset(kraken_id: str, ccxt_symbol: str, market_config: dict):
    """
    Registra un asset scoperto a runtime nel registro dinamico.
    Chiamata da KrakenIntegration.aggiungi_asset().
    Dopo questa chiamata, get_ticker/get_human_name/get_config funzionano
    per l'asset esattamente come per quelli statici.
    """
    kid = kraken_id.upper().strip()
    # Garantisce che precision sia sempre int — f":.{precision}f" richiede int.
    # ccxt può restituire precision come float (es. 8.0) per alcuni exchange.
    _DYNAMIC_ASSETS[kid] = {
        "ccxt_symbol":   ccxt_symbol,
        "precision":     int(market_config.get("precision_price", 8) or 8),
        "vol_precision": int(market_config.get("precision_amount", 4) or 4),
        "min_size":      float(market_config.get("min_amount") or 1.0),
        "max_leverage":  3 if market_config.get("has_margin") else 1,
        "is_cross":      False,
        "dna": (
            f"Asset dinamico: {ccxt_symbol}. "
            f"Vol 24h: ${market_config.get('volume_24h_usd', 0):,.0f}. "
            f"Margine: {'sì' if market_config.get('has_margin') else 'no'}. "
            f"Aggiunto via watchlist dinamica CHIMERA."
        ),
    }

    # ── Futures ticker automatico ────────────────────────────────────────
    # FIX-D (2026-04-26): RIMOSSO l'auto-mapping PF_<asset>.
    # Kraken Futures NON ha un perp per ogni asset spot — mappare automaticamente
    # tutti i ticker spot a PF_<asset> generava 404 deterministici a runtime su
    # /recentliquidations, /liquidations, /openinterest per asset come RAVE.
    # Soluzione corretta: i perp vanno aggiunti SOLO esplicitamente in FUTURES_MAPPING
    # (vedi MEME_COINS update sopra). get_futures_ticker() ritornerà None per asset
    # senza perp, e i chiamanti (engine_la._get_liquidations, _get_open_interest)
    # gestiscono già None come "nessun dato disponibile".
    # Se in futuro vuoi auto-discovery, fai una HEAD request all'endpoint
    # /tickers prima di aggiungere il mapping.


def unregister_dynamic_asset(kraken_id: str):
    """Rimuove un asset dal registro dinamico. Chiamata da rimuovi_asset()."""
    _DYNAMIC_ASSETS.pop(kraken_id.upper().strip(), None)


def get_dynamic_ccxt_symbol(kraken_id: str) -> str:
    """
    Restituisce il simbolo ccxt di un asset dinamico (es. 'BONK/USD').
    Usato da EngineLA quando get_human_name non basta.
    Ritorna stringa vuota se non trovato.
    """
    return _DYNAMIC_ASSETS.get(kraken_id.upper().strip(), {}).get("ccxt_symbol", "")

# ═══════════════════════════════════════════════════════════════
# HELPER FUNCTIONS
# ═══════════════════════════════════════════════════════════════

def is_asset_supported(asset_name):
    """
    Verifica se un asset è supportato dal bot.
    
    Args:
        asset_name (str): Nome dell'asset da verificare
        
    Returns:
        bool: True se supportato, False altrimenti
    """
    if not asset_name:
        return False
    name_up = asset_name.upper().replace(" ", "").replace("/", "")
    return (name_up in ASSET_MAPPING or
            name_up in ASSET_PRINCIPALI or
            name_up in MEME_COINS or
            name_up in _DYNAMIC_ASSETS)


def get_ticker(asset_name):
    """
    Trasforma nomi umani in codici Kraken (es. BTC/USD -> XXBTZUSD)
    o viceversa, assicurando compatibilità totale con l'Engine.
    Restituisce SEMPRE il ticker ufficiale Kraken.
    
    Utilizzato da:
        - bot_la.py linea 978: `asset_list.get_ticker(asset)`
        - trade_manager.py: normalizzazione ticker
    
    Args:
        asset_name (str): Nome asset in qualsiasi formato
        
    Returns:
        str: Ticker Kraken ufficiale (es. XXBTZUSD)
    """
    if not asset_name:
        return ""
    
    name_upper = asset_name.upper().replace(" ", "").replace("/", "")
    
    # Controllo migrazioni legacy
    if name_upper in LEGACY_MAPPING:
        name_upper = LEGACY_MAPPING[name_upper]
    
    # Caso 1: È già una chiave (es. XXBTZUSD), la restituiamo
    if name_upper in ASSET_MAPPING:
        return name_upper
        
    # Caso 2: È un valore (es. BTC/USD), cerchiamo la chiave corrispondente
    for kraken_code, human_name in ASSET_MAPPING.items():
        h_norm = human_name.upper().replace("/", "")
        if h_norm == name_upper or human_name.upper() == name_upper:
            return kraken_code
            
    # Caso 3: Casi speciali comuni
    special_cases = {
        "BTC": "XXBTZUSD",
        "XBT": "XXBTZUSD",
        "ETH": "XETHZUSD",
        "XRP": "XXRPZUSD",
        "DOGE": "XDGUSD",
        "SOL": "SOLUSD"
    }
    if name_upper in special_cases:
        return special_cases[name_upper]
    
    # Caso 4: Se finisce con USD e non è nel mapping, proviamo a vedere se esiste come asset principale
    if not name_upper.endswith("USD"):
        name_with_usd = name_upper + "USD"
        if name_with_usd in ASSET_MAPPING:
            return name_with_usd

    # Caso 5: Registro dinamico (asset aggiunti a runtime via /add Telegram)
    if name_upper in _DYNAMIC_ASSETS:
        return name_upper
    name_noslash = name_upper.replace("/", "")
    if name_noslash in _DYNAMIC_ASSETS:
        return name_noslash

    return name_upper


def get_human_name(kraken_ticker):
    """
    Trasforma codici Kraken in nomi umani (es. XXBTZUSD -> BTC/USD)
    
    Args:
        kraken_ticker (str): Ticker Kraken
        
    Returns:
        str: Nome umano leggibile
    """
    if not kraken_ticker:
        return ""
    
    ticker_upper = kraken_ticker.upper()
    
    # Se il ticker è una chiave nel mapping, restituiamo il suo nome umano
    if ticker_upper in ASSET_MAPPING:
        return ASSET_MAPPING[ticker_upper]
        
    # Fallback registro dinamico: asset aggiunti a runtime
    if ticker_upper in _DYNAMIC_ASSETS:
        return _DYNAMIC_ASSETS[ticker_upper]["ccxt_symbol"]

    # Se non lo troviamo, restituiamo il ticker originale
    return ticker_upper


def get_futures_ticker(asset_name):
    """
    Restituisce il ticker per le API Futures di Kraken.
    Necessario per recuperare funding_rate e open_interest.
    
    Args:
        asset_name (str): Nome asset
        
    Returns:
        str: Ticker futures (es. PI_XBTUSD) o None se non disponibile
    """
    kraken_ticker = get_ticker(asset_name)
    return FUTURES_MAPPING.get(kraken_ticker)


def get_cross_ticker(cross_name):
    """
    Restituisce il ticker per un cross-asset specifico.
    
    Args:
        cross_name (str): Nome cross (es. CROSS_ETH_BTC)
        
    Returns:
        str: Ticker cross
    """
    return CROSS_PAIRS.get(cross_name, cross_name)


def get_config(asset_name):
    """
    Ritorna la configurazione completa dell'asset.
    Include DNA, precision, min_size, max_leverage.
    
    Args:
        asset_name (str): Nome asset
        
    Returns:
        dict: Configurazione asset o dict vuoto se non trovato
    """
    kraken_ticker = get_ticker(asset_name)
    config = ASSET_CONFIG.get(kraken_ticker)
    if config is not None:
        return config
    # Fallback registro dinamico
    return _DYNAMIC_ASSETS.get(kraken_ticker, {})


def clampa_leva(asset_name, leva_raw):
    """
    Clampa la leva al valore valido Kraken più vicino per difetto.
    Previene EOrder:Invalid leverage su apertura e chiusura posizioni.

    Esempi:
        clampa_leva("XXBTZUSD", 13) -> 10
        clampa_leva("XXBTZUSD", 4)  -> 3
        clampa_leva("SOLUSD", 7)    -> 5
        clampa_leva("XXBTZUSD", 1)  -> 1 (SPOT)
        clampa_leva("ASSETX", 7)   -> 5 (fallback [2,3,5])
    """
    try:
        leva_int = int(leva_raw or 1)
        if leva_int <= 1:
            return 1
        kraken_ticker = get_ticker(asset_name)
        allowed = ALLOWED_LEVERAGES.get(kraken_ticker, [2, 3, 5])
        validi = sorted([l for l in allowed if l <= leva_int])
        return validi[-1] if validi else 1
    except Exception:
        return 1


def get_max_leverage(asset_name):
    """
    Restituisce la leva massima consentita per un asset.
    Utilizzato da TradeManager per validazione leva dinamica.
    
    Args:
        asset_name (str): Nome asset
        
    Returns:
        int: Leva massima (default 1 se asset non trovato)
    """
    config = get_config(asset_name)
    return config.get("max_leverage", 1)


def get_min_size(asset_name):
    """
    Restituisce la size minima per un asset.
    
    Args:
        asset_name (str): Nome asset
        
    Returns:
        float: Size minima (default 0.001 se asset non trovato)
    """
    config = get_config(asset_name)
    return config.get("min_size", 0.001)


def get_dna(asset_name):
    """
    Restituisce il DNA dell'asset (caratteristiche comportamentali).
    Utilizzato da Brain AI per contestualizzare le analisi.
    
    Args:
        asset_name (str): Nome asset
        
    Returns:
        str: DNA dell'asset o stringa vuota
    """
    config = get_config(asset_name)
    return config.get("dna", "")


def filtra_asset_istituzionali(lista_asset):
    """
    Filtra una lista mantenendo solo asset istituzionali supportati.
    
    Args:
        lista_asset (list): Lista asset da filtrare
        
    Returns:
        list: Asset filtrati
    """
    return [a for a in lista_asset if a in ASSET_PRINCIPALI]


def get_all_assets(include_meme: bool = False):
    """
    Restituisce tutti gli asset principali supportati.

    Args:
        include_meme: se True, include anche MEME_COINS

    Returns:
        list: Lista asset
    """
    result = ASSET_PRINCIPALI.copy()
    if include_meme:
        for m in MEME_COINS:
            if m not in result:
                result.append(m)
    return result


def get_high_beta_assets():
    """
    Restituisce asset ad alta volatilità (beta > 1 vs BTC).
    
    Returns:
        list: Asset high-beta
    """
    return HIGH_BETA_ASSETS.copy()


def is_high_beta(asset_name):
    """
    Verifica se un asset è high-beta.
    
    Args:
        asset_name (str): Nome asset
        
    Returns:
        bool: True se high-beta
    """
    ticker = get_ticker(asset_name)
    return ticker in HIGH_BETA_ASSETS


# ═══════════════════════════════════════════════════════════════
# DEBUG & TESTING
# ═══════════════════════════════════════════════════════════════

def print_asset_info(asset_name):
    """
    Stampa tutte le informazioni disponibili su un asset.
    Utile per debug.
    
    Args:
        asset_name (str): Nome asset
    """
    ticker = get_ticker(asset_name)
    config = get_config(asset_name)
    
    print(f"\n{'='*60}")
    print(f"📊 ASSET INFO: {asset_name}")
    print(f"{'='*60}")
    print(f"Ticker Kraken:    {ticker}")
    print(f"Nome Umano:       {get_human_name(ticker)}")
    print(f"Futures Ticker:   {get_futures_ticker(asset_name) or 'N/A'}")
    print(f"High Beta:        {'✅ YES' if is_high_beta(asset_name) else '❌ NO'}")
    print(f"-"*60)
    
    if config:
        print(f"Min Size:         {config.get('min_size', 'N/A')}")
        print(f"Max Leverage:     {config.get('max_leverage', 'N/A')}x")
        print(f"Precision:        {config.get('precision', 'N/A')}")
        print(f"Vol Precision:    {config.get('vol_precision', 'N/A')}")
        print(f"-"*60)
        print(f"DNA:\n{config.get('dna', 'N/A')}")
    else:
        print("⚠️  Nessuna configurazione trovata")
    
    print(f"{'='*60}\n")


if __name__ == "__main__":
    # Test del modulo
    print("\n🧪 Testing Asset List Module...\n")
    
    # Test normalizzazione ticker
    test_cases = [
        "BTC/USD",
        "XXBTZUSD",
        "BTC",
        "eth",
        "SOL/USD",
        "DOGE"
    ]
    
    print("📋 Ticker Normalization Tests:")
    for case in test_cases:
        normalized = get_ticker(case)
        human = get_human_name(normalized)
        print(f"  {case:15} -> {normalized:15} -> {human}")
    
    print("\n" + "="*60 + "\n")
    
    # Info dettagliate per ogni asset principale
    for asset in ASSET_PRINCIPALI:
        print_asset_info(asset)
    
    print("\n✅ Test completato!")