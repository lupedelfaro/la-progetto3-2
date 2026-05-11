# -*- coding: utf-8 -*-
"""
AssetProfiles — memoria istituzionale per asset.

Modifica I (v11): un trader istituzionale che opera su BTC sa cose specifiche
di BTC che non valgono per SOL: come reagisce alle ore di apertura US, come
si comporta sui tagli round, come usa il funding rate come sentiment retail.
Per ogni asset ci sono idiosincrasie operative che la conoscenza generica
del mercato non cattura.

Questo modulo fornisce a Gemini un piccolo "memo da operatore" per ogni asset:
- Natura e ruolo nel panorama crypto
- Orari caldi (quando il flusso istituzionale è più presente)
- Pattern tipici (cose che succedono spesso su QUESTO asset)
- Cose da sapere (idiosincrasie operative)
- Note dal NightReview (lezioni empiriche dal bot stesso)

I profili sono basati su:
1. Comportamento storico noto degli asset crypto principali
2. Lezioni reali estratte dal NightReview del bot (DB chimera.db).

Quando un asset non ha un profilo dedicato, viene usato un default generico
che indica chiaramente a Gemini la mancanza di informazione specifica.
"""

import logging


# ──────────────────────────────────────────────────────────────────────────
# I PROFILI — uno per asset principale
# ──────────────────────────────────────────────────────────────────────────
# Ogni profilo è un dict con 4 chiavi semplici. Tenuto stretto (~5 righe)
# perché va dentro un prompt che ha già molti dati.

ASSET_PROFILES = {
    # ─── BTC ────────────────────────────────────────────────────────────
    'XXBTZUSD': {
        'natura': (
            "Asset di riferimento del mercato crypto. È il LEADER macro: "
            "tutti gli altri asset (specialmente le L1) lo seguono con beta variabile. "
            "Reagisce ai flussi ETF spot USA, alle decisioni Fed, ai dati macro USA. "
            "Bassa volatilità relativa rispetto alle altcoin (ATR ~1-2% del prezzo)."
        ),
        'orari_caldi': (
            "Apertura US (13:30 UTC, 14:30 ora italiana): aumenta il flusso. "
            "16:00-21:00 UTC: sessione più liquida e direzionale. "
            "Tra 02:00-08:00 UTC: spesso range stretto, evitare scalping aggressivi."
        ),
        'pattern_tipici': (
            "Round numbers (70k, 75k, 80k, 85k, 90k, 100k) sono magneti — il prezzo "
            "tende a testarli e spesso a generare reazioni. Squeeze del funding rate "
            "(funding_z > |1.5|) precede spesso inversioni nette nelle 24-48h. "
            "Dopo capitulation con vol > 3x e CVD ribaltato → bottom locale spesso "
            "confermato. Grandi liquidations 24h (>50M USD) indicano squeeze "
            "in corso — bias contrario al lato liquidato (long liquidations → "
            "possibile rimbalzo dopo lo shake; short liquidations → continuazione rialzista)."
        ),
        'cose_da_sapere': (
            "BTC è l'unico asset dove il funding rate è davvero leading: quando "
            "esplode in un senso, il prezzo si gira nei 2-3 giorni successivi. "
            "Non confondere range stretto con compressione esplosiva: BTC fa "
            "spesso settimane in range senza esplodere. Cerca conferma di volume "
            "PRIMA di credere a un breakout."
        ),
        'lezione_bot': (
            "Storico bot: due perdite consecutive in condizioni incerte → in regime "
            "non chiaro su BTC, ridurre sizing e cap voto a 7 (non FLAT automatico). "
            "BTC trenda spesso a lungo, perdere il treno è un errore quanto entrare male."
        ),
    },

    # ─── ETH ────────────────────────────────────────────────────────────
    'XETHZUSD': {
        'natura': (
            "Beta amplificato di BTC. Reagisce 1.2-1.5x alle mosse BTC sotto stress, "
            "ma in trend stabili spesso ha comportamento autonomo (narrative ETH-specific). "
            "Sensibile a: ETH ETF inflows, EIP/upgrade, attività on-chain (gas fees), "
            "movimenti di staking ETH (Coinbase, Lido)."
        ),
        'orari_caldi': (
            "Allineato a BTC sulle ore principali (13:30-21:00 UTC). "
            "Più reattivo durante annunci ETF e durante dati on-chain importanti. "
            "Notte asiatica (00:00-06:00 UTC): spesso range, evitare scalping."
        ),
        'pattern_tipici': (
            "Ratio ETH/BTC è il segnale chiave: in salita = risk-on (ETH leader), "
            "in discesa = risk-off (BTC sicuro). Lead/lag su BTC: se ETH si muove "
            "PRIMA di BTC, BTC tende a seguire entro 30-60 min. Se BTC si muove "
            "prima, ETH amplifica. Round numbers ETH: 2000, 2500, 3000, 3500 — "
            "tutti reazioni significative."
        ),
        'cose_da_sapere': (
            "ETH ha un comportamento doppio: amplificatore di BTC nei momenti di "
            "stress, asset autonomo nei momenti di calma. Determinare quale dei due "
            "modi è attivo guardando la correlazione ETH-BTC sulle ultime ore. "
            "ATR tipico: 1.5-2.5% del prezzo, più volatile di BTC."
        ),
        'lezione_bot': (
            "Storico bot: stop-loss ampi e tentativi di short in recupero hanno "
            "generato perdite significative. Su ETH evita short quando il ciclo "
            "è in RECUPERO_INIZIALE o RECUPERO_MEDIO senza conferma forte."
        ),
    },

    # ─── SOL ────────────────────────────────────────────────────────────
    'SOLUSD': {
        'natura': (
            "L1 ad alto beta. Più volatile di ETH (ATR tipico 2-4%). Sensibile a: "
            "narrativa SOL-specific (memecoin season, ecosystem updates, JUP/JTO/PYTH), "
            "movimenti delle whale on-chain, periodi di outage di rete (rari ma "
            "impattanti). In trend forte può fare 10-20% in poche ore."
        ),
        'orari_caldi': (
            "Più reattivo nelle ore US (14:00-22:00 UTC). Asia notte spesso piatta. "
            "Importante: la sessione US su SOL è dove avvengono i breakout veri."
        ),
        'pattern_tipici': (
            "Tendenza a generare grossi movimenti durante hype memecoin (BONK, WIF, "
            "POPCAT in passato). Round numbers: 100, 150, 200. Più reattivo di BTC "
            "ai movimenti di Trump/SEC/regolatori. Gap di volume tra 80-90 spesso "
            "magnetici. Se BTC scende del 2%, SOL può scendere del 4-5%."
        ),
        'cose_da_sapere': (
            "SOL è IL beta crypto. Quando vuoi cavalcare il movimento BTC, SOL "
            "te lo amplifica — ma anche quando sbagli direzione, ti fa più male. "
            "Stop più larghi rispetto a BTC (in % del prezzo). Sui pullback "
            "violenti (>5% intraday) spesso bounce rapido se BTC tiene."
        ),
        'lezione_bot': (
            "Storico bot: conflitti direzionali in recupero iniziale e operazioni "
            "non validate. Evita di entrare contro il trend di BTC su SOL — "
            "amplifica solo gli errori. SOL è da seguire, non da anticipare."
        ),
    },

    # ─── XRP ────────────────────────────────────────────────────────────
    'XXRPZUSD': {
        'natura': (
            "Asset legato strettamente alla narrativa regolatoria USA (SEC, ETF spot). "
            "Comportamento BIMODALE: lunghi periodi di range stretto (mesi), "
            "interrotti da movimenti esplosivi su news (ruling, settlement, ETF). "
            "Tra le grandi cap, è quella con la struttura più 'asimmetrica'."
        ),
        'orari_caldi': (
            "Più reattivo durante giorni di pubblicazione di sentenze/news SEC. "
            "Sessione US e early Asia (per i flussi giapponesi, dove XRP è popolare). "
            "Generalmente bassa attività in europea pura (07:00-13:00 UTC)."
        ),
        'pattern_tipici': (
            "Round numbers: 0.50, 1.00, 1.50, 2.00 — magneti psicologici fortissimi. "
            "Spike improvvisi spesso seguiti da giornate di consolidamento (rangebound). "
            "Lead pattern: prima muove a piccoli volumi, poi esplode."
        ),
        'cose_da_sapere': (
            "XRP è notoriamente difficile per scalping veloce — il book ha spesso "
            "spoofing concentrato sui livelli psicologici. Non confondere tightness "
            "del book con conviction: XRP può stare nello stesso 1% di range per ore "
            "e poi muoversi del 5% in 10 minuti. Better swing che scalping."
        ),
        'lezione_bot': (
            "Storico bot: errori di direzione e timing in fase di recupero. "
            "Su XRP la pazienza paga: è meglio perdere il primo 30% del movimento "
            "ed entrare con conferma, che anticipare e farsi whipsawed."
        ),
    },

    # ─── DOGE ───────────────────────────────────────────────────────────
    'XDGUSD': {
        'natura': (
            "Memecoin storica, alto beta crypto. Volatilità tipica simile a SOL "
            "ma più correlata al sentiment retail. Sensibile a: tweet di Musk, "
            "news Tesla/X, hype memecoin season, movimenti generali di altcoin. "
            "Liquidità decente ma inferiore a BTC/ETH."
        ),
        'orari_caldi': (
            "Sessione US, soprattutto durante ore di mercato azionario USA "
            "(14:30-21:00 UTC) — quando i retail americani sono attivi."
        ),
        'pattern_tipici': (
            "Pump-and-fade tipico: salita rapida con CVD positivo, poi inversione "
            "altrettanto rapida quando i compratori si esauriscono. Cifre tonde "
            "(0.10, 0.15, 0.20, 0.25) sono fortissime. Tendenza al double-top "
            "in zona di resistenza con violenza nello short side."
        ),
        'cose_da_sapere': (
            "DOGE è puro sentiment retail: i grossi non lo usano per portafoglio. "
            "Quindi i pattern di book pressure e iceberg sono poco affidabili "
            "(meno mani forti). Affidati di più a velocity, CVD e funding rate. "
            "Mai entrare su breakout senza conferma di volume sostenuto."
        ),
        'lezione_bot': (
            "Storico bot: performance positiva grazie a un'operazione ma WR bassa "
            "su DOGE. Implica che bisogna essere selettivi: pochi trade ad alta "
            "convinzione, no scalping continuo."
        ),
    },

    # ─── ATOMICITY (le altre altcoin tipiche) ──────────────────────────
    'POLUSD': {
        'natura': (
            "L1 a piccola/media capitalizzazione. Ex-MATIC. Volatilità medio-alta. "
            "Beta su BTC alto (1.5x+). Liquidità inferiore alle major."
        ),
        'orari_caldi': "Sessione US e annunci ecosystem (zkEVM, partnership).",
        'pattern_tipici': (
            "Sensibile al sentiment generale altcoin. Reagisce ai movimenti BTC ma "
            "con ritardo di 15-30 min e amplificazione."
        ),
        'cose_da_sapere': (
            "Liquidità inferiore = stop più larghi necessari. Slippage maggiore "
            "su ordini grossi. Evitare scalping con leva alta."
        ),
        'lezione_bot': (
            "Storico bot: eccessiva esposizione LONG in regime di mean reversion "
            "incerto ha generato perdite. Su POL aspetta confermati, no anticipi."
        ),
    },

    'ZECUSD': {
        'natura': (
            "Privacy coin. Bassissima liquidità relativa. Volatilità imprevedibile. "
            "Movimenti spesso slegati da BTC, guidati da news privacy/regolazione."
        ),
        'orari_caldi': "Sessione US per le news, altrimenti scarsamente prevedibile.",
        'pattern_tipici': (
            "Spike improvvisi di +20% su news. Long range stretti tra spike. "
            "Spread bid/ask ampi rispetto alle major."
        ),
        'cose_da_sapere': (
            "Per le sue dimensioni piccole, ZEC è facile da manipolare. "
            "Pattern di book pressure spesso fuorvianti. Operare con dimensioni piccole."
        ),
        'lezione_bot': (
            "Storico bot: tentativi LONG in compressione vicino ai massimi falliti "
            "per esaurimento trend. ZEC non sale per inerzia, sale per news."
        ),
    },
    'XZECZUSD': None,  # alias

    'TAOUSD': {
        'natura': (
            "Token AI-narrative (Bittensor). Alto beta, alta volatilità. "
            "Sensibile alla narrativa AI/ML del momento."
        ),
        'orari_caldi': "Sessione US, soprattutto su news AI/big tech earnings.",
        'pattern_tipici': "Movimenti rapidi con velocity esplosiva. Stop molto larghi.",
        'cose_da_sapere': (
            "TAO si muove più per narrativa che per fondamentali on-chain. "
            "Quando il sentiment AI è caldo, scaglioni rialzi violenti."
        ),
        'lezione_bot': (
            "Storico bot: perdite concentrate su SHORT con virtual SL e time stop, "
            "gain su LONG. Su TAO il bias rialzista è strutturale — short richiede "
            "conferma forte."
        ),
    },

    'FETUSD': {
        'natura': (
            "Token AI-narrative (Fetch.ai → ASI alliance). Comportamento simile "
            "a TAO ma con liquidità inferiore."
        ),
        'orari_caldi': "Sessione US, news AI.",
        'pattern_tipici': "Pump-and-dump frequenti. Difficile da scalpare.",
        'cose_da_sapere': "Liquidità bassa → spread ampi. Stop larghi obbligatori.",
        'lezione_bot': (
            "Storico bot: due LONG in trend rialzista persi per TP/SL non ottimali. "
            "Indica che lo SL era troppo stretto rispetto alla volatilità tipica."
        ),
    },

    'BONKUSD': {
        'natura': (
            "Memecoin Solana. Altissima volatilità (ATR può essere 5-10%). "
            "Movimenti dominati da hype + flussi memecoin season."
        ),
        'orari_caldi': "Sessione US, ma anche notte asiatica per la community SOL.",
        'pattern_tipici': "Pump enormi seguiti da dump enormi. Quasi mai in range stretto.",
        'cose_da_sapere': (
            "Su BONK il rischio è asimmetrico verso il basso: i pump si esauriscono "
            "rapidamente, i dump invece capitulano. Non comprare in cima ai breakout. "
            "Stop ampi obbligatori."
        ),
        'lezione_bot': (
            "Storico bot: performance buona su LONG allineati al ciclo di recupero, "
            "perdite su SHORT contro recupero. Su BONK il bias del ciclo è dominante."
        ),
    },

    # ─── Profili semplici per asset minor ────────────────────────────────
    'AAVEUSD': {
        'natura': "Token DeFi (lending). Beta moderato su ETH.",
        'orari_caldi': "Sessione US, news DeFi.",
        'pattern_tipici': "Reagisce a TVL DeFi e annunci protocollari.",
        'cose_da_sapere': "Liquidità decente. Comportamento più ETH-like che SOL-like.",
        'lezione_bot': (
            "Storico bot: performance negativa su swing non profittevoli. "
            "Su AAVE preferire MOMENTUM corto che swing lungo."
        ),
    },
    'DOTUSD': {
        'natura': "L1 Polkadot. Volatilità media. Beta su BTC.",
        'orari_caldi': "Sessione US.",
        'pattern_tipici': "Spesso in trend lenti, poco esplosivo.",
        'cose_da_sapere': "Asset 'noioso', preferire MOMENTUM su breakout chiari.",
        'lezione_bot': (
            "Storico bot: short compulsivi in trend avverso senza variazione di setup. "
            "Su DOT non insistere su una direzione che non funziona."
        ),
    },
    'NEARUSD': {
        'natura': "L1 alto beta. Volatilità medio-alta.",
        'orari_caldi': "Sessione US.",
        'pattern_tipici': "Movimenti correlati a BTC con amplificazione.",
        'cose_da_sapere': "Liquidità decente. Beta tipico 1.3-1.5x BTC.",
        'lezione_bot': "Singolo SHORT perso per time stop e Hurst collapse.",
    },
    'AVAXUSD': {
        'natura': "L1 medio beta. Reattivo a narrativa subnet/ecosystem.",
        'orari_caldi': "Sessione US.",
        'pattern_tipici': "Movimenti decenti su breakout, range lunghi.",
        'cose_da_sapere': "Voto IA basso storicamente — selettività richiesta.",
        'lezione_bot': "Operatività rischiosa. Preferire alta convinzione o FLAT.",
    },
    'LINKUSD': {
        'natura': "Oracle. Beta moderato. Comportamento ETH-like ma più lento.",
        'orari_caldi': "Sessione US.",
        'pattern_tipici': "Trend lenti, range estesi.",
        'cose_da_sapere': "Asset più 'tecnico' — preferisce setup MOMENTUM puliti.",
        'lezione_bot': "Operazioni singole insufficienti per pattern stabile.",
    },
    'ATOMUSD': {
        'natura': "L1 Cosmos. Beta moderato. Spesso in regime mean-reverting.",
        'orari_caldi': "Sessione US.",
        'pattern_tipici': "Range estesi, breakout poco affidabili.",
        'cose_da_sapere': "Hurst tendenzialmente sotto 0.5 = mean-reverting.",
        'lezione_bot': (
            "Tentativo SHORT MOMENTUM fallito su Hurst 0.38 (mean-reverting). "
            "Su ATOM evita momentum quando Hurst < 0.45."
        ),
    },
    'ADAUSD': {
        'natura': "L1 Cardano. Beta moderato. Movimenti spesso lenti.",
        'orari_caldi': "Sessione US.",
        'pattern_tipici': "Range lunghi, breakout su news ecosystem.",
        'cose_da_sapere': "Liquidità decente ma volatilità contenuta.",
        'lezione_bot': "Operazioni miste. Da gestire con SL stretti — virtual SL preferibile.",
    },
    'SUIUSD': {
        'natura': "L1 nuovo. Alto beta. Volatilità alta.",
        'orari_caldi': "Sessione US.",
        'pattern_tipici': "Movimenti spesso esplosivi, poco prevedibili.",
        'cose_da_sapere': "Liquidità inferiore alle L1 mature.",
        'lezione_bot': (
            "Perdite su intraday, gain su swing. Su SUI lo swing trade ha edge "
            "migliore dell'intraday."
        ),
    },
}


# Profilo di default per asset senza memo dedicato
DEFAULT_PROFILE = {
    'natura': "Asset crypto generico. Nessun profilo dedicato disponibile per questo ticker.",
    'orari_caldi': "Sessione US (13:30-21:00 UTC) tipicamente più liquida per crypto.",
    'pattern_tipici': "Comportamento dominato da correlazione con BTC. "
                      "Beta variabile, da verificare empiricamente nel ciclo corrente.",
    'cose_da_sapere': (
        "Senza profilo dedicato, opera con cautela: usa setup conservativi, "
        "preferisci MOMENTUM su trend chiari piuttosto che entry su pattern complessi. "
        "Stop più larghi e dimensionamento ridotto."
    ),
    'lezione_bot': "Nessuna lezione storica disponibile su questo asset.",
}


# ──────────────────────────────────────────────────────────────────────────
# Funzioni di accesso
# ──────────────────────────────────────────────────────────────────────────

_logger = logging.getLogger("AssetProfiles")


# Mappa alias espliciti — risolve ambiguità ticker Kraken (XZECZUSD→ZECUSD ecc.)
ASSET_ALIASES = {
    'XZECZUSD': 'ZECUSD',
}


def get_profile(asset: str) -> dict:
    """
    Ritorna il profilo dell'asset, oppure il default se non c'è memo dedicato.
    Gestisce alias espliciti tramite ASSET_ALIASES.
    """
    # Risolvi alias espliciti prima
    asset_resolved = ASSET_ALIASES.get(asset, asset)

    profile = ASSET_PROFILES.get(asset_resolved)
    if profile is None:
        # Caso 1: la chiave non esiste affatto → default
        if asset_resolved not in ASSET_PROFILES:
            return DEFAULT_PROFILE
        # Caso 2: la chiave esiste ma il valore è None → cerca alias rimuovendo prefissi Kraken
        clean = asset_resolved.replace('XX', 'X').replace('Z', '', 1) if asset_resolved.startswith('XX') else asset_resolved
        if clean in ASSET_PROFILES and ASSET_PROFILES[clean] is not None:
            return ASSET_PROFILES[clean]
        return DEFAULT_PROFILE
    return profile


def format_for_prompt(asset: str) -> str:
    """
    Produce il blocco testuale del profilo asset per il prompt di Gemini.
    Stretto: ~6-8 righe in totale.
    """
    p = get_profile(asset)
    is_default = (p is DEFAULT_PROFILE)
    prefix = "PROFILO ASSET (memoria istituzionale)" if not is_default else \
             "PROFILO ASSET (generico — nessun memo dedicato disponibile)"

    block = (
        f"═══════════════════════════════════════════════════════════════\n"
        f"{prefix} — {asset}\n"
        f"═══════════════════════════════════════════════════════════════\n"
        f"  • Natura: {p['natura']}\n"
        f"  • Orari caldi: {p['orari_caldi']}\n"
        f"  • Pattern tipici: {p['pattern_tipici']}\n"
        f"  • Cose da sapere: {p['cose_da_sapere']}\n"
        f"  • Lezione storica del bot: {p['lezione_bot']}\n"
    )
    return block


def has_dedicated_profile(asset: str) -> bool:
    """True se l'asset ha un profilo dedicato (non default)."""
    p = get_profile(asset)
    return p is not DEFAULT_PROFILE


def list_known_assets() -> list:
    """Ritorna la lista di asset con profilo dedicato (utile per debug/diagnostica)."""
    return [k for k, v in ASSET_PROFILES.items() if v is not None]
