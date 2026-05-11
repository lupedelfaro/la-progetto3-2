# -*- coding: utf-8 -*-
"""
CHIMERA v4.0 — kraken_integration.py
Modulo di integrazione nativa completa con Kraken Exchange.

Funzionalità:
  - Discovery dinamica di tutti i mercati Kraken
  - Screener multi-criterio (volume, momentum, spread, margine)
  - Watchlist dinamica persistente (runtime, senza toccare asset_list.py)
  - Analisi mercato: OHLCV, orderbook, spread, trade recenti
  - Piazzamento ordini manuale: market, limit, stop-loss
  - Comandi Telegram: /markets /screener /watchlist /order /balance /kraken

INTEGRAZIONE in bot_la.py:
  1. from core.kraken_integration import KrakenIntegration
  2. kraken_int = KrakenIntegration(performer=performer, alerts=alerts)
  3. Aggiungere nel blocco comandi Telegram la chiamata:
       kraken_int.gestisci_comando_telegram(cmd, ...)
  4. Per usare la watchlist dinamica come lista asset:
       assets_loop = kraken_int.get_watchlist() or al_config.ASSET_PRINCIPALI

NOTA: Non modifica asset_list.py né alcun modulo esistente.
"""

import logging
import time
import json
import os
import ccxt
from datetime import datetime, timezone
from typing import Optional

try:
    from core.chimera_errors import ErrorTracker
except ImportError:
    # Stub se il modulo non è ancora disponibile
    class ErrorTracker:
        def __init__(self, name): pass
        def capture(self, e, method, ctx=None): pass

try:
    from core import config_la
    from core import asset_list as al_config
    from core.asset_list import (
        register_dynamic_asset,
        unregister_dynamic_asset,
        get_dynamic_ccxt_symbol,
        MEME_COINS,
    )
except ImportError:
    config_la = None
    al_config = None
    MEME_COINS = []
    def register_dynamic_asset(*a, **kw): pass
    def unregister_dynamic_asset(*a, **kw): pass
    def get_dynamic_ccxt_symbol(*a, **kw): return ""


# ═══════════════════════════════════════════════════════════════════════════════
# COSTANTI
# ═══════════════════════════════════════════════════════════════════════════════

WATCHLIST_FILE = os.path.join(
    os.path.dirname(os.path.abspath(__file__)), '..', 'chimera_watchlist.json'
)

# Soglie default screener
DEFAULT_MIN_VOLUME_USD     = 500_000      # volume 24h minimo per considerare l'asset
DEFAULT_MIN_VOLUME_MARGIN  = 1_000_000   # soglia più alta per asset a margine
DEFAULT_MAX_SPREAD_PCT     = 0.15        # spread massimo in % per liquidità accettabile
DEFAULT_TOP_N              = 10          # quanti asset restituisce lo screener di default
MARKETS_CACHE_TTL          = 300         # secondi di validità cache mercati (5 min)


# ═══════════════════════════════════════════════════════════════════════════════
# CLASSE PRINCIPALE
# ═══════════════════════════════════════════════════════════════════════════════

class KrakenIntegration:
    """
    Wrapper completo Kraken API per CHIMERA.
    Usa la stessa connessione ccxt di PerformerLA/EngineLA ma aggiunge:
      - discovery e screening di tutti i mercati
      - watchlist dinamica persistente
      - ordini manuali via Telegram
    """

    def __init__(self, performer=None, alerts=None):
        """
        Args:
            performer: istanza PerformerLA — se passata, riusa il suo exchange
                       invece di crearne uno nuovo (evita connessioni doppie)
            alerts:    istanza TelegramAlerts — per inviare risposte ai comandi
        """
        self.logger = logging.getLogger("KrakenIntegration")
        _err = ErrorTracker("KrakenIntegration")
        self._err = _err
        self.alerts = alerts

        # ── Exchange: riusa quello di PerformerLA se disponibile ────────────
        if performer and hasattr(performer, 'exchange'):
            self.exchange = performer.exchange
            self.logger.info("✅ KrakenIntegration: riuso exchange di PerformerLA")
        else:
            api_key    = getattr(config_la, 'KRAKEN_KEY', '') if config_la else ''
            api_secret = getattr(config_la, 'KRAKEN_SECRET', '') if config_la else ''
            self.exchange = ccxt.kraken({
                'apiKey': api_key,
                'secret': api_secret,
                'enableRateLimit': True,
                'timeout': 30000,
                'options': {'fetchTradesWarning': False},
            })
            self.logger.info("✅ KrakenIntegration: nuovo exchange ccxt.kraken inizializzato")

        # ── Cache mercati ────────────────────────────────────────────────────
        self._markets_cache:  list = []
        self._tickers_cache:  dict = {}
        self._markets_ts:     float = 0.0

        # ── Watchlist dinamica ───────────────────────────────────────────────
        self._watchlist: list = self._carica_watchlist()  # manuale (da JSON)
        self._auto_watchlist: list = []                   # automatica (in RAM, non persiste)
        self._auto_refresh_ts: float = 0.0
        self._auto_refresh_ttl: float = 6 * 3600         # refresh auto ogni 6 ore
        # Reidrata il dynamic registry di asset_list al riavvio del bot.
        if self._watchlist:
            self._reidrata_registro()

    # ═══════════════════════════════════════════════════════════════════════════
    # WATCHLIST DINAMICA
    # ═══════════════════════════════════════════════════════════════════════════

    def _carica_watchlist(self) -> list:
        """Carica watchlist da file JSON. Ritorna lista vuota se non esiste."""
        try:
            if os.path.exists(WATCHLIST_FILE):
                with open(WATCHLIST_FILE, 'r') as f:
                    data = json.load(f)
                    wl = data.get('watchlist', [])
                    self.logger.info(f"📋 Watchlist caricata: {wl}")
                    return wl
        except Exception as e:
            self._err.capture(e, "_carica_watchlist", {"file": WATCHLIST_FILE})
            self.logger.error(f"❌ Errore caricamento watchlist: {e}")
        return []

    def _salva_watchlist(self):
        """Salva watchlist corrente su file JSON."""
        try:
            with open(WATCHLIST_FILE, 'w') as f:
                json.dump({
                    'watchlist': self._watchlist,
                    'aggiornato': datetime.now(timezone.utc).isoformat()
                }, f, indent=2)
        except Exception as e:
            self._err.capture(e, "_salva_watchlist", {})
            self.logger.error(f"❌ Errore salvataggio watchlist: {e}")

    def get_watchlist(self) -> list:
        """
        Ritorna la lista completa di asset da analizzare nel ciclo principale.
        Ordine di priorità (deduplicata):
          1. Lista statica in asset_list.ASSET_PRINCIPALI (sempre inclusa)
          2. Auto-watchlist: top N per volume, aggiornata ogni 6 ore
          3. Watchlist manuale: asset aggiunti via /add Telegram (persistente)
        """
        static   = list(al_config.ASSET_PRINCIPALI) if al_config else []
        combined = list(static)
        for a in (self._auto_watchlist + self._watchlist):
            if a not in combined:
                combined.append(a)
        return combined

    def aggiungi_asset(self, kraken_ticker: str) -> tuple:
        """
        Aggiunge un asset alla watchlist dinamica.
        kraken_ticker: es. 'ADAUSD', 'LINKUSD', 'AVAXUSD'
        Returns: (success: bool, messaggio: str)
        """
        ticker = kraken_ticker.upper().strip()
        if ticker in self._watchlist:
            return False, f"⚠️ `{ticker}` è già nella watchlist."

        # Verifica che l'asset esista su Kraken
        markets = self.get_all_markets(min_volume_usd=0)
        kraken_ids = [m['kraken_id'] for m in markets]
        ccxt_symbols = [m['symbol'].replace('/', '') for m in markets]
        all_known = set(kraken_ids + ccxt_symbols + [m['symbol'] for m in markets])

        # Normalizza: accetta sia 'ADA/USD' che 'ADAUSD' che 'XADAZUSD'
        found = None
        for m in markets:
            if (ticker == m['kraken_id'] or
                ticker == m['symbol'].replace('/', '') or
                ticker == m['symbol'] or
                ticker.replace('/', '') == m['symbol'].replace('/', '')):
                found = m
                break

        if not found:
            return False, (
                f"❌ `{ticker}` non trovato sui mercati Kraken.\n"
                f"Usa /markets per vedere tutti gli asset disponibili."
            )

        # Usa il kraken_id come formato interno (compatibile con ccxt Kraken)
        id_interno = found['kraken_id']
        self._watchlist.append(id_interno)
        self._salva_watchlist()

        # Registra nel dynamic registry → get_ticker/get_human_name/get_config
        # funzioneranno per questo asset in tutto il bot
        try:
            register_dynamic_asset(id_interno, found['symbol'], found)
            self.logger.info(f"📋 Dynamic registry: {id_interno} → {found['symbol']}")
        except Exception as e_reg:
            _err.capture(e_reg, "aggiungi_asset", {"module": "KrakenIntegration"})
            self.logger.warning(f"⚠️ register_dynamic_asset({id_interno}): {e_reg}")

        # Formato prezzo adattivo (BONK ~$0.000015 → 8 decimali)
        _p = found['price']
        _pfmt = (f"${_p:.8f}" if _p < 0.001 else
                 f"${_p:.6f}" if _p < 1 else f"${_p:,.4f}")

        return True, (
            f"✅ `{id_interno}` aggiunto alla watchlist.\n"
            f"Vol 24h: ${found['volume_24h_usd']:,.0f} | "
            f"Prezzo: {_pfmt} | "
            f"Margine: {'✅' if found['has_margin'] else '❌'}"
        )

    def rimuovi_asset(self, kraken_ticker: str) -> tuple:
        """
        Rimuove un asset dalla watchlist dinamica.
        Returns: (success: bool, messaggio: str)
        """
        ticker = kraken_ticker.upper().strip()
        if ticker in self._watchlist:
            self._watchlist.remove(ticker)
            self._salva_watchlist()
            try: unregister_dynamic_asset(ticker)
            except Exception: pass
            return True, f"🗑️ `{ticker}` rimosso dalla watchlist."

        # Prova match parziale
        for item in self._watchlist:
            if ticker in item or item in ticker:
                self._watchlist.remove(item)
                self._salva_watchlist()
                try: unregister_dynamic_asset(item)
                except Exception: pass
                return True, f"🗑️ `{item}` rimosso dalla watchlist."

        return False, f"⚠️ `{ticker}` non trovato nella watchlist."

    def reset_watchlist(self):
        """Svuota la watchlist dinamica (torna alla lista statica)."""
        self._watchlist = []
        self._salva_watchlist()

    def auto_populate(self, top_n: int = 10, min_volume_usd: float = 2_000_000,
                      margin_only: bool = False, force: bool = False) -> list:
        """
        Scopre e registra automaticamente i top N asset Kraken per volume 24h.
        Chiamata al startup del bot e ogni 6 ore (refresh automatico).

        NON richiede /add Telegram — il bot analizza autonomamente i migliori asset.

        Args:
            top_n         : numero massimo di asset da aggiungere (default 20)
            min_volume_usd: volume 24h minimo in USD (default $1M)
            margin_only   : se True, solo asset tradabili con leva
            force         : ignora il TTL di 6 ore e forza il refresh

        Returns:
            Lista degli asset nella _auto_watchlist dopo il popolamento.
        """
        now = time.time()
        # Rispetta il TTL a meno che non sia forzato
        if not force and (now - self._auto_refresh_ts) < self._auto_refresh_ttl:
            return list(self._auto_watchlist)

        try:
            self.logger.info(f"🔭 Auto-populate: cerco top {top_n} asset Kraken "
                             f"(vol>${min_volume_usd/1e6:.0f}M, "
                             f"margine={'sì' if margin_only else 'no'})...")

            markets = self.get_all_markets(min_volume_usd=min_volume_usd,
                                           force_refresh=True)

            if margin_only:
                markets = [m for m in markets if m.get('has_margin')]

            # ── Blacklist asset non tradabili ────────────────────────────
            # Stablecoin (peg a $1 o altra valuta fiat): volume alto ma
            # nessun edge di trading — escluse sempre dall'auto-populate.
            # Aggiungere altri ticker se necessario.
            BLACKLIST = {
                "USDTZUSD",  # Tether
                "USDCUSD",   # USD Coin
                "DAIUSD",    # DAI
                "PAXUSD",    # PAX
                "TBTCUSD",   # Wrapped BTC (non il vero BTC)
                "WBTCUSD",   # Wrapped BTC
                "ZEURZUSD",  # EUR stablecoin
                "ZGBPZUSD",  # GBP stablecoin
                "XCADZUSD",  # CAD stablecoin
                "PYUSD",     # PayPal USD
                "EURCUSD",   # Circle EUR
            }

            # Filtra: no blacklist, prezzo > 0, spread ragionevole
            markets_filtrati = [
                m for m in markets
                if m["kraken_id"] not in BLACKLIST
                and m["price"] > 0
                and m["spread_pct"] < 2.0        # spread < 2% = minima liquidità
                and abs(m["change_24h_pct"]) < 80 # scarta pump estremi anomali
            ]

            # Prendi i top N (già ordinati per volume decrescente)
            top_markets = markets_filtrati[:top_n]

            # Ottieni lista statica da non duplicare inutilmente
            static = set(al_config.ASSET_PRINCIPALI) if al_config else set()

            nuovi, aggiornati = 0, 0
            nuova_auto = []

            for m in top_markets:
                kid    = m['kraken_id']       # es. "BONKUSD"
                symbol = m['symbol']          # es. "BONK/USD"

                nuova_auto.append(kid)

                # Registra nel dynamic registry (overwrite se già presente)
                try:
                    register_dynamic_asset(kid, symbol, m)
                    if kid not in (static | set(self._auto_watchlist)):
                        nuovi += 1
                    else:
                        aggiornati += 1
                except Exception as e_r:
                    _err.capture(e_r, "auto_populate", {"module": "KrakenIntegration"})
                    self.logger.warning(f"⚠️ auto_populate register {kid}: {e_r}")

            self._auto_watchlist = nuova_auto
            self._auto_refresh_ts = now

            nuovi_non_statici = [a for a in nuova_auto if a not in static]
            n_esclusi = len(markets) - len(markets_filtrati)
            self.logger.info(
                f"✅ Auto-populate completato: {len(nuova_auto)} asset totali "
                f"({len(nuovi_non_statici)} oltre ai 5 statici). "
                f"Nuovi: {nuovi} | Aggiornati: {aggiornati} | "
                f"Esclusi (stablecoin/spread): {n_esclusi}"
            )

            if self.alerts and nuovi > 0:
                extra = [a for a in nuovi_non_statici
                         if a not in set(self._watchlist)][:5]
                if extra:
                    righe = "\n".join(f"  \u2022 `{a}`" for a in extra)
                    coda  = f"\n  _...e altri {len(nuovi_non_statici)-5}_" if len(nuovi_non_statici) > 5 else ""
                    self.alerts.invia_alert(
                        f"\U0001f52d *Auto-populate CHIMERA*\n"
                        f"Aggiunti {nuovi} nuovi asset all'analisi automatica:\n"
                        + righe + coda
                    )

        except Exception as e:
            self._err.capture(e, "auto_populate",
                              {"top_n": top_n, "margin_only": margin_only})
            self.logger.error(f"❌ auto_populate fallito: {e}")

        return list(self._auto_watchlist)

    def necessita_refresh_auto(self) -> bool:
        """True se è ora di aggiornare la auto-watchlist (ogni 6 ore)."""
        return (time.time() - self._auto_refresh_ts) >= self._auto_refresh_ttl

    def _reidrata_registro(self):
        """
        Ricostruisce _DYNAMIC_ASSETS in asset_list per:
          1. Asset nella watchlist manuale (JSON)
          2. Meme coin pre-configurate (MEME_COINS in asset_list)
        Chiamata al riavvio del bot — i metadati vengono recuperati da Kraken.
        """
        try:
            markets = self.get_all_markets(min_volume_usd=0)
            if not markets:
                # get_all_markets ha fallito (Kraken instabile all'avvio) — skip silenzioso
                # Il registro verrà reidratato al prossimo ciclo auto-populate
                self.logger.debug("🔄 Reidratazione skippata: mercati non disponibili (Kraken instabile)")
                return
            mkt_by_id = {m["kraken_id"]: m for m in markets}

            # Asset in watchlist manuale
            da_reidratare = list(self._watchlist)

            # Meme coin pre-configurate
            for kid in MEME_COINS:
                if kid not in da_reidratare:
                    da_reidratare.append(kid)

            non_trovati = []
            for kraken_id in da_reidratare:
                found = mkt_by_id.get(kraken_id)
                if found:
                    register_dynamic_asset(kraken_id, found["symbol"], found)
                    self.logger.debug(f"🔄 Reidratato: {kraken_id} → {found['symbol']}")
                else:
                    non_trovati.append(kraken_id)

            if non_trovati:
                # Logga in un unico warning invece di uno per asset
                self.logger.debug(
                    f"ℹ️ Non trovati su Kraken (watchlist/meme): {', '.join(non_trovati)}"
                )
        except Exception as e:
            _err.capture(e, "_reidrata_registro", {"module": "KrakenIntegration"})
            self.logger.warning(f"⚠️ Reidratazione registro dinamico fallita: {e}")

    # ═══════════════════════════════════════════════════════════════════════════
    # DISCOVERY MERCATI
    # ═══════════════════════════════════════════════════════════════════════════

    def get_all_markets(self,
                        quote_currency: str = 'USD',
                        min_volume_usd: float = DEFAULT_MIN_VOLUME_USD,
                        force_refresh: bool = False) -> list:
        """
        Recupera tutti i mercati spot Kraken denominati in USD con volume minimo.
        Risultato ordinato per volume 24h decrescente.
        Cache 5 minuti per non colpire il rate limit.

        Returns:
            Lista di dict con: symbol, kraken_id, base, price, volume_24h_usd,
            change_24h_pct, bid, ask, spread_pct, has_margin,
            precision_price, precision_amount, min_amount
        """
        now = time.time()
        cache_valida = (
            self._markets_cache and
            (now - self._markets_ts) < MARKETS_CACHE_TTL and
            not force_refresh
        )
        if cache_valida:
            if min_volume_usd > 0:
                return [m for m in self._markets_cache if m['volume_24h_usd'] >= min_volume_usd]
            return list(self._markets_cache)

        import ccxt
        for _attempt in range(3):
            try:
                self.logger.info("🔍 Caricamento mercati Kraken in corso...")
                markets = self.exchange.load_markets(reload=True)
                tickers = self.exchange.fetch_tickers()

                result = []
                for symbol, market in markets.items():
                    if not symbol.endswith(f'/{quote_currency}'):
                        continue
                    if not market.get('spot', False):
                        continue

                    ticker = tickers.get(symbol, {})
                    vol_usd = float(ticker.get('quoteVolume', 0) or 0)

                    bid   = float(ticker.get('bid',  0) or 0)
                    ask   = float(ticker.get('ask',  0) or 0)
                    price = float(ticker.get('last', 0) or 0)
                    mid   = (bid + ask) / 2 if (bid and ask) else price
                    spread_pct = ((ask - bid) / mid * 100) if mid > 0 else 0.0

                    result.append({
                        'symbol':          symbol,
                        'kraken_id':       market.get('id', ''),
                        'base':            market.get('base', ''),
                        'quote':           market.get('quote', ''),
                        'volume_24h_usd':  vol_usd,
                        'price':           price,
                        'change_24h_pct':  float(ticker.get('percentage', 0) or 0),
                        'bid':             bid,
                        'ask':             ask,
                        'spread_pct':      round(spread_pct, 4),
                        'has_margin':      market.get('margin', False),
                        'precision_price': market.get('precision', {}).get('price', 2),
                        'precision_amount':market.get('precision', {}).get('amount', 8),
                        'min_amount':      (market.get('limits', {}) or {}).get('amount', {}).get('min', 0),
                    })

                result.sort(key=lambda x: x['volume_24h_usd'], reverse=True)
                self._markets_cache = result
                self._tickers_cache = tickers
                self._markets_ts    = now
                self.logger.info(f"✅ {len(result)} mercati USD trovati su Kraken")
                break  # successo

            except (ccxt.NetworkError, ccxt.RequestTimeout) as e:
                _err.capture(e, "get_all_markets", {"module": "KrakenIntegration"})
                _wait = 15 * (_attempt + 1)
                if _attempt < 2:
                    self.logger.warning(
                        f"⚠️ get_all_markets timeout (tentativo {_attempt+1}/3): "
                        f"attendo {_wait}s... (Kraken instabile)"
                    )
                    time.sleep(_wait)
                    result = self._markets_cache or []  # usa cache mentre aspetta
                else:
                    self.logger.warning(
                        f"⚠️ Mercati Kraken non disponibili dopo 3 tentativi — uso cache"
                    )
                    result = self._markets_cache or []
            except Exception as e:
                self._err.capture(e, "get_all_markets", {"quote": quote_currency})
                self.logger.error(f"❌ Errore get_all_markets: {e}")
                result = self._markets_cache or []
                break

        if min_volume_usd > 0:
            return [m for m in result if m['volume_24h_usd'] >= min_volume_usd]
        return list(result)

    def get_margin_markets(self, min_volume_usd: float = DEFAULT_MIN_VOLUME_MARGIN) -> list:
        """Solo mercati con margine disponibile (tradabili con leva)."""
        markets = self.get_all_markets(min_volume_usd=min_volume_usd)
        return [m for m in markets if m.get('has_margin')]

    # ═══════════════════════════════════════════════════════════════════════════
    # SCREENER MULTI-CRITERIO
    # ═══════════════════════════════════════════════════════════════════════════

    def screen_top_movers(self, top_n: int = DEFAULT_TOP_N,
                          min_volume_usd: float = DEFAULT_MIN_VOLUME_USD) -> list:
        """
        Top N asset per variazione % assoluta nelle ultime 24h.
        Identifica asset con forte momentum (sia long che short).
        """
        markets = self.get_all_markets(min_volume_usd=min_volume_usd)
        return sorted(markets, key=lambda x: abs(x['change_24h_pct']), reverse=True)[:top_n]

    def screen_top_volume(self, top_n: int = DEFAULT_TOP_N,
                          min_volume_usd: float = 0) -> list:
        """Top N asset per volume 24h (già ordinati per default)."""
        markets = self.get_all_markets(min_volume_usd=min_volume_usd)
        return markets[:top_n]

    def screen_low_spread(self, max_spread_pct: float = DEFAULT_MAX_SPREAD_PCT,
                          min_volume_usd: float = DEFAULT_MIN_VOLUME_USD) -> list:
        """Asset con spread bid/ask basso = buona liquidità per operatività."""
        markets = self.get_all_markets(min_volume_usd=min_volume_usd)
        return [m for m in markets if 0 < m['spread_pct'] <= max_spread_pct]

    def screen_margin_top(self, top_n: int = DEFAULT_TOP_N) -> list:
        """
        Top N asset a margine ordinati per volume.
        Utile per identificare nuovi asset da aggiungere alla watchlist.
        """
        return self.get_margin_markets()[:top_n]

    def screen_bullish_momentum(self, min_change_pct: float = 3.0,
                                 min_volume_usd: float = DEFAULT_MIN_VOLUME_USD) -> list:
        """Asset con rialzo > min_change_pct nelle ultime 24h."""
        markets = self.get_all_markets(min_volume_usd=min_volume_usd)
        candidates = [m for m in markets if m['change_24h_pct'] >= min_change_pct]
        return sorted(candidates, key=lambda x: x['change_24h_pct'], reverse=True)

    def screen_bearish_momentum(self, min_drop_pct: float = 3.0,
                                 min_volume_usd: float = DEFAULT_MIN_VOLUME_USD) -> list:
        """Asset con ribasso > min_drop_pct nelle ultime 24h."""
        markets = self.get_all_markets(min_volume_usd=min_volume_usd)
        candidates = [m for m in markets if m['change_24h_pct'] <= -min_drop_pct]
        return sorted(candidates, key=lambda x: x['change_24h_pct'])

    def screener_completo(self, top_n: int = 5) -> dict:
        """
        Esegue tutti gli screener e restituisce un report unificato.
        Usato dal comando Telegram /screener.
        """
        return {
            'top_volume':   self.screen_top_volume(top_n),
            'top_movers':   self.screen_top_movers(top_n),
            'margin_top':   self.screen_margin_top(top_n),
            'bullish':      self.screen_bullish_momentum()[:top_n],
            'bearish':      self.screen_bearish_momentum()[:top_n],
            'low_spread':   self.screen_low_spread()[:top_n],
        }

    # ═══════════════════════════════════════════════════════════════════════════
    # ANALISI MERCATO
    # ═══════════════════════════════════════════════════════════════════════════

    def get_ohlcv(self, symbol: str, timeframe: str = '1h', limit: int = 200) -> list:
        """
        OHLCV storico. Timeframe: 1m, 5m, 15m, 30m, 1h, 4h, 1d, 1w.
        Returns: list of [timestamp, open, high, low, close, volume]
        """
        try:
            return self.exchange.fetch_ohlcv(symbol, timeframe=timeframe, limit=limit)
        except Exception as e:
            self._err.capture(e, "get_ohlcv", {"symbol": symbol, "tf": timeframe})
            self.logger.error(f"❌ OHLCV {symbol} {timeframe}: {e}")
            return []

    def get_orderbook(self, symbol: str, depth: int = 20) -> dict:
        """
        Order book con bid/ask levels e calcolo imbalance.
        imbalance > 0 = pressione buy, < 0 = pressione sell.
        """
        try:
            ob = self.exchange.fetch_order_book(symbol, limit=depth)
            bids = ob.get('bids', [])
            asks = ob.get('asks', [])

            bid_vol = sum(b[1] for b in bids) if bids else 0
            ask_vol = sum(a[1] for a in asks) if asks else 0
            total   = bid_vol + ask_vol
            imbalance = (bid_vol - ask_vol) / total if total > 0 else 0

            return {
                'bids':       bids,
                'asks':       asks,
                'best_bid':   bids[0][0] if bids else 0,
                'best_ask':   asks[0][0] if asks else 0,
                'bid_volume': bid_vol,
                'ask_volume': ask_vol,
                'imbalance':  round(imbalance, 4),
                'spread':     (asks[0][0] - bids[0][0]) if (bids and asks) else 0,
            }
        except Exception as e:
            self._err.capture(e, "get_orderbook", {"symbol": symbol})
            self.logger.error(f"❌ OrderBook {symbol}: {e}")
            return {}

    def get_recent_trades(self, symbol: str, limit: int = 50) -> list:
        """Ultimi N trade eseguiti. Utile per CVD manuale e analisi flusso."""
        try:
            trades = self.exchange.fetch_trades(symbol, limit=limit)
            return [{
                'timestamp': t['timestamp'],
                'datetime':  t['datetime'],
                'side':      t['side'],
                'price':     t['price'],
                'amount':    t['amount'],
                'cost':      t['cost'],
            } for t in trades]
        except Exception as e:
            self._err.capture(e, "get_recent_trades", {"symbol": symbol})
            self.logger.error(f"❌ Recent trades {symbol}: {e}")
            return []

    def get_ticker_full(self, symbol: str) -> dict:
        """Ticker completo con spread, variazione, volumi."""
        try:
            t     = self.exchange.fetch_ticker(symbol)
            bid   = float(t.get('bid',  0) or 0)
            ask   = float(t.get('ask',  0) or 0)
            price = float(t.get('last', 0) or 0)
            mid   = (bid + ask) / 2 if (bid and ask) else price
            spread_pct = ((ask - bid) / mid * 100) if mid > 0 else 0
            return {
                'symbol':        symbol,
                'price':         price,
                'bid':           bid,
                'ask':           ask,
                'spread_pct':    round(spread_pct, 4),
                'volume_24h':    float(t.get('quoteVolume', 0) or 0),
                'change_24h_pct':float(t.get('percentage', 0) or 0),
                'high_24h':      float(t.get('high', 0) or 0),
                'low_24h':       float(t.get('low',  0) or 0),
                'vwap_24h':      float(t.get('vwap', 0) or 0),
            }
        except Exception as e:
            self._err.capture(e, "get_ticker_full", {"symbol": symbol})
            self.logger.error(f"❌ Ticker {symbol}: {e}")
            return {}

    # ═══════════════════════════════════════════════════════════════════════════
    # PIAZZAMENTO ORDINI
    # ═══════════════════════════════════════════════════════════════════════════

    def place_market_order(self, symbol: str, side: str,
                           amount: float, leverage: int = 1) -> dict:
        """
        Piazza un ordine market (esecuzione immediata al prezzo corrente).

        Args:
            symbol:   es. 'BTC/USD', 'ETH/USD'
            side:     'buy' o 'sell'
            amount:   quantità in unità base (es. 0.001 per BTC)
            leverage: 1 = spot, >1 = margine (richiede account margin abilitato)

        Returns:
            dict con id, status, filled, average, cost — oppure {'error': msg}
        """
        try:
            params = {}
            if leverage > 1:
                params['leverage'] = leverage

            order = self.exchange.create_market_order(symbol, side, amount, params=params)
            self.logger.info(
                f"✅ MARKET {side.upper()} {amount} {symbol} | "
                f"leva={leverage}x | ID={order.get('id')}"
            )
            return self._normalizza_ordine(order)

        except Exception as e:
            self._err.capture(e, "place_market_order",
                              {"symbol": symbol, "side": side, "amount": amount})
            self.logger.error(f"❌ Market order {side} {symbol}: {e}")
            return {'error': str(e)}

    def place_limit_order(self, symbol: str, side: str,
                          amount: float, price: float, leverage: int = 1) -> dict:
        """
        Piazza un ordine limit (si esegue solo al prezzo specificato o migliore).

        Args:
            price: prezzo limite in USD
        """
        try:
            params = {}
            if leverage > 1:
                params['leverage'] = leverage

            order = self.exchange.create_limit_order(symbol, side, amount, price, params=params)
            self.logger.info(
                f"✅ LIMIT {side.upper()} {amount} {symbol} @ {price} | "
                f"leva={leverage}x | ID={order.get('id')}"
            )
            return self._normalizza_ordine(order)

        except Exception as e:
            self._err.capture(e, "place_limit_order",
                              {"symbol": symbol, "side": side, "price": price})
            self.logger.error(f"❌ Limit order {side} {symbol}: {e}")
            return {'error': str(e)}

    def place_stop_loss_order(self, symbol: str, side: str,
                               amount: float, stop_price: float,
                               leverage: int = 1) -> dict:
        """
        Piazza un ordine stop-loss market su Kraken.
        Quando il prezzo tocca stop_price, esegue a mercato.

        Args:
            side:       'sell' per proteggere un long, 'buy' per proteggere uno short
            stop_price: prezzo di trigger
        """
        try:
            params = {'stopPrice': stop_price}
            if leverage > 1:
                params['leverage'] = leverage

            order = self.exchange.create_order(
                symbol, 'stop-loss', side, amount, stop_price, params=params
            )
            self.logger.info(
                f"🛑 STOP-LOSS {side.upper()} {amount} {symbol} "
                f"@ trigger={stop_price} | ID={order.get('id')}"
            )
            return self._normalizza_ordine(order)

        except Exception as e:
            self._err.capture(e, "place_stop_loss_order",
                              {"symbol": symbol, "stop_price": stop_price})
            self.logger.error(f"❌ Stop-loss {side} {symbol}: {e}")
            return {'error': str(e)}

    def place_take_profit_order(self, symbol: str, side: str,
                                 amount: float, tp_price: float,
                                 leverage: int = 1) -> dict:
        """
        Piazza un ordine take-profit limit su Kraken.
        """
        try:
            params = {}
            if leverage > 1:
                params['leverage'] = leverage

            order = self.exchange.create_limit_order(
                symbol, side, amount, tp_price, params=params
            )
            self.logger.info(
                f"🎯 TAKE-PROFIT {side.upper()} {amount} {symbol} "
                f"@ {tp_price} | ID={order.get('id')}"
            )
            return self._normalizza_ordine(order)

        except Exception as e:
            self._err.capture(e, "place_take_profit_order",
                              {"symbol": symbol, "tp_price": tp_price})
            self.logger.error(f"❌ Take-profit {side} {symbol}: {e}")
            return {'error': str(e)}

    def cancel_order(self, order_id: str, symbol: Optional[str] = None) -> dict:
        """Cancella un ordine aperto per ID."""
        try:
            result = self.exchange.cancel_order(order_id, symbol)
            self.logger.info(f"🗑️ Ordine {order_id} cancellato")
            return {'ok': True, 'id': order_id}
        except Exception as e:
            self._err.capture(e, "cancel_order", {"order_id": order_id})
            self.logger.error(f"❌ Cancel order {order_id}: {e}")
            return {'error': str(e)}

    def cancel_all_orders(self, symbol: Optional[str] = None) -> dict:
        """Cancella tutti gli ordini aperti (opzionalmente solo per un symbol)."""
        try:
            open_orders = self.exchange.fetch_open_orders(symbol)
            cancellati = 0
            for o in open_orders:
                res = self.cancel_order(o['id'], o.get('symbol'))
                if not res.get('error'):
                    cancellati += 1
            self.logger.info(f"🗑️ Cancellati {cancellati}/{len(open_orders)} ordini")
            return {'ok': True, 'cancellati': cancellati, 'totale': len(open_orders)}
        except Exception as e:
            self._err.capture(e, "cancel_all_orders", {})
            self.logger.error(f"❌ Cancel all orders: {e}")
            return {'error': str(e)}

    def get_open_orders(self, symbol: Optional[str] = None) -> list:
        """Lista ordini aperti. Opzionale filtro per symbol."""
        try:
            return self.exchange.fetch_open_orders(symbol)
        except Exception as e:
            self._err.capture(e, "get_open_orders", {"symbol": symbol})
            self.logger.error(f"❌ Open orders: {e}")
            return []

    # ═══════════════════════════════════════════════════════════════════════════
    # ACCOUNT
    # ═══════════════════════════════════════════════════════════════════════════

    def get_balance_summary(self) -> dict:
        """
        Balance sintetico: solo valute con saldo > 0, con valore USD stimato.
        """
        try:
            bal = self.exchange.fetch_balance()
            result = {}
            for currency, data in bal.items():
                if not isinstance(data, dict):
                    continue
                total = float(data.get('total', 0) or 0)
                if total > 0:
                    result[currency] = {
                        'free':  float(data.get('free',  0) or 0),
                        'used':  float(data.get('used',  0) or 0),
                        'total': total,
                    }
            return result
        except Exception as e:
            self._err.capture(e, "get_balance_summary", {})
            self.logger.error(f"❌ Balance: {e}")
            return {}

    def get_open_positions(self) -> list:
        """Posizioni a margine aperte con PnL non realizzato."""
        try:
            # Usa endpoint privato Kraken diretto (più affidabile di ccxt.fetch_positions)
            res = self.exchange.private_post_openpositions()
            raw = res.get('result', {}) or {}
            positions = []
            for pos_id, p in raw.items():
                pair = p.get('pair', '')
                side = p.get('type', '')
                vol  = float(p.get('vol', 0) or 0)
                cost = float(p.get('cost', 0) or 0)
                net  = float(p.get('net', 0) or 0)
                positions.append({
                    'id':        pos_id,
                    'pair':      pair,
                    'side':      side,
                    'volume':    vol,
                    'cost_usd':  cost,
                    'pnl_usd':   net,
                    'leverage':  p.get('leverage', '?'),
                    'opened_at': p.get('time', '?'),
                })
            return positions
        except Exception as e:
            self._err.capture(e, "get_open_positions", {})
            self.logger.error(f"❌ Open positions: {e}")
            return []

    # ═══════════════════════════════════════════════════════════════════════════
    # GESTIONE COMANDI TELEGRAM
    # ═══════════════════════════════════════════════════════════════════════════

    def gestisci_comando_telegram(self, cmd: str, args: list = None) -> bool:
        """
        Entry point unico per tutti i comandi Telegram di KrakenIntegration.
        Da chiamare nel blocco `comandi` di bot_la.py.

        Comandi gestiti:
            /markets [top_n]        — lista top mercati per volume
            /screener               — screener completo multi-criterio
            /watchlist              — mostra watchlist corrente
            /add TICKER             — aggiunge asset alla watchlist
            /remove TICKER          — rimuove asset dalla watchlist
            /ticker SYMBOL          — dati ticker completo di un asset
            /orderbook SYMBOL       — orderbook bid/ask con imbalance
            /balance                — balance account
            /positions              — posizioni margin aperte
            /orders                 — ordini aperti
            /buy SYMBOL QTY [PRICE] — piazza ordine buy (market se no PRICE)
            /sell SYMBOL QTY [PRICE]— piazza ordine sell (market se no PRICE)
            /cancel ORDER_ID        — cancella un ordine
            /cancelall [SYMBOL]     — cancella tutti gli ordini

        Returns:
            True se il comando è stato gestito, False altrimenti
        """
        if not self.alerts:
            self.logger.warning("⚠️ gestisci_comando_telegram: alerts non configurato")
            return False

        args = args or []
        cmd  = cmd.strip().lower()

        try:
            # ── /markets ─────────────────────────────────────────────────────
            if cmd == '/markets':
                top_n = int(args[0]) if args else 15
                self._cmd_markets(top_n)
                return True

            # ── /screener ────────────────────────────────────────────────────
            elif cmd == '/screener':
                self._cmd_screener()
                return True

            # ── /watchlist ───────────────────────────────────────────────────
            elif cmd == '/watchlist':
                self._cmd_watchlist()
                return True

            # ── /meme ────────────────────────────────────────────────────────
            elif cmd == '/meme':
                self._cmd_meme()
                return True

            # ── /add TICKER ──────────────────────────────────────────────────
            elif cmd.startswith('/add'):
                ticker = args[0].upper() if args else cmd.replace('/add', '').strip().upper()
                if not ticker:
                    self.alerts.invia_alert("⚠️ Uso: `/add TICKER` (es. `/add ADAUSD`)")
                else:
                    ok, msg = self.aggiungi_asset(ticker)
                    self.alerts.invia_alert(msg)
                return True

            # ── /remove TICKER ───────────────────────────────────────────────
            elif cmd.startswith('/remove'):
                ticker = args[0].upper() if args else cmd.replace('/remove', '').strip().upper()
                if not ticker:
                    self.alerts.invia_alert("⚠️ Uso: `/remove TICKER` (es. `/remove ADAUSD`)")
                else:
                    ok, msg = self.rimuovi_asset(ticker)
                    self.alerts.invia_alert(msg)
                return True

            # ── /ticker SYMBOL ───────────────────────────────────────────────
            elif cmd.startswith('/ticker'):
                symbol = args[0].upper() if args else ''
                if not symbol:
                    self.alerts.invia_alert("⚠️ Uso: `/ticker BTC/USD`")
                else:
                    self._cmd_ticker(symbol)
                return True

            # ── /orderbook SYMBOL ────────────────────────────────────────────
            elif cmd.startswith('/orderbook'):
                symbol = args[0].upper() if args else ''
                if not symbol:
                    self.alerts.invia_alert("⚠️ Uso: `/orderbook BTC/USD`")
                else:
                    self._cmd_orderbook(symbol)
                return True

            # ── /balance ─────────────────────────────────────────────────────
            elif cmd == '/balance':
                self._cmd_balance()
                return True

            # ── /positions ───────────────────────────────────────────────────
            elif cmd == '/positions':
                self._cmd_positions()
                return True

            # ── /orders ──────────────────────────────────────────────────────
            elif cmd == '/orders':
                self._cmd_orders()
                return True

            # ── /buy SYMBOL QTY [PRICE] ──────────────────────────────────────
            elif cmd.startswith('/buy'):
                self._cmd_ordine('buy', args)
                return True

            # ── /sell SYMBOL QTY [PRICE] ─────────────────────────────────────
            elif cmd.startswith('/sell'):
                self._cmd_ordine('sell', args)
                return True

            # ── /cancel ORDER_ID ─────────────────────────────────────────────
            elif cmd.startswith('/cancel') and not cmd.startswith('/cancelall'):
                order_id = args[0] if args else ''
                if not order_id:
                    self.alerts.invia_alert("⚠️ Uso: `/cancel ORDER_ID`")
                else:
                    res = self.cancel_order(order_id)
                    if res.get('error'):
                        self.alerts.invia_alert(f"❌ Errore: {res['error']}")
                    else:
                        self.alerts.invia_alert(f"✅ Ordine `{order_id}` cancellato.")
                return True

            # ── /cancelall [SYMBOL] ──────────────────────────────────────────
            elif cmd == '/cancelall':
                symbol = args[0].upper() if args else None
                res = self.cancel_all_orders(symbol)
                if res.get('error'):
                    self.alerts.invia_alert(f"❌ Errore: {res['error']}")
                else:
                    self.alerts.invia_alert(
                        f"🗑️ Cancellati `{res['cancellati']}` ordini su `{res['totale']}` aperti."
                    )
                return True

            # ── /autoscan [N] ────────────────────────────────────────────────
            elif cmd.startswith('/autoscan'):
                top_n = int(args[0]) if args else 10
                self.alerts.invia_alert(
                    f"🔭 Auto-populate in corso (top {top_n} asset per volume)..."
                )
                result = self.auto_populate(top_n=top_n, force=True)
                static = set(al_config.ASSET_PRINCIPALI) if al_config else set()
                extra = [a for a in result if a not in static]
                self.alerts.invia_alert(
                    "\u2705 *Auto-populate completato*\n"
                    f"Totale asset monitorati: *{len(result)}*\n"
                    f"  \u2022 Statici: {len(static)}\n"
                    f"  \u2022 Auto-scansione: {len(extra)}\n"
                    f"  \u2022 Manuali (/add): {len(self._watchlist)}\n\n"
                    + ("Extra rispetto ai 5 statici:\n"
                       + "\n".join(f"  `{a}`" for a in extra[:10])
                       + (f"\n  _...e altri {len(extra)-10}_" if len(extra) > 10 else "")
                       if extra else "_Tutti gli asset rientrano già nei 5 statici._")
                )
                return True
                # ── /kraken (help) ───────────────────────────────────────────────
            elif cmd == '/kraken':
                self._cmd_help()
                return True

        except Exception as e:
            self._err.capture(e, "gestisci_comando_telegram", {"cmd": cmd})
            self.logger.error(f"❌ Errore gestione comando {cmd}: {e}")
            if self.alerts:
                self.alerts.invia_alert(f"❌ Errore comando `{cmd}`: {e}")

        return False

    # ═══════════════════════════════════════════════════════════════════════════
    # HANDLER COMANDI INTERNI
    # ═══════════════════════════════════════════════════════════════════════════

    def _cmd_markets(self, top_n: int = 15):
        markets = self.get_all_markets()[:top_n]
        if not markets:
            self.alerts.invia_alert("❌ Impossibile recuperare i mercati Kraken.")
            return

        lines = [f"📊 *TOP {top_n} MERCATI KRAKEN (vol 24h)*\n"]
        for i, m in enumerate(markets, 1):
            chg = m['change_24h_pct']
            chg_icon = "🟢" if chg > 0 else ("🔴" if chg < 0 else "⚪")
            margin = "📐" if m['has_margin'] else "  "
            lines.append(
                f"{i:02d}. {margin} `{m['symbol']}` {chg_icon} {chg:+.1f}%\n"
                f"     💵 ${m['price']:>12,.4f} | Vol: ${m['volume_24h_usd']/1e6:.1f}M"
            )

        lines.append("\n📐 = margine disponibile")
        self.alerts.invia_alert('\n'.join(lines))

    def _cmd_screener(self):
        screen = self.screener_completo(top_n=5)
        lines = ["🔭 *KRAKEN SCREENER COMPLETO*\n"]

        def fmt_lista(titolo, lista, emoji=''):
            if not lista:
                return
            lines.append(f"\n{emoji} *{titolo}*")
            for m in lista:
                chg = m['change_24h_pct']
                lines.append(
                    f"  • `{m['symbol']}` {chg:+.1f}% | ${m['volume_24h_usd']/1e6:.1f}M"
                )

        fmt_lista("TOP VOLUME",          screen['top_volume'], "💰")
        fmt_lista("TOP MOVERS",          screen['top_movers'], "⚡")
        fmt_lista("MARGINE TOP",         screen['margin_top'], "📐")
        fmt_lista("MOMENTUM BULLISH",    screen['bullish'],    "🟢")
        fmt_lista("MOMENTUM BEARISH",    screen['bearish'],    "🔴")
        fmt_lista("SPREAD BASSO",        screen['low_spread'], "🎯")

        self.alerts.invia_alert('\n'.join(lines))

    def _cmd_meme(self):
        """Mostra stato e dati live delle meme coin pre-configurate."""
        lines = ["🐸 *MEME COINS — Kraken*\n"]
        nella_watchlist = set(self._watchlist)
        nella_auto      = set(self._auto_watchlist)

        for kid in MEME_COINS:
            # Cerca i dati live nella cache dei mercati
            found = next(
                (m for m in self._markets_cache if m["kraken_id"] == kid),
                None
            )
            if found:
                chg  = found["change_24h_pct"]
                icon = "🟢" if chg > 0 else ("🔴" if chg < 0 else "⚪")
                p    = found["price"]
                pfmt = (f"${p:.8f}" if p < 0.001 else
                        f"${p:.6f}" if p < 1    else f"${p:,.4f}")
                stato = ("📋 watchlist" if kid in nella_watchlist else
                         ("🔭 auto"     if kid in nella_auto      else
                          "💤 inattivo"))
                lines.append(
                    f"{icon} `{kid}` — {stato}\n"
                    f"   Prezzo: {pfmt} | {chg:+.1f}% | "
                    f"Vol: ${found['volume_24h_usd']/1e6:.1f}M"
                )
            else:
                lines.append(f"⚪ `{kid}` — dati non in cache (usa /autoscan)")

        lines.append(
            "\n_Aggiungi con /add TICKER | "
            "Attiva auto-scan con /autoscan_"
        )
        self.alerts.invia_alert("\n".join(lines))

    def _cmd_watchlist(self):
        wl = self._watchlist
        static = getattr(al_config, 'ASSET_PRINCIPALI', []) if al_config else []
        lines = ["📋 *WATCHLIST CHIMERA*\n"]

        if wl:
            lines.append("*Dinamica (runtime):*")
            for a in wl:
                lines.append(f"  • `{a}`")
        else:
            lines.append("_Nessuna watchlist dinamica — uso lista statica_")

        lines.append("\n*Lista statica (asset\\_list.py):*")
        for a in static:
            lines.append(f"  • `{a}`")

        lines.append(
            "\n_Comandi: /add TICKER · /remove TICKER_\n"
            "_Watchlist dinamica prende precedenza sulla statica_"
        )
        self.alerts.invia_alert('\n'.join(lines))

    def _cmd_ticker(self, symbol: str):
        # Normalizza: BTCUSD -> BTC/USD
        if '/' not in symbol and len(symbol) > 3:
            # Prova a trovare nei mercati
            markets = self.get_all_markets(min_volume_usd=0)
            found = next((m for m in markets
                          if symbol == m['symbol'].replace('/', '') or
                          symbol == m['kraken_id']), None)
            if found:
                symbol = found['symbol']

        t = self.get_ticker_full(symbol)
        if not t:
            self.alerts.invia_alert(f"❌ Ticker non trovato: `{symbol}`")
            return

        chg = t['change_24h_pct']
        chg_icon = "🟢" if chg > 0 else ("🔴" if chg < 0 else "⚪")
        _p = t['price']
        _pfmt = (f"${_p:.8f}" if _p < 0.001 else
                 f"${_p:.6f}" if _p < 1 else f"${_p:,.4f}")
        self.alerts.invia_alert(
            f"📈 *{t['symbol']}* — Dati Live\n\n"
            f"💵 Prezzo:    `{_pfmt}`\n"
            f"{chg_icon} Var 24h:  `{chg:+.2f}%`\n"
            f"📊 Vol 24h:  `${t['volume_24h']:.0f}`\n"
            f"🔺 High:     `${t['high_24h']:,.6f}`\n"
            f"🔻 Low:      `${t['low_24h']:,.6f}`\n"
            f"📉 VWAP:     `${t['vwap_24h']:,.6f}`\n"
            f"⚖️  Spread:   `{t['spread_pct']:.4f}%`\n"
            f"   Bid/Ask:  `${t['bid']:,.6f}` / `${t['ask']:,.6f}`"
        )

    def _cmd_orderbook(self, symbol: str):
        if '/' not in symbol:
            markets = self.get_all_markets(min_volume_usd=0)
            found = next((m for m in markets
                          if symbol == m['symbol'].replace('/', '') or
                          symbol == m['kraken_id']), None)
            if found:
                symbol = found['symbol']

        ob = self.get_orderbook(symbol, depth=10)
        if not ob:
            self.alerts.invia_alert(f"❌ OrderBook non disponibile per `{symbol}`")
            return

        imb = ob['imbalance']
        imb_icon = "🟢 BUY" if imb > 0.1 else ("🔴 SELL" if imb < -0.1 else "⚪ NEUTRO")

        bids_lines = '\n'.join(
            f"  `{b[0]:>12,.4f}` × {b[1]:.4f}" for b in ob['bids'][:5]
        )
        asks_lines = '\n'.join(
            f"  `{a[0]:>12,.4f}` × {a[1]:.4f}" for a in ob['asks'][:5]
        )

        self.alerts.invia_alert(
            f"📖 *OrderBook — {symbol}*\n\n"
            f"🟢 *BID (top 5)*\n{bids_lines}\n\n"
            f"🔴 *ASK (top 5)*\n{asks_lines}\n\n"
            f"⚖️  Spread: `${ob['spread']:,.6f}`\n"
            f"📊 Imbalance: `{imb:+.2f}` → {imb_icon}\n"
            f"   Vol Bid: {ob['bid_volume']:.4f} | Vol Ask: {ob['ask_volume']:.4f}"
        )

    def _cmd_balance(self):
        bal = self.get_balance_summary()
        if not bal:
            self.alerts.invia_alert("❌ Impossibile recuperare il balance.")
            return

        lines = ["💼 *BALANCE KRAKEN*\n"]
        for currency, data in bal.items():
            if currency in ('info', 'timestamp', 'datetime', 'free', 'used', 'total'):
                continue
            lines.append(
                f"• `{currency}`: "
                f"Free={data['free']:.6f} | "
                f"Used={data['used']:.6f} | "
                f"Tot={data['total']:.6f}"
            )

        if len(lines) == 1:
            lines.append("_Nessun saldo rilevato_")

        self.alerts.invia_alert('\n'.join(lines))

    def _cmd_positions(self):
        positions = self.get_open_positions()
        if not positions:
            self.alerts.invia_alert("📭 Nessuna posizione margin aperta su Kraken.")
            return

        lines = [f"📊 *POSIZIONI APERTE ({len(positions)})*\n"]
        for p in positions:
            pnl = p['pnl_usd']
            pnl_icon = "🟢" if pnl > 0 else ("🔴" if pnl < 0 else "⚪")
            side_icon = "📈" if p['side'] == 'buy' else "📉"
            lines.append(
                f"{side_icon} `{p['pair']}` — {p['side'].upper()}\n"
                f"  Vol: {p['volume']} | Cost: ${p['cost_usd']:,.2f} | "
                f"PnL: {pnl_icon} ${pnl:+,.2f}\n"
                f"  Leva: {p['leverage']}x"
            )

        self.alerts.invia_alert('\n'.join(lines))

    def _cmd_orders(self):
        orders = self.get_open_orders()
        if not orders:
            self.alerts.invia_alert("📭 Nessun ordine aperto su Kraken.")
            return

        lines = [f"📋 *ORDINI APERTI ({len(orders)})*\n"]
        for o in orders[:10]:  # max 10 per non sforare il limite Telegram
            otype  = o.get('type', '?')
            side   = o.get('side', '?')
            symbol = o.get('symbol', '?')
            price  = o.get('price', 0) or 0
            amount = o.get('amount', 0) or 0
            oid    = o.get('id', '?')
            lines.append(
                f"• [{otype.upper()} {side.upper()}] `{symbol}`\n"
                f"  Qty: {amount} | Prezzo: ${price:,.4f}\n"
                f"  ID: `{oid}`"
            )

        self.alerts.invia_alert('\n'.join(lines))

    def _cmd_ordine(self, side: str, args: list):
        """
        Gestisce /buy e /sell.
        Syntax: /buy SYMBOL QTY [PRICE] [LEVERAGEx]
        Esempi:
          /buy BTC/USD 0.001          → market buy 0.001 BTC
          /buy BTC/USD 0.001 85000    → limit buy 0.001 BTC @ 85000
          /sell ETH/USD 0.1 3200 3x   → limit sell con leva 3x
        """
        if len(args) < 2:
            self.alerts.invia_alert(
                f"⚠️ Uso: `/{side} SYMBOL QUANTITA [PREZZO] [LEVERAGEx]`\n"
                f"Esempi:\n"
                f"  `/{side} BTC/USD 0.001` — market order\n"
                f"  `/{side} BTC/USD 0.001 85000` — limit order\n"
                f"  `/{side} ETH/USD 0.1 3200 3x` — limit con leva 3x"
            )
            return

        symbol = args[0].upper()
        # Normalizza: BTCUSD -> BTC/USD
        if '/' not in symbol and len(symbol) > 3:
            markets = self.get_all_markets(min_volume_usd=0)
            found = next((m for m in markets
                          if symbol == m['symbol'].replace('/', '') or
                          symbol == m['kraken_id']), None)
            if found:
                symbol = found['symbol']

        try:
            amount = float(args[1])
        except ValueError:
            self.alerts.invia_alert(f"❌ Quantità non valida: `{args[1]}`")
            return

        price    = None
        leverage = 1

        for arg in args[2:]:
            if arg.lower().endswith('x'):
                try:
                    leverage = int(arg[:-1])
                except ValueError:
                    pass
            else:
                try:
                    price = float(arg)
                except ValueError:
                    pass

        # Conferma pre-ordine
        order_type = "MARKET" if price is None else f"LIMIT @ ${price:,.4f}"
        lev_str    = f" leva {leverage}x" if leverage > 1 else " spot"
        self.alerts.invia_alert(
            f"⏳ Piazzo ordine:\n"
            f"• Tipo:   `{order_type}`\n"
            f"• Side:   `{side.upper()}`\n"
            f"• Asset:  `{symbol}`\n"
            f"• Qty:    `{amount}`\n"
            f"• Leva:   `{leverage}x`{lev_str}"
        )

        # Piazza l'ordine
        if price is None:
            result = self.place_market_order(symbol, side, amount, leverage)
        else:
            result = self.place_limit_order(symbol, side, amount, price, leverage)

        if result.get('error'):
            self.alerts.invia_alert(f"❌ Ordine fallito:\n`{result['error']}`")
        else:
            status  = result.get('status', '?')
            avg     = result.get('average', result.get('price', 0))
            filled  = result.get('filled', 0)
            cost    = result.get('cost', 0)
            oid     = result.get('id', '?')
            self.alerts.invia_alert(
                f"✅ *ORDINE ESEGUITO*\n"
                f"• ID:      `{oid}`\n"
                f"• Status:  `{status}`\n"
                f"• Filled:  `{filled}`\n"
                f"• Avg:     `${avg:,.4f}`\n"
                f"• Costo:   `${cost:,.2f}`"
            )

    def _cmd_help(self):
        self.alerts.invia_alert(
            "🔧 *KRAKEN INTEGRATION — COMANDI*\n\n"
            "*📊 Mercati & Analisi*\n"
            "`/autoscan [N]` — auto-popola watchlist con top N asset\n"
            "`/markets [N]` — top N mercati per volume\n"
            "`/screener` — screener multi-criterio\n"
            "`/ticker SYMBOL` — dati live (es. `/ticker BTC/USD`)\n"
            "`/orderbook SYMBOL` — orderbook + imbalance\n\n"
            "*📋 Watchlist*\n"
            "`/watchlist` — mostra watchlist corrente\n"
            "`/meme` — stato live delle meme coin\n"
            "`/add TICKER` — aggiunge asset (es. `/add ADAUSD`)\n"
            "`/remove TICKER` — rimuove asset\n\n"
            "*💼 Account*\n"
            "`/balance` — saldo account\n"
            "`/positions` — posizioni margin aperte\n"
            "`/orders` — ordini aperti\n\n"
            "*📈 Ordini*\n"
            "`/buy SYMBOL QTY [PREZZO] [LEVERAGEx]`\n"
            "`/sell SYMBOL QTY [PREZZO] [LEVERAGEx]`\n"
            "`/cancel ORDER_ID` — cancella ordine\n"
            "`/cancelall [SYMBOL]` — cancella tutti\n\n"
            "_Esempi:_\n"
            "`/buy BTC/USD 0.001` — market buy\n"
            "`/sell ETH/USD 0.1 3200 3x` — limit sell 3x leva"
        )

    # ═══════════════════════════════════════════════════════════════════════════
    # UTILITÀ
    # ═══════════════════════════════════════════════════════════════════════════

    def _normalizza_ordine(self, order: dict) -> dict:
        """Estrae i campi essenziali da un ordine ccxt."""
        if not order:
            return {}
        return {
            'id':       order.get('id'),
            'status':   order.get('status'),
            'type':     order.get('type'),
            'side':     order.get('side'),
            'symbol':   order.get('symbol'),
            'amount':   order.get('amount', 0),
            'filled':   order.get('filled', 0),
            'price':    order.get('price', 0),
            'average':  order.get('average', 0),
            'cost':     order.get('cost', 0),
            'fee':      (order.get('fee') or {}).get('cost', 0),
            'datetime': order.get('datetime'),
        }
