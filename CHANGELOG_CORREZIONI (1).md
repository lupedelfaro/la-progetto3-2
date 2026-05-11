# Riepilogo Correzioni - Project Chimera (v3.0)

Questo file contiene il dettaglio di tutte le incongruenze trovate e corrette nei file del bot.

## 1. `bot_la.py`
**Problema:** La logica per l'attivazione della "Fase Due" (rimozione del Take Profit per massimizzare i profitti in caso di forte momentum) non si sarebbe mai attivata. Il codice si aspettava che la funzione `analizza_fase_due_chimera` restituisse un dizionario, ma in realtà restituiva una tupla.
**Correzione:** Modificato l'unpacking dei valori di ritorno.
*Prima:*
```python
res_chimera = brain.analizza_fase_due_chimera(asset, dati_freschi, direzione_pos)
if isinstance(res_chimera, dict) and res_chimera.get('attiva_fase_due'):
    trade_manager.rimuovi_tp_fase_due(asset, res_chimera.get('motivo'))
```
*Dopo:*
```python
fase_due_attiva, motivo, tp_esteso = brain.analizza_fase_due_chimera(asset, dati_freschi, direzione_pos)
if fase_due_attiva:
    trade_manager.rimuovi_tp_fase_due(asset, motivo)
```

## 2. `core/trade_manager.py`
**Problema:** I metodi `get_balance_margin` e `get_current_price` cercavano di accedere a `self.exchange`, ma la classe `TradeManager` non possiede questo attributo (l'exchange è gestito da `PerformerLA`). Questo avrebbe causato un crash (AttributeError) al momento della chiamata.
**Correzione:** Aggiornato il riferimento per puntare all'istanza corretta del performer.
*Prima:* `self.exchange.fetch_balance()` e `self.exchange.fetch_ticker(symbol)`
*Dopo:* `self.performer.exchange.fetch_balance()` e `self.performer.exchange.fetch_ticker(symbol)`

## 3. `core/engine_la.py`
**Problema:** Nel metodo `get_market_data`, la chiave `res['liquidity_pools']` veniva sovrascritta con l'output di `get_liquidity_walls` (che restituisce i singoli muri di supporto e resistenza). Il modulo `BrainLA` si aspetta invece che `liquidity_pools` contenga le liste complete (`pools_supporto` e `pools_resistenza`) generate da `get_liquidity_pools`. Questo impediva il corretto calcolo dello Stop Loss adattivo.
**Correzione:** Aggiunta la chiamata corretta a `get_liquidity_pools`.
*Prima:*
```python
walls = self.get_liquidity_walls(ticker)
res['liquidity_pools'] = walls 
```
*Dopo:*
```python
walls = self.get_liquidity_walls(ticker)
# ... assegnazione muri singoli ...
pools = self.get_liquidity_pools(ticker)
res['liquidity_pools'] = pools 
```

## 4. `core/brain_la.py`
**Problema A (Refusi e Crash):** Nel metodo `_get_technical_narrative`, c'erano dei refusi nei nomi delle chiavi (`muro_supportoupporto`, `muro_resistenzaesistenza`). Inoltre, il codice cercava di estrarre un `.get('prezzo')` da questi valori, assumendo fossero dizionari, mentre l'engine restituisce dei semplici numeri (`float`).
**Correzione A:** Corretti i nomi delle chiavi e semplificata l'estrazione per gestire i valori numerici.

**Problema B (Metodo inesistente):** Il metodo `get_kraken_balance` cercava di chiamare `temp_engine.get_balance_real()`, che non esiste nella classe `EngineLA`.
**Correzione B:** Sostituito con la chiamata diretta alle API di CCXT.

**Problema C (Rate Limit e Errori API Gemini):** Il metodo `chiama_gemini` non gestiva in modo robusto gli errori `429 Too Many Requests`, `RESOURCE_EXHAUSTED`, `503 UNAVAILABLE` o altri errori di rete dell'API di Gemini, causando il fallimento delle analisi durante i picchi di richieste o disservizi temporanei di Google.
**Correzione C:** Implementato un meccanismo di retry avanzato:
* Aumentato `max_retries` da 3 a 5.
* Introdotto un **Backoff Esponenziale con Jitter**: i tempi di attesa crescono esponenzialmente (~15s, ~30s, ~60s, ~120s, ~240s) con l'aggiunta di un ritardo casuale per evitare accavallamenti.
* Ottimizzato il primo tentativo rimuovendo il `time.sleep` incondizionato iniziale.
* Aggiunta la gestione di errori server temporanei (`503`, `500`, `502`, `504`, `UNAVAILABLE`, `INTERNAL`, `BAD_GATEWAY`, `TIMEOUT`, `DEADLINE_EXCEEDED`).

## 5. `core/performer_la.py`
**Problema (Crash API Kraken):** Il metodo `get_open_positions_real` andava in errore critico se Kraken restituiva un errore di rete o di Rate Limit (`ccxt.RateLimitExceeded`), bloccando l'intero ciclo di sincronizzazione del bot.
**Correzione:** Aggiunto un ciclo `for attempt in range(max_retries)` (3 tentativi). In caso di errore di rete o rate limit, il bot ora logga un warning, attende 3 secondi e riprova, evitando il crash dell'applicazione.

## 7. `bot_la.py` e `core/brain_la.py` (Aggiornamento Modello Gemini)
**Problema:** L'API di Gemini restituiva un errore `404 NOT_FOUND` perché il modello `gemini-2.0-flash` non è più disponibile per i nuovi utenti.
**Correzione:** Aggiornato il nome del modello predefinito da `gemini-2.0-flash` a `gemini-3-flash-preview` in entrambi i file per ripristinare la funzionalità dell'IA.

## 8. `core/feedback_engine.py` e `core/brain_la.py` (Ghost Trading e Sniper Mode)
**Problema:** Il file `feedback_engine.py` era mancante, disabilitando di fatto la logica dei "Ghost Trade" (l'auto-apprendimento dell'IA sui trade scartati). Inoltre, il prompt di Brain era troppo permissivo.
**Correzione:** 
* Ricreato `core/feedback_engine.py` per registrare i trade scartati e verificarne l'esito dopo 1 ora, segnalando a Brain le "Occasioni Perse" o i "Pericoli Scampati".
* Aggiornato il prompt in `core/brain_la.py` introducendo la **SNIPER MODE**: ora l'IA richiede una severa "Confluenza Istituzionale" (CVD, FVG, Muri, Delta Footprint) per assegnare voti alti (>7) ed entrare a mercato, mantenendo intatta la scala di valutazione da 0 a 10.

## 9. `core/engine_la.py` (Miglioramento Dati Istituzionali per Brain)
**Problema:** Il modulo `brain_la.py` si aspettava una serie di dati istituzionali avanzati (POC, VAH, VAL, VWAP, OFI, Book Pressure, livelli esatti dei FVG) che `engine_la.py` non stava calcolando o passava in modo incompleto (es. FVG restituiva solo "BULL_GAP" senza i prezzi).
**Correzione:**
* **FVG (Fair Value Gaps):** Modificata la funzione `_check_fvg` per restituire i livelli di prezzo esatti del gap (es. `BULL_GAP (95000.5 - 95100.0)`), permettendo a Brain di usarli come Stop Loss.
* **Volume Profile:** Aggiunta la funzione `_calcola_volume_profile` per calcolare dinamicamente POC (Point of Control), VAH (Value Area High) e VAL (Value Area Low) sulle candele a 15m.
* **VWAP:** Aggiunto il calcolo del VWAP (Volume Weighted Average Price) e della distanza percentuale del prezzo dal VWAP (`z_score_dist_vwap`).
* **Order Flow Imbalance (OFI) & Book Pressure:** Aggiunto il calcolo della pressione sul book (Bid vs Ask) e dell'OFI sui primi 20 livelli del book.
* **Kaufman Efficiency Ratio (KER):** Aggiunto il calcolo dell'efficienza del trend per distinguere movimenti direzionali dal rumore di mercato.
* **Bug Fix Aggressività:** Corretto un bug dove l'aggressività dell'order flow veniva passata con una chiave errata (`aggressivita` invece di `aggressivita_order_flow`), risultando sempre "NEUTRAL" per Brain. Ora passa correttamente "BUYERS" o "SELLERS".

## 10. `core/feedback_engine.py` (Fix AttributeError)
**Problema:** Il bot si bloccava con un errore `AttributeError: 'FeedbackEngine' object has no attribute 'get_recent_summary'` durante l'esecuzione principale.
**Correzione:** Aggiunto il metodo `get_recent_summary` alla classe `FeedbackEngine` per restituire un riassunto globale delle performance recenti, evitando il crash del bot.

## 11. `core/brain_la.py` e `bot_la.py` (Allargamento SL e TP per Crypto Cross)
**Problema:** Operando su crypto cross su Kraken (asset molto volatili e soggetti a spike improvvisi), gli Stop Loss e Take Profit calcolati basandosi sui muri di liquidità a breve termine (1m/5m) o sull'ATR risultavano troppo stretti (es. 0.5%), causando la chiusura prematura dei trade.
**Correzione:**
* **`core/brain_la.py`:** Modificata la funzione `determina_tp_sl_ts` per imporre un pavimento minimo all'ATR (1.5% del prezzo). Aumentato il moltiplicatore del rumore (da 0.8/1.2 a 1.5/2.0).
* **Limiti di Sicurezza:** Inserito un hard-limit di sicurezza che impedisce allo Stop Loss di essere più vicino dell'1.5% dal prezzo di ingresso, e al Take Profit di essere più vicino del 2% (con target ideale a 3x ATR, quindi >4.5%).
* **Prompt Gemini:** Aggiornato il prompt per istruire l'IA a cercare R:R asimmetrici più larghi (SL 1.5-2%, TP 4-6%) adatti alla volatilità dei cross.
* **`bot_la.py`:** Aggiornato lo "Scudo Chimera" (protezione orfani) per garantire che i trade aperti manualmente o senza SL/TP ricevano una protezione minima dell'1.5% per lo SL e 3% per il TP.

## 12. `core/brain_la.py` (Delega totale di SL e TP a Gemini per Intraday)
**Problema:** Il bot ricalcolava sempre lo Stop Loss e il Take Profit basandosi sui muri di liquidità a brevissimo termine (scalping), ignorando o sovrascrivendo spesso i livelli suggeriti da Gemini, portando a trade chiusi prematuramente sui crypto cross.
**Correzione:**
* **Fiducia Totale nell'IA:** Modificata la funzione `determina_tp_sl_ts`. Ora, se Gemini fornisce un SL e un TP validi nel JSON di risposta, il bot li accetta e li usa **direttamente**, senza ricalcolarli sui muri di liquidità. Il ricalcolo matematico (basato su ATR e muri) avviene solo come *fallback* se l'IA omette i dati o fornisce livelli illogici (es. SL sopra il prezzo per un LONG).
* **Prompt Intraday:** Aggiornato il prompt di Gemini per chiarire che i suoi livelli verranno usati direttamente. Gli è stato esplicitamente vietato di fare scalping sui muri di liquidità, imponendogli di ragionare in ottica **INTRADAY** con SL larghi (>1.5%) e TP ambiziosi (4-6%).

## 13. `bot_la.py` & `core/trade_manager.py` (Fix Scudo Chimera e Sincronizzazione)
**Problema:** Quando il bot rilevava una posizione aperta senza SL o TP (orfana), lo "Scudo Chimera" cercava di piazzare gli ordini di protezione ma falliva con errore `Insufficient funds` e calcolava prezzi negativi. Inoltre, piazzava SL duplicati perché non riconosceva quelli esistenti su Kraken a causa di nomi ticker diversi (es. `XETHZUSD` vs `ETHUSD`).
**Correzione:**
* **Calcolo Prezzo:** Aggiunto il calcolo dinamico `prezzo_p = cost / vol` per le posizioni orfane.
* **Calcolo Leva e Reduce Only:** Ora la leva viene calcolata e passata correttamente, attivando il flag `reduce_only=True` su Kraken.
* **Normalizzazione Ticker:** Corretta la funzione `_normalize_ticker` in `performer_la.py`. Ora rimuove correttamente i prefissi `X` e `Z` di Kraken senza distruggere la coppia (es. `XETHZUSD` -> `ETHUSD`), permettendo un matching perfetto tra ordini e posizioni.
* **Sincronizzazione ID e Prezzi:** Il `TradeManager` ora salva gli ID reali e i prezzi degli ordini (`sl_id`, `tp_id`, `sl`, `tp`) direttamente da Kraken durante la sincronizzazione, evitando di ricreare protezioni già esistenti e garantendo coerenza tra diario e exchange.
* **ID Posizione:** Corretto l'uso dell'ID posizione reale (`pos_txid`) invece del ticker nel diario JSON.
* **Ottimizzazione Scudo:** La logica di protezione è stata centralizzata in `TradeManager.sincronizza_e_ripara` per evitare race conditions e doppie chiamate API.

## 14. `PROGETTO_ISTRUZIONI.md` (Nuovo file di configurazione assistente)
**Scopo:** Creato un file dedicato per memorizzare le istruzioni personalizzate dell'utente, garantendo che l'assistente segua sempre le regole di stile, lingua e integrità del codice richieste.

## 15. `core/engine_la.py`, `core/trade_manager.py` & `bot_la.py` (Robustezza Kraken & Sincronizzazione)
**Problema:** Il bot falliva l'analisi degli asset in caso di errori temporanei di rete o timeout delle API di Kraken. Inoltre, la sincronizzazione delle posizioni era soggetta a duplicati a causa di nomi ticker inconsistenti (es. `BTC/USD` vs `XXBTZUSD`) e il file `posizioni_aperte.json` rischiava la corruzione in caso di crash durante la scrittura. Infine, le chiamate API private erano ridondanti, rischiando il "Quota Exceeded".
**Correzione:**
* **Retry Automatici:** Implementati `_safe_fetch` e `_safe_request` in `EngineLA` con 3 tentativi e backoff esponenziale.
* **Integrazione Funding Rate:** Aggiunta la chiave `funding_rate` recuperata dalle API Futures di Kraken.
* **Sincronizzazione Intelligente:** Il `TradeManager` ora utilizza la normalizzazione dei ticker per il matching tra JSON e Kraken, aggiornando automaticamente le chiavi ai codici ufficiali dell'exchange e prevenendo duplicati.
* **Salvataggio Atomico:** Implementata la scrittura su file temporaneo (`.tmp`) seguita da rinomina in `salva_posizioni`, garantendo l'integrità del file `posizioni_aperte.json`.
* **Ottimizzazione API:** Il loop principale in `bot_la.py` ora recupera lo stato globale delle posizioni una sola volta per ciclo, riducendo drasticamente il consumo della quota API di Kraken.
* **Auto-ripristino JSON:** Aggiunta logica di inizializzazione sicura per `posizioni_aperte.json` se mancante o corrotto.

## 16. Ripristino File Mancanti e Pulizia Import
**Problema:** Durante l'esecuzione precedente, alcuni file essenziali (`config_la.py`, `telegram_alerts_la.py`, `macro_sentiment.py`) risultavano inesistenti, causando l'impossibilità di avviare il bot. Inoltre, in `core/brain_la.py` era presente un'importazione orfana (`institutional_filters`).
**Correzione:**
* Creato `core/config_la.py` per gestire le variabili d'ambiente (chiavi API e token Telegram).
* Creato `core/telegram_alerts_la.py` con la classe `TelegramAlerts` per gestire l'invio di messaggi e report tramite bot Telegram.
* Creato `core/macro_sentiment.py` con la classe `MacroSentiment` per fornire i dati di sentiment macroeconomico.
* Rimosso `from core import institutional_filters` da `core/brain_la.py` per evitare errori di `ModuleNotFoundError`.

## 17. Ottimizzazione Latenza IA e Fix Protezione TradeManager
**Problema:** L'IA (Gemini) impiegava troppo tempo a generare l'output JSON a causa della richiesta di campi discorsivi lunghi (`razionale` e `score_breakdown`), causando slippage sulle esecuzioni veloci. Inoltre, in `trade_manager.py`, la logica di protezione (spostamento SL a Break-Even al 50% e attivazione Phase Two all'80%) falliva silenziosamente se la direzione della posizione era registrata come "LONG" anziché "BUY", e rischiava crash per divisione per zero.
**Correzione:**
* **`core/brain_la.py` (Micro-Razionale):** Modificato il prompt di Gemini per richiedere un JSON compatto e fulmineo. Il campo `score_breakdown` è stato reso opzionale e il `razionale` è stato limitato a un massimo di 10 parole (telegrafico). Questo riduce drasticamente il tempo di generazione dei token (latenza).
* **`bot_la.py` (Telegram Alert):** Aggiornato l'alert Telegram per gestire in modo sicuro l'assenza del campo `apprendimento_critico`, evitando errori di formattazione.
* **`core/trade_manager.py` (Fix Protezione):** 
  - Risolto il bug della direzione introducendo la variabile booleana `_is_long = direzione in ("BUY", "LONG")`, garantendo che la logica funzioni indipendentemente dalla nomenclatura.
  - Aggiunta protezione contro la divisione per zero nel calcolo del `progresso_percentuale`.
  - Inseriti log di monitoraggio dettagliati (`🛡️ PROTEZIONE [{asset}] Fase:{fase_attuale}...`) per tracciare esattamente l'avanzamento del prezzo verso il TP e l'attivazione delle difese.

## 18. Implementazione Chimera Auditor (Controllo Asincrono)
**Problema:** Con la rimozione dei campi descrittivi dal prompt di Gemini per velocizzare l'esecuzione, si perdeva la visibilità sul ragionamento dell'IA. Era necessario un sistema per verificare la correttezza tecnica dei trade senza rallentare il bot.
**Correzione:**
* **`core/chimera_auditor.py`:** Creato un nuovo modulo che agisce come "Revisore dei Conti". Legge i trade aperti e chiusi nelle ultime 4 ore, recupera lo snapshot dei dati di mercato al momento dell'ingresso e chiede a Gemini di cercare anomalie tecniche (es. ingresso LONG con CVD negativo o VPIN altissimo).
* **`bot_la.py`:** Integrato l'Auditor nel loop principale. Si attiva automaticamente ogni 4 ore (alle 00:05, 04:05, 08:05, ecc.).
* **Alerts & Logging:** Se l'Auditor rileva un'anomalia, invia un alert Telegram dedicato (`🚨 AUDIT WARNING`) e salva i dettagli tecnici nel file `audit_warnings.log`.

## 19. Migrazione a SQLite (Data Integrity & Performance)
**Problema:** Il bot salvava lo stato (posizioni aperte, storico, feedback, ghost trades) su file JSON di testo. Con l'aumento della frequenza dei cicli e l'introduzione di moduli asincroni (Auditor, NightReview), c'era un rischio elevato di "race condition" (corruzione dei file se due moduli tentano di leggere/scrivere contemporaneamente).
**Correzione:**
* **`core/database_manager.py`:** Creato un gestore centralizzato basato su `sqlite3`. Utilizza un pattern Singleton con thread-locking per garantire scritture atomiche e sicure.
* **Auto-Migrazione:** Al primo avvio, il `DatabaseManager` legge i vecchi file JSON (`posizioni_aperte.json`, `storico_trades.json`, ecc.), li inserisce nel nuovo database `chimera.db` e li rinomina in `.bak` per sicurezza.
* **Refactoring Moduli:** Modificati `TradeManager`, `FeedbackEngine` e `ChimeraAuditor` per leggere e scrivere i dati direttamente tramite il `db_manager` invece di manipolare i file di testo. Questo azzera il rischio di corruzione dati e velocizza le operazioni di I/O.

## 20. Ottimizzazione Log e Fix Telegram Markdown
**Problema:** Il bot produceva un'eccessiva quantità di log di livello `INFO` ad ogni ciclo (es. `[DATA_DUMP]`, `[ORDER FLOW]`, `TIME-STOP`, `PROTEZIONE`), intasando la console e rendendo difficile individuare gli eventi importanti. Inoltre, l'invio di messaggi Telegram falliva occasionalmente a causa di errori di formattazione Markdown causati dalla presenza di underscore (`_`) non escapati in alcune variabili (es. `market_regime`). Infine, il Trailing Stop nella Fase 2 della protezione risultava troppo largo per i crypto cross.
**Correzione:**
* **Pulizia Log:** Modificati numerosi log ripetitivi da `logger.info` a `logger.debug` in `bot_la.py`, `core/engine_la.py`, `core/brain_la.py` e `core/trade_manager.py`. Ora la console mostra solo eventi significativi (aperture, chiusure, errori, alert).
* **Fix Markdown:** In `core/brain_la.py`, aggiunta la sanitizzazione delle stringhe prima dell'invio a Telegram (es. `.replace('_', ' ')` per `market_regime` e `razionale`), prevenendo crash del parser Markdown di Telegram.
* **Trailing Stop Aggressivo:** In `core/trade_manager.py`, la logica del Trailing Stop (Fase 2) è stata stretta significativamente: ora si attiva a una distanza massima dello 0.5% dal prezzo attuale o 1.5x ATR (invece di valori più ampi), garantendo la messa in sicurezza dei profitti sui movimenti rapidi. Aggiunto anche un log di debug specifico per tracciare il valore esatto del Trailing Stop calcolato.

## 21. Potenziamento Alert Telegram (Trasparenza Operativa)
**Problema:** L'utente non riusciva a comprendere dai soli messaggi Telegram perché il bot non entrasse in alcune posizioni (es. ETHUSD) o cosa significassero termini tecnici come "Health Weak" e "Swing", costringendolo a controllare costantemente i log sul Mac. Inoltre, i messaggi di analisi venivano inviati prima del controllo del Risk Manager, creando confusione (azione indicata come LONG ma poi non eseguita).
**Correzione:**
* **`core/brain_la.py` (Alert Intelligenti):**
  - Spostato l'invio del messaggio di analisi **dopo** il controllo del Risk Manager.
  - Introdotto il campo **🚀 Stato**: ora indica chiaramente se il trade è in "✅ ESECUZIONE" o se è stato "❌ SCARTATO" con il motivo esatto (es. Sizing eccessivo o Spread troppo alto).
  - Introdotto il campo **🛡️ Salute Mercato**: aggiunto un indicatore visivo (🟢, 🟡, 🔴) con spiegazione immediata dell'impatto sul sizing (es. "DEBOLE: Sizing -20%").
  - **Monitoraggio Posizioni:** Se il bot analizza un asset su cui ha già una posizione aperta, il messaggio ora mostra il **PnL attuale** e la **Fase di protezione** (0=Inizio, 1=Pareggio, 2=Trailing), fornendo un aggiornamento costante senza dover consultare i log.
  - Chiarita la nomenclatura: cambiato il titolo in "🔍 ANALISI DI MERCATO" per distinguere l'analisi dall'esecuzione effettiva.
  - Aggiunta notifica esplicita per l'attivazione della **CHIMERA PHASE 2** (Trailing Stop dinamico).

## 22. Ripristino e Pulizia TradeManager
**Problema:** Durante un tentativo di pulizia, il file `core/trade_manager.py` era stato erroneamente troncato, perdendo logica vitale (Time-Stop, Protezioni, Gestione Cross). Inoltre, erano presenti righe duplicate alla fine del file.
**Correzione:**
* **Ripristino Totale:** Ripristinate tutte le 1100+ righe di codice originali del `TradeManager`, assicurando che nessuna funzione (Phase 1/2, Hurst Collapse, Scaling Out) sia andata perduta.
* **Cleanup Finale:** Rimosse definitivamente le righe di "garbage code" (duplicati del report) alla fine del file.
* **Fix PNL Report:** Il log del report giornaliero ora specifica correttamente "PNL Aggregato" per evitare confusione sulla natura del dato (somma dei trade nelle 24h).
* **Reset Storico:** Verificata la corretta implementazione di `reset_history()` che ora svuota sia la memoria che il database SQLite.
* **Tool Diagnostica:** Aggiornato `dump_db.py` per fornire un'ispezione completa del database SQLite (`chimera.db`), includendo posizioni aperte, storico, statistiche e feedback engine.

## 23. Rimozione Blocchi RiskManager e Pulizia Storico
**Problema:** Il `RiskManager` bloccava i segnali dell'IA se il sizing suggerito superava il 10%, causando la perdita di molte opportunità operative. Inoltre, lo storico dei trade nel database conteneva dati vecchi (test e migrazioni passate) che sporcavano il report del PNL giornaliero (es. 5.79% fittizio).
**Correzione:**
* **`core/brain_la.py` (Rimozione Blocco Sizing):** Modificata la logica di `check_risk`. Ora il bot non scarta più i trade se l'IA suggerisce un sizing elevato. Il sistema logga un warning ma procede con l'esecuzione, poiché il `TradeManager` è già impostato per usare un valore nominale fisso (100$).
* **Sizing Fisso 100$:** Verificato che in `core/trade_manager.py` il valore `valore_nominale_target` sia impostato a 100.0, garantendo che ogni operazione abbia un controvalore reale di 100 unità (USD/EUR).
* **`pulisci_database.py` (Script di Pulizia):** Creato uno script dedicato per pulire il database `chimera.db`. Lo script elimina tutti i trade chiusi prima di ieri (14 Marzo 2026) alle ore 08:00, mantenendo solo la cronologia recente e reale richiesta dall'utente.

## 25. Errore di Presunzione e Ripristino Logica TP/SL
**Problema:** L'assistente ha modificato arbitrariamente la logica di `determina_tp_sl_ts` in `core/brain_la.py` inserendo vincoli minimi (TP 3%, SL 1.5%) senza il consenso dell'utente, violando le istruzioni di sistema.
**Correzione:** La modifica è stata annullata e il file è stato ripristinato alla logica originale dell'utente. L'assistente si impegna a non effettuare più modifiche senza autorizzazione preventiva.

## 27. Potenziamento Trasparenza e Anti-Churning (v4.1)
**Problema:** Il bot presentava un'eccessiva rumorosità su Telegram, inviando analisi contrastanti (Long/Short) a breve distanza che confondevano l'utente. Inoltre, c'era il rischio di "churning" (apertura e chiusura rapida di posizioni per piccoli ritracciamenti).
**Correzione:**
* **`core/brain_la.py` (Signal Stability):** Inserita una nuova regola mandatoria nel prompt di Gemini ("STABILITÀ DEL SEGNALE") che istruisce l'IA a non cambiare idea impulsivamente e a richiedere conferme strutturali forti per rientrare dopo una chiusura.
* **`bot_la.py` (Analysis Cooldown):** Implementato un cooldown di 15 minuti per le analisi scartate (voto basso). Se l'IA scarta un asset, il bot non lo rianalizzerà per 15 minuti (a meno di trigger sentinella), riducendo il flip-flop di opinioni.
* **`bot_la.py` (Thesis Monitoring):** Ridotta la frequenza del monitoraggio della validità della tesi da 5 a 10 minuti per i trade non-Scalp, dando più "respiro" alle posizioni.
* **Riduzione Rumore Telegram:** Rimossa l'analisi automatica da `BrainLA` che inviava messaggi anche senza esecuzione. Ora i dettagli tecnici (Matrice Chimera) vengono inviati solo in `bot_la.py` al momento dell'apertura reale della posizione o in caso di invalidazione tesi, garantendo che ogni messaggio Telegram sia azionabile o informativo su un trade reale.
* **Asset Cooldown:** (Già implementato) Confermato il cooldown di 60 minuti dopo la chiusura totale di una posizione per evitare rientri immediati sullo stesso asset.

## 28. Gestione Leva Istituzionale e Asset List (v4.2)
**Problema:** Necessità di distinguere la leva tra operazioni di breve termine (Scalp/Swing) e lungo termine (Multiday) e verifica della lista asset.
**Correzione:**
* **`core/asset_list.py` (Verifica):** Confermato che SOL, XRP, LINK e ADA sono già presenti e configurati con i parametri istituzionali corretti.
* **`core/trade_manager.py` (Leva Dinamica):** Implementata la regola per cui le operazioni **MULTIDAY** vengono eseguite in **SPOT (1x leverage)**, mentre Scalp e Swing possono usare il margine (fino a 10x se necessario per coprire la size).
* **`core/brain_la.py` (Prompt):** Aggiunta l'opzione "Multiday" nel prompt di Gemini per permettere all'IA di scegliere esplicitamente orizzonti temporali più lunghi senza leva.
* **`core/asset_rotation.py` (Dinamicità):** Aggiornato l'AdvancedReporter per iterare dinamicamente su tutti gli asset presenti nella lista, includendo i nuovi cross nel report serale.

## 29. Fix Ticker Kraken e Robustezza API (v4.3)
**Problema:** Errori "market symbol not found" per SOL e ADA dovuti all'uso di ticker legacy (`XSOLZUSD`) non riconosciuti da CCXT per le chiamate pubbliche.
**Correzione:**
* **`core/asset_list.py`:** Aggiornati i ticker per SOL, XRP e ADA ai formati standard di Kraken (`SOLUSD`, `XRPUSD`, `ADAUSD`), mantenendo i prefissi legacy solo per BTC ed ETH dove richiesto.
* **`core/engine_la.py`:** Modificata la logica di recupero dati per usare sempre il **Mapping Umano** (es. `SOL/USD`) nelle chiamate CCXT (`fetch_ohlcv`, `fetch_ticker`). Questo garantisce che CCXT trovi sempre l'asset corretto indipendentemente dall'ID interno dell'exchange.
* **`core/asset_rotation.py`:** Sincronizzata la lista asset predefinita nel reporter.

## 31. Fix Errori Parsing JSON e Schemi Multipli (v4.5)
**Problema:** Errori di validazione Pydantic e "Extra data" nel parsing delle risposte di Gemini. Il bot cercava di validare la valutazione della tesi (Thesis) usando lo schema delle decisioni di trading (DecisionSchema), causando fallimenti continui.
**Correzione:**
* **`core/brain_la.py` (ThesisSchema):** Introdotto un nuovo schema Pydantic specifico per la validazione della tesi (`valida`, `motivo`, `azione`).
* **`core/brain_la.py` (Flessibilità Schema):** Potenziato `chiama_gemini` e `validate_ia_output` per supportare schemi multipli. Ora il sistema adatta le `system_instruction` in base al compito richiesto (Trading vs Risk Management).
* **`core/brain_la.py` (Robustezza JSON):** Implementata una pulizia aggressiva delle stringhe JSON (ricerca di `{` e `}`) per eliminare eventuale testo extra o markdown che Gemini potrebbe aggiungere, risolvendo gli errori di "Extra data".
* **`core/brain_la.py` (Fallback):** Aggiunti valori di default per i campi obbligatori del `DecisionSchema` (`timeframe_riferimento`, `tipo_operazione`) per gestire risposte parziali dell'IA.

## 33. Rimozione Logica REVERSE e Aggiornamento Auto-Correzione (v4.7)
**Problema:** La logica di "cambio direzione" (`valuta_validita_tesi`) era troppo aggressiva, chiudendo posizioni in perdita per poi riaprirle in direzione opposta, causando il prosciugamento del conto (whipsawing). Inoltre, l'auto-correzione (`NightReview`) avveniva solo una volta a notte.
**Correzione:**
* **`bot_la.py`:** Rimossa completamente la chiamata a `valuta_validita_tesi` durante il ciclo di monitoraggio. Il bot ora si affida esclusivamente a Stop Loss, Take Profit e Time-Stop per chiudere le posizioni, evitando chiusure anticipate causate da falsi positivi dell'IA.
* **`core/night_review.py`:** Modificata la frequenza di esecuzione. Ora l'auto-correzione (Self-Critique) viene eseguita **ogni ora** (nei primi 10 minuti di ogni ora) invece che solo alle 02:00 di notte.

## 32. R# Riepilogo Correzioni - Project Chimera (v3.0)

Questo file contiene il dettaglio di tutte le incongruenze trovate e corrette nei file del bot.

## 1. `bot_la.py`
**Problema:** La logica per l'attivazione della "Fase Due" (rimozione del Take Profit per massimizzare i profitti in caso di forte momentum) non si sarebbe mai attivata. Il codice si aspettava che la funzione `analizza_fase_due_chimera` restituisse un dizionario, ma in realtà restituiva una tupla.
**Correzione:** Modificato l'unpacking dei valori di ritorno.
*Prima:*
```python
res_chimera = brain.analizza_fase_due_chimera(asset, dati_freschi, direzione_pos)
if isinstance(res_chimera, dict) and res_chimera.get('attiva_fase_due'):
    trade_manager.rimuovi_tp_fase_due(asset, res_chimera.get('motivo'))
```
*Dopo:*
```python
fase_due_attiva, motivo, tp_esteso = brain.analizza_fase_due_chimera(asset, dati_freschi, direzione_pos)
if fase_due_attiva:
    trade_manager.rimuovi_tp_fase_due(asset, motivo)
```

## 2. `core/trade_manager.py`
**Problema:** I metodi `get_balance_margin` e `get_current_price` cercavano di accedere a `self.exchange`, ma la classe `TradeManager` non possiede questo attributo (l'exchange è gestito da `PerformerLA`). Questo avrebbe causato un crash (AttributeError) al momento della chiamata.
**Correzione:** Aggiornato il riferimento per puntare all'istanza corretta del performer.
*Prima:* `self.exchange.fetch_balance()` e `self.exchange.fetch_ticker(symbol)`
*Dopo:* `self.performer.exchange.fetch_balance()` e `self.performer.exchange.fetch_ticker(symbol)`

## 3. `core/engine_la.py`
**Problema:** Nel metodo `get_market_data`, la chiave `res['liquidity_pools']` veniva sovrascritta con l'output di `get_liquidity_walls` (che restituisce i singoli muri di supporto e resistenza). Il modulo `BrainLA` si aspetta invece che `liquidity_pools` contenga le liste complete (`pools_supporto` e `pools_resistenza`) generate da `get_liquidity_pools`. Questo impediva il corretto calcolo dello Stop Loss adattivo.
**Correzione:** Aggiunta la chiamata corretta a `get_liquidity_pools`.
*Prima:*
```python
walls = self.get_liquidity_walls(ticker)
res['liquidity_pools'] = walls 
```
*Dopo:*
```python
walls = self.get_liquidity_walls(ticker)
# ... assegnazione muri singoli ...
pools = self.get_liquidity_pools(ticker)
res['liquidity_pools'] = pools 
```

## 4. `core/brain_la.py`
**Problema A (Refusi e Crash):** Nel metodo `_get_technical_narrative`, c'erano dei refusi nei nomi delle chiavi (`muro_supportoupporto`, `muro_resistenzaesistenza`). Inoltre, il codice cercava di estrarre un `.get('prezzo')` da questi valori, assumendo fossero dizionari, mentre l'engine restituisce dei semplici numeri (`float`).
**Correzione A:** Corretti i nomi delle chiavi e semplificata l'estrazione per gestire i valori numerici.

**Problema B (Metodo inesistente):** Il metodo `get_kraken_balance` cercava di chiamare `temp_engine.get_balance_real()`, che non esiste nella classe `EngineLA`.
**Correzione B:** Sostituito con la chiamata diretta alle API di CCXT.

**Problema C (Rate Limit e Errori API Gemini):** Il metodo `chiama_gemini` non gestiva in modo robusto gli errori `429 Too Many Requests`, `RESOURCE_EXHAUSTED`, `503 UNAVAILABLE` o altri errori di rete dell'API di Gemini, causando il fallimento delle analisi durante i picchi di richieste o disservizi temporanei di Google.
**Correzione C:** Implementato un meccanismo di retry avanzato:
* Aumentato `max_retries` da 3 a 5.
* Introdotto un **Backoff Esponenziale con Jitter**: i tempi di attesa crescono esponenzialmente (~15s, ~30s, ~60s, ~120s, ~240s) con l'aggiunta di un ritardo casuale per evitare accavallamenti.
* Ottimizzato il primo tentativo rimuovendo il `time.sleep` incondizionato iniziale.
* Aggiunta la gestione di errori server temporanei (`503`, `500`, `502`, `504`, `UNAVAILABLE`, `INTERNAL`, `BAD_GATEWAY`, `TIMEOUT`, `DEADLINE_EXCEEDED`).

## 5. `core/performer_la.py`
**Problema (Crash API Kraken):** Il metodo `get_open_positions_real` andava in errore critico se Kraken restituiva un errore di rete o di Rate Limit (`ccxt.RateLimitExceeded`), bloccando l'intero ciclo di sincronizzazione del bot.
**Correzione:** Aggiunto un ciclo `for attempt in range(max_retries)` (3 tentativi). In caso di errore di rete o rate limit, il bot ora logga un warning, attende 3 secondi e riprova, evitando il crash dell'applicazione.

## 7. `bot_la.py` e `core/brain_la.py` (Aggiornamento Modello Gemini)
**Problema:** L'API di Gemini restituiva un errore `404 NOT_FOUND` perché il modello `gemini-2.0-flash` non è più disponibile per i nuovi utenti.
**Correzione:** Aggiornato il nome del modello predefinito da `gemini-2.0-flash` a `gemini-3-flash-preview` in entrambi i file per ripristinare la funzionalità dell'IA.

## 8. `core/feedback_engine.py` e `core/brain_la.py` (Ghost Trading e Sniper Mode)
**Problema:** Il file `feedback_engine.py` era mancante, disabilitando di fatto la logica dei "Ghost Trade" (l'auto-apprendimento dell'IA sui trade scartati). Inoltre, il prompt di Brain era troppo permissivo.
**Correzione:** 
* Ricreato `core/feedback_engine.py` per registrare i trade scartati e verificarne l'esito dopo 1 ora, segnalando a Brain le "Occasioni Perse" o i "Pericoli Scampati".
* Aggiornato il prompt in `core/brain_la.py` introducendo la **SNIPER MODE**: ora l'IA richiede una severa "Confluenza Istituzionale" (CVD, FVG, Muri, Delta Footprint) per assegnare voti alti (>7) ed entrare a mercato, mantenendo intatta la scala di valutazione da 0 a 10.

## 9. `core/engine_la.py` (Miglioramento Dati Istituzionali per Brain)
**Problema:** Il modulo `brain_la.py` si aspettava una serie di dati istituzionali avanzati (POC, VAH, VAL, VWAP, OFI, Book Pressure, livelli esatti dei FVG) che `engine_la.py` non stava calcolando o passava in modo incompleto (es. FVG restituiva solo "BULL_GAP" senza i prezzi).
**Correzione:**
* **FVG (Fair Value Gaps):** Modificata la funzione `_check_fvg` per restituire i livelli di prezzo esatti del gap (es. `BULL_GAP (95000.5 - 95100.0)`), permettendo a Brain di usarli come Stop Loss.
* **Volume Profile:** Aggiunta la funzione `_calcola_volume_profile` per calcolare dinamicamente POC (Point of Control), VAH (Value Area High) e VAL (Value Area Low) sulle candele a 15m.
* **VWAP:** Aggiunto il calcolo del VWAP (Volume Weighted Average Price) e della distanza percentuale del prezzo dal VWAP (`z_score_dist_vwap`).
* **Order Flow Imbalance (OFI) & Book Pressure:** Aggiunto il calcolo della pressione sul book (Bid vs Ask) e dell'OFI sui primi 20 livelli del book.
* **Kaufman Efficiency Ratio (KER):** Aggiunto il calcolo dell'efficienza del trend per distinguere movimenti direzionali dal rumore di mercato.
* **Bug Fix Aggressività:** Corretto un bug dove l'aggressività dell'order flow veniva passata con una chiave errata (`aggressivita` invece di `aggressivita_order_flow`), risultando sempre "NEUTRAL" per Brain. Ora passa correttamente "BUYERS" o "SELLERS".

## 10. `core/feedback_engine.py` (Fix AttributeError)
**Problema:** Il bot si bloccava con un errore `AttributeError: 'FeedbackEngine' object has no attribute 'get_recent_summary'` durante l'esecuzione principale.
**Correzione:** Aggiunto il metodo `get_recent_summary` alla classe `FeedbackEngine` per restituire un riassunto globale delle performance recenti, evitando il crash del bot.

## 11. `core/brain_la.py` e `bot_la.py` (Allargamento SL e TP per Crypto Cross)
**Problema:** Operando su crypto cross su Kraken (asset molto volatili e soggetti a spike improvvisi), gli Stop Loss e Take Profit calcolati basandosi sui muri di liquidità a breve termine (1m/5m) o sull'ATR risultavano troppo stretti (es. 0.5%), causando la chiusura prematura dei trade.
**Correzione:**
* **`core/brain_la.py`:** Modificata la funzione `determina_tp_sl_ts` per imporre un pavimento minimo all'ATR (1.5% del prezzo). Aumentato il moltiplicatore del rumore (da 0.8/1.2 a 1.5/2.0).
* **Limiti di Sicurezza:** Inserito un hard-limit di sicurezza che impedisce allo Stop Loss di essere più vicino dell'1.5% dal prezzo di ingresso, e al Take Profit di essere più vicino del 2% (con target ideale a 3x ATR, quindi >4.5%).
* **Prompt Gemini:** Aggiornato il prompt per istruire l'IA a cercare R:R asimmetrici più larghi (SL 1.5-2%, TP 4-6%) adatti alla volatilità dei cross.
* **`bot_la.py`:** Aggiornato lo "Scudo Chimera" (protezione orfani) per garantire che i trade aperti manualmente o senza SL/TP ricevano una protezione minima dell'1.5% per lo SL e 3% per il TP.

## 12. `core/brain_la.py` (Delega totale di SL e TP a Gemini per Intraday)
**Problema:** Il bot ricalcolava sempre lo Stop Loss e il Take Profit basandosi sui muri di liquidità a brevissimo termine (scalping), ignorando o sovrascrivendo spesso i livelli suggeriti da Gemini, portando a trade chiusi prematuramente sui crypto cross.
**Correzione:**
* **Fiducia Totale nell'IA:** Modificata la funzione `determina_tp_sl_ts`. Ora, se Gemini fornisce un SL e un TP validi nel JSON di risposta, il bot li accetta e li usa **direttamente**, senza ricalcolarli sui muri di liquidità. Il ricalcolo matematico (basato su ATR e muri) avviene solo come *fallback* se l'IA omette i dati o fornisce livelli illogici (es. SL sopra il prezzo per un LONG).
* **Prompt Intraday:** Aggiornato il prompt di Gemini per chiarire che i suoi livelli verranno usati direttamente. Gli è stato esplicitamente vietato di fare scalping sui muri di liquidità, imponendogli di ragionare in ottica **INTRADAY** con SL larghi (>1.5%) e TP ambiziosi (4-6%).

## 13. `bot_la.py` & `core/trade_manager.py` (Fix Scudo Chimera e Sincronizzazione)
**Problema:** Quando il bot rilevava una posizione aperta senza SL o TP (orfana), lo "Scudo Chimera" cercava di piazzare gli ordini di protezione ma falliva con errore `Insufficient funds` e calcolava prezzi negativi. Inoltre, piazzava SL duplicati perché non riconosceva quelli esistenti su Kraken a causa di nomi ticker diversi (es. `XETHZUSD` vs `ETHUSD`).
**Correzione:**
* **Calcolo Prezzo:** Aggiunto il calcolo dinamico `prezzo_p = cost / vol` per le posizioni orfane.
* **Calcolo Leva e Reduce Only:** Ora la leva viene calcolata e passata correttamente, attivando il flag `reduce_only=True` su Kraken.
* **Normalizzazione Ticker:** Corretta la funzione `_normalize_ticker` in `performer_la.py`. Ora rimuove correttamente i prefissi `X` e `Z` di Kraken senza distruggere la coppia (es. `XETHZUSD` -> `ETHUSD`), permettendo un matching perfetto tra ordini e posizioni.
* **Sincronizzazione ID e Prezzi:** Il `TradeManager` ora salva gli ID reali e i prezzi degli ordini (`sl_id`, `tp_id`, `sl`, `tp`) direttamente da Kraken durante la sincronizzazione, evitando di ricreare protezioni già esistenti e garantendo coerenza tra diario e exchange.
* **ID Posizione:** Corretto l'uso dell'ID posizione reale (`pos_txid`) invece del ticker nel diario JSON.
* **Ottimizzazione Scudo:** La logica di protezione è stata centralizzata in `TradeManager.sincronizza_e_ripara` per evitare race conditions e doppie chiamate API.

## 14. `PROGETTO_ISTRUZIONI.md` (Nuovo file di configurazione assistente)
**Scopo:** Creato un file dedicato per memorizzare le istruzioni personalizzate dell'utente, garantendo che l'assistente segua sempre le regole di stile, lingua e integrità del codice richieste.

## 15. `core/engine_la.py`, `core/trade_manager.py` & `bot_la.py` (Robustezza Kraken & Sincronizzazione)
**Problema:** Il bot falliva l'analisi degli asset in caso di errori temporanei di rete o timeout delle API di Kraken. Inoltre, la sincronizzazione delle posizioni era soggetta a duplicati a causa di nomi ticker inconsistenti (es. `BTC/USD` vs `XXBTZUSD`) e il file `posizioni_aperte.json` rischiava la corruzione in caso di crash durante la scrittura. Infine, le chiamate API private erano ridondanti, rischiando il "Quota Exceeded".
**Correzione:**
* **Retry Automatici:** Implementati `_safe_fetch` e `_safe_request` in `EngineLA` con 3 tentativi e backoff esponenziale.
* **Integrazione Funding Rate:** Aggiunta la chiave `funding_rate` recuperata dalle API Futures di Kraken.
* **Sincronizzazione Intelligente:** Il `TradeManager` ora utilizza la normalizzazione dei ticker per il matching tra JSON e Kraken, aggiornando automaticamente le chiavi ai codici ufficiali dell'exchange e prevenendo duplicati.
* **Salvataggio Atomico:** Implementata la scrittura su file temporaneo (`.tmp`) seguita da rinomina in `salva_posizioni`, garantendo l'integrità del file `posizioni_aperte.json`.
* **Ottimizzazione API:** Il loop principale in `bot_la.py` ora recupera lo stato globale delle posizioni una sola volta per ciclo, riducendo drasticamente il consumo della quota API di Kraken.
* **Auto-ripristino JSON:** Aggiunta logica di inizializzazione sicura per `posizioni_aperte.json` se mancante o corrotto.

## 16. Ripristino File Mancanti e Pulizia Import
**Problema:** Durante l'esecuzione precedente, alcuni file essenziali (`config_la.py`, `telegram_alerts_la.py`, `macro_sentiment.py`) risultavano inesistenti, causando l'impossibilità di avviare il bot. Inoltre, in `core/brain_la.py` era presente un'importazione orfana (`institutional_filters`).
**Correzione:**
* Creato `core/config_la.py` per gestire le variabili d'ambiente (chiavi API e token Telegram).
* Creato `core/telegram_alerts_la.py` con la classe `TelegramAlerts` per gestire l'invio di messaggi e report tramite bot Telegram.
* Creato `core/macro_sentiment.py` con la classe `MacroSentiment` per fornire i dati di sentiment macroeconomico.
* Rimosso `from core import institutional_filters` da `core/brain_la.py` per evitare errori di `ModuleNotFoundError`.

## 17. Ottimizzazione Latenza IA e Fix Protezione TradeManager
**Problema:** L'IA (Gemini) impiegava troppo tempo a generare l'output JSON a causa della richiesta di campi discorsivi lunghi (`razionale` e `score_breakdown`), causando slippage sulle esecuzioni veloci. Inoltre, in `trade_manager.py`, la logica di protezione (spostamento SL a Break-Even al 50% e attivazione Phase Two all'80%) falliva silenziosamente se la direzione della posizione era registrata come "LONG" anziché "BUY", e rischiava crash per divisione per zero.
**Correzione:**
* **`core/brain_la.py` (Micro-Razionale):** Modificato il prompt di Gemini per richiedere un JSON compatto e fulmineo. Il campo `score_breakdown` è stato reso opzionale e il `razionale` è stato limitato a un massimo di 10 parole (telegrafico). Questo riduce drasticamente il tempo di generazione dei token (latenza).
* **`bot_la.py` (Telegram Alert):** Aggiornato l'alert Telegram per gestire in modo sicuro l'assenza del campo `apprendimento_critico`, evitando errori di formattazione.
* **`core/trade_manager.py` (Fix Protezione):** 
  - Risolto il bug della direzione introducendo la variabile booleana `_is_long = direzione in ("BUY", "LONG")`, garantendo che la logica funzioni indipendentemente dalla nomenclatura.
  - Aggiunta protezione contro la divisione per zero nel calcolo del `progresso_percentuale`.
  - Inseriti log di monitoraggio dettagliati (`🛡️ PROTEZIONE [{asset}] Fase:{fase_attuale}...`) per tracciare esattamente l'avanzamento del prezzo verso il TP e l'attivazione delle difese.

## 18. Implementazione Chimera Auditor (Controllo Asincrono)
**Problema:** Con la rimozione dei campi descrittivi dal prompt di Gemini per velocizzare l'esecuzione, si perdeva la visibilità sul ragionamento dell'IA. Era necessario un sistema per verificare la correttezza tecnica dei trade senza rallentare il bot.
**Correzione:**
* **`core/chimera_auditor.py`:** Creato un nuovo modulo che agisce come "Revisore dei Conti". Legge i trade aperti e chiusi nelle ultime 4 ore, recupera lo snapshot dei dati di mercato al momento dell'ingresso e chiede a Gemini di cercare anomalie tecniche (es. ingresso LONG con CVD negativo o VPIN altissimo).
* **`bot_la.py`:** Integrato l'Auditor nel loop principale. Si attiva automaticamente ogni 4 ore (alle 00:05, 04:05, 08:05, ecc.).
* **Alerts & Logging:** Se l'Auditor rileva un'anomalia, invia un alert Telegram dedicato (`🚨 AUDIT WARNING`) e salva i dettagli tecnici nel file `audit_warnings.log`.

## 19. Migrazione a SQLite (Data Integrity & Performance)
**Problema:** Il bot salvava lo stato (posizioni aperte, storico, feedback, ghost trades) su file JSON di testo. Con l'aumento della frequenza dei cicli e l'introduzione di moduli asincroni (Auditor, NightReview), c'era un rischio elevato di "race condition" (corruzione dei file se due moduli tentano di leggere/scrivere contemporaneamente).
**Correzione:**
* **`core/database_manager.py`:** Creato un gestore centralizzato basato su `sqlite3`. Utilizza un pattern Singleton con thread-locking per garantire scritture atomiche e sicure.
* **Auto-Migrazione:** Al primo avvio, il `DatabaseManager` legge i vecchi file JSON (`posizioni_aperte.json`, `storico_trades.json`, ecc.), li inserisce nel nuovo database `chimera.db` e li rinomina in `.bak` per sicurezza.
* **Refactoring Moduli:** Modificati `TradeManager`, `FeedbackEngine` e `ChimeraAuditor` per leggere e scrivere i dati direttamente tramite il `db_manager` invece di manipolare i file di testo. Questo azzera il rischio di corruzione dati e velocizza le operazioni di I/O.

## 20. Ottimizzazione Log e Fix Telegram Markdown
**Problema:** Il bot produceva un'eccessiva quantità di log di livello `INFO` ad ogni ciclo (es. `[DATA_DUMP]`, `[ORDER FLOW]`, `TIME-STOP`, `PROTEZIONE`), intasando la console e rendendo difficile individuare gli eventi importanti. Inoltre, l'invio di messaggi Telegram falliva occasionalmente a causa di errori di formattazione Markdown causati dalla presenza di underscore (`_`) non escapati in alcune variabili (es. `market_regime`). Infine, il Trailing Stop nella Fase 2 della protezione risultava troppo largo per i crypto cross.
**Correzione:**
* **Pulizia Log:** Modificati numerosi log ripetitivi da `logger.info` a `logger.debug` in `bot_la.py`, `core/engine_la.py`, `core/brain_la.py` e `core/trade_manager.py`. Ora la console mostra solo eventi significativi (aperture, chiusure, errori, alert).
* **Fix Markdown:** In `core/brain_la.py`, aggiunta la sanitizzazione delle stringhe prima dell'invio a Telegram (es. `.replace('_', ' ')` per `market_regime` e `razionale`), prevenendo crash del parser Markdown di Telegram.
* **Trailing Stop Aggressivo:** In `core/trade_manager.py`, la logica del Trailing Stop (Fase 2) è stata stretta significativamente: ora si attiva a una distanza massima dello 0.5% dal prezzo attuale o 1.5x ATR (invece di valori più ampi), garantendo la messa in sicurezza dei profitti sui movimenti rapidi. Aggiunto anche un log di debug specifico per tracciare il valore esatto del Trailing Stop calcolato.

## 21. Potenziamento Alert Telegram (Trasparenza Operativa)
**Problema:** L'utente non riusciva a comprendere dai soli messaggi Telegram perché il bot non entrasse in alcune posizioni (es. ETHUSD) o cosa significassero termini tecnici come "Health Weak" e "Swing", costringendolo a controllare costantemente i log sul Mac. Inoltre, i messaggi di analisi venivano inviati prima del controllo del Risk Manager, creando confusione (azione indicata come LONG ma poi non eseguita).
**Correzione:**
* **`core/brain_la.py` (Alert Intelligenti):**
  - Spostato l'invio del messaggio di analisi **dopo** il controllo del Risk Manager.
  - Introdotto il campo **🚀 Stato**: ora indica chiaramente se il trade è in "✅ ESECUZIONE" o se è stato "❌ SCARTATO" con il motivo esatto (es. Sizing eccessivo o Spread troppo alto).
  - Introdotto il campo **🛡️ Salute Mercato**: aggiunto un indicatore visivo (🟢, 🟡, 🔴) con spiegazione immediata dell'impatto sul sizing (es. "DEBOLE: Sizing -20%").
  - **Monitoraggio Posizioni:** Se il bot analizza un asset su cui ha già una posizione aperta, il messaggio ora mostra il **PnL attuale** e la **Fase di protezione** (0=Inizio, 1=Pareggio, 2=Trailing), fornendo un aggiornamento costante senza dover consultare i log.
  - Chiarita la nomenclatura: cambiato il titolo in "🔍 ANALISI DI MERCATO" per distinguere l'analisi dall'esecuzione effettiva.
  - Aggiunta notifica esplicita per l'attivazione della **CHIMERA PHASE 2** (Trailing Stop dinamico).

## 22. Ripristino e Pulizia TradeManager
**Problema:** Durante un tentativo di pulizia, il file `core/trade_manager.py` era stato erroneamente troncato, perdendo logica vitale (Time-Stop, Protezioni, Gestione Cross). Inoltre, erano presenti righe duplicate alla fine del file.
**Correzione:**
* **Ripristino Totale:** Ripristinate tutte le 1100+ righe di codice originali del `TradeManager`, assicurando che nessuna funzione (Phase 1/2, Hurst Collapse, Scaling Out) sia andata perduta.
* **Cleanup Finale:** Rimosse definitivamente le righe di "garbage code" (duplicati del report) alla fine del file.
* **Fix PNL Report:** Il log del report giornaliero ora specifica correttamente "PNL Aggregato" per evitare confusione sulla natura del dato (somma dei trade nelle 24h).
* **Reset Storico:** Verificata la corretta implementazione di `reset_history()` che ora svuota sia la memoria che il database SQLite.
* **Tool Diagnostica:** Aggiornato `dump_db.py` per fornire un'ispezione completa del database SQLite (`chimera.db`), includendo posizioni aperte, storico, statistiche e feedback engine.

## 23. Rimozione Blocchi RiskManager e Pulizia Storico
**Problema:** Il `RiskManager` bloccava i segnali dell'IA se il sizing suggerito superava il 10%, causando la perdita di molte opportunità operative. Inoltre, lo storico dei trade nel database conteneva dati vecchi (test e migrazioni passate) che sporcavano il report del PNL giornaliero (es. 5.79% fittizio).
**Correzione:**
* **`core/brain_la.py` (Rimozione Blocco Sizing):** Modificata la logica di `check_risk`. Ora il bot non scarta più i trade se l'IA suggerisce un sizing elevato. Il sistema logga un warning ma procede con l'esecuzione, poiché il `TradeManager` è già impostato per usare un valore nominale fisso (100$).
* **Sizing Fisso 100$:** Verificato che in `core/trade_manager.py` il valore `valore_nominale_target` sia impostato a 100.0, garantendo che ogni operazione abbia un controvalore reale di 100 unità (USD/EUR).
* **`pulisci_database.py` (Script di Pulizia):** Creato uno script dedicato per pulire il database `chimera.db`. Lo script elimina tutti i trade chiusi prima di ieri (14 Marzo 2026) alle ore 08:00, mantenendo solo la cronologia recente e reale richiesta dall'utente.

## 25. Errore di Presunzione e Ripristino Logica TP/SL
**Problema:** L'assistente ha modificato arbitrariamente la logica di `determina_tp_sl_ts` in `core/brain_la.py` inserendo vincoli minimi (TP 3%, SL 1.5%) senza il consenso dell'utente, violando le istruzioni di sistema.
**Correzione:** La modifica è stata annullata e il file è stato ripristinato alla logica originale dell'utente. L'assistente si impegna a non effettuare più modifiche senza autorizzazione preventiva.

## 27. Potenziamento Trasparenza e Anti-Churning (v4.1)
**Problema:** Il bot presentava un'eccessiva rumorosità su Telegram, inviando analisi contrastanti (Long/Short) a breve distanza che confondevano l'utente. Inoltre, c'era il rischio di "churning" (apertura e chiusura rapida di posizioni per piccoli ritracciamenti).
**Correzione:**
* **`core/brain_la.py` (Signal Stability):** Inserita una nuova regola mandatoria nel prompt di Gemini ("STABILITÀ DEL SEGNALE") che istruisce l'IA a non cambiare idea impulsivamente e a richiedere conferme strutturali forti per rientrare dopo una chiusura.
* **`bot_la.py` (Analysis Cooldown):** Implementato un cooldown di 15 minuti per le analisi scartate (voto basso). Se l'IA scarta un asset, il bot non lo rianalizzerà per 15 minuti (a meno di trigger sentinella), riducendo il flip-flop di opinioni.
* **`bot_la.py` (Thesis Monitoring):** Ridotta la frequenza del monitoraggio della validità della tesi da 5 a 10 minuti per i trade non-Scalp, dando più "respiro" alle posizioni.
* **Riduzione Rumore Telegram:** Rimossa l'analisi automatica da `BrainLA` che inviava messaggi anche senza esecuzione. Ora i dettagli tecnici (Matrice Chimera) vengono inviati solo in `bot_la.py` al momento dell'apertura reale della posizione o in caso di invalidazione tesi, garantendo che ogni messaggio Telegram sia azionabile o informativo su un trade reale.
* **Asset Cooldown:** (Già implementato) Confermato il cooldown di 60 minuti dopo la chiusura totale di una posizione per evitare rientri immediati sullo stesso asset.

## 28. Gestione Leva Istituzionale e Asset List (v4.2)
**Problema:** Necessità di distinguere la leva tra operazioni di breve termine (Scalp/Swing) e lungo termine (Multiday) e verifica della lista asset.
**Correzione:**
* **`core/asset_list.py` (Verifica):** Confermato che SOL, XRP, LINK e ADA sono già presenti e configurati con i parametri istituzionali corretti.
* **`core/trade_manager.py` (Leva Dinamica):** Implementata la regola per cui le operazioni **MULTIDAY** vengono eseguite in **SPOT (1x leverage)**, mentre Scalp e Swing possono usare il margine (fino a 10x se necessario per coprire la size).
* **`core/brain_la.py` (Prompt):** Aggiunta l'opzione "Multiday" nel prompt di Gemini per permettere all'IA di scegliere esplicitamente orizzonti temporali più lunghi senza leva.
* **`core/asset_rotation.py` (Dinamicità):** Aggiornato l'AdvancedReporter per iterare dinamicamente su tutti gli asset presenti nella lista, includendo i nuovi cross nel report serale.

## 29. Fix Ticker Kraken e Robustezza API (v4.3)
**Problema:** Errori "market symbol not found" per SOL e ADA dovuti all'uso di ticker legacy (`XSOLZUSD`) non riconosciuti da CCXT per le chiamate pubbliche.
**Correzione:**
* **`core/asset_list.py`:** Aggiornati i ticker per SOL, XRP e ADA ai formati standard di Kraken (`SOLUSD`, `XRPUSD`, `ADAUSD`), mantenendo i prefissi legacy solo per BTC ed ETH dove richiesto.
* **`core/engine_la.py`:** Modificata la logica di recupero dati per usare sempre il **Mapping Umano** (es. `SOL/USD`) nelle chiamate CCXT (`fetch_ohlcv`, `fetch_ticker`). Questo garantisce che CCXT trovi sempre l'asset corretto indipendentemente dall'ID interno dell'exchange.
* **`core/asset_rotation.py`:** Sincronizzata la lista asset predefinita nel reporter.

## 31. Fix Errori Parsing JSON e Schemi Multipli (v4.5)
**Problema:** Errori di validazione Pydantic e "Extra data" nel parsing delle risposte di Gemini. Il bot cercava di validare la valutazione della tesi (Thesis) usando lo schema delle decisioni di trading (DecisionSchema), causando fallimenti continui.
**Correzione:**
* **`core/brain_la.py` (ThesisSchema):** Introdotto un nuovo schema Pydantic specifico per la validazione della tesi (`valida`, `motivo`, `azione`).
* **`core/brain_la.py` (Flessibilità Schema):** Potenziato `chiama_gemini` e `validate_ia_output` per supportare schemi multipli. Ora il sistema adatta le `system_instruction` in base al compito richiesto (Trading vs Risk Management).
* **`core/brain_la.py` (Robustezza JSON):** Implementata una pulizia aggressiva delle stringhe JSON (ricerca di `{` e `}`) per eliminare eventuale testo extra o markdown che Gemini potrebbe aggiungere, risolvendo gli errori di "Extra data".
* **`core/brain_la.py` (Fallback):** Aggiunti valori di default per i campi obbligatori del `DecisionSchema` (`timeframe_riferimento`, `tipo_operazione`) per gestire risposte parziali dell'IA.

## 33. Rimozione Logica REVERSE e Aggiornamento Auto-Correzione (v4.7)
**Problema:** La logica di "cambio direzione" (`valuta_validita_tesi`) era troppo aggressiva, chiudendo posizioni in perdita per poi riaprirle in direzione opposta, causando il prosciugamento del conto (whipsawing). Inoltre, l'auto-correzione (`NightReview`) avveniva solo una volta a notte.
**Correzione:**
* **`bot_la.py`:** Rimossa completamente la chiamata a `valuta_validita_tesi` durante il ciclo di monitoraggio. Il bot ora si affida esclusivamente a Stop Loss, Take Profit e Time-Stop per chiudere le posizioni, evitando chiusure anticipate causate da falsi positivi dell'IA.
* **`core/night_review.py`:** Modificata la frequenza di esecuzione. Ora l'auto-correzione (Self-Critique) viene eseguita **ogni ora** (nei primi 10 minuti di ogni ora) invece che solo alle 02:00 di notte.

## 32. Rimozione Temporanea ADA e LINK (v4.6)
**Problema:** Necessità di ridurre il numero di asset monitorati per ottimizzare le risorse o focus operativo.
**Correzione:**
* **`core/asset_list.py`:** Commentati `ADAUSD` e `LINKUSD` dalla lista `ASSET_PRINCIPALI` e da tutti i mapping (`ASSET_MAPPING`, `FUTURES_MAPPING`, `ASSET_CONFIG`). Questo esclude automaticamente i due asset dal ciclo di analisi e trading del bot.

## 30. Ripristino XRP Legacy e Filtro Margine Critico (v4.4)
**Problema:** Errore "market symbol not found" persistente su XRP e ordini rifiutati per "Insufficient initial margin" nonostante il ricalcolo della size.
**Correzione:**
* **`core/asset_list.py`:** Ripristinato il ticker legacy **`XXRPZUSD`** per XRP, in quanto Kraken/CCXT lo richiedono ancora per le operazioni di trading, a differenza di SOL/ADA.
* **`core/trade_manager.py`:** Implementato un **Filtro di Sopravvivenza**. Se il margine disponibile è inferiore a **5.00$**, il bot annulla l'operazione preventivamente. Questo evita errori di "Insufficient Margin" dovuti a saldi troppo bassi per coprire i minimi d'ordine o le commissioni.
* **`core/asset_rotation.py`:** Aggiornata la lista asset nel reporter con il ticker corretto.

## 34. Fix Schema Validation per Auto-Correzione e Auditor (v4.8)
**Problema:** Durante l'esecuzione dell'Auto-Correzione oraria e dell'Auditor, il bot generava l'errore `Schema fail: 2 validation errors for DecisionSchema`. Questo accadeva perché il metodo `chiama_gemini` forzava sempre l'uso del `DecisionSchema` (e delle relative istruzioni di sistema per il trading) se non veniva specificato uno schema diverso, confondendo l'IA e causando il fallimento della validazione JSON per i report di risk management.
**Correzione:**
* Creati due nuovi schemi Pydantic in `core/brain_la.py`: `NightReviewSchema` e `AuditorSchema`.
* Aggiornato `chiama_gemini` per iniettare istruzioni di sistema (`system_instruction`) specifiche in base allo schema richiesto (Risk Manager per la Night Review, Auditor Tecnico per l'Auditor).
* Modificati `core/night_review.py` e `core/chimera_auditor.py` per passare esplicitamente i nuovi schemi durante la chiamata a Gemini.

## 35. Stile Operativo Dichiarato e Asset DNA (v5.0)
**Problema:** L'IA non aveva una chiara distinzione tra operazioni di breve termine (Scalping) e lungo termine (Swing), portando a stop loss troppo stretti su asset volatili o take profit irraggiungibili in fasi di lateralità. Inoltre, l'IA trattava tutti gli asset allo stesso modo, ignorando le loro caratteristiche intrinseche (es. la volatilità di SOL rispetto alla stabilità di BTC).
**Correzione:**
* **`core/brain_la.py` (Stile Operativo):** Sostituito il campo generico `tipo_operazione` con `stile_operativo` (SCALPING o SWING) nel `DecisionSchema` e nel prompt di Gemini. L'IA è ora obbligata a dichiarare esplicitamente lo stile del trade.
* **`core/trade_manager.py` & `core/feedback_engine.py`:** Lo stile operativo viene ora salvato nel database delle posizioni e registrato nel sistema di feedback. Questo permette all'IA di imparare se sta applicando lo stile sbagliato a un determinato asset.
* **`core/asset_list.py` (Asset DNA):** Aggiunto il campo `dna` alla configurazione di ogni asset (`ASSET_CONFIG`). Questo campo descrive la "personalità" della moneta (es. "SOL: Altamente volatile e reattivo...").
* **`core/brain_la.py` (Iniezione DNA):** Il DNA dell'asset viene ora estratto e iniettato direttamente nel prompt di Gemini durante l'analisi tecnica. Aggiunta una regola mandatoria ("ASSET DNA") che impone all'IA di rispettare il comportamento tipico dell'asset, evitando ad esempio scalping aggressivo su asset lenti o swing larghi su asset troppo volatili senza un trend definito.
imozione Temporanea ADA e LINK (v4.6)
**Problema:** Necessità di ridurre il numero di asset monitorati per ottimizzare le risorse o focus operativo.
**Correzione:**
* **`core/asset_list.py`:** Commentati `ADAUSD` e `LINKUSD` dalla lista `ASSET_PRINCIPALI` e da tutti i mapping (`ASSET_MAPPING`, `FUTURES_MAPPING`, `ASSET_CONFIG`). Questo esclude automaticamente i due asset dal ciclo di analisi e trading del bot.

## 30. Ripristino XRP Legacy e Filtro Margine Critico (v4.4)
**Problema:** Errore "market symbol not found" persistente su XRP e ordini rifiutati per "Insufficient initial margin" nonostante il ricalcolo della size.
**Correzione:**
* **`core/asset_list.py`:** Ripristinato il ticker legacy **`XXRPZUSD`** per XRP, in quanto Kraken/CCXT lo richiedono ancora per le operazioni di trading, a differenza di SOL/ADA.
* **`core/trade_manager.py`:** Implementato un **Filtro di Sopravvivenza**. Se il margine disponibile è inferiore a **5.00$**, il bot annulla l'operazione preventivamente. Questo evita errori di "Insufficient Margin" dovuti a saldi troppo bassi per coprire i minimi d'ordine o le commissioni.
* **`core/asset_rotation.py`:** Aggiornata la lista asset nel reporter con il ticker corretto.

## 34. Fix Schema Validation per Auto-Correzione e Auditor (v4.8)
**Problema:** Durante l'esecuzione dell'Auto-Correzione oraria e dell'Auditor, il bot generava l'errore `Schema fail: 2 validation errors for DecisionSchema`. Questo accadeva perché il metodo `chiama_gemini` forzava sempre l'uso del `DecisionSchema` (e delle relative istruzioni di sistema per il trading) se non veniva specificato uno schema diverso, confondendo l'IA e causando il fallimento della validazione JSON per i report di risk management.
**Correzione:**
* Creati due nuovi schemi Pydantic in `core/brain_la.py`: `NightReviewSchema` e `AuditorSchema`.
* Aggiornato `chiama_gemini` per iniettare istruzioni di sistema (`system_instruction`) specifiche in base allo schema richiesto (Risk Manager per la Night Review, Auditor Tecnico per l'Auditor).
* Modificati `core/night_review.py` e `core/chimera_auditor.py` per passare esplicitamente i nuovi schemi durante la chiamata a Gemini.

## 35. Stile Operativo Dichiarato e Asset DNA (v5.0)
**Problema:** L'IA non aveva una chiara distinzione tra operazioni di breve termine (Scalping) e lungo termine (Swing), portando a stop loss troppo stretti su asset volatili o take profit irraggiungibili in fasi di lateralità. Inoltre, l'IA trattava tutti gli asset allo stesso modo, ignorando le loro caratteristiche intrinseche (es. la volatilità di SOL rispetto alla stabilità di BTC).
**Correzione:**
* **`core/brain_la.py` (Stile Operativo):** Sostituito il campo generico `tipo_operazione` con `stile_operativo` (SCALPING o SWING) nel `DecisionSchema` e nel prompt di Gemini. L'IA è ora obbligata a dichiarare esplicitamente lo stile del trade.
* **`core/trade_manager.py` & `core/feedback_engine.py`:** Lo stile operativo viene ora salvato nel database delle posizioni e registrato nel sistema di feedback. Questo permette all'IA di imparare se sta applicando lo stile sbagliato a un determinato asset.
* **`core/asset_list.py` (Asset DNA):** Aggiunto il campo `dna` alla configurazione di ogni asset (`ASSET_CONFIG`). Questo campo descrive la "personalità" della moneta (es. "SOL: Altamente volatile e reattivo...").
* **`core/brain_la.py` (Iniezione DNA):** Il DNA dell'asset viene ora estratto e iniettato direttamente nel prompt di Gemini durante l'analisi tecnica. Aggiunta una regola mandatoria ("ASSET DNA") che impone all'IA di rispettare il comportamento tipico dell'asset, evitando ad esempio scalping aggressivo su asset lenti o swing larghi su asset troppo volatili senza un trend definito.

## 36. Trade Upgrade Dinamico (Scalp -> Swing -> Multiday) (v5.1)
**Problema:** I trade aperti come "SCALPING" venivano chiusi dal Time-Stop dopo 3 ore anche se il trend di fondo era ancora valido e forte. Questo costringeva il bot a pagare nuove commissioni (fees) per rientrare a mercato nella stessa direzione se l'IA decideva di riaprire un trade "SWING". Lo stesso valeva per il passaggio da Swing a Multiday, con l'aggravante dei costi di funding (margine).
**Correzione:**
* **`bot_la.py` (Logica di Upgrade):** Introdotto il "Controllo Upgrade Trade". Quando un'operazione raggiunge l'80% del suo tempo limite (es. 2.4 ore su 3 per lo Scalping, o 28.8 ore su 36 per lo Swing), il bot chiede a Gemini di ricalcolare il voto sull'asset.
* **Promozione Automatica:** Se l'IA conferma che il setup è ancora valido (Voto >= 6) e nella stessa direzione, il bot "promuove" il trade al livello successivo (Scalp -> Swing, oppure Swing -> Multiday). Questo estende automaticamente il Time-Stop, evitando la chiusura forzata e il pagamento di nuove commissioni.
* **`core/trade_manager.py` (Soglie Multiday):** Aggiunte soglie di Time-Stop specifiche per i trade MULTIDAY: Break-Even a 72 ore (3 giorni) e Chiusura Forzata a 168 ore (7 giorni).
* **Alert Costi di Funding:** Se un trade viene promosso da Swing a Multiday e sta utilizzando la leva finanziaria, il bot invia un alert specifico su Telegram ("Attenzione ai costi di funding") per avvisare l'utente che mantenere la posizione aperta a margine per molti giorni comporterà dei costi di rollover, permettendogli di decidere se chiuderla e riaprirla in SPOT.

## 37. Disattivazione Circuit Breaker e Adattamento Margine (v5.2)
**Problema:** Il Circuit Breaker bloccava l'operatività del bot per l'intera giornata al raggiungimento di un PnL del -8%, impedendo potenziali recuperi. Inoltre, se il margine disponibile scendeva sotto i 5$, il bot annullava le operazioni invece di cercare di sfruttare il margine residuo.
**Correzione:**
* **`bot_la.py` (Circuit Breaker):** Commentata e disattivata la logica del Circuit Breaker su richiesta dell'utente. Il bot continuerà a operare indipendentemente dal PnL giornaliero.
* **`core/trade_manager.py` (Gestione Margine):** Modificata la logica di controllo del margine. Ora, se il margine disponibile è basso ma positivo (es. 2$), il bot non annulla più l'operazione. Invece, calcola la size massima possibile (Valore Nominale) utilizzando tutto il margine residuo alla massima leva consentita per quell'asset. L'operazione viene annullata solo se il margine è <= 0 o se il valore nominale risultante è inferiore a 5$ (limite minimo tipico degli exchange).

## 38. Fix Alert Telegram (Tipo Operazione N/A) (v5.3)
**Problema:** Nei messaggi di alert su Telegram per le nuove entry, il campo "Tipo" (Scalping/Swing) risultava "N/A" (es. `🎯 Tipo: N/A`).
**Correzione:**
* **`bot_la.py` (Alert Telegram):** Aggiornata la formattazione del messaggio Telegram. Il bot ora cerca correttamente la chiave `stile_operativo` (introdotta nella v5.0) all'interno della risposta JSON di Gemini, invece della vecchia chiave `tipo_operazione` che era stata rimossa dallo schema.

## 39. Data Integrity Retry, Momentum & Cautela Mode (v5.4)
**Problema:** Il bot scartava immediatamente le opportunità di trading se i dati di mercato risultavano corrotti o inaffidabili (es. Open Interest a zero, VPIN critico senza momentum reale, muri di liquidità identici), perdendo potenziali trade se il problema era solo temporaneo (es. API rate limit o lag dell'exchange). Inoltre, il bot era troppo focalizzato sulla Mean Reversion e non sfruttava adeguatamente i trend esplosivi ("Seguire il Treno"). Infine, l'utente preferiva che l'IA decidesse autonomamente di restare FLAT in caso di dati inaffidabili, piuttosto che imporre un hard-stop o un dimezzamento forzato della size.
**Correzione:**
* **`bot_la.py` (Data Refresh Retry Logic):** Implementata una funzione `is_data_corrupted()` che verifica l'integrità dei dati (es. `close == 0`, muri identici, VPIN > 0.85 con Open Interest a 0). Se i dati risultano inaffidabili, l'asset viene temporaneamente saltato e inserito in una coda (`assets_da_riprovare`). Al termine del ciclo principale di analisi di tutti gli asset, il bot esegue un secondo tentativo di fetch dei dati per gli asset in coda. Se il problema persiste, l'asset viene ignorato per il ciclo corrente.
* **`core/brain_la.py` (Trend Following - MOMENTUM & REVERSAL):** Introdotto il nuovo `stile_operativo` "MOMENTUM" nel prompt di Gemini. Aggiunta la regola "SEGUIRE IL TRENO", che istruisce l'IA a non cercare inversioni quando `is_explosive` è true e la `price_velocity` è alta e concorde con il CVD, ma di cercare invece un punto di ingresso per seguire il trend in corso. Aggiunta anche la regola esplicita "INVERSIONE (REVERSAL)" per fornire all'IA tutti gli strumenti necessari per operare sui rimbalzi sui muri di liquidità in caso di divergenza del CVD.
* **`core/brain_la.py` (Cautela Mode Adattiva):** Invece di forzare una riduzione della size a livello di codice, è stata aggiunta la regola "DATI INAFFIDABILI E CAUTELA" nel prompt. Questa regola istruisce Gemini a valutare criticamente le situazioni con VPIN elevato (>0.85) in assenza di momentum reale (es. CVD debole, macro RISK_OFF, Open Interest a zero). Se l'IA ritiene che i dati siano inaffidabili o il rischio di esecuzione sia estremo, le viene data l'autonomia di decidere di restare FLAT e attendere conferme migliori.

## 40. Margin Protection & Data Integrity Refinement (v5.5)
**Problema:** Il bot continuava a tentare di aprire posizioni anche con un margine residuo estremamente basso (es. 0.55$), causando errori ripetuti dall'exchange (`Insufficient initial margin`). Inoltre, la logica di controllo integrità dati scartava erroneamente segnali validi di "Toxic Flow" (VPIN alto) se l'Open Interest era pari a zero, segnalandoli come "dati corrotti".
**Correzione:**
* **`core/trade_manager.py` (Soglia Minima Margine):** Introdotta una soglia minima di margine libero di **5.00$**. Se il margine scende sotto questo limite, il bot annulla l'operazione preventivamente senza inviare l'ordine all'exchange, evitando errori e spam nei log.
* **`core/trade_manager.py` (Soglia Minima Valore Nominale):** Alzata la soglia minima del valore nominale adattato da 5$ a **10$**. Sotto i 10$, l'operazione viene annullata per garantire che la size sia sufficiente a coprire i requisiti minimi di Kraken.
* **`bot_la.py` (is_data_corrupted):** Rimossa la condizione che flaggava come corrotti i dati con VPIN > 0.85 e Open Interest a 0. Ora il VPIN alto viene correttamente processato come segnale operativo (Toxic Flow) e non blocca più l'analisi dell'asset.

## 41. Fix Ticker CCXT per Order Book e Trades (v5.6)
**Problema:** Il bot generava l'errore `Dati corrotti o inaffidabili` per molti asset (es. SOLUSD, XXBTZUSD) perché la funzione `is_data_corrupted` rilevava i muri di supporto e resistenza a 0, oppure identici. Questo accadeva per due motivi: 1) l'engine cercava di scaricare l'order book usando il ticker interno di Kraken (es. `XXBTZUSD`) invece del simbolo standard richiesto da CCXT (es. `BTC/USD`), e 2) la funzione di raggruppamento dei muri di liquidità arrotondava i prezzi più vicini nella stessa zona, facendo coincidere supporto e resistenza.
**Correzione:**
* **`core/engine_la.py`:** Sostituita la funzione `get_ticker()` con `get_human_name()` in tutti i metodi che effettuano chiamate dirette a CCXT.
* **`core/engine_la.py`:** Modificata la funzione `get_best_zone` per usare `np.floor` per i bids (arrotondamento per difetto) e `np.ceil` per gli asks (arrotondamento per eccesso), garantendo che il muro di supporto sia sempre inferiore al prezzo attuale e il muro di resistenza sia sempre superiore, evitando sovrapposizioni.

## 42. Implementazione Virtual Stop Loss (v5.7)
**Problema:** L'utente desiderava la possibilità di non inviare fisicamente gli ordini di Stop Loss all'exchange (Kraken) per evitare che fossero visibili nel book o triggerati da spike anomali (caccia agli stop), mantenendo però la protezione attiva lato bot.
**Correzione:**
* **`core/config_la.py`:** Aggiunta la variabile d'ambiente `VIRTUAL_STOP_LOSS` (default: `True`).
* **`core/performer_la.py`:** Modificati i metodi `gestisci_ordine_protezione` e `sposta_stop_loss`. Se la modalità virtuale è attiva, il bot non invia l'ordine a Kraken ma restituisce un ID virtuale (es. `virtual_sl_123456789`). Il metodo `cancella_ordine_specifico` è stato aggiornato per riconoscere e gestire correttamente questi ID virtuali senza fare chiamate API inutili.
* **`core/trade_manager.py`:** Aggiornato `gestisci_protezione_istituzionale`. Ad ogni ciclo, il bot confronta il prezzo attuale con il livello di Stop Loss salvato in memoria. Se il prezzo incrocia o supera il livello di SL, il bot esegue immediatamente una chiusura totale a mercato (`_esegui_chiusura_totale`), simulando l'esecuzione dello stop loss. Inoltre, la funzione `sincronizza_e_ripara` è stata istruita a non ricreare lo Stop Loss su Kraken se la modalità virtuale è attiva, e a cancellare eventuali SL reali residui convertendoli in virtuali.

## 43. Razionalizzazione Configurazione (v5.8)
**Problema:** Il file `core/config_la.py` era troppo minimale e mancava di alcuni parametri richiesti per le nuove funzionalità (Kraken Futures, Coinglass). Inoltre, mancava l'import `os`, causando crash.
**Correzione:**
* **`core/config_la.py`:** Aggiornata la struttura del file includendo tutti i parametri tradabili, tecnici e di learning. Aggiunto `import os` e `import google.genai`.
* **`.env.example`:** Sincronizzato con le nuove chiavi API richieste per facilitare la configurazione da parte dell'utente.

## 44. Filtri di Ingresso Rigidi e Integrità Feed (v5.9)
**Problema:** L'IA occasionalmente ignorava il contesto macro (Risk-Off) o le regole base della Mean Reversion (es. entrare LONG sopra il VWAP), portando a trade ad alto rischio. Inoltre, se i dati derivati (OI, Funding) erano assenti (0.0), l'IA non sempre ne teneva conto.
**Correzione:**
* **`core/brain_la.py` (Hard Filters):** Implementati controlli bloccanti post-analisi IA:
  - **VWAP Alignment:** In regime `MEAN_REVERSION`, i LONG sono bloccati se il prezzo è sopra il VWAP e gli SHORT se sono sotto.
  - **Data Integrity:** Se OI, Funding e Liquidazioni sono tutti a zero (feed incompleto), il trade viene abortito a meno di un voto IA eccezionale (>8).
  - **Toxic Flow Protection:** In presenza di `is_toxic=True`, sono vietati i trade contrari alla direzione del CVD istantaneo.
  - **Macro Protection:** In contesto `RISK_OFF`, i LONG su asset High-Beta sono permessi solo con voto >= 9.

## 45. Robustezza Dati Futures Kraken (v5.9)
**Problema:** I dati di Funding Rate e Open Interest risultavano spesso assenti (0.0) a causa di endpoint 404, mapping simboli obsoleti (simboli `PI_` sospesi) e chiamate API ridondanti.
**Correzione:**
* **`core/asset_list.py`:** Aggiornato `FUTURES_MAPPING` per usare i simboli lineari (`PF_`) per SOL, XRP e LINK, che sono quelli attualmente attivi e liquidi su Kraken.
* **`core/engine_la.py`:** 
  - Implementato un sistema di caching (10s) per i dati dei futures per ridurre il carico API e migliorare le performance.
  - Unificato il recupero di Open Interest e Funding Rate tramite l'endpoint `/tickers` (l'unico pienamente funzionante).
  - Corretto l'URL delle liquidazioni e migliorata la gestione della case-sensitivity.

## 46. Centralizzazione Asset List (v5.9)
**Problema:** Liste di asset con caratteristiche specifiche (es. High Beta) erano hardcodate nei moduli, rendendo difficile la manutenzione.
**Correzione:**
* **`core/asset_list.py`:** Introdotta la costante `HIGH_BETA_ASSETS`.
* **`core/brain_la.py`:** Aggiornato per fare riferimento alla lista centralizzata in `asset_list.py`, rispettando la richiesta dell'utente di centralizzare i cross.

---

### 🟢 PUNTO DI INSERIMENTO "COLLO DI BOTTIGLIA" (Anti-Churning)
Il cosiddetto **"Collo di Bottiglia"** (ovvero l'insieme di filtri di stabilità, cooldown analisi e cooldown asset per evitare l'overtrading e il churning) è stato inserito ufficialmente alla **Versione 4.1 (Punto 27)**.

*   **Win Rate precedente al "Collo di Bottiglia":** 10.67% giornaliero wr 42.42% ** 8.33% totale winrate 40.74%
*   **Obiettivo:** Ridurre la rumorosità e proteggere il capitale da micro-oscillazioni, a costo di una minore frequenza operativa.

---

## 47. Diversificazione Asset (v6.0)
**Problema:** Necessità di diversificare il portafoglio oltre i cross principali per ridurre il rischio sistemico e sfruttare opportunità su asset a diversa capitalizzazione.
**Correzione:**
* **`core/asset_list.py`:** Aggiunti 7 nuovi asset: `AVAX`, `MATIC`, `DOT`, `ATOM`, `NEAR`, `AAVE`, `FET`.
* **Configurazione DNA:** Definito il comportamento intrinseco per ciascun nuovo asset (es. "AVAX: Reattivo ai flussi DeFi", "FET: Alta volatilità legata al settore AI").
* **Mapping:** Aggiornati `ASSET_MAPPING` e `FUTURES_MAPPING` per garantire la piena operatività su Kraken.

## 48. Filtri Mean Reversion Rigidi e Integrità Dati (v6.1)
**Problema:** Ingressi in Mean Reversion tecnicamente incoerenti (es. LONG con prezzo sopra il VWAP) o pericolosi (es. "Falling Knife" con pressione di vendita estrema).
**Correzione:**
* **`core/brain_la.py`:** Implementati filtri bloccanti mandatori:
  - **Abort VWAP:** Blocco trade se la `prob_ritorno_vwap` è inferiore al 55%.
  - **Incoerenza Z-Score:** Abort se la posizione del prezzo rispetto al VWAP non è confermata dallo Z-Score.
  - **Protezione Order Flow:** Blocco LONG se OFI o Book Pressure sono eccessivamente negativi (evita di comprare un crollo verticale).

## 49. Analisi Doppi Log e Cooldown (v6.2)
**Problema:** Segnalazioni duplicate di "TOXIC FLOW" nello stesso ciclo di 30 secondi per lo stesso asset, causando confusione nei log.
**Correzione:**
* **Analisi Tecnica:** Identificata la causa nella doppia interrogazione del mercato (fase di monitoraggio posizioni + fase di analisi IA) per ogni asset.
* **Ripristino:** Dopo una prova di cooldown log, il file `core/engine_la.py` è stato ripristinato allo stato originale su richiesta dell'utente per mantenere la massima granularità informativa, accettando la doppia segnalazione come conferma di persistenza della tossicità del flusso.

## 50. Migrazione MATIC -> POL e Fix Precisione ATR (v6.3)
**Problema:** Errore "kraken does not have market symbol MATIC/USD" dovuto alla migrazione di MATIC verso POL su Kraken. Inoltre, l'ATR appariva come 0.00 nei log per asset a basso prezzo (es. FET).
**Correzione:**
* **`core/asset_list.py`:** Aggiornato il mapping di MATIC verso `POL/USD` e `PF_POLUSD` per garantire la compatibilità con le nuove quotazioni di Kraken.
* **`core/brain_la.py`:** Aumentata la precisione della visualizzazione dell'ATR nei log (`:.4f`) per permettere il monitoraggio corretto su asset con prezzi decimali bassi.

## 51. Rollback: Rimozione Filtri Rigidi (v6.5)
**Problema:** I filtri introdotti nella v6.1 (Mean Reversion, Toxic Flow, VWAP) erano troppo restrittivi e bloccavano l'operatività del bot, causando la perdita di occasioni di trading (scalping e trend). La precedente correzione (riduzione size) non era sufficiente a ripristinare il comportamento originale desiderato dall'utente.
**Correzione:**
* **`core/brain_la.py`:** Eliminati completamente i blocchi di codice relativi ai filtri "Mean Reversion (VWAP ALIGNMENT & PROBABILITY)", "TOXIC FLOW (VPIN)", "FEED INCOMPLETE" e "MACRO RISK-OFF".
* **Risultato:** Il bot è stato riportato allo stato precedente alla v6.1. Ora l'IA ha la totale libertà di decidere gli ingressi senza essere bloccata da filtri hardcoded, restituendo al bot la sua piena operatività.

## 52. Roadmap Quantitativa - Fase 1.2: Volatility-Adjusted Sizing (Formula Istituzionale) in `core/trade_manager.py`.
53. Roadmap Quantitativa - Fase 1.3: Implementazione recupero dati multi-timeframe (15m, 1h, 4h) con calcolo Hurst in `core/engine_la.py`.
**Problema:** I filtri introdotti nella v6.1 (Mean Reversion, Toxic Flow, VWAP) erano troppo restrittivi e bloccavano l'operatività del bot, causando la perdita di occasioni di trading (scalping e trend). La precedente correzione (riduzione size) non era sufficiente a ripristinare il comportamento originale desiderato dall'utente.
**Correzione:**
* **`core/brain_la.py`:** Eliminati completamente i blocchi di codice relativi ai filtri "Mean Reversion (VWAP ALIGNMENT & PROBABILITY)", "TOXIC FLOW (VPIN)", "FEED INCOMPLETE" e "MACRO RISK-OFF".
* **Risultato:** Il bot è stato riportato allo stato precedente alla v6.1. Ora l'IA ha la totale libertà di decidere gli ingressi senza essere bloccata da filtri hardcoded, restituendo al bot la sua piena operatività.

## 53. Roadmap Quantitativa - Fase 1.3: Implementazione recupero dati multi-timeframe (15m, 1h, 4h) con calcolo Hurst in `core/engine_la.py`.
**Problema:** L'IA prendeva decisioni basandosi solo sul timeframe corrente, senza avere una visione d'insieme del regime di mercato su timeframe superiori.
**Correzione:**
* **`core/engine_la.py`:** Aggiunta la funzione `get_market_data_multi_tf` per recuperare e analizzare i dati su 15m, 1h e 4h, calcolando l'esponente di Hurst per determinare il regime di mercato (trend vs mean-reverting) su più orizzonti temporali.

## 54. Fix Calcolo Margine e Integrazione Regime Classifier Multi-Timeframe (v6.4)
**Problema:** Il bot riportava "Capitale totale <= 0" e non riusciva a calcolare la size dinamica perché la funzione `get_balance_margin` non riusciva a estrarre correttamente il saldo USD/ZUSD dalla risposta grezza di Kraken. Inoltre, i dati multi-timeframe (Hurst) calcolati nella Fase 1.3 non venivano passati all'IA per l'analisi.
**Correzione:**
* **`core/performer_la.py`:** Modificata `get_available_margin` per accettare un parametro `asset` (default 'ZUSD'), rendendola flessibile per diverse valute.
* **`core/trade_manager.py`:** Aggiornata `get_balance_margin` per utilizzare `self.performer.get_available_margin()` come metodo primario per recuperare il margine libero. Aggiunto un fallback robusto per parsare la risposta grezza di `fetch_balance` se la chiave `free` è vuota. Ottimizzato il controllo del margine minimo (Sopravvivenza) riutilizzando il `capitale_totale` già fetchato.
* **`bot_la.py`:** Integrato il recupero dei dati multi-timeframe tramite `engine.get_market_data_multi_tf(asset)` e aggiunti al dizionario `dati_mercato_chimera`.
* **`core/brain_la.py`:** Aggiornata `full_global_strategy` per estrarre i dati JSON-serializzabili (es. esponente di Hurst) dai DataFrame multi-timeframe e includerli nel prompt di Gemini. Aggiunta una regola mandatoria nel prompt per istruire l'IA a valutare la confluenza multi-timeframe del regime di mercato.

## 55. Roadmap Quantitativa - Fase 1.4: Smart Execution (Limit vs Market) e Daily Killswitch (v6.5)
**Problema:** L'esecuzione esclusivamente a mercato (Market) causava slippage e commissioni elevate (Taker), riducendo l'efficienza del bot. Inoltre, mancava un sistema di protezione del capitale (Killswitch) in caso di perdite giornaliere eccessive.
**Correzione:**
* **`core/trade_manager.py`:** Implementata la logica di **Daily Killswitch**. Il bot ora traccia il PnL giornaliero in USD e blocca l'apertura di nuove posizioni se la perdita supera il 3% del capitale totale. Aggiunto ricalcolo automatico del PnL odierno all'avvio.
* **`core/performer_la.py`:** Implementata la **Smart Execution**. Il bot ora sceglie il tipo di ordine in base alla convinzione dell'IA:
    * **Voto >= 9:** Esecuzione **MARKET** per garantire l'ingresso immediato su segnali forti.
    * **Voto < 9:** Esecuzione **LIMIT** (Maker) al prezzo attuale per risparmiare commissioni e migliorare il prezzo medio.
* **`bot_la.py`:** Integrato il controllo preventivo del Killswitch nel loop di trading per proteggere il capitale prima di ogni analisi IA.

## 56. Fix Critici: MATIC Migration, Schema NightReview e Rilevamento Capitale (v6.6)
**Problema:** Tre errori bloccanti rilevati nei log: 1) Kraken ha migrato MATIC a POL, causando errori API. 2) La NightReview falliva se l'IA restituiva un voto decimale (es. 8.5). 3) Errori temporanei nel recupero del saldo bloccavano il calcolo della size.
**Correzione:**
* **`core/asset_list.py`:** Sostituito integralmente `MATICUSD` con `POLUSD` in tutti i mapping (Spot e Futures) per allinearsi alla migrazione di Kraken.
* **`core/brain_la.py`:** Aggiornato `NightReviewSchema` cambiando il tipo di `voto_performance` da `int` a `Union[int, float]` per supportare voti decimali.
* **`bot_la.py`:** Aggiunto un controllo di sicurezza nel loop principale: se il capitale rilevato è <= 0, il bot attende 10 secondi e riprova il ciclo invece di crashare, gestendo così eventuali glitch temporanei delle API.

## 57. Fix Ghost Trade Verification (v6.7)
**Problema:** Il bot continuava a riportare l'errore `kraken does not have market symbol MATICUSD` durante la verifica dei Ghost Trades, poiché i record salvati nel database prima della migrazione contenevano ancora il vecchio ticker.
**Correzione:**
* **`core/feedback_engine.py`:** Modificato il metodo `verifica_esiti_ghost` per utilizzare la funzione `get_ticker()` prima di ogni chiamata all'exchange. Questo garantisce che i vecchi record `MATICUSD` vengano mappati correttamente su `POLUSD` durante la fase di verifica dei prezzi, eliminando l'errore API.

## 58. Ottimizzazione Costi Gemini e Aggressività Strategia (v6.8)
**Problema:** I costi delle API Gemini erano elevati a causa di chiamate continue anche in mercati piatti. Inoltre, la strategia era troppo conservativa, perdendo alcune opportunità di ingresso.
**Correzione:**
* **`core/brain_la.py`:** Implementato `ThinkingLevel.LOW` per ridurre il consumo di token e la latenza. Aggiornato il prompt di sistema per incoraggiare l'IA a essere meno conservativa e massimizzare gli ingressi.
* **`core/config_la.py`:** Abbassata la `BRAIN_SOGGLIA` da 5.5 a 5.0 per facilitare l'apertura di nuove posizioni.
* **`bot_la.py`:** Aggiunto un filtro tecnico pre-IA: il bot ora salta la chiamata all'IA se la volatilità (ATR) è < 0.05% o se il volume è nullo, risparmiando budget API durante le fasi laterali estreme.

## 59. Sblocco Trend Following e Priorità Momentum (v6.9)
**Problema:** Il bot restava "spettatore" durante forti movimenti direzionali (candele 1h/4h) a causa di filtri troppo restrittivi su VPIN (Toxic Flow) e Z-Score, che interpretavano la forza del trend come un pericolo o un'anomalia statistica.
**Correzione:**
* **`core/brain_la.py`:** Implementato il bypass automatico dello Z-Score se viene rilevato un trend forte (Hurst > 0.6 o Velocity > 0.0008). Aggiornate le istruzioni di sistema per dare priorità assoluta al Trend Following.
* **`bot_la.py`:** Modificata la logica del VPIN: se il movimento è confermato dalla sentinella o dalla volatilità, il VPIN alto viene interpretato come forza del trend e non blocca più l'analisi.
* **`core/config_la.py`:** Ripristinata la `BRAIN_SOGGLIA` a 6.0 per mantenere la qualità, ma con filtri tecnici ora ottimizzati per non perdere le cavalcate di mercato.

## 60. Override Aggressivo Difese (v7.0)
**Problema:** Il bot era ancora troppo conservativo durante movimenti di mercato improvvisi (es. drop dell'1%), bloccato da filtri di sicurezza multipli (Killswitch, Cooldown, Night Review, Alpha Decay).
**Correzione:**
* Applicato il tag `# [OVERRIDE_AGGRESSIVO]` per identificare e bypassare temporaneamente i seguenti sistemi:
  * **Killswitch & Cooldown:** Bypassati in `bot_la.py`.
  * **Volatilità & Volume:** Soglie abbassate drasticamente (0.001%) per forzare l'analisi.
  * **Toxic Flow (VPIN):** Tolleranza alzata al 99% per favorire ingressi.
  * **Incoerenza Dati:** Bypassata la sospensione dell'asset.
  * **Z-Score:** Tolleranza raddoppiata da 2 a 4 in `core/brain_la.py`.
  * **Alpha Decay & Streak Loss:** Bypassati in `core/brain_la.py`.
  * **Night Review (Prior):** Rimosso il FLAT forzato, l'IA ora ignora i dict limitanti se in disaccordo.
* Aggiunto log esplicito `🔥 [OVERRIDE_AGGRESSIVO] Invio {asset} a Gemini...` per monitorare esattamente cosa viene inviato all'IA.
* `BRAIN_SOGGLIA` mantenuta a 6.0.

## 61. Potenziamento Esecuzione e Gestione Dinamica (v8.0)
**Problema:** Necessità di migliorare l'efficacia dei Take Profit e Stop Loss, proteggere i profitti in modo più intelligente e gestire le inversioni di mercato senza chiusure totali premature.
**Correzione:**
* **`core/brain_la.py` (SL/TP Dinamici):** La funzione `determina_tp_sl_ts` è stata completamente riscritta. Ora abbandona il TP fisso al 2% in favore di livelli dinamici calcolati in base alla distanza dai **muri di liquidità** (supporto/resistenza) e alla volatilità **ATR** (3.5x ATR). Lo Stop Loss viene anch'esso posizionato strategicamente dietro i muri di protezione.
* **`core/trade_manager.py` (Trailing Stop Intelligente):** Implementato il metodo `gestisci_trailing_stop_intelligente`. Sposta lo Stop Loss in tempo reale basandosi sulla forza del trend (**Hurst > 0.6**) e sulla volatilità. Se il trend è solido, lo stop viene stretto (1.5x ATR) per "mungere" il movimento; se il trend è incerto, viene lasciato più respiro (2.5x ATR).
* **`core/trade_manager.py` (Chiusure Parziali Strategiche):** Implementata la funzione `_gestisci_chiusura_parziale_strategica`. Se la posizione è in profitto (>1%) ma viene rilevata una **pressione eccessiva (>80%)** sul muro di liquidità opposto (resistenza per i Long, supporto per gli Short), il bot chiude automaticamente il **50% della size**. Questo permette di incassare profitto prima di un potenziale rimbalzo, lasciando correre il resto con lo SL a pareggio.
* **Integrazione Monitoraggio:** Le nuove logiche sono state integrate direttamente nel loop di `gestisci_protezione_istituzionale`, garantendo un controllo millimetrico ad ogni ciclo del bot.

## 62. Fix Volume Minimo e Incoerenza Dati (v8.1)
**Problema:** Due errori bloccanti: 1) `volume minimum not met` su AAVEUSD durante le chiusure parziali (size troppo piccola per Kraken). 2) `ABORT: Incoerenza Dati` su FETUSD (filtro Z-Score vs VWAP troppo rigido per asset volatili).
**Correzione:**
* **`core/trade_manager.py` (Fix Volume):** Aggiunto un controllo preventivo in `_chiudi_parzialmente`. Se la quantità da chiudere è inferiore al `min_size` dell'asset, il bot esegue una chiusura totale per sicurezza invece di generare un errore API.
* **`core/brain_la.py` (Fix Incoerenza):** Rilassato il filtro di integrità dati. La tolleranza dello Z-Score rispetto alla VWAP è stata alzata da 1.0 a **2.0**. Questo evita lo scarto di opportunità su asset ad alta volatilità (come FET) dove brevi divergenze statistiche sono normali.
* **`core/asset_list.py` (Ottimizzazione):** Verificati i parametri di `min_size` per AAVEUSD (0.01) e FETUSD (1.0) per garantire la coerenza con i nuovi controlli.

## 63. Ripristino Take Profit Dinamico e Fix Log (v8.2)
**Problema:** Il bot loggava ancora "FISSO 2%" per il Take Profit, suggerendo una logica hardcoded non desiderata.
**Correzione:**
* **`core/brain_la.py` (Logica TP):** Verificato che la logica sia 100% dinamica (Muri o 3.5x ATR). Rimosso il testo statico "(FISSO 2%)" dal log, sostituito dalla variabile `tp_type` per riflettere la reale origine del calcolo.
* **`core/brain_la.py` (Cleanup):** Rimosso un blocco `except` ridondante nella funzione `determina_tp_sl_ts`.
* **Riavvio Bot:** Eseguito il riavvio del processo per caricare le nuove logiche dinamiche ed eliminare i residui della vecchia versione.

## 64. Aggiornamento Parametri Rischio e Soglia Ingresso (v8.3)
**Problema:** Richiesta dell'utente di raddoppiare il rischio per trade e aumentare la selettività dell'IA.
**Correzione:**
* **`core/config_la.py` (Rischio):** Raddoppiato `RISK_PER_TRADE` da 0.015 (1.5%) a **0.03 (3%)**.
* **`core/config_la.py` (Killswitch):** Alzato `KILLSWITCH_GIORNALIERO` da 0.04 (4%) a **0.06 (6%)** per accomodare la maggiore size dei trade.
* **`core/config_la.py` (Soglia IA):** Alzato `BRAIN_SOGGLIA` da 6.0 a **7.0**. Il bot ora entrerà solo su segnali con un voto Gemini molto alto, riducendo i falsi positivi.
* **Riavvio Bot:** Riavviato il server per applicare immediatamente i nuovi parametri di gestione del capitale.

## 65. Incremento Rischio al 5% (v8.4)
**Problema:** Richiesta specifica dell'utente di alzare il rischio al 5% per trade.
**Correzione:**
* **`core/config_la.py` (Rischio):** Alzato `RISK_PER_TRADE` al **0.05 (5%)**.
* **`core/config_la.py` (Killswitch):** Alzato `KILLSWITCH_GIORNALIERO` al **0.10 (10%)** per permettere una gestione coerente con il nuovo rischio.
* **Riavvio Bot:** Riavviato il server per rendere operative le nuove impostazioni.

## 66. Separazione Cervello Strategico e Matematico (v8.5)
**Problema:** L'IA (Gemini) veniva incaricata di calcolare parametri matematici precisi (Stop Loss, Take Profit, Sizing, Leverage), sprecando token e generando valori spesso imprecisi o incoerenti con le regole di risk management, portando a performance subottimali.
**Correzione:**
* **`core/brain_la.py` (Prompt Engineering):** Modificato radicalmente il prompt inviato a Gemini. L'IA ora agisce esclusivamente come "Cervello Strategico", valutando solo la direzione (LONG/SHORT/FLAT), la convinzione (voto) e il razionale. Rimossi dal prompt e dallo schema JSON i campi `sl`, `tp`, `sizing` e `leverage`.
* **`core/brain_la.py` (Traduzione Numeri-Linguistica):** Potenziata la funzione `_get_technical_narrative` per tradurre i dati numerici grezzi (Hurst, VPIN, Z-Score) in concetti linguistici descrittivi (es. "Forte Trend Direzionale", "Estremamente Tossico"). Questo aiuta l'IA a comprendere meglio il contesto di mercato.
* **`core/brain_la.py` & `core/trade_manager.py` (Cervello Matematico):** Il calcolo di Stop Loss, Take Profit, Sizing e Leva è stato delegato interamente a Python. `determina_tp_sl_ts` ora calcola SL e TP matematicamente basandosi su ATR e muri di liquidità. Il Sizing viene calcolato in base al rischio massimo consentito e alla distanza dello SL, garantendo un risk management perfetto e indipendente dall'IA.

## 69. Spiegazione Esaustiva di Tutti gli Strumenti all'IA (v8.8)
**Problema:** L'utente ha richiesto che l'IA non ricevesse solo i pattern pre-calcolati, ma una spiegazione completa e dettagliata di *tutti* gli strumenti e le metriche calcolate dal motore di mercato, per avere un quadro clinico totale.
**Correzione:**
* **`core/brain_la.py` (Dashboard Completa):** Aggiunta una seconda sezione alla funzione `_get_technical_narrative` chiamata "PANORAMICA COMPLETA DI TUTTI GLI STRUMENTI".
* **Metriche Spiegate:** Ora il prompt include una lista esplicita con il valore e la spiegazione del significato per ogni singola metrica:
  * **Azione del Prezzo:** Prezzo, Velocità, ATR, BB Width, Squeeze Status.
  * **Cinematica:** Regime, Hurst, Kaufman, Trend Strength.
  * **Microstruttura:** Book Pressure, Distanza Muri Supporto/Resistenza.
  * **Order Flow:** OFI, VPIN, CVD USD, Divergenza CVD, Aggressività.
  * **Volumi:** VWAP, Z-Score VWAP, POC.
  * **Derivati & Macro:** Funding Rate, Open Interest Trend, Liquidazioni, Correlazione BTC, Health Index.
* **Risultato:** Gemini ora ha letteralmente un "cruscotto quantitativo" davanti agli occhi. Sa esattamente quali strumenti esistono, quanto valgono in quel momento e cosa significano, permettendogli di ragionare in modo olistico su tutto il dataset.

## 70. Centralizzazione Cross-Asset e Intermarket (v8.9)
**Problema:** Alcuni cross-asset utilizzati per l'analisi intermarket (es. `ETH/BTC`, `BTC/USDT`) erano hardcodati direttamente nel codice di `core/engine_la.py`, `core/brain_la.py` e `core/asset_rotation.py`, violando la direttiva di centralizzazione dei ticker in `core/asset_list.py`.
**Correzione:**
* **`core/asset_list.py`:** Introdotte costanti globali (`CROSS_ETH_BTC`, `CROSS_BTC_USDT`, ecc.) e mappatura `CROSS_PAIRS` per gestire centralmente i ticker dei cross-asset.
* **`core/asset_list.py`:** Aggiunta la funzione helper `get_cross_ticker(cross_name)` per recuperare i ticker corretti in modo dinamico.
* **`core/engine_la.py`:** Aggiornati i metodi `_get_intermarket_data` e `_get_macro_correlation` per utilizzare le costanti di `asset_list.py` invece delle stringhe hardcodate.
* **`core/brain_la.py`:** Aggiornata la generazione del prompt per Gemini per recuperare il nome umano del cross direttamente da `asset_list.CROSS_PAIRS` usando le costanti centralizzate.
* **`core/asset_rotation.py`:** Sostituita la lista di asset hardcodata nel report serale con il riferimento dinamico a `asset_list.ASSET_PRINCIPALI`.

## 71. Risoluzione Ghost Trades e Falsi PnL (v9.0)
**Problema:** Il bot registrava chiusure di posizioni con profitti/perdite fittizi (es. POLUSD) anche quando l'ordine reale non era mai stato eseguito o la posizione era già chiusa. Questo accadeva perché il bot assumeva erroneamente che un errore "Insufficient funds" in fase di chiusura significasse "posizione già chiusa dall'exchange", procedendo a calcolare un PnL statistico basato sul prezzo attuale.
**Correzione:**
* **`core/trade_manager.py` (`_chiudi_statisticamente`):** Aggiunta verifica dell'ordine di entrata (`entry_id`) tramite `fetch_my_trades` prima di procedere alla chiusura statistica. Se l'ordine non ha mai generato trade reali, la posizione viene rimossa come "FANTASMA" senza registrare PnL.
* **`core/trade_manager.py` (`_esegui_chiusura_totale`):** Introdotta verifica preventiva dell'esistenza della posizione su Kraken tramite `get_open_positions_real`. Se la posizione non esiste, il bot verifica se è un fantasma (mai aperta) o se è stata chiusa esternamente prima di sincronizzare il diario.
* **`core/trade_manager.py` (`apri_posizione`):** Migliorata la conferma post-apertura. Se dopo l'invio dell'ordine la posizione non appare su Kraken, il bot ora verifica se l'ordine è ancora aperto (LIMIT) o se è svanito (rifiutato/cancellato), evitando di aggiungere posizioni non confermate al diario.
* **Risultato:** Eliminazione dei profitti "fantasma" che falsavano le statistiche giornaliere e totali, garantendo che il diario rifletta solo operazioni realmente avvenute sull'exchange.

## 72. Fix "Invalid leverage" e Ottimizzazione Sizing (v9.1)
**Problema:** Gli ordini venivano rifiutati da Kraken con l'errore `EOrder:Invalid leverage` (es. su FETUSD). Questo accadeva perché il bot tentava di usare leve non supportate per lo specifico asset o lato dell'operazione (BUY/SELL), oppure perché la leva calcolata non rientrava tra quelle esatte permesse dall'exchange (es. 4x invece di 2, 3, 5).
**Correzione:**
* **`core/engine_la.py`:** Potenziata `get_asset_leverage_info` per supportare il parametro `side`. Ora recupera e filtra le leve consentite specificamente per il lato dell'operazione (buy/sell) direttamente dai mercati di Kraken.
* **`core/trade_manager.py`:** 
  - Implementato un incrocio rigoroso tra la leva richiesta, il limite utente e il limite reale di Kraken (`abs_max_lev`).
  - Aggiunta la logica di arrotondamento alla leva permessa più vicina: se la leva calcolata non è nella lista `allowed_leverages`, il bot seleziona automaticamente il valore valido immediatamente inferiore, evitando il rifiuto dell'ordine.
  - Ripristinati i controlli di `min_size` e `leva_minima_necessaria` per garantire la stabilità del calcolo della size in ogni condizione di margine.
* **`core/performer_la.py`:** Ottimizzata la costruzione dei parametri dell'ordine per assicurare che la leva sia passata come stringa intera, come richiesto dall'API di Kraken.

## 73. Eliminazione Definitiva Ghost Trades e Chiusure "Fantasma" (v9.2)
**Problema:** Il bot registrava chiusure vincenti fittizie (es. POLUSD) quando riceveva un errore `Insufficient funds` da Kraken durante un tentativo di chiusura. Il bot assumeva erroneamente che l'errore significasse "posizione già chiusa", mentre spesso era dovuto a disallineamenti di ticker (es. POLUSD vs XPOLZUSD) o errori di sizing, lasciando la posizione APERTA su Kraken ma chiudendola nel diario.
**Correzione:**
* **`core/performer_la.py` (`get_open_positions_real`):** Implementata la doppia mappatura dei ticker (originale e normalizzato). Ora le posizioni sono rilevabili sia come `XPOLZUSD` che come `POLUSD`, eliminando i falsi negativi durante la verifica dell'esistenza.
* **`core/trade_manager.py` (`_esegui_chiusura_totale` e `_esegui_chiusura_parziale`):** 
  - Introdotto matching robusto tramite normalizzazione prima di ogni tentativo di chiusura.
  - **Rimosso l'automatismo su `Insufficient funds`**: se il bot rileva che la posizione esiste ancora su Kraken (tramite `p_real`), l'errore `Insufficient funds` viene trattato come un fallimento dell'ordine e NON come una chiusura avvenuta. La posizione rimane nel diario per essere gestita correttamente nei cicli successivi, evitando PnL fittizi.
* **Risultato:** Sincronizzazione perfetta tra diario interno e Kraken. Le chiusure vengono registrate solo se confermate dall'exchange o se la posizione è realmente sparita dopo verifica incrociata.

## 74. Sincronizzazione Dinamica Regole Kraken (Precisione, Size, Leva) (v9.3)
**Problema:** Il bot utilizzava valori hardcodati nel file `asset_list.py` per determinare la precisione dei prezzi, dei volumi e la size minima degli ordini. Questo causava errori di esecuzione (es. "Invalid price precision", "Volume minimum not met") se le regole di Kraken cambiavano o se i valori manuali erano imprecisi, innescando cicli di retry fallimentari che tentavano di "indovinare" i parametri corretti abbassandoli progressivamente.
**Correzione:**
* **`core/trade_manager.py` & `core/performer_la.py`:** Rimossi tutti i cicli `while` di retry basati su tentativi ed errori per la formattazione di prezzi e volumi.
* **Precisione Dinamica:** Implementato l'uso obbligatorio di `exchange.load_markets()` seguito da `exchange.price_to_precision()` e `exchange.amount_to_precision()` forniti da CCXT. Ora il bot formatta i numeri esattamente secondo le regole attuali dell'endpoint `AssetPairs` di Kraken per ogni specifico asset.
* **Size Minima Dinamica:** In `apri_posizione` e `_chiudi_parzialmente`, il bot ora interroga direttamente i limiti del mercato (`market['limits']['amount']['min']`) per determinare la size minima reale, usando i valori di `asset_list.py` solo come fallback di emergenza.
* **Risultato:** Il bot è ora 100% allineato in tempo reale con le regole di routing e formattazione di Kraken, eliminando gli errori di invio ordini dovuti a disallineamenti di configurazione statica.

## 75. Fix "Insufficient funds" su Chiusure Parziali e Gestione SPOT (v9.4)
**Problema:** Durante la chiusura parziale o totale di posizioni a margine, Kraken restituiva l'errore `EOrder:Insufficient funds` nonostante l'uso del parametro `reduce_only=True`. Questo accadeva perché il bot memorizzava o calcolava una leva diversa da quella reale della posizione su Kraken (es. memorizzava 2x ma la posizione era 3x). Kraken, non trovando la posizione con la leva specificata, tentava di aprire una nuova posizione opposta, fallendo per mancanza di margine libero. Inoltre, le posizioni SPOT (leva 1x) venivano erroneamente classificate come "fantasma" e rimosse dal diario perché non apparivano nell'endpoint `OpenPositions` di Kraken (che restituisce solo posizioni a margine).
**Correzione:**
* **`core/trade_manager.py` (`_chiudi_parzialmente` e `_esegui_chiusura_totale`):**
  - **Estrazione Leva Reale:** Il bot ora estrae dinamicamente la leva reale richiamando l'ordine primario (`entry_id`) su Kraken prima di inviare l'ordine di chiusura. Questo garantisce che il parametro `leverage` inviato a Kraken corrisponda esattamente alla posizione esistente, permettendo al flag `reduce_only` di funzionare correttamente senza richiedere margine aggiuntivo ed evitando parsing testuali (regex).
  - **Gestione Posizioni SPOT:** Aggiunta una logica specifica per le posizioni con leva 1x (SPOT). Se la posizione non viene trovata tra quelle a margine, il bot ora verifica il saldo reale (`fetch_balance`) dell'asset base (es. BTC per XXBTZUSD). Se il saldo è sufficiente, la posizione viene riconosciuta e chiusa correttamente, evitando la rimozione errata dal diario.
  - **Fix Parsing Leva:** Corretto un potenziale `TypeError` durante il parsing della leva dal diario quando il valore salvato era `None` (tipico delle operazioni MULTIDAY/SPOT).
  - **Uso Simbolo Unificato:** In `_chiudi_parzialmente`, l'ordine di chiusura ora utilizza il simbolo unificato di CCXT (es. `BTC/USD`) invece del ticker di Kraken (`XXBTZUSD`), assicurando che CCXT applichi correttamente tutte le regole di formattazione e i parametri specifici dell'exchange.
* **Risultato:** Eliminazione dell'errore `Insufficient funds` durante le chiusure, garantendo che le posizioni a margine vengano ridotte correttamente e che le posizioni SPOT vengano tracciate e chiuse senza essere scambiate per posizioni fantasma.

## 76. Analisi Performance DB + Correzioni Strutturali (v9.5) — 2026-03-22

### Analisi dati reali
Analisi completa di 189 trade reali dal DB `chimera.db`. Risultati principali:
- **110 trade "senza regime"** = trascritti da Kraken all'avvio, non aperti dal bot. WR 74% dovuto a posizioni già in profitto ereditate durante trend BTC/POL favorevole — non performance del bot.
- **79 trade autonomi del bot**: WR 45.6%, PF 1.10, PnL netto fees ~-$8. Identico alla versione vecchia (PF 1.10).
- **CVD al momento dell'entry**: differenza 1653% tra WIN e LOSS — il segnale predittivo più forte nel DB.
- **SHORT**: WR 33%, CVD al momento dello short LOSS positivo nel 51% dei casi — il bot shortava contro il flusso reale.
- **Voto 9**: Z-VWAP medio 2.547 (segnale già corso), WR 29% peggiore del voto 7 (WR 44%).

### Correzioni applicate

**`core/engine_la.py` — Step 1 (fix metodi esistenti):**
- `_calcola_funding_zscore`: bug `except` ritornava il valore grezzo; warm-up alzato a 20
- `_get_put_call_ratio`: era placeholder fisso 1.0; ora legge openInterestLong/Short da Kraken Futures
- `get_market_data_multi_tf`: timeframe map sbagliato (`'15'` invece di `'15m'`); ora restituisce struttura completa con hurst, kaufman, ATR, EMA, trend_dir, vol_relativo
- `_calcola_volume_profile`: esteso a 7 giorni (168 candele 1h) invece di 25h; aggiunto VWAP ancorato
- `_check_portfolio_correlation`: matrice da 3 a 28 coppie; default da 0.5 a 0.65

**`core/engine_la.py` — Step 2 (livelli strutturali):**
- `_calcola_swing_levels`: swing high/low su H1 (100 candele) e H4 (60 candele), finestra 3 candele
- `_calcola_pivot_points`: pivot classici daily (R1/R2/R3/S1/S2/S3) e weekly
- `_calcola_ema_struttura`: EMA 20/50/200 su 15m, 1h, 4h con `ema_confluence_score` -3/+3

**`core/engine_la.py` — Step 3 (metodologia istituzionale):**
- `_calcola_fvg_storici`: tutti i FVG non colmati su H1/H4, filtra gap < 0.1%, max 5 per TF
- `_calcola_order_blocks`: OB bullish/bearish su H1 (soglia 0.6%) e H4 (soglia 1.2%)
- `_calcola_breaker_blocks`: OB violati che invertono ruolo, riusa OB già calcolati (0 API extra)

**`core/engine_la.py` — Step 4 (BOS/CHoCH + sweep history):**
- `_calcola_bos_choch`: struttura H1 (UPTREND/DOWNTREND/LATERALE), BOS e CHoCH con livelli
- `_aggiorna_sweep_history`: traccia test e sweep dei muri correnti, calcola `supporto_score` 0-1

**`core/brain_la.py`:** tutti i nuovi dati integrati nel JSON Gemini e nella narrativa tecnica (11 punti invece di 7)

**`core/bot_la.py` — filtri pre-Gemini (Step 2 bot):**
- Pre-score 6 condizioni: se < 3 non chiama Gemini
- Cooldown macro: BEARISH + Z < -1.0 + VPIN > 0.65 → cooldown 2h
- Confluenza breakdown: se 2+ categorie sotto 4/10 → FLAT forzato
- Filtro ore bassa liquidità 01-06 UTC (con bypass sentinella)
- Streak loss adattivo: streak 5+ → soglia +1 + sizing -50%

**Fix `datetime.utcnow()` deprecato** in `bot_la.py` → `datetime.now(timezone.utc)`

## 77. Sentinella riscritta e fix filtro bassa liquidità (v9.6) — 2026-03-22

**`core/engine_la.py` — `check_sentinel`:**
- Buffer rolling 30s con timestamp reali (non confronto tra chiamate consecutive)
- Richiede almeno 2 condizioni su 3: Δprezzo ≥ 0.25%, volume spike ≥ 1.5x, CVD delta > 50k in 30s
- Soglie adattive per ora UTC (alta liquidità 07-20 UTC più strette)
- Cooldown 45s tra trigger sullo stesso asset (evita raffica di analisi)

**`core/bot_la.py` — filtro bassa liquidità:**
- Bug critico: riga `salta_entry = is_aperta and not forza_questo_ciclo` sovrascriveva il filtro bassa liquidità rendendolo completamente inutile
- Fix: logica unificata in una singola espressione che combina tutti i motivi di blocco
- Sentinella bypassa il blocco orario se rileva movimento reale

## 78. SignalStateEngine — traiettoria temporale indicatori (v9.7) — 2026-03-22

**Problema radice identificato dai dati:**
Brain riceveva snapshot statici senza contesto temporale. Non sapeva se CVD stava salendo o scendendo. Il voto 9 aveva Z-VWAP medio 2.547 perché Gemini vedeva un movimento forte e lo interpretava come "alta convinzione" — non sapendo che era già finito.

**`core/signal_state_engine.py` — nuovo modulo:**
- Buffer rolling 60 campioni (~120s) per CVD, VPIN, velocity, book_pressure, z_score_dist_vwap
- `calcola_derivate()`: delta 30s e 120s con timestamp reali, accelerazione CVD
- `_classifica_fase()`: FORMAZIONE / BREAKOUT / ESTENSIONE / ESAURIMENTO
- `_exhaustion_score()`: score 0-100 basato su Z-VWAP, CVD acceleration, velocity delta
- `_valida_short()`: 4 condizioni strutturali (CVD in calo, BP < 0.48, Hurst < 0.60, Z-VWAP > 0.5)
- `_narrativa()`: testo leggibile con regole vincolanti per Gemini ("⛔ REGOLA VINCOLANTE: exhaustion > 70, voto MAX = 6")

**`core/engine_la.py`:** chiama `signal_state.aggiorna()` alla fine di ogni `get_market_data()`, aggiunge tutti i campi derivati al dizionario risultante

**`core/brain_la.py`:**
- Nuovo blocco pre-Gemini: se `entry_phase == ESAURIMENTO` → FLAT senza chiamare Gemini
- Veto SHORT post-Gemini: se Gemini dice SELL ma `short_conditions_met == False` → FLAT
- Cap voto post-Gemini: `exhaustion > 70` → voto massimo 6; `exhaustion > 45` → voto massimo 8
- Narrativa `signal_narrative` aggiunta all'inizio del prompt come primo blocco

**`core/bot_la.py`:** import e inizializzazione `SignalStateEngine`, collegamento a `engine.signal_state`

## 75. Sessione Fix Strutturale — Z-Score, Veti, Sentinella, Persistenza (2026-03-23)

### Problemi rilevati
- Z-Score = 0.00 su tutti gli asset dalle 01:46 UTC: buffer WS con soglia 10 candele invece di 100
- "Dissonanza Flusso Estrema" bloccava BTC su pattern di accumulo istituzionale (ABORT errato)
- Veti SHORT bloccavano short in downtrend richiedendo Z > 0 (opposto del necessario)
- Sentinella richiedeva 2/3 condizioni simultanee — candele grandi su 15m non venivano rilevate
- Dati sentinella non venivano passati a Brain/Gemini
- Ad ogni restart: SignalStateEngine, funding_history, sentinel_buffer ripartivano da zero
- Numerosi return FLAT prima di Gemini (ABORT_AUDIT_FAIL, NO_CONFLUENCE, ABORT_VELOCITY, ecc.)
- Voto globale Gemini indipendente dalle categorie (poteva dare categoria 2/10 e voto 7)
- Exhaustion era hard cap (voto massimo 6) — bloccava implicitamente con soglia a 7
- Penalità si sommavano fino a -11 senza cap
- Nessun boost per segnali forti confermanti
- `_apply_prior` aveva ancora un FLAT quando diff >= 4
- `rolling_volatility` = None (mai calcolato)
- `whale_delta` = 0 (non veniva calcolato in get_detailed_order_flow)
- `HVN/LVN` = [] (volume profile non estraeva i nodi)
- `funding_z_score` = 0 ad ogni restart (buffer resettato)
- `put_call_ratio` = 1.0 (openInterestLong/Short non disponibile in Kraken standard)
- `liquidazioni_24h` = 0 (endpoint rotto)

### Correzioni applicate

**`core/engine_la.py`:**
- Buffer WS OHLCV: soglia alzata da 10 a 100 candele; se parziale usa REST
- `whale_delta`: calcolato in `get_detailed_order_flow` (trade sopra 95° percentile)
- `rolling_volatility`: aggiunto (std rendimenti su 20 candele × 100)
- `HVN/LVN`: estratti da `_calcola_volume_profile` (bins > 75° e < 25° percentile)
- `funding_z_score`: precaricat storico da API Kraken Futures al primo ciclo
- `put_call_ratio`: 3 fonti in cascata (ticker, endpoint /openinterest, basis mark/last)
- `liquidazioni_24h`: 3 URL in cascata + stima da OI change
- `check_sentinel` riscritta: 1 sola condizione basta (era 2/3), finestre 10s/30s/60s,
  cooldown 30s (era 45s), salva `last_sentinel_data` con dettagli movimento
- `get_sentinel_data()`: nuovo metodo per recuperare dati trigger
- Persistenza funding_history su `chimera_funding_state.json`
- `_carica_funding_history()` / `_salva_funding_history()` aggiunti

**`core/signal_state_engine.py`:**
- `_valida_short` rimosso — nessun veto, Gemini decide come per i long
- Persistenza buffer su `chimera_signal_state.json` ogni 30s
- `_carica_stato()` / `_salva_stato()`: ricarica campioni freschi (< 5 min) al restart

**`core/brain_la.py`:**
- "Dissonanza Flusso Estrema" declassata da ABORT a log informativo
- Tutti i return FLAT convertiti in penalità voto:
  - ABORT_AUDIT_FAIL → -3
  - ABORT_DATA_INCONSISTENCY → -2
  - ESAURIMENTO → -4
  - NO_CONFLUENCE_* → -2
  - ABORT_VELOCITY_* → -2
  - ABORT_CRITICAL_AUDIT → -2
- Cap penalità totale a -4 (era illimitato, poteva sommare -11)
- Exhaustion: da hard cap (voto max 6) a penalità (-2 se >70, -1 se >45)
- `_apply_prior` FLAT (diff>=4) → penalità -3 invece di FLAT
- Voto globale ancorato: `voto_finale = round(0.6 * voto_gemini + 0.4 * media_categorie)`
- Boost +1 ciascuno (cap +2) per: whale_delta allineato, EMA confluence >=3, CVD massiccio
- FLAT rimosso da `_policy_adjust` quando voto <= 0
- Dati sentinella in narrativa: `⚡ MOVIMENTO IMPROVVISO RILEVATO` come prima riga

**`core/bot_la.py`:**
- Sentinel data passato a `dati_mercato_chimera` e a Brain
- Pre-score < 3: non blocca più, passa a Gemini
- Cooldown macro 2h: rimosso, passa a Gemini
- Breakdown confluenza: da FLAT a penalità -1 per categoria debole
- Engine Health Check automatico: all'avvio + ogni 4h, alert Telegram se score < 80%
- `_controlla_salute_engine()`: controlla 15 indicatori critici, distingue critici/minori

**`diagnostica_engine.py` (nuovo file):**
- Script standalone per diagnostica completa 60+ indicatori Engine
- Mostra ✅ REALE / ⚠️ DEFAULT / ❌ NONE per ogni indicatore
- Engine Score percentuale finale

---

## Sessione 23 Marzo 2026 — Pulizia Veti, Short, Sentinella, Persistenza

### Fix Z-Score rotto (engine_la.py)
**Problema:** Z-Score = 0.00 dalle 01:46 in poi. Il buffer WS OHLCV si attivava con soli 10 candles mentre `_calcola_zscore` richiede window=100. Con meno di 100 candele restituiva 0.0 silenziosamente.
**Correzione:**
- Soglia WS alzata a 100 candles prima di usare il buffer
- Se buffer WS parziale (10-99 candles): fallback a fetch REST per garantire sempre 100 candele
- Z-Score ora restituisce valori reali: -1.06, -1.81, -1.72 ecc.

### Fix Dissonanza Flusso Estrema (brain_la.py)
**Problema:** `_audit_dati_tecnici` classificava BUYERS+OFI negativo come anomalia critica e mandava ABORT. Questa è la firma dell'accumulo istituzionale — bloccare questo pattern significava bloccare i migliori ingressi.
**Correzione:** Rimosso dall'elenco anomalie. Ora loggato come INFO con nota "pattern normale in accumulo, NON bloccante."

### Fix whale_delta sempre 0 (engine_la.py)
**Problema:** `get_detailed_order_flow` non restituiva mai `whale_delta` — chiave assente dal dict di ritorno.
**Correzione:** Aggiunto calcolo: isola trade sopra 95° percentile per dimensione USD, calcola net delta buy/sell whale, normalizza -1/+1.

### Fix rolling_volatility None (engine_la.py)
**Problema:** Indicatore non esisteva nel codice.
**Correzione:** Aggiunto calcolo: deviazione standard rendimenti su 20 candele × 100. Nella narrativa Brain: label 🔴 ALTA / 🟡 MEDIA / 🟢 BASSA.

### Fix HVN/LVN liste sempre vuote (engine_la.py)
**Problema:** `_calcola_volume_profile` calcolava POC/VAH/VAL ma non estraeva HVN e LVN. Le liste ritornavano sempre vuote.
**Correzione:** Aggiunti nel return: HVN = bin con volume ≥ 75° percentile, LVN = bin ≤ 25° percentile. Max 10 nodi ciascuno.

### Fix funding_z_score = 0 dopo restart (engine_la.py)
**Problema:** Il buffer funding si svuotava ad ogni restart. Servivano 20 cicli (~5 ore) per avere lo Z-Score.
**Correzione:** `_calcola_funding_zscore` ora precaricha gli ultimi 50 funding rates storici via endpoint Kraken Futures se il buffer è sotto soglia warmup.

### Fix put_call_ratio sempre 1.0 (engine_la.py)
**Problema:** Cercava `openInterestLong/Short` che Kraken non espone sempre.
**Correzione:** 3 fonti in cascata: ticker futures, endpoint `/openinterest`, basis mark vs last come proxy.

### Fix liquidazioni_24h sempre 0 (engine_la.py)
**Problema:** Endpoint `recent-liquidations` rotto/cambiato.
**Correzione:** 3 URL in cascata + stima da OI change negativo × prezzo × 0.3.

### Fix funding_rate -0.0 mostrato come DEFAULT (diagnostica_engine.py + bot_la.py)
**Problema:** Valore reale -0.0000x arrotondava a 0 con tolleranza 1e-6.
**Correzione:** Tolleranza 1e-9 per campi funding in `check()` della diagnostica e in `_controlla_salute_engine`.

### Veti SHORT rimossi completamente
**Problema:** `_valida_short` in signal_state_engine e veto post-Gemini in brain_la bloccavano tutti gli short. Con Z-VWAP sempre negativo in downtrend, il veto si attivava sempre.
**Correzione:**
- `signal_state_engine.py`: `_valida_short` restituisce sempre `(True, 'SHORT abilitato — decisione a Gemini')`
- `brain_la.py`: rimosso blocco post-Gemini `if direzione in ('SELL','SHORT') and not short_struct_ok`
- Prompt Gemini: rimossa regola D ("short_strutturale_ok=FALSE → nessun SELL"), aggiunta regola F ("BUY e SELL sono entrambi validi")

### Sentinella riscritta (engine_la.py)
**Problema:** Richiedeva 2 condizioni su 3 simultaneamente. Una candela grande su 15 minuti poteva non triggerare se il volume 24h non era abbastanza alto.
**Correzione:**
- Triggera su UNA SOLA condizione forte: Δ10s ≥ 0.15%, Δ30s ≥ 0.25%, Δ60s ≥ 0.40%, CVD accel + Δ30s ≥ 0.10%, vol spike + Δ30s ≥ 0.10%
- Finestre multiple: 10s, 30s, 60s (era solo 30s)
- Cooldown 30s (era 45s)
- Salva `last_sentinel_data` con: motivo, direzione UP/DOWN, chg10s/30s/60s, cvd_30s, n_trades
- Nuovo metodo `get_sentinel_data(ticker)` per recuperare i dati

### Dati sentinella passati a Brain (bot_la.py + brain_la.py)
**Problema:** Quando la sentinella scattava, Brain non sapeva perché né in che direzione si era mosso il mercato.
**Correzione:**
- `bot_la.py`: `sentinel_data` aggiunto a `dati_mercato_chimera` dopo ogni trigger
- `brain_la.py`: narrativa include `⚡ MOVIMENTO IMPROVVISO RILEVATO 📈/📉: motivo | CVD30s=val`

### Persistenza stato tra restart (engine_la.py + signal_state_engine.py)
**Problema:** Ad ogni restart: SignalStateEngine azzerava tutti i buffer CVD/VPIN/exhaustion, funding_history richiedeva 20 cicli per ripartire.
**Correzione:**
- `chimera_signal_state.json`: buffer SignalStateEngine salvato ogni 30s. Al restart carica campioni < 5 minuti.
- `chimera_funding_state.json`: funding history salvata ad ogni aggiornamento.
- Log al riavvio: "⚙️ SignalStateEngine: ricaricati buffer per N asset dal disco."

### Tutti i veti convertiti in penalità voto (brain_la.py + bot_la.py)
**Problema:** Numerosi blocchi nel codice restituivano FLAT prima ancora che Gemini potesse analizzare. Il bot non vedeva opportunità legittime.
**Correzione — ogni blocco → penalità voto:**
- `ABORT_AUDIT_FAIL` → -3
- `ABORT_DATA_INCONSISTENCY` → -2
- `entry_phase == ESAURIMENTO` → -4
- `NO_CONFLUENCE_BULL/BEAR_TOXIC` → -2
- `ABORT_VELOCITY_CRASH/SQUEEZE` → -2
- `ABORT_CRITICAL_AUDIT` → -2 (era FLAT forzato)
- Pre-score < 3 in bot_la → passa a Gemini (non blocca)
- Cooldown macro 2h → rimosso, passa a Gemini
- Breakdown 2+ categorie deboli → -1 per categoria (era FLAT)
- Penalità totale cappata a -4 per evitare cumuli assurdi
- Exhaustion: da hard cap a penalità graduata (-1 se >45, -2 se >70), solo se fase != ESAURIMENTO

### Logica voti sistemata (brain_la.py)
**Problema:** Exhaustion veniva penalizzato due volte (una in `_penalita_esaurimento` e una nel blocco post).
**Correzione:** Exhaustion post-Gemini ora attivo SOLO se `entry_phase != 'ESAURIMENTO'` — nessuna doppia penalità.

### Engine Health Check automatico (bot_la.py)
**Aggiunta:** `_controlla_salute_engine()` gira all'avvio e ogni 4 ore. Controlla 15 indicatori critici. Alert Telegram solo se ci sono problemi. Silenzioso se tutto OK. Log: "🟢 Engine Health OK — 93% indicatori reali (14/15)".

### Bug critico: chiusura Virtual SL sempre fallita (trade_manager.py)
**Problema:** `❌ Errore chiusura totale SOLUSD: unsupported operand type(s) for +=: 'float' and 'str'` — ogni chiusura da virtual SL crashava. Kraken restituisce fee come stringhe (`"0.00234"` non `0.00234`). Il bot non chiudeva la posizione, che poi veniva recuperata dalla sincronizzazione con size e leva sbagliata.
**Correzione:** `float(... or 0)` su tutti i punti dove si legge `fee` e `pnl_netto` da `real_data` in `_esegui_chiusura_totale` e `_chiudi_statisticamente`.

### Bug: voto_ia non aggiornato dopo penalità confluenza (bot_la.py)
**Problema:** `voto_ia` veniva letto dal decision prima che la penalità confluenza lo modificasse. Il threshold check usava il voto non penalizzato.
**Correzione:** Aggiunto `voto_ia = decision.get('voto', 0)` dopo il blocco penalità confluenza.

### Bug: Errore macro TradFi NoneType (macro_sentiment.py)
**Problema:** yfinance versioni recenti restituisce DataFrame con colonne multi-livello. Accedere a `['Close']` direttamente crashava con `NoneType is not subscriptable`.
**Correzione:** Appiattimento colonne prima dell'accesso con `dxy.columns = [c[0] if isinstance(c, tuple) else c for c in dxy.columns]`.

### Gemini 429 spam (brain_la.py)
**Problema:** Quando la quota si esauriva, il bot continuava a chiamare Gemini ogni ciclo sprecando tempo.
**Correzione:** Aggiunto `_gemini_quota_until` in `__init__`. Dopo ogni 429 il cooldown è 60 secondi — le chiamate durante il cooldown vengono saltate silenziosamente.

### Fix chiavi muro SL/TP sempre zero (brain_la.py)
**Problema:** `determina_tp_sl_ts` leggeva `muro_resistenza_prezzo` e `muro_supporto_prezzo` che non esistono. Engine produce `muro_resistenza` e `muro_supporto`. Risultato: i muri di liquidità non venivano MAI usati per SL/TP da sempre.
**Correzione:** Chiavi corrette. Log aggiunto quando il muro viene trovato e usato.

### Fix cap 2% SL distruggeva R:R (brain_la.py)
**Problema:** `sl_f = min(sl_f, prezzo_ingresso * 0.98)` forzava SL ad almeno 2% per LONG e `max(sl_f, entry * 1.02)` per SHORT. Con ATR piccolo (XRP, DOGE) SL > TP → R:R < 1.
**Correzione:** Cap rimosso. SL posizionato dove indicano muri o ATR×moltiplicatore. Cap massimo 5% (era minimo 2%).

### Fix R:R enforcement 1.5:1 (brain_la.py)
**Aggiunto:** Se TP < 1.5×SL il TP viene espanso automaticamente al minimo 1.5×SL per tutti gli stili.

### Fix stile_operativo non calibrava SL/TP (brain_la.py)
**Problema:** `determina_tp_sl_ts` usava sempre moltiplicatori SWING (2.2x SL, 3.5x TP) indipendentemente dallo stile. Uno SCALPING su BTC riceveva SL 5% e TP 7.5%.
**Correzione:** Moltiplicatori per stile:
- SCALPING: SL 1.0×ATR, TP 1.5×ATR, cap SL 1.5%
- MOMENTUM: SL 1.5×ATR, TP 2.5×ATR, cap SL 5%
- SWING: SL 2.2×ATR, TP 3.5×ATR, cap SL 5%

### Fix Gemini sceglie stile_operativo arbitrariamente (brain_la.py)
**Problema:** Il prompt elencava SCALPING/SWING/MOMENTUM senza nessuna regola su quando usarli. Gemini sceglieva a caso.
**Correzione:** Aggiunta regola 12: SCALPING = segnale < 15min su 1m-5m, MOMENTUM = breakout 15m, SWING = struttura H1 confermata solo su 1h+. "Il 90% dei segnali su 15m è SCALPING o MOMENTUM."

### Fix SWING aveva automaticamente leva massima (brain_la.py)
**Problema:** `if tipo_op == "SWING": leva_finale = max_lev_consentita` dava sempre la leva massima per SWING indipendentemente da voto e regime. XRP SWING voto 7 MEAN_REVERSION riceveva 5x invece di 3x.
**Correzione:** Stessa logica voto-based per tutti gli stili. SWING cappato a max 5x per tenere conto della durata maggiore.

### Fix trade LIMIT rimosso subito dopo apertura (trade_manager.py)
**Problema:** Un ordine LIMIT appena piazzato non risulta tra le posizioni a margine di Kraken. La sincronizzazione lo rimuoveva come "posizione fantasma" generando TRADE SINCRONIZZATO WIN fasullo. Il controllo ordini pendenti usava matching esatto `pair == ticker_reale` che falliva per differenze di formato.
**Correzione:** Grace period 5 minuti — posizioni aperte negli ultimi 5 minuti non vengono mai rimosse. Matching ordini pendenti normalizzato tramite `_normalize_ticker`.

### Fix thinking_level blocca tutte le chiamate Gemini (brain_la.py)
**Problema:** Il file originale del progetto aveva `"thinking_config": {"thinking_level": ThinkingLevel.LOW}` nella chiamata API. Il modello corrente non supporta questo parametro → errore 400 INVALID_ARGUMENT su OGNI chiamata. Il bot era completamente cieco dalle 19:37 alle 23:09.
**Nota:** Questo bug era nel file originale, non nei file aggiornati. Ogni volta che il bot viene riavviato con il vecchio `brain_la.py` si ripresenta.

### Nuovo modulo: analytics_report.py
**Funzionalità:**
- Fee reali da Kraken Ledger invece di stima 0.26%
- Statistiche complete: per asset, per direzione LONG/SHORT, per ora UTC, per giorno settimana, per stile, per voto IA
- Report automatico giornaliero alle 23:00 UTC
- Report automatico settimanale domenica 20:00 UTC
- Report su richiesta via comando Telegram
- Eseguibile standalone: `python3 analytics_report.py [giorni]`

### Nuovo: comandi Telegram dal bot (bot_la.py + telegram_alerts_la.py)
**Comandi disponibili:**
- /stats o /report → report ultimi 7 giorni
- /report1 → report oggi
- /report7 → report 7 giorni
- /report30 → report 30 giorni
- /ml → stato XGBoost (attivo/attesa, trade disponibili, feature)
- /help → lista comandi

**Implementazione:** polling non-bloccante (timeout=0) in `controlla_comandi()` su TelegramAlerts. Solo messaggi dal chat_id configurato vengono accettati.

**File da aggiungere:**
- `analytics_report.py` → nella cartella principale (stesso livello di bot_la.py)

**File da aggiornare:**
- `bot_la.py` → import + istanziazione + loop principale
- `telegram_alerts_la.py` → metodo `controlla_comandi()` aggiunto

### Fix trailing stop troppo aggressivo (trade_manager.py)
**Problema:** Il trailing si attivava a 0% di profitto (appena il prezzo superava l'entry) con distanza 1.5×ATR fissa. Su BTC SCALPING con ATR=$489: distanza trailing=$733 mentre il TP era a $1590. Il trailing spostava lo SL a break-even prima ancora che il trade avesse respirato, causando chiusure premature a +0.05$ invece del TP a +$1590.
**Esempio reale:** BTC SHORT 23:09, a 0.42% di profitto (19% del percorso verso TP) il trailing era già attivo e stringeva.
**Correzione:**
- Soglia attivazione: il trailing scatta solo dopo 40% del percorso entry→TP
- Distanza calibrata per stile: SCALPING 0.8×ATR, MOMENTUM 1.2×ATR, SWING 2.0×ATR
- Se Hurst > 0.65 (trend forte): distanza ridotta del 20%
- Log migliorato: mostra avanzamento% e stile

**Impatto simulato su BTC SHORT SCALPING:**
- Prima: trailing attivo a 0.42% (19% percorso), SL a $734 di distanza
- Dopo: trailing inattivo fino a 44% percorso (~1% profitto), SL a $391 di distanza

### Fix verifica chiusura parziale su Kraken (trade_manager.py)
**Problema:** La chiusura parziale inviava l'ordine e registrava il PnL senza mai verificare che Kraken avesse effettivamente ridotto la size. Un ordine con ID non garantisce l'esecuzione.
**Correzione:** Dopo 2s dall'invio, confronta la size reale su Kraken con quella attesa (size_totale - size_da_chiudere). Log: "VERIFICA KRAKEN: size confermata" o "⚠️ size Kraken diversa da attesa — possibile esecuzione parziale o fallita."

### Fix muri di liquidità sempre a 0.1% (engine_la.py)
**Problema:** `bin_size = last_price * 0.001` troppo piccolo. Con XRP a 1.45, ogni bin = $0.00145 → il "muro" era sempre il livello immediatamente adiacente nel book (dist 0.1%), non un vero cluster di liquidità. Pressione sempre 1.0 (falsa).
**Correzione:** Bin size adattivo per asset:
- BTC (>10000): 0.2% — trova cluster ogni ~$140
- ETH/SOL (>100): 0.3% — cluster ogni ~$6
- Altcoin (<100): 0.5% — livelli psicologici reali
Inoltre salta il bin del prezzo corrente — un muro deve essere distaccato dal prezzo, non il livello 1 del book.

### Fix rate limit Kraken — cache posizioni e ordini (performer_la.py + trade_manager.py)
**Problema:** Ogni ciclo del loop principale chiamava `get_open_positions_real()` globale + una per ogni asset in `is_posizione_aperta_su_kraken` + una in `sincronizza_e_ripara`. Con 8 asset = ~17 chiamate API private/ciclo. Kraken limita a ~15/min → timeout a cascata su tutti gli asset.
**Correzione:** Cache TTL 25s in `PerformerLA`:
- `get_open_positions_real(force=False)`: restituisce cache se < 25s, altrimenti chiama API
- `get_open_orders_real(force=False)`: stessa logica
- `force=True`: usato nei 4 punti critici (dopo apertura ordine, dopo chiusura totale ×2, dopo chiusura parziale)
- Cache invalidata esplicitamente prima di ogni `force=True`
- Se API fallisce e cache disponibile: usa cache vecchia con warning invece di eccezione
- Metodo `invalidate_cache()` aggiunto per uso futuro pulito
**Impatto:** Da ~17 chiamate/ciclo a 1-2 chiamate/ciclo (una ogni 25s).

### Fix TP al 9% su XRP MOMENTUM (brain_la.py)
**Problema:** Gemini suggeriva SL=1.35147 su XRP LONG (entry 1.42293) — distanza 5%, tipica di uno SWING su timeframe H1+. Su un trade MOMENTUM il SL veniva accettato senza controllo. Il muro di resistenza era a solo 0.27% (1.4297) ma il R:R enforcement espandeva il TP a 1.5×0.0711 = 1.5293 (+7.5%) ignorando il muro.
**Causa radice:** SL di Gemini accettato senza cap per stile + muri troppo vicini (<0.3%) non filtrati.
**Correzione:**
1. Cap SL per stile: SCALPING 1.5%, MOMENTUM 3.5%, SWING 7%. Se Gemini suggerisce oltre il cap, viene ignorato e si usa ATR.
2. Soglia minima muro: muri a distanza < 0.3% non vengono usati come SL (sono spread normale, non livelli reali).
3. Cap SL assoluto per stile nel fallback ATR: SCALPING max 1.5%, MOMENTUM max 3.5%, SWING max 5%.
**Risultato simulato su XRP:** SL 0.43% (ATR×1.5) invece di 5%, TP al muro reale 1.86%, R:R 4.3.

### Nuova penalità: correlazione BTC contro-trend (brain_la.py)
**Problema:** Il bot apriva LONG su altcoin (ETH, SOL) mentre BTC era in downtrend confermato, perdendo -1.39$ in 2 trade il 24/03 su -1.7$ totali giornalieri.
**Soluzione:** Penalità voto graduata -1/-2/-3 basata su 3 condizioni BTC:
1. Struttura H1 BTC opposta al segnale (DOWNTREND con LONG) → -1
2. Hurst BTC > 0.55 + price velocity in direzione opposta → -1
3. Regime TRENDING BTC con velocity contraria → -1

**Regole:**
- Si applica SOLO su altcoin con correlazione_driver >= 0.60 (non su BTC stesso)
- Non blocca mai SHORT su altcoin quando BTC scende (direzione giusta)
- Confluisce nel cap totale penalità -4 già esistente
- Soglia attivazione voto minimo 6 — con 2-3 condizioni avverse un voto 7 diventa 4-5 (bloccato)
- Dati BTC recuperati da Engine cache — zero chiamate API aggiuntive

**Simulazione su trade 24/03:**
- ETH LONG 00:07: -2 → voto 5 🚫 (perdita -0.66$ evitata)
- SOL LONG 13:45: -3 → voto 4 🚫 (perdita -0.73$ evitata)
- SOL LONG 07:46: 0 → voto 7 ✅ (BTC saliva, corretto passare)
- SOL SHORT 15:44: 0 → voto 7 ✅ (SHORT con BTC in discesa, corretto)

### Sizing formula corretta — rimosso size_factor arbitrario (trade_manager.py)
**Problema:** Il sizing moltiplicava RISK_PER_TRADE × size_factor (0.3-0.7 da Gemini) producendo size casuali. Con SL larghi (5%) e size_factor 0.5 il notional era ridicolo (~$80).
**Correzione:** Usa sempre RISK_PER_TRADE direttamente senza size_factor. Aggiunto cap esplicito: max 40% del capitale × leva per singolo trade (evita che BTC con SL 0.5% usi $800 notional su $80 di capitale).
**Risultato:** Con RISK_PER_TRADE=10% e SL corretti (0.5-1.5%), notional $160-320$ su $80 capitale → 1% movimento = $1.6-3.2$.

### Time-stop intelligente — sospeso in profitto con trend confermato (trade_manager.py)
**Problema:** Il time-stop chiudeva trade in perdita anche quando la direzione era giusta (SOL SHORT con BTC in discesa, perso -0.71$).
**Logica nuova:**
- In profitto + trend confermato (H1 allineato + Hurst>0.52) → time-stop SOSPESO, gestisce solo il trailing
- In profitto + trend incerto → soglie +50%
- In perdita + trend ancora favorevole → soglie +30% (potrebbe recuperare)
- In perdita + trend invertito → soglie -20% (chiudi prima che peggiori)

**Simulazione su trade 24/03:**
- ETH LONG 00:07 (contro trend): soglie -20% → exit prima
- SOL SHORT 15:44 (con trend): soglie +30%, time-stop NON scatta a 3h
- SOL LONG 13:45 (contro trend): soglie -20%
