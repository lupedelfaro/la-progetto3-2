# Verifica progressiva del bot

## Obiettivo
Controllare i bug, i collegamenti tra file e la logica d'ingresso.
Dopo aver capito il comportamento attuale, valutare il passaggio da spot a futures/perpetual.

## Regole
- Analizzare un file alla volta.
- Non inviare ordini reali durante le verifiche.
- Distinguere i problemi confermati da quelli da provare.
- Dopo ogni passaggio, aggiornare questo registro.

## Stato
- Fase: analisi del flusso di bot_la.py.
- File in esame: bot_la.py.
- Migrazione a futures/perpetual: non iniziata.

## File controllati
| File | Stato | Note |
|---|---|---|
| bot_la.py | In analisi | Verificare flusso dei segnali, filtri e tutti i percorsi che possono inviare ordini |

## Prossimo passo
Seguire il flusso da dati di mercato a decisione e apertura della posizione.
Poi controllare i moduli chiamati, uno alla volta.
