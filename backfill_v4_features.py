#!/usr/bin/env python3
"""
CHIMERA — Backfill feature v4 nel DB storico
Recupera le feature recuperabili, imposta 0 per le non derivabili
"""
import sqlite3, json, os, sys

# Path DB
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH  = os.path.join(BASE_DIR, "chimera.db")

print(f"DB: {DB_PATH}")

conn = sqlite3.connect(DB_PATH)
rows = conn.execute("SELECT rowid, data FROM storico_trades ORDER BY rowid ASC").fetchall()

aggiornati = 0
invariati  = 0

for rowid, raw in rows:
    try:
        t    = json.loads(raw)
        snap = t.get('chimera_snapshot', {})
        if isinstance(snap, str):
            try: snap = json.loads(snap)
            except: snap = {}

        modificato = False

        # 1. sr_flip_detected — derivabile da sr_flip_tipo
        if snap.get('sr_flip_detected') is None:
            tipo = str(snap.get('sr_flip_tipo', '') or '')
            snap['sr_flip_detected'] = bool(tipo and tipo != 'no' and tipo != '')
            modificato = True

        # 2. minimo_volume_ratio — già presente in quasi tutti, default 1.0
        if snap.get('minimo_volume_ratio') is None:
            snap['minimo_volume_ratio'] = 1.0
            modificato = True

        # 3. sr_res_piu_vicina — non derivabile, default 0
        if snap.get('sr_res_piu_vicina') is None:
            snap['sr_res_piu_vicina'] = 0
            modificato = True

        # 4. sr_sup_piu_vicina — non derivabile, default 0
        if snap.get('sr_sup_piu_vicina') is None:
            snap['sr_sup_piu_vicina'] = 0
            modificato = True

        # 5. pivot_weekly — usa pivot_daily come proxy se disponibile
        if snap.get('pivot_weekly') is None:
            snap['pivot_weekly'] = float(snap.get('pivot_daily', 0) or 0)
            modificato = True

        # 6. sentinel_trigger — non derivabile, default False
        if snap.get('sentinel_trigger') is None:
            snap['sentinel_trigger'] = False
            modificato = True

        # 7. exhaustion_score — non derivabile, default 0
        if snap.get('exhaustion_score') is None:
            snap['exhaustion_score'] = 0
            modificato = True

        # 8. cvd_delta_30s — non derivabile, default 0
        if snap.get('cvd_delta_30s') is None:
            snap['cvd_delta_30s'] = 0
            modificato = True

        # 9. cvd_delta_120s — non derivabile, default 0
        if snap.get('cvd_delta_120s') is None:
            snap['cvd_delta_120s'] = 0
            modificato = True

        # 10. iceberg_presenti — non derivabile, default False
        if snap.get('iceberg_presenti') is None:
            snap['iceberg_presenti'] = False
            modificato = True

        if modificato:
            t['chimera_snapshot'] = snap
            conn.execute(
                "UPDATE storico_trades SET data = ? WHERE rowid = ?",
                (json.dumps(t, ensure_ascii=False), rowid)
            )
            aggiornati += 1
        else:
            invariati += 1

    except Exception as e:
        print(f"  ⚠️ Errore rowid {rowid}: {e}")

conn.commit()
conn.close()

print(f"\n✅ Aggiornati: {aggiornati} | Invariati: {invariati}")
print("Pronto per retrain.")
