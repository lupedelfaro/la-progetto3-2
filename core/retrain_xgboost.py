#!/usr/bin/env python3
"""
CHIMERA — Retrain XGBoost con 52 feature (include strutturali del ciclo)

Uso:
  cd /Users/LauraAndrea/Desktop/la\ progetto3\ 2
  python3 retrain_xgboost.py
"""

import os, sys, json, sqlite3, time
import numpy as np

BASE_DIR    = os.path.dirname(os.path.abspath(__file__))
DB_PATH     = os.path.join(BASE_DIR, 'chimera.db')
MODEL_PATH  = os.path.join(BASE_DIR, 'chimera_ml_model.json')
SCALER_PATH = os.path.join(BASE_DIR, 'chimera_ml_scaler.pkl')
DATA_DA     = "2026-03-20"
DATA_PIVOT  = "2026-04-01"

sys.path.insert(0, BASE_DIR)
sys.path.insert(0, os.path.join(BASE_DIR, "core"))

def main():
    print(f"\n{'='*60}")
    print("CHIMERA — Retrain XGBoost (52 feature)")
    print(f"{'='*60}\n")

    try:
        import xgboost as xgb
        print(f"✅ XGBoost {xgb.__version__}")
    except ImportError:
        print("❌ XGBoost non trovato. Attiva il venv.")
        sys.exit(1)

    try:
        from chimera_ml import FEATURE_NAMES, ChimeraML
        print(f"✅ chimera_ml — {len(FEATURE_NAMES)} feature")
        strutturali = ['ciclo_recupero_norm','minimo_qualita_num','sr_flip_num','ciclo_fase_num']
        mancanti = [f for f in strutturali if f not in FEATURE_NAMES]
        if mancanti:
            print(f"❌ Feature mancanti: {mancanti} — sostituisci chimera_ml.py")
            sys.exit(1)
        print(f"✅ Feature strutturali presenti")
    except Exception as e:
        print(f"❌ Import chimera_ml: {e}")
        sys.exit(1)

    conn = sqlite3.connect(DB_PATH)
    storico_rows = conn.execute("SELECT data FROM storico_trades ORDER BY rowid ASC").fetchall()
    ghost_rows   = conn.execute("SELECT data FROM ghost_trades").fetchall()
    conn.close()

    storico = []
    for r in storico_rows:
        try: storico.append(json.loads(r[0]))
        except: pass

    ghost_count = 0
    for r in ghost_rows:
        try:
            g = json.loads(r[0])
            if g.get('stato') != 'CLOSED' or g.get('esito') not in ('WIN','LOSS'): continue
            snap = g.get('snapshot', {})
            if not snap: continue
            storico.append({
                "asset": g.get("asset",""), "direzione": g.get("direzione",""),
                "voto_ia": g.get("voto_ia", 5), "esito": g.get("esito"),
                "pnl_netto_usd": g.get("pnl_perc", 0),
                "chimera_snapshot": snap, "fonte": "VIRTUAL_BRAIN",
                "data_apertura": g.get("data",""), "data_chiusura": g.get("data_chiusura",""),
                "decision_source": snap.get("decision_source","GEMINI"),
                "leverage": snap.get("leverage", 1),
                "macro_sentiment": snap.get("macro_sentiment","NEUTRAL"),
                "entry_phase": snap.get("entry_phase","FORMAZIONE"),
            })
            ghost_count += 1
        except: pass

    trade_ml = [
        t for t in storico
        if t.get("chimera_snapshot") and t.get("esito") in ("WIN","LOSS")
        and t.get("direzione") and t.get("fonte") != "STORICO_SIMULATO"
        and str(t.get("data_apertura","") or "") >= DATA_DA
        and isinstance(t.get("chimera_snapshot"), dict)
        and t["chimera_snapshot"].get("ciclo_fase","SCONOSCIUTO") not in ("SCONOSCIUTO","")
    ]
    trade_ml.sort(key=lambda t: str(t.get("data_apertura","") or ""))

    n_totali = len(trade_ml)
    n_win  = sum(1 for t in trade_ml if t.get("esito")=="WIN")
    n_loss = n_totali - n_win
    n_ciclo = sum(1 for t in trade_ml
                  if isinstance(t.get('chimera_snapshot'), dict)
                  and t['chimera_snapshot'].get('ciclo_fase','SCONOSCIUTO') != 'SCONOSCIUTO')

    print(f"📊 Trade: {n_totali} (WIN={n_win} {n_win/n_totali:.0%} | LOSS={n_loss}) + {ghost_count} ghost")
    print(f"📊 Con feature strutturali: {n_ciclo}/{n_totali} ({n_ciclo/n_totali:.0%})")

    if n_totali < 30 or n_win == 0 or n_loss == 0:
        print("❌ Dataset insufficiente")
        sys.exit(1)

    ml = ChimeraML.__new__(ChimeraML)
    ml._xgb_disponibile = True
    ml._xgb = xgb

    X_list, y_list, w_list = [], [], []
    errori = 0

    for t in trade_ml:
        try:
            snap = t.get('chimera_snapshot', {})
            if isinstance(snap, str):
                try: snap = json.loads(snap)
                except: snap = {}
            snap['voto_ia']         = t.get('voto_ia', 5)
            snap['leverage']        = t.get('leverage', snap.get('leverage', 1))
            snap['macro_sentiment'] = t.get('macro_sentiment', snap.get('macro_sentiment','NEUTRAL'))
            snap['decision_source'] = t.get('decision_source', snap.get('decision_source','GEMINI'))
            snap['entry_phase']     = t.get('entry_phase', snap.get('entry_phase','FORMAZIONE'))

            features = ml._estrai_features(
                snap,
                t.get('asset', ''),
                t.get('direzione', 'BUY'),
                int(t.get('voto_ia', 5) or 5),
                str(snap.get('macro_sentiment', t.get('macro_sentiment','NEUTRAL')) or 'NEUTRAL'),
                str(snap.get('decision_source', t.get('decision_source','GEMINI')) or 'GEMINI'),
            )
            if features is None:
                errori += 1
                continue

            label = 1.0 if t.get('esito') == 'WIN' else 0.0
            peso = 2.0 if str(t.get('data_apertura','')) >= DATA_PIVOT else 1.0
            if snap.get('ciclo_fase','SCONOSCIUTO') != 'SCONOSCIUTO':
                peso *= 1.5

            X_list.append(features)
            y_list.append(label)
            w_list.append(peso)
        except:
            errori += 1

    print(f"📊 Feature estratte: {len(X_list)} (errori: {errori})")

    X = np.nan_to_num(np.array(X_list, dtype=np.float32), nan=0.0, posinf=0.0, neginf=0.0)
    y = np.array(y_list, dtype=np.float32)
    w = np.array(w_list, dtype=np.float32)

    scaler_mean = X.mean(axis=0)
    scaler_std  = X.std(axis=0)
    scaler_std[scaler_std == 0] = 1.0
    X_norm = (X - scaler_mean) / scaler_std

    n_train = int(len(X_norm) * 0.8)
    X_train, X_val = X_norm[:n_train], X_norm[n_train:]
    y_train, y_val = y[:n_train], y[n_train:]

    dtrain = xgb.DMatrix(X_train, label=y_train, feature_names=FEATURE_NAMES, weight=w[:n_train])
    dval   = xgb.DMatrix(X_val,   label=y_val,   feature_names=FEATURE_NAMES)

    params = {
        "objective": "binary:logistic", "eval_metric": "auc",
        "max_depth": 4, "learning_rate": 0.05, "subsample": 0.8,
        "colsample_bytree": 0.8, "min_child_weight": 3, "gamma": 0.1,
        "scale_pos_weight": n_loss/n_win if n_win > 0 else 1.0,
        "nthread": 2, "seed": 42, "verbosity": 0,
    }

    print(f"\n🏋️  Addestramento ({n_train} train | {len(X_val)} val)...")
    t0 = time.time()
    model = xgb.train(params, dtrain, num_boost_round=300,
                      evals=[(dval,"val")], early_stopping_rounds=20, verbose_eval=False)
    elapsed = time.time() - t0

    y_pred = model.predict(dval)
    try:
        from sklearn.metrics import roc_auc_score
        auc = roc_auc_score(y_val, y_pred)
    except:
        pos = y_pred[y_val==1]; neg = y_pred[y_val==0]
        auc = float(np.mean([p>n for p in pos for n in neg])) if len(pos)>0 and len(neg)>0 else 0.5

    top10 = sorted(model.get_score(importance_type="gain").items(), key=lambda x: -x[1])[:10]

    print(f"\n{'='*60}")
    print(f"✅ Completato in {elapsed:.1f}s | AUC: {auc:.3f}")
    print(f"\n📊 Top 10 feature:")
    strutturali_set = {'ciclo_recupero_norm','minimo_qualita_num','sr_flip_num','ciclo_fase_num'}
    for i,(feat,score) in enumerate(top10,1):
        tag = " ◀ STRUTTURALE" if feat in strutturali_set else ""
        print(f"   {i:>2}. {feat:<30} {score:>8.0f}{tag}")

    model.save_model(MODEL_PATH)
    print(f"\n💾 Modello → {MODEL_PATH}")

    try:
        import pickle
        with open(SCALER_PATH,'wb') as f:
            pickle.dump({'mean': scaler_mean, 'std': scaler_std}, f)
        print(f"💾 Scaler → {SCALER_PATH}")
    except Exception as e:
        print(f"⚠️ Scaler: {e}")

    rating = "🎯 Solido" if auc >= 0.65 else "📈 Decente" if auc >= 0.55 else "⚠️ Debole"
    print(f"\n{rating} — AUC {auc:.3f}")
    print(f"{'='*60}\n")

if __name__ == '__main__':
    main()
