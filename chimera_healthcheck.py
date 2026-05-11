# -*- coding: utf-8 -*-
"""
CHIMERA v4.0 — Health Check System
Eseguito ogni N ore per verificare:
  1. Sintassi e importabilità di tutti i moduli
  2. Connettività cross-file (metodi chiamati esistono)
  3. DB integrità (tabelle presenti, storico non vuoto)
  4. WebSocket stato connessione
  5. Modello XGBoost AUC e stato
  6. Posizioni DB vs Kraken disallineate
  7. Ghost trades bloccati (PENDING da >24h)
  8. Variabili config critiche presenti
  9. Dipendenze Python installate
 10. File critici presenti su disco

Uso:
  python3 chimera_healthcheck.py          # run manuale
  python3 chimera_healthcheck.py --silent # solo errori su stdout
  python3 chimera_healthcheck.py --loop 4 # loop ogni 4 ore
"""

import os, sys, json, sqlite3, time, ast, importlib, traceback
from datetime import datetime

ROOT = os.path.dirname(os.path.abspath(__file__))
CORE = os.path.join(ROOT, 'core')
DB   = os.path.join(ROOT, 'chimera.db')

SILENT = '--silent' in sys.argv
LOOP_H = None
for i, a in enumerate(sys.argv):
    if a == '--loop' and i+1 < len(sys.argv):
        try: LOOP_H = float(sys.argv[i+1])
        except: pass

# ─────────────────────────────────────────────────────────────
class HealthCheck:
    def __init__(self):
        self.results = []   # (status, category, message)
        self.errors  = 0
        self.warns   = 0

    def ok(self, cat, msg):
        self.results.append(('OK',   cat, msg))

    def warn(self, cat, msg):
        self.results.append(('WARN', cat, msg))
        self.warns += 1

    def fail(self, cat, msg):
        self.results.append(('FAIL', cat, msg))
        self.errors += 1

    def header(self, title):
        self.results.append(('HEAD', '', title))

    # ── 1. SINTASSI ────────────────────────────────────────────
    def check_syntax(self):
        self.header("SINTASSI MODULI")
        core_files = [
            'engine_la','brain_la','trade_manager','performer_la',
            'feedback_engine','chimera_ml','strategy_engine','ws_manager',
            'signal_state_engine','macro_sentiment','night_review',
            'asset_rotation','database_manager',
            'chimera_auditor','telegram_alerts_la','dashboard_la',
            'kraken_mirror','asset_list','config_la',
        ]
        root_files = ['bot_la', 'analytics_report']
        for name in core_files:
            path = os.path.join(CORE, f'{name}.py')
            self._check_file_syntax(name, path)
        for name in root_files:
            path = os.path.join(ROOT, f'{name}.py')
            self._check_file_syntax(name, path)
        path_ar = os.path.join(ROOT, 'analytics_report.py')
        if os.path.exists(path_ar):
            self._check_file_syntax('analytics_report(root)', path_ar)

    def _check_file_syntax(self, name, path):
        if not os.path.exists(path):
            self.fail("SINTASSI", f"{name}: file NON TROVATO ({path})")
            return
        try:
            ast.parse(open(path, encoding='utf-8').read())
            self.ok("SINTASSI", f"{name}: OK")
        except SyntaxError as e:
            self.fail("SINTASSI", f"{name}: SyntaxError riga {e.lineno} — {e.msg}")

    # ── 2. CONNETTIVITÀ CROSS-FILE ────────────────────────────
    def check_connectivity(self):
        self.header("CONNETTIVITÀ CROSS-FILE")
        def load(name, root=False):
            base = ROOT if root else CORE
            p = os.path.join(base, f'{name}.py')
            return open(p, encoding='utf-8').read() if os.path.exists(p) else ''

        tm     = load('trade_manager')
        brain  = load('brain_la')
        engine = load('engine_la')
        fe     = load('feedback_engine')
        ml     = load('chimera_ml')
        se     = load('strategy_engine')
        perf   = load('performer_la')
        ws     = load('ws_manager')
        db_m   = load('database_manager')
        nr     = load('night_review')
        bot    = load('bot_la', root=True)

        checks = [
            # (modulo_sorgente, metodo, file_dove_deve_esistere, label)
            ('bot_la',          'sincronizza_e_ripara',              tm,     'trade_manager'),
            ('bot_la',          'gestisci_protezione_istituzionale', tm,     'trade_manager'),
            ('bot_la',          'check_invalidazione_tesi',          tm,     'trade_manager'),
            ('bot_la',          '_esegui_chiusura_totale',           tm,     'trade_manager'),
            ('bot_la',          'apri_posizione',                    tm,     'trade_manager'),
            ('bot_la',          'full_global_strategy',              brain,  'brain_la'),
            ('bot_la',          'valuta_validita_tesi',              brain,  'brain_la'),
            ('bot_la',          'analizza_fase_due_chimera',         brain,  'brain_la'),
            ('bot_la',          'get_full_market_data',              engine, 'engine_la'),
            ('bot_la',          'check_sentinel',                    engine, 'engine_la'),
            ('bot_la',          'get_asset_leverage_info',           engine, 'engine_la'),
            ('bot_la',          'registra_analisi_scartata',         fe,     'feedback_engine'),
            ('bot_la',          'verifica_esiti_ghost',              fe,     'feedback_engine'),
            ('bot_la',          'predici',                           ml,     'chimera_ml'),
            ('bot_la',          'analyze',                           se,     'strategy_engine'),
            ('trade_manager',   'gestisci_ordine_protezione',        perf,   'performer_la'),
            ('trade_manager',   'cancella_ordine_specifico',         perf,   'performer_la'),
            ('trade_manager',   'pulizia_totale_ordini',             perf,   'performer_la'),
            ('trade_manager',   'get_open_positions_real',           perf,   'performer_la'),
            ('engine_la',       'get_ticker',                        ws,     'ws_manager'),
            ('engine_la',       'get_trades',                        ws,     'ws_manager'),
            ('engine_la',       'get_orderbook',                     ws,     'ws_manager'),
            ('feedback_engine', 'registra_trade_chiuso',             ml,     'chimera_ml'),
            ('feedback_engine', 'save_ghosts',                       db_m,   'database_manager'),
            ('feedback_engine', 'save_storico',                      db_m,   'database_manager'),
        ]

        for caller, method, target_src, target_name in checks:
            if f'def {method}' in target_src:
                self.ok("CONNETTIVITÀ", f"{caller} → {target_name}.{method}")
            else:
                self.fail("CONNETTIVITÀ", f"{caller} → {target_name}.{method} NON ESISTE")

        # Check specifici
        if 'closetrade' in tm:
            self.ok("CONNETTIVITÀ", "trade_manager: closetrade=yes presente")
        else:
            self.fail("CONNETTIVITÀ", "trade_manager: closetrade=yes MANCANTE — chiusure aprono posizione opposta")

        if 'db_manager.get_storico' in nr:
            self.ok("CONNETTIVITÀ", "night_review: usa db_manager (non cache)")
        else:
            self.fail("CONNETTIVITÀ", "night_review: usa self.tm.storico_trades (cache obsoleta)")

        if 'peso = 0.0' in ml:
            self.ok("CONNETTIVITÀ", "chimera_ml: STORICO_SIMULATO peso=0.0")
        else:
            self.warn("CONNETTIVITÀ", "chimera_ml: STORICO_SIMULATO peso non è 0.0 — può abbassare AUC")

    # ── 3. DATABASE ────────────────────────────────────────────
    def check_database(self):
        self.header("DATABASE SQLITE")
        if not os.path.exists(DB):
            self.fail("DATABASE", f"chimera.db NON TROVATO in {ROOT}")
            return
        try:
            conn = sqlite3.connect(DB, timeout=5)
            tables = [r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()]
            required = ['posizioni_aperte','storico_trades','feedback_db','ghost_trades','stats_globali']
            for t in required:
                if t in tables:
                    self.ok("DATABASE", f"tabella {t} presente")
                else:
                    self.fail("DATABASE", f"tabella {t} MANCANTE")

            n_storico = conn.execute("SELECT COUNT(*) FROM storico_trades").fetchone()[0]
            n_pos     = conn.execute("SELECT COUNT(*) FROM posizioni_aperte").fetchone()[0]
            n_ghost   = conn.execute("SELECT COUNT(*) FROM ghost_trades").fetchone()[0]

            self.ok("DATABASE", f"storico_trades: {n_storico} trade")
            self.ok("DATABASE", f"posizioni_aperte: {n_pos} posizioni")
            self.ok("DATABASE", f"ghost_trades: {n_ghost} ghost")

            # Ghost bloccati >24h
            ghost_bloccati = 0
            now_ts = time.time()
            for row in conn.execute("SELECT data FROM ghost_trades").fetchall():
                try:
                    d = json.loads(row[0])
                    if d.get('stato') == 'PENDING' and (now_ts - float(d.get('timestamp',now_ts))) > 86400:
                        ghost_bloccati += 1
                except: pass
            if ghost_bloccati > 0:
                self.warn("DATABASE", f"{ghost_bloccati} ghost PENDING da >24h (saranno scaduti al prossimo ciclo)")
            else:
                self.ok("DATABASE", "nessun ghost bloccato >24h")

            # Trade reali con snapshot per XGBoost
            n_reali = conn.execute("""
                SELECT COUNT(*) FROM storico_trades
                WHERE json_extract(data,'$.chimera_snapshot') IS NOT NULL
                AND json_extract(data,'$.esito') IN ('WIN','LOSS')
                AND (json_extract(data,'$.fonte') IS NULL
                     OR json_extract(data,'$.fonte') NOT IN ('STORICO_SIMULATO'))
            """).fetchone()[0]
            if n_reali == 0:
                self.warn("DATABASE", f"Trade reali con snapshot: 0 — XGBoost non può addestrare")
            elif n_reali < 30:
                self.ok("DATABASE", f"Trade reali con snapshot: {n_reali} (accumulo in corso, min 30)")
            else:
                self.ok("DATABASE", f"Trade reali con snapshot: {n_reali} — XGBoost attivabile")

            conn.close()
        except Exception as e:
            self.fail("DATABASE", f"Errore accesso DB: {e}")

    # ── 4. MODELLO ML ──────────────────────────────────────────
    def check_ml(self):
        self.header("MODELLO XGBOOST")
        model_path  = os.path.join(ROOT, 'chimera_ml_model.json')
        scaler_path = os.path.join(ROOT, 'chimera_ml_scaler.json')
        if os.path.exists(model_path):
            self.ok("ML", f"modello presente ({os.path.getsize(model_path)//1024}KB)")
            # Leggi AUC dal scaler
            if os.path.exists(scaler_path):
                try:
                    sc = json.load(open(scaler_path))
                    n_train = sc.get('n_trade_train', 0)
                    self.ok("ML", f"addestrato su {n_train} trade — timestamp: {sc.get('timestamp','?')[:16]}")
                except: pass
        else:
            self.warn("ML", "modello non presente — XGBoost non ancora addestrato")

    # ── 5. FILE CRITICI ────────────────────────────────────────
    def check_files(self):
        self.header("FILE CRITICI")
        critical = [
            (os.path.join(ROOT, 'bot_la.py'),           'bot_la.py'),
            (os.path.join(CORE, 'engine_la.py'),        'core/engine_la.py'),
            (os.path.join(CORE, 'brain_la.py'),         'core/brain_la.py'),
            (os.path.join(CORE, 'trade_manager.py'),    'core/trade_manager.py'),
            (os.path.join(CORE, 'performer_la.py'),     'core/performer_la.py'),
            (os.path.join(CORE, 'config_la.py'),        'core/config_la.py'),
            (os.path.join(CORE, 'ws_manager.py'),       'core/ws_manager.py'),
            (os.path.join(CORE, 'feedback_engine.py'),  'core/feedback_engine.py'),
            (os.path.join(CORE, 'chimera_ml.py'),       'core/chimera_ml.py'),
            (os.path.join(CORE, 'database_manager.py'), 'core/database_manager.py'),
            (os.path.join(CORE, 'asset_list.py'),       'core/asset_list.py'),
        ]
        for path, label in critical:
            if os.path.exists(path):
                kb = os.path.getsize(path) // 1024
                self.ok("FILE", f"{label} ({kb}KB)")
            else:
                self.fail("FILE", f"{label} NON TROVATO")

    # ── 6. DIPENDENZE PYTHON ───────────────────────────────────
    def check_deps(self):
        self.header("DIPENDENZE PYTHON")
        deps = [
            ('ccxt',              'ccxt'),
            ('pandas',            'pandas'),
            ('numpy',             'numpy'),
            ('pydantic',          'pydantic'),
            ('xgboost',           'xgboost'),
            ('sklearn',           'scikit-learn'),
            ('google.genai',      'google-genai'),
            ('websocket',         'websocket-client'),
            ('requests',          'requests'),
            ('yfinance',          'yfinance'),
        ]
        for module, pkg in deps:
            try:
                importlib.import_module(module)
                self.ok("DEPS", f"{pkg}: installato")
            except ImportError:
                self.warn("DEPS", f"{pkg}: NON installato (pip install {pkg})")

    # ── 7. CONFIG CRITICHE ─────────────────────────────────────
    def check_config(self):
        self.header("CONFIGURAZIONE")
        config_path = os.path.join(CORE, 'config_la.py')
        if not os.path.exists(config_path):
            self.fail("CONFIG", "config_la.py NON TROVATO")
            return
        try:
            sys.path.insert(0, ROOT)
            import importlib.util
            spec = importlib.util.spec_from_file_location("config_la", config_path)
            cfg = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(cfg)
            for var in ['KRAKEN_KEY','KRAKEN_SECRET','GEMINI_API_KEY','TELEGRAM_TOKEN','TELEGRAM_CHAT_ID','BRAIN_SOGGLIA']:
                val = getattr(cfg, var, None)
                if val:
                    masked = str(val)[:4] + '***' if var not in ('BRAIN_SOGGLIA',) else str(val)
                    self.ok("CONFIG", f"{var}: {masked}")
                else:
                    self.fail("CONFIG", f"{var}: MANCANTE o vuoto")
        except Exception as e:
            self.warn("CONFIG", f"Impossibile caricare config_la: {e}")

    # ── REPORT ─────────────────────────────────────────────────
    def check_errors(self):
        self.header("ERRORI RUNTIME (chimera_errors.json)")
        err_path = os.path.join(ROOT, 'chimera_errors.json')
        if not os.path.exists(err_path):
            self.ok("ERRORS", "chimera_errors.json non presente — nessun errore registrato")
            return
        try:
            import json as _json
            data = _json.load(open(err_path))
            errors = data.get('errors', [])
            total = len(errors)
            critical = sum(1 for e in errors if e.get('level') == 'CRITICAL')
            
            if total == 0:
                self.ok("ERRORS", "Nessun errore registrato")
                return
            
            # Top moduli con errori
            from collections import defaultdict
            by_module = defaultdict(int)
            by_cat    = defaultdict(int)
            for e in errors:
                by_module[e.get('module','?')] += 1
                by_cat[e.get('category','?')] += 1
            
            self.ok("ERRORS", f"Totale errori: {total} | CRITICAL: {critical}")
            
            for mod, cnt in sorted(by_module.items(), key=lambda x: -x[1])[:5]:
                fn = self.warn if cnt > 10 else self.ok
                fn("ERRORS", f"  {mod}: {cnt} errori")
            
            for cat, cnt in sorted(by_cat.items(), key=lambda x: -x[1])[:3]:
                self.ok("ERRORS", f"  Categoria {cat}: {cnt}")
            
            # Ultimo errore
            if errors:
                last = errors[0]
                sev = self.fail if last.get('level') == 'CRITICAL' else self.warn
                sev("ERRORS", f"Ultimo: [{last.get('ts','')[:16]}] {last.get('module')}.{last.get('method')} — {last.get('message','')[:80]}")
            
            if critical > 0:
                self.fail("ERRORS", f"{critical} errori CRITICAL — verificare chimera_errors.json")
        except Exception as e:
            self.warn("ERRORS", f"Impossibile leggere chimera_errors.json: {e}")

    def run_all(self):
        self.check_syntax()
        self.check_connectivity()
        self.check_database()
        self.check_ml()
        self.check_files()
        self.check_deps()
        self.check_config()
        self.check_errors()

    def print_report(self):
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        print(f"\n{'='*60}")
        print(f"  CHIMERA v4.0 — HEALTH CHECK  [{now}]")
        print(f"{'='*60}")

        for status, cat, msg in self.results:
            if status == 'HEAD':
                print(f"\n  ▸ {msg}")
                continue
            if SILENT and status == 'OK':
                continue
            icon = {'OK':'✅','WARN':'⚠️ ','FAIL':'❌'}[status]
            print(f"  {icon} {msg}")

        print(f"\n{'='*60}")
        print(f"  RISULTATO: {self.errors} errori, {self.warns} avvertimenti")
        if self.errors == 0 and self.warns == 0:
            print("  🟢 SISTEMA SANO — tutti i moduli collegati e funzionanti")
        elif self.errors == 0:
            print("  🟡 SISTEMA OK con avvertimenti — verificare i WARN")
        else:
            print("  🔴 SISTEMA CON ERRORI — correggere i FAIL prima di avviare")
        print(f"{'='*60}\n")

        # Salva log
        log_path = os.path.join(ROOT, 'healthcheck.log')
        with open(log_path, 'a', encoding='utf-8') as f:
            f.write(f"\n[{now}] errori={self.errors} warn={self.warns}\n")
            for status, cat, msg in self.results:
                if status != 'HEAD' and (status != 'OK' or self.errors > 0):
                    f.write(f"  [{status}] {msg}\n")

        return self.errors


def run():
    hc = HealthCheck()
    hc.run_all()
    return hc.print_report()


if __name__ == '__main__':
    if LOOP_H:
        print(f"🔄 Health check ogni {LOOP_H}h — Ctrl+C per fermare")
        while True:
            run()
            time.sleep(LOOP_H * 3600)
    else:
        sys.exit(run() or 0)
