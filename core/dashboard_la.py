# -*- coding: utf-8 -*-
from flask import Flask, render_template_string, jsonify, request
import threading
import datetime
import json
import os

class DashboardLA:
    def __init__(self, brain_instance, port=5000):
        self.app = Flask("L&A_Dashboard")
        self.brain = brain_instance
        self.port = port
        self._setup_routes()

    def _setup_routes(self):
        @self.app.route("/")
        def index():
            # 1. Recupero dati reali dal TradeManager
            # Usiamo .copy() per evitare conflitti mentre il bot scrive sul file
            posizioni_raw = getattr(self.brain.trade_manager, 'posizioni_aperte', {})
            posizioni = posizioni_raw.copy()
            
            # --- CALCOLO P&L IN TEMPO REALE PER DASHBOARD ---
            for asset, p in posizioni.items():
                try:
                    # Recuperiamo l'ultimo prezzo che il bot ha salvato nella posizione
                    prezzo_attuale = p.get('prezzo_corrente', 0)
                    prezzo_entrata = p.get('p_entrata', 0)
                    
                    if prezzo_attuale > 0 and prezzo_entrata > 0:
                        diff = (prezzo_attuale - prezzo_entrata) / prezzo_entrata
                        pnl = diff * 100
                        # Se la posizione è Short (SELL), il profitto è invertito
                        if p.get('direzione') == 'SELL':
                            pnl = -pnl
                        p['pnl_perc'] = round(pnl, 2)
                    else:
                        p['pnl_perc'] = 0.0
                except Exception as e:
                    print(f"Errore calcolo P&L Dashboard per {asset}: {e}")
                    p['pnl_perc'] = 0.0
            # ------------------------------------------------

            # 2. Calcolo Win Rate Reale dallo storico
            win_rate = "0.0%"
            try:
                if os.path.exists("storico_trades.json"):
                    with open("storico_trades.json", "r") as f:
                        storico = json.load(f)
                    if len(storico) > 0:
                        vinti = len([t for t in storico if t.get('pnl_finale', 0) > 0])
                        win_rate = f"{(vinti / len(storico)) * 100:.1f}%"
            except:
                win_rate = "0.0%"

            # 3. Calcolo esposizione reale
            try:
                esposizione_tot = sum([float(p.get('size', 0) or 0) for p in posizioni.values()])
            except:
                esposizione_tot = 0.0
                
            html_template = """
            <!DOCTYPE html>
            <html lang="it">
            <head>
                <meta charset="UTF-8">
                <meta name="viewport" content="width=device-width, initial-scale=1.0">
                <title>L&A Institutional Dashboard</title>
                <style>
                    body { font-family: 'Inter', sans-serif; background: #0b0e11; color: #eaecef; padding: 15px; margin: 0; }
                    .header { display: flex; justify-content: space-between; align-items: center; border-bottom: 1px solid #2b3139; padding-bottom: 10px; }
                    .stat-grid { display: grid; grid-template-columns: 1fr 1fr; gap: 10px; margin: 15px 0; }
                    .stat-card { background: #1e2329; padding: 12px; border-radius: 8px; text-align: center; border: 1px solid #2b3139; }
                    .card { background: #1e2329; border-radius: 12px; padding: 15px; margin-bottom: 12px; border-left: 6px solid #474d57; position: relative; }
                    .buy { border-left-color: #02c076; }
                    .sell { border-left-color: #f84960; }
                    .pnl-pos { color: #02c076; font-weight: bold; font-size: 20px; }
                    .pnl-neg { color: #f84960; font-weight: bold; font-size: 20px; }
                    .panic-btn { background: #f84960; color: white; border: none; width: 100%; padding: 16px; border-radius: 8px; font-weight: bold; font-size: 16px; cursor: pointer; margin-top: 10px; }
                    .reasoning { font-size: 12px; color: #848e9c; background: #2b3139; padding: 8px; border-radius: 4px; margin-top: 10px; line-height: 1.4; }
                    table { width: 100%; margin-top: 10px; font-size: 13px; border-collapse: collapse; }
                    th { text-align: left; color: #848e9c; padding-bottom: 5px; }
                    td { padding: 3px 0; }
                </style>
                <script>
                    function panic() {
                        if(confirm("⚠️ ANDREA, CONFERMI CHIUSURA TOTALE?")) {
                            fetch('/panic', {method: 'POST'})
                            .then(res => res.json())
                            .then(data => alert(data.message));
                        }
                    }
                    // Refresh automatico ogni 15 secondi per vedere i prezzi muoversi
                    setTimeout(function(){ location.reload(); }, 15000);
                </script>
            </head>
            <body>
                <div class="header">
                    <h3 style="margin:0; color:#f0b90b;">L&A COMMAND</h3>
                    <div style="text-align: right;">
                        <span style="color: #02c076; font-size: 14px;">● ONLINE</span><br>
                        <small style="color: #848e9c;">Sync: {{ time }}</small>
                    </div>
                </div>

                <div class="stat-grid">
                    <div class="stat-card">
                        <small style="color: #848e9c;">Esposizione Totale</small><br>
                        <span style="font-size: 18px;">{{ esposizione_tot|round(4) }} Units</span>
                    </div>
                    <div class="stat-card">
                        <small style="color: #848e9c;">Win Rate Reale</small><br>
                        <span style="font-size: 18px; color: #f0b90b;">{{ win_rate }}</span>
                    </div>
                </div>

                {% for asset, p in posizioni.items() %}
                <div class="card {{ 'buy' if p.get('direzione') == 'BUY' else 'sell' }}">
                    <div style="display: flex; justify-content: space-between; align-items: center;">
                        <span style="font-size: 20px; font-weight: bold;">{{ asset }}</span>
                        <span class="{{ 'pnl-pos' if p.get('pnl_perc', 0) >= 0 else 'pnl-neg' }}">
                            {{ '+' if p.get('pnl_perc', 0) > 0 }}{{ p.get('pnl_perc', 0.0) }}%
                        </span>
                    </div>
                    <table>
                        <tr><th>DIR</th><th>ENTRY</th><th>LAST</th><th>SL</th></tr>
                        <tr>
                            <td style="color: {{ '#02c076' if p.get('direzione') == 'BUY' else '#f84960' }}">
                                <b>{{ p.get('direzione', 'N/D') }}</b>
                            </td>
                            <td>{{ p.get('p_entrata', '0.0') }}</td>
                            <td style="color: #f0b90b;">{{ p.get('prezzo_corrente', '---') }}</td>
                            <td style="color: #f84960;">{{ p.get('sl', '0.0') }}</td>
                        </tr>
                    </table>
                    <div class="reasoning">
                        🤖 <b>Analisi BrainLA:</b> {{ p.get('motivazione', 'Analisi in corso...')[:200] }}
                    </div>
                </div>
                {% else %}
                <div style="text-align: center; padding: 40px; color: #848e9c;">📭 Nessuna posizione aperta nel TradeManager.</div>
                {% endfor %}

                <button class="panic-btn" onclick="panic()">🚨 PANIC: CLOSE ALL POSITIONS</button>
            </body>
            </html>
            """
            now = datetime.datetime.now().strftime("%H:%M:%S")
            return render_template_string(html_template, 
                                          posizioni=posizioni, 
                                          esposizione_tot=esposizione_tot, 
                                          win_rate=win_rate, 
                                          time=now)

        @self.app.route("/panic", methods=["POST"])
        def panic():
            try:
                # Nota: assicurati che l'oggetto OMS o TradeManager abbia questo metodo
                self.brain.trade_manager.close_all_positions()
                return jsonify({"status": "success", "message": "Panic mode: Tutte le posizioni chiuse!"})
            except Exception as e:
                return jsonify({"status": "error", "message": str(e)}), 500

    def run(self):
        # Eseguiamo Flask in un thread separato per non bloccare il bot
        thread = threading.Thread(target=self.app.run, kwargs={"host": "0.0.0.0", "port": self.port, "debug": False, "use_reloader": False})
        thread.daemon = True
        thread.start()