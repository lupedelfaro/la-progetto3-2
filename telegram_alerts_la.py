import sys
# -*- coding: utf-8 -*-
"""
L&A Institutional Bot - TelegramAlerts
Modulo per la gestione degli alert e delle notifiche Telegram.
Versione razionalizzata, centralizzata e pronta all'uso.
"""

import logging
import requests
from core import asset_list
from core import config_la
from core.chimera_errors import ErrorTracker
_err = ErrorTracker("Telegram")

class TelegramAlerts:
    """
    Gestisce alert/notifiche via Telegram.
    """

    def __init__(self, token=None, chat_id=None):
        self.token = token or config_la.TELEGRAM_TOKEN
        self.chat_id = chat_id or config_la.TELEGRAM_CHAT_ID
        self.logger = logging.getLogger("TelegramAlerts")

    def _clean_markdown(self, text):
        """Escapa caratteri speciali per evitare errori Markdown di Telegram."""
        if not text: return ""
        # Caratteri che rompono il Markdown V1 se non chiusi o usati male
        chars_to_escape = ['_', '*', '`', '[']
        for char in chars_to_escape:
            text = text.replace(char, f"\\{char}")
        return text

    def invia_alert(self, messaggio):
        """
        Invia un messaggio Telegram al chat_id configurato.
        Tenta con Markdown, se fallisce riprova come testo semplice.
        Include retry per errori di rete temporanei.
        """
        import time
        url = f"https://api.telegram.org/bot{self.token}/sendMessage"
        
        # Tentativo 1: Markdown
        data = {
            "chat_id": self.chat_id,
            "text": messaggio,
            "parse_mode": "Markdown"
        }
        
        max_retries = 3
        for attempt in range(max_retries):
            try:
                response = requests.post(url, data=data, timeout=10)
                if response.status_code == 200:
                    self.logger.info(f"✅ Alert Telegram inviato (Markdown)")
                    return True
                elif response.status_code == 400:
                    # Probabile errore Markdown, riprova Plain Text subito
                    self.logger.warning(f"⚠️ Telegram Markdown failure (400), riprovo testo semplice...")
                    data.pop("parse_mode", None)
                    response_plain = requests.post(url, data=data, timeout=10)
                    if response_plain.status_code == 200:
                        self.logger.info(f"✅ Alert Telegram inviato (Plain Text)")
                        return True
                    else:
                        self.logger.error(f"❌ Telegram API failure totale: {response_plain.text}")
                        return False
                else:
                    self.logger.warning(f"⚠️ Telegram API error {response.status_code} (Tentativo {attempt+1}/{max_retries}). Attendo 2s...")
                    time.sleep(2)
            except Exception as e:
                _err.capture(e, getattr(sys, '_getframe', lambda: None)() and sys._getframe().f_code.co_name or '', {"module": "Telegram"})
                self.logger.warning(f"⚠️ Errore invio alert Telegram (Tentativo {attempt+1}/{max_retries}): {e}. Attendo 3s...")
                time.sleep(3)
        
        self.logger.error(f"❌ Falliti tutti i tentativi di invio alert Telegram.")
        return False
            
    def invia_stats_complete(self, stats):
        """Invia statistiche complete (Daily + Total)."""
        testo = "📊 *STATISTICHE PERFORMANCE*\n\n"
        
        testo += "📅 *OGGI (24h):*\n"
        testo += f"• PnL: `${stats['daily']['pnl']}` ({stats['daily']['pnl_pct_real']}%)\n"
        testo += f"• Win Rate: `{stats['daily']['win_rate']}%` \n"
        testo += f"• Trades: {stats['daily']['trades']}\n\n"
        
        testo += "🌍 *TOTALE STORICO:*\n"
        testo += f"• PnL: `${stats['total']['pnl']}` ({stats['total']['pnl_pct_real']}%)\n"
        testo += f"• Win Rate: `{stats['total']['win_rate']}%` \n"
        testo += f"• Trades: {stats['total']['trades']}\n\n"
        
        pos_aperte = [self._clean_markdown(p) for p in stats.get('posizioni_aperte', [])]
        testo += f"📌 *Posizioni Aperte:* {', '.join(pos_aperte) if pos_aperte else 'Nessuna'}"
        
        return self.invia_alert(testo)

    def invia_report_serale(self, dati_report):
        """
        Formatta e invia il report giornaliero generato dal TradeManager.
        """
        testo = "📊 *REPORT GIORNALIERO OPERATIVO*\n\n"
        testo += f"💰 *PNL 24h:* `${dati_report['pnl_totale_24h']}` \n"
        testo += f"✅ *Trades Chiusi:* {dati_report['trades_chiusi']}\n"
        testo += f"🎯 *Win Rate:* {dati_report['win_rate']}%\n\n"
        
        if dati_report.get('dettaglio'):
            testo += "*Dettaglio Trade:*\n"
            for d in dati_report['dettaglio']:
                testo += f"• {self._clean_markdown(d)}\n"
        
        return self.invia_alert(testo)

    # ──────────────────────────────────────────────────────────────────────
    # RICEZIONE COMANDI TELEGRAM (/stats /report7 /report30 /ml)
    # ──────────────────────────────────────────────────────────────────────

    def controlla_comandi(self):
        """
        Polling leggero dei messaggi in arrivo (non-bloccante, timeout=0).
        Chiamare ogni ciclo del bot.
        Restituisce lista di comandi ricevuti: ['/stats', '/report7', ...]
        Solo dal chat_id configurato.
        """
        if not hasattr(self, '_update_offset'):
            self._update_offset = 0

        try:
            url = f"https://api.telegram.org/bot{self.token}/getUpdates"
            resp = requests.get(url, params={
                'offset': self._update_offset,
                'timeout': 0,
                'limit': 10
            }, timeout=5)

            if resp.status_code != 200:
                return []
            data = resp.json()
            if not data.get('ok'):
                return []

            comandi = []
            for update in data.get('result', []):
                self._update_offset = update['update_id'] + 1
                msg   = update.get('message', {})
                testo = msg.get('text', '').strip().lower()
                chat  = str(msg.get('chat', {}).get('id', ''))

                # Solo dal chat_id autorizzato
                if chat != str(self.chat_id):
                    self.logger.warning(f"⚠️ Comando da chat non autorizzato: {chat}")
                    continue

                if testo.startswith('/'):
                    comandi.append(testo)
                    self.logger.info(f"📩 Comando Telegram ricevuto: {testo}")
                    # Gestione immediata /errors
                    if testo == '/errors':
                        try:
                            from core.chimera_errors import ErrorTracker
                            summary = ErrorTracker.get_summary(top_n=5)
                            msg = (
                                f"🔴 *ERRORI RUNTIME CHIMERA*\n"
                                f"Totale: {summary['total']} | CRITICAL: {summary['critical_count']}\n\n"
                                f"*Top moduli:*\n"
                                + "\n".join(f"  {m}: {c}" for m, c in summary['top_modules'][:5])
                                + "\n\n*Top categorie:*\n"
                                + "\n".join(f"  {c}: {n}" for c, n in summary['top_categories'][:5])
                            )
                            if summary['recent']:
                                last = summary['recent'][0]
                                msg += f"\n\n*Ultimo errore:*\n`{last.get('module')}.{last.get('method')}: {last.get('message','')[:100]}`"
                            self.invia_alert(msg)
                        except Exception as e_err:
                            _err.capture(e_err, "controlla_comandi", {"module": "Telegram"})
                            self.invia_alert(f"⚠️ Errore lettura errori: {e_err}")

            return comandi
        except Exception as e:
            _err.capture(e, "controlla_comandi", {"module": "Telegram"})
            self.logger.debug(f"Polling comandi: {e}")
            return []