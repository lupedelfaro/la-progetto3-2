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

class TelegramAlerts:
    """
    Gestisce alert/notifiche via Telegram.
    """

    def __init__(self, token=None, chat_id=None):
        self.token = token or config_la.TELEGRAM_TOKEN
        self.chat_id = chat_id or config_la.TELEGRAM_CHAT_ID
        self.logger = logging.getLogger("TelegramAlerts")

    def invia_alert(self, messaggio):
        """
        Invia un messaggio Telegram al chat_id configurato.
        """
        url = f"https://api.telegram.org/bot{self.token}/sendMessage"
        data = {
            "chat_id": self.chat_id,
            "text": messaggio,
            "parse_mode": "Markdown"  # Abilita il grassetto e lo stile grafico
        }
        try:
            response = requests.post(url, data=data)
            if response.status_code == 200:
                self.logger.info(f"✅ Alert Telegram inviato")
                return True
            else:
                self.logger.warning(f"⚠️ Telegram API failure: {response.text}")
                return False
        except Exception as e:
            self.logger.error(f"⚠️ Errore invio alert Telegram: {e}")
            return False
            
    def invia_report_serale(self, dati_report):
        """
        Formatta e invia il report giornaliero generato dal TradeManager.
        """
        testo = "📊 *REPORT GIORNALIERO OPERATIVO*\n\n"
        testo += f"💰 *PNL 24h:* `{dati_report['pnl_totale_24h']}%` \n"
        testo += f"✅ *Trades Chiusi:* {dati_report['trades_chiusi']}\n"
        testo += f"🎯 *Win Rate:* {dati_report['win_rate']}%\n\n"
        
        if dati_report.get('dettaglio'):
            testo += "*Dettaglio Trade:*\n"
            for d in dati_report['dettaglio']:
                testo += f"• {d}\n"
        
        pos_aperte = dati_report.get('posizioni_ancora_aperte') or dati_report.get('posizioni_aperte')
        testo += f"\n📌 *Posizioni Aperte:* {', '.join(pos_aperte) if pos_aperte else 'Nessuna'}"
        
        return self.invia_alert(testo)