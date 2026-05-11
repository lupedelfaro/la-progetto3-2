import json
import logging
import os
import hashlib
from datetime import datetime, timedelta
from typing import TYPE_CHECKING
from core.chimera_errors import ErrorTracker

_err = ErrorTracker("ChimeraAuditor")

try:
    from core.brain_la import AuditorSchema
except ImportError as _e:
    _err.capture(_e, "unknown", {"module": "ChimeraAuditor"})
    AuditorSchema = None
    logging.getLogger("ChimeraAuditor").error(f"❌ Impossibile importare AuditorSchema da brain_la: {_e}")

if TYPE_CHECKING:
    from core.brain_la import BrainLA

# Campi che possono legittimamente essere 0 su certi asset.
# PAXG: nessun futures, nessuna struttura SMC → fvg_bear, ob_bear, breaker ecc. = 0 per design.
# Mercato SILENZIO: cvd_delta_*, exhaustion_score ecc. = 0 per design.
_CAMPI_OPZIONALI_OK_A_ZERO = frozenset({
    "fvg_bear_piu_vicino", "fvg_bull_piu_vicino",
    "ob_bull_piu_vicino", "ob_bear_piu_vicino",
    "breaker_piu_vicino_sopra", "breaker_piu_vicino_sotto",
    "funding_rate", "funding_z_score", "liquidazioni_24h", "open_interest",
    "bos_level", "choch_level", "ha_daily_sr_level",
    "cvd_delta_30s", "cvd_delta_120s", "cvd_acceleration", "signal_age_s",
    "exhaustion_score", "sr_res_piu_vicina", "sr_sup_piu_vicina",
})


class ChimeraAuditor:
    def __init__(self, brain: 'BrainLA', alerts=None):
        self.brain  = brain
        self.alerts = alerts
        self.logger = logging.getLogger("ChimeraAuditor")
        self.log_file = "audit_warnings.log"
        # Memoria audit già eseguiti: {trade_hash: datetime}
        # Previene ri-segnalazione dello stesso trade ad ogni ciclo di 4h
        self._audit_eseguiti: dict = {}
        self._audit_ttl_ore = 24

    def _trade_hash(self, trade: dict) -> str:
        """Fingerprint univoco: asset + data_apertura + snapshot."""
        asset    = trade.get("asset", "")
        data     = trade.get("data_apertura", "")
        snap_str = json.dumps(trade.get("chimera_snapshot", {}), sort_keys=True)
        return hashlib.md5(f"{asset}|{data}|{snap_str}".encode()).hexdigest()

    def _gia_auditato(self, trade_hash: str) -> bool:
        if trade_hash not in self._audit_eseguiti:
            return False
        ore = (datetime.now() - self._audit_eseguiti[trade_hash]).total_seconds() / 3600
        return ore < self._audit_ttl_ore

    def _segna_auditato(self, trade_hash: str) -> None:
        self._audit_eseguiti[trade_hash] = datetime.now()
        # Pulizia: tieni solo ultimi 500
        if len(self._audit_eseguiti) > 500:
            cutoff = datetime.now() - timedelta(hours=self._audit_ttl_ore)
            self._audit_eseguiti = {k: v for k, v in self._audit_eseguiti.items() if v > cutoff}

    def esegui_audit(self, ore_indietro=4):
        """
        Analizza trade aperti/chiusi nelle ultime ore_indietro.
        Ogni trade viene auditato UNA SOLA VOLTA ogni 24h (via hash).
        """
        self.logger.info(f"🕵️‍♂️ Avvio Chimera Auditor (analisi ultime {ore_indietro} ore)...")
        trades = self._recupera_trades_recenti(ore_indietro)

        if not trades:
            msg = f"✅ *AUDIT COMPLETATO*\nNessun trade recente nelle ultime {ore_indietro} ore."
            self.logger.info(msg)
            if self.alerts:
                self.alerts.invia_alert(msg)
            return

        anomalie_trovate = 0
        trade_nuovi = 0

        for trade in trades:
            t_hash = self._trade_hash(trade)
            if self._gia_auditato(t_hash):
                self.logger.debug(
                    f"⏭️ [AUDITOR] Skip trade già auditato: "
                    f"{trade.get('asset')} {trade.get('data_apertura','')[:16]}"
                )
                continue

            # Rispetta cooldown Gemini se attivo — non sovraccaricare durante ciclo normale
            if hasattr(self.brain, '_gemini_quota_until'):
                import time as _t
                if _t.time() < self.brain._gemini_quota_until:
                    self.logger.warning("🕵️ [AUDITOR] Quota Gemini in cooldown — audit sospeso.")
                    break

            # Delay tra analisi per non sovraccaricare Gemini
            import time as _t
            _t.sleep(3)

            trade_nuovi += 1
            self._segna_auditato(t_hash)
            esito = self._analizza_singolo_trade(trade)
            if esito and esito.get('anomalia_rilevata'):
                anomalie_trovate += 1
                self._segnala_anomalia(trade, esito)

        if trade_nuovi == 0:
            self.logger.info("✅ [AUDITOR] Tutti i trade recenti già analizzati — nessun nuovo audit necessario.")
            return

        if anomalie_trovate == 0:
            msg = f"✅ *AUDIT COMPLETATO*\nNessuna anomalia su {trade_nuovi} nuovi trade analizzati."
            self.logger.info(msg)
            with open(self.log_file, "a") as f:
                f.write(f"[{datetime.now().isoformat()}] AUDIT OK: {trade_nuovi} trades.\n")
            if self.alerts:
                self.alerts.invia_alert(msg)
        else:
            msg = f"⚠️ *AUDIT COMPLETATO*\nTrovate {anomalie_trovate} anomalie su {trade_nuovi} trade."
            self.logger.warning(msg)
            if self.alerts:
                self.alerts.invia_alert(msg)

    def _recupera_trades_recenti(self, ore_indietro):
        from core.database_manager import db_manager
        trades = []
        limite = datetime.now() - timedelta(hours=ore_indietro)

        try:
            for asset, dati in db_manager.get_posizioni().items():
                try:
                    if datetime.fromisoformat(dati.get("data_apertura", "").replace("Z", "")) >= limite:
                        if "chimera_snapshot" in dati:
                            trades.append(dati)
                except Exception:
                    pass
        except Exception as e:
            _err.capture(e, "_recupera_trades_recenti", {"module": "ChimeraAuditor"})
            self.logger.error(f"Errore lettura posizioni per audit: {e}")

        try:
            for dati in db_manager.get_storico():
                try:
                    if datetime.fromisoformat(dati.get("data_apertura", "").replace("Z", "")) >= limite:
                        if "chimera_snapshot" in dati:
                            trades.append(dati)
                except Exception:
                    pass
        except Exception as e:
            _err.capture(e, "_recupera_trades_recenti", {"module": "ChimeraAuditor"})
            self.logger.error(f"Errore lettura storico per audit: {e}")

        return trades

    def _analizza_singolo_trade(self, trade):
        asset     = trade.get("asset", "UNKNOWN")
        direzione = trade.get("direzione", "UNKNOWN")
        snapshot  = trade.get("chimera_snapshot", {})
        if not snapshot:
            return None

        # Campi a zero accettabili per questo specifico snapshot
        campi_ok = [
            c for c in _CAMPI_OPZIONALI_OK_A_ZERO
            if snapshot.get(c) in (0, 0.0, None, "")
        ]
        if campi_ok:
            nota_campi = (
                "\nNOTA IMPORTANTE: I seguenti campi a 0 sono ATTESI e NON sono anomalie "
                "(asset senza futures/SMC o fase SILENZIO): "
                + ", ".join(campi_ok[:10])
                + ("..." if len(campi_ok) > 10 else "") + "."
            )
        else:
            nota_campi = ""

        prompt = (
            f"Sei un Revisore dei Conti Quantitativo (Auditor). Analizza questo trade.\n"
            f"Asset: {asset} | Direzione: {direzione}\n"
            f"Snapshot:\n{json.dumps(snapshot, indent=2)}"
            f"{nota_campi}\n\n"
            f"Cerca SOLO anomalie tecniche REALI (dati corrotti, valori impossibili).\n"
            f"NON segnalare: campi nella NOTA sopra, ingressi rischiosi, assenza struttura SMC.\n"
            f"Anomalie REALI da segnalare:\n"
            f"- ATR=0 con prezzo>0\n"
            f"- spread>5% del prezzo\n"
            f"- VPIN fuori range [0,1]\n"
            f"- RSI fuori range [0,100]\n"
            f"- close=0 o negativo\n"
            f"- volume_24h=0 su asset liquido (spread<1%)\n\n"
            f"Rispondi ESCLUSIVAMENTE in JSON:\n"
            f'{{"anomalia_rilevata": true/false, '
            f'"gravita": "ALTA/MEDIA/BASSA", '
            f'"descrizione_problema": "max 30 parole. Vuoto se nessuna anomalia."}}'
        )

        try:
            if AuditorSchema is None:
                self.logger.error("❌ AuditorSchema non disponibile — audit saltato.")
                return None
            return self.brain.chiama_gemini(prompt, is_json=True, schema_class=AuditorSchema)
        except Exception as e:
            _err.capture(e, "_analizza_singolo_trade", {"module": "ChimeraAuditor"})
            self.logger.error(f"Errore LLM durante audit: {e}")
            return None

    def _segnala_anomalia(self, trade, esito):
        asset        = trade.get("asset", "UNKNOWN")
        data_apertura = trade.get("data_apertura", "UNKNOWN")
        desc         = esito.get("descrizione_problema", "Anomalia sconosciuta")
        gravita      = esito.get("gravita", "MEDIA")

        msg = (
            f"🚨 *AUDIT WARNING ({gravita})*\n"
            f"Asset: {asset}\n"
            f"Data: {data_apertura}\n"
            f"Problema: {desc}\n"
            f"Controlla `audit_warnings.log` per i dettagli."
        )
        if self.alerts:
            self.alerts.invia_alert(msg)

        try:
            with open(self.log_file, "a") as f:
                f.write(f"[{datetime.now().isoformat()}] ASSET: {asset} | GRAVITA: {gravita} | DATA_TRADE: {data_apertura}\n")
                f.write(f"PROBLEMA: {desc}\n")
                f.write(f"SNAPSHOT: {json.dumps(trade.get('chimera_snapshot', {}))}\n")
                f.write("-" * 60 + "\n")
        except Exception as e:
            _err.capture(e, "_segnala_anomalia", {"module": "ChimeraAuditor"})
            self.logger.error(f"Impossibile scrivere su {self.log_file}: {e}")
