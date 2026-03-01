# -*- coding: utf-8 -*-
"""
L&A Institutional Bot - Backtest Strategy
Esegue backtesting sulle strategie operative su dati storici.
Versione razionalizzata e pronta all'uso.
"""

import logging
import pandas as pd
from core.engine_la import EngineLA
from core.brain_la import BrainLA
from core.config_la import get_risk_settings

def backtest(asset, dati, brain_params=None):
    """
    Esegue backtest singolo su una serie di dati.
    Args:
        asset (str): nome asset
        dati (pd.DataFrame): serie OHLCV
        brain_params (dict): parametri Brain (opzionale)
    Returns:
        dict: risultati, trade log, win/loss
    """
    settings = get_risk_settings()
    engine = EngineLA()
    brain = BrainLA(**(brain_params or {}))
    equity = 10000
    trades = []
    for i in range(len(dati)):
        row = dati.iloc[i]
        dati_engine = {}  # Estrarre indicatori da row
        macro_sentiment = "NEUTRAL"
        decision = brain.calcola_voto(dati_engine, macro_sentiment)
        if decision["direzione"] != "FLAT":
            # Simulazione trade base
            pnl = row["close"] * settings["risk_per_trade"] * (1 if decision["direzione"] == "BUY" else -1)
            equity += pnl
            trades.append({"asset": asset, "time": row.name, "pnl": pnl, "voto": decision["voto"]})
    wins = sum(1 for t in trades if t["pnl"] > 0)
    losses = sum(1 for t in trades if t["pnl"] <= 0)
    return {
        "asset": asset,
        "trades": trades,
        "equity_final": equity,
        "win_rate": (wins/(wins+losses)*100) if (wins+losses) else 0
    }

def main():
    logging.basicConfig(level=logging.INFO)
    asset = "BTC"
    engine = EngineLA()
    # Fetch dati (esempio solo daily)
    dati = engine.get_dati_multitimeframe(asset)["1d"]
    results = backtest(asset, dati)
    print(f"Risultati Backtest {asset}:")
    print(f"Equity finale: {results['equity_final']:.2f}")
    print(f"Win rate: {results['win_rate']:.2f}%")
    print(f"Totale trade: {len(results['trades'])}")

if __name__ == "__main__":
    main()