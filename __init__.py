# -*- coding: utf-8 -*-
"""
CHIMERA v4.0 - Core Trading Modules
"""

__version__ = "4.0.0"
__author__ = "L&A Institutional Trading"

# Import principali
from .engine_la import EngineLA
from .brain_la import BrainLA
from .trade_manager import TradeManager
from .performer_la import PerformerLA
from .telegram_alerts_la import TelegramAlerts
from .feedback_engine import FeedbackEngine
from .macro_sentiment import MacroSentiment

__all__ = [
    'EngineLA',
    'BrainLA',
    'TradeManager',
    'PerformerLA',
    'TelegramAlerts',
    'FeedbackEngine',
    'MacroSentiment',
]