import importlib
import pytest

MODULES = [
    "bot_la",                    # dalla root
    "core.asset_list",
    "core.brain_la",
    "core.engine_la",
    "core.feedback_engine",
    "core.institutional_filters",
    "core.macro_sentiment",
    "core.performer_la",
    "core.trade_manager",
    "core.telegram_alerts_la",
]

@pytest.mark.parametrize("module_name", MODULES)
def test_import_module(module_name):
    """Controlla che ogni file Python principale possa essere importato senza errori."""
    try:
        importlib.import_module(module_name)
    except ImportError as e:
        pytest.fail(f"Import del modulo '{module_name}' fallito: {e}")