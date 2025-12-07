import re
import sys
import types
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def _install_stub_modules() -> None:
    if "pandas" not in sys.modules:
        pandas_stub = types.ModuleType("pandas")
        pandas_stub.Timestamp = type("Timestamp", (), {})
        pandas_stub.DataFrame = type("DataFrame", (), {})
        pandas_stub.Series = type("Series", (), {})
        pandas_stub.to_datetime = lambda *args, **kwargs: None
        pandas_stub.isna = lambda val: val != val
        pandas_stub.errors = types.SimpleNamespace(SettingWithCopyWarning=RuntimeWarning)
        sys.modules["pandas"] = pandas_stub

    if "questionary" not in sys.modules:
        questionary_stub = types.ModuleType("questionary")
        questionary_stub.Choice = type("Choice", (), {})
        questionary_stub.select = lambda *args, **kwargs: None
        questionary_stub.path = lambda *args, **kwargs: None
        questionary_stub.autocomplete = lambda *args, **kwargs: None
        questionary_stub.text = lambda *args, **kwargs: None
        questionary_stub.confirm = lambda *args, **kwargs: None
        sys.modules["questionary"] = questionary_stub


_install_stub_modules()

from tracker_hacker.cli import _summarize_history_changes


def _strip_colors(text: str) -> str:
    return re.sub(r"\x1b\[[0-9;]*m", "", text)


def test_fields_and_query_changes_render_api_only():
    changes = [
        {
            "field": "Fields",
            "old_value": "old_field__c, shared__c",
            "new_value": "old_field__c, shared__c, new_field__c",
        },
        {
            "field": "Query",
            "old_value": "SELECT shared__c, old_field__c FROM Obj WHERE old_field__c = 'x'",
            "new_value": "SELECT shared__c, new_field__c FROM Obj WHERE shared__c = 'y'",
        },
    ]

    summary = _strip_colors(_summarize_history_changes(changes))

    assert "Fields added: new_field__c" in summary
    assert "Fields removed: " not in summary
    assert "Query added: new_field__c" in summary
    assert "Query removed: old_field__c" in summary
    assert "shared__c" not in summary.split("|")[0]


def test_other_changes_keep_descriptions():
    changes = [
        {"field": "Logic", "old_value": "1 AND 2", "new_value": "(1 AND 2) OR 3"}
    ]

    summary = _strip_colors(_summarize_history_changes(changes))

    assert "Logic:" in summary
    assert "1 AND 2" in summary


def test_summary_wraps_long_lines():
    repeated_phrase = " more detail" * 20
    changes = [
        {
            "field": "Description",
            "old_value": "Initial summary",
            "new_value": f"Initial summary with{repeated_phrase}",
        }
    ]

    summary = _strip_colors(_summarize_history_changes(changes))

    assert "\n" in summary
