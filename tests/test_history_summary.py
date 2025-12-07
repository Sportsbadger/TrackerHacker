import re
import sys
import types
import pytest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def _install_stub_modules() -> None:
    if "pandas" not in sys.modules:
        try:
            import pandas  # type: ignore  # noqa: F401
        except ModuleNotFoundError:
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
from tracker_hacker.history_restore import HistoryStateOption, build_history_state_options


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

    lines = summary.splitlines()

    assert any(line.startswith("- Fields added: new_field__c") for line in lines)
    assert not any(line.startswith("- Fields removed:") for line in lines)
    assert any(line.startswith("- Query added: new_field__c") for line in lines)
    assert any(line.startswith("- Query removed: old_field__c") for line in lines)


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

    summary = _strip_colors(_summarize_history_changes(changes, wrap_width=80))

    assert "\n" in summary


def test_long_field_lists_collapse_until_expanded():
    added = ", ".join(f"field_{idx}__c" for idx in range(7))
    changes = [
        {
            "field": "Fields",
            "old_value": "",
            "new_value": added,
        },
    ]

    collapsed = _strip_colors(_summarize_history_changes(changes))
    assert "7 fields" in collapsed
    assert "field_0__c" not in collapsed

    expanded = _strip_colors(_summarize_history_changes(changes, expanded=True))
    assert "field_0__c" in expanded and "field_6__c" in expanded


def test_query_tokens_parsed_with_brackets():
    changes = [
        {
            "field": "Query",
            "old_value": "SELECT [old_field__c] FROM Obj",
            "new_value": "SELECT [new_field__c], [shared__c] FROM Obj",
        }
    ]

    summary = _strip_colors(_summarize_history_changes(changes))

    assert "Query added: new_field__c" in summary
    assert "Query removed: old_field__c" in summary
    assert "SELECT [old_field__c]" not in summary


def test_expanded_field_changes_show_details_when_tokens_missing():
    changes = [
        {
            "field": "Fields",
            "old_value": "alpha__c, beta__c",
            "new_value": "beta__c, alpha__c",
        }
    ]

    summary = _strip_colors(_summarize_history_changes(changes, expanded=True))
    assert summary == "No change details recorded"


def test_duplicate_field_change_messages_collapsed():
    changes = [
        {"field": "Fields", "old_value": "alpha__c, beta__c", "new_value": "beta__c, alpha__c"},
        {"field": "Fields", "old_value": "one__c, two__c", "new_value": "two__c, one__c"},
    ]

    summary = _strip_colors(_summarize_history_changes(changes))

    assert summary == "No change details recorded"


def test_expanded_query_changes_show_tokens_without_snippets():
    changes = [
        {
            "field": "Query",
            "old_value": "SELECT one__c, two__c FROM Obj WHERE Status = 'Open'",
            "new_value": "SELECT one__c, two__c FROM Obj WHERE Status = 'Closed'",
        }
    ]

    summary = _strip_colors(_summarize_history_changes(changes, expanded=True))
    assert summary.startswith("- Query: SELECT, one__c, two__c, FROM, Obj, WHERE, Status")


def test_expanded_summary_inserts_spacing_between_entries():
    changes = [
        {"field": "Fields", "old_value": "", "new_value": "alpha__c"},
        {"field": "Query", "old_value": "SELECT alpha__c FROM Obj", "new_value": "SELECT beta__c FROM Obj"},
    ]

    summary = _strip_colors(_summarize_history_changes(changes, expanded=True))

    entries = summary.split("\n\n")

    assert any(line.startswith("- Fields added") for line in entries[0].splitlines())
    assert any(line.startswith("- Query added") for line in entries[1].splitlines())


def test_expanded_summary_omits_non_field_details_when_present():
    changes = [
        {"field": "Fields", "old_value": "", "new_value": "alpha__c"},
        {"field": "Logic", "old_value": "1 AND 2", "new_value": "(1 AND 2) OR 3"},
    ]

    summary = _strip_colors(_summarize_history_changes(changes, expanded=True))

    assert "Logic:" not in summary
    assert summary.startswith("- Fields added: alpha__c")


def test_expanded_summary_surfaces_added_and_removed_tokens():
    changes = [
        {
            "field": "Fields",
            "old_value": "alpha__c, beta__c",
            "new_value": "beta__c, gamma__c",
        },
        {
            "field": "Query",
            "old_value": "SELECT alpha__c FROM Obj",
            "new_value": "SELECT beta__c, gamma__c FROM Obj",
        },
    ]

    summary = _strip_colors(_summarize_history_changes(changes, expanded=True))

    assert "Fields added: gamma__c" in summary
    assert "Fields removed: alpha__c" in summary
    assert "Query added: beta__c, gamma__c" in summary
    assert "Query removed: alpha__c" in summary


def test_reordered_field_list_is_ignored_in_collapsed_summary():
    changes = [
        {
            "field": "Fields",
            "old_value": "alpha__c, beta__c, gamma__c",
            "new_value": "gamma__c, alpha__c, beta__c",
        }
    ]

    summary = _strip_colors(_summarize_history_changes(changes))

    assert summary == "No change details recorded"


def test_reordered_query_list_is_included_in_summary():
    changes = [
        {
            "field": "Query",
            "old_value": "SELECT alpha__c, beta__c FROM Obj",
            "new_value": "SELECT beta__c, alpha__c FROM Obj",
        }
    ]

    summary = _strip_colors(_summarize_history_changes(changes))

    assert "Query: values changed" in summary


def test_history_state_options_drop_reorder_and_resize_map():
    import pandas as pd

    try:
        history_df = pd.DataFrame(
            [
                {
                    "Tracker": "Test Tracker",
                    "id Tracker": "1",
                    "Modify Date": "2024-07-01",
                    "Field": "Fields",
                    "Old Value": "alpha__c, beta__c",
                    "New Value": "beta__c, alpha__c",
                },
                {
                    "Tracker": "Test Tracker",
                    "id Tracker": "1",
                    "Modify Date": "2024-07-02",
                    "Field": "Fields",
                    "Old Value": "alpha__c, beta__c",
                    "New Value": "alpha__c, beta__c, gamma__c",
                },
                {
                    "Tracker": "Test Tracker",
                    "id Tracker": "1",
                    "Modify Date": "2024-07-02",
                    "Field": "Resize Map",
                    "Old Value": "old resize",
                    "New Value": "new resize",
                },
            ]
        )
    except TypeError:
        pytest.skip("pandas stubbed; real DataFrame required")

    options = build_history_state_options(history_df, "Test Tracker")

    assert len(options) == 1
    assert options[0].fields_changed == ["Fields"]


def test_history_state_options_keep_query_reorder():
    import pandas as pd

    try:
        history_df = pd.DataFrame(
            [
                {
                    "Tracker": "Test Tracker",
                    "id Tracker": "1",
                    "Modify Date": "2024-07-03",
                    "Field": "Query",
                    "Old Value": "SELECT alpha__c, beta__c FROM Obj",
                    "New Value": "SELECT beta__c, alpha__c FROM Obj",
                }
            ]
        )
    except TypeError:
        pytest.skip("pandas stubbed; real DataFrame required")

    options = build_history_state_options(history_df, "Test Tracker")

    assert len(options) == 1
    assert options[0].fields_changed == ["Query"]


def test_choice_titles_align_after_hyphen(monkeypatch):
    from tracker_hacker import cli

    monkeypatch.setattr(
        cli.shutil,
        "get_terminal_size",
        lambda fallback=(120, 20): types.SimpleNamespace(columns=60, lines=20),
    )

    option = HistoryStateOption(
        tracker_name="Test",
        tracker_id="1",
        restore_to="2024-07-01",
        fields_changed=[],
        changes=[{"field": "Description", "old_value": "a", "new_value": "b" * 80}],
    )

    title = _strip_colors(cli._format_history_choice_title(option))
    lines = title.splitlines()

    assert len(lines) > 1
    prefix = f"{option.restore_to} - "
    expected_indent = " " * (len(prefix) + cli.CHOICE_POINTER_PADDING)
    assert lines[0].startswith(prefix)
    assert "- -" not in lines[0]
    assert lines[1].startswith(expected_indent)
