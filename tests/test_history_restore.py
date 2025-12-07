import pytest

pd = pytest.importorskip("pandas")

from tracker_hacker.history_restore import _parse_timestamp, build_history_state_options


def test_parse_timestamp_uses_day_first_format():
    parsed = _parse_timestamp("02/12/2023 15:30")

    assert parsed is not None
    assert parsed.day == 2
    assert parsed.month == 12


def test_resize_map_rows_are_excluded_from_state_options():
    history_df = pd.DataFrame(
        [
            {
                "Tracker": "Example",
                "id Tracker": "1",
                "Modify Date": "01/12/2023",
                "Field": "Resize Map",
                "Old Value": "old",
                "New Value": "new",
            },
            {
                "Tracker": "Example",
                "id Tracker": "1",
                "Modify Date": "01/12/2023",
                "Field": "Status",
                "Old Value": "Pending",
                "New Value": "Approved",
            },
        ]
    )

    options = build_history_state_options(history_df, "Example")

    assert len(options) == 1
    assert options[0].fields_changed == ["Status"]
    assert all(change["field"] == "Status" for change in options[0].changes)
