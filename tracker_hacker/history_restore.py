from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

import pandas as pd


HISTORY_REQUIRED_COLUMNS = [
    'Tracker', 'id Tracker', 'Modify Date', 'Old Value', 'New Value'
]
HISTORY_FIELD_COLUMNS = ['Field', 'API Field']


@dataclass
class RestoreResult:
    tracker_name: str
    tracker_id: str
    restore_to: pd.Timestamp
    before_row: pd.Series
    restored_row: pd.Series
    applied_changes: List[Dict[str, Any]] = field(default_factory=list)
    skipped_changes: List[Dict[str, Any]] = field(default_factory=list)
    delta: List[Dict[str, Any]] = field(default_factory=list)
    history_rows_used: int = 0


@dataclass
class HistoryStateOption:
    tracker_name: str
    tracker_id: str
    restore_to: pd.Timestamp
    fields_changed: List[str]
    changes: List[Dict[str, Any]] = field(default_factory=list)
    history_row_indices: List[int] = field(default_factory=list)


def _normalize_tracker_name(val: Any) -> str:
    return str(val).strip() if val is not None else ''


def _parse_timestamp(dt_value: Any) -> Optional[pd.Timestamp]:
    if isinstance(dt_value, pd.Timestamp):
        return dt_value
    try:
        ts = pd.to_datetime(dt_value, errors='coerce', dayfirst=True)
        if pd.isna(ts):
            return None
        if isinstance(ts, pd.Timestamp) and ts.tzinfo:
            return ts.tz_convert(None)
        return ts
    except Exception:
        return None


def _get_field_name(row: pd.Series) -> Optional[str]:
    for col_name in HISTORY_FIELD_COLUMNS:
        field_val = row.get(col_name)
        if pd.notna(field_val) and str(field_val).strip():
            return str(field_val).strip()
    return None


def _is_ignored_field(field_name: Optional[str]) -> bool:
    if field_name is None:
        return False
    normalized = str(field_name).strip().lower()
    return normalized in {"label map", "resize map"}


def _format_value(val: Any) -> str:
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return '(empty)'
    return str(val)


def _row_delta(before: pd.Series, after: pd.Series) -> List[Dict[str, Any]]:
    deltas: List[Dict[str, Any]] = []
    for col in before.index:
        before_val = before.get(col)
        after_val = after.get(col)
        values_equal = (pd.isna(before_val) and pd.isna(after_val)) or (before_val == after_val)
        if values_equal:
            continue
        deltas.append({'column': col, 'before': before_val, 'after': after_val})
    return deltas


def validate_history_dataframe(history_df: pd.DataFrame) -> List[str]:
    missing_columns = [col for col in HISTORY_REQUIRED_COLUMNS if col not in history_df.columns]
    return missing_columns


def get_history_tracker_names(history_df: pd.DataFrame) -> List[str]:
    history_df = history_df.copy()

    if 'Tracker' not in history_df.columns and 'Tracker Name' in history_df.columns:
        history_df['Tracker'] = history_df['Tracker Name']

    if 'Tracker' not in history_df.columns:
        return []

    history_df['Tracker'] = history_df['Tracker'].apply(_normalize_tracker_name)
    tracker_names = [name for name in history_df['Tracker'] if name]
    return sorted(set(tracker_names))


def get_history_changes_for_timestamp(history_df: pd.DataFrame, tracker_name: str, restore_to: Any) -> List[Dict[str, Any]]:
    history_df = history_df.copy()

    if 'Tracker' not in history_df.columns and 'Tracker Name' in history_df.columns:
        history_df['Tracker'] = history_df['Tracker Name']
    if 'id Tracker' not in history_df.columns and 'Tracker Name Id' in history_df.columns:
        history_df['id Tracker'] = history_df['Tracker Name Id']

    missing_history_cols = validate_history_dataframe(history_df)
    if missing_history_cols:
        raise ValueError(f"History dataframe missing required columns: {missing_history_cols}")

    tracker_name_str = _normalize_tracker_name(tracker_name)
    if not tracker_name_str:
        raise ValueError("A Tracker name must be provided for restore operations.")

    parsed_restore_ts = _parse_timestamp(restore_to)
    if parsed_restore_ts is None:
        raise ValueError("Unable to parse restore timestamp. Please provide a valid date/time.")

    history_df['__parsed_modify_date'] = history_df['Modify Date'].apply(_parse_timestamp)
    history_df['Tracker'] = history_df['Tracker'].apply(_normalize_tracker_name)

    change_rows = history_df[
        (history_df['Tracker'] == tracker_name_str) &
        (history_df['__parsed_modify_date'] == parsed_restore_ts)
    ]

    changes: List[Dict[str, Any]] = []
    for _, row in change_rows.iterrows():
        field_name = _get_field_name(row) or 'Unknown field'
        if _is_ignored_field(field_name):
            continue

        changes.append({
            'field': field_name,
            'old_value': row.get('Old Value'),
            'new_value': row.get('New Value'),
            'modified_by': row.get('Modified By') or row.get('Last Modified By Name'),
            'recorded_at': row.get('__parsed_modify_date') or row.get('Modify Date'),
        })

    return changes


def build_history_state_options(history_df: pd.DataFrame, tracker_name: str) -> List[HistoryStateOption]:
    history_df = history_df.copy()

    if 'Tracker' not in history_df.columns and 'Tracker Name' in history_df.columns:
        history_df['Tracker'] = history_df['Tracker Name']
    if 'id Tracker' not in history_df.columns and 'Tracker Name Id' in history_df.columns:
        history_df['id Tracker'] = history_df['Tracker Name Id']

    missing_cols = validate_history_dataframe(history_df)
    if missing_cols:
        raise ValueError(f"History dataframe missing required columns: {missing_cols}")

    tracker_name_str = _normalize_tracker_name(tracker_name)
    if not tracker_name_str:
        raise ValueError("A Tracker name must be provided for restore operations.")

    history_df['Tracker'] = history_df['Tracker'].apply(_normalize_tracker_name)
    history_df['__parsed_modify_date'] = history_df['Modify Date'].apply(_parse_timestamp)

    tracker_history = history_df[
        (history_df['Tracker'] == tracker_name_str) &
        history_df['__parsed_modify_date'].notna()
    ].copy()

    if tracker_history.empty:
        return []

    tracker_history['__field_name'] = tracker_history.apply(_get_field_name, axis=1)
    tracker_history = tracker_history[~tracker_history['__field_name'].apply(_is_ignored_field)].copy()

    if tracker_history.empty:
        return []

    grouped = tracker_history.groupby('__parsed_modify_date')
    options: List[HistoryStateOption] = []
    for modify_ts, group in grouped:
        fields_changed: List[str] = []
        changes: List[Dict[str, Any]] = []
        for _, row in group.iterrows():
            field_name = row.get('__field_name') or _get_field_name(row) or 'Unknown field'
            if field_name not in fields_changed:
                fields_changed.append(field_name)
            changes.append({
                'field': field_name,
                'old_value': row.get('Old Value'),
                'new_value': row.get('New Value'),
                'modified_by': row.get('Modified By') or row.get('Last Modified By Name'),
                'recorded_at': row.get('__parsed_modify_date') or row.get('Modify Date'),
            })
        options.append(
            HistoryStateOption(
                tracker_name=tracker_name_str,
                tracker_id=_normalize_tracker_name(group['id Tracker'].iloc[0]),
                restore_to=modify_ts,
                fields_changed=fields_changed,
                changes=changes,
                history_row_indices=list(group.index),
            )
        )

    options.sort(key=lambda opt: opt.restore_to, reverse=True)
    return options


def restore_tracker_state(current_df: pd.DataFrame, history_df: pd.DataFrame, tracker_name: str,
                          restore_to: Any) -> RestoreResult:
    if current_df is None:
        raise ValueError("Current tracker data is not loaded.")

    if 'Tracker' not in current_df.columns:
        if 'Tracker Name' in current_df.columns:
            current_df = current_df.copy()
            current_df['Tracker'] = current_df['Tracker Name']
        else:
            raise ValueError("Current tracker data is missing required 'Tracker' column.")

    history_df = history_df.copy()
    if 'Tracker' not in history_df.columns and 'Tracker Name' in history_df.columns:
        history_df['Tracker'] = history_df['Tracker Name']
    if 'id Tracker' not in history_df.columns and 'Tracker Name Id' in history_df.columns:
        history_df['id Tracker'] = history_df['Tracker Name Id']

    missing_history_cols = validate_history_dataframe(history_df)
    if missing_history_cols:
        raise ValueError(f"History dataframe missing required columns: {missing_history_cols}")

    tracker_name_str = _normalize_tracker_name(tracker_name)
    if not tracker_name_str:
        raise ValueError("A Tracker name must be provided for restore operations.")

    parsed_restore_ts = _parse_timestamp(restore_to)
    if parsed_restore_ts is None:
        raise ValueError("Unable to parse restore timestamp. Please provide a valid date/time.")

    tracker_rows = current_df[current_df['Tracker'].astype(str).apply(_normalize_tracker_name) == tracker_name_str]
    if tracker_rows.empty:
        raise ValueError(f"Tracker '{tracker_name_str}' not found in current dataset.")

    base_row = tracker_rows.iloc[0].copy()
    tracker_id_value = _normalize_tracker_name(base_row.get('id Tracker'))

    history_df['__parsed_modify_date'] = history_df['Modify Date'].apply(_parse_timestamp)
    history_df['Tracker'] = history_df['Tracker'].apply(_normalize_tracker_name)
    history_df['__field_name'] = history_df.apply(_get_field_name, axis=1)
    history_df['__is_ignored_field'] = history_df['__field_name'].apply(_is_ignored_field)

    relevant_history = history_df[
        (history_df['Tracker'] == tracker_name_str) &
        history_df['__parsed_modify_date'].notna() &
        (history_df['__parsed_modify_date'] >= parsed_restore_ts) &
        (~history_df['__is_ignored_field'])
    ].sort_values('__parsed_modify_date', ascending=False)

    applied_changes: List[Dict[str, Any]] = []
    skipped_changes: List[Dict[str, Any]] = []

    working_row = base_row.copy()

    for _, hist_row in relevant_history.iterrows():
        field_name = _get_field_name(hist_row)
        if not field_name:
            skipped_changes.append({
                'reason': 'Missing field column in history row',
                'row_data': hist_row.to_dict()
            })
            continue
        if field_name not in working_row.index:
            skipped_changes.append({
                'reason': f"Field '{field_name}' not present in tracker dataset",
                'row_data': hist_row.to_dict()
            })
            continue

        previous_value = working_row[field_name]
        restored_value = hist_row.get('Old Value')
        working_row[field_name] = restored_value

        applied_changes.append({
            'field': field_name,
            'change_recorded_at': hist_row['__parsed_modify_date'],
            'modified_by': hist_row.get('Modified By') or hist_row.get('Last Modified By Name'),
            'current_value': previous_value,
            'history_new_value': hist_row.get('New Value'),
            'restored_value': restored_value,
        })

    delta = _row_delta(base_row, working_row)

    return RestoreResult(
        tracker_name=tracker_name_str,
        tracker_id=tracker_id_value,
        restore_to=parsed_restore_ts,
        before_row=base_row,
        restored_row=working_row,
        applied_changes=applied_changes,
        skipped_changes=skipped_changes,
        delta=delta,
        history_rows_used=len(relevant_history)
    )


def write_restore_report(result: RestoreResult, output_dir: Path, filename_prefix: Optional[str] = None) -> Dict[str, Path]:
    output_dir.mkdir(parents=True, exist_ok=True)
    timestamp_suffix = datetime.now().strftime('%Y%m%d_%H%M%S')
    prefix_tracker = result.tracker_name or result.tracker_id
    prefix = filename_prefix or f"restore_{prefix_tracker}_{timestamp_suffix}"

    summary_lines = [
        f"Tracker: {result.tracker_name}",
        f"Tracker ID: {result.tracker_id}",
        f"Restore to: {result.restore_to}",
        f"History rows applied: {result.history_rows_used}",
        f"Fields touched: {', '.join(sorted({c['field'] for c in result.applied_changes}) or ['None'])}",
        "",
        "Applied changes (most recent first):",
    ]

    if result.applied_changes:
        for change in result.applied_changes:
            summary_lines.append(
                f"- {change['field']}: {_format_value(change['current_value'])} -> {_format_value(change['restored_value'])}"
                f" (recorded at {change['change_recorded_at']}, by {change.get('modified_by') or 'unknown'})"
            )
    else:
        summary_lines.append("- None (target time is at or after last change)")

    if result.applied_changes:
        summary_lines.append("\nDetailed applied change breakdown:")
        summary_lines.append("Field | Current value | Restored value | History new value | Recorded at/by")
        summary_lines.append("----- | ------------- | -------------- | ----------------- | --------------")
        for change in result.applied_changes:
            summary_lines.append(
                " | ".join([
                    change['field'],
                    _format_value(change['current_value']),
                    _format_value(change['restored_value']),
                    _format_value(change.get('history_new_value')),
                    f"{change['change_recorded_at']} by {change.get('modified_by') or 'unknown'}",
                ])
            )

    if result.delta:
        summary_lines.append("\nBefore vs. restored snapshot:")
        for diff in result.delta:
            summary_lines.append(
                f"* {diff['column']}: '{_format_value(diff['before'])}' -> '{_format_value(diff['after'])}'"
            )
    else:
        summary_lines.append("\nNo differences between current row and restored snapshot.")

    if result.skipped_changes:
        summary_lines.append("\nSkipped history rows:")
        for skip in result.skipped_changes:
            reason = skip.get('reason', 'Unknown reason')
            summary_lines.append(f"- {reason}")

    summary_path = output_dir / f"{prefix}_summary.txt"
    summary_path.write_text('\n'.join(summary_lines))

    restored_row_path = output_dir / f"{prefix}_restored_row.csv"
    result.restored_row.to_frame().T.to_csv(restored_row_path, index=False)

    return {'summary': summary_path, 'restored_row': restored_row_path}
