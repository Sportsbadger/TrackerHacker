import re
import shutil
import warnings
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path
from textwrap import indent

import pandas as pd
import questionary
from questionary import Choice

from tracker_hacker import state
from tracker_hacker.audit import master_audit
from tracker_hacker.data_loader import load_source_data_csv
from tracker_hacker.history_restore import (
    build_history_state_options,
    get_history_tracker_names,
    restore_tracker_state,
    write_restore_report,
)
from tracker_hacker.modifications import identify_modifications, load_swap_pairs_csv, modify_trackers
from tracker_hacker.utils import handle_cancel


@contextmanager
def _suppress_restore_warnings():
    with warnings.catch_warnings():
        warnings.filterwarnings("ignore", category=pd.errors.SettingWithCopyWarning)
        warnings.filterwarnings("ignore", category=FutureWarning)
        yield


def _summarize_history_changes(
    changes,
    *,
    collapse_threshold: int = 5,
    expanded: bool = False,
    wrap_width: int | None = None,
    wrap: bool = True,
    bullet_prefix: bool = True,
):
    def _is_empty(value):
        if value is None:
            return True
        if isinstance(value, str):
            return not value.strip()
        try:
            return pd.isna(value)
        except Exception:
            return False

    def _clean_string(value):
        if value is None:
            return ""
        return str(value).strip()

    def _extract_field_tokens(value: object) -> list[str]:
        raw = _clean_string(value)
        if not raw:
            return []

        normalized = raw.replace('[', '').replace(']', '')
        normalized_no_literals = re.sub(r"'[^']*'|\"[^\"]*\"", " ", normalized)
        tokens = re.findall(
            r"[A-Za-z0-9_]+(?:__[cr])?(?:\.[A-Za-z0-9_]+(?:__[cr])?)*",
            normalized_no_literals,
        )
        unique_tokens: list[str] = []

        def _add_unique(token: str) -> None:
            cleaned = token.strip().strip(',').strip('"\'')
            if cleaned and cleaned not in unique_tokens:
                unique_tokens.append(cleaned)

        for token in tokens:
            _add_unique(token)

        if unique_tokens:
            return unique_tokens

        # Fallback: split on common delimiters to capture bracketed or unconventional field names.
        for segment in re.split(r"[\s,;|\n]+", normalized):
            if not segment:
                continue
            if re.fullmatch(r"[A-Za-z0-9_\.]+(?:__[cr])?", segment):
                _add_unique(segment)

        return unique_tokens

    def _diff_field_tokens(old_val: object, new_val: object) -> tuple[list[str], list[str]]:
        old_tokens = _extract_field_tokens(old_val)
        new_tokens = _extract_field_tokens(new_val)
        added = [tok for tok in new_tokens if tok not in old_tokens]
        removed = [tok for tok in old_tokens if tok not in new_tokens]
        return added, removed

    def _window_excerpt(text, center_start, center_end, context=25, max_len=120):
        """Return a short excerpt around the changed region."""
        start = max(center_start - context, 0)
        end = min(center_end + context, len(text))
        excerpt = text[start:end]
        if len(excerpt) > max_len:
            excerpt = excerpt[: max_len // 2 - 2] + " … " + excerpt[-max_len // 2 + 2 :]
        prefix = "…" if start > 0 else ""
        suffix = "…" if end < len(text) else ""
        return f"{prefix}{excerpt}{suffix}" if excerpt else "(empty)"

    def _describe_change(old_val, new_val):
        old_str = _clean_string(old_val)
        new_str = _clean_string(new_val)

        if _is_empty(old_val) and not _is_empty(new_val):
            return f"set to '{_window_excerpt(new_str, 0, len(new_str))}'"
        if not _is_empty(old_val) and _is_empty(new_val):
            return f"cleared from '{_window_excerpt(old_str, 0, len(old_str))}'"

        if old_str == new_str:
            return "no change recorded"

        from difflib import SequenceMatcher

        matcher = SequenceMatcher(None, old_str, new_str)
        diff_chunks = [op for op in matcher.get_opcodes() if op[0] != "equal"]
        if not diff_chunks:
            return f"changed to '{_window_excerpt(new_str, 0, len(new_str))}'"

        # Focus on the first differing span.
        tag, i1, i2, j1, j2 = diff_chunks[0]
        old_excerpt = _window_excerpt(old_str, i1, i2)
        new_excerpt = _window_excerpt(new_str, j1, j2)
        change_label = {"replace": "updated", "delete": "removed", "insert": "added"}.get(tag, "changed")
        return f"{change_label}: '{old_excerpt}' -> '{new_excerpt}'"

    if not changes:
        return "No change details recorded"

    added_fields: list[str] = []
    removed_fields: list[str] = []
    other_changes: list[str] = []
    contextual_field_changes: list[str] = []

    def _append_unique(collection: list[str], value: str) -> None:
        if value and value not in collection:
            collection.append(value)

    for change in changes:
        field_name = str(change.get("field", "Unknown field")).strip() or "Unknown field"
        old_val = change.get("old_value")
        new_val = change.get("new_value")

        field_name_lower = field_name.lower()
        if field_name_lower in {"fields", "query"}:
            added_tokens, removed_tokens = _diff_field_tokens(old_val, new_val)

            def _format_tokens(tokens: list[str], action: str) -> str:
                if expanded or len(tokens) <= collapse_threshold:
                    return f"{field_name} {action}: {', '.join(tokens)}"
                plural = "fields" if len(tokens) != 1 else "field"
                return f"{field_name} {action}: {len(tokens)} {plural} (expand to view)"

            if added_tokens:
                _append_unique(contextual_field_changes, _format_tokens(added_tokens, "added"))
            if removed_tokens:
                _append_unique(contextual_field_changes, _format_tokens(removed_tokens, "removed"))

            no_token_diff = not added_tokens and not removed_tokens and not (
                _is_empty(old_val) and _is_empty(new_val)
            )
            if not no_token_diff:
                continue

            display_tokens = list(
                dict.fromkeys(
                    _extract_field_tokens(old_val) + _extract_field_tokens(new_val)
                )
            )

            if expanded:
                if display_tokens:
                    _append_unique(
                        contextual_field_changes,
                        f"{field_name}: {', '.join(display_tokens)} (values updated)",
                    )
                else:
                    _append_unique(
                        contextual_field_changes,
                        f"{field_name}: values updated",
                    )
            else:
                _append_unique(
                    other_changes,
                    f"{field_name}: values changed (expand to view details)",
                )
            continue

        if _is_empty(old_val) and not _is_empty(new_val):
            added_fields.append(field_name)
            continue
        if not _is_empty(old_val) and _is_empty(new_val):
            removed_fields.append(field_name)
            continue

        description = _describe_change(old_val, new_val)
        other_changes.append(f"{field_name}: {description}")

    summary_lines = []

    def _format_field_list(items: list[str], label: str) -> str:
        unique_items = list(dict.fromkeys(items))
        if expanded or len(unique_items) <= collapse_threshold:
            return f"{label}: {', '.join(unique_items)}"
        plural = "fields" if len(unique_items) != 1 else "field"
        return f"{label}: {len(unique_items)} {plural} (expand to view)"

    if added_fields:
        summary_lines.append(_format_field_list(added_fields, "Fields added"))
    if removed_fields:
        summary_lines.append(_format_field_list(removed_fields, "Fields removed"))
    summary_lines.extend(contextual_field_changes)
    summary_lines.extend(other_changes)

    if expanded:
        field_focus = [
            line for line in summary_lines if line.lower().startswith(("fields", "query"))
        ]
        if field_focus:
            summary_lines = field_focus

    if not summary_lines:
        return "No change details recorded"

    def _wrap_line(text: str) -> str:
        if not wrap:
            return text
        from textwrap import fill

        terminal_width = wrap_width or shutil.get_terminal_size(fallback=(120, 20)).columns
        adjusted_width = max(40, terminal_width)
        return fill(
            text,
            width=adjusted_width,
            break_long_words=False,
            break_on_hyphens=False,
            subsequent_indent="  ",
        )

    line_prefix = "- " if bullet_prefix else ""
    separator = "\n\n" if expanded else "\n"
    formatted_lines = [f"{line_prefix}{_wrap_line(line)}" for line in summary_lines if line]
    return separator.join(formatted_lines)


CHOICE_POINTER_PADDING = 3


def _format_history_choice_title(option):
    restore_label = str(option.restore_to)
    prefix = f"{restore_label} - "
    pointer_adjusted_width = shutil.get_terminal_size(fallback=(120, 20)).columns - CHOICE_POINTER_PADDING
    wrap_width = max(40, pointer_adjusted_width)
    summary = _summarize_history_changes(
        option.changes,
        wrap_width=wrap_width,
        wrap=False,
        bullet_prefix=False,
    )

    from textwrap import wrap

    available_width = max(10, wrap_width - len(prefix))
    wrapped_segments = wrap(
        summary,
        width=available_width,
        break_long_words=False,
        break_on_hyphens=False,
    )

    if not wrapped_segments:
        return prefix.rstrip()

    lines = [f"{prefix}{wrapped_segments[0]}"]
    hanging_indent = " " * (len(prefix) + CHOICE_POINTER_PADDING)
    for segment in wrapped_segments[1:]:
        lines.append(f"{hanging_indent}{segment}")

    return "\n".join(lines)


def _prompt_restore_state_selection(
    state_choices: list[Choice],
    *,
    select_fn=None,
    confirm_fn=None,
    apply_select_fn=None,
):
    select_fn = select_fn or questionary.select
    confirm_fn = confirm_fn or questionary.confirm
    apply_select_fn = apply_select_fn or questionary.select
    operation_flow_control = None

    while operation_flow_control != "return_to_menu":
        try:
            chosen_state_option = select_fn(
                "Select restore state (grouped by Modify Date):",
                choices=state_choices,
            ).ask()
        except KeyboardInterrupt:
            return handle_cancel("Restore state selection interrupted.", return_to_menu=True), None

        if chosen_state_option is None:
            return handle_cancel("Restore state selection cancelled.", return_to_menu=True), None
        if not hasattr(chosen_state_option, "changes"):
            print("Invalid selection. Please choose a restore option.")
            continue

        try:
            show_detail = confirm_fn("Show detailed change list?", default=False).ask()
        except KeyboardInterrupt:
            return handle_cancel("Detailed change prompt interrupted.", return_to_menu=True), None

        if show_detail:
            detailed_summary = _summarize_history_changes(
                chosen_state_option.changes,
                expanded=True,
            )
            print("\nDetailed change summary:")
            print(detailed_summary)
        elif show_detail is None:
            return handle_cancel("Detailed change prompt cancelled.", return_to_menu=True), None

        try:
            apply_choice = apply_select_fn(
                "Apply this restore state?",
                choices=[
                    Choice("Apply restore", "apply"),
                    Choice("Back to history selection", "reselect"),
                    Choice("<Cancel>", "cancel"),
                ],
            ).ask()
        except KeyboardInterrupt:
            return handle_cancel("Apply selection interrupted.", return_to_menu=True), None

        if apply_choice in {None, "cancel"}:
            return handle_cancel("Restore operation cancelled.", return_to_menu=True), None
        if apply_choice == "reselect":
            continue

        return operation_flow_control, chosen_state_option

    return operation_flow_control, None


def main_loop():
    script_dir = Path(__file__).resolve().parent.parent
    output_dir = script_dir / 'outputs'
    output_dir.mkdir(exist_ok=True)

    print("--- Sitetracker Tracker Modifier Tool ---")

    load_result = load_source_data_csv()
    if load_result is not True:
        if load_result == "trigger_exit":
            return
        print("Failed to load initial source data. Exiting program.")
        return

    while True:
        print("\n--- Main Menu ---")
        current_source_file = getattr(state.main_df, '_source_file_name', 'N/A')
        if state.main_df is not None:
            print(f"Currently loaded data: {len(state.main_df)} trackers from '{current_source_file}'.")
        else:
            print("No data currently loaded. Please load source data.")

        main_menu_choices = [Choice("Load New Source Data", value="load_data")]
        if state.main_df is not None:
            main_menu_choices.extend([
                Choice("Remove Fields (from Audit file)", value="remove"),
                Choice("Swap Fields", value="swap"),
                Choice("Add Field", value="add"),
                Choice("Audit Fields", value="audit"),
                Choice("Restore Tracker from History", value="restore"),
            ])
        main_menu_choices.append(Choice("Exit", value="exit"))

        chosen_action = None
        try:
            chosen_action = questionary.select("Select action:", choices=main_menu_choices).ask()
        except KeyboardInterrupt:
            chosen_action = handle_cancel("Action selection interrupted.", return_to_menu=True)

        if chosen_action is None or chosen_action == "exit":
            if handle_cancel("Exiting tool.", trigger_exit=True) == "trigger_exit":
                break
        if chosen_action == "return_to_menu":
            continue
        if chosen_action == "trigger_exit":
            break

        if chosen_action == "load_data":
            load_result = load_source_data_csv()
            if load_result == "trigger_exit":
                break
            continue

        if state.main_df is None:
            print("No data loaded. Please load source data first.")
            continue

        removal_plan_from_audit = {}
        field_swap_map_cmd = {}
        fields_to_add_list_cmd = []
        operation_flow_control = None

        try:
            if chosen_action == "remove":
                try:
                    print("\nSelect an audit CSV file to specify which fields to remove from which trackers.")
                    audit_csv_path_str = questionary.path("Path to Audit CSV (or directory):", default=str(output_dir)).ask()
                    if audit_csv_path_str is None:
                        operation_flow_control = handle_cancel("Removal from audit file cancelled.", return_to_menu=True)
                    else:
                        audit_path_obj = Path(audit_csv_path_str)
                        selected_audit_file = None
                        if audit_path_obj.is_dir():
                            audit_files = sorted([f for f in audit_path_obj.glob("*.csv") if f.is_file()])
                            if not audit_files:
                                print(f"No audit CSVs found in {audit_path_obj}.")
                                operation_flow_control = "return_to_menu"
                            else:
                                audit_choices = [Choice(title=f.name, value=f) for f in audit_files]
                                audit_choices.insert(0, Choice("<Cancel>", None))
                                selected_audit_file = questionary.select("Select audit file:", choices=audit_choices).ask()
                                if selected_audit_file is None:
                                    operation_flow_control = handle_cancel("Audit file selection cancelled.", return_to_menu=True)
                        elif audit_path_obj.is_file() and audit_path_obj.suffix.lower() == ".csv":
                            selected_audit_file = audit_path_obj
                        else:
                            print("Invalid path for audit CSV.")
                            operation_flow_control = "return_to_menu"

                        if selected_audit_file and operation_flow_control != "return_to_menu":
                            print(f"Parsing audit file: {selected_audit_file.name}...")
                            audit_df = pd.read_csv(selected_audit_file, dtype={'Tracker Name Id': str})
                            if 'Tracker Name Id' not in audit_df.columns:
                                print("Audit file is missing the required 'Tracker Name Id' column.")
                                operation_flow_control = "return_to_menu"
                            else:
                                for _, audit_row in audit_df.iterrows():
                                    tracker_id = audit_row['Tracker Name Id']
                                    if pd.isna(tracker_id):
                                        continue
                                    fields_to_remove_for_this_tracker = []
                                    for col_header in audit_df.columns:
                                        match = re.match(r".* \(as (.*)\) - Columns$", col_header)
                                        if match and pd.notna(audit_row[col_header]):
                                            contextual_path = match.group(1)
                                            fields_to_remove_for_this_tracker.append(contextual_path)
                                    if fields_to_remove_for_this_tracker:
                                        removal_plan_from_audit[tracker_id] = sorted(list(set(fields_to_remove_for_this_tracker)))
                                if not removal_plan_from_audit:
                                    print("No removal actions could be parsed from the audit file.")
                                    operation_flow_control = "return_to_menu"
                except KeyboardInterrupt:
                    operation_flow_control = handle_cancel("Field removal input interrupted.", return_to_menu=True)

            elif chosen_action == "swap":
                try:
                    swap_input_method_result = questionary.select(
                        "How to provide swap pairs?",
                        choices=[Choice("Manually enter pairs", "manual"), Choice("Load from CSV", "csv"), Choice("Cancel", "cancel_swap")]
                    ).ask()
                    if swap_input_method_result is None or swap_input_method_result == "cancel_swap":
                        operation_flow_control = handle_cancel("Swap operation cancelled.", return_to_menu=True)
                    else:
                        swap_input_method = swap_input_method_result
                        if swap_input_method == "manual":
                            print("\nEnter 'OldFullFieldAPI,NewFullFieldAPI' (these should be full contextual paths). Blank to end.")
                            while True:
                                try:
                                    manual_pair_str = questionary.text("Pair (blank to end):").ask()
                                except KeyboardInterrupt:
                                    manual_pair_str = handle_cancel("Swap pair entry interrupted.", return_to_menu=True)
                                    break
                                if manual_pair_str == "return_to_menu":
                                    operation_flow_control = "return_to_menu"
                                    break
                                if manual_pair_str is None:
                                    operation_flow_control = handle_cancel("Swap pair entry cancelled.", return_to_menu=True)
                                    break
                                if not manual_pair_str.strip():
                                    break
                                try:
                                    old_f, new_f = [x.strip() for x in manual_pair_str.split(',')]
                                    if old_f and new_f:
                                        if old_f in field_swap_map_cmd:
                                            print(f"Warning: Overwriting mapping for '{old_f}'. New: '{new_f}'.")
                                        field_swap_map_cmd[old_f] = new_f
                                    else:
                                        print("Invalid: Both old and new fields required.")
                                except ValueError:
                                    print("Invalid format. Use 'Old,New'.")
                            if operation_flow_control == "return_to_menu":
                                break
                        elif swap_input_method == "csv":
                            try:
                                swap_csv_path_str = questionary.path("Path to swap pairs CSV (or directory):", default=str(script_dir)).ask()
                            except KeyboardInterrupt:
                                swap_csv_path_str = handle_cancel("Swap CSV selection interrupted.", return_to_menu=True)
                            if swap_csv_path_str == "return_to_menu":
                                operation_flow_control = "return_to_menu"
                            elif swap_csv_path_str is None:
                                operation_flow_control = handle_cancel("Swap CSV selection cancelled.", return_to_menu=True)
                            else:
                                swap_csv_path_obj = Path(swap_csv_path_str)
                                selected_swap_csv_file_path = None
                                if swap_csv_path_obj.is_dir():
                                    swap_csv_files = sorted([f for f in swap_csv_path_obj.glob("*.csv") if f.is_file()])
                                    if not swap_csv_files:
                                        print(f"No CSVs in {swap_csv_path_obj}.")
                                        operation_flow_control = "return_to_menu"
                                    elif len(swap_csv_files) == 1:
                                        selected_swap_csv_file_path = swap_csv_files[0]
                                        print(f"Auto-selected swap CSV: {selected_swap_csv_file_path.name}")
                                    else:
                                        swap_file_choices = [Choice(title=c.name, value=c) for c in swap_csv_files]
                                        swap_file_choices.insert(0, Choice(title="<Cancel selection>", value=None))
                                        try:
                                            selected_swap_obj = questionary.select("Select swap CSV:", choices=swap_file_choices).ask()
                                        except KeyboardInterrupt:
                                            selected_swap_obj = handle_cancel("Swap CSV list selection interrupted.", return_to_menu=True)
                                        if selected_swap_obj == "return_to_menu":
                                            operation_flow_control = "return_to_menu"
                                        elif selected_swap_obj is None:
                                            operation_flow_control = handle_cancel("Swap CSV selection from directory cancelled.", return_to_menu=True)
                                        else:
                                            selected_swap_csv_file_path = selected_swap_obj
                                elif swap_csv_path_obj.is_file() and swap_csv_path_obj.suffix.lower() == ".csv":
                                    selected_swap_csv_file_path = swap_csv_path_obj
                                else:
                                    print(f"Invalid swap CSV path: {swap_csv_path_obj}")
                                    operation_flow_control = "return_to_menu"
                                if selected_swap_csv_file_path and operation_flow_control != "return_to_menu":
                                    loaded_map = load_swap_pairs_csv(selected_swap_csv_file_path)
                                    if loaded_map:
                                        field_swap_map_cmd = loaded_map
                                    else:
                                        print(f"Failed to load swap pairs from {selected_swap_csv_file_path.name}.")
                                        operation_flow_control = "return_to_menu"
                                elif operation_flow_control != "return_to_menu" and selected_swap_csv_file_path is None:
                                    operation_flow_control = "return_to_menu"
                        if not field_swap_map_cmd and operation_flow_control is None:
                            print("No swap pairs defined.")
                            operation_flow_control = "return_to_menu"
                except KeyboardInterrupt:
                    operation_flow_control = handle_cancel("Swap setup interrupted.", return_to_menu=True)
            elif chosen_action == "add":
                try:
                    add_fields_str = questionary.text("Fields to add (comma-separated canonical API names):").ask()
                    if add_fields_str is None:
                        operation_flow_control = handle_cancel("Field addition input cancelled.", return_to_menu=True)
                    else:
                        fields_to_add_list_cmd = [f.strip() for f in add_fields_str.split(',') if f.strip()]
                        if not fields_to_add_list_cmd:
                            print("No fields specified for addition.")
                            operation_flow_control = "return_to_menu"
                except KeyboardInterrupt:
                    operation_flow_control = handle_cancel("Field addition input interrupted.", return_to_menu=True)
            elif chosen_action == "restore":
                try:
                    history_path_str = questionary.path(
                        "Path to tracker history CSV (or directory):",
                        default=str(script_dir)
                    ).ask()
                    if history_path_str is None:
                        operation_flow_control = handle_cancel("Restore operation cancelled.", return_to_menu=True)
                    else:
                        history_path_obj = Path(history_path_str)
                        selected_history_csv = None
                        if history_path_obj.is_dir():
                            history_csvs = sorted([f for f in history_path_obj.glob("*.csv") if f.is_file()])
                            if not history_csvs:
                                print(f"No CSV files found in {history_path_obj}.")
                                operation_flow_control = "return_to_menu"
                            elif len(history_csvs) == 1:
                                selected_history_csv = history_csvs[0]
                                print(f"Automatically selected history CSV: {selected_history_csv.name}")
                            else:
                                history_choices = [Choice(title=f.name, value=f) for f in history_csvs]
                                history_choices.insert(0, Choice(title="<Cancel>", value=None))
                                selected_history_csv = questionary.select(
                                    "Select history CSV file:", choices=history_choices
                                ).ask()
                                if selected_history_csv is None:
                                    operation_flow_control = handle_cancel("History CSV selection cancelled.", return_to_menu=True)
                        elif history_path_obj.is_file() and history_path_obj.suffix.lower() == ".csv":
                            selected_history_csv = history_path_obj
                        else:
                            print(f"Invalid path for history CSV: {history_path_obj}")
                            operation_flow_control = "return_to_menu"

                        if selected_history_csv and operation_flow_control != "return_to_menu":
                            with _suppress_restore_warnings():
                                try:
                                    history_df = pd.read_csv(selected_history_csv)
                                except Exception as exc:
                                    print(f"Failed to load history CSV '{selected_history_csv}': {exc}")
                                    operation_flow_control = "return_to_menu"
                                else:
                                    tracker_name_choices = get_history_tracker_names(history_df)
                                    if not tracker_name_choices:
                                        print("History CSV is missing tracker names in the 'Tracker' column.")
                                        operation_flow_control = "return_to_menu"
                                    else:
                                        tracker_name_default = tracker_name_choices[0] if tracker_name_choices else ""
                                        tracker_name_selected = questionary.autocomplete(
                                            "Tracker to restore:",
                                            choices=tracker_name_choices,
                                            default=tracker_name_default
                                        ).ask()
                                        if tracker_name_selected is None:
                                            operation_flow_control = handle_cancel("Tracker selection cancelled.", return_to_menu=True)
                                        else:
                                            try:
                                                history_state_options = build_history_state_options(history_df, tracker_name_selected)
                                            except ValueError as exc:
                                                print(f"Restore failed: {exc}")
                                                operation_flow_control = "return_to_menu"
                                            else:
                                                if not history_state_options:
                                                    print("No history states available for this tracker.")
                                                    operation_flow_control = "return_to_menu"
                                                else:
                                                    state_choices = []
                                                    for opt in history_state_options:
                                                        formatted_title = _format_history_choice_title(opt)
                                                        state_choices.append(
                                                            Choice(title=formatted_title, value=opt)
                                                        )
                                                    state_choices.insert(0, Choice(title="<Cancel>", value=None))

                                                    operation_flow_control, chosen_state_option = _prompt_restore_state_selection(state_choices)
                                                    if operation_flow_control == "return_to_menu":
                                                        continue

                                                    try:
                                                        restore_result = restore_tracker_state(
                                                            state.main_df,
                                                            history_df,
                                                            tracker_name_selected,
                                                            chosen_state_option.restore_to
                                                        )
                                                    except ValueError as exc:
                                                        print(f"Restore failed: {exc}")
                                                        operation_flow_control = "return_to_menu"
                                                    else:
                                                        selector = state.main_df['Tracker'].astype(str) == restore_result.tracker_name
                                                        state.main_df.loc[selector, restore_result.restored_row.index] = restore_result.restored_row.values

                                                        ts_restore = datetime.now().strftime("%Y%m%d_%H%M%S")
                                                        report_paths = write_restore_report(
                                                            restore_result,
                                                            output_dir,
                                                            filename_prefix=f"restore_{restore_result.tracker_name}_{ts_restore}"
                                                        )
                                                        print(
                                                            f"\nRestored tracker '{restore_result.tracker_name}' back to {restore_result.restore_to}."
                                                            f" Summary: {report_paths['summary']} | Restored row: {report_paths['restored_row']}"
                                                        )
                                                        operation_flow_control = "return_to_menu"
                except KeyboardInterrupt:
                    operation_flow_control = handle_cancel("Restore setup interrupted.", return_to_menu=True)
            elif chosen_action == "audit":
                try:
                    audit_input_fields_str = questionary.text("Audit canonical field names (e.g., Status__c) (comma-separated):").ask()
                    if audit_input_fields_str is None:
                        operation_flow_control = handle_cancel("Audit input cancelled.", return_to_menu=True)
                    else:
                        canonical_fields_for_audit = [f.strip() for f in audit_input_fields_str.split(',') if f.strip()]
                        if not canonical_fields_for_audit:
                            print("No fields for audit.")
                        else:
                            audit_data_rows = master_audit(state.main_df, canonical_fields_for_audit, detailed_report=True)
                            if not audit_data_rows:
                                print("No trackers found containing any of the specified audit fields.")
                            else:
                                audit_out_df = pd.DataFrame(audit_data_rows)
                                fixed_cols = ['Index', 'Tracker Name Id', 'Tracker Name', 'Owner ID', 'ObjectName']
                                dynamic_cols = sorted([col for col in audit_out_df.columns if col not in fixed_cols])
                                audit_out_df = audit_out_df[fixed_cols + dynamic_cols]
                                ts_audit = datetime.now().strftime("%Y%m%d%H%M%S")
                                audit_f_path = output_dir / f"audit_{ts_audit}.csv"
                                audit_out_df.to_csv(audit_f_path, index=False, encoding='utf-8-sig')
                                print(f"Audit saved to {audit_f_path}")
                                from tracker_hacker.utils import prompt_to_open_report
                                prompt_to_open_report(audit_f_path, description="audit report")
                    operation_flow_control = "return_to_menu"
                except KeyboardInterrupt:
                    operation_flow_control = handle_cancel("Audit input interrupted.", return_to_menu=True)

        except KeyboardInterrupt:
            operation_flow_control = handle_cancel(f"Input for {chosen_action} interrupted.", return_to_menu=True)

        if operation_flow_control == "return_to_menu":
            continue

        final_indices_to_process = []

        if chosen_action == "remove":
            if not removal_plan_from_audit:
                print("No removal plan available.")
                continue
            tracker_ids_list = list(removal_plan_from_audit.keys())
            indexed_tracker_ids = {i: tracker_id for i, tracker_id in enumerate(tracker_ids_list)}
            print(f"Trackers identified from audit file: {len(tracker_ids_list)}")
            choices = [
                Choice(title=f"[{i}] Tracker ID: {t_id}", value=i)
                for i, t_id in indexed_tracker_ids.items()
            ]
            choices.insert(0, Choice("<Select All>", "__ALL__"))
            try:
                selected_indices_choices = questionary.checkbox("Select trackers to modify (by audit Tracker Name Id):", choices=choices).ask()
                if selected_indices_choices is None:
                    operation_flow_control = handle_cancel("Selection cancelled.", return_to_menu=True)
                elif "__ALL__" in selected_indices_choices:
                    final_indices_to_process = [idx for idx, row in state.main_df.iterrows() if row['Tracker Name Id'] in tracker_ids_list]
                else:
                    selected_tracker_ids = [indexed_tracker_ids[s] for s in selected_indices_choices if isinstance(s, int)]
                    final_indices_to_process = [idx for idx, row in state.main_df.iterrows() if row['Tracker Name Id'] in selected_tracker_ids]
            except KeyboardInterrupt:
                operation_flow_control = handle_cancel("Tracker selection interrupted.", return_to_menu=True)

        else:  # For Swap, Add
            if not field_swap_map_cmd and not fields_to_add_list_cmd:
                print("No fields/operations defined for modification.")
                continue

            identified_modifications_cmd = identify_modifications(
                state.main_df,
                canonical_fields_to_remove=[],
                swap_map_input=field_swap_map_cmd,
                canonical_fields_to_add=fields_to_add_list_cmd
            )
            if not identified_modifications_cmd:
                print("No trackers require modification.")
                continue

            try:
                mods_to_display = identified_modifications_cmd
                print(f"Trackers identified for modification: {len(mods_to_display)}")
                choices = [
                    Choice(title=f"Index {idx}: {state.main_df.loc[idx, 'Tracker Name']} -> Modifies: {', '.join(cols)}", value=idx)
                    for idx, cols in mods_to_display.items()
                ]
                choices.insert(0, Choice("<Select All>", "__ALL__"))

                selected = questionary.checkbox("Select trackers to modify:", choices=choices).ask()
                if selected is None:
                    operation_flow_control = handle_cancel("Selection cancelled.", return_to_menu=True)
                elif "__ALL__" in selected:
                    final_indices_to_process = list(mods_to_display.keys())
                else:
                    final_indices_to_process = [s for s in selected if isinstance(s, int)]

            except KeyboardInterrupt:
                operation_flow_control = handle_cancel("Tracker selection interrupted.", return_to_menu=True)
            if operation_flow_control == "return_to_menu":
                continue

        if not final_indices_to_process:
            print("No trackers ultimately selected for modification.")
            continue

        ts_mod = datetime.now().strftime("%Y%m%d%H%M%S")
        try:
            modify_trackers(
                state.main_df,
                final_indices_to_process,
                removal_plan_from_audit if chosen_action == 'remove' else [],
                field_swap_map_cmd,
                fields_to_add_list_cmd,
                output_dir,
                ts_mod
            )
        except KeyboardInterrupt:
            handle_cancel("Modification process interrupted by user.", return_to_menu=False)
            print("Note: If process was interrupted during writing, files might be incomplete.")
            break

        print("-" * 30)


def run_app():
    try:
        main_loop()
    except SystemExit:
        pass
    except Exception as e:
        print(f"\nAn unexpected critical error occurred in the main execution block: {e}")
        import traceback
        traceback.print_exc()
    print("\nTracker Modifier Tool session has concluded.")


if __name__ == "__main__":
    run_app()
