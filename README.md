# TrackerHacker

TrackerHacker is a CLI toolkit for auditing and modifying Sitetracker tracker exports. The tools load tracker CSV exports, flag problematic rows, and generate cleaned copies so you can re-import trackers with confidence. A step-by-step walkthrough with programmatic examples is available in [docs/USAGE.md](docs/USAGE.md).

## Prerequisites
- Python 3.10+
- Access to a Sitetracker tracker CSV export
- Recommended: a Python virtual environment so dependencies stay isolated

Install dependencies from the repository root:
```bash
python -m venv .venv
source .venv/bin/activate
pip install pandas questionary
```

## Required CSV columns
The primary CSV you load **must** contain the following columns (case-sensitive):

- `Tracker Name Id`
- `ObjectName`
- `Tracker Name`
- `Owner ID`
- `Fields`
- `Filters`
- `Logic`
- `Query`
- `Formatting`
- `OrderBy(Long)`
- `ResizeMap`
- `Label Map`

If any of these columns are missing, the loader will reject the file and print the missing column names.

### Optional helper CSVs
- Swap-pairs CSV: requires columns `OldFieldAPI` and `NewFieldAPI` to map legacy field API names to replacements.
- Audit removal file: can be a JSON-like mapping of `Tracker Name Id` to lists of field API names for forced removal.

## Running the CLI
From the repository root, run the interactive CLI with:
```bash
python TrackerHacker.py
```
You will be prompted for the path to your Sitetracker export CSV (or the directory containing it). If a directory is provided and multiple CSVs are found, you will be asked to pick one.

### Typical workflow
1. **Load data**: Select the CSV export. The CLI validates required columns and stores the dataframe in memory.
2. **Audit**: Scan for canonical fields across `Fields`, `Filters`, `Logic`, `Query`, `Formatting`, `OrderBy(Long)`, `ResizeMap`, and `Label Map`. Reports can be summary index lists or detailed column-by-column breakdowns.
3. **Identify modifications**: Detect where fields need removal, swapping, or addition before committing changes.
4. **Modify trackers**: Apply removals, swaps, and additions to selected rows. Backup copies of affected rows are preserved. Updated CSVs are written under `outputs/` with timestamps.
5. **Review outputs**: CSVs and JSON reports are saved in `outputs/`; the CLI can prompt you to open reports directly.
6. **Restore from history**: Load a tracker history CSV to reconstruct a single tracker to a specific "Modify Date" point in time, update the in-memory dataset, and save a restore report.

## Module and function guide
Use these functions directly if you prefer scripting instead of the interactive CLI:

### `tracker_hacker.cli`
- `main_loop()`: Runs the menu-driven workflow (load data, audit, modify, export).
- `run_app()`: Thin wrapper that starts `main_loop()`; invoked by `TrackerHacker.py`.

### `tracker_hacker.data_loader`
- `load_source_data_csv()`: Prompt for a CSV path, validate required columns, load data into `tracker_hacker.state.main_df`, and trigger JSON validation for the `Filters` column.

### `tracker_hacker.audit`
- `master_audit(df_to_audit, canonical_fields_to_check, detailed_report=False)`: Search rows for canonical fields, returning either indices or per-column context details.
- `audit_indices(df_to_audit, canonical_fields_to_check)`: Convenience wrapper that returns only index lists of matching rows.

### `tracker_hacker.modifications`
- `load_swap_pairs_csv(path, old_col_name="OldFieldAPI", new_col_name="NewFieldAPI")`: Read swap mappings from CSV, returning a `{old: new}` dictionary.
- `identify_modifications(df_to_check, canonical_fields_to_remove=None, swap_map_input=None, canonical_fields_to_add=None)`: Flag columns in each row requiring changes (removals, swaps, additions).
- `modify_trackers(df_orig, selected_indices, removal_instructions, swap_map_cmd, add_list_cmd, output_dir_path, timestamp_str)`: Apply removals, swaps, additions, and save modified and backup CSVs with timestamped filenames.

### `tracker_hacker.json_checker`
- `check_and_report_malformed_json(df_to_check, output_dir_path)`: Validate JSON stored in `Filters`; writes a report for malformed rows.

### `tracker_hacker.utils`
Helper utilities used across the toolkit:
- `handle_cancel(...)`: Standardized cancellation handler for menu prompts.
- `prompt_to_open_report(report_path, description=None)`: Offer to open generated reports.
- `remove_field_from_text(...)`: Remove a field path from comma-separated lists.
- `remove_key_value_entry(...)`: Strip a key from `key=value` or `key:value` mappings.
- `swap_field_in_text(...)`: Replace one field path with another within strings.
- `add_fields_to_list(...)`: Append canonical fields to comma-separated lists if missing.
- `generate_sitetracker_filter_label(...)`: Build a label from a fully-qualified field path.
- `get_sitetracker_filter_sobject(...)`: Extract the sObject from a field path for filter labeling.
- `find_contextual_occurrences_of_field(...)`: Locate canonical field names in text while preserving context.
- `update_logic(...)`: Rebuild `Logic` expressions after filter removals.
- `update_query(...)`: Remove references from `Query` clauses based on contextual paths.

### `tracker_hacker.history_restore`
- `restore_tracker_state(current_df, history_df, tracker_id, restore_to)`: Replays history entries newer than `restore_to` to rebuild the tracker row as it existed at that time and report applied/skipped changes.
- `write_restore_report(result, output_dir, filename_prefix=None)`: Writes a human-readable summary and CSV snapshot of the restored row.

#### Restoring a tracker from a history CSV via the CLI
1. Choose **Restore Tracker from History** in the main menu.
2. Select a tracker history CSV (must include columns like `Tracker`, `id Tracker`, `Modify Date`, `Field`/`API Field`, `Old Value`, `New Value`).
3. Pick the **Tracker** (plain English name) to restore and enter the target "Modify Date" timestamp to roll back to.
4. Optionally replace the in-memory tracker row and save a summary plus restored-row CSV under `outputs/`.

## Outputs
- Validation and modification artifacts are written under `outputs/` (created automatically).
- Filenames include timestamps so runs remain separate.

## Notes
- All prompts are interrupt-safe: pressing `Ctrl+C` or selecting cancel returns you to the main menu.
- CSV reading is tolerant of empty files or malformed JSON, with explicit error messages where applicable.
