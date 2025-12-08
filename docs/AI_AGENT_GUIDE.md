# TrackerHacker – AI Coding Agent Brief

## Mission overview
- CLI toolkit for auditing and modifying Sitetracker tracker CSV exports.
- Focuses on cleaning field references, swapping/adding/removing fields, validating embedded JSON, and restoring tracker rows from history exports.
- Entry point: `TrackerHacker.py` (runs `tracker_hacker.cli.run_app`).

## Architecture map
- **State**: `tracker_hacker.state` stores the active dataframe (`main_df`) and session metadata used across menu actions.
- **CLI**: `tracker_hacker.cli` orchestrates the menu workflow (load data → audit/modify/restore → write outputs).
- **Data loading**: `tracker_hacker.data_loader.load_source_data_csv()` validates required columns and caches the dataframe.
- **Auditing**: `tracker_hacker.audit.master_audit()` scans tracker rows for canonical fields; reports can be summary (row indices) or detailed (column contexts).
- **Modifications**: `tracker_hacker.modifications` handles field removal, swapping, and addition; `modify_trackers()` writes modified CSVs plus backups.
- **JSON validation**: `tracker_hacker.json_checker.check_and_report_malformed_json()` inspects the `Filters` column for malformed JSON and writes a report.
- **History restore**: `tracker_hacker.history_restore` rebuilds a tracker row to a point-in-time state and writes a restore report/snapshot.
- **Utilities**: `tracker_hacker.utils` provides shared helpers (prompt handling, field path manipulation, query/logic updates, label generation).

## Data contracts
- **Required CSV columns**: `Tracker Name Id`, `ObjectName`, `Tracker Name`, `Owner ID`, `Fields`, `Filters`, `Logic`, `Query`, `Formatting`, `OrderBy(Long)`, `ResizeMap`, `Label Map`.
- **Optional helpers**:
  - Swap pairs CSV: columns `OldFieldAPI`, `NewFieldAPI`.
  - Audit removal mapping: JSON-like mapping of `Tracker Name Id` → list of field API names.
- **Outputs**: Artifacts land in `outputs/` with timestamps (audit summaries/detailed reports, modified CSVs, backups, restore reports).

## Common workflows
1. **Load source data**: Point to the tracker export CSV (or containing directory). Loader validates required columns and runs JSON checks on `Filters`.
2. **Audit fields**: Use summary or detailed mode to find canonical field occurrences across `Fields`, `Filters`, `Logic`, `Query`, `Formatting`, `OrderBy(Long)`, `ResizeMap`, `Label Map`.
3. **Modify trackers**: Drive removals, swaps, and additions (manually or via audit/swap CSVs). Outputs include modified files plus backups.
4. **Restore from history**: Select a tracker history CSV, choose a tracker + target "Modify Date", and apply/inspect the reconstructed row.

## Development guidance for agents
- **Python version**: 3.10+; core deps are `pandas` and `questionary`. Favor built-ins and existing helpers over new dependencies.
- **Paths & files**: Use `pathlib.Path` for filesystem work. Write artifacts under `outputs/`; create the directory if absent.
- **Style**: Prefer pure, typed functions with guard clauses. Keep CLI prompts interrupt-safe via `tracker_hacker.utils.handle_cancel()`.
- **Testing**: Run `pytest` from the repo root. Tests cover history restoration and CLI cancel handling.
- **Safety**: Validate CSV headers before processing; treat embedded JSON cautiously and surface clear errors.

## Quick navigation
- CLI loop: `tracker_hacker/cli.py` (`run_app`, `main_loop`).
- Audit utilities: `tracker_hacker/audit.py`.
- Modification pipeline: `tracker_hacker/modifications.py`.
- Data loading: `tracker_hacker/data_loader.py`.
- Restore helpers: `tracker_hacker/history_restore.py`.
- Shared helpers: `tracker_hacker/utils.py` and `tracker_hacker/constants.py`.
