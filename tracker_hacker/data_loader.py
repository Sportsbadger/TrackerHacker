from pathlib import Path

import pandas as pd
import questionary
from questionary import Choice

from tracker_hacker.constants import REQUIRED_COLUMNS
from tracker_hacker.json_checker import check_and_report_malformed_json
from tracker_hacker import state
from tracker_hacker.utils import handle_cancel


def load_source_data_csv():
    script_dir = Path(__file__).resolve().parent.parent
    output_dir_for_errors = script_dir / 'outputs'
    output_dir_for_errors.mkdir(exist_ok=True)

    try:
        p_input_str = questionary.path(
            "Path to Sitetracker CSV export (or directory containing it):",
            default=str(script_dir)
        ).ask()
        if p_input_str is None:
            return handle_cancel("Source data loading cancelled.", return_to_menu=True)
    except KeyboardInterrupt:
        return handle_cancel("Source data loading interrupted.", return_to_menu=True)

    path_obj_input = Path(p_input_str)
    path_to_load_main_csv = None

    if path_obj_input.is_dir():
        csv_files_in_dir = sorted([f for f in path_obj_input.glob("*.csv") if f.is_file()])
        if not csv_files_in_dir:
            print(f"No CSV files found in directory: {path_obj_input}")
            return False
        elif len(csv_files_in_dir) == 1:
            path_to_load_main_csv = csv_files_in_dir[0]
            print(f"Automatically selected data CSV: {path_to_load_main_csv.name}")
        else:
            main_csv_choices = [Choice(title=c.name, value=c) for c in csv_files_in_dir]
            main_csv_choices.insert(0, Choice(title="<Cancel selection>", value=None))

            selected_main_csv_obj = None
            try:
                selected_main_csv_obj = questionary.select("Select data CSV file:", choices=main_csv_choices).ask()
            except KeyboardInterrupt:
                return handle_cancel("CSV file selection from directory interrupted.", return_to_menu=True)

            if selected_main_csv_obj is None:
                print("No data CSV selected.")
                return False

            if isinstance(selected_main_csv_obj, Path):
                path_to_load_main_csv = selected_main_csv_obj
            else:
                print(f"Invalid selection received: '{selected_main_csv_obj}'. Cancelling load.")
                return False

    elif path_obj_input.is_file() and path_obj_input.suffix.lower() == ".csv":
        path_to_load_main_csv = path_obj_input
    else:
        print(f"Invalid path or not a CSV file: {path_obj_input}")
        return False

    if not isinstance(path_to_load_main_csv, Path):
        print("Could not determine a valid data CSV file path.")
        return False

    try:
        temp_df = pd.read_csv(path_to_load_main_csv, dtype={'Tracker Name Id': str})
        missing = [col for col in REQUIRED_COLUMNS if col not in temp_df.columns]
        if missing:
            print(f"Error: Input CSV '{path_to_load_main_csv.name}' missing required columns: {missing}")
            state.main_df = None
            return False
        state.main_df = temp_df
        state.main_df._source_file_name = path_to_load_main_csv.name
        print(f"Successfully loaded {len(state.main_df)} trackers from '{path_to_load_main_csv.name}'.")

        check_and_report_malformed_json(state.main_df, output_dir_for_errors)

        return True
    except FileNotFoundError:
        print(f"Error: File not found: {path_to_load_main_csv}")
    except pd.errors.EmptyDataError:
        print(f"Error: File is empty: {path_to_load_main_csv.name}")
    except Exception as e:
        filename_for_error = path_to_load_main_csv.name if isinstance(path_to_load_main_csv, Path) else str(path_to_load_main_csv)
        print(f"Error loading CSV '{filename_for_error}': {e}")
    state.main_df = None
    return False
