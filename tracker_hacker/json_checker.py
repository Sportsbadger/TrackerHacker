import json
from datetime import datetime

import pandas as pd


def check_and_report_malformed_json(df_to_check, output_dir_path):
    json_columns_to_check = ['Filters', 'Formatting']
    malformed_rows_indices = []
    error_details_for_file = []
    print("\nChecking for malformed JSON in 'Filters', 'Formatting' columns...")
    for idx, row_data in df_to_check.iterrows():
        row_has_at_least_one_malformed_json_column = False
        for col_name in json_columns_to_check:
            if col_name not in row_data:
                continue
            json_str_original_val = row_data.get(col_name)
            if pd.isna(json_str_original_val):
                json_str_for_parsing = ""
            else:
                json_str_for_parsing = str(json_str_original_val)
            json_str_stripped = json_str_for_parsing.strip()
            if not json_str_stripped:
                continue
            if json_str_stripped.lower() == "null":
                continue
            if json_str_stripped.lower() == "nan":
                continue
            if json_str_stripped == '[]' and col_name in ['Filters', 'Formatting']:
                continue
            try:
                if not isinstance(json_str_original_val, (dict, list)):
                    json.loads(json_str_for_parsing)
            except json.JSONDecodeError as e:
                if not row_has_at_least_one_malformed_json_column:
                    malformed_rows_indices.append(idx)
                    row_has_at_least_one_malformed_json_column = True
                context_snippet = ""
                context_window = 30
                string_for_context = json_str_for_parsing
                if hasattr(e, 'pos') and e.pos is not None:
                    start = max(0, e.pos - context_window)
                    end = min(len(string_for_context), e.pos + context_window)
                    char_at_pos = string_for_context[e.pos] if e.pos < len(string_for_context) else "[?]"
                    if start <= e.pos < len(string_for_context):
                        context_snippet = (
                            string_for_context[start:e.pos]
                            + f" >>>>{char_at_pos}<<<< "
                            + string_for_context[e.pos + 1:end]
                        )
                    elif e.pos == len(string_for_context):
                        context_snippet = string_for_context[start:end] + " >>>>[END_OF_STRING]<<<< "
                    else:
                        context_snippet = string_for_context[start:end]
                    if start > 0:
                        context_snippet = "..." + context_snippet
                    if end < len(string_for_context):
                        context_snippet = context_snippet + "..."
                error_details_for_file.append({
                    'Index': idx, 'Tracker Name Id': row_data.get('Tracker Name Id', 'N/A'),
                    'Tracker Name': row_data.get('Tracker Name', 'N/A'), 'ObjectName': row_data.get('ObjectName', 'N/A'),
                    'Malformed Column': col_name, 'JSON Error Message': str(e),
                    'Error Context Snippet': context_snippet, 'Problematic Value (Full)': json_str_for_parsing
                })
    if malformed_rows_indices:
        timestamp_str = datetime.now().strftime("%Y%m%d%H%M%S")
        error_filename_full_rows = output_dir_path / f"malformed_json_trackers_{timestamp_str}.csv"
        error_filename_details = output_dir_path / f"malformed_json_details_{timestamp_str}.csv"
        if malformed_rows_indices:
            error_df_to_save = df_to_check.loc[list(set(malformed_rows_indices))].copy()
            error_df_to_save.to_csv(error_filename_full_rows, index=False, encoding='utf-8-sig')
            print(f"Warning: Found {len(set(malformed_rows_indices))} trackers with malformed JSON in 'Filters' or 'Formatting'.")
            print(f"         The full data for these trackers has been saved to: {error_filename_full_rows}")
        if error_details_for_file:
            error_details_df = pd.DataFrame(error_details_for_file)
            detail_cols_order = ['Index', 'Tracker Name Id', 'Tracker Name', 'Owner ID', 'ObjectName',
                                 'Malformed Column', 'JSON Error Message', 'Error Context Snippet',
                                 'Problematic Value (Full)']
            actual_detail_cols = [col for col in detail_cols_order if col in error_details_df.columns]
            error_details_df = error_details_df[actual_detail_cols]
            error_details_df.to_csv(error_filename_details, index=False, encoding='utf-8-sig')
            print(f"         A detailed report of JSON errors has been saved to: {error_filename_details}")
    else:
        print("No malformed JSON found in 'Filters', 'Formatting' columns during initial check.")
    print("-" * 30)
