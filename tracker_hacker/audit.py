import json

import pandas as pd

from tracker_hacker.utils import find_contextual_occurrences_of_field


# --- START: UNIFIED MASTER AUDIT FUNCTION and WRAPPER ---
def master_audit(df_to_audit, canonical_fields_to_check, detailed_report=False):
    cols_to_search = ['Fields', 'Filters', 'Logic', 'Query', 'Formatting', 'OrderBy(Long)', 'ResizeMap', 'Label Map']

    if detailed_report:
        audit_data_rows = []
    else:
        found_indices = []

    for idx, row_data in df_to_audit.iterrows():
        row_had_any_audit_match = False

        if detailed_report:
            audit_entry = {'Index': idx, 'Tracker Name Id': row_data.get('Tracker Name Id', ''),
                           'Tracker Name': row_data['Tracker Name'], 'Owner ID': row_data['Owner ID'],
                           'ObjectName': row_data['ObjectName']}

        parsed_filters_for_row = None
        filters_json_str = str(row_data.get('Filters', '[]'))
        if filters_json_str.strip():
            try:
                parsed_filters_for_row = json.loads(filters_json_str)
            except (json.JSONDecodeError, TypeError):
                pass

        for canonical_field_to_audit in canonical_fields_to_check:
            all_found_contextual_paths_in_row = set()
            columns_where_found_map = {}
            found_in_filters_structurally = []

            if isinstance(parsed_filters_for_row, list):
                for filter_condition_dict in parsed_filters_for_row:
                    if isinstance(filter_condition_dict, dict):
                        filter_field_val = filter_condition_dict.get('field', '')
                        if filter_field_val == canonical_field_to_audit or \
                           (filter_field_val.endswith("." + canonical_field_to_audit)):
                            found_in_filters_structurally.append(filter_field_val)
                            all_found_contextual_paths_in_row.add(filter_field_val)
                            columns_where_found_map.setdefault(filter_field_val, []).append('Filters (Structured)')

            for col_name_audit in cols_to_search:
                col_content_str = str(row_data.get(col_name_audit, ''))
                if not col_content_str:
                    continue
                contextual_paths_in_this_col = find_contextual_occurrences_of_field(col_content_str, canonical_field_to_audit)
                for path in contextual_paths_in_this_col:
                    all_found_contextual_paths_in_row.add(path)
                    if col_name_audit == 'Filters' and path in found_in_filters_structurally:
                        pass
                    else:
                        columns_where_found_map.setdefault(path, []).append(col_name_audit)

            if all_found_contextual_paths_in_row:
                row_had_any_audit_match = True
                if detailed_report:
                    for contextual_path in sorted(list(all_found_contextual_paths_in_row)):
                        col_key_prefix = f"{canonical_field_to_audit} (as {contextual_path})"
                        audit_entry[f"{col_key_prefix} - Columns"] = ', '.join(
                            sorted(list(set(columns_where_found_map.get(contextual_path, []))))
                        )
                        audit_entry[f"{col_key_prefix} - In Filter Def?"] = (contextual_path in found_in_filters_structurally)
                else:
                    break

        if row_had_any_audit_match:
            if detailed_report:
                audit_data_rows.append(audit_entry)
            else:
                found_indices.append(idx)

    return audit_data_rows if detailed_report else found_indices


def audit_indices(df_to_audit, canonical_fields_to_check):
    return master_audit(df_to_audit, canonical_fields_to_check, detailed_report=False)
# --- END: UNIFIED MASTER AUDIT FUNCTION and WRAPPER ---
