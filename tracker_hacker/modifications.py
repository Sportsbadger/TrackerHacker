import json
import re

import pandas as pd

from tracker_hacker.utils import (
    add_fields_to_list,
    find_contextual_occurrences_of_field,
    generate_sitetracker_filter_label,
    get_sitetracker_filter_sobject,
    prompt_to_open_report,
    remove_field_from_text,
    remove_key_value_entry,
    swap_field_in_text,
    update_logic,
    update_query,
)


def load_swap_pairs_csv(path, old_col_name="OldFieldAPI", new_col_name="NewFieldAPI"):
    try:
        df_swap = pd.read_csv(path)
        if old_col_name not in df_swap.columns or new_col_name not in df_swap.columns:
            print(f"Error: Swap pairs CSV ('{path.name}') must contain columns '{old_col_name}' and '{new_col_name}'.")
            print(f"Found columns: {df_swap.columns.tolist()}")
            return None
        swap_map = {}
        for index, row in df_swap.iterrows():
            old_field = str(row[old_col_name]).strip()
            new_field = str(row[new_col_name]).strip()
            if not old_field or not new_field:
                print(f"Warning: Skipping row {index + 2} in '{path.name}' (empty old/new field).")
                continue
            if old_field in swap_map:
                print(f"Warning: Duplicate old field '{old_field}' in '{path.name}'. Using latest: '{new_field}'.")
            swap_map[old_field] = new_field
        if not swap_map:
            print(f"No valid swap pairs found in '{path.name}'.")
            return None
        print(f"Successfully loaded {len(swap_map)} swap pairs from '{path.name}'.")
        return swap_map
    except FileNotFoundError:
        print(f"Error: Swap pairs CSV not found: {path}")
        return None
    except pd.errors.EmptyDataError:
        print(f"Error: Swap pairs CSV '{path.name}' is empty.")
        return None
    except Exception as e:
        print(f"Error reading swap pairs CSV '{path.name}': {e}")
        return None


def identify_modifications(df_to_check, canonical_fields_to_remove=None, swap_map_input=None, canonical_fields_to_add=None):
    mods = {}
    for idx, row_data in df_to_check.iterrows():
        modified_cols = []
        filters_list_parsed_for_row = None
        needs_filters_parsed = (canonical_fields_to_remove or swap_map_input)
        if needs_filters_parsed:
            try:
                filters_json_str = str(row_data.get('Filters', '[]'))
                filters_list_parsed_for_row = json.loads(filters_json_str) if filters_json_str.strip() else []
                if not isinstance(filters_list_parsed_for_row, list):
                    filters_list_parsed_for_row = []
            except (json.JSONDecodeError, TypeError):
                filters_list_parsed_for_row = []
        if canonical_fields_to_remove:
            for f_rem_canonical in canonical_fields_to_remove:
                if filters_list_parsed_for_row is not None:
                    for f_dict in filters_list_parsed_for_row:
                        if isinstance(f_dict, dict) and f_dict.get('field', ''):
                            field_in_filter = f_dict.get('field', '')
                            if field_in_filter == f_rem_canonical or field_in_filter.endswith("." + f_rem_canonical):
                                modified_cols.extend(['Filters', 'Logic'])
                                break
                for col_name in ['Fields', 'Logic', 'Query', 'Formatting', 'OrderBy(Long)', '.ResizeMap', 'Label Map']:
                    if col_name == 'Filters' and 'Filters' in modified_cols:
                        continue
                    text_content = str(row_data.get(col_name, ''))
                    if find_contextual_occurrences_of_field(text_content, f_rem_canonical):
                        modified_cols.append(col_name)
        if swap_map_input:
            for old_api_full_path, new_api_full_path in swap_map_input.items():
                new_field_exists = False
                if filters_list_parsed_for_row is not None:
                    if any(isinstance(f_d, dict) and f_d.get('field', '') == new_api_full_path for f_d in filters_list_parsed_for_row):
                        new_field_exists = True
                if not new_field_exists:
                    fields_str_check = str(row_data.get('Fields', ''))
                    if re.search(rf"\b{re.escape(new_api_full_path)}\b", fields_str_check):
                        new_field_exists = True
                field_to_detect = old_api_full_path
                if filters_list_parsed_for_row is not None:
                    if any(isinstance(f_d, dict) and f_d.get('field', '') == field_to_detect for f_d in filters_list_parsed_for_row):
                        modified_cols.append('Filters')
                        if new_field_exists:
                            modified_cols.append('Logic')
                for col_name in ['Fields', 'Logic', 'Query', 'Formatting', 'OrderBy(Long)', 'ResizeMap', 'Label Map']:
                    if col_name == 'Filters' and 'Filters' in modified_cols:
                        continue
                    if re.search(rf"\b{re.escape(field_to_detect)}\b", str(row_data.get(col_name, ''))):
                        modified_cols.append(col_name)
        if canonical_fields_to_add:
            current_flds_val = str(row_data.get('Fields', ''))
            items_in_flds = [i.strip() for i in current_flds_val.split(',') if i.strip()]
            if any(f_add_canon not in items_in_flds for f_add_canon in canonical_fields_to_add):
                modified_cols.append('Fields')
        if modified_cols:
            mods[idx] = sorted(set(modified_cols))
    return mods


# --- START: CORRECTED modify_trackers function ---
def modify_trackers(df_orig, selected_indices, removal_instructions, swap_map_cmd, add_list_cmd, output_dir_path, timestamp_str):
    backup_df = df_orig.loc[selected_indices].copy()
    modified_df_copy = df_orig.copy()
    print(f"Processing {len(selected_indices)} selected trackers...")

    for idx in selected_indices:
        base_tracker_sobject_name = str(modified_df_copy.loc[idx, 'ObjectName']) if 'ObjectName' in modified_df_copy.columns else ''

        # --- Build effective removal and swap lists for this row ---
        effective_row_remove_list_for_processing = []

        # Handle removals coming from an audit file (if removal_instructions is a dict)
        if isinstance(removal_instructions, dict):
            tracker_id_for_row = modified_df_copy.loc[idx, 'Tracker Name Id']
            effective_row_remove_list_for_processing.extend(removal_instructions.get(tracker_id_for_row, []))

        # Handle swap-to-remove logic
        effective_row_swap_map = {}
        if swap_map_cmd:
            for old_api, new_api in swap_map_cmd.items():
                new_field_exists_in_row = False
                try:
                    f_str_check = str(modified_df_copy.loc[idx, 'Filters'] if pd.notna(modified_df_copy.loc[idx, 'Filters']) else '[]')
                    if f_str_check.strip():
                        f_list_check = json.loads(f_str_check)
                        if isinstance(f_list_check, list) and any(isinstance(fd, dict) and fd.get('field', '') == new_api for fd in f_list_check):
                            new_field_exists_in_row = True
                except (json.JSONDecodeError, TypeError):
                    pass
                if not new_field_exists_in_row:
                    fields_col_str = str(modified_df_copy.loc[idx, 'Fields'] if pd.notna(modified_df_copy.loc[idx, 'Fields']) else '')
                    if re.search(rf"\b{re.escape(new_api)}\b", fields_col_str):
                        new_field_exists_in_row = True

                if new_field_exists_in_row:
                    print(f"  Row {idx}: New swap field '{new_api}' already exists. Converting swap of '{old_api}' to its removal.")
                    if old_api not in effective_row_remove_list_for_processing:
                        effective_row_remove_list_for_processing.append(old_api)
                else:
                    effective_row_swap_map[old_api] = new_api

        # Remove fields
        if effective_row_remove_list_for_processing:
            contextual_paths_to_remove = []
            for rem_full_api in effective_row_remove_list_for_processing:
                contextual_paths_to_remove.append(rem_full_api)
                contextual_paths_to_remove.extend(find_contextual_occurrences_of_field(
                    str(modified_df_copy.loc[idx, 'Fields']), rem_full_api.split('.')[-1]
                ))
            contextual_paths_to_remove = sorted(set(contextual_paths_to_remove))

            modified_df_copy.loc[idx, 'Fields'] = remove_field_from_text(
                modified_df_copy.loc[idx, 'Fields'] if pd.notna(modified_df_copy.loc[idx, 'Fields']) else '',
                effective_row_remove_list_for_processing[0]
            )

            new_filters_list = []
            filters_logic_positions_removed = []
            try:
                filters_json_str = str(modified_df_copy.loc[idx, 'Filters'] if pd.notna(modified_df_copy.loc[idx, 'Filters']) else '[]')
                filters_list = json.loads(filters_json_str) if filters_json_str.strip() else []
                if isinstance(filters_list, list):
                    for pos, f_cond in enumerate(filters_list, start=1):
                        if isinstance(f_cond, dict):
                            field_val = f_cond.get('field', '')
                            if field_val in contextual_paths_to_remove or any(field_val.startswith(rem + '.') for rem in contextual_paths_to_remove):
                                filters_logic_positions_removed.append(pos)
                            else:
                                new_filters_list.append(f_cond)
                modified_df_copy.loc[idx, 'Filters'] = json.dumps(new_filters_list)
            except (json.JSONDecodeError, TypeError):
                pass

            modified_df_copy.loc[idx, 'Logic'] = update_logic(str(modified_df_copy.loc[idx, 'Logic']), filters_logic_positions_removed)

            modified_df_copy.loc[idx, 'Query'] = update_query(
                str(modified_df_copy.loc[idx, 'Query']),
                contextual_paths_to_remove
            )

            formatting_col_content = str(modified_df_copy.loc[idx, 'Formatting'] if pd.notna(modified_df_copy.loc[idx, 'Formatting']) else '')
            formatting_lines = []
            for line in formatting_col_content.split('\n'):
                kv_parts = line.split('=')
                if len(kv_parts) == 2:
                    left_key = kv_parts[0].strip()
                    field_val_left = left_key.split(':')[0].strip()
                    if field_val_left in contextual_paths_to_remove or any(field_val_left.startswith(rem + '.') for rem in contextual_paths_to_remove):
                        continue
                formatting_lines.append(line)
            modified_df_copy.loc[idx, 'Formatting'] = '\n'.join(formatting_lines)

            modified_df_copy.loc[idx, 'OrderBy(Long)'] = remove_key_value_entry(
                modified_df_copy.loc[idx, 'OrderBy(Long)'] if pd.notna(modified_df_copy.loc[idx, 'OrderBy(Long)']) else '',
                effective_row_remove_list_for_processing[0]
            )

            resize_map_str = str(modified_df_copy.loc[idx, 'ResizeMap'] if pd.notna(modified_df_copy.loc[idx, 'ResizeMap']) else '')
            modified_df_copy.loc[idx, 'ResizeMap'] = remove_key_value_entry(resize_map_str, effective_row_remove_list_for_processing[0])

            label_map_str = str(modified_df_copy.loc[idx, 'Label Map'] if pd.notna(modified_df_copy.loc[idx, 'Label Map']) else '')
            modified_df_copy.loc[idx, 'Label Map'] = remove_key_value_entry(label_map_str, effective_row_remove_list_for_processing[0], separator=':')

        # Swap fields
        if effective_row_swap_map:
            for old_api_full, new_api_full in effective_row_swap_map.items():
                modified_df_copy.loc[idx, 'Fields'] = swap_field_in_text(
                    modified_df_copy.loc[idx, 'Fields'] if pd.notna(modified_df_copy.loc[idx, 'Fields']) else '',
                    old_api_full,
                    new_api_full
                )

                # Update Filters
                try:
                    filters_str_existing = str(modified_df_copy.loc[idx, 'Filters'] if pd.notna(modified_df_copy.loc[idx, 'Filters']) else '[]')
                    filters_list = json.loads(filters_str_existing) if filters_str_existing.strip() else []
                    if isinstance(filters_list, list):
                        for f_dict in filters_list:
                            if isinstance(f_dict, dict) and f_dict.get('field', '') == old_api_full:
                                f_dict['field'] = new_api_full
                                if 'label' in f_dict:
                                    f_dict['label'] = generate_sitetracker_filter_label(new_api_full)
                                if 'sobject' in f_dict:
                                    f_dict['sobject'] = get_sitetracker_filter_sobject(new_api_full, base_tracker_sobject_name)
                        modified_df_copy.loc[idx, 'Filters'] = json.dumps(filters_list)
                except (json.JSONDecodeError, TypeError):
                    pass

                # Update Logic
                logic_str_curr = str(modified_df_copy.loc[idx, 'Logic'])
                modified_df_copy.loc[idx, 'Logic'] = swap_field_in_text(logic_str_curr, old_api_full, new_api_full)

                # Update Query
                query_str_curr = str(modified_df_copy.loc[idx, 'Query'])
                modified_df_copy.loc[idx, 'Query'] = swap_field_in_text(query_str_curr, old_api_full, new_api_full)

                # Update Formatting
                formatting_curr = str(modified_df_copy.loc[idx, 'Formatting'] if pd.notna(modified_df_copy.loc[idx, 'Formatting']) else '')
                formatting_lines_new = []
                for line in formatting_curr.split('\n'):
                    formatting_lines_new.append(swap_field_in_text(line, old_api_full, new_api_full))
                modified_df_copy.loc[idx, 'Formatting'] = '\n'.join(formatting_lines_new)

                modified_df_copy.loc[idx, 'OrderBy(Long)'] = swap_field_in_text(
                    modified_df_copy.loc[idx, 'OrderBy(Long)'] if pd.notna(modified_df_copy.loc[idx, 'OrderBy(Long)']) else '',
                    old_api_full,
                    new_api_full
                )

                modified_df_copy.loc[idx, 'ResizeMap'] = swap_field_in_text(
                    modified_df_copy.loc[idx, 'ResizeMap'] if pd.notna(modified_df_copy.loc[idx, 'ResizeMap']) else '',
                    old_api_full,
                    new_api_full
                )

                modified_df_copy.loc[idx, 'Label Map'] = swap_field_in_text(
                    modified_df_copy.loc[idx, 'Label Map'] if pd.notna(modified_df_copy.loc[idx, 'Label Map']) else '',
                    old_api_full,
                    new_api_full
                )

        # Add fields
        if add_list_cmd:
            cols_to_add_fields_to = ['Fields']
            for col_add in cols_to_add_fields_to:
                curr_val_add = str(modified_df_copy.loc[idx, col_add] if pd.notna(modified_df_copy.loc[idx, col_add]) else '')
                modified_df_copy.loc[idx, col_add] = add_fields_to_list(curr_val_add, add_list_cmd)

    output_f = output_dir_path / f"modified_{timestamp_str}.csv"
    backup_f = output_dir_path / f"backup_{timestamp_str}.csv"
    modified_df_copy.loc[selected_indices].to_csv(output_f, index=False, encoding='utf-8-sig')
    backup_df.to_csv(backup_f, index=False, encoding='utf-8-sig')
    print(f"Modified data for {len(selected_indices)} trackers saved to {output_f}")
    print(f"Backup of original selected trackers saved to {backup_f}")
    prompt_to_open_report(output_f, description="modified tracker report")
    prompt_to_open_report(backup_f, description="backup of modified trackers")
# --- END: CORRECTED modify_trackers function ---
