"""
README - Tracker Modifier Tool

v1.9.5 Fixed AttributeError in modify_trackers for Swap/Add actions.

A command-line utility to modify Sitetracker tracker definitions via CSV export/import.
"""

import pandas as pd
import re
import json
import questionary
import sys
import webbrowser
from pathlib import Path
from questionary import Choice
from datetime import datetime

# Required columns
REQUIRED_COLUMNS = [
    'Tracker Name Id', 
    'ObjectName', 'Tracker Name', 'Owner ID', 'Fields', 'Filters', 'Logic', 'Query',
    'Formatting', 'OrderBy(Long)', 'ResizeMap', 'Label Map'
]

# --- Global DataFrame ---
main_df = None 

# Helper for graceful cancellation
def handle_cancel(message="Operation cancelled by user.", return_to_menu=False, trigger_exit=False):
    print(f"\n{message}")
    if trigger_exit:
        return "trigger_exit"
    if return_to_menu:
        return "return_to_menu"
    return None

def prompt_to_open_report(report_path, description=None):
    report_path_obj = Path(report_path) if report_path else None
    if not report_path_obj or not report_path_obj.exists():
        return

    desc = description or report_path_obj.name
    try:
        wants_open = questionary.confirm(
            f"Open {desc}? ({report_path_obj})", default=False
        ).ask()
    except KeyboardInterrupt:
        handle_cancel("Report opening prompt interrupted.")
        return

    if wants_open:
        try:
            webbrowser.open(report_path_obj.resolve().as_uri())
            print(f"Opening {report_path_obj}...")
        except Exception as e:
            print(f"Unable to open {report_path_obj}: {e}")

# Malformed JSON Checker
def check_and_report_malformed_json(df_to_check, output_dir_path):
    json_columns_to_check = ['Filters', 'Formatting'] 
    malformed_rows_indices = []
    error_details_for_file = [] 
    print("\nChecking for malformed JSON in 'Filters', 'Formatting' columns...")
    for idx, row_data in df_to_check.iterrows():
        row_has_at_least_one_malformed_json_column = False
        for col_name in json_columns_to_check: 
            if col_name not in row_data: continue
            json_str_original_val = row_data.get(col_name) 
            if pd.isna(json_str_original_val): json_str_for_parsing = "" 
            else: json_str_for_parsing = str(json_str_original_val)
            json_str_stripped = json_str_for_parsing.strip()
            if not json_str_stripped: continue
            if json_str_stripped.lower() == "null": continue
            if json_str_stripped.lower() == "nan": continue 
            if json_str_stripped == '[]' and col_name in ['Filters', 'Formatting']: continue
            try:
                if not isinstance(json_str_original_val, (dict, list)): json.loads(json_str_for_parsing) 
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
                    if start <= e.pos < len(string_for_context) : 
                        context_snippet = (string_for_context[start:e.pos] + f" >>>>{char_at_pos}<<<< " + string_for_context[e.pos+1:end])
                    elif e.pos == len(string_for_context): context_snippet = string_for_context[start:end] + " >>>>[END_OF_STRING]<<<< "
                    else: context_snippet = string_for_context[start:end]
                    if start > 0: context_snippet = "..." + context_snippet
                    if end < len(string_for_context): context_snippet = context_snippet + "..."
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

# Load and validate CSV (for main source data)
def load_source_data_csv():
    global main_df
    script_dir = Path(__file__).parent 
    output_dir_for_errors = script_dir / 'outputs' 
    output_dir_for_errors.mkdir(exist_ok=True) 
    
    try:
        p_input_str = questionary.path("Path to Sitetracker CSV export (or directory containing it):", 
                                     default=str(script_dir)).ask()
        if p_input_str is None: return handle_cancel("Source data loading cancelled.", return_to_menu=True)
    except KeyboardInterrupt: return handle_cancel("Source data loading interrupted.", return_to_menu=True)

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
            main_df = None 
            return False
        main_df = temp_df 
        main_df._source_file_name = path_to_load_main_csv.name 
        print(f"Successfully loaded {len(main_df)} trackers from '{path_to_load_main_csv.name}'.")
        
        check_and_report_malformed_json(main_df, output_dir_for_errors) 
        
        return True 
    except FileNotFoundError:
        print(f"Error: File not found: {path_to_load_main_csv}")
    except pd.errors.EmptyDataError:
        print(f"Error: File is empty: {path_to_load_main_csv.name}")
    except Exception as e:
        filename_for_error = path_to_load_main_csv.name if isinstance(path_to_load_main_csv, Path) else str(path_to_load_main_csv)
        print(f"Error loading CSV '{filename_for_error}': {e}")
    main_df = None 
    return False

# Load swap pairs from a CSV
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
    except FileNotFoundError: print(f"Error: Swap pairs CSV not found: {path}"); return None
    except pd.errors.EmptyDataError: print(f"Error: Swap pairs CSV '{path.name}' is empty."); return None
    except Exception as e: print(f"Error reading swap pairs CSV '{path.name}': {e}"); return None

# Text helpers
def remove_field_from_text(text_content, field_api_path_to_remove):
    if not field_api_path_to_remove or not field_api_path_to_remove.strip(): return str(text_content)
    items = [item.strip() for item in str(text_content).split(',') if item.strip()]
    kept_items = [item for item in items if item != field_api_path_to_remove]
    return ', '.join(kept_items)

def remove_key_value_entry(kv_string, key_to_remove, separator='='):
    if not kv_string or not key_to_remove:
        return str(kv_string)
    items = [item.strip() for item in str(kv_string).split(',') if item.strip()]
    kept_items = []
    for item in items:
        parts = item.split(separator, 1)
        if len(parts) == 2:
            key = parts[0].strip()
            if key != key_to_remove:
                kept_items.append(item)
        else: 
            kept_items.append(item)
    return ', '.join(kept_items)

def swap_field_in_text(text, old_exact_api, new_exact_api):
    return re.sub(r"\b" + re.escape(old_exact_api) + r"\b", new_exact_api, text)

def add_fields_to_list(text, fields_to_add_list_canonical):
    items = [i.strip() for i in text.split(',') if i.strip()]
    for f_canon in fields_to_add_list_canonical:
        if f_canon not in items: items.append(f_canon)
    return ','.join(items)

def generate_sitetracker_filter_label(full_api_path):
    if not full_api_path: return ""
    final_segment = full_api_path.split('.')[-1]
    label_root = final_segment
    if final_segment.endswith('__c'): label_root = final_segment[:-3]
    elif final_segment.endswith('__r'): label_root = final_segment[:-3]
    return label_root.replace('_', ' ').title()

def get_sitetracker_filter_sobject(full_api_path, base_tracker_sobject_name):
    if not full_api_path: return base_tracker_sobject_name 
    parts = full_api_path.split('.')
    if len(parts) == 1: return base_tracker_sobject_name 
    else:
        object_determining_relationship_name = parts[-2]
        if object_determining_relationship_name.endswith('__r'):
            return object_determining_relationship_name[:-3] + "__c"
        else: return object_determining_relationship_name

def find_contextual_occurrences_of_field(text_to_search, canonical_field_name):
    if not text_to_search or not canonical_field_name: return []
    pattern = rf"\b((?:[a-zA-Z0-9_]+__r\.)*{re.escape(canonical_field_name)})\b"
    found_paths = set()
    matches = re.findall(pattern, str(text_to_search)) 
    for match in matches: found_paths.add(match) 
    return sorted(list(found_paths))

def update_logic(logic_str, removed_positions):
    if not logic_str: return ''
    tokens = re.split(r'(\bAND\b|\bOR\b|\(|\)|\b\d+\b)', logic_str)
    new_tokens = []
    for tok in tokens:
        t = tok.strip()
        if t.isdigit():
            pos = int(t)
            if pos not in removed_positions:
                shift = sum(1 for r_pos in removed_positions if r_pos < pos)
                new_tokens.append(str(pos - shift))
        elif t.upper() in ('AND', 'OR') or t in ('(', ')'): new_tokens.append(t.upper())
    collapsed, prev = [], None
    for t_col in new_tokens:
        if t_col == prev and t_col in ('AND', 'OR'): continue
        collapsed.append(t_col); prev = t_col
    digits = [tok_d for tok_d in collapsed if tok_d.isdigit()]
    unique_digits = sorted(set(digits), key=int)
    if len(unique_digits) == 1: return unique_digits[0]
    s_logic = ' '.join(collapsed)
    s_logic = re.sub(r'\(\s*', '(', s_logic); s_logic = re.sub(r'\s*\)', ')', s_logic)
    s_logic = re.sub(r'\b(AND|OR)\s*$', '', s_logic, flags=re.IGNORECASE)
    s_logic = re.sub(r'^\s*(AND|OR)\s+', '', s_logic, flags=re.IGNORECASE)
    return s_logic.strip()

def update_query(query_str, contextual_paths_to_remove): 
    sel_match = re.match(r"(?i)(SELECT\s+)(.*?)(\s+FROM\s+)", query_str)
    if sel_match:
        before_select, select_items_str, after_select = sel_match.groups()
        fields_in_select = [f.strip() for f in select_items_str.split(',')]
        cleaned_select_fields = [
            expr for expr in fields_in_select 
            if not (expr in contextual_paths_to_remove or 
                    any(expr.startswith(rem_path + ".") for rem_path in contextual_paths_to_remove))
        ]
        query_str = before_select + ",".join(cleaned_select_fields) + after_select + query_str[sel_match.end():]
    m = re.match(r"(.*?WHERE\s+)(.*?)(\s+ORDER\s+BY.*|$)", query_str, flags=re.IGNORECASE | re.DOTALL)
    if not m: return query_str
    prefix_w, condition_p, suffix_o = m.group(1), m.group(2), m.group(3)
    parts_cond = re.split(r"(\bAND\b|\bOR\b)", condition_p, flags=re.IGNORECASE)
    new_cond_parts = []
    idx_p = 0
    while idx_p < len(parts_cond):
        part_c = parts_cond[idx_p].strip()
        if part_c.upper() in ('AND', 'OR'):
            if new_cond_parts and new_cond_parts[-1].upper() not in ('AND', 'OR'): new_cond_parts.append(part_c.upper())
            idx_p += 1; continue
        condition_segment_should_be_removed = False
        for f_rem_contextual in contextual_paths_to_remove:
            if re.search(rf"\b{re.escape(f_rem_contextual)}\b", part_c, flags=re.IGNORECASE):
                condition_segment_should_be_removed = True; break 
        if condition_segment_should_be_removed:
            if new_cond_parts and new_cond_parts[-1].upper() in ('AND', 'OR'): new_cond_parts.pop()
        else: new_cond_parts.append(part_c)
        idx_p += 1
    if new_cond_parts and new_cond_parts[-1].upper() in ('AND', 'OR'): new_cond_parts.pop()
    if not new_cond_parts:
        prefix_no_w = query_str[:m.start(1)] + prefix_w.strip()[:-5].rstrip()
        return (prefix_no_w + suffix_o).strip()
    s_cond = ' '.join(new_cond_parts)
    s_cond = re.sub(r'\s+', ' ', s_cond).strip()
    s_cond = re.sub(r'^\s*(AND|OR)\s+', '', s_cond, flags=re.IGNORECASE)
    s_cond = re.sub(r'\s+(AND|OR)\s*$', '', s_cond, flags=re.IGNORECASE)
    if not s_cond:
        prefix_no_w = query_str[:m.start(1)] + prefix_w.strip()[:-5].rstrip()
        return (prefix_no_w + suffix_o).strip()
    if not (s_cond.startswith("(") and s_cond.endswith(")") and s_cond.count("(") == s_cond.count(")")):
        s_cond = f"({s_cond})"
    return f"{prefix_w.strip()} {s_cond} {suffix_o.strip()}".strip()

# --- START: UNIFIED MASTER AUDIT FUNCTION and WRAPPER ---
def master_audit(df_to_audit, canonical_fields_to_check, detailed_report=False):
    cols_to_search = ['Fields','Filters','Logic','Query','Formatting','OrderBy(Long)','ResizeMap','Label Map']
    
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
                if not col_content_str: continue
                contextual_paths_in_this_col = find_contextual_occurrences_of_field(col_content_str, canonical_field_to_audit)
                for path in contextual_paths_in_this_col:
                    all_found_contextual_paths_in_row.add(path)
                    if col_name_audit == 'Filters' and path in found_in_filters_structurally: pass
                    else: columns_where_found_map.setdefault(path, []).append(col_name_audit)

            if all_found_contextual_paths_in_row:
                row_had_any_audit_match = True
                if detailed_report:
                    for contextual_path in sorted(list(all_found_contextual_paths_in_row)):
                        col_key_prefix = f"{canonical_field_to_audit} (as {contextual_path})"
                        audit_entry[f"{col_key_prefix} - Columns"] = ', '.join(sorted(list(set(columns_where_found_map.get(contextual_path, [])))))
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
                if not isinstance(filters_list_parsed_for_row, list): filters_list_parsed_for_row = []
            except (json.JSONDecodeError, TypeError): filters_list_parsed_for_row = []
        if canonical_fields_to_remove: 
            for f_rem_canonical in canonical_fields_to_remove: 
                if filters_list_parsed_for_row is not None:
                    for f_dict in filters_list_parsed_for_row:
                        if isinstance(f_dict, dict) and f_dict.get('field',''):
                            field_in_filter = f_dict.get('field','')
                            if field_in_filter == f_rem_canonical or field_in_filter.endswith("." + f_rem_canonical):
                                modified_cols.extend(['Filters', 'Logic']); break 
                for col_name in ['Fields', 'Logic', 'Query', 'Formatting', 'OrderBy(Long)', '.ResizeMap', 'Label Map']:
                    if col_name == 'Filters' and 'Filters' in modified_cols: continue
                    text_content = str(row_data.get(col_name, ''))
                    if find_contextual_occurrences_of_field(text_content, f_rem_canonical):
                        modified_cols.append(col_name)
        if swap_map_input: 
            for old_api_full_path, new_api_full_path in swap_map_input.items(): 
                new_field_exists = False 
                if filters_list_parsed_for_row is not None:
                    if any(isinstance(f_d,dict) and f_d.get('field','') == new_api_full_path for f_d in filters_list_parsed_for_row): new_field_exists = True
                if not new_field_exists: 
                    fields_str_check = str(row_data.get('Fields',''))
                    if re.search(rf"\b{re.escape(new_api_full_path)}\b", fields_str_check): new_field_exists = True
                field_to_detect = old_api_full_path
                if filters_list_parsed_for_row is not None:
                    if any(isinstance(f_d,dict) and f_d.get('field','') == field_to_detect for f_d in filters_list_parsed_for_row):
                        modified_cols.append('Filters') 
                        if new_field_exists: modified_cols.append('Logic')
                for col_name in ['Fields', 'Logic', 'Query', 'Formatting', 'OrderBy(Long)', 'ResizeMap', 'Label Map']:
                    if col_name == 'Filters' and 'Filters' in modified_cols: continue
                    if re.search(rf"\b{re.escape(field_to_detect)}\b", str(row_data.get(col_name, ''))): modified_cols.append(col_name)
        if canonical_fields_to_add: 
            current_flds_val = str(row_data.get('Fields',''))
            items_in_flds = [i.strip() for i in current_flds_val.split(',') if i.strip()]
            if any(f_add_canon not in items_in_flds for f_add_canon in canonical_fields_to_add):
                modified_cols.append('Fields')
        if modified_cols: mods[idx] = sorted(set(modified_cols))
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
                        if isinstance(f_list_check, list) and any(isinstance(fd,dict) and fd.get('field','') == new_api for fd in f_list_check):
                            new_field_exists_in_row = True
                except (json.JSONDecodeError, TypeError): pass
                if not new_field_exists_in_row:
                    fields_col_str = str(modified_df_copy.loc[idx, 'Fields'] if pd.notna(modified_df_copy.loc[idx, 'Fields']) else '')
                    if re.search(rf"\b{re.escape(new_api)}\b", fields_col_str): new_field_exists_in_row = True

                if new_field_exists_in_row:
                    print(f"  Row {idx}: New swap field '{new_api}' already exists. Converting swap of '{old_api}' to its removal.")
                    if old_api not in effective_row_remove_list_for_processing:
                         effective_row_remove_list_for_processing.append(old_api)
                else: 
                    effective_row_swap_map[old_api] = new_api

        # Finalize the unique list of contextual paths to remove
        effective_row_remove_list_for_processing = sorted(list(set(effective_row_remove_list_for_processing)))

        # --- Apply Removals ---
        if effective_row_remove_list_for_processing:
            print(f"  Row {idx}: Applying removal for contextual paths: {effective_row_remove_list_for_processing}")
            try:
                f_str = str(modified_df_copy.loc[idx, 'Filters'] if pd.notna(modified_df_copy.loc[idx, 'Filters']) else '[]')
                f_list = json.loads(f_str) if f_str.strip() else []
                if not isinstance(f_list, list): f_list = []
            except (json.JSONDecodeError, TypeError): f_list = []
            new_f_list_r, removed_pos_list_r = [], []
            for i, f_dict in enumerate(f_list, 1):
                fld_val = f_dict.get('field', '') if isinstance(f_dict, dict) else ''
                if fld_val in effective_row_remove_list_for_processing: 
                    removed_pos_list_r.append(i)
                else: new_f_list_r.append(f_dict)
            modified_df_copy.loc[idx, 'Filters'] = json.dumps(new_f_list_r, separators=(',',':')) if new_f_list_r else '[]'
            if removed_pos_list_r: 
                orig_log_r = str(modified_df_copy.loc[idx, 'Logic'] if pd.notna(modified_df_copy.loc[idx, 'Logic']) else '')
                modified_df_copy.loc[idx, 'Logic'] = update_logic(orig_log_r, removed_pos_list_r)
            orig_q_r = str(modified_df_copy.loc[idx, 'Query'] if pd.notna(modified_df_copy.loc[idx, 'Query']) else '')
            if orig_q_r: modified_df_copy.loc[idx, 'Query'] = update_query(orig_q_r, effective_row_remove_list_for_processing)
            
            for col_r_name in ['Fields', 'OrderBy(Long)']:
                curr_v_r = str(modified_df_copy.loc[idx, col_r_name] if pd.notna(modified_df_copy.loc[idx, col_r_name]) else '')
                for contextual_f_rem in effective_row_remove_list_for_processing:
                     curr_v_r = remove_field_from_text(curr_v_r, contextual_f_rem) 
                modified_df_copy.loc[idx, col_r_name] = curr_v_r
            
            for col_r_name in ['ResizeMap', 'Label Map']:
                curr_kv_r = str(modified_df_copy.loc[idx, col_r_name] if pd.notna(modified_df_copy.loc[idx, col_r_name]) else '')
                separator = '=' 
                if col_r_name == 'Label Map' and ':' in curr_kv_r and '=' not in curr_kv_r.split(',')[0]: separator = ':' 
                
                # For key-value, the key is usually the canonical name, not the full path.
                # Extract the final segment from each path to remove.
                canonical_keys_to_remove = {path.split('.')[-1] for path in effective_row_remove_list_for_processing}
                for key_to_remove in canonical_keys_to_remove:
                    curr_kv_r = remove_key_value_entry(curr_kv_r, key_to_remove, separator=separator)
                modified_df_copy.loc[idx, col_r_name] = curr_kv_r
            
            try: 
                fmt_s_r = str(modified_df_copy.loc[idx, 'Formatting'] if pd.notna(modified_df_copy.loc[idx, 'Formatting']) else '[]')
                fmt_l_r = json.loads(fmt_s_r) if fmt_s_r.strip() else []
                if not isinstance(fmt_l_r, list): fmt_l_r = []
            except (json.JSONDecodeError, TypeError): fmt_l_r = []
            cleaned_fmt_l_r = [obj for obj in fmt_l_r if isinstance(obj, dict) and not any(
                (isinstance(f_d,dict) and f_d.get('field','') in effective_row_remove_list_for_processing) 
                for f_d in obj.get('filters',[]) if isinstance(obj.get('filters'),list)
            )]
            modified_df_copy.loc[idx, 'Formatting'] = json.dumps(cleaned_fmt_l_r, separators=(',',':')) if cleaned_fmt_l_r else '[]'
        
        # --- Apply Swaps ---
        if effective_row_swap_map: 
            for old_api, new_api in effective_row_swap_map.items():
                for col_to_swap in ['Fields', 'Filters', 'Logic', 'Query', 'Formatting', 'OrderBy(Long)', 'ResizeMap', 'Label Map']:
                    current_col_val_str = str(modified_df_copy.loc[idx, col_to_swap] if pd.notna(modified_df_copy.loc[idx, col_to_swap]) else '')
                    if col_to_swap == 'Filters' and old_api in current_col_val_str: 
                        try:
                            if current_col_val_str.strip():
                                temp_filters_list = json.loads(current_col_val_str)
                                if isinstance(temp_filters_list, list):
                                    made_structural_filter_swap = False
                                    for filter_item_dict in temp_filters_list:
                                        if isinstance(filter_item_dict, dict) and filter_item_dict.get('field') == old_api:
                                            filter_item_dict['field'] = new_api
                                            filter_item_dict['label'] = generate_sitetracker_filter_label(new_api)
                                            filter_item_dict['sobject'] = get_sitetracker_filter_sobject(new_api, base_tracker_sobject_name)
                                            made_structural_filter_swap = True
                                    if made_structural_filter_swap:
                                        modified_df_copy.loc[idx, col_to_swap] = json.dumps(temp_filters_list, separators=(',',':'))
                                        continue 
                        except (json.JSONDecodeError, TypeError) as e_json:
                            print(f"Warning: Filters JSON processing error for swap (row {idx}, field {old_api}): {e_json}. Text swap fallback.")
                        except KeyError as e_key: 
                             print(f"Warning: Missing 'ObjectName' for Filter swap (row {idx}, field {old_api}): {e_key}. Text swap fallback.")
                        modified_df_copy.loc[idx, col_to_swap] = swap_field_in_text(current_col_val_str, old_api, new_api) 
                    else: 
                        modified_df_copy.loc[idx, col_to_swap] = swap_field_in_text(current_col_val_str, old_api, new_api)
        
        # --- Apply Additions ---
        if add_list_cmd: 
            cols_to_add_fields_to = ['Fields', 'OrderBy(Long)']
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

# CLI
def main_loop():
    global main_df
    script_dir = Path(__file__).parent
    output_dir = script_dir / 'outputs'
    output_dir.mkdir(exist_ok=True)

    print("--- Sitetracker Tracker Modifier Tool ---")
    
    load_result = load_source_data_csv()
    if load_result is not True: 
        if load_result == "trigger_exit": return 
        print("Failed to load initial source data. Exiting program.")
        return
        
    while True: 
        print("\n--- Main Menu ---")
        current_source_file = getattr(main_df, '_source_file_name', 'N/A')
        if main_df is not None:
             print(f"Currently loaded data: {len(main_df)} trackers from '{current_source_file}'.")
        else:
            print(f"No data currently loaded. Please load source data.")

        main_menu_choices = [ Choice("Load New Source Data", value="load_data") ]
        if main_df is not None: 
            main_menu_choices.extend([
                Choice("Remove Fields (from Audit file)", value="remove"), 
                Choice("Swap Fields", value="swap"),
                Choice("Add Field", value="add"), 
                Choice("Audit Fields", value="audit"),
            ])
        main_menu_choices.append(Choice("Exit", value="exit"))
        
        chosen_action = None 
        try:
            chosen_action = questionary.select("Select action:", choices=main_menu_choices).ask()
        except KeyboardInterrupt: chosen_action = handle_cancel("Action selection interrupted.", return_to_menu=True)
        
        if chosen_action is None or chosen_action == "exit": 
            if handle_cancel("Exiting tool.", trigger_exit=True) == "trigger_exit": break 
        if chosen_action == "return_to_menu": continue 
        if chosen_action == "trigger_exit": break

        if chosen_action == "load_data":
            load_result = load_source_data_csv()
            if load_result == "trigger_exit": break
            continue 

        if main_df is None: 
            print("No data loaded. Please load source data first."); continue

        removal_plan_from_audit = {} 
        field_swap_map_cmd = {}
        fields_to_add_list_cmd = []
        operation_flow_control = None 

        try: 
            if chosen_action == "remove":
                try:
                    print("\nSelect an audit CSV file to specify which fields to remove from which trackers.")
                    audit_csv_path_str = questionary.path("Path to Audit CSV (or directory):", default=str(output_dir)).ask()
                    if audit_csv_path_str is None: operation_flow_control = handle_cancel("Removal from audit file cancelled.", return_to_menu=True)
                    else:
                        audit_path_obj = Path(audit_csv_path_str)
                        selected_audit_file = None
                        if audit_path_obj.is_dir():
                            audit_files = sorted([f for f in audit_path_obj.glob("*.csv") if f.is_file()])
                            if not audit_files: print(f"No audit CSVs found in {audit_path_obj}."); operation_flow_control="return_to_menu"
                            else:
                                audit_choices = [Choice(title=f.name, value=f) for f in audit_files]
                                audit_choices.insert(0, Choice("<Cancel>", None))
                                selected_audit_file = questionary.select("Select audit file:", choices=audit_choices).ask()
                                if selected_audit_file is None: operation_flow_control = handle_cancel("Audit file selection cancelled.", return_to_menu=True)
                        elif audit_path_obj.is_file() and audit_path_obj.suffix.lower() == ".csv":
                            selected_audit_file = audit_path_obj
                        else: print("Invalid path for audit CSV."); operation_flow_control="return_to_menu"

                        if selected_audit_file and operation_flow_control != "return_to_menu":
                            print(f"Parsing audit file: {selected_audit_file.name}...")
                            audit_df = pd.read_csv(selected_audit_file, dtype={'Tracker Name Id': str})
                            if 'Tracker Name Id' not in audit_df.columns:
                                print("Audit file is missing the required 'Tracker Name Id' column."); operation_flow_control="return_to_menu"
                            else:
                                for _, audit_row in audit_df.iterrows():
                                    tracker_id = audit_row['Tracker Name Id']
                                    if pd.isna(tracker_id): continue
                                    fields_to_remove_for_this_tracker = []
                                    for col_header in audit_df.columns:
                                        match = re.match(r".* \(as (.*)\) - Columns$", col_header)
                                        if match and pd.notna(audit_row[col_header]):
                                            contextual_path = match.group(1)
                                            fields_to_remove_for_this_tracker.append(contextual_path)
                                    if fields_to_remove_for_this_tracker:
                                        removal_plan_from_audit[tracker_id] = sorted(list(set(fields_to_remove_for_this_tracker)))
                                if not removal_plan_from_audit:
                                    print("No removal actions could be parsed from the audit file."); operation_flow_control = "return_to_menu"
                except KeyboardInterrupt: operation_flow_control = handle_cancel("Field removal input interrupted.", return_to_menu=True)

            elif chosen_action == "swap":
                try:
                    swap_input_method_result = questionary.select("How to provide swap pairs?",
                        choices=[Choice("Manually enter pairs", "manual"), Choice("Load from CSV", "csv"), Choice("Cancel", "cancel_swap")]).ask()
                    if swap_input_method_result is None or swap_input_method_result == "cancel_swap": operation_flow_control = handle_cancel("Swap operation cancelled.", return_to_menu=True)
                    else: 
                        swap_input_method = swap_input_method_result 
                        if swap_input_method == "manual":
                            print("\nEnter 'OldFullFieldAPI,NewFullFieldAPI' (these should be full contextual paths). Blank to end.")
                            while True: 
                                try:
                                    manual_pair_str = questionary.text("Pair (blank to end):").ask()
                                except KeyboardInterrupt: manual_pair_str = handle_cancel("Swap pair entry interrupted.", return_to_menu=True); break
                                if manual_pair_str == "return_to_menu": operation_flow_control = "return_to_menu"; break
                                if manual_pair_str is None : operation_flow_control = handle_cancel("Swap pair entry cancelled.", return_to_menu=True); break 
                                if not manual_pair_str.strip(): break
                                try:
                                    old_f, new_f = [x.strip() for x in manual_pair_str.split(',')]
                                    if old_f and new_f: 
                                        if old_f in field_swap_map_cmd: print(f"Warning: Overwriting mapping for '{old_f}'. New: '{new_f}'.")
                                        field_swap_map_cmd[old_f] = new_f
                                    else: print("Invalid: Both old and new fields required.")
                                except ValueError: print("Invalid format. Use 'Old,New'.")
                            if operation_flow_control == "return_to_menu": break 
                        elif swap_input_method == "csv":
                            try:
                                swap_csv_path_str = questionary.path("Path to swap pairs CSV (or directory):", default=str(script_dir)).ask()
                            except KeyboardInterrupt: swap_csv_path_str = handle_cancel("Swap CSV selection interrupted.", return_to_menu=True)
                            if swap_csv_path_str == "return_to_menu": operation_flow_control = "return_to_menu"
                            elif swap_csv_path_str is None: operation_flow_control = handle_cancel("Swap CSV selection cancelled.", return_to_menu=True)
                            else:
                                swap_csv_path_obj = Path(swap_csv_path_str)
                                selected_swap_csv_file_path = None 
                                if swap_csv_path_obj.is_dir():
                                    swap_csv_files = sorted([f for f in swap_csv_path_obj.glob("*.csv") if f.is_file()])
                                    if not swap_csv_files: print(f"No CSVs in {swap_csv_path_obj}."); operation_flow_control = "return_to_menu"
                                    elif len(swap_csv_files) == 1: selected_swap_csv_file_path = swap_csv_files[0]; print(f"Auto-selected swap CSV: {selected_swap_csv_file_path.name}")
                                    else:
                                        swap_file_choices = [Choice(title=c.name, value=c) for c in swap_csv_files]
                                        swap_file_choices.insert(0, Choice(title="<Cancel selection>", value=None))
                                        try:
                                            selected_swap_obj = questionary.select("Select swap CSV:", choices=swap_file_choices).ask()
                                        except KeyboardInterrupt: selected_swap_obj = handle_cancel("Swap CSV list selection interrupted.", return_to_menu=True)
                                        if selected_swap_obj == "return_to_menu": operation_flow_control = "return_to_menu"
                                        elif selected_swap_obj is None: operation_flow_control = handle_cancel("Swap CSV selection from directory cancelled.", return_to_menu=True)
                                        else: selected_swap_csv_file_path = selected_swap_obj
                                elif swap_csv_path_obj.is_file() and swap_csv_path_obj.suffix.lower() == ".csv": selected_swap_csv_file_path = swap_csv_path_obj
                                else: print(f"Invalid swap CSV path: {swap_csv_path_obj}"); operation_flow_control = "return_to_menu"
                                if selected_swap_csv_file_path and operation_flow_control != "return_to_menu":
                                    loaded_map = load_swap_pairs_csv(selected_swap_csv_file_path) 
                                    if loaded_map: field_swap_map_cmd = loaded_map
                                    else: print(f"Failed to load swap pairs from {selected_swap_csv_file_path.name}."); operation_flow_control = "return_to_menu"
                                elif operation_flow_control != "return_to_menu" and selected_swap_csv_file_path is None: 
                                    operation_flow_control = "return_to_menu"
                        if not field_swap_map_cmd and operation_flow_control is None : print("No swap pairs defined."); operation_flow_control = "return_to_menu"
                except KeyboardInterrupt: operation_flow_control = handle_cancel("Swap setup interrupted.", return_to_menu=True)
            elif chosen_action == "add":
                try:
                    add_fields_str = questionary.text("Fields to add (comma-separated canonical API names):").ask()
                    if add_fields_str is None: operation_flow_control = handle_cancel("Field addition input cancelled.", return_to_menu=True)
                    else:
                        fields_to_add_list_cmd = [f.strip() for f in add_fields_str.split(',') if f.strip()]
                        if not fields_to_add_list_cmd: print("No fields specified for addition."); operation_flow_control = "return_to_menu"
                except KeyboardInterrupt: operation_flow_control = handle_cancel("Field addition input interrupted.", return_to_menu=True)
            elif chosen_action == "audit":
                try:
                    audit_input_fields_str = questionary.text("Audit canonical field names (e.g., Status__c) (comma-separated):").ask()
                    if audit_input_fields_str is None: operation_flow_control = handle_cancel("Audit input cancelled.", return_to_menu=True)
                    else:
                        canonical_fields_for_audit = [f.strip() for f in audit_input_fields_str.split(',') if f.strip()]
                        if not canonical_fields_for_audit: print("No fields for audit.")
                        else: 
                            audit_data_rows = master_audit(main_df, canonical_fields_for_audit, detailed_report=True)
                            if not audit_data_rows: print("No trackers found containing any of the specified audit fields.")
                            else:
                                audit_out_df = pd.DataFrame(audit_data_rows)
                                fixed_cols = ['Index', 'Tracker Name Id', 'Tracker Name', 'Owner ID', 'ObjectName']
                                dynamic_cols = sorted([col for col in audit_out_df.columns if col not in fixed_cols])
                                audit_out_df = audit_out_df[fixed_cols + dynamic_cols]
                                ts_audit = datetime.now().strftime("%Y%m%d%H%M%S")
                                audit_f_path = output_dir / f"audit_{ts_audit}.csv"
                                audit_out_df.to_csv(audit_f_path, index=False, encoding='utf-8-sig')
                                print(f"Audit saved to {audit_f_path}")
                                prompt_to_open_report(audit_f_path, description="audit report")
                    operation_flow_control = "return_to_menu"
                except KeyboardInterrupt: operation_flow_control = handle_cancel("Audit input interrupted.", return_to_menu=True)
        
        except KeyboardInterrupt: 
            operation_flow_control = handle_cancel(f"Input for {chosen_action} interrupted.", return_to_menu=True)
        
        if operation_flow_control == "return_to_menu": continue 
        if operation_flow_control == "trigger_exit": break 
        
        final_indices_to_process = []
        if chosen_action == "remove":
            if not removal_plan_from_audit: print("No removal actions defined."); continue
            print(f"\n--- Confirm Removal Plan ---")
            print(f"The audit file specifies removals for {len(removal_plan_from_audit)} unique trackers.")
            tracker_ids_from_plan = list(removal_plan_from_audit.keys())
            main_df_ids_str = main_df['Tracker Name Id'].astype(str) 
            matched_indices = main_df[main_df_ids_str.isin(tracker_ids_from_plan)].index.tolist()
            unmatched_ids = [tid for tid in tracker_ids_from_plan if not main_df_ids_str.isin([tid]).any()]
            if unmatched_ids: print(f"Warning: {len(unmatched_ids)} IDs from audit file not found in current data and will be skipped.")
            if not matched_indices: print("No trackers from the audit file matched the currently loaded data."); continue

            print(f"Matched trackers found in current data: {len(matched_indices)}")
            print(f"Displaying up to {min(len(matched_indices), 10)} trackers below:")
            for i, idx in enumerate(matched_indices[:10]):
                tracker_id = main_df.loc[idx, 'Tracker Name Id']
                tracker_name = main_df.loc[idx, 'Tracker Name']
                fields_to_remove = removal_plan_from_audit.get(tracker_id, [])
                print(f"  - Index: {idx}, Name: {tracker_name[:40]}, Fields to remove: {fields_to_remove}")
            if len(matched_indices) > 10: print(f"  ...and {len(matched_indices) - 10} more trackers.")
            try:
                confirm = questionary.confirm(f"Proceed with these removals on {len(matched_indices)} trackers?", default=False).ask()
                if confirm is None or not confirm: operation_flow_control = handle_cancel("Removal cancelled at confirmation.", return_to_menu=True)
            except KeyboardInterrupt: operation_flow_control = handle_cancel("Confirmation interrupted.", return_to_menu=True)
            if operation_flow_control == "return_to_menu": continue
            final_indices_to_process = matched_indices

        else: # For Swap, Add
            if not field_swap_map_cmd and not fields_to_add_list_cmd:
                print("No fields/operations defined for modification.")
                continue 
            
            # Since remove is now driven by audit file, we need a way to get indices for swap/add.
            # We will use the existing selection mechanism for these.
            
            # Create a temporary remove_list for identify_modifications, as it's not used by swap/add.
            # This is simpler than making identify_modifications handle different actions.
            
            identified_modifications_cmd = identify_modifications(
                main_df,
                canonical_fields_to_remove=[],
                swap_map_input=field_swap_map_cmd,
                canonical_fields_to_add=fields_to_add_list_cmd
            )
            if not identified_modifications_cmd: print("No trackers require modification."); continue

            try:
                mods_to_display = identified_modifications_cmd
                print(f"Trackers identified for modification: {len(mods_to_display)}")
                choices = [Choice(title=f"Index {idx}: {main_df.loc[idx, 'Tracker Name']} -> Modifies: {', '.join(cols)}", value=idx) for idx, cols in mods_to_display.items()]
                choices.insert(0, Choice("<Select All>", "__ALL__"))
                
                selected = questionary.checkbox("Select trackers to modify:", choices=choices).ask()
                if selected is None: operation_flow_control = handle_cancel("Selection cancelled.", return_to_menu=True)
                elif "__ALL__" in selected: final_indices_to_process = list(mods_to_display.keys())
                else: final_indices_to_process = [s for s in selected if isinstance(s, int)]

            except KeyboardInterrupt: operation_flow_control = handle_cancel("Tracker selection interrupted.", return_to_menu=True)
            if operation_flow_control == "return_to_menu": continue
        
        if not final_indices_to_process:
            print("No trackers ultimately selected for modification.")
            continue 
            
        ts_mod = datetime.now().strftime("%Y%m%d%H%M%S")
        try:
            modify_trackers(main_df, final_indices_to_process, 
                            removal_plan_from_audit if chosen_action == 'remove' else [], 
                            field_swap_map_cmd, fields_to_add_list_cmd, output_dir, ts_mod)
        except KeyboardInterrupt:
            handle_cancel("Modification process interrupted by user.", return_to_menu=False) 
            print("Note: If process was interrupted during writing, files might be incomplete.")
            break 
        
        print("-" * 30) 

if __name__ == "__main__":
    try:
        main_loop() 
    except SystemExit: 
        pass 
    except Exception as e: 
        print(f"\nAn unexpected critical error occurred in the main execution block: {e}")
        import traceback
        traceback.print_exc()
    print("\nTracker Modifier Tool session has concluded.")