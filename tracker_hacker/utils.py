import re
import webbrowser
from pathlib import Path

import questionary


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


def remove_field_from_text(text_content, field_api_path_to_remove):
    if not field_api_path_to_remove or not field_api_path_to_remove.strip():
        return str(text_content)
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
        if f_canon not in items:
            items.append(f_canon)
    return ','.join(items)


def generate_sitetracker_filter_label(full_api_path):
    if not full_api_path:
        return ""
    final_segment = full_api_path.split('.')[-1]
    label_root = final_segment
    if final_segment.endswith('__c'):
        label_root = final_segment[:-3]
    elif final_segment.endswith('__r'):
        label_root = final_segment[:-3]
    return label_root.replace('_', ' ').title()


def get_sitetracker_filter_sobject(full_api_path, base_tracker_sobject_name):
    if not full_api_path:
        return base_tracker_sobject_name
    parts = full_api_path.split('.')
    if len(parts) == 1:
        return base_tracker_sobject_name
    object_determining_relationship_name = parts[-2]
    if object_determining_relationship_name.endswith('__r'):
        return object_determining_relationship_name[:-3] + "__c"
    return object_determining_relationship_name


def find_contextual_occurrences_of_field(text_to_search, canonical_field_name):
    if not text_to_search or not canonical_field_name:
        return []
    pattern = rf"\b((?:[a-zA-Z0-9_]+__r\.)*{re.escape(canonical_field_name)})\b"
    found_paths = set()
    matches = re.findall(pattern, str(text_to_search))
    for match in matches:
        found_paths.add(match)
    return sorted(list(found_paths))


def update_logic(logic_str, removed_positions):
    if not logic_str:
        return ''
    tokens = re.split(r'(\bAND\b|\bOR\b|\(|\)|\b\d+\b)', logic_str)
    new_tokens = []
    for tok in tokens:
        t = tok.strip()
        if t.isdigit():
            pos = int(t)
            if pos not in removed_positions:
                shift = sum(1 for r_pos in removed_positions if r_pos < pos)
                new_tokens.append(str(pos - shift))
        elif t.upper() in ('AND', 'OR') or t in ('(', ')'):
            new_tokens.append(t.upper())
    collapsed, prev = [], None
    for t_col in new_tokens:
        if t_col == prev and t_col in ('AND', 'OR'):
            continue
        collapsed.append(t_col)
        prev = t_col
    digits = [tok_d for tok_d in collapsed if tok_d.isdigit()]
    unique_digits = sorted(set(digits), key=int)
    if len(unique_digits) == 1:
        return unique_digits[0]
    s_logic = ' '.join(collapsed)
    s_logic = re.sub(r'\(\s*', '(', s_logic)
    s_logic = re.sub(r'\s*\)', ')', s_logic)
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
    if not m:
        return query_str
    prefix_w, condition_p, suffix_o = m.group(1), m.group(2), m.group(3)
    parts_cond = re.split(r"(\bAND\b|\bOR\b)", condition_p, flags=re.IGNORECASE)
    new_cond_parts = []
    idx_p = 0
    while idx_p < len(parts_cond):
        part_c = parts_cond[idx_p].strip()
        if part_c.upper() in ('AND', 'OR'):
            if new_cond_parts and new_cond_parts[-1].upper() not in ('AND', 'OR'):
                new_cond_parts.append(part_c.upper())
            idx_p += 1
            continue
        condition_segment_should_be_removed = False
        for f_rem_contextual in contextual_paths_to_remove:
            if re.search(rf"\b{re.escape(f_rem_contextual)}\b", part_c, flags=re.IGNORECASE):
                condition_segment_should_be_removed = True
                break
        if condition_segment_should_be_removed:
            if new_cond_parts and new_cond_parts[-1].upper() in ('AND', 'OR'):
                new_cond_parts.pop()
        else:
            new_cond_parts.append(part_c)
        idx_p += 1
    if new_cond_parts and new_cond_parts[-1].upper() in ('AND', 'OR'):
        new_cond_parts.pop()
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
