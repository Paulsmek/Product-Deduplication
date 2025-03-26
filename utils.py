import pandas as pd
import re
import numpy as np

# Normalize function for product names
def normalize_text(text):
    if pd.isna(text):
        return ''
    lower_string = text.lower()
    return re.sub(r'[^\w\s]', '', lower_string)

def add_normalized_fields(df):
    df['normalized_name'] = df['product_name'].apply(normalize_text)
    df['normalized_title'] = df['product_title'].apply(normalize_text)
    df['normalized_brand'] = df['brand'].apply(normalize_text)
    return df

def detect_list_like_columns(df):
    return [
        col for col in df.columns
        if df[col].dropna().apply(lambda x: isinstance(x, (list, np.ndarray))).any()
    ]

def merge_group(group, list_merge_fields=None):
    if list_merge_fields is None:
        list_merge_fields = detect_list_like_columns(group)

    merged = {}

    for col in group.columns:
        series = group[col].dropna()
        
        if series.empty:
            if col in list_merge_fields:
                merged[col] = []
            else:
                merged[col] = None
            continue

        if col in ['description', 'product_summary']:
            descriptions = series.astype(str).map(str.strip).tolist()
            descriptions = [desc for desc in descriptions if desc]
            merged[col] = max(descriptions, key=len) if descriptions else None
            continue

        if col in list_merge_fields:
            unique_items = set()
            has_non_empty = False

            for val in series:
                if isinstance(val, np.ndarray):
                    val = val.tolist()
                if isinstance(val, list):
                    filtered = [str(item).strip() for item in val if item != '']
                    if filtered:
                        has_non_empty = True
                        unique_items.update(filtered)
                elif isinstance(val, str) and val.strip():
                    has_non_empty = True
                    unique_items.add(val.strip())

            if has_non_empty:
                merged[col] = sorted(unique_items)
            else:
                merged[col] = []
            continue


        flat_vals = []
        for val in series:
            if isinstance(val, np.ndarray):
                val = val.tolist()
            if isinstance(val, list):
                flat_vals.append(', '.join(map(str, val)))
            elif isinstance(val, dict):
                flat_vals.append(str(val))
            else:
                flat_vals.append(str(val).strip())

        flat_vals = [val for val in flat_vals if val != '']

        if not flat_vals:
            merged[col] = None
        elif len(set(flat_vals)) == 1:
            merged[col] = flat_vals[0]
        else:
            merged[col] = '; '.join(sorted(set(flat_vals)))

    return pd.Series(merged)
