import pandas as pd

def merge_group(group):
    merged = {}

    for col in group.columns:
        # Drop NaN values
        series = group[col].dropna()

        # Skip empty columns
        if series.empty:
            merged[col] = None
            continue

        # Special case: description -> keep the longest
        if col == 'description':
            descriptions = series.astype(str).map(str.strip).tolist()
            descriptions = [desc for desc in descriptions if desc]
            merged[col] = max(descriptions, key=len) if descriptions else None
            continue

        # Default logic for all other fields
        flat_vals = []
        for val in series:
            if isinstance(val, (list, dict)):
                flat_vals.append(str(val))
            else:
                flat_vals.append(str(val).strip())

        # Remove empty strings
        flat_vals = [val for val in flat_vals if val != '']

        if not flat_vals:
            merged[col] = None
        elif len(set(flat_vals)) == 1:
            merged[col] = flat_vals[0]
        else:
            merged[col] = '; '.join(sorted(set(flat_vals)))

    return pd.Series(merged)
