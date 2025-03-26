import pandas as pd
import numpy as np
from utils import merge_group, add_normalized_fields

# Load dataset
file_path = 'veridion_product_deduplication_challenge.snappy.parquet'
data = pd.read_parquet(file_path)

# Function to print duplicates for verification
def print_duplicates(data_group):
    output_file = 'potential_duplicates.txt'

    with open(output_file, 'w', encoding='utf-8') as f:
        f.write(f"Showing details for first {min(20, len(data_group))} duplicate groups:\n\n")

        for name, group in list(data_group)[:20]:
            f.write(f"Group: '{name}' —> {len(group)} entries\n\n")
            f.write(group.to_string(index=False))
            f.write("\n\n" + "-" * 120 + "\n\n")


# Function to convert lists/dicts before parquet file
def fix_before_parq(df):
    for col in df.columns:
        def convert(x):
            if isinstance(x, (list, tuple, np.ndarray)):
                if len(x) == 0:
                    return '[]'
                try:
                    return '[' + ', '.join(str(item) for item in x) + ']'
                except Exception:
                    return str(x)
            elif isinstance(x, dict):
                return str(x)
            else:
                return x

        if df[col].dropna().apply(lambda x: isinstance(x, (list, tuple, dict, np.ndarray))).any():
            df[col] = df[col].apply(convert)

    return df


def to_bool_safe(val):
    if isinstance(val, bool):
        return val
    if isinstance(val, str):
        val_lower = val.strip().lower()
        if val_lower in {'true', 'yes', '1'}:
            return True
        elif val_lower in {'false', 'no', '0'}:
            return False
    return pd.NA


# Add normalized name column, and also normalised brand name
data = add_normalized_fields(data)

# Group by normalized main columns and filter to get dups
grouped = data.groupby(['normalized_name', 'normalized_title', 'normalized_brand'])

# Get non dups and concatenate them
non_duplicate_groups = [group for _, group in grouped if len(group) == 1]
non_duplicates = pd.concat(non_duplicate_groups)
non_duplicates = add_normalized_fields(non_duplicates)

filtered_groups = [group for _, group in grouped if len(group) > 1]

    # Show how many groups are to be merged #
# print(f" Total groups merged: {len(filtered_groups)}")

# Merge all duplicates
merged_rows = [merge_group(group) for group in filtered_groups]
merged_entries = pd.DataFrame(merged_rows)
merged_entries = add_normalized_fields(merged_entries)

# Merge all groups and fix some fields
data_post_merge = pd.concat([non_duplicates, merged_entries], ignore_index=True)
data_post_merge['eco_friendly'] = data_post_merge['eco_friendly'].apply(to_bool_safe).astype('boolean')
data_post_merge['manufacturing_year'] = pd.to_numeric(
    data_post_merge['manufacturing_year'], errors='coerce'
).astype('Int32')
data_post_merge['eco_friendly'] = data_post_merge['eco_friendly'].astype(object)


    # Verify if there are dups in final_data #
# grouped2 = data_post_merge.groupby(['normalized_name','normalized_title', 'normalized_brand'])
# filtered2_groups = [(key, group) for key, group in grouped2 if len(group) > 1]
# print_duplicates(filtered2_groups)

final_data = fix_before_parq(data_post_merge)

    # Parquet file with normalised fields #
# final_data.to_parquet('final_deduplicated.parquet', index=False)

# Parquet file with no normalised fields
final_data_nonormal = final_data.drop(columns=['normalized_name', 'normalized_title', 'normalized_brand'])
final_data_nonormal.to_parquet('final_clean_deduplicated.parquet', index=False)