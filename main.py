import pandas as pd
import re

# Load dataset
file_path = 'veridion_product_deduplication_challenge.snappy.parquet'
data = pd.read_parquet(file_path)

# Normalize function for product names
def normalize_text(text):
    if pd.isna(text):
        return ''
    lower_string = text.lower()
    
    # Remove all punctuation except words and space
    no_punc_string = re.sub(r'[^\w\s]','', lower_string)
    return no_punc_string

# Function to print duplicates for verification
def print_duplicates(data_group):
    output_file = 'potential_duplicates.txt'

    with open(output_file, 'w', encoding='utf-8') as f:
        # Show up to 10 duplicates
        for name, group in list(data_group)[0:10]:
            f.write(f" Group: '{name}' —> {len(group)} entries\n")
            f.write(group[['applicability','product_name', 'product_title','brand', 'price', 'description']].to_string(index=False))
            f.write("\n\n" + "-"*100 + "\n\n")

def merge_group(group):
    merged = {}

    for col in group.columns:
        # Drop NaN values
        series = group[col].dropna()

        # Flatten lists to strings
        flat_vals = []
        for val in series:
            if isinstance(val, list):
                flat_vals.append(str(val))
            else:
                flat_vals.append(str(val).strip())

        # Remove empty strings
        flat_vals = [val for val in flat_vals if val != '']

        if not flat_vals:
            merged[col] = None
        elif len(set(flat_vals)) == 1: # Same values, keep one
             merged[col] = flat_vals[0]
        else:
            merged[col] = '; '.join(sorted(set(flat_vals)))

    return pd.Series(merged)


# Add normalized name column, and also normalised brand name
data['normalized_name'] = data['product_name'].apply(normalize_text)
data['normalized_title'] = data['product_title'].apply(normalize_text)
data['normalized_brand'] = data['brand'].apply(normalize_text)

# Group by normalized main columns and filter to get dups
grouped = data.groupby(['normalized_name','normalized_title', 'normalized_brand'])
filtered_groups = [(key, group) for key, group in grouped if len(group) > 1]

# 'grouped' for complete data, 'filtered_groups' to check merger [_______________]
merged_entries = pd.DataFrame([merge_group(group) for _, group in filtered_groups])
merged_entries['normalized_name'] = merged_entries['product_name'].apply(normalize_text)
merged_entries['normalized_title'] = merged_entries['product_title'].apply(normalize_text)
merged_entries['normalized_brand'] = merged_entries['brand'].apply(normalize_text)

grouped2 = merged_entries.groupby(['normalized_name', 'normalized_title', 'normalized_brand'])

print_duplicates(grouped2)
