import pandas as pd
import re
from utils import merge_group

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

def add_normalized_fields(df):
    df['normalized_name'] = df['product_name'].apply(normalize_text)
    df['normalized_title'] = df['product_title'].apply(normalize_text)
    df['normalized_brand'] = df['brand'].apply(normalize_text)
    return df

# Function to print duplicates for verification
def print_duplicates(data_group):
    output_file = 'potential_duplicates.txt'

    with open(output_file, 'w', encoding='utf-8') as f:
        # Show up to 10 duplicates
        for name, group in list(data_group)[0:10]:
            f.write(f" Group: '{name}' —> {len(group)} entries\n")
            f.write(group[['product_name', 'product_title','brand', 'price', 'page_url', 'description']].to_string(index=False))
            f.write("\n\n" + "-"*100 + "\n\n")

# Function to convert lists/dicts to strings before parquet file
def lists_or_dicts_to_string(df):
    for col in df.columns:
        if df[col].apply(lambda x: isinstance(x, (list, dict))).any():
            df[col] = df[col].apply(lambda x: str(x) if isinstance(x, (list, dict)) else x)
    return df

# Add normalized name column, and also normalised brand name
data = add_normalized_fields(data)

# Group by normalized main columns and filter to get dups
grouped = data.groupby(['normalized_name','normalized_title', 'normalized_brand'])
filtered_groups = [(key, group) for key, group in grouped if len(group) > 1]

# Get non dups and concatenate them
non_duplicate_groups = [group for _, group in grouped if len(group) == 1]
non_duplicates = pd.concat(non_duplicate_groups)
non_duplicates = add_normalized_fields(non_duplicates)

# Merge all groups
merged_entries = pd.concat([merge_group(group) for _, group in filtered_groups])
merged_entries = add_normalized_fields(merged_entries)


data_post_merge = pd.concat([non_duplicates, merged_entries], ignore_index=True)

# Verify if there are dups in final_data
# grouped2 = data_post_merge.groupby(['normalized_name','normalized_title', 'normalized_brand'])
# filtered2_groups = [(key, group) for key, group in grouped2 if len(group) > 1]
# print_duplicates(filtered2_groups)

# Parquet file with normalised fields
final_data_stringed = lists_or_dicts_to_string(data_post_merge)
final_data_stringed.to_parquet('final_deduplicated.parquet', index=False)

# Parquet file with no normalised fields
final_data_nonormal = final_data_stringed.drop(columns=['normalized_name', 'normalized_title', 'normalized_brand'])
final_data_stringed.to_parquet('final_clean_deduplicated.parquet', index=False)