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
 
    # remove all punctuation except words and space
    no_punc_string = re.sub(r'[^\w\s]','', lower_string)
    return no_punc_string

# Function to print duplicates for verification
def print_duplicates(data_group):
    output_file = 'potential_duplicates.txt'

    with open(output_file, 'w', encoding='utf-8') as f:
        f.write(f"Potential Duplicate Groups (Top {min(10, len(data_group))} shown):\n\n")

        # Show up to 10 duplicates
        for name, group in list(data_group)[0:10]:
            f.write(f" Group: '{name}' —> {len(group)} entries\n")
            f.write(group[['product_name', 'brand', 'price', 'description']].to_string(index=False))
            f.write("\n\n" + "-"*100 + "\n\n")

# Add normalized name column
data['normalized_name'] = data['product_name'].apply(normalize_text)

# Get data by name that appear more than once and exclude ''
dup_data = data[data['normalized_name'].duplicated(keep=False) & (data['normalized_name'] != '')]

# Group by normalized name
grouped = dup_data.groupby('normalized_name')

print_duplicates(grouped)
