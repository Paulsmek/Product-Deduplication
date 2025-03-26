import pandas as pd

# Load original and deduplicated data
file_path1 = 'input/veridion_product_deduplication_challenge.snappy.parquet'
file_path2 = 'deduplicated_parquets/final_clean_deduplicated.parquet'

df_original = pd.read_parquet(file_path1)
df_deduped = pd.read_parquet(file_path2)

# Function to print sample entries to a file
def print_samples(dataframe, n, output_file):
    with open(output_file, 'w', encoding='utf-8') as f:
        f.write(f"Showing the first {n} entries from this dataset:\n\n")
        f.write(dataframe.head(n).to_string(index=False))
        f.write("\n\n" + "-" * 120 + "\n\n")
    print(f"✅ Written first {n} entries to: {output_file}")

# Output both original and deduplicated samples
print_samples(df_original, n=50, output_file='checker_output/sample_original_output.txt')
print_samples(df_deduped, n=50, output_file='checker_output/sample_dedup_output.txt')
