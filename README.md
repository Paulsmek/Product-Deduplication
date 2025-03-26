  Veridion Product Deduplication — README
-Objective-
  The goal of this project is to identify and merge duplicate product entries in a dataset of scraped product data. These duplicates often arise due to inconsistent naming, branding, color formats, or feature descriptions.

-How Duplicates Are Identified-
  To identify potential duplicates, I normalize key textual fields: product_name, product_title, brand

  Normalization is done by: lowercasing all text, removing punctuation
  This helps unify entries like: "SuperCool Widget", "Supercool widget", and "SuperCool-Widget!" -> all normalize to -> "supercool widget"
  These three normalized fields are then used as the grouping keys. If two or more entries share the same normalized values, they are considered potential duplicates.

-How Duplicates Are Merged-
  For each group of potential duplicates, I apply column-specific merging strategies:
  
  -General Columns:
    Remove null or empty values; Flatten list or dict fields into clean strings;
    If values differ, I join them with ";" to preserve all unique info
    If all values are identical → use the single value directly

  -List-like Columns (miscellaneous_features, intended_industries, etc.)
    These fields contain lists or arrays, so I:
    Merge all values into a single list
    Remove duplicates
    Return an empty list [] if all entries were empty
    Preserve original structure in output by converting them to strings like:
    "[Feature A, Feature B]"

  -Color Field
    Originally I tried to normalize color using structured fields like:
    [{"original": "Titanium", "simple": "Gray"}, {"original": "Blue", "simple": "Blue"}] -> ['Titanim Gray', 'Blue']
    But this introduced inconsistency across the dataset, so I dropped this idea.
    Now, colors are treated just like any other list-like field and merged normally.

  -Text Fields (description, product_summary)
    I preserve the longest non-empty string from the group — assuming longer descriptions tend to be more informative.

-Output & Validation
  After merging:
    Final deduplicated data is saved to deduplicated_parquets/final_clean_deduplicated.parquet
    Sample outputs are printed to checker_output
    These allow easy side-by-side comparison and validation of merging logic.

-Thought Process
  This challenge required a balance between:
  Precision: Avoiding false positives in deduplication
  Preservation: Keeping important data when merging

  Rather than relying on fuzzy string matching or ML, I focused on simple normalization and semantic grouping,
  trusting that key fields (name/title/brand) are enough to group correctly. Merging logic was made robust and flexible to handle
  messy data like mixed types (str, list, dict, None).

-Folder Structure
├── input/
│   └── veridion_product_deduplication_challenge.snappy.parquet
├── deduplicated_parquets/
│   └── final_clean_deduplicated.parquet
├── checker_output/
│   ├── sample_original_output.txt
│   ├── sample_dedup_output.txt
│   └── potential_duplicates.txt
├── main.py
├── utils.py
├── checker.py
└── README.md
