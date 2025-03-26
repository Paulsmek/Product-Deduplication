# Veridion Product Deduplication — README

## Objective

The goal of this project is to identify and merge duplicate product entries in a dataset of scraped product data. These duplicates often arise due to inconsistent naming, branding, color formats, or feature descriptions.

---

##  How Duplicates Are Identified

To identify potential duplicates, I normalize key textual fields:

- `product_name`
- `product_title`
- `brand`

### Normalization includes:

- Lowercasing all text
- Removing punctuation

This helps unify entries like: "SuperCool Widget", "Supercool widget", and "SuperCool-Widget!" → all normalize to → "supercool widget"

These three normalized fields are used as **grouping keys**. If two or more entries share the same normalized values, they are considered potential duplicates.

---

##  How Duplicates Are Merged

For each group of potential duplicates, I apply **column-specific merging strategies**:

###  General Columns
- Remove null or empty values
- Flatten list or dict fields into clean strings
- If values differ → join with `; ` to preserve all unique info
- If all values are identical → use the single value directly

---

### List-like Columns
*(e.g., `miscellaneous_features`, `intended_industries`, etc.)*

These fields contain lists or arrays, so I:

- Merge all values into a single list
- Remove duplicates
- Return an empty list `[]` if all entries were empty
- Preserve the structure by converting into strings like: "[Feature A, Feature B]"
---

### Color Field

Originally, I attempted normalization like:
[{"original": "Titanium", "simple": "Gray"}, {"original": "Blue", "simple": "Blue"}] → ['Titanium Gray', 'Blue']

But this introduced inconsistency, so I dropped this idea.

Now, color is treated just like any other list-like field and merged normally.

---
### Text Fields
(e.g., description, product_summary)

I preserve the longest non-empty string from the group
(assuming longer descriptions are more informative)

## Output & Validation
*After merging*:

Final deduplicated data is saved to:
- deduplicated_parquets/final_clean_deduplicated.parquet

Sample outputs for visual inspection are saved in:
- checker_output/sample_original_output.txt
- checker_output/sample_dedup_output.txt
- checker_output/potential_duplicates.txt

These allow easy side-by-side comparison and verification of the deduplication process.

## Thought Process
This challenge required balancing two competing priorities:

- Precision: Avoiding false positives in deduplication
- Preservation: Keeping important data when merging

Rather than relying on fuzzy matching or ML, I opted for simple normalization and semantic grouping. I assumed that product name, title, and brand provide enough signal for identifying duplicates.
Merging logic was written to be robust and flexible, accounting for:

- Mixed types (str, list, dict, None)
- Incomplete or sparse fields
- Inconsistencies in formatting

## 📁 Folder Structure

```text
├── input/
│   └── veridion_product_deduplication_challenge.snappy.parquet
│
├── deduplicated_parquets/
│   └── final_clean_deduplicated.parquet
│
├── checker_output/
│   ├── sample_original_output.txt
│   ├── sample_dedup_output.txt
│   └── potential_duplicates.txt
│
├── main.py
├── utils.py
├── checker.py
└── README.md







