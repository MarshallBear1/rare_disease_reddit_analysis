#!/usr/bin/env python3
# 07_filter_empty_body_labels.py
# -------------------------------------------------------------------
# Remove any rows whose `body` column is empty or missing,
# producing a cleaned CSV (e.g., labels_<model>-v2.csv).
# Usage:
#   python scripts/07_filter_empty_body_labels.py \
#     -i data/flat/model_labels/labels_MODEL.csv \
#     -o data/flat/model_labels/labels_MODEL-v2.csv
# -------------------------------------------------------------------
import argparse
from pathlib import Path
import pandas as pd

def main(input_path: str, output_path: str):
    df = pd.read_csv(input_path)
    # Keep only rows where `body` is non-null and not just whitespace
    mask = df['body'].fillna('').astype(str).str.strip() != ''
    df_filtered = df.loc[mask]

    # Ensure output directory exists
    out_path = Path(output_path)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    df_filtered.to_csv(out_path, index=False)
    print(f"[DONE] wrote {len(df_filtered):,} rows → {out_path}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Filter out rows with empty `body` field"
    )
    parser.add_argument(
        '-i', '--input', required=True,
        help='Path to input CSV'
    )
    parser.add_argument(
        '-o', '--output', required=True,
        help='Path to output filtered CSV'
    )
    args = parser.parse_args()
    main(args.input, args.output)
