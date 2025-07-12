#!/usr/bin/env python
# ────────────────────────────────────────────────────────────────────
# 06_multi_label_consensus.py  –  majority-vote merge of model labels
#
# Example:
#   python scripts/06_multi_label_consensus.py \
#          -i data/flat/model_labels \
#          -o data/flat/model_labels/labels_consensus.csv
#
# The input *can be* either
#   • a directory  (every file whose name starts with ‘labels_’ is loaded), OR
#   • an explicit comma-separated list of CSV files.
# -------------------------------------------------------------------
import argparse
import pathlib
import sys
from collections import Counter, defaultdict

import pandas as pd
from tqdm import tqdm

# ────────────────────────────────────────────────────────────────────
# helper
# ────────────────────────────────────────────────────────────────────
def normalise_labels(cell: str | float) -> list[str]:
    """Split “a;b;c” → ['a','b','c']; tolerate NaN/empty."""
    if not isinstance(cell, str):
        return []
    return [lbl.strip() for lbl in cell.split(";") if lbl.strip()]

# ────────────────────────────────────────────────────────────────────
def read_label_file(fp: pathlib.Path) -> pd.DataFrame:
    """Return DataFrame with index (subreddit, post_id) and one ‘labels’ column"""
    df = pd.read_csv(fp, dtype=str, keep_default_na=False)
    key_cols = ["subreddit", "post_id"]
    if not all(k in df.columns for k in key_cols):
        sys.exit(f"❌  {fp.name} missing key columns {key_cols}")
    df = df.set_index(key_cols)
    df = df[["labels"]]          # keep only the label column
    df.columns = [fp.stem]       # e.g. ‘labels_gpt-4o-mini’ → same header
    return df

# ────────────────────────────────────────────────────────────────────
def consensus_vote(row: pd.Series, thr: int) -> str:
    """
    row = one Pandas row with N model-columns each holding “a;b”.
    Return labels joined by ‘;’ that reach ≥ thr votes across models.
    """
    votes: Counter[str] = Counter()
    for cell in row.dropna():
        votes.update(normalise_labels(cell))
    keep = [lab for lab, n in votes.items() if n >= thr]
    return ";".join(sorted(keep))

# ────────────────────────────────────────────────────────────────────
def main(in_arg: str, out_csv: str, vote_threshold: int) -> None:
    in_paths: list[pathlib.Path]
    p = pathlib.Path(in_arg)
    if p.is_dir():
        in_paths = sorted(p.glob("labels_*.csv"))
    else:
        in_paths = [pathlib.Path(x.strip()) for x in in_arg.split(",")]

    if len(in_paths) < 2:
        sys.exit("❌  Need at least TWO per-model CSVs for a consensus")

    # --- load & merge ------------------------------------------------
    dfs = [read_label_file(fp) for fp in in_paths]
    merged = pd.concat(dfs, axis=1, join="inner")   # keep only rows present in ALL files
    print(f"[INFO] {len(merged):,} rows present in ALL {len(dfs)} files")

    # --- compute consensus ------------------------------------------
    merged["consensus_labels"] = tqdm(
        merged.apply(consensus_vote, axis=1, thr=vote_threshold),
        total=len(merged),
        desc="Consensus"
    )

    # --- attach back the non-label columns (take them from the 1st file) ---
    base_df = pd.read_csv(in_paths[0], dtype=str, keep_default_na=False)
    base_df = base_df.set_index(["subreddit", "post_id"])
    final = base_df.join(merged[["consensus_labels"]])

    # --- statistics / sanity check ----------------------------------
    n_empty = (final["consensus_labels"] == "").sum()
    uniq_labels = set(
        lab
        for row in final["consensus_labels"].str.split(";").dropna()
        for lab in row
        if lab
    )
    print(
        f"\nSummary\n───────\n"
        f"Posts processed:  {len(final):8,}\n"
        f"Empty consensus:  {n_empty:8,}\n"
        f"Distinct labels:  {len(uniq_labels):8,} → {sorted(uniq_labels)}"
    )

    # --- write -------------------------------------------------------
    out_fp = pathlib.Path(out_csv)
    out_fp.parent.mkdir(parents=True, exist_ok=True)
    final.reset_index().to_csv(out_fp, index=False)
    print(f"\n[SAVED] {out_fp}\n")

# ────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    ap = argparse.ArgumentParser(
        description="Majority-vote merge of model label CSVs"
    )
    ap.add_argument(
        "-i", "--input",
        required=True,
        help=(
            "Directory containing per-model CSVs  *or*  a comma-separated list "
            "of CSV files (each file must contain ‘labels’ column plus "
            "subreddit & post_id)."
        ),
    )
    ap.add_argument(
        "-o", "--output",
        default="data/flat/model_labels/labels_consensus.csv",
        help="Output CSV path"
    )
    ap.add_argument(
        "-t", "--threshold",
        type=int,
        default=2,
        help="Votes (models) needed for a label to be kept"
    )
    args = ap.parse_args()

    main(args.input, args.output, args.threshold)
