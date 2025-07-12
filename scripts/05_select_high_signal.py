#!/usr/bin/env python
"""
Select “high–signal” Reddit posts per subreddit.

•  Computes a simple engagement score:
       score  +  log1p(num_comments)  +  2×total_awards   (awards=0 if absent)
•  Keeps only subreddits with ≥ MIN_POSTS rows
•  For every remaining subreddit keeps the top TOP_PCT % posts
   (but never more than MAX_PER_SUB)
•  Everything with author == [deleted] / [removed] is discarded first
•  Writes:
      data/flat/selected_high_signal_posts.csv
      data/flat/_audit_high_signal_counts.csv
"""

from __future__ import annotations

import argparse
import math
from pathlib import Path

import numpy as np
import pandas as pd


# --------------------------------------------------------------------------- #
# helper functions
# --------------------------------------------------------------------------- #
def engagement_score(df: pd.DataFrame) -> pd.Series:
    """
    Robust engagement score that works even when columns are missing.
    Ensures we always return a Series aligned to df.index.
    """
    def col_or_zero(name: str) -> pd.Series:
        if name in df.columns:
            return df[name].fillna(0)
        # return a Series of zeros of correct length
        return pd.Series(0, index=df.index, dtype="float64")

    s = col_or_zero("score")
    c = col_or_zero("num_comments")
    a = col_or_zero("total_awards")

    return s + np.log1p(c) + 2 * a


def pick_top_pct(group: pd.DataFrame, *, pct: float, cap: int) -> pd.DataFrame:
    """
    Return the `pct`-percent most-engaged rows of *group*, capped at *cap* rows.
    """
    if group.empty:
        return group

    n_keep = max(1, math.ceil(len(group) * pct / 100))
    n_keep = min(n_keep, cap)
    return group.nlargest(n_keep, "engagement")


# --------------------------------------------------------------------------- #
# main
# --------------------------------------------------------------------------- #
def main() -> None:
    ap = argparse.ArgumentParser(
        description="Select high-signal posts per subreddit",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    ap.add_argument("-i", "--in_path", default="data/flat/all_posts_comments.csv",
                    help="input CSV (plain or .gz)")
    ap.add_argument("-o", "--out_path",
                    default="data/flat/selected_high_signal_posts.csv",
                    help="output CSV")
    ap.add_argument("-m", "--min_posts", type=int, default=10,
                    help="minimum #posts per subreddit to be considered")
    ap.add_argument("-t", "--top_pct", type=float, default=10,
                    help="percentage of top-engagement posts to keep")
    ap.add_argument("-n", "--max_per_sub", type=int, default=20,
                    help="cap per subreddit")
    args = ap.parse_args()

    in_path = Path(args.in_path)
    out_path = Path(args.out_path)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    print(f"[LOAD ] {in_path}")
    df = pd.read_csv(in_path)

    # strip stray blank-name columns (there is one just after “subreddit”)
    df = df.loc[:, df.columns.str.strip().astype(bool)]
    df.columns = df.columns.str.strip()

    # remove deleted / removed authors if column exists
    if "author" in df.columns:
        before = len(df)
        df = df[~df["author"].fillna("").str.lower().isin(
            ["[deleted]", "[removed]", ""])]
        print(f"[CLEAN] dropped {before - len(df):,} deleted / removed authors")

    # compute engagement
    df["engagement"] = engagement_score(df)

    # keep only subreddits with enough posts
    counts = df.groupby("subreddit").size()
    keep_subs = counts[counts >= args.min_posts].index
    print(f"[TRIM ] keeping {len(keep_subs):4} subs ≥ {args.min_posts} posts;"
          f" dropping {len(counts) - len(keep_subs)}")
    posts = df[df["subreddit"].isin(keep_subs)]

    # nothing left?  exit gracefully
    if posts.empty:
        print("[WARN ] nothing left after trimming – check your thresholds")
        return

    # pick high-engagement rows
    top_posts = (
        posts.groupby("subreddit", group_keys=False)
        .apply(pick_top_pct, pct=args.top_pct, cap=args.max_per_sub)
    )
    print(f"[ENG  ] selected {len(top_posts):,} posts in top {args.top_pct:g} % engagement")

    # ------------------------------------------------------------------ #
    # save
    # ------------------------------------------------------------------ #
    try:            # Py ≥3.8
        out_path.unlink(missing_ok=True)
    except TypeError:   # Py 3.7 fallback
        if out_path.exists():
            out_path.unlink()

    print(f"[SAVE ] writing {len(top_posts):,} rows →  {out_path}")
    top_posts.to_csv(out_path, index=False)

    # audit table
    audit_path = out_path.parent / "_audit_high_signal_counts.csv"
    (
        top_posts.groupby("subreddit")
        .size()
        .rename("n_posts")
        .reset_index()
        .to_csv(audit_path, index=False)
    )
    print(f"[AUDIT] per-sub counts  →  {audit_path}")
    print("[DONE ]")


if __name__ == "__main__":
    main()
