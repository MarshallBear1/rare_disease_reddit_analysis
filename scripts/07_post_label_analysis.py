#!/usr/bin/env python
# 07_post_label_analysis.py
# ---------------------------------------------------------------
import pandas as pd, numpy as np, matplotlib.pyplot as plt
from pathlib import Path

LABELS   = Path("data/flat/model_labels/labels_consensus.csv")
POSTS    = Path("data/flat/selected_high_signal_posts.csv")
OUT_DIR  = Path("results");  OUT_DIR.mkdir(exist_ok=True, parents=True)

# 1️⃣  merge
posts = pd.read_csv(POSTS, dtype=str)
labs  = pd.read_csv(LABELS, dtype=str)
df    = posts.merge(labs[["subreddit","post_id","consensus_labels"]],
                    on=["subreddit","post_id"], how="left")
df.to_parquet(OUT_DIR/"posts_annotated.parquet", index=False)

# 2️⃣  label frequency sanity plot
labs_long = (df["consensus_labels"]
             .str.split(";", expand=True)
             .stack().value_counts())
labs_long.plot(kind="barh")
plt.gca().invert_yaxis(); plt.tight_layout()
plt.savefig(OUT_DIR/"label_freq.png", dpi=300)
plt.close()

# 3️⃣  latency summary per label
lat = df.assign(label=df["consensus_labels"].str.split(";")) \
        .explode("label")
lat["latency_min"] = pd.to_numeric(lat["latency_min"], errors="coerce")
summary = (lat.groupby("label")["latency_min"]
              .agg(n_posts="size",
                   median_min="median",
                   iqr_min=lambda x: np.subtract(*np.percentile(
                                     x.dropna(), [75,25]))))
summary.to_csv(OUT_DIR/"latency_by_label.csv")
print(summary.head())

print("\n[OK] results in", OUT_DIR.resolve())
