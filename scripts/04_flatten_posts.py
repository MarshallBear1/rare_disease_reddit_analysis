#!/usr/bin/env python
# ────────────────────────────────────────────────────────────────────
# 04b_reflatten_with_parent.py
# -------------------------------------------------------------------
# Re-flatten every raw *.jsonl / *.jsonl.gz (one Reddit submission per
# line, with nested comments) into a single CSV suitable for latency
# analysis.  Ensures both post_id and parent_id are present.
# -------------------------------------------------------------------
import json, gzip, csv, glob, pathlib
from tqdm import tqdm

RAW_GLOB   = "data/raw/*.jsonl*"          # ← matches .jsonl and .jsonl.gz
OUT_CSV    = "data/flat/all_posts_comments.csv"

FIELDNAMES = [
    "subreddit", "post_id", "comment_id", "parent_id", "is_post",
    "author", "created_utc", "score", "num_comments", "body"
]

# ─── helpers ───────────────────────────────────────────────────────
def flatten_one_thread(rec: dict, writer):
    """Write one submission plus every comment/reply to writer."""
    sub  = rec["subreddit"]
    pid  = rec["id"]                     # submission ID
    # ---------- submission row -------------------------------------
    writer.writerow({
        "subreddit":    sub,
        "post_id":      pid,
        "comment_id":   "",              # none – this *is* the post
        "parent_id":    f"t3_{pid}",     # canonical self-parent
        "is_post":      1,
        "author":       rec.get("author", "[deleted]"),
        "created_utc":  rec["created_utc"],
        "score":        rec.get("score", 0),
        "num_comments": rec.get("num_comments", 0),
        "body":         rec.get("selftext", "").replace("\n", " ").strip(),
    })

    # ---------- depth-first over comment tree ----------------------
    stack = rec.get("comments", [])
    while stack:
        c = stack.pop()
        writer.writerow({
            "subreddit":    sub,
            "post_id":      pid,
            "comment_id":   c["id"],
            "parent_id":    c.get("parent_id", ""),
            "is_post":      0,
            "author":       c.get("author", "[deleted]"),
            "created_utc":  c["created_utc"],
            "score":        c.get("score", 0),
            "num_comments": 0,
            "body":         c.get("body", "").replace("\n", " ").strip(),
        })
        stack.extend(c.get("replies", []))   # push replies onto stack

# ─── main ──────────────────────────────────────────────────────────
def main():
    pathlib.Path("data/flat").mkdir(parents=True, exist_ok=True)

    with open(OUT_CSV, "w", newline="", encoding="utf-8") as fh:
        w = csv.DictWriter(fh, fieldnames=FIELDNAMES)
        w.writeheader()

        files = sorted(glob.glob(RAW_GLOB))
        for fp in tqdm(files, desc="flattening", unit="file"):
            opener = gzip.open if fp.endswith(".gz") else open
            with opener(fp, "rt", encoding="utf-8") as jf:
                for line in jf:
                    flatten_one_thread(json.loads(line), w)

    print(f"[DONE] wrote → {OUT_CSV}")

if __name__ == "__main__":
    main()
