#!/usr/bin/env python3
# 04b_reflatten_with_parent.py
# ------------------------------------------------------------------
# Re-flatten every raw *.jsonl (one post+thread per line) into one CSV
# with both post_id *and* parent_id filled so we can measure reply latency.
# ------------------------------------------------------------------

import json, gzip, csv, glob, os, pathlib
from tqdm import tqdm

# adjust these paths if needed
RAW_DIR    = "data/raw/*.jsonl*"           # raw dumps (jsonl or jsonl.gz)
OUT_CSV    = "data/flat/all_posts_comments.csv"
FIELDNAMES = [
    "subreddit", "post_id", "comment_id", "parent_id", "is_post",
    "author",    "created_utc", "score",      "num_comments", "body"
]

def flatten_one_thread(rec: dict, writer: csv.DictWriter):
    """Emit one row for the post and one for each nested comment."""
    sub   = rec["subreddit"]
    pid   = rec["id"]
    # write the post row
    writer.writerow({
        "subreddit":    sub,
        "post_id":      pid,
        "comment_id":   "",
        "parent_id":    f"t3_{pid}",
        "is_post":      1,
        "author":       rec.get("author","[deleted]"),
        "created_utc":  rec["created_utc"],
        "score":        rec.get("score",0),
        "num_comments": rec.get("num_comments",0),
        "body":         rec.get("selftext",""
                                ).replace("\n"," ").strip(),
    })

    # depth-first walk of comments
    stack = rec.get("comments", [])
    while stack:
        c = stack.pop()
        cid = c["id"]
        writer.writerow({
            "subreddit":    sub,
            "post_id":      pid,
            "comment_id":   cid,
            "parent_id":    c.get("parent_id", ""),
            "is_post":      0,
            "author":       c.get("author","[deleted]"),
            "created_utc":  c["created_utc"],
            "score":        c.get("score",0),
            "num_comments": 0,
            "body":         c.get("body",""
                                    ).replace("\n"," ").strip(),
        })
        # push any replies onto the stack
        stack.extend(c.get("replies", []))


def main():
    # ensure output folder exists
    pathlib.Path(os.path.dirname(OUT_CSV)).mkdir(parents=True, exist_ok=True)

    with open(OUT_CSV, "w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=FIELDNAMES)
        writer.writeheader()

        files = sorted(glob.glob(RAW_DIR))
        for fp in tqdm(files, desc="flattening raw threads", unit="file"):
            opener = gzip.open if fp.endswith(".gz") else open
            with opener(fp, "rt", encoding="utf-8") as jf:
                for line in jf:
                    rec = json.loads(line)
                    flatten_one_thread(rec, writer)

    print(f"[DONE] wrote → {OUT_CSV}")


if __name__ == "__main__":
    main()
