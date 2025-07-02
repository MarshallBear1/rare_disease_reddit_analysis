"""
01c_find_subs_redditapi.py
────────────────────────────────────────────────────────
Live-search Reddit for rare-disease terms.
• Exact search_by_name  → fallback fuzzy search
• Handles 403/404, resumes if CSV exists
• Prints ASCII-only progress lines (Windows safe)
"""

import os, csv, time, pathlib, re
from dotenv import load_dotenv, find_dotenv
import praw
from prawcore.exceptions import Forbidden, NotFound

BASE  = pathlib.Path(__file__).resolve().parents[1]
VOCAB = BASE / "data" / "cleaned" / "ordo_terms.tsv"
META  = BASE / "data" / "meta"; META.mkdir(parents=True, exist_ok=True)
OUT   = META / "candidate_subreddits.csv"

load_dotenv(find_dotenv())
reddit = praw.Reddit(
    client_id     = os.getenv("REDDIT_CLIENT_ID"),
    client_secret = os.getenv("REDDIT_CLIENT_SECRET"),
    username      = os.getenv("REDDIT_USERNAME"),
    password      = os.getenv("REDDIT_PASSWORD"),
    user_agent    = os.getenv("REDDIT_USER_AGENT"),
)
reddit.read_only = True

NUMERIC_RE = re.compile(r"^[0-9\s/.,-]+$")
terms = [
    t.strip() for t in VOCAB.read_text("utf-8").splitlines()
    if t.strip() and not NUMERIC_RE.fullmatch(t.strip())
]
print(f"[SCAN] {len(terms):,} non-numeric terms to query")

if OUT.exists():
    with OUT.open() as fh:
        next(fh)
        seen = {line.split(",")[0].lower() for line in fh}
    mode = "a"
    print(f"[SCAN] Resuming — {len(seen)} subs already collected")
else:
    seen, mode = set(), "w"

with OUT.open(mode, newline="", encoding="utf-8") as fh:
    writer = csv.DictWriter(
        fh, fieldnames=["name", "subscribers", "public_description", "matched_term"]
    )
    if mode == "w":
        writer.writeheader()

    for idx, term in enumerate(terms, 1):
        hits = []
        try:
            hits = list(reddit.subreddits.search_by_name(term, exact=True, include_nsfw=False))
        except NotFound:
            pass
        except Exception as e:
            print("[WARN] exact-match error:", term, e)

        if not hits:
            try:
                hits = reddit.subreddits.search(term, limit=3)
            except Exception as e:
                print("[WARN] fuzzy-search error:", term, e)
                continue

        for sub in hits:
            sname = sub.display_name.lower()
            if sname in seen:
                continue
            seen.add(sname)
            try:
                subs_count = sub.subscribers
            except (Forbidden, NotFound):
                subs_count = 0
            try:
                desc = (sub.public_description or "")[:300]
            except (Forbidden, NotFound):
                desc = ""
            writer.writerow(
                {"name": sub.display_name,
                 "subscribers": subs_count,
                 "public_description": desc,
                 "matched_term": term}
            )

        if idx % 60 == 0:
            print(f"[SCAN] {idx}/{len(terms)} terms — {len(seen)} unique subs")
        time.sleep(1.1)

print(f"[SCAN] DONE: {len(seen)} subs → {OUT}")
