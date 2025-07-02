"""
01c_find_subs_redditapi.py
──────────────────────────────────────────────────────────────
Live-search Reddit for every rare-disease term (ordo_terms.tsv)
and build data/meta/candidate_subreddits.csv

 • Exact match via search_by_name(..., exact=True)
 • Fallback fuzzy search(term, limit=3)
 • Handles private / banned subs (403/404)
 • Resumes if CSV already contains rows
"""

import os, csv, time, pathlib, re
from dotenv import load_dotenv, find_dotenv
import praw
from prawcore.exceptions import Forbidden, NotFound

# ─── paths ────────────────────────────────────────────────────────
BASE   = pathlib.Path(__file__).resolve().parents[1]
VOCAB  = BASE / "data" / "cleaned" / "ordo_terms.tsv"
META   = BASE / "data" / "meta"; META.mkdir(parents=True, exist_ok=True)
OUT    = META / "candidate_subreddits.csv"

# ─── load Reddit creds ────────────────────────────────────────────
load_dotenv(find_dotenv())

reddit = praw.Reddit(
    client_id     = os.getenv("REDDIT_CLIENT_ID"),
    client_secret = os.getenv("REDDIT_CLIENT_SECRET"),
    username      = os.getenv("REDDIT_USERNAME"),
    password      = os.getenv("REDDIT_PASSWORD"),
    user_agent    = os.getenv("REDDIT_USER_AGENT"),
)
reddit.read_only = True

# ─── vocabulary ───────────────────────────────────────────────────
NUMERIC_RE = re.compile(r"^[0-9\s/.,-]+$")
terms = [
    t.strip() for t in VOCAB.read_text(encoding="utf-8").splitlines()
    if t.strip() and not NUMERIC_RE.fullmatch(t.strip())
]
print(f"🔍  {len(terms):,} non-numeric terms to query")

# ─── resume support ───────────────────────────────────────────────
if OUT.exists():
    with OUT.open() as fh:
        next(fh)                              # skip header
        seen = {line.split(",")[0].lower() for line in fh}
    mode = "a"
    print(f"🔄  Resuming — {len(seen)} subs already collected")
else:
    seen = set()
    mode = "w"

# ─── main loop ────────────────────────────────────────────────────
with OUT.open(mode, newline="", encoding="utf-8") as fh:
    writer = csv.DictWriter(
        fh,
        fieldnames=["name", "subscribers", "public_description", "matched_term"],
    )
    if mode == "w":
        writer.writeheader()

    for idx, term in enumerate(terms, 1):
        hits = []
        try:
            hits = list(
                reddit.subreddits.search_by_name(term, exact=True, include_nsfw=False)
            )
        except NotFound:
            pass                                # invalid chars → skip exact step
        except Exception as e:
            print("⚠️  exact-match error:", term, e)

        if not hits:
            try:
                hits = reddit.subreddits.search(term, limit=3)
            except Exception as e:
                print("⚠️  fuzzy-search error:", term, e)
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
                {
                    "name": sub.display_name,
                    "subscribers": subs_count,
                    "public_description": desc,
                    "matched_term": term,
                }
            )

        if idx % 60 == 0:
	print(f"…{idx}/{len(terms)} terms — {len(seen)} unique subs")
