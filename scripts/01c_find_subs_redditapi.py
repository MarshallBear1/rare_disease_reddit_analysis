"""
01c_find_subs_redditapi.py
------------------------------------------------------------
Live-search Reddit for every rare-disease term and build
data/meta/candidate_subreddits.csv  with automatic 429 back-off.
"""

import os
import csv
import time
import pathlib
import re

from dotenv import load_dotenv, find_dotenv
import praw
from prawcore import NotFound, Forbidden, TooManyRequests

# ---------- paths / creds ----------------------------------------
BASE  = pathlib.Path(__file__).resolve().parents[1]
VOCAB = BASE / "data" / "cleaned" / "ordo_terms.tsv"
META  = BASE / "data" / "meta"
META.mkdir(parents=True, exist_ok=True)
OUT   = META / "candidate_subreddits.csv"

load_dotenv(find_dotenv())

reddit = praw.Reddit(
    client_id=os.getenv("REDDIT_CLIENT_ID"),
    client_secret=os.getenv("REDDIT_CLIENT_SECRET"),
    username=os.getenv("REDDIT_USERNAME"),
    password=os.getenv("REDDIT_PASSWORD"),
    user_agent=os.getenv("REDDIT_USER_AGENT"),
)
reddit.read_only = True

# ---------- vocabulary -------------------------------------------
NUMERIC_RE = re.compile(r"^[0-9\s/.,-]+$")
terms = [
    t.strip()
    for t in VOCAB.read_text(encoding="utf-8").splitlines()
    if t.strip() and not NUMERIC_RE.fullmatch(t.strip())
]
print(f"[SCAN] {len(terms):,} non-numeric terms to query")

# ---------- resume support ---------------------------------------
if OUT.exists():
    with OUT.open(encoding="utf-8", errors="ignore") as fh:
        next(fh, None)
        seen = {line.split(",")[0].lower() for line in fh}
    mode = "a"
    print(f"[SCAN] Resuming - {len(seen)} subs already collected")
else:
    seen, mode = set(), "w"

# ---------- helper with retry ------------------------------------
def safe_call(func, *args, **kw):
    """Call a PRAW method; back off 70 s on 429."""
    while True:
        try:
            return func(*args, **kw)
        except TooManyRequests as exc:
            print("[BACKOFF] 429 TooManyRequests – sleeping 70 s")
            time.sleep(70)
        except Exception as err:
            # log but return empty list / None so loop continues
            print("[WARN] API error:", err)
            return None

# ---------- main loop --------------------------------------------
with OUT.open(mode, newline="", encoding="utf-8") as fh:
    writer = csv.DictWriter(
        fh,
        fieldnames=["name", "subscribers", "public_description", "matched_term"],
    )
    if mode == "w":
        writer.writeheader()

    for idx, term in enumerate(terms, 1):
        hits = safe_call(
            reddit.subreddits.search_by_name,
            term,
            exact=True,
            include_nsfw=False,
        ) or []

        if not hits:
            hits = safe_call(reddit.subreddits.search, term, limit=3) or []

        for sub in hits:
            name_lc = sub.display_name.lower()
            if name_lc in seen:
                continue
            seen.add(name_lc)

            # subscribers / description may raise 403/404
            try:
                subs_cnt = sub.subscribers
            except (Forbidden, NotFound):
                subs_cnt = 0
            try:
                desc = (sub.public_description or "")[:300]
            except (Forbidden, NotFound):
                desc = ""

            writer.writerow(
                {
                    "name": sub.display_name,
                    "subscribers": subs_cnt,
                    "public_description": desc,
                    "matched_term": term,
                }
            )

        if idx % 60 == 0:
            print(f"[SCAN] {idx}/{len(terms)} terms - {len(seen)} unique subs")

        time.sleep(1.0)  # baseline delay (works with back-off)

print(f"[SCAN] DONE - {len(seen)} subs saved \u2192 {OUT}")
