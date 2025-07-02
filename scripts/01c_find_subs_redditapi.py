"""
01c_find_subs_redditapi.py
------------------------------------------------------------
Live-search Reddit for every rare-disease term and append to
data/meta/candidate_subreddits.csv .

• Exact search_by_name  ➜  fallback fuzzy search
• Handles private / banned subs (403 / 404)
• Resumes safely if CSV already exists
• All prints are pure ASCII (Windows-safe)
"""

import os
import csv
import time
import pathlib
import re

from dotenv import load_dotenv, find_dotenv
import praw
from prawcore.exceptions import Forbidden, NotFound

# ---------- paths and credentials ---------------------------------
BASE_DIR = pathlib.Path(__file__).resolve().parents[1]
VOCAB    = BASE_DIR / "data" / "cleaned" / "ordo_terms.tsv"
META_DIR = BASE_DIR / "data" / "meta"
META_DIR.mkdir(parents=True, exist_ok=True)
OUT_CSV  = META_DIR / "candidate_subreddits.csv"

load_dotenv(find_dotenv())                 # read .env

reddit = praw.Reddit(
    client_id     = os.getenv("REDDIT_CLIENT_ID"),
    client_secret = os.getenv("REDDIT_CLIENT_SECRET"),
    username      = os.getenv("REDDIT_USERNAME"),
    password      = os.getenv("REDDIT_PASSWORD"),
    user_agent    = os.getenv("REDDIT_USER_AGENT"),
)
reddit.read_only = True

# ---------- build vocabulary --------------------------------------
NUMERIC_RE = re.compile(r"^[0-9\s/.,-]+$")

terms = [
    t.strip()
    for t in VOCAB.read_text(encoding="utf-8").splitlines()
    if t.strip() and not NUMERIC_RE.fullmatch(t.strip())
]

print(f"[SCAN] {len(terms):,} non-numeric terms to query")

# ---------- resume support ---------------------------------------
if OUT_CSV.exists():
    with OUT_CSV.open(encoding="utf-8", errors="ignore") as fh:
        next(fh, None)                     # skip header if present
        seen = {line.split(",")[0].lower() for line in fh}
    csv_mode = "a"
    print(f"[SCAN] Resuming - {len(seen)} subs already collected")
else:
    seen = set()
    csv_mode = "w"

# ---------- main loop --------------------------------------------
with OUT_CSV.open(csv_mode, newline="", encoding="utf-8") as fh:
    writer = csv.DictWriter(
        fh,
        fieldnames=["name", "subscribers", "public_description", "matched_term"],
    )
    if csv_mode == "w":
        writer.writeheader()

    for idx, term in enumerate(terms, 1):
        # exact title / name
        try:
            hits = list(
                reddit.subreddits.search_by_name(term, exact=True, include_nsfw=False)
            )
        except NotFound:
            hits = []
        except Exception as exc:
            print("[WARN] exact-match error:", term, exc)
            hits = []

        # fallback fuzzy search
        if not hits:
            try:
                hits = reddit.subreddits.search(term, limit=3)
            except Exception as exc:
                print("[WARN] fuzzy-search error:", term, exc)
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
            print(f"[SCAN] {idx}/{len(terms)} terms - {len(seen)} unique subs")

        time.sleep(1.1)   # stay under 60 requests/min

print(f"[SCAN] DONE: {len(seen)} subs → {OUT_CSV}")
