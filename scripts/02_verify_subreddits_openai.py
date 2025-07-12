"""
02_verify_subreddits_openai.py
================================================================
Label each candidate subreddit Yes/No via GPT-4o-mini.

Logic
-----
1. Is r/<name> primarily a disease subreddit?
2. If yes, is that disease *rare* (< 1 in 2 000 prevalence)?
Return exactly one keyword:
    rare  → store as yes
    common or not_disease → store as no
    anything else → error

Features
--------
• Opens CSVs in UTF-8 (errors=ignore)  → Windows-safe
• Skips rows already labelled          → resume support
• Prints progress every 50 rows
"""

import csv
import pathlib
import os
import time
from dotenv import load_dotenv, find_dotenv
from openai import OpenAI

# ---------- paths -------------------------------------------------
BASE   = pathlib.Path(__file__).resolve().parents[1]
CAND   = BASE / "data" / "meta" / "candidate_subreddits.csv"
OUT    = BASE / "data" / "meta" / "verified_subreddits.csv"
OUT.parent.mkdir(parents=True, exist_ok=True)

# ---------- OpenAI client ----------------------------------------
load_dotenv(find_dotenv())
client = OpenAI()

PROMPT_TEMPLATE = (
    "You are a medical domain expert.\n"
    "TASK: Classify the subreddit r/{name}.\n"
    "1. Is the community primarily about a disease / medical condition?\n"
    "   If NO → reply exactly: not_disease\n"
    "2. If YES, does the condition meet the standard definition of a RARE "
    "disease (affects fewer than 1 in 2 000 people)?\n"
    "   • If YES → reply exactly: rare\n"
    "   • If NO  → reply exactly: common\n\n"
    "Public description:\n\"{desc}\"\n"
    "Reply with one keyword only: rare, common, or not_disease."
)

# ---------- helper ------------------------------------------------
def label_row(row: dict) -> str:
    """Return yes / no / error based on GPT classification."""
    prompt = PROMPT_TEMPLATE.format(
        name=row["name"], desc=row["public_description"]
    )
    try:
        resp = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[{"role": "user", "content": prompt}],
            temperature=0,
        )
        keyword = resp.choices[0].message.content.strip().lower()
    except Exception as exc:
        print("[WARN] OpenAI error:", exc, flush=True)
        return "error"

    if keyword == "rare":
        return "yes"
    if keyword in {"common", "not_disease"}:
        return "no"
    return "error"

# ---------- main --------------------------------------------------
with CAND.open(encoding="utf-8", errors="ignore") as src, \
     OUT.open("w", newline="", encoding="utf-8") as dst:

    reader = csv.DictReader(src)
    fieldnames = reader.fieldnames + ["gpt_label"]
    writer = csv.DictWriter(dst, fieldnames=fieldnames)
    writer.writeheader()

    total_rows = sum(1 for _ in open(CAND, encoding="utf-8", errors="ignore")) - 1
    src.seek(0); next(reader)  # reset after counting

    yes_count = 0
    for idx, row in enumerate(reader, 1):
        # resume support
        if row.get("gpt_label"):
            if row["gpt_label"] == "yes":
                yes_count += 1
            writer.writerow(row)
            continue

        row["gpt_label"] = label_row(row)
        if row["gpt_label"] == "yes":
            yes_count += 1
        writer.writerow(row)

        if idx % 50 == 0 or idx == total_rows:
            print(
                f"[GPT] {idx}/{total_rows} processed - {yes_count} yes",
                flush=True,
            )

        time.sleep(0.5)  # ~120 req/min

print(f"[GPT] DONE - verified list at {OUT}", flush=True)
