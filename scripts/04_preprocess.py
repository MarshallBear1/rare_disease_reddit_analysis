#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
scripts/04_preprocess.py
————————————————————————————————————————————
• load raw *.jsonl from data/raw/
• basic text hygiene   (lower-case, de-emoji, strip urls)
• compute engagement = score + log(comments+1) + 2*awards
• compute latency     = minutes to first comment  (NaN if none)
• keep posts 2015-01-01…today, drop deleted authors / empty text
• save   → data/clean/<sub>.parquet      (pyarrow backend)
"""

import os, json, re, pathlib, datetime as dt, math, typing as T
import pandas as pd
from tqdm import tqdm

RAW_DIR   = pathlib.Path("data/raw")
CLEAN_DIR = pathlib.Path("data/clean")
MIN_DATE  = dt.datetime(2015, 1, 1, tzinfo=dt.timezone.utc)

url_re   = re.compile(r"https?://\S+")
emoji_re = re.compile(
    "["                       # remove most emojis – keeps ♀ ♂ etc.
    "\U0001F600-\U0001F64F"
    "\U0001F300-\U0001F5FF"
    "\U0001F680-\U0001F6FF"
    "\U0001F1E0-\U0001F1FF"
    "]+",
    flags=re.UNICODE,
)

def clean(text: str) -> str:
    text = url_re.sub(" ", text or "")
    text = emoji_re.sub(" ", text)
    text = re.sub(r"\s+", " ", text.lower()).strip()
    return text

def enrich(row: dict) -> dict:
    """Add engagement + latency; drop if outside date window."""
    created = dt.datetime.fromtimestamp(
        row["created_utc"], tz=dt.timezone.utc
    )
    if created < MIN_DATE:
        return None

    body = f'{row.get("title","")} {row.get("selftext","")}'
    body = clean(body)
    if len(body) < 20 or row.get("author") in {"[deleted]", None}:
        return None

    row["clean_text"] = body
    row["engagement"] = (
        row.get("score", 0)
        + math.log(row.get("num_comments", 0) + 1)
        + 2 * row.get("total_awards", 0)
    )

    first_reply_ts = row.get("first_comment_utc")
    row["latency_min"] = (
        (first_reply_ts - row["created_utc"]) / 60
        if first_reply_ts
        else None
    )
    return row

def process_file(path: pathlib.Path) -> None:
    out_pq = CLEAN_DIR / (path.stem + ".parquet")
    if out_pq.exists():
        print(f"✓ {path.stem:25}  already done")
        return

    records: list[dict] = []
    with path.open(encoding="utf-8") as f:
        for line in f:
            raw = json.loads(line)
            ok = enrich(raw)
            if ok:
                records.append(ok)

    if not records:
        print(f"⚠ {path.stem:25}  nothing kept")
        return

    df = pd.DataFrame.from_records(records)
    df.to_parquet(out_pq, index=False)
    print(f"→ {path.stem:25}  {len(df):5,d} rows")

def main() -> None:
    CLEAN_DIR.mkdir(exist_ok=True, parents=True)
    files = sorted(RAW_DIR.glob("*.jsonl"))
    for p in tqdm(files, unit="sub"):
        process_file(p)

if __name__ == "__main__":
    main()
