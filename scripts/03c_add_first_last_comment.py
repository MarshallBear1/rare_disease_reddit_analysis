#!/usr/bin/env python3
# ────────────────────────────────────────────────────────────────────
# 03c_add_first_last_comment.py
#
# Add first- and last-comment timestamps to every submission thread
# by hitting Pushshift in bulk.
#
# Usage:  python scripts/03c_add_first_last_comment.py
# -------------------------------------------------------------------
import asyncio, aiohttp, gzip, json, math, pathlib, time
from textwrap import dedent
from tqdm import tqdm

RAW_DIR  = pathlib.Path("data/raw")        # input dumps r_*.jsonl.gz
OUT_DIR  = pathlib.Path("data/raw_fc")     # enriched dumps (“fc” = first comment)
OUT_DIR.mkdir(parents=True, exist_ok=True)

BATCH_SIZE   = 200       # Pushshift limit per request
CONCURRENCY  = 8         # simultaneous HTTP sessions
PUSH_URL     = (
    "https://api.pushshift.io/reddit/comment/search"
    "?link_id={ids}&sort={dir}&sort_type=created_utc&size=1"
)

# ────────────────────────────────────────────────────────────────────
async def fetch_comment_times(session, ids, *, direction):
    """
    direction='asc'  -> first comment
    direction='desc' -> last  comment
    Returns {post_id : created_utc}
    """
    url = PUSH_URL.format(ids=",".join(f"t3_{i}" for i in ids), dir=direction)
    backoff = 2
    while True:
        try:
            async with session.get(url, timeout=40) as r:
                if r.status != 200:
                    raise RuntimeError(r.status)
                data = await r.json()
                return {c["link_id"][3:]: c["created_utc"]
                        for c in data.get("data", [])}
        except Exception:
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 60)

# ────────────────────────────────────────────────────────────────────
async def enrich_file(fp: pathlib.Path):
    out_fp = OUT_DIR / fp.name
    if out_fp.exists():
        print(f"✓ {fp.stem:25} already enriched")
        return

    # 1️⃣  load all submissions (light-weight, comments stripped)
    subs = []
    with gzip.open(fp, "rt", encoding="utf-8") as fh:
        for line in fh:
            rec = json.loads(line)
            if rec.get("is_post", True):      # older dumps may miss the flag
                subs.append(rec)

    batches = [subs[i:i+BATCH_SIZE] for i in range(0, len(subs), BATCH_SIZE)]

    # 2️⃣  async Pushshift look-ups
    async with aiohttp.ClientSession() as sess:
        sem   = asyncio.Semaphore(CONCURRENCY)

        async def _job(batch):
            ids = [s["id"] for s in batch]
            async with sem:
                first = await fetch_comment_times(sess, ids, direction="asc")
                last  = await fetch_comment_times(sess, ids, direction="desc")
                for sub in batch:
                    pid = sub["id"]
                    sub["first_comment_utc"] = first.get(pid)   # ⟨None⟩ if absent
                    sub["last_comment_utc"]  = last.get(pid)

        tasks = [_job(batch) for batch in batches]

        # single progress-bar 👇
        for coro in tqdm(asyncio.as_completed(tasks),
                         total=len(tasks),
                         desc=fp.stem,
                         ncols=80):
            await coro

    # 3️⃣  write gzip
    with gzip.open(out_fp, "wt", encoding="utf-8") as out:
        for rec in subs:
            out.write(json.dumps(rec) + "\n")

    print(f"→ {fp.stem:25}  {len(subs):7,d} posts enriched")

# ────────────────────────────────────────────────────────────────────
async def amain():
    files = sorted(RAW_DIR.glob("r_*.jsonl.gz"))
    if not files:
        print("No raw dumps found in data/raw/")
        return
    await asyncio.gather(*(enrich_file(f) for f in files))

def main():
    asyncio.run(amain())

if __name__ == "__main__":
    main()
