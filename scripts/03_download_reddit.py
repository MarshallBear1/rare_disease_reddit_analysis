#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
03_download_reddit.py   –   robust Reddit scraper
Writes UTF-8 .jsonl.gz per subreddit into data/raw/
"""

import os, sys, time, gzip, json, pathlib, datetime as dt
import praw, prawcore

# ── console: force UTF-8 on Windows so arrows work; fallback to plain ASCII
try:
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8")
    ARROW = "→"
except Exception:          # still CP-1252? use ASCII arrow
    ARROW = "->"

# ── paths & auth ──────────────────────────────────────────────
RAW_DIR   = pathlib.Path("data/raw");      RAW_DIR.mkdir(parents=True, exist_ok=True)
SUB_LIST  = pathlib.Path("data/meta/rare_subs_crosschecked.txt")
MIN_YEAR  = 2015
BIG_SLEEP = 120
CLIENT_ID     = os.getenv("REDDIT_CLIENT_ID")
CLIENT_SECRET = os.getenv("REDDIT_CLIENT_SECRET")
USER_AGENT    = "RareDiseaseScraper/0.1 by u/<yourname>"

# ── helpers ──────────────────────────────────────────────────
def submission_to_dict(p):
    return dict(
        is_post=True, subreddit=p.subreddit.display_name, id=p.id,
        author=str(p.author) if p.author else "[deleted]",
        created_utc=int(p.created_utc), score=p.score,
        num_comments=p.num_comments, title=p.title, body=p.selftext or "",
        url=p.url,
    )

def comment_to_dict(c):
    return dict(
        is_post=False, subreddit=c.subreddit.display_name,
        parent_id=c.parent_id, id=c.id,
        author=str(c.author) if c.author else "[deleted]",
        created_utc=int(c.created_utc), score=c.score, body=c.body or "",
    )

def dump(path, objs):
    with gzip.open(path, "wt", encoding="utf-8") as gz:
        for o in objs:
            gz.write(json.dumps(o, ensure_ascii=False) + "\n")

# ── scrape one sub ───────────────────────────────────────────
def scrape_sub(reddit, name):
    outfile = RAW_DIR / f"r_{name}.jsonl.gz"
    if outfile.exists():
        return True

    try:
        sub  = reddit.subreddit(name)
        buff = []

        for post in sub.new(limit=None):
            if dt.datetime.utcfromtimestamp(post.created_utc).year < MIN_YEAR:
                continue
            buff.append(submission_to_dict(post))

            try:
                post.comments.replace_more(limit=None)
                buff.extend(comment_to_dict(c) for c in post.comments.list())
            except prawcore.exceptions.PrawcoreException:
                pass         # ignore comment-level hiccups

        if buff:
            dump(outfile, buff)
            print(f"[OK ] {name:25} {ARROW} {len(buff):6,d} objs")
        else:
            print(f"[WARN] {name:25} {ARROW} no data")
        return True

    except prawcore.exceptions.Forbidden:
        print(f"[WARN] {name:25} {ARROW} 403 forbidden")
    except prawcore.exceptions.NotFound:
        print(f"[WARN] {name:25} {ARROW} 404 not-found")
    except prawcore.exceptions.TooManyRequests:
        print(f"[WARN] {name:25} {ARROW} 429 – sleeping {BIG_SLEEP}s")
        time.sleep(BIG_SLEEP)
        return scrape_sub(reddit, name)      # one retry
    except Exception as e:
        print(f"[WARN] {name:25} {ARROW} {e}")
    return False

# ── main loop ────────────────────────────────────────────────
def main():
    reddit = praw.Reddit(
        client_id=CLIENT_ID, client_secret=CLIENT_SECRET,
        user_agent=USER_AGENT, ratelimit_seconds=BIG_SLEEP
    )

    subs = [s.strip() for s in SUB_LIST.read_text().splitlines() if s.strip()]
    print(f"[INFO] {len(subs)} subreddits to scrape.\n")

    for s in subs:
        scrape_sub(reddit, s)
        time.sleep(1.2)

if __name__ == "__main__":
    main()
