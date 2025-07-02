#!/usr/bin/env bash
set -e    # abort on first error

source venv/Scripts/activate  # adjust if needed

echo "=== Live Reddit scan started ==="
python scripts/01c_find_subs_redditapi.py

echo "=== GPT verification started ==="
python scripts/02_verify_subreddits_openai.py

echo "=== All done.  Verified subs at data/meta/verified_subreddits.csv ==="
