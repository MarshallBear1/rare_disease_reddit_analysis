#!/usr/bin/env bash
# Keeps the whole pipeline unattended:
# 1) scan Reddit for candidate subs
# 2) GPT-verify rare-disease subs

source venv/Scripts/activate     # adjust if your venv path differs

echo "=== Live Reddit scan started ==="
python scripts/01c_find_subs_redditapi.py

echo "=== GPT verification started ==="
python scripts/02_verify_subreddits_openai.py
