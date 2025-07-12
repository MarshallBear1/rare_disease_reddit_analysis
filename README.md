# Rare-Disease Reddit Analysis

## 1. Project Goal
Analyse information-seeking and peer support in all rare-disease subreddits.

## 2. Folder Structure
data/
cleaned/
meta/
raw/
notebooks/
scripts/
python -m venv venv
source venv/Scripts/activate   # Windows Git Bash
pip install -r requirements.txt
# Build vocab
python scripts/00_build_vocab.py
# Live candidate scan
python scripts/01c_find_subs_redditapi.py
# GPT verify
python scripts/02_verify_subreddits_openai.py
