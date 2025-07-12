"""
01_find_candidate_subs.py
──────────────────────────────────────────────────────────
Scan Pushshift “subreddits_meta_only_YYYY-MM.zst” dumps and
flag any subreddit whose name, title or description matches
our rare-disease vocabulary (ordo_terms.tsv).

USAGE  (test single month first):
    python scripts/01_find_candidate_subs.py 2024-01

Outputs CSV:
    data/meta/candidate_subreddits.csv
Columns: name, subscribers, public_description, matched_term
"""

import sys, json, pathlib, urllib.request, io, csv, zstandard as zstd
from rapidfuzz import fuzz, process

BASE      = pathlib.Path(__file__).resolve().parents[1]
VOCAB_TSV = BASE / "data" / "cleaned" / "ordo_terms.tsv"
OUT_CSV   = BASE / "data" / "meta"   / "candidate_subreddits.csv"
TMP_DIR   = BASE / "data" / "meta"   / "tmp_zst"
TMP_DIR.mkdir(parents=True, exist_ok=True)

# ------------------------------------------------------------------ #
# 1.  Load vocabulary (lower-case) + acronyms
terms = [t.strip() for t in VOCAB_TSV.read_text(encoding="utf-8").splitlines() if t.strip()]
terms_lower = {t.lower() for t in terms}
acronyms    = {t for t in terms if t.isupper() and 3 <= len(t) <= 6}

def text_has_term(txt: str) -> str | None:
    """Return the first matching term or None."""
    low = txt.lower()
    # fast exact match
    for t in terms_lower:
        if t in low:
            return t
    # fuzzy for acronyms (ALS, SMA…)
    res, score, _ = process.extractOne(low, acronyms, scorer=fuzz.partial_ratio)
    return res if score >= 95 else None

# ------------------------------------------------------------------ #
# 2.  Download + stream-read .zst
URL_TMPL = "https://files.pushshift.io/reddit/subreddits/meta/subreddits_meta_only_{month}.zst"

def scan_month(month: str, writer):
    fn = TMP_DIR / f"{month}.zst"
    if not fn.exists():
        print(f"⏬  {month}.zst")
        urllib.request.urlretrieve(URL_TMPL.format(month=month), fn)
    dctx = zstd.ZstdDecompressor(max_window_size=2147483648)
    with fn.open("rb") as fh, dctx.stream_reader(fh) as reader:
        buff = io.TextIOWrapper(reader, encoding="utf-8", errors="ignore")
        for line in buff:
            doc = json.loads(line)
            name  = doc.get("name", "")
            title = doc.get("title", "") or ""
            desc  = doc.get("public_description", "") or ""
            blob  = " ".join([name, title, desc])
            hit   = text_has_term(blob)
            if hit:
                writer.writerow({
                    "name": name,
                    "subscribers": doc.get("subscribers", ""),
                    "public_description": desc[:300],  # truncate
                    "matched_term": hit
                })

# ------------------------------------------------------------------ #
if __name__ == "__main__":
    if len(sys.argv) < 2:
        sys.exit("Usage:  python scripts/01_find_candidate_subs.py YYYY-MM [YYYY-MM …]")
    months = sys.argv[1:]
    header_written = OUT_CSV.exists()
    with OUT_CSV.open("a", newline="", encoding="utf-8") as csvfile:
        writer = csv.DictWriter(csvfile,
                                fieldnames=["name","subscribers",
                                            "public_description","matched_term"])
        if not header_written:
            writer.writeheader()
        for m in months:
            try:
                scan_month(m, writer)
            except Exception as exc:
                print(f"⚠️  Skipped {m}: {exc}")
    print(f"✅  Candidate list updated → {OUT_CSV}")
