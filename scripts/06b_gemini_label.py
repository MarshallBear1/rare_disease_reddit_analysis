#!/usr/bin/env python
# ────────────────────────────────────────────────────────────────────
# 06b_gemini_label.py – label posts with Google Gemini (single model)
# -------------------------------------------------------------------
# Example:
#   python scripts/06b_gemini_label.py \
#          -i data/flat/selected_high_signal_posts.csv \
#          -o data/flat/model_labels \
#          -m gemini-1.5-flash
# -------------------------------------------------------------------
import os, re, json, time, argparse, textwrap
from pathlib import Path

import pandas as pd
import backoff
import google.generativeai as genai
from tqdm import tqdm

# ─── CONFIG ─────────────────────────────────────────────────────────
MAX_CHARS = 4_000        # truncate only if post > 4 k chars
SLEEP     = 0.3          # polite pause between calls
TEMP      = 0.0          # deterministic output
N_LABELS  = 3            # keep ≤ 3 labels/post

CATEGORIES = {
    "experience":        "Personal narrative of one's condition, symptoms, or story.",
    "clinical":          "Discussion of symptoms, diagnosis, test results, medication or other treatment details.",
    "advice_seeking":    "Explicit request for information, guidance, or recommendations.",
    "advice_giving":     "Providing factual information, how-to instructions, or recommendations.",
    "emotional_support": "Offering encouragement, empathy, sympathy, prayer, etc. toward others.",
    "emotional_share":   "Expressing one’s own feelings, frustration, fear, gratitude, etc., without asking or advising.",
    "lifestyle":         "Daily management, coping, diet, exercise, routines, stress-reduction.",
    "community":         "Introductions, thanks, congratulations, humour about the community, meta-discussion.",
    "news":              "Links or summaries of external news, research, policy changes, resources.",
    "off_topic":         "Material unrelated to health / subreddit focus, obvious spam/ads, or pure jokes."
}

LABEL_BLOCK = "\n".join(f"- {lab}" for lab in CATEGORIES)
SYS_PROMPT = (
    "You are a qualitative health-research assistant.\n"
    "Assign ONLY the exact labels (max 3) from the list below that apply to the post.\n"
    f"{LABEL_BLOCK}\n\n"
    'Return **only** this JSON (no other text):\n'
    '{"labels": ["label1", "label2", ...]}'
)

# ─── AUTH ───────────────────────────────────────────────────────────
if "GOOGLE_API_KEY" not in os.environ:
    raise SystemExit("❌  Set GOOGLE_API_KEY in your environment first.")
genai.configure(api_key=os.environ["GOOGLE_API_KEY"])

# ─── HELPERS ────────────────────────────────────────────────────────
json_pat = re.compile(r"\{.*?\}", re.S)   # first {...} block, non-greedy

def build_prompt(text: str) -> str:
    snippet = text if len(text) <= MAX_CHARS else textwrap.shorten(
        text, MAX_CHARS, placeholder=" (…)")
    return f"POST:\n{snippet}\n\n{SYS_PROMPT}"

def safe_parse_labels(raw: str) -> list[str]:
    """Grab the first JSON object in raw text & validate."""
    m = json_pat.search(raw)
    if not m:
        return []
    try:
        obj = json.loads(m.group(0))
        if isinstance(obj, dict) and isinstance(obj.get("labels"), list):
            return [l for l in obj["labels"] if l in CATEGORIES]
    except Exception:
        pass
    return []

@backoff.on_exception(backoff.expo, Exception, max_tries=4, factor=2)
def gemini_labels(model_name: str, post_txt: str) -> list[str]:
    model = genai.GenerativeModel(model_name)
    rsp   = model.generate_content(
        build_prompt(post_txt),
        generation_config={"temperature": TEMP}
    )
    return safe_parse_labels(rsp.text)[:N_LABELS]

# ─── MAIN ───────────────────────────────────────────────────────────
def main(input_csv: str, out_dir: str, model_name: str):
    df = pd.read_csv(input_csv)
    labels_out = []

    for idx, txt in enumerate(tqdm(df["body"].fillna(""), desc="Labelling", unit="post")):
        labs = gemini_labels(model_name, str(txt))
        labels_out.append(";".join(labs))
        if (idx + 1) % 250 == 0:                # heartbeat every 250 posts
            print(f"✓ {idx+1} posts – latest labels: {labs}")
        time.sleep(SLEEP)

    df_out = df.copy()
    df_out["labels"] = labels_out

    Path(out_dir).mkdir(parents=True, exist_ok=True)
    safe_name = model_name.replace("/", "_")
    out_path  = Path(out_dir) / f"labels_{safe_name}.csv"
    df_out.to_csv(out_path, index=False)
    print(f"[DONE] wrote → {out_path}")

# ─── CLI ────────────────────────────────────────────────────────────
if __name__ == "__main__":
    ap = argparse.ArgumentParser(description="Label posts with a Gemini model")
    ap.add_argument("-i", "--input",     required=True, help="Input CSV of posts")
    ap.add_argument("-o", "--out_dir",   required=True, help="Output directory")
    ap.add_argument(
        "-m", "--model_name",
        default="gemini-1.5-flash",
        help="Gemini model name (e.g. gemini-1.5-flash, gemini-1.5-pro)"
    )
    args = ap.parse_args()
    main(args.input, args.out_dir, args.model_name)
