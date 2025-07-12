#!/usr/bin/env python3
# ──────────────────────────────────────────────────────────────
# 06c_deepseek_label.py  –  label Reddit posts with DeepSeek-chat
# ----------------------------------------------------------------
# Example:
#   python scripts/06c_deepseek_label.py \
#          -i data/flat/selected_high_signal_posts.csv \
#          -o data/flat/model_labels \
#          -m deepseek-chat
# ----------------------------------------------------------------
import os, json, time, argparse, textwrap, re
from pathlib import Path

import pandas as pd
import requests
import backoff
from tqdm import tqdm

# ─── CONFIG ────────────────────────────────────────────────────
MAX_CHARS   = 4_000          # truncate only if >4 k characters
SLEEP       = 0.3            # polite pause between API calls
TEMP        = 0.0            # deterministic output
N_LABELS    = 3              # keep max 3 labels per post
DEBUG_ROWS  = 3              # print first N raw replies for inspection

CATEGORIES = {
    "experience":        "Personal narrative of one's condition, symptoms, or story.",
    "clinical":          "Discussion of symptoms, diagnosis, test results, medication or other treatment details.",
    "advice_seeking":    "Explicit request for information, guidance, or recommendations.",
    "advice_giving":     "Providing factual information, how-to instructions, or recommendations.",
    "emotional_support": "Offering encouragement, empathy, sympathy, prayer, etc. toward others.",
    "emotional_share":   "Expressing one’s own feelings, frustration, fear, gratitude, etc., without asking or advising.",
    "lifestyle":         "Daily management, coping, diet, exercise, routines, stress reduction.",
    "community":         "Introductions, thanks, congratulations, humour about the community, meta-discussion.",
    "news":              "Links or summaries of external news, research, policy changes, resources.",
    "off_topic":         "Material unrelated to health / subreddit focus, obvious spam/ads, or pure jokes."
}

BASE_URL = "https://api.deepseek.com/v1/chat/completions"
LABEL_BLOCK = "\n".join(f"- {k}" for k in CATEGORIES)

# ─── BUILD PROMPT / PAYLOAD ────────────────────────────────────
def build_payload(model: str, text: str) -> dict:
    snippet = text if len(text) <= MAX_CHARS else textwrap.shorten(
        text, MAX_CHARS, placeholder=" (…)")

    prompt = (
        f"POST:\n{snippet}\n\n"
        "You are a medical text-classification assistant.\n"
        "Choose ONLY the labels that apply (max 3).\n"
        f"Allowed labels:\n{LABEL_BLOCK}\n\n"
        'Return exactly this JSON – **no commentary, no markdown**:\n'
        '{"labels":["label1","label2"]}'
    )

    return {
        "model": model,
        "messages": [
            {"role": "system",
             "content": "Reply ONLY with a JSON object."},
            {"role": "user", "content": prompt}
        ],
        # Tell DeepSeek we expect JSON
        "response_format": {"type": "json_object"},
        "temperature": TEMP
    }

# ─── API CALL WITH RETRY + FALLBACK PARSER ─────────────────────
@backoff.on_exception(backoff.expo,
                      (requests.exceptions.RequestException,),
                      max_tries=4, factor=2)
def deepseek_labels(model: str, post_txt: str, row_idx: int = 0) -> list[str]:
    api_key = os.environ.get("DEEPSEEK_API_KEY")
    if not api_key:
        raise RuntimeError("Set DEEPSEEK_API_KEY env-var first.")

    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type":  "application/json"
    }
    resp = requests.post(
        BASE_URL,
        headers=headers,
        json=build_payload(model, post_txt),
        timeout=120
    )
    if resp.status_code != 200:
        raise requests.HTTPError(f"[{resp.status_code}] {resp.text[:120]}…")

    content = resp.json()["choices"][0]["message"]["content"]

    # DEBUG – show first few raw responses
    if row_idx < DEBUG_ROWS:
        print(f"\n↳ RAW reply #{row_idx+1} → {content[:160].replace(chr(10),' ')}\n")

    # 1) strict attempt – should succeed if JSON-only came back
    try:
        labels = json.loads(content)["labels"]
    except Exception:
        # 2) fallback: extract the first {...} block & parse
        m = re.search(r"{.*}", content, re.S)
        try:
            labels = json.loads(m.group(0))["labels"] if m else []
        except Exception:
            labels = []

    # filter + cap
    return [l for l in labels if l in CATEGORIES][:N_LABELS]

# ─── MAIN DRIVER ───────────────────────────────────────────────
def main(input_csv: str, out_dir: str, model: str):
    df = pd.read_csv(input_csv)
    out_dir = Path(out_dir); out_dir.mkdir(parents=True, exist_ok=True)
    safe_model = model.replace("/", "_")
    out_path   = out_dir / f"labels_{safe_model}.csv"

    results = []
    for i, txt in enumerate(tqdm(df["body"].fillna(""), desc="Labelling", unit="post")):
        results.append(";".join(deepseek_labels(model, str(txt), i)))
        time.sleep(SLEEP)

    df_out = df.copy()
    df_out["labels"] = results
    df_out.to_csv(out_path, index=False)
    print(f"\n[DONE] wrote → {out_path}")

    # ─ sanity summary ─
    check = df_out["labels"].fillna("")
    flat  = [lab for row in check.str.split(";") for lab in row if lab]
    print("\nSummary")
    print("───────")
    print(f"Posts processed:  {len(df_out):6,d}")
    print(f"Empty-label rows: {(check == '').sum():6,d}")
    print(f"Distinct labels:  {len(set(flat)):6,d} → {sorted(set(flat))}")

# ─── CLI ────────────────────────────────────────────────────────
if __name__ == "__main__":
    p = argparse.ArgumentParser(description="Label posts via DeepSeek-chat")
    p.add_argument("-i", "--input",     required=True, help="Input CSV")
    p.add_argument("-o", "--out_dir",   required=True, help="Output dir")
    p.add_argument("-m", "--model",     default="deepseek-chat",
                   help="DeepSeek model name (e.g. deepseek-chat)")
    args = p.parse_args()
    main(args.input, args.out_dir, args.model)
