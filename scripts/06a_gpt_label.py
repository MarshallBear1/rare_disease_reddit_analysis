#!/usr/bin/env python
# 06a_gpt_label.py   ── label Reddit posts with a single OpenAI model
# ---------------------------------------------------------------------
import os, json, time, argparse, textwrap, pandas as pd
from pathlib import Path
from tqdm import tqdm

##############################################################################
# 0 ── categories & prompt boilerplate                                      ##
##############################################################################
CATEGORIES = {
    "experience":        "Personal narrative of one's condition, symptoms, or story.",
    "clinical":          "Discussion of symptoms, diagnosis, test results, medication or other treatment details.",
    "advice_seeking":    "Explicit request for information, guidance, or recommendations.",
    "advice_giving":     "Providing factual information, how-to instructions, or recommendations.",
    "emotional_support": "Offering encouragement, empathy, sympathy, prayer, etc. toward others.",
    "emotional_share":   "Expressing one's own feelings, frustration, fear, gratitude, etc., without asking or advising.",
    "lifestyle":         "Daily management, coping, diet, exercise, routines, stress reduction.",
    "community":         "Introductions, thanks, congratulations, humour *about the community*, meta-discussion.",
    "news":              "Links or summaries of external news, research, policy changes, resources.",
    "off_topic":         "Material unrelated to health / subreddit focus, spam / ads, or pure jokes."
}
LABELS        = list(CATEGORIES.keys())
MAX_CHARS     = 4_000
N_LABELS      = 5
TEMPERATURE   = 0.0
SLEEP_BETWEEN = 0.3          # polite pause between calls

LABEL_BLOCK = "\n".join(f"- {l}" for l in LABELS)
SYSTEM_MSG = (
    "You are a qualitative health-research assistant.\n"
    "Assign UP TO FIVE labels from the list below to the POST.\n"
    'Return ONLY valid JSON like: {"labels": ["…", "…"]}\n'
    f"{LABEL_BLOCK}"
)

##############################################################################
# 1 ── minimal retry decorator (avoid external deps)                         ##
##############################################################################
def retry3(func):
    def wrapped(*a, **kw):
        for attempt in range(3):
            try:
                return func(*a, **kw)
            except Exception as e:
                if attempt == 2:
                    raise
                print("WARN", e, "- retrying"); time.sleep(1.5)
    return wrapped

##############################################################################
# 2 ── OpenAI caller                                                        ##
##############################################################################
import openai
openai.api_key = os.getenv("OPENAI_API_KEY") or ""

@retry3
# ---- replace lines 55-57 in the previous file with this -------------------
def gpt_labels(model: str, post_txt: str) -> list[str]:
    # truncate long posts for cost control
    snippet = textwrap.shorten(post_txt, MAX_CHARS, placeholder=" (…)")
    prompt  = f"POST:\n{snippet}"
    chat    = openai.chat.completions.create(
        model=model, temperature=TEMPERATURE,
        messages=[{"role": "system", "content": SYSTEM_MSG},
                  {"role": "user",   "content": prompt}]
    )

    raw = chat.choices[0].message.content.strip()
    try:
        data = json.loads(raw)
        if isinstance(data, dict) and isinstance(data.get("labels"), list):
            return [l for l in data["labels"] if l in LABELS][:N_LABELS]
    except Exception:
        pass
    return ["off_topic"]

##############################################################################
# 3 ── main workflow                                                        ##
##############################################################################
def run(input_csv: str, outdir: str, model: str):
    df = pd.read_csv(input_csv)
    df["body"] = df["body"].fillna("")

    labels = []
    for txt in tqdm(df["body"], desc="labelling", unit="post"):
        labels.append(";".join(gpt_labels(model, txt)))
        time.sleep(SLEEP_BETWEEN)

    df_out = df.copy()
    df_out["labels"] = labels

    out_path = Path(outdir)
    out_path.mkdir(parents=True, exist_ok=True)
    out_file = out_path / f"labels_{model}.csv"
    df_out.to_csv(out_file, index=False)
    print("[DONE] wrote", out_file)

##############################################################################
# 4 ── CLI                                                                  ##
##############################################################################
if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("-i", "--input_csv", required=True,
                    help="CSV with a 'body' column")
    ap.add_argument("-o", "--outdir", default="model_labels")
    ap.add_argument("-m", "--model",  default="gpt-3.5-turbo",
                    help="OpenAI chat model (e.g. gpt-4o-mini)")
    args = ap.parse_args()
    run(args.input_csv, args.outdir, args.model)
