#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
scripts/02b_crosscheck_openai.py
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
For every row in data/meta/verified_subreddits_crosschecked.csv:
* Ask GPT-4o whether the “matched_term” is a rare disease
  (prevalence < 1 / 2 000) or not.
* Write the answer into the CSV (columns keep, prevalence_est).
* Produce data/meta/rare_subs_crosschecked.txt with the names to keep.
"""

import sys
# ---- ensure Windows console prints UTF-8 characters ---------------
if hasattr(sys.stdout, "reconfigure"):          # PY ≥ 3.7
    sys.stdout.reconfigure(encoding="utf-8")
# -------------------------------------------------------------------

import csv, json, os, re, shutil, time
from pathlib import Path

import openai
from openai import RateLimitError, APIConnectionError, Timeout

# ---------- CONFIG --------------------------------------------------
MODEL        = "gpt-4o"                 # or "gpt-4o-mini" if that is your tier
INPUT_CSV    = Path("data/meta/verified_subreddits_crosschecked.csv")
BACKUP_CSV   = INPUT_CSV.with_suffix(".bak")
OUTPUT_TXT   = Path("data/meta/rare_subs_crosschecked.txt")
# --------------------------------------------------------------------


def query_prevalence(disease: str) -> tuple[str, str]:
    """
    Return ("yes" | "no", prevalence_string)
    keep == "yes"  -> prevalence < 1 / 2 000   (rare)
    keep == "no"   -> prevalence >= 1 / 2 000  OR not a disease
    """
    prompt = (
        "You are a medical librarian.\n"
        "Consult reliable prevalence sources for the disease below and reply "
        "ONLY with a JSON object inside a ```json code-fence.\n\n"
        'Rules:\n'
        '  • "is_rare": "yes" if prevalence < 1 / 2000, '
        '"no" if common, "not_disease" if this is not a disease.\n'
        '  • "prevalence": short free-text like "~ 1 / 100 000" (blank if unknown).\n\n'
        "Example good answer:\n"
        '```json\n'
        '{"is_rare":"yes","prevalence":"~ 1 / 100 000"}\n'
        "```\n\n"
        f"Now evaluate:\n{disease}"
    )

    resp = openai.chat.completions.create(
        model=MODEL,
        messages=[{"role": "user", "content": prompt}],
        temperature=0
    )
    reply = resp.choices[0].message.content

    # Extract the JSON inside ```json ... ```
    m = re.search(r"```json\s*({.*?})\s*```", reply, re.S)
    if not m:
        raise ValueError("GPT did not return a parsable JSON block")
    data = json.loads(m.group(1))

    verdict = data.get("is_rare", "").lower()
    prevalence = data.get("prevalence", "")

    if verdict == "yes":
        return "yes", prevalence
    elif verdict in ("no", "not_disease"):
        return "no", prevalence
    else:
        raise ValueError(f"Unexpected verdict value: {verdict}")


def main() -> None:
    # 1. back-up the original CSV
    shutil.copy(INPUT_CSV, BACKUP_CSV)
    print(f"[INFO] Backup → {BACKUP_CSV}")

    rare_names: list[str] = []
    updated_rows: list[dict] = []

    with INPUT_CSV.open(newline="", encoding="utf-8") as fin:
        reader = csv.DictReader(fin)
        total_rows = sum(1 for _ in fin)
        fin.seek(0)
        reader = csv.DictReader(fin)          # re-initialise iterator

        for idx, row in enumerate(reader, start=1):
            disease = row["matched_term"]
            try:
                keep, preval = query_prevalence(disease)
            except (RateLimitError, Timeout):
                print(f"[WARN] Rate-limit / timeout on '{disease}', retry in 20 s")
                time.sleep(20)
                keep, preval = "no", ""
            except APIConnectionError as e:
                print(f"[WARN] Connection error on '{disease}': {e}; skip")
                keep, preval = "no", ""
            except Exception as e:
                print(f"[WARN] Parse/API error on '{disease}': {e}; skip")
                keep, preval = "no", ""

            row["keep"] = keep
            row["prevalence_est"] = preval
            updated_rows.append(row)

            if keep == "yes":
                rare_names.append(row["name"])

            print(f"[XCHK] {idx}/{total_rows} processed – kept {len(rare_names)}")

    # 2. overwrite the CSV with new data
    with INPUT_CSV.open("w", newline="", encoding="utf-8") as fout:
        writer = csv.DictWriter(fout, fieldnames=reader.fieldnames)
        writer.writeheader()
        writer.writerows(updated_rows)

    # 3. write the list of rare subreddit names
    OUTPUT_TXT.parent.mkdir(parents=True, exist_ok=True)
    OUTPUT_TXT.write_text("\n".join(rare_names), encoding="utf-8")

    print(f"[DONE] {len(rare_names)} rare subreddits written → {OUTPUT_TXT}")


if __name__ == "__main__":
    main()
