import os
import csv
import random
from pathlib import Path

RAW_PATH = os.getenv("AMAZON_TRAIN_PATH", "data/raw/amazon/train.ft.txt")
OUT_PATH = os.getenv("AMAZON_OUT_PATH", "data/processed/amazon_sample.csv")
SAMPLE_N = int(os.getenv("SAMPLE_N", "50000"))  # keep 50k for laptop dev

def parse_line(line: str):
    line = line.strip()
    if not line:
        return None
    if line.startswith("__label__1"):
        label = "negative"
        text = line.replace("__label__1", "", 1).strip()
    elif line.startswith("__label__2"):
        label = "positive"
        text = line.replace("__label__2", "", 1).strip()
    else:
        return None
    return text, label

def main():
    raw = Path(RAW_PATH)
    if not raw.exists():
        raise FileNotFoundError(f"Cannot find: {RAW_PATH}")

    Path(OUT_PATH).parent.mkdir(parents=True, exist_ok=True)

    # reservoir sampling (won't load entire file into RAM)
    sample = []
    seen = 0

    with raw.open("r", encoding="utf-8", errors="ignore") as f:
        for line in f:
            parsed = parse_line(line)
            if not parsed:
                continue
            seen += 1
            if len(sample) < SAMPLE_N:
                sample.append(parsed)
            else:
                j = random.randint(1, seen)
                if j <= SAMPLE_N:
                    sample[j - 1] = parsed

    with open(OUT_PATH, "w", newline="", encoding="utf-8") as out:
        w = csv.writer(out)
        w.writerow(["text", "label"])
        w.writerows(sample)

    print(f"✅ Created sample CSV: {OUT_PATH}  | rows={len(sample)}")

if __name__ == "__main__":
    main()
