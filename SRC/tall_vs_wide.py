#!/usr/bin/env python3
"""
Count CSV/TXT files as TALL vs WIDE without modifying anything.

TALL rule:
  header contains 'payer_name' AND ('plan_name' OR 'payer_plan')  [case-insensitive]
WIDE rule:
  everything else

Just scans and counts - does NOT move, copy, or modify any files.
"""

import csv, sys, itertools
from pathlib import Path

# ---------- Your data location ----------
DEFAULT_IN = Path(r"D:\BANA 650 Projects - HealthCare-Analytics\Combined Data")

DELIMS = [",", "|", "\t", ";"]
MAX_SCAN_LINES = 200

def norm_token(s: str) -> str:
    if not isinstance(s, str):
        s = str(s)
    # strip BOM, zero-width, NBSP, outer spaces
    return s.replace("\ufeff","").replace("\u200b","").replace("\xa0"," ").strip()

def should_skip(p: Path) -> bool:
    parts_lower = [seg.lower() for seg in p.parts]
    if ".venv" in parts_lower or "site-packages" in parts_lower:
        return True
    if any(seg.startswith(".") for seg in p.parts):  # hidden dirs/files
        return True
    return False

def looks_like_header(cols) -> bool:
    cols_norm = [norm_token(c).lower() for c in cols]
    s = set(cols_norm)
    return ("payer_name" in s) and (("plan_name" in s) or ("payer_plan" in s))

def find_header_row(path: Path):
    """
    Return (normalized_header, delimiter) if a qualifying header is found
    within the first MAX_SCAN_LINES; else (None, None).
    """
    try:
        with open(path, "r", encoding="utf-8", errors="ignore", newline="") as f:
            lines = list(itertools.islice(f, MAX_SCAN_LINES))
    except Exception:
        return None, None

    # Try CSV parsing with multiple delimiters for each candidate line
    for d in DELIMS:
        for raw in lines:
            if not raw.strip(): 
                continue
            if raw.lstrip().startswith(("#", "//")):
                continue
            try:
                row = next(csv.reader([raw], delimiter=d))
            except Exception:
                continue
            if not row:
                continue
            cols = [norm_token(c) for c in row]
            if looks_like_header(cols):
                return cols, d

    # Fallback: if a pipe is visually present but parsing failed, manual split by pipe
    for raw in lines:
        if "|" in raw:
            cols = [norm_token(c) for c in raw.split("|")]
            if looks_like_header(cols):
                return cols, "|"

    return None, None

def classify_file(path: Path) -> str:
    cols, _ = find_header_row(path)
    return "tall" if cols is not None else "wide"

def main():
    in_dir = DEFAULT_IN

    print("=" * 60)
    print(f"Scanning: {in_dir}")
    print("=" * 60)

    if not in_dir.exists():
        print(f"\n[ERROR] Folder does not exist: {in_dir}", file=sys.stderr)
        sys.exit(1)

    # Find all CSV and TXT files recursively
    files = (p for p in in_dir.rglob("*") if p.is_file() and p.suffix.lower() in (".csv", ".txt"))
    files = [p for p in files if not should_skip(p)]
    files = sorted(files)

    if not files:
        print("\n[ERROR] No CSV or TXT files found in the specified folder.")
        sys.exit(1)

    print(f"\nTotal files found: {len(files)}")
    print("\nAnalyzing files...\n")

    tall_count = 0
    wide_count = 0
    error_count = 0
    
    tall_files = []
    wide_files = []

    for p in files:
        try:
            label = classify_file(p)
            if label == "tall":
                tall_count += 1
                tall_files.append(p.name)
            else:
                wide_count += 1
                wide_files.append(p.name)
        except Exception as e:
            error_count += 1
            print(f"[ERROR] Could not process {p.name}: {e}", file=sys.stderr)

    # Print summary
    print("=" * 60)
    print("RESULTS:")
    print("=" * 60)
    print(f"TALL files:  {tall_count}")
    print(f"WIDE files:  {wide_count}")
    if error_count > 0:
        print(f"Errors:      {error_count}")
    print(f"Total:       {len(files)}")
    print("=" * 60)
    
    # Optional: show file names by category
    if tall_files:
        print(f"\nTALL files ({tall_count}):")
        for fname in tall_files:
            print(f"  - {fname}")
    
    if wide_files:
        print(f"\nWIDE files ({wide_count}):")
        for fname in wide_files:
            print(f"  - {fname}")

if __name__ == "__main__":
    main()