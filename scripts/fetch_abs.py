"""
fetch_abs.py
------------
Fetches ABS challenge data from Baseball Savant Statcast Search.
Since has_review=1 isn't filtering server-side, we download all pitches
and filter locally to rows where 'des' contains "challenges" — these are
the ABS challenge events.
"""

import csv
import json
import urllib.request
import urllib.error
from datetime import datetime, date
import sys
import os
import io
import re

SEASON_START = "2026-03-26"
OUTPUT_FILE  = "data/abs-challenges.json"


def today_str():
    return date.today().isoformat()


def fetch_savant(start_date, end_date):
    """
    Fetch pitches from Statcast Search and filter to ABS challenges locally.
    ABS challenge rows always have 'des' containing the word 'challenges'.
    e.g. "Ryan Jeffers challenges (called strike), call overturned to ball."
    """
    params = "&".join([
        "all=true",
        "hfGT=R%7C",              # Regular season
        "hfSea=2026%7C",          # 2026 season
        "player_type=batter",
        f"game_date_gt={start_date}",
        f"game_date_lt={end_date}",
        "min_pitches=0",
        "min_results=0",
        "sort_col=game_date",
        "sort_order=desc",
        "type=details",
    ])
    url = f"https://baseballsavant.mlb.com/statcast_search/csv?{params}"
    print(f"Fetching Savant CSV (full pitch data, will filter locally)...")

    req = urllib.request.Request(url, headers={
        "User-Agent": (
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
        ),
        "Accept": "text/html,application/xhtml+xml,*/*;q=0.9",
        "Accept-Language": "en-US,en;q=0.9",
        "Referer": "https://baseballsavant.mlb.com/statcast_search",
    })

    try:
        with urllib.request.urlopen(req, timeout=120) as resp:
            raw_bytes = resp.read()
            content_encoding = resp.headers.get("Content-Encoding", "")
            if "gzip" in content_encoding:
                import gzip
                raw = gzip.decompress(raw_bytes).decode("utf-8")
            else:
                raw = raw_bytes.decode("utf-8", errors="replace")
    except urllib.error.HTTPError as e:
        raise RuntimeError(f"HTTP {e.code}: {e.reason}")
    except urllib.error.URLError as e:
        raise RuntimeError(f"URL error: {e.reason}")

    raw = raw.lstrip("\ufeff")
    lines = [l for l in raw.strip().split("\n") if l.strip()]
    print(f"  Total lines from Savant: {len(lines)}")

    if len(lines) < 2:
        raise RuntimeError(f"Too few lines ({len(lines)})")

    reader = csv.DictReader(io.StringIO(raw))
    all_rows = list(reader)
    print(f"  Total pitches: {len(all_rows)}")

    # Filter to ABS challenge rows only
    # ABS challenge rows have 'des' containing "challenges"
    abs_rows = [
        r for r in all_rows
        if "challenges" in (r.get("des") or "").lower()
    ]
    print(f"  ABS challenge rows: {len(abs_rows)}")

    if len(abs_rows) == 0:
        # Log sample of 'des' values to help debug
        sample_des = [r.get("des", "") for r in all_rows[:10]]
        print(f"  Sample 'des' values: {sample_des}")
        raise RuntimeError("No ABS challenge rows found after filtering")

    return abs_rows


def parse_row(row):
    """Convert a Statcast row to our challenge format."""
    des = row.get("des") or ""
    des_lower = des.lower()

    # Challenger: "Name challenges (...)"
    m = re.match(r"^(.+?)\s+challenges", des)
    challenger = m.group(1).strip() if m else (row.get("player_name") or "?")

    # Role
    if "called strike" in des_lower:
        role = "Batter"
    else:
        pitcher = row.get("pitcher") or ""
        pitcher_last = pitcher.split()[-1].lower() if pitcher else ""
        role = "Pitcher" if (pitcher_last and pitcher_last in des_lower) else "Catcher"

    # Result
    result = "Overturned" if "overturned" in des_lower else "Confirmed"

    try:
        inning = int(row.get("inning") or 0)
    except (ValueError, TypeError):
        inning = 0
    try:
        balls   = int(row.get("balls")   or 0)
        strikes = int(row.get("strikes") or 0)
    except (ValueError, TypeError):
        balls = strikes = 0

    half = "top" if (row.get("inning_topbot") or "Top") == "Top" else "bottom"

    return {
        "game_pk":    str(row.get("game_pk") or ""),
        "game_date":  (row.get("game_date") or "")[:10],
        "home":       (row.get("home_team") or "?").upper(),
        "away":       (row.get("away_team") or "?").upper(),
        "umpire":     row.get("hp_umpire") or "Unknown",
        "inning":     inning,
        "half":       half,
        "balls":      balls,
        "strikes":    strikes,
        "batter":     row.get("player_name") or "?",
        "pitcher":    row.get("pitcher") or "?",
        "challenger": challenger,
        "role":       role,
        "result":     result,
        "desc":       des,
    }


def main():
    today = today_str()
    print(f"=== ABS Data Fetch — {today} ===")

    try:
        rows = fetch_savant(SEASON_START, today)
    except Exception as e:
        print(f"FATAL: {e}")
        if os.path.exists(OUTPUT_FILE):
            print("Keeping existing data file.")
        sys.exit(0)

    challenges = []
    errors = 0
    for row in rows:
        try:
            challenges.append(parse_row(row))
        except Exception as e:
            errors += 1
            if errors <= 3:
                print(f"  Row error: {e}")

    print(f"Parsed {len(challenges)} challenges ({errors} errors)")

    if len(challenges) == 0:
        print("ERROR: Zero challenges. Keeping existing data.")
        sys.exit(0)

    overturned = sum(1 for c in challenges if c["result"] == "Overturned")
    games      = len(set(c["game_pk"] for c in challenges))
    pct        = round(100 * overturned / len(challenges)) if challenges else 0

    output = {
        "generated_at":     datetime.utcnow().isoformat() + "Z",
        "season":           2026,
        "source":           "Baseball Savant / Statcast",
        "total_challenges": len(challenges),
        "total_overturned": overturned,
        "overturn_pct":     pct,
        "total_games":      games,
        "challenges":       challenges,
    }

    os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)
    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(output, f, indent=2, ensure_ascii=False)

    print(f"✅ {len(challenges)} challenges | {overturned} overturned ({pct}%) | {games} games")


if __name__ == "__main__":
    main()
