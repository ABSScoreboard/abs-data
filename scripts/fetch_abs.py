"""
fetch_abs.py v5
---------------
Fetches ABS challenge data from Baseball Savant Statcast Search.
Filters locally to ABS challenge rows using the 'des' field.
Handles Statcast name format ("Last, First" → "First Last").
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


def fix_name(name):
    """
    Statcast returns names as "Last, First" — convert to "First Last".
    Also handles "Last, First M." middle initials.
    """
    if not name:
        return "?"
    name = name.strip()
    if "," in name:
        parts = name.split(",", 1)
        last  = parts[0].strip()
        first = parts[1].strip()
        return f"{first} {last}"
    return name


def fetch_savant(start_date, end_date):
    """
    Download Statcast pitch data and filter to ABS challenge rows.
    ABS challenge rows have 'des' containing 'challenges'.
    """
    params = "&".join([
        "all=true",
        "hfGT=R%7C",
        "hfSea=2026%7C",
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
    print(f"Fetching Savant data ({start_date} to {end_date})...")

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
    reader = csv.DictReader(io.StringIO(raw))
    all_rows = list(reader)
    print(f"  Total pitches downloaded: {len(all_rows)}")

    if len(all_rows) == 0:
        raise RuntimeError("No rows returned from Savant")

    # Log available columns once for debugging
    if all_rows:
        cols = list(all_rows[0].keys())
        print(f"  Columns: {cols[:15]}")

    # Filter to ABS challenge rows
    # The 'des' field contains the play description
    # ABS challenges always read: "Name challenges (called strike/ball), call overturned/confirmed."
    abs_rows = [r for r in all_rows if "challenges" in (r.get("des") or "").lower()]
    print(f"  ABS challenge rows (des contains 'challenges'): {len(abs_rows)}")

    # If 'des' filter found nothing, try 'description' field
    if len(abs_rows) == 0:
        abs_rows = [r for r in all_rows if "challenges" in (r.get("description") or "").lower()]
        print(f"  ABS challenge rows (description contains 'challenges'): {len(abs_rows)}")

    # If still nothing, log sample des values to debug
    if len(abs_rows) == 0:
        print("  Sample 'des' values (first 5 rows):")
        for r in all_rows[:5]:
            print(f"    des={repr(r.get('des','')[:80])}")
        print("  Sample 'description' values (first 5 rows):")
        for r in all_rows[:5]:
            print(f"    description={repr(r.get('description','')[:80])}")
        # Log all unique field names containing 'review' or 'abs'
        review_fields = [k for k in all_rows[0].keys()
                        if "review" in k.lower() or "abs" in k.lower() or "challenge" in k.lower()]
        print(f"  Review/ABS-related columns: {review_fields}")
        raise RuntimeError("Could not find ABS challenge rows — check column names in logs")

    return abs_rows


def parse_row(row):
    """Convert a Statcast CSV row to our challenge format."""
    des      = row.get("des") or row.get("description") or ""
    des_lower = des.lower()

    # Fix name format: Statcast uses "Last, First" — convert to "First Last"
    batter_raw  = row.get("player_name") or row.get("batter") or "?"
    batter      = fix_name(batter_raw)

    # Pitcher: Statcast sometimes stores as ID or "Last, First"
    pitcher_raw = row.get("pitcher_name") or row.get("matchup_pitcher_name") or ""
    if not pitcher_raw or pitcher_raw.isdigit():
        pitcher_raw = ""
    pitcher = fix_name(pitcher_raw) if pitcher_raw else "?"

    # Challenger: extract from description
    # Pattern: "First Last challenges (...)"
    m = re.match(r"^(.+?)\s+challenges", des)
    challenger = m.group(1).strip() if m else batter

    # Role: called strike = batter challenged; ball = fielder challenged
    if "called strike" in des_lower:
        role = "Batter"
    else:
        # Check if challenger name matches pitcher
        if pitcher and pitcher != "?" and pitcher.lower() in des_lower:
            role = "Pitcher"
        else:
            role = "Catcher"

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
        "batter":     batter,
        "pitcher":    pitcher,
        "challenger": challenger,
        "role":       role,
        "result":     result,
        "desc":       des,
    }


def main():
    today = today_str()
    print(f"=== ABS Data Fetch v5 — {today} ===")

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
        print("ERROR: Zero challenges parsed. Keeping existing data.")
        sys.exit(0)

    # Quick sanity check
    overturned = sum(1 for c in challenges if c["result"] == "Overturned")
    games      = len(set(c["game_pk"] for c in challenges))
    pct        = round(100 * overturned / len(challenges)) if challenges else 0

    print(f"Sanity check: {len(challenges)} challenges, {overturned} overturned ({pct}%), {games} games")
    if len(challenges) > 5000:
        print(f"WARNING: {len(challenges)} challenges seems too high — filter may not be working")
    if pct < 30 or pct > 80:
        print(f"WARNING: {pct}% overturn rate seems unusual (expected 50-65%)")

    # Sample first 3 challenges for verification
    print("Sample challenges:")
    for c in challenges[:3]:
        print(f"  {c['away']}@{c['home']} {c['game_date']} | {c['challenger']} ({c['role']}) | {c['result']}")

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

    print(f"✅ Written: {len(challenges)} challenges | {overturned} overturned ({pct}%) | {games} games")


if __name__ == "__main__":
    main()
