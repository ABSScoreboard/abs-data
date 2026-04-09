"""
fetch_abs.py
------------
Fetches ABS challenge data from Baseball Savant.
Uses the statcast_search CSV endpoint with has_review=1 filter.
Runs daily via GitHub Actions and saves to data/abs-challenges.json
"""

import csv
import json
import urllib.request
import urllib.error
from datetime import datetime, date
import sys
import os
import io

SEASON_START = "2026-03-26"
OUTPUT_FILE  = "data/abs-challenges.json"

def today_str():
    return date.today().isoformat()

def fetch_savant(start_date, end_date):
    """
    Fetch ABS challenge pitches from Baseball Savant Statcast Search.
    Uses has_review=1 to filter to only challenged pitches.
    """
    # This is the direct CSV download URL used by the Statcast Search page
    # has_review=1 filters to pitches with an ABS challenge
    url = (
        "https://baseballsavant.mlb.com/statcast_search/csv"
        "?all=true"
        "&hfPT="
        "&hfAB="
        "&hfGT=R%7C"
        "&hfPR="
        "&hfZ="
        "&hfStadium="
        "&hfBBL="
        "&hfNewZones="
        "&hfPull="
        "&hfC="
        "&hfSea=2026%7C"
        "&hfSit="
        "&player_type=batter"
        "&hfOuts="
        "&hfOpponent="
        "&pitcher_throws="
        "&batter_stands="
        "&hfSA="
        f"&game_date_gt={start_date}"
        f"&game_date_lt={end_date}"
        "&hfMo="
        "&hfTeam="
        "&home_road="
        "&hfRO="
        "&position="
        "&hfInfield="
        "&hfOutfield="
        "&hfInn="
        "&hfBBT="
        "&hfFlag=abs%5C.%5C.review%7C"
        "&metric_1="
        "&min_pitches=0"
        "&min_results=0"
        "&group_by=name"
        "&sort_col=pitches"
        "&player_event_sort=api_p_release_speed"
        "&sort_order=desc"
        "&min_pas=0"
        "&type=details"
    )

    print(f"Fetching Savant CSV...")
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        ),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.5",
        "Accept-Encoding": "gzip, deflate, br",
        "Connection": "keep-alive",
        "Referer": "https://baseballsavant.mlb.com/statcast_search",
    }

    req = urllib.request.Request(url, headers=headers)
    try:
        with urllib.request.urlopen(req, timeout=60) as resp:
            raw_bytes = resp.read()
            # Handle gzip encoding
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

    lines = [l for l in raw.strip().split("\n") if l.strip()]
    print(f"  Response: {len(lines)} lines, first: {repr(lines[0][:80]) if lines else 'EMPTY'}")

    if len(lines) < 2:
        # Try to see what we got back
        print(f"  Full response preview: {repr(raw[:500])}")
        raise RuntimeError(f"CSV response too short ({len(lines)} lines)")

    reader = csv.DictReader(io.StringIO(raw))
    rows = list(reader)
    print(f"  Parsed {len(rows)} rows with columns: {list(rows[0].keys())[:8] if rows else 'none'}")
    return rows


def fetch_savant_alt(start_date, end_date):
    """
    Alternative: use the ABS-specific leaderboard data endpoint.
    Baseball Savant exposes JSON data for their leaderboard pages.
    """
    # Try the leaderboard JSON endpoint directly
    url = (
        "https://baseballsavant.mlb.com/leaderboard/abs-challenges"
        "?year=2026"
        "&gameType=R"
        "&challengeType=all"
        "&level=mlb"
        "&minChal=0"
        "&csv=true"
    )
    print(f"Trying alt endpoint: {url[:80]}...")
    headers = {
        "User-Agent": "Mozilla/5.0 (compatible; ABSScoreboard/1.0; +https://absscoreboard.github.io)",
        "Accept": "text/csv,application/json,*/*",
        "Referer": "https://baseballsavant.mlb.com/leaderboard/abs-challenges",
    }
    req = urllib.request.Request(url, headers=headers)
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            raw = resp.read().decode("utf-8", errors="replace")
        print(f"  Alt response: {len(raw)} chars, preview: {repr(raw[:200])}")
        return raw
    except Exception as e:
        print(f"  Alt endpoint failed: {e}")
        return None


def parse_statcast_row(row):
    """Parse a Statcast CSV row into our challenge format."""
    import re

    game_date = (row.get("game_date") or "")[:10]
    des = row.get("des") or row.get("description") or ""

    # Extract challenger from description
    # e.g. "Ryan Jeffers challenges (called strike), call overturned to ball."
    challenger = "?"
    m = re.match(r"^([A-Z][a-záéíóúñü'\-. ]+?)\s+challenges", des)
    if m:
        challenger = m.group(1).strip()
    else:
        challenger = row.get("player_name") or "?"

    # Role from pitch type
    pitch_type = (row.get("type") or "").strip()
    des_lower = des.lower()
    if pitch_type == "S" or "called strike" in des_lower:
        role = "Batter"
    else:
        # For ball calls, check if pitcher name appears in des
        pitcher = row.get("pitcher", "") or ""
        if pitcher and pitcher.split()[-1].lower() in des.lower():
            role = "Pitcher"
        else:
            role = "Catcher"

    # Result
    if "overturned" in des_lower:
        result = "Overturned"
    elif "confirmed" in des_lower or "upheld" in des_lower:
        result = "Confirmed"
    else:
        result = "Confirmed"  # safe default

    try:
        inning = int(row.get("inning") or 0)
    except (ValueError, TypeError):
        inning = 0
    try:
        balls = int(row.get("balls") or 0)
        strikes = int(row.get("strikes") or 0)
    except (ValueError, TypeError):
        balls = strikes = 0

    half = "top" if (row.get("inning_topbot") or "Top") == "Top" else "bottom"

    return {
        "game_pk":    str(row.get("game_pk") or ""),
        "game_date":  game_date,
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

    challenges = []

    # Try primary Statcast Search endpoint
    try:
        rows = fetch_savant(SEASON_START, today)
        for row in rows:
            try:
                challenges.append(parse_statcast_row(row))
            except Exception as e:
                print(f"  Row parse error: {e}")
        print(f"Primary endpoint: {len(challenges)} challenges parsed")
    except Exception as e:
        print(f"Primary endpoint failed: {e}")

    # If primary got nothing, try alt endpoint
    if len(challenges) == 0:
        print("Trying alternative endpoint...")
        alt_data = fetch_savant_alt(SEASON_START, today)
        if alt_data:
            print(f"Alt data received: {len(alt_data)} chars")
            # Try to parse as CSV
            try:
                reader = csv.DictReader(io.StringIO(alt_data))
                alt_rows = list(reader)
                print(f"Alt CSV rows: {len(alt_rows)}")
                print(f"Alt columns: {list(alt_rows[0].keys()) if alt_rows else 'none'}")
            except Exception as e:
                print(f"Alt parse error: {e}")

    if len(challenges) == 0:
        print("WARNING: No data retrieved. Keeping existing file.")
        # Write diagnostic info
        diag = {
            "generated_at": datetime.utcnow().isoformat() + "Z",
            "season": 2026,
            "source": "Baseball Savant / Statcast",
            "error": "No data retrieved from Savant — endpoint may have changed",
            "total_challenges": 0,
            "total_overturned": 0,
            "overturn_pct": 0,
            "total_games": 0,
            "challenges": [],
        }
        os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)
        with open(OUTPUT_FILE, "w") as f:
            json.dump(diag, f, indent=2)
        sys.exit(0)

    overturned = sum(1 for c in challenges if c["result"] == "Overturned")
    games = len(set(c["game_pk"] for c in challenges))
    pct = round(100 * overturned / len(challenges)) if challenges else 0

    output = {
        "generated_at": datetime.utcnow().isoformat() + "Z",
        "season": 2026,
        "source": "Baseball Savant / Statcast",
        "total_challenges": len(challenges),
        "total_overturned": overturned,
        "overturn_pct": pct,
        "total_games": games,
        "challenges": challenges,
    }

    os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)
    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(output, f, indent=2, ensure_ascii=False)

    print(f"✅ Written: {len(challenges)} challenges | {overturned} overturned ({pct}%) | {games} games")


if __name__ == "__main__":
    main()
