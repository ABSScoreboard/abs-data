"""
fetch_abs.py
------------
Fetches ABS challenge data from Baseball Savant's Statcast Search CSV endpoint.
Runs daily via GitHub Actions and saves output to data/abs-challenges.json
which is then served by GitHub Pages for the ABS Tracker app to consume.

Data source: baseballsavant.mlb.com (complete Hawk-Eye data, ~366 challenges)
vs GUMBO API (incomplete, ~250 challenges due to hasReview flag gaps)
"""

import csv
import json
import urllib.request
import urllib.error
from datetime import datetime, date
import sys
import os

# ── CONFIG ─────────────────────────────────────────────────────────────────
SEASON_START = "2026-03-26"
OUTPUT_FILE  = "data/abs-challenges.json"

# Baseball Savant Statcast Search endpoint — same source their leaderboard uses
# Parameters:
#   has_review=1          → only pitches that had an ABS challenge
#   game_type=R           → regular season
#   season=2026
#   type=details          → pitch-level detail (includes challenger info)
#   player_type=batter    → required param (we filter by has_review anyway)
SAVANT_URL = (
    "https://baseballsavant.mlb.com/statcast_search/csv"
    "?all=true"
    "&hfPT="
    "&hfAB="
    "&hfGT=R%7C"           # Regular season
    "&hfPR="
    "&hfZ="
    "&hfStadium="
    "&hfBBL="
    "&hfNewZones="
    "&hfPull="
    "&hfC="
    "&hfSea=2026%7C"       # 2026 season
    "&hfSit="
    "&player_type=batter"
    "&hfOuts="
    "&hfOpponent="
    "&pitcher_throws="
    "&batter_stands="
    "&hfSA="
    "&game_date_gt={start}"
    "&game_date_lt={end}"
    "&hfMo="
    "&hfTeam="
    "&home_road="
    "&hfRO="
    "&position="
    "&hfInfield="
    "&hfOutfield="
    "&hfInn="
    "&hfBBT="
    "&hfFlag=is%5C.%5C.abs%5C.%5C.review%7C"  # KEY: filter to ABS reviews only
    "&metric_1="
    "&hfInn="
    "&min_pitches=0"
    "&min_results=0"
    "&group_by=name"
    "&sort_col=pitches"
    "&player_event_sort=api_p_release_speed"
    "&sort_order=desc"
    "&min_pas=0"
    "&type=details"
    "&player_id="
)

# ── HELPERS ─────────────────────────────────────────────────────────────────

def today_str():
    return date.today().isoformat()

def fetch_csv(url):
    """Fetch CSV from URL, return list of dicts."""
    print(f"Fetching: {url[:100]}...")
    req = urllib.request.Request(url, headers={
        "User-Agent": "Mozilla/5.0 (compatible; ABSScoreboard/1.0)",
        "Accept":     "text/csv,application/csv,*/*",
    })
    try:
        with urllib.request.urlopen(req, timeout=60) as resp:
            raw = resp.read().decode("utf-8")
    except urllib.error.HTTPError as e:
        print(f"HTTP error {e.code}: {e.reason}")
        raise
    except urllib.error.URLError as e:
        print(f"URL error: {e.reason}")
        raise

    lines = raw.strip().split("\n")
    if not lines or len(lines) < 2:
        raise ValueError(f"CSV response too short ({len(lines)} lines)")

    reader = csv.DictReader(lines)
    rows = list(reader)
    print(f"  → {len(rows)} rows received")
    return rows

def map_role(row):
    """
    Determine challenger role (Batter/Catcher/Pitcher) from Statcast fields.
    Statcast provides:
      - events: the pitch outcome
      - description: text description
      - stand: batter handedness
      - p_throws: pitcher handedness
      - bat_score / fld_score: team scores (unused)
      - type: B=ball, S=strike, X=in play
    For ABS challenges:
      - if type == 'S' (called strike) → batter challenged → Batter
      - if type == 'B' (called ball)   → fielder challenged → Catcher or Pitcher
    We use 'fielder_2' (catcher position id) to distinguish catcher vs pitcher.
    """
    pitch_type = row.get("type", "").strip()
    description = row.get("description", "").lower()

    # Called strike → batter challenged
    if pitch_type == "S" or "called_strike" in description:
        return "Batter"

    # Called ball → fielder challenged
    # Savant includes 'des' field with challenger description
    des = row.get("des", "").lower()
    if "pitcher" in des:
        return "Pitcher"

    return "Catcher"  # default for fielder challenges

def map_result(row):
    """
    Determine if challenge was overturned.
    Savant's 'description' field contains the result:
      - 'ball' → call confirmed as ball (if original was ball, confirmed; if strike, overturned)
      - 'called_strike' → call confirmed as strike
    Better: use 'events' and 'description' together.
    Actually the cleanest field is 'hc_x'/'hc_y' not relevant here.
    Use 'des' which contains text like "ABS review: Call overturned" or "ABS review: Call confirmed"
    """
    des = row.get("des", "").lower()
    description = row.get("description", "").lower()

    if "overturned" in des or "overturned" in description:
        return "Overturned"
    if "confirmed" in des or "confirmed" in description or "upheld" in des:
        return "Confirmed"

    # Fallback: compare original call type vs final
    # type: B=ball, S=strike
    # If original was S (strike) and description says ball → overturned
    pitch_code = row.get("type", "").strip()
    if "ball" in description and pitch_code == "S":
        return "Overturned"
    if "called_strike" in description and pitch_code == "B":
        return "Overturned"

    return "Confirmed"  # safe default

def parse_row(row):
    """Convert a Statcast CSV row to our challenge format."""
    try:
        inning = int(row.get("inning", 0))
    except (ValueError, TypeError):
        inning = 0

    try:
        balls   = int(row.get("balls",   0))
        strikes = int(row.get("strikes", 0))
    except (ValueError, TypeError):
        balls = strikes = 0

    game_date = row.get("game_date", "")[:10]  # YYYY-MM-DD

    # Challenger name — Savant stores in 'player_name' for the batter,
    # but for catcher challenges it's different. We use 'des' to extract.
    # The most reliable: use batter_id / pitcher_id matching like GUMBO approach
    # but Savant gives us 'player_name' = batter always.
    # For fielder challenges, look at 'fielder_2_id' etc.
    # Best available: parse from 'des' field
    des = row.get("des", "")
    challenger = extract_challenger_from_des(des, row)

    home_team = row.get("home_team", "?").upper()
    away_team = row.get("away_team", "?").upper()

    half = "top" if row.get("inning_topbot", "Top") == "Top" else "bottom"

    return {
        "game_pk":   row.get("game_pk", ""),
        "game_date": game_date,
        "home":      home_team,
        "away":      away_team,
        "umpire":    row.get("hp_umpire", "Unknown"),
        "inning":    inning,
        "half":      half,
        "balls":     balls,
        "strikes":   strikes,
        "batter":    row.get("player_name", "?"),
        "pitcher":   row.get("pitcher_name", row.get("pitcher", "?")),
        "challenger": challenger,
        "role":      map_role(row),
        "result":    map_result(row),
        "desc":      des,
        # Extra Savant fields useful for verification
        "plate_x":   row.get("plate_x", ""),
        "plate_z":   row.get("plate_z", ""),
    }

def extract_challenger_from_des(des, row):
    """
    Extract challenger name from the description field.
    Savant 'des' looks like:
      "Ryan Jeffers challenges (called strike), call overturned to ball."
      "Salvador Perez challenges (ball), call confirmed."
    """
    if not des:
        return row.get("player_name", "?")

    # Pattern: "Name challenges"
    import re
    m = re.match(r"^([A-Z][a-záéíóúñü'.\- ]+(?:[A-Z][a-záéíóúñü'.\- ]+)*)\s+challenges", des)
    if m:
        return m.group(1).strip()

    # Fallback to batter name
    return row.get("player_name", "?")

def build_umpire_cache(rows):
    """Build gamePk → umpire name cache from Statcast rows."""
    cache = {}
    for row in rows:
        gk = row.get("game_pk", "")
        ump = row.get("hp_umpire", "")
        if gk and ump and ump != "Unknown":
            cache[gk] = ump
    return cache

# ── MAIN ────────────────────────────────────────────────────────────────────

def main():
    today = today_str()
    print(f"=== ABS Data Fetch — {today} ===")

    url = SAVANT_URL.format(start=SEASON_START, end=today)

    try:
        rows = fetch_csv(url)
    except Exception as e:
        print(f"FATAL: Could not fetch Savant data: {e}")
        # If we have existing data, keep it rather than writing empty
        if os.path.exists(OUTPUT_FILE):
            print("Keeping existing data file.")
            sys.exit(0)
        sys.exit(1)

    if not rows:
        print("WARNING: No rows returned. Keeping existing data.")
        sys.exit(0)

    # Parse all challenges
    challenges = []
    errors = 0
    for row in rows:
        try:
            challenges.append(parse_row(row))
        except Exception as e:
            errors += 1
            if errors <= 3:
                print(f"  Row parse error: {e} — row keys: {list(row.keys())[:5]}")

    print(f"Parsed {len(challenges)} challenges ({errors} errors)")

    if len(challenges) == 0:
        print("ERROR: Zero challenges parsed. Something is wrong. Keeping existing data.")
        sys.exit(0)

    # Compute summary stats
    overturned = sum(1 for c in challenges if c["result"] == "Overturned")
    games      = len(set(c["game_pk"] for c in challenges))
    pct        = round(100 * overturned / len(challenges)) if challenges else 0

    output = {
        "generated_at": datetime.utcnow().isoformat() + "Z",
        "season":       2026,
        "source":       "Baseball Savant / Statcast",
        "total_challenges": len(challenges),
        "total_overturned": overturned,
        "overturn_pct":     pct,
        "total_games":      games,
        "challenges":       challenges,
    }

    # Ensure output directory exists
    os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)

    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(output, f, indent=2, ensure_ascii=False)

    print(f"✅ Written to {OUTPUT_FILE}")
    print(f"   {len(challenges)} challenges | {overturned} overturned ({pct}%) | {games} games")

if __name__ == "__main__":
    main()
