"""
fetch_abs.py v9
---------------
ABS challenge data from MLB Stats API.
Uses hasReview=True from details (confirmed present in API) to find challenges.
Also scans description AND event fields for challenge text.
"""

import json
import urllib.request
import urllib.error
from datetime import datetime, date, timedelta
import sys
import os
import re

SEASON_START = "2026-03-26"
OUTPUT_FILE  = "data/abs-challenges.json"
BASE = "https://statsapi.mlb.com/api/v1"


def today_str():
    return date.today().isoformat()

def yesterday_str():
    return (date.today() - timedelta(days=1)).isoformat()

def fetch(url, timeout=45):
    req = urllib.request.Request(url, headers={"User-Agent": "ABSScoreboard/1.0"})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return json.loads(r.read().decode("utf-8"))



# MLB team name → standard abbreviation lookup
# Needed because the API sometimes returns full names instead of abbreviations
TEAM_ABBREV = {
    "Arizona Diamondbacks": "ARI", "Atlanta Braves": "ATL",
    "Baltimore Orioles": "BAL", "Boston Red Sox": "BOS",
    "Chicago Cubs": "CHC", "Chicago White Sox": "CWS",
    "Cincinnati Reds": "CIN", "Cleveland Guardians": "CLE",
    "Colorado Rockies": "COL", "Detroit Tigers": "DET",
    "Houston Astros": "HOU", "Kansas City Royals": "KC",
    "Los Angeles Angels": "LAA", "Los Angeles Dodgers": "LAD",
    "Miami Marlins": "MIA", "Milwaukee Brewers": "MIL",
    "Minnesota Twins": "MIN", "New York Mets": "NYM",
    "New York Yankees": "NYY", "Oakland Athletics": "OAK",
    "Athletics": "ATH", "Philadelphia Phillies": "PHI",
    "Pittsburgh Pirates": "PIT", "San Diego Padres": "SD",
    "San Francisco Giants": "SF", "Seattle Mariners": "SEA",
    "St. Louis Cardinals": "STL", "Tampa Bay Rays": "TB",
    "Texas Rangers": "TEX", "Toronto Blue Jays": "TOR",
    "Washington Nationals": "WSH",
}

def team_abbrev(team_dict):
    """Get standard team abbreviation, falling back gracefully."""
    # Try direct abbreviation field first
    ab = team_dict.get("abbreviation", "")
    if ab and len(ab) <= 4:
        return ab
    # Try name lookup
    name = team_dict.get("name", "")
    if name in TEAM_ABBREV:
        return TEAM_ABBREV[name]
    # Last resort: first 3 chars of name
    return name[:3].upper() if name else "?"

def get_games(start_date, end_date):
    url = (
        f"{BASE}/schedule"
        f"?sportId=1&gameType=R"
        f"&startDate={start_date}&endDate={end_date}"
        f"&hydrate=officials"
    )
    data = fetch(url)
    games = []
    for day in data.get("dates", []):
        for g in day.get("games", []):
            status = g.get("status", {})
            state  = status.get("abstractGameState", "")
            code   = status.get("abstractGameCode", "")
            if state not in ("Final", "Game Over") and code != "F":
                continue
            teams   = g.get("teams", {})
            home    = teams.get("home", {}).get("team", {})
            away    = teams.get("away", {}).get("team", {})
            home_ab = team_abbrev(home)
            away_ab = team_abbrev(away)
            ump = "Unknown"
            for off in g.get("officials", []):
                if off.get("officialType") == "Home Plate":
                    ump = off.get("official", {}).get("fullName", "Unknown")
                    break
            games.append({
                "game_pk":   g["gamePk"],
                "game_date": day["date"],
                "home": home_ab, "away": away_ab, "umpire": ump,
            })
    return games


def get_challenges(game, verbose=False):
    """
    Fetch play-by-play and extract ABS challenges.
    Detection: details.hasReview == True OR description contains 'challenges'
    """
    url = f"{BASE}/game/{game['game_pk']}/playByPlay"
    try:
        data = fetch(url, timeout=45)
    except Exception as e:
        print(f"  ERROR {game['away']}@{game['home']}: {e}")
        return []

    challenges = []
    has_review_count = 0
    desc_challenge_count = 0

    for play in data.get("allPlays", []):
        about   = play.get("about", {})
        matchup = play.get("matchup", {})
        inning  = about.get("inning", 0)
        half    = about.get("halfInning", "top")
        batter  = matchup.get("batter",  {}).get("fullName", "?")
        pitcher = matchup.get("pitcher", {}).get("fullName", "?")

        for ev in play.get("playEvents", []):
            details    = ev.get("details", {})
            has_review = details.get("hasReview", False)
            description = details.get("description", "")
            event_type  = details.get("event", "")

            # Track both detection methods
            desc_has_challenge = "challenges" in description.lower()
            if has_review:
                has_review_count += 1
            if desc_has_challenge:
                desc_challenge_count += 1

            # Use EITHER signal to detect a challenge
            if not has_review and not desc_has_challenge:
                continue

            # For hasReview events without challenge text in description,
            # check reviewDetails for the full description
            review = ev.get("reviewDetails", {})
            challenger_name = review.get("player", {}).get("fullName", "")
            is_overturned   = review.get("isOverturned", None)

            # Build the best description we have
            if desc_has_challenge:
                desc = description
            elif challenger_name:
                # Reconstruct from reviewDetails
                call_type = "called strike" if details.get("call", {}).get("code") == "C" else "ball"
                result_txt = "call overturned" if is_overturned else "call confirmed"
                desc = f"{challenger_name} challenges ({call_type}), {result_txt}."
            else:
                desc = description or event_type or "ABS Challenge"

            desc_lower = desc.lower()

            # Challenger name
            m = re.match(r"^(.+?)\s+challenges", desc)
            if m:
                challenger = m.group(1).strip()
            elif challenger_name:
                challenger = challenger_name
            else:
                challenger = batter

            # Role
            if "called strike" in desc_lower:
                role = "Batter"
            else:
                # Check call code from details
                call_code = details.get("call", {}).get("code", "")
                if call_code == "C":  # called strike
                    role = "Batter"
                else:
                    pitcher_last = pitcher.split()[-1].lower() if pitcher else ""
                    if pitcher_last and pitcher_last in desc_lower:
                        role = "Pitcher"
                    else:
                        role = "Catcher"

            # Result
            if is_overturned is not None:
                result = "Overturned" if is_overturned else "Confirmed"
            else:
                result = "Overturned" if "overturned" in desc_lower else "Confirmed"

            count   = ev.get("count", {})
            challenges.append({
                "game_pk":    str(game["game_pk"]),
                "game_date":  game["game_date"],
                "home":       game["home"],
                "away":       game["away"],
                "umpire":     game["umpire"],
                "inning":     inning,
                "half":       half,
                "balls":      count.get("balls",   0),
                "strikes":    count.get("strikes", 0),
                "batter":     batter,
                "pitcher":    pitcher,
                "challenger": challenger,
                "role":       role,
                "result":     result,
                "desc":       desc,
            })

    if verbose:
        print(f"  {game['away']}@{game['home']}: hasReview={has_review_count} descMatch={desc_challenge_count} found={len(challenges)}")

    return challenges


def main():
    today    = today_str()
    end_date = yesterday_str()
    print(f"=== ABS Data Fetch v9 — {today} ===")
    print(f"Date range: {SEASON_START} to {end_date}")

    try:
        games = get_games(SEASON_START, end_date)
    except Exception as e:
        print(f"FATAL: Schedule failed: {e}")
        sys.exit(0)

    print(f"Completed games: {len(games)}")

    all_challenges = []
    # Log first 5 games verbosely to verify detection
    for i, game in enumerate(games):
        chals = get_challenges(game, verbose=(i < 5))
        if chals:
            all_challenges.extend(chals)
            if i >= 5:
                print(f"  {game['away']}@{game['home']} {game['game_date']}: {len(chals)}")

    total = len(all_challenges)
    ov    = sum(1 for c in all_challenges if c["result"] == "Overturned")
    gc    = len(set(c["game_pk"] for c in all_challenges))
    pct   = round(100 * ov / total) if total else 0

    print(f"\nTotal: {total} challenges | {ov} overturned ({pct}%) | {gc} games")

    if total == 0:
        print("ERROR: No challenges found. Keeping existing data.")
        sys.exit(0)

    output = {
        "generated_at":     datetime.utcnow().isoformat() + "Z",
        "season":           2026,
        "source":           "MLB Stats API (hasReview + description scan)",
        "total_challenges": total,
        "total_overturned": ov,
        "overturn_pct":     pct,
        "total_games":      gc,
        "challenges":       all_challenges,
    }

    os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)
    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(output, f, indent=2, ensure_ascii=False)

    print(f"✅ Written: {total} challenges | {ov} overturned ({pct}%) | {gc} games")


if __name__ == "__main__":
    main()
