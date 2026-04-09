"""
fetch_abs.py v7
---------------
Fetches ABS challenge data from MLB Stats API.
Scans ALL play descriptions for "challenges" to get 100% coverage.
Fixed: removed restrictive fields= param from schedule call.
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

def fetch(url):
    req = urllib.request.Request(url, headers={"User-Agent": "ABSScoreboard/1.0"})
    with urllib.request.urlopen(req, timeout=30) as r:
        return json.loads(r.read().decode("utf-8"))


def get_games(start_date, end_date):
    """Get all completed regular season games with umpire info."""
    # No fields= filter — let the API return full data so we don't miss nested fields
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

            # Safely extract team abbreviations
            teams   = g.get("teams", {})
            home    = teams.get("home", {}).get("team", {})
            away    = teams.get("away", {}).get("team", {})
            home_ab = home.get("abbreviation") or home.get("name", "?")[:3].upper()
            away_ab = away.get("abbreviation") or away.get("name", "?")[:3].upper()

            # Extract home plate umpire
            ump = "Unknown"
            for off in g.get("officials", []):
                if off.get("officialType") == "Home Plate":
                    ump = off.get("official", {}).get("fullName", "Unknown")
                    break

            games.append({
                "game_pk":   g["gamePk"],
                "game_date": day["date"],
                "home":      home_ab,
                "away":      away_ab,
                "umpire":    ump,
            })
    return games


def get_challenges(game):
    """
    Get all ABS challenges for a game by scanning play descriptions.
    Every ABS challenge has description containing "challenges".
    """
    url = f"{BASE}/game/{game['game_pk']}/playByPlay"
    try:
        data = fetch(url)
    except Exception as e:
        print(f"  ⚠ game {game['game_pk']}: {e}")
        return []

    challenges = []
    for play in data.get("allPlays", []):
        about   = play.get("about", {})
        matchup = play.get("matchup", {})
        inning  = about.get("inning", 0)
        half    = about.get("halfInning", "top")
        batter  = matchup.get("batter",  {}).get("fullName", "?")
        pitcher = matchup.get("pitcher", {}).get("fullName", "?")

        for ev in play.get("playEvents", []):
            desc = ev.get("details", {}).get("description") or ""
            if "challenges" not in desc.lower():
                continue

            count   = ev.get("count", {})
            balls   = count.get("balls",   0)
            strikes = count.get("strikes", 0)

            # Challenger name from description text
            m          = re.match(r"^(.+?)\s+challenges", desc)
            challenger = m.group(1).strip() if m else batter

            # Role
            desc_lower = desc.lower()
            if "called strike" in desc_lower:
                role = "Batter"
            else:
                pitcher_last = pitcher.split()[-1].lower() if pitcher else ""
                if pitcher_last and pitcher_last in desc_lower:
                    role = "Pitcher"
                else:
                    role = "Catcher"

            result = "Overturned" if "overturned" in desc_lower else "Confirmed"

            challenges.append({
                "game_pk":    str(game["game_pk"]),
                "game_date":  game["game_date"],
                "home":       game["home"],
                "away":       game["away"],
                "umpire":     game["umpire"],
                "inning":     inning,
                "half":       half,
                "balls":      balls,
                "strikes":    strikes,
                "batter":     batter,
                "pitcher":    pitcher,
                "challenger": challenger,
                "role":       role,
                "result":     result,
                "desc":       desc,
            })

    return challenges


def main():
    today    = today_str()
    end_date = yesterday_str()
    print(f"=== ABS Data Fetch v7 — {today} ===")
    print(f"Date range: {SEASON_START} to {end_date}")

    try:
        games = get_games(SEASON_START, end_date)
    except Exception as e:
        print(f"FATAL: Schedule fetch failed: {e}")
        if os.path.exists(OUTPUT_FILE):
            print("Keeping existing data.")
        sys.exit(0)

    print(f"Completed games: {len(games)}")

    all_challenges = []
    for game in games:
        chals = get_challenges(game)
        if chals:
            all_challenges.extend(chals)
            print(f"  {game['away']}@{game['home']} {game['game_date']}: {len(chals)} challenges")

    total    = len(all_challenges)
    ov       = sum(1 for c in all_challenges if c["result"] == "Overturned")
    gcount   = len(set(c["game_pk"] for c in all_challenges))
    pct      = round(100 * ov / total) if total else 0

    print(f"\nTotal: {total} challenges | {ov} overturned ({pct}%) | {gcount} games")

    if total == 0:
        print("ERROR: No challenges found. Keeping existing data.")
        sys.exit(0)

    output = {
        "generated_at":     datetime.utcnow().isoformat() + "Z",
        "season":           2026,
        "source":           "MLB Stats API (description scan)",
        "total_challenges": total,
        "total_overturned": ov,
        "overturn_pct":     pct,
        "total_games":      gcount,
        "challenges":       all_challenges,
    }

    os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)
    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(output, f, indent=2, ensure_ascii=False)

    print(f"✅ Done.")


if __name__ == "__main__":
    main()
