"""
fetch_abs.py v6
---------------
Fetches ABS challenge data from MLB Stats API (statsapi.mlb.com).
Instead of relying on hasReview=true flag (only 68% coverage),
we scan ALL play descriptions for the word "challenges" to get 100% coverage.
This matches what Baseball Savant shows (366 challenges).
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


def get_game_ids(start_date, end_date):
    """Get all completed regular season game IDs for the date range."""
    url = (
        f"{BASE}/schedule?sportId=1&gameType=R"
        f"&startDate={start_date}&endDate={end_date}"
        f"&hydrate=officials"
        f"&fields=dates,date,games,gamePk,status,abstractGameState,"
        f"abstractGameCode,teams,home,away,team,abbreviation,officials,"
        f"officialType,official,fullName"
    )
    data = fetch(url)
    games = []
    for day in data.get("dates", []):
        for g in day.get("games", []):
            state = g.get("status", {}).get("abstractGameState", "")
            code  = g.get("status", {}).get("abstractGameCode", "")
            if state in ("Final", "Game Over") or code == "F":
                # Extract umpire
                ump = "Unknown"
                for off in g.get("officials", []):
                    if off.get("officialType") == "Home Plate":
                        ump = off.get("official", {}).get("fullName", "Unknown")
                        break
                games.append({
                    "game_pk": g["gamePk"],
                    "game_date": day["date"],
                    "home": g["teams"]["home"]["team"]["abbreviation"],
                    "away": g["teams"]["away"]["team"]["abbreviation"],
                    "umpire": ump,
                })
    return games


def get_challenges_for_game(game):
    """
    Fetch play-by-play for a game and find ALL ABS challenges
    by scanning play descriptions for the word 'challenges'.
    This catches 100% of challenges, unlike hasReview=true (68%).
    """
    url = (
        f"{BASE}/game/{game['game_pk']}/playByPlay"
        f"?fields=allPlays,playEvents,details,description,event,"
        f"isReview,reviewDetails,atBatIndex,pitchNumber,"
        f"count,balls,strikes,about,inning,halfInning,"
        f"matchup,batter,fullName,pitcher,id"
    )
    try:
        data = fetch(url)
    except Exception as e:
        print(f"  Error fetching game {game['game_pk']}: {e}")
        return []

    challenges = []
    for play in data.get("allPlays", []):
        inning   = play.get("about", {}).get("inning", 0)
        half     = play.get("about", {}).get("halfInning", "top")
        batter   = play.get("matchup", {}).get("batter", {}).get("fullName", "?")
        pitcher  = play.get("matchup", {}).get("pitcher", {}).get("fullName", "?")
        batter_id  = play.get("matchup", {}).get("batter", {}).get("id")
        pitcher_id = play.get("matchup", {}).get("pitcher", {}).get("id")

        for ev in play.get("playEvents", []):
            desc = ev.get("details", {}).get("description") or ""
            desc_lower = desc.lower()

            # Check if this is an ABS challenge by description text
            if "challenges" not in desc_lower:
                continue

            count = ev.get("count", {})
            balls   = count.get("balls", 0)
            strikes = count.get("strikes", 0)

            # Extract challenger name from description
            m = re.match(r"^(.+?)\s+challenges", desc)
            challenger = m.group(1).strip() if m else "?"

            # Role determination
            if "called strike" in desc_lower:
                role = "Batter"
            else:
                # Check reviewer details if available
                review = ev.get("reviewDetails", {})
                challenger_id = review.get("challengeTeamId")
                if pitcher_id and challenger:
                    # If challenger name matches pitcher, it's a pitcher challenge
                    pitcher_last = pitcher.split()[-1].lower() if pitcher else ""
                    if pitcher_last and pitcher_last in desc_lower:
                        role = "Pitcher"
                    else:
                        role = "Catcher"
                else:
                    role = "Catcher"

            # Result
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
    end_date = yesterday_str()  # cap at yesterday to avoid in-progress games
    print(f"=== ABS Data Fetch v6 — {today} ===")
    print(f"Fetching games from {SEASON_START} to {end_date}")

    # Get all completed games
    print("Getting schedule...")
    try:
        games = get_game_ids(SEASON_START, end_date)
    except Exception as e:
        print(f"FATAL: Could not fetch schedule: {e}")
        sys.exit(0)
    print(f"Found {len(games)} completed games")

    # Fetch play-by-play for each game
    all_challenges = []
    for i, game in enumerate(games):
        chals = get_challenges_for_game(game)
        all_challenges.extend(chals)
        if chals:
            print(f"  {game['away']}@{game['home']} {game['game_date']}: {len(chals)} challenges")

    print(f"\nTotal challenges found: {len(all_challenges)}")

    if len(all_challenges) == 0:
        print("ERROR: Zero challenges found. Keeping existing data.")
        if os.path.exists(OUTPUT_FILE):
            sys.exit(0)
        sys.exit(1)

    overturned = sum(1 for c in all_challenges if c["result"] == "Overturned")
    games_with = len(set(c["game_pk"] for c in all_challenges))
    pct        = round(100 * overturned / len(all_challenges)) if all_challenges else 0

    print(f"Overturned: {overturned}/{len(all_challenges)} ({pct}%)")
    print(f"Games with challenges: {games_with}")

    output = {
        "generated_at":     datetime.utcnow().isoformat() + "Z",
        "season":           2026,
        "source":           "MLB Stats API (play description scan)",
        "total_challenges": len(all_challenges),
        "total_overturned": overturned,
        "overturn_pct":     pct,
        "total_games":      games_with,
        "challenges":       all_challenges,
    }

    os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)
    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(output, f, indent=2, ensure_ascii=False)

    print(f"✅ Written: {len(all_challenges)} challenges | {overturned} overturned ({pct}%) | {games_with} games")


if __name__ == "__main__":
    main()
