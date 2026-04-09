"""
fetch_abs.py v8
---------------
ABS challenge data from MLB Stats API.
Added verbose logging to diagnose why challenges aren't being found.
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
            home_ab = home.get("abbreviation", home.get("name","?")[:3].upper())
            away_ab = away.get("abbreviation", away.get("name","?")[:3].upper())
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


def get_challenges(game, debug=False):
    """Fetch play-by-play and find all ABS challenges."""
    url = f"{BASE}/game/{game['game_pk']}/playByPlay"
    try:
        data = fetch(url, timeout=45)
    except Exception as e:
        print(f"  ERROR game {game['game_pk']} ({game['away']}@{game['home']}): {e}")
        return []

    all_plays = data.get("allPlays", [])

    if debug:
        # Log structure of first play to understand the data
        if all_plays:
            p = all_plays[0]
            print(f"  DEBUG play keys: {list(p.keys())}")
            evs = p.get("playEvents", [])
            if evs:
                ev = evs[0]
                print(f"  DEBUG event keys: {list(ev.keys())}")
                det = ev.get("details", {})
                print(f"  DEBUG details keys: {list(det.keys())}")
                print(f"  DEBUG description: {repr(det.get('description',''))}")
                print(f"  DEBUG event: {repr(det.get('event',''))}")

    challenges = []
    for play in all_plays:
        about   = play.get("about", {})
        matchup = play.get("matchup", {})
        inning  = about.get("inning", 0)
        half    = about.get("halfInning", "top")
        batter  = matchup.get("batter",  {}).get("fullName", "?")
        pitcher = matchup.get("pitcher", {}).get("fullName", "?")

        for ev in play.get("playEvents", []):
            details = ev.get("details", {})

            # Try all possible description fields
            desc = (
                details.get("description") or
                details.get("event") or
                ev.get("description") or
                ""
            )

            if "challenges" not in desc.lower():
                continue

            count   = ev.get("count", {})
            balls   = count.get("balls",   0)
            strikes = count.get("strikes", 0)

            m          = re.match(r"^(.+?)\s+challenges", desc)
            challenger = m.group(1).strip() if m else batter

            desc_lower = desc.lower()
            if "called strike" in desc_lower:
                role = "Batter"
            else:
                pitcher_last = pitcher.split()[-1].lower() if pitcher else ""
                role = "Pitcher" if (pitcher_last and pitcher_last in desc_lower) else "Catcher"

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
    print(f"=== ABS Data Fetch v8 — {today} ===")
    print(f"Date range: {SEASON_START} to {end_date}")

    try:
        games = get_games(SEASON_START, end_date)
    except Exception as e:
        print(f"FATAL: Schedule failed: {e}")
        sys.exit(0)

    print(f"Completed games: {len(games)}")

    all_challenges = []
    errors = 0

    for i, game in enumerate(games):
        # Debug the first game in detail
        debug = (i == 0)
        chals = get_challenges(game, debug=debug)
        if chals:
            all_challenges.extend(chals)
            print(f"  {game['away']}@{game['home']} {game['game_date']}: {len(chals)} challenges")
        elif debug:
            print(f"  First game ({game['away']}@{game['home']}): 0 challenges")

    total  = len(all_challenges)
    ov     = sum(1 for c in all_challenges if c["result"] == "Overturned")
    gc     = len(set(c["game_pk"] for c in all_challenges))
    pct    = round(100 * ov / total) if total else 0

    print(f"\nTotal: {total} challenges | {ov} overturned ({pct}%) | {gc} games")

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
        "total_games":      gc,
        "challenges":       all_challenges,
    }

    os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)
    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(output, f, indent=2, ensure_ascii=False)

    print(f"✅ Done.")


if __name__ == "__main__":
    main()
