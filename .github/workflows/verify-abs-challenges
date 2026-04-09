"""
verify_challenges.py
--------------------
Cross-references our ABS challenge data against the MLB Stats API
to verify challenge counts match what actually happened in each game.

Tests 50 games randomly sampled across the season.
Compares: our count vs. MLB GUMBO play-by-play description scan
(which is the same source, so this verifies data integrity end-to-end).

Usage: python scripts/verify_challenges.py
"""

import json
import urllib.request
import random
import re
from collections import defaultdict

BACKEND_URL = "https://absscoreboard.github.io/abs-data/data/abs-challenges.json"
BASE = "https://statsapi.mlb.com/api/v1"
SAMPLE_SIZE = 50


def fetch(url):
    req = urllib.request.Request(url, headers={"User-Agent": "ABSScoreboard-Verify/1.0"})
    with urllib.request.urlopen(req, timeout=30) as r:
        return json.loads(r.read().decode("utf-8"))


def get_challenges_from_gumbo(game_pk):
    """Fetch play-by-play directly from GUMBO and count challenges."""
    url = f"{BASE}/game/{game_pk}/playByPlay"
    try:
        data = fetch(url)
    except Exception as e:
        return None, str(e)

    challenges = []
    for play in data.get("allPlays", []):
        matchup = play.get("matchup", {})
        batter  = matchup.get("batter",  {}).get("fullName", "?")
        pitcher = matchup.get("pitcher", {}).get("fullName", "?")
        inning  = play.get("about", {}).get("inning", 0)

        for ev in play.get("playEvents", []):
            details = ev.get("details", {})
            has_review = details.get("hasReview", False)
            desc = details.get("description", "")
            desc_lower = desc.lower()

            if not has_review and "challenges" not in desc_lower:
                continue

            # Get result from reviewDetails
            review = ev.get("reviewDetails", {})
            is_overturned = review.get("isOverturned")

            if "overturned" in desc_lower or is_overturned is True:
                result = "Overturned"
            else:
                result = "Confirmed"

            # Get challenger
            m = re.match(r"^(.+?)\s+challenges", desc)
            challenger = m.group(1).strip() if m else \
                         review.get("player", {}).get("fullName", "?")

            challenges.append({
                "inning":     inning,
                "challenger": challenger,
                "result":     result,
                "desc":       desc[:80],
            })

    return challenges, None


def main():
    print("=== ABS Challenge Verification ===")
    print(f"Sampling {SAMPLE_SIZE} games from backend data\n")

    # Load our backend data
    print("Loading backend JSON...")
    try:
        data = fetch(BACKEND_URL)
    except Exception as e:
        print(f"FATAL: Could not load backend: {e}")
        return

    total_in_backend = data.get("total_challenges", 0)
    print(f"Backend: {total_in_backend} total challenges, "
          f"{data.get('total_overturned', 0)} overturned "
          f"({data.get('overturn_pct', 0)}%)\n")

    # Group backend challenges by game
    by_game = defaultdict(list)
    for c in data.get("challenges", []):
        by_game[c["game_pk"]].append(c)

    game_pks = list(by_game.keys())
    if len(game_pks) < SAMPLE_SIZE:
        print(f"Only {len(game_pks)} games available — testing all")
        sample = game_pks
    else:
        random.seed(42)  # reproducible sample
        sample = random.sample(game_pks, SAMPLE_SIZE)

    print(f"Testing {len(sample)} games...\n")
    print(f"{'Game':>12} {'Date':>12} {'Our#':>6} {'GUMBO#':>7} {'Match':>6} {'Notes'}")
    print("-" * 70)

    passed = failed = errors = 0
    mismatches = []

    for game_pk in sorted(sample):
        our_chals = by_game[game_pk]
        our_count = len(our_chals)
        game_info = our_chals[0]
        label = f"{game_info['away']}@{game_info['home']}"
        date  = game_info["game_date"]

        gumbo_chals, err = get_challenges_from_gumbo(game_pk)

        if err:
            print(f"{label:>12} {date:>12} {our_count:>6} {'ERR':>7} {'?':>6}  {err[:30]}")
            errors += 1
            continue

        gumbo_count = len(gumbo_chals)
        match = our_count == gumbo_count

        if match:
            status = "✅"
            passed += 1
        else:
            status = "❌"
            failed += 1
            mismatches.append({
                "game": label,
                "date": date,
                "our": our_count,
                "gumbo": gumbo_count,
                "our_chals": our_chals,
                "gumbo_chals": gumbo_chals,
            })

        note = "" if match else f"diff={gumbo_count - our_count:+d}"
        print(f"{label:>12} {date:>12} {our_count:>6} {gumbo_count:>7} {status:>6}  {note}")

    print("-" * 70)
    print(f"\nResults: {passed} passed | {failed} failed | {errors} errors")
    print(f"Match rate: {round(100*passed/(passed+failed+errors))}%\n")

    if mismatches:
        print("=== MISMATCHES ===")
        for m in mismatches:
            print(f"\n{m['game']} {m['date']}: our={m['our']} gumbo={m['gumbo']}")
            our_set   = set((c["inning"], c["challenger"]) for c in m["our_chals"])
            gumbo_set = set((c["inning"], c["challenger"]) for c in m["gumbo_chals"])
            extra_ours  = our_set - gumbo_set
            extra_gumbo = gumbo_set - our_set
            if extra_ours:
                print(f"  In ours but not GUMBO: {extra_ours}")
            if extra_gumbo:
                print(f"  In GUMBO but not ours: {extra_gumbo}")
    else:
        print("✅ All sampled games match perfectly!")

    # Summary stats
    print(f"\n=== OVERALL ACCURACY ===")
    our_ov   = data.get("total_overturned", 0)
    our_tot  = data.get("total_challenges", 0)
    our_pct  = data.get("overturn_pct", 0)
    print(f"Total challenges in backend: {our_tot}")
    print(f"Overturned: {our_ov} ({our_pct}%)")
    print(f"Games with challenges: {data.get('total_games', 0)}")
    print(f"Sample match rate: {round(100*passed/(passed+failed+errors)) if (passed+failed+errors) else 0}%")


if __name__ == "__main__":
    main()
