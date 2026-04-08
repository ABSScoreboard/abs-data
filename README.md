[README.md](https://github.com/user-attachments/files/26582555/README.md)
# ABS Scoreboard — Data Backend

This repo automatically fetches ABS challenge data from Baseball Savant
and makes it available as a JSON file for the ABS Tracker web app.

## How it works

1. GitHub Actions runs `scripts/fetch_abs.py` three times daily (8am, 11am, 10pm ET)
2. The script fetches complete ABS challenge data from Baseball Savant's Statcast endpoint
3. The output is saved to `data/abs-challenges.json` and committed back to this repo
4. The JSON file is served via GitHub Pages at:
   `https://absscoreboard.github.io/abs-data/data/abs-challenges.json`
5. The ABS Tracker app reads from this URL instead of the GUMBO API

## Why this exists

The MLB GUMBO API (statsapi.mlb.com) only flags ~68% of ABS challenges
with `hasReview=true`. Baseball Savant uses the complete Hawk-Eye dataset
and captures 100% of challenges. This backend bridges that gap.

## Files

- `scripts/fetch_abs.py` — Python script that fetches and parses Savant data
- `.github/workflows/fetch-abs-data.yml` — GitHub Actions workflow (runs automatically)
- `data/abs-challenges.json` — Output file (auto-updated daily)

## Manual trigger

Go to **Actions** tab → **Fetch ABS Challenge Data** → **Run workflow**

## Data schema

Each challenge in `data.challenges[]`:

```json
{
  "game_pk":   "824136",
  "game_date": "2026-04-01",
  "home":      "KC",
  "away":      "MIN",
  "umpire":    "Andy Fletcher",
  "inning":    6,
  "half":      "top",
  "balls":     2,
  "strikes":   1,
  "batter":    "Ryan Jeffers",
  "pitcher":   "Alex Lange",
  "challenger": "Ryan Jeffers",
  "role":      "Batter",
  "result":    "Overturned",
  "desc":      "Ryan Jeffers challenges (called strike), call overturned."
}
```
