"""
BTTS Analysis Engine
--------------------
Downloads data from football-data.co.uk for the Top 5 UK leagues,
analyses fixtures for a given date (defaults to today), and returns
a ranked shortlist of the top 8 BTTS picks based on a weighted
scoring model.

Can be run any day of the week — it will analyse whatever fixtures
exist for the target date.

Data sources:
  - football-data.co.uk/fixtures.csv  →  upcoming fixtures (with kick-off times)
  - football-data.co.uk/mmz4281/{season}/{code}.csv  →  historical results per league

Scoring model:
  - Scoring Score  = home_scored_pct(last 6 home) × away_scored_pct(last 6 away)
  - BTTS Score     = home_btts_pct(last 6 home)   × away_btts_pct(last 6 away)
  - Confidence     = (80% × BTTS Score) + (20% × Scoring Score)  →  expressed as %
"""

import os
import pandas as pd
import requests
from io import StringIO
from datetime import date, timedelta
import warnings
warnings.filterwarnings("ignore")

# ── Configuration ─────────────────────────────────────────────────────────────

def _derive_season() -> str:
    """
    Auto-derive the football season code from today's date.
    UK football seasons run Aug → May. If we're in Aug+ of year X,
    the season code is XXYY (e.g. Aug 2025 → "2526").
    If we're in Jan–Jul of year Y, we're still in the previous
    season: (Y-1)Y (e.g. Feb 2026 → "2526").

    Override: set BTTS_SEASON env var to force a specific season (e.g. "2425").
    """
    override = os.environ.get("BTTS_SEASON")
    if override:
        return override
    today = date.today()
    if today.month >= 8:  # Aug–Dec = start of new season
        start_year = today.year
    else:  # Jan–Jul = still in season that started last Aug
        start_year = today.year - 1
    end_year = start_year + 1
    return f"{start_year % 100:02d}{end_year % 100:02d}"

SEASON = _derive_season()

# Division codes used in fixtures.csv — must match exactly
LEAGUE_CODES = {
    "E0":  "Premier League",
    "E1":  "Championship",
    "E2":  "League One",
    "E3":  "League Two",
}

BTTS_WEIGHT    = 0.80
SCORING_WEIGHT = 0.20
TOP_N          = 8
LOOKBACK_GAMES = 6  # Number of home/away games to look back over

FIXTURES_URL = "https://www.football-data.co.uk/fixtures.csv"

# Telegram has a 4096-character limit per message
TELEGRAM_MAX_LENGTH = 4096

# Day name lookup for user-friendly output
DAY_NAMES = {0: "Monday", 1: "Tuesday", 2: "Wednesday", 3: "Thursday",
             4: "Friday", 5: "Saturday", 6: "Sunday"}


# ── Data Fetching ─────────────────────────────────────────────────────────────

def download_fixtures() -> pd.DataFrame:
    """
    Download the master fixtures CSV from football-data.co.uk.
    This file contains ALL upcoming fixtures across all leagues,
    including kick-off times, and is updated regularly.
    """
    resp = requests.get(FIXTURES_URL, timeout=15)
    resp.raise_for_status()
    df = pd.read_csv(StringIO(resp.content.decode("utf-8-sig")))
    df.columns = df.columns.str.strip()

    df = df.dropna(subset=["HomeTeam", "AwayTeam", "Div"])
    df["Date"] = pd.to_datetime(df["Date"], dayfirst=True, errors="coerce")
    df = df.dropna(subset=["Date"])

    # Normalise Time column — not always present or populated
    if "Time" not in df.columns:
        df["Time"] = "TBC"
    else:
        df["Time"] = df["Time"].fillna("TBC").astype(str).str.strip()
        df.loc[df["Time"] == "", "Time"] = "TBC"

    return df


def download_league_history(season: str, code: str) -> pd.DataFrame:
    """
    Download a league's results CSV — used purely for historical
    stats (goals scored, BTTS records). Only completed matches
    with valid scorelines are kept.
    """
    url = f"https://www.football-data.co.uk/mmz4281/{season}/{code}.csv"
    resp = requests.get(url, timeout=15)
    resp.raise_for_status()
    df = pd.read_csv(StringIO(resp.content.decode("utf-8-sig")))
    df.columns = df.columns.str.strip()

    df = df.dropna(subset=["HomeTeam", "AwayTeam", "FTHG", "FTAG"])
    df["Date"] = pd.to_datetime(df["Date"], dayfirst=True, errors="coerce")
    df = df.dropna(subset=["Date"])
    df["FTHG"] = df["FTHG"].astype(int)
    df["FTAG"]  = df["FTAG"].astype(int)
    return df


def get_target_date() -> date:
    """Return today's date. Used as the default when no date is specified."""
    return date.today()


def get_next_saturday() -> date:
    """Return the date of the coming Saturday (or today if it is Saturday).
    Kept for backwards compatibility and the Saturday scheduled job."""
    today = date.today()
    days_ahead = (5 - today.weekday()) % 7
    return today + timedelta(days=days_ahead)


def get_fixtures_for_date(fixtures_df: pd.DataFrame, target: date) -> pd.DataFrame:
    """
    Filter the master fixtures CSV to games on the target date
    in the leagues we cover.
    """
    mask = (
        (fixtures_df["Date"].dt.date == target) &
        (fixtures_df["Div"].isin(LEAGUE_CODES.keys()))
    )
    return fixtures_df[mask].copy()


# ── Metric Calculations ───────────────────────────────────────────────────────

def _last_n_home(df: pd.DataFrame, team: str, before: date, n: int) -> pd.DataFrame:
    mask = (df["HomeTeam"] == team) & (df["Date"].dt.date < before)
    return df[mask].sort_values("Date", ascending=False).head(n)


def _last_n_away(df: pd.DataFrame, team: str, before: date, n: int) -> pd.DataFrame:
    mask = (df["AwayTeam"] == team) & (df["Date"].dt.date < before)
    return df[mask].sort_values("Date", ascending=False).head(n)


def home_scored_pct(df: pd.DataFrame, team: str, before: date) -> float:
    """% of last N home games where the home team scored."""
    games = _last_n_home(df, team, before, LOOKBACK_GAMES)
    if games.empty:
        return 0.5
    return (games["FTHG"] > 0).sum() / len(games)


def away_scored_pct(df: pd.DataFrame, team: str, before: date) -> float:
    """% of last N away games where the away team scored."""
    games = _last_n_away(df, team, before, LOOKBACK_GAMES)
    if games.empty:
        return 0.5
    return (games["FTAG"] > 0).sum() / len(games)


def home_btts_pct(df: pd.DataFrame, team: str, before: date) -> float:
    """% of last N home games where BTTS occurred."""
    games = _last_n_home(df, team, before, LOOKBACK_GAMES)
    if games.empty:
        return 0.5
    btts = ((games["FTHG"] > 0) & (games["FTAG"] > 0)).sum()
    return btts / len(games)


def away_btts_pct(df: pd.DataFrame, team: str, before: date) -> float:
    """% of last N away games where BTTS occurred."""
    games = _last_n_away(df, team, before, LOOKBACK_GAMES)
    if games.empty:
        return 0.5
    btts = ((games["FTHG"] > 0) & (games["FTAG"] > 0)).sum()
    return btts / len(games)


# ── Fixture Analysis ──────────────────────────────────────────────────────────

def analyse_fixture(df: pd.DataFrame, home: str, away: str,
                    fixture_date: date, league: str,
                    kickoff_time: str = "TBC") -> dict:
    """
    Run the full BTTS model for a single fixture.
    All historical lookups are strictly before the fixture date.
    """
    h_scored = home_scored_pct(df, home, fixture_date)
    a_scored = away_scored_pct(df, away, fixture_date)
    scoring_score = h_scored * a_scored

    h_btts = home_btts_pct(df, home, fixture_date)
    a_btts = away_btts_pct(df, away, fixture_date)
    btts_score = h_btts * a_btts

    confidence = (BTTS_WEIGHT * btts_score + SCORING_WEIGHT * scoring_score) * 100

    return {
        "league":        league,
        "date":          fixture_date.strftime("%d %b %Y"),
        "day_name":      DAY_NAMES[fixture_date.weekday()],
        "kickoff":       kickoff_time,
        "home":          home,
        "away":          away,
        # Scoring breakdown
        "h_scored_pct":  round(h_scored * 100, 1),
        "a_scored_pct":  round(a_scored * 100, 1),
        "scoring_score": round(scoring_score * 100, 1),
        # BTTS breakdown
        "h_btts_pct":    round(h_btts * 100, 1),
        "a_btts_pct":    round(a_btts * 100, 1),
        "btts_score":    round(btts_score * 100, 1),
        # Final
        "confidence":    round(confidence, 1),
    }


# ── Main Runner ───────────────────────────────────────────────────────────────

def run_analysis(target_date: date = None) -> list[dict]:
    """
    1. Download fixtures.csv to find games on the target date (defaults to today)
    2. Download each league's season CSV for historical stats only
    3. Score each fixture and return top N by confidence
    """
    if target_date is None:
        target_date = get_target_date()

    day_name = DAY_NAMES[target_date.weekday()]

    # Step 1 — Get upcoming fixtures with kick-off times
    print("  → Fetching upcoming fixtures...", end=" ")
    try:
        fixtures_df = download_fixtures()
    except Exception as e:
        print(f"FAILED ({e})")
        return []

    day_fixtures = get_fixtures_for_date(fixtures_df, target_date)

    if day_fixtures.empty:
        print(f"No fixtures found for {day_name} {target_date.strftime('%d %b %Y')}.")
        return []

    print(f"{len(day_fixtures)} fixtures on {day_name} {target_date.strftime('%d %b %Y')}")

    # Step 2 — Pre-load historical results for each league we need
    leagues_needed = day_fixtures["Div"].unique()
    history = {}

    for code in leagues_needed:
        if code not in LEAGUE_CODES:
            continue
        league_name = LEAGUE_CODES[code]
        print(f"  → Loading history: {league_name}...", end=" ")
        try:
            history[code] = download_league_history(SEASON, code)
            print(f"{len(history[code])} results loaded")
        except Exception as e:
            print(f"FAILED ({e})")

    # Step 3 — Score each fixture
    all_results = []

    for _, row in day_fixtures.iterrows():
        code = row["Div"]
        if code not in history:
            continue

        result = analyse_fixture(
            df=history[code],
            home=row["HomeTeam"],
            away=row["AwayTeam"],
            fixture_date=row["Date"].date(),
            league=LEAGUE_CODES[code],
            kickoff_time=row.get("Time", "TBC"),
        )
        all_results.append(result)

    # Sort by confidence, return top N
    all_results.sort(key=lambda x: x["confidence"], reverse=True)
    return all_results[:TOP_N]


# ── Formatters ────────────────────────────────────────────────────────────────

def format_terminal(results: list[dict], target_date: date = None) -> str:
    """Clean terminal-readable output."""
    if not results:
        if target_date:
            day_name = DAY_NAMES[target_date.weekday()]
            return f"No fixtures found for {day_name} {target_date.strftime('%d %b %Y')}."
        return "No fixtures found for today."

    day_label = f"{results[0]['day_name']} {results[0]['date']}"

    lines = [
        "=" * 62,
        f"  ⚽  BTTS TOP {TOP_N}  —  {day_label}",
        f"  Model: {int(BTTS_WEIGHT*100)}% BTTS weight / {int(SCORING_WEIGHT*100)}% Scoring weight",
        "=" * 62,
    ]

    for i, r in enumerate(results, 1):
        lines += [
            f"\n#{i}  {r['home']} vs {r['away']}",
            f"     {r['league']}  |  KO: {r['kickoff']}",
            f"",
            f"     HOME SCORING    {r['home']}: {r['h_scored_pct']}% scored in last {LOOKBACK_GAMES} home games",
            f"     AWAY SCORING    {r['away']}: {r['a_scored_pct']}% scored in last {LOOKBACK_GAMES} away games",
            f"     Scoring Score:  {r['scoring_score']:.1f}%",
            f"",
            f"     HOME BTTS       {r['home']}: {r['h_btts_pct']}% BTTS in last {LOOKBACK_GAMES} home games",
            f"     AWAY BTTS       {r['away']}: {r['a_btts_pct']}% BTTS in last {LOOKBACK_GAMES} away games",
            f"     BTTS Score:     {r['btts_score']:.1f}%",
            f"",
            f"     ★ CONFIDENCE:   {r['confidence']:.1f}%",
        ]

    lines.append("\n" + "=" * 62)
    return "\n".join(lines)


def format_telegram(results: list[dict], target_date: date = None) -> str:
    """Telegram-formatted output — compact, mobile-friendly."""
    if not results:
        if target_date:
            day_name = DAY_NAMES[target_date.weekday()]
            return f"⚽ No fixtures found for {day_name} {target_date.strftime('%d %b %Y')}."
        return "⚽ No fixtures found for today."

    day_label = f"{results[0]['day_name']} {results[0]['date']}"

    header = (
        f"*⚽ BTTS Top {TOP_N} — {day_label}*\n"
        f"_{int(BTTS_WEIGHT*100)}% BTTS / {int(SCORING_WEIGHT*100)}% Scoring_"
    )

    blocks = [header]
    for i, r in enumerate(results, 1):
        block = (
            f"\n*#{i} · {r['home']} vs {r['away']}*\n"
            f"_{r['league']} · {r['kickoff']}_\n"
            f"Scored: `{r['h_scored_pct']}%` | `{r['a_scored_pct']}%` → `{r['scoring_score']:.1f}%`\n"
            f"BTTS: `{r['h_btts_pct']}%` | `{r['a_btts_pct']}%` → `{r['btts_score']:.1f}%`\n"
            f"🎯 *Confidence: {r['confidence']:.1f}%*"
        )
        blocks.append(block)

    return ("\n" + "─" * 20 + "\n").join(blocks)


def split_telegram_messages(text: str) -> list[str]:
    """
    Split a long Telegram message into chunks that fit within the
    4096-character limit. Splits on double-newlines (between picks)
    to keep individual fixtures intact.
    """
    if len(text) <= TELEGRAM_MAX_LENGTH:
        return [text]

    messages = []
    current = ""

    for block in text.split("\n\n"):
        candidate = f"{current}\n\n{block}" if current else block
        if len(candidate) <= TELEGRAM_MAX_LENGTH:
            current = candidate
        else:
            if current:
                messages.append(current)
            current = block

    if current:
        messages.append(current)

    return messages


# ── Entry Point ───────────────────────────────────────────────────────────────

if __name__ == "__main__":
    print(f"\nRunning BTTS Analysis (season {SEASON})...\n")
    results = run_analysis()  # Defaults to today
    print(format_terminal(results))
