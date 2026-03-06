"""
BTTS Analysis Engine v2
-----------------------
Downloads data from football-data.co.uk for the Top 4 UK leagues,
analyses fixtures for a given date (defaults to today), and returns
a ranked shortlist of BTTS picks based on venue-specific BTTS form.

Can be run any day of the week — it will analyse whatever fixtures
exist for the target date.

Usage (via Telegram bot):
  /btts              → today's fixtures (best called weekdays from 2pm)
  /btts Saturday     → Saturday's fixtures
  /btts Sunday       → Sunday's fixtures
  /btts tomorrow     → tomorrow's fixtures
  /btts 2026-03-07   → specific date

  On Fridays after 5pm, fixtures.csv typically includes the full
  weekend slate — call /btts Saturday and /btts Sunday to get both days.

  Day names (Monday, Tuesday, etc.) resolve to the NEXT occurrence,
  including today if it matches. Short forms (Sat, Sun, Mon) also work.

Data sources:
  - football-data.co.uk/fixtures.csv  →  upcoming fixtures (with kick-off times & odds)
  - football-data.co.uk/mmz4281/{season}/{code}.csv  →  historical results per league

Scoring model (v2 — backtested across 3 seasons, season-isolated):
  - Confidence = home_btts_pct(last 7 home) × away_btts_pct(last 7 away) × 100
  - Expressed as X/7 for each team (the only values possible with a 7-game lookback)

Signal system:
  ⭐ HIGH CONVICTION  — Home BTTS ≥6/7 + Away BTTS ≥5/7 + Home odds ≥2.3
                        Backtested at 75-82% hit rate
  📋 STANDARD         — Both teams BTTS ≥5/7 in last 7 venue games
                        Backtested at ~61% hit rate (season-isolated).
  📊 SHORTLIST        — Remaining games (highest‑rated by confidence)


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

    Override: set BTTS_SEASON env var to force a specific season.
    """
    override = os.environ.get("BTTS_SEASON")
    if override:
        return override
    today = date.today()
    if today.month >= 8:
        start_year = today.year
    else:
        start_year = today.year - 1
    end_year = start_year + 1
    return f"{start_year % 100:02d}{end_year % 100:02d}"


SEASON = _derive_season()

# Division codes — must match fixtures.csv exactly
LEAGUE_CODES = {
    "E0": "Premier League",
    "E1": "Championship",
    "E2": "League One",
    "E3": "League Two",
}

# ── Model parameters (backtested, season-isolated) ───────────────────────────

LOOKBACK_GAMES = 7   # 7-game venue-specific lookback (optimal per backtest)
TOP_N          = 8   # Max picks to return per day
MIN_GAMES      = 7   # Team must have ≥7 venue games this season to qualify

# Signal thresholds
HIGH_CONVICTION_HOME_BTTS = 6  # Home BTTS in ≥6 of last 7 home games
HIGH_CONVICTION_AWAY_BTTS = 5  # Away BTTS in ≥5 of last 7 away games
HIGH_CONVICTION_HOME_ODDS = 2.3  # Home team odds ≥2.3 (not heavy favourite)

STANDARD_BTTS = 5  # Both teams BTTS in ≥5 of last 7 venue games

FIXTURES_URL = "https://www.football-data.co.uk/fixtures.csv"

TELEGRAM_MAX_LENGTH = 4096

DAY_NAMES = {
    0: "Monday", 1: "Tuesday", 2: "Wednesday", 3: "Thursday",
    4: "Friday", 5: "Saturday", 6: "Sunday",
}

# Odds column priority — football-data.co.uk uses various bookmaker columns.
# We try Bet365 first, then Pinnacle, then average, then any available.
HOME_ODDS_COLS = ["B365H", "PSH", "AvgH", "BWH", "IWH"]
AWAY_ODDS_COLS = ["B365A", "PSA", "AvgA", "BWA", "IWA"]


# ── Data Fetching ─────────────────────────────────────────────────────────────

def download_fixtures() -> pd.DataFrame:
    """
    Download the master fixtures CSV from football-data.co.uk.
    Contains ALL upcoming fixtures across all leagues, including
    kick-off times and (sometimes) early market odds.
    """
    resp = requests.get(FIXTURES_URL, timeout=15)
    resp.raise_for_status()
    df = pd.read_csv(StringIO(resp.content.decode("utf-8-sig")))
    df.columns = df.columns.str.strip()

    df = df.dropna(subset=["HomeTeam", "AwayTeam", "Div"])
    df["Date"] = pd.to_datetime(df["Date"], dayfirst=True, errors="coerce")
    df = df.dropna(subset=["Date"])

    if "Time" not in df.columns:
        df["Time"] = "TBC"
    else:
        df["Time"] = df["Time"].fillna("TBC").astype(str).str.strip()
        df.loc[df["Time"] == "", "Time"] = "TBC"

    return df


def download_league_history(season: str, code: str) -> pd.DataFrame:
    """
    Download a league's results CSV for historical stats.
    Only completed matches with valid scorelines are kept.
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
    df["FTAG"] = df["FTAG"].astype(int)
    return df


def _extract_odds(row: pd.Series, cols: list[str]) -> float | None:
    """
    Try to extract odds from a row, checking columns in priority order.
    Returns the first valid numeric value found, or None.
    """
    for col in cols:
        if col in row.index:
            val = pd.to_numeric(row.get(col), errors="coerce")
            if pd.notna(val) and val > 1.0:
                return round(float(val), 2)
    return None


def get_target_date() -> date:
    """Return today's date."""
    return date.today()


def get_next_saturday() -> date:
    """Return the date of the coming Saturday (or today if Saturday)."""
    today = date.today()
    days_ahead = (5 - today.weekday()) % 7
    return today + timedelta(days=days_ahead)


def get_fixtures_for_date(fixtures_df: pd.DataFrame, target: date) -> pd.DataFrame:
    """Filter fixtures to games on the target date in covered leagues."""
    mask = (
        (fixtures_df["Date"].dt.date == target)
        & (fixtures_df["Div"].isin(LEAGUE_CODES.keys()))
    )
    return fixtures_df[mask].copy()


def parse_target_date(text: str | None) -> date:
    """
    Parse a user-supplied date string into a date object.
    Supports multiple formats so the bot can handle:
      /btts              → today
      /btts Saturday     → the next Saturday (or today if it is Saturday)
      /btts Sunday       → the next Sunday
      /btts tomorrow     → tomorrow
      /btts 2026-03-07   → specific date

    Day names always resolve to the NEXT occurrence of that day,
    including today if today matches. This lets users call
    /btts Sunday on a Friday to get Sunday's fixtures.
    """
    if not text or not text.strip():
        return date.today()

    text = text.strip().lower()

    # Handle "today" and "tomorrow"
    if text == "today":
        return date.today()
    if text == "tomorrow":
        return date.today() + timedelta(days=1)

    # Handle day names: "monday", "tuesday", ..., "sunday"
    day_lookup = {
        "monday": 0, "mon": 0,
        "tuesday": 1, "tue": 1, "tues": 1,
        "wednesday": 2, "wed": 2,
        "thursday": 3, "thu": 3, "thurs": 3,
        "friday": 4, "fri": 4,
        "saturday": 5, "sat": 5,
        "sunday": 6, "sun": 6,
    }

    if text in day_lookup:
        target_dow = day_lookup[text]
        today = date.today()
        days_ahead = (target_dow - today.weekday()) % 7
        # If today matches, days_ahead is 0 — return today
        return today + timedelta(days=days_ahead)

    # Handle ISO date: YYYY-MM-DD
    try:
        return date.fromisoformat(text)
    except ValueError:
        pass

    # Handle DD/MM/YYYY or DD-MM-YYYY
    for fmt in ("%d/%m/%Y", "%d-%m-%Y", "%d %b %Y"):
        try:
            return pd.to_datetime(text, format=fmt).date()
        except (ValueError, TypeError):
            continue

    # Fallback: today
    return date.today()


# ── Metric Calculations ───────────────────────────────────────────────────────

def _last_n_home(df: pd.DataFrame, team: str, before: date, n: int) -> pd.DataFrame:
    """Last N home games for a team, strictly before the given date."""
    mask = (df["HomeTeam"] == team) & (df["Date"].dt.date < before)
    return df[mask].sort_values("Date", ascending=False).head(n)


def _last_n_away(df: pd.DataFrame, team: str, before: date, n: int) -> pd.DataFrame:
    """Last N away games for a team, strictly before the given date."""
    mask = (df["AwayTeam"] == team) & (df["Date"].dt.date < before)
    return df[mask].sort_values("Date", ascending=False).head(n)


def home_btts_count(df: pd.DataFrame, team: str, before: date) -> tuple[int, int]:
    """
    Count of BTTS occurrences in last N home games.
    Returns (btts_count, total_games). If fewer than MIN_GAMES,
    returns (0, 0) to signal insufficient data.
    """
    games = _last_n_home(df, team, before, LOOKBACK_GAMES)
    total = len(games)
    if total < MIN_GAMES:
        return (0, 0)
    btts = int(((games["FTHG"] > 0) & (games["FTAG"] > 0)).sum())
    return (btts, total)


def away_btts_count(df: pd.DataFrame, team: str, before: date) -> tuple[int, int]:
    """
    Count of BTTS occurrences in last N away games.
    Returns (btts_count, total_games). If fewer than MIN_GAMES,
    returns (0, 0) to signal insufficient data.
    """
    games = _last_n_away(df, team, before, LOOKBACK_GAMES)
    total = len(games)
    if total < MIN_GAMES:
        return (0, 0)
    btts = int(((games["FTHG"] > 0) & (games["FTAG"] > 0)).sum())
    return (btts, total)


def home_scored_pct(df: pd.DataFrame, team: str, before: date) -> float:
    """% of last N home games where the home team scored. Context only."""
    games = _last_n_home(df, team, before, LOOKBACK_GAMES)
    if len(games) < MIN_GAMES:
        return 0.0
    return (games["FTHG"] > 0).sum() / len(games)


def away_scored_pct(df: pd.DataFrame, team: str, before: date) -> float:
    """% of last N away games where the away team scored. Context only."""
    games = _last_n_away(df, team, before, LOOKBACK_GAMES)
    if len(games) < MIN_GAMES:
        return 0.0
    return (games["FTAG"] > 0).sum() / len(games)


# ── Signal Classification ─────────────────────────────────────────────────────

def classify_signal(h_btts_n: int, a_btts_n: int,
                    home_odds: float | None) -> str:
    """
    Classify a fixture into a signal tier based on backtested thresholds.

    ⭐ HIGH CONVICTION:
       Home BTTS ≥6/7 + Away BTTS ≥5/7 + Home odds ≥2.3
       Backtested: 75–82% hit rate
       Break-even odds: ~1.22. Typical BTTS market: 1.70–1.90. Huge edge.

    📋 STANDARD:
       Both teams BTTS ≥5/7
       Backtested: ~61% hit rate. Profitable above BTTS odds of ~1.64.

    📊 SHORTLIST:
       Remaining fixtures sorted by confidence (highest‑rated). Informational only.
    """
    if (h_btts_n >= HIGH_CONVICTION_HOME_BTTS
            and a_btts_n >= HIGH_CONVICTION_AWAY_BTTS
            and home_odds is not None
            and home_odds >= HIGH_CONVICTION_HOME_ODDS):
        return "HIGH_CONVICTION"

    if (h_btts_n >= STANDARD_BTTS
            and a_btts_n >= STANDARD_BTTS):
        return "STANDARD"

    return "SHORTLIST"


# ── Fixture Analysis ──────────────────────────────────────────────────────────

def analyse_fixture(
    df: pd.DataFrame,
    home: str,
    away: str,
    fixture_date: date,
    league: str,
    kickoff_time: str = "TBC",
    home_odds: float | None = None,
    away_odds: float | None = None,
) -> dict | None:
    """
    Run the BTTS model for a single fixture.

    Returns None if either team has insufficient history this season
    (fewer than 7 venue-specific games played).

    Confidence is purely BTTS-based:
      confidence = (h_btts / 7) × (a_btts / 7) × 100

    All historical lookups are strictly before the fixture date.
    """
    h_btts_n, h_total = home_btts_count(df, home, fixture_date)
    a_btts_n, a_total = away_btts_count(df, away, fixture_date)

    # Skip if either team hasn't played enough venue games this season
    if h_total == 0 or a_total == 0:
        return None

    h_btts_pct = h_btts_n / h_total
    a_btts_pct = a_btts_n / a_total

    # Confidence = pure BTTS product (no scoring weight)
    confidence = h_btts_pct * a_btts_pct * 100

    # Scored % retained as context info only
    h_scored = home_scored_pct(df, home, fixture_date)
    a_scored = away_scored_pct(df, away, fixture_date)

    # Classify signal tier
    signal = classify_signal(h_btts_n, a_btts_n, home_odds)

    return {
        "league":       league,
        "date":         fixture_date.strftime("%d %b %Y"),
        "day_name":     DAY_NAMES[fixture_date.weekday()],
        "kickoff":      kickoff_time,
        "home":         home,
        "away":         away,
        # BTTS breakdown (primary — drives confidence)
        "h_btts_n":     h_btts_n,
        "a_btts_n":     a_btts_n,
        "h_btts_pct":   round(h_btts_pct * 100, 1),
        "a_btts_pct":   round(a_btts_pct * 100, 1),
        # Scored % (context only — does not affect confidence)
        "h_scored_pct": round(h_scored * 100, 1),
        "a_scored_pct": round(a_scored * 100, 1),
        # Odds
        "home_odds":    home_odds,
        "away_odds":    away_odds,
        # Signal tier
        "signal":       signal,
        # Final confidence (pure BTTS)
        "confidence":   round(confidence, 1),
    }


# ── Main Runner ───────────────────────────────────────────────────────────────

def run_analysis(target_date: date = None) -> list[dict]:
    """
    1. Download fixtures.csv to find games on the target date
    2. Download each league's season CSV for historical stats
    3. Score each eligible fixture and return top N by confidence,
       with signal classification for each pick
    """
    if target_date is None:
        target_date = get_target_date()

    day_name = DAY_NAMES[target_date.weekday()]

    # Step 1 — Get upcoming fixtures with kick-off times (and odds if available)
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

    # Step 2 — Pre-load historical results for each league
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
    skipped = 0

    for _, row in day_fixtures.iterrows():
        code = row["Div"]
        if code not in history:
            continue

        # Extract odds from fixtures CSV if available
        home_odds = _extract_odds(row, HOME_ODDS_COLS)
        away_odds = _extract_odds(row, AWAY_ODDS_COLS)

        result = analyse_fixture(
            df=history[code],
            home=row["HomeTeam"],
            away=row["AwayTeam"],
            fixture_date=row["Date"].date(),
            league=LEAGUE_CODES[code],
            kickoff_time=row.get("Time", "TBC"),
            home_odds=home_odds,
            away_odds=away_odds,
        )

        if result is None:
            skipped += 1
            continue

        all_results.append(result)

    if skipped > 0:
        print(f"  → {skipped} fixtures skipped (insufficient venue history this season)")

    # Sort by confidence descending, return top N
    all_results.sort(key=lambda x: x["confidence"], reverse=True)
    return all_results[:TOP_N]


# ── Formatters ────────────────────────────────────────────────────────────────

def _signal_emoji(signal: str) -> str:
    """Return the emoji prefix for a signal tier."""
    return {
        "HIGH_CONVICTION": "⭐",
        "STANDARD": "📋",
        "SHORTLIST": "📊",
    }.get(signal, "")


def _signal_label(signal: str) -> str:
    """Return the human-readable label for a signal tier."""
    return {
        "HIGH_CONVICTION": "HIGH CONVICTION",
        "STANDARD": "STANDARD",
        "SHORTLIST": "SHORTLIST",
    }.get(signal, "")


def _format_odds(home_odds: float | None, away_odds: float | None) -> str:
    """Format odds pair for display. Returns '—' if unavailable."""
    if home_odds and away_odds:
        return f"H {home_odds:.2f} / A {away_odds:.2f}"
    elif home_odds:
        return f"H {home_odds:.2f}"
    elif away_odds:
        return f"A {away_odds:.2f}"
    return "—"


def format_terminal(results: list[dict], target_date: date = None) -> str:
    """Clean terminal-readable output with signal tiers and odds."""
    if not results:
        if target_date:
            day_name = DAY_NAMES[target_date.weekday()]
            return f"No fixtures found for {day_name} {target_date.strftime('%d %b %Y')}."
        return "No fixtures found for today."

    day_label = f"{results[0]['day_name']} {results[0]['date']}"

    lines = [
        "=" * 66,
        f"  ⚽  BTTS PICKS  —  {day_label}",
        f"  Model: LB{LOOKBACK_GAMES} · Confidence = BTTS form only · Season-isolated",
        "=" * 66,
    ]

    # Group by signal tier for clear visual hierarchy
    high = [r for r in results if r["signal"] == "HIGH_CONVICTION"]
    standard = [r for r in results if r["signal"] == "STANDARD"]
    shortlist = [r for r in results if r["signal"] == "SHORTLIST"]

    def _render_picks(picks, tier_label):
        # always show the tier header; if there are no picks, note that
        lines.append(f"\n{'─' * 66}")
        lines.append(f"  {tier_label}")
        lines.append(f"{'─' * 66}")

        if not picks:
            lines.append("  No matches fit that criteria")
            return

        for i, r in enumerate(picks, 1):
            emoji = _signal_emoji(r["signal"])
            odds_str = _format_odds(r["home_odds"], r["away_odds"])
            lines.extend([
                f"\n  {emoji} {r['home']} vs {r['away']}",
                f"     {r['league']}  |  KO: {r['kickoff']}  |  Odds: {odds_str}",
                f"",
                f"     HOME BTTS   {r['home']}: {r['h_btts_n']}/{LOOKBACK_GAMES} in last {LOOKBACK_GAMES} home games",
                f"     AWAY BTTS   {r['away']}: {r['a_btts_n']}/{LOOKBACK_GAMES} in last {LOOKBACK_GAMES} away games",
                f"",
                f"     Home scored: {r['h_scored_pct']}%  |  Away scored: {r['a_scored_pct']}%",
                f"",
                f"     ★ CONFIDENCE:  {r['confidence']:.1f}%",
            ])

    _render_picks(high, "⭐ HIGH CONVICTION  (H ≥6/7 BTTS · A ≥5/7 BTTS · H odds ≥2.3)")
    _render_picks(standard, "📋 STANDARD  (Both ≥5/7 BTTS)")
    _render_picks(shortlist, "📊 SHORTLIST  (remaining games, highest rated)")

    lines.append("\n" + "=" * 66)
    return "\n".join(lines)


def format_telegram(results: list[dict], target_date: date = None) -> str:
    """Telegram-formatted output — compact, mobile-friendly, tiered."""
    if not results:
        if target_date:
            day_name = DAY_NAMES[target_date.weekday()]
            return f"⚽ No fixtures found for {day_name} {target_date.strftime('%d %b %Y')}."
        return "⚽ No fixtures found for today."

    day_label = f"{results[0]['day_name']} {results[0]['date']}"

    header = f"*⚽ BTTS Picks — {day_label}*\n_LB{LOOKBACK_GAMES} · BTTS confidence · Season-isolated_"

    # Group by signal tier
    high = [r for r in results if r["signal"] == "HIGH_CONVICTION"]
    standard = [r for r in results if r["signal"] == "STANDARD"]
    shortlist = [r for r in results if r["signal"] == "SHORTLIST"]

    blocks = [header]

    def _render_tier(picks, tier_header, tier_note=""):
        # always render header, and show a note if there are no picks
        blocks.append(f"\n{'─' * 24}\n{tier_header}")
        if tier_note:
            blocks.append(f"_{tier_note}_")

        if not picks:
            blocks.append("_No matches fit that criteria._")
            return

        for r in picks:
            odds_str = _format_odds(r["home_odds"], r["away_odds"])
            block = (
                f"\n*{r['home']} vs {r['away']}*\n"
                f"_{r['league']} · {r['kickoff']}_\n"
                f"BTTS: `{r['h_btts_n']}/{LOOKBACK_GAMES}` H | `{r['a_btts_n']}/{LOOKBACK_GAMES}` A\n"
                f"Odds: {odds_str}\n"
                f"🎯 *Confidence: {r['confidence']:.1f}%*"
            )
            blocks.append(block)

    _render_tier(
        high,
        "⭐ *HIGH CONVICTION*",
        "H ≥6/7 BTTS · A ≥5/7 · H odds ≥2.3 · Backtest: 75%+",
    )
    _render_tier(
        standard,
        "📋 *STANDARD*",
        "Both ≥5/7 BTTS · Backtest: ~61%",
    )
    _render_tier(
        shortlist,
        "📊 *SHORTLIST*",
        "Remaining games (highest rated by confidence)",
    )

    # Scan summary and usage tips
    total = len(results)
    hc = len(high)
    footer = (
        f"\n🕐 Scan complete · {total} picks · {hc} high conviction\n"
        "_Weekdays from 2pm · Fridays from 5pm for the full weekend_\n"
        "_Use /btts Saturday or /btts Sunday for a specific day_"
    )
    blocks.append(footer)

    return "\n".join(blocks)


def split_telegram_messages(text: str) -> list[str]:
    """
    Split a long Telegram message into chunks that fit within the
    4096-character limit. Splits on separator lines to keep picks intact.
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
    import sys

    # Accept an optional argument: day name, "tomorrow", or YYYY-MM-DD
    # Examples: python btts_analysis.py Saturday
    #           python btts_analysis.py 2026-03-07
    #           python btts_analysis.py  (defaults to today)
    arg = sys.argv[1] if len(sys.argv) > 1 else None
    target = parse_target_date(arg)

    print(f"\nRunning BTTS Analysis v2 (season {SEASON})...")
    print(f"  Target date: {DAY_NAMES[target.weekday()]} {target.strftime('%d %b %Y')}\n")

    results = run_analysis(target_date=target)
    print(format_terminal(results, target_date=target))

    # Also show what the Telegram message would look like
    print("\n\n" + "=" * 66)
    print("  TELEGRAM PREVIEW")
    print("=" * 66)
    print(format_telegram(results, target_date=target))
