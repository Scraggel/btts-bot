"""
Over 2.5 Goals Analysis Engine
-------------------------------
Scans fixtures for two signals across 12 leagues using per-league
parameters derived from a 3-season backtest.

  Signal 1 (S1) — Leaky Home
    Gate: home team avg goals conceded per home game >= threshold
    Flags: home overs rate, away overs rate, home avg total goals,
           home leaky-dominant rate, away avg goals scored on road
    Minimum flags required: 3
    Odds floor: O2.5 market odds >= league minimum (where applicable)

  Signal 2 (S2) — Strong Away
    Three hard thresholds must ALL be met simultaneously:
      - Away team avg goals scored per away game >= threshold
      - Away team overs rate on road >= threshold
      - Home team avg goals conceded at home >= threshold
    Plus odds floor where applicable.

  Signal 3 (S3) — BTTS corroboration (self-contained, per-league parameters)
    Confidence = (weight × BTTS_product) + ((1-weight) × Scoring_product)
    Per-league: lookback, weight, confidence minimum, home odds minimum,
                over odds minimum. All parameters independent of btts_analysis.py.
    When S3 triggers on a fixture that also has S1 or S2, it corroborates
    the O2.5 pick — both teams likely to score implies 3+ goals.

Leagues:
  European (odds in dataset):
    E0  Premier League      E1  Championship
    E2  League One          E3  League Two
    D1  Bundesliga          D2  2. Bundesliga
    F1  Ligue 1             F2  Ligue 2
    N1  Eredivisie

  Non-European (no odds — verify manually):
    BRA  Brazil Serie A     MEX  Mexico Liga MX
    AUT  Austria Bundesliga

Data sources:
  European history:     football-data.co.uk/mmz4281/{season}/{code}.csv
  Non-European history: football-data.co.uk/new/{code}.csv
  All fixtures:         football-data.co.uk/fixtures.csv

Scheduling:
  Automated: 08:00 daily — covers next 24 hours (catches early Brazilian KOs)
  Manual:    /o25, /o25 tomorrow, /o25 YYYY-MM-DD, /o25 rescan
"""

import os
import pandas as pd
import requests
from io import StringIO
from datetime import date, datetime, timedelta
import warnings
warnings.filterwarnings("ignore")


# ─────────────────────────────────────────────────────────────────────────────
# Season derivation
# ─────────────────────────────────────────────────────────────────────────────

def _derive_season() -> str:
    """
    Auto-derive season code from today's date.
    UK seasons run Aug-May.  Aug 2025 -> '2526', Feb 2026 -> '2526'.
    Override with BTTS_SEASON env var if needed.
    """
    override = os.environ.get("BTTS_SEASON")
    if override:
        return override
    today = date.today()
    start_year = today.year if today.month >= 8 else today.year - 1
    return f"{start_year % 100:02d}{(start_year + 1) % 100:02d}"


SEASON = _derive_season()


# ─────────────────────────────────────────────────────────────────────────────
# League metadata
# ─────────────────────────────────────────────────────────────────────────────

LEAGUE_META = {
    "E0":  {"name": "Premier League",     "is_european": True},
    "E1":  {"name": "Championship",       "is_european": True},
    "E2":  {"name": "League One",         "is_european": True},
    "E3":  {"name": "League Two",         "is_european": True},
    "D1":  {"name": "Bundesliga",         "is_european": True},
    "D2":  {"name": "2. Bundesliga",      "is_european": True},
    "F1":  {"name": "Ligue 1",            "is_european": True},
    "F2":  {"name": "Ligue 2",            "is_european": True},
    "N1":  {"name": "Eredivisie",         "is_european": True},
    "BRA": {"name": "Brazil Serie A",     "is_european": False},
    "MEX": {"name": "Mexico Liga MX",     "is_european": False},
    "AUT": {"name": "Austria Bundesliga", "is_european": False},
}

NON_EU = {k for k, v in LEAGUE_META.items() if not v["is_european"]}


# ─────────────────────────────────────────────────────────────────────────────
# Per-league signal parameters
# Transcribed directly from SIGNALS_PARAMETERS.csv
#
# s1 = Signal 1 (Leaky Home) params, or None if not in the CSV for that league
# s2 = Signal 2 (Strong Away) params, or None if not in the CSV for that league
#
# Fields:
#   tier          - "Selective", "Balanced", or "Volume" (from CSV)
#   lookback      - number of recent venue games to use
#   home_concedes - home avg goals conceded per home game (gate for S1, threshold for S2)
#   home_overs    - home overs rate threshold (S1 flag), decimal
#   away_overs    - away overs rate threshold, decimal
#   home_total    - home avg total goals per home game (S1 flag)
#   home_leaky    - home leaky-dominant rate (S1 flag), decimal
#   away_scored   - away avg goals scored per away game
#   odds_floor    - minimum O2.5 market odds; None = no floor / no odds in dataset
#   breakeven     - reference breakeven for non-EU leagues
# ─────────────────────────────────────────────────────────────────────────────

SIGNAL_PARAMS = {

    # ── E0 — Premier League ──────────────────────────────────────────────────
    # Row 2:  E0  S1-Leaky_Home  Selective  LB6  away_scored=1.2  away_overs=62.5%
    #             home_concedes=1.25  home_overs=62.5%  home_total=3  home_leaky=35%
    # Row 3:  E0  S2-Strong_Away Volume     LB7  away_scored=1.5  away_overs=50%
    #             home_concedes=1.25  odds_floor=1.75
    "E0": {
        "s1": None,
        "s2": {
            "tier": "Volume", "lookback": 7,
            "away_scored": 1.5, "away_overs": 0.50, "home_concedes": 1.25,
            "odds_floor": 1.75,
        },
    },

    # ── E1 — Championship ────────────────────────────────────────────────────
    # Rows 4-5 are BTTS Balanced/Selective — no S1 or S2 O2.5 rows in the CSV
    "E1": {
        "s1": None,
        "s2": None,
    },

    # ── E2 — League One ──────────────────────────────────────────────────────
    # Row 6:  E2  S2-Strong_Away  Balanced  LB6  away_scored=1  away_overs=75%
    #             home_concedes=1.25  odds_floor=1.85
    # Row 7:  E2  BTTS  Selective — handled by btts_analysis
    "E2": {
        "s1": None,
        "s2": {
            "tier": "Balanced", "lookback": 6,
            "away_scored": 1.0, "away_overs": 0.75, "home_concedes": 1.25,
            "odds_floor": 1.80,
        },
    },

    # ── E3 — League Two ──────────────────────────────────────────────────────
    # Row 8:  E3  S2-Strong_Away  Selective  LB6  away_scored=1.5  away_overs=75%
    #             home_concedes=1.25  odds_floor=1.85
    "E3": {
        "s1": None,
        "s2": {
            "tier": "Selective", "lookback": 6,
            "away_scored": 1.5, "away_overs": 0.75, "home_concedes": 1.25,
            "odds_floor": 1.80,
        },
    },

    # ── D1 — Bundesliga ──────────────────────────────────────────────────────
    # Row 10: D1  S2-Strong_Away  Selective  LB7  away_scored=1.5  away_overs=62.5%
    #             home_concedes=1.5  odds_floor=1.65
    "D1": {
        "s1": None,
        "s2": {
            "tier": "Selective", "lookback": 7,
            "away_scored": 1.5, "away_overs": 0.625, "home_concedes": 1.5,
            "odds_floor": 1.65,
        },
    },

    # ── D2 — 2. Bundesliga ───────────────────────────────────────────────────
    # Row 12: D2  S2-Strong_Away  Selective  LB8  away_scored=1.25  away_overs=50%
    #             home_concedes=1.25  odds_floor=1.75
    "D2": {
        "s1": None,
        "s2": {
            "tier": "Selective", "lookback": 8,
            "away_scored": 1.25, "away_overs": 0.50, "home_concedes": 1.25,
            "odds_floor": 1.70,
        },
    },

    # ── F1 — Ligue 1 ─────────────────────────────────────────────────────────
    # Row 14: F1  S2-Strong_Away  Selective  LB8  away_scored=1.5  away_overs=50%
    #             home_concedes=1.25  odds_floor=1.85
    "F1": {
        "s1": None,
        "s2": {
            "tier": "Selective", "lookback": 8,
            "away_scored": 1.5, "away_overs": 0.50, "home_concedes": 1.25,
            "odds_floor": 1.85,
        },
    },

    # ── F2 — Ligue 2 ─────────────────────────────────────────────────────────
    # Row 15: F2  BTTS  Selective — handled by btts_analysis. No S1/S2 row.
    "F2": {
        "s1": None,
        "s2": None,
    },

    # ── N1 — Eredivisie ──────────────────────────────────────────────────────
    # Row 16: N1  S1-Leaky_Home  Balanced  LB8  away_scored=1.2  away_overs=62.5%
    #             home_concedes=0.75  home_overs=62.5%  home_total=2.5  home_leaky=35%
    # Row 17: N1  S2-Strong_Away  Selective  LB8  away_scored=1.25  away_overs=50%
    #             home_concedes=1.0  odds_floor=1.85
    "N1": {
        "s1": {
            "tier": "Balanced", "lookback": 8,
            "home_concedes": 0.75, "home_overs": 0.625, "away_overs": 0.625,
            "home_total": 2.5, "home_leaky": 0.35, "away_scored": 1.2,
            "odds_floor": 1.85,   # matches S2 floor for N1
        },
        "s2": {
            "tier": "Selective", "lookback": 8,
            "away_scored": 1.25, "away_overs": 0.50, "home_concedes": 1.0,
            "odds_floor": 1.85,
        },
    },

    # ── BRA — Brazil Serie A ─────────────────────────────────────────────────
    # Row 19: BRA  S2-Strong_Away  Selective  LB8  away_scored=1  away_overs=75%
    #              home_concedes=1  odds_floor=None  breakeven=1.29
    "BRA": {
        "s1": None,
        "s2": {
            "tier": "Selective", "lookback": 8,
            "away_scored": 1.0, "away_overs": 0.75, "home_concedes": 1.0,
            "odds_floor": None, "breakeven": 1.29,
        },
    },

    # ── MEX — Mexico Liga MX ─────────────────────────────────────────────────
    # Row 21: MEX  BTTS  Selective — handled by btts_analysis. No S1/S2 row.
    "MEX": {
        "s1": None,
        "s2": None,
    },

    # ── AUT — Austria Bundesliga ─────────────────────────────────────────────
    # Row 22: AUT  S2-Strong_Away  Selective  LB8  away_scored=1.5  away_overs=75%
    #              home_concedes=1  odds_floor=None  breakeven=1.57
    "AUT": {
        "s1": None,
        "s2": {
            "tier": "Selective", "lookback": 8,
            "away_scored": 1.5, "away_overs": 0.75, "home_concedes": 1.0,
            "odds_floor": None, "breakeven": 1.57,
        },
    },
}

# Minimum flags required for Signal 1 (from spec — always 3)
S1_MIN_FLAGS = 3

# ─────────────────────────────────────────────────────────────────────────────
# Signal 3 (S3) — BTTS corroboration parameters
# Transcribed from SIGNALS_PARAMETERS.csv (BTTS rows)
#
# Fields:
#   lookback      - venue-specific games lookback
#   weight        - BTTS score weight in confidence formula
#   conf_min      - minimum confidence % to qualify (0-100)
#   home_odds_min - minimum home match result odds; None = no filter
#   over_odds_min - minimum O2.5 market odds; None = no filter / non-EU
#
# Leagues not in this dict have no BTTS row in the CSV and are skipped for S3.
# ─────────────────────────────────────────────────────────────────────────────

S3_PARAMS = {
    # E0  BTTS  Balanced  LB5  weight=0.8  conf=50%  home_odds=2.3  over_odds=1.75
    "E0": {"lookback": 5,  "weight": 0.80, "conf_min": 50, "home_odds_min": 2.1,  "over_odds_min": 1.80},
    # E1  BTTS  Balanced  LB8  weight=0.9  conf=50%  home_odds=2.5  over_odds=1.85
    "E1": {"lookback": 8,  "weight": 0.90, "conf_min": 50, "home_odds_min": 2.1,  "over_odds_min": 1.80},
    # E2  BTTS  Selective  LB5  weight=0.8  conf=50%  home_odds=2.3  over_odds=1.75
    "E2": {"lookback": 5,  "weight": 0.80, "conf_min": 50, "home_odds_min": 2.1,  "over_odds_min": 1.80},
    # E3  BTTS  Volume  LB8  weight=0.9  conf=45%  home_odds=Any  over_odds=1.80
    "E3": {"lookback": 8,  "weight": 0.90, "conf_min": 45, "home_odds_min": 1.7, "over_odds_min": 1.80},
    # D1  BTTS  Volume  LB6  weight=0.8  conf=55%  home_odds=Any  over_odds=All
    "D1": {"lookback": 6,  "weight": 0.80, "conf_min": 55, "home_odds_min": 1.7, "over_odds_min": 1.75},
    # F2  BTTS  Selective  LB8  weight=0.9  conf=40%  home_odds=2.5  over_odds=1.85
    "F2": {"lookback": 8,  "weight": 0.90, "conf_min": 40, "home_odds_min": 2.3,  "over_odds_min": 1.85},
    # N1  BTTS  Volume  LB7  weight=0.7  conf=40%  home_odds=Any  over_odds=1.85
    "N1": {"lookback": 7,  "weight": 0.70, "conf_min": 40, "home_odds_min": 1.7, "over_odds_min": 1.85},
    # BRA  BTTS  Selective  LB8  weight=0.7  conf=55%  home_odds=Any  over_odds=None (non-EU)
    "BRA": {"lookback": 8, "weight": 0.70, "conf_min": 55, "home_odds_min": 1.7, "over_odds_min": None},
    # MEX  BTTS  Selective  LB8  weight=0.9  conf=50%  home_odds=Any  over_odds=None (non-EU)
    "MEX": {"lookback": 8, "weight": 0.90, "conf_min": 50, "home_odds_min": 1.7, "over_odds_min": None},
}




# ─────────────────────────────────────────────────────────────────────────────
# URL helpers
# ─────────────────────────────────────────────────────────────────────────────

# European leagues — fixtures and results are in the main football-data files
EU_FIXTURES_URL  = "https://www.football-data.co.uk/fixtures.csv"

# Non-European leagues — completely separate fixtures file
NON_EU_FIXTURES_URL = "https://www.football-data.co.uk/new_league_fixtures.csv"

# The Div column in new_league_fixtures.csv uses country/league name strings,
# not the short codes we use internally. This maps what the site puts in Div
# to our internal code so we can match fixtures to history files.
NON_EU_DIV_MAP = {
    "Brazil":  "BRA",
    "Mexico":  "MEX",
    "Austria": "AUT",
}

def _history_url(code: str) -> str:
    if code in NON_EU:
        # Non-EU results: football-data.co.uk/new/BRA.csv etc.
        return f"https://www.football-data.co.uk/new/{code}.csv"
    # European results: football-data.co.uk/mmz4281/2526/E0.csv etc.
    return f"https://www.football-data.co.uk/mmz4281/{SEASON}/{code}.csv"


# Home result odds — MaxH is best available across all tracked bookies
HOME_ODDS_COLS = ["MaxH", "AvgH", "B365H", "PSH", "BWH", "IWH"]
# O2.5 odds column priority — Max>2.5 gives the best available price across
# all bookmakers tracked by football-data.co.uk. Avg>2.5 is the market average.
# Individual bookies (B365, Pinnacle) used as fallback when Max/Avg not present.
O25_ODDS_COLS  = ["Max>2.5", "Avg>2.5", "B365>2.5", "P>2.5", "BW>2.5", ">2.5"]

TELEGRAM_MAX_LENGTH = 4096

DAY_NAMES = {
    0: "Monday", 1: "Tuesday", 2: "Wednesday",
    3: "Thursday", 4: "Friday", 5: "Saturday", 6: "Sunday",
}


# ─────────────────────────────────────────────────────────────────────────────
# Data fetching
# ─────────────────────────────────────────────────────────────────────────────

def _fetch_csv(url: str) -> pd.DataFrame:
    """Fetch a CSV from a URL and return a cleaned DataFrame."""
    resp = requests.get(url, timeout=15)
    resp.raise_for_status()
    df = pd.read_csv(StringIO(resp.content.decode("utf-8-sig")))
    df.columns = df.columns.str.strip()
    return df


def download_fixtures() -> pd.DataFrame:
    """
    Download upcoming fixtures from both sources and combine into one DataFrame.

    European leagues:     football-data.co.uk/fixtures.csv
    Non-European leagues: football-data.co.uk/new_league_fixtures.csv

    The non-EU file uses country/league names in the Div column (e.g. 'Brazil')
    rather than short codes. These are remapped to our internal codes via
    NON_EU_DIV_MAP before merging, so all downstream code sees consistent Div values.
    """
    frames = []

    # European fixtures
    try:
        eu = _fetch_csv(EU_FIXTURES_URL)
        eu = eu.dropna(subset=["HomeTeam", "AwayTeam", "Div"])
        frames.append(eu)
    except Exception as e:
        print(f"  -> WARNING: EU fixtures fetch failed ({e})")

    # Non-European fixtures
    try:
        non_eu = _fetch_csv(NON_EU_FIXTURES_URL)
        non_eu = non_eu.dropna(subset=["HomeTeam", "AwayTeam", "Div"])
        # Remap Div values from country names to internal codes
        non_eu["Div"] = non_eu["Div"].map(NON_EU_DIV_MAP)
        # Drop rows where Div didn't map (leagues we don't cover)
        non_eu = non_eu.dropna(subset=["Div"])
        frames.append(non_eu)
    except Exception as e:
        print(f"  -> WARNING: Non-EU fixtures fetch failed ({e})")

    if not frames:
        return pd.DataFrame()

    df = pd.concat(frames, ignore_index=True)
    df["Date"] = pd.to_datetime(df["Date"], dayfirst=True, errors="coerce")
    df = df.dropna(subset=["Date"])
    if "Time" not in df.columns:
        df["Time"] = "TBC"
    else:
        df["Time"] = df["Time"].fillna("TBC").astype(str).str.strip()
        df.loc[df["Time"] == "", "Time"] = "TBC"
    return df


def download_history(code: str) -> pd.DataFrame:
    """Download completed results for a league (European or non-European URL)."""
    url = _history_url(code)
    resp = requests.get(url, timeout=15)
    resp.raise_for_status()
    df = pd.read_csv(StringIO(resp.content.decode("utf-8-sig")))
    df.columns = df.columns.str.strip()
    df = df.dropna(subset=["HomeTeam", "AwayTeam", "FTHG", "FTAG"])
    df["Date"] = pd.to_datetime(df["Date"], dayfirst=True, errors="coerce")
    df = df.dropna(subset=["Date"])
    df["FTHG"] = pd.to_numeric(df["FTHG"], errors="coerce").fillna(0).astype(int)
    df["FTAG"] = pd.to_numeric(df["FTAG"], errors="coerce").fillna(0).astype(int)
    return df


def _extract_odds(row: pd.Series, cols: list) -> float | None:
    """Return first valid numeric odds value from a priority list of columns."""
    for col in cols:
        if col in row.index:
            val = pd.to_numeric(row.get(col), errors="coerce")
            if pd.notna(val) and val > 1.0:
                return round(float(val), 2)
    return None


# ─────────────────────────────────────────────────────────────────────────────
# Date helpers
# ─────────────────────────────────────────────────────────────────────────────

def parse_target_date(text: str | None) -> date:
    """
    Parse a user-supplied date argument for a specific-date override.
    O2.5 normally runs on a 24-hour rolling window — this is only used
    when the user explicitly passes a YYYY-MM-DD date via /o25 YYYY-MM-DD.
    Supports: today / tomorrow / YYYY-MM-DD
    Returns today if input is empty or unrecognised.
    """
    if not text or not text.strip():
        return date.today()
    text = text.strip().lower()
    if text == "today":
        return date.today()
    if text == "tomorrow":
        return date.today() + timedelta(days=1)
    try:
        return date.fromisoformat(text)
    except ValueError:
        pass
    return date.today()


def get_fixtures_in_window(fixtures_df: pd.DataFrame,
                            start_dt: datetime, end_dt: datetime) -> pd.DataFrame:
    """Return fixtures with kick-off between start_dt and end_dt."""
    mask = (
        (fixtures_df["Date"] >= pd.Timestamp(start_dt))
        & (fixtures_df["Date"] < pd.Timestamp(end_dt))
        & (fixtures_df["Div"].isin(LEAGUE_META.keys()))
    )
    return fixtures_df[mask].copy()


def get_fixtures_for_date(fixtures_df: pd.DataFrame, target: date) -> pd.DataFrame:
    """Return fixtures on a specific calendar date."""
    mask = (
        (fixtures_df["Date"].dt.date == target)
        & (fixtures_df["Div"].isin(LEAGUE_META.keys()))
    )
    return fixtures_df[mask].copy()


# ─────────────────────────────────────────────────────────────────────────────
# Statistical metric helpers
# ─────────────────────────────────────────────────────────────────────────────

def _last_n_home(df: pd.DataFrame, team: str, before: date, n: int) -> pd.DataFrame:
    """Last N completed home games, strictly before fixture date."""
    mask = (df["HomeTeam"] == team) & (df["Date"].dt.date < before)
    return df[mask].sort_values("Date", ascending=False).head(n)


def _last_n_away(df: pd.DataFrame, team: str, before: date, n: int) -> pd.DataFrame:
    """Last N completed away games, strictly before fixture date."""
    mask = (df["AwayTeam"] == team) & (df["Date"].dt.date < before)
    return df[mask].sort_values("Date", ascending=False).head(n)


def _min_req(n: int) -> int:
    """
    Minimum games required before computing a metric.
    Full lookback is required — if a team hasn't played enough venue games
    this season the signal is not triggered. No partial history fallback.
    """
    return n


def _avg_home_conceded(df, team, before, n):
    games = _last_n_home(df, team, before, n)
    if len(games) < _min_req(n):
        return None
    return games["FTAG"].mean()


def _avg_away_scored(df, team, before, n):
    games = _last_n_away(df, team, before, n)
    if len(games) < _min_req(n):
        return None
    return games["FTAG"].mean()


def _home_overs_rate(df, team, before, n):
    games = _last_n_home(df, team, before, n)
    if len(games) < _min_req(n):
        return None
    return ((games["FTHG"] + games["FTAG"]) > 2.5).mean()


def _away_overs_rate(df, team, before, n):
    games = _last_n_away(df, team, before, n)
    if len(games) < _min_req(n):
        return None
    return ((games["FTHG"] + games["FTAG"]) > 2.5).mean()


def _home_avg_total(df, team, before, n):
    games = _last_n_home(df, team, before, n)
    if len(games) < _min_req(n):
        return None
    return (games["FTHG"] + games["FTAG"]).mean()


def _home_leaky_rate(df, team, before, n):
    """
    Fraction of home games: team won/drew but conceded in a 3+ goal game.
    E.g. 2-1, 3-1, 4-2 — home team dominant but leaky.
    """
    games = _last_n_home(df, team, before, n)
    if len(games) < _min_req(n):
        return None
    leaky = games[
        (games["FTHG"] >= games["FTAG"])           # won or drew
        & (games["FTAG"] > 0)                      # conceded
        & ((games["FTHG"] + games["FTAG"]) >= 3)   # 3+ total goals
    ]
    return len(leaky) / len(games)


# ─────────────────────────────────────────────────────────────────────────────
# Signal 1 — Leaky Home
# ─────────────────────────────────────────────────────────────────────────────

def check_signal1(df: pd.DataFrame, home: str, away: str,
                  fixture_date: date, code: str) -> dict | None:
    """
    Evaluate Signal 1 (Leaky Home).

    1. Gate: home avg goals conceded per home game >= p["home_concedes"]
    2. Flags: count satisfied conditions out of 5 possible flags
    3. Must meet S1_MIN_FLAGS (3) to qualify
    4. Odds check is handled in the caller (needs the fixture row)

    Returns a result dict or None.
    """
    p = SIGNAL_PARAMS.get(code, {}).get("s1")
    if p is None:
        return None

    lb = p["lookback"]

    # Gate
    h_conc = _avg_home_conceded(df, home, fixture_date, lb)
    if h_conc is None or h_conc < p["home_concedes"]:
        return None

    # Flags
    flags = 0

    h_ov = _home_overs_rate(df, home, fixture_date, lb)
    if h_ov is not None and h_ov >= p["home_overs"]:
        flags += 1

    a_ov = _away_overs_rate(df, away, fixture_date, lb)
    if a_ov is not None and a_ov >= p["away_overs"]:
        flags += 1

    h_tot = _home_avg_total(df, home, fixture_date, lb)
    if h_tot is not None and h_tot >= p["home_total"]:
        flags += 1

    h_lk = _home_leaky_rate(df, home, fixture_date, lb)
    if h_lk is not None and h_lk >= p["home_leaky"]:
        flags += 1

    a_sc = _avg_away_scored(df, away, fixture_date, lb)
    if a_sc is not None and a_sc >= p["away_scored"]:
        flags += 1

    if flags < S1_MIN_FLAGS:
        return None

    return {
        "signal":     "S1",
        "tier":       p["tier"],
        "home_conc":  round(h_conc, 2),
        "flags":      flags,
        "odds_floor": p.get("odds_floor"),
        "breakeven":  p.get("breakeven"),
    }


# ─────────────────────────────────────────────────────────────────────────────
# Signal 2 — Strong Away
# ─────────────────────────────────────────────────────────────────────────────

def check_signal2(df: pd.DataFrame, home: str, away: str,
                  fixture_date: date, code: str) -> dict | None:
    """
    Evaluate Signal 2 (Strong Away).
    All three hard thresholds must be met simultaneously.
    Returns a result dict or None.
    """
    p = SIGNAL_PARAMS.get(code, {}).get("s2")
    if p is None:
        return None

    lb = p["lookback"]

    a_sc = _avg_away_scored(df, away, fixture_date, lb)
    if a_sc is None or a_sc < p["away_scored"]:
        return None

    a_ov = _away_overs_rate(df, away, fixture_date, lb)
    if a_ov is None or a_ov < p["away_overs"]:
        return None

    h_conc = _avg_home_conceded(df, home, fixture_date, lb)
    if h_conc is None or h_conc < p["home_concedes"]:
        return None

    return {
        "signal":      "S2",
        "tier":        p["tier"],
        "away_scored": round(a_sc, 2),
        "away_overs":  round(a_ov * 100, 1),
        "home_conc":   round(h_conc, 2),
        "odds_floor":  p.get("odds_floor"),
        "breakeven":   p.get("breakeven"),
    }


# ─────────────────────────────────────────────────────────────────────────────
# Odds floor check
# ─────────────────────────────────────────────────────────────────────────────

def passes_odds_floor(o25_odds: float | None, sig: dict,
                      code: str) -> tuple[bool, str]:
    """
    Returns (passes: bool, display_note: str).

    Non-EU leagues: always pass — user must verify manually.
    EU leagues:     fail if O2.5 odds are below the floor (pick is excluded).
                    If odds not in CSV, pass with a note.
    """
    odds_floor = sig.get("odds_floor")
    breakeven  = sig.get("breakeven")

    if code in NON_EU:
        be_str = f"BE: {breakeven:.2f}" if breakeven else "verify odds"
        return True, f"No odds data · {be_str}"

    if odds_floor is None:
        # No floor defined for this signal — always pass
        note = f"O2.5 {o25_odds:.2f}" if o25_odds else "O2.5 — (no floor)"
        return True, note

    if o25_odds is None:
        # Odds not available in fixture row — show pick with note
        return True, f"O2.5 not in data (floor: {odds_floor:.2f})"

    if o25_odds >= odds_floor:
        return True, f"O2.5 {o25_odds:.2f} (floor: {odds_floor:.2f})"
    else:
        return False, f"O2.5 {o25_odds:.2f} below floor {odds_floor:.2f}"


# ─────────────────────────────────────────────────────────────────────────────
# Main analysis runner
# ─────────────────────────────────────────────────────────────────────────────

def run_analysis(target_date: date = None,
                 use_24h_window: bool = False) -> list[dict]:
    """
    Run the O2.5 analysis.

    Args:
        target_date:     Analyse fixtures on this specific calendar date.
        use_24h_window:  If True (used by the 8am scheduled job), analyse all
                         fixtures in the next 24 hours. This catches early
                         Brazilian kick-offs that fall past midnight local time.

    Returns a list of qualifying fixture dicts, sorted:
      S1 first, S2 second; Selective before Balanced/Volume; then by kick-off.
    """
    print(f"\nO2.5 Analysis — season {SEASON}")

    # Download fixtures
    print("  -> Fetching fixtures...", end=" ", flush=True)
    try:
        fixtures_df = download_fixtures()
        print(f"{len(fixtures_df)} rows")
    except Exception as e:
        print(f"FAILED ({e})")
        return []

    # Select window
    if use_24h_window:
        now = datetime.now()
        end = now + timedelta(hours=24)
        day_fixtures = get_fixtures_in_window(fixtures_df, now, end)
        window_label = f"next 24h (until {end.strftime('%d %b %H:%M')})"
    else:
        if target_date is None:
            target_date = date.today()
        day_fixtures = get_fixtures_for_date(fixtures_df, target_date)
        window_label = f"{DAY_NAMES[target_date.weekday()]} {target_date.strftime('%d %b %Y')}"

    if day_fixtures.empty:
        print(f"  -> No fixtures for {window_label}")
        return []

    print(f"  -> {len(day_fixtures)} fixtures · {window_label}")

    # Load history for each league that has S1 or S2 configured
    leagues_needed = [
        c for c in day_fixtures["Div"].unique()
        if c in LEAGUE_META
        and (SIGNAL_PARAMS.get(c, {}).get("s1") is not None
             or SIGNAL_PARAMS.get(c, {}).get("s2") is not None)
    ]
    history = {}
    for code in leagues_needed:
        name = LEAGUE_META[code]["name"]
        print(f"  -> Loading {name}...", end=" ", flush=True)
        try:
            history[code] = download_history(code)
            print(f"{len(history[code])} results")
        except Exception as e:
            print(f"FAILED ({e})")

    # Analyse
    results = []
    skipped_odds = 0

    for _, row in day_fixtures.iterrows():
        code = row["Div"]
        if code not in history:
            continue

        df      = history[code]
        home    = row["HomeTeam"]
        away    = row["AwayTeam"]
        fdate   = row["Date"].date()
        kickoff = str(row.get("Time", "TBC")).strip() or "TBC"
        league  = LEAGUE_META[code]["name"]

        o25_odds  = _extract_odds(row, O25_ODDS_COLS)
        home_odds = _extract_odds(row, HOME_ODDS_COLS)

        for check_fn in (check_signal1, check_signal2):
            sig = check_fn(df, home, away, fdate, code)
            if sig is None:
                continue

            passes, odds_note = passes_odds_floor(o25_odds, sig, code)
            if not passes:
                skipped_odds += 1
                continue

            # S3 evaluated here — self-contained, no external dependency
            s3 = check_signal3(df, home, away, fdate, code, home_odds, o25_odds)

            results.append({
                "code":      code,
                "league":    league,
                "date":      fdate.strftime("%d %b %Y"),
                "day_name":  DAY_NAMES[fdate.weekday()],
                "kickoff":   kickoff,
                "home":      home,
                "away":      away,
                "home_odds": home_odds,
                "o25_odds":  o25_odds,
                "odds_note": odds_note,
                "is_non_eu": code in NON_EU,
                "s3":        s3,
                **sig,
            })

    if skipped_odds:
        print(f"  -> {skipped_odds} picks excluded (odds below floor)")
    print(f"  -> Complete. {len(results)} qualifying picks.")

    # Sort: S1 before S2, Selective before Balanced/Volume, then kickoff
    tier_order = {"Selective": 0, "Balanced": 1, "Volume": 2}
    results.sort(key=lambda x: (
        0 if x["signal"] == "S1" else 1,
        tier_order.get(x["tier"], 9),
        x["kickoff"],
    ))
    return results


# ─────────────────────────────────────────────────────────────────────────────
# Signal 3 — BTTS corroboration
# ─────────────────────────────────────────────────────────────────────────────

def check_signal3(df: pd.DataFrame, home: str, away: str,
                  fixture_date: date, code: str,
                  home_odds: float | None,
                  o25_odds: float | None) -> bool:
    """
    Evaluate Signal 3 (BTTS corroboration) for a fixture.

    Uses per-league parameters from S3_PARAMS — completely independent
    of btts_analysis.py which only covers UK leagues with fixed thresholds.

    Confidence formula — pure BTTS product:
      confidence = home_btts_pct × away_btts_pct × 100

    Full lookback required — if either team hasn't played enough venue games
    this season the signal returns False. No partial history fallback.

    Returns True if the fixture qualifies as S3, False otherwise.
    """
    p = S3_PARAMS.get(code)
    if p is None:
        return False  # League not in S3 parameters — skip

    lb   = p["lookback"]
    # Full lookback required — skip if team doesn't have enough games this season
    minr = lb

    # ── Home BTTS rate in last N home games ───────────────────────────────────
    h_games = _last_n_home(df, home, fixture_date, lb)
    if len(h_games) < minr:
        return False
    h_btts_pct = ((h_games["FTHG"] > 0) & (h_games["FTAG"] > 0)).mean()

    # ── Away BTTS rate in last N away games ───────────────────────────────────
    a_games = _last_n_away(df, away, fixture_date, lb)
    if len(a_games) < minr:
        return False
    a_btts_pct = ((a_games["FTHG"] > 0) & (a_games["FTAG"] > 0)).mean()

    # ── Confidence — pure BTTS product ────────────────────────────────────────
    confidence = h_btts_pct * a_btts_pct * 100

    if confidence < p["conf_min"]:
        return False

    # ── Home odds filter (None = no filter for leagues with 'Any') ────────────
    if p["home_odds_min"] is not None:
        if home_odds is None or home_odds < p["home_odds_min"]:
            return False

    # ── Over odds filter (None = no filter for non-EU leagues) ────────────────
    if p["over_odds_min"] is not None and code not in NON_EU:
        if o25_odds is not None and o25_odds < p["over_odds_min"]:
            return False

    return True


# ─────────────────────────────────────────────────────────────────────────────
# Telegram formatter
# ─────────────────────────────────────────────────────────────────────────────

def format_telegram(results: list[dict],
                    target_date: date = None,
                    use_24h_window: bool = False) -> str:
    """
    Format O2.5 results as a Telegram message.
    S3 is read directly from each result dict (evaluated in run_analysis).
    """

    # Empty result message
    if not results:
        if use_24h_window:
            end = datetime.now() + timedelta(hours=24)
            label = f"next 24h (until {end.strftime('%d %b %H:%M')})"
        elif target_date:
            label = f"{DAY_NAMES[target_date.weekday()]} {target_date.strftime('%d %b %Y')}"
        else:
            label = "today"
        return f"*📈 O2.5 Signals*\n_No qualifying fixtures for {label}._"

    # Window label for header
    if use_24h_window:
        end = datetime.now() + timedelta(hours=24)
        window_str = f"Next 24h — until {end.strftime('%d %b %H:%M')}"
    elif target_date:
        window_str = f"{DAY_NAMES[target_date.weekday()]} {target_date.strftime('%d %b %Y')}"
    else:
        window_str = results[0]["day_name"] + " " + results[0]["date"]

    blocks = [f"*📈 O2.5 Signals — {window_str}*"]

    def _tags(r: dict) -> str:
        """
        Build signal tag string.
        (S3) means BTTS form is strong on this fixture — both teams are
        likely to score, which corroborates the O2.5 signal. Higher confidence
        in the overs pick. Not a separate bet suggestion.
        """
        t = f"({r['signal']})"
        if r.get("s3"):
            t += "(S3)"
        return t

    def _render_group(picks: list, header: str, note: str = "") -> None:
        if not picks:
            return
        blocks.append(f"\n{'─' * 24}\n{header}")
        if note:
            blocks.append(f"_{note}_")
        for r in picks:
            tags = _tags(r)
            if r["signal"] == "S1":
                metrics = f"Home concedes: `{r['home_conc']}`/g · Flags: `{r['flags']}/5`"
            else:
                metrics = (
                    f"Away scored: `{r['away_scored']}`/g · "
                    f"Away O2.5: `{r['away_overs']}%` · "
                    f"Home concedes: `{r['home_conc']}`/g"
                )
            block = (
                f"\n*{r['home']} vs {r['away']}* {tags}\n"
                f"_{r['league']} · {r['kickoff']}_\n"
                f"{metrics}\n"
                f"Odds: {r['odds_note']}"
            )
            blocks.append(block)

    # S1 — Leaky Home
    s1_sel = [r for r in results if r["signal"] == "S1" and r["tier"] == "Selective"]
    s1_oth = [r for r in results if r["signal"] == "S1" and r["tier"] != "Selective"]
    _render_group(s1_sel, "🏠 *LEAKY HOME — Selective*",   "High ROI tier")
    _render_group(s1_oth, "🏠 *LEAKY HOME — Balanced/Volume*", "Good ROI + volume")

    # S2 — Strong Away
    s2_sel = [r for r in results if r["signal"] == "S2" and r["tier"] == "Selective"]
    s2_oth = [r for r in results if r["signal"] == "S2" and r["tier"] != "Selective"]
    _render_group(s2_sel, "✈️ *STRONG AWAY — Selective*",   "Best overall ROI signal")
    _render_group(s2_oth, "✈️ *STRONG AWAY — Balanced/Volume*", "Good ROI + volume")

    # Footer
    total  = len(results)
    sel    = sum(1 for r in results if r["tier"] == "Selective")
    scan_t = datetime.now().strftime("%H:%M")

    s3_note = f" · {sum(1 for r in results if r.get('s3'))} corroborated by BTTS form" if any(r.get("s3") for r in results) else ""
    blocks.append(
        f"\n🕙 {scan_t} · {total} picks · {sel} selective{s3_note}\n"
        "_Tags: (S1) Leaky Home · (S2) Strong Away_\n"
        "_(S3) = BTTS form corroborates this pick — both teams likely to score adds confidence to O2.5_\n"
        "_Non-EU: no odds in data — verify manually before betting_\n"
        "_/o25 rescan to refresh_"
    )

    return "\n".join(blocks)


def split_telegram_messages(text: str) -> list[str]:
    """Split a long message into <=4096-char chunks, keeping picks intact."""
    if len(text) <= TELEGRAM_MAX_LENGTH:
        return [text]
    messages, current = [], ""
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


# ─────────────────────────────────────────────────────────────────────────────
# CLI entry point
# ─────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import sys
    arg    = sys.argv[1] if len(sys.argv) > 1 else None
    target = parse_target_date(arg)
    print(f"Target: {DAY_NAMES[target.weekday()]} {target.strftime('%d %b %Y')}")
    results = run_analysis(target_date=target)
    print(format_telegram(results, target_date=target))
