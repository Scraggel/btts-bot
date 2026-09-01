"""
Over 2.5 Goals Analysis Engine
-------------------------------
Scans fixtures for two signals across 13 leagues using per-league
parameters derived from a 3-season backtest.

  Signal 1 (S1) — Leaky Home
    Gate: home team avg goals conceded per home game >= threshold
    Flags: home overs rate, away overs rate, home avg total goals,
           home leaky-dominant rate, away avg goals scored on road
    Minimum flags required: 3 by default, some leagues require 4
    Odds floor: O2.5 market odds >= league minimum (where applicable)

  Signal 2 (S2) — Strong Away
    Three hard thresholds must ALL be met simultaneously:
      - Away team avg goals scored per away game >= threshold
      - Away team overs rate on road >= threshold
      - Home team avg goals conceded at home >= threshold
    Plus odds floor where applicable.

  Signal 3 (S3) — BTTS Form (first-class signal, per-league parameters)
    Confidence = home_btts_pct (last N home games) x away_btts_pct (last N away games) x 100
    Three gates: confidence >= conf_min  AND  home_odds >= home_odds_min
                 AND  o25_odds >= o25_floor (aligned to S2 floor)
    Fires standalone when S1/S2 do not trigger; also shown as a corroboration
    tag (S1)(S3) or (S2)(S3) when S3 qualifies on the same fixture.

Leagues:
    E0  Premier League        E1  Championship
    E2  League One            E3  League Two
    D1  Bundesliga            D2  2. Bundesliga
    F1  Ligue 1               F2  Ligue 2
    N1  Eredivisie
    SP1 La Liga
    B1  Belgian Pro League
    P1  Primeira Liga         I1  Serie A

Data sources:
  History:   football-data.co.uk/mmz4281/{season}/{code}.csv
  Fixtures:  football-data.co.uk/fixtures.csv

Scheduling:
  Automated: 08:00 daily — covers next 24 hours
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


def _season_span(n_seasons: int) -> list[str]:
    """
    Return the last n_seasons season codes, most recent first.
    E.g. SEASON='2627', n_seasons=2 -> ['2627', '2526']
    Used by o25_lb to pull history that spans multiple seasons.
    """
    start_year = int(SEASON[:2])
    codes = []
    for i in range(n_seasons):
        y1 = start_year - i
        y2 = y1 + 1
        codes.append(f"{y1 % 100:02d}{y2 % 100:02d}")
    return codes


# ─────────────────────────────────────────────────────────────────────────────
# League metadata
# ─────────────────────────────────────────────────────────────────────────────

LEAGUE_META = {
    "E0":  {"name": "Premier League"},
    "E1":  {"name": "Championship"},
    "E2":  {"name": "League One"},
    "E3":  {"name": "League Two"},
    "D1":  {"name": "Bundesliga"},
    "D2":  {"name": "2. Bundesliga"},
    "F1":  {"name": "Ligue 1"},
    "F2":  {"name": "Ligue 2"},
    "N1":  {"name": "Eredivisie"},
    "SP1": {"name": "La Liga"},
    "B1":  {"name": "Belgian Pro League"},
    "P1":  {"name": "Primeira Liga"},
    "I1":  {"name": "Serie A"},
}


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
#   odds_floor    - minimum O2.5 market odds; None = no floor
# ─────────────────────────────────────────────────────────────────────────────

SIGNAL_PARAMS = {

    # ── E0 — Premier League ──────────────────────────────────────────────────
    "E0": {
        "s1": {
            "tier": "Selective", "lookback": 8,
            "home_concedes": 1.25, "home_overs": 0.625, "away_overs": 0.625,
            "home_total": 3, "home_leaky": 0.35, "away_scored": 1.2,
            "odds_floor": 1.85, "min_flags": 4,
        },
        "s2": {
            "tier": "Selective", "lookback": 6,
            "away_scored": 1.5, "away_overs": 0.625, "home_concedes": 1.25,
            "odds_floor": 1.75,
        },
    },

    # ── E1 — Championship ────────────────────────────────────────────────────
    "E1": {
        "s1": None,
        "s2": {
            "tier": "Selective", "lookback": 6,
            "away_scored": 1.5, "away_overs": 0.75, "home_concedes": 1.5,
            "odds_floor": 1.75,
        },
    },

    # ── E2 — League One ──────────────────────────────────────────────────────
    "E2": {
        "s1": None,
        "s2": {
            "tier": "Balanced", "lookback": 6,
            "away_scored": 1.25, "away_overs": 0.75, "home_concedes": 1.25,
            "odds_floor": 1.85,
        },
    },

    # ── E3 — League Two ──────────────────────────────────────────────────────
    "E3": {
        "s1": None,
        "s2": {
            "tier": "Selective", "lookback": 6,
            "away_scored": 1.5, "away_overs": 0.75, "home_concedes": 1.25,
            "odds_floor": 1.85,
        },
    },

    # ── D1 — Bundesliga ──────────────────────────────────────────────────────
    "D1": {
        "s1": None,
        "s2": {
            "tier": "Selective", "lookback": 7,
            "away_scored": 1.5, "away_overs": 0.625, "home_concedes": 1.5,
            "odds_floor": 1.65,
        },
    },

    # ── D2 — 2. Bundesliga ───────────────────────────────────────────────────
    "D2": {
        "s1": None,
        "s2": {
            "tier": "Selective", "lookback": 8,
            "away_scored": 1.25, "away_overs": 0.50, "home_concedes": 1.25,
            "odds_floor": 1.75,
        },
    },

    # ── F1 — Ligue 1 ─────────────────────────────────────────────────────────
    "F1": {
        "s1": None,
        "s2": {
            "tier": "Selective", "lookback": 8,
            "away_scored": 1.5, "away_overs": 0.625, "home_concedes": 1.25,
            "odds_floor": 1.85,
        },
    },

    # ── F2 — Ligue 2 ─────────────────────────────────────────────────────────
    "F2": {
        "s1": None,
        "s2": None,
    },

    # ── N1 — Eredivisie ──────────────────────────────────────────────────────
    "N1": {
        "s1": {
            "tier": "Selective", "lookback": 7,
            "home_concedes": 0.75, "home_overs": 0.625, "away_overs": 0.625,
            "home_total": 2.5, "home_leaky": 0.35, "away_scored": 1.2,
            "odds_floor": 1.85, "min_flags": 4,
        },
        "s2": {
            "tier": "Selective", "lookback": 7,
            "away_scored": 1.25, "away_overs": 0.625, "home_concedes": 1.0,
            "odds_floor": 1.85,
        },
    },

    # ── SP1 — La Liga ────────────────────────────────────────────────────────
    "SP1": {
        "s1": {
            "tier": "Balanced", "lookback": 6,
            "home_concedes": 1.5, "home_overs": 0.625, "away_overs": 0.625,
            "home_total": 3.0, "home_leaky": 0.45, "away_scored": 1.4,
            "odds_floor": 1.80, "min_flags": 3,
        },
        "s2": None,
    },

    # ── B1 — Belgian Pro League ──────────────────────────────────────────────
    "B1": {
        "s1": None,
        "s2": {
            "tier": "Balanced", "lookback": 8,
            "away_scored": 1.5, "away_overs": 0.75, "home_concedes": 1.25,
            "odds_floor": 1.75,
        },
    },

    # ── P1 — Primeira Liga ───────────────────────────────────────────────────
    "P1": {
        "s1": {"tier": "Balanced", "lookback": 6,
            "home_concedes": 1.0, "home_overs": 0.50, "away_overs": 0.625,
            "home_total": 2.5, "home_leaky": 0.45, "away_scored": 1.4,
            "odds_floor": 2.05, "min_flags": 3},
        "s2": None,                      # inverted curve, 1.7 bets/season, -7.8%
    },

    # ── I1 — Serie A ─────────────────────────────────────────────────────────
    "I1": {
        "s1": {"tier": "Selective", "lookback": 7,
            "home_concedes": 1.0, "home_overs": 0.625, "away_overs": 0.625,
            "home_total": 2.5, "home_leaky": 0.35, "away_scored": 1.2,
            "odds_floor": 2.05, "min_flags": 3},
        "s2": None,
    },
}

# Default minimum flags required for Signal 1.
# Individual leagues may override this via the "min_flags" key in their s1 params.
S1_MIN_FLAGS = 3

# ─────────────────────────────────────────────────────────────────────────────
# Signal 3 (S3) — BTTS form parameters
#
# S3 qualifies on three gates:
#   1. confidence = home_btts_pct (last N home games) x away_btts_pct (last N away games) x 100
#      must reach conf_min
#   2. home_odds >= home_odds_min  (filters out heavy favourites)
#   3. o25_odds >= o25_floor       (aligned to S2 odds floor for the same league)
#
# Fields:
#   lookback      - venue-specific games lookback (full window required)
#   conf_min      - minimum confidence % to qualify
#   home_odds_min - minimum home match result odds; None = no filter
#   o25_floor     - minimum O2.5 odds; None = no floor
#
# Leagues not in this dict have no S3 parameters and are skipped.
# ─────────────────────────────────────────────────────────────────────────────

S3_PARAMS = {
    "E0":  {"lookback": 6,  "conf_min": 50, "home_odds_min": 2.3,  "o25_floor": 1.80},
    "E1":  {"lookback": 8,  "conf_min": 50, "home_odds_min": 2.3,  "o25_floor": 1.85},
    "E2":  {"lookback": 6,  "conf_min": 50, "home_odds_min": 2.1,  "o25_floor": 1.85},
    "E3":  {"lookback": 8,  "conf_min": 50, "home_odds_min": 2.3,  "o25_floor": 1.80},
    "D1":  {"lookback": 6,  "conf_min": 50, "home_odds_min": 2.0,  "o25_floor": 1.65},
    "D2":  {"lookback": 8,  "conf_min": 50, "home_odds_min": 2.3,  "o25_floor": 1.85},
    "F2":  {"lookback": 6,  "conf_min": 45, "home_odds_min": 2.3,  "o25_floor": 1.85},
    "N1":  {"lookback": 6,  "conf_min": 50, "home_odds_min": 2.0,  "o25_floor": 1.80},
    "I1":  {"lookback": 7,  "conf_min": 45, "home_odds_min": 2.3,  "o25_floor": 1.90},
    "SP1": {"lookback": 7,  "conf_min": 50, "home_odds_min": 2.3,  "o25_floor": 1.85},
    "B1":  {"lookback": 8,  "conf_min": 50, "home_odds_min": 2.3,  "o25_floor": 1.85},
}

# ─────────────────────────────────────────────────────────────────────────────
# URL helpers
# ─────────────────────────────────────────────────────────────────────────────

FIXTURES_URL = "https://www.football-data.co.uk/fixtures.csv"


def _history_url(code: str, season: str = None) -> str:
    return f"https://www.football-data.co.uk/mmz4281/{season or SEASON}/{code}.csv"


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
    Download upcoming fixtures.
    Source: football-data.co.uk/fixtures.csv
    """
    df = _fetch_csv(FIXTURES_URL)
    df = df.dropna(subset=["HomeTeam", "AwayTeam", "Div"])
    df["Date"] = pd.to_datetime(df["Date"], dayfirst=True, errors="coerce")
    df = df.dropna(subset=["Date"])
    if "Time" not in df.columns:
        df["Time"] = "TBC"
    else:
        df["Time"] = df["Time"].fillna("TBC").astype(str).str.strip()
        df.loc[df["Time"] == "", "Time"] = "TBC"
    return df


def download_history(code: str) -> pd.DataFrame:
    """Download completed results for a league."""
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


def download_history_span(code: str, n_seasons: int = 2) -> pd.DataFrame:
    """
    Download and concatenate completed results for a league across the last
    n_seasons seasons (most recent first), for cross-season lookback (o25_lb).

    Each season's file only contains games that division actually played that
    season, so a team that was promoted or relegated simply won't appear in
    the season(s) it wasn't in this division — no extra filtering needed.
    """
    frames = []
    for season in _season_span(n_seasons):
        url = _history_url(code, season)
        try:
            resp = requests.get(url, timeout=15)
            resp.raise_for_status()
            df = pd.read_csv(StringIO(resp.content.decode("utf-8-sig")))
            df.columns = df.columns.str.strip()
            frames.append(df)
        except Exception as e:
            print(f"  -> WARNING: {code} {season} history fetch failed ({e})")

    if not frames:
        raise RuntimeError(f"No history available for {code} across {n_seasons} seasons")

    df = pd.concat(frames, ignore_index=True)
    df = df.dropna(subset=["HomeTeam", "AwayTeam", "FTHG", "FTAG"])
    df["Date"] = pd.to_datetime(df["Date"], dayfirst=True, errors="coerce")
    df = df.dropna(subset=["Date"])
    df["FTHG"] = pd.to_numeric(df["FTHG"], errors="coerce").fillna(0).astype(int)
    df["FTAG"] = pd.to_numeric(df["FTAG"], errors="coerce").fillna(0).astype(int)
    return df.sort_values("Date").reset_index(drop=True)


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
    3. Must meet the required flag count — defaults to S1_MIN_FLAGS (3) but
       individual leagues can override via "min_flags" in their s1 params.
    4. Odds check is handled in the caller (needs the fixture row)

    Returns a result dict or None.
    """
    p = SIGNAL_PARAMS.get(code, {}).get("s1")
    if p is None:
        return None

    lb = p["lookback"]
    # Per-league override; fall back to global default
    min_flags = p.get("min_flags", S1_MIN_FLAGS)

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

    if flags < min_flags:
        return None

    return {
        "signal":     "S1",
        "tier":       p["tier"],
        "home_conc":  round(h_conc, 2),
        "flags":      flags,
        "min_flags":  min_flags,
        "odds_floor": p.get("odds_floor"),
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
    }


# ─────────────────────────────────────────────────────────────────────────────
# Odds floor check
# ─────────────────────────────────────────────────────────────────────────────

def passes_odds_floor(o25_odds: float | None, sig: dict) -> tuple[bool, str]:
    """
    Returns (passes: bool, display_note: str).
    Fails if O2.5 odds are below the league's floor (pick excluded).
    If no floor is defined, or odds aren't present in the data, the pick
    passes through with a note.
    """
    odds_floor = sig.get("odds_floor")

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
#
# run_analysis() and o25_lb() share the same fixture-selection and signal
# logic below — the only thing that differs between them is how league
# history is loaded (single season vs. multi-season). That shared logic
# lives in _select_day_fixtures(), _leagues_needed(), _load_history(), and
# _analyze_fixtures().
# ─────────────────────────────────────────────────────────────────────────────

def _select_day_fixtures(fixtures_df: pd.DataFrame,
                         target_date: date = None,
                         use_24h_window: bool = False) -> tuple[pd.DataFrame, str]:
    """Pick the fixture window (24h rolling or a specific date) and label it."""
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
    return day_fixtures, window_label


def _leagues_needed(day_fixtures: pd.DataFrame) -> list[str]:
    """
    Leagues with S1, S2, or S3 configured that appear in this fixture window.
    S3-only leagues (e.g. F2) are included so standalone S3 picks can be
    evaluated even when S1/S2 are not active for that league.
    """
    return [
        c for c in day_fixtures["Div"].unique()
        if c in LEAGUE_META
        and (SIGNAL_PARAMS.get(c, {}).get("s1") is not None
             or SIGNAL_PARAMS.get(c, {}).get("s2") is not None
             or c in S3_PARAMS)
    ]


def _load_history(leagues_needed: list[str], history_fn) -> dict:
    """Load history for each needed league using the given loader function."""
    history = {}
    for code in leagues_needed:
        name = LEAGUE_META[code]["name"]
        print(f"  -> Loading {name}...", end=" ", flush=True)
        try:
            history[code] = history_fn(code)
            print(f"{len(history[code])} results")
        except Exception as e:
            print(f"FAILED ({e})")
    return history


def _analyze_fixtures(day_fixtures: pd.DataFrame, history: dict) -> list[dict]:
    """
    Run S1/S2/S3 checks over every fixture in day_fixtures, using the given
    per-league history dict. Returns a sorted list of qualifying picks.
    """
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

        # ── S1 / S2 ──────────────────────────────────────────────────────────
        # Track which fixtures already have an S1/S2 result so S3 can mark
        # itself as corroborating rather than standalone on the same fixture.
        s12_fixture_keys = set()

        for check_fn in (check_signal1, check_signal2):
            sig = check_fn(df, home, away, fdate, code)
            if sig is None:
                continue

            passes, odds_note = passes_odds_floor(o25_odds, sig)
            if not passes:
                skipped_odds += 1
                continue

            # S3 evaluated alongside S1/S2 to determine corroboration flag.
            s3_result = check_signal3(df, home, away, fdate, code, home_odds, o25_odds)
            s12_fixture_keys.add((home, away, fdate))

            results.append({
                "code":        code,
                "league":      league,
                "date":        fdate.strftime("%d %b %Y"),
                "day_name":    DAY_NAMES[fdate.weekday()],
                "kickoff":     kickoff,
                "home":        home,
                "away":        away,
                "home_odds":   home_odds,
                "o25_odds":    o25_odds,
                "odds_note":   odds_note,
                "s3":          s3_result is not None,   # corroboration flag
                **sig,
            })

        # ── S3 standalone ────────────────────────────────────────────────────
        # Only add a standalone S3 pick if this fixture was NOT already
        # captured by S1 or S2 above. S3-only leagues (e.g. F2) have no
        # S1/S2 config so they will always reach this branch.
        fixture_key = (home, away, fdate)
        if fixture_key not in s12_fixture_keys:
            s3_result = check_signal3(df, home, away, fdate, code, home_odds, o25_odds)
            if s3_result is not None:
                floor = s3_result.get("o25_floor")
                if o25_odds is not None:
                    odds_note = f"O2.5 {o25_odds:.2f}" + (f" (floor: {floor:.2f})" if floor else "")
                else:
                    odds_note = f"O2.5 not in data" + (f" (floor: {floor:.2f})" if floor else "")

                results.append({
                    "code":        code,
                    "league":      league,
                    "date":        fdate.strftime("%d %b %Y"),
                    "day_name":    DAY_NAMES[fdate.weekday()],
                    "kickoff":     kickoff,
                    "home":        home,
                    "away":        away,
                    "home_odds":   home_odds,
                    "o25_odds":    o25_odds,
                    "odds_note":   odds_note,
                    "s3":          False,   # this IS the S3 pick — no separate corroboration flag
                    **s3_result,
                })

    if skipped_odds:
        print(f"  -> {skipped_odds} picks excluded (odds below floor)")
    print(f"  -> Complete. {len(results)} qualifying picks.")

    # Sort: S1 before S2 before S3, Selective before Balanced/Volume, then kickoff
    tier_order = {"Selective": 0, "Balanced": 1, "Volume": 2}
    sig_order  = {"S1": 0, "S2": 1, "S3": 2}
    results.sort(key=lambda x: (
        sig_order.get(x["signal"], 9),
        tier_order.get(x["tier"], 9),
        x["kickoff"],
    ))
    return results


def run_analysis(target_date: date = None,
                 use_24h_window: bool = False) -> list[dict]:
    """
    Run the O2.5 analysis using a single season of history.

    Args:
        target_date:     Analyse fixtures on this specific calendar date.
        use_24h_window:  If True (used by the 8am scheduled job), analyse all
                         fixtures in the next 24 hours.

    Returns a list of qualifying fixture dicts, sorted:
      S1 first, S2 second; Selective before Balanced/Volume; then by kick-off.
    """
    print(f"\nO2.5 Analysis — season {SEASON}")

    print("  -> Fetching fixtures...", end=" ", flush=True)
    try:
        fixtures_df = download_fixtures()
        print(f"{len(fixtures_df)} rows")
    except Exception as e:
        print(f"FAILED ({e})")
        return []

    day_fixtures, window_label = _select_day_fixtures(fixtures_df, target_date, use_24h_window)
    if day_fixtures.empty:
        print(f"  -> No fixtures for {window_label}")
        return []
    print(f"  -> {len(day_fixtures)} fixtures · {window_label}")

    leagues_needed = _leagues_needed(day_fixtures)
    history = _load_history(leagues_needed, download_history)

    return _analyze_fixtures(day_fixtures, history)


def o25_lb(target_date: date = None,
          use_24h_window: bool = False,
          n_seasons: int = 2) -> list[dict]:
    """
    Run the O2.5 analysis using history that spans multiple seasons
    (this season plus the previous n_seasons - 1), instead of the
    current-season-only history run_analysis() uses.

    Same signals, same SIGNAL_PARAMS/S3_PARAMS, same fixture selection —
    the only difference is the lookback window can reach back across a
    season boundary, so signals can still fire early in a new season when
    a team hasn't yet played enough current-season games.

    Args:
        target_date:     Analyse fixtures on this specific calendar date.
        use_24h_window:  If True, analyse all fixtures in the next 24 hours.
        n_seasons:       How many seasons of history to pull, most recent
                         first (default 2 = this season + last season).

    Returns a list of qualifying fixture dicts, sorted the same way as
    run_analysis().
    """
    print(f"\nO2.5 Analysis (long-back, {n_seasons} seasons) — season {SEASON}")

    print("  -> Fetching fixtures...", end=" ", flush=True)
    try:
        fixtures_df = download_fixtures()
        print(f"{len(fixtures_df)} rows")
    except Exception as e:
        print(f"FAILED ({e})")
        return []

    day_fixtures, window_label = _select_day_fixtures(fixtures_df, target_date, use_24h_window)
    if day_fixtures.empty:
        print(f"  -> No fixtures for {window_label}")
        return []
    print(f"  -> {len(day_fixtures)} fixtures · {window_label}")

    leagues_needed = _leagues_needed(day_fixtures)
    history = _load_history(
        leagues_needed,
        lambda code: download_history_span(code, n_seasons),
    )

    return _analyze_fixtures(day_fixtures, history)


# ─────────────────────────────────────────────────────────────────────────────
# Signal 3 — BTTS corroboration
# ─────────────────────────────────────────────────────────────────────────────

def check_signal3(df: pd.DataFrame, home: str, away: str,
                  fixture_date: date, code: str,
                  home_odds: float | None,
                  o25_odds: float | None) -> dict | None:
    """
    Evaluate Signal 3 (BTTS form) for a fixture.

    S3 is a first-class signal — it fires in its own right when BTTS form
    criteria are met, and also corroborates S1/S2 picks on the same fixture.

    Three gates must all pass:
      1. confidence = home_btts_pct x away_btts_pct x 100  >=  conf_min
      2. home_odds >= home_odds_min  (filters heavy favourites)
      3. o25_odds >= o25_floor       (aligned to S2 floor for same league)

    Full lookback required — if either team hasn't played enough home/away
    games this season the signal returns None. No partial history fallback.

    Returns a result dict if the fixture qualifies, None otherwise.
    """
    p = S3_PARAMS.get(code)
    if p is None:
        return None  # League not in S3 parameters — skip

    lb = p["lookback"]

    # ── Home BTTS rate in last N home games ───────────────────────────────────
    h_games = _last_n_home(df, home, fixture_date, lb)
    if len(h_games) < lb:
        return None
    h_btts_pct = ((h_games["FTHG"] > 0) & (h_games["FTAG"] > 0)).mean()

    # ── Away BTTS rate in last N away games ───────────────────────────────────
    a_games = _last_n_away(df, away, fixture_date, lb)
    if len(a_games) < lb:
        return None
    a_btts_pct = ((a_games["FTHG"] > 0) & (a_games["FTAG"] > 0)).mean()

    # ── Gate 1: confidence threshold ──────────────────────────────────────────
    confidence = h_btts_pct * a_btts_pct * 100
    if confidence < p["conf_min"]:
        return None

    # ── Gate 2: home odds filter ───────────────────────────────────────────────
    if p["home_odds_min"] is not None:
        if home_odds is None or home_odds < p["home_odds_min"]:
            return None

    # ── Gate 3: O2.5 odds floor (aligned to S2 floor for this league) ─────────
    o25_floor = p["o25_floor"]
    if o25_floor is not None:
        if o25_odds is not None and o25_odds < o25_floor:
            return None

    return {
        "signal":     "S3",
        "tier":       "Selective",
        "h_btts_pct": round(h_btts_pct * 100, 1),
        "a_btts_pct": round(a_btts_pct * 100, 1),
        "confidence": round(confidence, 1),
        "o25_floor":  o25_floor,
    }


# ─────────────────────────────────────────────────────────────────────────────
# Telegram formatter
# ─────────────────────────────────────────────────────────────────────────────

def format_telegram(results: list[dict],
                    target_date: date = None,
                    use_24h_window: bool = False) -> str:
    """
    Format O2.5 results as a Telegram message.

    S3 can appear in two ways:
      - As a standalone pick (signal == 'S3') when only BTTS form triggered.
      - As a corroboration tag (s3 == True) on an S1 or S2 pick where S3
        also triggered on the same fixture.
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
        (S3) appended to an S1/S2 tag means BTTS form also qualifies on this
        fixture — both signals point at the same game, adding conviction.
        A standalone (S3) tag means only BTTS form triggered.
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
                metrics = f"Home concedes: `{r['home_conc']}`/g · Flags: `{r['flags']}/{r.get('min_flags', 3)}`"
            elif r["signal"] == "S2":
                metrics = (
                    f"Away scored: `{r['away_scored']}`/g · "
                    f"Away O2.5: `{r['away_overs']}%` · "
                    f"Home concedes: `{r['home_conc']}`/g"
                )
            else:  # S3 standalone
                metrics = (
                    f"Home BTTS: `{r['h_btts_pct']}%` · "
                    f"Away BTTS: `{r['a_btts_pct']}%` · "
                    f"Confidence: `{r['confidence']}%`"
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
    _render_group(s1_sel, "🏠 *LEAKY HOME — Selective*",        "High ROI tier")
    _render_group(s1_oth, "🏠 *LEAKY HOME — Balanced/Volume*",  "Good ROI + volume")

    # S2 — Strong Away
    s2_sel = [r for r in results if r["signal"] == "S2" and r["tier"] == "Selective"]
    s2_oth = [r for r in results if r["signal"] == "S2" and r["tier"] != "Selective"]
    _render_group(s2_sel, "✈️ *STRONG AWAY — Selective*",        "Best overall ROI signal")
    _render_group(s2_oth, "✈️ *STRONG AWAY — Balanced/Volume*",  "Good ROI + volume")

    # S3 — BTTS Form (standalone picks only)
    s3_sel = [r for r in results if r["signal"] == "S3" and r["tier"] == "Selective"]
    s3_oth = [r for r in results if r["signal"] == "S3" and r["tier"] != "Selective"]
    _render_group(s3_sel, "⚽ *BTTS FORM — Selective*",          "Strong BTTS form in both venues")
    _render_group(s3_oth, "⚽ *BTTS FORM — Balanced/Volume*",    "Good BTTS form")

    # Footer
    total   = len(results)
    sel     = sum(1 for r in results if r["tier"] == "Selective")
    s3_corr = sum(1 for r in results if r.get("s3"))
    scan_t  = datetime.now().strftime("%H:%M")

    corr_note = f" · {s3_corr} with BTTS corroboration" if s3_corr else ""
    blocks.append(
        f"\n🕙 {scan_t} · {total} picks · {sel} selective{corr_note}\n"
        "_Tags: (S1) Leaky Home · (S2) Strong Away · (S3) BTTS Form_\n"
        "_(S3) on an S1/S2 pick = BTTS form also qualifies — added conviction_\n"
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
