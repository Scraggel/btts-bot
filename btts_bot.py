"""
Sports Signals Telegram Bot
----------------------------
Combines two analysis engines in a single bot process:

  BTTS (S3) — manual only
    /btts              — today's BTTS picks
    /btts tomorrow     — tomorrow's BTTS picks
    /btts saturday     — this Saturday's picks
    /btts YYYY-MM-DD   — specific date

  O2.5 (S1 / S2 / S3) — scheduled + manual
    Scheduled:
      Tuesday  14:00 London — full upcoming fixtures scan (next 7 days)
      Friday   18:00 London — full upcoming fixtures scan (next 7 days)
    Manual:
      /o25               — next 24 hours
      /o25 YYYY-MM-DD    — specific date override

  Signal tags in O2.5 output:
    (S1)       Leaky Home signal triggered
    (S2)       Strong Away signal triggered
    (S1)(S3)   Leaky Home AND the same fixture also qualifies in /btts
    (S2)(S3)   Strong Away AND the same fixture also qualifies in /btts

"""
from dotenv import load_dotenv
load_dotenv()

import asyncio
import logging
import os
import sys
from datetime import date, datetime, timedelta

import pytz
from telegram import Update
from telegram.ext import Application, CommandHandler, ContextTypes

# ── btts_analysis is imported UNCHANGED ───────────────────────────────────────
from btts_analysis import (
    run_analysis            as btts_run_analysis,
    format_telegram         as btts_format_telegram,
    split_telegram_messages as btts_split_messages,
    get_next_saturday,
    DAY_NAMES,
)

# ── O2.5 engine ───────────────────────────────────────────────────────────────
from o25_analysis import (
    run_analysis            as o25_run_analysis,
    format_telegram         as o25_format_telegram,
    split_telegram_messages as o25_split_messages,
    parse_target_date       as o25_parse_date,
    download_fixtures,
    get_fixtures_in_window,
    LEAGUE_META,
)


# ─────────────────────────────────────────────────────────────────────────────
# Config
# ─────────────────────────────────────────────────────────────────────────────

BOT_TOKEN = os.getenv("BTTS_BOT_TOKEN", "")
CHAT_ID   = os.getenv("BTTS_CHAT_ID",   "")

LONDON_TZ = pytz.timezone("Europe/London")

# Scheduled scan times (London local time)
SCHEDULED_JOBS = [
    {"day": 1, "hour": 14, "minute": 0, "label": "Tuesday 14:00"},   # Tuesday = weekday 1
    {"day": 4, "hour": 18, "minute": 0, "label": "Friday 18:00"},    # Friday  = weekday 4
]

# How many days ahead the scheduled scan covers
SCHEDULED_SCAN_DAYS = 7

MAX_RETRIES      = 3
RETRY_DELAY_MINS = 15

logging.basicConfig(
    format="%(asctime)s — %(name)s — %(levelname)s — %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────────────────────
# Full-range scan (used by scheduled jobs)
# ─────────────────────────────────────────────────────────────────────────────

def run_full_scan(days_ahead: int = SCHEDULED_SCAN_DAYS) -> list[dict]:
    """
    Download fixtures and run O2.5 analysis across the next `days_ahead` days.
    Uses the existing get_fixtures_in_window helper with a wide window, then
    calls run_analysis once per date found so historical stats are scoped
    correctly per fixture date.
    """
    now = datetime.now()
    end = now + timedelta(days=days_ahead)

    fixtures_df = download_fixtures()
    window_fixtures = get_fixtures_in_window(fixtures_df, now, end)

    if window_fixtures.empty:
        return []

    # Collect unique fixture dates in the window
    fixture_dates = sorted(window_fixtures["Date"].dt.date.unique())

    all_results: list[dict] = []
    seen = set()   # deduplicate across date calls

    for fdate in fixture_dates:
        daily = o25_run_analysis(target_date=fdate)
        for r in daily:
            key = (r["home"], r["away"], r["date"])
            if key not in seen:
                seen.add(key)
                all_results.append(r)

    # Re-sort: S1 → S2 → S3, Selective → Balanced → Volume, then kickoff
    tier_order = {"Selective": 0, "Balanced": 1, "Volume": 2}
    sig_order  = {"S1": 0, "S2": 1, "S3": 2}
    all_results.sort(key=lambda x: (
        sig_order.get(x["signal"], 9),
        tier_order.get(x["tier"], 9),
        x.get("kickoff", ""),
    ))

    return all_results


def format_full_scan_telegram(results: list[dict], days_ahead: int = SCHEDULED_SCAN_DAYS) -> str:
    """
    Format a multi-day scheduled scan result.
    Groups picks by date for readability.
    """
    now = datetime.now(LONDON_TZ)
    scan_label = now.strftime("%A %d %b %Y, %H:%M")
    end_label  = (now + timedelta(days=days_ahead)).strftime("%d %b")

    if not results:
        return (
            f"*📈 O2.5 Scheduled Scan*\n"
            f"_{scan_label} → {end_label}_\n\n"
            f"_No qualifying fixtures in the next {days_ahead} days._"
        )

    # Group by date
    by_date: dict[str, list[dict]] = {}
    for r in results:
        by_date.setdefault(r["date"], []).append(r)

    blocks = [
        f"*📈 O2.5 Scheduled Scan — {days_ahead}-day window*",
        f"_{scan_label} → {end_label}_",
    ]

    for day_str, picks in by_date.items():
        day_name = picks[0]["day_name"]
        blocks.append(f"\n{'═' * 26}\n*{day_name} {day_str}*")

        for r in picks:
            tags = f"({r['signal']})"
            if r.get("s3"):
                tags += "(S3)"

            if r["signal"] == "S1":
                metrics = (
                    f"Home concedes: `{r['home_conc']}`/g · "
                    f"Flags: `{r['flags']}/5`"
                )
            elif r["signal"] == "S2":
                metrics = (
                    f"Away scored: `{r['away_scored']}`/g · "
                    f"Away O2.5: `{r['away_overs']}%` · "
                    f"Home concedes: `{r['home_conc']}`/g"
                )
            else:  # S3
                metrics = (
                    f"Home BTTS: `{r['h_btts_pct']}%` · "
                    f"Away BTTS: `{r['a_btts_pct']}%` · "
                    f"Confidence: `{r['confidence']}%`"
                )

            blocks.append(
                f"\n*{r['home']} vs {r['away']}* {tags}\n"
                f"🕐 `{r['kickoff']}` · {r['league']}\n"
                f"{metrics}\n"
                f"_{r['odds_note']}_"
            )

    total = len(results)
    blocks.append(f"\n{'─' * 24}\n_Total: {total} pick{'s' if total != 1 else ''}_")
    return "\n".join(blocks)


# ─────────────────────────────────────────────────────────────────────────────
# Shared helpers
# ─────────────────────────────────────────────────────────────────────────────

def _parse_btts_date(args: list[str]) -> date | None:
    if not args:
        return date.today()
    arg = args[0].lower().strip()
    if arg == "today":
        return date.today()
    elif arg == "tomorrow":
        return date.today() + timedelta(days=1)
    elif arg == "saturday":
        return get_next_saturday()
    else:
        try:
            return datetime.strptime(arg, "%Y-%m-%d").date()
        except ValueError:
            return None


async def _send_long(bot, chat_id: str, text: str, split_fn, parse_mode="Markdown"):
    """Send a (potentially long) message, splitting at 4096 chars."""
    chunks = split_fn(text)
    for chunk in chunks:
        await bot.send_message(chat_id=chat_id, text=chunk, parse_mode=parse_mode)


def _generic_split(text: str, limit: int = 4000) -> list[str]:
    """Simple splitter for messages that don't have their own split function."""
    if len(text) <= limit:
        return [text]
    chunks, current = [], ""
    for line in text.split("\n"):
        if len(current) + len(line) + 1 > limit:
            chunks.append(current)
            current = line
        else:
            current = (current + "\n" + line) if current else line
    if current:
        chunks.append(current)
    return chunks


# ─────────────────────────────────────────────────────────────────────────────
# BTTS command handlers
# ─────────────────────────────────────────────────────────────────────────────

async def cmd_btts(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    target = _parse_btts_date(context.args)
    if target is None:
        await update.message.reply_text(
            "❌ Invalid date. Use:\n"
            "  /btts — today\n"
            "  /btts tomorrow\n"
            "  /btts saturday\n"
            "  /btts YYYY-MM-DD"
        )
        return

    day_name = DAY_NAMES[target.weekday()]
    await update.message.reply_text(
        f"⏳ Fetching BTTS data for {day_name} {target.strftime('%d %b %Y')}..."
    )
    try:
        results = btts_run_analysis(target_date=target)
        message = btts_format_telegram(results, target_date=target)
        await _send_long(context.bot, update.message.chat_id,
                         message, btts_split_messages)
    except Exception as e:
        logger.error(f"BTTS analysis failed: {e}")
        await update.message.reply_text(f"❌ BTTS analysis failed: {e}")


# ─────────────────────────────────────────────────────────────────────────────
# O2.5 command handlers
# ─────────────────────────────────────────────────────────────────────────────

async def cmd_o25(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """
    /o25               -> next 24 hours
    /o25 YYYY-MM-DD    -> specific date override
    """
    args = context.args or []
    first_arg = args[0].lower().strip() if args else ""

    if first_arg and first_arg not in ("rescan",):
        # Specific date override
        target = o25_parse_date(first_arg)
        day_name = DAY_NAMES[target.weekday()]
        await update.message.reply_text(
            f"⏳ Fetching O2.5 data for {day_name} {target.strftime('%d %b %Y')}..."
        )
        try:
            results = o25_run_analysis(target_date=target)
            message = o25_format_telegram(results, target_date=target)
            await _send_long(context.bot, update.message.chat_id,
                             message, o25_split_messages)
        except Exception as e:
            logger.error(f"O2.5 analysis failed: {e}")
            await update.message.reply_text(f"❌ O2.5 analysis failed: {e}")
        return

    # Default: 24-hour rolling window
    await update.message.reply_text("⏳ Running O2.5 scan — next 24 hours...")
    try:
        results = o25_run_analysis(use_24h_window=True)
        message = o25_format_telegram(results, use_24h_window=True)
        await _send_long(context.bot, update.message.chat_id,
                         message, o25_split_messages)
    except Exception as e:
        logger.error(f"O2.5 scan failed: {e}")
        await update.message.reply_text(f"❌ O2.5 scan failed: {e}")


# ─────────────────────────────────────────────────────────────────────────────
# Scheduled full scan (Tuesday 14:00 + Friday 18:00 London time)
# ─────────────────────────────────────────────────────────────────────────────

async def scheduled_full_scan(context: ContextTypes.DEFAULT_TYPE) -> None:
    """
    Runs on Tuesday 14:00 and Friday 18:00 London time.
    Scans all fixtures across the next 7 days and delivers a full report.
    Retries up to MAX_RETRIES times if the data source is unavailable.
    """
    now_london = datetime.now(LONDON_TZ)
    label = now_london.strftime("%A %d %b, %H:%M")
    logger.info(f"Scheduled full O2.5 scan triggered — {label}")

    for attempt in range(1, MAX_RETRIES + 1):
        logger.info(f"Full scan attempt {attempt}/{MAX_RETRIES}")
        try:
            results = run_full_scan(days_ahead=SCHEDULED_SCAN_DAYS)
            message = format_full_scan_telegram(results, days_ahead=SCHEDULED_SCAN_DAYS)

            if not results and attempt < MAX_RETRIES:
                logger.warning(f"No results yet — retrying in {RETRY_DELAY_MINS}m")
                await asyncio.sleep(RETRY_DELAY_MINS * 60)
                continue

            await _send_long(context.bot, CHAT_ID, message, _generic_split)
            logger.info(f"Scheduled full scan sent — {len(results)} picks.")
            return

        except Exception as e:
            logger.error(f"Full scan attempt {attempt} failed: {e}")
            if attempt < MAX_RETRIES:
                await asyncio.sleep(RETRY_DELAY_MINS * 60)

    # All retries exhausted
    await context.bot.send_message(
        chat_id=CHAT_ID,
        text=(
            f"❌ Scheduled O2.5 scan failed after {MAX_RETRIES} attempts. "
            "Use /o25 to try a 24h manual scan."
        ),
    )


# ─────────────────────────────────────────────────────────────────────────────
# /start and /help
# ─────────────────────────────────────────────────────────────────────────────

async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    welcome_text = (
        "*⚽ Welcome to Sports Signals Bot*\n\n"
        "*O2.5 Signals*\n"
        "`/o25` — next 24 hours of O2.5 picks\n"
        "`/o25 YYYY-MM-DD` — specific date override\n"
        "_Scheduled full scan: Tuesday 14:00 & Friday 18:00 (London time)_\n\n"
        "*BTTS Signal* _(manual only)_\n"
        "`/btts` — today's BTTS picks\n"
        "`/btts tomorrow` — tomorrow's picks\n"
        "`/btts saturday` — this Saturday's picks\n"
        "`/btts YYYY-MM-DD` — specific date\n\n"
        "_Signal tags: (S1) Leaky Home · (S2) Strong Away · (S3) BTTS form corroborates O2.5_\n"
        "_Use /help to see all commands_"
    )
    await update.message.reply_text(welcome_text, parse_mode="Markdown")


async def cmd_help(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    help_text = (
        "*⚽ Sports Signals Bot — Commands*\n\n"
        "*📈 O2.5 Signals*\n"
        "`/o25` — next 24 hours of O2.5 picks (S1 + S2 + S3)\n"
        "`/o25 YYYY-MM-DD` — specific date override\n\n"
        "*📅 Scheduled Scans (automatic)*\n"
        "  • Tuesday 14:00 — full 7-day fixture scan\n"
        "  • Friday 18:00  — full 7-day fixture scan\n"
        "_Both use London time and cover all configured leagues_\n\n"
        "*⚽ BTTS Signal*\n"
        "`/btts` — today's BTTS picks\n"
        "`/btts tomorrow` — tomorrow's picks\n"
        "`/btts saturday` — this Saturday\n"
        "`/btts YYYY-MM-DD` — specific date\n"
        "_Manual only — call on demand_\n\n"
        "*Signal tags in O2.5 output:*\n"
        "`(S1)` Leaky Home triggered\n"
        "`(S2)` Strong Away triggered\n"
        "`(S3)` BTTS form corroborates — both teams likely to score\n"
    )
    await update.message.reply_text(help_text, parse_mode="Markdown")


# ─────────────────────────────────────────────────────────────────────────────
# Bot setup and entry point
# ─────────────────────────────────────────────────────────────────────────────

def main() -> None:
    if BOT_TOKEN:
        logger.info("BTTS_BOT_TOKEN is set")
    else:
        logger.error("BTTS_BOT_TOKEN environment variable is not set.")

    if CHAT_ID:
        logger.info("BTTS_CHAT_ID is set")
    else:
        logger.error("BTTS_CHAT_ID environment variable is not set.")

    if not BOT_TOKEN or not CHAT_ID:
        sys.exit(1)

    app = Application.builder().token(BOT_TOKEN).build()

    # Register command handlers
    app.add_handler(CommandHandler("start", cmd_start))
    app.add_handler(CommandHandler("help",  cmd_help))
    app.add_handler(CommandHandler("btts",  cmd_btts))
    app.add_handler(CommandHandler("o25",   cmd_o25))

    # ── Schedule Tuesday 14:00 and Friday 18:00 (London time) ─────────────────
    job_queue = app.job_queue

    for job in SCHEDULED_JOBS:
        # Build a timezone-aware time object
        run_time = datetime.now(LONDON_TZ).replace(
            hour=job["hour"],
            minute=job["minute"],
            second=0,
            microsecond=0,
        ).timetz()   # returns time with tzinfo attached

        job_queue.run_daily(
            scheduled_full_scan,
            time=run_time,
            days=(job["day"],),          # tuple — only this weekday
            name=f"scan_{job['label'].replace(' ', '_')}",
        )
        logger.info(f"Scheduled: {job['label']} London time")

    logger.info("Sports Signals Bot started.")
    logger.info("BTTS: manual only (/btts)")
    logger.info("/o25: manual 24-hour window")
    app.run_polling(allowed_updates=Update.ALL_TYPES)


if __name__ == "__main__":
    main()