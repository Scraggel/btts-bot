"""
Sports Signals Telegram Bot
----------------------------
Combines two analysis engines in a single bot process:

  BTTS (S3) — manual only
    /btts              — today's BTTS picks
    /btts tomorrow     — tomorrow's BTTS picks
    /btts saturday     — this Saturday's picks
    /btts YYYY-MM-DD   — specific date

  O2.5 (S1 / S2) — scheduled + manual
    Scheduled: every day at 08:00 London time, covers next 24 hours
               (catches early Brazilian kick-offs past midnight)
    /o25               — next 24 hours of O2.5 picks
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
from datetime import date, datetime, timedelta

from telegram import Update
from telegram.ext import Application, CommandHandler, ContextTypes

# ── btts_analysis is imported UNCHANGED — do not modify that file ─────────────
from btts_analysis import (
    run_analysis        as btts_run_analysis,
    format_telegram     as btts_format_telegram,
    split_telegram_messages as btts_split_messages,
    get_next_saturday,
    DAY_NAMES,
)

# ── O2.5 engine ───────────────────────────────────────────────────────────────
from o25_analysis import (
    run_analysis        as o25_run_analysis,
    format_telegram     as o25_format_telegram,
    split_telegram_messages as o25_split_messages,
    parse_target_date   as o25_parse_date,
)


# ─────────────────────────────────────────────────────────────────────────────
# Config
# ─────────────────────────────────────────────────────────────────────────────

BOT_TOKEN = os.getenv("BTTS_BOT_TOKEN", "")
CHAT_ID   = os.getenv("BTTS_CHAT_ID",   "")

# O2.5 scheduled time (08:00 London — covers next 24h including early Brazil KOs)
O25_SCHEDULED_HOUR   = 8
O25_SCHEDULED_MINUTE = 0

MAX_RETRIES      = 3
RETRY_DELAY_MINS = 15

logging.basicConfig(
    format="%(asctime)s — %(name)s — %(levelname)s — %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────────────────────
# Shared helpers
# ─────────────────────────────────────────────────────────────────────────────

def _parse_btts_date(args: list[str]) -> date | None:
    """
    Parse date args for /btts commands.
    Returns None for unrecognised input (caller sends error message).
    Kept identical to original btts_bot.py logic.
    """
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


async def _send_long(bot, chat_id, text: str, split_fn, parse_mode="Markdown"):
    """Send a (potentially long) message, splitting at 4096 chars."""
    chunks = split_fn(text)
    for chunk in chunks:
        await bot.send_message(chat_id=chat_id, text=chunk, parse_mode=parse_mode)


# ─────────────────────────────────────────────────────────────────────────────
# BTTS command handlers  (logic identical to original btts_bot.py)
# ─────────────────────────────────────────────────────────────────────────────

async def cmd_btts(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """
    /btts              -> analyse today
    /btts tomorrow     -> analyse tomorrow
    /btts saturday     -> analyse coming Saturday
    /btts 2026-03-15   -> analyse a specific date
    """
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
    /o25               -> next 24 hours (default + scheduled behaviour)
    /o25 rescan        -> same as above, explicit fresh scan
    /o25 YYYY-MM-DD    -> specific date override (edge case use)
    """
    args = context.args or []
    first_arg = args[0].lower().strip() if args else ""

    # Specific date override — only when a YYYY-MM-DD string is passed
    if first_arg and first_arg not in ("rescan",):
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

    # Default: 24-hour rolling window (covers today + early next-day KOs)
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
# Scheduled O2.5 job (08:00 daily — 24-hour window)
# ─────────────────────────────────────────────────────────────────────────────

async def scheduled_o25(context: ContextTypes.DEFAULT_TYPE) -> None:
    """
    Fires every morning at 08:00 London time.
    Scans fixtures in the next 24 hours (captures early Brazilian KOs).
    Retries up to MAX_RETRIES times if data source is unavailable.
    """
    for attempt in range(1, MAX_RETRIES + 1):
        logger.info(f"Scheduled O2.5 scan — attempt {attempt}/{MAX_RETRIES}")
        try:
            results = o25_run_analysis(use_24h_window=True)
            message = o25_format_telegram(results, use_24h_window=True)

            if not results and attempt < MAX_RETRIES:
                logger.warning(f"No O2.5 results — retrying in {RETRY_DELAY_MINS}m")
                await asyncio.sleep(RETRY_DELAY_MINS * 60)
                continue

            await _send_long(context.bot, CHAT_ID, message, o25_split_messages)
            logger.info("Scheduled O2.5 message sent.")
            return

        except Exception as e:
            logger.error(f"Scheduled O2.5 attempt {attempt} failed: {e}")
            if attempt < MAX_RETRIES:
                await asyncio.sleep(RETRY_DELAY_MINS * 60)

    # All retries exhausted
    await context.bot.send_message(
        chat_id=CHAT_ID,
        text=(
            f"❌ Scheduled O2.5 scan failed after {MAX_RETRIES} attempts. "
            "Use /o25 rescan to try manually."
        ),
    )


# ─────────────────────────────────────────────────────────────────────────────
# /start and /help
# ─────────────────────────────────────────────────────────────────────────────

async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    welcome_text = (
        "*⚽ Welcome to Sports Signals Bot*\n\n"
        "*O2.5 Signals* _(auto-delivered 08:00 daily)_\n"
        "`/o25` — next 24 hours of O2.5 picks\n"
        "`/o25 YYYY-MM-DD` — specific date override\n\n"
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
        "`/o25` — next 24 hours of O2.5 picks (S1 + S2)\n"
        "`/o25 YYYY-MM-DD` — specific date override\n"
        "_Auto-delivered: 08:00 daily · always covers next 24 hours_\n\n"
        "*⚽ BTTS Signal*\n"
        "`/btts` — today's BTTS picks\n"
        "`/btts tomorrow` — tomorrow's picks\n"
        "`/btts saturday` — this Saturday\n"
        "`/btts YYYY-MM-DD` — specific date\n"
        "_Manual only — call on demand_\n\n"
        "*Signal tags in O2.5 output:*\n"
        "`(S1)` Leaky Home triggered\n"
        "`(S2)` Strong Away triggered\n"
        "`(S3)` BTTS form corroborates — both teams likely to score adds confidence to the O2.5 pick\n"
    )
    await update.message.reply_text(help_text, parse_mode="Markdown")


# ─────────────────────────────────────────────────────────────────────────────
# Bot setup and entry point
# ─────────────────────────────────────────────────────────────────────────────

import sys

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
    app.add_handler(CommandHandler("btts",  cmd_btts))   # manual only, unchanged
    app.add_handler(CommandHandler("o25",   cmd_o25))    # manual + rescan

    # Schedule daily O2.5 scan at 08:00 London time
    job_queue = app.job_queue
    job_queue.run_daily(
        scheduled_o25,
        time=datetime.strptime(
            f"{O25_SCHEDULED_HOUR:02d}:{O25_SCHEDULED_MINUTE:02d}", "%H:%M"
        ).time(),
        name="daily_o25",
    )

    logger.info("Sports Signals Bot started.")
    logger.info(f"O2.5 scheduled: daily at {O25_SCHEDULED_HOUR:02d}:{O25_SCHEDULED_MINUTE:02d}")
    logger.info("BTTS: manual only (/btts)")
    app.run_polling(allowed_updates=Update.ALL_TYPES)


if __name__ == "__main__":
    main()
