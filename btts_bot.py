"""
BTTS Telegram Bot
-----------------
Analyses BTTS fixtures for any day. Defaults to today when run
on demand, and includes a scheduled Saturday 7am auto-delivery.

Setup:
  1. Create a bot via @BotFather on Telegram → get your BOT_TOKEN
  2. Get your personal chat ID via @userinfobot on Telegram
  3. Set environment variables (see .env.example)
  4. Deploy to a VPS or cloud service (Railway, Render, Hetzner etc.)

Commands:
  /btts              — Run analysis for today's fixtures
  /btts tomorrow     — Run analysis for tomorrow's fixtures
  /btts saturday     — Run analysis for the coming Saturday
  /btts YYYY-MM-DD   — Run analysis for a specific date
"""
from dotenv import load_dotenv
load_dotenv()
import asyncio
import logging
import os
from datetime import date, datetime, timedelta

from telegram import Update
from telegram.ext import Application, CommandHandler, ContextTypes

from btts_analysis import (
    run_analysis,
    format_telegram,
    split_telegram_messages,
    get_next_saturday,
    DAY_NAMES,
)

# ── Config ────────────────────────────────────────────────────────────────────
# All secrets via environment variables — never hardcode credentials.
BOT_TOKEN = os.getenv("BTTS_BOT_TOKEN", "")
CHAT_ID   = os.getenv("BTTS_CHAT_ID",   "")

# Scheduled send time every Saturday (24hr format, Europe/London timezone)
SCHEDULED_HOUR   = 7
SCHEDULED_MINUTE = 0

# Retry settings for scheduled job if data source is unavailable
MAX_RETRIES      = 3
RETRY_DELAY_MINS = 15

logging.basicConfig(
    format="%(asctime)s — %(name)s — %(levelname)s — %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger(__name__)


# ── Helpers ───────────────────────────────────────────────────────────────────

def parse_target_date(args: list[str]) -> date | None:
    """
    Parse the user's date argument into a date object.

    Supports:
      (no args)     → today
      "today"       → today
      "tomorrow"    → tomorrow
      "saturday"    → coming Saturday (or today if Saturday)
      "YYYY-MM-DD"  → specific date
    
    Returns None for unrecognised input (caller should send error).
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
        # Try parsing as a date string
        try:
            return datetime.strptime(arg, "%Y-%m-%d").date()
        except ValueError:
            return None  # Signal invalid input


async def send_long_message(bot, chat_id: int, text: str, parse_mode: str = "Markdown"):
    chunks = split_telegram_messages(text)
    for chunk in chunks:
        await bot.send_message(chat_id=chat_id, text=chunk, parse_mode=parse_mode)


# ── Handlers ──────────────────────────────────────────────────────────────────

async def cmd_btts(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """
    /btts              → analyse today
    /btts tomorrow     → analyse tomorrow
    /btts saturday     → analyse coming Saturday
    /btts 2026-03-15   → analyse a specific date
    """
    target = parse_target_date(context.args)

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
        f"⏳ Fetching data for {day_name} {target.strftime('%d %b %Y')}..."
    )

    try:
        results = run_analysis(target_date=target)
        message = format_telegram(results, target_date=target)
        await send_long_message(context.bot, update.message.chat_id, message)
    except Exception as e:
        logger.error(f"Analysis failed: {e}")
        await update.message.reply_text(f"❌ Analysis failed: {e}")


async def cmd_help(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Show available commands."""
    help_text = (
        "*⚽ BTTS Bot Commands*\n\n"
        "`/btts` — Analyse today's fixtures\n"
        "`/btts tomorrow` — Analyse tomorrow\n"
        "`/btts saturday` — Analyse coming Saturday\n"
        "`/btts YYYY-MM-DD` — Analyse a specific date\n"
        "`/help` — Show this message\n\n"
        "_Scheduled: auto-delivery every Saturday at 7am_"
    )
    await update.message.reply_text(help_text, parse_mode="Markdown")


async def scheduled_btts(context: ContextTypes.DEFAULT_TYPE) -> None:
    """
    Automatically fired every Saturday morning by the job queue.
    Analyses today's (Saturday) fixtures. Retries up to MAX_RETRIES
    times if the data source is unavailable.
    """
    target = date.today()  # Will be Saturday when the scheduler fires

    for attempt in range(1, MAX_RETRIES + 1):
        logger.info(f"Running scheduled BTTS analysis for {target} (attempt {attempt}/{MAX_RETRIES})...")
        try:
            results = run_analysis(target_date=target)
            message = format_telegram(results, target_date=target)

            # Check we actually got results
            if not results and attempt < MAX_RETRIES:
                logger.warning(f"No results returned — retrying in {RETRY_DELAY_MINS} minutes...")
                await asyncio.sleep(RETRY_DELAY_MINS * 60)
                continue

            await send_long_message(context.bot, CHAT_ID, message)
            logger.info("Scheduled analysis sent successfully.")
            return

        except Exception as e:
            logger.error(f"Scheduled analysis attempt {attempt} failed: {e}")
            if attempt < MAX_RETRIES:
                logger.info(f"Retrying in {RETRY_DELAY_MINS} minutes...")
                await asyncio.sleep(RETRY_DELAY_MINS * 60)

    # All retries exhausted
    await context.bot.send_message(
        chat_id=CHAT_ID,
        text=f"❌ Scheduled BTTS analysis failed after {MAX_RETRIES} attempts. "
             f"Try /btts manually later.",
    )


# ── Bot Setup ─────────────────────────────────────────────────────────────────

def main() -> None:
    if not BOT_TOKEN:
        print("ERROR: BTTS_BOT_TOKEN environment variable is not set.")
        return
    if not CHAT_ID:
        print("ERROR: BTTS_CHAT_ID environment variable is not set.")
        return

    app = Application.builder().token(BOT_TOKEN).build()

    # Command handlers
    app.add_handler(CommandHandler("btts", cmd_btts))
    app.add_handler(CommandHandler("help", cmd_help))

    # Schedule: every Saturday at 07:00 Europe/London
    job_queue = app.job_queue
    job_queue.run_daily(
        scheduled_btts,
        time=datetime.strptime(
            f"{SCHEDULED_HOUR:02d}:{SCHEDULED_MINUTE:02d}", "%H:%M"
        ).time(),
        days=(5,),  # 5 = Saturday (Mon=0 … Sun=6)
        name="saturday_btts",
    )

    logger.info("BTTS Bot started. Listening for commands...")
    logger.info(f"Scheduled: Saturday analysis at {SCHEDULED_HOUR:02d}:{SCHEDULED_MINUTE:02d}")
    app.run_polling(allowed_updates=Update.ALL_TYPES)


if __name__ == "__main__":
    main()
