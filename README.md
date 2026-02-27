BTTS Bot

A Telegram bot that analyzes football fixtures for "Both Teams To Score" predictions using data from football-data.co.uk.

## Setup

1. Create a bot via @BotFather on Telegram → get your BOT_TOKEN
2. Get your personal chat ID via @userinfobot on Telegram
3. Copy `.env.example` to `.env` and fill in your values
4. Deploy to a cloud service (see deployment options below)

## Commands

- `/btts` — Analyze today's fixtures
- `/btts tomorrow` — Analyze tomorrow's fixtures
- `/btts saturday` — Analyze the coming Saturday
- `/btts YYYY-MM-DD` — Analyze a specific date
- `/help` — Show available commands

The bot automatically sends analysis every Saturday at 7am.

## Local Development

```bash
pip install -r requirements.txt
cp .env.example .env  # Fill in your values
python btts_bot.py
```

## Deployment Options

### Railway (Recommended - Easiest)

1. Connect your GitHub repo to [Railway](https://railway.app)
2. Add environment variables in Railway dashboard:
   - `BTTS_BOT_TOKEN`
   - `BTTS_CHAT_ID`
3. Deploy automatically

### Render

1. Connect your GitHub repo to [Render](https://render.com)
2. Create a new Web Service
3. Set build command: `pip install -r requirements.txt`
4. Set start command: `python btts_bot.py`
5. Add environment variables

### Heroku

1. Create a new app on [Heroku](https://heroku.com)
2. Connect your GitHub repo
3. Add buildpacks: `heroku/python`
4. Set environment variables in Heroku dashboard
5. Deploy

### DigitalOcean App Platform

1. Create a new app on DigitalOcean
2. Connect your GitHub repo
3. Set runtime to Python
4. Add environment variables
5. Deploy

## Environment Variables

- `BTTS_BOT_TOKEN` — Your Telegram bot token
- `BTTS_CHAT_ID` — Your Telegram chat ID
- `BTTS_SEASON` — Optional: Force specific season (e.g., "2425")
