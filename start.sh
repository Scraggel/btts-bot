#!/bin/bash
# Deployment script for Railway/Render
# This script runs before the app starts

echo "Starting BTTS Bot deployment..."

# Check if required environment variables are set
if [ -z "$BTTS_BOT_TOKEN" ]; then
    echo "ERROR: BTTS_BOT_TOKEN environment variable is not set"
    exit 1
fi

if [ -z "$BTTS_CHAT_ID" ]; then
    echo "ERROR: BTTS_CHAT_ID environment variable is not set"
    exit 1
fi

echo "Environment variables check passed"
echo "Starting bot..."

# Start the bot
python btts_bot.py