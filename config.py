"""
Configuration for the Discord Greek Bot.
Loads secrets from .env and defines constants.
"""

import os
from dotenv import load_dotenv

load_dotenv()

# ── Secrets ──────────────────────────────────────────────────────────────────
DISCORD_BOT_TOKEN = os.getenv("DISCORD_BOT_TOKEN", "")
POLYGON_API_KEY   = os.getenv("POLYGON_API_KEY", "")
TRADIER_TOKEN     = os.getenv("TRADIER_TOKEN", "")

# ── Ticker & Timing ─────────────────────────────────────────────────────────
TICKER = os.getenv("TICKER", "QQQ")
UPDATE_INTERVAL = int(os.getenv("UPDATE_INTERVAL", "10"))  # seconds

# ── Discord Channel Config ──────────────────────────────────────────────────
CATEGORY_NAME = "Greeks"

CHANNELS = {
    "oi":       {"name": "oi",       "emoji": "📈", "title": "Open Interest",    "color": 0x3498DB},
    "dex":      {"name": "dex",      "emoji": "📐", "title": "Delta Exposure",   "color": 0xE67E22},
    "gex":      {"name": "gex",      "emoji": "⚡", "title": "Gamma Exposure",   "color": 0x9B59B6},
    "vex":      {"name": "vex",      "emoji": "🌊", "title": "Vega Exposure",    "color": 0x1ABC9C},
    "tex":      {"name": "tex",      "emoji": "⏳", "title": "Theta Exposure",   "color": 0xF39C12},
    "max-pain": {"name": "max-pain", "emoji": "🎯", "title": "Max Pain",         "color": 0xE74C3C},
}

# ── Chart Styling ────────────────────────────────────────────────────────────
CHART_BG_COLOR = "#0d1117"
CHART_TEXT_COLOR = "#c9d1d9"
CHART_GRID_COLOR = "#21262d"
CHART_ACCENT_COLORS = {
    "positive": "#3fb950",
    "negative": "#f85149",
    "neutral":  "#58a6ff",
    "highlight": "#ffd700",
}

# ── Data Config ──────────────────────────────────────────────────────────────
STRIKE_RANGE = 30          # ± from spot price
MAX_EXPIRATIONS = 3        # nearest N expirations to include
