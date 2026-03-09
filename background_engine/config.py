"""
Inference Engine Configuration
Set your API keys here or via environment variables.
"""
import os

# ─── Massive (formerly Polygon.io) ──────────────────────────────
MASSIVE_API_KEY = os.getenv("MASSIVE_API_KEY", "YOUR_MASSIVE_API_KEY")
MASSIVE_REST_BASE = "https://api.massive.com"
MASSIVE_WS_BASE = "wss://socket.massive.com"

# ─── Tradier ────────────────────────────────────────────────────
TRADIER_API_KEY = os.getenv("TRADIER_API_KEY", "YOUR_TRADIER_API_KEY")
TRADIER_REST_BASE = "https://api.tradier.com/v1"
TRADIER_WS_BASE = "wss://ws.tradier.com/v1"

# ─── TopStepX ───────────────────────────────────────────────────
TOPSTEPX_API_KEY = os.getenv("TOPSTEPX_API_KEY", "YOUR_TOPSTEPX_API_KEY")
TOPSTEPX_WS_BASE = os.getenv("TOPSTEPX_WS_BASE", "wss://api.topstepx.com")

# ─── Data Settings ──────────────────────────────────────────────
# Tickers to track
OPTIONS_TICKERS = ["QQQ", "SPY"]
EQUITY_TICKERS = ["QQQ", "SPY", "VIX"]
FUTURES_SYMBOLS = ["NQ", "ES", "YM", "RTY"]

# GEX refresh interval (seconds)
GEX_REFRESH_INTERVAL = 30

# Risk-free rate (updated from Massive Economy API at startup)
RISK_FREE_RATE = 0.043  # Default fallback ~4.3% SOFR

# Historical data logging
DB_PATH = os.getenv("INFERENCE_DB_PATH", "inference_data.db")

# ─── Alpha Framework Parameters ────────────────────────────────
# Transfer Entropy
TE_WINDOW_SIZE = 60       # seconds
TE_LAG = 1                # lag in samples
TE_BINS = 5               # discretization bins

# Shannon Entropy
ENTROPY_WINDOW = 120      # seconds
ENTROPY_STATES = 5        # number of order flow states

# Ising Magnetization
ISING_WINDOW = 60         # seconds
ISING_HERD_THRESHOLD = 0.7  # magnetization threshold for herding alert

# Reynolds Number
REYNOLDS_WINDOW = 300     # seconds
REYNOLDS_TURBULENT = 2000 # threshold for turbulent flow

# Percolation
PERCOLATION_WINDOW = 300  # seconds  
PERCOLATION_THRESHOLD = 0.6  # fraction of broken correlations for alert
