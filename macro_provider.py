"""
macro_provider.py
Fetches LIVE data from Alpha Vantage (news/econ) and FRED (yields/liquidity).
Falls back to simulation when APIs are unreachable.

Live endpoints used:
  Alpha Vantage:
    NEWS_SENTIMENT        → news feed with sentiment scores
    FEDERAL_FUNDS_RATE    → current Fed rate
    CPI                   → Consumer Price Index (YoY)
    REAL_GDP              → GDP growth (QoQ annualised)
    UNEMPLOYMENT          → unemployment rate
    TREASURY_YIELD        → 2Y and 10Y yields (for yield curve)
  FRED (free, no key required):
    DGS2                  → 2-Year Treasury Yield (daily)
    DGS10                 → 10-Year Treasury Yield (daily)
    DFII10                → 10-Year TIPS / Real Yield (daily)
    WALCL                 → Fed Balance Sheet total assets (weekly, $M)
    RRPONTSYD             → Overnight Reverse Repo (daily, $B)
    WTREGEN               → Treasury General Account / TGA (weekly, $M)
"""

import csv, io, json, math, os, random, time, threading
import urllib.request
from datetime import datetime, timedelta

# ── FRED free CSV fetcher (no API key required) ───────────────────────────────
_FRED_BASE = "https://fred.stlouisfed.org/graph/fredgraph.csv"

def _fred_latest(series_id: str) -> float:
    """Fetch most-recent value from FRED public CSV endpoint."""
    url = f"{_FRED_BASE}?id={series_id}"
    req = urllib.request.Request(url, headers={"User-Agent": "GreekBot/1.0"})
    with urllib.request.urlopen(req, timeout=10) as r:
        text = r.read().decode("utf-8")
    reader = csv.reader(io.StringIO(text))
    val = None
    for row in reader:
        if len(row) == 2 and row[1] not in (".", ""):
            try:
                val = float(row[1])
            except ValueError:
                pass
    return val if val is not None else 0.0

def _get_yields() -> dict:
    """Fetch daily Treasury yields and TIPS real yield from FRED."""
    try:
        y2   = _cached("fred:DGS2",   lambda: _fred_latest("DGS2"))
        y10  = _cached("fred:DGS10",  lambda: _fred_latest("DGS10"))
        tips = _cached("fred:DFII10", lambda: _fred_latest("DFII10"))
        spread = round(y10 - y2, 2)
        inf_breakeven = round(y10 - tips, 2)
        return {
            "y2":             round(y2, 2),
            "y10":            round(y10, 2),
            "tips":           round(tips, 2),
            "spread_2_10":    spread,
            "inf_breakeven":  inf_breakeven,
            "curve_label":    "Inverted" if spread < 0 else ("Normal" if spread > 0.2 else "Flat"),
            "curve_bias":     "BEAR" if spread < -0.1 else ("BULL" if spread > 0.2 else "NEUT"),
            "source":         "FRED",
        }
    except Exception as e:
        print(f"[macro/yields] FRED error: {e} — using simulation")
        return _sim_yields()

def _sim_yields() -> dict:
    random.seed(int(time.time() / 3600))
    y2   = round(random.uniform(4.2, 5.0), 2)
    y10  = round(random.uniform(3.9, 4.8), 2)
    tips = round(random.uniform(1.6, 2.4), 2)
    spread = round(y10 - y2, 2)
    return {
        "y2": y2, "y10": y10, "tips": tips,
        "spread_2_10": spread, "inf_breakeven": round(y10 - tips, 2),
        "curve_label": "Inverted" if spread < 0 else "Normal",
        "curve_bias": "BEAR" if spread < -0.1 else "BULL",
        "source": "simulated",
    }

def _get_liquidity() -> dict:
    """Fetch Fed Balance Sheet, Reverse Repo, and TGA from FRED."""
    try:
        # WALCL = millions → convert to trillions
        fed_bs_m  = _cached("fred:WALCL",    lambda: _fred_latest("WALCL"))
        # RRPONTSYD = billions
        repo_b    = _cached("fred:RRPONTSYD",lambda: _fred_latest("RRPONTSYD"))
        # WTREGEN = millions → convert to billions
        tga_m     = _cached("fred:WTREGEN",  lambda: _fred_latest("WTREGEN"))
        fed_bs_t  = round(fed_bs_m / 1e6, 2)   # → trillions
        tga_b     = round(tga_m    / 1e3, 1)    # → billions
        return {
            "fed_bs_t":   fed_bs_t,
            "repo_b":     round(repo_b, 1),
            "tga_b":      tga_b,
            # Bias: shrinking balance sheet = BEAR (tightening liquidity)
            "fed_bs_bias":  "BULL" if fed_bs_t > 7.5 else ("NEUT" if fed_bs_t > 6.5 else "BEAR"),
            "repo_bias":    "BEAR" if repo_b > 500 else ("NEUT" if repo_b > 200 else "BULL"),
            "tga_bias":     "BEAR" if tga_b > 700 else ("NEUT" if tga_b > 400 else "BULL"),
            "source":       "FRED",
        }
    except Exception as e:
        print(f"[macro/liquidity] FRED error: {e} — using simulation")
        return _sim_liquidity()

def _sim_liquidity() -> dict:
    random.seed(int(time.time() / 3600))
    fed_bs = round(random.uniform(6.5, 7.5), 2)
    repo   = round(random.uniform(100, 600), 1)
    tga    = round(random.uniform(300, 900), 1)
    return {
        "fed_bs_t": fed_bs, "repo_b": repo, "tga_b": tga,
        "fed_bs_bias": "NEUT", "repo_bias": "NEUT", "tga_bias": "NEUT",
        "source": "simulated",
    }

ALPHA_VANTAGE_KEY = os.environ.get("ALPHA_VANTAGE_KEY", "")

# ── Cache (1-hour TTL to stay inside AV free-tier 25 calls/day) ───────────────
_cache      = {}
_cache_ts   = {}
_cache_lock = threading.Lock()
CACHE_TTL   = 3600  # seconds

def _cached(key, fn):
    with _cache_lock:
        now = time.time()
        if key in _cache and now - _cache_ts.get(key, 0) < CACHE_TTL:
            return _cache[key]
    result = fn()
    with _cache_lock:
        _cache[key] = result
        _cache_ts[key] = time.time()
    return result

# ── Alpha Vantage HTTP fetch ───────────────────────────────────────────────────
_AV_BASE = "https://www.alphavantage.co/query"

def _av_get(params: dict) -> dict:
    params["apikey"] = ALPHA_VANTAGE_KEY
    qs  = "&".join(f"{k}={v}" for k, v in params.items())
    url = f"{_AV_BASE}?{qs}"
    req = urllib.request.Request(url, headers={"User-Agent": "GreekBot/1.0"})
    with urllib.request.urlopen(req, timeout=12) as r:
        return json.loads(r.read())

def _latest_value(data: dict) -> float:
    """Extract most-recent numeric value from AV time-series response."""
    rows = data.get("data", [])
    for row in rows:
        try:
            return float(row["value"])
        except (KeyError, ValueError):
            continue
    return 0.0

# ── Live fetchers ──────────────────────────────────────────────────────────────
def _live_news(ticker: str, count: int = 8) -> list:
    data   = _av_get({"function": "NEWS_SENTIMENT", "tickers": ticker, "limit": str(count)})
    feed   = data.get("feed", [])
    result = []
    label_map = {
        "Bullish":         "BULL",
        "Somewhat-Bullish":"BULL",
        "Neutral":         "NEUT",
        "Somewhat-Bearish":"BEAR",
        "Bearish":         "BEAR",
    }
    for item in feed[:count]:
        raw_label = item.get("overall_sentiment_label", "Neutral")
        score     = float(item.get("overall_sentiment_score", 0.0))
        result.append({
            "title":          item.get("title", ""),
            "label":          label_map.get(raw_label, "NEUT"),
            "score":          round(score, 4),
            "source":         item.get("source", ""),
            "time_published": item.get("time_published", "")[:16].replace("T", " "),
            "url":            item.get("url", ""),
        })
    return result

def _live_econ() -> list:
    def get(fn, **kw):
        return _cached(f"av:{fn}", lambda: _av_get({"function": fn, **kw}))

    fed   = _latest_value(get("FEDERAL_FUNDS_RATE", interval="monthly"))
    cpi   = _latest_value(get("CPI", interval="monthly"))
    gdp   = _latest_value(get("REAL_GDP", interval="quarterly"))
    unemp = _latest_value(get("UNEMPLOYMENT"))
    y10   = _latest_value(get("TREASURY_YIELD", interval="monthly", maturity="10year"))
    y2    = _latest_value(get("TREASURY_YIELD", interval="monthly", maturity="2year"))
    # PCE: AV doesn't have a dedicated PCE endpoint on free tier — estimate from CPI
    pce   = round(cpi * 0.88, 2)
    spread = round(y10 - y2, 2)

    def trend_arrow(v, threshold): return "▲" if v > threshold else "▼"
    def cpi_bias(c):   return "BEAR" if c > 3.5 else ("NEUT" if c > 2.5 else "BULL")
    def fed_bias(r):   return "BEAR" if r > 5.0 else ("NEUT" if r > 4.5 else "BULL")
    def unemp_bias(u): return "BULL" if u < 4.0 else ("NEUT" if u < 4.5 else "BEAR")
    def gdp_bias(g):   return "BULL" if g > 2.5 else ("NEUT" if g > 1.0 else "BEAR")

    return [
        {"name": "Fed Funds Rate", "value": f"{fed:.2f}%",   "raw": fed,
         "trend": trend_arrow(fed, 5.0), "bias": fed_bias(fed),   "weight": 0.25},
        {"name": "CPI (YoY)",      "value": f"{cpi:.1f}%",   "raw": cpi,
         "trend": trend_arrow(cpi, 3.0), "bias": cpi_bias(cpi),   "weight": 0.20},
        {"name": "Unemployment",   "value": f"{unemp:.1f}%", "raw": unemp,
         "trend": "▼" if unemp < 4.0 else "▲", "bias": unemp_bias(unemp), "weight": 0.15},
        {"name": "GDP Growth",     "value": f"{gdp:.1f}%",   "raw": gdp,
         "trend": trend_arrow(gdp, 2.0), "bias": gdp_bias(gdp),   "weight": 0.20},
        {"name": "PCE (YoY)",      "value": f"{pce:.1f}%",   "raw": pce,
         "trend": trend_arrow(pce, 3.0), "bias": cpi_bias(pce),   "weight": 0.10},
        {"name": "10Y Yield",      "value": f"{y10:.2f}%",   "raw": y10,
         "trend": trend_arrow(y10, 4.3), "bias": "BEAR" if y10 > 4.5 else "NEUT", "weight": 0.05},
        {"name": "Yield Curve",    "value": f"{spread:+.2f}%", "raw": spread,
         "trend": "▲" if spread > 0 else "▼",
         "bias": "BULL" if spread > 0.1 else ("NEUT" if spread > -0.2 else "BEAR"), "weight": 0.05},
    ]

# ── Simulation fallback (used when no AV key) ─────────────────────────────────
_NEWS_POOL = [
    ("Exchange-Traded Funds, Equity Futures Higher Pre-Bell as Tech Leads Gains",      "BULL"),
    ("WCLD Down 50% as Growth Investors Finally Get Cold Feet",                         "BEAR"),
    ("Exchange-Traded Funds, US Equities Mixed After Mid-Session Volatility Spike",     "NEUT"),
    ("Fed Officials Signal Patience on Rate Cuts Amid Stubborn Services Inflation",     "BEAR"),
    ("Strong Jobs Report Beats Expectations, Wage Growth Remains Elevated",             "BULL"),
    ("S&P 500 Eyes Record High as AI Spending Cycle Shows No Signs of Slowing",        "BULL"),
    ("Treasury Yields Spike After Weak 10Y Auction; Risk Assets Sell Off",             "BEAR"),
    ("Consumer Confidence Drops to 12-Month Low on Tariff Worries",                   "BEAR"),
    ("Retail Sales Surge Unexpectedly, Signaling Resilient US Consumer",               "BULL"),
    ("Options Market Implies Elevated Tail Risk Ahead of CPI Print",                   "NEUT"),
    ("Dollar Strengthens Sharply Against Majors; Commodities Under Pressure",          "BEAR"),
    ("VIX Drops Below 15 — Complacency or Justified Calm?",                           "NEUT"),
]
_SENTIMENT_SCORES = {"BULL": (0.20, 0.50), "BEAR": (-0.50, -0.15), "NEUT": (-0.10, 0.10)}

def _sim_news(ticker="SPY", count=6):
    random.seed(int(time.time() / 600))  # change every 10 min
    pool = _NEWS_POOL.copy(); random.shuffle(pool)
    items = []
    for title, label in pool[:count]:
        lo, hi = _SENTIMENT_SCORES[label]
        score  = round(random.uniform(lo, hi), 4)
        items.append({
            "title":          title,
            "label":          label,
            "score":          score,
            "source":         random.choice(["Reuters", "Bloomberg", "Barron's", "WSJ", "CNBC"]),
            "time_published": (datetime.utcnow() - timedelta(minutes=random.randint(5, 300))
                               ).strftime("%Y-%m-%d %H:%M"),
            "url": "",
        })
    return items

def _sim_econ():
    random.seed(int(time.time() / 3600))
    fed_rate   = round(random.uniform(4.75, 5.50), 2)
    cpi_yoy    = round(random.uniform(2.8,  4.2),  2)
    unemp      = round(random.uniform(3.6,  4.5),  1)
    gdp_growth = round(random.uniform(1.5,  3.5),  2)
    pce_yoy    = round(random.uniform(2.5,  3.8),  2)
    yield_10y  = round(random.uniform(3.9,  4.9),  2)
    yield_2y   = round(random.uniform(4.0,  5.1),  2)
    yi_spread  = round(yield_10y - yield_2y, 2)

    def cpi_bias(c):   return "BEAR" if c > 3.5 else ("NEUT" if c > 2.5 else "BULL")
    def fed_bias(r):   return "BEAR" if r > 5.0 else ("NEUT" if r > 4.5 else "BULL")
    def unemp_bias(u): return "BULL" if u < 4.0 else ("NEUT" if u < 4.5 else "BEAR")
    def gdp_bias(g):   return "BULL" if g > 2.5 else ("NEUT" if g > 1.0 else "BEAR")

    return [
        {"name": "Fed Funds Rate", "value": f"{fed_rate}%",   "raw": fed_rate,
         "trend": "▼" if fed_rate < 5.0 else "▲", "bias": fed_bias(fed_rate), "weight": 0.25},
        {"name": "CPI (YoY)",      "value": f"{cpi_yoy}%",   "raw": cpi_yoy,
         "trend": "▲" if cpi_yoy > 3.0 else "▼", "bias": cpi_bias(cpi_yoy),   "weight": 0.20},
        {"name": "Unemployment",   "value": f"{unemp}%",     "raw": unemp,
         "trend": "▼" if unemp < 4.0 else "▲", "bias": unemp_bias(unemp),     "weight": 0.15},
        {"name": "GDP Growth",     "value": f"{gdp_growth}%","raw": gdp_growth,
         "trend": "▲" if gdp_growth > 2.0 else "▼", "bias": gdp_bias(gdp_growth), "weight": 0.20},
        {"name": "PCE (YoY)",      "value": f"{pce_yoy}%",   "raw": pce_yoy,
         "trend": "▼" if pce_yoy < 3.0 else "▲", "bias": cpi_bias(pce_yoy),   "weight": 0.10},
        {"name": "10Y Yield",      "value": f"{yield_10y}%", "raw": yield_10y,
         "trend": "▲" if yield_10y > 4.3 else "▼",
         "bias": "BEAR" if yield_10y > 4.5 else "NEUT", "weight": 0.05},
        {"name": "Yield Curve",    "value": f"{yi_spread:+.2f}%", "raw": yi_spread,
         "trend": "▲" if yi_spread > 0 else "▼",
         "bias": "BULL" if yi_spread > 0.1 else ("NEUT" if yi_spread > -0.2 else "BEAR"),
         "weight": 0.05},
    ]

# ── Bias Scorer ────────────────────────────────────────────────────────────────
def _label_score(label):
    return {"BULL": 1.0, "NEUT": 0.0, "BEAR": -1.0}.get(label, 0.0)

def _compute_bias(news_items, econ_indicators):
    news_score = 0.0
    if news_items:
        news_score = sum(n["score"] for n in news_items) / len(news_items)
        news_score = max(-1.0, min(1.0, news_score))

    econ_total_w = sum(e["weight"] for e in econ_indicators)
    econ_score   = sum(_label_score(e["bias"]) * e["weight"] for e in econ_indicators)
    if econ_total_w > 0:
        econ_score /= econ_total_w

    combined = round(news_score * 0.40 + econ_score * 0.60, 3)

    if   combined >  0.45: label, color = "STRONGLY BULLISH", "#2ecc8a"
    elif combined >  0.15: label, color = "BULLISH",          "#2ecc8a"
    elif combined > -0.15: label, color = "NEUTRAL",          "#f5c542"
    elif combined > -0.45: label, color = "BEARISH",          "#e8435a"
    else:                  label, color = "STRONGLY BEARISH", "#e8435a"

    htf = "BULLISH" if econ_score > 0.1 else ("BEARISH" if econ_score < -0.1 else "NEUTRAL")
    return {
        "score": combined, "label": label, "color": color,
        "news_score": round(news_score, 3), "econ_score": round(econ_score, 3),
        "htf": htf,
    }

# ── Public API ─────────────────────────────────────────────────────────────────
def get_macro_data(ticker="SPY", api_key=""):
    key = api_key or ALPHA_VANTAGE_KEY
    live = bool(key and key not in ("demo", ""))

    if live:
        try:
            # News: cache per ticker (10-min TTL for news)
            news = _cached(f"av_news:{ticker}", lambda: _live_news(ticker))
            econ = _cached("av_econ", _live_econ)
        except Exception as e:
            print(f"[macro] Alpha Vantage error: {e} — falling back to simulation")
            live = False
            news = _sim_news(ticker)
            econ = _sim_econ()
    else:
        news = _sim_news(ticker)
        econ = _sim_econ()

    bias = _compute_bias(news, econ)
    yields     = _get_yields()
    liquidity  = _get_liquidity()
    return {
        "ticker":     ticker,
        "timestamp":  datetime.utcnow().isoformat(),
        "api_active": live,
        "news":       news,
        "econ":       econ,
        "bias":       bias,
        "yields":     yields,
        "liquidity":  liquidity,
    }
