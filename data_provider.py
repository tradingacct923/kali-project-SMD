"""
Data Provider — Tradier API
Fetches real options chain data from Tradier and computes all greeks exposures.
Falls back to a lightweight simulation if no token is configured.
"""

import os, math, json, time, threading
from datetime import datetime, date
from dotenv import load_dotenv

load_dotenv()

# Dynamic config reader
def _cfg():
    """Load config.json fresh each call so Settings changes take effect instantly."""
    import json as _j
    p = __import__('os').path.join(__import__('os').path.dirname(__import__('os').path.abspath(__file__)), 'config.json')
    try:
        with open(p) as _f: return _j.load(_f)
    except Exception: return {}

# ── BSM market parameters ─────────────────────────────────────────────────────
# r: annualised risk-free rate (default = 4.5% ≈ current Fed Funds)
# q: continuous dividend yield  (default = 0.5% ≈ QQQ/SPY ETFs)
# Override via environment variables if needed.
_R = 0.045  # fallback; overridden from config.json at call time
_Q = 0.005  # fallback; overridden from config.json at call time

# ── BSM helpers (no external deps, pure math) ─────────────────────────────────
_SQRT2   = math.sqrt(2.0)
_SQRT2PI = math.sqrt(2.0 * math.pi)

def _npdf(x: float) -> float:
    """Standard normal PDF N'(x)."""
    return math.exp(-0.5 * x * x) / _SQRT2PI

def _ncdf(x: float) -> float:
    """Standard normal CDF N(x) via erf."""
    return 0.5 * (1.0 + math.erf(x / _SQRT2))

def _bsm_vanna_charm(S: float, K: float, T: float, sigma: float,
                     r: float = _R, q: float = _Q,
                     option_type: str = "call") -> tuple:
    """
    Compute exact Vanna and Charm using BSM with continuous dividend yield.

    Parameters
    ----------
    S          : spot price
    K          : strike price
    T          : time to expiry in years  (> 0)
    sigma      : implied volatility       (> 0)
    r          : risk-free rate (continuous, annualised)
    q          : continuous dividend yield
    option_type: 'call' or 'put'

    Returns
    -------
    (vanna, charm)

    Formulas
    --------
    d1    = [ln(S/K) + (r - q + σ²/2)·T] / (σ·√T)
    d2    = d1 - σ·√T

    Vanna = -e^(-qT) · N'(d1) · d2/σ
          (≡ ∂Delta/∂σ  ≡  ∂Vega/∂S,  same sign for calls & puts)

    Charm_call = q·e^(-qT)·N(d1)  - e^(-qT)·N'(d1)·[2(r-q)T - d2·σ·√T]
                                                       / (2·T·σ·√T)
    Charm_put  = -q·e^(-qT)·N(-d1) - e^(-qT)·N'(d1)·[2(r-q)T - d2·σ·√T]
                                                       / (2·T·σ·√T)
    """
    if T <= 1e-6 or sigma <= 1e-6 or S <= 0 or K <= 0:
        return 0.0, 0.0
    # Read r/q from config when caller used module defaults
    if r == _R and q == _Q:
        _c3 = _cfg()
        r = float(_c3.get("risk_free_rate", _R))
        q = float(_c3.get("dividend_yield", _Q))

    sqrt_T = math.sqrt(T)
    d1 = (math.log(S / K) + (r - q + 0.5 * sigma * sigma) * T) / (sigma * sqrt_T)
    d2 = d1 - sigma * sqrt_T

    eq_T  = math.exp(-q * T)   # e^(-qT)
    nd1   = _npdf(d1)           # N'(d1)

    # ── Vanna: -e^(-qT) · N'(d1) · d2/σ ─────────────────────────────────────
    vanna = -eq_T * nd1 * d2 / sigma

    # ── Charm common term: e^(-qT)·N'(d1)·[2(r-q)T - d2·σ·√T]/(2T·σ·√T) ───
    charm_common = eq_T * nd1 * (2.0 * (r - q) * T - d2 * sigma * sqrt_T) \
                   / (2.0 * T * sigma * sqrt_T)

    if option_type == "call":
        charm = q * eq_T * _ncdf(d1)  - charm_common
    else:
        charm = -q * eq_T * _ncdf(-d1) - charm_common

    return vanna, charm

# ── Tradier config ────────────────────────────────────────────────────────────
_TRADIER_BASE_PROD    = "https://api.tradier.com/v1"
_TRADIER_BASE_SANDBOX = "https://sandbox.tradier.com/v1"

def _get_token():
    """Read token fresh each call so Settings changes take effect immediately."""
    import json as _j
    cfg_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "config.json")
    try:
        with open(cfg_path) as f:
            cfg = _j.load(f)
        t = cfg.get("options_api_key", "")
        if t:
            return t
    except Exception:
        pass
    return os.getenv("TRADIER_TOKEN", "")

def _tradier_get(path, params=None):
    import urllib.request, urllib.parse
    token = _get_token()
    if not token:
        raise ValueError("No Tradier token configured. Add it in Settings → Options Data API.")
    base = _TRADIER_BASE_PROD
    url  = base + path
    if params:
        url += "?" + urllib.parse.urlencode(params)
    req = urllib.request.Request(url, headers={
        "Authorization": f"Bearer {token}",
        "Accept": "application/json",
    })
    with urllib.request.urlopen(req, timeout=15) as r:
        return json.loads(r.read())

# ── Cache to avoid hammering Tradier on every /api/data poll ──────────────────
_cache      = {}
_cache_ts   = {}
_cache_lock = threading.Lock()
CACHE_TTL   = 60  # seconds

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

# ── Tradier helpers ───────────────────────────────────────────────────────────
def _fetch_quote(ticker):
    d = _tradier_get("/markets/quotes", {"symbols": ticker, "greeks": "false"})
    q = d["quotes"]["quote"]
    return float(q.get("last") or q.get("close") or q.get("bid", 0))

def _tradier_timesales(ticker, start, end, interval="5min"):
    """
    Real-time intraday OHLCV from Tradier /markets/timesales.
    start / end : datetime objects (ET market time).
    Returns DataFrame with DatetimeIndex and Open/High/Low/Close/Volume columns.
    """
    import pandas as pd
    params = {
        "symbol":         ticker,
        "interval":       interval,
        "start":          start.strftime("%Y-%m-%d %H:%M"),
        "end":            end.strftime("%Y-%m-%d %H:%M"),
        "session_filter": "open",
    }
    try:
        d    = _tradier_get("/markets/timesales", params)
        ser  = d.get("series") or {}
        data = ser.get("data", [])
        if not data:
            return pd.DataFrame()
        if isinstance(data, dict):
            data = [data]
        df = pd.DataFrame(data)
        df["time"] = pd.to_datetime(df["time"])
        df = df.set_index("time").rename(columns={
            "open": "Open", "high": "High",
            "low":  "Low",  "close": "Close", "volume": "Volume",
        })
        return df
    except Exception:
        return pd.DataFrame()

def _tradier_history(ticker, start, end, interval="daily"):
    """
    Real-time daily/weekly OHLCV from Tradier /markets/history.
    start / end : date or datetime objects.
    Returns DataFrame with DatetimeIndex and Open/High/Low/Close/Volume columns.
    """
    import pandas as pd
    s = start.date() if hasattr(start, "date") else start
    e = end.date()   if hasattr(end,   "date") else end
    params = {
        "symbol":   ticker,
        "interval": interval,
        "start":    s.strftime("%Y-%m-%d"),
        "end":      e.strftime("%Y-%m-%d"),
    }
    try:
        d    = _tradier_get("/markets/history", params)
        hist = d.get("history") or {}
        days = hist.get("day", [])
        if not days:
            return pd.DataFrame()
        if isinstance(days, dict):
            days = [days]
        df = pd.DataFrame(days)
        df["date"] = pd.to_datetime(df["date"])
        df = df.set_index("date").rename(columns={
            "open": "Open", "high": "High",
            "low":  "Low",  "close": "Close", "volume": "Volume",
        })
        return df
    except Exception:
        return pd.DataFrame()

def _fetch_expirations(ticker):
    d = _tradier_get("/markets/options/expirations", {"symbol": ticker, "includeallroots": "true", "strikes": "false"})
    dates = d.get("expirations", {}).get("date", [])
    if isinstance(dates, str):
        dates = [dates]
    today = date.today()
    result = []
    for ds in dates:
        dt = date.fromisoformat(ds)
        dte = (dt - today).days
        label = dt.strftime("(%a)%b %d '%y")
        result.append({"date": ds, "label": label, "dte": dte})
    return result

def _fetch_chain(ticker, expiration_date):
    """Returns list of option contract dicts (calls + puts) with greeks."""
    d = _tradier_get("/markets/options/chains", {
        "symbol": ticker,
        "expiration": expiration_date,
        "greeks": "true",
    })
    opts = d.get("options", {}).get("option", [])
    if isinstance(opts, dict):  # single option edge case
        opts = [opts]
    return opts or []

# ── Exposure calculators ──────────────────────────────────────────────────────
def _safe(v, default=0.0):
    try:
        return float(v) if v is not None else default
    except (TypeError, ValueError):
        return default

def _dte_weight(dte):
    if dte <= 0:  return 3.0
    if dte <= 2:  return 2.5
    if dte <= 7:  return 2.0
    if dte <= 14: return 1.5
    if dte <= 28: return 1.2
    if dte <= 56: return 1.0
    return 0.7

def _build_exposures(ticker, expirations, spot):
    """
    Fetches every expiration chain and builds per-strike/exp exposure dicts.
    Returns oi, dex, gex_per_exp, vex, tex, vannex, cex keyed by strike, plus
    oi_full (all strikes, no range filter) used for an accurate Max Pain calc.
    """
    # We only process the first MAX_EXPIRATIONS expirations to keep it fast
    _c = _cfg()
    exps_to_use = expirations[:int(_c.get("max_expirations", 3))]

    # Collect per-strike, per-expiration data
    # Each metric: { strike: [ {label, dte, calls_val, puts_val}, ... ] }
    oi_data     = {}   # range-filtered (for heatmaps/bars)
    oi_full     = {}   # ALL strikes — used only for Max Pain
    dex_data    = {}
    gex_data    = {}
    vex_data    = {}
    tex_data    = {}
    vannex_data = {}
    cex_data    = {}
    rex_data    = {}

    center = round(spot)
    _sr = int(_cfg().get("strike_range", 30))
    strike_min = center - _sr
    strike_max = center + _sr

    for exp in exps_to_use:
        label = exp["label"]
        dte   = exp["dte"]
        t     = max(dte, 1) / 365.0
        chain = _cached(f"chain:{ticker}:{exp['date']}", lambda d=exp["date"]: _fetch_chain(ticker, d))

        # Index by strike
        calls = {_safe(c["strike"]): c for c in chain if c.get("option_type") == "call"}
        puts  = {_safe(c["strike"]): c for c in chain if c.get("option_type") == "put"}

        all_strikes = sorted(set(calls) | set(puts))
        filtered    = [s for s in all_strikes if strike_min <= s <= strike_max]

        for s in filtered:
            c = calls.get(s, {})
            p = puts.get(s, {})

            # Open interest
            c_oi = _safe(c.get("open_interest"))
            p_oi = _safe(p.get("open_interest"))

            # Greeks
            c_delta = _safe(c.get("greeks", {}).get("delta") if c.get("greeks") else None)
            p_delta = _safe(p.get("greeks", {}).get("delta") if p.get("greeks") else None)
            c_gamma = _safe(c.get("greeks", {}).get("gamma") if c.get("greeks") else None)
            p_gamma = _safe(p.get("greeks", {}).get("gamma") if p.get("greeks") else None)
            c_vega  = _safe(c.get("greeks", {}).get("vega")  if c.get("greeks") else None)
            p_vega  = _safe(p.get("greeks", {}).get("vega")  if p.get("greeks") else None)
            c_theta = _safe(c.get("greeks", {}).get("theta") if c.get("greeks") else None)
            p_theta = _safe(p.get("greeks", {}).get("theta") if p.get("greeks") else None)
            c_rho   = _safe(c.get("greeks", {}).get("rho")   if c.get("greeks") else None)
            p_rho   = _safe(p.get("greeks", {}).get("rho")   if p.get("greeks") else None)

            # ── Vanna & Charm: exact BSM with continuous dividend yield ──────
            c_iv = _safe(c.get("greeks", {}).get("mid_iv") if c.get("greeks") else None, 0.20)
            p_iv = _safe(p.get("greeks", {}).get("mid_iv") if p.get("greeks") else None, 0.20)
            c_iv = max(c_iv, 0.01)  # floor at 1% to avoid division by zero
            p_iv = max(p_iv, 0.01)

            c_vanna, c_charm = _bsm_vanna_charm(spot, s, t, c_iv, option_type="call")
            p_vanna, p_charm = _bsm_vanna_charm(spot, s, t, p_iv, option_type="put")

            # ── Exposure calcs ──────────────────────────────────────────────
            # Tradier greeks are per-share; OI × 100 = total shares per contract.
            # GEX:     Gamma × OI × 100 × Spot/100   → $ delta added per 1% spot move
            #          (= Gamma × OI × Spot; the Spot/100 factor converts from
            #          "per $1 move" to "per 1% move" on a per-share basis)
            # DEX:     Delta × OI × 100 × Spot        → $ notional delta
            # VEX:     Vega  × OI × 100               → $ per 1-vol-point move
            # TEX:     Theta × OI × 100               → $ per calendar day (negative = decay)
            # VannaEX: Vanna × OI × 100               → signed $
            # CharmEX: Charm × OI × 100               → signed $ per day

            def append(d, calls_val, puts_val):
                d.setdefault(s, []).append({
                    "label": label, "dte": dte,
                    "calls_val": float(calls_val),
                    "puts_val":  float(puts_val),
                })

            append(oi_data,
                   c_oi,
                   p_oi)

            append(dex_data,
                   c_delta     * c_oi * 100 * spot,    # calls: positive delta notional
                   -abs(p_delta) * p_oi * 100 * spot)  # puts:  negative delta notional

            append(gex_data,
                   c_gamma * c_oi * 100 * (spot * spot / 100),   # calls: standard $ GEX = gamma × shares × spot²/100
                   p_gamma * p_oi * 100 * (spot * spot / 100))   # puts:  standard $ GEX

            append(vex_data,
                   c_vega  * c_oi * 100,
                   p_vega  * p_oi * 100)

            append(tex_data,
                   c_theta * c_oi * 100,               # theta already negative
                   p_theta * p_oi * 100)

            append(vannex_data,
                   c_vanna * c_oi * 100,               # signed
                   p_vanna * p_oi * 100)               # signed

            append(cex_data,
                   c_charm * c_oi * 100,               # signed
                   p_charm * p_oi * 100)               # signed

            append(rex_data,
                   c_rho * c_oi * 100,                 # REX = Rho × OI × 100
                   p_rho * p_oi * 100)

        # ── Full-chain OI (no strike filter) — used for Max Pain only ──────
        for s in all_strikes:
            c_all = calls.get(s, {})
            p_all = puts.get(s, {})
            c_oi_all = _safe(c_all.get("open_interest"))
            p_oi_all = _safe(p_all.get("open_interest"))
            oi_full.setdefault(s, []).append({
                "label": label, "dte": dte,
                "calls_val": float(c_oi_all),
                "puts_val":  float(p_oi_all),
            })

    return oi_data, dex_data, gex_data, vex_data, tex_data, vannex_data, cex_data, rex_data, oi_full


def _build_gex_result(gex_data, spot):
    net_gex = {}
    for s, exps in gex_data.items():
        net_gex[s] = sum(e["calls_val"] - e["puts_val"] for e in exps)

    above = {s: v for s, v in net_gex.items() if s > spot and v > 0}
    below = {s: v for s, v in net_gex.items() if s < spot and v < 0}
    call_wall  = max(above, key=above.get)  if above else spot + 5
    put_wall   = min(below, key=below.get)  if below else spot - 5
    major_wall = max(net_gex, key=lambda s: abs(net_gex[s])) if net_gex else spot

    return {
        "net_gex": net_gex, "per_exp": gex_data,
        "call_wall": call_wall, "put_wall": put_wall, "major_wall": major_wall,
    }


def _build_max_pain(oi_data, spot):
    """Max pain = strike where total option payout to holders is minimized."""
    strikes = sorted(oi_data.keys())
    if not strikes:
        return {"max_pain_strike": spot, "payout_by_strike": {}}

    # Sum OI per strike
    oi_by_strike = {}
    for s, exps in oi_data.items():
        oi_by_strike[s] = {
            "calls": sum(e["calls_val"] for e in exps),
            "puts":  sum(e["puts_val"]  for e in exps),
        }

    best_strike = spot
    best_pain   = float("inf")
    payout = {}
    for candidate in strikes:
        total = 0.0
        for s, oi in oi_by_strike.items():
            if s < candidate:  # calls OTM, puts ITM
                total += oi["puts"] * (candidate - s)
            elif s > candidate:  # calls ITM, puts OTM
                total += oi["calls"] * (s - candidate)
        payout[candidate] = total
        if total < best_pain:
            best_pain   = total
            best_strike = candidate

    return {"max_pain_strike": best_strike, "payout_by_strike": payout}


def _build_iv_surface(ticker, spot, expirations):
    """Build IV surface from real chain data."""
    exps_to_use = expirations[:int(_cfg().get("max_expirations", 3)) + 4]
    strike_step = 5
    center = round(spot / strike_step) * strike_step
    strikes = [float(s) for s in range(center - 60, center + 65, strike_step)]

    def _extract_iv(opt):
        """Try multiple IV fields in priority order."""
        g = opt.get("greeks") or {}
        candidates = [
            g.get("mid_iv"),
            g.get("smv_vol"),
            # avg of bid/ask IV if both present
            ((_safe(g.get("bid_iv")) + _safe(g.get("ask_iv"))) / 2
             if g.get("bid_iv") and g.get("ask_iv") else None),
            opt.get("implied_volatility"),
        ]
        for c in candidates:
            try:
                v = float(c)
                if 0.01 <= v <= 2.0:   # clamp to 1% – 200%
                    return v
            except (TypeError, ValueError):
                continue
        return None

    surface = []
    for exp in exps_to_use:
        dte   = max(exp["dte"], 1)
        chain = _cached(f"chain:{ticker}:{exp['date']}", lambda d=exp["date"]: _fetch_chain(ticker, d))

        # Collect best IV per strike (average call + put if both present)
        iv_sum   = {}
        iv_count = {}
        for opt in chain:
            s  = _safe(opt.get("strike"))
            iv = _extract_iv(opt)
            if iv is not None and s in [sk for sk in strikes]:
                iv_sum[s]   = iv_sum.get(s, 0.0) + iv
                iv_count[s] = iv_count.get(s, 0) + 1

        iv_by_strike = {s: iv_sum[s] / iv_count[s] for s in iv_sum}

        # Build row — None for missing, real value for present
        row = [iv_by_strike.get(s) for s in strikes]

        # Linear interpolation only across interior gaps (not edges)
        ivs = list(row)
        for i in range(len(ivs)):
            if ivs[i] is None:
                prev_i = next((j for j in range(i-1, -1, -1) if row[j] is not None), None)
                next_i = next((j for j in range(i+1, len(row)) if row[j] is not None), None)
                if prev_i is not None and next_i is not None:
                    # Only interpolate gaps ≤ 6 strikes wide (30pts)
                    if next_i - prev_i <= 6:
                        t = (i - prev_i) / (next_i - prev_i)
                        ivs[i] = round(row[prev_i] * (1-t) + row[next_i] * t, 4)
                    # else leave as None — Plotly will skip it cleanly

        ivs = [round(v, 4) if v is not None else None for v in ivs]
        surface.append({"label": exp["label"], "dte": dte, "ivs": ivs})

    return {
        "strikes": strikes,
        "expirations": [{"label": e["label"], "dte": max(e["dte"], 1)} for e in exps_to_use],
        "surface": surface,
    }


# ── IV surface (used by /api/volatility) ─────────────────────────────────────
def calculate_iv_surface(spot=None, ticker="QQQ"):
    if spot is None:
        spot = _cached(f"quote:{ticker}", lambda: _fetch_quote(ticker))
    exps = _cached(f"exps:{ticker}", lambda: _fetch_expirations(ticker))
    result = _build_iv_surface(ticker, spot, exps)
    result["spot"] = spot
    return result


# ── Main fetch ────────────────────────────────────────────────────────────────
def fetch_all(ticker):
    try:
        spot = _cached(f"quote:{ticker}", lambda: _fetch_quote(ticker))
        exps = _cached(f"exps:{ticker}",  lambda: _fetch_expirations(ticker))

        oi, dex, gex_raw, vex, tex, vannex, cex, rex, oi_full = _build_exposures(ticker, exps, spot)

        print(f"[data] LIVE {ticker} ${spot:.2f}  |  {len(exps)} expirations loaded")
        return {
            "ticker": ticker, "spot": spot,
            "oi":      oi,
            "dex":     dex,
            "gex":     _build_gex_result(gex_raw, spot),
            "vex":     vex,
            "tex":     tex,
            "vannex":  vannex,
            "cex":     cex,
            "rex":     rex,
            # Max pain uses full-chain OI (all strikes, not just ±range)
            # so far-OTM positions that drive the real max pain are captured.
            "max_pain": _build_max_pain(oi_full, spot),
            "timestamp": datetime.now(),
        }
    except ValueError as e:
        raise
    except Exception as e:
        print(f"[data] Tradier error: {e}")
        raise


# ── 365-DTE OI chart ──────────────────────────────────────────────────────────
def build_oi365(ticker):
    """
    Fetch OI for all expirations up to 365 DTE in parallel.
    Returns list of {label, dte, strikes: {strike: {calls, puts}}} sorted by DTE.
    """
    spot = _cached(f"quote:{ticker}", lambda: _fetch_quote(ticker))
    all_exps = _cached(f"exps:{ticker}", lambda: _fetch_expirations(ticker))
    exps_365 = [e for e in all_exps if 0 <= e["dte"] <= 365]

    center = round(spot)
    _c = _cfg()
    sr = int(_c.get("strike_range", 30))
    strike_min = center - sr
    strike_max = center + sr

    results = {}
    lock = threading.Lock()

    def _fetch_one(exp):
        try:
            chain = _cached(
                f"chain:{ticker}:{exp['date']}",
                lambda d=exp["date"]: _fetch_chain(ticker, d)
            )
            by_strike = {}
            for opt in chain:
                s = _safe(opt.get("strike"))
                if not (strike_min <= s <= strike_max):
                    continue
                oi_val = _safe(opt.get("open_interest"))
                otype  = opt.get("option_type", "")
                if s not in by_strike:
                    by_strike[s] = {"calls": 0.0, "puts": 0.0}
                if otype == "call":
                    by_strike[s]["calls"] += oi_val
                elif otype == "put":
                    by_strike[s]["puts"]  += oi_val
            with lock:
                results[exp["date"]] = {
                    "label":   exp["label"],
                    "dte":     exp["dte"],
                    "strikes": by_strike,
                }
        except Exception as e:
            print(f"[oi365] {exp['date']}: {e}")

    threads = [threading.Thread(target=_fetch_one, args=(e,)) for e in exps_365]
    for t in threads: t.start()
    for t in threads: t.join(timeout=20)

    # Sort by DTE ascending
    sorted_exps = sorted(results.values(), key=lambda x: x["dte"])

    # Build aggregate: list of expirations with per-strike OI
    # Also compute per-expiration P/C ratio
    out = []
    for exp in sorted_exps:
        total_c = sum(v["calls"] for v in exp["strikes"].values())
        total_p = sum(v["puts"]  for v in exp["strikes"].values())
        pc = round(total_p / total_c, 2) if total_c > 0 else 0.0
        out.append({
            "label":   exp["label"],
            "dte":     exp["dte"],
            "pc":      pc,
            "total_oi": int(total_c + total_p),
            "strikes": {str(int(k)): v for k, v in exp["strikes"].items()},
        })

    return {"spot": spot, "expirations": out}


# ── Market Topology + Entropy Manifold ───────────────────────────────────────

def _kmeans_np(X, k=6, n_iter=150, seed=42):
    """Lightweight K-means using only numpy."""
    import numpy as np
    rng = np.random.default_rng(seed)
    centers = X[rng.choice(len(X), k, replace=False)].copy()
    labels  = np.zeros(len(X), dtype=int)
    for _ in range(n_iter):
        dists      = np.array([np.sum((X - c) ** 2, axis=1) for c in centers]).T
        new_labels = np.argmin(dists, axis=1)
        if np.array_equal(new_labels, labels):
            break
        labels = new_labels
        for i in range(k):
            mask = labels == i
            if mask.any():
                centers[i] = X[mask].mean(axis=0)
    return labels, centers


def build_topology(ticker):
    """
    3-D market state-space (Trend × Momentum × Volatility).
    Returns history cloud, recent breadcrumbs, current cross, regime label.
    """
    import numpy as np
    from datetime import datetime, timedelta

    _end   = datetime.today()
    _start = _end - timedelta(days=730)
    hist_df = _tradier_history(ticker, _start, _end)
    if hist_df.empty or "Close" not in hist_df.columns:
        return {"history": [], "recent": [], "current": [0,0,0], "base": [0,0,0], "regime": "N/A"}
    close   = hist_df["Close"].dropna().values
    log_ret = np.log(close[1:] / close[:-1])

    w = 20
    features = []
    for t in range(w, len(log_ret)):
        window = log_ret[t - w: t]
        # Trend: linear regression slope on log-price window
        x = np.arange(w, dtype=float)
        slope = (w * np.dot(x, window) - x.sum() * window.sum()) / \
                (w * np.dot(x, x) - x.sum() ** 2)
        mom = window.sum()                  # log-return over window
        vol = window.std() * np.sqrt(252)   # annualised realised vol
        features.append([slope * 500, mom * 100, vol])

    X  = np.array(features)
    mu = X.mean(axis=0);  sd = X.std(axis=0) + 1e-10
    Z  = (X - mu) / sd                     # z-scored features

    labels, centers = _kmeans_np(Z, k=6)

    recent_n  = min(30, len(Z))
    history_Z = Z[:-recent_n]
    recent_Z  = Z[-recent_n:]
    hist_lbls = labels[:-recent_n]

    cur = Z[-1]
    cid = int(labels[-1])
    dist = float(np.linalg.norm(cur - centers[cid]))

    if dist > 2.5:
        regime = "OUTLIER ⚠"
    elif cur[2] > 2.0:
        regime = "EXTREME VOLATILITY"
    elif cur[0] > 2.5:
        regime = "STRONG TREND"
    else:
        regime = "NORMAL REGIME"

    def _pts(arr, lbl=None):
        out = []
        for i, p in enumerate(arr):
            row = [round(float(p[0]), 3), round(float(p[1]), 3), round(float(p[2]), 3)]
            if lbl is not None:
                row.append(int(lbl[i]))
            out.append(row)
        return out

    return {
        "history":    _pts(history_Z, hist_lbls),
        "recent":     _pts(recent_Z),
        "current":    [round(float(v), 3) for v in cur],
        "base":       [round(float(v), 3) for v in Z[-5:].mean(axis=0)],
        "regime":     regime,
        "cluster_id": cid,
    }


def build_entropy(ticker):
    """
    Market Entropy Manifold — matches reference implementation exactly.
    Uses 60d / 5m intraday data, sklearn PCA on [Close, Entropy, Momentum].
      X = PCA1  (STATE)
      Y = PCA2  (MOMENTUM)
      Z = Entropy_Smooth  (CHAOS — rolling std of % returns)
    """
    import numpy as np
    import pandas as pd
    from datetime import datetime, timedelta
    import warnings
    warnings.filterwarnings("ignore")

    _end   = datetime.today()
    _start = _end - timedelta(days=20)   # Tradier caps timesales at ~1000 rows (~20 trading days)
    data = _tradier_timesales(ticker, _start, _end, interval="5min")

    if data.empty:
        return {"path": [], "current": [0,0,0],
                "current_entropy": 0, "threshold": 0, "status": "N/A"}

    data["Returns"]        = data["Close"].pct_change()
    data.dropna(inplace=True)
    data["Entropy"]        = data["Returns"].rolling(window=14).std()
    data["Entropy_Smooth"] = data["Entropy"].rolling(window=5).mean()
    data["Momentum"]       = (data["Close"].rolling(14).mean()
                               - data["Close"].rolling(50).mean())
    data.dropna(inplace=True)

    if len(data) < 20:
        return {"path": [], "current": [0,0,0],
                "current_entropy": 0, "threshold": 0, "status": "N/A"}

    features = data[["Close", "Entropy_Smooth", "Momentum"]].values
    # StandardScaler equivalent (numpy only)
    mu = features.mean(axis=0)
    sd = features.std(axis=0) + 1e-10
    scaled = (features - mu) / sd
    # PCA(n_components=2) equivalent via SVD (numpy only)
    centered = scaled - scaled.mean(axis=0)
    U, S, Vt = np.linalg.svd(centered, full_matrices=False)
    coords = centered @ Vt[:2].T

    data = data.copy()
    data["PCA1"] = coords[:, 0]   # STATE axis
    data["PCA2"] = coords[:, 1]   # MOMENTUM axis

    chaos_threshold = float(data["Entropy_Smooth"].quantile(0.95))
    curr            = data.iloc[-1]
    current_e       = float(curr["Entropy_Smooth"])
    status          = "CRITICAL" if current_e > chaos_threshold else "STABLE FLOW"

    # Down-sample to ≤1000 points to keep JSON size reasonable
    step   = max(1, len(data) // 1000)
    sample = data.iloc[::step]

    path = [
        [round(float(r["PCA1"]), 4),
         round(float(r["PCA2"]), 4),
         round(float(r["Entropy_Smooth"]), 7)]
        for _, r in sample.iterrows()
    ]

    return {
        "path":            path,
        "current":         [round(float(curr["PCA1"]), 4),
                            round(float(curr["PCA2"]), 4),
                            round(float(curr["Entropy_Smooth"]), 7)],
        "current_entropy": round(current_e, 7),
        "threshold":       round(chaos_threshold, 7),
        "status":          status,
    }

