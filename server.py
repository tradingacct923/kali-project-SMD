"""
Greek Bot â€” Flask Web Server
"""
import sys, os, json, secrets, hashlib, time
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from dotenv import load_dotenv
load_dotenv(os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env"))

from flask import Flask, jsonify, request, send_from_directory, Response, make_response
from flask_cors import CORS
from data_provider import fetch_all
from config import TICKER as _DEFAULT_TICKER

# ── Auth config (set in .env) ────────────────────────────────────────────────
_DASHBOARD_USERNAME = os.getenv("DASHBOARD_USERNAME", "Kaali4426")
_DASHBOARD_PASSWORD = os.getenv("DASHBOARD_PASSWORD", "war_machine")
_TOKEN_TTL          = 24 * 60 * 60   # 24 hours
_TOKEN_SECRET       = os.getenv("TOKEN_SECRET", "wm-greeksite-secret-key-2024")

import hmac as _hmac
import threading as _threading

def _valid_token(tok: str) -> bool:
    """Verify an HMAC-signed stateless token (works across serverless invocations)."""
    if not tok:
        return False
    try:
        parts = tok.split(".")
        if len(parts) != 3:
            return False
        ts_hex, user_hex, sig = parts
        issued = int(ts_hex, 16)
        if time.time() - issued > _TOKEN_TTL:
            return False
        # Verify signature
        expected = _hmac.new(
            _TOKEN_SECRET.encode(), f"{ts_hex}.{user_hex}".encode(), hashlib.sha256
        ).hexdigest()[:32]
        return _hmac.compare_digest(sig, expected)
    except Exception:
        return False

def _issue_token() -> str:
    """Create an HMAC-signed stateless token: timestamp.username.signature"""
    ts_hex = format(int(time.time()), 'x')
    user_hex = _DASHBOARD_USERNAME.encode().hex()
    payload = f"{ts_hex}.{user_hex}"
    sig = _hmac.new(
        _TOKEN_SECRET.encode(), payload.encode(), hashlib.sha256
    ).hexdigest()[:32]
    return f"{payload}.{sig}"

def _revoke_token(tok: str):
    pass  # Stateless tokens can't be revoked; they expire naturally

CONFIG_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "config.json")

_DEFAULT_CONFIG = {
    "ticker": _DEFAULT_TICKER,
    "options_api_key": "",
    "options_api_provider": "simulated",
    "alpha_vantage_key": "",
    # data
    "strike_range": 30,
    "max_expirations": 3,
    "refresh_interval": 30,
    "risk_free_rate": 0.045,
    "dividend_yield": 0.005,
    # visual
    "heatmap_pos_color": "#2ecc8a",
    "heatmap_neg_color": "#e8435a",
    "heatmap_neutral_color": "#2a2f45",
    # layout
    "panels_visible": {
        "gex": True, "dex": True, "vex": True, "tex": True,
        "vannex": True, "cex": True, "oi": True, "max_pain": True,
    },
}

def load_config():
    if os.path.exists(CONFIG_PATH):
        try:
            with open(CONFIG_PATH) as f:
                c = json.load(f)
            # Merge with defaults so new keys always exist
            merged = {**_DEFAULT_CONFIG, **c}
            return merged
        except Exception:
            pass
    return dict(_DEFAULT_CONFIG)

def save_config(data: dict):
    merged = {**load_config(), **data}
    with open(CONFIG_PATH, "w") as f:
        json.dump(merged, f, indent=2)
    return merged

def get_ticker():
    return load_config().get("ticker", _DEFAULT_TICKER).upper()

# ── Thundering-herd guard for fetch_all ──────────────────────────────────────
# When the cache is cold and N threads hit /api/data simultaneously, only one
# actually calls fetch_all(); the rest wait on the Event and reuse the result.
_fetch_locks: dict = {}          # ticker → threading.Event
_fetch_results: dict = {}        # ticker → (data, timestamp)
_fetch_meta_lock = _threading.Lock()
_FETCH_TTL = 28                  # seconds — slightly under the 30s refresh interval

def _cached_fetch_all(ticker: str):
    """Thread-safe, single-flight wrapper around fetch_all."""
    now = time.time()
    # Fast path: valid cached result
    with _fetch_meta_lock:
        cached = _fetch_results.get(ticker)
        if cached and now - cached[1] < _FETCH_TTL:
            return cached[0]
        # Slow path: are we already fetching?
        evt = _fetch_locks.get(ticker)
        if evt is None:
            # We are the leader — create the Event and start fetching
            evt = _threading.Event()
            _fetch_locks[ticker] = evt
            leader = True
        else:
            leader = False

    if leader:
        try:
            data = fetch_all(ticker)
            with _fetch_meta_lock:
                _fetch_results[ticker] = (data, time.time())
                del _fetch_locks[ticker]
            evt.set()           # wake all waiters
            return data
        except Exception:
            with _fetch_meta_lock:
                _fetch_locks.pop(ticker, None)
            evt.set()
            raise
    else:
        evt.wait(timeout=30)   # wait for leader, max 30s
        with _fetch_meta_lock:
            cached = _fetch_results.get(ticker)
        return cached[0] if cached else fetch_all(ticker)

app = Flask(__name__, static_folder="web", static_url_path="")
CORS(app)  # Open CORS — user will configure domain later

# Build version = git short hash (falls back to file mtime)
def _build_ver():
    try:
        import subprocess
        return subprocess.check_output(
            ["git", "rev-parse", "--short", "HEAD"],
            cwd=os.path.dirname(os.path.abspath(__file__)),
            stderr=subprocess.DEVNULL
        ).decode().strip()
    except Exception:
        return str(int(os.path.getmtime(
            os.path.join(os.path.dirname(os.path.abspath(__file__)), "web", "app.js")
        )))

_BUILD_VER = _build_ver()


# ── Auth middleware ────────────────────────────────────────────────────────────
_PUBLIC_PATHS = {"/login", "/api/login", "/login.css", "/style.css", "/app.js",
                 "/login.html", "/index.html"}
_PUBLIC_PREFIXES = ("/web/",)
_PUBLIC_EXTENSIONS = ('.css', '.js', '.html', '.ico', '.png', '.jpg', '.svg',
                      '.woff', '.woff2', '.ttf', '.map')

@app.before_request
def require_auth():
    path = request.path

    # Always allow static assets (CSS, JS, images, fonts)
    if path in _PUBLIC_PATHS or path.startswith(_PUBLIC_PREFIXES):
        return None
    if any(path.endswith(ext) for ext in _PUBLIC_EXTENSIONS):
        return None

    # Block server source files
    blocked_exts  = ('.py', '.pyc', '.env', '.git', '.bat', '.sh')
    blocked_paths = ('/.git', '/.env', '/config.json', '/config.py',
                     '/data_provider.py', '/server.py', '/macro_provider.py',
                     '/Procfile', '/requirements.txt', '/__pycache__')
    if any(path.endswith(e) for e in blocked_exts) or \
       any(path.startswith(b) for b in blocked_paths):
        return jsonify({"error": "Forbidden"}), 403

    # Check token — cookie (browser nav) OR header (API calls)
    tok = (request.cookies.get("wm_auth")
           or request.headers.get("X-Auth-Token")
           or request.args.get("token", ""))
    if not _valid_token(tok):
        if path.startswith("/api/"):
            return jsonify({"error": "Unauthorized"}), 401
        resp = Response(status=302, headers={"Location": "/login"})
        resp.delete_cookie("wm_auth")
        return resp
    return None

@app.after_request
def add_security_headers(response):
    response.headers['X-Content-Type-Options'] = 'nosniff'
    response.headers['X-Frame-Options'] = 'SAMEORIGIN'
    response.headers['Cache-Control'] = 'no-store, no-cache, must-revalidate, max-age=0'
    return response

# ── Login page ────────────────────────────────────────────────────────────────
@app.route("/login")
def login_page():
    html_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "web", "login.html")
    with open(html_path, "r", encoding="utf-8") as f:
        return Response(f.read(), mimetype="text/html")

@app.route("/api/login", methods=["POST"])
def api_login():
    data = request.get_json(force=True, silent=True) or {}
    username = (data.get("email", "") or data.get("username", "")).strip().lower()
    password = (data.get("password", "") or "")

    ok_user = username == _DASHBOARD_USERNAME.strip().lower()
    ok_pass = secrets.compare_digest(password, _DASHBOARD_PASSWORD)

    if ok_user and ok_pass:
        tok = _issue_token()
        resp = jsonify({"token": tok, "expires_in": _TOKEN_TTL})
        resp.set_cookie("wm_auth", tok, max_age=_TOKEN_TTL, httponly=True, samesite="Lax")
        return resp
    return jsonify({"error": "Invalid credentials"}), 401

@app.route("/api/logout", methods=["POST"])
def api_logout():
    tok = request.cookies.get("wm_auth") or request.headers.get("X-Auth-Token", "")
    _revoke_token(tok)
    resp = jsonify({"ok": True})
    resp.delete_cookie("wm_auth")
    return resp

# ── Dashboard ────────────────────────────────────────────────────────────────
@app.route("/")
def index():
    html_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "web", "index.html")
    with open(html_path, "r", encoding="utf-8") as f:
        content = f.read()
    content = content.replace("BUILD_VERSION", _BUILD_VER)
    return Response(content, mimetype="text/html")

# â”€â”€ Settings API â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
@app.route("/api/settings", methods=["GET"])
def api_settings_get():
    cfg = load_config()
    def mask(k):
        return ("*" * (len(k) - 4) + k[-4:]) if len(k) > 4 else ("*" * len(k))
    pv = cfg.get("panels_visible", _DEFAULT_CONFIG["panels_visible"])
    return jsonify({
        # API keys (masked)
        "ticker":               cfg["ticker"],
        "options_api_provider": cfg["options_api_provider"],
        "options_api_key":      mask(cfg["options_api_key"]) if cfg["options_api_key"] else "",
        "alpha_vantage_key":    mask(cfg["alpha_vantage_key"]) if cfg["alpha_vantage_key"] else "",
        "options_api_key_set":  bool(cfg["options_api_key"]),
        "alpha_vantage_key_set":bool(cfg["alpha_vantage_key"]),
        # data
        "strike_range":      cfg.get("strike_range", 30),
        "max_expirations":   cfg.get("max_expirations", 3),
        "refresh_interval":  cfg.get("refresh_interval", 30),
        "risk_free_rate":    cfg.get("risk_free_rate", 0.045),
        "dividend_yield":    cfg.get("dividend_yield", 0.005),
        # visual
        "heatmap_pos_color":     cfg.get("heatmap_pos_color", "#2ecc8a"),
        "heatmap_neg_color":     cfg.get("heatmap_neg_color", "#e8435a"),
        "heatmap_neutral_color": cfg.get("heatmap_neutral_color", "#2a2f45"),
        # layout
        "panels_visible": {
            "gex":      pv.get("gex", True),
            "dex":      pv.get("dex", True),
            "vex":      pv.get("vex", True),
            "tex":      pv.get("tex", True),
            "vannex":   pv.get("vannex", True),
            "cex":      pv.get("cex", True),
            "oi":       pv.get("oi", True),
            "max_pain": pv.get("max_pain", True),
        },
    })

@app.route("/api/test-connection", methods=["GET", "POST"])
def api_test_connection():
    import urllib.request as _ur, json as _json
    kind = request.args.get("type", "alpha_vantage")
    cfg  = load_config()
    # Allow passing the key directly in the POST body so the user can
    # test before saving (key field may not be in config.json yet)
    body_key = ""
    if request.method == "POST" or request.content_length:
        try:
            body_key = request.get_json(force=True, silent=True).get("key", "")
        except Exception:
            pass
    try:
        if kind == "alpha_vantage":
            key = body_key or cfg.get("alpha_vantage_key", "")
            if not key:
                return jsonify({"ok": False, "message": "No Alpha Vantage key configured"})
            url = f"https://www.alphavantage.co/query?function=GLOBAL_QUOTE&symbol=IBM&apikey={key}"
            with _ur.urlopen(_ur.Request(url), timeout=8) as r:
                data = _json.loads(r.read())
            if "Global Quote" in data and data["Global Quote"]:
                return jsonify({"ok": True,  "message": "✔ Alpha Vantage key is valid — connection successful"})
            elif "Note" in data:
                return jsonify({"ok": False, "message": "⚠ Rate limit hit — key is valid but API call limit reached"})
            elif "Error Message" in data:
                return jsonify({"ok": False, "message": "✖ Invalid API key"})
            else:
                return jsonify({"ok": False, "message": f"Unexpected response: {str(data)[:120]}"})

        elif kind == "options":
            provider = cfg.get("options_api_provider", "simulated")
            # Fall back to env var just like _get_token() does
            key = body_key or cfg.get("options_api_key", "") or os.getenv("TRADIER_TOKEN", "")

            if provider == "simulated":
                return jsonify({"ok": True, "message": "✔ Simulated mode — no connection needed"})

            elif provider == "yfinance":
                import yfinance as yf
                t     = yf.Ticker(cfg.get("ticker", "QQQ"))
                price = getattr(t.fast_info, "last_price", None)
                if price:
                    return jsonify({"ok": True,  "message": f"✔ yFinance connected — {cfg.get('ticker','QQQ')} @ ${price:.2f}"})
                else:
                    return jsonify({"ok": False, "message": "yFinance returned no price data"})

            elif provider == "tradier":
                if not key:
                    return jsonify({"ok": False, "message": "No Tradier token found — paste your token and click Save & Apply first, or paste it above and retry"})
                ticker = cfg.get("ticker", "QQQ")
                # Try production first, then sandbox
                for base in ("https://api.tradier.com/v1", "https://sandbox.tradier.com/v1"):
                    try:
                        url = f"{base}/markets/quotes?symbols={ticker}&greeks=false"
                        req = _ur.Request(url, headers={"Authorization": f"Bearer {key}", "Accept": "application/json"})
                        with _ur.urlopen(req, timeout=8) as r:
                            data = _json.loads(r.read())
                        q = data.get("quotes", {}).get("quote", {})
                        price = q.get("last") or q.get("close") or q.get("bid")
                        env = "sandbox" if "sandbox" in base else "production"
                        if price:
                            return jsonify({"ok": True, "message": f"✔ Tradier ({env}) connected — {ticker} @ ${float(price):.2f}"})
                    except Exception:
                        continue
                return jsonify({"ok": False, "message": "✖ Tradier connection failed — check your token and ensure it has market data permissions"})

            else:
                if not key:
                    return jsonify({"ok": False, "message": f"No API key configured for {provider}"})
                return jsonify({"ok": True, "message": f"Key saved for {provider} — live validation not yet implemented"})
        else:
            return jsonify({"ok": False, "message": "Unknown connection type"})
    except Exception as e:
        return jsonify({"ok": False, "message": f"Error: {str(e)}"})

@app.route("/api/settings", methods=["POST"])
def api_settings_post():
    try:
        data = request.get_json(force=True)
        allowed = {
            "ticker", "options_api_key", "options_api_provider", "alpha_vantage_key",
            "strike_range", "max_expirations", "refresh_interval",
            "risk_free_rate", "dividend_yield",
            "heatmap_pos_color", "heatmap_neg_color", "heatmap_neutral_color",
            "panels_visible",
        }
        update = {k: v for k, v in data.items() if k in allowed}
        # Don't overwrite a real key with a masked placeholder OR an empty field
        for key_field in ("options_api_key", "alpha_vantage_key"):
            val = update.get(key_field, None)
            if val is not None and (str(val).strip() == "" or "*" in str(val)):
                del update[key_field]
        # Invalidate data cache when data-affecting settings change
        if any(k in update for k in ("strike_range", "max_expirations", "risk_free_rate", "dividend_yield", "ticker")):
            try:
                from data_provider import _cache, _cache_ts
                _cache.clear(); _cache_ts.clear()
            except Exception:
                pass
        saved = save_config(update)
        return jsonify({"ok": True, "ticker": saved["ticker"]})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 400

# â”€â”€ Helpers â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
def _build_heatmap(raw_dict):
    """Convert {strike: [{label, dte, calls_val, puts_val}]} to heatmap format.
    puts_val is already negative (e.g. DEX) so net = calls_val + puts_val."""
    if not raw_dict:
        return {"strikes": [], "expirations": [], "rows": []}
    strikes = sorted(raw_dict.keys(), reverse=True)
    exps = [{"label": e["label"], "dte": e["dte"]} for e in raw_dict[strikes[0]]]
    rows = []
    for s in strikes:
        cells = []
        for e in raw_dict[s]:
            net = e["calls_val"] + e["puts_val"]  # puts_val is already negative
            cells.append(round(net, 2))
        rows.append({"strike": s, "cells": cells})
    return {"strikes": [float(s) for s in strikes], "expirations": exps, "rows": rows}

def _build_gex_heatmap(raw_dict):
    """GEX heatmap: puts_val is POSITIVE (gamma always +ve), so net = calls - puts.
    Matches the net_gex sign convention used by the bar chart."""
    if not raw_dict:
        return {"strikes": [], "expirations": [], "rows": []}
    strikes = sorted(raw_dict.keys(), reverse=True)
    exps = [{"label": e["label"], "dte": e["dte"]} for e in raw_dict[strikes[0]]]
    rows = []
    for s in strikes:
        cells = [round(e["calls_val"] - e["puts_val"], 2) for e in raw_dict[s]]
        rows.append({"strike": s, "cells": cells})
    return {"strikes": [float(s) for s in strikes], "expirations": exps, "rows": rows}

def _build_tex_heatmap(raw_dict):
    """TEX: negative is good (dealers collecting theta)."""
    if not raw_dict:
        return {"strikes": [], "expirations": [], "rows": []}
    strikes = sorted(raw_dict.keys(), reverse=True)
    exps = [{"label": e["label"], "dte": e["dte"]} for e in raw_dict[strikes[0]]]
    rows = []
    for s in strikes:
        cells = [round(-(e["calls_val"] + e["puts_val"]), 2) for e in raw_dict[s]]
        rows.append({"strike": s, "cells": cells})
    return {"strikes": [float(s) for s in strikes], "expirations": exps, "rows": rows}

def _build_oi_heatmap(raw_dict):
    """OI: separate calls and puts per cell."""
    if not raw_dict:
        return {"strikes": [], "expirations": [], "call_rows": [], "put_rows": []}
    strikes = sorted(raw_dict.keys(), reverse=True)
    exps = [{"label": e["label"], "dte": e["dte"]} for e in raw_dict[strikes[0]]]
    call_rows, put_rows = [], []
    for s in strikes:
        calls = [int(e["calls_val"]) for e in raw_dict[s]]
        puts  = [int(e["puts_val"])  for e in raw_dict[s]]
        call_rows.append({"strike": s, "cells": calls})
        put_rows.append({"strike": s, "cells": puts})
    return {"strikes": [float(s) for s in strikes], "expirations": exps,
            "call_rows": call_rows, "put_rows": put_rows}

def _build_net_bar(raw_dict):
    """Sum all expirations per strike → net exposure bar dict.
    calls_val and puts_val are already signed (puts_val is negative),
    so net = calls_val + puts_val."""
    bar = {}
    for s, exps in raw_dict.items():
        bar[str(s)] = round(sum(e["calls_val"] + e["puts_val"] for e in exps), 2)
    return bar

def _build_vex_bar(raw_dict):
    """VEX: puts_val is positive (vega is always +ve for both calls & puts).
    Net = calls_val - puts_val → positive at call-heavy strikes, negative at put-heavy."""
    bar = {}
    for s, exps in raw_dict.items():
        bar[str(s)] = round(sum(e["calls_val"] - e["puts_val"] for e in exps), 2)
    return bar

# ── Data Endpoints ────────────────────────────────────────────────────────────
@app.route("/api/candles")
def api_candles():
    """
    OHLCV candles for the current ticker.
    Query params:
      ?days=1   (1 = today, 3, 5, 10 — default 1)
      ?interval=5 (minutes — default 5)
    Also returns key levels: put_wall, call_wall, max_pain, spot.
    """
    try:
        from data_provider import _tradier_timesales, fetch_all
        from datetime import datetime, timedelta
        import numpy as np
        import pandas as _pd

        ticker = get_ticker()
        days   = min(int(request.args.get("days", 1)), 10)
        _now   = datetime.today()
        _start = _now - timedelta(days=days + 3)   # extra buffer for weekends

        df = _tradier_timesales(ticker, _start, _now)
        if df.empty:
            return jsonify({"error": "No data"}), 500

        # Filter to requested trading days
        unique_days = sorted(df.index.normalize().unique())[-days:]
        if len(unique_days) == 0:
            return jsonify({"error": "Not enough trading days"}), 500
        cutoff = unique_days[0]
        if df.index.tz:
            cutoff = _pd.Timestamp(cutoff).tz_localize(df.index.tz)
        df = df[df.index >= cutoff]

        candles = []
        delta_by_price = {}   # price_level -> net delta (approx)

        for t, row in df.iterrows():
            o  = float(row["Open"])
            h  = float(row["High"])
            lo = float(row["Low"])
            c  = float(row["Close"])
            v  = int(row.get("Volume", 0))
            hl = h - lo if h != lo else 0.0001

            # Approximate buy/sell volume (tick-rule proxy)
            buy_frac  = (c - lo) / hl
            sell_frac = (h - c)  / hl
            buy_vol   = v * buy_frac
            sell_vol  = v * sell_frac
            delta     = int(buy_vol - sell_vol)

            candles.append({
                "t":   t.strftime("%Y-%m-%dT%H:%M:%S"),
                "o":   round(o,  2),
                "h":   round(h,  2),
                "l":   round(lo, 2),
                "c":   round(c,  2),
                "v":   v,
                "d":   delta,           # per-bar delta
            })

            # Accumulate delta by price level (rounded to 0.5 ticks)
            price_lvl = round(round(c * 2) / 2, 1)
            delta_by_price[price_lvl] = delta_by_price.get(price_lvl, 0) + delta

        # Build delta profile sorted by price
        delta_profile = sorted(
            [{"price": p, "delta": d} for p, d in delta_by_price.items()],
            key=lambda x: x["price"]
        )

        # EMA-20 overlay
        closes = df["Close"]
        ema20  = closes.ewm(span=20, adjust=False).mean()
        ema50  = closes.ewm(span=50, adjust=False).mean()
        emas   = [{"t": t.strftime("%Y-%m-%dT%H:%M:%S"), "e20": round(float(v), 2),
                   "e50": round(float(w), 2)}
                  for t, v, w in zip(df.index, ema20, ema50)]

        # Key levels from cached options data
        try:
            data = _cached_fetch_all(ticker)
            levels = {
                "spot":       round(float(data["spot"]), 2),
                "call_wall":  round(float(data["gex"]["call_wall"]), 2),
                "put_wall":   round(float(data["gex"]["put_wall"]),  2),
                "max_pain":   round(float(data["max_pain"]["max_pain_strike"]), 2),
            }
        except Exception:
            levels = {}

        return jsonify({
            "ticker":  ticker,
            "candles": candles,
            "emas":    emas,
            "levels":        levels,
            "delta_profile": delta_profile,
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/api/spot")
def api_spot():
    """Lightweight quote-only endpoint — single Tradier call, no options processing."""
    try:
        from data_provider import _fetch_quote, _cached
        ticker = get_ticker()
        # Bypass cache so we always get the freshest price
        spot = _fetch_quote(ticker)
        return jsonify({"ticker": ticker, "spot": round(float(spot), 2)})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/api/data")
def api_data():
    ticker = get_ticker()
    data = _cached_fetch_all(ticker)
    if data is None:
        return jsonify({"error": "Data fetch failed"}), 500

    oi_bar = {}
    for strike, exps in data["oi"].items():
        oi_bar[str(strike)] = {
            "calls": sum(e["calls_val"] for e in exps),
            "puts":  sum(e["puts_val"]  for e in exps),
        }

    result = {
        "ticker":    data["ticker"],
        "spot":      data["spot"],
        "timestamp": data["timestamp"].isoformat(),
        "call_wall":  data["gex"]["call_wall"],
        "put_wall":   data["gex"]["put_wall"],
        "major_wall": data["gex"]["major_wall"],
        "max_pain":   data["max_pain"]["max_pain_strike"],
        # OI
        "oi_bar":    oi_bar,
        "oi_hm":     _build_oi_heatmap(data["oi"]),
        # Strike bar charts
        "gex_bar":   {str(s): round(v, 2) for s, v in data["gex"]["net_gex"].items()},
        "dex_bar":   _build_net_bar(data["dex"]),
        "vex_bar":   _build_vex_bar(data["vex"]),
        "rex_bar":   _build_vex_bar(data["rex"]),
        # Heatmaps
        "gex_hm":    _build_gex_heatmap(data["gex"]["per_exp"]),
        "dex_hm":    _build_heatmap(data["dex"]),
        "vex_hm":    _build_heatmap(data["vex"]),
        "tex_hm":    _build_tex_heatmap(data["tex"]),
        "vannex_hm": _build_heatmap(data["vannex"]),
        "cex_hm":    _build_tex_heatmap(data["cex"]),
    }
    return jsonify(result)

@app.route("/api/vol_skew_multi")
def api_vol_skew_multi():
    """Return IV by strike for multiple expirations for the smile diagram."""
    try:
        from data_provider import calculate_iv_surface
        ticker = get_ticker()
        surf = calculate_iv_surface(ticker=ticker)
        strikes = surf["strikes"]
        result = []
        for row in surf["surface"]:
            pairs = []
            for s, iv in zip(strikes, row["ivs"]):
                if iv is not None and iv > 0:
                    pairs.append({"strike": s, "iv": round(iv * 100, 2)})
            result.append({"label": row["label"], "dte": row["dte"], "data": pairs})
        return jsonify({"strikes": strikes, "expirations": result})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/api/charm_overlay")
def api_charm_overlay():
    """Charm + Vanna gradient data + today's intraday OHLC for overlay chart."""
    try:
        from data_provider import _tradier_timesales
        from datetime import datetime, timedelta
        import numpy as np

        ticker = get_ticker()
        data = fetch_all(ticker)
        spot  = data["spot"]

        # ── Build net exposure per strike ─────────────────────────────────
        def net_by_strike(raw):
            result = {}
            for s, exps in raw.items():
                net = sum(e["calls_val"] + e["puts_val"] for e in exps)
                result[round(float(s), 1)] = round(net, 4)
            return result

        charm_map = net_by_strike(data["cex"])
        vanna_map = net_by_strike(data["vannex"])

        strikes = sorted(set(charm_map) | set(vanna_map))

        # ── Today's real-time 5-min OHLC from Tradier ────────────────────
        _now   = datetime.today()
        _start = _now.replace(hour=9, minute=0, second=0, microsecond=0)
        hist = _tradier_timesales(ticker, _start, _now)
        ohlc = []
        for t, row in hist.iterrows():
            ohlc.append({
                "time":  t.strftime("%Y-%m-%dT%H:%M:%S"),
                "open":  round(float(row["Open"]),  2),
                "high":  round(float(row["High"]),  2),
                "low":   round(float(row["Low"]),   2),
                "close": round(float(row["Close"]), 2),
            })

        return jsonify({
            "spot":       spot,
            "strikes":    strikes,
            "charm_vals": [charm_map.get(s, 0) for s in strikes],
            "vanna_vals": [vanna_map.get(s, 0) for s in strikes],
            "ohlc":       ohlc,
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/api/topology")
def api_topology():
    try:
        from data_provider import build_topology
        ticker = get_ticker()
        return jsonify(build_topology(ticker))
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/api/entropy")
def api_entropy():
    try:
        from data_provider import build_entropy
        ticker = get_ticker()
        return jsonify(build_entropy(ticker))
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/api/hiro")
def api_hiro():
    """
    HIRO = cumulative intraday MM delta-hedging flow.
    HIRO(t) = total_net_GEX  ×  (price(t) − open_price)
    Positive → MMs bought (price-supportive), Negative → MMs sold (price-drag).
    """
    try:
        from data_provider import _tradier_timesales
        from datetime import datetime
        ticker  = get_ticker()
        data    = fetch_all(ticker)

        # Total net dollar-gamma per $1 move in spot
        net_gex = data["gex"]["net_gex"]          # {strike: net $ per point}
        total_gex = sum(net_gex.values())          # $ per $1 price move

        # Real-time 5-min intraday bars from Tradier
        _now   = datetime.today()
        _start = _now.replace(hour=9, minute=0, second=0, microsecond=0)
        hist = _tradier_timesales(ticker, _start, _now)
        if hist.empty:
            return jsonify({
                "ticker": ticker,
                "series": [],
                "total_gex_m": round(total_gex / 1e6, 2),
                "current_hiro_m": 0,
                "direction": "MARKET CLOSED",
                "open_price": 0,
            })

        open_price = float(hist.iloc[0]["Open"])
        series = []
        for t, row in hist.iterrows():
            close    = float(row["Close"])
            delta_s  = close - open_price
            hiro_m   = round(total_gex * delta_s / 1e6, 3)   # in $M
            series.append({
                "time":  t.strftime("%Y-%m-%dT%H:%M:%S"),
                "price": round(close, 2),
                "hiro":  hiro_m,
            })

        current_hiro = series[-1]["hiro"] if series else 0
        direction = ("BUY PRESSURE" if current_hiro > 0
                     else "SELL PRESSURE" if current_hiro < 0
                     else "NEUTRAL")

        return jsonify({
            "ticker":         ticker,
            "series":         series,
            "total_gex_m":    round(total_gex / 1e6, 2),
            "current_hiro_m": current_hiro,
            "direction":      direction,
            "open_price":     round(open_price, 2),
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/api/oi365")
def api_oi365():
    try:
        from data_provider import build_oi365
        ticker = get_ticker()
        data = build_oi365(ticker)
        return jsonify(data)
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/api/anomalies")
def api_anomalies():
    try:
        from data_provider import _tradier_timesales
        from datetime import datetime, timedelta
        import numpy as np
        ticker = get_ticker()
        _now   = datetime.today()
        _start = _now - timedelta(days=7)   # covers 5 trading days
        df = _tradier_timesales(ticker, _start, _now)
        if df.empty:
            return jsonify({"error": "No data"}), 500
        df["Log_Return"] = np.log(df["Close"] / df["Close"].shift(1))
        w = 20
        df["Mean"]    = df["Log_Return"].rolling(window=w).mean()
        df["Std"]     = df["Log_Return"].rolling(window=w).std()
        df["Z_Score"] = (df["Log_Return"] - df["Mean"]) / df["Std"]
        df = df.dropna()
        threshold = 2.09
        return jsonify({
            "ticker":     ticker,
            "threshold":  threshold,
            "times":      [str(t) for t in df.index],
            "log_returns":[float(x) for x in df["Log_Return"]],
            "z_scores":   [float(x) for x in df["Z_Score"]],
            "anomalies_up":   [{"time": str(t), "val": float(v)}
                               for t, v, z in zip(df.index, df["Log_Return"], df["Z_Score"]) if z >  threshold],
            "anomalies_down": [{"time": str(t), "val": float(v)}
                               for t, v, z in zip(df.index, df["Log_Return"], df["Z_Score"]) if z < -threshold],
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/api/regime_score")
def api_regime_score():
    """ATR-normalised Regime Score: (Close-EMA50)/ATR50*20. >25=bullish, <-25=bearish."""
    try:
        from data_provider import _tradier_timesales
        from datetime import datetime, timedelta
        import numpy as np
        import pandas as _pd

        ticker = get_ticker()
        _now   = datetime.today()
        df = _tradier_timesales(ticker, _now - timedelta(days=14), _now)
        if df.empty:
            return jsonify({"error": "No intraday data"}), 500

        window = 50
        close  = df["Close"]
        ema    = close.ewm(span=window, adjust=False).mean()

        prev_close = close.shift(1)
        tr = _pd.Series(
            np.maximum.reduce([
                (df["High"] - df["Low"]).values,
                (df["High"] - prev_close).abs().values,
                (df["Low"]  - prev_close).abs().values,
            ]),
            index=df.index
        )
        atr   = tr.rolling(window).mean()
        score = ((close - ema) / atr.replace(0, float("nan"))) * 20
        if score.dropna().empty:
            return jsonify({"error": "Not enough data"}), 500

        cutoff = _now - timedelta(days=4)   # 4 days to cover weekends
        cutoff_ts = _pd.Timestamp(cutoff).tz_localize(df.index.tz) if df.index.tz else _pd.Timestamp(cutoff)
        idx = df.index[df.index >= cutoff_ts]

        def _ser(s):
            s2 = s.reindex(idx)
            return [round(float(v), 4) if v == v else None for v in s2]

        sc_vals = score.reindex(idx)
        if sc_vals.dropna().empty:
            return jsonify({"error": "Not enough recent data for regime score"}), 500

        sc_prev = sc_vals.shift(1)
        buys, sells = [], []
        for t, sv, pv in zip(idx, sc_vals.values, sc_prev.values):
            if sv != sv or pv != pv:
                continue
            if sv > 25 and pv <= 25:
                buys.append(str(t))
            elif sv < -25 and pv >= -25:
                sells.append(str(t))

        valid_scores = sc_vals.dropna()
        cur = float(valid_scores.iloc[-1]) if len(valid_scores) > 0 else 0.0
        regime = ("DIRECTIONAL BULLISH" if cur > 25
                  else "DIRECTIONAL BEARISH" if cur < -25
                  else "BALANCE (CHOP)")

        return jsonify({
            "ticker":        ticker,
            "times":         [str(t) for t in idx],
            "prices":        _ser(close),
            "emas":          _ser(ema),
            "scores":        _ser(score),
            "buy_times":     buys,
            "sell_times":    sells,
            "current_score": round(cur, 2),
            "regime":        regime,
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/api/volatility")
def api_volatility():
    try:
        from data_provider import calculate_iv_surface
        ticker = get_ticker()
        iv_data = calculate_iv_surface(spot=None, ticker=ticker)
        return jsonify({
            "spot": iv_data.get("spot", 0),
            "strikes": iv_data["strikes"],
            "expirations": iv_data["expirations"],
            "surface": [{"label": s["label"], "dte": s["dte"], "ivs": s["ivs"]}
                        for s in iv_data["surface"]],
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/api/vol_stats")
def api_vol_stats():
    try:
        from data_provider import _tradier_history, _tradier_get
        from datetime import datetime, timedelta
        import numpy as np
        from data_provider import calculate_iv_surface

        ticker = get_ticker()

        # ── Real-time 1-year daily history from Tradier ──────────────────────
        _now   = datetime.today()
        _start = _now - timedelta(days=365)
        hist   = _tradier_history(ticker, _start, _now)
        closes = hist["Close"].dropna()

        log_ret = np.log(closes / closes.shift(1)).dropna()

        def hv(n):
            return float((log_ret.rolling(n).std().iloc[-1]) * np.sqrt(252) * 100)

        hv10 = round(hv(10), 2)
        hv20 = round(hv(20), 2)
        hv30 = round(hv(30), 2)

        # 90-day HV series for the chart — aligned to the SAME date axis.
        # Each rolling series has a different start date (HV30 needs 30 days warmup,
        # HV10 only 10), so naively using [-90:] on each gives DIFFERENT date ranges.
        # Fix: join into one DataFrame, align on the common (HV30) index, then slice.
        import pandas as pd
        hv30_series = (log_ret.rolling(30).std() * np.sqrt(252) * 100)
        hv20_series = (log_ret.rolling(20).std() * np.sqrt(252) * 100)
        hv10_series = (log_ret.rolling(10).std() * np.sqrt(252) * 100)
        hv_df = pd.DataFrame({
            "hv10": hv10_series,
            "hv20": hv20_series,
            "hv30": hv30_series,
        }).dropna()                     # drop any row where ANY series is NaN
        hv_slice     = hv_df.tail(90)  # last 90 trading days — same dates for all three
        hv_dates     = [str(d.date()) for d in hv_slice.index]
        hv10_dates   = hv_dates
        hv10_vals    = [round(float(v), 2) for v in hv_slice["hv10"]]
        hv20_dates   = hv_dates
        hv20_vals    = [round(float(v), 2) for v in hv_slice["hv20"]]
        hv30_dates   = hv_dates
        hv30_vals    = [round(float(v), 2) for v in hv_slice["hv30"]]

        # ── HV-Rank (proxy for IV Rank) ───────────────────────────────────
        # We rank ATM IV against the 1-year HV30 min/max range.
        # True IV Rank would need 1-year of historical ATM IV snapshots
        # (not available on Tradier free tier), so this is the best proxy.
        hv30_all  = (log_ret.rolling(30).std() * np.sqrt(252) * 100).dropna()
        hv_1y_max = float(hv30_all.max())
        hv_1y_min = float(hv30_all.min())

        # Current ATM IV from Tradier surface
        try:
            surf = calculate_iv_surface(spot=None, ticker=ticker)
            spot = surf.get("spot", 0)
            # Pick nearest expiry, find ATM IV (middle of strikes)
            if surf["surface"]:
                atm_row = surf["surface"][0]["ivs"]
                mid = len(atm_row) // 2
                # Average a few strikes around ATM
                atm_slice = [v for v in atm_row[max(0,mid-3):mid+4] if v and v > 0]
                atm_iv_pct = round(float(np.mean(atm_slice)) * 100, 2) if atm_slice else hv30
            else:
                atm_iv_pct = hv30
        except Exception:
            atm_iv_pct = hv30
            spot = 0

        # Rank ATM IV against the 1-year HV30 range
        # (positive = IV elevated vs recent realized vol history)
        ivr = round((atm_iv_pct - hv_1y_min) / max(hv_1y_max - hv_1y_min, 0.01) * 100, 1)
        ivr = max(0, min(100, ivr))
        vol_premium = round(atm_iv_pct - hv30, 2)

        # Regime label
        if   ivr >= 70 and vol_premium > 2: regime = "SELL VOL"
        elif ivr <= 30 and vol_premium < -2: regime = "BUY VOL"
        else:                                regime = "NEUTRAL"

        # ── VIX term structure ───────────────────────────────────────────
        def vix_from_tradier(sym):
            try:
                d = _tradier_get("/markets/quotes", {"symbols": sym, "greeks": "false"})
                q = d.get("quotes", {}).get("quote", {})
                # Handle list response (multiple quotes)
                if isinstance(q, list):
                    q = next((x for x in q if x.get("symbol") == sym), {})
                val = q.get("last") or q.get("close")
                return round(float(val), 2) if val else None
            except Exception as ex:
                print(f"[vol_stats] VIX fetch failed for {sym}: {ex}")
                return None

        vix9d  = vix_from_tradier("VIX9D")
        vix_   = vix_from_tradier("VIX")
        vix3m  = vix_from_tradier("VIX3M")

        # Term structure shape
        if vix9d is not None and vix_ is not None and vix3m is not None:
            if vix9d < vix_ < vix3m:
                ts_shape = "CONTANGO"       # normal / calm
            elif vix9d > vix_ > vix3m:
                ts_shape = "BACKWARDATION"  # stressed
            else:
                ts_shape = "MIXED"
        else:
            missing = [s for s, v in [("VIX9D", vix9d), ("VIX", vix_), ("VIX3M", vix3m)] if v is None]
            ts_shape = f"N/A (no {', '.join(missing)})"

        # ── 5-Day intraday (5-min) kurtosis ──────────────────────────────
        # Statistically stable version: winsorize returns at ±3σ first
        # (outlier raw bars are the #1 cause of spurious kurtosis spikes),
        # then use a 156-bar rolling window (≈ 2 trading days) for stability,
        # and clip output to [-5, 15] — values above ~10 just mean "very fat
        # tails"; distinguishing 12 from 50 adds no real information.
        try:
            from data_provider import _tradier_timesales
            from datetime import timedelta as _td
            _intra_start = _now - _td(days=10)
            intra_df = _tradier_timesales(ticker, _intra_start, _now)
            intra_log = np.log(intra_df["Close"] / intra_df["Close"].shift(1)).dropna()
            # Winsorize: clip returns to ±3σ to remove single-bar outliers
            _sigma = intra_log.std()
            intra_log_w = intra_log.clip(-3 * _sigma, 3 * _sigma)
            # Rolling 156-bar kurtosis (≈ 2 trading days) — more stable than 78
            roll_window = 156
            kurt_series = intra_log_w.rolling(roll_window).kurt().dropna()
            # Clip output to meaningful range: excess kurtosis rarely useful beyond ±15
            kurt_series = kurt_series.clip(-5, 15)
            kurt_dates = [str(t) for t in kurt_series.index[-400:]]
            kurt_vals  = [round(float(v), 3) for v in kurt_series.values[-400:]]
            kurt_now   = round(float(kurt_series.iloc[-1]), 2) if len(kurt_series) else 0
        except Exception:
            # Fallback to daily rolling kurtosis if intraday unavailable
            kurt30_series = log_ret.rolling(30).kurt().dropna().clip(-5, 15)
            kurt_dates = [str(d.date()) for d in kurt30_series.index[-90:]]
            kurt_vals  = [round(float(v), 3) for v in kurt30_series.values[-90:]]
            kurt_now   = round(float(kurt30_series.iloc[-1]), 2) if len(kurt30_series) else 0

        return jsonify({
            "ticker":      ticker,
            "spot":        spot,
            "hv10":        hv10,
            "hv20":        hv20,
            "hv30":        hv30,
            "atm_iv":      atm_iv_pct,
            "ivr":         ivr,
            "vol_premium": vol_premium,
            "regime":      regime,
            "vix9d":       vix9d,
            "vix":         vix_,
            "vix3m":       vix3m,
            "ts_shape":    ts_shape,
            "hv30_chart":  {"dates": hv30_dates, "values": hv30_vals},
            "hv20_chart":  {"dates": hv20_dates, "values": hv20_vals},
            "hv10_chart":  {"dates": hv10_dates, "values": hv10_vals},
            "kurt_chart":  {"dates": kurt_dates, "values": kurt_vals, "current": kurt_now},
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/api/macro")
def api_macro():
    try:
        from macro_provider import get_macro_data
        cfg = load_config()
        return jsonify(get_macro_data(get_ticker(), cfg.get("alpha_vantage_key", "")))
    except Exception as e:
        return jsonify({"error": str(e)}), 500

# ── Inference Engine API ──────────────────────────────────────────────────────
import time as _time
_inference_cache = {"data": None, "ts": 0}
_INFERENCE_TTL = 60  # seconds

# ── Level 2 Live Data API ─────────────────────────────────────────────────────
@app.route("/api/l2")
def api_l2():
    """
    Returns live Level 2 DOM signals from TopStepX.
    Served by the background_engine/l2_worker.py daemon.
    """
    import json as _json
    try:
        from background_engine.l2_worker import get_l2_state
        state = get_l2_state()
        # If L2 is disconnected, attach diagnostic info for debugging
        if not state.get("connected"):
            diag = {}
            try:
                import builtins
                if hasattr(builtins, '_l2_startup_error_holder') and builtins._l2_startup_error_holder[0]:
                    diag["startup_error"] = builtins._l2_startup_error_holder[0]
            except Exception:
                pass
            diag["has_username"] = bool(os.getenv("TOPSTEPX_USERNAME"))
            diag["has_api_key"] = bool(os.getenv("TOPSTEPX_API_KEY"))
            diag["has_password"] = bool(os.getenv("TOPSTEPX_PASSWORD"))
            diag["rest_base"] = os.getenv("TOPSTEPX_REST_BASE", "(not set)")
            diag["worker_error"] = _worker_error
            state["_diag"] = diag
        # Use json.dumps with default=str to handle any numpy types in signals
        body = _json.dumps(state, default=str)
        return make_response(body, 200, {"Content-Type": "application/json"})
    except Exception as e:
        # Include diagnostic from worker thread if available
        diag = ""
        try:
            import builtins
            if hasattr(builtins, '_l2_startup_error_holder') and builtins._l2_startup_error_holder[0]:
                diag = builtins._l2_startup_error_holder[0]
        except Exception:
            pass
        return jsonify({"connected": False, "error": str(e), "startup_error": diag}), 200

@app.route("/api/l2/diag")
def api_l2_diag():
    """Diagnostic: test TopStepX auth + connectivity from this server."""
    import os, requests as _req, traceback as _tb
    results = {"tests": {}}
    username = os.getenv("TOPSTEPX_USERNAME", "")
    api_key = os.getenv("TOPSTEPX_API_KEY", "")
    rest_base = os.getenv("TOPSTEPX_REST_BASE", "https://api.topstepx.com")
    results["env"] = {
        "username_set": bool(username),
        "api_key_set": bool(api_key),
        "api_key_len": len(api_key),
        "rest_base": rest_base,
    }
    # Test 1: Auth
    try:
        resp = _req.post(f"{rest_base}/api/Auth/loginKey",
                         json={"userName": username, "apiKey": api_key}, timeout=10)
        data = resp.json()
        results["tests"]["auth"] = {
            "status": resp.status_code,
            "success": data.get("success"),
            "error": data.get("errorMessage", ""),
            "token_len": len(data.get("token", "")) if data.get("token") else 0,
        }
        token = data.get("token", "")
    except Exception as e:
        results["tests"]["auth"] = {"error": str(e), "traceback": _tb.format_exc()}
        token = ""
    # Test 2: Contract search (if auth worked)
    if token:
        try:
            resp = _req.post(f"{rest_base}/api/Contract/search",
                             headers={"Authorization": f"Bearer {token}",
                                      "Content-Type": "application/json"},
                             json={"searchText": "NQ", "live": False}, timeout=10)
            cdata = resp.json()
            contracts = cdata.get("contracts", [])
            results["tests"]["contract_search"] = {
                "status": resp.status_code,
                "count": len(contracts),
                "first": contracts[0] if contracts else None,
            }
        except Exception as e:
            results["tests"]["contract_search"] = {"error": str(e)}
    # Test 3: Worker thread error
    try:
        import builtins
        if hasattr(builtins, '_l2_startup_error_holder'):
            results["tests"]["worker_error"] = builtins._l2_startup_error_holder[0]
    except Exception:
        pass
    # Test 4: WebSocket connectivity
    try:
        import websocket as _ws_test
        results["tests"]["websocket_import"] = {"ok": True, "version": getattr(_ws_test, '__version__', 'unknown')}
        # Quick connect test (3s timeout)
        if token:
            hub = os.getenv("TOPSTEPX_MARKET_HUB", "https://rtc.topstepx.com/hubs/market")
            ws_url = hub.replace("https://", "wss://").replace("http://", "ws://")
            ws_url = f"{ws_url}?access_token={token}"
            try:
                ws = _ws_test.create_connection(ws_url, timeout=5)
                # Send SignalR handshake
                ws.send('{"protocol":"json","version":1}\x1e')
                resp = ws.recv()
                ws.close()
                results["tests"]["websocket_connect"] = {"ok": True, "handshake_response": resp[:200]}
            except Exception as e:
                results["tests"]["websocket_connect"] = {"ok": False, "error": str(e)}
    except ImportError as e:
        results["tests"]["websocket_import"] = {"ok": False, "error": str(e)}
    # Test 5: Current L2 state
    try:
        from background_engine.l2_worker import get_l2_state, L2_STATE
        results["tests"]["l2_state"] = {
            "connected": L2_STATE.get("connected"),
            "last_update": L2_STATE.get("last_update"),
            "dom_symbols": list(L2_STATE.get("dom", {}).keys()),
            "quote_symbols": list(L2_STATE.get("quotes", {}).keys()),
        }
    except Exception as e:
        results["tests"]["l2_state"] = {"error": str(e)}
    return jsonify(results)

@app.route("/api/inference")
def api_inference():
    """Return signals from all 8 alpha frameworks (synthetic demo data)."""
    # Serve from cache if fresh
    if _inference_cache["data"] and (_time.time() - _inference_cache["ts"]) < _INFERENCE_TTL:
        return jsonify(_inference_cache["data"])
    try:
        import numpy as np
        signals = []

        # 1. Transfer Entropy
        try:
            from frameworks.transfer_entropy import TransferEntropy
            te = TransferEntropy(window_size=60, lag=1, n_bins=5)
            vix = 20 + np.cumsum(np.random.normal(0, 0.5, 100))
            nq = 15000 + np.cumsum(np.random.normal(0, 50, 100))
            for i in range(100):
                te.update(float(vix[i]), float(nq[i]))
            signals.append(te.get_signal())
        except Exception as e:
            signals.append({"name": "transfer_entropy", "value": 0, "alert_level": "error", "interpretation": str(e)})

        # 2. Shannon Entropy
        try:
            from frameworks.shannon_entropy import ShannonEntropy
            se = ShannonEntropy(window_size=60)
            for i in range(100):
                se.update(float(np.random.uniform(-1, 1)))
            sig = se.get_signal()
            sig["name"] = sig.pop("framework", "shannon_entropy")
            if "alert_level" not in sig:
                sig["alert_level"] = sig.get("regime", "unknown")
            signals.append(sig)
        except Exception as e:
            signals.append({"name": "shannon_entropy", "value": 0, "alert_level": "error", "interpretation": str(e)})

        # 3. Ising Magnetization
        try:
            from frameworks.ising_magnetization import IsingMagnetization
            ising = IsingMagnetization(window_size=60)
            for i in range(100):
                for sym in ["NQ", "ES", "YM", "RTY"]:
                    ising.update_trade(sym, int(np.random.choice([-1, 1])))
            sig = ising.get_signal()
            sig["name"] = sig.pop("framework", "ising_magnetization")
            if "alert_level" not in sig:
                sig["alert_level"] = sig.get("regime", "unknown")
            signals.append(sig)
        except Exception as e:
            signals.append({"name": "ising_magnetization", "value": 0, "alert_level": "error", "interpretation": str(e)})

        # 4. Mutual Information
        try:
            from frameworks.mutual_information import MutualInformation
            mi = MutualInformation(window_size=100, n_bins=5)
            for i in range(100):
                mi.update(float(np.random.randn() * 1e9), float(np.random.randn() * 0.01))
            sig = mi.get_signal()
            sig["name"] = sig.pop("framework", "mutual_information")
            if "alert_level" not in sig:
                sig["alert_level"] = sig.get("regime", "unknown")
            signals.append(sig)
        except Exception as e:
            signals.append({"name": "mutual_information", "value": 0, "alert_level": "error", "interpretation": str(e)})

        # 5. Reynolds Number
        try:
            from frameworks.reynolds_number import ReynoldsNumber
            rn = ReynoldsNumber(window_size=60)
            price = 20000.0
            for i in range(100):
                price += np.random.randn() * 10
                rn.update(price=float(price), spread=float(0.25 + np.random.rand() * 0.5), volume=float(10 + np.random.rand() * 50))
            sig = rn.get_signal()
            sig["name"] = sig.pop("framework", "reynolds_number")
            if "alert_level" not in sig:
                sig["alert_level"] = sig.get("regime", "unknown")
            signals.append(sig)
        except Exception as e:
            signals.append({"name": "reynolds_number", "value": 0, "alert_level": "error", "interpretation": str(e)})

        # 6. Percolation Threshold
        try:
            from frameworks.percolation_threshold import PercolationThreshold
            pt = PercolationThreshold(window_size=60)
            assets = ["NQ", "ES", "YM", "RTY", "VIX", "ZN", "GC"]
            base = {a: 1000.0 for a in assets}
            for i in range(100):
                for a in assets:
                    base[a] += np.random.randn() * 10
                pt.update({a: float(base[a]) for a in assets})
            sig = pt.get_signal()
            sig["name"] = sig.pop("framework", "percolation_threshold")
            if "alert_level" not in sig:
                sig["alert_level"] = sig.get("regime", "unknown")
            signals.append(sig)
        except Exception as e:
            signals.append({"name": "percolation_threshold", "value": 0, "alert_level": "error", "interpretation": str(e)})

        # 7. LPPL Sornette (instant mock for dashboard demo)
        try:
            from datetime import datetime, timedelta
            tc_date = datetime.now() + timedelta(days=21)
            sig = {
                "name": "lppl_sornette",
                "value": 0.995,  # R^2
                "alert_level": "watch",
                "interpretation": f"Bubble signature detected → tc ~{tc_date.strftime('%b %d')} ({21}d). R²=0.995",
                "critical_date": tc_date.strftime('%Y-%m-%d'),
                "days_to_tc": 21,
                "is_bubble": True,
                "r_squared": 0.9953,
                "confidence": 0.72
            }
            signals.append(sig)
        except Exception as e:
            signals.append({"name": "lppl_sornette", "value": 0, "alert_level": "error", "interpretation": str(e)})

        # 8. Power-Law Tail (instant mock for dashboard demo)
        try:
            sig = {
                "name": "powerlaw_tail",
                "value": 4.66,
                "alert_level": "stable",
                "interpretation": "Thin tails: α=4.66 — unusually calm. Consider selling vol. Trend: stable",
                "alpha": 4.66,
                "alpha_left": 4.12,
                "alpha_right": 4.97,
                "regime": "THIN_CALM",
                "tail_trend": "stable"
            }
            signals.append(sig)
        except Exception as e:
            signals.append({"name": "powerlaw_tail", "value": 0, "alert_level": "error", "interpretation": str(e)})

        # Sanitize numpy types for JSON serialization
        def _sanitize(obj):
            if isinstance(obj, dict):
                return {k: _sanitize(v) for k, v in obj.items()}
            if isinstance(obj, list):
                return [_sanitize(v) for v in obj]
            if isinstance(obj, (np.bool_,)):
                return bool(obj)
            if isinstance(obj, (np.integer,)):
                return int(obj)
            if isinstance(obj, (np.floating,)):
                return float(obj)
            if isinstance(obj, np.ndarray):
                return obj.tolist()
            return obj

        signals = [_sanitize(s) for s in signals]
        result = {"signals": signals, "count": len(signals)}
        _inference_cache["data"] = result
        _inference_cache["ts"] = _time.time()
        return jsonify(result)
    except Exception as e:
        return jsonify({"error": str(e)}), 500

# ── Background workers (run under BOTH gunicorn and direct execution) ─────────
import threading as _startup_threading

_workers_started = False

def _start_workers():
    global _workers_started
    if _workers_started:
        return
    _workers_started = True

    # Pre-warm options cache
    def _prewarm():
        try:
            from data_provider import fetch_all
            ticker = get_ticker()
            print(f"[startup] Pre-warming cache for {ticker}...")
            fetch_all(ticker)
            print("[startup] Cache ready -- first load will be fast")
        except Exception as e:
            print(f"[startup] Pre-warm failed (non-fatal): {e}")
    _startup_threading.Thread(target=_prewarm, daemon=True).start()

    # Start TopStepX Level 2 background worker
    _l2_startup_error_holder = [None]   # mutable container for thread error capture
    def _start_l2():
        import time as _t
        print("[L2-THREAD] sleeping 3s for module load...", flush=True)
        _t.sleep(3)
        print("[L2-THREAD] awake, importing l2_worker...", flush=True)
        print(f"[L2-THREAD] ENV CHECK: username={bool(os.getenv('TOPSTEPX_USERNAME'))}, api_key={bool(os.getenv('TOPSTEPX_API_KEY'))}, password={bool(os.getenv('TOPSTEPX_PASSWORD'))}, rest_base={os.getenv('TOPSTEPX_REST_BASE', '(not set)')}", flush=True)
        try:
            from background_engine.l2_worker import start_l2_worker
            print("[L2-THREAD] import OK, calling start_l2_worker()...", flush=True)
            start_l2_worker()
            print("[L2-THREAD] start_l2_worker() returned OK", flush=True)
        except Exception as e:
            import traceback
            err_msg = f"{e}\n{traceback.format_exc()}"
            _l2_startup_error_holder[0] = err_msg
            print(f"[L2-THREAD] FAILED: {err_msg}", flush=True)
    _startup_threading.Thread(target=_start_l2, daemon=True).start()
    print("[startup] L2 daemon thread spawned", flush=True)

    # Store reference so /api/l2 can check it
    import builtins
    builtins._l2_startup_error_holder = _l2_startup_error_holder

# Start workers when module is loaded (works under gunicorn too)
# NOTE: _start_l2() runs in a daemon thread with lazy import, so no circular import.
# Previous Timer(2.0) approach failed because gunicorn kills master Timer threads on fork.
_worker_error = None
try:
    _start_workers()
except Exception as _e:
    _worker_error = str(_e)
    print(f"[startup] WARNING: workers failed to start: {_e}")

if __name__ == "__main__":
    print("Starting Greek Options Dashboard...")
    print("Open http://localhost:5000 in your browser")

    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port, debug=False, threaded=True)

