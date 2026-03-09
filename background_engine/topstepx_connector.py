"""
TopStepX Level 2 Connector — Real Implementation
Uses the ProjectX Gateway API (SignalR over WebSocket).

Auth:   POST https://api.topstepx.com/api/Auth/loginKey
Hub:    https://rtc.topstepx.com/hubs/market

Events received:
  GatewayDepth  (contractId, data)  — DOM updates, DomType enum
  GatewayTrade  (contractId, data)  — Tape prints (buy/sell aggressors)
  GatewayQuote  (contractId, data)  — BBO snapshot

DomType enum:
  0=Unknown,1=Ask,2=Bid,3=BestAsk,4=BestBid,5=Trade,
  6=Reset,7=Low,8=High,9=NewBestBid,10=NewBestAsk,11=Fill
"""

import json
import logging
import os
import time
import threading
import requests
from collections import defaultdict, deque
from typing import Callable, Optional
from dotenv import load_dotenv

# Load .env from project root
_HERE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
load_dotenv(os.path.join(_HERE, ".env"))

log = logging.getLogger(__name__)

# ── TopStepX API URLs (loaded from .env or defaults) ────────────────────────
REST_BASE   = os.getenv("TOPSTEPX_REST_BASE",  "https://api.topstepx.com")
MARKET_HUB  = os.getenv("TOPSTEPX_MARKET_HUB", "https://rtc.topstepx.com/hubs/market")

# ── Contract IDs for the front-month futures we care about ──────────────────
# These are searched dynamically at startup via /api/Contract/search
# so we don't hard-code expiry codes.
SYMBOL_SEARCH = {
    "NQ":  "NQ",   # E-mini Nasdaq
    "ES":  "EP",   # E-mini S&P  (ProjectX uses "EP" for ES)
    "YM":  "YM",   # E-mini Dow
    "RTY": "RTY",  # E-mini Russell
}


class DomType:
    Unknown    = 0
    Ask        = 1
    Bid        = 2
    BestAsk    = 3
    BestBid    = 4
    Trade      = 5
    Reset      = 6
    Low        = 7
    High       = 8
    NewBestBid = 9
    NewBestAsk = 10
    Fill       = 11


class TopStepXConnector:
    """
    Real Level 2 connector for TopStepX via ProjectX Gateway API.

    Usage:
        conn = TopStepXConnector(
            username="...", api_key="...",
            on_dom_update=my_dom_cb,
            on_trade=my_trade_cb,
        )
        conn.start(symbols=["NQ", "ES", "YM", "RTY"])
        ...
        conn.stop()

    Callbacks:
        on_dom_update(symbol: str, dom: dict)
            dom = {
                "bids":      {price: volume, ...},
                "asks":      {price: volume, ...},
                "best_bid":  float,
                "best_ask":  float,
                "mid_price": float,
                "spread":    float,
                "bid_total": int,
                "ask_total": int,
                "imbalance": float,   # (bids-asks)/(bids+asks) in [-1, +1]
                "timestamp": str,
            }

        on_trade(symbol: str, trade: dict)
            trade = {
                "price":     float,
                "volume":    int,
                "side":      "buy" | "sell",
                "spin":      +1 | -1,          ← for Ising model
                "timestamp": str,
            }
    """

    def __init__(
        self,
        username: str,
        api_key: str,
        on_dom_update: Callable = None,
        on_trade: Callable = None,
        on_quote: Callable = None,
    ):
        self.username      = username
        self.api_key       = api_key
        self.on_dom_update = on_dom_update
        self.on_trade      = on_trade
        self.on_quote      = on_quote

        self._token: Optional[str] = None
        self._connection = None
        self._running    = False

        # Per-contract ID → our friendly symbol name
        self._contract_to_symbol: dict[str, str] = {}
        self._symbol_to_contract: dict[str, str] = {}

        # Rolling DOM state per symbol (price → volume)
        self._dom_bids: dict[str, dict] = defaultdict(dict)
        self._dom_asks: dict[str, dict] = defaultdict(dict)
        self._dom_best_bid: dict[str, float] = {}
        self._dom_best_ask: dict[str, float] = {}

        # Public snapshots available to framework engines
        self.dom_snapshots:  dict[str, dict] = {}
        self.trade_buffer:   dict[str, deque] = defaultdict(lambda: deque(maxlen=10_000))
        self.quote_snapshot: dict[str, dict]  = {}
        self._last_depth_ts: float = 0.0   # watchdog: last GatewayDepth event time
        self._symbols_requested: list[str] = []

    # ── Authentication ───────────────────────────────────────────────────────

    def authenticate(self) -> str:
        """POST /api/Auth/loginKey → JWT token."""
        url = f"{REST_BASE}/api/Auth/loginKey"
        payload = {"userName": self.username, "apiKey": self.api_key}
        log.info("TopStepX: authenticating...")
        resp = requests.post(url, json=payload, timeout=10)
        resp.raise_for_status()
        data = resp.json()
        if not data.get("success", False):
            raise RuntimeError(f"Auth failed: {data.get('errorMessage', data)}")
        self._token = data["token"]
        log.info("TopStepX: authenticated ✓  (token %s...)", self._token[:12])
        return self._token

    # ── Contract Discovery ───────────────────────────────────────────────────

    def _search_contract(self, symbol_code: str) -> Optional[str]:
        """Search for the front-month contract ID for a symbol code."""
        url = f"{REST_BASE}/api/Contract/search"
        headers = {
            "Authorization": f"Bearer {self._token}",
            "Content-Type": "application/json",
        }
        payload = {"searchText": symbol_code, "live": False}
        try:
            resp = requests.post(url, headers=headers, json=payload, timeout=10)
            resp.raise_for_status()
            contracts = resp.json().get("contracts", [])
            if contracts:
                cid = contracts[0]["id"]
                log.info("TopStepX: %s → %s", symbol_code, cid)
                return cid
            else:
                log.warning("TopStepX: no contracts returned for %s", symbol_code)
        except Exception as e:
            log.warning("TopStepX: contract search failed for %s: %s", symbol_code, e)
        return None

    def _resolve_contracts(self, symbols: list[str]) -> list[str]:
        """Map human symbols ('NQ', 'ES', ...) → ProjectX contract IDs."""
        contract_ids = []
        for sym in symbols:
            pxcode = SYMBOL_SEARCH.get(sym, sym)
            cid = self._search_contract(pxcode)
            if cid:
                self._contract_to_symbol[cid] = sym
                self._symbol_to_contract[sym] = cid
                contract_ids.append(cid)
            else:
                log.warning("TopStepX: could not find contract for %s — skipping", sym)
        return contract_ids

    def retrieve_bars(self, contract_id: str, minutes: int = 500) -> list[dict]:
        """Fetch historical bars using POST /api/History/retrieveBars.
        Returns list of {t, o, h, l, c, v} dicts sorted oldest-first."""
        from datetime import datetime, timedelta, timezone
        url = f"{REST_BASE}/api/History/retrieveBars"
        headers = {
            "Authorization": f"Bearer {self._token}",
            "Content-Type": "application/json",
        }
        now = datetime.now(timezone.utc)
        payload = {
            "contractId": contract_id,
            "live": False,
            "startTime": (now - timedelta(minutes=minutes)).isoformat(),
            "endTime": now.isoformat(),
            "unit": 2,          # Minute
            "unitNumber": 1,    # 1-minute bars
            "limit": min(minutes, 20000),
            "includePartialBar": True,
        }
        try:
            resp = requests.post(url, headers=headers, json=payload, timeout=15)
            resp.raise_for_status()
            data = resp.json()
            bars = data.get("bars", [])
            # Sort oldest first
            bars.sort(key=lambda b: b.get("t", ""))
            log.info("TopStepX: retrieved %d bars for %s", len(bars), contract_id)
            return bars
        except Exception as e:
            log.warning("TopStepX: retrieve_bars failed: %s", e)
            return []

    def list_available_contracts(self, live: bool = False) -> list[dict]:
        """POST /api/Contract/available — lists all available contracts."""
        url = f"{REST_BASE}/api/Contract/available"
        headers = {
            "Authorization": f"Bearer {self._token}",
            "Content-Type": "application/json",
        }
        try:
            resp = requests.post(url, headers=headers, json={"live": live}, timeout=10)
            resp.raise_for_status()
            return resp.json().get("contracts", [])
        except Exception as e:
            log.warning("TopStepX: list_available_contracts failed: %s", e)
            return []

    # ── SignalR Connection ───────────────────────────────────────────────────

    def start(self, symbols: list[str] = None):
        """Connect and start streaming. Runs in a background thread."""
        if symbols is None:
            symbols = ["NQ", "ES", "YM", "RTY"]

        self.authenticate()
        contract_ids = self._resolve_contracts(symbols)
        if not contract_ids:
            raise RuntimeError("No contracts resolved — cannot stream Level 2 data.")

        self._running = True
        self._symbols_requested = symbols
        t = threading.Thread(
            target=self._run_signalr,
            args=(contract_ids,),
            daemon=True,
            name="topstepx-l2",
        )
        t.start()
        # Start watchdog
        wd = threading.Thread(target=self._watchdog, daemon=True, name="topstepx-watchdog")
        wd.start()
        log.info("TopStepX: L2 streaming thread started for %s", symbols)

    def stop(self):
        self._running = False
        if self._connection:
            try:
                self._connection.stop()
            except Exception:
                pass

    def _watchdog(self):
        """Restart connection if GatewayDepth goes silent for > 5 minutes."""
        import time as _t
        SILENCE_LIMIT = 300  # 5 minutes
        _t.sleep(60)         # give startup time
        while self._running:
            _t.sleep(30)
            silent_for = _t.time() - self._last_depth_ts
            if self._last_depth_ts > 0 and silent_for > SILENCE_LIMIT:
                log.warning(
                    "TopStepX watchdog: no DOM events for %.0fs — restarting...", silent_for
                )
                try:
                    if self._connection:
                        self._connection.stop()
                except Exception:
                    pass
                _t.sleep(3)
                # Re-auth and reconnect
                try:
                    self.authenticate()
                    contract_ids = self._resolve_contracts(self._symbols_requested)
                    if contract_ids:
                        self._last_depth_ts = _t.time()  # reset so we don't loop
                        t = threading.Thread(
                            target=self._run_signalr,
                            args=(contract_ids,),
                            daemon=True, name="topstepx-l2"
                        )
                        t.start()
                        log.info("TopStepX watchdog: reconnected.")
                except Exception as e:
                    log.error("TopStepX watchdog: reconnect failed: %s", e)

    def _run_signalr(self, contract_ids: list[str]):
        """Main SignalR loop using raw websocket — tolerates malformed JSON from TopStepX."""
        import websocket as _ws   # pip install websocket-client
        import json as _json

        hub_url = MARKET_HUB.replace("https://", "wss://").replace("http://", "ws://")
        hub_url = f"{hub_url}?access_token={self._token}"

        # SignalR handshake
        HANDSHAKE = '{"protocol":"json","version":1}\x1e'
        PING      = '{"type":6}\x1e'       # heartbeat
        SEP       = '\x1e'                 # SignalR record separator

        _subscribed = [False]

        def _send_subscriptions(ws):
            for cid in contract_ids:
                for method in ("SubscribeContractQuotes",
                               "SubscribeContractTrades",
                               "SubscribeContractMarketDepth"):
                    msg = _json.dumps({
                        "type": 1,
                        "target": method,
                        "arguments": [cid],
                    }) + SEP
                    ws.send(msg)
                log.info("TopStepX WS: subscribed to %s (%s)",
                         cid, self._contract_to_symbol.get(cid, "?"))
            _subscribed[0] = True

        def _dispatch(data: dict):
            """Route an invocation message to the right handler."""
            target = data.get("target", "")
            args   = data.get("arguments", [])
            if not args:
                return
            # GatewayDepth  → [contractId, depth_data]
            # GatewayQuote  → [contractId, quote_data]   (contractId confirmed working)
            # GatewayTrade  → [trade_data]  symbolId inside payload (per API docs)
            if target == "GatewayDepth" and len(args) >= 2:
                on_gateway_depth(args)
            elif target == "GatewayTrade":
                on_gateway_trade(args)   # handles both 1-arg and 2-arg forms
            elif target == "GatewayQuote" and len(args) >= 2:
                on_gateway_quote(args)

        def on_gateway_depth(args):
            """GatewayDepth: args = [contractId, [depth_event, ...]]
            args[1] is a LIST of depth events (batch), not a single dict."""
            try:
                contract_id = args[0]
                symbol = self._contract_to_symbol.get(contract_id)
                if not symbol:
                    return
                events = args[1]
                if isinstance(events, dict):
                    events = [events]   # handle single-event form just in case
                bids = self._dom_bids[symbol]
                asks = self._dom_asks[symbol]
                ts = ""
                for data in events:
                    dom_type = data.get("type", 0)
                    price    = float(data.get("price", 0))
                    volume   = int(data.get("volume", 0))
                    ts       = data.get("timestamp", ts) # Use the last timestamp in the batch
                    if dom_type == DomType.Reset:
                        bids.clear(); asks.clear()
                    elif dom_type in (DomType.Bid, DomType.NewBestBid, DomType.BestBid):
                        if volume == 0: bids.pop(price, None)
                        else: bids[price] = volume
                        if dom_type in (DomType.BestBid, DomType.NewBestBid):
                            self._dom_best_bid[symbol] = price
                    elif dom_type in (DomType.Ask, DomType.NewBestAsk, DomType.BestAsk):
                        if volume == 0: asks.pop(price, None)
                        else: asks[price] = volume
                        if dom_type in (DomType.BestAsk, DomType.NewBestAsk):
                            self._dom_best_ask[symbol] = price
                self._last_depth_ts = time.time()
                snap = self._build_dom_snapshot(symbol, ts)
                self.dom_snapshots[symbol] = snap
                if self.on_dom_update:
                    self.on_dom_update(symbol, snap)
            except Exception as e:
                log.debug("GatewayDepth parse error: %s", e)

        def on_gateway_trade(args):
            """GatewayTrade: args = [contractId, [trade_event, ...]]
            args[1] is a LIST of trade events (batch), not a single dict."""
            try:
                contract_id = args[0]
                symbol = self._contract_to_symbol.get(contract_id)
                if not symbol:
                    # Fallback: check symbolId inside events
                    events = args[1] if len(args) >= 2 else args[0]
                    if isinstance(events, dict): events = [events]
                    for ev in events:
                        sid = ev.get('symbolId', '')
                        for cid, sym in self._contract_to_symbol.items():
                            if sid and sym in sid:
                                symbol = sym; break
                        if symbol: break
                if not symbol:
                    return
                events = args[1] if len(args) >= 2 else args[0]
                if isinstance(events, dict):
                    events = [events]   # single-event form
                for data in events:
                    price      = float(data.get("price", 0))
                    volume     = int(data.get("volume", 0))
                    ts         = data.get("timestamp", "")
                    trade_type = int(data.get("type", 0))
                    side = "buy" if trade_type == 0 else "sell"
                    spin = 1   if trade_type == 0 else -1
                    trade = {"price": price, "volume": volume, "side": side, "spin": spin, "timestamp": ts}
                    self.trade_buffer[symbol].append(trade)
                    if self.on_trade:
                        self.on_trade(symbol, trade)
            except Exception as e:
                log.debug("GatewayTrade parse error: %s", e)

        def on_gateway_quote(args):
            try:
                contract_id = args[0]
                data        = args[1]
                symbol = self._contract_to_symbol.get(contract_id)
                if not symbol:
                    return
                quote = {
                    "last_price": float(data.get("lastPrice") or 0),
                    "best_bid":   float(data.get("bestBid")   or 0),
                    "best_ask":   float(data.get("bestAsk")   or 0),
                    "volume":     int(data.get("volume")       or 0),
                    "change":     float(data.get("change")     or 0),
                    "change_pct": float(data.get("changePercent") or 0),
                    "timestamp":  data.get("timestamp", ""),
                }
                mid = (quote["best_bid"] + quote["best_ask"]) / 2.0 \
                      if (quote["best_bid"] and quote["best_ask"]) else quote["last_price"]
                quote["mid_price"] = mid
                self.quote_snapshot[symbol] = quote
                if self.on_quote:
                    self.on_quote(symbol, quote)
            except Exception as e:
                log.debug("GatewayQuote parse error: %s", e)


        def on_message(ws, raw):
            # Handle binary frames (depth events can be binary)
            if isinstance(raw, bytes):
                try:
                    raw = raw.decode('utf-8')
                except Exception:
                    return  # truly unparseable binary
            # TopStepX sends multiple concatenated JSON objects separated by \x1e
            parts = raw.split(SEP)
            for part in parts:
                part = part.strip()
                if not part:
                    continue
                try:
                    data = _json.loads(part)
                except Exception:
                    # Malformed packet — skip without killing the connection
                    log.debug("TopStepX WS: skipping malformed packet (%d bytes)", len(part))
                    continue

                msg_type = data.get("type", 0)
                if msg_type == 1:          # Invocation
                    try:
                        _dispatch(data)
                    except Exception as e:
                        log.debug("TopStepX dispatch error: %s", e)
                elif msg_type == 6:        # Ping → pong
                    try:
                        ws.send(PING)
                    except Exception:
                        pass
                # type 3 = completion, type 7 = close — ignore

        def on_open(ws):
            log.info("TopStepX WS: connected, sending handshake...")
            ws.send(HANDSHAKE)
            # Small delay then subscribe
            import threading as _t
            def _sub():
                import time as _tm; _tm.sleep(0.5)
                _send_subscriptions(ws)
                self._on_open(contract_ids)
            _t.Thread(target=_sub, daemon=True).start()

        def on_error(ws, err):
            log.error("TopStepX WS error: %s", err)

        def on_close(ws, code, msg):
            log.warning("TopStepX WS: closed (code=%s) — will reconnect", code)

        while self._running:
            try:
                ws = _ws.WebSocketApp(
                    hub_url,
                    on_open=on_open,
                    on_message=on_message,
                    on_error=on_error,
                    on_close=on_close,
                )
                self._connection = ws
                log.info("TopStepX WS: connecting to %s...", MARKET_HUB)
                ws.run_forever(ping_interval=20, ping_timeout=10)
            except Exception as e:
                log.error("TopStepX WS loop error: %s", e)
            if self._running:
                log.info("TopStepX WS: reconnecting in 5s...")
                time.sleep(5)

    def _on_open(self, contract_ids: list[str]):
        log.info("TopStepX: connected — subscribed to %d contracts", len(contract_ids))


    # ── DOM Snapshot Builder ─────────────────────────────────────────────────

    def _build_dom_snapshot(self, symbol: str, ts: str) -> dict:
        bids = self._dom_bids[symbol]
        asks = self._dom_asks[symbol]

        best_bid = self._dom_best_bid.get(symbol) or (max(bids) if bids else 0)
        best_ask = self._dom_best_ask.get(symbol) or (min(asks) if asks else 0)

        bid_total = sum(bids.values())
        ask_total = sum(asks.values())
        total     = bid_total + ask_total

        mid    = (best_bid + best_ask) / 2.0 if best_bid and best_ask else 0
        spread = best_ask - best_bid if best_bid and best_ask else 0
        imb    = (bid_total - ask_total) / total if total > 0 else 0

        return {
            "bids":      dict(sorted(bids.items(), reverse=True)),
            "asks":      dict(sorted(asks.items())),
            "best_bid":  best_bid,
            "best_ask":  best_ask,
            "mid_price": mid,
            "spread":    spread,
            "bid_total": bid_total,
            "ask_total": ask_total,
            "imbalance": imb,
            "timestamp": ts,
        }

    # ── Data Access (for framework engines) ─────────────────────────────────

    def get_mid_price(self, symbol: str) -> float:
        snap = self.dom_snapshots.get(symbol) or self.quote_snapshot.get(symbol, {})
        return snap.get("mid_price", 0.0)

    def get_order_imbalance(self, symbol: str) -> float:
        snap = self.dom_snapshots.get(symbol, {})
        return snap.get("imbalance", 0.0)

    def get_recent_trades(self, symbol: str, n: int = 100) -> list[dict]:
        return list(self.trade_buffer[symbol])[-n:]

    def get_trade_spins(self, symbol: str, n: int = 100) -> list[int]:
        return [t["spin"] for t in self.get_recent_trades(symbol, n)]

    def get_all_mid_prices(self) -> dict[str, float]:
        result = {}
        for sym in self._symbol_to_contract:
            mid = self.get_mid_price(sym)
            if mid > 0:
                result[sym] = mid
        return result

    def is_connected(self) -> bool:
        return self._running and self._connection is not None
