"""
Massive (formerly Polygon.io) API Connector
Provides: Real-time Greeks, IV, Options Chain, Stocks, Futures, Economy data.
Docs: https://massive.com/docs
"""
import asyncio
import json
import logging
import time
from datetime import datetime, timedelta
from typing import Any, Callable

import aiohttp
import requests

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import MASSIVE_API_KEY, MASSIVE_REST_BASE, MASSIVE_WS_BASE

logger = logging.getLogger(__name__)


class MassiveConnector:
    """
    Connector for Massive (Polygon.io) REST + WebSocket API.
    
    REST: Option chain snapshots, stock quotes, economy data
    WebSocket: Real-time options/stock trades & quotes streaming
    """

    def __init__(self, api_key: str = None):
        self.api_key = api_key or MASSIVE_API_KEY
        self.base_url = MASSIVE_REST_BASE
        self.ws_url = MASSIVE_WS_BASE
        self.session = None
        self._ws = None
        self._ws_callbacks: dict[str, list[Callable]] = {}

    # ─────────────────────────────────────────────────────
    #  REST API Methods
    # ─────────────────────────────────────────────────────

    def _get(self, endpoint: str, params: dict = None) -> dict:
        """Make authenticated GET request to Massive REST API."""
        params = params or {}
        params["apiKey"] = self.api_key
        url = f"{self.base_url}{endpoint}"
        
        resp = requests.get(url, params=params, timeout=10)
        resp.raise_for_status()
        return resp.json()

    # ── Options ──────────────────────────────────────────

    def get_option_chain(self, ticker: str, expiration_date: str = None,
                         strike_price: float = None, contract_type: str = None,
                         limit: int = 250) -> list[dict]:
        """
        Get full option chain snapshot WITH greeks for a ticker.
        Returns real-time: delta, gamma, theta, vega, IV per contract.
        
        Args:
            ticker: Underlying symbol (e.g., "QQQ")
            expiration_date: Filter by expiry (YYYY-MM-DD)
            strike_price: Filter by strike
            contract_type: "call" or "put"
            limit: Max results per page
        
        Returns:
            List of option contract dicts with greeks
        """
        params = {"limit": limit}
        if expiration_date:
            params["expiration_date"] = expiration_date
        if strike_price:
            params["strike_price"] = strike_price
        if contract_type:
            params["contract_type"] = contract_type

        endpoint = f"/v3/snapshot/options/{ticker}"
        data = self._get(endpoint, params)
        
        results = data.get("results", [])
        
        # Handle pagination
        while data.get("next_url"):
            next_url = data["next_url"]
            if "apiKey" not in next_url:
                next_url += f"&apiKey={self.api_key}"
            resp = requests.get(next_url, timeout=10)
            resp.raise_for_status()
            data = resp.json()
            results.extend(data.get("results", []))

        return results

    def get_option_chain_parsed(self, ticker: str, expiration_date: str = None) -> list[dict]:
        """
        Get option chain and parse into a flat, easy-to-use format.
        
        Returns list of dicts with:
            strike, type, expiry, bid, ask, last, volume, oi,
            delta, gamma, theta, vega, iv, underlying_price
        """
        raw_chain = self.get_option_chain(ticker, expiration_date=expiration_date)
        parsed = []

        for contract in raw_chain:
            details = contract.get("details", {})
            greeks = contract.get("greeks", {})
            day = contract.get("day", {})
            underlying = contract.get("underlying_asset", {})
            last_quote = contract.get("last_quote", {})

            parsed.append({
                "ticker": details.get("ticker"),
                "strike": details.get("strike_price"),
                "type": details.get("contract_type"),  # "call" or "put"
                "expiry": details.get("expiration_date"),
                "bid": last_quote.get("bid", 0),
                "ask": last_quote.get("ask", 0),
                "last": day.get("close", 0),
                "volume": day.get("volume", 0),
                "oi": contract.get("open_interest", 0),
                "delta": greeks.get("delta", 0),
                "gamma": greeks.get("gamma", 0),
                "theta": greeks.get("theta", 0),
                "vega": greeks.get("vega", 0),
                "iv": contract.get("implied_volatility", 0),
                "underlying_price": underlying.get("price", 0),
            })

        return parsed

    def get_options_contracts(self, ticker: str) -> list[dict]:
        """List all available option contracts for a ticker."""
        return self._get(f"/v3/reference/options/contracts",
                         {"underlying_ticker": ticker, "limit": 1000}).get("results", [])

    # ── Stocks ───────────────────────────────────────────

    def get_stock_snapshot(self, ticker: str) -> dict:
        """Get real-time stock snapshot (price, volume, OHLC)."""
        data = self._get(f"/v2/snapshot/locale/us/markets/stocks/tickers/{ticker}")
        return data.get("ticker", {})

    def get_stock_price(self, ticker: str) -> float:
        """Quick helper: get latest price for a stock/ETF."""
        snap = self.get_stock_snapshot(ticker)
        # Try last trade, fall back to previous close
        last_trade = snap.get("lastTrade", {})
        if last_trade and last_trade.get("p"):
            return last_trade["p"]
        return snap.get("prevDay", {}).get("c", 0)

    def get_stock_quotes(self, ticker: str, timestamp_from: str = None,
                         timestamp_to: str = None, limit: int = 100) -> list[dict]:
        """Get historical NBBO quotes for a stock."""
        params = {"limit": limit}
        if timestamp_from:
            params["timestamp.gte"] = timestamp_from
        if timestamp_to:
            params["timestamp.lte"] = timestamp_to
        return self._get(f"/v3/quotes/{ticker}", params).get("results", [])

    # ── Futures ──────────────────────────────────────────

    def get_futures_snapshot(self, ticker: str) -> dict:
        """Get futures contract snapshot."""
        data = self._get(f"/v3/snapshot/futures/{ticker}")
        return data.get("results", [])

    # ── Economy ──────────────────────────────────────────

    def get_treasury_yields(self) -> list[dict]:
        """Get current treasury yields (for risk-free rate in BSM)."""
        return self._get("/v1/economy/treasury-yields").get("results", [])

    def get_risk_free_rate(self) -> float:
        """Get 3-month T-bill yield as risk-free rate for BSM calculations."""
        try:
            yields = self.get_treasury_yields()
            for y in yields:
                if y.get("maturity") == "3m":
                    return y.get("yield", 0.043) / 100  # Convert to decimal
            return 0.043  # Fallback
        except Exception as e:
            logger.warning(f"Failed to fetch risk-free rate: {e}, using fallback")
            return 0.043

    # ─────────────────────────────────────────────────────
    #  WebSocket Streaming
    # ─────────────────────────────────────────────────────

    async def connect_websocket(self, channels: list[str],
                                on_message: Callable[[dict], None] = None):
        """
        Connect to Massive WebSocket for real-time streaming.
        
        Channels:
            "T.QQQ"      - Stock trades
            "Q.QQQ"      - Stock quotes (NBBO)
            "T.O:QQQ*"   - Options trades for QQQ
            "Q.O:QQQ*"   - Options quotes for QQQ
            "AM.QQQ"     - Per-minute aggregates
            "A.QQQ"      - Per-second aggregates
        
        Args:
            channels: List of channel strings to subscribe to
            on_message: Callback function for each message
        """
        # Determine cluster based on channel type
        if any("O:" in c for c in channels):
            cluster = "options"
        elif any(c.startswith("XT.") or c.startswith("XQ.") for c in channels):
            cluster = "futures"
        else:
            cluster = "stocks"

        ws_url = f"{self.ws_url}/{cluster}"

        async with aiohttp.ClientSession() as session:
            async with session.ws_connect(ws_url) as ws:
                self._ws = ws

                # Authenticate
                await ws.send_json({"action": "auth", "params": self.api_key})
                auth_resp = await ws.receive_json()
                logger.info(f"WS Auth: {auth_resp}")

                # Subscribe
                await ws.send_json({
                    "action": "subscribe",
                    "params": ",".join(channels)
                })
                sub_resp = await ws.receive_json()
                logger.info(f"WS Subscribe: {sub_resp}")

                # Listen
                async for msg in ws:
                    if msg.type == aiohttp.WSMsgType.TEXT:
                        data = json.loads(msg.data)
                        if on_message:
                            for event in data:
                                on_message(event)
                    elif msg.type in (aiohttp.WSMsgType.CLOSED,
                                      aiohttp.WSMsgType.ERROR):
                        logger.error(f"WS connection closed/error: {msg}")
                        break

    async def disconnect_websocket(self):
        """Close WebSocket connection."""
        if self._ws:
            await self._ws.close()
            self._ws = None

    # ─────────────────────────────────────────────────────
    #  Convenience Methods
    # ─────────────────────────────────────────────────────

    def get_todays_0dte_chain(self, ticker: str) -> list[dict]:
        """Get only today's expiring options (0DTE) for dealer positioning."""
        today = datetime.now().strftime("%Y-%m-%d")
        return self.get_option_chain_parsed(ticker, expiration_date=today)

    def get_near_term_chain(self, ticker: str, num_expiries: int = 4) -> list[dict]:
        """Get options for the nearest N expirations (for multi-expiry GEX)."""
        # Get all contracts to find upcoming expiries
        contracts = self.get_options_contracts(ticker)
        
        # Extract unique expiration dates, sort ascending
        expiries = sorted(set(
            c.get("expiration_date") for c in contracts
            if c.get("expiration_date", "") >= datetime.now().strftime("%Y-%m-%d")
        ))[:num_expiries]

        all_options = []
        for exp in expiries:
            chain = self.get_option_chain_parsed(ticker, expiration_date=exp)
            all_options.extend(chain)

        return all_options


# ─── Quick Test ─────────────────────────────────────────────────
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    mc = MassiveConnector()

    print("=" * 60)
    print("MASSIVE CONNECTOR TEST")
    print("=" * 60)

    # Test stock price
    try:
        price = mc.get_stock_price("QQQ")
        print(f"\nQQQ Price: ${price:.2f}")
    except Exception as e:
        print(f"\nQQQ Price: FAILED ({e})")

    # Test option chain
    try:
        chain = mc.get_option_chain_parsed("QQQ")
        print(f"\nQQQ Option Chain: {len(chain)} contracts loaded")
        if chain:
            sample = chain[0]
            print(f"  Sample: {sample['type']} {sample['strike']} exp={sample['expiry']}")
            print(f"  Greeks: Δ={sample['delta']:.4f} Γ={sample['gamma']:.6f} "
                  f"Θ={sample['theta']:.4f} V={sample['vega']:.4f} IV={sample['iv']:.2%}")
    except Exception as e:
        print(f"\nOption Chain: FAILED ({e})")

    # Test risk-free rate
    try:
        r = mc.get_risk_free_rate()
        print(f"\nRisk-Free Rate: {r:.4f} ({r*100:.2f}%)")
    except Exception as e:
        print(f"\nRisk-Free Rate: FAILED ({e})")
