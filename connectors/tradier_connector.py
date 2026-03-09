"""
Tradier API Connector
Provides: Real-time price streaming for QQQ, SPY, VIX via WebSocket.
Docs: https://documentation.tradier.com/
"""
import asyncio
import json
import logging
import time
from typing import Callable

import aiohttp
import requests

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import TRADIER_API_KEY, TRADIER_REST_BASE

logger = logging.getLogger(__name__)


class TradierConnector:
    """
    Tradier connector — used ONLY for real-time price streaming.
    
    DO NOT use Tradier for greeks (they're ~1 hour delayed).
    Use Massive for greeks instead.
    """

    def __init__(self, api_key: str = None):
        self.api_key = api_key or TRADIER_API_KEY
        self.base_url = TRADIER_REST_BASE
        self._headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Accept": "application/json",
        }
        self._stream_session_id = None

    # ─────────────────────────────────────────────────────
    #  REST API — Prices Only (these ARE real-time)
    # ─────────────────────────────────────────────────────

    def _get(self, endpoint: str, params: dict = None) -> dict:
        """Make authenticated GET request."""
        url = f"{self.base_url}{endpoint}"
        resp = requests.get(url, headers=self._headers, params=params, timeout=10)
        resp.raise_for_status()
        return resp.json()

    def get_quote(self, symbols: list[str]) -> list[dict]:
        """
        Get real-time quotes for one or more symbols.
        
        Returns: last, bid, ask, volume, change, etc.
        NOTE: This IS real-time from Tradier.
        """
        data = self._get("/markets/quotes", {"symbols": ",".join(symbols)})
        quotes = data.get("quotes", {}).get("quote", [])
        # Normalize to list
        if isinstance(quotes, dict):
            quotes = [quotes]
        return quotes

    def get_price(self, symbol: str) -> float:
        """Quick helper: get last price for a symbol."""
        quotes = self.get_quote([symbol])
        if quotes:
            return quotes[0].get("last", 0)
        return 0

    def get_vix(self) -> float:
        """Get current VIX level."""
        return self.get_price("VIX")

    # ─────────────────────────────────────────────────────
    #  WebSocket Streaming (Real-time prices)
    # ─────────────────────────────────────────────────────

    def _create_stream_session(self) -> str:
        """Create a streaming session and return the session ID."""
        url = f"{self.base_url}/markets/events/session"
        resp = requests.post(url, headers=self._headers, timeout=10)
        resp.raise_for_status()
        data = resp.json()
        session_id = data.get("stream", {}).get("sessionid")
        if not session_id:
            raise RuntimeError(f"Failed to create stream session: {data}")
        return session_id

    async def stream_quotes(self, symbols: list[str],
                            on_quote: Callable[[dict], None] = None,
                            on_trade: Callable[[dict], None] = None):
        """
        Stream real-time quotes for symbols via Tradier WebSocket.
        
        This is your FASTEST price feed for VIX, QQQ, SPY.
        Use for Transfer Entropy calculations.
        
        Args:
            symbols: List of symbols to stream ["QQQ", "SPY", "VIX"]
            on_quote: Callback for quote updates (bid/ask)
            on_trade: Callback for trade updates (last price)
        """
        session_id = self._create_stream_session()
        ws_url = "wss://ws.tradier.com/v1/markets/events"

        async with aiohttp.ClientSession() as session:
            async with session.ws_connect(ws_url) as ws:
                # Subscribe
                payload = {
                    "symbols": symbols,
                    "sessionid": session_id,
                    "linebreak": True,
                    "filter": ["quote", "trade"],
                }
                await ws.send_json(payload)
                logger.info(f"Tradier WS: Streaming {symbols}")

                # Listen
                async for msg in ws:
                    if msg.type == aiohttp.WSMsgType.TEXT:
                        try:
                            data = json.loads(msg.data)
                            event_type = data.get("type")

                            if event_type == "quote" and on_quote:
                                on_quote({
                                    "symbol": data.get("symbol"),
                                    "bid": data.get("bid"),
                                    "ask": data.get("ask"),
                                    "bidsize": data.get("bidsize"),
                                    "asksize": data.get("asksize"),
                                    "timestamp": data.get("date"),
                                })
                            elif event_type == "trade" and on_trade:
                                on_trade({
                                    "symbol": data.get("symbol"),
                                    "price": data.get("price"),
                                    "size": data.get("size"),
                                    "timestamp": data.get("date"),
                                    "exchange": data.get("exch"),
                                })
                        except json.JSONDecodeError:
                            continue
                    elif msg.type in (aiohttp.WSMsgType.CLOSED,
                                      aiohttp.WSMsgType.ERROR):
                        logger.error(f"Tradier WS closed/error: {msg}")
                        break

    # ─────────────────────────────────────────────────────
    #  WARNING: Greeks from Tradier are DELAYED
    # ─────────────────────────────────────────────────────

    def get_option_chain_DO_NOT_USE_FOR_GREEKS(self, symbol: str,
                                                expiration: str = None) -> list[dict]:
        """
        ⚠️  WARNING: Tradier greeks are ~1 HOUR DELAYED.
        
        This method exists only for reference.
        Use MassiveConnector.get_option_chain_parsed() for real-time greeks.
        """
        params = {"symbol": symbol, "greeks": "true"}
        if expiration:
            params["expiration"] = expiration
        data = self._get("/markets/options/chains", params)
        logger.warning("⚠️  Tradier greeks are ~1hr delayed! Use Massive instead.")
        return data.get("options", {}).get("option", [])


# ─── Quick Test ─────────────────────────────────────────────────
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    tc = TradierConnector()

    print("=" * 60)
    print("TRADIER CONNECTOR TEST")
    print("=" * 60)

    # Test quotes
    try:
        quotes = tc.get_quote(["QQQ", "SPY", "VIX"])
        for q in quotes:
            print(f"  {q['symbol']}: ${q.get('last', 'N/A')}")
    except Exception as e:
        print(f"  Quote test FAILED: {e}")

    # Test VIX
    try:
        vix = tc.get_vix()
        print(f"\n  VIX: {vix:.2f}")
    except Exception as e:
        print(f"  VIX test FAILED: {e}")
