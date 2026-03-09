"""
GEX (Gamma Exposure) Calculator Engine

Computes:
- GEX per strike (call and put separately)
- Total GEX (net dealer gamma)
- Zero-Gamma Flip Level (where GEX switches sign)
- Put Wall / Call Wall (max GEX concentrations)
- 0DTE dealer positioning

Data source: Massive API option chain with real-time greeks.
"""
import logging
from datetime import datetime
from typing import Optional

import numpy as np

logger = logging.getLogger(__name__)


class GEXCalculator:
    """
    Compute Gamma Exposure from option chain data.
    
    GEX_per_strike = gamma × OI × 100 × spot²
    
    Convention: Market makers are net SHORT options.
    - Calls: MM is short → positive gamma → they BUY dips, SELL rips (stabilizing)
    - Puts:  MM is short → negative gamma → they SELL dips, BUY rips (destabilizing)
    
    Total GEX = Σ(call_GEX) - Σ(put_GEX)
    Positive total GEX → market pinned (low vol)
    Negative total GEX → market volatile (big moves)
    """

    def __init__(self):
        self.last_gex: dict = {}
        self.last_update: float = 0

    def compute_gex(self, chain: list[dict], spot: float = None) -> dict:
        """
        Compute full GEX analysis from parsed option chain.
        
        Args:
            chain: List of option dicts from MassiveConnector.get_option_chain_parsed()
                   Each must have: strike, type, gamma, oi, underlying_price
            spot: Override spot price (otherwise uses chain's underlying_price)
        
        Returns:
            {
                "spot": float,
                "total_gex": float,
                "call_gex": float,
                "put_gex": float,
                "zero_gamma_level": float,        # Where GEX flips sign
                "call_wall": {"strike": float, "gex": float},
                "put_wall": {"strike": float, "gex": float},
                "gex_by_strike": [{"strike", "call_gex", "put_gex", "net_gex"}, ...],
                "regime": str,                    # "positive_gamma" or "negative_gamma"
                "timestamp": str,
            }
        """
        if not chain:
            return {"error": "Empty chain"}

        # Determine spot price
        if spot is None:
            prices = [c["underlying_price"] for c in chain if c.get("underlying_price")]
            spot = prices[0] if prices else 0

        # Group by strike
        strikes = {}
        for c in chain:
            strike = c.get("strike", 0)
            if strike == 0:
                continue

            gamma = abs(c.get("gamma", 0))
            oi = c.get("oi", 0)
            contract_type = c.get("type", "").lower()

            # GEX = gamma × OI × 100 (contract multiplier) × spot²
            gex = gamma * oi * 100 * spot * spot

            if strike not in strikes:
                strikes[strike] = {"strike": strike, "call_gex": 0, "put_gex": 0}

            if contract_type == "call":
                strikes[strike]["call_gex"] += gex
            elif contract_type == "put":
                strikes[strike]["put_gex"] += gex

        # Calculate net GEX per strike
        gex_by_strike = []
        for s, data in sorted(strikes.items()):
            # Convention: put GEX is negative (destabilizing)
            net = data["call_gex"] - data["put_gex"]
            gex_by_strike.append({
                "strike": s,
                "call_gex": data["call_gex"],
                "put_gex": data["put_gex"],
                "net_gex": net,
            })

        # Totals
        total_call_gex = sum(g["call_gex"] for g in gex_by_strike)
        total_put_gex = sum(g["put_gex"] for g in gex_by_strike)
        total_gex = total_call_gex - total_put_gex

        # Call Wall = strike with max call GEX
        call_wall = max(gex_by_strike, key=lambda x: x["call_gex"]) if gex_by_strike else None

        # Put Wall = strike with max put GEX
        put_wall = max(gex_by_strike, key=lambda x: x["put_gex"]) if gex_by_strike else None

        # Zero-Gamma Flip = where cumulative GEX changes sign
        zero_gamma = self._find_zero_gamma(gex_by_strike, spot)

        # Regime classification
        regime = "positive_gamma" if total_gex > 0 else "negative_gamma"

        result = {
            "spot": spot,
            "total_gex": total_gex,
            "call_gex": total_call_gex,
            "put_gex": total_put_gex,
            "zero_gamma_level": zero_gamma,
            "call_wall": {
                "strike": call_wall["strike"] if call_wall else 0,
                "gex": call_wall["call_gex"] if call_wall else 0,
            },
            "put_wall": {
                "strike": put_wall["strike"] if put_wall else 0,
                "gex": put_wall["put_gex"] if put_wall else 0,
            },
            "gex_by_strike": gex_by_strike,
            "regime": regime,
            "timestamp": datetime.now().isoformat(),
        }

        self.last_gex = result
        self.last_update = datetime.now().timestamp()
        return result

    def _find_zero_gamma(self, gex_by_strike: list[dict], spot: float) -> float:
        """
        Find the price level where net GEX crosses zero.
        This is the 'gamma flip' — above it price is pinned, below it price runs.
        """
        if len(gex_by_strike) < 2:
            return spot

        # Find where net_gex changes sign
        for i in range(1, len(gex_by_strike)):
            prev = gex_by_strike[i - 1]
            curr = gex_by_strike[i]
            
            if prev["net_gex"] * curr["net_gex"] < 0:  # Sign change
                # Linear interpolation
                try:
                    ratio = abs(prev["net_gex"]) / (abs(prev["net_gex"]) + abs(curr["net_gex"]))
                    zero_level = prev["strike"] + ratio * (curr["strike"] - prev["strike"])
                    return round(zero_level, 2)
                except ZeroDivisionError:
                    continue

        return spot  # No flip found → return spot

    def compute_0dte_gex(self, chain: list[dict], spot: float = None) -> dict:
        """
        Compute GEX for only today's expiring options (0DTE).
        
        0DTE gamma is extremely high near expiry and drives
        massive intraday dealer hedging flows.
        """
        today = datetime.now().strftime("%Y-%m-%d")
        dte_chain = [c for c in chain if c.get("expiry") == today]

        if not dte_chain:
            return {"0dte_gex": 0, "0dte_contracts": 0, "message": "No 0DTE options found"}

        gex_data = self.compute_gex(dte_chain, spot)
        gex_data["0dte_contracts"] = len(dte_chain)
        gex_data["is_0dte"] = True
        return gex_data

    def format_summary(self, gex: dict) -> str:
        """Format GEX data as a readable summary."""
        if "error" in gex:
            return f"GEX Error: {gex['error']}"

        lines = [
            f"═══ GEX Summary @ {gex.get('timestamp', 'N/A')} ═══",
            f"  Spot:             ${gex['spot']:.2f}",
            f"  Total GEX:        {gex['total_gex']:,.0f}",
            f"  Regime:           {gex['regime'].upper().replace('_', ' ')}",
            f"  Zero-Gamma Flip:  ${gex['zero_gamma_level']:.2f}",
            f"  Call Wall:        ${gex['call_wall']['strike']:.0f} (GEX: {gex['call_wall']['gex']:,.0f})",
            f"  Put Wall:         ${gex['put_wall']['strike']:.0f} (GEX: {gex['put_wall']['gex']:,.0f})",
        ]
        return "\n".join(lines)
