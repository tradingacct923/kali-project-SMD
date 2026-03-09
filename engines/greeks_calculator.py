"""
Higher-Order Greeks Calculator

Computes 2nd and 3rd order Greeks from Black-Scholes closed-form formulas.
Uses: Spot (S), Strike (K), Time (T), IV (σ), Risk-Free Rate (r)
All sourced from Massive API — no ThetaData needed.

2nd Order: Vanna, Charm, Vomma
3rd Order: Speed, Color, Ultima, Zomma
"""
import math
import logging
from typing import Optional

import numpy as np
from scipy.stats import norm

logger = logging.getLogger(__name__)


class GreeksCalculator:
    """
    Calculate all Greeks (1st, 2nd, 3rd order) using Black-Scholes.
    
    Inputs per contract:
        S     = spot price (underlying)
        K     = strike price
        T     = time to expiry in years
        sigma = implied volatility (annualized)
        r     = risk-free rate (annualized)
        q     = dividend yield (default 0)
    """

    def __init__(self, risk_free_rate: float = 0.043):
        self.r = risk_free_rate

    def set_risk_free_rate(self, rate: float):
        """Update risk-free rate (call after fetching from Massive Economy API)."""
        self.r = rate

    # ─────────────────────────────────────────────────────
    #  Core BSM Components
    # ─────────────────────────────────────────────────────

    @staticmethod
    def _d1(S: float, K: float, T: float, r: float, sigma: float, q: float = 0) -> float:
        """BSM d1 parameter."""
        if T <= 0 or sigma <= 0 or S <= 0 or K <= 0:
            return 0
        return (math.log(S / K) + (r - q + sigma ** 2 / 2) * T) / (sigma * math.sqrt(T))

    @staticmethod
    def _d2(S: float, K: float, T: float, r: float, sigma: float, q: float = 0) -> float:
        """BSM d2 parameter."""
        if T <= 0 or sigma <= 0:
            return 0
        d1 = GreeksCalculator._d1(S, K, T, r, sigma, q)
        return d1 - sigma * math.sqrt(T)

    # ─────────────────────────────────────────────────────
    #  1st Order Greeks (for validation against Massive)
    # ─────────────────────────────────────────────────────

    def delta(self, S, K, T, sigma, is_call=True, q=0):
        """Delta: ∂Price/∂Spot"""
        d1 = self._d1(S, K, T, self.r, sigma, q)
        if is_call:
            return math.exp(-q * T) * norm.cdf(d1)
        return math.exp(-q * T) * (norm.cdf(d1) - 1)

    def gamma(self, S, K, T, sigma, q=0):
        """Gamma: ∂²Price/∂Spot² (same for calls and puts)"""
        if T <= 0 or sigma <= 0 or S <= 0:
            return 0
        d1 = self._d1(S, K, T, self.r, sigma, q)
        return math.exp(-q * T) * norm.pdf(d1) / (S * sigma * math.sqrt(T))

    def theta(self, S, K, T, sigma, is_call=True, q=0):
        """Theta: ∂Price/∂Time (per day)"""
        if T <= 0 or sigma <= 0:
            return 0
        d1 = self._d1(S, K, T, self.r, sigma, q)
        d2 = self._d2(S, K, T, self.r, sigma, q)
        
        term1 = -(S * math.exp(-q * T) * norm.pdf(d1) * sigma) / (2 * math.sqrt(T))
        if is_call:
            term2 = -self.r * K * math.exp(-self.r * T) * norm.cdf(d2)
            term3 = q * S * math.exp(-q * T) * norm.cdf(d1)
        else:
            term2 = self.r * K * math.exp(-self.r * T) * norm.cdf(-d2)
            term3 = -q * S * math.exp(-q * T) * norm.cdf(-d1)
        
        return (term1 + term2 + term3) / 365  # Per day

    def vega(self, S, K, T, sigma, q=0):
        """Vega: ∂Price/∂IV (per 1% IV change)"""
        if T <= 0 or sigma <= 0:
            return 0
        d1 = self._d1(S, K, T, self.r, sigma, q)
        return S * math.exp(-q * T) * norm.pdf(d1) * math.sqrt(T) / 100

    # ─────────────────────────────────────────────────────
    #  2nd Order Greeks — THE KEY ADDITIONS
    # ─────────────────────────────────────────────────────

    def vanna(self, S, K, T, sigma, q=0):
        """
        Vanna: ∂Delta/∂IV = ∂Vega/∂Spot
        
        Measures how delta changes with volatility.
        Critical for: Vol-of-vol analysis, dealer hedging in vol moves.
        When vanna is high + IV drops → dealers buy aggressively → support.
        """
        if T <= 0 or sigma <= 0 or S <= 0:
            return 0
        d1 = self._d1(S, K, T, self.r, sigma, q)
        d2 = self._d2(S, K, T, self.r, sigma, q)
        return -math.exp(-q * T) * norm.pdf(d1) * d2 / sigma

    def charm(self, S, K, T, sigma, is_call=True, q=0):
        """
        Charm (Delta Decay): ∂Delta/∂Time
        
        How much delta changes as time passes (per day).
        Critical for: 0DTE trading — charm is HUGE near expiry.
        Tells you how fast dealer hedging needs change through the day.
        """
        if T <= 0 or sigma <= 0:
            return 0
        d1 = self._d1(S, K, T, self.r, sigma, q)
        d2 = self._d2(S, K, T, self.r, sigma, q)
        
        pdf_d1 = norm.pdf(d1)
        term = 2 * (self.r - q) * T - d2 * sigma * math.sqrt(T)
        
        if is_call:
            charm_val = q * math.exp(-q * T) * norm.cdf(d1)
            charm_val -= math.exp(-q * T) * pdf_d1 * term / (2 * T * sigma * math.sqrt(T))
        else:
            charm_val = -q * math.exp(-q * T) * norm.cdf(-d1)
            charm_val -= math.exp(-q * T) * pdf_d1 * term / (2 * T * sigma * math.sqrt(T))
        
        return charm_val / 365  # Per day

    def vomma(self, S, K, T, sigma, q=0):
        """
        Vomma (Volga): ∂Vega/∂IV = ∂²Price/∂IV²
        
        Vega convexity — how vega itself changes with vol.
        High vomma: options become more vol-sensitive as vol increases.
        Key for: Vol-of-vol strategies, tail risk positioning.
        """
        if T <= 0 or sigma <= 0 or S <= 0:
            return 0
        d1 = self._d1(S, K, T, self.r, sigma, q)
        d2 = self._d2(S, K, T, self.r, sigma, q)
        vega_val = self.vega(S, K, T, sigma, q) * 100  # Raw vega (undo /100)
        return vega_val * d1 * d2 / sigma

    # ─────────────────────────────────────────────────────
    #  3rd Order Greeks
    # ─────────────────────────────────────────────────────

    def speed(self, S, K, T, sigma, q=0):
        """
        Speed: ∂Gamma/∂Spot = ∂³Price/∂Spot³
        
        How fast gamma changes as spot moves.
        Key for: Predicting gamma acceleration near key strikes.
        """
        if T <= 0 or sigma <= 0 or S <= 0:
            return 0
        d1 = self._d1(S, K, T, self.r, sigma, q)
        gamma_val = self.gamma(S, K, T, sigma, q)
        return -(gamma_val / S) * (d1 / (sigma * math.sqrt(T)) + 1)

    def color(self, S, K, T, sigma, q=0):
        """
        Color (Gamma Decay): ∂Gamma/∂Time
        
        How gamma changes as time passes (per day).
        Critical for: Tracking how dealer hedging urgency changes through the day.
        """
        if T <= 0 or sigma <= 0 or S <= 0:
            return 0
        d1 = self._d1(S, K, T, self.r, sigma, q)
        d2 = self._d2(S, K, T, self.r, sigma, q)
        pdf_d1 = norm.pdf(d1)
        
        term1 = 2 * (self.r - q) * T
        term2 = 1
        term3 = d1 * (2 * (self.r -q) * T - d2 * sigma * math.sqrt(T)) / (sigma * math.sqrt(T))
        
        color_val = -math.exp(-q * T) * pdf_d1 / (2 * S * T * sigma * math.sqrt(T))
        color_val *= (term1 + term2 + term3)
        
        return color_val / 365  # Per day

    def zomma(self, S, K, T, sigma, q=0):
        """
        Zomma: ∂Gamma/∂IV
        
        How gamma changes with volatility.
        Key for: Understanding how vol shocks affect dealer positioning.
        """
        if T <= 0 or sigma <= 0 or S <= 0:
            return 0
        d1 = self._d1(S, K, T, self.r, sigma, q)
        d2 = self._d2(S, K, T, self.r, sigma, q)
        gamma_val = self.gamma(S, K, T, sigma, q)
        return gamma_val * (d1 * d2 - 1) / sigma

    def ultima(self, S, K, T, sigma, q=0):
        """
        Ultima: ∂Vomma/∂IV = ∂³Price/∂IV³
        
        Third derivative of price w.r.t volatility.
        Relevant for: Extreme tail-risk scenarios and vol-of-vol-of-vol.
        """
        if T <= 0 or sigma <= 0 or S <= 0:
            return 0
        d1 = self._d1(S, K, T, self.r, sigma, q)
        d2 = self._d2(S, K, T, self.r, sigma, q)
        vega_val = self.vega(S, K, T, sigma, q) * 100
        
        return (-vega_val / (sigma ** 2)) * (
            d1 * d2 * (1 - d1 * d2) + d1 ** 2 + d2 ** 2
        )

    # ─────────────────────────────────────────────────────
    #  Bulk Computation
    # ─────────────────────────────────────────────────────

    def compute_all_greeks(self, S, K, T, sigma, is_call=True, q=0) -> dict:
        """
        Compute ALL greeks for a single contract.
        
        Args:
            S: spot price
            K: strike price
            T: time to expiry in years
            sigma: implied volatility (decimal, e.g., 0.25 for 25%)
            is_call: True for call, False for put
            q: dividend yield
        
        Returns:
            Dict with all 1st, 2nd, and 3rd order greeks
        """
        return {
            # 1st Order
            "delta": self.delta(S, K, T, sigma, is_call, q),
            "gamma": self.gamma(S, K, T, sigma, q),
            "theta": self.theta(S, K, T, sigma, is_call, q),
            "vega": self.vega(S, K, T, sigma, q),
            # 2nd Order
            "vanna": self.vanna(S, K, T, sigma, q),
            "charm": self.charm(S, K, T, sigma, is_call, q),
            "vomma": self.vomma(S, K, T, sigma, q),
            # 3rd Order
            "speed": self.speed(S, K, T, sigma, q),
            "color": self.color(S, K, T, sigma, q),
            "zomma": self.zomma(S, K, T, sigma, q),
            "ultima": self.ultima(S, K, T, sigma, q),
        }

    def enrich_chain_with_higher_greeks(self, chain: list[dict],
                                        risk_free_rate: float = None) -> list[dict]:
        """
        Take a parsed option chain (from Massive) and add 2nd/3rd order Greeks.
        
        This is the key function that fills the gap between Massive and ThetaData.
        Massive gives you delta/gamma/theta/vega.
        This adds: vanna, charm, vomma, speed, color, zomma, ultima.
        """
        r = risk_free_rate or self.r

        for contract in chain:
            S = contract.get("underlying_price", 0)
            K = contract.get("strike", 0)
            iv = contract.get("iv", 0)
            expiry_str = contract.get("expiry", "")
            is_call = contract.get("type", "").lower() == "call"

            if not all([S, K, iv, expiry_str]):
                continue

            # Compute time to expiry in years
            try:
                expiry_date = datetime.strptime(expiry_str, "%Y-%m-%d") if isinstance(
                    expiry_str, str) else expiry_str
                T = max((expiry_date - datetime.now()).total_seconds() / (365.25 * 86400), 1e-6)
            except (ValueError, TypeError):
                continue

            # Add 2nd order greeks
            contract["vanna"] = self.vanna(S, K, T, iv)
            contract["charm"] = self.charm(S, K, T, iv, is_call)
            contract["vomma"] = self.vomma(S, K, T, iv)

            # Add 3rd order greeks
            contract["speed"] = self.speed(S, K, T, iv)
            contract["color"] = self.color(S, K, T, iv)
            contract["zomma"] = self.zomma(S, K, T, iv)
            contract["ultima"] = self.ultima(S, K, T, iv)

        return chain


# ─────────────────────────────────────────────────────────
# Import helper
# ─────────────────────────────────────────────────────────
from datetime import datetime


# ─── Quick Test ─────────────────────────────────────────────────
if __name__ == "__main__":
    gc = GreeksCalculator(risk_free_rate=0.043)

    # Example: QQQ call, spot=490, strike=490 (ATM), 7 DTE, 22% IV
    S, K, T, sigma = 490, 490, 7 / 365, 0.22

    print("=" * 60)
    print("GREEKS CALCULATOR TEST — QQQ ATM Call, 7 DTE, 22% IV")
    print("=" * 60)

    all_greeks = gc.compute_all_greeks(S, K, T, sigma, is_call=True)
    
    print("\n1st Order:")
    print(f"  Delta:  {all_greeks['delta']:.6f}")
    print(f"  Gamma:  {all_greeks['gamma']:.6f}")
    print(f"  Theta:  {all_greeks['theta']:.6f} (per day)")
    print(f"  Vega:   {all_greeks['vega']:.6f} (per 1% IV)")

    print("\n2nd Order:")
    print(f"  Vanna:  {all_greeks['vanna']:.6f}")
    print(f"  Charm:  {all_greeks['charm']:.6f} (per day)")
    print(f"  Vomma:  {all_greeks['vomma']:.6f}")

    print("\n3rd Order:")
    print(f"  Speed:  {all_greeks['speed']:.8f}")
    print(f"  Color:  {all_greeks['color']:.8f} (per day)")
    print(f"  Zomma:  {all_greeks['zomma']:.8f}")
    print(f"  Ultima: {all_greeks['ultima']:.8f}")
