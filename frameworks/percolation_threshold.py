"""
Percolation Threshold — Systemic Risk Detector

From percolation theory in physics. Models the market as a network
where correlations between assets are "bonds" connecting nodes.

When too many bonds break simultaneously (correlations collapse),
the network "percolates" — systemic stress propagates across all assets.

Detection:
- Compute pairwise correlations between NQ, ES, YM, RTY, QQQ, SPY, VIX
- Track what fraction of correlations have broken (deviated from normal)
- When >60% break simultaneously → systemic event

This is your ULTIMATE tail-risk detector.

Data sources:
  - NQ, ES, YM, RTY: TopStepX Level 2
  - QQQ, SPY, VIX: Tradier WebSocket
  - OR: All from Massive stock snapshots
"""
import logging
from collections import deque
from itertools import combinations

import numpy as np

logger = logging.getLogger(__name__)


class PercolationThreshold:
    """
    Detect systemic risk through correlation breakdown across assets.
    
    Normal: Assets maintain stable pairwise correlations.
    Crisis: Multiple correlations break simultaneously → percolation.
    """

    DEFAULT_SYMBOLS = ["NQ", "ES", "YM", "RTY", "QQQ", "SPY", "VIX"]

    def __init__(self, symbols: list[str] = None, window_size: int = 300,
                 correlation_window: int = 60, threshold: float = 0.6):
        """
        Args:
            symbols: Assets to track
            window_size: Historical observations for "normal" correlation baseline
            correlation_window: Rolling window for current correlation
            threshold: Fraction of broken bonds for percolation alert (0-1)
        """
        self.symbols = symbols or self.DEFAULT_SYMBOLS
        self.window_size = window_size
        self.corr_window = correlation_window
        self.threshold = threshold
        
        # Price buffers per symbol
        self.price_buffers: dict[str, deque] = {
            s: deque(maxlen=window_size) for s in self.symbols
        }
        
        # Baseline correlations (established from first N observations)
        self.baseline_corr: dict[tuple, float] = {}
        self.baseline_established = False
        
        # Latest state
        self.broken_fraction: float = 0.0
        self.percolating: bool = False
        self.regime: str = "stable"

    def update(self, prices: dict[str, float]) -> dict:
        """
        Add new price observations for all tracked symbols.
        
        Args:
            prices: {"NQ": 20100, "ES": 5950, "VIX": 18.5, ...}
        
        Returns:
            {
                "broken_fraction": float,      # 0-1 of broken correlations
                "percolating": bool,            # Systemic stress detected
                "regime": str,                  # "stable", "stressed", "percolating"
                "broken_pairs": list,           # Which pairs have broken
                "correlation_matrix": dict,     # Current pairwise correlations
            }
        """
        # Update price buffers
        for symbol, price in prices.items():
            if symbol in self.price_buffers:
                self.price_buffers[symbol].append(price)

        # Need minimum data
        min_len = min(len(buf) for buf in self.price_buffers.values())
        if min_len < self.corr_window + 10:
            return {
                "broken_fraction": 0, "percolating": False,
                "regime": "insufficient_data", "broken_pairs": [],
                "correlation_matrix": {},
            }

        # Establish baseline if not yet done
        if not self.baseline_established and min_len >= self.window_size // 2:
            self._establish_baseline()
            self.baseline_established = True

        # Compute current correlations
        current_corr = self._compute_current_correlations()

        # Compare to baseline and detect breaks
        broken_pairs = []
        total_pairs = 0

        for pair, curr_corr in current_corr.items():
            baseline = self.baseline_corr.get(pair)
            if baseline is None:
                continue
            
            total_pairs += 1
            
            # A correlation is "broken" if it has deviated significantly
            # For positive correlations: they should stay positive (break if drops below 0.3)
            # For negative correlations (VIX): they should stay negative
            deviation = abs(curr_corr - baseline)
            
            if deviation > 0.5:  # Major deviation
                broken_pairs.append({
                    "pair": pair,
                    "baseline": baseline,
                    "current": curr_corr,
                    "deviation": deviation,
                })

        # Compute broken fraction
        self.broken_fraction = len(broken_pairs) / max(total_pairs, 1)
        self.percolating = self.broken_fraction > self.threshold

        # Regime
        if self.broken_fraction > self.threshold:
            self.regime = "percolating"
        elif self.broken_fraction > self.threshold * 0.5:
            self.regime = "stressed"
        else:
            self.regime = "stable"

        return {
            "broken_fraction": self.broken_fraction,
            "percolating": self.percolating,
            "regime": self.regime,
            "broken_pairs": broken_pairs,
            "total_pairs": total_pairs,
            "correlation_matrix": current_corr,
        }

    def _establish_baseline(self):
        """Compute baseline pairwise correlations from historical data."""
        self.baseline_corr = self._compute_correlations(use_full_buffer=True)
        logger.info(f"Baseline correlations established from {len(self.baseline_corr)} pairs")

    def _compute_current_correlations(self) -> dict:
        """Compute correlations using only the recent window."""
        return self._compute_correlations(use_full_buffer=False)

    def _compute_correlations(self, use_full_buffer: bool = False) -> dict:
        """Compute pairwise Pearson correlations."""
        # Get returns
        returns = {}
        for symbol, buffer in self.price_buffers.items():
            prices = np.array(list(buffer))
            if use_full_buffer:
                r = np.diff(np.log(np.maximum(prices, 1e-6)))
            else:
                recent = prices[-self.corr_window:]
                r = np.diff(np.log(np.maximum(recent, 1e-6)))
            if len(r) > 5:
                returns[symbol] = r

        # Pairwise correlations
        corr = {}
        for sym1, sym2 in combinations(returns.keys(), 2):
            r1, r2 = returns[sym1], returns[sym2]
            min_len = min(len(r1), len(r2))
            if min_len > 5:
                try:
                    c = np.corrcoef(r1[-min_len:], r2[-min_len:])[0, 1]
                    if not np.isnan(c):
                        corr[(sym1, sym2)] = c
                except Exception:
                    continue

        return corr

    def get_signal(self) -> dict:
        return {
            "framework": "percolation_threshold",
            "value": self.broken_fraction,
            "percolating": self.percolating,
            "regime": self.regime,
            "interpretation": self._interpret(),
        }

    def _interpret(self) -> str:
        pct = self.broken_fraction * 100
        if self.regime == "percolating":
            return f"🚨 PERCOLATION — {pct:.0f}% of correlations BROKEN, systemic event"
        elif self.regime == "stressed":
            return f"⚡ STRESSED — {pct:.0f}% correlations deviating, watch for cascade"
        else:
            return f"✅ STABLE — {pct:.0f}% deviation, correlations holding"


if __name__ == "__main__":
    perc = PercolationThreshold(
        symbols=["NQ", "ES", "YM", "RTY"],
        window_size=200, correlation_window=50, threshold=0.6,
    )
    np.random.seed(42)

    print("=" * 60)
    print("PERCOLATION THRESHOLD TEST")
    print("=" * 60)

    # Scenario 1: Normal market — assets correlated
    print("\n--- Normal (correlated assets) ---")
    base = 20000
    for i in range(200):
        common = np.random.randn() * 10
        prices = {
            "NQ": base + common + np.random.randn() * 2,
            "ES": 6000 + common * 0.3 + np.random.randn() * 1,
            "YM": 40000 + common * 0.5 + np.random.randn() * 5,
            "RTY": 2200 + common * 0.2 + np.random.randn() * 1,
        }
        base += common * 0.1
        result = perc.update(prices)
    print(f"  Broken: {result['broken_fraction']:.2%}")
    print(f"  Regime: {result['regime']}")

    # Scenario 2: Crisis — correlations break
    print("\n--- Crisis (correlations breaking) ---")
    for i in range(100):
        # Each asset moves independently (correlations break)
        prices = {
            "NQ": base + np.random.randn() * 100,
            "ES": 6000 + np.random.randn() * 50,
            "YM": 40000 + np.random.randn() * 200,
            "RTY": 2200 + np.random.randn() * 30,
        }
        result = perc.update(prices)
    print(f"  Broken: {result['broken_fraction']:.2%}")
    print(f"  Regime: {result['regime']}")
    print(f"  Percolating: {result['percolating']}")
