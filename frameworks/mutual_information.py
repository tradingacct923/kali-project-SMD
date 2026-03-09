"""
Mutual Information — GEX ↔ Price Dependency Detector

Measures how much KNOWING GEX levels reduces uncertainty about price moves.
Unlike correlation (linear only), MI captures NON-LINEAR dependencies.

High MI(GEX, Price) → GEX is actively driving price → trade the gamma levels
Low MI → GEX and price are decoupled → gamma levels less reliable

Data sources:
  - GEX levels: From GEXCalculator (computed from Massive greeks)
  - Price: QQQ from Tradier or Massive
"""
import logging
import math
from collections import deque

import numpy as np

logger = logging.getLogger(__name__)


class MutualInformation:
    """
    Compute Mutual Information between GEX and price changes.
    
    I(X;Y) = Σ p(x,y) × log[ p(x,y) / (p(x) × p(y)) ]
    
    Interpretation:
    - High MI → GEX levels are predictive of price moves
    - Low MI → GEX and price are independent
    """

    def __init__(self, window_size: int = 120, n_bins: int = 5):
        """
        Args:
            window_size: Number of observations in rolling window
            n_bins: Bins for discretizing continuous values
        """
        self.window_size = window_size
        self.n_bins = n_bins
        
        self.gex_buffer = deque(maxlen=window_size)
        self.price_buffer = deque(maxlen=window_size)
        
        self.mi: float = 0.0
        self.regime: str = "unknown"

    def update(self, gex_value: float, price_change: float) -> dict:
        """
        Add new GEX and price change observation.
        
        Args:
            gex_value: Current total GEX (from GEXCalculator)
            price_change: Recent price change (%, from Tradier/Massive)
        
        Returns:
            {
                "mutual_information": float,    # MI in bits
                "normalized_mi": float,         # 0-1 scale
                "regime": str,                  # "coupled", "transitional", "decoupled"
                "gex_predictive": bool,         # Whether GEX is predictive of price
            }
        """
        self.gex_buffer.append(gex_value)
        self.price_buffer.append(price_change)

        if len(self.gex_buffer) < 30:
            return {
                "mutual_information": 0, "normalized_mi": 0,
                "regime": "insufficient_data", "gex_predictive": False,
            }

        gex = np.array(list(self.gex_buffer))
        prices = np.array(list(self.price_buffer))

        self.mi = self._compute_mi(gex, prices)
        max_mi = math.log2(self.n_bins)
        normalized = self.mi / max_mi if max_mi > 0 else 0

        if normalized > 0.3:
            self.regime = "coupled"
        elif normalized > 0.15:
            self.regime = "transitional"
        else:
            self.regime = "decoupled"

        return {
            "mutual_information": self.mi,
            "normalized_mi": normalized,
            "regime": self.regime,
            "gex_predictive": normalized > 0.2,
        }

    def _compute_mi(self, x: np.ndarray, y: np.ndarray) -> float:
        """Compute MI using histogram-based estimation."""
        try:
            # 2D histogram
            hist_2d, x_edges, y_edges = np.histogram2d(
                x, y, bins=self.n_bins, density=True
            )
            
            # Marginals
            hist_x = np.sum(hist_2d, axis=1)
            hist_y = np.sum(hist_2d, axis=0)
            
            # Bin widths for proper probability calculation
            dx = np.diff(x_edges)
            dy = np.diff(y_edges)
            
            mi = 0.0
            for i in range(self.n_bins):
                for j in range(self.n_bins):
                    p_xy = hist_2d[i, j] * dx[i] * dy[j]
                    p_x = hist_x[i] * dx[i]
                    p_y = hist_y[j] * dy[j]
                    
                    if p_xy > 1e-10 and p_x > 1e-10 and p_y > 1e-10:
                        mi += p_xy * math.log2(p_xy / (p_x * p_y))
            
            return max(mi, 0)
        except Exception as e:
            logger.debug(f"MI computation error: {e}")
            return 0.0

    def get_signal(self) -> dict:
        return {
            "framework": "mutual_information",
            "value": self.mi,
            "regime": self.regime,
            "interpretation": (
                "🔗 GEX is DRIVING price — trade gamma levels with confidence"
                if self.regime == "coupled"
                else "🔓 GEX and price are DECOUPLED — gamma levels less reliable"
            ),
        }


if __name__ == "__main__":
    mi = MutualInformation(window_size=100)
    np.random.seed(42)

    print("=" * 60)
    print("MUTUAL INFORMATION TEST")
    print("=" * 60)

    # Scenario 1: GEX and price coupled
    print("\n--- Coupled (GEX predicts price) ---")
    for i in range(100):
        gex = np.random.randn() * 1e9
        price_change = -gex / 1e10 + np.random.randn() * 0.001
        result = mi.update(gex, price_change)
    print(f"  MI:         {result['mutual_information']:.6f} bits")
    print(f"  Normalized: {result['normalized_mi']:.4f}")
    print(f"  Regime:     {result['regime']}")

    # Scenario 2: GEX and price independent
    print("\n--- Decoupled (independent) ---")
    mi2 = MutualInformation(window_size=100)
    for i in range(100):
        gex = np.random.randn() * 1e9
        price_change = np.random.randn() * 0.01
        result = mi2.update(gex, price_change)
    print(f"  MI:         {result['mutual_information']:.6f} bits")
    print(f"  Normalized: {result['normalized_mi']:.4f}")
    print(f"  Regime:     {result['regime']}")
