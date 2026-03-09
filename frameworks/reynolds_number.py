"""
Reynolds Number — Market Flow Regime Classifier

Borrowed from fluid dynamics. Classifies market microstructure as:
- LAMINAR (low Re): Orderly flow → mean reversion strategies work
- TURBULENT (high Re): Chaotic flow → momentum strategies work

Re = (velocity × characteristic_length) / viscosity

In market terms:
- Velocity = rate of price change
- Characteristic length = typical trade size / volume
- Viscosity = bid-ask spread acts as "friction"

Data sources:
  - TopStepX Level 2 (price velocity, spread, trade sizes)
"""
import logging
from collections import deque

import numpy as np

logger = logging.getLogger(__name__)


class ReynoldsNumber:
    """
    Compute market Reynolds Number for flow regime classification.
    
    Low Re → Laminar → Scalp mean reversion
    High Re → Turbulent → Ride momentum
    """

    # Regime thresholds (calibrated for financial markets)
    LAMINAR_THRESHOLD = 500
    TURBULENT_THRESHOLD = 2000

    def __init__(self, window_size: int = 300):
        """
        Args:
            window_size: Number of observations for computation
        """
        self.window_size = window_size
        
        self.price_buffer = deque(maxlen=window_size)
        self.spread_buffer = deque(maxlen=window_size)
        self.volume_buffer = deque(maxlen=window_size)
        self.timestamp_buffer = deque(maxlen=window_size)
        
        self.reynolds: float = 0.0
        self.regime: str = "unknown"

    def update(self, price: float, spread: float, volume: float, 
               timestamp: float = None) -> dict:
        """
        Add new market observation.
        
        Args:
            price: Current mid-price (from TopStepX DOM)
            spread: Current bid-ask spread
            volume: Trade volume/size
            timestamp: Unix timestamp
        
        Returns:
            {
                "reynolds_number": float,
                "regime": str,          # "laminar", "transitional", "turbulent"
                "velocity": float,      # Price velocity
                "viscosity": float,     # Market friction
                "strategy": str,        # Recommended strategy
            }
        """
        import time as _time
        self.price_buffer.append(price)
        self.spread_buffer.append(max(spread, 1e-6))
        self.volume_buffer.append(volume)
        self.timestamp_buffer.append(timestamp or _time.time())

        if len(self.price_buffer) < 30:
            return {
                "reynolds_number": 0, "regime": "insufficient_data",
                "velocity": 0, "viscosity": 0, "strategy": "wait",
            }

        prices = np.array(list(self.price_buffer))
        spreads = np.array(list(self.spread_buffer))
        volumes = np.array(list(self.volume_buffer))

        # Velocity = absolute rate of price change (momentum)
        returns = np.abs(np.diff(prices) / prices[:-1])
        velocity = np.mean(returns) * 10000  # Scale to basis points

        # Characteristic length = normalized volume intensity
        char_length = np.mean(volumes) / max(np.std(volumes), 1e-6)

        # Viscosity = average spread (market friction)
        viscosity = np.mean(spreads) / np.mean(prices) * 10000  # Spread in bps

        # Reynolds Number
        if viscosity > 0:
            self.reynolds = (velocity * char_length) / viscosity
        else:
            self.reynolds = 0

        # Regime classification
        if self.reynolds < self.LAMINAR_THRESHOLD:
            self.regime = "laminar"
            strategy = "MEAN REVERSION — scalp fades, tight stops"
        elif self.reynolds < self.TURBULENT_THRESHOLD:
            self.regime = "transitional"
            strategy = "CAUTION — regime shift possible, reduce size"
        else:
            self.regime = "turbulent"
            strategy = "MOMENTUM — ride trends, wide stops"

        return {
            "reynolds_number": self.reynolds,
            "regime": self.regime,
            "velocity": velocity,
            "viscosity": viscosity,
            "char_length": char_length,
            "strategy": strategy,
        }

    def get_signal(self) -> dict:
        return {
            "framework": "reynolds_number",
            "value": self.reynolds,
            "regime": self.regime,
            "interpretation": (
                "🌊 TURBULENT — momentum strategies preferred"
                if self.regime == "turbulent"
                else "🏊 LAMINAR — mean reversion strategies preferred"
                if self.regime == "laminar"
                else "⚠️ TRANSITIONAL — caution, regime may shift"
            ),
        }


if __name__ == "__main__":
    rn = ReynoldsNumber(window_size=100)
    np.random.seed(42)

    print("=" * 60)
    print("REYNOLDS NUMBER TEST")
    print("=" * 60)

    # Scenario 1: Calm market (small moves, tight spreads)
    print("\n--- Laminar (calm market) ---")
    price = 20000.0
    for i in range(100):
        price += np.random.randn() * 2  # Small moves
        spread = 0.25 + np.random.rand() * 0.1  # Tight spread
        vol = 10 + np.random.rand() * 5
        result = rn.update(price, spread, vol)
    print(f"  Re:      {result['reynolds_number']:.2f}")
    print(f"  Regime:  {result['regime']}")
    print(f"  Strat:   {result['strategy']}")

    # Scenario 2: Volatile market (big moves, wide spreads)
    print("\n--- Turbulent (volatile market) ---")
    rn2 = ReynoldsNumber(window_size=100)
    price = 20000.0
    for i in range(100):
        price += np.random.randn() * 50  # Big moves
        spread = 2.0 + np.random.rand() * 3  # Wide spread
        vol = 50 + np.random.rand() * 100  # Heavy volume
        result = rn2.update(price, spread, vol)
    print(f"  Re:      {result['reynolds_number']:.2f}")
    print(f"  Regime:  {result['regime']}")
    print(f"  Strat:   {result['strategy']}")
