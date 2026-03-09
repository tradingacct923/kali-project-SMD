"""
Ising Magnetization — Cross-Asset Herding Detector

Based on the Ising model from statistical mechanics.
Each trade is a "spin" — +1 for buy aggressor, -1 for sell aggressor.
Magnetization M = |Σ spins| / N measures alignment.

M → 1: HERDING — all participants aligned (strong trend, momentum)
M → 0: RANDOM — no alignment (mean reversion regime)

Key alpha: When M spikes across MULTIPLE futures simultaneously
(NQ, ES, YM, RTY all herding), a major move is coming or underway.

Data sources:
  - TopStepX Level 2 trades (buy/sell aggressor classification)
"""
import logging
from collections import deque

import numpy as np

logger = logging.getLogger(__name__)


class IsingMagnetization:
    """
    Compute Ising Magnetization across one or more futures contracts.
    
    Single-asset: M = |Σ spin_i| / N for one contract
    Cross-asset: Average M across NQ, ES, YM, RTY — when ALL are high,
                 systemic herding is occurring.
    """

    def __init__(self, window_size: int = 60, herd_threshold: float = 0.7):
        """
        Args:
            window_size: Number of trades in rolling window
            herd_threshold: Magnetization threshold for herding alert (0-1)
        """
        self.window_size = window_size
        self.herd_threshold = herd_threshold
        
        # Per-symbol spin buffers
        self.spin_buffers: dict[str, deque] = {}
        
        # Latest values
        self.magnetizations: dict[str, float] = {}
        self.cross_magnetization: float = 0.0
        self.herding: bool = False
        self.regime: str = "random"

    def update_trade(self, symbol: str, spin: int) -> dict:
        """
        Add a new trade spin for a symbol.
        
        Args:
            symbol: Futures symbol ("NQ", "ES", "YM", "RTY")
            spin: +1 (buy aggressor) or -1 (sell aggressor)
                  (from TopStepXConnector trade parse)
        
        Returns:
            Dict with per-symbol and cross-asset magnetization
        """
        if symbol not in self.spin_buffers:
            self.spin_buffers[symbol] = deque(maxlen=self.window_size)

        self.spin_buffers[symbol].append(spin)

        return self._compute()

    def update_batch(self, trades: dict[str, list[int]]) -> dict:
        """
        Batch update with multiple trades across symbols.
        
        Args:
            trades: {"NQ": [1, -1, 1, 1], "ES": [-1, -1, 1], ...}
        """
        for symbol, spins in trades.items():
            if symbol not in self.spin_buffers:
                self.spin_buffers[symbol] = deque(maxlen=self.window_size)
            for spin in spins:
                self.spin_buffers[symbol].append(spin)

        return self._compute()

    def _compute(self) -> dict:
        """Compute magnetization for all symbols and cross-asset average."""
        
        # Per-symbol magnetization
        self.magnetizations = {}
        directions = {}
        
        for symbol, buffer in self.spin_buffers.items():
            if len(buffer) < 10:
                self.magnetizations[symbol] = 0
                directions[symbol] = "neutral"
                continue

            spins = np.array(list(buffer))
            n = len(spins)
            
            # Magnetization = |mean spin|
            # Raw mean tells us direction, absolute tells us alignment
            mean_spin = spins.mean()
            M = abs(mean_spin)
            self.magnetizations[symbol] = M

            # Direction of herding
            if mean_spin > 0.1:
                directions[symbol] = "bullish"
            elif mean_spin < -0.1:
                directions[symbol] = "bearish"
            else:
                directions[symbol] = "neutral"

        # Cross-asset magnetization (average across all symbols)
        if self.magnetizations:
            self.cross_magnetization = np.mean(list(self.magnetizations.values()))
        else:
            self.cross_magnetization = 0

        # Herding detection
        self.herding = self.cross_magnetization > self.herd_threshold
        
        # Count how many contracts are herding individually
        herding_count = sum(1 for m in self.magnetizations.values() 
                           if m > self.herd_threshold)
        
        # Regime
        if herding_count >= 3:  # 3+ of 4 futures herding
            self.regime = "systemic_herding"
        elif herding_count >= 2:
            self.regime = "partial_herding"
        elif self.cross_magnetization > 0.5:
            self.regime = "mild_alignment"
        else:
            self.regime = "random"

        # Direction consensus
        dir_values = list(directions.values())
        if dir_values.count("bullish") >= 3:
            consensus = "bullish"
        elif dir_values.count("bearish") >= 3:
            consensus = "bearish"
        else:
            consensus = "mixed"

        return {
            "per_symbol": self.magnetizations,
            "directions": directions,
            "cross_magnetization": self.cross_magnetization,
            "herding": self.herding,
            "herding_count": herding_count,
            "regime": self.regime,
            "consensus": consensus,
        }

    def get_signal(self) -> dict:
        """Get the current Ising signal for the inference engine."""
        return {
            "framework": "ising_magnetization",
            "value": self.cross_magnetization,
            "herding": self.herding,
            "regime": self.regime,
            "interpretation": self._interpret(),
        }

    def _interpret(self) -> str:
        if self.regime == "systemic_herding":
            return "🔥 SYSTEMIC HERDING — 3+ futures aligned, major move in progress"
        elif self.regime == "partial_herding":
            return "⚡ Partial herding — 2 futures aligned, watch for expansion"
        elif self.regime == "mild_alignment":
            return "📊 Mild alignment — some directional bias, not yet herding"
        else:
            return "🎲 Random flow — no herding, mean reversion likely"


# ─── Quick Test ─────────────────────────────────────────────────
if __name__ == "__main__":
    ising = IsingMagnetization(window_size=50, herd_threshold=0.7)

    print("=" * 60)
    print("ISING MAGNETIZATION TEST")
    print("=" * 60)

    np.random.seed(42)

    # Scenario 1: Random market (no herding)
    print("\n--- Random Market ---")
    for i in range(50):
        for sym in ["NQ", "ES", "YM", "RTY"]:
            spin = 1 if np.random.rand() > 0.5 else -1
            result = ising.update_trade(sym, spin)
    print(f"  Cross M:  {result['cross_magnetization']:.4f}")
    print(f"  Herding:  {result['herding']}")
    print(f"  Regime:   {result['regime']}")

    # Scenario 2: All buying (herding)
    print("\n--- Herding (all buying) ---")
    ising2 = IsingMagnetization(window_size=50, herd_threshold=0.7)
    for i in range(50):
        for sym in ["NQ", "ES", "YM", "RTY"]:
            # 85% buy probability = strong herding
            spin = 1 if np.random.rand() > 0.15 else -1
            result = ising2.update_trade(sym, spin)
    print(f"  Cross M:  {result['cross_magnetization']:.4f}")
    print(f"  Herding:  {result['herding']}")
    print(f"  Regime:   {result['regime']}")
    print(f"  Per sym:  {result['per_symbol']}")
    print(f"  Dirs:     {result['directions']}")
    print(f"  Consensus:{result['consensus']}")
