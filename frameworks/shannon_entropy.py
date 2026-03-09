"""
Shannon Entropy — Order Flow Regime Detector

Measures the PREDICTABILITY of order flow from TopStepX Level 2 data.

Low Entropy → Structured, ordered market → Signals are exploitable
High Entropy → Random, chaotic market → Signals are noise

Key insight: Before major moves, entropy DROPS because institutional
order flow becomes structured (herding into one direction).
After the move, entropy SPIKES as participants scramble.

Data sources:
  - TopStepX Level 2 order imbalance (real-time)
  - Massive NOI (Net Order Imbalance) WebSocket (bonus feed)
"""
import logging
import math
from collections import deque, Counter

import numpy as np

logger = logging.getLogger(__name__)


class ShannonEntropy:
    """
    Compute Shannon Entropy of order flow states in real-time.
    
    H(X) = -Σ p(x) × log₂(p(x))
    
    States are discretized order imbalance levels:
    - STRONG_BID:  imbalance > +0.4
    - WEAK_BID:    +0.1 < imbalance < +0.4
    - NEUTRAL:     -0.1 < imbalance < +0.1
    - WEAK_ASK:    -0.4 < imbalance < -0.1
    - STRONG_ASK:  imbalance < -0.4
    """

    # State definitions
    STATES = ["STRONG_BID", "WEAK_BID", "NEUTRAL", "WEAK_ASK", "STRONG_ASK"]
    THRESHOLDS = [0.4, 0.1, -0.1, -0.4]  # Upper bounds for state classification

    def __init__(self, window_size: int = 120, n_states: int = 5):
        """
        Args:
            window_size: Number of observations in rolling window
            n_states: Number of discretized states (default 5)
        """
        self.window_size = window_size
        self.n_states = n_states
        self.state_buffer = deque(maxlen=window_size)
        
        # Maximum possible entropy for normalization
        self.max_entropy = math.log2(n_states)
        
        # Latest values
        self.entropy: float = 0.0
        self.normalized_entropy: float = 0.0
        self.regime: str = "unknown"

    def classify_state(self, imbalance: float) -> str:
        """
        Classify order imbalance into discrete state.
        
        Args:
            imbalance: Order imbalance from -1 (all asks) to +1 (all bids)
        """
        if imbalance > self.THRESHOLDS[0]:
            return "STRONG_BID"
        elif imbalance > self.THRESHOLDS[1]:
            return "WEAK_BID"
        elif imbalance > self.THRESHOLDS[2]:
            return "NEUTRAL"
        elif imbalance > self.THRESHOLDS[3]:
            return "WEAK_ASK"
        else:
            return "STRONG_ASK"

    def update(self, imbalance: float) -> dict:
        """
        Add new order imbalance observation and recompute entropy.
        
        Call this every time you get a DOM update from TopStepX.
        
        Args:
            imbalance: Order book imbalance from -1 to +1
                       (from TopStepXConnector.get_order_imbalance())
        
        Returns:
            {
                "entropy": float,             # Raw Shannon entropy (bits)
                "normalized_entropy": float,   # 0 (structured) to 1 (random)
                "regime": str,                # "structured", "transitional", "chaotic"
                "dominant_state": str,         # Most frequent state
                "state_distribution": dict,    # Probability per state
            }
        """
        state = self.classify_state(imbalance)
        self.state_buffer.append(state)

        if len(self.state_buffer) < 20:  # Minimum for reliable entropy
            return {
                "entropy": 0, "normalized_entropy": 0,
                "regime": "insufficient_data",
                "dominant_state": state,
                "state_distribution": {},
            }

        # Count state frequencies
        counts = Counter(self.state_buffer)
        total = len(self.state_buffer)

        # Compute probabilities
        probs = {s: counts.get(s, 0) / total for s in self.STATES}

        # Shannon Entropy: H = -Σ p(x) × log₂(p(x))
        self.entropy = 0.0
        for p in probs.values():
            if p > 0:
                self.entropy -= p * math.log2(p)

        # Normalize to [0, 1]
        self.normalized_entropy = self.entropy / self.max_entropy if self.max_entropy > 0 else 0

        # Regime classification
        if self.normalized_entropy < 0.4:
            self.regime = "structured"
        elif self.normalized_entropy < 0.7:
            self.regime = "transitional"
        else:
            self.regime = "chaotic"

        # Dominant state
        dominant = max(probs, key=probs.get)

        return {
            "entropy": self.entropy,
            "normalized_entropy": self.normalized_entropy,
            "regime": self.regime,
            "dominant_state": dominant,
            "state_distribution": probs,
        }

    def update_multi(self, imbalances: dict[str, float]) -> dict:
        """
        Update with imbalances from multiple futures simultaneously.
        Uses the average imbalance across NQ, ES, YM, RTY.
        
        Args:
            imbalances: {"NQ": 0.3, "ES": 0.2, "YM": -0.1, "RTY": 0.15}
        """
        if not imbalances:
            return self.update(0)
        avg_imbalance = sum(imbalances.values()) / len(imbalances)
        return self.update(avg_imbalance)

    def get_signal(self) -> dict:
        """Get the current Shannon Entropy signal for the inference engine."""
        return {
            "framework": "shannon_entropy",
            "value": self.normalized_entropy,
            "regime": self.regime,
            "interpretation": self._interpret(),
        }

    def _interpret(self) -> str:
        if self.regime == "structured":
            return "🎯 Order flow is STRUCTURED — signals are exploitable, high conviction"
        elif self.regime == "transitional":
            return "⚡ Order flow transitioning — watch for regime change"
        else:
            return "🌊 Order flow is CHAOTIC — reduce position size, signals are noisy"


# ─── Quick Test ─────────────────────────────────────────────────
if __name__ == "__main__":
    se = ShannonEntropy(window_size=100)

    print("=" * 60)
    print("SHANNON ENTROPY TEST")
    print("=" * 60)

    np.random.seed(42)

    # Scenario 1: Structured market (mostly bids — institutional buying)
    print("\n--- Structured Market (one-sided buying) ---")
    for i in range(100):
        imb = 0.3 + np.random.randn() * 0.1  # Biased toward bids
        result = se.update(np.clip(imb, -1, 1))
    print(f"  Entropy:    {result['entropy']:.4f} bits")
    print(f"  Normalized: {result['normalized_entropy']:.4f}")
    print(f"  Regime:     {result['regime']}")
    print(f"  Dominant:   {result['dominant_state']}")

    # Scenario 2: Random market (noise, no structure)
    print("\n--- Chaotic Market (random noise) ---")
    se2 = ShannonEntropy(window_size=100)
    for i in range(100):
        imb = np.random.randn() * 0.5  # Random
        result = se2.update(np.clip(imb, -1, 1))
    print(f"  Entropy:    {result['entropy']:.4f} bits")
    print(f"  Normalized: {result['normalized_entropy']:.4f}")
    print(f"  Regime:     {result['regime']}")
    print(f"  Dominant:   {result['dominant_state']}")
