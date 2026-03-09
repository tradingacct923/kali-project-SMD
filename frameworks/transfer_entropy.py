"""
Transfer Entropy (TE) — VIX → NQ Causality Detector

The #1 alpha framework. Measures DIRECTIONAL information flow between
VIX and NQ price series. Unlike correlation (which is symmetric),
Transfer Entropy tells you WHO is causing WHOM.

Before crashes:
  - TE(VIX → NQ) spikes significantly above TE(NQ → VIX)
  - Information starts flowing FROM VIX TO NQ (VIX leads)
  - This happens 10-60 minutes before the crash materializes in NQ

Normal markets:
  - TE is roughly symmetric — both feed each other
  - Low absolute values

Data sources:
  - VIX price: Tradier WebSocket (real-time)
  - NQ mid-price: TopStepX Level 2 (real-time)
"""
import logging
import math
from collections import deque
from typing import Optional

import numpy as np

logger = logging.getLogger(__name__)


class TransferEntropy:
    """
    Compute Transfer Entropy between two time series in real-time.
    
    TE(X→Y) = Σ p(y_{t+1}, y_t, x_t) × log[ p(y_{t+1}|y_t,x_t) / p(y_{t+1}|y_t) ]
    
    High TE(VIX→NQ) = VIX is CAUSING NQ moves = danger
    High TE(NQ→VIX) = NQ is CAUSING VIX moves = normal
    """

    def __init__(self, window_size: int = 60, lag: int = 1, n_bins: int = 5):
        """
        Args:
            window_size: Number of data points in rolling window
            lag: Time lag for transfer entropy (in samples)
            n_bins: Bins for discretization of continuous price data
        """
        self.window_size = window_size
        self.lag = lag
        self.n_bins = n_bins
        
        # Rolling buffers for VIX and NQ price changes
        self.vix_buffer = deque(maxlen=window_size + lag + 1)
        self.nq_buffer = deque(maxlen=window_size + lag + 1)
        
        # Latest computed values
        self.te_vix_to_nq: float = 0.0
        self.te_nq_to_vix: float = 0.0
        self.net_te: float = 0.0  # Positive = VIX causing NQ
        self.alert_level: str = "normal"

    def update(self, vix_price: float, nq_price: float) -> dict:
        """
        Add new price observations and recompute Transfer Entropy.
        
        Call this every time you get a new VIX quote (from Tradier)
        and NQ mid-price (from TopStepX).
        
        Args:
            vix_price: Current VIX level
            nq_price: Current NQ mid-price
        
        Returns:
            {
                "te_vix_to_nq": float,
                "te_nq_to_vix": float,
                "net_te": float,        # Positive = VIX leading (danger)
                "causality_ratio": float, # TE(VIX→NQ) / TE(NQ→VIX)
                "alert_level": str,      # "normal", "elevated", "critical"
            }
        """
        self.vix_buffer.append(vix_price)
        self.nq_buffer.append(nq_price)

        # Need enough data for computation
        if len(self.vix_buffer) < self.window_size + self.lag + 1:
            return {
                "te_vix_to_nq": 0, "te_nq_to_vix": 0,
                "net_te": 0, "causality_ratio": 1.0,
                "alert_level": "insufficient_data",
                "data_points": len(self.vix_buffer),
                "required": self.window_size + self.lag + 1,
            }

        # Convert to returns (log changes)
        vix = np.array(list(self.vix_buffer))
        nq = np.array(list(self.nq_buffer))
        
        vix_returns = np.diff(np.log(np.maximum(vix, 1e-6)))
        nq_returns = np.diff(np.log(np.maximum(nq, 1e-6)))

        # Compute TE in both directions
        self.te_vix_to_nq = self._compute_te(vix_returns, nq_returns)
        self.te_nq_to_vix = self._compute_te(nq_returns, vix_returns)
        self.net_te = self.te_vix_to_nq - self.te_nq_to_vix

        # Causality ratio
        denom = max(self.te_nq_to_vix, 1e-10)
        causality_ratio = self.te_vix_to_nq / denom

        # Alert classification
        if causality_ratio > 3.0:
            self.alert_level = "critical"
        elif causality_ratio > 1.5:
            self.alert_level = "elevated"
        else:
            self.alert_level = "normal"

        return {
            "te_vix_to_nq": self.te_vix_to_nq,
            "te_nq_to_vix": self.te_nq_to_vix,
            "net_te": self.net_te,
            "causality_ratio": causality_ratio,
            "alert_level": self.alert_level,
        }

    def _compute_te(self, source: np.ndarray, target: np.ndarray) -> float:
        """
        Compute Transfer Entropy from source → target.
        
        TE(X→Y) quantifies how much knowing X's past reduces
        uncertainty about Y's future, beyond what Y's own past provides.
        
        Uses histogram-based estimation with adaptive binning.
        """
        n = min(len(source), len(target)) - self.lag
        if n < 10:
            return 0.0

        # Discretize using quantile-based binning (more robust than equal-width)
        src = source[:n]
        tgt_past = target[:n]
        tgt_future = target[self.lag:n + self.lag]

        src_bins = self._quantile_bin(src)
        tgt_past_bins = self._quantile_bin(tgt_past)
        tgt_future_bins = self._quantile_bin(tgt_future)

        # Compute joint and conditional probabilities via counting
        te = 0.0
        total = len(src_bins)

        # Build joint distributions
        # p(y_{t+1}, y_t, x_t) — full joint
        # p(y_{t+1}, y_t) — target joint
        # p(y_t, x_t) — source-target past joint
        # p(y_t) — target past marginal

        joint_3 = {}  # (y_future, y_past, x_past) → count
        joint_yx = {}  # (y_future, y_past) → count
        joint_xy = {}  # (y_past, x_past) → count
        marg_y = {}    # y_past → count

        for i in range(total):
            yf = tgt_future_bins[i]
            yp = tgt_past_bins[i]
            xp = src_bins[i]

            key3 = (yf, yp, xp)
            key_yx = (yf, yp)
            key_xy = (yp, xp)

            joint_3[key3] = joint_3.get(key3, 0) + 1
            joint_yx[key_yx] = joint_yx.get(key_yx, 0) + 1
            joint_xy[key_xy] = joint_xy.get(key_xy, 0) + 1
            marg_y[yp] = marg_y.get(yp, 0) + 1

        # Compute TE
        for key3, count3 in joint_3.items():
            yf, yp, xp = key3
            p_joint3 = count3 / total
            p_yf_given_yp_xp = count3 / max(joint_xy.get((yp, xp), 1), 1)
            p_yf_given_yp = joint_yx.get((yf, yp), 1) / max(marg_y.get(yp, 1), 1)

            if p_yf_given_yp > 0 and p_yf_given_yp_xp > 0:
                te += p_joint3 * math.log2(p_yf_given_yp_xp / p_yf_given_yp)

        return max(te, 0)  # TE is non-negative by definition

    def _quantile_bin(self, data: np.ndarray) -> np.ndarray:
        """Discretize continuous data into bins using quantiles."""
        try:
            quantiles = np.linspace(0, 100, self.n_bins + 1)
            edges = np.percentile(data, quantiles)
            # Handle edge case where all values are the same
            if edges[0] == edges[-1]:
                return np.zeros(len(data), dtype=int)
            binned = np.digitize(data, edges[1:-1])
            return binned
        except Exception:
            return np.zeros(len(data), dtype=int)

    def get_signal(self) -> dict:
        """Get the current Transfer Entropy signal for the inference engine."""
        return {
            "framework": "transfer_entropy",
            "value": self.net_te,
            "alert": self.alert_level,
            "interpretation": self._interpret(),
        }

    def _interpret(self) -> str:
        if self.alert_level == "critical":
            return "⚠️ VIX is STRONGLY CAUSING NQ moves — crash risk HIGH"
        elif self.alert_level == "elevated":
            return "⚡ VIX causality elevated — watch for breakdown"
        else:
            return "✅ Normal bidirectional flow — no directional dominance"


# ─── Quick Test ─────────────────────────────────────────────────
if __name__ == "__main__":
    te = TransferEntropy(window_size=50, lag=1, n_bins=5)

    # Simulate: normal market (random walk both)
    np.random.seed(42)
    vix_base, nq_base = 20.0, 20000.0

    print("=" * 60)
    print("TRANSFER ENTROPY TEST")
    print("=" * 60)

    # Phase 1: Normal market — VIX and NQ move independently
    print("\n--- Normal Market (independent) ---")
    for i in range(70):
        vix_base += np.random.randn() * 0.1
        nq_base += np.random.randn() * 10
        result = te.update(max(vix_base, 10), max(nq_base, 18000))
    print(f"  TE(VIX→NQ): {result['te_vix_to_nq']:.6f}")
    print(f"  TE(NQ→VIX): {result['te_nq_to_vix']:.6f}")
    print(f"  Net TE:     {result['net_te']:.6f}")
    print(f"  Alert:      {result['alert_level']}")

    # Phase 2: Pre-crash — VIX starts causing NQ moves
    print("\n--- Pre-Crash (VIX leads NQ) ---")
    te2 = TransferEntropy(window_size=50, lag=1, n_bins=5)
    vix_base, nq_base = 20.0, 20000.0
    for i in range(70):
        vix_shock = np.random.randn() * 0.3
        vix_base += vix_shock
        # NQ follows VIX with a lag — this creates TE(VIX→NQ) > TE(NQ→VIX)
        nq_base -= vix_shock * 100 + np.random.randn() * 5
        result = te2.update(max(vix_base, 10), max(nq_base, 18000))
    print(f"  TE(VIX→NQ): {result['te_vix_to_nq']:.6f}")
    print(f"  TE(NQ→VIX): {result['te_nq_to_vix']:.6f}")
    print(f"  Net TE:     {result['net_te']:.6f}")
    print(f"  Ratio:      {result['causality_ratio']:.2f}x")
    print(f"  Alert:      {result['alert_level']}")
