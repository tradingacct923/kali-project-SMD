"""
Power-Law Tail Alpha (α) — Fat Tail Risk Monitor

Monitors the tail exponent α of the return distribution in real-time.
When α drops below 3, tail risk is exploding even if VIX looks calm.

Method: Hill Estimator
    α̂ = (1/k) Σᵢ₌₁ᵏ [ln(X_(n-i+1)) - ln(X_(n-k))]⁻¹

    Where X_(1) ≤ ... ≤ X_(n) are ordered absolute returns
    and k is the number of tail observations used.

Interpretation:
    α < 2:   Extremely fat tails — Lévy regime, infinite variance
    α ≈ 2-3: Very fat tails — crash regime, use extreme caution
    α ≈ 3:   Pareto tails — normal fat tails for financial data
    α ≈ 3-4: Moderate tails — typical calm regime
    α > 4:   Thin tails — unusually calm, consider selling vol

Historical context:
    - Normal markets: α ≈ 3-4
    - Pre-crash (2008, 2020): α dropped to 1.5-2.5
    - Flash crashes: α can spike below 2 in minutes

Data source:
    NQ 5-year 1-min bars (or live Tradier price stream)
"""
import logging
import math
import numpy as np
from collections import deque
from typing import Optional

logger = logging.getLogger(__name__)


class PowerLawTail:
    """
    Real-time power-law tail exponent (α) monitor.
    
    Uses the Hill estimator on rolling windows of returns.
    Tracks α for both left tail (crashes) and right tail (melt-ups)
    separately, giving directional tail risk awareness.
    
    Usage:
        monitor = PowerLawTail(window_size=500)
        
        # Feed 1-min or daily returns
        monitor.update(price)  # updates internal return buffer
        signal = monitor.get_signal()
    """
    
    # Tail classification thresholds
    ALPHA_EXTREME = 2.0    # Lévy regime — infinite variance
    ALPHA_CRASH   = 2.5    # Crash regime
    ALPHA_FAT     = 3.0    # Normal fat tails
    ALPHA_NORMAL  = 4.0    # Moderate tails
    # Above 4.0 = thin/calm tails
    
    def __init__(self, window_size: int = 500, tail_fraction: float = 0.10,
                 min_observations: int = 100):
        """
        Args:
            window_size:     Number of returns to keep in rolling window
            tail_fraction:   Fraction of data to use as tail (default 10%)
            min_observations: Minimum returns before computing α
        """
        self.window_size = window_size
        self.tail_fraction = tail_fraction
        self.min_observations = min_observations
        
        # Internal state
        self._prices = deque(maxlen=window_size + 1)
        self._returns = deque(maxlen=window_size)
        
        # Results
        self.alpha_left = None      # Left tail (crash) exponent
        self.alpha_right = None     # Right tail (melt-up) exponent
        self.alpha_combined = None  # Both tails combined
        self.alpha_history = deque(maxlen=500)  # Track α over time
        self.regime = 'unknown'
    
    def update(self, price: float) -> Optional[dict]:
        """
        Add a new price observation, compute log return, and update α.
        
        Args:
            price: Current price (1-min or daily close)
            
        Returns:
            dict with current tail risk assessment, or None if insufficient data
        """
        self._prices.append(price)
        
        if len(self._prices) < 2:
            return None
        
        # Compute log return
        prev = self._prices[-2]
        if prev <= 0 or price <= 0:
            return None
        
        log_return = math.log(price / prev)
        self._returns.append(log_return)
        
        if len(self._returns) < self.min_observations:
            return None
        
        # Compute tail exponents
        returns = np.array(self._returns)
        abs_returns = np.abs(returns)
        
        # Number of tail observations
        k = max(10, int(len(returns) * self.tail_fraction))
        
        # Combined α (both tails)
        self.alpha_combined = self._hill_estimator(abs_returns, k)
        
        # Left tail only (negative returns = crashes)
        neg_returns = np.abs(returns[returns < 0])
        if len(neg_returns) > 20:
            k_left = max(5, int(len(neg_returns) * self.tail_fraction))
            self.alpha_left = self._hill_estimator(neg_returns, k_left)
        
        # Right tail only (positive returns = melt-ups)
        pos_returns = returns[returns > 0]
        if len(pos_returns) > 20:
            k_right = max(5, int(len(pos_returns) * self.tail_fraction))
            self.alpha_right = self._hill_estimator(pos_returns, k_right)
        
        # Track history
        if self.alpha_combined is not None:
            self.alpha_history.append(self.alpha_combined)
        
        # Classify regime
        self.regime = self._classify_regime()
        
        return {
            'alpha_combined': self.alpha_combined,
            'alpha_left': self.alpha_left,
            'alpha_right': self.alpha_right,
            'regime': self.regime,
            'n_returns': len(self._returns),
        }
    
    @staticmethod
    def _hill_estimator(data: np.ndarray, k: int) -> Optional[float]:
        """
        Hill estimator for the tail exponent α.
        
        α̂ = k / Σᵢ₌₁ᵏ [ln(X_(n-i+1)) - ln(X_(n-k))]
        
        Args:
            data: Array of absolute values (|returns|)
            k:    Number of upper-order statistics to use
            
        Returns:
            Estimated tail exponent α, or None on error
        """
        if len(data) < k + 1 or k < 2:
            return None
        
        # Sort ascending, take top k+1
        sorted_data = np.sort(data)
        
        # Use the top k order statistics
        # X_(n-k) is the threshold, X_(n-i+1) for i=1..k are the exceedances
        threshold = sorted_data[-(k + 1)]
        
        if threshold <= 0:
            # Can't take log of zero — filter zeros
            sorted_data = sorted_data[sorted_data > 0]
            if len(sorted_data) < k + 1:
                return None
            threshold = sorted_data[-(k + 1)]
            if threshold <= 0:
                return None
        
        tail = sorted_data[-k:]
        log_ratios = np.log(tail) - np.log(threshold)
        
        # Filter out any zeros or negatives (can happen with ties)
        log_ratios = log_ratios[log_ratios > 0]
        
        if len(log_ratios) < 2:
            return None
        
        # Hill estimator: inverse of mean log exceedance
        mean_log = np.mean(log_ratios)
        if mean_log <= 0:
            return None
        
        alpha = 1.0 / mean_log
        
        # Sanity bound — α outside [0.5, 10] is likely noise
        return max(0.5, min(10.0, alpha))
    
    def _classify_regime(self) -> str:
        """Classify the current tail risk regime."""
        if self.alpha_combined is None:
            return 'unknown'
        
        a = self.alpha_combined
        
        if a < self.ALPHA_EXTREME:
            return 'levy_extreme'       # Infinite variance — maximum danger
        elif a < self.ALPHA_CRASH:
            return 'crash_fat_tails'    # Crash-like tail risk
        elif a < self.ALPHA_FAT:
            return 'elevated'           # Fatter than normal
        elif a < self.ALPHA_NORMAL:
            return 'normal'             # Typical financial tails
        else:
            return 'thin_calm'          # Unusually calm — consider selling vol
    
    def get_trend(self) -> Optional[str]:
        """
        Detect if α is trending down (worsening) or up (improving).
        Uses last 50 observations.
        """
        if len(self.alpha_history) < 50:
            return None
        
        recent = np.array(list(self.alpha_history)[-50:])
        
        # Simple linear regression slope
        x = np.arange(len(recent))
        slope = np.polyfit(x, recent, 1)[0]
        
        if slope < -0.01:
            return 'deteriorating'  # α falling — tails getting fatter
        elif slope > 0.01:
            return 'improving'      # α rising — tails thinning
        else:
            return 'stable'
    
    def get_signal(self) -> dict:
        """
        Get current tail risk signal for the inference engine.
        
        Returns dict compatible with signal_aggregator:
            name, value, confidence, alert_level, interpretation
        """
        if self.alpha_combined is None:
            return {
                'name': 'powerlaw_tail',
                'value': 0.0,
                'confidence': 0.0,
                'alert_level': 'inactive',
                'interpretation': 'Insufficient data for tail estimation',
            }
        
        # Value: normalized score (0 = safe, 1 = extreme fat tails)
        # Map alpha: 5 → 0.0 (safe), 2 → 1.0 (danger)
        value = max(0, min(1.0, (4.0 - self.alpha_combined) / 2.0))
        
        # Confidence based on sample size and stability
        n = len(self._returns)
        size_conf = min(1.0, n / self.window_size)
        
        # Stability: low variance in recent α = more confident
        if len(self.alpha_history) > 20:
            recent_std = np.std(list(self.alpha_history)[-20:])
            stability = max(0, 1 - recent_std / 2.0)  # Low std = high stability
        else:
            stability = 0.5
        
        confidence = size_conf * 0.6 + stability * 0.4
        
        trend = self.get_trend()
        
        # Alert level
        alpha = self.alpha_combined
        if alpha < self.ALPHA_EXTREME:
            alert = 'critical'
            interp = (f"🔴 LÉVY REGIME: α={alpha:.2f} — infinite variance territory. "
                     f"Left tail α={self.alpha_left:.2f if self.alpha_left else '?'}. "
                     f"Extreme crash risk. Trend: {trend or '?'}")
        elif alpha < self.ALPHA_CRASH:
            alert = 'elevated'
            interp = (f"⚠️ CRASH-LEVEL TAILS: α={alpha:.2f} — below Pareto threshold. "
                     f"Left={self.alpha_left:.2f if self.alpha_left else '?'}, "
                     f"Right={self.alpha_right:.2f if self.alpha_right else '?'}. "
                     f"Trend: {trend or '?'}")
        elif alpha < self.ALPHA_FAT:
            alert = 'watch'
            interp = (f"Elevated tails: α={alpha:.2f} (normal ≈ 3-4). "
                     f"Trend: {trend or 'stable'}")
        elif alpha < self.ALPHA_NORMAL:
            alert = 'normal'
            interp = f"Normal regime: α={alpha:.2f}. Typical financial fat tails."
        else:
            alert = 'calm'
            interp = (f"Thin tails: α={alpha:.2f} — unusually calm. "
                     f"Consider selling vol. Trend: {trend or 'stable'}")
        
        return {
            'name': 'powerlaw_tail',
            'value': round(value, 4),
            'confidence': round(confidence, 4),
            'alert_level': alert,
            'interpretation': interp,
            'alpha_combined': round(self.alpha_combined, 4) if self.alpha_combined else None,
            'alpha_left': round(self.alpha_left, 4) if self.alpha_left else None,
            'alpha_right': round(self.alpha_right, 4) if self.alpha_right else None,
            'regime': self.regime,
            'trend': trend,
        }


def analyze_nq_tails(csv_path: str = r"D:\nq data  5 years quant.csv",
                     window_minutes: int = 500):
    """
    Run Power-Law Tail analysis on NQ historical data.
    
    Returns the final signal from the most recent window.
    """
    try:
        import pandas as pd
    except ImportError:
        logger.error("pandas required: pip install pandas")
        return None
    
    df = pd.read_csv(csv_path)
    df['dt'] = pd.to_datetime(df['DateTime'], format='%m/%d/%Y %H:%M:%S')
    df.sort_values('dt', inplace=True)
    
    # Filter to RTH (9:30-16:00 ET)
    df.set_index('dt', inplace=True)
    rth = df.between_time('09:30', '16:00')
    
    prices = rth['Close'].values
    
    monitor = PowerLawTail(window_size=window_minutes, tail_fraction=0.10)
    
    result = None
    for p in prices:
        r = monitor.update(p)
        if r is not None:
            result = r
    
    signal = monitor.get_signal()
    logger.info(f"Power-Law Tail: α={signal.get('alpha_combined')}, "
                f"regime={signal.get('regime')}, trend={signal.get('trend')}")
    return signal
