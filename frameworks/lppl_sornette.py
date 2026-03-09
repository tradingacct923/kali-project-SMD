"""
LPPL Sornette Model — Crash Timing Predictor

The most validated crash prediction model in quantitative finance.
Detects log-periodic oscillations that accelerate before crashes.

Equation:
    ln[p(t)] = A + B(tc - t)^β + C(tc - t)^β · cos(ω · ln(tc - t) + φ)

Where:
    tc = critical time (predicted crash date)
    β  = power-law exponent (0.1 < β < 0.9)
    ω  = log-periodic frequency (typically 5 < ω < 15)
    φ  = phase parameter
    A  = ln(price at crash)
    B  = amplitude (negative for bubble)
    C  = log-periodic amplitude

Reference:
    Sornette, D. (2003). "Why Stock Markets Crash"
    Sornette, D. & Johansen, A. (2001). Significance of log-periodic precursors

Data source:
    NQ 5-year 1-min bars → resampled to daily close for model fitting
"""
import logging
import math
import numpy as np
from datetime import datetime, timedelta
from typing import Optional

logger = logging.getLogger(__name__)


class LPPLSornette:
    """
    Log-Periodic Power Law (LPPL) crash timing model.
    
    Fits the Sornette LPPL equation to price data to estimate:
      1. Critical time tc — when the bubble/crash resolves
      2. Confidence score — how well the LPPL pattern fits
      3. Days until critical — countdown to crash window
    
    Usage:
        model = LPPLSornette()
        model.fit(dates, prices)  # daily close prices
        signal = model.get_signal()
    """
    
    # Parameter bounds from literature (Sornette 2003, Johansen 2001)
    BOUNDS = {
        'beta_min': 0.1,    'beta_max': 0.9,
        'omega_min': 4.0,   'omega_max': 25.0,
        'tc_min_days': 5,   'tc_max_days': 252,  # 1 week to 1 year out
    }
    
    # Fit quality thresholds
    R2_GOOD = 0.85      # R² above this = credible fit
    R2_STRONG = 0.92    # R² above this = high confidence
    
    def __init__(self, window_days: int = 252, min_points: int = 60):
        """
        Args:
            window_days: Number of trading days to use for fitting (default 1 year)
            min_points:  Minimum data points required for fit
        """
        self.window_days = window_days
        self.min_points = min_points
        
        # Fit results
        self.params = None          # (tc, A, B, C, beta, omega, phi)
        self.r_squared = 0.0
        self.tc_date = None         # Estimated critical date
        self.days_to_tc = None      # Days until critical time
        self.fit_timestamp = None
        self.is_bubble = False      # True if pattern suggests bubble
        self.confidence = 0.0       # Overall confidence [0, 1]
        
        # Data
        self._dates = None
        self._prices = None
    
    @staticmethod
    def _lppl_func(t, tc, A, B, C, beta, omega, phi):
        """
        LPPL function: ln[p(t)] = A + B(tc-t)^β + C(tc-t)^β·cos(ω·ln(tc-t) + φ)
        
        t:     time index (days as float)
        tc:    critical time (crash point)
        A:     log-price at crash
        B:     power-law amplitude (negative for bubbles)
        C:     log-periodic amplitude
        beta:  power-law exponent
        omega: log-periodic frequency
        phi:   phase
        """
        dt = tc - t
        # Guard against negative or zero dt
        dt = np.maximum(dt, 1e-6)
        dt_beta = np.power(dt, beta)
        return A + B * dt_beta + C * dt_beta * np.cos(omega * np.log(dt) + phi)
    
    def _generate_initial_guesses(self, t, log_prices, n_guesses=20):
        """
        Generate multiple initial parameter guesses for robust fitting.
        Uses a coarse grid over (tc, beta, omega) and solves for (A, B, C)
        via linear regression (Filimonov & Sornette 2013 trick).
        """
        guesses = []
        T = len(t)
        
        tc_candidates = np.linspace(
            t[-1] + self.BOUNDS['tc_min_days'],
            t[-1] + self.BOUNDS['tc_max_days'],
            8
        )
        beta_candidates = [0.2, 0.4, 0.6, 0.8]
        omega_candidates = [6.0, 9.0, 13.0, 18.0]
        
        count = 0
        for tc in tc_candidates:
            for beta in beta_candidates:
                for omega in omega_candidates:
                    if count >= n_guesses:
                        break
                    
                    dt = tc - t
                    dt = np.maximum(dt, 1e-6)
                    dt_beta = np.power(dt, beta)
                    
                    # For each phi candidate, solve linear system for A, B, C
                    phi = 0.0
                    cos_term = np.cos(omega * np.log(dt) + phi)
                    
                    # Build design matrix: log_price = A + B*dt^β + C*dt^β*cos(...)
                    X = np.column_stack([
                        np.ones(T),
                        dt_beta,
                        dt_beta * cos_term
                    ])
                    
                    try:
                        # Solve via least squares
                        coeffs, _, _, _ = np.linalg.lstsq(X, log_prices, rcond=None)
                        A, B, C = coeffs
                        
                        # Only keep if B < 0 (bubble signature) and reasonable params
                        if B < 0 and abs(C) < abs(B):
                            guesses.append((tc, A, B, C, beta, omega, phi))
                            count += 1
                    except (np.linalg.LinAlgError, ValueError):
                        continue
        
        # If we got no bubble-signature guesses, generate generic ones
        if not guesses:
            mean_lp = np.mean(log_prices)
            for tc in tc_candidates[:3]:
                guesses.append((
                    tc, mean_lp, -0.1, 0.01, 0.5, 9.0, 0.0
                ))
        
        return guesses
    
    def fit(self, dates: list, prices: list) -> dict:
        """
        Fit LPPL model to daily close prices.
        
        Args:
            dates:  list of datetime objects (daily timestamps, EST)
            prices: list of float close prices
            
        Returns:
            dict with fit results: tc_date, days_to_tc, confidence, r_squared,
            is_bubble, params
        """
        try:
            from scipy.optimize import minimize
        except ImportError:
            logger.error("scipy required for LPPL fitting: pip install scipy")
            return self._empty_result()
        
        if len(prices) < self.min_points:
            logger.warning(f"LPPL: Need {self.min_points} points, got {len(prices)}")
            return self._empty_result()
        
        # Use last window_days of data
        n = min(len(prices), self.window_days)
        prices_arr = np.array(prices[-n:], dtype=float)
        dates_arr = dates[-n:]
        
        # Convert to time index (days from start)
        t = np.arange(n, dtype=float)
        log_prices = np.log(prices_arr)
        
        # Generate initial guesses
        guesses = self._generate_initial_guesses(t, log_prices, n_guesses=20)
        
        best_result = None
        best_cost = float('inf')
        
        for guess in guesses:
            try:
                result = self._fit_single(t, log_prices, guess)
                if result is not None and result['cost'] < best_cost:
                    # Validate parameter bounds
                    p = result['params']
                    tc, A, B, C, beta, omega, phi = p
                    
                    if (self.BOUNDS['beta_min'] <= beta <= self.BOUNDS['beta_max'] and
                        self.BOUNDS['omega_min'] <= omega <= self.BOUNDS['omega_max'] and
                        tc > t[-1]):
                        best_cost = result['cost']
                        best_result = result
            except Exception as e:
                continue
        
        if best_result is None:
            logger.warning("LPPL: No valid fit found")
            return self._empty_result()
        
        # Extract results
        tc, A, B, C, beta, omega, phi = best_result['params']
        self.params = best_result['params']
        self.r_squared = best_result['r_squared']
        
        # Convert tc to calendar date
        days_from_end = tc - t[-1]
        self.days_to_tc = max(1, int(days_from_end))
        last_date = dates_arr[-1]
        if isinstance(last_date, str):
            last_date = datetime.strptime(last_date.split()[0], '%m/%d/%Y')
        self.tc_date = last_date + timedelta(days=self.days_to_tc)
        
        # Determine if bubble pattern
        self.is_bubble = B < 0  # Negative B = super-exponential growth (bubble)
        
        # Confidence scoring
        self.confidence = self._compute_confidence(best_result)
        self.fit_timestamp = datetime.now()
        
        return {
            'tc_date': self.tc_date,
            'days_to_tc': self.days_to_tc,
            'confidence': self.confidence,
            'r_squared': self.r_squared,
            'is_bubble': self.is_bubble,
            'beta': beta,
            'omega': omega,
            'params': self.params,
        }
    
    def _fit_single(self, t, log_prices, initial_guess):
        """Fit LPPL with a single initial guess using scipy minimize."""
        from scipy.optimize import minimize
        
        tc0, A0, B0, C0, beta0, omega0, phi0 = initial_guess
        x0 = [tc0, A0, B0, C0, beta0, omega0, phi0]
        
        def cost(x):
            tc, A, B, C, beta, omega, phi = x
            # Enforce constraints
            if tc <= t[-1] or beta <= 0 or beta >= 1 or omega < 2:
                return 1e12
            try:
                predicted = self._lppl_func(t, tc, A, B, C, beta, omega, phi)
                residuals = log_prices - predicted
                return np.sum(residuals ** 2)
            except (FloatingPointError, ValueError, OverflowError):
                return 1e12
        
        bounds = [
            (t[-1] + 1, t[-1] + self.BOUNDS['tc_max_days']),  # tc
            (None, None),          # A
            (None, 0),             # B (negative for bubble)
            (None, None),          # C
            (self.BOUNDS['beta_min'], self.BOUNDS['beta_max']),   # beta
            (self.BOUNDS['omega_min'], self.BOUNDS['omega_max']), # omega
            (-2 * math.pi, 2 * math.pi),  # phi
        ]
        
        try:
            result = minimize(
                cost, x0, method='Nelder-Mead',
                options={'maxiter': 5000, 'xatol': 1e-8, 'fatol': 1e-8}
            )
            
            if not result.success and result.fun > 1e10:
                return None
            
            # Compute R²
            predicted = self._lppl_func(t, *result.x)
            ss_res = np.sum((log_prices - predicted) ** 2)
            ss_tot = np.sum((log_prices - np.mean(log_prices)) ** 2)
            r2 = 1 - ss_res / ss_tot if ss_tot > 0 else 0
            
            return {
                'params': tuple(result.x),
                'cost': result.fun,
                'r_squared': r2,
            }
        except Exception:
            return None
    
    def _compute_confidence(self, result):
        """
        Score confidence [0, 1] based on:
          1. R² goodness of fit
          2. Parameter plausibility (beta, omega in Sornette ranges)
          3. Days to critical time (too far = less reliable)
        """
        tc, A, B, C, beta, omega, phi = result['params']
        r2 = result['r_squared']
        
        # R² score (0-0.4)
        r2_score = min(0.4, max(0, (r2 - 0.5) / (0.95 - 0.5)) * 0.4)
        
        # Beta in sweet spot 0.2-0.8 (0-0.2)
        beta_center = abs(beta - 0.5) / 0.4  # 0 at center, 1 at edges
        beta_score = 0.2 * max(0, 1 - beta_center)
        
        # Omega in Sornette range 5-15 (0-0.2)
        if 5 <= omega <= 15:
            omega_score = 0.2
        elif 4 <= omega <= 25:
            omega_score = 0.1
        else:
            omega_score = 0.0
        
        # Time proximity: closer tc = more urgent/reliable (0-0.2)
        days = max(1, self.days_to_tc)
        if days <= 20:
            time_score = 0.2   # Very close — high urgency
        elif days <= 60:
            time_score = 0.15
        elif days <= 120:
            time_score = 0.10
        else:
            time_score = 0.05  # Far out — less reliable
        
        return min(1.0, r2_score + beta_score + omega_score + time_score)
    
    def _empty_result(self):
        """Return empty result dict when fitting fails."""
        return {
            'tc_date': None,
            'days_to_tc': None,
            'confidence': 0.0,
            'r_squared': 0.0,
            'is_bubble': False,
            'beta': None,
            'omega': None,
            'params': None,
        }
    
    def get_signal(self) -> dict:
        """
        Get the current LPPL signal for the inference engine.
        
        Returns dict compatible with signal_aggregator:
            name, value, confidence, alert_level, interpretation
        """
        if self.params is None or self.confidence < 0.2:
            return {
                'name': 'lppl_sornette',
                'value': 0.0,
                'confidence': 0.0,
                'alert_level': 'inactive',
                'interpretation': 'No valid LPPL fit — insufficient pattern',
                'tc_date': None,
                'days_to_tc': None,
            }
        
        # Value: urgency score (0-1) based on proximity and confidence
        proximity = max(0, 1 - (self.days_to_tc / 60))  # Max urgency within 60 days
        value = proximity * self.confidence
        
        # Alert level
        if self.days_to_tc <= 10 and self.confidence > 0.5:
            alert = 'critical'
            interp = (f"⚠️ CRASH WINDOW: {self.days_to_tc} days "
                     f"(~{self.tc_date.strftime('%b %d')}). "
                     f"R²={self.r_squared:.3f}, β={self.params[4]:.2f}, ω={self.params[5]:.1f}")
        elif self.days_to_tc <= 30 and self.confidence > 0.4:
            alert = 'elevated'
            interp = (f"LPPL bubble fit → critical ~{self.tc_date.strftime('%b %d')} "
                     f"({self.days_to_tc}d). Confidence={self.confidence:.0%}")
        elif self.is_bubble and self.confidence > 0.3:
            alert = 'watch'
            interp = (f"Bubble signature detected → tc ~{self.tc_date.strftime('%b %d')} "
                     f"({self.days_to_tc}d). R²={self.r_squared:.3f}")
        else:
            alert = 'normal'
            interp = f"LPPL: No imminent crash pattern. R²={self.r_squared:.3f}"
        
        return {
            'name': 'lppl_sornette',
            'value': round(value, 4),
            'confidence': round(self.confidence, 4),
            'alert_level': alert,
            'interpretation': interp,
            'tc_date': self.tc_date,
            'days_to_tc': self.days_to_tc,
            'r_squared': round(self.r_squared, 4),
            'is_bubble': self.is_bubble,
        }


def load_nq_daily(csv_path: str = r"D:\nq data  5 years quant.csv"):
    """
    Load NQ 1-min CSV and resample to daily close for LPPL fitting.
    Timestamps are EST (US Eastern).
    
    Returns: (dates_list, prices_list) of daily close prices
    """
    try:
        import pandas as pd
    except ImportError:
        logger.error("pandas required: pip install pandas")
        return [], []
    
    df = pd.read_csv(csv_path)
    
    # Parse datetime — format: "01/01/2020 18:00:00"
    df['dt'] = pd.to_datetime(df['DateTime'], format='%m/%d/%Y %H:%M:%S')
    df.set_index('dt', inplace=True)
    df.sort_index(inplace=True)
    
    # Filter to RTH only (9:30 AM - 4:00 PM ET) for cleaner daily close
    rth = df.between_time('09:30', '16:00')
    
    # Resample to daily close
    daily = rth['Close'].resample('B').last().dropna()
    
    dates = daily.index.to_pydatetime().tolist()
    prices = daily.values.tolist()
    
    logger.info(f"LPPL: Loaded {len(prices)} daily bars from {dates[0]} to {dates[-1]}")
    return dates, prices

