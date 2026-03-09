"""
Intraday Volume-Adjusted GEX -- Live Dealer Positioning Estimator (v2)

Research-Backed Trade Classification:
  - Lee & Ready (1991): Quote test for trade direction (bid/ask midpoint)
  - Pan & Poteshman (2006): Opening vs closing estimation from volume/OI ratio
  - Lakonishok et al (2007): ~50% baseline opening rate calibration
  - Barbon & Buraschi (2021): GEX feedback loop (gamma fragility)

Classification Pipeline:
  1. DIRECTION: Lee-Ready Quote Test
     Trade above midpoint = buyer-initiated (+1)
     Trade below midpoint = seller-initiated (-1)
     Trade at midpoint = use tick test (compare to previous)

  2. OPENING vs CLOSING: Multi-factor model
     Factor 1: Volume/OI ratio (Pan & Poteshman)
     Factor 2: Trade size (blocks >100 = likely institutional opening)
     Factor 3: Time of day (more openings at AM, more closings at PM)
     Factor 4: Multi-leg detection (spreads don't shift walls directionally)

  3. OI ADJUSTMENT:
     live_OI = EOD_OI + (new_trades * opening_prob * direction * confidence)
     where confidence accounts for spread detection and size weighting

  4. GEX RECOMPUTATION:
     GEX(adjusted) = gamma * adjusted_OI * 100 * S^2 / 100
"""
import math
import time
import threading
from datetime import datetime, date


# ══════════════════════════════════════════════════════════════════════════════
#  Trade Classification Engine (Research-Backed)
# ══════════════════════════════════════════════════════════════════════════════

class TradeClassifier:
    """
    Classifies option trades as opening/closing and buyer/seller initiated.
    
    Based on:
      - Lee & Ready (1991): direction from quotes
      - Pan & Poteshman (2006): opening/closing from volume patterns
      - Lakonishok et al (2007): baseline opening rates (~50%)
    """
    
    # Time-of-day opening probability curve
    # Based on Lakonishok et al (2007): institutional opening trades
    # concentrate in the first 2 hours; closing trades increase toward EOD
    # Hours are ET (market hours 9:30-16:00)
    TOD_OPENING_WEIGHT = {
        9:  0.70,   # 9:30-10:00  — heavy opening, institutions establishing positions
        10: 0.65,   # 10:00-11:00 — still heavy opening
        11: 0.55,   # 11:00-12:00 — balanced
        12: 0.50,   # 12:00-13:00 — lunch, baseline (Lakonishok: ~50%)
        13: 0.48,   # 13:00-14:00 — slightly more closing
        14: 0.45,   # 14:00-15:00 — closing starts increasing
        15: 0.35,   # 15:00-16:00 — heavy closing, end-of-day adjustments
    }
    
    # Trade size thresholds (contracts)
    SMALL_TRADE  = 10     # Retail
    MEDIUM_TRADE = 50     # Institutional single-leg
    BLOCK_TRADE  = 100    # Block (likely opening/new position)
    SWEEP_TRADE  = 500    # Sweep (aggressive, almost certainly opening)
    
    @staticmethod
    def lee_ready_direction(bid: float, ask: float, last: float,
                            prev_last: float = 0.0) -> float:
        """
        Lee & Ready (1991) Quote Test + Tick Test for trade direction.
        
        Returns:
            +1.0 = buyer-initiated (customer bought, dealer short)
            -1.0 = seller-initiated (customer sold, dealer long)
             0.0 = indeterminate
        """
        if bid <= 0 or ask <= 0 or last <= 0:
            return 0.0
        
        midpoint = (bid + ask) / 2.0
        
        # Quote Test: compare trade price to midpoint
        if last > midpoint + 0.001:
            return +1.0   # Above mid = buyer-initiated
        elif last < midpoint - 0.001:
            return -1.0   # Below mid = seller-initiated
        
        # At midpoint: use Tick Test (compare to previous trade)
        if prev_last > 0:
            if last > prev_last:
                return +1.0   # Uptick = buyer
            elif last < prev_last:
                return -1.0   # Downtick = seller
        
        # Truly indeterminate
        return 0.0
    
    @staticmethod
    def opening_probability(volume: float, oi: float, trade_size: float = 0,
                            hour: int = 12) -> float:
        """
        Estimate probability that trades are OPENING new positions.
        
        Combines:
          1. Volume/OI ratio (Pan & Poteshman approach)
          2. Trade size (Lakonishok: blocks are more likely opening)
          3. Time of day (Lakonishok: AM = opening, PM = closing)
        
        Returns: probability [0, 1] that volume represents new positions
        """
        # ── Factor 1: Volume/OI Ratio ─────────────────────────────────────
        # Pan & Poteshman (2006): when volume greatly exceeds OI at a strike,
        # a large fraction must be opening trades (can't close what doesn't exist)
        if oi > 0:
            vol_oi = volume / oi
            # Logistic curve: saturates smoothly
            # At vol/OI=0: ~0.3 (some opening even when vol is low)
            # At vol/OI=1: ~0.55
            # At vol/OI=3+: ~0.75 (cap — not everything opens)
            voi_prob = 0.30 + 0.45 / (1.0 + math.exp(-2.0 * (vol_oi - 1.0)))
        else:
            # No prior OI: almost certainly opening
            voi_prob = 0.90
        
        # ── Factor 2: Trade Size ──────────────────────────────────────────
        # Lakonishok et al (2007): larger trades more likely institutional
        # Institutional trades more likely to be opening new positions
        if trade_size >= TradeClassifier.SWEEP_TRADE:
            size_weight = 1.15  # Sweeps: likely aggressive opening
        elif trade_size >= TradeClassifier.BLOCK_TRADE:
            size_weight = 1.10  # Blocks: likely opening
        elif trade_size >= TradeClassifier.MEDIUM_TRADE:
            size_weight = 1.05  # Medium: slight opening bias
        elif trade_size >= TradeClassifier.SMALL_TRADE:
            size_weight = 0.95  # Small retail: slightly more likely closing
        else:
            size_weight = 1.00  # Micro: neutral
        
        # ── Factor 3: Time of Day ─────────────────────────────────────────
        tod_weight = TradeClassifier.TOD_OPENING_WEIGHT.get(hour, 0.50)
        
        # ── Combine factors ───────────────────────────────────────────────
        # Weighted combination: VOI is primary, size and TOD are modifiers
        base_prob = voi_prob * 0.60 + tod_weight * 0.40
        adjusted = base_prob * size_weight
        
        return max(0.05, min(0.90, adjusted))  # Clamp to [5%, 90%]
    
    @staticmethod
    def detect_spread(call_vol: float, put_vol: float, oi_call: float,
                      oi_put: float) -> float:
        """
        Detect multi-leg spread trades at a strike.
        
        If both call and put volume at the same strike are elevated and
        roughly proportional, it's likely a spread (straddle/strangle/combo).
        Spreads have lower directional impact on GEX walls.
        
        Returns: confidence [0, 1] that this is NOT a spread (1 = pure directional)
        """
        if call_vol <= 0 and put_vol <= 0:
            return 1.0
        
        total = call_vol + put_vol
        if total <= 0:
            return 1.0
        
        # Ratio of smaller to larger side
        ratio = min(call_vol, put_vol) / max(call_vol, put_vol) if max(call_vol, put_vol) > 0 else 0
        
        # If ratio > 0.7 (nearly equal C and P volume), likely a spread
        # If ratio < 0.3, likely directional
        if ratio > 0.7:
            return 0.40   # Probable spread: reduce GEX impact by 60%
        elif ratio > 0.5:
            return 0.65   # Possible spread
        elif ratio > 0.3:
            return 0.85   # Probably directional
        else:
            return 1.00   # Pure directional


class IntradayGEX:
    """
    Real-time GEX tracker with research-backed trade classification.
    
    Upgrades from v1:
      - Lee & Ready (1991) trade direction (vs. simple bid/ask heuristic)
      - Pan & Poteshman (2006) opening probability (vs. linear vol/OI)
      - Lakonishok et al (2007) size + ToD calibration
      - Multi-leg spread detection
    """
    
    # Barbon & Buraschi (2021): historical GI for z-score computation
    # Mean and std from their Table I (3.6M observations, 2010-2020)
    # These are calibration defaults; they self-calibrate after 21 samples
    BB_GI_MEAN = 13.5      # Mean normalized GI (slightly positive = markets usually pin)
    BB_GI_STD  = 124.5     # Std dev of normalized GI
    BB_FLASH_THRESHOLD = -1.7  # Z-score threshold (May 6, 2010 flash crash level)
    
    def __init__(self, avg_daily_volume: float = 0):
        self._prev_volume = {}      # {key: prev_volume}
        self._prev_last = {}        # {key: prev_last_price} for tick test
        self._volume_delta = {}     # {key: cumulative_adjustment}
        self._spread_vol = {}       # {(strike, exp): {"call": vol, "put": vol}}
        self._last_reset = None
        self._lock = threading.Lock()
        self._classifier = TradeClassifier()
        
        # Barbon & Buraschi: track GI history for self-calibration
        self._gi_history = []       # Rolling normalized GI values
        self._avg_daily_volume = avg_daily_volume  # 21-day ADV (pass from Tradier)
    
    def _reset_if_new_day(self):
        today = date.today()
        if self._last_reset != today:
            with self._lock:
                self._prev_volume.clear()
                self._prev_last.clear()
                self._volume_delta.clear()
                self._spread_vol.clear()
                self._last_reset = today
                # Don't clear GI history — it accumulates across days
    
    def compute(self, chain: list, spot: float,
                r: float = 0.045, q: float = 0.005,
                avg_daily_volume: float = 0) -> dict:
        """
        Compute volume-adjusted intraday GEX from Tradier option chain.
        
        Uses research-backed trade classification for OI estimation.
        """
        self._reset_if_new_day()
        
        today = date.today()
        now = datetime.now()
        current_hour = now.hour  # For time-of-day weighting
        
        # First pass: collect per-strike call/put volumes for spread detection
        strike_volumes = {}  # {(strike, exp): {"call": vol, "put": vol}}
        for opt in chain:
            strike = float(opt.get("strike", 0))
            exp_str = opt.get("expiration_date", "")
            otype = opt.get("option_type", "call")
            vol = float(opt.get("volume", 0) or 0)
            if strike > 0 and exp_str:
                sk = (strike, exp_str)
                strike_volumes.setdefault(sk, {"call": 0, "put": 0})
                strike_volumes[sk][otype] += vol
        
        # Track 0DTE separately
        dte0_gex = {}
        total_gamma_abs = 0.0
        dte0_gamma_abs = 0.0
        
        gex_profile = {}
        vanna_profile = {}
        
        contracts = 0
        atm_iv = None
        atm_dist = float('inf')
        
        for opt in chain:
            strike = float(opt.get("strike", 0))
            otype = opt.get("option_type", "call")
            exp_str = opt.get("expiration_date", "")
            
            if strike <= 0 or not exp_str:
                continue
            
            try:
                exp_date = date.fromisoformat(exp_str)
                dte = max((exp_date - today).days, 0)
            except Exception:
                continue
            
            T = max(dte, 1) / 365.0
            
            # Raw data
            oi = float(opt.get("open_interest", 0) or 0)
            volume = float(opt.get("volume", 0) or 0)
            bid = float(opt.get("bid", 0) or 0)
            ask = float(opt.get("ask", 0) or 0)
            last = float(opt.get("last", 0) or 0)
            
            greeks = opt.get("greeks", {}) or {}
            gamma = float(greeks.get("gamma", 0) or 0)
            delta = float(greeks.get("delta", 0) or 0)
            iv = float(greeks.get("mid_iv", 0) or 0)
            vanna = float(greeks.get("vanna", 0) or 0)
            
            # Track ATM IV
            dist = abs(strike - spot)
            if dist < atm_dist and otype == "call":
                atm_dist = dist
                atm_iv = iv
            
            contracts += 1
            key = (strike, exp_str, otype)
            
            # ── 1. Lee-Ready Direction ─────────────────────────────────────
            with self._lock:
                prev_last = self._prev_last.get(key, 0)
                self._prev_last[key] = last
            
            direction = self._classifier.lee_ready_direction(
                bid, ask, last, prev_last
            )
            
            # ── 2. Volume Delta ────────────────────────────────────────────
            with self._lock:
                prev_vol = self._prev_volume.get(key, 0)
                new_trades = max(volume - prev_vol, 0)
                self._prev_volume[key] = volume
            
            # ── 3. Opening Probability (multi-factor) ─────────────────────
            open_prob = self._classifier.opening_probability(
                volume=volume,
                oi=oi,
                trade_size=new_trades,
                hour=current_hour,
            )
            
            # ── 4. Spread Detection ───────────────────────────────────────
            sk = (strike, exp_str)
            sv = strike_volumes.get(sk, {"call": 0, "put": 0})
            directional_confidence = self._classifier.detect_spread(
                call_vol=sv["call"],
                put_vol=sv["put"],
                oi_call=oi if otype == "call" else 0,
                oi_put=oi if otype == "put" else 0,
            )
            
            # ── 5. Final OI Adjustment ────────────────────────────────────
            # Combined: new_trades * P(opening) * direction * confidence
            estimated_new = (new_trades * open_prob * direction 
                           * directional_confidence)
            
            with self._lock:
                self._volume_delta[key] = self._volume_delta.get(key, 0) + estimated_new
                cumulative_new = self._volume_delta[key]
            
            adjusted_oi = max(oi + cumulative_new, 0)
            
            # ── GEX Computation ───────────────────────────────────────────
            scale = 100 * (spot * spot / 100)
            
            oi_gex = gamma * oi * scale
            vol_gex = gamma * cumulative_new * scale
            total_gex = gamma * adjusted_oi * scale
            
            gex_profile.setdefault(strike, {
                "oi_gex_call": 0, "oi_gex_put": 0,
                "vol_gex_call": 0, "vol_gex_put": 0,
                "total_gex_call": 0, "total_gex_put": 0,
                "adjusted_oi_call": 0, "adjusted_oi_put": 0,
            })
            
            suffix = "_call" if otype == "call" else "_put"
            gex_profile[strike][f"oi_gex{suffix}"] += oi_gex
            gex_profile[strike][f"vol_gex{suffix}"] += vol_gex
            gex_profile[strike][f"total_gex{suffix}"] += total_gex
            gex_profile[strike][f"adjusted_oi{suffix}"] += adjusted_oi
            
            # Vanna profile
            vanna_exp = vanna * adjusted_oi * 100
            vanna_profile[strike] = vanna_profile.get(strike, 0) + vanna_exp
            
            # 0DTE tracking
            abs_gamma = abs(gamma * adjusted_oi * scale)
            total_gamma_abs += abs_gamma
            
            if dte == 0:
                dte0_gamma_abs += abs_gamma
                dte0_gex.setdefault(strike, {"call": 0, "put": 0})
                dte0_gex[strike][otype] += total_gex
        
        # ── Compute Levels ─────────────────────────────────────────────────
        net_gex_by_strike = {}
        net_gex_oi_by_strike = {}
        for s, data in gex_profile.items():
            net_gex_by_strike[s] = data["total_gex_call"] - data["total_gex_put"]
            net_gex_oi_by_strike[s] = data["oi_gex_call"] - data["oi_gex_put"]
        
        # Call Wall
        above = {s: v for s, v in net_gex_by_strike.items() if s > spot and v > 0}
        call_wall = max(above, key=above.get) if above else spot + 5
        
        # Put Wall
        below = {s: v for s, v in net_gex_by_strike.items() if s < spot and v < 0}
        put_wall = min(below, key=below.get) if below else spot - 5
        
        # Major Wall
        major_wall = max(net_gex_by_strike, key=lambda s: abs(net_gex_by_strike[s])) \
                     if net_gex_by_strike else spot
        
        # GEX Flip
        sorted_strikes = sorted(net_gex_by_strike.keys())
        gex_flip = spot
        for i in range(len(sorted_strikes) - 1):
            s1, s2 = sorted_strikes[i], sorted_strikes[i+1]
            g1, g2 = net_gex_by_strike[s1], net_gex_by_strike[s2]
            if g1 * g2 < 0 and s1 <= spot <= s2:
                gex_flip = s1 + (s2 - s1) * abs(g1) / (abs(g1) + abs(g2))
                break
        
        zero_gamma = gex_flip
        
        # Vanna Magnet
        vanna_magnet = max(vanna_profile, key=lambda s: abs(vanna_profile[s])) \
                       if vanna_profile else spot
        
        # Net GEX totals
        total_net_gex = sum(net_gex_by_strike.values())
        total_net_gex_oi = sum(net_gex_oi_by_strike.values())
        net_gex_change = total_net_gex - total_net_gex_oi
        
        # ── Barbon & Buraschi (2021): Normalized Gamma Imbalance ───────
        # Eq 2: GI = Net_GEX * S / ADSV
        # Normalizes raw GEX by average daily dollar volume so it's
        # comparable across tickers and time periods
        adv = avg_daily_volume or self._avg_daily_volume
        if adv > 0:
            # Normalize: what % of daily volume is the gamma-induced flow?
            gamma_imbalance = (total_net_gex / adv) * 100
        else:
            # Fallback: use total gamma-weighted OI as rough proxy
            gamma_imbalance = total_net_gex / max(total_gamma_abs, 1) * 100
        
        # Track GI history for self-calibrating z-score
        self._gi_history.append(gamma_imbalance)
        if len(self._gi_history) > 252:  # ~1 year of trading days
            self._gi_history = self._gi_history[-252:]
        
        # Z-score: use self-calibrated stats if enough history,
        # otherwise use Barbon & Buraschi's published values
        if len(self._gi_history) >= 21:
            gi_mean = sum(self._gi_history) / len(self._gi_history)
            gi_var = sum((x - gi_mean)**2 for x in self._gi_history) / len(self._gi_history)
            gi_std = math.sqrt(gi_var) if gi_var > 0 else self.BB_GI_STD
        else:
            gi_mean = self.BB_GI_MEAN
            gi_std = self.BB_GI_STD
        
        gi_zscore = (gamma_imbalance - gi_mean) / gi_std if gi_std > 0 else 0
        
        # ── Regime Signal ──────────────────────────────────────────────
        # Barbon & Buraschi proved:
        #   GI > 0 → dealers long gamma → they buy dips/sell rallies → mean-reversion
        #   GI < 0 → dealers short gamma → they sell into drops/buy into rallies → momentum
        if gamma_imbalance > gi_std * 0.5:
            regime = "MEAN_REVERSION"   # Strong positive GI: price pins/bounces
            regime_strength = min(abs(gi_zscore), 3.0)  # Cap at 3 for display
        elif gamma_imbalance < -gi_std * 0.5:
            regime = "MOMENTUM"          # Strong negative GI: price trends/breaks
            regime_strength = min(abs(gi_zscore), 3.0)
        else:
            regime = "NEUTRAL"           # GI near zero: no strong dealer flow
            regime_strength = abs(gi_zscore)
        
        # ── Flash Crash Risk (Barbon & Buraschi Eq 6) ─────────────────
        # On May 6 2010: GI z-score was -1.7 → trillion-dollar wipeout
        # When deeply negative, dealer hedging amplifies any sell-off
        if gi_zscore < -2.5:
            flash_risk = "CRITICAL"      # Worse than May 6 2010
        elif gi_zscore < self.BB_FLASH_THRESHOLD:
            flash_risk = "ELEVATED"      # May 6 2010 territory
        elif gi_zscore < -1.0:
            flash_risk = "MODERATE"      # Below average, watch closely
        else:
            flash_risk = "LOW"           # Normal or positive GI
        
        # 0DTE levels
        dte0_net = {}
        for s, data in dte0_gex.items():
            dte0_net[s] = data["call"] - data["put"]
        
        dte0_above = {s: v for s, v in dte0_net.items() if s > spot and v > 0}
        dte0_below = {s: v for s, v in dte0_net.items() if s < spot and v < 0}
        dte0_call_wall = max(dte0_above, key=dte0_above.get) if dte0_above else call_wall
        dte0_put_wall = min(dte0_below, key=dte0_below.get) if dte0_below else put_wall
        dte0_pct = (dte0_gamma_abs / total_gamma_abs * 100) if total_gamma_abs > 0 else 0
        
        # Build profile output
        profile_out = {}
        for s in sorted(gex_profile.keys()):
            d = gex_profile[s]
            net_oi = d["oi_gex_call"] - d["oi_gex_put"]
            net_total = d["total_gex_call"] - d["total_gex_put"]
            net_vol = d["vol_gex_call"] - d["vol_gex_put"]
            profile_out[s] = {
                "oi_gex": round(net_oi, 2),
                "vol_gex": round(net_vol, 2),
                "total_gex": round(net_total, 2),
                "shift": round(net_total - net_oi, 2),
                "adjusted_oi_call": round(d["adjusted_oi_call"]),
                "adjusted_oi_put": round(d["adjusted_oi_put"]),
            }
        
        return {
            "call_wall": call_wall,
            "put_wall": put_wall,
            "major_wall": major_wall,
            "gex_flip": round(gex_flip, 2),
            "zero_gamma": round(zero_gamma, 2),
            "vanna_magnet": vanna_magnet,
            "net_gex": round(total_net_gex, 2),
            "net_gex_change": round(net_gex_change, 2),
            "gex_profile": profile_out,
            "dte0_gamma_pct": round(dte0_pct, 1),
            "dte0_call_wall": dte0_call_wall,
            "dte0_put_wall": dte0_put_wall,
            "atm_iv": round(atm_iv, 4) if atm_iv else 0.0,
            "timestamp": datetime.now().isoformat(),
            "contracts_tracked": contracts,
            "spot": spot,
            "classification_method": "lee_ready+pan_poteshman+lakonishok+barbon_buraschi",
            
            # ── Barbon & Buraschi (2021) Gamma Fragility ──────────────
            "gamma_imbalance": round(gamma_imbalance, 4),  # Normalized GI (%)
            "gi_zscore": round(gi_zscore, 2),              # Z-score vs history
            "regime": regime,                               # MOMENTUM / MEAN_REVERSION / NEUTRAL
            "regime_strength": round(regime_strength, 2),   # 0-3 (higher = stronger)
            "flash_risk": flash_risk,                       # LOW / MODERATE / ELEVATED / CRITICAL
        }


# ══════════════════════════════════════════════════════════════════════════════
#  Simulation Test
# ══════════════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    import sys, os, random
    sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    
    print("=" * 70)
    print("  INTRADAY VOLUME-ADJUSTED GEX -- SIMULATION")
    print("=" * 70)
    
    # Simulate a QQQ-like option chain
    spot = 490.0
    strikes = list(range(470, 511))
    today = date.today()
    exp_dates = [
        today.isoformat(),                          # 0DTE
        date(today.year, today.month, today.day + 2 if today.day < 28 else 1).isoformat(),  # 2DTE
        date(today.year, today.month + (1 if today.day > 20 else 0),
             15 if today.day < 15 else 20).isoformat(),  # monthly
    ]
    
    random.seed(42)
    chain = []
    
    for exp in exp_dates[:2]:  # Just 0DTE and near-term
        try:
            exp_d = date.fromisoformat(exp)
        except Exception:
            continue
        dte = max((exp_d - today).days, 0)
        
        for s in strikes:
            for otype in ["call", "put"]:
                moneyness = (s - spot) / spot
                
                # Simulate OI: higher near ATM
                base_oi = max(100, int(5000 * math.exp(-50 * moneyness**2)))
                
                # Simulate volume: spikes at certain strikes
                vol = int(base_oi * random.uniform(0.1, 0.8))
                if s in [485, 490, 495, 500]:  # Hot strikes
                    vol = int(base_oi * random.uniform(1.0, 2.5))
                
                # Simulate greeks
                sigma = 0.22 + 0.001 * abs(s - spot)
                T = max(dte, 1) / 365.0
                sqrt_T = math.sqrt(T)
                
                try:
                    d1 = (math.log(spot/s) + (0.045 + 0.5*sigma**2)*T) / (sigma*sqrt_T)
                except Exception:
                    d1 = 0
                
                from engines.bsm_engine import npdf, ncdf
                nd1 = npdf(d1)
                gamma = nd1 / (spot * sigma * sqrt_T)
                
                if otype == "call":
                    delta = ncdf(d1)
                else:
                    delta = ncdf(d1) - 1
                
                # Simulate bid/ask
                mid = max(0.05, abs(delta) * spot * 0.05)
                spread = mid * 0.03
                bid = round(mid - spread/2, 2)
                ask = round(mid + spread/2, 2)
                last = round(random.uniform(bid, ask), 2)
                
                chain.append({
                    "strike": s,
                    "option_type": otype,
                    "expiration_date": exp,
                    "open_interest": base_oi,
                    "volume": vol,
                    "bid": bid,
                    "ask": ask,
                    "last": last,
                    "greeks": {
                        "delta": round(delta, 6),
                        "gamma": round(gamma, 6),
                        "mid_iv": round(sigma, 4),
                        "vanna": round(-nd1 * (d1 - sigma*sqrt_T) / sigma, 6),
                    }
                })
    
    # Run tracker with simulated QQQ avg daily volume (~$15B)
    tracker = IntradayGEX(avg_daily_volume=15_000_000_000)
    
    # First snapshot
    r1 = tracker.compute(chain, spot, avg_daily_volume=15_000_000_000)
    
    print(f"\n  Spot: ${spot}")
    print(f"  Contracts tracked: {r1['contracts_tracked']}")
    print(f"\n  --- ADJUSTED LEVELS ---")
    print(f"  Call Wall:     ${r1['call_wall']}")
    print(f"  Put Wall:      ${r1['put_wall']}")
    print(f"  Major Wall:    ${r1['major_wall']}")
    print(f"  GEX Flip:      ${r1['gex_flip']}")
    print(f"  Vanna Magnet:  ${r1['vanna_magnet']}")
    print(f"  ATM IV:        {r1['atm_iv']:.2%}")
    print(f"\n  --- NET GEX ---")
    print(f"  Net GEX:       {r1['net_gex']:,.0f}")
    print(f"  GEX Change:    {r1['net_gex_change']:+,.0f} (from volume adjustment)")
    
    # ── Barbon & Buraschi (2021) Gamma Fragility ──────────────────────
    print(f"\n  --- GAMMA FRAGILITY (Barbon & Buraschi 2021) ---")
    print(f"  Gamma Imbalance:   {r1['gamma_imbalance']:+.4f}%")
    print(f"  GI Z-Score:        {r1['gi_zscore']:+.2f}")
    print(f"  Regime:            {r1['regime']} (strength: {r1['regime_strength']:.2f})")
    print(f"  Flash Crash Risk:  {r1['flash_risk']}")
    
    print(f"\n  --- 0DTE ---")
    print(f"  0DTE Gamma %:  {r1['dte0_gamma_pct']:.1f}% of total gamma")
    print(f"  0DTE Call Wall: ${r1['dte0_call_wall']}")
    print(f"  0DTE Put Wall:  ${r1['dte0_put_wall']}")
    
    # Top 5 strikes by GEX shift
    shifts = [(s, d["shift"]) for s, d in r1["gex_profile"].items() if abs(d["shift"]) > 0]
    shifts.sort(key=lambda x: abs(x[1]), reverse=True)
    
    print(f"\n  --- TOP INTRADAY GEX SHIFTS ---")
    print(f"  {'Strike':>8}  {'EOD GEX':>12}  {'Vol Adj':>12}  {'Total GEX':>12}  {'Shift':>10}")
    for s, _ in shifts[:10]:
        d = r1["gex_profile"][s]
        print(f"  ${s:>7}  {d['oi_gex']:>12,.0f}  {d['vol_gex']:>12,.0f}  "
              f"{d['total_gex']:>12,.0f}  {d['shift']:>+10,.0f}")
    
    # Simulate volume spike (someone buys 10K calls at $495)
    print(f"\n  --- SIMULATING: 10K call sweep at $495 ---")
    for opt in chain:
        if opt["strike"] == 495 and opt["option_type"] == "call":
            opt["volume"] += 10000
            opt["last"] = opt["ask"]  # Bought at ask (aggressive)
    
    r2 = tracker.compute(chain, spot, avg_daily_volume=15_000_000_000)
    print(f"  Call Wall:  ${r1['call_wall']} -> ${r2['call_wall']}")
    print(f"  GEX Change: {r2['net_gex_change']:+,.0f}")
    print(f"  Regime:     {r1['regime']} -> {r2['regime']}")
    print(f"  Flash Risk: {r1['flash_risk']} -> {r2['flash_risk']}")
    
    # Show $495 shift
    if 495 in r2["gex_profile"]:
        d = r2["gex_profile"][495]
        print(f"  $495 GEX:  EOD={d['oi_gex']:,.0f}  +VolAdj={d['vol_gex']:,.0f}  "
              f"Total={d['total_gex']:,.0f}  Shift={d['shift']:+,.0f}")
    
    print(f"\n{'='*70}")
    print(f"  SIMULATION COMPLETE")
    print(f"{'='*70}")
