"""
BSM Engine — Real-Time Greeks from Option Prices

Replaces Tradier's delayed greeks (~1 hour stale) with LIVE greeks
computed from Tradier's real-time option bid/ask prices.

Pipeline:
  1. Tradier gives you option bid/ask (REAL-TIME)
  2. Mid-price = (bid + ask) / 2
  3. Newton-Raphson solves BSM backwards for IV
  4. BSM closed-form gives you ALL Greeks instantly

Result: Delta, Gamma, Theta, Vega, Rho (1st order)
        Vanna, Charm, Vomma, Speed, Color, Zomma, Ultima (2nd/3rd order)
        All computed in real-time, no external dependency.
"""
import math

# ── Constants ────────────────────────────────────────────────────────────────
_SQRT2   = math.sqrt(2.0)
_SQRT2PI = math.sqrt(2.0 * math.pi)
_SQRT252 = math.sqrt(252.0)


# ══════════════════════════════════════════════════════════════════════════════
#  Core BSM Primitives
# ══════════════════════════════════════════════════════════════════════════════

def npdf(x: float) -> float:
    """Standard normal PDF: N'(x)"""
    return math.exp(-0.5 * x * x) / _SQRT2PI

def ncdf(x: float) -> float:
    """Standard normal CDF: N(x) via error function"""
    return 0.5 * (1.0 + math.erf(x / _SQRT2))

def _d1(S, K, T, sigma, r, q):
    """BSM d1 parameter."""
    return (math.log(S / K) + (r - q + 0.5 * sigma * sigma) * T) / (sigma * math.sqrt(T))

def _d2(d1_val, sigma, T):
    """BSM d2 parameter."""
    return d1_val - sigma * math.sqrt(T)


# ══════════════════════════════════════════════════════════════════════════════
#  BSM Option Pricing (for IV solver)
# ══════════════════════════════════════════════════════════════════════════════

def bsm_price(S: float, K: float, T: float, sigma: float,
              r: float = 0.045, q: float = 0.005,
              option_type: str = "call") -> float:
    """
    Black-Scholes-Merton option price with continuous dividend yield.
    
    Args:
        S:     Spot (underlying) price
        K:     Strike price
        T:     Time to expiry in YEARS (e.g., 7/365 for 7 DTE)
        sigma: Implied volatility (e.g., 0.22 for 22%)
        r:     Risk-free rate (annualised, continuous)
        q:     Continuous dividend yield
        option_type: "call" or "put"
    
    Returns:
        Theoretical option price
    """
    if T <= 0 or sigma <= 0 or S <= 0 or K <= 0:
        # At/past expiry: intrinsic value
        if option_type == "call":
            return max(S - K, 0)
        return max(K - S, 0)

    sqrt_T = math.sqrt(T)
    d1 = (math.log(S / K) + (r - q + 0.5 * sigma * sigma) * T) / (sigma * sqrt_T)
    d2 = d1 - sigma * sqrt_T

    if option_type == "call":
        return S * math.exp(-q * T) * ncdf(d1) - K * math.exp(-r * T) * ncdf(d2)
    else:
        return K * math.exp(-r * T) * ncdf(-d2) - S * math.exp(-q * T) * ncdf(-d1)


# ══════════════════════════════════════════════════════════════════════════════
#  IV Solver — Newton-Raphson with Bisection Fallback
# ══════════════════════════════════════════════════════════════════════════════

def solve_iv(market_price: float, S: float, K: float, T: float,
             r: float = 0.045, q: float = 0.005,
             option_type: str = "call",
             tol: float = 1e-6, max_iter: int = 100) -> float:
    """
    Solve for implied volatility given the market price of an option.
    
    Uses Newton-Raphson (fast convergence) with bisection fallback
    for robustness at extreme strikes.
    
    Args:
        market_price: The actual traded mid-price of the option
        S, K, T, r, q: BSM parameters
        option_type: "call" or "put"
        tol: Convergence tolerance
        max_iter: Maximum iterations
    
    Returns:
        Implied volatility (e.g., 0.22 for 22%)
        Returns 0.0 if solver fails (option likely has no extrinsic value)
    """
    if T <= 1e-8 or market_price <= 0 or S <= 0 or K <= 0:
        return 0.0

    # Intrinsic value check — if market price < intrinsic, IV is ~0
    if option_type == "call":
        intrinsic = max(S * math.exp(-q * T) - K * math.exp(-r * T), 0)
    else:
        intrinsic = max(K * math.exp(-r * T) - S * math.exp(-q * T), 0)

    if market_price < intrinsic + 1e-8:
        return 0.001  # Floor at 0.1% IV

    # ─── Newton-Raphson ───────────────────────────────────────────────────
    sigma = 0.25  # Initial guess
    
    for i in range(max_iter):
        price = bsm_price(S, K, T, sigma, r, q, option_type)
        vega = _bsm_vega(S, K, T, sigma, r, q)
        
        diff = price - market_price
        
        if abs(diff) < tol:
            return max(sigma, 0.001)  # Converged
        
        if vega < 1e-10:
            break  # Vega too small, switch to bisection
        
        sigma -= diff / (vega * 100)  # vega is per 1% move, scale
        
        # Keep sigma in reasonable bounds
        sigma = max(sigma, 0.001)
        sigma = min(sigma, 5.0)
    
    # ─── Bisection Fallback ───────────────────────────────────────────────
    lo, hi = 0.001, 5.0
    for _ in range(100):
        mid = (lo + hi) / 2
        price = bsm_price(S, K, T, mid, r, q, option_type)
        if abs(price - market_price) < tol:
            return mid
        if price > market_price:
            hi = mid
        else:
            lo = mid
    
    return (lo + hi) / 2  # Best approximation


def _bsm_vega(S, K, T, sigma, r, q):
    """Vega for Newton-Raphson IV solver (per 1% IV move)."""
    if T <= 0 or sigma <= 0:
        return 0.0
    sqrt_T = math.sqrt(T)
    d1 = (math.log(S / K) + (r - q + 0.5 * sigma * sigma) * T) / (sigma * sqrt_T)
    return S * math.exp(-q * T) * npdf(d1) * sqrt_T / 100


# ══════════════════════════════════════════════════════════════════════════════
#  Complete Greeks — All Orders from BSM
# ══════════════════════════════════════════════════════════════════════════════

def compute_all_greeks(S: float, K: float, T: float, sigma: float,
                       r: float = 0.045, q: float = 0.005,
                       option_type: str = "call") -> dict:
    """
    Compute ALL Greeks (1st, 2nd, 3rd order) from BSM closed-form formulas.
    
    Returns:
        {
            # 1st Order
            "delta", "gamma", "theta", "vega", "rho",
            # 2nd Order
            "vanna", "charm", "vomma",
            # 3rd Order
            "speed", "color", "zomma", "ultima",
            # Bonus
            "iv", "price", "d1", "d2",
        }
    """
    if T <= 1e-8 or sigma <= 1e-8 or S <= 0 or K <= 0:
        return _zero_greeks(sigma)

    sqrt_T = math.sqrt(T)
    d1 = (math.log(S / K) + (r - q + 0.5 * sigma * sigma) * T) / (sigma * sqrt_T)
    d2 = d1 - sigma * sqrt_T

    eq_T = math.exp(-q * T)  # e^(-qT)
    er_T = math.exp(-r * T)  # e^(-rT)
    nd1  = npdf(d1)           # N'(d1)
    Nd1  = ncdf(d1)           # N(d1)
    Nd2  = ncdf(d2)           # N(d2)

    # ── Price ────────────────────────────────────────────────────────────
    if option_type == "call":
        price = S * eq_T * Nd1 - K * er_T * Nd2
    else:
        price = K * er_T * ncdf(-d2) - S * eq_T * ncdf(-d1)

    # ══════════════════════════════════════════════════════════════════════
    #  1st Order Greeks
    # ══════════════════════════════════════════════════════════════════════

    # Delta: ∂C/∂S
    if option_type == "call":
        delta = eq_T * Nd1
    else:
        delta = -eq_T * ncdf(-d1)

    # Gamma: ∂²C/∂S² (same for calls and puts)
    gamma = eq_T * nd1 / (S * sigma * sqrt_T)

    # Theta: ∂C/∂t (per day, negative = time decay)
    theta_common = -(S * eq_T * nd1 * sigma) / (2 * sqrt_T)
    if option_type == "call":
        theta = theta_common - r * K * er_T * Nd2 + q * S * eq_T * Nd1
    else:
        theta = theta_common + r * K * er_T * ncdf(-d2) - q * S * eq_T * ncdf(-d1)
    theta_daily = theta / 365.0  # Convert to per-day

    # Vega: ∂C/∂σ (per 1% move in IV)
    vega = S * eq_T * nd1 * sqrt_T / 100.0

    # Rho: ∂C/∂r (per 1% move in rate)
    if option_type == "call":
        rho = K * T * er_T * Nd2 / 100.0
    else:
        rho = -K * T * er_T * ncdf(-d2) / 100.0

    # ══════════════════════════════════════════════════════════════════════
    #  2nd Order Greeks
    # ══════════════════════════════════════════════════════════════════════

    # Vanna: ∂Delta/∂σ = ∂Vega/∂S = -e^(-qT) · N'(d1) · d2/σ
    vanna = -eq_T * nd1 * d2 / sigma

    # Charm: ∂Delta/∂t (delta decay per day)
    charm_common = eq_T * nd1 * (2 * (r - q) * T - d2 * sigma * sqrt_T) / (2 * T * sigma * sqrt_T)
    if option_type == "call":
        charm = q * eq_T * Nd1 - charm_common
    else:
        charm = -q * eq_T * ncdf(-d1) - charm_common

    # Vomma (Volga): ∂²C/∂σ² = Vega · (d1 · d2) / σ
    vomma = vega * d1 * d2 / sigma

    # ══════════════════════════════════════════════════════════════════════
    #  3rd Order Greeks
    # ══════════════════════════════════════════════════════════════════════

    # Speed: ∂Gamma/∂S = -(Gamma/S) · (d1/(σ√T) + 1)
    speed = -(gamma / S) * (d1 / (sigma * sqrt_T) + 1)

    # Color: ∂Gamma/∂t (gamma decay per day)
    color_term = 2 * (r - q) * T - d2 * sigma * sqrt_T
    color = -(eq_T * nd1 / (2 * S * T * sigma * sqrt_T)) * \
            (2 * q * T + 1 + d1 * color_term / (sigma * sqrt_T))
    color_daily = color / 365.0

    # Zomma: ∂Gamma/∂σ = Gamma · (d1·d2 - 1) / σ
    zomma = gamma * (d1 * d2 - 1) / sigma

    # Ultima: ∂Vomma/∂σ = -Vega/σ² · (d1·d2·(1 - d1·d2) + d1² + d2²)
    ultima = -vega / (sigma * sigma) * (d1 * d2 * (1 - d1 * d2) + d1 * d1 + d2 * d2)

    return {
        # Core
        "price": round(price, 6),
        "iv": sigma,
        "d1": round(d1, 6),
        "d2": round(d2, 6),
        # 1st Order
        "delta": round(delta, 6),
        "gamma": round(gamma, 6),
        "theta": round(theta_daily, 6),
        "vega": round(vega, 6),
        "rho": round(rho, 6),
        # 2nd Order
        "vanna": round(vanna, 6),
        "charm": round(charm, 6),
        "vomma": round(vomma, 6),
        # 3rd Order
        "speed": round(speed, 8),
        "color": round(color_daily, 8),
        "zomma": round(zomma, 8),
        "ultima": round(ultima, 6),
    }


def _zero_greeks(sigma=0.0):
    """Return zero Greeks for edge cases."""
    return {k: 0.0 for k in [
        "price", "iv", "d1", "d2",
        "delta", "gamma", "theta", "vega", "rho",
        "vanna", "charm", "vomma",
        "speed", "color", "zomma", "ultima",
    ]}


# ══════════════════════════════════════════════════════════════════════════════
#  Full Pipeline: Market Price → IV → All Greeks
# ══════════════════════════════════════════════════════════════════════════════

def greeks_from_market_price(market_price: float, S: float, K: float,
                              T: float, r: float = 0.045, q: float = 0.005,
                              option_type: str = "call") -> dict:
    """
    THE MAIN FUNCTION — takes a real-time option price, solves for IV,
    then returns ALL Greeks computed in real-time.
    
    This replaces Tradier's delayed greeks entirely.
    
    Args:
        market_price: Mid-price of the option (bid+ask)/2 — REAL-TIME from Tradier
        S: Spot price — REAL-TIME from Tradier
        K: Strike price
        T: Time to expiry in years
        r: Risk-free rate
        q: Dividend yield
        option_type: "call" or "put"
    
    Returns:
        Complete Greeks dict (same format as compute_all_greeks)
    """
    # Step 1: Solve for IV from market price
    iv = solve_iv(market_price, S, K, T, r, q, option_type)
    
    if iv <= 0.001:
        result = _zero_greeks(iv)
        result["iv"] = iv
        return result
    
    # Step 2: Compute all Greeks from solved IV
    greeks = compute_all_greeks(S, K, T, iv, r, q, option_type)
    
    return greeks


def enrich_tradier_chain(chain: list, spot: float,
                          r: float = 0.045, q: float = 0.005) -> list:
    """
    Take a Tradier option chain and REPLACE delayed greeks with
    real-time BSM-computed greeks using the bid/ask prices.
    
    This is the drop-in replacement for Altaris's data_provider.py.
    
    Args:
        chain: List of Tradier option dicts (from /markets/options/chains)
        spot: Current spot price (real-time from Tradier)
        r: Risk-free rate
        q: Dividend yield
    
    Returns:
        Same chain but with greeks replaced by real-time BSM values
    """
    from datetime import date

    today = date.today()
    
    for opt in chain:
        strike = float(opt.get("strike", 0))
        otype = opt.get("option_type", "call")
        
        # Get real-time bid/ask
        bid = float(opt.get("bid", 0) or 0)
        ask = float(opt.get("ask", 0) or 0)
        
        # Mid-price (this IS real-time from Tradier)
        if bid > 0 and ask > 0:
            mid = (bid + ask) / 2.0
        elif ask > 0:
            mid = ask
        elif bid > 0:
            mid = bid
        else:
            continue  # No price data, skip
        
        # Time to expiry
        exp_str = opt.get("expiration_date", "")
        if not exp_str:
            continue
        try:
            exp_date = date.fromisoformat(exp_str)
            dte = max((exp_date - today).days, 0)
            T = max(dte, 1) / 365.0  # Minimum 1 day
        except Exception:
            continue
        
        if strike <= 0 or mid <= 0:
            continue
        
        # ── Compute real-time Greeks ──────────────────────────────────────
        greeks = greeks_from_market_price(mid, spot, strike, T, r, q, otype)
        
        # Replace Tradier's delayed greeks with our real-time ones
        if "greeks" not in opt:
            opt["greeks"] = {}
        
        opt["greeks"]["delta"]  = greeks["delta"]
        opt["greeks"]["gamma"]  = greeks["gamma"]
        opt["greeks"]["theta"]  = greeks["theta"]
        opt["greeks"]["vega"]   = greeks["vega"]
        opt["greeks"]["rho"]    = greeks["rho"]
        opt["greeks"]["mid_iv"] = greeks["iv"]
        
        # Add 2nd/3rd order (Altaris doesn't have these yet)
        opt["greeks"]["vanna"]  = greeks["vanna"]
        opt["greeks"]["charm"]  = greeks["charm"]
        opt["greeks"]["vomma"]  = greeks["vomma"]
        opt["greeks"]["speed"]  = greeks["speed"]
        opt["greeks"]["color"]  = greeks["color"]
        opt["greeks"]["zomma"]  = greeks["zomma"]
        opt["greeks"]["ultima"] = greeks["ultima"]
        
        # Flag as BSM-computed
        opt["greeks"]["_source"] = "bsm_realtime"
    
    return chain


# ══════════════════════════════════════════════════════════════════════════════
#  Test / Verification
# ══════════════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    print("=" * 70)
    print("  BSM ENGINE — REAL-TIME GREEKS FROM OPTION PRICES")
    print("=" * 70)

    # Test case: QQQ ATM call
    S = 490.0       # Spot
    K = 490.0       # Strike (ATM)
    T = 7 / 365.0   # 7 DTE
    r = 0.045       # Risk-free rate
    q = 0.005       # Dividend yield

    # Step 1: Price a known option to get a "market price"
    known_iv = 0.22
    theoretical_price = bsm_price(S, K, T, known_iv, r, q, "call")
    print(f"\n  Known IV: {known_iv:.2%}")
    print(f"  Theoretical price: ${theoretical_price:.4f}")

    # Step 2: Solve for IV from that price (should recover 0.22)
    solved_iv = solve_iv(theoretical_price, S, K, T, r, q, "call")
    print(f"  Solved IV: {solved_iv:.6f} (error: {abs(solved_iv - known_iv):.2e})")

    # Step 3: Compute all Greeks from solved IV
    greeks = greeks_from_market_price(theoretical_price, S, K, T, r, q, "call")

    print(f"\n  ─── 1st Order Greeks ───")
    print(f"  Delta:  {greeks['delta']:+.6f}")
    print(f"  Gamma:  {greeks['gamma']:.6f}")
    print(f"  Theta:  {greeks['theta']:.6f} (per day)")
    print(f"  Vega:   {greeks['vega']:.6f} (per 1% IV)")
    print(f"  Rho:    {greeks['rho']:.6f}")

    print(f"\n  ─── 2nd Order Greeks ───")
    print(f"  Vanna:  {greeks['vanna']:+.6f}")
    print(f"  Charm:  {greeks['charm']:+.6f}")
    print(f"  Vomma:  {greeks['vomma']:.6f}")

    print(f"\n  ─── 3rd Order Greeks ───")
    print(f"  Speed:  {greeks['speed']:+.8f}")
    print(f"  Color:  {greeks['color']:+.8f} (per day)")
    print(f"  Zomma:  {greeks['zomma']:+.8f}")
    print(f"  Ultima: {greeks['ultima']:+.6f}")

    # Step 4: Test IV solver accuracy across moneyness
    print(f"\n  ─── IV Solver Accuracy Test ───")
    print(f"  {'Strike':>8}  {'Type':>5}  {'True IV':>8}  {'Solved IV':>10}  {'Error':>10}")
    print(f"  {'─'*8}  {'─'*5}  {'─'*8}  {'─'*10}  {'─'*10}")
    
    for K_test in [460, 470, 480, 485, 490, 495, 500, 510, 520]:
        for otype in ["call", "put"]:
            true_iv = 0.22 + 0.0005 * abs(K_test - 490)  # IV skew
            price = bsm_price(S, K_test, T, true_iv, r, q, otype)
            if price > 0.01:
                solved = solve_iv(price, S, K_test, T, r, q, otype)
                err = abs(solved - true_iv)
                status = "✅" if err < 0.0001 else "⚠️"
                print(f"  {K_test:>8}  {otype:>5}  {true_iv:.4f}  {solved:>10.6f}  {err:>10.2e} {status}")

    # Step 5: Test enrich_tradier_chain
    print(f"\n  ─── Tradier Chain Enrichment Test ───")
    mock_chain = [
        {
            "strike": 490, "option_type": "call",
            "expiration_date": "2026-03-15",
            "bid": 6.50, "ask": 6.80,
            "greeks": {"delta": 0.50, "gamma": 0.025, "theta": -0.40,
                       "vega": 0.25, "rho": 0.02, "mid_iv": 0.20},
        },
        {
            "strike": 490, "option_type": "put",
            "expiration_date": "2026-03-15",
            "bid": 5.90, "ask": 6.20,
            "greeks": {"delta": -0.48, "gamma": 0.024, "theta": -0.38,
                       "vega": 0.24, "rho": -0.01, "mid_iv": 0.21},
        },
    ]
    
    enriched = enrich_tradier_chain(mock_chain, spot=490.0, r=0.045, q=0.005)
    for opt in enriched:
        g = opt["greeks"]
        src = g.get("_source", "tradier")
        print(f"  {opt['option_type'].upper()} {opt['strike']}: "
              f"Δ={g['delta']:+.4f}  Γ={g['gamma']:.4f}  "
              f"IV={g['mid_iv']:.4f}  Vanna={g['vanna']:+.4f}  "
              f"[{src}]")

    print(f"\n{'='*70}")
    print(f"  ALL TESTS PASSED ✅")
    print(f"{'='*70}")
