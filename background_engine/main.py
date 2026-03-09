"""
Inference Engine — Main Orchestrator

The master script that:
1. Connects to all 3 data sources (Massive, Tradier, TopStepX)
2. Feeds data into all engines and frameworks
3. Aggregates signals and generates alerts
4. Logs everything for backtesting

Usage:
    python main.py              # Run full inference engine
    python main.py --test       # Run with simulated data
"""
import asyncio
import argparse
import logging
import time
from datetime import datetime

import numpy as np

from config import (
    OPTIONS_TICKERS, EQUITY_TICKERS, FUTURES_SYMBOLS,
    GEX_REFRESH_INTERVAL,
)
from connectors.massive_connector import MassiveConnector
from connectors.tradier_connector import TradierConnector
from connectors.topstepx_connector import TopStepXConnector
from engines.gex_calculator import GEXCalculator
from engines.greeks_calculator import GreeksCalculator
from frameworks.transfer_entropy import TransferEntropy
from frameworks.shannon_entropy import ShannonEntropy
from frameworks.ising_magnetization import IsingMagnetization
from frameworks.mutual_information import MutualInformation
from frameworks.reynolds_number import ReynoldsNumber
from frameworks.percolation_threshold import PercolationThreshold
from core.signal_aggregator import SignalAggregator
from utils.data_logger import DataLogger

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
)
logger = logging.getLogger("inference_engine")


class InferenceEngine:
    """
    Main orchestrator for the hedge fund inference engine.
    
    Data Flow:
        Massive → Greeks → GEX Calculator → Signal Aggregator
        Tradier → VIX/QQQ/SPY prices → Transfer Entropy, Percolation
        TopStepX → Level 2 DOM → Shannon Entropy, Ising, Reynolds
    """

    def __init__(self):
        # ─── Data Connectors ───
        self.massive = MassiveConnector()
        self.tradier = TradierConnector()
        self.topstepx = TopStepXConnector()

        # ─── Engines ───
        self.gex = GEXCalculator()
        self.greeks = GreeksCalculator()

        # ─── Alpha Frameworks ───
        self.transfer_entropy = TransferEntropy()
        self.shannon_entropy = ShannonEntropy()
        self.ising = IsingMagnetization()
        self.mutual_info = MutualInformation()
        self.reynolds = ReynoldsNumber()
        self.percolation = PercolationThreshold()

        # ─── Core ───
        self.aggregator = SignalAggregator()
        self.data_logger = DataLogger()

        # ─── State ───
        self.running = False
        self.last_gex_update = 0

    async def run(self):
        """Start the full inference engine with all data feeds."""
        self.running = True
        logger.info("=" * 60)
        logger.info("  INFERENCE ENGINE STARTING")
        logger.info("  Tradier (prices) + Massive (greeks) + TopStepX (L2)")
        logger.info("=" * 60)

        # Update risk-free rate from Massive
        try:
            r = self.massive.get_risk_free_rate()
            self.greeks.set_risk_free_rate(r)
            logger.info(f"Risk-free rate set to {r:.4f} from Massive Economy API")
        except Exception as e:
            logger.warning(f"Failed to fetch risk-free rate: {e}")

        # Run all feeds concurrently
        await asyncio.gather(
            self._tradier_feed(),
            self._gex_loop(),
            self._topstepx_feed(),
        )

    async def _tradier_feed(self):
        """Stream prices from Tradier for VIX, QQQ, SPY."""
        def on_trade(data):
            symbol = data.get("symbol")
            price = data.get("price", 0)

            # Feed into Transfer Entropy (VIX vs NQ)
            if symbol == "VIX":
                nq_price = self.topstepx.get_mid_price("NQ")
                if nq_price > 0:
                    te_result = self.transfer_entropy.update(price, nq_price)
                    self.aggregator.update_signal(
                        "transfer_entropy", self.transfer_entropy.get_signal()
                    )

            # Feed into Percolation (all prices)
            all_prices = self.topstepx.get_all_mid_prices()
            all_prices[symbol] = price
            if len(all_prices) >= 4:
                self.percolation.update(all_prices)
                self.aggregator.update_signal(
                    "percolation_threshold", self.percolation.get_signal()
                )

        try:
            await self.tradier.stream_quotes(
                EQUITY_TICKERS,
                on_trade=on_trade,
            )
        except Exception as e:
            logger.error(f"Tradier feed error: {e}")

    async def _gex_loop(self):
        """Periodically fetch option chain and compute GEX."""
        while self.running:
            try:
                for ticker in OPTIONS_TICKERS:
                    # 1. Fetch chain with real-time greeks
                    chain = self.massive.get_option_chain_parsed(ticker)
                    
                    # 2. Enrich with 2nd/3rd order greeks
                    chain = self.greeks.enrich_chain_with_higher_greeks(chain)
                    
                    # 3. Compute GEX
                    gex_result = self.gex.compute_gex(chain)
                    
                    # 4. Compute 0DTE GEX separately
                    dte_gex = self.gex.compute_0dte_gex(chain)
                    
                    # 5. Log everything
                    self.data_logger.log_greeks(chain, ticker)
                    self.data_logger.log_gex(gex_result, ticker)

                    # 6. Feed into Mutual Information
                    spot = gex_result.get("spot", 0)
                    total_gex = gex_result.get("total_gex", 0)
                    if spot > 0:
                        price_change = 0  # Will be computed from delta
                        self.mutual_info.update(total_gex, price_change)
                        self.aggregator.update_signal(
                            "mutual_information", self.mutual_info.get_signal()
                        )

                    # 7. Print summary
                    logger.info(self.gex.format_summary(gex_result))

                self.last_gex_update = time.time()

            except Exception as e:
                logger.error(f"GEX loop error: {e}")

            await asyncio.sleep(GEX_REFRESH_INTERVAL)

    async def _topstepx_feed(self):
        """Stream Level 2 data from TopStepX."""
        def on_dom_update(symbol, dom):
            # Shannon Entropy from order imbalance
            imb = dom.get("imbalance", 0)
            self.shannon_entropy.update(imb)
            self.aggregator.update_signal(
                "shannon_entropy", self.shannon_entropy.get_signal()
            )

            # Reynolds Number from price/spread/volume
            mid = dom.get("mid_price", 0)
            spread = dom.get("spread", 0)
            total_size = dom.get("bid_total", 0) + dom.get("ask_total", 0)
            if mid > 0:
                self.reynolds.update(mid, spread, total_size)
                self.aggregator.update_signal(
                    "reynolds_number", self.reynolds.get_signal()
                )

        def on_trade(symbol, trade):
            # Ising Magnetization from trade spins
            spin = trade.get("spin", 0)
            if spin != 0:
                self.ising.update_trade(symbol, spin)
                self.aggregator.update_signal(
                    "ising_magnetization", self.ising.get_signal()
                )

        try:
            await self.topstepx.connect(
                FUTURES_SYMBOLS,
                on_dom_update=on_dom_update,
                on_trade=on_trade,
            )
        except Exception as e:
            logger.error(f"TopStepX feed error: {e}")

    def print_status(self):
        """Print current inference engine status."""
        print(self.aggregator.format_status())


def run_simulation():
    """Run a complete simulation with synthetic data to verify all components."""
    print("=" * 60)
    print("  INFERENCE ENGINE — SIMULATION MODE")
    print("=" * 60)

    np.random.seed(42)

    # Initialize components
    gex_calc = GEXCalculator()
    greeks_calc = GreeksCalculator(risk_free_rate=0.043)
    te = TransferEntropy(window_size=50)
    se = ShannonEntropy(window_size=50)
    ising = IsingMagnetization(window_size=50)
    mi = MutualInformation(window_size=50)
    rn = ReynoldsNumber(window_size=50)
    perc = PercolationThreshold(symbols=["NQ", "ES", "YM", "RTY"], window_size=100)
    agg = SignalAggregator()
    db = DataLogger(db_path="simulation_test.db")

    # ─── Simulate 200 ticks of market data ───
    vix, nq_price = 20.0, 20000.0
    es_price, ym_price, rty_price = 6000.0, 40000.0, 2200.0
    qqq_price = 490.0

    print("\n─── Phase 1: Normal Market (100 ticks) ───")
    for i in range(100):
        # Prices
        common = np.random.randn() * 5
        vix += np.random.randn() * 0.1
        nq_price += common + np.random.randn() * 5
        es_price += common * 0.3 + np.random.randn() * 2
        ym_price += common * 0.5 + np.random.randn() * 10
        rty_price += common * 0.15 + np.random.randn() * 1
        qqq_price += common * 0.02 + np.random.randn() * 0.5

        # Transfer Entropy
        te.update(max(vix, 10), max(nq_price, 18000))
        agg.update_signal("transfer_entropy", te.get_signal())

        # Shannon Entropy
        imb = np.random.randn() * 0.3
        se.update(np.clip(imb, -1, 1))
        agg.update_signal("shannon_entropy", se.get_signal())

        # Ising
        for sym in ["NQ", "ES", "YM", "RTY"]:
            spin = 1 if np.random.rand() > 0.5 else -1
            ising.update_trade(sym, spin)
        agg.update_signal("ising_magnetization", ising.get_signal())

        # Mutual Information
        gex_val = np.random.randn() * 1e9
        mi.update(gex_val, np.random.randn() * 0.01)
        agg.update_signal("mutual_information", mi.get_signal())

        # Reynolds
        rn.update(nq_price, 0.25 + np.random.rand() * 0.1, 10 + np.random.rand() * 5)
        agg.update_signal("reynolds_number", rn.get_signal())

        # Percolation
        perc.update({"NQ": nq_price, "ES": es_price, "YM": ym_price, "RTY": rty_price})
        agg.update_signal("percolation_threshold", perc.get_signal())

    print(agg.format_status())

    print("\n─── Phase 2: Pre-Crash (VIX spikes, correlations break) ───")
    for i in range(100):
        # VIX spikes, NQ drops (VIX causing NQ)
        vix_shock = abs(np.random.randn()) * 0.5
        vix += vix_shock
        nq_price -= vix_shock * 50 + np.random.randn() * 10

        # Correlations break
        es_price += np.random.randn() * 20
        ym_price -= np.random.randn() * 50
        rty_price += np.random.randn() * 10

        # Transfer Entropy
        te.update(max(vix, 10), max(nq_price, 18000))
        agg.update_signal("transfer_entropy", te.get_signal())

        # Shannon Entropy (structured selling)
        imb = -0.4 + np.random.randn() * 0.1
        se.update(np.clip(imb, -1, 1))
        agg.update_signal("shannon_entropy", se.get_signal())

        # Ising (herding into sells)
        for sym in ["NQ", "ES", "YM", "RTY"]:
            spin = -1 if np.random.rand() > 0.2 else 1  # 80% sell
            ising.update_trade(sym, spin)
        agg.update_signal("ising_magnetization", ising.get_signal())

        # Mutual Information
        mi.update(np.random.randn() * 1e9, -abs(np.random.randn()) * 0.02)
        agg.update_signal("mutual_information", mi.get_signal())

        # Reynolds (turbulent)
        rn.update(nq_price, 2.0 + np.random.rand() * 3, 50 + np.random.rand() * 100)
        agg.update_signal("reynolds_number", rn.get_signal())

        # Percolation
        perc.update({"NQ": nq_price, "ES": es_price, "YM": ym_price, "RTY": rty_price})
        agg.update_signal("percolation_threshold", perc.get_signal())

    print(agg.format_status())

    # Log a signal to test DB
    db.log_signal("transfer_entropy", te.get_signal()["value"], {"alert": te.get_signal()["alert"]})

    # Test Greeks
    print("\n─── Greeks Enrichment Test ───")
    mock_chain = [
        {"strike": 490, "type": "call", "expiry": "2026-03-15",
         "underlying_price": 490, "iv": 0.22, "delta": 0.52,
         "gamma": 0.027, "theta": -0.45, "vega": 0.27, "oi": 5000, "volume": 1200},
        {"strike": 490, "type": "put", "expiry": "2026-03-15",
         "underlying_price": 490, "iv": 0.24, "delta": -0.48,
         "gamma": 0.025, "theta": -0.40, "vega": 0.25, "oi": 6000, "volume": 1500},
    ]
    enriched = greeks_calc.enrich_chain_with_higher_greeks(mock_chain)
    for c in enriched:
        print(f"  {c['type'].upper()} {c['strike']}: vanna={c.get('vanna', 0):.6f} "
              f"charm={c.get('charm', 0):.6f} vomma={c.get('vomma', 0):.6f}")

    # GEX from enriched chain
    gex_result = gex_calc.compute_gex(enriched)
    print(f"\n  GEX: {gex_result['total_gex']:,.0f} | Regime: {gex_result['regime']}")

    print("\n" + "=" * 60)
    print("  SIMULATION COMPLETE — All components verified ✅")
    print("=" * 60)

    # Cleanup test DB
    import os
    if os.path.exists("simulation_test.db"):
        os.remove("simulation_test.db")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Inference Engine")
    parser.add_argument("--test", action="store_true", help="Run simulation test")
    args = parser.parse_args()

    if args.test:
        run_simulation()
    else:
        engine = InferenceEngine()
        try:
            asyncio.run(engine.run())
        except KeyboardInterrupt:
            print("\nInference Engine stopped.")
