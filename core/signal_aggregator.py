"""
Signal Aggregator — The Core of the Inference Engine

Combines all 6 alpha frameworks into a unified regime score and
generates multi-tier trading alerts based on framework convergence.

Framework Convergence = Alpha
When multiple independent frameworks agree on direction/regime,
the probability of the signal being correct increases non-linearly.

Tier 1: 1 framework triggers (informational, log only)
Tier 2: 2-3 frameworks agree (elevated, prepare position)
Tier 3: 4+ frameworks converge (high conviction, trade)
"""
import logging
from datetime import datetime
from typing import Optional

logger = logging.getLogger(__name__)


class SignalAggregator:
    """
    Aggregates signals from all 6 alpha frameworks into a unified
    regime score and generates convergence-based alerts.
    """

    # Default weights (can be calibrated from backtest)
    DEFAULT_WEIGHTS = {
        "transfer_entropy": 0.25,     # Highest — crash detection
        "shannon_entropy": 0.15,      # Regime quality
        "ising_magnetization": 0.20,  # Herding = momentum
        "mutual_information": 0.10,   # GEX relevance
        "reynolds_number": 0.15,      # Flow regime
        "percolation_threshold": 0.15, # Tail risk
    }

    def __init__(self, weights: dict = None):
        self.weights = weights or self.DEFAULT_WEIGHTS
        
        # Store latest signals from each framework
        self.signals: dict = {}
        
        # Alert history
        self.alert_history: list = []
        
        # Latest aggregate
        self.regime_score: float = 0.0
        self.alert_tier: int = 0
        self.market_regime: str = "unknown"

    def update_signal(self, framework: str, signal: dict):
        """
        Update the latest signal from a framework.
        
        Args:
            framework: Framework name (e.g., "transfer_entropy")
            signal: Signal dict from framework.get_signal()
        """
        signal["updated_at"] = datetime.now().isoformat()
        self.signals[framework] = signal

    def compute(self) -> dict:
        """
        Aggregate all signals into unified regime score and alerts.
        
        Returns:
            {
                "regime_score": float,        # -1 (extreme bearish) to +1 (extreme bullish)
                "risk_score": float,          # 0 (safe) to 1 (danger)
                "alert_tier": int,            # 0, 1, 2, or 3
                "alert_message": str,
                "market_regime": str,         # "trending_up", "trending_down", "mean_reverting", "crisis"
                "framework_agreement": int,   # How many frameworks agree
                "signals_summary": dict,      # Per-framework summary
                "recommended_action": str,
            }
        """
        if not self.signals:
            return {
                "regime_score": 0, "risk_score": 0, "alert_tier": 0,
                "alert_message": "No signals available",
                "market_regime": "unknown", "framework_agreement": 0,
                "signals_summary": {}, "recommended_action": "Wait for data",
            }

        # Extract normalized values and interpret each framework
        bearish_count = 0
        bullish_count = 0
        risk_signals = 0
        total_risk = 0.0
        active_frameworks = 0
        
        signals_summary = {}

        # ─── Transfer Entropy ───
        te = self.signals.get("transfer_entropy", {})
        if te:
            active_frameworks += 1
            te_alert = te.get("alert", "normal")
            if te_alert == "critical":
                bearish_count += 1
                risk_signals += 1
                total_risk += 0.9
                signals_summary["transfer_entropy"] = "⚠️ VIX CAUSING NQ — crash risk"
            elif te_alert == "elevated":
                bearish_count += 1
                total_risk += 0.5
                signals_summary["transfer_entropy"] = "⚡ VIX causality elevated"
            else:
                signals_summary["transfer_entropy"] = "✅ Normal flow"

        # ─── Shannon Entropy ───
        se = self.signals.get("shannon_entropy", {})
        if se:
            active_frameworks += 1
            se_regime = se.get("regime", "unknown")
            if se_regime == "structured":
                signals_summary["shannon_entropy"] = "🎯 Structured — signals exploitable"
            elif se_regime == "chaotic":
                risk_signals += 1
                total_risk += 0.3
                signals_summary["shannon_entropy"] = "🌊 Chaotic — reduce size"
            else:
                signals_summary["shannon_entropy"] = "⚡ Transitional"

        # ─── Ising Magnetization ───
        ising = self.signals.get("ising_magnetization", {})
        if ising:
            active_frameworks += 1
            if ising.get("herding"):
                regime = ising.get("regime", "")
                if "systemic" in regime:
                    risk_signals += 1
                    # Direction matters
                    signals_summary["ising_magnetization"] = "🔥 SYSTEMIC HERDING"
                else:
                    signals_summary["ising_magnetization"] = "⚡ Partial herding"
            else:
                signals_summary["ising_magnetization"] = "🎲 No herding — random"

        # ─── Mutual Information ───
        mi = self.signals.get("mutual_information", {})
        if mi:
            active_frameworks += 1
            if mi.get("regime") == "coupled":
                signals_summary["mutual_information"] = "🔗 GEX driving price"
            else:
                signals_summary["mutual_information"] = "🔓 GEX decoupled"

        # ─── Reynolds Number ───
        rn = self.signals.get("reynolds_number", {})
        if rn:
            active_frameworks += 1
            rn_regime = rn.get("regime", "unknown")
            if rn_regime == "turbulent":
                bullish_count += 1  # Momentum works
                signals_summary["reynolds_number"] = "🌊 Turbulent — momentum"
            elif rn_regime == "laminar":
                signals_summary["reynolds_number"] = "🏊 Laminar — mean reversion"
            else:
                signals_summary["reynolds_number"] = "⚠️ Transitional"

        # ─── Percolation Threshold ───
        perc = self.signals.get("percolation_threshold", {})
        if perc:
            active_frameworks += 1
            if perc.get("percolating"):
                bearish_count += 1
                risk_signals += 1
                total_risk += 1.0  # Maximum risk
                signals_summary["percolation_threshold"] = "🚨 PERCOLATING — systemic"
            elif perc.get("regime") == "stressed":
                risk_signals += 1
                total_risk += 0.5
                signals_summary["percolation_threshold"] = "⚡ Stressed"
            else:
                signals_summary["percolation_threshold"] = "✅ Stable"

        # ─── Compute Aggregate Scores ───
        
        # Risk score: 0 (safe) to 1 (danger)
        risk_score = min(total_risk / max(active_frameworks, 1), 1.0)
        
        # Alert tier based on convergence
        if risk_signals >= 4:
            self.alert_tier = 3
        elif risk_signals >= 2:
            self.alert_tier = 2
        elif risk_signals >= 1:
            self.alert_tier = 1
        else:
            self.alert_tier = 0

        # Market regime
        if risk_signals >= 3:
            self.market_regime = "crisis"
        elif bearish_count > bullish_count and bearish_count >= 2:
            self.market_regime = "trending_down"
        elif bullish_count > bearish_count and bullish_count >= 2:
            self.market_regime = "trending_up"
        else:
            self.market_regime = "mean_reverting"

        # Recommended action
        actions = {
            0: "📊 Normal — trade your standard setups",
            1: "👀 Watch — one framework flagging, stay alert",
            2: "⚡ Prepare — multiple frameworks agree, reduce exposure or position for move",
            3: "🚨 ACT — high conviction convergence, full hedge or aggressive positioning",
        }

        alert_msg = self._build_alert_message(risk_signals, active_frameworks)

        result = {
            "regime_score": self.regime_score,
            "risk_score": risk_score,
            "alert_tier": self.alert_tier,
            "alert_message": alert_msg,
            "market_regime": self.market_regime,
            "framework_agreement": risk_signals,
            "active_frameworks": active_frameworks,
            "signals_summary": signals_summary,
            "recommended_action": actions[self.alert_tier],
            "timestamp": datetime.now().isoformat(),
        }

        # Log tier 2+ alerts
        if self.alert_tier >= 2:
            self.alert_history.append(result)

        return result

    def _build_alert_message(self, risk_signals: int, active: int) -> str:
        if risk_signals == 0:
            return "All clear — no frameworks flagging risk"
        elif risk_signals == 1:
            return f"1/{active} frameworks flagging — monitor"
        elif risk_signals == 2:
            return f"⚡ {risk_signals}/{active} frameworks converging — elevated risk"
        elif risk_signals == 3:
            return f"🔥 {risk_signals}/{active} frameworks converging — HIGH risk"
        else:
            return f"🚨 {risk_signals}/{active} frameworks CONVERGING — CRITICAL"

    def get_dashboard_data(self) -> dict:
        """Get all data needed for the dashboard display."""
        return {
            "aggregate": self.compute(),
            "individual_signals": self.signals,
            "alert_history": self.alert_history[-50:],  # Last 50 alerts
        }

    def format_status(self) -> str:
        """Format a readable status string."""
        result = self.compute()
        lines = [
            "═" * 60,
            f"  INFERENCE ENGINE — {result['timestamp']}",
            "═" * 60,
            f"  Alert Tier:  {'🚨' * result['alert_tier']} Tier {result['alert_tier']}",
            f"  Risk Score:  {result['risk_score']:.2f}",
            f"  Regime:      {result['market_regime'].upper().replace('_', ' ')}",
            f"  Agreement:   {result['framework_agreement']}/{result['active_frameworks']} frameworks",
            f"  Action:      {result['recommended_action']}",
            "",
            "  ─── Framework Signals ───",
        ]
        for fw, summary in result["signals_summary"].items():
            name = fw.replace("_", " ").title()
            lines.append(f"  {name:30s} {summary}")
        lines.append("═" * 60)
        return "\n".join(lines)
