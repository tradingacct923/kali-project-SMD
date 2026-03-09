# ⚡ GreekSite — Options & Futures Intelligence Dashboard

Real-time options analytics dashboard with Level 2 futures data, 8 alpha frameworks, and macro intelligence.

## 🏗️ Architecture

```
server.py              ← Flask API (17+ endpoints)
data_provider.py       ← Tradier options chain + greeks pipeline
macro_provider.py      ← FRED yields/liquidity + Alpha Vantage econ data
├── web/               ← Dashboard frontend (HTML/JS/CSS)
├── frameworks/        ← 8 quantitative alpha frameworks
│   ├── transfer_entropy.py       VIX→NQ causal flow
│   ├── shannon_entropy.py        Order flow chaos detection
│   ├── ising_magnetization.py    Cross-contract herding
│   ├── mutual_information.py     GEX↔Price coupling
│   ├── reynolds_number.py        Market flow regime
│   ├── percolation_threshold.py  Systemic cascade risk
│   ├── lppl_sornette.py          Bubble detection (LPPL)
│   └── powerlaw_tail.py          Fat-tail risk (Hill estimator)
└── background_engine/ ← Level 2 data pipeline
    ├── topstepx_connector.py     SignalR connector (ProjectX API)
    └── l2_worker.py              Background daemon → feeds frameworks
```

## 🚀 Quick Start

### 1. Clone & Install
```bash
git clone https://github.com/YOUR_USERNAME/GreekSite.git
cd GreekSite
pip install -r requirements.txt
```

### 2. Configure
```bash
cp config.json.example config.json    # Add your Tradier API key
cp .env.example .env                  # Add your TopStepX credentials
```

### 3. Run
```bash
python server.py
# Open http://localhost:5000
```

## 🔑 API Keys Required

| Service | Purpose | Get it at |
|---------|---------|-----------|
| **Tradier** | Options chains, greeks, OHLCV | [tradier.com](https://tradier.com) |
| **TopStepX** | Level 2 DOM, tape (futures) | [topstep.com](https://topstep.com) → Settings → API |
| **Alpha Vantage** | News sentiment, macro data | [alphavantage.co](https://alphavantage.co) (optional) |

## 📊 Dashboard Tabs

- **GEX / DEX / VEX / TEX** — Gamma, Delta, Vega, Theta exposure profiles
- **VannaEX / CharmEX** — Second-order greek overlays
- **OI Analysis** — Open interest by strike + max pain
- **Vol Surface** — 3D implied volatility surface
- **Macro** — Yield curve, Fed balance sheet, CPI, GDP, news sentiment
- **Inference Engine** — 8 alpha framework signals (live or synthetic)
- **Level 2** — Real-time DOM depth + order flow from TopStepX

## 🌐 Deploy (Render / Railway)

1. Push to GitHub (secrets are in `.gitignore`)
2. Connect repo on [render.com](https://render.com) or [railway.app](https://railway.app)
3. Set environment variables in the platform dashboard:
   - `TRADIER_TOKEN`
   - `TOPSTEPX_USERNAME`, `TOPSTEPX_API_KEY`
   - `ALPHA_VANTAGE_KEY` (optional)
4. Deploy — it picks up the `Procfile` automatically

## 📄 License

Private — All rights reserved.
