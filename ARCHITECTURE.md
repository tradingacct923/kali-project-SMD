# 🏛️ Altaris GreekSite Architecture (War Machine)

The Altaris Inference Engine and GreekSite dashboard have been fully migrated and integrated into a professional, hedge-fund grade quantitative project structure. 

Everything is now safely self-contained inside: `C:\Users\aruna\OneDrive\war mechine\GreekSite\`

---

## 📂 Core Architecture Map

```mermaid
graph TD
    Root[GreekSite (Root Server)] --> API(server.py)
    Root --> Web[web/ (Dashboard Frontend)]
    Root --> FW[frameworks/ (Alpha Mathematics)]
    Root --> BE[background_engine/ (Live Data Daemon)]
    
    FW --> TE(transfer_entropy.py)
    FW --> SE(shannon_entropy.py)
    FW --> IM(ising_magnetization.py)
    FW --> MI(mutual_information.py)
    FW --> RN(reynolds_number.py)
    FW --> PT(percolation_threshold.py)
    FW --> LS(lppl_sornette.py)
    FW --> PL(powerlaw_tail.py)
    
    Web --> HTML(index.html)
    Web --> JS(app.js)
    Web --> CSS(css/)
    
    BE --> C(config.py)
    BE --> M(main.py)
    BE --> T(test_api.py...)
```

### 1. `server.py` (The API Brain)
This is the main Flask Server. It handles all endpoints including real-time spots, history, volatility layers, and the newly integrated `/api/inference` endpoint. 
- **What changed:** It now utilizes a lightning-fast *Server-Side Memory Cache* (60s TTL) and has been decoupled completely from the scratch workspace. It now directly imports your mathematical files locally.

### 2. `frameworks/` (The Quantitative Laboratory)
This directory houses the pure mathematical logic. It is strictly isolated from the web server code, ensuring institutional-grade separation of concerns.
- `lppl_sornette.py`: Market crash bubble detection via log-periodic power law (O(N) Scipy Optimize)
- `powerlaw_tail.py`: Heavy-tail risk detection via Hill Estimators
- `transfer_entropy.py` & `shannon_entropy.py`: Market flow and chaos metrics (Information Theory)
- `ising_magnetization.py` & `percolation_threshold.py`: Order flow directionality (Statistical Mechanics)
- `reynolds_number.py`: Market turbulence indicators (Fluid Dynamics)
- `mutual_information.py`: Gamma/Price coupling correlation

### 3. `web/` (The Client Dashboard)
The visualization layer. 
- `index.html`: Contains all sidebar buttons and structural UI for the 10 tabs, including the new Alpha Engine zones (Inference, Crash Risk, Flow Analysis).
- `app.js`: Contains robust dynamic javascript functions (`loadInference()`, `loadCrashRisk()`, `loadFlow()`) that fetch data asynchronously from the API, parse JSON, configure CSS alert badges, and render the signals onto your screen.

### 4. `background_engine/` (The Upcoming Pipeline)
This is a newly created, isolated folder for all background operations. Right now it contains your API testing files and experiments (`test_tradier.py`, `test_api.py`, `config.py`). 
- **The specific purpose of this folder** is to house the 24/7 background worker we will build next. This worker will constantly ingest live market data (like TopStepX Level 2), compute the heavy Alpha frameworks, and feed the results cleanly into `server.py` without lagging the dashboard.

---

### Why this is Hedge-Fund Grade:
* **Separation of Concerns:** The UI (`web/`), the API (`server.py`), and the Math (`frameworks/`) are rigorously isolated from one another.
* **Non-Blocking Architecture:** With `threaded=True` and server-side caching, the dashboard UI operates completely independently of how slow or heavy the background math is. 
* **Scalability:** By placing future data ingestion scripts strictly inside `background_engine/`, we ensure that scaling data sources (adding Alpaca, Rithmic, Topstep) will *never* require breaking changes to the core Dashboard.
