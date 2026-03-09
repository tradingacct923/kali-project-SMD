"""
Historical Data Logger

Stores Greeks, GEX levels, and OI snapshots to SQLite.
This solves the one gap Massive has vs ThetaData: no historical Greeks.
After running for a few months, you'll have your own historical database.
"""
import json
import logging
import sqlite3
import time
from datetime import datetime
from typing import Optional

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import DB_PATH

logger = logging.getLogger(__name__)


class DataLogger:
    """
    SQLite-based historical data logger.
    
    Stores:
    - Greeks snapshots (per contract, per timestamp)
    - GEX levels (total GEX, zero-gamma, walls)
    - OI snapshots (daily)
    - Alpha framework signals (TE, entropy, etc.)
    """

    def __init__(self, db_path: str = None):
        self.db_path = db_path or DB_PATH
        self._init_db()

    def _init_db(self):
        """Create tables if they don't exist."""
        conn = sqlite3.connect(self.db_path)
        c = conn.cursor()

        # Greeks per contract over time
        c.execute("""
            CREATE TABLE IF NOT EXISTS greeks_history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp TEXT NOT NULL,
                ticker TEXT NOT NULL,
                strike REAL NOT NULL,
                contract_type TEXT NOT NULL,
                expiry TEXT NOT NULL,
                underlying_price REAL,
                delta REAL, gamma REAL, theta REAL, vega REAL, iv REAL,
                vanna REAL, charm REAL, vomma REAL,
                speed REAL, color REAL, zomma REAL, ultima REAL,
                oi INTEGER,
                volume INTEGER
            )
        """)

        # GEX snapshots over time
        c.execute("""
            CREATE TABLE IF NOT EXISTS gex_history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp TEXT NOT NULL,
                ticker TEXT NOT NULL,
                spot REAL,
                total_gex REAL,
                call_gex REAL,
                put_gex REAL,
                zero_gamma_level REAL,
                call_wall_strike REAL,
                call_wall_gex REAL,
                put_wall_strike REAL,
                put_wall_gex REAL,
                regime TEXT
            )
        """)

        # Alpha framework signals
        c.execute("""
            CREATE TABLE IF NOT EXISTS signals_history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp TEXT NOT NULL,
                signal_type TEXT NOT NULL,
                value REAL,
                metadata TEXT
            )
        """)

        # Create indices for fast lookups
        c.execute("CREATE INDEX IF NOT EXISTS idx_greeks_ts ON greeks_history(timestamp)")
        c.execute("CREATE INDEX IF NOT EXISTS idx_greeks_ticker ON greeks_history(ticker)")
        c.execute("CREATE INDEX IF NOT EXISTS idx_gex_ts ON gex_history(timestamp)")
        c.execute("CREATE INDEX IF NOT EXISTS idx_signals_ts ON signals_history(timestamp)")
        c.execute("CREATE INDEX IF NOT EXISTS idx_signals_type ON signals_history(signal_type)")

        conn.commit()
        conn.close()
        logger.info(f"DataLogger initialized: {self.db_path}")

    # ─────────────────────────────────────────────────────
    #  Write Methods
    # ─────────────────────────────────────────────────────

    def log_greeks(self, chain: list[dict], ticker: str = "QQQ"):
        """Log a full option chain snapshot with all Greeks."""
        conn = sqlite3.connect(self.db_path)
        c = conn.cursor()
        ts = datetime.now().isoformat()

        for contract in chain:
            c.execute("""
                INSERT INTO greeks_history 
                (timestamp, ticker, strike, contract_type, expiry,
                 underlying_price, delta, gamma, theta, vega, iv,
                 vanna, charm, vomma, speed, color, zomma, ultima,
                 oi, volume)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                ts, ticker,
                contract.get("strike", 0),
                contract.get("type", ""),
                contract.get("expiry", ""),
                contract.get("underlying_price", 0),
                contract.get("delta", 0),
                contract.get("gamma", 0),
                contract.get("theta", 0),
                contract.get("vega", 0),
                contract.get("iv", 0),
                contract.get("vanna", 0),
                contract.get("charm", 0),
                contract.get("vomma", 0),
                contract.get("speed", 0),
                contract.get("color", 0),
                contract.get("zomma", 0),
                contract.get("ultima", 0),
                contract.get("oi", 0),
                contract.get("volume", 0),
            ))

        conn.commit()
        conn.close()
        logger.info(f"Logged {len(chain)} Greeks snapshots for {ticker}")

    def log_gex(self, gex_data: dict, ticker: str = "QQQ"):
        """Log a GEX calculation result."""
        conn = sqlite3.connect(self.db_path)
        c = conn.cursor()

        c.execute("""
            INSERT INTO gex_history
            (timestamp, ticker, spot, total_gex, call_gex, put_gex,
             zero_gamma_level, call_wall_strike, call_wall_gex,
             put_wall_strike, put_wall_gex, regime)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            gex_data.get("timestamp", datetime.now().isoformat()),
            ticker,
            gex_data.get("spot", 0),
            gex_data.get("total_gex", 0),
            gex_data.get("call_gex", 0),
            gex_data.get("put_gex", 0),
            gex_data.get("zero_gamma_level", 0),
            gex_data.get("call_wall", {}).get("strike", 0),
            gex_data.get("call_wall", {}).get("gex", 0),
            gex_data.get("put_wall", {}).get("strike", 0),
            gex_data.get("put_wall", {}).get("gex", 0),
            gex_data.get("regime", ""),
        ))

        conn.commit()
        conn.close()

    def log_signal(self, signal_type: str, value: float, metadata: dict = None):
        """Log an alpha framework signal (TE, entropy, magnetization, etc.)."""
        conn = sqlite3.connect(self.db_path)
        c = conn.cursor()

        c.execute("""
            INSERT INTO signals_history (timestamp, signal_type, value, metadata)
            VALUES (?, ?, ?, ?)
        """, (
            datetime.now().isoformat(),
            signal_type,
            value,
            json.dumps(metadata) if metadata else None,
        ))

        conn.commit()
        conn.close()

    # ─────────────────────────────────────────────────────
    #  Read Methods (for backtesting)
    # ─────────────────────────────────────────────────────

    def get_greeks_history(self, ticker: str, strike: float = None,
                           start: str = None, end: str = None,
                           limit: int = 1000) -> list[dict]:
        """Retrieve historical Greeks for a ticker (and optionally a strike)."""
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        c = conn.cursor()

        query = "SELECT * FROM greeks_history WHERE ticker = ?"
        params = [ticker]

        if strike:
            query += " AND strike = ?"
            params.append(strike)
        if start:
            query += " AND timestamp >= ?"
            params.append(start)
        if end:
            query += " AND timestamp <= ?"
            params.append(end)

        query += f" ORDER BY timestamp DESC LIMIT {limit}"
        c.execute(query, params)

        results = [dict(row) for row in c.fetchall()]
        conn.close()
        return results

    def get_gex_history(self, ticker: str = "QQQ",
                         start: str = None, end: str = None,
                         limit: int = 1000) -> list[dict]:
        """Retrieve historical GEX snapshots."""
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        c = conn.cursor()

        query = "SELECT * FROM gex_history WHERE ticker = ?"
        params = [ticker]

        if start:
            query += " AND timestamp >= ?"
            params.append(start)
        if end:
            query += " AND timestamp <= ?"
            params.append(end)

        query += f" ORDER BY timestamp DESC LIMIT {limit}"
        c.execute(query, params)

        results = [dict(row) for row in c.fetchall()]
        conn.close()
        return results

    def get_signals_history(self, signal_type: str = None,
                             start: str = None, end: str = None,
                             limit: int = 1000) -> list[dict]:
        """Retrieve historical alpha signals."""
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        c = conn.cursor()

        query = "SELECT * FROM signals_history WHERE 1=1"
        params = []

        if signal_type:
            query += " AND signal_type = ?"
            params.append(signal_type)
        if start:
            query += " AND timestamp >= ?"
            params.append(start)
        if end:
            query += " AND timestamp <= ?"
            params.append(end)

        query += f" ORDER BY timestamp DESC LIMIT {limit}"
        c.execute(query, params)

        results = [dict(row) for row in c.fetchall()]
        conn.close()
        return results
