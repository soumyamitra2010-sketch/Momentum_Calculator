"""
Database Layer — DuckDB storage for stock prices and metadata.

Schema:
  - stocks: ticker, name, sector, market_cap, universe membership
  - prices: ticker, date, close (adjusted close)
  - benchmarks: name, date, close
  - download_log: tracks last download per ticker
"""

import os
import duckdb
import pandas as pd
from datetime import datetime

DB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "momentum_v2.duckdb")


class Database:
    """DuckDB wrapper for the Momentum Calculator v2."""

    def __init__(self, db_path: str = DB_PATH):
        self.db_path = db_path
        self.conn = duckdb.connect(db_path)
        self._init_schema()

    def _init_schema(self):
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS stocks (
                ticker    TEXT PRIMARY KEY,
                name      TEXT,
                sector    TEXT,
                market_cap_cr DOUBLE,
                universe  TEXT
            )
        """)
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS prices (
                ticker TEXT,
                date   DATE,
                close  DOUBLE,
                PRIMARY KEY (ticker, date)
            )
        """)
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS benchmarks (
                name  TEXT,
                date  DATE,
                close DOUBLE,
                PRIMARY KEY (name, date)
            )
        """)
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS download_log (
                ticker         TEXT PRIMARY KEY,
                last_download  DATE,
                rows_count     INTEGER
            )
        """)

    # ── Stock Metadata ────────────────────────────────────────────────────

    def upsert_stocks(self, stocks: list[dict], universe: str):
        """Insert or update stock metadata."""
        for s in stocks:
            self.conn.execute("""
                INSERT INTO stocks (ticker, name, sector, market_cap_cr, universe)
                VALUES (?, ?, ?, ?, ?)
                ON CONFLICT (ticker) DO UPDATE SET
                    name = EXCLUDED.name,
                    sector = EXCLUDED.sector,
                    market_cap_cr = EXCLUDED.market_cap_cr,
                    universe = EXCLUDED.universe
            """, [s["ticker"], s.get("name", ""), s.get("sector", "Unknown"),
                  s.get("market_cap_cr", 0), universe])

    def get_stocks(self, universe: str = None) -> list[dict]:
        """Get stock list, optionally filtered by universe."""
        if universe:
            rows = self.conn.execute(
                "SELECT ticker, name, sector, market_cap_cr, universe FROM stocks WHERE universe = ?",
                [universe]
            ).fetchall()
        else:
            rows = self.conn.execute(
                "SELECT ticker, name, sector, market_cap_cr, universe FROM stocks"
            ).fetchall()
        return [{"ticker": r[0], "name": r[1], "sector": r[2],
                 "market_cap_cr": r[3], "universe": r[4]} for r in rows]

    # ── Price Data ────────────────────────────────────────────────────────

    def insert_prices(self, ticker: str, prices: dict[str, float]):
        """Bulk insert prices for a ticker. prices = {date_str: close}."""
        if not prices:
            return
        rows = [(ticker, date_str, close) for date_str, close in prices.items()]
        self.conn.executemany("""
            INSERT INTO prices (ticker, date, close)
            VALUES (?, ?, ?)
            ON CONFLICT (ticker, date) DO UPDATE SET close = EXCLUDED.close
        """, rows)

    def insert_benchmark(self, name: str, prices: dict[str, float]):
        """Bulk insert benchmark prices."""
        if not prices:
            return
        rows = [(name, date_str, close) for date_str, close in prices.items()]
        self.conn.executemany("""
            INSERT INTO benchmarks (name, date, close)
            VALUES (?, ?, ?)
            ON CONFLICT (name, date) DO UPDATE SET close = EXCLUDED.close
        """, rows)

    def log_download(self, ticker: str, row_count: int):
        """Record that a ticker was downloaded today."""
        today = datetime.now().strftime("%Y-%m-%d")
        self.conn.execute("""
            INSERT INTO download_log (ticker, last_download, rows_count)
            VALUES (?, ?, ?)
            ON CONFLICT (ticker) DO UPDATE SET
                last_download = EXCLUDED.last_download,
                rows_count = EXCLUDED.rows_count
        """, [ticker, today, row_count])

    def needs_download(self, ticker: str) -> bool:
        """Check if a ticker needs downloading (not downloaded today)."""
        today = datetime.now().strftime("%Y-%m-%d")
        row = self.conn.execute(
            "SELECT last_download FROM download_log WHERE ticker = ?", [ticker]
        ).fetchone()
        if row is None:
            return True
        return str(row[0]) != today

    def tickers_needing_download(self, tickers: list[str]) -> list[str]:
        """Return which tickers from the list need downloading today."""
        today = datetime.now().strftime("%Y-%m-%d")
        # Get all tickers that were already downloaded today
        downloaded = set()
        rows = self.conn.execute(
            "SELECT ticker FROM download_log WHERE last_download = ?", [today]
        ).fetchall()
        for r in rows:
            downloaded.add(r[0])
        return [t for t in tickers if t not in downloaded]

    # ── Query Helpers — Vectorized ────────────────────────────────────────

    def get_price_matrix(self, universe: str = None,
                         start_date: str = None, end_date: str = None) -> pd.DataFrame:
        """
        Return a pivoted DataFrame: index=date (sorted), columns=tickers, values=close.
        This is the primary data structure for the vectorized engine.
        """
        conditions = []
        params = []
        if universe:
            conditions.append("p.ticker IN (SELECT ticker FROM stocks WHERE universe = ?)")
            params.append(universe)
        if start_date:
            conditions.append("p.date >= ?")
            params.append(start_date)
        if end_date:
            conditions.append("p.date <= ?")
            params.append(end_date)

        where = f"WHERE {' AND '.join(conditions)}" if conditions else ""

        df = self.conn.execute(f"""
            SELECT p.date, p.ticker, p.close
            FROM prices p
            {where}
            ORDER BY p.date
        """, params).fetchdf()

        if df.empty:
            return pd.DataFrame()

        # Pivot: index=date, columns=ticker, values=close
        pivot = df.pivot(index="date", columns="ticker", values="close")
        pivot.index = pd.to_datetime(pivot.index)
        pivot.sort_index(inplace=True)
        return pivot

    def get_benchmark_matrix(self, start_date: str = None,
                             end_date: str = None) -> pd.DataFrame:
        """Return pivoted DataFrame for benchmarks: index=date, columns=benchmark_name."""
        conditions = []
        params = []
        if start_date:
            conditions.append("date >= ?")
            params.append(start_date)
        if end_date:
            conditions.append("date <= ?")
            params.append(end_date)
        where = f"WHERE {' AND '.join(conditions)}" if conditions else ""

        df = self.conn.execute(f"""
            SELECT date, name, close
            FROM benchmarks
            {where}
            ORDER BY date
        """, params).fetchdf()

        if df.empty:
            return pd.DataFrame()

        pivot = df.pivot(index="date", columns="name", values="close")
        pivot.index = pd.to_datetime(pivot.index)
        pivot.sort_index(inplace=True)
        return pivot

    def get_trading_days(self, start_date: str = None, end_date: str = None) -> list[str]:
        """Return sorted list of all trading days."""
        conditions = []
        params = []
        if start_date:
            conditions.append("date >= ?")
            params.append(start_date)
        if end_date:
            conditions.append("date <= ?")
            params.append(end_date)
        where = f"WHERE {' AND '.join(conditions)}" if conditions else ""

        rows = self.conn.execute(f"""
            SELECT DISTINCT date FROM prices {where} ORDER BY date
        """, params).fetchall()
        return [str(r[0]) for r in rows]

    def get_date_range(self) -> tuple[str, str] | None:
        """Return (first_date, last_date) from prices table."""
        row = self.conn.execute(
            "SELECT MIN(date), MAX(date) FROM prices"
        ).fetchone()
        if row and row[0]:
            return (str(row[0]), str(row[1]))
        return None

    def get_stats(self) -> dict:
        """Return summary statistics about the database."""
        stocks_count = self.conn.execute("SELECT COUNT(*) FROM stocks").fetchone()[0]
        price_rows = self.conn.execute("SELECT COUNT(*) FROM prices").fetchone()[0]
        bench_rows = self.conn.execute("SELECT COUNT(*) FROM benchmarks").fetchone()[0]
        unique_tickers = self.conn.execute("SELECT COUNT(DISTINCT ticker) FROM prices").fetchone()[0]
        date_range = self.get_date_range()
        return {
            "stocks_registered": stocks_count,
            "tickers_with_prices": unique_tickers,
            "total_price_rows": price_rows,
            "benchmark_rows": bench_rows,
            "date_range": date_range,
        }

    def close(self):
        self.conn.close()
