"""
Data Loader — Downloads stock prices from Yahoo Finance and stores in DuckDB.

Handles:
- Incremental downloads (only fetches what's missing)
- Corporate proxy & SSL bypass
- Rate limiting and retries
- Progress reporting
"""

import os
import time
import requests
import urllib3
from datetime import datetime

from db import Database
from stock_universe import get_universe, get_tickers, yahoo_ticker, BENCHMARKS_V2

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

PROXY_URL = "http://zs-proxy.agl.int/"


def _make_session() -> requests.Session:
    """Create a requests Session with proxy and SSL settings."""
    session = requests.Session()
    session.verify = False
    session.headers.update({
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                       "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    })
    proxy = os.environ.get("HTTPS_PROXY") or os.environ.get("HTTP_PROXY") or PROXY_URL
    session.proxies = {"http": proxy, "https": proxy}
    return session


def _fetch_yahoo_chart(session: requests.Session, ticker: str,
                       start_date: str, end_date: str) -> dict[str, float]:
    """
    Fetch close prices from Yahoo Finance v8 chart API.
    Returns {date_str: close_price} or empty dict on failure.
    """
    start_ts = int(datetime.strptime(start_date, "%Y-%m-%d").timestamp())
    end_ts = int(datetime.strptime(end_date, "%Y-%m-%d").timestamp()) + 86400

    url = (f"https://query2.finance.yahoo.com/v8/finance/chart/{ticker}"
           f"?period1={start_ts}&period2={end_ts}&interval=1d")

    for attempt in range(3):
        try:
            resp = session.get(url, timeout=20)
            if resp.status_code == 429:
                time.sleep(2 * (attempt + 1))
                continue
            if resp.status_code != 200:
                return {}
            data = resp.json()
            result = data.get("chart", {}).get("result")
            if not result:
                return {}
            timestamps = result[0].get("timestamp", [])
            closes = result[0].get("indicators", {}).get("adjclose")
            if not closes:
                closes = result[0].get("indicators", {}).get("quote", [{}])
                close_vals = closes[0].get("close", []) if closes else []
            else:
                close_vals = closes[0].get("adjclose", [])

            if not timestamps or not close_vals:
                return {}

            prices = {}
            for ts, val in zip(timestamps, close_vals):
                if val is not None and val > 0:
                    date_str = datetime.utcfromtimestamp(ts).strftime("%Y-%m-%d")
                    prices[date_str] = round(float(val), 2)
            return prices
        except Exception:
            if attempt < 2:
                time.sleep(1)
    return {}


def download_stock_data(universe_name: str = "nifty50",
                        start_date: str = "2010-01-01",
                        force: bool = False,
                        progress_callback=None) -> dict:
    """
    Download stock prices for a universe and store in DuckDB.

    Args:
        universe_name: 'nifty50', 'nifty100', etc.
        start_date: How far back to fetch
        force: If True, re-download even if already cached today
        progress_callback: Optional callable(current, total, ticker, status)

    Returns:
        dict with download stats
    """
    db = Database()
    stocks = get_universe(universe_name)
    tickers = get_tickers(stocks)
    today = datetime.now().strftime("%Y-%m-%d")

    # Register stock metadata
    db.upsert_stocks(stocks, universe_name)

    # Determine what needs downloading
    if force:
        to_download = tickers
    else:
        to_download = db.tickers_needing_download(tickers)

    total = len(to_download)
    if total == 0:
        print(f"[Data] All {len(tickers)} tickers already up-to-date for today.")
        db.close()
        return {"downloaded": 0, "total": len(tickers), "skipped": len(tickers)}

    print("=" * 60)
    print(f"  Downloading {total} stocks for {universe_name} ({start_date} to {today})")
    print("=" * 60)

    session = _make_session()
    downloaded = 0
    failed = []

    for idx, ticker in enumerate(to_download):
        yahoo_t = yahoo_ticker(ticker)
        prices = _fetch_yahoo_chart(session, yahoo_t, start_date, today)

        if prices:
            db.insert_prices(ticker, prices)
            db.log_download(ticker, len(prices))
            downloaded += 1
            status = f"{len(prices)} days"
            print(f"  [{idx+1}/{total}] {ticker}: {status}")
        else:
            failed.append(ticker)
            status = "FAILED"
            print(f"  [{idx+1}/{total}] {ticker}: {status}")

        if progress_callback:
            progress_callback(idx + 1, total, ticker, status)

        # Rate limiting: pause every 5 tickers
        if (idx + 1) % 5 == 0:
            time.sleep(0.5)

    # Download benchmarks
    print(f"\nDownloading {len(BENCHMARKS_V2)} benchmarks...")
    for bm in BENCHMARKS_V2:
        name = bm["name"]
        if not force and not db.needs_download(f"__bm__{name}"):
            print(f"  {name}: cached")
            continue

        prices = _fetch_yahoo_chart(session, bm["yahoo"], start_date, today)
        if prices:
            db.insert_benchmark(name, prices)
            db.log_download(f"__bm__{name}", len(prices))
            print(f"  {name}: {len(prices)} days")
        else:
            print(f"  {name}: FAILED")

    stats = db.get_stats()
    print(f"\nSummary: {stats['tickers_with_prices']} tickers, "
          f"{stats['total_price_rows']} price rows, "
          f"{stats['benchmark_rows']} benchmark rows")
    if failed:
        print(f"  Failed tickers: {', '.join(failed)}")

    db.close()

    return {
        "downloaded": downloaded,
        "failed": failed,
        "total": total,
        "skipped": len(tickers) - total,
        "stats": stats,
    }


def get_data_status(universe_name: str = "nifty50") -> dict:
    """Check current data status without downloading."""
    db = Database()
    stocks = get_universe(universe_name)
    tickers = get_tickers(stocks)

    stats = db.get_stats()
    to_download = db.tickers_needing_download(tickers)

    db.close()
    return {
        "universe": universe_name,
        "universe_size": len(tickers),
        "needs_download": len(to_download),
        "up_to_date": len(tickers) - len(to_download),
        **stats,
    }


if __name__ == "__main__":
    import sys
    universe = sys.argv[1] if len(sys.argv) > 1 else "nifty50"
    start = sys.argv[2] if len(sys.argv) > 2 else "2010-01-01"
    force = "--force" in sys.argv
    download_stock_data(universe, start, force=force)
