"""
ETF Universe Data Module
Downloads live market data from NSE via yfinance (.NS suffix).
Caches data daily to avoid repeated downloads.
"""

import os
import ssl
import json
import pandas as pd
from datetime import datetime

PROXY_URL = "http://zs-proxy.agl.int/"

ETF_UNIVERSE = [
    {"scrip": "ABSLPSE", "sector": "ETF - PSE", "segment": "", "market_cap": 850, "lcp": 10.51},
    {"scrip": "ALPHA", "sector": "ETF - Alpha", "segment": "", "market_cap": 1200, "lcp": 45.30},
    {"scrip": "AONETOTAL", "sector": "ETF - Top 750", "segment": "", "market_cap": 3500, "lcp": 120.00},
    {"scrip": "AUTOBEES", "sector": "ETF - Auto", "segment": "", "market_cap": 2800, "lcp": 230.50},
    {"scrip": "BANKBEES", "sector": "ETF - Bank", "segment": "", "market_cap": 15000, "lcp": 450.20},
    {"scrip": "BFSI", "sector": "ETF - Fin Services", "segment": "", "market_cap": 4500, "lcp": 55.60},
    {"scrip": "COMMOIETF", "sector": "ETF - Commodities", "segment": "", "market_cap": 600, "lcp": 88.90},
    {"scrip": "CONSUMBEES", "sector": "ETF - Consumption", "segment": "", "market_cap": 1800, "lcp": 65.40},
    {"scrip": "CONSUMER", "sector": "ETF - New Age Consumption", "segment": "", "market_cap": 900, "lcp": 32.10},
    {"scrip": "CPSEETF", "sector": "ETF - CPSE", "segment": "", "market_cap": 7500, "lcp": 78.30},
    {"scrip": "DIVOPPBEES", "sector": "ETF - Dividend Opportunities 50", "segment": "", "market_cap": 2100, "lcp": 42.80},
    {"scrip": "ESG", "sector": "ETF - Nifty 100 ESG Sector Leaders", "segment": "", "market_cap": 1600, "lcp": 38.50},
    {"scrip": "FINIETF", "sector": "ETF - Fin Services Ex-Bank", "segment": "", "market_cap": 1100, "lcp": 29.70},
    {"scrip": "FMCGIETF", "sector": "ETF - FMCG", "segment": "", "market_cap": 3200, "lcp": 55.90},
    {"scrip": "GILT5YBEES", "sector": "ETF - Fixed Income", "segment": "", "market_cap": 5000, "lcp": 28.10},
    {"scrip": "GOLDBEES", "sector": "ETF - GOLD", "segment": "", "market_cap": 18000, "lcp": 52.40},
    {"scrip": "GROWWEV", "sector": "ETF - EV and New Age Automotive", "segment": "", "market_cap": 400, "lcp": 18.90},
    {"scrip": "GROWWRAIL", "sector": "ETF - Railways PSU", "segment": "", "market_cap": 350, "lcp": 22.50},
    {"scrip": "HDFCGROWTH", "sector": "ETF - Nifty Growth Sectors 15", "segment": "", "market_cap": 2200, "lcp": 35.60},
    {"scrip": "HDFCSML250", "sector": "ETF - SmallCap", "segment": "", "market_cap": 1500, "lcp": 15.80},
    {"scrip": "HEALTHIETF", "sector": "ETF - Healthcare", "segment": "", "market_cap": 2600, "lcp": 48.30},
    {"scrip": "ICICIB22", "sector": "ETF - Bharat 22 Index", "segment": "", "market_cap": 6000, "lcp": 95.20},
    {"scrip": "INFRAIETF", "sector": "ETF - Infrastructure", "segment": "", "market_cap": 1900, "lcp": 62.10},
    {"scrip": "ITBEES", "sector": "ETF - IT", "segment": "", "market_cap": 8500, "lcp": 38.70},
    {"scrip": "JUNIORBEES", "sector": "ETF - Next 50", "segment": "", "market_cap": 7000, "lcp": 680.50},
    {"scrip": "LIQUIDCASE", "sector": "ETF - Liquid Assets", "segment": "", "market_cap": 4000, "lcp": 1050.00},
    {"scrip": "LOWVOLIETF", "sector": "ETF - Top 100", "segment": "", "market_cap": 800, "lcp": 42.30},
    {"scrip": "LTGILTBEES", "sector": "ETF - Fixed Income", "segment": "", "market_cap": 3000, "lcp": 30.20},
    {"scrip": "MAKEINDIA", "sector": "ETF - Manufacturing", "segment": "", "market_cap": 1300, "lcp": 28.90},
    {"scrip": "METALIETF", "sector": "ETF - Metal", "segment": "", "market_cap": 2000, "lcp": 18.60},
    {"scrip": "MID150BEES", "sector": "ETF - MidCap", "segment": "", "market_cap": 3800, "lcp": 18.50},
    {"scrip": "MIDSMALL", "sector": "ETF - MidSmallCap", "segment": "", "market_cap": 1700, "lcp": 12.40},
    {"scrip": "MNC", "sector": "ETF - MNC", "segment": "", "market_cap": 900, "lcp": 350.60},
    {"scrip": "MOCAPITAL", "sector": "ETF - Capital Markets", "segment": "", "market_cap": 500, "lcp": 22.80},
    {"scrip": "MODEFENCE", "sector": "ETF - Defence", "segment": "", "market_cap": 700, "lcp": 35.40},
    {"scrip": "MOM30IETF", "sector": "ETF - Top 200", "segment": "", "market_cap": 1100, "lcp": 19.50},
    {"scrip": "MOMENTUM50", "sector": "ETF - Top 500", "segment": "", "market_cap": 1400, "lcp": 22.10},
    {"scrip": "MOREALTY", "sector": "ETF - Realty", "segment": "", "market_cap": 600, "lcp": 15.30},
    {"scrip": "MSCIINDIA", "sector": "ETF - MSCI India Index", "segment": "", "market_cap": 2500, "lcp": 28.40},
    {"scrip": "MULTICAP", "sector": "ETF - Multicap", "segment": "", "market_cap": 1000, "lcp": 14.20},
    {"scrip": "NIFTYBEES", "sector": "ETF - Top 50", "segment": "", "market_cap": 25000, "lcp": 260.50},
    {"scrip": "OILIETF", "sector": "ETF - Oil and Gas", "segment": "", "market_cap": 1200, "lcp": 18.90},
    {"scrip": "PHARMABEES", "sector": "ETF - Pharma", "segment": "", "market_cap": 3500, "lcp": 19.20},
    {"scrip": "PSUBNKBEES", "sector": "ETF - PSU Bank", "segment": "", "market_cap": 5500, "lcp": 72.30},
    {"scrip": "PVTBANIETF", "sector": "ETF - Pvt Bank", "segment": "", "market_cap": 4200, "lcp": 32.50},
    {"scrip": "SELECTIPO", "sector": "ETF - BSE Select IPO", "segment": "", "market_cap": 300, "lcp": 12.80},
    {"scrip": "SILVERBEES", "sector": "ETF - SILVER", "segment": "", "market_cap": 8000, "lcp": 72.60},
    {"scrip": "TNIDETF", "sector": "ETF - Digital", "segment": "", "market_cap": 500, "lcp": 15.40},
    {"scrip": "TOP10ADD", "sector": "ETF - Top 10", "segment": "", "market_cap": 600, "lcp": 11.20},
]


# Benchmarks: ETF-type use prices already downloaded with the universe;
# index-type are fetched separately.
BENCHMARKS = [
    {"name": "Nifty 50",         "yahoo": "^NSEI",           "etf_scrip": None},
    {"name": "Midcap 150",       "yahoo": "MID150BEES.NS",   "etf_scrip": "MID150BEES"},
    {"name": "Smallcap 250",     "yahoo": "HDFCSML250.NS",   "etf_scrip": "HDFCSML250"},
    {"name": "Next 50",          "yahoo": "JUNIORBEES.NS",   "etf_scrip": "JUNIORBEES"},
    {"name": "Gold",             "yahoo": "GOLDBEES.NS",     "etf_scrip": "GOLDBEES"},
    {"name": "Silver",           "yahoo": "SILVERBEES.NS",   "etf_scrip": "SILVERBEES"},
]

CACHE_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), ".price_cache.json")
CACHE_VERSION = 2  # bump when cache structure changes


def download_all_data(start_date="2020-01-01"):
    """
    Download live ETF prices (with .NS suffix) and multiple benchmarks.
    Returns (etf_prices, benchmark_prices, trading_days).
    - etf_prices: {scrip: {date_str: close_price}}
    - benchmark_prices: {name: {date_str: close_price}}  (multi-benchmark)
    - trading_days: sorted list of date strings (actual market trading days)
    """
    today = datetime.now().strftime("%Y-%m-%d")

    # Try loading from daily cache
    if os.path.exists(CACHE_FILE):
        try:
            with open(CACHE_FILE, "r") as f:
                cache = json.load(f)
            if cache.get("cache_date") == today and cache.get("cache_version") == CACHE_VERSION:
                bm = cache["benchmark_prices"]
                n_bm = len(bm) if isinstance(bm, dict) and not any(isinstance(v, (int, float)) for v in list(bm.values())[:1]) else 0
                print(f"[Cache] Using cached data: {len(cache['etf_prices'])} ETFs, "
                      f"{n_bm} benchmarks, {len(cache['trading_days'])} trading days")
                return cache["etf_prices"], cache["benchmark_prices"], cache["trading_days"]
        except (json.JSONDecodeError, KeyError):
            pass

    print("=" * 60)
    print("  Downloading live market data from NSE")
    print("=" * 60)

    etf_prices, etf_days = _download_etf_prices(start_date, today)
    benchmark_prices = _download_benchmarks(etf_prices, start_date, today)

    # Trading days = union of all days with data
    all_days = set(etf_days)
    for bm_data in benchmark_prices.values():
        all_days.update(bm_data.keys())
    trading_days = sorted(all_days)

    bm_summary = ", ".join(f"{k}: {len(v)}d" for k, v in benchmark_prices.items())
    print(f"\nSummary: {len(etf_prices)} ETFs, {len(benchmark_prices)} benchmarks, "
          f"{len(trading_days)} trading days")
    print(f"  Benchmarks: {bm_summary}")

    # Save to cache
    try:
        with open(CACHE_FILE, "w") as f:
            json.dump({
                "cache_date": today,
                "cache_version": CACHE_VERSION,
                "etf_prices": etf_prices,
                "benchmark_prices": benchmark_prices,
                "trading_days": trading_days,
            }, f)
        print("[Cache] Saved to .price_cache.json")
    except Exception as e:
        print(f"[Cache] Warning: Could not save cache: {e}")

    return etf_prices, benchmark_prices, trading_days


def _make_session():
    """Create a requests Session with proxy and SSL settings for corporate networks."""
    import requests
    import urllib3
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
    session = requests.Session()
    session.verify = False
    session.headers.update({
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                       "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    })
    proxy = os.environ.get("HTTPS_PROXY") or os.environ.get("HTTP_PROXY") or PROXY_URL
    session.proxies = {"http": proxy, "https": proxy}
    return session


def _fetch_yahoo_chart(session, ticker, start_date, end_date):
    """Fetch close prices from Yahoo Finance v8 chart API directly.
    Returns dict {date_str: close_price} or empty dict on failure.
    """
    import time
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


def _download_etf_prices(start_date, end_date):
    """Download ETF close prices from NSE (.NS suffix) using Yahoo chart API."""
    import time
    print(f"\nDownloading {len(ETF_UNIVERSE)} ETFs ({start_date} to {end_date})...")

    etf_prices = {}
    trading_days = set()
    session = _make_session()
    total = len(ETF_UNIVERSE)

    for idx, etf in enumerate(ETF_UNIVERSE):
        ticker_ns = etf["scrip"] + ".NS"
        scrip = etf["scrip"]
        prices = _fetch_yahoo_chart(session, ticker_ns, start_date, end_date)
        if prices:
            etf_prices[scrip] = prices
            trading_days.update(prices.keys())
            print(f"  [{idx+1}/{total}] {scrip}: {len(prices)} days")
        else:
            print(f"  [{idx+1}/{total}] {scrip}: no data")
        # small delay to avoid rate-limiting
        if (idx + 1) % 5 == 0:
            time.sleep(0.5)

    print(f"  Loaded {len(etf_prices)} / {total} ETFs")
    return etf_prices, trading_days


def _download_benchmarks(etf_prices, start_date, end_date):
    """Download all benchmarks. Reuses ETF price data where possible."""
    import time
    print(f"\nDownloading {len(BENCHMARKS)} benchmarks...")
    session = _make_session()
    result = {}
    for bm in BENCHMARKS:
        name = bm["name"]
        etf_scrip = bm.get("etf_scrip")
        if etf_scrip and etf_scrip in etf_prices:
            result[name] = etf_prices[etf_scrip]
            print(f"  {name}: {len(result[name])} days (from ETF {etf_scrip})")
        else:
            prices = _fetch_yahoo_chart(session, bm["yahoo"], start_date, end_date)
            if prices:
                result[name] = prices
                print(f"  {name}: {len(prices)} days (downloaded {bm['yahoo']})")
            else:
                print(f"  {name}: no data for {bm['yahoo']} — skipped")
            time.sleep(0.3)
    return result
