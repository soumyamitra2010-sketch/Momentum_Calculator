"""
ETF Universe Data Module
Contains the ETF universe metadata and synthetic price generation for backtesting.
"""

import random
import math
from datetime import datetime, timedelta

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


def generate_trading_days(start_date: str, end_date: str) -> list:
    """Generate list of trading days (weekdays) between start and end dates."""
    start = datetime.strptime(start_date, "%Y-%m-%d")
    end = datetime.strptime(end_date, "%Y-%m-%d")
    days = []
    current = start
    while current <= end:
        if current.weekday() < 5:  # Mon-Fri
            days.append(current.strftime("%Y-%m-%d"))
        current += timedelta(days=1)
    return days


def generate_synthetic_prices(seed: int = 42) -> dict:
    """
    Generate synthetic daily price data for all ETFs from 2022-01-01 to 2026-04-18.
    Returns dict: {ticker: {date_str: close_price}}
    Each ETF gets a unique drift/volatility profile seeded deterministically.
    """
    random.seed(seed)
    trading_days = generate_trading_days("2022-01-01", "2026-04-18")

    all_prices = {}
    for i, etf in enumerate(ETF_UNIVERSE):
        ticker = etf["scrip"]
        base_price = etf["lcp"] * random.uniform(0.5, 1.5)
        # Unique drift and volatility per ETF
        annual_drift = random.uniform(-0.05, 0.25)
        annual_vol = random.uniform(0.12, 0.45)
        daily_drift = annual_drift / 252
        daily_vol = annual_vol / math.sqrt(252)

        prices = {}
        price = base_price
        rng = random.Random(seed + i * 1000)
        for day in trading_days:
            shock = rng.gauss(0, 1)
            price *= math.exp(daily_drift - 0.5 * daily_vol ** 2 + daily_vol * shock)
            price = max(price, 0.01)  # floor
            prices[day] = round(price, 2)
        all_prices[ticker] = prices

    return all_prices


# Benchmark: simple Nifty50 proxy
def generate_benchmark_prices(seed: int = 99) -> dict:
    """Generate a synthetic Nifty50 benchmark series."""
    trading_days = generate_trading_days("2022-01-01", "2026-04-18")
    rng = random.Random(seed)
    price = 17000.0
    daily_drift = 0.10 / 252
    daily_vol = 0.15 / math.sqrt(252)
    prices = {}
    for day in trading_days:
        shock = rng.gauss(0, 1)
        price *= math.exp(daily_drift - 0.5 * daily_vol ** 2 + daily_vol * shock)
        prices[day] = round(price, 2)
    return prices
