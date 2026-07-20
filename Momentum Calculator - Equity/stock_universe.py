"""
Stock Universe Module — Nifty 50 / 100 / 200 / 500 constituents with sectors.

Provides:
- Hardcoded Nifty 50 and Nifty Next 50 (= Nifty 100)
- CSV loader for Nifty 200 / 500
- Sector classification
- Benchmark definitions
"""

import os
import csv

# ── Sector Constants ──────────────────────────────────────────────────────────

SECTORS = [
    "Financial Services",
    "Information Technology",
    "Oil Gas & Consumable Fuels",
    "Fast Moving Consumer Goods",
    "Automobile and Auto Components",
    "Healthcare",
    "Power",
    "Construction",
    "Consumer Durables",
    "Construction Materials",
    "Metals & Mining",
    "Telecommunication",
    "Services",
    "Capital Goods",
    "Chemicals",
    "Real Estate",
    "Consumer Services",
    "Textiles",
    "Diversified",
    "Media Entertainment & Publication",
    "Realty",
]

# ── Nifty 50 Constituents ────────────────────────────────────────────────────

NIFTY_50 = [
    {"ticker": "RELIANCE",    "name": "Reliance Industries",        "sector": "Oil Gas & Consumable Fuels",      "market_cap_cr": 1900000},
    {"ticker": "TCS",         "name": "Tata Consultancy Services",  "sector": "Information Technology",           "market_cap_cr": 1500000},
    {"ticker": "HDFCBANK",    "name": "HDFC Bank",                  "sector": "Financial Services",               "market_cap_cr": 1350000},
    {"ticker": "INFY",        "name": "Infosys",                    "sector": "Information Technology",           "market_cap_cr": 750000},
    {"ticker": "ICICIBANK",   "name": "ICICI Bank",                 "sector": "Financial Services",               "market_cap_cr": 900000},
    {"ticker": "HINDUNILVR",  "name": "Hindustan Unilever",         "sector": "Fast Moving Consumer Goods",       "market_cap_cr": 580000},
    {"ticker": "ITC",         "name": "ITC Limited",                "sector": "Fast Moving Consumer Goods",       "market_cap_cr": 580000},
    {"ticker": "SBIN",        "name": "State Bank of India",        "sector": "Financial Services",               "market_cap_cr": 720000},
    {"ticker": "BHARTIARTL",  "name": "Bharti Airtel",              "sector": "Telecommunication",                "market_cap_cr": 850000},
    {"ticker": "KOTAKBANK",   "name": "Kotak Mahindra Bank",        "sector": "Financial Services",               "market_cap_cr": 380000},
    {"ticker": "LT",          "name": "Larsen & Toubro",            "sector": "Construction",                     "market_cap_cr": 500000},
    {"ticker": "BAJFINANCE",  "name": "Bajaj Finance",              "sector": "Financial Services",               "market_cap_cr": 450000},
    {"ticker": "HCLTECH",     "name": "HCL Technologies",           "sector": "Information Technology",           "market_cap_cr": 420000},
    {"ticker": "AXISBANK",    "name": "Axis Bank",                  "sector": "Financial Services",               "market_cap_cr": 350000},
    {"ticker": "ASIANPAINT",  "name": "Asian Paints",               "sector": "Consumer Durables",                "market_cap_cr": 280000},
    {"ticker": "MARUTI",      "name": "Maruti Suzuki India",        "sector": "Automobile and Auto Components",   "market_cap_cr": 380000},
    {"ticker": "SUNPHARMA",   "name": "Sun Pharmaceutical",         "sector": "Healthcare",                       "market_cap_cr": 420000},
    {"ticker": "TITAN",       "name": "Titan Company",              "sector": "Consumer Durables",                "market_cap_cr": 310000},
    {"ticker": "TATAMOTORS",  "name": "Tata Motors",                "sector": "Automobile and Auto Components",   "market_cap_cr": 300000},
    {"ticker": "NTPC",        "name": "NTPC Limited",               "sector": "Power",                            "market_cap_cr": 350000},
    {"ticker": "ULTRACEMCO",  "name": "UltraTech Cement",           "sector": "Construction Materials",           "market_cap_cr": 320000},
    {"ticker": "ONGC",        "name": "Oil & Natural Gas Corp",     "sector": "Oil Gas & Consumable Fuels",      "market_cap_cr": 340000},
    {"ticker": "M&M",         "name": "Mahindra & Mahindra",        "sector": "Automobile and Auto Components",   "market_cap_cr": 370000},
    {"ticker": "POWERGRID",   "name": "Power Grid Corp",            "sector": "Power",                            "market_cap_cr": 290000},
    {"ticker": "JSWSTEEL",    "name": "JSW Steel",                  "sector": "Metals & Mining",                  "market_cap_cr": 230000},
    {"ticker": "ADANIPORTS",  "name": "Adani Ports & SEZ",          "sector": "Services",                         "market_cap_cr": 310000},
    {"ticker": "TATASTEEL",   "name": "Tata Steel",                 "sector": "Metals & Mining",                  "market_cap_cr": 200000},
    {"ticker": "NESTLEIND",   "name": "Nestle India",               "sector": "Fast Moving Consumer Goods",       "market_cap_cr": 230000},
    {"ticker": "WIPRO",       "name": "Wipro",                      "sector": "Information Technology",           "market_cap_cr": 260000},
    {"ticker": "BAJAJFINSV",  "name": "Bajaj Finserv",              "sector": "Financial Services",               "market_cap_cr": 270000},
    {"ticker": "TECHM",       "name": "Tech Mahindra",              "sector": "Information Technology",           "market_cap_cr": 160000},
    {"ticker": "HDFCLIFE",    "name": "HDFC Life Insurance",        "sector": "Financial Services",               "market_cap_cr": 150000},
    {"ticker": "SBILIFE",     "name": "SBI Life Insurance",         "sector": "Financial Services",               "market_cap_cr": 170000},
    {"ticker": "INDUSINDBK",  "name": "IndusInd Bank",              "sector": "Financial Services",               "market_cap_cr": 110000},
    {"ticker": "ADANIENT",    "name": "Adani Enterprises",          "sector": "Metals & Mining",                  "market_cap_cr": 350000},
    {"ticker": "COALINDIA",   "name": "Coal India",                 "sector": "Oil Gas & Consumable Fuels",      "market_cap_cr": 300000},
    {"ticker": "GRASIM",      "name": "Grasim Industries",          "sector": "Construction Materials",           "market_cap_cr": 180000},
    {"ticker": "DIVISLAB",    "name": "Divi's Laboratories",        "sector": "Healthcare",                       "market_cap_cr": 150000},
    {"ticker": "CIPLA",       "name": "Cipla",                      "sector": "Healthcare",                       "market_cap_cr": 130000},
    {"ticker": "APOLLOHOSP",  "name": "Apollo Hospitals",           "sector": "Healthcare",                       "market_cap_cr": 90000},
    {"ticker": "EICHERMOT",   "name": "Eicher Motors",              "sector": "Automobile and Auto Components",   "market_cap_cr": 130000},
    {"ticker": "DRREDDY",     "name": "Dr. Reddy's Laboratories",   "sector": "Healthcare",                       "market_cap_cr": 110000},
    {"ticker": "BPCL",        "name": "Bharat Petroleum",           "sector": "Oil Gas & Consumable Fuels",      "market_cap_cr": 140000},
    {"ticker": "TATACONSUM",  "name": "Tata Consumer Products",     "sector": "Fast Moving Consumer Goods",       "market_cap_cr": 110000},
    {"ticker": "HINDALCO",    "name": "Hindalco Industries",        "sector": "Metals & Mining",                  "market_cap_cr": 150000},
    {"ticker": "HEROMOTOCO",  "name": "Hero MotoCorp",              "sector": "Automobile and Auto Components",   "market_cap_cr": 100000},
    {"ticker": "BRITANNIA",   "name": "Britannia Industries",       "sector": "Fast Moving Consumer Goods",       "market_cap_cr": 120000},
    {"ticker": "BAJAJ-AUTO",  "name": "Bajaj Auto",                 "sector": "Automobile and Auto Components",   "market_cap_cr": 110000},
    {"ticker": "SHRIRAMFIN",  "name": "Shriram Finance",            "sector": "Financial Services",               "market_cap_cr": 100000},
    {"ticker": "TRENT",       "name": "Trent Limited",              "sector": "Consumer Services",                "market_cap_cr": 190000},
]

# ── Nifty Next 50 Constituents ───────────────────────────────────────────────

NIFTY_NEXT_50 = [
    {"ticker": "ADANIGREEN",  "name": "Adani Green Energy",         "sector": "Power",                            "market_cap_cr": 250000},
    {"ticker": "ADANIPOWER",  "name": "Adani Power",                "sector": "Power",                            "market_cap_cr": 180000},
    {"ticker": "AMBUJACEM",   "name": "Ambuja Cements",             "sector": "Construction Materials",           "market_cap_cr": 140000},
    {"ticker": "BANKBARODA",  "name": "Bank of Baroda",             "sector": "Financial Services",               "market_cap_cr": 130000},
    {"ticker": "BERGEPAINT",  "name": "Berger Paints India",        "sector": "Consumer Durables",                "market_cap_cr": 70000},
    {"ticker": "BOSCHLTD",    "name": "Bosch",                      "sector": "Automobile and Auto Components",   "market_cap_cr": 95000},
    {"ticker": "CANBK",       "name": "Canara Bank",                "sector": "Financial Services",               "market_cap_cr": 110000},
    {"ticker": "CHOLAFIN",    "name": "Cholamandalam Inv & Fin",    "sector": "Financial Services",               "market_cap_cr": 105000},
    {"ticker": "COLPAL",      "name": "Colgate-Palmolive India",    "sector": "Fast Moving Consumer Goods",       "market_cap_cr": 70000},
    {"ticker": "DABUR",       "name": "Dabur India",                "sector": "Fast Moving Consumer Goods",       "market_cap_cr": 95000},
    {"ticker": "DLF",         "name": "DLF Limited",                "sector": "Realty",                           "market_cap_cr": 180000},
    {"ticker": "GAIL",        "name": "GAIL India",                 "sector": "Oil Gas & Consumable Fuels",      "market_cap_cr": 130000},
    {"ticker": "GODREJCP",    "name": "Godrej Consumer Products",   "sector": "Fast Moving Consumer Goods",       "market_cap_cr": 120000},
    {"ticker": "HAL",         "name": "Hindustan Aeronautics",      "sector": "Capital Goods",                    "market_cap_cr": 300000},
    {"ticker": "HAVELLS",     "name": "Havells India",              "sector": "Consumer Durables",                "market_cap_cr": 100000},
    {"ticker": "ICICIPRULI",  "name": "ICICI Prudential Life",      "sector": "Financial Services",               "market_cap_cr": 90000},
    {"ticker": "ICICIGI",     "name": "ICICI Lombard General Ins",  "sector": "Financial Services",               "market_cap_cr": 80000},
    {"ticker": "INDIGO",      "name": "InterGlobe Aviation",        "sector": "Services",                         "market_cap_cr": 170000},
    {"ticker": "IOC",         "name": "Indian Oil Corporation",     "sector": "Oil Gas & Consumable Fuels",      "market_cap_cr": 200000},
    {"ticker": "IRCTC",       "name": "Indian Railway Catering",    "sector": "Consumer Services",                "market_cap_cr": 70000},
    {"ticker": "JINDALSTEL",  "name": "Jindal Steel & Power",       "sector": "Metals & Mining",                  "market_cap_cr": 90000},
    {"ticker": "LICI",        "name": "Life Insurance Corp",        "sector": "Financial Services",               "market_cap_cr": 600000},
    {"ticker": "LTIM",        "name": "LTIMindtree",                "sector": "Information Technology",           "market_cap_cr": 150000},
    {"ticker": "LUPIN",       "name": "Lupin Limited",              "sector": "Healthcare",                       "market_cap_cr": 90000},
    {"ticker": "MARICO",      "name": "Marico Limited",             "sector": "Fast Moving Consumer Goods",       "market_cap_cr": 85000},
    {"ticker": "NHPC",        "name": "NHPC Limited",               "sector": "Power",                            "market_cap_cr": 90000},
    {"ticker": "OBEROIRLTY",  "name": "Oberoi Realty",              "sector": "Realty",                           "market_cap_cr": 70000},
    {"ticker": "OFSS",        "name": "Oracle Financial Services",  "sector": "Information Technology",           "market_cap_cr": 75000},
    {"ticker": "PAGEIND",     "name": "Page Industries",            "sector": "Textiles",                         "market_cap_cr": 50000},
    {"ticker": "PEL",         "name": "Piramal Enterprises",        "sector": "Financial Services",               "market_cap_cr": 55000},
    {"ticker": "PERSISTENT",  "name": "Persistent Systems",         "sector": "Information Technology",           "market_cap_cr": 80000},
    {"ticker": "PIDILITIND",  "name": "Pidilite Industries",        "sector": "Chemicals",                        "market_cap_cr": 140000},
    {"ticker": "PNB",         "name": "Punjab National Bank",       "sector": "Financial Services",               "market_cap_cr": 110000},
    {"ticker": "POLYCAB",     "name": "Polycab India",              "sector": "Consumer Durables",                "market_cap_cr": 90000},
    {"ticker": "SBICARD",     "name": "SBI Cards & Payment",        "sector": "Financial Services",               "market_cap_cr": 70000},
    {"ticker": "SIEMENS",     "name": "Siemens",                    "sector": "Capital Goods",                    "market_cap_cr": 190000},
    {"ticker": "TORNTPHARM",  "name": "Torrent Pharmaceuticals",    "sector": "Healthcare",                       "market_cap_cr": 70000},
    {"ticker": "TVSMOTOR",    "name": "TVS Motor Company",          "sector": "Automobile and Auto Components",   "market_cap_cr": 100000},
    {"ticker": "UNIONBANK",   "name": "Union Bank of India",        "sector": "Financial Services",               "market_cap_cr": 80000},
    {"ticker": "VEDL",        "name": "Vedanta Limited",            "sector": "Metals & Mining",                  "market_cap_cr": 170000},
    {"ticker": "ZOMATO",      "name": "Zomato Limited",             "sector": "Consumer Services",                "market_cap_cr": 180000},
    {"ticker": "ZYDUSLIFE",   "name": "Zydus Lifesciences",         "sector": "Healthcare",                       "market_cap_cr": 80000},
    {"ticker": "ABB",         "name": "ABB India",                  "sector": "Capital Goods",                    "market_cap_cr": 150000},
    {"ticker": "ACC",         "name": "ACC Limited",                "sector": "Construction Materials",           "market_cap_cr": 45000},
    {"ticker": "PIIND",       "name": "PI Industries",              "sector": "Chemicals",                        "market_cap_cr": 60000},
    {"ticker": "NAUKRI",      "name": "Info Edge (Naukri)",         "sector": "Consumer Services",                "market_cap_cr": 80000},
    {"ticker": "MPHASIS",     "name": "Mphasis",                    "sector": "Information Technology",           "market_cap_cr": 55000},
    {"ticker": "BEL",         "name": "Bharat Electronics",         "sector": "Capital Goods",                    "market_cap_cr": 200000},
    {"ticker": "VOLTAS",      "name": "Voltas",                     "sector": "Consumer Durables",                "market_cap_cr": 45000},
    {"ticker": "MAXHEALTH",   "name": "Max Healthcare Institute",   "sector": "Healthcare",                       "market_cap_cr": 85000},
]

# ── Benchmarks for V2 ────────────────────────────────────────────────────────

BENCHMARKS_V2 = [
    {"name": "Nifty 50",      "yahoo": "^NSEI",          "type": "index"},
    {"name": "Nifty Bank",    "yahoo": "^NSEBANK",       "type": "index"},
    {"name": "Nifty IT",      "yahoo": "^CNXIT",         "type": "index"},
    {"name": "Nifty Midcap",  "yahoo": "NIFTYMID50.NS",  "type": "index"},
    {"name": "Gold",          "yahoo": "GOLDBEES.NS",    "type": "etf"},
]


# ── Helper Functions ──────────────────────────────────────────────────────────

def get_universe(name: str) -> list[dict]:
    """
    Get stock list for a given universe.
    name: 'nifty50', 'nifty100', 'nifty200', 'nifty500', or 'custom'
    """
    name = name.lower().strip()

    if name == "nifty50":
        return NIFTY_50
    elif name == "nifty100":
        return NIFTY_50 + NIFTY_NEXT_50
    elif name in ("nifty200", "nifty500"):
        # Try loading from CSV file in the same directory
        csv_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), f"{name}_constituents.csv")
        if os.path.exists(csv_path):
            return _load_from_csv(csv_path, name)
        # Fallback: return Nifty 100 with a warning
        print(f"[Warning] {csv_path} not found. Using Nifty 100 as fallback.")
        print(f"  To use {name}, place a CSV file with columns: ticker, name, sector, market_cap_cr")
        return NIFTY_50 + NIFTY_NEXT_50
    else:
        raise ValueError(f"Unknown universe: {name}. Use: nifty50, nifty100, nifty200, nifty500")


def _load_from_csv(filepath: str, universe_name: str) -> list[dict]:
    """Load stock universe from a CSV file."""
    stocks = []
    with open(filepath, "r", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        for row in reader:
            stocks.append({
                "ticker": row["ticker"].strip().upper(),
                "name": row.get("name", row["ticker"]).strip(),
                "sector": row.get("sector", "Unknown").strip(),
                "market_cap_cr": float(row.get("market_cap_cr", 0)),
            })
    print(f"[Universe] Loaded {len(stocks)} stocks from {filepath}")
    return stocks


def get_sector_map(stocks: list[dict]) -> dict[str, str]:
    """Return {ticker: sector} mapping."""
    return {s["ticker"]: s["sector"] for s in stocks}


def get_unique_sectors(stocks: list[dict]) -> list[str]:
    """Return sorted list of unique sectors in the universe."""
    return sorted(set(s["sector"] for s in stocks))


def get_tickers(stocks: list[dict]) -> list[str]:
    """Return list of tickers."""
    return [s["ticker"] for s in stocks]


def yahoo_ticker(nse_ticker: str) -> str:
    """Convert NSE ticker to Yahoo Finance format."""
    # Special cases for tickers with special characters
    return f"{nse_ticker}.NS"
