"""
ETF Universe Data Module - CORRECTED SYMBOLS VERSION
Downloads live market data from NSE via yfinance (.NS suffix).
Caches data daily to avoid repeated downloads.
"""

import os
import ssl
import json
import pandas as pd
from datetime import datetime

PROXY_URL = "http://zs-proxy.agl.int/"

# Default 49 ETF Universe (original)
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

# Corrected ETF Universe with proper ticker symbols
ALL_ETF_UNIVERSE = [
    # ============ INDEX ETFs (Large Cap) ============
    {"scrip": "NIFTYBEES", "sector": "Index - Large Cap", "segment": "Nifty 50", "market_cap": 25000, "lcp": 260.50},
    {"scrip": "JUNIORBEES", "sector": "Index - Large Cap", "segment": "Nifty Next 50", "market_cap": 7000, "lcp": 680.50},
    {"scrip": "SETFNIF50", "sector": "Index - Large Cap", "segment": "Nifty 50", "market_cap": 1800, "lcp": 198.30},
    {"scrip": "NIFTYIETF", "sector": "Index - Large Cap", "segment": "Nifty 50", "market_cap": 12000, "lcp": 262.40},  # CORRECTED: ICICINIFTY -> NIFTYIETF
    {"scrip": "HDFCNIFTY", "sector": "Index - Large Cap", "segment": "Nifty 50", "market_cap": 3500, "lcp": 258.90},  # CORRECTED: HDFCNIFTY works
    {"scrip": "LICNFNHGP", "sector": "Index - Large Cap", "segment": "Nifty 50", "market_cap": 2200, "lcp": 260.20},
    {"scrip": "BSLNIFTY", "sector": "Index - Large Cap", "segment": "Nifty 50", "market_cap": 1100, "lcp": 259.80},
    {"scrip": "MON100", "sector": "Index - Large Cap", "segment": "Nifty 100", "market_cap": 1200, "lcp": 180.50},  # ADDED: Nifty 100 Index
    {"scrip": "NIF100BEES", "sector": "Index - Large Cap", "segment": "Nifty 100", "market_cap": 4500, "lcp": 195.60},
    {"scrip": "SENSEXETF", "sector": "Index - Large Cap", "segment": "BSE Sensex", "market_cap": 1600, "lcp": 685.20},
    {"scrip": "MAFANG", "sector": "Thematic - Global", "segment": "US FAANG", "market_cap": 800, "lcp": 85.20},  # ADDED: US Tech Giants
    {"scrip": "MASPTOP50", "sector": "Index - Large Cap", "segment": "S&P Top 50", "market_cap": 900, "lcp": 180.00},
    {"scrip": "MONQ50", "sector": "Index - Large Cap", "segment": "Nasdaq 100", "market_cap": 600, "lcp": 42.50},
    
    # ============ INDEX ETFs (Mid/Small Cap) ============
    {"scrip": "MID150BEES", "sector": "Index - Mid Cap", "segment": "Nifty Midcap 150", "market_cap": 3800, "lcp": 18.50},
    {"scrip": "MID150CASE", "sector": "Index - Mid Cap", "segment": "Nifty Midcap 150", "market_cap": 550, "lcp": 175.35},  # ADDED: Midcap 150 ETF
    {"scrip": "HDFCMID150", "sector": "Index - Mid Cap", "segment": "Nifty Midcap 150", "market_cap": 1400, "lcp": 135.20},
    {"scrip": "MIDCAP", "sector": "Index - Mid Cap", "segment": "Nifty Midcap 50", "market_cap": 2800, "lcp": 185.40},  # ADDED: Midcap ETF
    {"scrip": "MIDCAPETF", "sector": "Index - Mid Cap", "segment": "Midcap Index", "market_cap": 1200, "lcp": 135.00},
    {"scrip": "HDFCSML250", "sector": "Index - Small Cap", "segment": "Nifty Smallcap 250", "market_cap": 1500, "lcp": 15.80},
    {"scrip": "MOSMALL250", "sector": "Index - Small Cap", "segment": "Motilal Smallcap 250", "market_cap": 400, "lcp": 15.90},
    
    # ============ SECTOR ETFs - BANKING & FINANCIAL ============
    {"scrip": "BANKBEES", "sector": "Banking & Financial", "segment": "Nifty Bank", "market_cap": 15000, "lcp": 450.20},
    {"scrip": "BANKIETF", "sector": "Banking & Financial", "segment": "Nifty Bank", "market_cap": 5200, "lcp": 451.30},
    {"scrip": "SETFNIFBK", "sector": "Banking & Financial", "segment": "Nifty Bank", "market_cap": 2200, "lcp": 450.80},
    {"scrip": "ICICIBANK", "sector": "Banking & Financial", "segment": "Nifty Bank", "market_cap": 3800, "lcp": 452.10},  # CORRECTED: ICICIBANK works
    {"scrip": "HDFCBANK", "sector": "Banking & Financial", "segment": "Nifty Bank", "market_cap": 6200, "lcp": 450.90},  # CORRECTED: HDFCBANK works
    {"scrip": "KOTAKBANK", "sector": "Banking & Financial", "segment": "Nifty Bank", "market_cap": 2800, "lcp": 451.50},  # CORRECTED: KOTAKBANK works
    {"scrip": "PSUBNKBEES", "sector": "Banking & Financial", "segment": "Nifty PSU Bank", "market_cap": 5500, "lcp": 72.30},
    {"scrip": "PSUBNKIETF", "sector": "Banking & Financial", "segment": "Nifty PSU Bank", "market_cap": 3000, "lcp": 72.40},
    {"scrip": "BANKBETA", "sector": "Banking & Financial", "segment": "Nifty Bank", "market_cap": 2500, "lcp": 450.30},
    {"scrip": "PVTBANIETF", "sector": "Banking & Financial", "segment": "Nifty Private Bank", "market_cap": 4200, "lcp": 32.50},
    {"scrip": "BFSI", "sector": "Banking & Financial", "segment": "Nifty Financial Services", "market_cap": 4500, "lcp": 55.60},
    {"scrip": "FINIETF", "sector": "Banking & Financial", "segment": "Nifty Fin Services Ex-Bank", "market_cap": 1100, "lcp": 29.70},
    
    # ============ SECTOR ETFs - IT & TECHNOLOGY ============
    {"scrip": "ITBEES", "sector": "IT & Technology", "segment": "Nifty IT", "market_cap": 8500, "lcp": 38.70},
    {"scrip": "ITIETF", "sector": "IT & Technology", "segment": "Nifty IT", "market_cap": 2800, "lcp": 38.90},
    {"scrip": "TNIDETF", "sector": "IT & Technology", "segment": "Digital", "market_cap": 500, "lcp": 15.40},
    
    # ============ SECTOR ETFs - PHARMA & HEALTHCARE ============
    {"scrip": "PHARMABEES", "sector": "Pharma & Healthcare", "segment": "Nifty Pharma", "market_cap": 3500, "lcp": 19.20},
    {"scrip": "HEALTHIETF", "sector": "Pharma & Healthcare", "segment": "Nifty Healthcare", "market_cap": 2600, "lcp": 48.30},
    
    # ============ SECTOR ETFs - FMCG & CONSUMPTION ============
    {"scrip": "FMCGIETF", "sector": "FMCG & Consumption", "segment": "Nifty FMCG", "market_cap": 3200, "lcp": 55.90},
    {"scrip": "CONSUMBEES", "sector": "FMCG & Consumption", "segment": "Nifty India Consumption", "market_cap": 1800, "lcp": 65.40},
    {"scrip": "CONSUMER", "sector": "FMCG & Consumption", "segment": "Nifty Consumption", "market_cap": 900, "lcp": 32.10},
    
    # ============ SECTOR ETFs - AUTO & AUTO ANCILLARY ============
    {"scrip": "AUTOBEES", "sector": "Auto & Auto Ancillary", "segment": "Nifty Auto", "market_cap": 2800, "lcp": 230.50},
    {"scrip": "AUTOIETF", "sector": "Auto & Auto Ancillary", "segment": "Nifty Auto", "market_cap": 1600, "lcp": 230.80},
    {"scrip": "GROWWEV", "sector": "Auto & Auto Ancillary", "segment": "EV & New Age Auto", "market_cap": 400, "lcp": 18.90},
    {"scrip": "EVINDIA", "sector": "Auto & Auto Ancillary", "segment": "EV India", "market_cap": 350, "lcp": 18.80},
    
    # ============ SECTOR ETFs - INFRASTRUCTURE ============
    {"scrip": "INFRAIETF", "sector": "Infrastructure", "segment": "Nifty Infrastructure", "market_cap": 1900, "lcp": 62.10},
    {"scrip": "INFRABEES", "sector": "Infrastructure", "segment": "Infrastructure Index", "market_cap": 1400, "lcp": 62.30},
    {"scrip": "GROWWRAIL", "sector": "Infrastructure", "segment": "Railways PSU", "market_cap": 350, "lcp": 22.50},
    {"scrip": "MAKEINDIA", "sector": "Infrastructure", "segment": "Manufacturing", "market_cap": 1300, "lcp": 28.90},
    
    # ============ SECTOR ETFs - METALS ============
    {"scrip": "METALIETF", "sector": "Metals", "segment": "Nifty Metal", "market_cap": 2000, "lcp": 18.60},
    {"scrip": "METAL", "sector": "Metals", "segment": "Metal Index", "market_cap": 1200, "lcp": 18.70},
    
    # ============ SECTOR ETFs - OIL & GAS ============
    {"scrip": "OILIETF", "sector": "Oil & Gas", "segment": "Nifty Oil & Gas", "market_cap": 1200, "lcp": 18.90},
    {"scrip": "OIL", "sector": "Oil & Gas", "segment": "Oil & Gas Index", "market_cap": 850, "lcp": 19.00},
    
    # ============ SECTOR ETFs - REALTY ============
    {"scrip": "MOREALTY", "sector": "ETF - Realty", "segment": "Realty Index", "market_cap": 600, "lcp": 15.30},
    
    # ============ THEMATIC ETFs ============
    {"scrip": "CPSEETF", "sector": "Thematic - PSU", "segment": "CPSE Index", "market_cap": 7500, "lcp": 78.30},
    {"scrip": "ICICIB22", "sector": "Thematic - PSU", "segment": "Bharat 22", "market_cap": 6000, "lcp": 95.20},
    {"scrip": "ABSLPSE", "sector": "Thematic - PSU", "segment": "PSE Index", "market_cap": 850, "lcp": 10.51},
    {"scrip": "MOCAPITAL", "sector": "Thematic - Capital Markets", "segment": "Capital Markets", "market_cap": 500, "lcp": 22.80},
    {"scrip": "MODEFENCE", "sector": "Thematic - Defence", "segment": "Defence Index", "market_cap": 700, "lcp": 35.40},
    {"scrip": "MOM30IETF", "sector": "Thematic - Momentum", "segment": "Top 200 Momentum", "market_cap": 1100, "lcp": 19.50},
    {"scrip": "MOMENTUM50", "sector": "Thematic - Momentum", "segment": "Top 500 Momentum", "market_cap": 1400, "lcp": 22.10},
    {"scrip": "MOM100", "sector": "Thematic - Momentum", "segment": "Nifty 100 Momentum", "market_cap": 500, "lcp": 22.00},
    {"scrip": "MNC", "sector": "Thematic - MNC", "segment": "MNC Index", "market_cap": 900, "lcp": 350.60},
    {"scrip": "GROWWDEFNC", "sector": "Thematic - Defence", "segment": "Defence Sector", "market_cap": 400, "lcp": 35.50},
    {"scrip": "ALPHA", "sector": "Thematic - Alpha", "segment": "Alpha 50", "market_cap": 1200, "lcp": 45.30},
    {"scrip": "HDFCGROWTH", "sector": "Thematic - Growth", "segment": "Growth Sectors", "market_cap": 2200, "lcp": 35.60},
    {"scrip": "MSCIINDIA", "sector": "Thematic - Global", "segment": "MSCI India", "market_cap": 2500, "lcp": 28.40},
    {"scrip": "ESG", "sector": "Thematic - ESG", "segment": "ESG Sector Leaders", "market_cap": 1600, "lcp": 38.50},
    {"scrip": "DIVOPPBEES", "sector": "Thematic - Dividend", "segment": "Dividend Opp 50", "market_cap": 2100, "lcp": 42.80},
    {"scrip": "SELECTIPO", "sector": "Thematic - IPO", "segment": "Select IPO", "market_cap": 300, "lcp": 12.80},
    {"scrip": "LOWVOLIETF", "sector": "Thematic - Low Vol", "segment": "Low Volatility", "market_cap": 800, "lcp": 42.30},
    {"scrip": "TOP10ADD", "sector": "Thematic - Top 10", "segment": "Nifty Top 10", "market_cap": 600, "lcp": 11.20},
    {"scrip": "MULTICAP", "sector": "Thematic - Multicap", "segment": "Multicap 50", "market_cap": 1000, "lcp": 14.20},
    {"scrip": "MIDSMALL", "sector": "Thematic - MidSmall", "segment": "MidSmallCap 400", "market_cap": 1700, "lcp": 12.40},
    {"scrip": "AONETOTAL", "sector": "Thematic - Top 750", "segment": "Top 750", "market_cap": 3500, "lcp": 120.00},
    {"scrip": "LIQUIDCASE", "sector": "Thematic - Liquid", "segment": "Liquid Assets", "market_cap": 4000, "lcp": 1050.00},
    {"scrip": "MAHKTECH", "sector": "Thematic - Technology", "segment": "Technology", "market_cap": 1200, "lcp": 28.50},  # ADDED: Mahindra Manulife Tech ETF
    {"scrip": "SBIBPB", "sector": "Thematic - Banking", "segment": "Private Bank", "market_cap": 2800, "lcp": 145.20},  # ADDED: SBI Private Bank ETF
    {"scrip": "MOVALUE", "sector": "Thematic - Value", "segment": "Value Investing", "market_cap": 900, "lcp": 18.75},  # ADDED: Motilal Oswal Value ETF
    {"scrip": "VAL30IETF", "sector": "Thematic - Value", "segment": "Nifty 500 Value 50", "market_cap": 750, "lcp": 22.30},  # ADDED: ICICI Pru Value 30 ETF
    
    # ============ COMMODITY ETFs - COMMODITIES ============
    {"scrip": "COMMOIETF", "sector": "ETF - Commodities", "segment": "Commodities", "market_cap": 600, "lcp": 88.90},
    
    # ============ COMMODITY ETFs - GOLD ============
    {"scrip": "GOLDBEES", "sector": "Commodity - Gold", "segment": "Gold", "market_cap": 18000, "lcp": 52.40},
    {"scrip": "SETFGOLD", "sector": "Commodity - Gold", "segment": "Gold", "market_cap": 4200, "lcp": 52.45},
    {"scrip": "HDFCGOLD", "sector": "Commodity - Gold", "segment": "Gold", "market_cap": 3800, "lcp": 52.30},
    {"scrip": "AXISGOLD", "sector": "Commodity - Gold", "segment": "Gold", "market_cap": 2800, "lcp": 52.40},
    {"scrip": "GOLDCASE", "sector": "Commodity - Gold", "segment": "Gold", "market_cap": 350, "lcp": 52.50},
    {"scrip": "GROWWGOLD", "sector": "Commodity - Gold", "segment": "Gold", "market_cap": 400, "lcp": 52.60},  # ADDED: Groww Gold ETF
    {"scrip": "GOLDBETA", "sector": "Commodity - Gold", "segment": "Gold", "market_cap": 600, "lcp": 52.55},
    {"scrip": "GOLDIETF", "sector": "Commodity - Gold", "segment": "Gold", "market_cap": 450, "lcp": 52.48},  # ADDED: Nippon India Gold ETF
    {"scrip": "TATAGOLD", "sector": "Commodity - Gold", "segment": "Gold", "market_cap": 380, "lcp": 52.35},  # ADDED: Tata Gold ETF
    {"scrip": "GOLD1", "sector": "Commodity - Gold", "segment": "Gold", "market_cap": 320, "lcp": 52.42},  # ADDED: Gold ETF
    
    # ============ COMMODITY ETFs - SILVER ============
    {"scrip": "SILVERBEES", "sector": "Commodity - Silver", "segment": "Silver", "market_cap": 8000, "lcp": 72.60},
    {"scrip": "SILVER", "sector": "Commodity - Silver", "segment": "Silver", "market_cap": 4200, "lcp": 72.70},
    {"scrip": "HDFCSILVER", "sector": "Commodity - Silver", "segment": "Silver", "market_cap": 1600, "lcp": 72.60},
    {"scrip": "SBISILVER", "sector": "Commodity - Silver", "segment": "Silver", "market_cap": 550, "lcp": 72.60},
    {"scrip": "SILVERCASE", "sector": "Commodity - Silver", "segment": "Silver", "market_cap": 420, "lcp": 72.75},
    {"scrip": "SILVER1", "sector": "Commodity - Silver", "segment": "Silver", "market_cap": 700, "lcp": 72.80},
    {"scrip": "AXISILVER", "sector": "Commodity - Silver", "segment": "Silver", "market_cap": 650, "lcp": 72.70},
    {"scrip": "TATSILV", "sector": "Commodity - Silver", "segment": "Silver", "market_cap": 480, "lcp": 72.65},  # ADDED: Tata Silver ETF
    {"scrip": "SILVERIETF", "sector": "Commodity - Silver", "segment": "Silver", "market_cap": 350, "lcp": 72.55},  # ADDED: Silver ETF
    
    # ============ FIXED INCOME / DEBT ETFs ============
    {"scrip": "GILT5YBEES", "sector": "Fixed Income", "segment": "5 Year Gilt", "market_cap": 5000, "lcp": 28.10},
    {"scrip": "LTGILTBEES", "sector": "Fixed Income", "segment": "Long Term Gilt", "market_cap": 3000, "lcp": 30.20},
    {"scrip": "LIQUIDETF", "sector": "Fixed Income", "segment": "Liquid", "market_cap": 2100, "lcp": 1000.80},
]

# Extract unique sectors for sector filter
ALL_ETF_SECTORS = sorted(set(e["sector"] for e in ALL_ETF_UNIVERSE))

# ETF Selection Constraints
MIN_ETF_SELECTION = 20
MAX_ETF_SELECTION = 75

# Benchmarks for comparison
BENCHMARKS = [
    {"name": "Nifty 50",         "yahoo": "^NSEI",           "etf_scrip": None},
    {"name": "Midcap 150",       "yahoo": "MID150BEES.NS",   "etf_scrip": "MID150BEES"},
    {"name": "Smallcap 250",     "yahoo": "HDFCSML250.NS",   "etf_scrip": "HDFCSML250"},
    {"name": "Next 50",          "yahoo": "JUNIORBEES.NS",   "etf_scrip": "JUNIORBEES"},
    {"name": "Gold",             "yahoo": "GOLDBEES.NS",     "etf_scrip": "GOLDBEES"},
    {"name": "Silver",           "yahoo": "SILVERBEES.NS",   "etf_scrip": "SILVERBEES"},
]

CACHE_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), ".price_cache.json")
CACHE_VERSION = 18  # v18 - cleaned list, removed delisted ETFs with no data

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
    """Create a requests Session with proxy and SSL settings for corporate networks.
    Tries direct connection first, falls back to proxy only if direct fails."""
    import requests
    import urllib3
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
    session = requests.Session()
    session.verify = False
    session.headers.update({
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                       "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    })

    # Test direct connection first
    try:
        test_resp = session.get("https://query2.finance.yahoo.com/v8/finance/chart/AAPL?interval=1d&range=1d", timeout=5)
        if test_resp.status_code == 200 and test_resp.json().get("chart", {}).get("result"):
            print(f"[Network] Direct connection OK (status {test_resp.status_code}), using no proxy")
            return session
    except Exception:
        pass

    # Fall back to proxy
    session.proxies = {
        "http": PROXY_URL,
        "https": PROXY_URL,
    }
    print(f"[Network] Using proxy: {PROXY_URL}")
    return session


def _fetch_yahoo_chart(session, ticker, start_date, end_date):
    """Fetch price data using Yahoo Finance chart API."""
    import time
    # Convert dates to timestamps
    start_ts = int(datetime.strptime(start_date, "%Y-%m-%d").timestamp())
    end_ts = int(datetime.strptime(end_date, "%Y-%m-%d").timestamp())
    
    url = f"https://query2.finance.yahoo.com/v8/finance/chart/{ticker}"
    params = {
        "period1": start_ts,
        "period2": end_ts,
        "interval": "1d",
        "includePrePost": "true",
    }
    
    max_retries = 3
    for attempt in range(max_retries):
        try:
            resp = session.get(url, params=params, timeout=10)
            resp.raise_for_status()
            data = resp.json()
            
            if "chart" in data and data["chart"]["result"]:
                result = data["chart"]["result"][0]
                if "timestamp" in result and "close" in result["indicators"]["quote"][0]:
                    timestamps = result["timestamp"]
                    closes = result["indicators"]["quote"][0]["close"]
                    
                    # Convert timestamps to date strings and map to close prices
                    prices = {}
                    for ts, close in zip(timestamps, closes):
                        if close is not None:  # Skip None values (holidays, no data)
                            date_str = datetime.fromtimestamp(ts).strftime("%Y-%m-%d")
                            prices[date_str] = close
                    
                    return prices
            else:
                return {}
        except Exception as e:
            if attempt == 0:
                print(f"[Debug] {ticker}: {type(e).__name__}: {e}")
            if attempt < max_retries - 1:
                time.sleep(1)
    return {}


def _download_etf_prices(start_date, end_date):
    """Download ETF close prices from NSE (.NS suffix) using Yahoo chart API."""
    import time
    print(f"\nDownloading {len(ALL_ETF_UNIVERSE)} ETFs ({start_date} to {end_date})...")

    etf_prices = {}
    trading_days = set()
    session = _make_session()
    total = len(ALL_ETF_UNIVERSE)

    for idx, etf in enumerate(ALL_ETF_UNIVERSE):
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
