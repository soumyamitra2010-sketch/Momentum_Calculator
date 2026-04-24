# Momentum ETF Calculator — Full Project Prompt

Build a complete **Momentum-based ETF Rotation Backtester** as a single-page web application with a Python Flask backend. The app ranks Indian NSE-listed ETFs by multi-timeframe momentum scores, runs historical backtests with unit-based portfolio tracking, and displays results with interactive charts and validation tables.

---

## Architecture

```
momentum-calculator/
├── etf_data.py        # ETF universe, benchmarks, live Yahoo Finance data downloader with cache
├── engine.py          # Core momentum engine: indicators, ranking, rebalancing, backtest
├── app.py             # Flask API server serving HTML + REST endpoints
├── export_csv.py      # Standalone CSV exporter for daily rankings
├── index.html         # Single-page dark-themed UI (Chart.js charts, tabs, tables)
├── requirements.txt   # Python dependencies
└── .price_cache.json  # Auto-generated daily price cache (gitignored)
```

---

## 1. ETF Universe & Data Layer (`etf_data.py`)

### ETF Universe
Define **49 NSE-listed ETFs** with metadata:
```python
ETF_UNIVERSE = [
    {"scrip": "ABSLPSE",     "sector": "ETF - PSE",                           "segment": "", "market_cap": 850,   "lcp": 10.51},
    {"scrip": "ALPHA",       "sector": "ETF - Alpha",                         "segment": "", "market_cap": 1200,  "lcp": 45.30},
    {"scrip": "AONETOTAL",   "sector": "ETF - Top 750",                       "segment": "", "market_cap": 3500,  "lcp": 120.00},
    {"scrip": "AUTOBEES",    "sector": "ETF - Auto",                          "segment": "", "market_cap": 2800,  "lcp": 230.50},
    {"scrip": "BANKBEES",    "sector": "ETF - Bank",                          "segment": "", "market_cap": 15000, "lcp": 450.20},
    {"scrip": "BFSI",        "sector": "ETF - Fin Services",                  "segment": "", "market_cap": 4500,  "lcp": 55.60},
    {"scrip": "COMMOIETF",   "sector": "ETF - Commodities",                   "segment": "", "market_cap": 600,   "lcp": 88.90},
    {"scrip": "CONSUMBEES",  "sector": "ETF - Consumption",                   "segment": "", "market_cap": 1800,  "lcp": 65.40},
    {"scrip": "CONSUMER",    "sector": "ETF - New Age Consumption",           "segment": "", "market_cap": 900,   "lcp": 32.10},
    {"scrip": "CPSEETF",     "sector": "ETF - CPSE",                          "segment": "", "market_cap": 7500,  "lcp": 78.30},
    {"scrip": "DIVOPPBEES",  "sector": "ETF - Dividend Opportunities 50",     "segment": "", "market_cap": 2100,  "lcp": 42.80},
    {"scrip": "ESG",         "sector": "ETF - Nifty 100 ESG Sector Leaders",  "segment": "", "market_cap": 1600,  "lcp": 38.50},
    {"scrip": "FINIETF",     "sector": "ETF - Fin Services Ex-Bank",          "segment": "", "market_cap": 1100,  "lcp": 29.70},
    {"scrip": "FMCGIETF",    "sector": "ETF - FMCG",                          "segment": "", "market_cap": 3200,  "lcp": 55.90},
    {"scrip": "GILT5YBEES",  "sector": "ETF - Fixed Income",                  "segment": "", "market_cap": 5000,  "lcp": 28.10},
    {"scrip": "GOLDBEES",    "sector": "ETF - GOLD",                          "segment": "", "market_cap": 18000, "lcp": 52.40},
    {"scrip": "GROWWEV",     "sector": "ETF - EV and New Age Automotive",     "segment": "", "market_cap": 400,   "lcp": 18.90},
    {"scrip": "GROWWRAIL",   "sector": "ETF - Railways PSU",                  "segment": "", "market_cap": 350,   "lcp": 22.50},
    {"scrip": "HDFCGROWTH",  "sector": "ETF - Nifty Growth Sectors 15",       "segment": "", "market_cap": 2200,  "lcp": 35.60},
    {"scrip": "HDFCSML250",  "sector": "ETF - SmallCap",                      "segment": "", "market_cap": 1500,  "lcp": 15.80},
    {"scrip": "HEALTHIETF",  "sector": "ETF - Healthcare",                    "segment": "", "market_cap": 2600,  "lcp": 48.30},
    {"scrip": "ICICIB22",    "sector": "ETF - Bharat 22 Index",               "segment": "", "market_cap": 6000,  "lcp": 95.20},
    {"scrip": "INFRAIETF",   "sector": "ETF - Infrastructure",                "segment": "", "market_cap": 1900,  "lcp": 62.10},
    {"scrip": "ITBEES",      "sector": "ETF - IT",                            "segment": "", "market_cap": 8500,  "lcp": 38.70},
    {"scrip": "JUNIORBEES",  "sector": "ETF - Next 50",                       "segment": "", "market_cap": 7000,  "lcp": 680.50},
    {"scrip": "LIQUIDCASE",  "sector": "ETF - Liquid Assets",                 "segment": "", "market_cap": 4000,  "lcp": 1050.00},
    {"scrip": "LOWVOLIETF",  "sector": "ETF - Top 100",                       "segment": "", "market_cap": 800,   "lcp": 42.30},
    {"scrip": "LTGILTBEES",  "sector": "ETF - Fixed Income",                  "segment": "", "market_cap": 3000,  "lcp": 30.20},
    {"scrip": "MAKEINDIA",   "sector": "ETF - Manufacturing",                 "segment": "", "market_cap": 1300,  "lcp": 28.90},
    {"scrip": "METALIETF",   "sector": "ETF - Metal",                         "segment": "", "market_cap": 2000,  "lcp": 18.60},
    {"scrip": "MID150BEES",  "sector": "ETF - MidCap",                        "segment": "", "market_cap": 3800,  "lcp": 18.50},
    {"scrip": "MIDSMALL",    "sector": "ETF - MidSmallCap",                   "segment": "", "market_cap": 1700,  "lcp": 12.40},
    {"scrip": "MNC",         "sector": "ETF - MNC",                           "segment": "", "market_cap": 900,   "lcp": 350.60},
    {"scrip": "MOCAPITAL",   "sector": "ETF - Capital Markets",               "segment": "", "market_cap": 500,   "lcp": 22.80},
    {"scrip": "MODEFENCE",   "sector": "ETF - Defence",                       "segment": "", "market_cap": 700,   "lcp": 35.40},
    {"scrip": "MOM30IETF",   "sector": "ETF - Top 200",                       "segment": "", "market_cap": 1100,  "lcp": 19.50},
    {"scrip": "MOMENTUM50",  "sector": "ETF - Top 500",                       "segment": "", "market_cap": 1400,  "lcp": 22.10},
    {"scrip": "MOREALTY",    "sector": "ETF - Realty",                         "segment": "", "market_cap": 600,   "lcp": 15.30},
    {"scrip": "MSCIINDIA",   "sector": "ETF - MSCI India Index",              "segment": "", "market_cap": 2500,  "lcp": 28.40},
    {"scrip": "MULTICAP",    "sector": "ETF - Multicap",                      "segment": "", "market_cap": 1000,  "lcp": 14.20},
    {"scrip": "NIFTYBEES",   "sector": "ETF - Top 50",                        "segment": "", "market_cap": 25000, "lcp": 260.50},
    {"scrip": "OILIETF",     "sector": "ETF - Oil and Gas",                   "segment": "", "market_cap": 1200,  "lcp": 18.90},
    {"scrip": "PHARMABEES",  "sector": "ETF - Pharma",                        "segment": "", "market_cap": 3500,  "lcp": 19.20},
    {"scrip": "PSUBNKBEES",  "sector": "ETF - PSU Bank",                      "segment": "", "market_cap": 5500,  "lcp": 72.30},
    {"scrip": "PVTBANIETF",  "sector": "ETF - Pvt Bank",                      "segment": "", "market_cap": 4200,  "lcp": 32.50},
    {"scrip": "SELECTIPO",   "sector": "ETF - BSE Select IPO",                "segment": "", "market_cap": 300,   "lcp": 12.80},
    {"scrip": "SILVERBEES",  "sector": "ETF - SILVER",                        "segment": "", "market_cap": 8000,  "lcp": 72.60},
    {"scrip": "TNIDETF",     "sector": "ETF - Digital",                       "segment": "", "market_cap": 500,   "lcp": 15.40},
    {"scrip": "TOP10ADD",    "sector": "ETF - Top 10",                        "segment": "", "market_cap": 600,   "lcp": 11.20},
]
```

### Benchmarks (6 benchmarks)
```python
BENCHMARKS = [
    {"name": "Nifty 50",       "yahoo": "^NSEI",          "etf_scrip": None},
    {"name": "Midcap 150",     "yahoo": "MID150BEES.NS",  "etf_scrip": "MID150BEES"},
    {"name": "Smallcap 250",   "yahoo": "HDFCSML250.NS",  "etf_scrip": "HDFCSML250"},
    {"name": "Next 50",        "yahoo": "JUNIORBEES.NS",  "etf_scrip": "JUNIORBEES"},
    {"name": "Gold",           "yahoo": "GOLDBEES.NS",    "etf_scrip": "GOLDBEES"},
    {"name": "Silver",         "yahoo": "SILVERBEES.NS",  "etf_scrip": "SILVERBEES"},
]
```

Where `etf_scrip` is set, reuse the already-downloaded ETF price data instead of fetching separately.

### Data Download
- Use **Yahoo Finance v8 Chart API directly** (NOT the yfinance library) to support corporate proxy environments:
  ```
  https://query2.finance.yahoo.com/v8/finance/chart/{TICKER}?period1={ts}&period2={ts}&interval=1d
  ```
- ETF tickers use `.NS` suffix (e.g. `NIFTYBEES.NS`)
- Support corporate HTTP proxy via `PROXY_URL` constant (default `http://zs-proxy.agl.int/`), also checks `HTTPS_PROXY`/`HTTP_PROXY` env vars
- Disable SSL verification (`verify=False`) and set Chrome User-Agent header
- Retry up to 3 times with backoff on HTTP 429
- Small delay (0.5s every 5 ETFs) to avoid rate-limiting
- **Daily cache**: Save all data to `.price_cache.json` with `cache_date` and `CACHE_VERSION` (currently 2). Reload from cache if same day and version.

### Return Format
`download_all_data()` returns:
- `etf_prices`: `{scrip: {date_str: close_price}}`
- `benchmark_prices`: `{benchmark_name: {date_str: close_price}}` (multi-benchmark dict)
- `trading_days`: sorted list of all unique date strings across ETFs and benchmarks

---

## 2. Momentum Engine (`engine.py`)

### Class: `MomentumEngine`
Initialized by calling `download_all_data()`. Stores `prices`, `benchmark_prices`, `trading_days`, `etf_meta`.

### Indicator Functions
All indicator functions use **95% tolerance** for gap handling (holidays/missing data):
- `return_over(ticker, date, window)` — Simple return over N trading days
- `ema200(ticker, date)` — 200-day exponential moving average
- `sharpe_return(ticker, date, window=252)` — Annualized Sharpe ratio
- `rsi(ticker, date, period=14)` — Relative Strength Index
- `volatility(ticker, date, window=252)` — Annualized volatility
- `has_history(ticker, date, lookback)` — Check if ETF has enough data (95% of lookback)

### Ranking Algorithm (`rank_universe`)
1. Filter ETFs: require `has_history` for EVERY timeframe (not just max)
2. Optional EMA filter: skip if price <= EMA200
3. Compute weighted momentum score: `score = Σ(return_i × weight_i)`
4. Sort by score desc, tie-break by Sharpe desc, then market_cap desc

### Portfolio Selection
`select_portfolio(date, timeframes, weights, ema_filter, portfolio_size)` — Returns top N tickers.

### Rebalancing Logic (Partial Swap)
`rebalance(portfolio, date, ...)` — **Only exits ETFs ranked below `exit_rank` threshold** (default: 2× portfolio size). Does NOT sell continuing ETFs. Only the capital from exited ETFs is redistributed equally to new entries. Continuing ETFs keep their original units, buy prices, and buy dates.

### Backtest Engine (`run_backtest`)
**Unit-based portfolio tracking** (not percentage-based):
- Track `units`, `buy_prices`, `buy_dates` per ETF
- Portfolio value = Σ(units × current_price) each day
- On rebalance: only exit→entry capital swaps with optional transaction cost
- Daily equity curve + per-benchmark scaled curves (all start at `initial_capital`)

**Multi-benchmark handling**:
- Compute `bench_start_prices` per benchmark — search backward from start date, then **forward** if benchmark has no data before start (handles late-starting ETFs like HDFCSML250)
- Benchmark curves: `initial_capital × (current_price / start_price)`
- Primary benchmark (`Nifty 50`) used for beta and alpha calculations

**Output includes**:
- `equity_curve`: `[{date, value}]`
- `benchmark_curves`: `{name: [{date, value}]}` for all 6 benchmarks
- `benchmark_names`: list of benchmark names
- `events`: Initial selection + each rebalance with full `holdings_detail` per ETF
- `metrics`: total_return, cagr, sharpe, max_drawdown, win_rate, ann_volatility, beta
- `final_portfolio`, `final_weights`, `final_holdings_detail`
- `universe_snapshot`: All ETFs ranked with returns, sharpe, RSI, volatility
- `monthly_summary`: Month-by-month validation with per-benchmark returns, alpha, drawdown

### Monthly Summary Builder (`_build_monthly_summary`)
Per month: open, close, high, low, return%, cumulative return%, benchmark returns (per benchmark), alpha% (vs Nifty 50), drawdown from peak, active portfolio, rebalance events, exits, entries, transaction costs.

---

## 3. Flask API Server (`app.py`)

### Endpoints
| Endpoint | Method | Description |
|----------|--------|-------------|
| `/` | GET | Serve `index.html` |
| `/api/universe` | GET | Return ETF_UNIVERSE list |
| `/api/indicators` | GET | Current indicators for all ETFs on a date |
| `/api/rankings` | GET | Ranked ETFs with scores |
| `/api/info` | GET | Date range, ETF count, benchmarks loaded |
| `/api/backtest` | POST | Run full backtest, return JSON |
| `/api/export_csv` | POST | Generate and download CSV of rankings |

### Backtest POST body:
```json
{
  "timeframes": [252, 50, 20],
  "weights": [1, 1, 1],
  "ema_filter": false,
  "portfolio_size": 5,
  "start_date": "2023-06-01",
  "end_date": "2026-04-17",
  "frequency": "monthly",
  "rebal_day": 1,
  "initial_capital": 1000000,
  "transaction_cost_pct": 0,
  "exit_rank": 0
}
```

Portfolio size constrained to `[5, 6, 7]`. Frequency: `weekly` or `monthly`.

---

## 4. Frontend (`index.html`)

### Design
- **Dark theme** (CSS custom properties): bg `#0f1117`, cards `#1a1d29`, accent `#6366f1` (indigo)
- Single-page with 5 tabs: Configuration, Results, Universe, Events Log, Monthly View
- Chart.js 4.4.4 for all charts
- Fully responsive, mobile-friendly grid layouts

### Configuration Tab
- Start/end date pickers (end date auto-populated from `/api/info`)
- Portfolio size dropdown (5/6/7)
- Rebalance frequency (monthly/weekly) + rebalance day
- Initial capital, transaction cost %, exit rank threshold
- EMA filter toggle switch
- Dynamic timeframe rows: window (days) + weight, add/remove buttons
- Default: 252d/50d/20d with equal weight 1

### Results Tab
- **Metrics row**: 8 cards (Total Return, CAGR, Sharpe, Max Drawdown, Win Rate, Volatility, Beta, Final Capital) with green/red coloring
- **Equity Curve chart**: Portfolio line (solid indigo) + selectable benchmark lines (dashed, color-coded)
  - **Benchmark selector**: Checkboxes above chart, only "Nifty 50" checked by default, user can toggle any combination
  - Charts re-render instantly on checkbox change using stored `lastResult`
  - Downsampled to max 500 points for performance
  - Y-axis in Lakhs (₹L), tooltips with full ₹ formatting
- **Final Portfolio table**: #, Scrip, Sector, Units, Buy Price, Buy Date, Current Price, Invested, Current Value, P&L%, Weight, Rank + Total row

### Universe Tab
- Full ranked ETF table with dynamic return columns per timeframe
- Score, Sharpe, RSI, Volatility, In Portfolio indicator
- Portfolio ETFs highlighted with accent background

### Events Log Tab
- Timeline of all rebalancing events with:
  - Per-ETF holdings detail table (units, buy price, current price, invested, current value, P&L%)
  - Exit (red) / Entry (green) labels
  - Capital and transaction cost

### Monthly View Tab
- **Monthly Equity Chart**: Same benchmark selector pattern as daily chart
- **Month Explorer**: Dropdown to select any month, shows:
  - Running portfolio table with units, buy price, current value, P&L%, weight, rank
  - Month start/end/return metrics
  - Event log for that specific month
- **Monthly Validation Table**: Dynamic columns:
  - Month, Open, Close, High, Low, Month Ret%, Cumul. Ret%
  - Per-benchmark return% columns (dynamically built from `benchmark_names`)
  - Alpha%, DD from Peak%, Peak, Trading Days, Rebalances, Exits, Entries, Txn Cost, Portfolio

### Benchmark Selector (shared component)
```javascript
// Built for both daily (#bmSelectorDaily) and monthly (#bmSelectorMonthly) charts
// Color palette: ['#8b8fa3', '#f59e0b', '#ef4444', '#10b981', '#fbbf24', '#94a3b8', '#ec4899']
// Dash patterns: [[5,3],[8,4],[3,3],[6,3],[10,5],[4,4],[7,3]]
// Default: only "Nifty 50" checked
// Colors stay consistent per benchmark regardless of selection
```

---

## 5. CSV Exporter (`export_csv.py`)

Standalone script that exports **daily** data for all trading days:
- For every trading day between start and end: all ETFs with Close, Price Nd ago, Return%, Rank per timeframe, Combined Score, Overall Rank, In Portfolio flag
- Heavy indicators (Sharpe, RSI, Volatility, EMA200) only computed on rebalance dates for speed
- ETFs with 252d data ranked before those without
- Default: `start_date="2023-01-02"`, `rebal_day=21`, output `momentum_rankings_v2.csv`

---

## 6. Key Design Decisions

1. **Direct Yahoo API** instead of yfinance library — works behind corporate proxies that block pip packages or have SSL issues
2. **95% tolerance** on all indicator lookbacks — handles market holidays and minor data gaps without skipping ETFs entirely
3. **Unit-based tracking** — portfolio value derived from actual units held, not percentage allocations. Gives realistic P&L per ETF.
4. **Partial rebalancing** — Only exits poorly-ranked ETFs, keeps winners. Reduces turnover and transaction costs.
5. **Forward search for benchmark start prices** — Handles benchmarks (like HDFCSML250) that have no data before the backtest start date. Searches forward for first available price instead of falling back to 1.0 (which would cause absurd scaling).
6. **Daily cache with version** — Avoids re-downloading ~50 tickers on every server restart. Bumping `CACHE_VERSION` forces refresh.
7. **Benchmark colors are index-stable** — Each benchmark always gets the same color regardless of which are selected for display.

---

## 7. Requirements

```
flask>=3.0
numpy>=1.24
pandas>=1.5
requests
```

Note: `yfinance` is listed in requirements.txt but not actually used (direct API calls instead). `requests` is used but comes with Flask.

---

## 8. Running the Project

```bash
python -m venv .venv
.venv\Scripts\activate        # Windows
pip install -r requirements.txt
python app.py                 # Starts Flask on http://localhost:5000
```

For CSV export:
```bash
python export_csv.py          # Outputs momentum_rankings_v2.csv
```
