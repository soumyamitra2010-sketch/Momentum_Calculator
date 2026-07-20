# Momentum ETF Calculator — Technical Documentation

## 1. Overview

The **Momentum ETF Calculator** is a web-based backtesting tool that selects and rebalances a portfolio of Indian NSE-listed ETFs using a momentum-based ranking strategy. It supports one-time lump sum investment, recurring SIP (Systematic Investment Plan), or a combination of both.

**Tech Stack:** Python (Flask backend) + Single-page HTML/CSS/JS frontend  
**Data Source:** NSE via Yahoo Finance API (`.NS` suffix tickers)  
**URL:** `http://localhost:5000`

---

## 2. Architecture

```
┌──────────────┐     HTTP/JSON     ┌──────────────┐
│  index.html  │ ◄───────────────► │   app.py     │
│  (Frontend)  │                   │ (Flask API)  │
└──────────────┘                   └──────┬───────┘
                                          │
                                   ┌──────▼───────┐
                                   │  engine.py   │
                                   │ (Core Logic) │
                                   └──────┬───────┘
                                          │
                                   ┌──────▼───────┐
                                   │ etf_data.py  │
                                   │ (Data Layer) │
                                   └──────────────┘
```

### Files

| File | Purpose |
|---|---|
| `app.py` | Flask API server — routes, validation, serves `index.html` |
| `engine.py` | Core momentum engine — indicators, ranking, rebalancing, backtest simulation |
| `etf_data.py` | ETF universe definition, benchmark list, Yahoo Finance data download & caching |
| `index.html` | Single-file frontend — UI, charts (Chart.js), all JavaScript logic |
| `requirements.txt` | Python dependencies: `flask`, `numpy`, `pandas`, `yfinance` |
| `.price_cache.json` | Daily price cache (auto-generated, not committed) |

---

## 3. ETF Universe

**49 NSE-listed ETFs** across sectors:

| Category | Examples |
|---|---|
| Broad Market | NIFTYBEES (Top 50), JUNIORBEES (Next 50), MID150BEES, HDFCSML250 |
| Sectoral | BANKBEES, ITBEES, PHARMABEES, AUTOBEES, FMCGIETF, HEALTHIETF |
| Thematic | CPSEETF, INFRAIETF, MAKEINDIA, MODEFENCE, GROWWEV, GROWWRAIL |
| Factor | ALPHA, LOWVOLIETF, MOM30IETF, MOMENTUM50, ESG |
| Commodity | GOLDBEES, SILVERBEES, COMMOIETF, OILIETF |
| Fixed Income | GILT5YBEES, LTGILTBEES, LIQUIDCASE, ICICIB22 |

Each ETF has: `scrip` (ticker), `sector`, `market_cap` (indicative ₹Cr).

### Benchmarks (6)

| Benchmark | Source |
|---|---|
| Nifty 50 | Yahoo `^NSEI` (primary benchmark for Beta) |
| Midcap 150 | From ETF `MID150BEES` |
| Smallcap 250 | From ETF `HDFCSML250` |
| Next 50 | From ETF `JUNIORBEES` |
| Gold | From ETF `GOLDBEES` |
| Silver | From ETF `SILVERBEES` |

---

## 4. Data Layer (`etf_data.py`)

### Download Flow
1. Check `.price_cache.json` — if today's cache exists with correct version, use it
2. Otherwise, download all 49 ETFs from Yahoo Finance v8 chart API (`query2.finance.yahoo.com`)
3. Download 6 benchmarks (1 index via `^NSEI`, 5 reuse ETF prices already downloaded)
4. Merge trading days from all sources
5. Save to cache for same-day reuse

### Data Format
- `etf_prices`: `{scrip: {date_str: close_price}}` — e.g., `{"NIFTYBEES": {"2024-01-02": 260.5, ...}}`
- `benchmark_prices`: `{name: {date_str: close_price}}` — e.g., `{"Nifty 50": {"2024-01-02": 21750.0, ...}}`
- `trading_days`: sorted list of `"YYYY-MM-DD"` strings (union of all data)

### Proxy & SSL
- Uses corporate proxy (`HTTPS_PROXY` env var or hardcoded fallback)
- SSL verification disabled for corporate network compatibility
- 3 retry attempts per download with 1s delay

---

## 5. Indicators (`engine.py`)

### 5.1 Return Over Period
```
return_over(ticker, date, window) → float | None
```
- Calculates `(price_end / price_start) - 1` over `window` trading days
- Requires 95% of data points present (allows 5% gaps for holidays)

### 5.2 EMA 200
```
ema200(ticker, date) → float | None
```
- 200-day Exponential Moving Average
- Smoothing factor `k = 2 / (200 + 1)`
- Used as trend filter: only include ETFs where `close > EMA200`

### 5.3 Sharpe Ratio
```
sharpe_return(ticker, date, window=252) → float | None
```
- `Sharpe = (mean_daily_return / std_daily_return) × √252`
- Annualised, calculated over 252 trading days (1 year)

### 5.4 RSI (Relative Strength Index)
```
rsi(ticker, date, period=14) → float | None
```
- Standard 14-period RSI
- `RSI = 100 - (100 / (1 + RS))` where `RS = avg_gain / avg_loss`

### 5.5 Volatility
```
volatility(ticker, date, window=252) → float | None
```
- Annualised volatility: `std(daily_returns) × √252`

---

## 6. Ranking & Selection Algorithm

### Momentum Score Calculation

For each ETF on a given date:
1. Calculate returns over each configured timeframe (e.g., 252d, 50d, 20d)
2. Apply user-defined weights to each timeframe return
3. **Score = Σ(return_i × weight_i)** (weights are normalised to sum to 1)

### Ranking Rules
1. **Primary sort:** Score descending (highest momentum first)
2. **Tie-break 1:** Sharpe ratio descending
3. **Tie-break 2:** Market cap descending

### Filters
- **History filter:** ETF must have ≥95% data for ALL timeframes (not just max)
- **EMA filter (optional):** Close price must be above 200-day EMA

### Portfolio Selection
- Top N ETFs by rank (N = portfolio_size: 5, 6, or 7)

---

## 7. Investment Plans

### 7.1 One Time Investment (`onetime`)
- Full `initial_capital` invested at the start of the backtest period
- Divided equally among the selected portfolio ETFs
- **Initial month rebalancing: SKIPPED** (portfolio holds as-is for first month)

### 7.2 SIP (`sip`)
- No money invested at start (`initial_capital = 0`)
- Portfolio ETFs are identified at start but no units purchased
- At **each rebalance date** (including the first one in the start month):
  - SIP amount is divided equally among current portfolio ETFs
  - Units are purchased at current market prices
  - Portfolio rebalancing (exits/entries) also happens simultaneously

### 7.3 Both (`both`)
- **Initial capital** invested as lump sum at start (like One Time)
- **Initial month:** Portfolio rebalancing is SKIPPED for lump sum, but SIP amount IS invested on the rebalance date (divided equally across current ETFs)
- **Subsequent months:** Both portfolio rebalancing and SIP injection happen at each rebalance date

### SIP Injection Logic (at each rebalance date)
```
sip_alloc = sip_amount / number_of_portfolio_etfs
For each ETF in portfolio:
    new_units = sip_alloc / current_price
    Weighted avg buy price is updated:
        total_cost = (old_units × old_buy_price) + (new_units × current_price)
        new_buy_price = total_cost / total_units
```

---

## 8. Rebalancing Logic

### Rebalance Date Calculation
- **Monthly:** On `rebal_day` of each month (1-28, mapped to nearest trading day)
- **Weekly:** On specified weekday (0=Mon to 4=Fri)
- **Quarterly:** On `rebal_day` of Mar, Jun, Sep, Dec

The actual rebalance uses `rebal_day - 1` as the effective day to align with start-of-period.

### Rebalance Decision
At each rebalance date:
1. Re-rank the entire universe using current momentum scores
2. Check each portfolio ETF's new rank
3. **Exit rule:** If an ETF's rank exceeds `exit_rank_threshold`, it is exited
   - Default: `exit_rank = 0` → auto-set to `2 × portfolio_size`
4. **Entry rule:** Top-ranked ETFs not in portfolio replace exited ones (1:1 swap)

### Capital Flow on Rebalance (with changes)
```
1. Calculate exit value for each exited ETF (units × current_price)
2. Apply transaction cost: cost = exit_value × txn_cost_pct / 100
3. Exit pool = exit_value - cost
4. Divide exit pool equally among new entries
5. Buy new ETF units at current prices
6. If SIP active: inject SIP amount equally across ALL current portfolio ETFs
```

### Capital Flow on Rebalance (no changes)
```
1. No exits or entries
2. If SIP active: inject SIP amount equally across ALL current portfolio ETFs
```

### Event Types
| Event | Description |
|---|---|
| `INITIAL_SELECTION` | Portfolio selected at start of backtest |
| `REBALANCE` | Portfolio rebalanced with exits and/or entries |
| `REBALANCE_NO_CHANGE` | Rebalance date but no changes needed |
| `SIP_INVESTMENT` | SIP-only injection (used for "Both" plan in initial month) |

---

## 9. Benchmark Tracking

Benchmarks use a **unit-based tracking system** that mirrors the investment plan:

### One Time Investment
- Full `initial_capital` buys benchmark units at start price
- Value = `benchmark_units × current_benchmark_price`

### SIP / Both
- For **SIP:** Starts with 0 benchmark units
- For **Both:** Initial capital buys benchmark units at start + SIP adds more
- At each SIP injection point, benchmark units are purchased:
  ```
  new_bench_units = sip_amount / current_benchmark_price
  total_bench_units += new_bench_units
  ```
- This ensures a fair apples-to-apples comparison: benchmark grows with the same investment cadence

---

## 10. Performance Metrics

| Metric | Formula |
|---|---|
| **Total Return** | `(final_capital / total_invested - 1) × 100` |
| **CAGR** | `(final / invested)^(1/years) - 1` where years = trading_days / 252 |
| **Sharpe Ratio** | `(mean_period_return / std_period_return) × √(periods_per_year)` |
| **Max Drawdown** | Maximum peak-to-trough decline in equity curve |
| **Win Rate** | % of measurement periods with positive return |
| **Ann. Volatility** | `std_period_return × √(periods_per_year)` |
| **Beta** | Covariance(portfolio, Nifty 50) / Variance(Nifty 50) using period returns |
| **Total Trades** | Count of all entry + exit transactions |
| **Total Invested** | `initial_capital + (sip_amount × number_of_sip_installments)` |

### Invested Amount by Plan
- **One Time:** `total_invested = initial_capital`
- **SIP:** `total_invested = sip_amount × number_of_rebalance_dates`
- **Both:** `total_invested = initial_capital + (sip_amount × number_of_rebalance_dates)`

---

## 11. API Endpoints

### `GET /`
Serves the `index.html` frontend.

### `GET /api/universe`
Returns the full ETF universe definition (49 ETFs with scrip, sector, market_cap).

### `GET /api/info`
Returns metadata: date range, ETFs loaded, benchmarks available.

### `GET /api/indicators?date=YYYY-MM-DD&timeframes=252,50,20`
Returns current indicators for all ETFs on a given date:
- Returns for each timeframe, Sharpe, RSI, Volatility, EMA200, Close, above_ema flag

### `GET /api/rankings?date=YYYY-MM-DD&timeframes=252,50,20&weights=1,1,1&ema_filter=false`
Returns ranked ETFs with scores.

### `POST /api/backtest`
Runs a full backtest. Request body (JSON):
```json
{
  "timeframes": [252, 50, 20],
  "weights": [1, 1, 1],
  "ema_filter": false,
  "portfolio_size": 5,
  "start_date": "2023-01-01",
  "end_date": "2026-04-24",
  "frequency": "monthly",
  "rebal_day": 1,
  "initial_capital": 1000000,
  "transaction_cost_pct": 0,
  "exit_rank": 0,
  "investment_plan": "onetime",
  "sip_amount": 50000
}
```

**Response includes:**
- `config` — Echoed configuration
- `metrics` — All performance metrics
- `equity_curve` — `[{date, value}]` at measurement points
- `benchmark_curves` — `{name: [{date, value}]}` for each benchmark
- `events` — All portfolio events (selections, rebalances, SIP investments)
- `final_portfolio` — Current holdings at end
- `final_holdings_detail` — Per-ETF units, buy price, current value, P&L%
- `universe_snapshot` — All ETFs ranked at end date
- `monthly_summary` — Month-by-month performance table

---

## 12. Frontend Features

### Tabs
1. **Setup** — Configuration form with all parameters
2. **Results** — Metrics cards, equity chart with benchmark overlay
3. **Universe** — All ETFs ranked with indicators at backtest end date
4. **Events** — Timeline of all portfolio changes
5. **Monthly** — Month-by-month performance validation table

### Configuration Controls
| Control | Options/Range |
|---|---|
| Timeframes | Configurable windows (default: 252, 50, 20 days) with weights |
| Start/End Date | Date pickers |
| Portfolio Size | 5, 6, or 7 ETFs |
| Rebalance Frequency | Weekly, Monthly, Quarterly |
| Rebalance Day | 0-60 (context-dependent) |
| Investment Plan | One Time Investment, SIP, Both |
| Initial Capital | ₹10,000+ (shown for One Time and Both) |
| SIP Amount | ₹1,000+ per month (shown for SIP and Both) |
| Transaction Cost | 0-2% |
| Exit Rank Threshold | 0 = auto (2× portfolio size), or manual 1-49 |
| 200-day EMA Filter | On/Off toggle |

### Dynamic UI Behavior
- **One Time Investment** selected → Shows "Initial Capital" input only
- **SIP** selected → Shows "SIP Amount (each Month)" input only
- **Both** selected → Shows both "Initial Capital" and "SIP Amount" inputs

---

## 13. Monthly Summary Table

Each row contains:
| Column | Description |
|---|---|
| Month | YYYY-MM |
| Open/Close/High/Low | Portfolio value at measurement points |
| Return % | Month-over-month return |
| Cumulative Return % | Return since inception vs total invested |
| Benchmark Returns | Per-benchmark monthly return % |
| Alpha % | Portfolio return - Nifty 50 return |
| Drawdown from Peak % | Current decline from all-time high |
| Portfolio | Active ETF holdings |
| Rebalances | Count of rebalances in the month |
| Exits/Entries | ETFs swapped |
| Txn Costs | Transaction costs incurred |

---

## 14. Unit-Based Portfolio Tracking

The engine tracks actual **units held** rather than abstract weights:

```
units = {etf: number_of_units}
buy_prices = {etf: weighted_avg_buy_price}
buy_dates = {etf: purchase_date}
```

- **Portfolio value** = Σ(units[etf] × current_price[etf])
- **P&L per ETF** = `(current_price / buy_price - 1) × 100`
- **Weights** = derived from `(etf_value / total_portfolio_value)`

This approach correctly handles:
- Partial swaps (only exited ETFs' value redistributed to new entries)
- SIP additions (new units added to existing holdings with weighted avg cost)
- Accurate per-ETF P&L tracking across multiple buy events

---

## 15. Edge Cases & Guards

| Scenario | Handling |
|---|---|
| Insufficient history | Start date pushed forward to ensure `max(timeframes) + 1` days of data |
| No ETFs pass filters | Returns error: "No ETFs pass filters on start date" |
| Missing price data | Returns `None`, ETF skipped from ranking |
| 95% data threshold | Allows 5% missing data points (market holidays, gaps) |
| Zero capital (SIP start) | Guards against division-by-zero in returns and benchmarks |
| Weekend/holiday rebal date | Mapped to next trading day via `_next_trading_day()` |

---

## 16. How to Run

```bash
# Install dependencies
pip install -r requirements.txt

# Run server
python app.py

# Open browser
http://localhost:5000
```

The server downloads live data on first run (takes ~1 min for 49 ETFs). Subsequent runs use the daily cache and start instantly.
