"""
Momentum Engine V2 — Vectorized ranking, log-regression × R², sector diversification.

Key improvements over v1:
- Pandas/NumPy vectorized operations instead of per-ticker loops
- Log-linear regression × R² for risk-adjusted momentum scoring
- Sector diversification with max-per-sector cap ("Skip & Fill")
- DuckDB-backed price storage for ~2.5M rows
- Supports Nifty 50 / 100 / 200 / 500 universes
- Survivorship bias warning flag
"""

import math
import numpy as np
import pandas as pd
from datetime import datetime, timedelta

from db import Database
from stock_universe import get_universe, get_sector_map, BENCHMARKS_V2


class MomentumEngineV2:
    """Vectorized momentum engine for large stock universes."""

    def __init__(self, universe_name: str = "nifty50"):
        self.universe_name = universe_name
        self.db = Database()

        # Load universe metadata
        self.stocks = get_universe(universe_name)
        self.sector_map = get_sector_map(self.stocks)
        self.stock_meta = {s["ticker"]: s for s in self.stocks}
        self.tickers = list(self.stock_meta.keys())

        # Load price matrices (pivoted DataFrames)
        self.price_df = self.db.get_price_matrix()  # all tickers
        self.bench_df = self.db.get_benchmark_matrix()

        if self.price_df.empty:
            self.trading_days = []
        else:
            self.trading_days = [d.strftime("%Y-%m-%d") for d in self.price_df.index]

        self._primary_bm = "Nifty 50"

    def reload_data(self, universe_name: str = None):
        """Reload price data (after a download)."""
        if universe_name:
            self.universe_name = universe_name
            self.stocks = get_universe(universe_name)
            self.sector_map = get_sector_map(self.stocks)
            self.stock_meta = {s["ticker"]: s for s in self.stocks}
            self.tickers = list(self.stock_meta.keys())

        self.price_df = self.db.get_price_matrix()
        self.bench_df = self.db.get_benchmark_matrix()
        if not self.price_df.empty:
            self.trading_days = [d.strftime("%Y-%m-%d") for d in self.price_df.index]
        else:
            self.trading_days = []

    # ── Price Helpers ─────────────────────────────────────────────────────

    def _get_price(self, ticker: str, date: str) -> float | None:
        try:
            ts = pd.Timestamp(date)
            # Find nearest prior date
            idx = self.price_df.index.get_indexer([ts], method="ffill")[0]
            if idx < 0:
                return None
            val = self.price_df.iloc[idx].get(ticker)
            return float(val) if pd.notna(val) else None
        except (KeyError, IndexError):
            return None

    def _date_index(self, date: str) -> int | None:
        ts = pd.Timestamp(date)
        idx = self.price_df.index.get_indexer([ts], method="ffill")[0]
        return int(idx) if idx >= 0 else None

    def _next_trading_day(self, date: str) -> str | None:
        ts = pd.Timestamp(date)
        idx = self.price_df.index.searchsorted(ts, side="left")
        if idx < len(self.price_df.index):
            return self.price_df.index[idx].strftime("%Y-%m-%d")
        return None

    # ── Vectorized Indicators ─────────────────────────────────────────────

    def calc_returns(self, window: int) -> pd.DataFrame:
        """Vectorized return calculation for all tickers over window days."""
        return self.price_df / self.price_df.shift(window) - 1

    def calc_ema(self, span: int = 200) -> pd.DataFrame:
        """Vectorized EMA for all tickers."""
        return self.price_df.ewm(span=span, adjust=False).mean()

    def calc_volatility(self, window: int = 252) -> pd.DataFrame:
        """Annualized rolling volatility for all tickers."""
        daily_ret = self.price_df.pct_change()
        return daily_ret.rolling(window).std() * np.sqrt(252)

    def calc_rsi(self, period: int = 14) -> pd.DataFrame:
        """Vectorized RSI for all tickers."""
        delta = self.price_df.diff()
        gain = delta.clip(lower=0)
        loss = (-delta).clip(lower=0)
        avg_gain = gain.rolling(period).mean()
        avg_loss = loss.rolling(period).mean()
        rs = avg_gain / avg_loss.replace(0, 1e-10)
        return 100 - (100 / (1 + rs))

    def calc_sharpe(self, window: int = 252) -> pd.DataFrame:
        """Annualized Sharpe ratio for all tickers."""
        daily_ret = self.price_df.pct_change()
        rolling_mean = daily_ret.rolling(window).mean()
        rolling_std = daily_ret.rolling(window).std()
        return (rolling_mean / rolling_std.replace(0, 1e-10)) * np.sqrt(252)

    # ── Log-Linear Regression Score (per ticker) ─────────────────────────

    def log_regression_score(self, ticker: str, date: str, window: int = 252) -> dict | None:
        """
        Calculate log-linear regression slope × R² for a single ticker.

        Returns dict with:
            annualized_return: e^(slope*252) - 1
            r_squared: coefficient of determination
            score: annualized_return × r_squared
        Or None if insufficient data.
        """
        ts = pd.Timestamp(date)
        idx = self._date_index(date)
        if idx is None or idx < window:
            return None

        if ticker not in self.price_df.columns:
            return None

        series = self.price_df[ticker].iloc[max(0, idx - window + 1):idx + 1]
        series = series.dropna()

        if len(series) < window * 0.90:
            return None

        log_prices = np.log(series.values)
        x = np.arange(len(log_prices))

        # Linear regression: log(price) = slope * day + intercept
        slope, intercept = np.polyfit(x, log_prices, 1)

        # R² calculation
        predicted = slope * x + intercept
        ss_res = np.sum((log_prices - predicted) ** 2)
        ss_tot = np.sum((log_prices - np.mean(log_prices)) ** 2)
        r_squared = max(0, 1 - (ss_res / ss_tot)) if ss_tot > 0 else 0

        # Annualize: daily slope → annual return
        annualized_return = np.exp(slope * 252) - 1

        return {
            "annualized_return": annualized_return,
            "r_squared": r_squared,
            "score": annualized_return * r_squared,
        }

    def calc_log_regression_scores(self, date: str, window: int = 252) -> dict:
        """
        Calculate log-regression scores for ALL tickers in universe on a date.
        Returns {ticker: {annualized_return, r_squared, score}} or None for each.
        """
        idx = self._date_index(date)
        if idx is None or idx < window:
            return {}

        results = {}
        # Only consider tickers that are in the universe
        universe_tickers = set(self.tickers) & set(self.price_df.columns)

        for ticker in universe_tickers:
            series = self.price_df[ticker].iloc[max(0, idx - window + 1):idx + 1].dropna()
            if len(series) < window * 0.90:
                continue

            log_prices = np.log(series.values)
            x = np.arange(len(log_prices))
            slope, intercept = np.polyfit(x, log_prices, 1)

            predicted = slope * x + intercept
            ss_res = np.sum((log_prices - predicted) ** 2)
            ss_tot = np.sum((log_prices - np.mean(log_prices)) ** 2)
            r_squared = max(0, 1 - (ss_res / ss_tot)) if ss_tot > 0 else 0

            annualized_return = np.exp(slope * 252) - 1

            results[ticker] = {
                "annualized_return": annualized_return,
                "r_squared": r_squared,
                "score": annualized_return * r_squared,
            }

        return results

    # ── Ranking Algorithm ─────────────────────────────────────────────────

    def rank_universe(self, date: str, config: dict) -> list[dict]:
        """
        Rank all stocks by momentum score.

        config keys:
            timeframes: list[int]     — lookback windows (e.g. [252, 126, 63])
            weights: list[float]      — weight per timeframe
            ema_filter: bool          — require price > 200-day EMA
            ranking_method: str       — 'weighted_return' (v1) or 'log_regression' (v2)
            regression_window: int    — window for log regression (default 252)

        Returns list of dicts sorted by score descending:
            [{ticker, score, sharpe, r_squared, volatility, sector, market_cap_cr, ...}]
        """
        ranking_method = config.get("ranking_method", "log_regression")
        timeframes = config.get("timeframes", [252, 126, 63])
        raw_weights = config.get("weights", [1, 1, 1])
        wsum = sum(raw_weights) or 1
        weights = [w / wsum for w in raw_weights]
        ema_filter = config.get("ema_filter", False)
        regression_window = config.get("regression_window", 252)

        idx = self._date_index(date)
        if idx is None:
            return []

        # Pre-compute vectorized indicators at this date
        ema_200 = self.calc_ema(200)
        sharpe_df = self.calc_sharpe(252)
        vol_df = self.calc_volatility(252)
        rsi_df = self.calc_rsi(14)

        candidates = []
        universe_tickers = set(self.tickers) & set(self.price_df.columns)

        if ranking_method == "log_regression":
            # Enhanced: log-regression × R²
            reg_scores = self.calc_log_regression_scores(date, regression_window)

            for ticker in universe_tickers:
                if ticker not in reg_scores:
                    continue

                close = self._get_price(ticker, date)
                if close is None or close <= 0:
                    continue

                # EMA filter
                if ema_filter:
                    try:
                        ema_val = float(ema_200.iloc[idx].get(ticker, np.nan))
                        if pd.isna(ema_val) or close <= ema_val:
                            continue
                    except (IndexError, KeyError):
                        continue

                reg = reg_scores[ticker]
                try:
                    sh = float(sharpe_df.iloc[idx].get(ticker, 0))
                    vol = float(vol_df.iloc[idx].get(ticker, 0))
                    rsi_val = float(rsi_df.iloc[idx].get(ticker, 50))
                except (IndexError, KeyError):
                    sh, vol, rsi_val = 0, 0, 50

                if pd.isna(sh): sh = 0
                if pd.isna(vol): vol = 0
                if pd.isna(rsi_val): rsi_val = 50

                meta = self.stock_meta.get(ticker, {})
                candidates.append({
                    "ticker": ticker,
                    "score": reg["score"],
                    "annualized_return": reg["annualized_return"],
                    "r_squared": reg["r_squared"],
                    "sharpe": sh,
                    "volatility": vol,
                    "rsi": rsi_val,
                    "close": close,
                    "sector": meta.get("sector", "Unknown"),
                    "market_cap_cr": meta.get("market_cap_cr", 0),
                    "name": meta.get("name", ticker),
                })

        else:
            # V1-style: weighted returns
            for ticker in universe_tickers:
                # Check sufficient history for all timeframes
                has_all = True
                for tf in timeframes:
                    if idx < tf:
                        has_all = False
                        break
                    series = self.price_df[ticker].iloc[max(0, idx - tf):idx + 1].dropna()
                    if len(series) < tf * 0.95:
                        has_all = False
                        break
                if not has_all:
                    continue

                close = self._get_price(ticker, date)
                if close is None or close <= 0:
                    continue

                # EMA filter
                if ema_filter:
                    try:
                        ema_val = float(ema_200.iloc[idx].get(ticker, np.nan))
                        if pd.isna(ema_val) or close <= ema_val:
                            continue
                    except (IndexError, KeyError):
                        continue

                # Weighted return score
                returns = []
                for tf in timeframes:
                    start_price = self.price_df[ticker].iloc[max(0, idx - tf)]
                    if pd.notna(start_price) and start_price > 0:
                        returns.append(close / start_price - 1)
                    else:
                        returns.append(0)

                score = sum(r * w for r, w in zip(returns, weights))

                try:
                    sh = float(sharpe_df.iloc[idx].get(ticker, 0))
                    vol = float(vol_df.iloc[idx].get(ticker, 0))
                    rsi_val = float(rsi_df.iloc[idx].get(ticker, 50))
                except (IndexError, KeyError):
                    sh, vol, rsi_val = 0, 0, 50

                if pd.isna(sh): sh = 0
                if pd.isna(vol): vol = 0
                if pd.isna(rsi_val): rsi_val = 50

                meta = self.stock_meta.get(ticker, {})
                candidates.append({
                    "ticker": ticker,
                    "score": score,
                    "annualized_return": 0,
                    "r_squared": 0,
                    "sharpe": sh,
                    "volatility": vol,
                    "rsi": rsi_val,
                    "close": close,
                    "sector": meta.get("sector", "Unknown"),
                    "market_cap_cr": meta.get("market_cap_cr", 0),
                    "name": meta.get("name", ticker),
                })

        # Sort: score desc → sharpe desc → market_cap desc
        candidates.sort(key=lambda x: (-x["score"], -x["sharpe"], -x["market_cap_cr"]))

        # Assign ranks
        for i, c in enumerate(candidates):
            c["rank"] = i + 1

        return candidates

    # ── Sector-Constrained Selection ──────────────────────────────────────

    def select_portfolio(self, date: str, config: dict) -> list[str]:
        """
        Select portfolio with sector diversification (Skip & Fill).

        config keys (in addition to rank_universe keys):
            portfolio_size: int           — number of stocks (e.g., 10-20)
            max_sector_pct: float         — max fraction per sector (0.20 = 20%)
            sector_diversification: bool  — enable/disable sector caps

        Returns list of selected tickers.
        """
        ranked = self.rank_universe(date, config)
        portfolio_size = config.get("portfolio_size", 10)
        diversify = config.get("sector_diversification", True)
        max_sector_pct = config.get("max_sector_pct", 0.25)

        if not diversify:
            return [c["ticker"] for c in ranked[:portfolio_size]]

        # Skip & Fill: enforce max stocks per sector
        max_per_sector = max(1, int(portfolio_size * max_sector_pct))
        selected = []
        sector_count = {}

        for candidate in ranked:
            sector = candidate["sector"]
            if sector_count.get(sector, 0) >= max_per_sector:
                continue  # Sector full — skip
            selected.append(candidate["ticker"])
            sector_count[sector] = sector_count.get(sector, 0) + 1
            if len(selected) >= portfolio_size:
                break

        return selected

    # ── Rebalancing ───────────────────────────────────────────────────────

    def rebalance(self, portfolio: list, date: str, config: dict) -> dict:
        """
        Rebalance portfolio with sector-aware selection.

        Returns dict with new_portfolio, exits, entries, rankings.
        """
        exit_rank = config.get("exit_rank", 0)
        portfolio_size = config.get("portfolio_size", 10)
        if exit_rank <= 0:
            exit_rank = int(portfolio_size * 1.5)  # v2: 1.5x (was 2x in v1)

        ranked = self.rank_universe(date, config)
        rank_map = {c["ticker"]: c["rank"] for c in ranked}

        # Determine exits
        exits = [t for t in portfolio if rank_map.get(t, len(ranked) + 1) > exit_rank]

        # Select replacements respecting sector diversification
        diversify = config.get("sector_diversification", True)
        max_sector_pct = config.get("max_sector_pct", 0.25)
        max_per_sector = max(1, int(portfolio_size * max_sector_pct))

        remaining = [t for t in portfolio if t not in exits]
        sector_count = {}
        if diversify:
            for t in remaining:
                sector = self.sector_map.get(t, "Unknown")
                sector_count[sector] = sector_count.get(sector, 0) + 1

        replacements = []
        for candidate in ranked:
            ticker = candidate["ticker"]
            if ticker in remaining or ticker in replacements:
                continue
            if len(replacements) >= len(exits):
                break
            if diversify:
                sector = candidate["sector"]
                if sector_count.get(sector, 0) >= max_per_sector:
                    continue
                sector_count[sector] = sector_count.get(sector, 0) + 1
            replacements.append(ticker)

        new_portfolio = remaining + replacements

        return {
            "new_portfolio": new_portfolio,
            "exits": exits,
            "entries": replacements,
            "rankings": {t: rank_map.get(t) for t in new_portfolio},
        }

    # ── Rebalancing Date Helpers ──────────────────────────────────────────

    def _get_rebalancing_dates(self, start: str, end: str,
                               frequency: str, rebal_day: int) -> list[str]:
        """Generate rebalancing dates."""
        dates = []
        s = datetime.strptime(start, "%Y-%m-%d")
        e = datetime.strptime(end, "%Y-%m-%d")
        current = s

        if frequency == "weekly":
            while current.weekday() != rebal_day:
                current += timedelta(days=1)
            while current <= e:
                td = self._next_trading_day(current.strftime("%Y-%m-%d"))
                if td and td >= start and td <= end:
                    dates.append(td)
                current += timedelta(weeks=1)
        elif frequency == "quarterly":
            while current <= e:
                try:
                    target = current.replace(day=min(rebal_day, 28))
                except ValueError:
                    target = current.replace(day=28)
                if target < s:
                    month = current.month
                    year = current.year
                    if month <= 3:
                        current = current.replace(year=year, month=4, day=1)
                    elif month <= 6:
                        current = current.replace(year=year, month=7, day=1)
                    elif month <= 9:
                        current = current.replace(year=year, month=10, day=1)
                    else:
                        current = current.replace(year=year + 1, month=1, day=1)
                    continue
                td = self._next_trading_day(target.strftime("%Y-%m-%d"))
                if td and td <= end:
                    dates.append(td)
                month = current.month
                year = current.year
                if month <= 3:
                    current = current.replace(year=year, month=4, day=1)
                elif month <= 6:
                    current = current.replace(year=year, month=7, day=1)
                elif month <= 9:
                    current = current.replace(year=year, month=10, day=1)
                else:
                    current = current.replace(year=year + 1, month=1, day=1)
        else:  # monthly
            while current <= e:
                try:
                    target = current.replace(day=min(rebal_day, 28))
                except ValueError:
                    target = current.replace(day=28)
                if target < s:
                    if current.month == 12:
                        current = current.replace(year=current.year + 1, month=1, day=1)
                    else:
                        current = current.replace(month=current.month + 1, day=1)
                    continue
                td = self._next_trading_day(target.strftime("%Y-%m-%d"))
                if td and td <= end:
                    dates.append(td)
                if current.month == 12:
                    current = current.replace(year=current.year + 1, month=1, day=1)
                else:
                    current = current.replace(month=current.month + 1, day=1)

        return sorted(set(dates))

    def _get_period_end_dates(self, start: str, end: str, frequency: str) -> list[str]:
        """Get period-end dates for measurement."""
        dates = []
        s = datetime.strptime(start, "%Y-%m-%d")
        e = datetime.strptime(end, "%Y-%m-%d")

        if frequency == "weekly":
            current = s
            while current.weekday() != 4:
                current += timedelta(days=1)
            while current <= e:
                td = self._next_trading_day(current.strftime("%Y-%m-%d"))
                if td and td >= start and td <= end:
                    dates.append(td)
                current += timedelta(weeks=1)
        else:  # monthly / quarterly
            current = s.replace(day=1)
            while current <= e:
                if current.month == 12:
                    next_month = current.replace(year=current.year + 1, month=1, day=1)
                else:
                    next_month = current.replace(month=current.month + 1, day=1)
                last_day = (next_month - timedelta(days=1)).day
                target = current.replace(day=last_day)
                if target >= s:
                    td = self._next_trading_day(target.strftime("%Y-%m-%d"))
                    if td and td >= start and td <= end:
                        dates.append(td)
                current = next_month

        return sorted(set(dates))

    # ── Backtest ──────────────────────────────────────────────────────────

    def run_backtest(self, config: dict) -> dict:
        """
        Full backtest with all v2 features.

        config keys:
            timeframes, weights, ema_filter, ranking_method, regression_window,
            portfolio_size, max_sector_pct, sector_diversification,
            start_date, end_date, frequency, rebal_day,
            initial_capital, transaction_cost_pct, exit_rank,
            investment_plan ('onetime'|'sip'|'both'), sip_amount
        """
        if not self.trading_days:
            return {"error": "No price data loaded. Download data first."}

        # Extract config
        timeframes = config.get("timeframes", [252, 126, 63])
        portfolio_size = config.get("portfolio_size", 10)
        start_date = config.get("start_date", "2015-01-01")
        end_date = config.get("end_date", self.trading_days[-1])
        frequency = config.get("frequency", "monthly")
        rebal_day = config.get("rebal_day", 1)
        initial_capital = config.get("initial_capital", 1000000)
        txn_cost_pct = config.get("transaction_cost_pct", 0.5)  # v2 default: 0.5%
        investment_plan = config.get("investment_plan", "onetime")
        sip_amount = config.get("sip_amount", 0)
        total_invested = 0

        # Ensure enough history
        max_tf = max(timeframes)
        regression_window = config.get("regression_window", 252)
        required_history = max(max_tf, regression_window)
        actual_start_idx = self._date_index(start_date)
        if actual_start_idx is None or actual_start_idx < required_history + 1:
            actual_start_idx = required_history + 1
        if actual_start_idx >= len(self.price_df):
            return {"error": "Not enough data for backtest with these parameters"}
        actual_start = self.trading_days[actual_start_idx]
        if actual_start > end_date:
            return {"error": "Start date requires more history than available"}

        # Initial portfolio selection
        portfolio = self.select_portfolio(actual_start, config)
        if not portfolio:
            return {"error": "No stocks pass filters on start date"}

        # Build rebalancing dates
        effective_rebal_day = max(1, rebal_day - 1)
        all_rebal_dates = set(self._get_rebalancing_dates(
            actual_start, end_date, frequency, effective_rebal_day))
        start_month = actual_start[:7]

        if investment_plan == "sip":
            rebal_dates = all_rebal_dates
        else:
            rebal_dates = {d for d in all_rebal_dates if d[:7] != start_month}

        sip_only_dates = set()
        if investment_plan == "both":
            sip_only_dates = {d for d in all_rebal_dates if d[:7] == start_month}

        period_end_dates = set(self._get_period_end_dates(actual_start, end_date, frequency))
        measurement_dates = sorted(rebal_dates | sip_only_dates | period_end_dates)
        if actual_start not in measurement_dates:
            measurement_dates = [actual_start] + measurement_dates

        # ── Unit-based portfolio tracking ─────────────────────────────────
        if investment_plan == "sip":
            capital = 0
            total_invested = 0
        else:
            capital = initial_capital
            total_invested = initial_capital

        units = {}
        buy_prices = {}
        buy_dates = {}

        if capital > 0:
            alloc_per = capital / len(portfolio)
            for ticker in portfolio:
                bp = self._get_price(ticker, actual_start)
                units[ticker] = alloc_per / bp if bp and bp > 0 else 0
                buy_prices[ticker] = bp or 0
                buy_dates[ticker] = actual_start

        def _portfolio_value(day):
            return sum(
                units.get(t, 0) * (self._get_price(t, day) or 0) for t in units
            )

        def _build_holdings_detail(day):
            detail = {}
            for t in units:
                cp = self._get_price(t, day) or 0
                invested = units[t] * buy_prices.get(t, 0)
                cv = units[t] * cp
                pnl = ((cp / buy_prices[t]) - 1) * 100 if buy_prices.get(t, 0) > 0 else 0
                detail[t] = {
                    "units": round(units[t], 4),
                    "buy_price": round(buy_prices.get(t, 0), 2),
                    "buy_date": buy_dates.get(t, ""),
                    "current_price": round(cp, 2),
                    "invested": round(invested, 2),
                    "current_value": round(cv, 2),
                    "pnl_pct": round(pnl, 2),
                    "sector": self.sector_map.get(t, "Unknown"),
                }
            return detail

        def _inject_sip(day):
            """Inject SIP amount into portfolio, return amount invested."""
            nonlocal total_invested
            if sip_amount <= 0 or investment_plan not in ("sip", "both"):
                return 0
            total_invested += sip_amount
            alloc = sip_amount / len(portfolio) if portfolio else 0
            for t in portfolio:
                bp_s = self._get_price(t, day)
                if bp_s and bp_s > 0:
                    new_u = alloc / bp_s
                    old_u = units.get(t, 0)
                    old_cost = old_u * buy_prices.get(t, 0)
                    units[t] = old_u + new_u
                    total_cost = old_cost + (new_u * bp_s)
                    buy_prices[t] = total_cost / units[t] if units[t] > 0 else bp_s
                    if t not in buy_dates:
                        buy_dates[t] = day
            # Buy benchmark units
            for bm_name in self.bench_df.columns if not self.bench_df.empty else []:
                bp_bm = self._get_bench_price(bm_name, day)
                if bp_bm and bp_bm > 0:
                    bench_units[bm_name] = bench_units.get(bm_name, 0) + sip_amount / bp_bm
            return sip_amount

        def _get_bench_price_helper(bm_name, day):
            """Get benchmark price on a date."""
            try:
                ts = pd.Timestamp(day)
                idx = self.bench_df.index.get_indexer([ts], method="ffill")[0]
                if idx < 0:
                    return None
                val = self.bench_df.iloc[idx].get(bm_name)
                return float(val) if pd.notna(val) else None
            except (KeyError, IndexError):
                return None

        self._get_bench_price = _get_bench_price_helper

        # Events, curves, metrics tracking
        events = []
        equity_curve = []
        benchmark_curves = {name: [] for name in (self.bench_df.columns if not self.bench_df.empty else [])}
        period_returns_list = []
        total_trades = 0

        # Benchmark unit tracking
        bench_units = {}
        for bm_name in benchmark_curves:
            bp = _get_bench_price_helper(bm_name, actual_start)
            if investment_plan == "sip":
                bench_units[bm_name] = 0
            else:
                bench_units[bm_name] = initial_capital / bp if bp and bp > 0 else 0

        # Initial event
        w_init = 1.0 / len(portfolio) if portfolio else 0
        events.append({
            "date": actual_start,
            "type": "INITIAL_SELECTION",
            "portfolio": list(portfolio),
            "weights": {t: round(w_init, 4) for t in portfolio},
            "holdings_detail": _build_holdings_detail(actual_start),
            "capital": round(capital, 2),
        })

        start_idx = self.trading_days.index(actual_start)
        end_idx = self._date_index(end_date)
        if end_idx is None:
            end_idx = len(self.trading_days) - 1

        prev_portfolio_value = capital
        last_measure_value = capital

        # ── Main Loop ─────────────────────────────────────────────────────
        for i in range(start_idx, end_idx + 1):
            day = self.trading_days[i]
            portfolio_value = _portfolio_value(day)

            if i > start_idx:
                capital = portfolio_value

            is_measurement = day in measurement_dates
            is_rebal = day in rebal_dates and day != actual_start
            is_sip_only = day in sip_only_dates and day != actual_start

            # Record measurement point
            if is_measurement:
                period_return = (capital / last_measure_value - 1) if last_measure_value > 0 else 0
                period_returns_list.append(period_return)

                equity_curve.append({"date": day, "value": round(capital, 2)})
                for bm_name in benchmark_curves:
                    bp_now = _get_bench_price_helper(bm_name, day) or 1
                    bm_val = bench_units.get(bm_name, 0) * bp_now
                    benchmark_curves[bm_name].append({"date": day, "value": round(bm_val, 2)})

                last_measure_value = capital

            # Rebalance
            if is_rebal:
                result = self.rebalance(portfolio, day, config)

                if result["exits"] or result["entries"]:
                    total_trades += len(result["exits"]) + len(result["entries"])

                    exit_details = {}
                    for t in result["exits"]:
                        ep = self._get_price(t, day) or 0
                        eu = units.get(t, 0)
                        exit_details[t] = {
                            "units": round(eu, 4),
                            "exit_price": round(ep, 2),
                            "exit_value": round(eu * ep, 2),
                        }

                    exit_value = sum(d["exit_value"] for d in exit_details.values())
                    cost = exit_value * txn_cost_pct / 100.0
                    exit_pool = exit_value - cost

                    for t in result["exits"]:
                        units.pop(t, None)
                        buy_prices.pop(t, None)
                        buy_dates.pop(t, None)

                    n_entries = len(result["entries"])
                    if n_entries > 0 and exit_pool > 0:
                        alloc_each = exit_pool / n_entries
                        for t in result["entries"]:
                            bp_r = self._get_price(t, day)
                            units[t] = alloc_each / bp_r if bp_r and bp_r > 0 else 0
                            buy_prices[t] = bp_r or 0
                            buy_dates[t] = day

                    portfolio = result["new_portfolio"]
                    sip_invested = _inject_sip(day)
                    capital = _portfolio_value(day)

                    events.append({
                        "date": day,
                        "type": "REBALANCE",
                        "exits": result["exits"],
                        "entries": result["entries"],
                        "exit_details": exit_details,
                        "portfolio": result["new_portfolio"],
                        "holdings_detail": _build_holdings_detail(day),
                        "rankings": result["rankings"],
                        "capital": round(capital, 2),
                        "txn_cost": round(cost, 2),
                        "exit_value": round(exit_value, 2),
                        "sip_invested": round(sip_invested, 2),
                    })
                else:
                    sip_invested = _inject_sip(day)
                    capital = _portfolio_value(day)
                    events.append({
                        "date": day,
                        "type": "REBALANCE_NO_CHANGE",
                        "portfolio": list(portfolio),
                        "holdings_detail": _build_holdings_detail(day),
                        "capital": round(capital, 2),
                        "sip_invested": round(sip_invested, 2),
                    })

            # SIP-only injection (Both plan, initial month)
            if is_sip_only and sip_amount > 0 and investment_plan == "both":
                sip_invested = _inject_sip(day)
                capital = _portfolio_value(day)
                events.append({
                    "date": day,
                    "type": "SIP_INVESTMENT",
                    "portfolio": list(portfolio),
                    "holdings_detail": _build_holdings_detail(day),
                    "capital": round(capital, 2),
                    "sip_invested": round(sip_invested, 2),
                })

            prev_portfolio_value = capital

        # ── Performance Metrics ───────────────────────────────────────────
        total_days = end_idx - start_idx
        total_years = total_days / 252.0 if total_days > 0 else 1

        effective_invested = total_invested if total_invested > 0 else initial_capital
        total_return = (capital / effective_invested) - 1 if effective_invested > 0 else 0
        cagr = (capital / effective_invested) ** (1 / total_years) - 1 if (total_years > 0 and effective_invested > 0) else 0

        n_periods = len(period_returns_list)
        if n_periods > 1:
            mean_r = sum(period_returns_list) / n_periods
            var_r = sum((r - mean_r) ** 2 for r in period_returns_list) / n_periods
            std_r = math.sqrt(var_r) if var_r > 0 else 1e-10
            periods_per_year = n_periods / total_years if total_years > 0 else 12
            sharpe = (mean_r / std_r) * math.sqrt(periods_per_year)
        else:
            sharpe, std_r = 0, 0

        # Max Drawdown
        first_val = equity_curve[0]["value"] if equity_curve else effective_invested
        peak = first_val if first_val > 0 else 1
        max_dd = 0
        max_dd_date = None
        for pt in equity_curve:
            if pt["value"] > peak:
                peak = pt["value"]
            dd = (peak - pt["value"]) / peak
            if dd > max_dd:
                max_dd = dd
                max_dd_date = pt["date"]

        # Win Rate
        positive_periods = sum(1 for r in period_returns_list if r > 0)
        win_rate = positive_periods / n_periods if n_periods > 0 else 0

        # Annualized Volatility
        periods_per_year = n_periods / total_years if total_years > 0 else 12
        ann_vol = std_r * math.sqrt(periods_per_year) if n_periods > 1 else 0

        # Beta vs primary benchmark
        bench_period_returns = []
        bench_prev_value = None
        for pt in equity_curve:
            bp = _get_bench_price_helper(self._primary_bm, pt["date"])
            if bp is not None and bp > 0:
                if bench_prev_value is not None and bench_prev_value > 0:
                    bench_period_returns.append((bp / bench_prev_value) - 1)
                bench_prev_value = bp

        # Align lengths — trim portfolio returns to match benchmark returns from the end
        pr_aligned = period_returns_list[-len(bench_period_returns):] if bench_period_returns else []
        if len(bench_period_returns) == len(pr_aligned) and len(pr_aligned) > 1:
            mean_b = sum(bench_period_returns) / len(bench_period_returns)
            mean_p = sum(pr_aligned) / len(pr_aligned)
            cov = sum((pr_aligned[i] - mean_p) * (bench_period_returns[i] - mean_b)
                       for i in range(len(bench_period_returns))) / len(bench_period_returns)
            var_b = sum((b - mean_b) ** 2 for b in bench_period_returns) / len(bench_period_returns)
            beta = cov / var_b if var_b > 0 else 0
        else:
            beta = 0

        # Sector concentration of final portfolio
        sector_weights = {}
        total_val = sum(units.get(t, 0) * (self._get_price(t, self.trading_days[end_idx]) or 0) for t in portfolio)
        for t in portfolio:
            cp = self._get_price(t, self.trading_days[end_idx]) or 0
            val = units.get(t, 0) * cp
            sector = self.sector_map.get(t, "Unknown")
            sector_weights[sector] = sector_weights.get(sector, 0) + (val / total_val if total_val > 0 else 0)

        metrics = {
            "total_return": round(total_return * 100, 2),
            "cagr": round(cagr * 100, 2),
            "sharpe": round(sharpe, 2),
            "max_drawdown": round(max_dd * 100, 2),
            "max_drawdown_date": max_dd_date,
            "win_rate": round(win_rate * 100, 2),
            "annualised_volatility": round(ann_vol * 100, 2),
            "beta": round(beta, 2),
            "total_days": total_days,
            "initial_capital": initial_capital,
            "final_capital": round(capital, 2),
            "total_trades": total_trades,
            "total_invested": round(total_invested, 2),
            "investment_plan": investment_plan,
            "sip_amount": sip_amount,
            "sector_concentration": {s: round(w * 100, 1) for s, w in sorted(sector_weights.items(), key=lambda x: -x[1])},
        }

        # Universe snapshot at end date
        final_date = self.trading_days[end_idx]
        final_ranked = self.rank_universe(final_date, config)
        universe_snapshot = []
        for c in final_ranked:
            c["in_portfolio"] = c["ticker"] in portfolio
            c["score"] = round(c["score"] * 100, 2)
            c["annualized_return"] = round(c.get("annualized_return", 0) * 100, 2)
            c["r_squared"] = round(c.get("r_squared", 0), 3)
            c["sharpe"] = round(c.get("sharpe", 0), 2)
            c["volatility"] = round(c.get("volatility", 0) * 100, 1)
            c["rsi"] = round(c.get("rsi", 50), 1)
            c["close"] = round(c.get("close", 0), 2)
            universe_snapshot.append(c)

        # Final holdings detail
        final_holdings_detail = _build_holdings_detail(final_date)
        final_weights = {}
        for t in portfolio:
            cp = self._get_price(t, final_date) or 0
            cv = units.get(t, 0) * cp
            final_weights[t] = round(cv / total_val, 4) if total_val > 0 else 0

        # Monthly summary
        monthly_summary = self._build_monthly_summary(
            equity_curve, benchmark_curves, events, effective_invested
        )

        # Survivorship bias warning
        survivorship_warning = (
            f"⚠ Survivorship Bias: This backtest uses today's {self.universe_name.upper()} "
            f"constituents applied to historical dates. Stocks that were delisted, merged, "
            f"or dropped from the index are excluded, which may overstate returns."
        )

        return {
            "config": {
                "timeframes": timeframes,
                "weights": config.get("weights", [1, 1, 1]),
                "ema_filter": config.get("ema_filter", False),
                "ranking_method": config.get("ranking_method", "log_regression"),
                "regression_window": config.get("regression_window", 252),
                "portfolio_size": portfolio_size,
                "sector_diversification": config.get("sector_diversification", True),
                "max_sector_pct": config.get("max_sector_pct", 0.25),
                "start_date": actual_start,
                "end_date": end_date,
                "frequency": frequency,
                "rebal_day": rebal_day,
                "txn_cost_pct": txn_cost_pct,
                "exit_rank": config.get("exit_rank", 0),
                "investment_plan": investment_plan,
                "sip_amount": sip_amount,
                "universe": self.universe_name,
            },
            "metrics": metrics,
            "equity_curve": equity_curve,
            "benchmark_curves": benchmark_curves,
            "benchmark_names": list(benchmark_curves.keys()),
            "events": events,
            "final_portfolio": portfolio,
            "final_weights": final_weights,
            "final_holdings_detail": final_holdings_detail,
            "final_capital": round(capital, 2),
            "universe_snapshot": universe_snapshot,
            "monthly_summary": monthly_summary,
            "survivorship_warning": survivorship_warning,
        }

    def _build_monthly_summary(self, equity_curve, benchmark_curves, events,
                                initial_capital):
        """Month-by-month validation table."""
        eq_by_month = {}
        for pt in equity_curve:
            ym = pt["date"][:7]
            eq_by_month.setdefault(ym, []).append(pt)

        bm_by_month = {}
        for bm_name, bm_curve in benchmark_curves.items():
            bm_by_month[bm_name] = {}
            for pt in bm_curve:
                ym = pt["date"][:7]
                bm_by_month[bm_name].setdefault(ym, []).append(pt)

        ev_by_month = {}
        for ev in events:
            ym = ev["date"][:7]
            ev_by_month.setdefault(ym, []).append(ev)

        portfolio_state = []
        for ev in events:
            if ev.get("portfolio"):
                portfolio_state.append((ev["date"], ev["portfolio"]))

        primary = self._primary_bm
        months = sorted(eq_by_month.keys())
        summary = []
        peak_value = initial_capital

        for ym in months:
            eq_pts = eq_by_month[ym]
            month_open = eq_pts[0]["value"]
            month_close = eq_pts[-1]["value"]
            month_high = max(p["value"] for p in eq_pts)
            month_low = min(p["value"] for p in eq_pts)
            month_return_pct = ((month_close - month_open) / month_open * 100) if month_open else 0
            cumulative_return = ((month_close - initial_capital) / initial_capital * 100) if initial_capital > 0 else 0

            bm_returns = {}
            for bm_name in benchmark_curves:
                pts = bm_by_month.get(bm_name, {}).get(ym, [])
                if pts:
                    bm_o = pts[0]["value"]
                    bm_c = pts[-1]["value"]
                    bm_returns[bm_name] = round(((bm_c - bm_o) / bm_o * 100) if bm_o else 0, 2)
                else:
                    bm_returns[bm_name] = 0.0

            primary_bm_ret = bm_returns.get(primary, 0.0)

            if month_close > peak_value:
                peak_value = month_close
            dd_from_peak = ((peak_value - month_close) / peak_value * 100) if peak_value else 0

            active_pf = []
            for ev_date, pf_list in portfolio_state:
                if ev_date[:7] <= ym:
                    active_pf = pf_list

            month_events = ev_by_month.get(ym, [])
            n_rebalances = sum(1 for e in month_events if e["type"] == "REBALANCE")
            exits, entries = [], []
            txn_costs = 0.0
            for e in month_events:
                if e["type"] == "REBALANCE":
                    exits.extend(e.get("exits", []))
                    entries.extend(e.get("entries", []))
                    txn_costs += e.get("txn_cost", 0)

            summary.append({
                "month": ym,
                "open": round(month_open, 2),
                "close": round(month_close, 2),
                "high": round(month_high, 2),
                "low": round(month_low, 2),
                "return_pct": round(month_return_pct, 2),
                "cumulative_return_pct": round(cumulative_return, 2),
                "benchmark_returns": bm_returns,
                "alpha_pct": round(month_return_pct - primary_bm_ret, 2),
                "drawdown_from_peak_pct": round(dd_from_peak, 2),
                "portfolio": active_pf,
                "rebalances": n_rebalances,
                "exits": exits,
                "entries": entries,
                "txn_costs": round(txn_costs, 2),
            })

        return summary
