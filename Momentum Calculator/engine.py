"""
Momentum Engine: ETF Selection, Rebalancing, and Backtest Simulation.
"""

import math
from datetime import datetime, timedelta
from etf_data import ETF_UNIVERSE, generate_synthetic_prices, generate_benchmark_prices, generate_trading_days


class MomentumEngine:
    """Core engine for momentum-based ETF selection, rebalancing, and backtesting."""

    def __init__(self):
        self.prices = generate_synthetic_prices()
        self.benchmark_prices = generate_benchmark_prices()
        self.trading_days = generate_trading_days("2022-01-01", "2026-04-18")
        self.etf_meta = {e["scrip"]: e for e in ETF_UNIVERSE}

    # ── Indicator Helpers ─────────────────────────────────────────────────

    def _get_price(self, ticker: str, date: str) -> float | None:
        return self.prices.get(ticker, {}).get(date)

    def _get_price_series(self, ticker: str, end_date: str, lookback: int) -> list:
        """Return up to `lookback` prices ending at or before `end_date`."""
        idx = self._date_index(end_date)
        if idx is None:
            return []
        start = max(0, idx - lookback + 1)
        series = []
        for i in range(start, idx + 1):
            d = self.trading_days[i]
            p = self.prices.get(ticker, {}).get(d)
            if p is not None:
                series.append(p)
        return series

    def _date_index(self, date: str) -> int | None:
        """Find index of date (or nearest prior trading day) in trading_days."""
        if date in self.trading_days:
            return self.trading_days.index(date)
        # find nearest prior
        for i in range(len(self.trading_days) - 1, -1, -1):
            if self.trading_days[i] <= date:
                return i
        return None

    def _next_trading_day(self, date: str) -> str | None:
        idx = self._date_index(date)
        if idx is None:
            return self.trading_days[0] if self.trading_days else None
        if self.trading_days[idx] == date:
            return date
        if idx + 1 < len(self.trading_days):
            return self.trading_days[idx + 1]
        return None

    def has_history(self, ticker: str, date: str, lookback: int) -> bool:
        series = self._get_price_series(ticker, date, lookback)
        return len(series) >= lookback

    def return_over(self, ticker: str, date: str, window: int) -> float | None:
        series = self._get_price_series(ticker, date, window + 1)
        if len(series) < window + 1:
            return None
        return (series[-1] / series[0]) - 1.0

    def ema200(self, ticker: str, date: str) -> float | None:
        series = self._get_price_series(ticker, date, 200)
        if len(series) < 200:
            return None
        k = 2 / (200 + 1)
        ema = series[0]
        for p in series[1:]:
            ema = p * k + ema * (1 - k)
        return ema

    def sharpe_return(self, ticker: str, date: str, window: int = 252) -> float | None:
        series = self._get_price_series(ticker, date, window + 1)
        if len(series) < window + 1:
            return None
        daily_returns = [(series[i] / series[i - 1]) - 1 for i in range(1, len(series))]
        if not daily_returns:
            return None
        mean_r = sum(daily_returns) / len(daily_returns)
        var_r = sum((r - mean_r) ** 2 for r in daily_returns) / len(daily_returns)
        std_r = math.sqrt(var_r) if var_r > 0 else 1e-10
        return (mean_r / std_r) * math.sqrt(252)

    def rsi(self, ticker: str, date: str, period: int = 14) -> float | None:
        series = self._get_price_series(ticker, date, period + 1)
        if len(series) < period + 1:
            return None
        gains, losses = [], []
        for i in range(1, len(series)):
            change = series[i] - series[i - 1]
            gains.append(max(0, change))
            losses.append(max(0, -change))
        avg_gain = sum(gains) / period
        avg_loss = sum(losses) / period
        if avg_loss == 0:
            return 100.0
        rs = avg_gain / avg_loss
        return 100.0 - (100.0 / (1.0 + rs))

    def volatility(self, ticker: str, date: str, window: int = 252) -> float | None:
        series = self._get_price_series(ticker, date, window + 1)
        if len(series) < window + 1:
            return None
        daily_returns = [(series[i] / series[i - 1]) - 1 for i in range(1, len(series))]
        mean_r = sum(daily_returns) / len(daily_returns)
        var_r = sum((r - mean_r) ** 2 for r in daily_returns) / len(daily_returns)
        return math.sqrt(var_r) * math.sqrt(252)

    # ── Selection Algorithm ───────────────────────────────────────────────

    def rank_universe(self, date: str, timeframes: list, weights: list,
                      ema_filter: bool) -> list:
        """
        Rank ETFs by weighted momentum score.
        Returns list of (ticker, score, sharpe, market_cap) sorted descending.
        """
        candidates = []
        for etf in ETF_UNIVERSE:
            ticker = etf["scrip"]
            # Require sufficient history for EVERY timeframe, not just the max
            has_all = True
            for tf in timeframes:
                if not self.has_history(ticker, date, tf + 1):
                    has_all = False
                    break
            if not has_all:
                continue
            if ema_filter:
                close = self._get_price(ticker, date)
                ema = self.ema200(ticker, date)
                if close is None or ema is None or close <= ema:
                    continue
            returns = []
            skip = False
            for tf in timeframes:
                r = self.return_over(ticker, date, tf)
                if r is None:
                    skip = True
                    break
                returns.append(r)
            if skip:
                continue
            score = sum(r * w for r, w in zip(returns, weights))
            sharpe = self.sharpe_return(ticker, date) or 0.0
            mcap = etf.get("market_cap", 0)
            candidates.append((ticker, score, sharpe, mcap))

        # Sort: score desc, tie-break sharpe desc, then market_cap desc
        candidates.sort(key=lambda x: (-x[1], -x[2], -x[3]))
        return candidates

    def select_portfolio(self, date: str, timeframes: list, weights: list,
                         ema_filter: bool, portfolio_size: int) -> list:
        ranked = self.rank_universe(date, timeframes, weights, ema_filter)
        selected = ranked[:portfolio_size]
        return [ticker for ticker, *_ in selected]

    # ── Rebalancing ───────────────────────────────────────────────────────

    def rebalance(self, portfolio: list, date: str, timeframes: list,
                  weights: list, ema_filter: bool) -> dict:
        """
        Rebalance portfolio on given date.
        Returns dict with new_portfolio, exits, entries, weights, rankings.
        """
        ranked = self.rank_universe(date, timeframes, weights, ema_filter)
        rank_map = {ticker: i + 1 for i, (ticker, *_) in enumerate(ranked)}
        exit_rank = 2 * len(portfolio)

        exits = [etf for etf in portfolio if rank_map.get(etf, len(ranked) + 1) > exit_rank]
        replacements = []
        for ticker, *_ in ranked:
            if ticker not in portfolio and len(replacements) < len(exits):
                replacements.append(ticker)

        new_portfolio = [etf for etf in portfolio if etf not in exits] + replacements
        w = 1.0 / len(new_portfolio) if new_portfolio else 0
        wts = {etf: w for etf in new_portfolio}

        return {
            "new_portfolio": new_portfolio,
            "exits": exits,
            "entries": replacements,
            "weights": wts,
            "rankings": {t: rank_map.get(t, None) for t in new_portfolio},
        }

    # ── Rebalancing Day Helpers ───────────────────────────────────────────

    def _get_rebalancing_dates(self, start: str, end: str, frequency: str,
                               rebal_day: int) -> list:
        """
        Generate rebalancing dates between start and end.
        frequency: 'weekly' or 'monthly'
        rebal_day: for weekly 0=Mon..4=Fri; for monthly 1..28
        """
        dates = []
        s = datetime.strptime(start, "%Y-%m-%d")
        e = datetime.strptime(end, "%Y-%m-%d")
        current = s

        if frequency == "weekly":
            # Advance to first rebal_day
            while current.weekday() != rebal_day:
                current += timedelta(days=1)
            while current <= e:
                td = self._next_trading_day(current.strftime("%Y-%m-%d"))
                if td and td >= start and td <= end:
                    dates.append(td)
                current += timedelta(weeks=1)
        else:  # monthly
            while current <= e:
                # set to rebal_day of current month
                try:
                    target = current.replace(day=min(rebal_day, 28))
                except ValueError:
                    target = current.replace(day=28)
                if target < s:
                    # move to next month
                    if current.month == 12:
                        current = current.replace(year=current.year + 1, month=1, day=1)
                    else:
                        current = current.replace(month=current.month + 1, day=1)
                    continue
                td = self._next_trading_day(target.strftime("%Y-%m-%d"))
                if td and td <= end:
                    dates.append(td)
                # next month
                if current.month == 12:
                    current = current.replace(year=current.year + 1, month=1, day=1)
                else:
                    current = current.replace(month=current.month + 1, day=1)

        return sorted(set(dates))

    # ── Backtest ──────────────────────────────────────────────────────────

    def run_backtest(self, config: dict) -> dict:
        """
        Run a full backtest.
        config keys:
            timeframes: list[int], weights: list[float], ema_filter: bool,
            portfolio_size: int, start_date: str, end_date: str,
            frequency: str ('weekly'|'monthly'), rebal_day: int,
            initial_capital: float, transaction_cost_pct: float
        """
        timeframes = config.get("timeframes", [252, 50, 20])
        raw_weights = config.get("weights", [1, 1, 1])
        wsum = sum(raw_weights)
        weights = [w / wsum for w in raw_weights]
        ema_filter = config.get("ema_filter", False)
        portfolio_size = config.get("portfolio_size", 5)
        start_date = config.get("start_date", "2023-01-01")
        end_date = config.get("end_date", "2026-04-18")
        frequency = config.get("frequency", "monthly")
        rebal_day = config.get("rebal_day", 1)
        initial_capital = config.get("initial_capital", 1000000)
        txn_cost_pct = config.get("transaction_cost_pct", 0.0)

        # Need enough history before start
        max_tf = max(timeframes)
        actual_start_idx = self._date_index(start_date)
        if actual_start_idx is None or actual_start_idx < max_tf + 1:
            # push start forward
            actual_start_idx = max_tf + 1
        if actual_start_idx >= len(self.trading_days):
            return {"error": "Not enough data for backtest"}
        actual_start = self.trading_days[actual_start_idx]
        if actual_start > end_date:
            return {"error": "Start date requires more history than available"}

        # Initial selection
        portfolio = self.select_portfolio(actual_start, timeframes, weights,
                                          ema_filter, portfolio_size)
        if not portfolio:
            return {"error": "No ETFs pass filters on start date"}

        w = 1.0 / len(portfolio)
        holdings = {etf: w for etf in portfolio}

        # Rebalancing dates
        rebal_dates = set(self._get_rebalancing_dates(actual_start, end_date,
                                                       frequency, rebal_day))

        # Track performance
        capital = initial_capital
        events = []
        equity_curve = []
        benchmark_curve = []
        daily_returns_list = []

        events.append({
            "date": actual_start,
            "type": "INITIAL_SELECTION",
            "portfolio": list(portfolio),
            "weights": {etf: round(w, 4) for etf in portfolio},
            "capital": round(capital, 2),
        })

        # Get benchmark start price
        bench_start_price = self.benchmark_prices.get(actual_start, 17000)
        start_idx = self.trading_days.index(actual_start)
        end_idx = self._date_index(end_date)
        if end_idx is None:
            end_idx = len(self.trading_days) - 1

        prev_portfolio_value = capital

        for i in range(start_idx, end_idx + 1):
            day = self.trading_days[i]

            # Calculate portfolio value
            port_return = 0.0
            if i > start_idx:
                prev_day = self.trading_days[i - 1]
                for etf, weight in holdings.items():
                    p_today = self._get_price(etf, day)
                    p_prev = self._get_price(etf, prev_day)
                    if p_today and p_prev and p_prev > 0:
                        port_return += weight * ((p_today / p_prev) - 1)
                capital *= (1 + port_return)

            daily_returns_list.append(port_return)

            bench_price = self.benchmark_prices.get(day, bench_start_price)
            equity_curve.append({"date": day, "value": round(capital, 2)})
            benchmark_curve.append({
                "date": day,
                "value": round(initial_capital * bench_price / bench_start_price, 2)
            })

            # Rebalance check
            if day in rebal_dates and day != actual_start:
                result = self.rebalance(portfolio, day, timeframes, weights, ema_filter)
                if result["exits"] or result["entries"]:
                    # Apply transaction cost
                    turnover = len(result["exits"]) * (1.0 / len(portfolio)) * 2
                    cost = capital * turnover * txn_cost_pct / 100.0
                    capital -= cost

                    portfolio = result["new_portfolio"]
                    holdings = result["weights"]
                    events.append({
                        "date": day,
                        "type": "REBALANCE",
                        "exits": result["exits"],
                        "entries": result["entries"],
                        "portfolio": result["new_portfolio"],
                        "weights": {k: round(v, 4) for k, v in result["weights"].items()},
                        "rankings": result["rankings"],
                        "capital": round(capital, 2),
                        "txn_cost": round(cost, 2),
                    })
                else:
                    events.append({
                        "date": day,
                        "type": "REBALANCE_NO_CHANGE",
                        "portfolio": list(portfolio),
                        "capital": round(capital, 2),
                    })

            prev_portfolio_value = capital

        # ── Compute Performance Metrics ───────────────────────────────────
        total_days = end_idx - start_idx
        total_years = total_days / 252.0 if total_days > 0 else 1

        total_return = (capital / initial_capital) - 1
        cagr = (capital / initial_capital) ** (1 / total_years) - 1 if total_years > 0 else 0

        # Sharpe
        if daily_returns_list:
            mean_r = sum(daily_returns_list) / len(daily_returns_list)
            var_r = sum((r - mean_r) ** 2 for r in daily_returns_list) / len(daily_returns_list)
            std_r = math.sqrt(var_r) if var_r > 0 else 1e-10
            sharpe = (mean_r / std_r) * math.sqrt(252)
        else:
            sharpe = 0

        # Max Drawdown
        peak = initial_capital
        max_dd = 0
        for pt in equity_curve:
            if pt["value"] > peak:
                peak = pt["value"]
            dd = (peak - pt["value"]) / peak
            if dd > max_dd:
                max_dd = dd

        # Win Rate (positive daily returns)
        positive_days = sum(1 for r in daily_returns_list if r > 0)
        win_rate = positive_days / len(daily_returns_list) if daily_returns_list else 0

        # Annualised Volatility
        ann_vol = std_r * math.sqrt(252) if daily_returns_list else 0

        # Beta vs benchmark
        bench_returns = []
        for i in range(start_idx, end_idx + 1):
            if i == start_idx:
                bench_returns.append(0)
                continue
            day = self.trading_days[i]
            prev_day = self.trading_days[i - 1]
            bp = self.benchmark_prices.get(day, 0)
            bp_prev = self.benchmark_prices.get(prev_day, 0)
            if bp_prev > 0:
                bench_returns.append((bp / bp_prev) - 1)
            else:
                bench_returns.append(0)

        if len(bench_returns) > 1 and len(daily_returns_list) == len(bench_returns):
            mean_b = sum(bench_returns) / len(bench_returns)
            mean_p = sum(daily_returns_list) / len(daily_returns_list)
            cov = sum((daily_returns_list[i] - mean_p) * (bench_returns[i] - mean_b)
                       for i in range(len(bench_returns))) / len(bench_returns)
            var_b = sum((b - mean_b) ** 2 for b in bench_returns) / len(bench_returns)
            beta = cov / var_b if var_b > 0 else 0
        else:
            beta = 0

        metrics = {
            "total_return": round(total_return * 100, 2),
            "cagr": round(cagr * 100, 2),
            "sharpe": round(sharpe, 2),
            "max_drawdown": round(max_dd * 100, 2),
            "win_rate": round(win_rate * 100, 2),
            "annualised_volatility": round(ann_vol * 100, 2),
            "beta": round(beta, 2),
            "total_days": total_days,
            "initial_capital": initial_capital,
            "final_capital": round(capital, 2),
        }

        # Current indicators for universe
        final_date = self.trading_days[end_idx]
        universe_snapshot = []
        ranked = self.rank_universe(final_date, timeframes, weights, ema_filter)
        for rank_idx, (ticker, score, sh, mc) in enumerate(ranked):
            ret_pcts = {}
            for tf in timeframes:
                r = self.return_over(ticker, final_date, tf)
                ret_pcts[f"ret_{tf}d"] = round(r * 100, 2) if r is not None else None
            universe_snapshot.append({
                "rank": rank_idx + 1,
                "scrip": ticker,
                "sector": self.etf_meta[ticker]["sector"],
                "score": round(score * 100, 2),
                "sharpe": round(sh, 2),
                "rsi": round(self.rsi(ticker, final_date) or 0, 1),
                "volatility": round((self.volatility(ticker, final_date) or 0) * 100, 1),
                **ret_pcts,
                "in_portfolio": ticker in portfolio,
            })

        # ── Monthly Validation Summary ────────────────────────────────────
        monthly_summary = self._build_monthly_summary(
            equity_curve, benchmark_curve, events, initial_capital
        )

        return {
            "config": {
                "timeframes": timeframes,
                "weights": [round(w, 4) for w in weights],
                "ema_filter": ema_filter,
                "portfolio_size": portfolio_size,
                "start_date": actual_start,
                "end_date": end_date,
                "frequency": frequency,
                "rebal_day": rebal_day,
                "txn_cost_pct": txn_cost_pct,
            },
            "metrics": metrics,
            "equity_curve": equity_curve,
            "benchmark_curve": benchmark_curve,
            "events": events,
            "final_portfolio": portfolio,
            "final_weights": {etf: round(holdings.get(etf, 0), 4) for etf in portfolio},
            "universe_snapshot": universe_snapshot,
            "monthly_summary": monthly_summary,
        }

    def _build_monthly_summary(self, equity_curve, benchmark_curve, events,
                                initial_capital):
        """Build a month-by-month validation table from backtest data."""
        # Group equity curve by month
        eq_by_month = {}
        for pt in equity_curve:
            ym = pt["date"][:7]
            eq_by_month.setdefault(ym, []).append(pt)

        bm_by_month = {}
        for pt in benchmark_curve:
            ym = pt["date"][:7]
            bm_by_month.setdefault(ym, []).append(pt)

        # Group events by month
        ev_by_month = {}
        for ev in events:
            ym = ev["date"][:7]
            ev_by_month.setdefault(ym, []).append(ev)

        # Build the active portfolio state over time
        portfolio_state = []
        for ev in events:
            if ev.get("portfolio"):
                portfolio_state.append((ev["date"], ev["portfolio"]))

        months = sorted(eq_by_month.keys())
        summary = []
        cumulative_return = 0.0
        peak_value = initial_capital

        for ym in months:
            eq_pts = eq_by_month[ym]
            bm_pts = bm_by_month.get(ym, [])

            month_open = eq_pts[0]["value"]
            month_close = eq_pts[-1]["value"]
            month_high = max(p["value"] for p in eq_pts)
            month_low = min(p["value"] for p in eq_pts)
            month_return_pct = ((month_close - month_open) / month_open * 100) if month_open else 0
            cumulative_return = ((month_close - initial_capital) / initial_capital * 100)

            # Benchmark
            bm_open = bm_pts[0]["value"] if bm_pts else 0
            bm_close = bm_pts[-1]["value"] if bm_pts else 0
            bm_return_pct = ((bm_close - bm_open) / bm_open * 100) if bm_open else 0

            # Drawdown from peak
            if month_close > peak_value:
                peak_value = month_close
            dd_from_peak = ((peak_value - month_close) / peak_value * 100) if peak_value else 0

            # Active portfolio for this month
            active_pf = []
            for ev_date, pf_list in portfolio_state:
                if ev_date[:7] <= ym:
                    active_pf = pf_list

            # Events this month
            month_events = ev_by_month.get(ym, [])
            n_rebalances = sum(1 for e in month_events if e["type"] == "REBALANCE")
            exits = []
            entries = []
            txn_costs = 0.0
            for e in month_events:
                if e["type"] == "REBALANCE":
                    exits.extend(e.get("exits", []))
                    entries.extend(e.get("entries", []))
                    txn_costs += e.get("txn_cost", 0)

            trading_days_count = len(eq_pts)

            summary.append({
                "month": ym,
                "open": round(month_open, 2),
                "close": round(month_close, 2),
                "high": round(month_high, 2),
                "low": round(month_low, 2),
                "return_pct": round(month_return_pct, 2),
                "cumulative_return_pct": round(cumulative_return, 2),
                "benchmark_return_pct": round(bm_return_pct, 2),
                "alpha_pct": round(month_return_pct - bm_return_pct, 2),
                "drawdown_from_peak_pct": round(dd_from_peak, 2),
                "peak_value": round(peak_value, 2),
                "trading_days": trading_days_count,
                "portfolio": active_pf,
                "rebalances": n_rebalances,
                "exits": exits,
                "entries": entries,
                "txn_costs": round(txn_costs, 2),
            })

        return summary
