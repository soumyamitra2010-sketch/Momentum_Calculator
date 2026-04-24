"""
Momentum Engine: ETF Selection, Rebalancing, and Backtest Simulation.
"""

import math
from datetime import datetime, timedelta
from etf_data import ETF_UNIVERSE, BENCHMARKS, download_all_data


class MomentumEngine:
    """Core engine for momentum-based ETF selection, rebalancing, and backtesting."""

    def __init__(self):
        self.prices, self.benchmark_prices, self.trading_days = download_all_data()
        self.etf_meta = {e["scrip"]: e for e in ETF_UNIVERSE}
        # Primary benchmark for beta calculation
        self._primary_bm = "Nifty 50"

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
        # Allow up to 5% missing data points (holidays / gaps)
        return len(series) >= lookback * 0.95

    def return_over(self, ticker: str, date: str, window: int) -> float | None:
        series = self._get_price_series(ticker, date, window + 1)
        if len(series) < (window + 1) * 0.95:
            return None
        return (series[-1] / series[0]) - 1.0

    def ema200(self, ticker: str, date: str) -> float | None:
        series = self._get_price_series(ticker, date, 200)
        if len(series) < 200 * 0.95:
            return None
        k = 2 / (200 + 1)
        ema = series[0]
        for p in series[1:]:
            ema = p * k + ema * (1 - k)
        return ema

    def sharpe_return(self, ticker: str, date: str, window: int = 252) -> float | None:
        series = self._get_price_series(ticker, date, window + 1)
        if len(series) < (window + 1) * 0.95:
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
        if len(series) < (period + 1) * 0.95:
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
        if len(series) < (window + 1) * 0.95:
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
                  weights: list, ema_filter: bool, exit_rank: int = 0) -> dict:
        """
        Rebalance portfolio on given date.
        exit_rank: rank threshold above which an ETF is exited. 0 = auto (2x portfolio size).
        Returns dict with new_portfolio, exits, entries, weights, rankings.
        """
        ranked = self.rank_universe(date, timeframes, weights, ema_filter)
        rank_map = {ticker: i + 1 for i, (ticker, *_) in enumerate(ranked)}
        if exit_rank <= 0:
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
        frequency: 'weekly', 'monthly', or 'quarterly'
        rebal_day: for weekly 0=Mon..4=Fri; for monthly/quarterly 1..28
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
        elif frequency == "quarterly":
            # Quarterly: every 3 months (Mar, Jun, Sep, Dec)
            while current <= e:
                # set to rebal_day of current month
                try:
                    target = current.replace(day=min(rebal_day, 28))
                except ValueError:
                    target = current.replace(day=28)
                if target < s:
                    # move to next quarter
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
                # next quarter
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

    def _get_period_end_dates(self, start: str, end: str, frequency: str) -> list:
        """
        Get period-end dates (month-end, week-end, quarter-end, or year-end).
        Used for measurement points when calculating drawdown and metrics.
        """
        dates = []
        s = datetime.strptime(start, "%Y-%m-%d")
        e = datetime.strptime(end, "%Y-%m-%d")
        
        if frequency == "weekly":
            # Get all Fridays (week ends)
            current = s
            while current.weekday() != 4:  # Friday = 4
                current += timedelta(days=1)
            while current <= e:
                td = self._next_trading_day(current.strftime("%Y-%m-%d"))
                if td and td >= start and td <= end:
                    dates.append(td)
                current += timedelta(weeks=1)
        elif frequency == "quarterly":
            # Quarter ends: Mar, Jun, Sep, Dec
            quarter_ends = [3, 6, 9, 12]
            year = s.year
            month = s.month
            
            # Find first quarter end >= start month
            for qe in quarter_ends:
                if qe >= month:
                    try:
                        target = datetime(year, qe, 1)
                        # Get last day of month
                        if qe == 12:
                            next_month = datetime(year + 1, 1, 1)
                        else:
                            next_month = datetime(year, qe + 1, 1)
                        last_day = (next_month - timedelta(days=1)).day
                        target = datetime(year, qe, last_day)
                    except:
                        target = datetime(year, qe, 28)
                    
                    if target >= s:
                        td = self._next_trading_day(target.strftime("%Y-%m-%d"))
                        if td and td >= start and td <= end:
                            dates.append(td)
            
            # Continue to next quarters
            while True:
                # Move to next quarter
                if month <= 3:
                    month = 6
                elif month <= 6:
                    month = 9
                elif month <= 9:
                    month = 12
                else:
                    month = 3
                    year += 1
                
                try:
                    if month == 3:
                        target = datetime(year, 3, 31)
                    elif month == 6:
                        target = datetime(year, 6, 30)
                    elif month == 9:
                        target = datetime(year, 9, 30)
                    else:  # Dec
                        target = datetime(year, 12, 31)
                except:
                    target = datetime(year, month, 28)
                
                if target > e:
                    break
                
                td = self._next_trading_day(target.strftime("%Y-%m-%d"))
                if td and td <= end:
                    dates.append(td)
        else:  # monthly
            current = s.replace(day=1)
            while current <= e:
                # Get last day of current month
                if current.month == 12:
                    next_month = current.replace(year=current.year + 1, month=1, day=1)
                else:
                    next_month = current.replace(month=current.month + 1, day=1)
                last_day = (next_month - timedelta(days=1)).day
                try:
                    target = current.replace(day=last_day)
                except ValueError:
                    target = current.replace(day=28)
                
                if target >= s:
                    td = self._next_trading_day(target.strftime("%Y-%m-%d"))
                    if td and td >= start and td <= end:
                        dates.append(td)
                
                # Move to next month
                current = next_month
        
        return sorted(set(dates))

    # ── Backtest ──────────────────────────────────────────────────────────

    def run_backtest(self, config: dict) -> dict:
        """
        Run a full backtest.
        config keys:
            timeframes: list[int], weights: list[float], ema_filter: bool,
            portfolio_size: int, start_date: str, end_date: str,
            frequency: str ('weekly'|'monthly'|'quarterly'), rebal_day: int,
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
        exit_rank_threshold = config.get("exit_rank", 0)
        investment_plan = config.get("investment_plan", "onetime")
        sip_amount = config.get("sip_amount", 0)
        total_invested = 0  # Track total money invested for SIP metrics

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

        # Rebalancing dates (use day-1 for actual rebalance)
        effective_rebal_day = max(1, rebal_day - 1)
        all_rebal_dates = set(self._get_rebalancing_dates(actual_start, end_date,
                                                       frequency, effective_rebal_day))
        start_month = actual_start[:7]  # "YYYY-MM"

        # For onetime: skip initial month entirely
        # For sip: keep all rebalance dates (SIP invests from first rebalance)
        # For both: keep initial month for SIP-only injection, skip portfolio rebalancing
        if investment_plan == "sip":
            rebal_dates = all_rebal_dates
        else:
            rebal_dates = {d for d in all_rebal_dates if d[:7] != start_month}
        # SIP injection dates: for sip/both, include initial month
        sip_only_dates = set()  # dates where only SIP happens (no portfolio rebalance)
        if investment_plan == "both":
            sip_only_dates = {d for d in all_rebal_dates if d[:7] == start_month}
        
        # Get period-end dates for metric calculation (rebalance dates take priority)
        period_end_dates = set(self._get_period_end_dates(actual_start, end_date, frequency))
        # Measurement points = rebal dates + sip-only dates + period ends
        measurement_dates = sorted(rebal_dates | sip_only_dates | period_end_dates)
        if actual_start not in measurement_dates:
            measurement_dates = [actual_start] + measurement_dates

        # ── Unit-based portfolio tracking ─────────────────────────────────
        if investment_plan == "sip":
            # SIP-only: no initial capital, first investment happens at first rebalance
            capital = 0
            total_invested = 0
        else:
            # One-time or Both: invest initial capital at start
            capital = initial_capital
            total_invested = initial_capital

        units = {}       # {etf: number_of_units}
        buy_prices = {}  # {etf: price_at_purchase (weighted avg)}
        buy_dates = {}   # {etf: date_of_purchase}

        if capital > 0:
            alloc_per_etf = capital / len(portfolio)
            for etf in portfolio:
                bp = self._get_price(etf, actual_start)
                units[etf] = alloc_per_etf / bp if bp and bp > 0 else 0
                buy_prices[etf] = bp or 0
                buy_dates[etf] = actual_start
        else:
            alloc_per_etf = 0

        def _build_holdings_detail(day):
            """Per-ETF detail: units, buy price, current price, invested, current value, P&L%."""
            detail = {}
            for etf_t in units:
                cp = self._get_price(etf_t, day) or 0
                invested = round(units[etf_t] * buy_prices.get(etf_t, 0), 2)
                current_val = round(units[etf_t] * cp, 2)
                pnl = round(((cp / buy_prices[etf_t]) - 1) * 100, 2) if buy_prices.get(etf_t, 0) > 0 else 0
                detail[etf_t] = {
                    "units": round(units[etf_t], 4),
                    "buy_price": round(buy_prices.get(etf_t, 0), 2),
                    "buy_date": buy_dates.get(etf_t, ""),
                    "current_price": round(cp, 2),
                    "invested": invested,
                    "current_value": current_val,
                    "pnl_pct": pnl,
                }
            return detail

        def _compute_weights():
            """Derive current weights from unit values."""
            if not units:
                return {}
            vals = {}
            total = 0
            for etf_t in units:
                # use last known price
                cp = self._get_price(etf_t, self.trading_days[min(i, len(self.trading_days) - 1)]) or 0
                v = units[etf_t] * cp
                vals[etf_t] = v
                total += v
            return {etf_t: round(v / total, 4) if total > 0 else 0 for etf_t, v in vals.items()}

        events = []
        equity_curve = []  # Now tracks only at measurement points
        benchmark_curves = {name: [] for name in self.benchmark_prices}
        period_returns_list = []  # Returns between measurement points (not daily)
        total_trades = 0  # Count of entry + exit transactions

        w_init = 1.0 / len(portfolio)
        events.append({
            "date": actual_start,
            "type": "INITIAL_SELECTION",
            "portfolio": list(portfolio),
            "weights": {etf: round(w_init, 4) for etf in portfolio},
            "holdings_detail": _build_holdings_detail(actual_start),
            "capital": round(capital, 2),
        })

        start_idx = self.trading_days.index(actual_start)
        end_idx = self._date_index(end_date)
        if end_idx is None:
            end_idx = len(self.trading_days) - 1

        # Get benchmark start prices for each benchmark
        bench_start_prices = {}
        for bm_name, bm_data in self.benchmark_prices.items():
            bp = None
            # Look backward from start for a price
            for j in range(start_idx, -1, -1):
                bp = bm_data.get(self.trading_days[j])
                if bp is not None:
                    break
            if bp is None:
                # Benchmark has no data before start — use its first available price
                for j in range(start_idx + 1, end_idx + 1):
                    bp = bm_data.get(self.trading_days[j])
                    if bp is not None:
                        break
            bench_start_prices[bm_name] = bp if bp is not None else 1.0

        prev_portfolio_value = capital
        last_measure_value = capital
        last_measure_date = actual_start

        # Benchmark unit tracking for SIP simulation
        # For onetime: lump sum into benchmark at start
        # For sip/both: buy benchmark units at each SIP injection point
        bench_units = {}  # {bm_name: total_units}
        for bm_name in self.benchmark_prices:
            bp = bench_start_prices[bm_name]
            if investment_plan == "sip":
                bench_units[bm_name] = 0  # no initial investment
            else:
                bench_units[bm_name] = initial_capital / bp if bp > 0 else 0

        for i in range(start_idx, end_idx + 1):
            day = self.trading_days[i]

            # Calculate portfolio value from actual units held
            portfolio_value = sum(
                units.get(etf, 0) * (self._get_price(etf, day) or 0)
                for etf in units
            )
            if i > start_idx:
                port_return = (portfolio_value / prev_portfolio_value - 1) if prev_portfolio_value > 0 else 0
                capital = portfolio_value
            else:
                port_return = 0

            # Record at measurement points (rebalance dates + period ends)
            is_measurement = day in measurement_dates
            is_rebal = day in rebal_dates and day != actual_start
            is_sip_only = day in sip_only_dates and day != actual_start
            
            if is_measurement:
                # Calculate return since last measurement point
                period_return = (capital / last_measure_value - 1) if last_measure_value > 0 else 0
                period_returns_list.append(period_return)
                
                equity_curve.append({"date": day, "value": round(capital, 2)})
                for bm_name, bm_data in self.benchmark_prices.items():
                    bp_now = bm_data.get(day, bench_start_prices[bm_name])
                    bm_value = bench_units[bm_name] * bp_now
                    benchmark_curves[bm_name].append({
                        "date": day,
                        "value": round(bm_value, 2)
                    })
                
                last_measure_value = capital
                last_measure_date = day

            # Rebalance check
            if is_rebal:
                result = self.rebalance(portfolio, day, timeframes, weights, ema_filter, exit_rank_threshold)
                if result["exits"] or result["entries"]:
                    # Count trades: each exit and each entry is one trade
                    total_trades += len(result["exits"]) + len(result["entries"])

                    # Calculate exit value for each ETF individually
                    exit_details = {}
                    for etf in result["exits"]:
                        etf_exit_price = self._get_price(etf, day) or 0
                        etf_units = units.get(etf, 0)
                        etf_exit_value = etf_units * etf_exit_price
                        exit_details[etf] = {
                            "units": round(etf_units, 4),
                            "exit_price": round(etf_exit_price, 2),
                            "exit_value": round(etf_exit_value, 2),
                        }

                    # Total exit value from all exited ETFs
                    exit_value = sum(d["exit_value"] for d in exit_details.values())

                    # Apply transaction cost on the swapped amount
                    cost = exit_value * txn_cost_pct / 100.0
                    exit_pool = exit_value - cost

                    # Remove exited ETFs from holdings
                    for etf in result["exits"]:
                        units.pop(etf, None)
                        buy_prices.pop(etf, None)
                        buy_dates.pop(etf, None)

                    # Allocate exit pool equally among new entries
                    n_entries = len(result["entries"])
                    if n_entries > 0 and exit_pool > 0:
                        alloc_each = exit_pool / n_entries
                        for etf in result["entries"]:
                            bp_r = self._get_price(etf, day)
                            units[etf] = alloc_each / bp_r if bp_r and bp_r > 0 else 0
                            buy_prices[etf] = bp_r or 0
                            buy_dates[etf] = day

                    portfolio = result["new_portfolio"]

                    # ── SIP injection on rebalance date ──
                    sip_invested_this_month = 0
                    if sip_amount > 0 and investment_plan in ("sip", "both"):
                        sip_invested_this_month = sip_amount
                        total_invested += sip_amount
                        sip_alloc = sip_amount / len(portfolio) if portfolio else 0
                        for etf in portfolio:
                            bp_s = self._get_price(etf, day)
                            if bp_s and bp_s > 0:
                                new_units = sip_alloc / bp_s
                                old_units = units.get(etf, 0)
                                old_cost = old_units * buy_prices.get(etf, 0)
                                units[etf] = old_units + new_units
                                # Update weighted average buy price
                                total_cost = old_cost + (new_units * bp_s)
                                buy_prices[etf] = total_cost / units[etf] if units[etf] > 0 else bp_s
                                if etf not in buy_dates:
                                    buy_dates[etf] = day
                        # Buy benchmark units with same SIP amount
                        for bm_name, bm_data in self.benchmark_prices.items():
                            bp_bm = bm_data.get(day, bench_start_prices[bm_name])
                            if bp_bm > 0:
                                bench_units[bm_name] += sip_amount / bp_bm

                    # Recalculate total capital after swap + SIP
                    capital = sum(
                        units.get(etf, 0) * (self._get_price(etf, day) or 0)
                        for etf in portfolio
                    )

                    events.append({
                        "date": day,
                        "type": "REBALANCE",
                        "exits": result["exits"],
                        "entries": result["entries"],
                        "exit_details": exit_details,
                        "portfolio": result["new_portfolio"],
                        "weights": _compute_weights(),
                        "holdings_detail": _build_holdings_detail(day),
                        "rankings": result["rankings"],
                        "capital": round(capital, 2),
                        "txn_cost": round(cost, 2),
                        "exit_value": round(exit_value, 2),
                        "sip_invested": round(sip_invested_this_month, 2),
                    })
                else:
                    # No portfolio change, but still inject SIP
                    sip_invested_this_month = 0
                    if sip_amount > 0 and investment_plan in ("sip", "both"):
                        sip_invested_this_month = sip_amount
                        total_invested += sip_amount
                        sip_alloc = sip_amount / len(portfolio) if portfolio else 0
                        for etf in portfolio:
                            bp_s = self._get_price(etf, day)
                            if bp_s and bp_s > 0:
                                new_units = sip_alloc / bp_s
                                old_units = units.get(etf, 0)
                                old_cost = old_units * buy_prices.get(etf, 0)
                                units[etf] = old_units + new_units
                                total_cost = old_cost + (new_units * bp_s)
                                buy_prices[etf] = total_cost / units[etf] if units[etf] > 0 else bp_s
                                if etf not in buy_dates:
                                    buy_dates[etf] = day
                        # Buy benchmark units with same SIP amount
                        for bm_name, bm_data in self.benchmark_prices.items():
                            bp_bm = bm_data.get(day, bench_start_prices[bm_name])
                            if bp_bm > 0:
                                bench_units[bm_name] += sip_amount / bp_bm

                        capital = sum(
                            units.get(etf, 0) * (self._get_price(etf, day) or 0)
                            for etf in portfolio
                        )

                    events.append({
                        "date": day,
                        "type": "REBALANCE_NO_CHANGE",
                        "portfolio": list(portfolio),
                        "holdings_detail": _build_holdings_detail(day),
                        "capital": round(capital, 2),
                        "sip_invested": round(sip_invested_this_month, 2),
                    })

            # SIP-only injection (for "both" plan in the initial month)
            if is_sip_only and sip_amount > 0 and investment_plan == "both":
                sip_invested_this_date = sip_amount
                total_invested += sip_amount
                sip_alloc = sip_amount / len(portfolio) if portfolio else 0
                for etf in portfolio:
                    bp_s = self._get_price(etf, day)
                    if bp_s and bp_s > 0:
                        new_units = sip_alloc / bp_s
                        old_units = units.get(etf, 0)
                        old_cost = old_units * buy_prices.get(etf, 0)
                        units[etf] = old_units + new_units
                        total_cost_val = old_cost + (new_units * bp_s)
                        buy_prices[etf] = total_cost_val / units[etf] if units[etf] > 0 else bp_s
                        if etf not in buy_dates:
                            buy_dates[etf] = day
                # Buy benchmark units with same SIP amount
                for bm_name, bm_data in self.benchmark_prices.items():
                    bp_bm = bm_data.get(day, bench_start_prices[bm_name])
                    if bp_bm > 0:
                        bench_units[bm_name] += sip_amount / bp_bm

                capital = sum(
                    units.get(etf, 0) * (self._get_price(etf, day) or 0)
                    for etf in portfolio
                )

                events.append({
                    "date": day,
                    "type": "SIP_INVESTMENT",
                    "portfolio": list(portfolio),
                    "holdings_detail": _build_holdings_detail(day),
                    "capital": round(capital, 2),
                    "sip_invested": round(sip_invested_this_date, 2),
                })

            prev_portfolio_value = capital

        # ── Compute Performance Metrics ───────────────────────────────────
        total_days = end_idx - start_idx
        total_years = total_days / 252.0 if total_days > 0 else 1

        # For SIP/Both, use total_invested; for one-time, use initial_capital
        effective_invested = total_invested if total_invested > 0 else initial_capital
        total_return = (capital / effective_invested) - 1 if effective_invested > 0 else 0
        cagr = (capital / effective_invested) ** (1 / total_years) - 1 if (total_years > 0 and effective_invested > 0) else 0

        # Sharpe (calculated from period returns, not daily)
        n_periods = len(period_returns_list)
        if n_periods > 1:
            mean_r = sum(period_returns_list) / n_periods
            var_r = sum((r - mean_r) ** 2 for r in period_returns_list) / n_periods
            std_r = math.sqrt(var_r) if var_r > 0 else 1e-10
            # Annualize based on number of periods per year
            periods_per_year = n_periods / total_years if total_years > 0 else 12
            sharpe = (mean_r / std_r) * math.sqrt(periods_per_year) if std_r > 0 else 0
        else:
            sharpe = 0
            std_r = 0

        # Max Drawdown
        first_value = equity_curve[0]["value"] if equity_curve else effective_invested
        peak = first_value if first_value > 0 else 1
        max_dd = 0
        max_dd_date = None
        max_dd_peak = peak
        for pt in equity_curve:
            if pt["value"] > peak:
                peak = pt["value"]
            dd = (peak - pt["value"]) / peak
            if dd > max_dd:
                max_dd = dd
                max_dd_date = pt["date"]
                max_dd_peak = peak

        # Win Rate (positive period returns)
        positive_periods = sum(1 for r in period_returns_list if r > 0)
        win_rate = positive_periods / n_periods if n_periods > 0 else 0

        # Annualised Volatility (based on period returns)
        periods_per_year = n_periods / total_years if total_years > 0 else 12
        ann_vol = std_r * math.sqrt(periods_per_year) if n_periods > 1 else 0

        # Beta vs primary benchmark (Nifty 50) - calculated at measurement points only
        primary_bm_data = self.benchmark_prices.get(self._primary_bm, {})
        bench_period_returns = []
        bench_prev_value = None
        
        for pt in equity_curve:
            day = pt["date"]
            bp = primary_bm_data.get(day, 0)
            if bench_prev_value is not None and bench_prev_value > 0:
                bench_period_returns.append((bp / bench_prev_value) - 1)
            bench_prev_value = bp
        
        # Align portfolio and benchmark period returns
        if len(bench_period_returns) == len(period_returns_list) and len(period_returns_list) > 1:
            mean_b = sum(bench_period_returns) / len(bench_period_returns)
            mean_p = sum(period_returns_list) / len(period_returns_list)
            cov = sum((period_returns_list[i] - mean_p) * (bench_period_returns[i] - mean_b)
                       for i in range(len(bench_period_returns))) / len(bench_period_returns)
            var_b = sum((b - mean_b) ** 2 for b in bench_period_returns) / len(bench_period_returns)
            beta = cov / var_b if var_b > 0 else 0
        else:
            beta = 0

        metrics = {
            "total_return": round(total_return * 100, 2),
            "cagr": round(cagr * 100, 2),
            "sharpe": round(sharpe, 2),
            "max_drawdown": round(max_dd * 100, 2),
            "max_drawdown_date": max_dd_date,
            "max_drawdown_peak": round(max_dd_peak, 2),
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
        }

        # Current indicators for universe
        final_date = self.trading_days[end_idx]

        # Compute final holdings detail from actual units
        final_holdings_detail = {}
        final_total_value = 0
        for etf in portfolio:
            cp = self._get_price(etf, final_date) or 0
            cv = units.get(etf, 0) * cp
            final_total_value += cv
        final_weights = {}
        for etf in portfolio:
            cp = self._get_price(etf, final_date) or 0
            cv = units.get(etf, 0) * cp
            invested = units.get(etf, 0) * buy_prices.get(etf, 0)
            pnl = round(((cp / buy_prices[etf]) - 1) * 100, 2) if buy_prices.get(etf, 0) > 0 else 0
            final_weights[etf] = round(cv / final_total_value, 4) if final_total_value > 0 else 0
            final_holdings_detail[etf] = {
                "units": round(units.get(etf, 0), 4),
                "buy_price": round(buy_prices.get(etf, 0), 2),
                "buy_date": buy_dates.get(etf, ""),
                "current_price": round(cp, 2),
                "invested": round(invested, 2),
                "current_value": round(cv, 2),
                "pnl_pct": pnl,
            }

        # Build universe snapshot for ALL ETFs with available data
        universe_snapshot = []
        all_etf_scores = []
        for etf_info in ETF_UNIVERSE:
            ticker = etf_info["scrip"]
            close = self._get_price(ticker, final_date)
            if close is None:
                continue
            ret_pcts = {}
            returns_list = []
            skip_score = False
            for tf in timeframes:
                r = self.return_over(ticker, final_date, tf)
                ret_pcts[f"ret_{tf}d"] = round(r * 100, 2) if r is not None else None
                if r is not None:
                    returns_list.append(r)
                else:
                    skip_score = True
            score = sum(r * w for r, w in zip(returns_list, weights[:len(returns_list)])) if returns_list else 0
            sh = self.sharpe_return(ticker, final_date) or 0
            all_etf_scores.append((ticker, score, sh, skip_score, ret_pcts))

        # Sort: full-data ETFs first by score desc, then partial-data
        full = [(t, s, sh, skip, rp) for t, s, sh, skip, rp in all_etf_scores if not skip]
        partial = [(t, s, sh, skip, rp) for t, s, sh, skip, rp in all_etf_scores if skip]
        full.sort(key=lambda x: -x[1])
        partial.sort(key=lambda x: -x[1])
        ranked_all = full + partial

        for rank_idx, (ticker, score, sh, skip, ret_pcts) in enumerate(ranked_all):
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
            equity_curve, benchmark_curves, events, effective_invested
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
                "exit_rank": exit_rank_threshold,
                "investment_plan": investment_plan,
                "sip_amount": sip_amount,
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
        }

    def _build_monthly_summary(self, equity_curve, benchmark_curves, events,
                                initial_capital):
        """Build a month-by-month validation table from backtest data."""
        # Group equity curve by month
        eq_by_month = {}
        for pt in equity_curve:
            ym = pt["date"][:7]
            eq_by_month.setdefault(ym, []).append(pt)

        # Group each benchmark curve by month
        bm_by_month = {}  # {bm_name: {ym: [pts]}}
        for bm_name, bm_curve in benchmark_curves.items():
            bm_by_month[bm_name] = {}
            for pt in bm_curve:
                ym = pt["date"][:7]
                bm_by_month[bm_name].setdefault(ym, []).append(pt)

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

        # Primary benchmark name for alpha calc
        primary = self._primary_bm
        months = sorted(eq_by_month.keys())
        summary = []
        cumulative_return = 0.0
        peak_value = initial_capital

        for ym in months:
            eq_pts = eq_by_month[ym]

            month_open = eq_pts[0]["value"]
            month_close = eq_pts[-1]["value"]
            month_high = max(p["value"] for p in eq_pts)
            month_low = min(p["value"] for p in eq_pts)
            month_return_pct = ((month_close - month_open) / month_open * 100) if month_open else 0
            cumulative_return = ((month_close - initial_capital) / initial_capital * 100) if initial_capital > 0 else 0

            # Per-benchmark monthly returns
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
                "benchmark_returns": bm_returns,
                "alpha_pct": round(month_return_pct - primary_bm_ret, 2),
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