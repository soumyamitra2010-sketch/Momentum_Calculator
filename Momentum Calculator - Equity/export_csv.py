"""
Export momentum ranking data to CSV for validation.
Generates a CSV with 252d/50d/20d returns, individual rank per timeframe,
combined score (equal weight), and overall rank for every rebalance date.

Usage:  python export_csv.py
Output: momentum_rankings.csv
"""

import csv
from engine import MomentumEngine
from etf_data import ETF_UNIVERSE


def export_rankings_csv(
    start_date="2023-01-02",
    end_date="2026-04-18",
    frequency="monthly",
    rebal_day=21,
    timeframes=None,
    exit_rank=0,
    output_file="momentum_rankings.csv",
):
    if timeframes is None:
        timeframes = [252, 50, 20]

    engine = MomentumEngine()

    # Determine actual start (need enough history)
    max_tf = max(timeframes)
    idx = engine._date_index(start_date)
    if idx is None or idx < max_tf + 1:
        idx = max_tf + 1
    actual_start = engine.trading_days[idx]
    end_idx = engine._date_index(end_date)
    actual_end = engine.trading_days[end_idx] if end_idx else engine.trading_days[-1]

    # Get rebalancing dates (include actual_start for initial selection)
    rebal_dates = engine._get_rebalancing_dates(actual_start, actual_end, frequency, rebal_day)
    rebal_set = set(rebal_dates) | {actual_start}

    # Build list of ALL trading days from actual_start to actual_end
    start_idx_td = engine.trading_days.index(actual_start)
    end_idx_td = engine._date_index(actual_end)
    all_dates = engine.trading_days[start_idx_td : end_idx_td + 1]

    print(f"Exporting daily data for {len(all_dates)} trading days from {actual_start} to {actual_end}")
    print(f"Rebalance dates: {len(rebal_set)} | Timeframes: {timeframes}, Equal weight: {1/len(timeframes):.4f} each")

    # Run initial selection to track portfolio state
    weights = [1 / len(timeframes)] * len(timeframes)
    portfolio = engine.select_portfolio(actual_start, timeframes, weights, False, 5)

    # CSV header
    header = [
        "Date",
        "Is_Rebalance_Date",
        "Scrip",
        "Sector",
        "Close",
    ]
    for tf in timeframes:
        header.append(f"Price_{tf}d_Ago")
    for tf in timeframes:
        header.append(f"Return_{tf}d_%")
    for tf in timeframes:
        header.append(f"Rank_{tf}d")
    header += [
        "Has_252d_Data",
        "Combined_Score_%",
        "Overall_Rank",
        "In_Portfolio",
        "Sharpe_252d",
        "RSI_14",
        "Volatility_%",
        "EMA200",
        "Above_EMA200",
    ]

    rows = []
    day_count = 0

    for date in all_dates:
        is_rebal = date in rebal_set
        day_count += 1

        # Compute returns for ALL ETFs (include those without full history)
        etf_data = []
        for etf in ETF_UNIVERSE:
            ticker = etf["scrip"]
            close = engine._get_price(ticker, date)
            if close is None:
                continue  # no price at all on this date

            # Check history availability per timeframe
            has_252d = engine.has_history(ticker, date, 252 + 1)
            returns = {}
            prices_ago = {}
            for tf in timeframes:
                r = engine.return_over(ticker, date, tf)
                returns[tf] = r
                # Get the price tf trading days ago
                series = engine._get_price_series(ticker, date, tf + 1)
                prices_ago[tf] = series[0] if len(series) >= tf + 1 else None

            # Compute combined score only from available returns
            available = {tf: r for tf, r in returns.items() if r is not None}
            if available:
                combined_score = sum(r * w for tf, (r, w) in
                                     zip(timeframes,
                                         zip([returns[tf] for tf in timeframes],
                                             weights))
                                     if returns[tf] is not None)
                # Normalize by the sum of weights used
                w_used = sum(w for tf, w in zip(timeframes, weights) if returns[tf] is not None)
                combined_score = combined_score / w_used if w_used else 0
            else:
                combined_score = -999  # no returns at all

            # Compute heavy indicators only on rebalance dates to keep speed
            if is_rebal:
                sharpe = engine.sharpe_return(ticker, date) or 0
                rsi = engine.rsi(ticker, date) or 0
                vol = engine.volatility(ticker, date) or 0
                ema = engine.ema200(ticker, date)
                above_ema = (close > ema) if (close and ema) else None
            else:
                sharpe = ""
                rsi = ""
                vol = ""
                ema = None
                above_ema = None

            etf_data.append({
                "scrip": ticker,
                "sector": etf["sector"],
                "close": close,
                "returns": returns,
                "prices_ago": prices_ago,
                "has_252d": has_252d,
                "combined_score": combined_score,
                "sharpe": sharpe,
                "rsi": rsi,
                "volatility": vol,
                "ema": ema,
                "above_ema": above_ema,
            })

        # Rank by each individual timeframe (ETFs without that return ranked last)
        for tf in timeframes:
            with_return = [x for x in etf_data if x["returns"][tf] is not None]
            without_return = [x for x in etf_data if x["returns"][tf] is None]
            sorted_with = sorted(with_return, key=lambda x: -x["returns"][tf])
            for rank, item in enumerate(sorted_with, 1):
                item[f"rank_{tf}"] = rank
            for rank, item in enumerate(without_return, len(sorted_with) + 1):
                item[f"rank_{tf}"] = rank

        # Rank by combined score (ETFs without 252d ranked after those with)
        full_data = [x for x in etf_data if x["has_252d"]]
        partial_data = [x for x in etf_data if not x["has_252d"]]
        sorted_full = sorted(full_data, key=lambda x: -x["combined_score"])
        sorted_partial = sorted(partial_data, key=lambda x: -x["combined_score"])
        for rank, item in enumerate(sorted_full, 1):
            item["overall_rank"] = rank
        for rank, item in enumerate(sorted_partial, len(sorted_full) + 1):
            item["overall_rank"] = rank
        sorted_by_score = sorted_full + sorted_partial

        # Rebalance portfolio on rebalance dates (after initial)
        if is_rebal and date != actual_start:
            result = engine.rebalance(portfolio, date, timeframes, weights, False, exit_rank)
            portfolio = result["new_portfolio"]

        # Write rows sorted by overall rank
        for item in sorted_by_score:
            row = [
                date,
                "YES" if is_rebal else "",
                item["scrip"],
                item["sector"],
                round(item["close"], 2) if item["close"] else "",
            ]
            for tf in timeframes:
                p = item["prices_ago"][tf]
                row.append(round(p, 2) if p is not None else "")
            for tf in timeframes:
                r = item["returns"][tf]
                row.append(round(r * 100, 2) if r is not None else "")
            for tf in timeframes:
                row.append(item[f"rank_{tf}"])
            row += [
                "YES" if item["has_252d"] else "NO",
                round(item["combined_score"] * 100, 2) if item["combined_score"] != -999 else "",
                item["overall_rank"],
                "YES" if item["scrip"] in portfolio else "",
                round(item["sharpe"], 2) if item["sharpe"] != "" else "",
                round(item["rsi"], 1) if item["rsi"] != "" else "",
                round(item["volatility"] * 100, 1) if item["volatility"] != "" else "",
                round(item["ema"], 2) if item["ema"] else "",
                "YES" if item["above_ema"] else ("NO" if item["above_ema"] is not None else ""),
            ]
            rows.append(row)

        if day_count % 50 == 0 or is_rebal:
            print(f"  {date}: {len(etf_data)} ETFs {'[REBAL]' if is_rebal else ''} portfolio: {portfolio}")

    # Write CSV
    with open(output_file, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(header)
        writer.writerows(rows)

    print(f"\nExported {len(rows)} rows to {output_file}")
    return output_file


if __name__ == "__main__":
    export_rankings_csv(output_file="momentum_rankings_v2.csv")
