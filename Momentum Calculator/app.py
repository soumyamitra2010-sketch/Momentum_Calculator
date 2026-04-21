"""
Flask API Server for Momentum ETF Calculator.
Run: python app.py
Access: http://localhost:5000
"""

import json
import io
import csv
from flask import Flask, request, jsonify, send_file, Response
from engine import MomentumEngine
from etf_data import ETF_UNIVERSE

app = Flask(__name__, static_folder=".", static_url_path="")


@app.after_request
def add_cors_headers(response):
    response.headers["Access-Control-Allow-Origin"] = "*"
    response.headers["Access-Control-Allow-Headers"] = "Content-Type"
    response.headers["Access-Control-Allow-Methods"] = "GET, POST, OPTIONS"
    return response

engine = MomentumEngine()


@app.route("/")
def index():
    return send_file("index.html")


@app.route("/api/universe", methods=["GET"])
def get_universe():
    return jsonify(ETF_UNIVERSE)


@app.route("/api/indicators", methods=["GET"])
def get_indicators():
    """Return current indicators for all ETFs on a given date."""
    last_day = engine.trading_days[-1] if engine.trading_days else "2025-01-01"
    date = request.args.get("date", last_day)
    timeframes = [int(t) for t in request.args.get("timeframes", "252,50,20").split(",")]
    result = []
    for etf in ETF_UNIVERSE:
        ticker = etf["scrip"]
        row = {"scrip": ticker, "sector": etf["sector"]}
        for tf in timeframes:
            r = engine.return_over(ticker, date, tf)
            row[f"return_{tf}d"] = round(r * 100, 2) if r is not None else None
        row["sharpe"] = round(engine.sharpe_return(ticker, date) or 0, 2)
        row["rsi"] = round(engine.rsi(ticker, date) or 0, 1)
        row["volatility"] = round((engine.volatility(ticker, date) or 0) * 100, 1)
        ema = engine.ema200(ticker, date)
        close = engine._get_price(ticker, date)
        row["ema200"] = round(ema, 2) if ema else None
        row["close"] = round(close, 2) if close else None
        row["above_ema"] = (close > ema) if (close and ema) else None
        result.append(row)
    return jsonify(result)


@app.route("/api/rankings", methods=["GET"])
def get_rankings():
    """Return ranked ETFs on a given date with given params."""
    last_day = engine.trading_days[-1] if engine.trading_days else "2025-01-01"
    date = request.args.get("date", last_day)
    timeframes = [int(t) for t in request.args.get("timeframes", "252,50,20").split(",")]
    raw_weights = [float(w) for w in request.args.get("weights", "1,1,1").split(",")]
    wsum = sum(raw_weights)
    weights = [w / wsum for w in raw_weights]
    ema_filter = request.args.get("ema_filter", "false").lower() == "true"

    ranked = engine.rank_universe(date, timeframes, weights, ema_filter)
    result = []
    for i, (ticker, score, sharpe, mcap) in enumerate(ranked):
        result.append({
            "rank": i + 1,
            "scrip": ticker,
            "sector": engine.etf_meta[ticker]["sector"],
            "score": round(score * 100, 2),
            "sharpe": round(sharpe, 2),
            "market_cap": mcap,
        })
    return jsonify(result)


@app.route("/api/info", methods=["GET"])
def get_info():
    """Return available date range and metadata."""
    return jsonify({
        "first_date": engine.trading_days[0] if engine.trading_days else None,
        "last_date": engine.trading_days[-1] if engine.trading_days else None,
        "total_trading_days": len(engine.trading_days),
        "etfs_loaded": len(engine.prices),
        "benchmarks_loaded": list(engine.benchmark_prices.keys()),
    })


@app.route("/api/backtest", methods=["POST"])
def run_backtest():
    """Run a full backtest with the given configuration."""
    config = request.get_json()
    if not config:
        return jsonify({"error": "JSON body required"}), 400

    # Validate required fields
    allowed_sizes = [5, 6, 7]
    ps = config.get("portfolio_size", 5)
    if ps not in allowed_sizes:
        return jsonify({"error": f"portfolio_size must be one of {allowed_sizes}"}), 400

    allowed_freq = ["weekly", "monthly"]
    freq = config.get("frequency", "monthly")
    if freq not in allowed_freq:
        return jsonify({"error": f"frequency must be one of {allowed_freq}"}), 400

    result = engine.run_backtest(config)
    return jsonify(result)


@app.route("/api/export_csv", methods=["POST"])
def export_csv():
    """Export momentum rankings CSV for all rebalance dates."""
    config = request.get_json() or {}
    timeframes = config.get("timeframes", [252, 50, 20])
    raw_weights = config.get("weights", [1, 1, 1])
    wsum = sum(raw_weights)
    weights = [w / wsum for w in raw_weights]
    start_date = config.get("start_date", "2023-01-01")
    end_date = config.get("end_date", "2026-04-18")
    frequency = config.get("frequency", "monthly")
    rebal_day = config.get("rebal_day", 1)
    portfolio_size = config.get("portfolio_size", 5)
    exit_rank_val = config.get("exit_rank", 0)

    max_tf = max(timeframes)
    idx = engine._date_index(start_date)
    if idx is None or idx < max_tf + 1:
        idx = max_tf + 1
    actual_start = engine.trading_days[idx]
    end_idx = engine._date_index(end_date)
    actual_end = engine.trading_days[end_idx] if end_idx else engine.trading_days[-1]

    rebal_dates = engine._get_rebalancing_dates(actual_start, actual_end, frequency, rebal_day)
    all_dates = sorted(set([actual_start] + rebal_dates))

    portfolio = engine.select_portfolio(actual_start, timeframes, weights, False, portfolio_size)

    header = ["Date", "Scrip", "Sector", "Close"]
    for tf in timeframes:
        header.append(f"Price_{tf}d_Ago")
    for tf in timeframes:
        header.append(f"Return_{tf}d_%")
    for tf in timeframes:
        header.append(f"Rank_{tf}d")
    header += ["Has_252d_Data", "Combined_Score_%", "Overall_Rank", "In_Portfolio",
               "Sharpe_252d", "RSI_14", "Volatility_%", "EMA200", "Above_EMA200"]

    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(header)

    for date in all_dates:
        etf_data = []
        for etf in ETF_UNIVERSE:
            ticker = etf["scrip"]
            close = engine._get_price(ticker, date)
            if close is None:
                continue
            has_252d = engine.has_history(ticker, date, 252 + 1)
            returns = {}
            prices_ago = {}
            for tf in timeframes:
                r = engine.return_over(ticker, date, tf)
                returns[tf] = r
                series = engine._get_price_series(ticker, date, tf + 1)
                prices_ago[tf] = series[0] if len(series) >= tf + 1 else None
            available = {tf: r for tf, r in returns.items() if r is not None}
            if available:
                combined = sum(r * w for tf, (r, w) in
                               zip(timeframes, zip([returns[tf] for tf in timeframes], weights))
                               if returns[tf] is not None)
                w_used = sum(w for tf, w in zip(timeframes, weights) if returns[tf] is not None)
                combined = combined / w_used if w_used else 0
            else:
                combined = -999
            sharpe = engine.sharpe_return(ticker, date) or 0
            rsi_val = engine.rsi(ticker, date) or 0
            vol = engine.volatility(ticker, date) or 0
            ema = engine.ema200(ticker, date)
            above = (close > ema) if (close and ema) else None
            etf_data.append({"scrip": ticker, "sector": etf["sector"], "close": close,
                             "returns": returns, "prices_ago": prices_ago,
                             "has_252d": has_252d, "combined": combined, "sharpe": sharpe,
                             "rsi": rsi_val, "vol": vol, "ema": ema, "above": above})

        for tf in timeframes:
            with_r = [x for x in etf_data if x["returns"][tf] is not None]
            without_r = [x for x in etf_data if x["returns"][tf] is None]
            for rank, item in enumerate(sorted(with_r, key=lambda x: -x["returns"][tf]), 1):
                item[f"rank_{tf}"] = rank
            for rank, item in enumerate(without_r, len(with_r) + 1):
                item[f"rank_{tf}"] = rank
        full = sorted([x for x in etf_data if x["has_252d"]], key=lambda x: -x["combined"])
        partial = sorted([x for x in etf_data if not x["has_252d"]], key=lambda x: -x["combined"])
        for rank, item in enumerate(full, 1):
            item["overall_rank"] = rank
        for rank, item in enumerate(partial, len(full) + 1):
            item["overall_rank"] = rank
        sorted_data = full + partial

        if date != actual_start and date in rebal_dates:
            result = engine.rebalance(portfolio, date, timeframes, weights, False, exit_rank_val)
            portfolio = result["new_portfolio"]

        for item in sorted_data:
            row = [date, item["scrip"], item["sector"],
                   round(item["close"], 2) if item["close"] else ""]
            for tf in timeframes:
                p = item["prices_ago"][tf]
                row.append(round(p, 2) if p is not None else "")
            for tf in timeframes:
                r = item["returns"][tf]
                row.append(round(r * 100, 2) if r is not None else "")
            for tf in timeframes:
                row.append(item[f"rank_{tf}"])
            row += ["YES" if item["has_252d"] else "NO",
                    round(item["combined"] * 100, 2) if item["combined"] != -999 else "",
                    item["overall_rank"],
                    "YES" if item["scrip"] in portfolio else "",
                    round(item["sharpe"], 2), round(item["rsi"], 1),
                    round(item["vol"] * 100, 1),
                    round(item["ema"], 2) if item["ema"] else "",
                    "YES" if item["above"] else ("NO" if item["above"] is not None else "")]
            writer.writerow(row)

    output.seek(0)
    return Response(output.getvalue(), mimetype="text/csv",
                    headers={"Content-Disposition": "attachment; filename=momentum_rankings.csv"})


if __name__ == "__main__":
    print("Starting Momentum Calculator server on http://localhost:5000")
    app.run(debug=True, port=5000)
