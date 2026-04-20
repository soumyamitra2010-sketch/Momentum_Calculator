"""
Flask API Server for Momentum ETF Calculator.
Run: python app.py
Access: http://localhost:5000
"""

import json
from flask import Flask, request, jsonify, send_file
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
    date = request.args.get("date", "2026-04-18")
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
    date = request.args.get("date", "2026-04-18")
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


if __name__ == "__main__":
    print("Starting Momentum Calculator server on http://localhost:5000")
    app.run(debug=True, port=5000)
