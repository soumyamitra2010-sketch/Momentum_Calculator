"""
Flask API Server V2 — Momentum Stock Calculator.
Run: python app_v2.py
Access: http://localhost:5001
"""

import json
from flask import Flask, request, jsonify, send_file
from engine_v2 import MomentumEngineV2
from data_loader import download_stock_data, get_data_status
from stock_universe import get_universe, get_unique_sectors, BENCHMARKS_V2

app = Flask(__name__, static_folder=".", static_url_path="")

engine = None  # Lazy init after data check


def _get_engine(universe: str = "nifty50") -> MomentumEngineV2:
    global engine
    if engine is None or engine.universe_name != universe:
        engine = MomentumEngineV2(universe)
    return engine


@app.after_request
def add_cors_headers(response):
    response.headers["Access-Control-Allow-Origin"] = "*"
    response.headers["Access-Control-Allow-Headers"] = "Content-Type"
    response.headers["Access-Control-Allow-Methods"] = "GET, POST, OPTIONS"
    return response


# ── Pages ─────────────────────────────────────────────────────────────────────

@app.route("/")
def index():
    return send_file("index_v2.html")


# ── Data Management ───────────────────────────────────────────────────────────

@app.route("/api/v2/data/status", methods=["GET"])
def data_status():
    """Check data status for a universe."""
    universe = request.args.get("universe", "nifty50")
    status = get_data_status(universe)
    return jsonify(status)


@app.route("/api/v2/data/download", methods=["POST"])
def data_download():
    """Trigger data download for a universe."""
    body = request.get_json() or {}
    universe = body.get("universe", "nifty50")
    start_date = body.get("start_date", "2010-01-01")
    force = body.get("force", False)

    result = download_stock_data(universe, start_date, force=force)

    # Reload engine with new data
    global engine
    engine = MomentumEngineV2(universe)

    return jsonify(result)


# ── Universe & Metadata ──────────────────────────────────────────────────────

@app.route("/api/v2/universe", methods=["GET"])
def get_universe_api():
    """Return stock universe with sectors."""
    universe = request.args.get("universe", "nifty50")
    stocks = get_universe(universe)
    return jsonify({
        "universe": universe,
        "count": len(stocks),
        "stocks": stocks,
        "sectors": get_unique_sectors(stocks),
        "benchmarks": [b["name"] for b in BENCHMARKS_V2],
    })


@app.route("/api/v2/info", methods=["GET"])
def get_info():
    """Return data range and metadata."""
    universe = request.args.get("universe", "nifty50")
    eng = _get_engine(universe)
    return jsonify({
        "first_date": eng.trading_days[0] if eng.trading_days else None,
        "last_date": eng.trading_days[-1] if eng.trading_days else None,
        "total_trading_days": len(eng.trading_days),
        "tickers_loaded": len([t for t in eng.tickers if t in eng.price_df.columns]) if not eng.price_df.empty else 0,
        "benchmarks_loaded": list(eng.bench_df.columns) if not eng.bench_df.empty else [],
        "universe": universe,
        "survivorship_bias_warning": True,
    })


# ── Rankings & Indicators ────────────────────────────────────────────────────

@app.route("/api/v2/rankings", methods=["GET"])
def get_rankings():
    """Return ranked stocks on a given date."""
    universe = request.args.get("universe", "nifty50")
    eng = _get_engine(universe)

    last_day = eng.trading_days[-1] if eng.trading_days else "2025-01-01"
    date = request.args.get("date", last_day)
    ranking_method = request.args.get("ranking_method", "log_regression")
    regression_window = int(request.args.get("regression_window", "252"))
    timeframes = [int(t) for t in request.args.get("timeframes", "252,126,63").split(",")]
    raw_weights = [float(w) for w in request.args.get("weights", "1,1,1").split(",")]
    ema_filter = request.args.get("ema_filter", "false").lower() == "true"

    config = {
        "timeframes": timeframes,
        "weights": raw_weights,
        "ema_filter": ema_filter,
        "ranking_method": ranking_method,
        "regression_window": regression_window,
    }

    ranked = eng.rank_universe(date, config)

    # Format for API
    result = []
    for c in ranked:
        result.append({
            "rank": c["rank"],
            "ticker": c["ticker"],
            "name": c.get("name", c["ticker"]),
            "sector": c["sector"],
            "score": round(c["score"] * 100, 2),
            "annualized_return": round(c.get("annualized_return", 0) * 100, 2),
            "r_squared": round(c.get("r_squared", 0), 3),
            "sharpe": round(c["sharpe"], 2),
            "volatility": round(c["volatility"] * 100, 1),
            "rsi": round(c["rsi"], 1),
            "close": round(c["close"], 2),
            "market_cap_cr": c["market_cap_cr"],
        })

    return jsonify(result)


# ── Backtest ──────────────────────────────────────────────────────────────────

@app.route("/api/v2/backtest", methods=["POST"])
def run_backtest():
    """Run a full v2 backtest."""
    config = request.get_json()
    if not config:
        return jsonify({"error": "JSON body required"}), 400

    universe = config.get("universe", "nifty50")
    eng = _get_engine(universe)

    if not eng.trading_days:
        return jsonify({"error": "No data loaded. Download data first."}), 400

    # Validate
    ps = config.get("portfolio_size", 10)
    if ps < 3 or ps > 50:
        return jsonify({"error": "portfolio_size must be between 3 and 50"}), 400

    allowed_freq = ["weekly", "monthly", "quarterly"]
    freq = config.get("frequency", "monthly")
    if freq not in allowed_freq:
        return jsonify({"error": f"frequency must be one of {allowed_freq}"}), 400

    allowed_plans = ["onetime", "sip", "both"]
    plan = config.get("investment_plan", "onetime")
    if plan not in allowed_plans:
        return jsonify({"error": f"investment_plan must be one of {allowed_plans}"}), 400

    allowed_methods = ["weighted_return", "log_regression"]
    method = config.get("ranking_method", "log_regression")
    if method not in allowed_methods:
        return jsonify({"error": f"ranking_method must be one of {allowed_methods}"}), 400

    max_sector_pct = config.get("max_sector_pct", 0.25)
    if max_sector_pct < 0.1 or max_sector_pct > 1.0:
        return jsonify({"error": "max_sector_pct must be between 0.1 and 1.0"}), 400

    result = eng.run_backtest(config)

    if "error" in result:
        return jsonify(result), 400

    return jsonify(result)


if __name__ == "__main__":
    print("=" * 60)
    print("  Momentum Calculator V2 — Stock-based Momentum Engine")
    print("  http://localhost:5001")
    print("=" * 60)
    app.run(debug=True, port=5001)
