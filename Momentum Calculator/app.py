"""
Flask API Server for Momentum ETF Calculator.
Run: python app.py
Access: http://localhost:5000
"""

import json
import io
import csv
import os
from datetime import datetime
from flask import Flask, request, jsonify, send_file, Response
from engine import MomentumEngine
from etf_data import ETF_UNIVERSE
# Profile storage
PROFILE_FILE = "profiles.json"
MAX_PROFILES = 10


def load_profiles():
    """Load profiles from JSON file."""
    if os.path.exists(PROFILE_FILE):
        try:
            with open(PROFILE_FILE, 'r') as f:
                return json.load(f)
        except Exception:
            return {}
    return {}


def save_profiles(profiles):
    """Save profiles to JSON file."""
    with open(PROFILE_FILE, 'w') as f:
        json.dump(profiles, f, indent=2)

app = Flask(__name__, static_folder=".", static_url_path="")


@app.after_request
def add_cors_headers(response):
    response.headers["Access-Control-Allow-Origin"] = "*"
    response.headers["Access-Control-Allow-Headers"] = "Content-Type"
    response.headers["Access-Control-Allow-Methods"] = "GET, POST, OPTIONS"
    # Add cache-busting headers for HTML files
    if 'text/html' in response.content_type:
        response.headers["Cache-Control"] = "no-cache, no-store, must-revalidate"
        response.headers["Pragma"] = "no-cache"
        response.headers["Expires"] = "0"
    return response

engine = MomentumEngine()

# Constants for validation
allowed_sizes = [5, 6, 7] + list(range(20, 76))  # Allow 20-75 for custom selection

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
    filter_reversals = request.args.get("filter_reversals", "false").lower() == "true"

    ranked = engine.rank_universe(date, timeframes, weights, ema_filter, filter_reversals=filter_reversals)
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
    # Import ETF universe data
    from etf_data import ALL_ETF_UNIVERSE, ETF_UNIVERSE
    
    # Get available ETFs (intersection of ALL_ETF_UNIVERSE and loaded prices)
    available_scrips = set(engine.prices.keys())
    available_etfs = [e for e in ALL_ETF_UNIVERSE if e["scrip"] in available_scrips]
    
    return jsonify({
        "first_date": engine.trading_days[0] if engine.trading_days else None,
        "last_date": engine.trading_days[-1] if engine.trading_days else None,
        "total_trading_days": len(engine.trading_days),
        "etfs_loaded": len(engine.prices),
        "benchmarks_loaded": list(engine.benchmark_prices.keys()),
        "all_etf_universe": ALL_ETF_UNIVERSE,
        "default_etf_universe": [e["scrip"] for e in ETF_UNIVERSE],
        "available_etfs": available_etfs,
        "min_etf_selection": 20,
        "max_etf_selection": 75,
    })


@app.route("/api/backtest", methods=["POST"])
def run_backtest():
    """Run a full backtest with the given configuration."""
    config = request.get_json()
    if not config:
        return jsonify({"error": "JSON body required"}), 400

    # Validate required fields
    ps = config.get("portfolio_size", 5)
    if ps not in allowed_sizes:
        return jsonify({"error": f"portfolio_size must be one of {allowed_sizes}"}), 400

    allowed_freq = ["weekly", "monthly", "quarterly"]
    freq = config.get("frequency", "monthly")
    if freq not in allowed_freq:
        return jsonify({"error": f"frequency must be one of {allowed_freq}"}), 400

    allowed_plans = ["onetime", "sip", "both"]
    plan = config.get("investment_plan", "onetime")
    if plan not in allowed_plans:
        return jsonify({"error": f"investment_plan must be one of {allowed_plans}"}), 400

    # Handle custom ETF universe if provided
    etf_universe = config.get("etf_universe")
    universe_mode = config.get("universe_mode", "default")
    filter_reversals = config.get("filter_reversals", False)
    use_rsi = config.get("use_rsi", False)
    use_regime_adapt = config.get("use_regime_adapt", False)
    use_vol_weighting = config.get("use_vol_weighting", False)
    
    print(f"[DEBUG] Backend received ETF universe: {etf_universe}")
    print(f"[DEBUG] Universe mode: {universe_mode}")
    print(f"[DEBUG] Filter reversals: {filter_reversals}")
    print(f"[DEBUG] Use RSI: {use_rsi}")
    print(f"[DEBUG] Use Regime Adaptation: {use_regime_adapt}")
    print(f"[DEBUG] Use Volatility Weighting: {use_vol_weighting}")
    print(f"[DEBUG] ETF universe type: {type(etf_universe)}")
    
    if etf_universe and isinstance(etf_universe, list) and len(etf_universe) >= 20:
        # Use custom universe - filter to only include ETFs that exist in our data
        available_etfs = set(engine.prices.keys())
        valid_custom = [e for e in etf_universe if e in available_etfs]
        print(f"[DEBUG] Valid custom ETFs: {valid_custom}")
        if len(valid_custom) >= 20:
            config["_custom_etf_list"] = valid_custom
            print(f"[DEBUG] Set custom ETF list with {len(valid_custom)} ETFs")
        else:
            print(f"[DEBUG] Not enough valid ETFs ({len(valid_custom)} < 20), using default")
    else:
        print(f"[DEBUG] Using default ETF universe")
    
    # Run the backtest using the engine
    result = engine.run_backtest(config)

    if "error" in result:
        return jsonify(result), 400

    return jsonify(result)


@app.route("/api/profiles", methods=["GET"])
def get_profiles():
    """Get all saved profiles."""
    profiles = load_profiles()
    return jsonify(profiles)


@app.route("/api/profiles", methods=["POST"])
def create_profile():
    """Create a new profile from backtest criteria for forward testing."""
    data = request.get_json()
    if not data or "name" not in data or "config" not in data:
        return jsonify({"error": "name and config required"}), 400

    profiles = load_profiles()

    if len(profiles) >= MAX_PROFILES:
        return jsonify({"error": f"Maximum {MAX_PROFILES} profiles allowed"}), 400

    # Get the latest available date (today from data perspective)
    latest_date = engine.trading_days[-1] if engine.trading_days else "2025-01-01"
    
    profile_id = f"profile_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    
    # Store config for forward testing - will start from today going forward
    profile_config = data["config"].copy()
    # The profile starts tracking from today (end date of backtest)
    profile_config["start_date"] = latest_date
    
    profile = {
        "id": profile_id,
        "name": data["name"],
        "config": profile_config,
        "created_at": datetime.now().isoformat(),
        "tracking_start_date": latest_date,
        "original_backtest_start": data["config"].get("start_date", ""),
    }

    profiles[profile_id] = profile
    save_profiles(profiles)

    return jsonify({"success": True, "profile": profile})


@app.route("/api/profiles/<profile_id>", methods=["GET"])
def get_profile(profile_id):
    """Get a specific profile."""
    profiles = load_profiles()
    if profile_id not in profiles:
        return jsonify({"error": "Profile not found"}), 404
    return jsonify(profiles[profile_id])


@app.route("/api/profiles/<profile_id>", methods=["DELETE"])
def delete_profile(profile_id):
    """Delete a profile."""
    profiles = load_profiles()
    if profile_id not in profiles:
        return jsonify({"error": "Profile not found"}), 404

    del profiles[profile_id]
    save_profiles(profiles)

    return jsonify({"success": True})


@app.route("/api/profiles/<profile_id>/run", methods=["POST"])
def run_profile(profile_id):
    """Run profile from tracking start date to today (forward test)."""
    profiles = load_profiles()
    if profile_id not in profiles:
        return jsonify({"error": "Profile not found"}), 404

    profile = profiles[profile_id]
    config = profile["config"].copy()
    
    print(f"[PROFILE RUN] Profile: {profile_id}")
    print(f"[PROFILE RUN] Universe mode: {config.get('universe_mode', 'NOT SET')}")
    print(f"[PROFILE RUN] ETF universe count: {len(config.get('etf_universe', []))}")
    print(f"[PROFILE RUN] First 10 ETFs: {config.get('etf_universe', [])[:10]}")
    
    # Handle custom ETF universe - convert etf_universe to _custom_etf_list
    etf_universe = config.get("etf_universe")
    universe_mode = config.get("universe_mode", "default")
    
    if universe_mode == "custom" and etf_universe and isinstance(etf_universe, list) and len(etf_universe) >= 20:
        # Filter to only include ETFs that exist in our data
        available_etfs = set(engine.prices.keys())
        valid_custom = [e for e in etf_universe if e in available_etfs]
        print(f"[PROFILE RUN] Valid custom ETFs: {len(valid_custom)}")
        if len(valid_custom) >= 20:
            config["_custom_etf_list"] = valid_custom
            print(f"[PROFILE RUN] Set custom ETF list with {len(valid_custom)} ETFs")
        else:
            print(f"[PROFILE RUN] Not enough valid ETFs ({len(valid_custom)} < 20), using default")
    else:
        print(f"[PROFILE RUN] Using default ETF universe")

    # Set end date to latest available data
    latest_date = engine.trading_days[-1] if engine.trading_days else "2025-01-01"
    config["end_date"] = latest_date
    
    # Ensure start date is the tracking start date (when profile was created)
    # This makes it a forward test from that point
    config["start_date"] = profile.get("tracking_start_date", latest_date)
    
    # For forward test, we typically want one-time investment at start
    # Keep the original investment plan settings

    # Run backtest with the profile config
    result = engine.run_backtest(config)
    
    print(f"[PROFILE RUN] Result universe mode: {result.get('config', {}).get('universe_mode', 'NOT SET')}")

    if "error" in result:
        return jsonify(result), 400

    return jsonify(result)


if __name__ == "__main__":
    print("Starting Momentum Calculator server on http://localhost:5000")
    app.run(debug=True, port=5000)
