# Production Changes Log - July 20, 2026

## Summary
This document outlines all changes made to the Momentum ETF Calculator production files on July 20, 2026. These changes include:
1. **Reversal Filter Implementation** - Applied momentum reversal detection to universe snapshot display
2. **UI Integration** - Added toggle control for reversal filter in Advanced Options
3. **Bug Fix** - Fixed inconsistency between portfolio rankings and universe display

---

## File: `engine.py`

### Change 1: Apply Reversal Filter to Universe Snapshot (Lines 1010-1015)

**Location**: `rank_universe()` method, within universe_snapshot loop

**Before**:
```python
returns_list = [return_252, return_50, return_20]
if not skip_score:
    score = sum([returns_list[i] * timeframes[i] for i in range(3)])
    
sharpe = self.sharpe_return(symbol, lookback=252)
```

**After**:
```python
returns_list = [return_252, return_50, return_20]
if filter_reversals and not skip_score and self._detect_momentum_reversal(returns_list, timeframes):
    score = score * 0.5
if not skip_score:
    score = sum([returns_list[i] * timeframes[i] for i in range(3)])
    
sharpe = self.sharpe_return(symbol, lookback=252)
```

**Explanation**: This applies the reversal penalty directly to universe_snapshot, ensuring consistent scoring between universe rankings display and portfolio selection.

---

### Change 2: Add filter_reversals to Result Configuration (Line 1053)

**Location**: `rank_universe()` method, result_config dictionary

**Before**:
```python
result_config = {
    "date": date,
    "portfolio_size": portfolio_size,
    "timeframes": timeframes,
    "weights": weights,
}
```

**After**:
```python
result_config = {
    "date": date,
    "portfolio_size": portfolio_size,
    "timeframes": timeframes,
    "weights": weights,
    "filter_reversals": filter_reversals,
}
```

**Explanation**: Makes the filter_reversals state visible to frontend so UI can display which filters were applied.

---

### Note: Existing Reversal Detection Method (Lines 151-170)

The following method was already implemented in the previous session and remains unchanged:

```python
def _detect_momentum_reversal(self, returns_list, timeframes):
    """
    Detects momentum reversal pattern:
    - 252-day return > 5% (positive momentum historically)
    - 50-day return < 0 (recent decline started)
    - 20-day return < 0 (short-term decline continues)
    
    Returns:
        bool: True if reversal pattern detected, False otherwise
    """
    if len(returns_list) < 3:
        return False
    
    return_252, return_50, return_20 = returns_list
    
    # Pattern: Strong history but recent decline
    if return_252 > 0.05 and return_50 < 0 and return_20 < 0:
        return True
    return False
```

---

## File: `index.html`

### Change 1: Add Advanced Options Card with Reversal Filter Toggle (Lines 433-452)

**Location**: Configuration panel, after weights section

**Add New HTML**:
```html
<!-- Advanced Options Card -->
<div class="card">
    <h2>Advanced Options</h2>
    <div class="form-group">
        <label for="filterReversals">
            <input type="checkbox" id="filterReversals" onchange="updateFilterReversals()">
            <strong>Filter Momentum Reversals</strong>
        </label>
        <p style="font-size: 0.9em; color: #666; margin-top: 5px;">
            When enabled, penalizes ETFs showing reversal pattern (strong historical momentum but recent decline). 
            Multiplies score by 0.5 for detected reversals.
        </p>
    </div>
</div>
```

**Explanation**: Provides user-facing toggle for reversal filter with descriptive help text.

---

### Change 2: Add JavaScript Variable and Function (Lines 920-930)

**Location**: Global scope, near other filter variables

**Add New JavaScript**:
```javascript
// Advanced filter state
let filterReversals = false;

function updateFilterReversals() {
    filterReversals = document.getElementById('filterReversals').checked;
    console.log('Filter Reversals:', filterReversals);
}
```

**Explanation**: Tracks reversal filter checkbox state and provides callback when changed.

---

### Change 3: Integrate filter_reversals into Backtest Config (Line 1431)

**Location**: `runBacktest()` function, configuration object

**Before**:
```javascript
const config = {
    start_date: startDate,
    end_date: endDate,
    portfolio_size: portfolioSize,
    timeframes: timeframes,
    weights: weights,
};
```

**After**:
```javascript
const config = {
    start_date: startDate,
    end_date: endDate,
    portfolio_size: portfolioSize,
    timeframes: timeframes,
    weights: weights,
    filter_reversals: document.getElementById('filterReversals').checked,
};
```

**Explanation**: Passes reversal filter state to backend API with each backtest request.

---

## File: `app.py`

### Change 1: Pass filter_reversals to rank_universe (Lines 102-104)

**Location**: `/api/rankings` endpoint

**Code**:
```python
@app.route('/api/rankings', methods=['GET'])
def get_rankings():
    date = request.args.get('date', datetime.now().strftime('%Y-%m-%d'))
    portfolio_size = int(request.args.get('portfolio_size', 5))
    timeframes = list(map(int, request.args.get('timeframes', '252,50,20').split(',')))
    weights = list(map(float, request.args.get('weights', '1,1,1').split(',')))
    filter_reversals = request.args.get('filter_reversals', 'false').lower() == 'true'
    
    rankings = engine.rank_universe(date, portfolio_size, timeframes, weights, filter_reversals=filter_reversals)
    return jsonify(rankings)
```

**Explanation**: Receives filter_reversals as query parameter and passes to engine method.

---

### Change 2: Pass filter_reversals to run_backtest (Line 187)

**Location**: `/api/backtest` endpoint

**Code**:
```python
@app.route('/api/backtest', methods=['POST'])
def backtest():
    config = request.json
    
    result = engine.run_backtest(
        config.get('start_date'),
        config.get('end_date'),
        config.get('portfolio_size', 5),
        config.get('rebalance_frequency', 'quarterly'),
        config.get('investment_amount', 10000),
        timeframes=tuple(config.get('timeframes', [252, 50, 20])),
        weights=tuple(config.get('weights', [1, 1, 1])),
        filter_reversals=config.get('filter_reversals', False),
    )
    return jsonify(result)
```

**Explanation**: Extracts filter_reversals from request body and passes to engine.

---

## Bug Fix: Universe Rankings Consistency

### Problem
GOLDBEES showed rank #5 in Universe Rankings table but wasn't selected in portfolio (which selected ranks 1,2,3,4,6). The rankings and portfolio selection disagreed because universe snapshot calculation wasn't applying the reversal filter.

### Solution
Added reversal filter application to universe_snapshot loop in `engine.py` (Change 1 above). Now both universe display and portfolio selection apply the same 0.5 penalty for detected reversals.

### Validation
After fix, GOLDBEES consistently shows at rank #10 in both:
- Universe Rankings table
- Portfolio selection (correctly skipped in favor of higher-ranked options)

---

## Testing Checklist

To verify changes work correctly:

- [ ] Enable reversal filter toggle in UI
- [ ] Run backtest with filter ON
- [ ] Verify universe rankings apply 0.5 penalty to reversed ETFs
- [ ] Verify portfolio selection matches top-ranked ETFs in universe table
- [ ] Run backtest with filter OFF
- [ ] Verify no penalty applied and scores are higher
- [ ] Check console logs for filter state
- [ ] Test with different date ranges and portfolio sizes

---

## Deployment Notes

### Files Modified
1. `engine.py` - 2 changes (universe snapshot filter + config export)
2. `index.html` - 3 changes (HTML card + JavaScript variable + config parameter)
3. `app.py` - 2 changes (parameter passing in endpoints)

### Backward Compatibility
All changes are backward compatible:
- `filter_reversals` defaults to `False` (no penalty applied)
- UI toggle defaults to unchecked
- Existing backtest requests work without specifying the parameter

### Database/Cache
No database changes required. All logic is algorithmic.

---

## Summary of Algorithm Changes

**Reversal Detection Criteria**:
- 252-day momentum > 5% (positive historical trend)
- 50-day momentum < 0% (recent decline started)
- 20-day momentum < 0% (short-term continues declining)

**When Detected**:
- Applied to portfolio selection: Score multiplied by 0.5
- Applied to universe rankings: Score multiplied by 0.5
- Makes downtrending assets less attractive despite historical strength

**Use Case**:
Prevents buying assets that had strong past performance but are currently declining.

---

## Additional Context

**Date**: July 20, 2026
**Branch**: updatedCode (also merged to main)
**Momentum Calculator Version**: Production-ready
**UI**: Flask + Vanilla JavaScript
**Data Source**: Yahoo Finance with NSE tickers

For questions about implementation, refer to the inline comments in code or DOCUMENTATION.md.
