"""Quick test of unit-based backtest output."""
import json
import requests

r = requests.post('http://localhost:5000/api/backtest', json={
    'start_date': '2023-01-01', 'end_date': '2026-04-18',
    'timeframes': [252, 50, 20], 'weights': [1, 1, 1],
    'portfolio_size': 5, 'frequency': 'monthly', 'rebal_day': 21,
    'initial_capital': 1000000
})
d = r.json()

if 'error' in d:
    print("ERROR:", d['error'])
    exit(1)

print("=== Final Portfolio ===")
for etf in d['final_portfolio']:
    hd = d['final_holdings_detail'].get(etf, {})
    w = d['final_weights'].get(etf, 0)
    print(f"  {etf}: units={hd.get('units',0):.2f}  "
          f"buy=Rs{hd.get('buy_price',0):.2f}  "
          f"cur=Rs{hd.get('current_price',0):.2f}  "
          f"invested=Rs{hd.get('invested',0):.2f}  "
          f"cur_val=Rs{hd.get('current_value',0):.2f}  "
          f"pnl={hd.get('pnl_pct',0):.2f}%  wt={w*100:.1f}%")

print(f"\nTotal Capital: Rs{d['final_capital']:,.2f}")

print(f"\n=== Initial Event ===")
ev0 = d['events'][0]
for etf, h in ev0['holdings_detail'].items():
    print(f"  {etf}: {h['units']:.2f} units @ Rs{h['buy_price']:.2f} = Rs{h['invested']:.2f}")

print(f"\n=== Events ({len(d['events'])} total) ===")
for ev in d['events']:
    if ev['type'] == 'REBALANCE' and ev.get('exits'):
        print(f"\n  {ev['date']} {ev['type']}")
        print(f"    Exits: {ev['exits']}")
        print(f"    Entries: {ev['entries']}")
        print(f"    Exit Value: Rs{ev.get('exit_value', 0):,.2f}")
        print(f"    Txn Cost: Rs{ev.get('txn_cost', 0):,.2f}")
        for etf2, h2 in ev['holdings_detail'].items():
            marker = " [NEW]" if etf2 in ev['entries'] else ""
            print(f"    {etf2}: {h2['units']:.2f} units @ Rs{h2['buy_price']:.2f} "
                  f"(bought {h2['buy_date']}) "
                  f"-> Rs{h2['current_value']:,.2f} ({h2['pnl_pct']:+.2f}%){marker}")
        print(f"    Total Capital: Rs{ev['capital']:,.2f}")

# Check universe snapshot
us = d.get('universe_snapshot', [])
print(f"\n=== Universe Snapshot: {len(us)} ETFs ===")
for u in us[:5]:
    print(f"  #{u['rank']} {u['scrip']}: score={u['score']:.1f} {'[IN PF]' if u['in_portfolio'] else ''}")
