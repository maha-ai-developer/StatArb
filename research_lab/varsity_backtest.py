"""
Simplified Varsity-Style Pair Trading Backtest

Based EXACTLY on Zerodha Varsity starb.pdf methodology:
- 200-day regression for pair parameters
- Fixed sigma (Standard Error of Residuals) from regression
- Z-Score = (Y - beta*X - intercept) / sigma
- Entry: ±2.0 SD
- Exit: ±1.0 SD  
- Stop: ±3.0 SD
- Beta-neutral position sizing

NO complexity:
- No Guardian
- No recalibration
- No basis tracking
- No train/test split
"""

import os
import json
import numpy as np
import pandas as pd
from typing import Dict, List, Tuple, Optional
from datetime import datetime

# Import config
import sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from infrastructure import config

# Try to import get_lot_size, fallback to default
try:
    from infrastructure.broker.nfo_utils import get_lot_size
except ImportError:
    try:
        from trading_floor.nfo_utils import get_lot_size
    except ImportError:
        # Fallback: return sensible default
        def get_lot_size(symbol):
            # Common lot sizes for NSE F&O
            LOT_SIZES = {
                'AXISBANK': 1200, 'SBIN': 1500, 'KOTAKBANK': 400,
                'HDFCBANK': 550, 'ICICIBANK': 1375, 'BAJAJFINSV': 125,
                'HDFCLIFE': 1100, 'NESTLEIND': 25, 'TATACONSUM': 375,
                'LT': 150, 'ADANIPORTS': 1250, 'CIPLA': 650,
                'APOLLOHOSP': 125, 'SUNPHARMA': 700, 'DRREDDY': 125
            }
            return LOT_SIZES.get(symbol, 500)

# ============================================
# VARSITY PARAMETERS (from starb.pdf)
# ============================================
Z_ENTRY = 2.0        # Entry at ±2.0 SD
Z_EXIT = 1.0         # Exit at ±1.0 SD
Z_STOP = 3.0         # Stop at ±3.0 SD
MAX_HOLD_DAYS = 30   # Max holding period

# Transaction Costs (Zerodha)
BROKERAGE_PER_ORDER = 20
STT_PCT = 0.000125
EXCHANGE_PCT = 0.00019
SLIPPAGE_PCT = 0.001

# Capital
CAPITAL = 500000


def load_pairs() -> List[Dict]:
    """Load validated pairs from pairs_candidates.json."""
    path = os.path.join(config.ARTIFACTS_DIR, "pairs_candidates.json")
    with open(path) as f:
        return json.load(f)


def load_price_data(symbol: str) -> Optional[pd.Series]:
    """Load closing prices for a symbol."""
    path = os.path.join(config.DATA_DIR, f"{symbol}_day.csv")
    if not os.path.exists(path):
        return None
    df = pd.read_csv(path)
    df['date'] = pd.to_datetime(df['date'])
    df = df.sort_values('date')
    return df.set_index('date')['close']


def calculate_transaction_costs(price_y: float, price_x: float, 
                                 qty_y: int, qty_x: int) -> float:
    """Calculate round-trip transaction costs."""
    turnover = (price_y * qty_y + price_x * qty_x) * 2  # Round trip
    
    brokerage = BROKERAGE_PER_ORDER * 4  # 2 legs × 2 (entry + exit)
    stt = turnover * STT_PCT * 0.5  # Only on sell side
    exchange = turnover * EXCHANGE_PCT
    slippage = turnover * SLIPPAGE_PCT
    
    return brokerage + stt + exchange + slippage


def backtest_pair(pair: Dict) -> Dict:
    """
    Backtest a single pair using EXACT Varsity methodology.
    
    Z-Score = (Y - beta*X - intercept) / sigma
    """
    # Extract pair data
    y_sym = pair.get('leg1') or pair.get('stock_y')
    x_sym = pair.get('leg2') or pair.get('stock_x')
    beta = pair.get('beta') or pair.get('hedge_ratio', 1.0)
    intercept = pair.get('intercept', 0.0)
    sigma = pair.get('sigma', 0.0)
    sector = pair.get('sector', 'UNKNOWN')
    
    # Must have sigma
    if sigma <= 0:
        return {"pair": f"{y_sym}-{x_sym}", "error": "No sigma"}
    
    # Load price data
    prices_y = load_price_data(y_sym)
    prices_x = load_price_data(x_sym)
    
    if prices_y is None or prices_x is None:
        return {"pair": f"{y_sym}-{x_sym}", "error": "No data"}
    
    # Align dates
    df = pd.DataFrame({'Y': prices_y, 'X': prices_x}).dropna()
    
    if len(df) < 50:
        return {"pair": f"{y_sym}-{x_sym}", "error": "Insufficient data"}
    
    # Get lot sizes
    lot_y = get_lot_size(y_sym)
    lot_x = get_lot_size(x_sym)
    
    # Position tracking
    position = 0  # 0=flat, 1=long, -1=short
    entry_price_y = 0.0
    entry_price_x = 0.0
    entry_date = None
    holding_days = 0
    
    # Results
    trades = []
    equity = CAPITAL
    daily_equity = [CAPITAL]
    
    # Calculate beta-neutral quantities
    # For every lot_y shares of Y, need (lot_y * beta) shares of X
    qty_y = lot_y
    qty_x = max(lot_y, round(lot_y * abs(beta)))  # At least 1 lot equivalent
    
    # Main loop
    for i, (date, row) in enumerate(df.iterrows()):
        price_y = row['Y']
        price_x = row['X']
        
        # Calculate Z-score (EXACT Varsity formula)
        residual = price_y - (beta * price_x + intercept)
        z = residual / sigma
        
        # Track daily equity (MTM)
        if position != 0:
            holding_days += 1
            if position == 1:  # Long: bought Y, sold X
                mtm = (price_y - entry_price_y) * qty_y + (entry_price_x - price_x) * qty_x
            else:  # Short: sold Y, bought X
                mtm = (entry_price_y - price_y) * qty_y + (price_x - entry_price_x) * qty_x
            daily_equity.append(equity + mtm)
        else:
            daily_equity.append(equity)
        
        # Position management
        if position != 0:
            # Check exits
            take_profit = (position == 1 and z >= -Z_EXIT) or \
                         (position == -1 and z <= Z_EXIT)
            stop_loss = abs(z) >= Z_STOP
            time_exit = holding_days >= MAX_HOLD_DAYS
            
            if take_profit or stop_loss or time_exit:
                # Calculate P&L
                if position == 1:
                    pnl = (price_y - entry_price_y) * qty_y + (entry_price_x - price_x) * qty_x
                else:
                    pnl = (entry_price_y - price_y) * qty_y + (price_x - entry_price_x) * qty_x
                
                # Subtract costs
                costs = calculate_transaction_costs(price_y, price_x, qty_y, qty_x)
                pnl -= costs
                
                # Record trade
                reason = "TP" if take_profit else ("SL" if stop_loss else "TIME")
                trades.append({
                    "entry_date": entry_date.strftime("%Y-%m-%d"),
                    "exit_date": date.strftime("%Y-%m-%d"),
                    "direction": "LONG" if position == 1 else "SHORT",
                    "entry_z": entry_z,
                    "exit_z": round(z, 2),
                    "pnl": round(pnl, 2),
                    "days": holding_days,
                    "reason": reason
                })
                
                equity += pnl
                position = 0
                holding_days = 0
        
        else:
            # Check entries
            if z <= -Z_ENTRY:  # Long signal
                position = 1
                entry_price_y = price_y
                entry_price_x = price_x
                entry_date = date
                entry_z = round(z, 2)
                holding_days = 0
            
            elif z >= Z_ENTRY:  # Short signal
                position = -1
                entry_price_y = price_y
                entry_price_x = price_x
                entry_date = date
                entry_z = round(z, 2)
                holding_days = 0
    
    # Calculate metrics
    total_return = ((equity - CAPITAL) / CAPITAL) * 100
    winning = [t for t in trades if t['pnl'] > 0]
    losing = [t for t in trades if t['pnl'] <= 0]
    
    # Sharpe ratio (daily)
    if len(daily_equity) > 10:
        returns = np.diff(daily_equity) / np.array(daily_equity[:-1])
        returns = returns[~np.isnan(returns) & ~np.isinf(returns)]
        if len(returns) > 5 and np.std(returns) > 0:
            sharpe = (np.mean(returns) / np.std(returns)) * np.sqrt(252)
        else:
            sharpe = 0.0
    else:
        sharpe = 0.0
    
    # Max drawdown
    if len(daily_equity) > 1:
        eq_arr = np.array(daily_equity)
        running_max = np.maximum.accumulate(eq_arr)
        drawdown = running_max - eq_arr
        max_dd = float(np.max(drawdown))
    else:
        max_dd = 0.0
    
    # Profit factor
    gross_profit = sum(t['pnl'] for t in trades if t['pnl'] > 0)
    gross_loss = abs(sum(t['pnl'] for t in trades if t['pnl'] < 0))
    if gross_loss > 100:
        pf = min(gross_profit / gross_loss, 10.0)
    elif gross_profit > 0:
        pf = 10.0
    else:
        pf = 0.0
    
    return {
        "pair": f"{y_sym}-{x_sym}",
        "leg1": y_sym,
        "leg2": x_sym,
        "sector": sector,
        "beta": round(beta, 3),
        "sigma": round(sigma, 2),
        "return_pct": round(total_return, 2),
        "trades": len(trades),
        "win_rate": round(len(winning) / max(len(trades), 1) * 100, 1),
        "sharpe_ratio": round(sharpe, 2),
        "max_drawdown": round(max_dd, 2),
        "profit_factor": round(pf, 2),
        "avg_days": round(np.mean([t['days'] for t in trades]) if trades else 0, 1),
        "trade_log": trades
    }


def run_varsity_backtest():
    """Run simplified Varsity-style backtest on all pairs."""
    print("\n" + "="*60)
    print("🎯 VARSITY-STYLE BACKTEST (Simplified)")
    print("="*60)
    print(f"   Entry: ±{Z_ENTRY} SD | Exit: ±{Z_EXIT} SD | Stop: ±{Z_STOP} SD")
    print(f"   Capital: ₹{CAPITAL:,}")
    print(f"   Formula: Z = (Y - β×X - c) / σ (FIXED)")
    print("="*60)
    
    pairs = load_pairs()
    print(f"\n📋 Testing {len(pairs)} pairs...\n")
    
    results = []
    for i, pair in enumerate(pairs):
        y_sym = pair.get('leg1') or pair.get('stock_y')
        x_sym = pair.get('leg2') or pair.get('stock_x')
        print(f"   [{i+1}/{len(pairs)}] {y_sym}-{x_sym}...", end="")
        
        result = backtest_pair(pair)
        results.append(result)
        
        if 'error' in result:
            print(f" ❌ {result['error']}")
        else:
            print(f" ✅ {result['trades']} trades, {result['return_pct']}%")
    
    # Sort by return
    valid = [r for r in results if 'error' not in r and r['trades'] > 0]
    valid.sort(key=lambda x: x['return_pct'], reverse=True)
    
    # Display results
    print("\n" + "="*60)
    print("🏆 RESULTS")
    print("="*60)
    
    print(f"\n{'Pair':<25} {'Return':<10} {'Trades':<8} {'Win%':<8} {'Sharpe':<8}")
    print("-"*60)
    
    for r in valid:
        print(f"{r['pair']:<25} {r['return_pct']:>6.1f}%    {r['trades']:<8} {r['win_rate']:<8.0f} {r['sharpe_ratio']:<8.2f}")
    
    # Summary
    total_trades = sum(r['trades'] for r in valid)
    winners = [r for r in valid if r['return_pct'] > 0]
    
    print("\n" + "-"*60)
    print(f"Total Pairs: {len(valid)} | Winners: {len(winners)}")
    print(f"Total Trades: {total_trades}")
    if valid:
        avg_return = np.mean([r['return_pct'] for r in valid])
        print(f"Avg Return: {avg_return:.1f}%")
    
    # Save results
    output_path = os.path.join(config.ARTIFACTS_DIR, "varsity_backtest_results.json")
    with open(output_path, 'w') as f:
        json.dump(valid, f, indent=2)
    print(f"\n📁 Saved to: {output_path}")
    
    return valid


if __name__ == "__main__":
    run_varsity_backtest()
