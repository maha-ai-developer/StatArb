#!/usr/bin/env python3
"""
Proper Train/Test Split Backtest (Zerodha Varsity Compliant)

This backtest properly separates:
1. TRAINING (60%): Calibrate β, σ, c from regression
2. TESTING (40%): Trade using fixed parameters from training

This eliminates look-ahead bias present in the previous backtest.
"""

import pandas as pd
import numpy as np
import os
import sys
import json
from datetime import datetime
from tabulate import tabulate
from typing import Dict, List, Tuple, Optional
from scipy import stats

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import infrastructure.config as config
from infrastructure.data.futures_utils import get_lot_size

# ============================================================
# CONFIGURATION (Zerodha Varsity Compliant)
# ============================================================

# Train/Test Split
TRAIN_PCT = 0.60       # 60% for calibration
TEST_PCT = 0.40        # 40% for testing

# Trading Parameters (per Varsity Page 47)
Z_ENTRY = 2.5          # Entry at ±2.5 SD
Z_EXIT = 1.0           # Exit at ±1.0 SD
Z_STOP = 3.0           # Stop loss at ±3.0 SD
MAX_HOLD_DAYS = 30     # Maximum holding period

# Capital
CAPITAL = 500000       # ₹5 lakh

# Transaction Costs
SLIPPAGE_PCT = 0.001   # 0.1% slippage
BROKERAGE = 40         # ₹40 per round trip

# Quality Filters (per AI analysis)
MIN_R_SQUARED = 0.64   # R² > 0.64 = Correlation > 0.8


def calibrate_on_training(prices_y: np.ndarray, prices_x: np.ndarray) -> Dict:
    """
    Calibrate regression parameters on training data only.
    
    Returns: beta, intercept, sigma (all from training period)
    """
    # Linear regression: Y = β*X + c + ε
    slope, intercept, r_value, p_value, std_err = stats.linregress(prices_x, prices_y)
    
    # Calculate residuals on training data
    residuals = prices_y - (slope * prices_x + intercept)
    
    # Fixed sigma from training period
    sigma = np.std(residuals)
    
    return {
        'beta': slope,
        'intercept': intercept,
        'sigma': sigma,
        'r_squared': r_value ** 2,
    }


def run_backtest_on_test(prices_y: np.ndarray, prices_x: np.ndarray,
                         beta: float, intercept: float, sigma: float,
                         lot_y: int, lot_x: int) -> Dict:
    """
    Run backtest on test period using FIXED parameters from training.
    
    This is the proper out-of-sample test.
    """
    n = len(prices_y)
    
    # State
    position = 0  # 0=flat, 1=long, -1=short
    entry_y, entry_x = 0.0, 0.0
    holding_days = 0
    equity = CAPITAL
    
    # Tracking
    trades = []
    daily_pnl = []
    
    for i in range(n):
        y = prices_y[i]
        x = prices_x[i]
        
        # Calculate Z-score with FIXED parameters
        residual = y - (beta * x + intercept)
        z = residual / sigma if sigma > 0 else 0
        
        if position != 0:
            holding_days += 1
            
            # MTM P&L
            if position == 1:  # Long spread
                pnl_y = (y - entry_y) * lot_y
                pnl_x = (entry_x - x) * lot_x
            else:  # Short spread
                pnl_y = (entry_y - y) * lot_y
                pnl_x = (x - entry_x) * lot_x
            
            mtm_pnl = pnl_y + pnl_x
            
            # Exit conditions
            take_profit = (position == 1 and z > -Z_EXIT) or (position == -1 and z < Z_EXIT)
            stop_loss = abs(z) > Z_STOP
            time_stop = holding_days >= MAX_HOLD_DAYS
            
            if take_profit or stop_loss or time_stop:
                # Apply costs
                turnover = (y * lot_y + x * lot_x)
                costs = turnover * SLIPPAGE_PCT + BROKERAGE
                net_pnl = mtm_pnl - costs
                
                equity += net_pnl
                
                trades.append({
                    'entry_day': i - holding_days,
                    'exit_day': i,
                    'holding_days': holding_days,
                    'pnl': net_pnl,
                    'exit_type': 'TP' if take_profit else ('SL' if stop_loss else 'TIME'),
                    'exit_z': z
                })
                
                position = 0
                holding_days = 0
        
        # Entry (only if flat)
        if position == 0:
            if z < -Z_ENTRY:
                # Long spread: Buy Y, Sell X
                position = 1
                entry_y, entry_x = y, x
                # Entry costs
                turnover = y * lot_y + x * lot_x
                equity -= turnover * SLIPPAGE_PCT + BROKERAGE / 2
                
            elif z > Z_ENTRY:
                # Short spread: Sell Y, Buy X
                position = -1
                entry_y, entry_x = y, x
                turnover = y * lot_y + x * lot_x
                equity -= turnover * SLIPPAGE_PCT + BROKERAGE / 2
        
        daily_pnl.append(equity)
    
    # Calculate metrics
    if len(trades) > 0:
        winning = [t for t in trades if t['pnl'] > 0]
        losing = [t for t in trades if t['pnl'] <= 0]
        
        total_pnl = sum(t['pnl'] for t in trades)
        win_rate = len(winning) / len(trades) * 100
        avg_pnl = total_pnl / len(trades)
        avg_hold = np.mean([t['holding_days'] for t in trades])
        
        # Profit factor
        gross_profit = sum(t['pnl'] for t in winning)
        gross_loss = abs(sum(t['pnl'] for t in losing))
        profit_factor = gross_profit / gross_loss if gross_loss > 0 else 10.0
        
        # Sharpe ratio
        if len(daily_pnl) > 10:
            returns = np.diff(daily_pnl) / np.array(daily_pnl[:-1])
            returns = returns[~np.isnan(returns) & ~np.isinf(returns)]
            if len(returns) > 5 and np.std(returns) > 0:
                sharpe = np.mean(returns) / np.std(returns) * np.sqrt(252)
            else:
                sharpe = 0
        else:
            sharpe = 0
        
        # Max drawdown
        equity_arr = np.array(daily_pnl)
        running_max = np.maximum.accumulate(equity_arr)
        drawdown = running_max - equity_arr
        max_dd = np.max(drawdown)
        max_dd_pct = max_dd / CAPITAL * 100
    else:
        total_pnl = 0
        win_rate = 0
        avg_pnl = 0
        avg_hold = 0
        profit_factor = 0
        sharpe = 0
        max_dd = 0
        max_dd_pct = 0
    
    return {
        'trades': len(trades),
        'winning': len([t for t in trades if t['pnl'] > 0]),
        'losing': len([t for t in trades if t['pnl'] <= 0]),
        'total_pnl': total_pnl,
        'return_pct': (equity - CAPITAL) / CAPITAL * 100,
        'win_rate': win_rate,
        'avg_pnl': avg_pnl,
        'avg_holding_days': avg_hold,
        'profit_factor': min(profit_factor, 10.0),
        'sharpe_ratio': sharpe,
        'max_drawdown': max_dd,
        'max_drawdown_pct': max_dd_pct,
        'trade_log': trades
    }


def run_split_backtest(pair: Dict) -> Dict:
    """
    Run proper train/test split backtest for a single pair.
    """
    y_sym = pair['leg1']
    x_sym = pair['leg2']
    
    # Load data
    y_file = os.path.join(config.DATA_DIR, f"{y_sym}_day.csv")
    x_file = os.path.join(config.DATA_DIR, f"{x_sym}_day.csv")
    
    if not os.path.exists(y_file) or not os.path.exists(x_file):
        return {'pair': f"{y_sym}-{x_sym}", 'error': 'Data not found'}
    
    df_y = pd.read_csv(y_file)
    df_x = pd.read_csv(x_file)
    
    # Merge on date
    df_y['date'] = pd.to_datetime(df_y['date'])
    df_x['date'] = pd.to_datetime(df_x['date'])
    df = pd.merge(df_y[['date', 'close']], df_x[['date', 'close']], 
                  on='date', suffixes=('_y', '_x'))
    df = df.sort_values('date')
    
    if len(df) < 100:
        return {'pair': f"{y_sym}-{x_sym}", 'error': f'Insufficient data ({len(df)} days)'}
    
    # Split data
    split_idx = int(len(df) * TRAIN_PCT)
    
    train_y = df['close_y'].iloc[:split_idx].values
    train_x = df['close_x'].iloc[:split_idx].values
    test_y = df['close_y'].iloc[split_idx:].values
    test_x = df['close_x'].iloc[split_idx:].values
    
    train_dates = df['date'].iloc[:split_idx]
    test_dates = df['date'].iloc[split_idx:]
    
    # STEP 1: Calibrate on training data
    params = calibrate_on_training(train_y, train_x)
    
    # STEP 1.5: Filter by training-period R² (CRITICAL per AI analysis)
    if params['r_squared'] < MIN_R_SQUARED:
        return {
            'pair': f"{y_sym}-{x_sym}",
            'error': f"Training R²={params['r_squared']:.3f} < {MIN_R_SQUARED}",
            'r_squared': params['r_squared']
        }
    
    # STEP 2: Backtest on test data with fixed parameters
    lot_y = get_lot_size(y_sym)
    lot_x = get_lot_size(x_sym)
    
    results = run_backtest_on_test(
        test_y, test_x,
        params['beta'], params['intercept'], params['sigma'],
        lot_y, lot_x
    )
    
    # Calculate Z-score distribution on test data
    test_residuals = test_y - (params['beta'] * test_x + params['intercept'])
    test_z = test_residuals / params['sigma']
    
    return {
        'pair': f"{y_sym}-{x_sym}",
        'leg1': y_sym,
        'leg2': x_sym,
        'sector': pair.get('sector', 'UNKNOWN'),
        # Training info
        'train_days': len(train_y),
        'train_start': str(train_dates.iloc[0].date()),
        'train_end': str(train_dates.iloc[-1].date()),
        'beta': round(params['beta'], 4),
        'intercept': round(params['intercept'], 2),
        'sigma': round(params['sigma'], 2),
        'r_squared': round(params['r_squared'], 3),
        # Testing info
        'test_days': len(test_y),
        'test_start': str(test_dates.iloc[0].date()),
        'test_end': str(test_dates.iloc[-1].date()),
        'test_z_min': round(test_z.min(), 2),
        'test_z_max': round(test_z.max(), 2),
        'test_z_entries': int((np.abs(test_z) > Z_ENTRY).sum()),
        # Results (OUT-OF-SAMPLE)
        'trades': results['trades'],
        'winning': results['winning'],
        'losing': results['losing'],
        'return_pct': round(results['return_pct'], 2),
        'win_rate': round(results['win_rate'], 1),
        'avg_pnl': round(results['avg_pnl'], 0),
        'avg_holding_days': round(results['avg_holding_days'], 1),
        'profit_factor': round(results['profit_factor'], 2),
        'sharpe_ratio': round(results['sharpe_ratio'], 2),
        'max_drawdown_pct': round(results['max_drawdown_pct'], 2),
        'lot_size_y': lot_y,
        'lot_size_x': lot_x
    }


def main():
    """Run proper train/test split backtest on all pairs."""
    print("=" * 70)
    print("📊 TRAIN/TEST SPLIT BACKTEST (Varsity Compliant)")
    print("=" * 70)
    print(f"   📈 Training: {TRAIN_PCT*100:.0f}% | Testing: {TEST_PCT*100:.0f}%")
    print(f"   📐 Z Thresholds: Entry ±{Z_ENTRY}, Exit ±{Z_EXIT}, Stop ±{Z_STOP}")
    print(f"   💰 Capital: ₹{CAPITAL:,}")
    print("=" * 70)
    
    # Load pairs
    if not os.path.exists(config.PAIRS_CANDIDATES_FILE):
        print(f"\n❌ No pairs found. Run: python cli.py scan_pairs")
        return
    
    with open(config.PAIRS_CANDIDATES_FILE) as f:
        pairs = json.load(f)
    
    print(f"\n💼 Testing {len(pairs)} pairs with proper train/test split...\n")
    
    results = []
    for i, pair in enumerate(pairs):
        y_sym = pair['leg1']
        x_sym = pair['leg2']
        print(f"   [{i+1}/{len(pairs)}] {y_sym}-{x_sym}...", end=" ", flush=True)
        
        result = run_split_backtest(pair)
        results.append(result)
        
        if 'error' in result:
            print(f"⚠️ {result['error']}")
        else:
            print(f"✅ Trades: {result['trades']}, Return: {result['return_pct']:+.1f}%")
    
    # Convert to DataFrame
    df = pd.DataFrame(results)
    valid = df[~df['trades'].isna() & (df.get('error').isna() if 'error' in df.columns else True)]
    
    if len(valid) == 0:
        print("\n❌ No valid results")
        return
    
    # Show results
    print(f"\n{'='*70}")
    print("🏆 OUT-OF-SAMPLE RESULTS (Testing Period Only)")
    print(f"{'='*70}")
    
    display_cols = ['pair', 'return_pct', 'win_rate', 'trades', 'sharpe_ratio', 'profit_factor', 'test_z_entries']
    display_df = valid[display_cols].sort_values('return_pct', ascending=False)
    
    print(tabulate(
        display_df,
        headers=['Pair', 'Return %', 'Win %', 'Trades', 'Sharpe', 'PF', 'Z Entries'],
        tablefmt='simple_grid',
        floatfmt='.1f'
    ))
    
    # Summary stats
    print(f"\n📊 SUMMARY:")
    print(f"   Pairs with trades: {(valid['trades'] > 0).sum()} / {len(valid)}")
    print(f"   Total trades: {valid['trades'].sum():.0f}")
    print(f"   Avg return: {valid['return_pct'].mean():.1f}%")
    print(f"   Avg Sharpe: {valid['sharpe_ratio'].mean():.2f}")
    
    # Show train/test periods
    if len(valid) > 0:
        sample = valid.iloc[0]
        print(f"\n📅 DATA SPLIT:")
        print(f"   Training: {sample['train_start']} to {sample['train_end']} ({sample['train_days']} days)")
        print(f"   Testing:  {sample['test_start']} to {sample['test_end']} ({sample['test_days']} days)")
    
    # Save results
    output_file = os.path.join(config.ARTIFACTS_DIR, "backtest_split_results.json")
    with open(output_file, 'w') as f:
        json.dump(results, f, indent=2)
    
    print(f"\n📁 Results saved to: {output_file}")
    
    # Save winners for live trading
    winners = valid[valid['trades'] > 0].sort_values('return_pct', ascending=False)
    if len(winners) > 0:
        live_config = []
        for _, row in winners.head(10).iterrows():
            live_config.append({
                'leg1': row['leg1'],
                'leg2': row['leg2'],
                'beta': row['beta'],
                'intercept': row['intercept'],
                'sigma': row['sigma'],
                'lot_size_y': row['lot_size_y'],
                'lot_size_x': row['lot_size_x'],
                'sector': row['sector'],
                'backtest_return': row['return_pct'],
                'backtest_trades': int(row['trades']),
                'backtest_sharpe': row['sharpe_ratio'],
            })
        
        with open(config.PAIRS_CONFIG, 'w') as f:
            json.dump(live_config, f, indent=2)
        
        print(f"✅ Saved {len(live_config)} pairs to {config.PAIRS_CONFIG}")


if __name__ == "__main__":
    main()
