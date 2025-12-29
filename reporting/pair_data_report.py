"""
Pair Data Report Generator - Checklist Phase 3

Generates structured pair data reports with:
- Y Stock (Dependent variable)
- X Stock (Independent variable)
- Intercept value
- Beta value
- ADF value
- Sigma (Standard Error of residuals)
- Current Z-Score
"""

import pandas as pd
import numpy as np
import json
import os
import sys
from datetime import datetime
from typing import Dict, List, Optional

import statsmodels.api as sm
from statsmodels.tsa.stattools import adfuller

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import infrastructure.config as config


def calculate_pair_metrics(y_prices: pd.Series, x_prices: pd.Series, 
                           hedge_ratio: float, intercept: float) -> Dict:
    """
    Calculate all metrics for a single pair.
    
    Returns:
        Dict with sigma, residual, z_score, adf_value
    """
    if len(y_prices) < 60 or len(x_prices) < 60:
        return None
    
    # Calculate residuals: Y - (beta * X + intercept)
    residuals = y_prices - (hedge_ratio * x_prices + intercept)
    
    # Sigma = Standard Error of Residuals
    sigma = residuals.std()
    
    # Current Residual (latest)
    current_residual = residuals.iloc[-1] if len(residuals) > 0 else 0
    
    # Z-Score = Current Residual / Sigma
    z_score = current_residual / sigma if sigma > 0 else 0
    
    # ADF Test on residuals
    try:
        adf_result = adfuller(residuals.dropna(), maxlag=1)
        adf_pvalue = adf_result[1]
    except:
        adf_pvalue = 1.0  # Assume non-stationary on error
    
    return {
        "sigma": round(sigma, 4),
        "current_residual": round(current_residual, 4),
        "z_score": round(z_score, 4),
        "adf_pvalue": round(adf_pvalue, 4),
        "is_stationary": adf_pvalue < 0.05
    }


def generate_pair_report(output_csv: Optional[str] = None) -> pd.DataFrame:
    """
    Generate pair data report for all configured pairs.
    
    Creates a structured report with:
    - Y Stock, X Stock
    - Intercept, Beta
    - ADF Value, Sigma
    - Current Z-Score
    
    Returns:
        DataFrame with pair metrics
    """
    print(f"\n📊 --- PAIR DATA REPORT: {datetime.now().strftime('%Y-%m-%d %H:%M')} ---\n")
    
    # Load pair configuration
    if not os.path.exists(config.PAIRS_CONFIG):
        print("❌ No pairs_config.json found.")
        return pd.DataFrame()
    
    with open(config.PAIRS_CONFIG, 'r') as f:
        pairs = json.load(f)
    
    print(f"📋 Analyzing {len(pairs)} configured pairs...\n")
    
    # Load price data
    from infrastructure.broker.kite_auth import get_kite
    from infrastructure.data.cache import DataCache
    
    try:
        kite = get_kite()
        cache = DataCache(kite)
        
        # Get instrument tokens
        instruments = kite.instruments("NSE")
        token_map = {i['tradingsymbol']: i['instrument_token'] for i in instruments}
        cache.set_tokens(token_map)
        
        # Collect all symbols
        all_symbols = set()
        for p in pairs:
            all_symbols.add(p['leg1'])
            all_symbols.add(p['leg2'])
        
        # Fetch data in parallel
        price_data = cache.parallel_fetch(list(all_symbols), interval="day")
        
    except Exception as e:
        print(f"⚠️ Could not fetch live data: {e}")
        print("   Using cached data from files...")
        price_data = {}
        for p in pairs:
            for sym in [p['leg1'], p['leg2']]:
                path = os.path.join(config.DATA_DIR, f"{sym}_day.csv")
                if os.path.exists(path):
                    df = pd.read_csv(path)
                    df['date'] = pd.to_datetime(df['date'])
                    df.set_index('date', inplace=True)
                    price_data[sym] = df['close']
    
    # Generate report
    report_data = []
    
    for p in pairs:
        y_sym = p['leg1']
        x_sym = p['leg2']
        hedge_ratio = p['hedge_ratio']
        intercept = p['intercept']
        
        y_prices = price_data.get(y_sym, pd.Series())
        x_prices = price_data.get(x_sym, pd.Series())
        
        # Align data
        combined = pd.concat([y_prices, x_prices], axis=1).dropna()
        if len(combined) < 60:
            print(f"   ⚠️ {y_sym}-{x_sym}: Insufficient data")
            continue
        
        y_aligned = combined.iloc[:, 0]
        x_aligned = combined.iloc[:, 1]
        
        # Calculate metrics
        metrics = calculate_pair_metrics(y_aligned, x_aligned, hedge_ratio, intercept)
        
        if metrics:
            row = {
                "Y_Stock": y_sym,
                "X_Stock": x_sym,
                "Sector": p.get('sector', ''),
                "Beta": hedge_ratio,
                "Intercept": intercept,
                "ADF_PValue": metrics['adf_pvalue'],
                "Stationary": "✅" if metrics['is_stationary'] else "❌",
                "Sigma": metrics['sigma'],
                "Z_Score": metrics['z_score'],
                "Signal": _get_signal(metrics['z_score'])
            }
            report_data.append(row)
            
            # Print status
            status = "🟢" if metrics['is_stationary'] else "🔴"
            print(f"   {status} {y_sym:>10} - {x_sym:<10} | Z: {metrics['z_score']:>6.2f} | ADF: {metrics['adf_pvalue']:.3f}")
    
    # Create DataFrame
    df_report = pd.DataFrame(report_data)
    
    if df_report.empty:
        print("\n❌ No pairs with sufficient data.")
        return df_report
    
    # Save to CSV
    output_path = output_csv or os.path.join(config.ARTIFACTS_DIR, "pair_data_report.csv")
    df_report.to_csv(output_path, index=False)
    print(f"\n✅ Report saved to: {output_path}")
    
    # Summary
    print(f"\n📈 Summary:")
    print(f"   Total Pairs: {len(df_report)}")
    print(f"   Stationary: {(df_report['Stationary'] == '✅').sum()}")
    print(f"   Non-Stationary: {(df_report['Stationary'] == '❌').sum()}")
    
    entry_signals = df_report[df_report['Signal'].isin(['LONG_SPREAD', 'SHORT_SPREAD'])]
    if len(entry_signals) > 0:
        print(f"\n🎯 Entry Signals Today:")
        for _, row in entry_signals.iterrows():
            print(f"   {row['Signal']}: {row['Y_Stock']}-{row['X_Stock']} (Z={row['Z_Score']:.2f})")
    
    return df_report


def _get_signal(z_score: float) -> str:
    """Determine trading signal from Z-Score."""
    if z_score <= -2.0:
        return "LONG_SPREAD"
    elif z_score >= 2.0:
        return "SHORT_SPREAD"
    elif abs(z_score) <= 1.0:
        return "EXIT"
    else:
        return "WAIT"


if __name__ == "__main__":
    generate_pair_report()
