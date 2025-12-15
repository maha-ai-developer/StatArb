# scan_momentum.py

import argparse
import pandas as pd
from datetime import datetime, timedelta
import time
import sys

# Import your existing Core modules
from broker.kite_auth import get_kite
from core.instrument_cache import get_instrument_token
from core.universe import UniverseBuilder

# CONFIG
OUTPUT_FILE = "symbols.txt"
LOOKBACK_DAYS = 365  # 1 Year Momentum
TOP_N = 10           # Portfolio Size

def get_yearly_return(kite, symbol):
    """
    Fetches daily data for 1 year and calculates % return.
    """
    token = get_instrument_token(symbol)
    if not token:
        return None

    to_date = datetime.now()
    from_date = to_date - timedelta(days=LOOKBACK_DAYS)

    try:
        # Fetch Daily Data (Fast & Efficient)
        records = kite.historical_data(
            instrument_token=token,
            from_date=from_date,
            to_date=to_date,
            interval="day"
        )
        
        if not records or len(records) < 200: # Ensure enough data
            return None

        # Calculate Momentum: (Close_Now - Close_1Yr_Ago) / Close_1Yr_Ago
        start_price = records[0]['close']
        end_price = records[-1]['close']
        
        if start_price == 0: return 0.0
        
        pct_return = ((end_price - start_price) / start_price) * 100
        return round(pct_return, 2)

    except Exception:
        # Silently ignore errors (like delisted stocks) to keep terminal clean
        return None

def main(csv_path):
    print(f"\n--- 🚀 STARTING MOMENTUM SCAN ---")
    print(f"Input Universe: {csv_path}")
    print(f"Strategy: Top {TOP_N} stocks by {LOOKBACK_DAYS}-day return\n")

    # 1. Connect to Kite
    try:
        kite = get_kite()
        print("[Auth] Connected to Zerodha.")
    except Exception as e:
        print(f"[Error] Auth failed: {e}")
        return

    # 2. Load Universe (NIFTY 500)
    ub = UniverseBuilder()
    try:
        full_list = ub.load_csv_universe(csv_path)
        print(f"[Universe] Loaded {len(full_list)} symbols to scan.\n")
    except Exception as e:
        print(f"[Error] Could not load universe: {e}")
        return

    # 3. Scan Loop with Visual Counter
    momentum_scores = []
    total_symbols = len(full_list)
    
    print("-" * 60)
    for i, symbol in enumerate(full_list):
        # Visual Update
        print(f"\r[Scan] {i+1:03}/{total_symbols} : Checking {symbol:<12} ", end="", flush=True)
        
        ret = get_yearly_return(kite, symbol)
        
        if ret is not None:
            momentum_scores.append({
                "symbol": symbol,
                "return": ret
            })
        
        # Rate limit safety (Avoid 429 Errors)
        time.sleep(0.1)

    print("\n" + "-" * 60)
    print("[Scan] Analysis Complete.")

    # 4. Rank & Filter
    df = pd.DataFrame(momentum_scores)
    
    if df.empty:
        print("❌ No data found for any symbols.")
        return

    # Sort descending
    df = df.sort_values(by="return", ascending=False)
    top_stocks = df.head(TOP_N)
    
    # --- NEW: TABULATE DISPLAY ---
    from tabulate import tabulate
    print(f"\n--- 🏆 MOMENTUM PORTFOLIO (Top {TOP_N}) ---")
    print(tabulate(top_stocks, headers='keys', tablefmt='psql', showindex=False))
    # -----------------------------

    # 5. Save to symbols.txt
    selected_symbols = top_stocks['symbol'].tolist()
    ub.save_symbols(selected_symbols, OUTPUT_FILE)
    
    print(f"\n✅ Saved to {OUTPUT_FILE}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Momentum Scanner")
    parser.add_argument("csv_file", help="Path to NIFTY 500 or Universe CSV file")
    args = parser.parse_args()
    
    main(args.csv_file)
