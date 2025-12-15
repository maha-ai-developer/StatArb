import pandas as pd
import statsmodels.api as sm
from statsmodels.tsa.stattools import adfuller
from datetime import datetime, timedelta
import time
import os
import argparse

# Import Core modules
from broker.kite_auth import get_kite
from core.instrument_cache import get_instrument_token
from core.universe import UniverseBuilder

# --- CONFIG ---
OUTPUT_FILE = "pairs.txt"
DEBUG_DATA_DIR = "debug_daily_data"
LOOKBACK_DAYS = 365   # Increased to 1 year for stability
P_VALUE_THRESHOLD = 0.02 # Stricter (was 0.05)
MIN_CORRELATION = 0.80   # Stocks must move together generally

def get_price_series(kite, symbol):
    """Fetches daily closing prices."""
    token = get_instrument_token(symbol)
    if not token: return None
    
    to_date = datetime.now()
    from_date = to_date - timedelta(days=LOOKBACK_DAYS)
    
    try:
        records = kite.historical_data(token, from_date, to_date, "day")
        if not records or len(records) < 150: return None
        
        df = pd.DataFrame(records)
        
        # Save for debug
        if not os.path.exists(DEBUG_DATA_DIR): os.makedirs(DEBUG_DATA_DIR)
        df.to_csv(f"{DEBUG_DATA_DIR}/{symbol}_daily.csv", index=False)
        
        return df.set_index('date')['close']
    except:
        return None

def check_cointegration(series_a, series_b):
    """
    Checks Correlation FIRST, then Cointegration.
    """
    # 1. Align Dates
    df = pd.DataFrame({'A': series_a, 'B': series_b}).dropna()
    if df.empty or len(df) < 150: return False, 1.0, 0, 0
    
    # 2. Check Correlation (The Logic Filter)
    # If they don't move together generally, don't pair them.
    corr = df['A'].corr(df['B'])
    if corr < MIN_CORRELATION:
        return False, 1.0, 0, 0

    # 3. Check Cointegration (The Math Filter)
    df['ratio'] = df['A'] / df['B']
    try:
        adf_result = adfuller(df['ratio'])
        p_value = adf_result[1]
        mean = df['ratio'].mean()
        std = df['ratio'].std()
        
        return p_value < P_VALUE_THRESHOLD, p_value, mean, std
    except:
        return False, 1.0, 0, 0

def main(csv_path, limit=None):
    print(f"\n--- 🐯 STARTING PRO PAIR SCAN ---")
    print(f"Universe: {csv_path}")
    print(f"Settings: P-Val < {P_VALUE_THRESHOLD} | Corr > {MIN_CORRELATION}\n")
    
    # 1. Load Universe
    ub = UniverseBuilder()
    try:
        symbols = ub.load_csv_universe(csv_path)
        
        # Optional Limit via CLI
        if limit:
            print(f"[Info] Limit applied: Scanning first {limit} symbols only.")
            symbols = symbols[:int(limit)]
        else:
            print(f"[Info] Scanning ALL {len(symbols)} symbols. (This will take time)")
            
    except Exception as e:
        print(f"[Error] Universe load failed: {e}")
        return
    
    kite = get_kite()
    data_cache = {}
    
    # 2. Download Data
    print(f"[Data] Downloading history...")
    for i, sym in enumerate(symbols):
        # Overwrite line to keep terminal clean
        print(f"\r    Fetching {i+1}/{len(symbols)}: {sym:<12}", end="", flush=True)
        series = get_price_series(kite, sym)
        if series is not None:
            data_cache[sym] = series
        time.sleep(0.1) # Respect API limits
    print("\n[Data] Download Complete.\n")

    # 3. Scan Pairs
    valid_pairs = []
    keys = list(data_cache.keys())
    n = len(keys)
    total_combinations = (n * (n - 1)) // 2
    scan_count = 0
    
    print(f"[Scan] Analyzing {total_combinations} combinations...")
    
    for i in range(len(keys)):
        for j in range(i + 1, len(keys)):
            scan_count += 1
            sym_a = keys[i]
            sym_b = keys[j]
            
            # Print status every 100 checks to reduce spam
            if scan_count % 100 == 0 or scan_count == total_combinations:
                print(f"\r    Progress: {scan_count}/{total_combinations} ({int(scan_count/total_combinations*100)}%) | Found: {len(valid_pairs)}", end="", flush=True)
            
            series_a = data_cache[sym_a]
            series_b = data_cache[sym_b]
            
            is_coint, p_val, mean, std = check_cointegration(series_a, series_b)
            
            if is_coint:
                valid_pairs.append({
                    "symbol_a": sym_a,
                    "symbol_b": sym_b,
                    "mean": mean,
                    "std": std,
                    "p_value": p_val
                })

    print(f"\n\n--- SCAN FINISHED ---")
    
    # 4. Save Results
    if valid_pairs:
        df = pd.DataFrame(valid_pairs)
        df = df.sort_values(by="p_value")
        df.to_csv(OUTPUT_FILE, index=False)
        print(f"✅ SUCCESS: Saved {len(valid_pairs)} pairs to {OUTPUT_FILE}")
        print(df.head(10))
    else:
        print("❌ RESULT: No valid pairs found.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("csv_file", help="Path to Universe CSV")
    parser.add_argument("--limit", help="Limit number of stocks to scan (e.g. 100)", default=None)
    args = parser.parse_args()
    main(args.csv_file, args.limit)
