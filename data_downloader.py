# data_downloader.py

import argparse
import os
import time
import pandas as pd
from datetime import datetime, timedelta
from broker.kite_auth import get_kite
from core.instrument_cache import get_instrument_token

# Maps user-friendly intervals to Kite API constants
INTERVAL_MAP = {
    "1m": "minute", "minute": "minute",
    "3m": "3minute", "5m": "5minute",
    "10m": "10minute", "15m": "15minute",
    "30m": "30minute", "60m": "60minute",
    "1h": "60minute", "1d": "day", "day": "day",
}

# Chunk sizes to avoid API limits (Day count per request)
CHUNK_SIZES = {
    "minute": 30, "3minute": 90, "5minute": 90, 
    "10minute": 90, "15minute": 180, "30minute": 180, 
    "60minute": 360, "day": 2000
}

def load_symbols_from_file(filepath):
    """
    Reads symbols from a text file. 
    Handles both Single lists (Momentum) and CSV-style Pair lists.
    """
    symbols = []
    if not os.path.exists(filepath):
        print(f"[Warn] File not found: {filepath}")
        return []
        
    with open(filepath, "r") as f:
        for line in f:
            line = line.strip()
            if not line: continue
            
            # --- FILTER: Skip Header Rows from Pairs.txt ---
            line_lower = line.lower()
            if "symbol_a" in line_lower or "mean" in line_lower or "p_value" in line_lower:
                continue
            # -----------------------------------------------

            # Handle Pair File (StockA,StockB,...)
            if "," in line: 
                parts = line.split(",")
                s1 = parts[0].strip()
                s2 = parts[1].strip()
                if s1: symbols.append(s1)
                if s2: symbols.append(s2)
            else: 
                # Handle Momentum/Single File
                symbols.append(line)
                
    return list(set(symbols)) # Remove duplicates

def fetch_in_chunks(kite, token, from_dt, to_dt, interval):
    """
    Downloads data in safe chunks to bypass 100-day API limits.
    """
    all_records = []
    current_start = from_dt
    chunk_days = CHUNK_SIZES.get(interval, 30)

    while current_start < to_dt:
        current_end = current_start + timedelta(days=chunk_days)
        if current_end > to_dt:
            current_end = to_dt
        
        try:
            batch = kite.historical_data(
                instrument_token=token,
                from_date=current_start,
                to_date=current_end,
                interval=interval
            )
            if batch:
                all_records.extend(batch)
            time.sleep(0.2) # Small buffer between chunks
        except Exception as e:
            print(f"    [Error] Chunk failed ({current_start.date()}): {e}")
        
        current_start = current_end

    return all_records

def fetch_history(kite, symbol, from_date, to_date, interval, output_folder="data", prefix=""):
    """
    Main worker function to download and save CSV.
    """
    api_interval = INTERVAL_MAP.get(interval.lower(), interval)
    token = get_instrument_token(symbol)
    
    # Visual: Print Start
    print(f"{prefix} Fetching {symbol:<12} ... ", end="", flush=True)
    
    if not token:
        print("❌ Token not found")
        return False

    if not os.path.exists(output_folder):
        os.makedirs(output_folder)

    filename = os.path.join(output_folder, f"{symbol}_{interval}.csv")
    
    try:
        req_from_dt = datetime.strptime(from_date, "%Y-%m-%d")
        req_to_dt = datetime.strptime(to_date, "%Y-%m-%d")
    except ValueError:
        print("❌ Invalid Date")
        return False
    
    current_from_dt = req_from_dt
    mode = 'w'
    
    # Check for existing data to Append instead of Overwrite
    if os.path.exists(filename):
        try:
            existing_df = pd.read_csv(filename)
            if not existing_df.empty and 'date' in existing_df.columns:
                last_str = existing_df.iloc[-1]['date']
                last_dt = pd.to_datetime(last_str)
                if last_dt.tzinfo is not None:
                    last_dt = last_dt.tz_localize(None)
                last_dt = last_dt.to_pydatetime()

                if last_dt >= req_to_dt:
                    print(f"✅ Up to date")
                    return True
                
                current_from_dt = last_dt + timedelta(days=1)
                if current_from_dt < req_from_dt: current_from_dt = req_from_dt
                mode = 'a'
        except:
            pass # If file corrupt, overwrite it

    # Safety check if dates flipped
    if current_from_dt >= req_to_dt + timedelta(days=1):
        print(f"✅ Up to date")
        return True

    # DOWNLOAD
    records = fetch_in_chunks(kite, token, current_from_dt, req_to_dt, api_interval)

    if not records:
        print("⚠️ No new data")
        return True

    # SAVE
    new_df = pd.DataFrame(records)
    if 'date' in new_df.columns:
        new_df['date'] = pd.to_datetime(new_df['date'])
        if new_df['date'].dt.tz is not None:
             new_df['date'] = new_df['date'].dt.tz_localize(None)

    if mode == 'a' and os.path.exists(filename):
        old_df = pd.read_csv(filename)
        old_df['date'] = pd.to_datetime(old_df['date'])
        if old_df['date'].dt.tz is not None:
             old_df['date'] = old_df['date'].dt.tz_localize(None)

        combined_df = pd.concat([old_df, new_df])
        combined_df.drop_duplicates(subset=['date'], keep='last', inplace=True)
        combined_df.sort_values(by='date', inplace=True)
        combined_df.to_csv(filename, index=False)
        print(f"✅ Appended ({len(new_df)} rows)")
    else:
        new_df.drop_duplicates(subset=['date'], keep='last', inplace=True)
        new_df.to_csv(filename, index=False)
        print(f"✅ Saved ({len(new_df)} rows)")
    
    return True

def download_bulk(symbols_file: str, from_date: str, to_date: str, interval: str):
    # 1. Load Symbols
    symbols = load_symbols_from_file(symbols_file)
    total = len(symbols)
    
    if total == 0:
        print("❌ No symbols found.")
        return

    print(f"\n--- 📥 BATCH DOWNLOAD: {total} Symbols ---")
    
    # 2. Connect
    try:
        kite = get_kite()
    except Exception as e:
        print(f"❌ Auth Failed: {e}")
        return

    # 3. Loop with Counter
    success_count = 0
    for i, sym in enumerate(symbols):
        # Format: [01/39]
        prefix = f"[{i+1:02}/{total}]"
        
        ok = fetch_history(kite, sym, from_date, to_date, interval, prefix=prefix)
        if ok:
            success_count += 1
        
        time.sleep(0.1) # Tiny rate limit

    print(f"\n--- ✅ COMPLETE: {success_count}/{total} Processed ---")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Bulk Data Downloader")
    parser.add_argument("--symbols", default="symbols.txt", help="Path to symbols file")
    parser.add_argument("--from-date", required=True, help="YYYY-MM-DD")
    parser.add_argument("--to-date", required=True, help="YYYY-MM-DD")
    parser.add_argument("--interval", default="5m", help="5m, 15m, 1d")
    
    args = parser.parse_args()
    
    download_bulk(args.symbols, args.from_date, args.to_date, args.interval)
