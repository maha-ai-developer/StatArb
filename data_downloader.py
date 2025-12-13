# data_downloader.py

import argparse
import os
import time
import pandas as pd
from datetime import datetime, timedelta
from broker.kite_auth import get_kite
from core.instrument_cache import get_instrument_token

INTERVAL_MAP = {
    "1m": "minute", "minute": "minute",
    "3m": "3minute", "5m": "5minute",
    "10m": "10minute", "15m": "15minute",
    "30m": "30minute", "60m": "60minute",
    "1h": "60minute", "1d": "day", "day": "day",
}

CHUNK_SIZES = {
    "minute": 30, "3minute": 90, "5minute": 90, 
    "10minute": 90, "15minute": 180, "30minute": 180, 
    "60minute": 360, "day": 2000
}

def fetch_in_chunks(kite, token, from_dt, to_dt, interval):
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
            time.sleep(0.4) 
        except Exception as e:
            print(f"    [Error] Chunk failed ({current_start.date()} - {current_end.date()}): {e}")
        
        current_start = current_end

    return all_records

def fetch_history(symbol: str, from_date: str, to_date: str, interval: str, output_folder: str = "data"):
    kite = get_kite()
    api_interval = INTERVAL_MAP.get(interval.lower(), interval)
    token = get_instrument_token(symbol)
    
    if not token:
        print(f"[Error] Token not found for {symbol}")
        return False

    if not os.path.exists(output_folder):
        os.makedirs(output_folder)

    filename = os.path.join(output_folder, f"{symbol}_{interval}.csv")
    
    # 1. Parse CLI dates (Naive)
    try:
        req_from_dt = datetime.strptime(from_date, "%Y-%m-%d")
        req_to_dt = datetime.strptime(to_date, "%Y-%m-%d")
    except ValueError:
        print(f"[Error] Invalid date format for {symbol}")
        return False
    
    current_from_dt = req_from_dt
    mode = 'w'
    
    # 2. Check Existing File
    if os.path.exists(filename):
        try:
            existing_df = pd.read_csv(filename)
            if not existing_df.empty and 'date' in existing_df.columns:
                last_str = existing_df.iloc[-1]['date']
                
                # CRITICAL FIX: Ensure last_dt is naive (remove timezone)
                last_dt = pd.to_datetime(last_str)
                if last_dt.tzinfo is not None:
                    last_dt = last_dt.tz_localize(None) # Remove timezone info
                
                last_dt = last_dt.to_pydatetime()

                # Check if up to date
                if last_dt >= req_to_dt:
                    # print(f"[Skip] {symbol} is up to date.")
                    return True
                
                # Start next day
                current_from_dt = last_dt + timedelta(days=1)
                
                if current_from_dt < req_from_dt:
                    current_from_dt = req_from_dt 
                
                mode = 'a'
                print(f"[Update] {symbol}: Appending from {current_from_dt.date()}...")
        except Exception as e:
            print(f"[Warn] File corrupt for {symbol}, re-downloading. Error: {e}")

    # 3. Safety Check (Now comparing naive vs naive)
    if current_from_dt >= req_to_dt + timedelta(days=1):
        return True

    # 4. Fetch
    records = fetch_in_chunks(kite, token, current_from_dt, req_to_dt, api_interval)

    if not records:
        return True

    # 5. Save & Clean
    new_df = pd.DataFrame(records)
    
    # Clean new data timezones too
    if 'date' in new_df.columns:
        new_df['date'] = pd.to_datetime(new_df['date'])
        # If Zerodha sends TZ-aware, make it naive
        if new_df['date'].dt.tz is not None:
             new_df['date'] = new_df['date'].dt.tz_localize(None)

    if mode == 'a' and os.path.exists(filename):
        old_df = pd.read_csv(filename)
        # Ensure old data is parsed correctly as datetime
        old_df['date'] = pd.to_datetime(old_df['date'])
        if old_df['date'].dt.tz is not None:
             old_df['date'] = old_df['date'].dt.tz_localize(None)

        combined_df = pd.concat([old_df, new_df])
        combined_df.drop_duplicates(subset=['date'], keep='last', inplace=True)
        combined_df.sort_values(by='date', inplace=True)
        combined_df.to_csv(filename, index=False)
        action = "Merged"
        count = len(combined_df)
    else:
        new_df.drop_duplicates(subset=['date'], keep='last', inplace=True)
        new_df.to_csv(filename, index=False)
        action = "Saved"
        count = len(new_df)
    
    print(f"[Success] {symbol}: {action} {count} rows.")
    return True

def download_bulk(symbols_file: str, from_date: str, to_date: str, interval: str):
    if not os.path.exists(symbols_file):
        print(f"[Error] Symbols file not found: {symbols_file}")
        return

    print(f"[Batch] Reading symbols from {symbols_file}...")
    with open(symbols_file, "r") as f:
        symbols = [line.strip().upper() for line in f if line.strip()]

    print(f"[Batch] Processing {len(symbols)} symbols...")
    
    success_count = 0
    for sym in symbols:
        if sym in ["SYMBOL", "TICKER", "NIFTY 50", "SENSEX"]:
            continue
            
        ok = fetch_history(sym, from_date, to_date, interval, output_folder="data")
        if ok:
            success_count += 1
        
        time.sleep(0.1)

    print(f"\n[Batch] Completed. {success_count}/{len(symbols)} processed.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Bulk Data Downloader")
    parser.add_argument("--symbols", default="symbols.txt", help="Path to symbols file")
    parser.add_argument("--from-date", required=True, help="Start Date (YYYY-MM-DD)")
    parser.add_argument("--to-date", required=True, help="End Date (YYYY-MM-DD)")
    parser.add_argument("--interval", default="5m", help="Interval (5m, 15m, 1d)")
    
    args = parser.parse_args()
    
    download_bulk(args.symbols, args.from_date, args.to_date, args.interval)
