# live/warmup.py
import pandas as pd
from datetime import datetime, timedelta
from broker.kite_auth import get_kite

def fetch_warmup_data(symbols, timeframe="5minute", lookback_days=5):
    """
    Fetches historical data for the given symbols to 'prime' the strategy.
    Returns a dictionary: { 'SBIN': [Candle1, Candle2...], ... }
    """
    print(f"\n[Warmup] Connecting to Kite to fetch {lookback_days} days of history...")
    
    try:
        kite = get_kite()
    except Exception as e:
        print(f"[Error] Could not connect to Kite for warmup: {e}")
        return {}

    # 1. Get Instrument Tokens (Required for History API)
    # We fetch ALL NSE instruments to map Symbol -> Token
    print("[Warmup] Fetching Instrument Tokens...")
    instruments = kite.instruments("NSE")
    token_map = {i['tradingsymbol']: i['instrument_token'] for i in instruments}

    warmup_data = {}
    from_date = datetime.now() - timedelta(days=lookback_days)
    to_date = datetime.now()
    
    # Kite API expects "5minute" string, not "5m"
    interval_map = {
        "1m": "minute",
        "3m": "3minute",
        "5m": "5minute",
        "15m": "15minute",
        "30m": "30minute",
        "60m": "60minute"
    }
    kite_interval = interval_map.get(timeframe, "5minute")

    # 2. Loop through symbols and fetch history
    print(f"[Warmup] Downloading data for {len(symbols)} symbols...")
    
    for symbol in symbols:
        token = token_map.get(symbol)
        if not token:
            print(f"[Warn] Token not found for {symbol}. Skipping warmup.")
            continue
            
        try:
            # Fetch Data
            records = kite.historical_data(
                instrument_token=token, 
                from_date=from_date, 
                to_date=to_date, 
                interval=kite_interval
            )
            
            # If we got data, store it
            if records:
                warmup_data[symbol] = records
                # print(f"  -> {symbol}: Loaded {len(records)} candles.")
                
        except Exception as e:
            print(f"[Error] Failed to fetch history for {symbol}: {e}")

    print(f"[Warmup] Complete. Loaded history for {len(warmup_data)} symbols.\n")
    return warmup_data
