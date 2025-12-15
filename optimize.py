import pandas as pd
import pandas_ta_classic as ta
import itertools
import json
import os
import sys

# CONFIGURATION
DATA_DIR = "data"
SYMBOLS_FILE = "symbols.txt"
OUTPUT_FILE = "strategies/momentum_config.json"

# SEARCH SPACE
ema_range = [10, 20, 50]
rsi_range = [50, 55, 60, 65]

def optimize_symbol(symbol):
    csv_path = os.path.join(DATA_DIR, f"{symbol}_5m.csv")
    
    if not os.path.exists(csv_path):
        return None

    try:
        df = pd.read_csv(csv_path)
    except Exception:
        return None

    best_pnl = -float('inf')
    best_params = {"ema": 20, "rsi": 55}

    combinations = list(itertools.product(ema_range, rsi_range))
    total_combos = len(combinations)

    # VISUAL: Print Start
    print(f"\n🔍 Optimizing {symbol} ({len(df)} candles)...")

    for idx, (ema_len, rsi_buy) in enumerate(combinations):
        # VISUAL: Show progress inline
        sys.stdout.write(f"\r    Testing Combo {idx+1}/{total_combos}: EMA {ema_len} | RSI {rsi_buy}   ")
        sys.stdout.flush()

        # 1. Calculate Indicators
        df['ema'] = ta.ema(df['close'], length=ema_len)
        df['rsi'] = ta.rsi(df['close'], length=14)
        
        capital = 100000
        position = 0
        
        # 2. Run Fast Simulation
        for i in range(50, len(df)):
            close = df.iloc[i]['close']
            ema = df.iloc[i]['ema']
            rsi = df.iloc[i]['rsi']
            
            # BUY LOGIC
            if position == 0 and close > ema and rsi > rsi_buy:
                position = int(capital / close)
                capital -= position * close
            
            # SELL LOGIC
            elif position > 0 and close < ema:
                capital += position * close
                position = 0
        
        # 3. Result
        final_value = capital + (position * df.iloc[-1]['close'])
        pnl = final_value - 100000
        
        if pnl > best_pnl:
            best_pnl = pnl
            best_params = {"ema": ema_len, "rsi": rsi_buy}

    # VISUAL: Clear line and show result
    sys.stdout.write(f"\r    ✅ RESULT: Best PnL ₹{best_pnl:.2f} (EMA {best_params['ema']}, RSI {best_params['rsi']})    \n")
    return best_params

# MAIN EXECUTION
if __name__ == "__main__":
    if not os.path.exists(SYMBOLS_FILE):
        print("❌ symbols.txt not found!")
        exit()

    with open(SYMBOLS_FILE, "r") as f:
        symbols = [s.strip().upper() for s in f.readlines() if s.strip()]

    print(f"--- 🚀 Optimizing {len(symbols)} Stocks ---")
    
    final_config = {}
    
    for sym in symbols:
        result = optimize_symbol(sym)
        if result:
            final_config[sym] = result

    os.makedirs("strategies", exist_ok=True)
    with open(OUTPUT_FILE, "w") as f:
        json.dump(final_config, f, indent=4)
        
    print(f"\n✨ Optimization Complete! Config saved to {OUTPUT_FILE}")
