# auto_trader.py

import pandas as pd
import subprocess
import os
import sys

# Configuration
INPUT_RESULTS = "backtest_results.csv"
OUTPUT_LIST = "live_universe.txt"
TOP_N = 10
MIN_WIN_RATE = 40.0  # Min 40% win rate required
MIN_TRADES = 3       # Min 3 trades required

def main():
    # ---------------------------------------------------------
    # 1. Run the Batch Backtest
    # ---------------------------------------------------------
    print("[AutoTrader] Starting Batch Backtest...")
    try:
        # Run batch_backtest.py using the same python interpreter
        subprocess.run([sys.executable, "batch_backtest.py"], check=True)
    except subprocess.CalledProcessError:
        print("[Error] Backtest failed. Aborting.")
        return

    # ---------------------------------------------------------
    # 2. Filter & Select Top Stocks
    # ---------------------------------------------------------
    if not os.path.exists(INPUT_RESULTS):
        print(f"[Error] {INPUT_RESULTS} not found.")
        return

    df = pd.read_csv(INPUT_RESULTS)
    print(f"\n[AutoTrader] Analyzing {len(df)} stocks...")

    # FILTER LOGIC:
    winners = df[
        (df["Return %"] > 0) & 
        (df["Win Rate"] >= MIN_WIN_RATE) & 
        (df["Trades"] >= MIN_TRADES)
    ]

    # Sort by highest return
    top_picks = winners.sort_values("Return %", ascending=False).head(TOP_N)
    
    if top_picks.empty:
        print("[AutoTrader] No stocks met the criteria!")
        return

    selected_symbols = top_picks["Symbol"].tolist()
    print(f"[AutoTrader] Selected Top {len(selected_symbols)} Stocks:")
    print(top_picks[["Symbol", "Return %", "Win Rate", "Trades"]].to_string(index=False))

    # ---------------------------------------------------------
    # 3. Save to live_universe.txt
    # ---------------------------------------------------------
    with open(OUTPUT_LIST, "w") as f:
        for sym in selected_symbols:
            f.write(f"{sym}\n")
    print(f"[AutoTrader] Saved symbols to {OUTPUT_LIST}")

    # ---------------------------------------------------------
    # 4. Launch Live Engine
    # ---------------------------------------------------------
    print("\n[AutoTrader] Launching Live Engine with selected stocks...")
    
    # Run the engine command
    cmd = [
        sys.executable, "cli.py", "engine",
        "--symbols-file", OUTPUT_LIST,
        "--timeframe", "5m",
        "--place-order"  # <--- WARNING: This places REAL orders. Remove if testing.
    ]
    
    try:
        subprocess.run(cmd)
    except KeyboardInterrupt:
        print("\n[AutoTrader] Engine stopped by user.")

if __name__ == "__main__":
    main()
