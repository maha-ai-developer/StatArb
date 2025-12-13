# batch_backtest.py

import os
import glob
import pandas as pd
from tabulate import tabulate
from backtest.engine import run_backtest

def run_batch():
    data_folder = "data"
    
    # 1. Find all CSVs
    pattern = os.path.join(data_folder, "*_*.csv")
    csv_files = glob.glob(pattern)
    
    total_files = len(csv_files)
    if total_files == 0:
        print(f"[Error] No CSV files found in {data_folder}/")
        return

    print(f"[Batch] Found {total_files} files. Starting backtest...\n")
    
    results = []
    
    # 2. Loop through files with a Counter
    for i, csv_path in enumerate(csv_files, 1):
        filename = os.path.basename(csv_path)
        # Extract symbol (e.g. "SBIN_5m.csv" -> "SBIN")
        symbol = filename.split("_")[0]
        
        # Print Progress: [ 1/50 ] Testing SBIN...
        print(f"[{i}/{total_files}] Testing {symbol:<10} ...", end="", flush=True)
        
        try:
            # Run the backtest
            res = run_backtest(csv_path=csv_path, initial_capital=100000)
            
            # Store data if valid
            if res:
                results.append({
                    "Symbol": symbol,
                    "Final Equity": res.get("final_equity", 0.0),
                    "Return %": res.get("return_pct", 0.0),
                    "Trades": res.get("trades", 0),
                    "Win Rate": res.get("win_rate", 0.0)
                })
                print(" Done.")  # Print Done on the same line
            else:
                print(" Skipped (No Data).")
                
        except Exception as e:
            print(f" Error: {e}")

    # 3. Save & Show Results
    if results:
        df_res = pd.DataFrame(results)
        # Sort by highest profit
        df_res = df_res.sort_values("Return %", ascending=False)
        
        output_file = "backtest_results.csv"
        df_res.to_csv(output_file, index=False)
        
        print(f"\n[Success] Processed {len(results)} stocks. Results saved to {output_file}")
        
        # Show Top 10 Leaderboard
        print("\nTOP 10 PERFORMERS:")
        print(tabulate(df_res.head(10), headers="keys", tablefmt="grid", showindex=False))
    else:
        print("\n[Warning] No results generated.")

if __name__ == "__main__":
    run_batch()