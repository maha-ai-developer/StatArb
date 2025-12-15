import pandas as pd
import os
from tabulate import tabulate

# CONFIG
TOP_N = 10  # <--- CHANGE THIS to 5, 10, or 20 as needed

def main():
    input_csv = "backtest_results_pairs.csv"
    output_txt = "live_pairs.txt"
    
    print(f"--- 🕵️  SELECTING TOP {TOP_N} PAIRS ---")
    
    if not os.path.exists(input_csv):
        print(f"❌ Error: {input_csv} not found.")
        return

    # 1. Load & Sort
    df = pd.read_csv(input_csv)
    winners = df[df['PnL'] > 0].copy()
    winners = winners.sort_values(by="PnL", ascending=False)
    
    if winners.empty:
        print("❌ No winning pairs found.")
        return

    # 2. Slice the Top N
    top_picks = winners.head(TOP_N)

    # 3. Save
    with open(output_txt, "w") as f:
        for _, row in top_picks.iterrows():
            f.write(f"{row['Symbol A']},{row['Symbol B']}\n")
            
    # 4. Show Result
    print(tabulate(top_picks[['Symbol A', 'Symbol B', 'PnL', 'Trades']], 
                   headers='keys', tablefmt='simple', showindex=False))
                   
    print(f"\n✅ SAVED ONLY THE TOP {TOP_N} TO {output_txt}")

if __name__ == "__main__":
    main()
