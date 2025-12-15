import os
import pandas as pd
from tabulate import tabulate
from backtest.engine import run_backtest

# CONFIG
DATA_DIR = "data"
SYMBOLS_FILE = "symbols.txt"
TIMEFRAME = "5m"
INITIAL_CAPITAL = 100000  # Default mock capital used by the engine

def main():
    print(f"--- 📉 STARTING MOMENTUM BACKTEST ---")
    
    if not os.path.exists(SYMBOLS_FILE):
        print(f"❌ Error: {SYMBOLS_FILE} not found. Run scan_momentum.py first.")
        return

    with open(SYMBOLS_FILE, "r") as f:
        target_symbols = [line.strip().upper() for line in f if line.strip()]

    print(f"[Config] Target List: {SYMBOLS_FILE}")
    print(f"[Config] Found {len(target_symbols)} symbols to test.")
    print(f"[Config] Initial Capital: ₹{INITIAL_CAPITAL}\n")

    results = []
    
    # 2. Iterate ONLY through the Target List
    for i, symbol in enumerate(target_symbols):
        csv_path = f"{DATA_DIR}/{symbol}_{TIMEFRAME}.csv"
        
        # Visual Progress
        print(f"\r[Testing] {i+1:02}/{len(target_symbols)}: {symbol:<12}", end="", flush=True)
        
        if not os.path.exists(csv_path):
            continue

        try:
            stats = run_backtest(
                csv_path=csv_path,
                symbol=symbol,
                exchange="NSE",
                timeframe=TIMEFRAME
            )
            
            if stats:
                # FIX: Calculate PnL manually if missing
                if 'total_pnl' not in stats:
                    if 'final_equity' in stats:
                        stats['total_pnl'] = stats['final_equity'] - INITIAL_CAPITAL
                    else:
                        stats['total_pnl'] = 0.0
                
                # Ensure Symbol is present
                if 'symbol' not in stats:
                    stats['symbol'] = symbol
                    
                results.append(stats)
                
        except Exception:
            pass

    print("\n\n" + "-"*60)
    
    # 3. Final Report
    if results:
        df = pd.DataFrame(results)
        
        # Select columns to display
        display_cols = ['symbol', 'total_pnl', 'trades', 'win_rate', 'return_pct']
        
        # Filter strictly for columns that exist to prevent crashing
        available_cols = [c for c in display_cols if c in df.columns]
        
        # PRINT TABLE
        print(tabulate(df[available_cols], headers='keys', tablefmt='grid', showindex=False))
        
        print("-" * 60)
        
        total_pnl = df['total_pnl'].sum()
        total_trades = df['trades'].sum() if 'trades' in df.columns else 0
        avg_win = df['win_rate'].mean() if 'win_rate' in df.columns else 0.0
        
        print(f"💰 TOTAL PORTFOLIO PnL:   ₹ {total_pnl:,.2f}")
        print(f"📈 AVERAGE WIN RATE:      {avg_win:.2f}%")
        print(f"🎲 TOTAL TRADES:          {total_trades}")
        print("-" * 60)
        
        df.to_csv("backtest_results_momentum.csv", index=False)
        print(f"[Saved] Detailed report to 'backtest_results_momentum.csv'")
    else:
        print("❌ No trades generated.")

if __name__ == "__main__":
    main()
