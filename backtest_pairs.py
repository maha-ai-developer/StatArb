import os
import pandas as pd
from tabulate import tabulate
# Assuming you have a 'strategies' folder with 'pair_strategy.py'
from strategies.pair_strategy import PairStrategy 

# CONFIG
DATA_DIR = "data"
PAIRS_FILE = "pairs.txt"
TIMEFRAME = "5m"

def load_data(symbol):
    """
    Robust data loader that handles different column naming conventions (Date/date, Close/close).
    Returns a Pandas Series of closing prices indexed by date.
    """
    path = f"{DATA_DIR}/{symbol}_{TIMEFRAME}.csv"
    if not os.path.exists(path):
        # print(f"[Data] Missing file: {path}") 
        return None
        
    try:
        df = pd.read_csv(path)
        
        # 1. Normalize columns to lowercase and strip whitespace
        df.columns = [c.lower().strip() for c in df.columns]
        
        # 2. Check for required columns
        if 'date' not in df.columns or 'close' not in df.columns:
            # print(f"[Data] Invalid columns in {symbol}: {df.columns}")
            return None

        # 3. Parse Dates
        df['date'] = pd.to_datetime(df['date'])
        
        # 4. Remove duplicates (crucial for time-series alignment)
        df = df.drop_duplicates(subset=['date'])
        
        return df.set_index('date')['close']
        
    except Exception as e:
        # print(f"[Data] Read error {symbol}: {e}")
        return None

def main():
    print(f"--- ⚖️  STARTING PAIR BACKTEST (Stats Enabled) ---")
    
    # 1. Load Strategy
    # Entry 1.5 means "Enter when price deviates 1.5 standard deviations from mean"
    try:
        strategy = PairStrategy(entry_threshold=1.5, exit_threshold=0.0)
        print(f"Strategy Loaded: Entry={strategy.entry_threshold} | Exit={strategy.exit_threshold}\n")
    except Exception as e:
        print(f"❌ Error initializing strategy: {e}")
        return

    # 2. Load Pairs WITH Statistics
    if not os.path.exists(PAIRS_FILE):
        print(f"❌ {PAIRS_FILE} not found. Run scan_pairs.py first.")
        return

    pairs_data = []
    print(f"[Config] Reading {PAIRS_FILE} for symbols and stats...")
    
    with open(PAIRS_FILE, "r") as f:
        lines = f.readlines()
        # Skip header if it exists (assuming header contains 'symbol_a')
        start_idx = 1 if "symbol_a" in lines[0].lower() else 0
        
        for line in lines[start_idx:]:
            parts = line.strip().split(',')
            # Expecting at least 5 parts: A, B, mean, std, p_value
            if len(parts) >= 5: 
                try:
                    pairs_data.append({
                        'a': parts[0].strip(),
                        'b': parts[1].strip(),
                        'mean': float(parts[2]), # <-- CRITICAL: Extract mean
                        'std': float(parts[3]),  # <-- CRITICAL: Extract std
                        'p_value': float(parts[4])
                    })
                except ValueError:
                    # Skips lines that are malformed or have non-numeric stats
                    continue 
            elif len(parts) >= 2:
                 # Fallback for old/minimal pairs file (will use dynamic stats)
                pairs_data.append({
                    'a': parts[0].strip(),
                    'b': parts[1].strip(),
                    'mean': None,
                    'std': None
                })


    print(f"[Config] Found {len(pairs_data)} pairs. Starting Backtest...\n")
    results = []

    # 3. Execution Loop
    for i, p in enumerate(pairs_data):
        sym_a = p['a']
        sym_b = p['b']
        
        # Visual Progress
        print(f"\r[Testing] {i+1:02}/{len(pairs_data)}: {sym_a} vs {sym_b:<12}", end="", flush=True)
        
        s1 = load_data(sym_a)
        s2 = load_data(sym_b)
        
        # Skip if data missing
        if s1 is None or s2 is None:
            continue
            
        # Skip if dates don't overlap sufficiently (e.g., less than 200 bars)
        common = s1.index.intersection(s2.index)
        if len(common) < 200:
            continue

        try:
            # RUN STRATEGY
            # FIX: Pass the pre-calculated mean and std (if available)
            # You must update strategies/pair_strategy.py to accept fixed_mean/fixed_std
            stats = strategy.run_backtest(
                s1, s2,
                fixed_mean=p['mean'],  # <-- Pass the pre-calculated mean
                fixed_std=p['std']     # <-- Pass the pre-calculated std
            )
            
            # Only record profitable/trading pairs
            if stats and stats['trades'] > 0:
                results.append({
                    'Pair': f"{sym_a}-{sym_b}",
                    'PnL': round(stats['total_pnl'], 2),
                    'Trades': stats['trades'],
                    'Win Rate': round(stats.get('win_rate', 0.0), 2),
                    'Sharpe': round(stats.get('sharpe', 0.0), 2),
                    'P-Value': round(p.get('p_value', 0.0), 8),
                })
                
        except Exception:
            # Fail silently to keep the loop moving fast
            pass

    print("\n\n" + "="*70)
    
    # 4. Final Report
    if results:
        df = pd.DataFrame(results)
        df = df.sort_values(by="PnL", ascending=False)
        
        # Print top 20 pairs
        print(tabulate(df.head(20), headers='keys', tablefmt='simple', showindex=False))
        
        print("="*70)
        print(f"💰 TOTAL PORTFOLIO PnL: ₹ {df['PnL'].sum():,.2f}")
        print(f"🎲 TOTAL TRADES:      {df['Trades'].sum()}")
        print(f"🏆 BEST PAIR:         {df.iloc[0]['Pair']} (PnL: ₹ {df.iloc[0]['PnL']})")
        print("="*70)
        
        df.to_csv("backtest_results_pairs.csv", index=False)
        print(f"[Saved] Report saved to 'backtest_results_pairs.csv'")
    else:
        print("❌ No trades generated.")
        print("   Tip: Check that 5m data files exist and the PairStrategy logic is correct.")

if __name__ == "__main__":
    main()
