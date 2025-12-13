# backtest/engine.py
from typing import Dict
import pandas as pd
from tabulate import tabulate

from strategies.combined_stack import compute_signals
from core.indicators import compute_all_indicators

def simple_backtest(df: pd.DataFrame, initial_capital: float = 100000.0) -> Dict:
    """
    Executes trades based on the 'signal' column.
    """
    position = 0
    entry_price = 0.0
    equity = initial_capital
    
    # Statistics
    trades_count = 0
    wins = 0
    
    for ts, row in df.iterrows():
        signal = row.get("signal", "HOLD")
        price = float(row["close"])

        # BUY Logic (Long Entry)
        if signal == "BUY" and position == 0:
            qty = int(equity // price)
            if qty > 0:
                position = qty
                entry_price = price
                equity -= position * price

        # SELL Logic (Long Exit)
        elif signal == "SELL" and position > 0:
            revenue = position * price
            profit = revenue - (position * entry_price)
            equity += revenue
            
            trades_count += 1
            if profit > 0: wins += 1
            
            position = 0
            entry_price = 0.0

    # Mark to market final position
    if position > 0:
        last_price = float(df.iloc[-1]["close"])
        equity += position * last_price

    ret_pct = (equity / initial_capital - 1) * 100
    win_rate = (wins / trades_count * 100) if trades_count > 0 else 0.0

    results = [
        ["Initial Capital", initial_capital],
        ["Final Equity", f"{equity:.2f}"],
        ["Return %", f"{ret_pct:.2f}%"],
        ["Total Trades", trades_count],
        ["Win Rate", f"{win_rate:.1f}%"]
    ]

    # Optional: Comment out print if you want silent batch runs
    # print("\n" + tabulate(results, headers=["Metric", "Value"], tablefmt="grid"))
    
    return {
        "final_equity": equity, 
        "return_pct": ret_pct,
        "trades": trades_count,
        "win_rate": win_rate
    }


def run_backtest(csv_path: str, initial_capital: float = 100000.0, **kwargs) -> Dict:
    """
    Main entry point. Accepts extra args (symbol, exchange) via kwargs.
    """
    try:
        df = pd.read_csv(csv_path)
    except FileNotFoundError:
        print(f"[Error] File not found: {csv_path}")
        return {}

    # 1. Normalize columns
    df.rename(columns={c: c.lower().strip() for c in df.columns}, inplace=True)
    
    # 2. Parse Dates
    if "date" in df.columns:
        df["date"] = pd.to_datetime(df["date"])
        df.set_index("date", inplace=True)
    elif "timestamp" in df.columns:
        df["timestamp"] = pd.to_datetime(df["timestamp"])
        df.set_index("timestamp", inplace=True)

    # 3. Check columns
    required = {"open", "high", "low", "close", "volume"}
    if not required.issubset(df.columns):
        print(f"[Error] CSV missing columns. Found: {df.columns.tolist()}")
        return {}

    # 4. Generate Signals (Rolling)
    df = compute_signals(df)

    # 5. Run Execution Simulation
    return simple_backtest(df, initial_capital)
