"""
Post-Trade Analytics Module - Checklist Phase 10

Performance metrics for completed trades:
- Win rate calculation
- Average profit per winning trade
- Average loss per losing trade
- Profit factor
- Maximum drawdown
- Sharpe ratio (if applicable)
"""

import sqlite3
import pandas as pd
import numpy as np
import os
import sys
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import infrastructure.config as config


class TradeAnalytics:
    """Post-trade analysis for pairs trading strategy."""
    
    def __init__(self, db_path: Optional[str] = None):
        self.db_path = db_path or os.path.join(config.DATA_DIR, "trades.db")
    
    def get_trades(self, days_back: int = 30) -> pd.DataFrame:
        """Load trades from database."""
        if not os.path.exists(self.db_path):
            print("❌ No trade database found.")
            return pd.DataFrame()
        
        conn = sqlite3.connect(self.db_path)
        
        # Get trades from last N days
        cutoff = (datetime.now() - timedelta(days=days_back)).strftime('%Y-%m-%d')
        query = f"""
            SELECT timestamp, symbol, side, quantity, price, strategy, mode
            FROM trades
            WHERE date(timestamp) >= '{cutoff}'
            ORDER BY timestamp
        """
        
        df = pd.read_sql_query(query, conn)
        conn.close()
        
        if not df.empty:
            df['timestamp'] = pd.to_datetime(df['timestamp'])
        
        return df
    
    def calculate_pair_pnl(self, df: pd.DataFrame) -> List[Dict]:
        """
        Calculate P&L for each pair trade.
        
        Pairs trades are matched by pairing BUY/SELL for the same symbol.
        """
        if df.empty:
            return []
        
        trades = []
        
        # Group by symbol to find matched trades
        for symbol in df['symbol'].unique():
            sym_df = df[df['symbol'] == symbol].sort_values('timestamp')
            
            buys = sym_df[sym_df['side'] == 'BUY'].reset_index(drop=True)
            sells = sym_df[sym_df['side'] == 'SELL'].reset_index(drop=True)
            
            # Match buys and sells
            for i in range(min(len(buys), len(sells))):
                entry = buys.iloc[i]
                exit = sells.iloc[i]
                
                # Calculate P&L
                pnl = (exit['price'] - entry['price']) * entry['quantity']
                pct_return = ((exit['price'] - entry['price']) / entry['price']) * 100
                
                # Calculate holding time (handle intraday and ensure non-negative)
                time_diff = exit['timestamp'] - entry['timestamp']
                holding_days = max(0, time_diff.total_seconds() / 86400)  # Convert to days, min 0
                
                trade_record = {
                    'symbol': symbol,
                    'entry_time': entry['timestamp'],
                    'exit_time': exit['timestamp'],
                    'entry_price': entry['price'],
                    'exit_price': exit['price'],
                    'quantity': entry['quantity'],
                    'pnl': round(pnl, 2),
                    'pct_return': round(pct_return, 2),
                    'holding_days': round(holding_days, 2),  # Round to 2 decimal places
                    'strategy': entry['strategy'],
                    'mode': entry['mode']
                }
                trades.append(trade_record)
        
        return trades
    
    def calculate_metrics(self, trades: List[Dict]) -> Dict:
        """
        Calculate performance metrics.
        
        Returns metrics per Phase 10 checklist:
        - Win rate
        - Average profit/loss
        - Profit factor
        - Maximum drawdown
        """
        if not trades:
            return {}
        
        df = pd.DataFrame(trades)
        
        # Separate winners and losers
        winners = df[df['pnl'] > 0]
        losers = df[df['pnl'] <= 0]
        
        # Win rate
        total_trades = len(df)
        win_count = len(winners)
        loss_count = len(losers)
        win_rate = (win_count / total_trades) * 100 if total_trades > 0 else 0
        
        # Average profit/loss
        avg_profit = winners['pnl'].mean() if len(winners) > 0 else 0
        avg_loss = abs(losers['pnl'].mean()) if len(losers) > 0 else 0
        
        # Profit factor = Gross Profit / Gross Loss
        gross_profit = winners['pnl'].sum() if len(winners) > 0 else 0
        gross_loss = abs(losers['pnl'].sum()) if len(losers) > 0 else 0.01  # Avoid div by 0
        profit_factor = gross_profit / gross_loss
        
        # Net P&L
        net_pnl = df['pnl'].sum()
        
        # Returns for risk metrics
        returns = df['pct_return'].values
        
        # Sharpe Ratio (annualized, assuming ~252 trading days)
        if len(returns) > 1 and returns.std() > 0:
            sharpe = (returns.mean() / returns.std()) * np.sqrt(252 / max(1, len(returns)))
        else:
            sharpe = 0
        
        # Maximum Drawdown
        cumulative = np.cumsum(df['pnl'].values)
        running_max = np.maximum.accumulate(cumulative)
        drawdown = running_max - cumulative
        max_drawdown = np.max(drawdown) if len(drawdown) > 0 else 0
        
        # Average holding period
        avg_holding = df['holding_days'].mean() if 'holding_days' in df.columns else 0
        
        return {
            'total_trades': total_trades,
            'winning_trades': win_count,
            'losing_trades': loss_count,
            'win_rate': round(win_rate, 2),
            'avg_profit': round(avg_profit, 2),
            'avg_loss': round(avg_loss, 2),
            'gross_profit': round(gross_profit, 2),
            'gross_loss': round(gross_loss, 2),
            'net_pnl': round(net_pnl, 2),
            'profit_factor': round(profit_factor, 2),
            'sharpe_ratio': round(sharpe, 2),
            'max_drawdown': round(max_drawdown, 2),
            'avg_holding_days': round(avg_holding, 1)
        }
    
    def generate_report(self, days_back: int = 30) -> Dict:
        """
        Generate comprehensive trade analytics report.
        """
        print(f"\n📊 --- TRADE ANALYTICS REPORT ---")
        print(f"   Period: Last {days_back} days")
        print(f"   Date: {datetime.now().strftime('%Y-%m-%d %H:%M')}\n")
        
        # Load trades
        df = self.get_trades(days_back)
        
        if df.empty:
            print("💤 No trades found in the specified period.")
            return {}
        
        print(f"📋 Found {len(df)} trade records\n")
        
        # Calculate P&L
        trades = self.calculate_pair_pnl(df)
        
        if not trades:
            print("❌ No matched trades found.")
            return {}
        
        # Calculate metrics
        metrics = self.calculate_metrics(trades)
        
        # Display results
        print("=" * 50)
        print("📈 PERFORMANCE SUMMARY")
        print("=" * 50)
        print(f"   Total Trades:     {metrics['total_trades']}")
        print(f"   Winners:          {metrics['winning_trades']}")
        print(f"   Losers:           {metrics['losing_trades']}")
        print(f"   Win Rate:         {metrics['win_rate']}%")
        print()
        print(f"   Avg Profit:       ₹{metrics['avg_profit']:,.2f}")
        print(f"   Avg Loss:         ₹{metrics['avg_loss']:,.2f}")
        print(f"   Profit Factor:    {metrics['profit_factor']:.2f}")
        print()
        print(f"   Gross Profit:     ₹{metrics['gross_profit']:,.2f}")
        print(f"   Gross Loss:       ₹{metrics['gross_loss']:,.2f}")
        print(f"   Net P&L:          ₹{metrics['net_pnl']:,.2f}")
        print()
        print(f"   Sharpe Ratio:     {metrics['sharpe_ratio']:.2f}")
        print(f"   Max Drawdown:     ₹{metrics['max_drawdown']:,.2f}")
        print(f"   Avg Holding:      {metrics['avg_holding_days']:.1f} days")
        print("=" * 50)
        
        # Verdict
        if metrics['profit_factor'] > 1.5 and metrics['win_rate'] > 50:
            print("\n🚀 VERDICT: Strong Strategy Performance")
        elif metrics['profit_factor'] > 1.0:
            print("\n➡️ VERDICT: Profitable but Room for Improvement")
        else:
            print("\n🔻 VERDICT: Strategy Needs Refinement")
        
        # Save detailed trades to CSV
        trades_df = pd.DataFrame(trades)
        output_path = os.path.join(config.ARTIFACTS_DIR, "trade_analytics.csv")
        trades_df.to_csv(output_path, index=False)
        print(f"\n📁 Detailed trades saved to: {output_path}")
        
        return metrics


def generate_trade_analytics(days_back: int = 30):
    """Generate trade analytics report."""
    analytics = TradeAnalytics()
    return analytics.generate_report(days_back)


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('--days', type=int, default=30, help="Days to analyze")
    args = parser.parse_args()
    
    generate_trade_analytics(args.days)
