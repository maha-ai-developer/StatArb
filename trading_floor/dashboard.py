"""
Live Dashboard - Zerodha Varsity Style Position Tracker

Provides real-time monitoring of pair trades with:
- Z-score tracking using FIXED sigma
- P&L per leg and total
- Entry/Exit signals based on thresholds
- Time-series logging

Usage:
    python trading_floor/dashboard.py
"""

import os
import sys
import json
import time
from datetime import datetime
from typing import Dict, List, Optional
from tabulate import tabulate

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import infrastructure.config as config
from trading_floor.position_tracker import PositionTracker, PairConfig


class LiveDashboard:
    """
    Live trading dashboard with Varsity-style position tracking.
    
    Features:
    - Real-time Z-score display
    - P&L tracking per leg
    - Signal alerts (entry/exit/stop)
    - Multi-pair monitoring
    """
    
    def __init__(self, capital: float = 500000):
        self.capital = capital
        self.trackers: Dict[str, PositionTracker] = {}
        self.active_positions: Dict[str, dict] = {}
        self.trade_history: List[dict] = []
        self._load_pairs_config()
        self._load_active_trades()
    
    def _load_pairs_config(self):
        """Load pair configurations from JSON."""
        # Prefer candidates file (has sigma), fallback to config
        config_file = config.PAIRS_CANDIDATES_FILE
        if not os.path.exists(config_file):
            config_file = config.PAIRS_CONFIG
        
        if not os.path.exists(config_file):
            print("❌ No pairs config found. Run scan_pairs first.")
            return
        
        with open(config_file, 'r') as f:
            pairs = json.load(f)
        
        # Import lot size utility
        from infrastructure.data.futures_utils import get_lot_size
        
        for p in pairs[:10]:  # Limit to top 10 pairs
            stock_y = p.get('leg1') or p.get('stock_y')
            stock_x = p.get('leg2') or p.get('stock_x')
            pair_key = f"{stock_y}-{stock_x}"
            
            # Get sigma - try multiple field names
            sigma = p.get('sigma', 0) or p.get('std_err', 0) or p.get('residual_std_dev', 0)
            
            pair_config = PairConfig(
                stock_y=stock_y,
                stock_x=stock_x,
                sector=p.get('sector', 'UNKNOWN'),
                beta=p.get('beta') or p.get('hedge_ratio', 1.0),
                intercept=p.get('intercept', 0.0),
                sigma=sigma,
                lot_size_y=p.get('lot_size_y', get_lot_size(stock_y)),
                lot_size_x=p.get('lot_size_x', get_lot_size(stock_x)),
                adf_value=p.get('adf_pvalue') or p.get('adf', 0.0)
            )
            
            self.trackers[pair_key] = PositionTracker(pair_config)
    
    def _load_active_trades(self):
        """Load any existing active trades from state."""
        state_file = os.path.join(config.DATA_DIR, "engine_state.json")
        if os.path.exists(state_file):
            try:
                with open(state_file, 'r') as f:
                    state = json.load(f)
                self.active_positions = state.get('active_trades', {})
                
                # Restore position states to trackers
                for pair_key, trade in self.active_positions.items():
                    if pair_key in self.trackers:
                        tracker = self.trackers[pair_key]
                        tracker.position.is_open = True
                        tracker.position.position_type = trade.get('side', 'LONG')
                        tracker.position.entry_price_y = trade.get('entry_price_y', 0.0)
                        tracker.position.entry_price_x = trade.get('entry_price_x', 0.0)
                        tracker.position.entry_z_score = trade.get('entry_zscore', 0.0)
                        tracker.position.entry_date = trade.get('entry_time', '')[:10] if trade.get('entry_time') else ''
                        tracker.position.entry_time = trade.get('entry_time', '')[11:16] if trade.get('entry_time') else ''
                        tracker.position.lots_y = trade.get('q1', 0) // tracker.config.lot_size_y if tracker.config.lot_size_y else 1
                        tracker.position.lots_x = trade.get('q2', 0) // tracker.config.lot_size_x if tracker.config.lot_size_x else 1
                        
                print(f"   📂 Loaded {len(self.active_positions)} active positions")
            except Exception as e:
                print(f"   ⚠️ Failed to load state: {e}")
    
    def update_prices(self, price_data: Dict[str, float]):
        """
        Update all trackers with latest prices.
        
        Args:
            price_data: Dict of symbol -> current price
        """
        signals = []
        
        for pair_key, tracker in self.trackers.items():
            price_y = price_data.get(tracker.config.stock_y, 0)
            price_x = price_data.get(tracker.config.stock_x, 0)
            
            if price_y <= 0 or price_x <= 0:
                continue
            
            # Update tracker
            result = tracker.update(price_y, price_x)
            
            # Check for signals
            if not tracker.position.is_open:
                should_enter, pos_type = tracker.check_entry_signal(price_y, price_x)
                if should_enter:
                    signals.append({
                        'pair': pair_key,
                        'type': 'ENTRY',
                        'direction': pos_type,
                        'z_score': result['z_score'],
                        'price_y': price_y,
                        'price_x': price_x
                    })
            else:
                if result['should_exit']:
                    signals.append({
                        'pair': pair_key,
                        'type': 'EXIT',
                        'reason': result['exit_reason'],
                        'z_score': result['z_score'],
                        'pnl': tracker.position.total_pnl
                    })
        
        return signals
    
    def display(self, clear_screen: bool = True):
        """Display the live dashboard."""
        if clear_screen:
            try:
                os.system('clear' if os.name == 'posix' else 'cls')
            except:
                print("\n" * 3)  # Fallback: just add blank lines
        
        output = []
        output.append("")
        output.append("=" * 80)
        output.append(f"📊 STAT ARB LIVE DASHBOARD | {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        output.append("=" * 80)
        
        # Capital Summary
        total_pnl = sum(t.position.total_pnl for t in self.trackers.values() if t.position.is_open)
        open_count = sum(1 for t in self.trackers.values() if t.position.is_open)
        output.append(f"\n💰 Capital: ₹{self.capital:,.0f} | Open Positions: {open_count} | Unrealized P&L: ₹{total_pnl:+,.0f}")
        
        # Active Positions Table
        if open_count > 0:
            output.append("\n" + "-" * 80)
            output.append("📈 ACTIVE POSITIONS")
            output.append("-" * 80)
            
            headers = ['Pair', 'Side', 'Entry Z', 'Curr Z', 'Entry Y', 'Curr Y', 'P&L Y', 'P&L X', 'Total P&L']
            data = []
            
            for pair_key, tracker in self.trackers.items():
                if tracker.position.is_open:
                    pos = tracker.position
                    data.append([
                        pair_key,
                        pos.position_type,
                        f'{pos.entry_z_score:.2f}',
                        f'{pos.current_z_score:.2f}',
                        f'{pos.entry_price_y:.0f}',
                        f'{pos.current_price_y:.0f}',
                        f'{pos.pnl_y:+,.0f}',
                        f'{pos.pnl_x:+,.0f}',
                        f'{pos.total_pnl:+,.0f}'
                    ])
            
            output.append(tabulate(data, headers=headers, tablefmt='simple'))
        
        # Z-Score Monitor for All Pairs
        output.append("\n" + "-" * 80)
        output.append("📊 Z-SCORE MONITOR (All Pairs)")
        output.append("-" * 80)
        
        headers = ['Pair', 'Z-Score', 'Status', 'Sigma', 'Beta', 'Sector']
        data = []
        
        for pair_key, tracker in self.trackers.items():
            z = tracker.position.current_z_score if tracker.position.is_open else 0
            if tracker.logs:
                z = tracker.logs[-1].z_score
            
            # Determine status
            if abs(z) > 3.0:
                status = "🔴 STOP"
            elif abs(z) > 2.5:
                status = "🟡 ENTRY"
            elif abs(z) < 1.0:
                status = "🟢 EXIT"
            else:
                status = "⚪ WAIT"
            
            # Add position indicator
            if tracker.position.is_open:
                status = f"📍 {tracker.position.position_type}"
            
            data.append([
                pair_key,
                f'{z:+.3f}',
                status,
                f'{tracker.config.sigma:.1f}',
                f'{tracker.config.beta:.3f}',
                tracker.config.sector
            ])
        
        output.append(tabulate(data, headers=headers, tablefmt='simple'))
        
        # Instructions
        output.append("\n" + "-" * 80)
        output.append("📋 TRADING RULES (Zerodha Varsity)")
        output.append("-" * 80)
        output.append("   🟡 ENTRY:  Z > +2.5 → SHORT pair (Sell Y, Buy X)")
        output.append("             Z < -2.5 → LONG pair (Buy Y, Sell X)")
        output.append("   🟢 EXIT:   Z reverts to ±1.0 → Take Profit")
        output.append("   🔴 STOP:   Z expands to ±3.0 → Stop Loss")
        
        output.append("")
        print('\n'.join(output))
    
    def run_demo(self, prices: Dict[str, float] = None):
        """Run a demo update with sample/live prices."""
        if prices is None:
            # Demo prices
            prices = {}
            for tracker in self.trackers.values():
                # Generate demo prices (in reality, fetch from broker)
                prices[tracker.config.stock_y] = 1000.0  # Placeholder
                prices[tracker.config.stock_x] = 500.0   # Placeholder
        
        signals = self.update_prices(prices)
        self.display()
        
        if signals:
            print("\n🔔 SIGNALS:")
            for s in signals:
                if s['type'] == 'ENTRY':
                    print(f"   ⚡ {s['pair']}: {s['direction']} at Z={s['z_score']:.2f}")
                else:
                    print(f"   🚪 {s['pair']}: EXIT ({s['reason']}) at Z={s['z_score']:.2f}, P&L: ₹{s['pnl']:+,.0f}")
    
    def save_state(self):
        """Save dashboard state."""
        state_file = os.path.join(config.DATA_DIR, "dashboard_state.json")
        state = {
            'trackers': {k: v.to_dict() for k, v in self.trackers.items()},
            'last_updated': datetime.now().isoformat()
        }
        with open(state_file, 'w') as f:
            json.dump(state, f, indent=2)


def run_live_dashboard():
    """Run the live dashboard with data from broker."""
    print("\n🚀 Starting Live Dashboard...")
    
    try:
        from infrastructure.broker.kite_auth import get_kite
        from infrastructure.data.cache import DataCache
        
        broker = get_kite()
        cache = DataCache(broker, max_workers=5)
        dashboard = LiveDashboard()
        
        if not dashboard.trackers:
            print("❌ No pairs configured. Run scan_pairs first.")
            return
        
        print(f"✅ Monitoring {len(dashboard.trackers)} pairs...")
        
        while True:
            # Fetch latest prices
            symbols = set()
            for tracker in dashboard.trackers.values():
                symbols.add(tracker.config.stock_y)
                symbols.add(tracker.config.stock_x)
            
            # Get current LTPs
            price_data = {}
            try:
                ltps = broker.ltp([f"NSE:{s}" for s in symbols])
                for key, val in ltps.items():
                    symbol = key.replace("NSE:", "")
                    price_data[symbol] = val.get('last_price', 0)
            except Exception as e:
                print(f"⚠️ LTP fetch failed: {e}")
                time.sleep(10)
                continue
            
            # Update dashboard
            signals = dashboard.update_prices(price_data)
            dashboard.display()
            
            # Alert on signals
            if signals:
                from trading_floor.alerts import send_alert
                for s in signals:
                    msg = f"{s['type']}: {s['pair']} at Z={s['z_score']:.2f}"
                    send_alert(msg, level='INFO')
            
            # Save state periodically
            dashboard.save_state()
            
            time.sleep(60)  # Update every minute
            
    except KeyboardInterrupt:
        print("\n🛑 Dashboard stopped.")
    except Exception as e:
        print(f"❌ Dashboard error: {e}")
        import traceback
        traceback.print_exc()


def run_paper_dashboard():
    """Run dashboard with historical/paper data for testing (continuous loop)."""
    print("\n📄 Running Paper Dashboard (Historical Data Mode)...")
    print("   Press Ctrl+C to stop\n")
    
    dashboard = LiveDashboard()
    
    if not dashboard.trackers:
        print("❌ No pairs configured.")
        return
    
    import pandas as pd
    
    # Load historical data for ALL pairs
    historical_data = {}
    for pair_key, tracker in dashboard.trackers.items():
        spot_y_file = os.path.join(config.DATA_DIR, f"{tracker.config.stock_y}_day.csv")
        spot_x_file = os.path.join(config.DATA_DIR, f"{tracker.config.stock_x}_day.csv")
        
        if os.path.exists(spot_y_file) and os.path.exists(spot_x_file):
            try:
                df_y = pd.read_csv(spot_y_file)
                df_x = pd.read_csv(spot_x_file)
                historical_data[tracker.config.stock_y] = df_y['close'].values
                historical_data[tracker.config.stock_x] = df_x['close'].values
            except Exception as e:
                print(f"   ⚠️ Failed to load data for {pair_key}: {e}")
    
    print(f"   📊 Loaded historical data for {len(historical_data)//2} pairs")
    
    try:
        refresh_interval = 5  # Seconds between updates for demo
        day_index = -1  # Start from most recent day
        
        while True:
            # Get prices for current day
            price_data = {}
            for symbol, prices in historical_data.items():
                if len(prices) > abs(day_index):
                    price_data[symbol] = float(prices[day_index])
            
            if price_data:
                # Update dashboard with prices
                signals = dashboard.update_prices(price_data)
                dashboard.display()
                
                # Show any signals
                if signals:
                    print("🔔 SIGNALS DETECTED:")
                    for s in signals:
                        if s['type'] == 'ENTRY':
                            print(f"   ⚡ {s['pair']}: {s['direction']} ENTRY at Z={s['z_score']:.2f}")
                            print(f"      → Y={s['price_y']:.2f}, X={s['price_x']:.2f}")
                        else:
                            print(f"   🚪 {s['pair']}: EXIT ({s['reason']}) at Z={s['z_score']:.2f}")
                            print(f"      → P&L: ₹{s['pnl']:+,.0f}")
                
                print(f"\n⏰ Next refresh in {refresh_interval}s... (Ctrl+C to stop)")
            else:
                print("❌ No price data available. Load historical data first.")
                break
            
            time.sleep(refresh_interval)
            
    except KeyboardInterrupt:
        print("\n🛑 Dashboard stopped.")
        dashboard.save_state()


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Live Trading Dashboard")
    parser.add_argument('--mode', choices=['live', 'paper'], default='paper',
                       help='Dashboard mode: live (real broker) or paper (historical)')
    args = parser.parse_args()
    
    if args.mode == 'live':
        run_live_dashboard()
    else:
        run_paper_dashboard()
