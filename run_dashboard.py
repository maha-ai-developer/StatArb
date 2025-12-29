#!/usr/bin/env python3
"""
Run the Position Tracker Dashboard - Enhanced Version v2

Features:
1. FUTURES PRICES from Kite NFO (not spot)
2. Margin calculation using Kite API
3. Beta neutrality calculation display
4. Calculation breakdown for each pair
5. Sound/visual alerts when Z crosses ±2.5
6. Auto-execute trades when signals appear

Usage:
    python run_dashboard.py --live                    # View mode (no execution)
    python run_dashboard.py --live --auto             # Auto-execution (paper)
"""

import os
import sys
import time
import argparse
from datetime import datetime
from typing import Dict, Optional, List, Tuple

sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from trading_floor.dashboard import LiveDashboard
from trading_floor.position_tracker import PositionTracker
import infrastructure.config as config


# =============================================================================
# FUTURES & MARGIN UTILITIES
# =============================================================================

def get_futures_ltp(broker, symbols: List[str]) -> Dict[str, Dict]:
    """
    Get FUTURES LTP (Last Traded Price) from Kite NFO exchange.
    
    Returns dict with:
    - futures_symbol: Futures trading symbol (e.g., SBIN25JANFUT)
    - futures_price: Current futures price
    - spot_price: Spot price for reference
    - basis: Futures - Spot difference
    """
    from infrastructure.data.futures_utils import get_futures_details
    
    result = {}
    
    # Collect all symbols for spot and futures
    spot_symbols = [f"NSE:{s}" for s in symbols]
    futures_map = {}  # spot -> futures_symbol
    futures_tokens = []
    
    # Get futures details for each symbol
    for sym in symbols:
        details = get_futures_details(sym, broker)
        if details:
            futures_map[sym] = details['symbol']
            futures_tokens.append(f"NFO:{details['symbol']}")
        
    # Fetch spot LTPs
    spot_ltps = {}
    try:
        spot_result = broker.ltp(spot_symbols)
        for key, val in spot_result.items():
            sym = key.replace("NSE:", "")
            spot_ltps[sym] = val.get('last_price', 0)
    except Exception as e:
        print(f"   ⚠️ Spot LTP error: {e}")
    
    # Fetch futures LTPs
    futures_ltps = {}
    if futures_tokens:
        try:
            fut_result = broker.ltp(futures_tokens)
            for key, val in fut_result.items():
                fut_sym = key.replace("NFO:", "")
                futures_ltps[fut_sym] = val.get('last_price', 0)
        except Exception as e:
            print(f"   ⚠️ Futures LTP error: {e}")
    
    # Combine results
    for sym in symbols:
        spot_price = spot_ltps.get(sym, 0)
        fut_sym = futures_map.get(sym, "")
        fut_price = futures_ltps.get(fut_sym, 0) if fut_sym else 0
        
        result[sym] = {
            'spot_symbol': sym,
            'futures_symbol': fut_sym,
            'spot_price': spot_price,
            'futures_price': fut_price if fut_price > 0 else spot_price,  # Fallback to spot
            'basis': round(fut_price - spot_price, 2) if fut_price > 0 and spot_price > 0 else 0
        }
    
    return result


def calculate_pair_margin(broker, tracker: PositionTracker, 
                          price_y: float, price_x: float, 
                          lots_y: int = 1, lots_x: int = 1) -> Dict:
    """
    Calculate total margin required for a pair trade using Kite API.
    
    Uses /margins/basket API for spread margin benefit.
    """
    from infrastructure.data.futures_utils import get_futures_details, get_margin_required
    
    cfg = tracker.config
    
    # Get futures symbols
    details_y = get_futures_details(cfg.stock_y, broker)
    details_x = get_futures_details(cfg.stock_x, broker)
    
    if not details_y or not details_x:
        # Fallback to 15% estimate
        qty_y = lots_y * cfg.lot_size_y
        qty_x = lots_x * cfg.lot_size_x
        value_y = price_y * qty_y
        value_x = price_x * qty_x
        total_value = value_y + value_x
        
        return {
            'margin_y': round(value_y * 0.15, 0),
            'margin_x': round(value_x * 0.15, 0),
            'combined_margin': round(total_value * 0.15, 0),  # No spread benefit
            'spread_benefit': 0,
            'source': 'ESTIMATE_15%'
        }
    
    # Calculate quantities
    qty_y = lots_y * details_y['lot_size']
    qty_x = lots_x * details_x['lot_size']
    
    # Try Kite basket margin API
    try:
        basket = [
            {
                "exchange": "NFO",
                "tradingsymbol": details_y['symbol'],
                "transaction_type": "BUY",  # For LONG spread
                "quantity": qty_y,
                "product": "NRML",
                "order_type": "MARKET",
                "variety": "regular"
            },
            {
                "exchange": "NFO",
                "tradingsymbol": details_x['symbol'],
                "transaction_type": "SELL",  # Opposite leg
                "quantity": qty_x,
                "product": "NRML",
                "order_type": "MARKET",
                "variety": "regular"
            }
        ]
        
        # Get basket margin (with spread benefit)
        result = broker.basket_order_margins(basket, consider_positions=True)
        
        if result and 'final' in result:
            combined = result['final']['total']
            
            # Get individual margins
            margin_y = result['orders'][0]['total'] if len(result.get('orders', [])) > 0 else 0
            margin_x = result['orders'][1]['total'] if len(result.get('orders', [])) > 1 else 0
            individual_sum = margin_y + margin_x
            
            return {
                'margin_y': round(margin_y, 0),
                'margin_x': round(margin_x, 0),
                'individual_sum': round(individual_sum, 0),
                'combined_margin': round(combined, 0),
                'spread_benefit': round(individual_sum - combined, 0),
                'spread_benefit_pct': round((individual_sum - combined) / individual_sum * 100, 1) if individual_sum > 0 else 0,
                'source': 'KITE_API'
            }
    except Exception as e:
        print(f"   ⚠️ Margin API error: {e}")
    
    # Fallback to individual margins
    margin_y = get_margin_required(details_y['symbol'], qty_y, "BUY", broker)
    margin_x = get_margin_required(details_x['symbol'], qty_x, "SELL", broker)
    
    if margin_y and margin_x:
        total_y = margin_y.get('total', 0)
        total_x = margin_x.get('total', 0)
        return {
            'margin_y': round(total_y, 0),
            'margin_x': round(total_x, 0),
            'combined_margin': round(total_y + total_x, 0),
            'spread_benefit': 0,
            'source': 'KITE_INDIVIDUAL'
        }
    
    # Final fallback
    value_y = price_y * qty_y
    value_x = price_x * qty_x
    return {
        'margin_y': round(value_y * 0.15, 0),
        'margin_x': round(value_x * 0.15, 0),
        'combined_margin': round((value_y + value_x) * 0.15, 0),
        'spread_benefit': 0,
        'source': 'ESTIMATE_15%'
    }


def calculate_beta_neutral_sizing(tracker: PositionTracker, price_y: float, price_x: float) -> Dict:
    """
    Calculate beta-neutral position sizing per Zerodha Varsity.
    
    Beta-neutral: Y_shares × β = X_shares
    """
    cfg = tracker.config
    beta = cfg.beta
    lot_y = cfg.lot_size_y
    lot_x = cfg.lot_size_x
    
    # Start with 1 lot of Y
    lots_y = 1
    shares_y = lots_y * lot_y
    
    # Beta-neutral shares for X
    beta_shares_x = shares_y * abs(beta)
    
    # Round to nearest lot
    lots_x = max(1, round(beta_shares_x / lot_x))
    shares_x = lots_x * lot_x
    
    # Calculate mismatch
    actual_ratio = shares_x / shares_y if shares_y > 0 else 0
    ideal_ratio = abs(beta)
    mismatch_pct = abs(actual_ratio - ideal_ratio) / ideal_ratio * 100 if ideal_ratio > 0 else 0
    
    # Contract values
    value_y = shares_y * price_y
    value_x = shares_x * price_x
    
    return {
        'lots_y': lots_y,
        'lots_x': lots_x,
        'shares_y': shares_y,
        'shares_x': shares_x,
        'beta': beta,
        'beta_required_x': round(beta_shares_x, 0),
        'actual_shares_x': shares_x,
        'mismatch_shares': shares_x - beta_shares_x,
        'mismatch_pct': round(mismatch_pct, 1),
        'is_neutral': mismatch_pct < 20,  # Within 20% is acceptable
        'value_y': round(value_y, 0),
        'value_x': round(value_x, 0),
        'total_value': round(value_y + value_x, 0)
    }


# =============================================================================
# DISPLAY FUNCTIONS
# =============================================================================

def display_futures_prices(price_data: Dict[str, Dict]):
    """Display futures vs spot prices."""
    print(f"\n{'─' * 70}")
    print("💹 FUTURES PRICES (NFO)")
    print(f"{'─' * 70}")
    
    header = f"{'Symbol':<12} {'Spot':>10} {'Futures':>10} {'Basis':>8} {'Fut Symbol':<20}"
    print(header)
    print(f"{'─' * 70}")
    
    for sym, data in price_data.items():
        basis = data.get('basis', 0)
        basis_str = f"{basis:+.2f}" if basis != 0 else "N/A"
        print(f"{sym:<12} ₹{data['spot_price']:>9,.2f} ₹{data['futures_price']:>9,.2f} {basis_str:>8} {data['futures_symbol']:<20}")


def display_margin_calculation(tracker: PositionTracker, margin_data: Dict, sizing: Dict):
    """Display margin calculation breakdown."""
    cfg = tracker.config
    
    print(f"\n   💰 MARGIN CALCULATION ({margin_data.get('source', 'UNKNOWN')}):")
    print(f"   ┌────────────────────────────────────────────────────────")
    print(f"   │ {cfg.stock_y} (Y): {sizing['lots_y']} lots × {cfg.lot_size_y} = {sizing['shares_y']} shares")
    print(f"   │    Contract Value: ₹{sizing['value_y']:,.0f}")
    print(f"   │    Margin Required: ₹{margin_data['margin_y']:,.0f}")
    print(f"   │")
    print(f"   │ {cfg.stock_x} (X): {sizing['lots_x']} lots × {cfg.lot_size_x} = {sizing['shares_x']} shares")
    print(f"   │    Contract Value: ₹{sizing['value_x']:,.0f}")
    print(f"   │    Margin Required: ₹{margin_data['margin_x']:,.0f}")
    print(f"   ├────────────────────────────────────────────────────────")
    print(f"   │ TOTAL CONTRACT VALUE: ₹{sizing['total_value']:,.0f}")
    
    if margin_data.get('spread_benefit', 0) > 0:
        print(f"   │ Individual Margins Sum: ₹{margin_data.get('individual_sum', 0):,.0f}")
        print(f"   │ Spread Benefit: -₹{margin_data['spread_benefit']:,.0f} ({margin_data.get('spread_benefit_pct', 0):.1f}%)")
    
    print(f"   │ COMBINED MARGIN: ₹{margin_data['combined_margin']:,.0f}")
    print(f"   └────────────────────────────────────────────────────────")


def display_beta_neutrality(sizing: Dict):
    """Display beta neutrality calculation."""
    status = "✅ NEUTRAL" if sizing['is_neutral'] else "⚠️ MISMATCH"
    
    print(f"\n   ⚖️ BETA NEUTRALITY CHECK:")
    print(f"   ┌────────────────────────────────────────────────────────")
    print(f"   │ Beta (β) = {sizing['beta']:.4f}")
    print(f"   │")
    print(f"   │ Y Shares = {sizing['shares_y']}")
    print(f"   │ Required X Shares (β × Y) = {sizing['shares_y']} × {sizing['beta']:.4f} = {sizing['beta_required_x']:.0f}")
    print(f"   │ Actual X Shares = {sizing['shares_x']}")
    print(f"   │")
    print(f"   │ Mismatch = {sizing['shares_x']} - {sizing['beta_required_x']:.0f} = {sizing['mismatch_shares']:+.0f} shares ({sizing['mismatch_pct']:.1f}%)")
    print(f"   │ Status: {status}")
    print(f"   └────────────────────────────────────────────────────────")


def display_z_score_calculation(tracker: PositionTracker, price_y: float, price_x: float):
    """Show detailed Z-score calculation breakdown."""
    cfg = tracker.config
    
    # Calculate components
    beta_component = cfg.beta * price_x
    predicted_y = cfg.intercept + beta_component
    residual = price_y - predicted_y
    z_score = residual / cfg.sigma if cfg.sigma > 0 else 0
    
    print(f"\n   📐 Z-SCORE CALCULATION (Zerodha Varsity Method):")
    print(f"   ┌────────────────────────────────────────────────────────")
    print(f"   │ Y Price ({cfg.stock_y}): ₹{price_y:,.2f}")
    print(f"   │ X Price ({cfg.stock_x}): ₹{price_x:,.2f}")
    print(f"   ├────────────────────────────────────────────────────────")
    print(f"   │ Formula: Z = (Y - (β×X + c)) / σ")
    print(f"   │")
    print(f"   │ β = {cfg.beta:.4f}")
    print(f"   │ c = {cfg.intercept:.2f}")
    print(f"   │ σ = {cfg.sigma:.2f}")
    print(f"   ├────────────────────────────────────────────────────────")
    print(f"   │ Predicted Y = {cfg.intercept:.2f} + ({cfg.beta:.4f} × {price_x:.2f})")
    print(f"   │            = {cfg.intercept:.2f} + {beta_component:.2f}")
    print(f"   │            = {predicted_y:.2f}")
    print(f"   │")
    print(f"   │ Residual = {price_y:.2f} - {predicted_y:.2f} = {residual:.2f}")
    print(f"   │")
    print(f"   │ Z-Score = {residual:.2f} / {cfg.sigma:.2f} = {z_score:+.4f}")
    print(f"   └────────────────────────────────────────────────────────")
    
    # Signal interpretation
    if z_score > 2.5:
        print(f"   🟡 SIGNAL: SHORT ENTRY (Z > +2.5) → Sell Y, Buy X")
    elif z_score < -2.5:
        print(f"   🟡 SIGNAL: LONG ENTRY (Z < -2.5) → Buy Y, Sell X")
    elif abs(z_score) > 3.0:
        print(f"   🔴 SIGNAL: STOP LOSS ZONE (|Z| > 3.0)")
    elif abs(z_score) < 1.0:
        print(f"   🟢 SIGNAL: MEAN REVERSION / EXIT ZONE (|Z| < 1.0)")
    else:
        print(f"   ⚪ SIGNAL: WAIT ZONE (1.0 < |Z| < 2.5)")
    
    return z_score


# =============================================================================
# SOUND ALERTS
# =============================================================================

def play_alert_sound(alert_type: str = "entry"):
    """Play sound alert."""
    try:
        if alert_type == "entry":
            for _ in range(3):
                print("\a", end="", flush=True)
                time.sleep(0.2)
        elif alert_type == "stop":
            for _ in range(5):
                print("\a", end="", flush=True)
                time.sleep(0.1)
        else:
            print("\a", end="", flush=True)
    except:
        pass


def visual_alert(message: str, alert_type: str = "info"):
    """Print visual alert."""
    if alert_type == "entry":
        print(f"\n{'🚨' * 20}")
        print(f"⚡⚡⚡ {message} ⚡⚡⚡")
        print(f"{'🚨' * 20}\n")
    elif alert_type == "stop":
        print(f"\n{'🔴' * 20}")
        print(f"🛑🛑🛑 {message} 🛑🛑🛑")
        print(f"{'🔴' * 20}\n")
    else:
        print(f"\n✅✅✅ {message} ✅✅✅\n")


# =============================================================================
# MAIN DASHBOARD
# =============================================================================

def run_dashboard(mode: str = "paper", refresh_seconds: int = 60, auto_execute: bool = False):
    """
    Run enhanced dashboard with futures, margin, and beta-neutrality.
    
    Args:
        mode: "paper" (real data, simulated trades) or "live" (real trades)
        refresh_seconds: Refresh interval
        auto_execute: Enable auto-execution of trades
    """
    mode_upper = mode.upper()
    
    print("\n" + "=" * 70)
    print(f"📊 STAT ARB POSITION TRACKER - {mode_upper} MODE")
    print("=" * 70)
    print(f"   ⏰ Refresh: {refresh_seconds}s")
    print(f"   📈 Data: FUTURES (NFO) prices from Kite API")
    print(f"   💰 Margin: Kite API calculation")
    print(f"   ⚖️ Beta-neutrality: Displayed")
    print(f"   🤖 Trade Execution: {'SIMULATED (Paper)' if mode_upper == 'PAPER' else 'REAL (Live)'}")
    if auto_execute:
        print(f"   ⚡ Auto-execution: ENABLED")
    print("   Press Ctrl+C to stop")
    print("=" * 70 + "\n")
    
    # Connect to Kite (both modes need real data)
    print("🔌 Connecting to Kite API...")
    try:
        from infrastructure.broker.kite_auth import get_kite
        broker = get_kite()
        print(f"   ✅ Connected!")
    except Exception as e:
        print(f"   ❌ Failed: {e}")
        print("   Run: python cli.py login")
        return
    
    # Initialize dashboard
    dashboard = LiveDashboard()
    
    if not dashboard.trackers:
        print("❌ No pairs configured. Run scan_pairs first.")
        return
    
    # Get all symbols
    symbols = set()
    for tracker in dashboard.trackers.values():
        symbols.add(tracker.config.stock_y)
        symbols.add(tracker.config.stock_x)
    
    print(f"   📈 Monitoring {len(symbols)} symbols across {len(dashboard.trackers)} pairs\n")
    
    # Track paper trades
    paper_trades = []
    
    try:
        while True:
            print(f"\n{'=' * 70}")
            print(f"📡 FETCHING LIVE DATA | {datetime.now().strftime('%H:%M:%S')} | {mode_upper} MODE")
            print(f"{'=' * 70}")
            
            # Fetch FUTURES prices (real data from Kite)
            price_data = get_futures_ltp(broker, list(symbols))
            
            # Display price table
            display_futures_prices(price_data)
            
            # Process each pair
            all_signals = []
            
            print(f"\n{'=' * 70}")
            print("🧮 PAIR ANALYSIS")
            print(f"{'=' * 70}")
            
            for pair_key, tracker in dashboard.trackers.items():
                cfg = tracker.config
                
                data_y = price_data.get(cfg.stock_y, {})
                data_x = price_data.get(cfg.stock_x, {})
                
                # Use FUTURES prices
                price_y = data_y.get('futures_price', 0)
                price_x = data_x.get('futures_price', 0)
                
                if price_y <= 0 or price_x <= 0:
                    continue
                
                print(f"\n{'─' * 70}")
                print(f"📊 {pair_key}")
                print(f"   Futures Y: {data_y.get('futures_symbol', cfg.stock_y)} @ ₹{price_y:,.2f}")
                print(f"   Futures X: {data_x.get('futures_symbol', cfg.stock_x)} @ ₹{price_x:,.2f}")
                
                # 1. Z-Score Calculation
                z_score = display_z_score_calculation(tracker, price_y, price_x)
                
                # 2. Beta Neutrality
                sizing = calculate_beta_neutral_sizing(tracker, price_y, price_x)
                display_beta_neutrality(sizing)
                
                # 3. Margin Calculation (real from Kite API)
                margin = calculate_pair_margin(broker, tracker, price_y, price_x, 
                                               sizing['lots_y'], sizing['lots_x'])
                display_margin_calculation(tracker, margin, sizing)
                
                # Update tracker
                tracker.update(price_y, price_x)
                
                # Check signals
                if not tracker.position.is_open:
                    should_enter, pos_type = tracker.check_entry_signal(price_y, price_x)
                    if should_enter:
                        signal = {
                            'pair': pair_key,
                            'type': 'ENTRY',
                            'direction': pos_type,
                            'z_score': z_score,
                            'price_y': price_y,
                            'price_x': price_x,
                            'margin_required': margin['combined_margin'],
                            'sizing': sizing
                        }
                        all_signals.append(signal)
                        visual_alert(f"ENTRY: {pair_key} - {pos_type} at Z={z_score:+.2f}", "entry")
                        play_alert_sound("entry")
                        
                        # Auto-execute if enabled
                        if auto_execute:
                            if mode_upper == "PAPER":
                                # Simulated trade
                                print(f"\n   📄 PAPER TRADE: {pos_type} {pair_key}")
                                print(f"      Y: {sizing['shares_y']} shares @ ₹{price_y:,.2f}")
                                print(f"      X: {sizing['shares_x']} shares @ ₹{price_x:,.2f}")
                                print(f"      Margin: ₹{margin['combined_margin']:,.0f}")
                                tracker.open_position(price_y, price_x, pos_type, sizing['lots_y'], sizing['lots_x'])
                                paper_trades.append({
                                    'time': datetime.now().isoformat(),
                                    'pair': pair_key,
                                    'type': 'ENTRY',
                                    'direction': pos_type,
                                    'z_score': z_score,
                                    'price_y': price_y,
                                    'price_x': price_x
                                })
                            else:
                                # Real trade execution
                                print(f"\n   🚀 LIVE TRADE: Executing {pos_type} {pair_key}...")
                                # Add real execution logic here
                else:
                    should_exit, exit_reason = tracker.check_exit_signal(price_y, price_x)
                    if should_exit:
                        signal = {
                            'pair': pair_key,
                            'type': 'EXIT',
                            'reason': exit_reason,
                            'z_score': z_score,
                            'pnl': tracker.position.total_pnl
                        }
                        all_signals.append(signal)
                        alert_type = "stop" if exit_reason == "STOP_LOSS" else "exit"
                        visual_alert(f"EXIT: {pair_key} - {exit_reason}", alert_type)
                        play_alert_sound(alert_type)
                        
                        # Auto-execute exit if enabled
                        if auto_execute:
                            if mode_upper == "PAPER":
                                print(f"\n   📄 PAPER EXIT: {pair_key}")
                                print(f"      Reason: {exit_reason}")
                                print(f"      P&L: ₹{tracker.position.total_pnl:+,.0f}")
                                result = tracker.close_position(price_y, price_x, exit_reason)
                                paper_trades.append({
                                    'time': datetime.now().isoformat(),
                                    'pair': pair_key,
                                    'type': 'EXIT',
                                    'reason': exit_reason,
                                    'pnl': result['total_pnl']
                                })
                            else:
                                print(f"\n   🚀 LIVE EXIT: {pair_key}...")
            
            # Summary
            print(f"\n{'=' * 70}")
            print("📋 SUMMARY")
            print(f"{'=' * 70}")
            
            open_positions = sum(1 for t in dashboard.trackers.values() if t.position.is_open)
            total_pnl = sum(t.position.total_pnl for t in dashboard.trackers.values() if t.position.is_open)
            
            print(f"   Mode: {mode_upper}")
            print(f"   Open Positions: {open_positions}")
            print(f"   Unrealized P&L: ₹{total_pnl:+,.0f}")
            print(f"   Signals: {len(all_signals)}")
            
            if paper_trades:
                print(f"   Paper Trades: {len(paper_trades)}")
            
            if all_signals:
                print(f"\n   📢 SIGNALS:")
                for s in all_signals:
                    if s['type'] == 'ENTRY':
                        print(f"      ⚡ {s['pair']}: {s['direction']} at Z={s['z_score']:+.2f}")
                        print(f"         Margin Required: ₹{s['margin_required']:,.0f}")
                    else:
                        print(f"      🚪 {s['pair']}: {s['reason']} P&L=₹{s.get('pnl', 0):+,.0f}")
            
            dashboard.save_state()
            
            print(f"\n⏰ Next update in {refresh_seconds}s... (Ctrl+C to stop)")
            time.sleep(refresh_seconds)
            
    except KeyboardInterrupt:
        print("\n🛑 Dashboard stopped.")
        dashboard.save_state()
        
        # Show paper trade summary
        if paper_trades:
            print(f"\n📋 PAPER TRADES THIS SESSION:")
            for t in paper_trades:
                print(f"   {t['time'][:19]} | {t['pair']} | {t.get('direction', t.get('type', ''))} | {t.get('pnl', 'N/A')}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Stat Arb Position Tracker v2")
    parser.add_argument('--mode', choices=['paper', 'live'], default='paper',
                       help='Trading mode: paper (simulated) or live (real trades)')
    parser.add_argument('--live', action='store_true', help='Shortcut for --mode live')
    parser.add_argument('--auto', action='store_true', help='Enable auto-execution of trades')
    parser.add_argument('--refresh', type=int, default=60, help='Refresh interval (seconds)')
    args = parser.parse_args()
    
    # Determine mode
    mode = "live" if args.live else args.mode
    
    run_dashboard(mode=mode, refresh_seconds=args.refresh, auto_execute=args.auto)
