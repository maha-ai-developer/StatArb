#!/usr/bin/env python
import argparse
import sys
import os
from typing import Dict, Any

# -----------------------------------------------------------
# Broker authentication imports
# -----------------------------------------------------------
from broker.kite_auth import (
    generate_login_url,
    exchange_request_token,
    get_kite,
)

# Required for fetching account details
from broker.kite_positions import fetch_account_snapshot 

# -----------------------------------------------------------
# Core Modules
# -----------------------------------------------------------
from core.instrument_cache import refresh_instrument_cache
from core.universe import UniverseBuilder
from data_downloader import download_bulk

# IMPORT THE NEW PROFESSIONAL ENGINE
from live.engine import LiveEngine

# ===========================================================
# CLI COMMAND HANDLERS
# ===========================================================

def cmd_login(_args):
    """Show Zerodha login URL."""
    url = generate_login_url()
    print("\n[LOGIN URL]\n", url)
    print("\nSteps:")
    print("1. Open this URL in browser")
    print("2. Login to Zerodha Kite")
    print("3. Copy request_token from redirect URL")
    print("4. Run:\n   python cli.py token --request_token <request_token>\n")

def cmd_token(args):
    """Exchange request_token -> access_token."""
    print("[CLI] Exchanging request_token for access_token...")
    try:
        access_token = exchange_request_token(args.request_token)
        print(f"[CLI] Access token updated successfully.")
    except Exception as e:
        print(f"[CLI] Error exchanging token: {e}")
        sys.exit(1)

def cmd_account(_args):
    """Fetches and prints the current account balance and positions."""
    print("--- 🏦 ACCOUNT SNAPSHOT ---")
    try:
        # fetch_account_snapshot returns 4 values: profile, margins, holdings, positions
        result = fetch_account_snapshot()
        
        # Validate result structure
        if not isinstance(result, (tuple, list)) or len(result) < 4:
            print(f"❌ Error: API returned unexpected format: {result}")
            return

        p_obj, m_obj, h_obj, pos_obj = result

        # --- 1. HANDLE PROFILE / USER INFO ---
        print("\n💰 BALANCE INFO:")
        user_info = "N/A"
        if isinstance(p_obj, dict):
            user_info = f"{p_obj.get('user_name', 'N/A')} ({p_obj.get('user_id', '')})"
        print(f"  User:               {user_info}")

        # --- 2. HANDLE MARGINS / EQUITY ---
        net_equity = 0.0
        available_cash = 0.0
        
        # The margin object is the FULL response from kite.margins()
        if isinstance(m_obj, dict):
            
            # The net total equity is available directly under 'net' in the full margins dict
            net_equity = float(m_obj.get('net', 0.0))
            
            # The granular details are often nested under 'equity'
            eq = m_obj.get('equity', {})
            
            # 2a. Try to get Available Cash from the nested 'available' dict
            avail_dict = eq.get('available', {})
            if isinstance(avail_dict, dict):
                # This should be the most reliable source for available cash
                available_cash = float(avail_dict.get('cash', 0.0))
            
            # 2b. Fallback: If cash is 0, but net equity is > 0, assume available cash is net equity
            # This handles the exact scenario you saw where the nested 'cash' is zeroed out
            if available_cash < 1.0 and net_equity > 1.0:
                 available_cash = net_equity
            
        
        print(f"  Total Equity:       ₹ {net_equity:,.2f}")
        print(f"  Available Cash:     ₹ {available_cash:,.2f}")

        # --- 3. HANDLE POSITIONS ---
        print("\n💼 POSITIONS:")
        positions_list = []
        if isinstance(pos_obj, dict):
            positions_list = pos_obj.get("net", [])
        elif isinstance(pos_obj, list):
            positions_list = pos_obj
        
        if not positions_list:
            print("  No open positions.")
        else:
            for p in positions_list:
                if isinstance(p, dict) and int(p.get("quantity", 0)) != 0:
                    sym = p.get('tradingsymbol', 'Unknown')
                    qty = int(p.get('quantity'))
                    pnl = float(p.get('pnl', 0.0))
                    print(f"  {sym:<12} | Qty: {qty:>4} | PnL: ₹{pnl:,.2f}")

        print("-----------------------------------")

    except Exception as e:
        print(f"❌ ERROR fetching account data: {e}")
        sys.exit(1)


def cmd_instruments_refresh(_args):
    """Refresh instrument cache."""
    refresh_instrument_cache()

def cmd_universe_build(_args):
    """Build NIFTY 50 universe."""
    builder = UniverseBuilder()
    builder.build_nifty_50()

def cmd_download(args):
    """Download historical data."""
    download_bulk(args.symbols, args.from_date, args.to_date, args.interval)

def cmd_engine(args):
    """Start the Trading Engine."""
    print("Starting Live Trading Engine...")
    
    # Process symbol files
    momentum_symbols = []
    if os.path.exists(args.momentum):
        with open(args.momentum, "r") as f:
            momentum_symbols = [line.strip().upper() for line in f if line.strip()]

    # Initialize Engine
    engine = LiveEngine(
        symbols=momentum_symbols,
        pair_file=args.pairs,
        timeframe=args.timeframe,
        place_order=args.place_order,
        risk_pct=args.risk_pct
    )
    
    # Start the core loop
    engine.start()


def main():
    parser = argparse.ArgumentParser(
        description="Algo Trading CLI Tool for Data, Backtesting, and Live Trading."
    )

    subparsers = parser.add_subparsers(title="command", dest="command", required=True)

    # 1. Login
    p_login = subparsers.add_parser("login", help="Show Zerodha login URL")
    p_login.set_defaults(func=cmd_login)

    # 2. Token Exchange
    p_token = subparsers.add_parser("token", help="Exchange request_token for access_token")
    p_token.add_argument("--request_token", required=True, help="Token from redirect URL")
    p_token.set_defaults(func=cmd_token)

    # 3. Account View (NEW COMMAND)
    p_acc = subparsers.add_parser("account", help="View live account margin and positions")
    p_acc.set_defaults(func=cmd_account)

    # 4. Instruments
    p_inst = subparsers.add_parser("instruments-refresh", help="Refresh instrument cache")
    p_inst.set_defaults(func=cmd_instruments_refresh)

    # 5. Universe
    p_uni = subparsers.add_parser("universe-build", help="Build NIFTY 50 universe")
    p_uni.set_defaults(func=cmd_universe_build)

    # 6. Download
    p_dl = subparsers.add_parser("download", help="Download historical data")
    p_dl.add_argument("--symbols", default="symbols.txt", help="File containing symbols")
    p_dl.add_argument("--from-date", required=True, help="YYYY-MM-DD")
    p_dl.add_argument("--to-date", required=True, help="YYYY-MM-DD")
    p_dl.add_argument("--interval", default="5m", help="5m, 15m, 60m, day")
    p_dl.set_defaults(func=cmd_download)

    # 7. ENGINE (UPDATED TO USE PRO SYSTEM)
    p_eng = subparsers.add_parser("engine", help="Start the Trading Engine")
    p_eng.add_argument("--momentum", default="symbols.txt", help="Path to momentum symbols file")
    p_eng.add_argument("--pairs", default="live_pairs.txt", help="Path to pairs file")
    p_eng.add_argument("--timeframe", default="5m", help="Candle interval (e.g., 5m)")
    p_eng.add_argument("--place-order", action="store_true", help="Set to place live orders (DANGER!)")
    p_eng.add_argument("--risk-pct", type=float, default=1.0, help="Risk percent per trade (e.g., 1.0)")
    p_eng.set_defaults(func=cmd_engine)


    # Parse and Execute
    if len(sys.argv) == 1:
        parser.print_help(sys.stderr)
        sys.exit(1)

    args = parser.parse_args()
    args.func(args)

if __name__ == "__main__":
    main()
