#!/usr/bin/env python

import argparse
import sys

# -----------------------------------------------------------
# Broker authentication imports
# -----------------------------------------------------------
from broker.kite_auth import (
    generate_login_url,
    exchange_request_token,
    get_kite,
)

# -----------------------------------------------------------
# Instrument cache + universe builder
# -----------------------------------------------------------
from core.instrument_cache import refresh_instrument_cache
from core.universe import UniverseBuilder


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
    print("4. Run:\n   python cli.py token <request_token>\n")


def cmd_token(args):
    """Exchange request_token → access_token."""
    print("[CLI] Exchanging request_token for access_token...")
    access_token = exchange_request_token(args.request_token)
    # We don't print full token for safety
    print(f"[CLI] Access token updated: {access_token[:6]}****")


def cmd_instruments_refresh(_args):
    """Refresh full instrument dump and cache."""
    print("[CLI] Refreshing instrument cache…")
    instruments = refresh_instrument_cache()
    # If refresh_instrument_cache returns list, show count; if None, just OK.
    if instruments is not None:
        print(f"[CLI] Done. Instruments count = {len(instruments)}")
    else:
        print("[CLI] Done.")


def cmd_account(_args):
    """Show basic account snapshot."""
    kite = get_kite()

    print("\n[ACCOUNT PROFILE]")
    try:
        profile = kite.profile()
    except Exception as e:
        print("[ERROR] Could not fetch profile:", e)
        profile = {}
    print(profile)

    print("\n[MARGINS]")
    try:
        margins = kite.margins()
    except Exception as e:
        print("[ERROR] Could not fetch margins:", e)
        margins = {}
    print(margins)

    print("\n[POSITIONS]")
    try:
        positions = kite.positions()
    except Exception as e:
        print("[ERROR] Could not fetch positions:", e)
        positions = {}
    print(positions)

    print("\n[HOLDINGS]")
    try:
        holdings = kite.holdings()
    except Exception as e:
        print("[ERROR] Could not fetch holdings:", e)
        holdings = {}
    print(holdings)
    print("")

def cmd_universe(args):
    """
    Universe construction based on price/volume.
    """
    u = UniverseBuilder()
    
    # FIX: Use correct method names from universe.py
    if args.top_price:
        # was u.top_price
        syms = u.top_by_price(args.top_price) 
    elif args.top_volume:
        # was u.top_volume
        syms = u.top_by_volume(args.top_volume)
    elif args.top_mcap:
        # UniverseBuilder doesn't have mcap logic yet, fallback or remove
        print("Warning: Market Cap filter not implemented in UniverseBuilder yet. Using Price.")
        syms = u.top_by_price(args.top_mcap)
    else:
        print("ERROR: choose one of --top-price / --top-volume")
        return

    # FIX: UniverseBuilder.save_symbols is static, or u.save_symbols instance method?
    # Your universe.py has a static method save_symbols.
    # But u.save() is not defined in the universe.py you uploaded.
    # Use the class method directly:
    UniverseBuilder.save_symbols(syms, "symbols.txt")
    
    # Print for user
    print("\n".join(syms))

def cmd_download(args):
    """Download historical data to CSV."""
    # Import locally to avoid circular dependencies
    from data_downloader import fetch_history
    
    fetch_history(
        symbol=args.symbol,
        from_date=args.from_date,
        to_date=args.to_date,
        interval=args.interval
    )


def cmd_engine(args):
    """
    Run live engine.

    For now this uses your existing live.engine.LiveEngine, which
    expects:
      - symbols: list[str]
      - timeframe: str (e.g. '5m')
      - min_bars: int
      - sizing_mode: str
      - risk_pct, stop_loss_pct, target_pct: floats
      - place_order: bool
    """
    from live.engine import LiveEngine  # delayed import to avoid side-effects

    # Load symbols
    if args.symbol:
        symbols = [args.symbol.strip().upper()]
    else:
        with open(args.symbols_file, "r") as f:
            symbols = [line.strip().upper() for line in f if line.strip()]

    print("[CLI] Starting engine for multi-symbol basket...")
    print("  symbols   =", symbols)
    print("  timeframe =", args.timeframe)
    print("  product   =", args.product)
    print("  place_order =", args.place_order)

    engine = LiveEngine(
        symbols=symbols,
        timeframe=args.timeframe,
        min_bars=args.min_bars,
        sizing_mode=args.sizing_mode,
        risk_pct=args.risk_pct,
        stop_loss_pct=args.stop_loss_pct,
        target_pct=args.target_pct,
        place_order=args.place_order,
    )

    engine.start()


def cmd_backtest(args):
    """
    Run a backtest on a CSV file.

    Example:
      python cli.py backtest --csv data/SBIN_5m.csv --symbol SBIN --exchange NSE --timeframe 5m
    """
    try:
        from backtest.engine import run_backtest
    except ImportError:
        print("[CLI] Backtest engine missing (backtest/engine.py not found or import error)")
        sys.exit(1)

    run_backtest(
        csv_path=args.csv,
        symbol=args.symbol,
        exchange=args.exchange,
        timeframe=args.timeframe,
    )


# ===========================================================
# ARGUMENT PARSER
# ===========================================================

def build_parser():
    parser = argparse.ArgumentParser(
        prog="cli.py",
        description="Algo trading CLI (Zerodha + multi-module stack)",
    )
    sub = parser.add_subparsers(dest="command")
    sub.required = True

    # ---------------- LOGIN ----------------
    p_login = sub.add_parser("login", help="Show Zerodha login URL")
    p_login.set_defaults(func=cmd_login)

    # ---------------- TOKEN ----------------
    p_token = sub.add_parser("token", help="Exchange request_token for access_token")
    p_token.add_argument(
        "request_token",
        help="Request token from browser redirect",
    )
    p_token.set_defaults(func=cmd_token)

    # ------------- INSTRUMENTS REFRESH -------------
    p_inst = sub.add_parser(
        "instruments-refresh",
        help="Refresh full instruments dump and cache",
    )
    p_inst.set_defaults(func=cmd_instruments_refresh)

    # ---------------- ACCOUNT ----------------
    p_acc = sub.add_parser("account", help="Show account snapshot")
    p_acc.set_defaults(func=cmd_account)

    # ---------------- UNIVERSE ----------------
    p_uni = sub.add_parser("universe", help="Build symbol universe into symbols.txt")
    p_uni.add_argument(
        "--top-price",
        type=int,
        dest="top_price",
        help="Select top N by last traded price",
    )
    p_uni.add_argument(
        "--top-volume",
        type=int,
        dest="top_volume",
        help="Select top N by traded volume",
    )
    p_uni.add_argument(
        "--top-mcap",
        type=int,
        dest="top_mcap",
        help="Select top N by market cap (if available)",
    )
    p_uni.set_defaults(func=cmd_universe)

	# ---------------- ENGINE ----------------
    p_eng = sub.add_parser("engine", help="Run live trading engine")

    grp = p_eng.add_mutually_exclusive_group(required=True)
    grp.add_argument(
        "--symbol",
        help="Single symbol, e.g. SBIN",
    )
    grp.add_argument(
        "--symbols-file",
        help="Path to file with one symbol per line (e.g. symbols.txt)",
    )

    p_eng.add_argument(
        "--timeframe",
        default="5m",
        help="Bar timeframe, e.g. 5m / 15m / 1d (default: 5m)",
    )
    p_eng.add_argument(
        "--min-bars",
        type=int,
        default=20,
        dest="min_bars",
        help="Minimum history bars before trading (default: 20)",
    )
    p_eng.add_argument(
        "--product",
        default="MIS",
        help="Zerodha product type (MIS/CNC/NRML etc)",
    )
    p_eng.add_argument(
        "--sizing-mode",
        default="risk_pct",
        dest="sizing_mode",
        help="Position sizing mode (default: risk_pct)",
    )
    p_eng.add_argument(
        "--risk-pct",
        type=float,
        default=1.0,
        dest="risk_pct",
        help="Risk % per trade (default: 1.0)",
    )
    p_eng.add_argument(
        "--stop-loss-pct",
        type=float,
        default=1.0,
        dest="stop_loss_pct",
        help="Stop loss % (default: 1.0)",
    )
    p_eng.add_argument(
        "--target-pct",
        type=float,
        default=2.0,
        dest="target_pct",
        help="Target % (default: 2.0)",
    )
    p_eng.add_argument(
        "--place-order",
        action="store_true",
        help="If set, actually place orders (otherwise paper-trade)",
    )
    p_eng.set_defaults(func=cmd_engine)

	# ---------------- DOWNLOAD ----------------
    p_dl = sub.add_parser("download", help="Download historical data from Zerodha")
    p_dl.add_argument("--symbol", required=True, help="Symbol (e.g., SBIN)")
    p_dl.add_argument("--from-date", required=True, help="YYYY-MM-DD")
    p_dl.add_argument("--to-date", required=True, help="YYYY-MM-DD")
    p_dl.add_argument("--interval", default="5m", help="minute, 3m, 5m, 15m, 60m, day")
    p_dl.set_defaults(func=cmd_download)

    # ---------------- BACKTEST ----------------
    p_bt = sub.add_parser("backtest", help="Run backtest on CSV data")
    p_bt.add_argument(
        "--csv",
        required=True,
        help="Path to CSV file with historical bars",
    )
    p_bt.add_argument(
        "--symbol",
        required=True,
        help="Symbol name in CSV / for reporting",
    )
    p_bt.add_argument(
        "--exchange",
        default="NSE",
        help="Exchange for the symbol (default: NSE)",
    )
    p_bt.add_argument(
        "--timeframe",
        default="1d",
        help="Timeframe of the CSV data (default: 1d)",
    )
    p_bt.set_defaults(func=cmd_backtest)

    return parser


def main():
    parser = build_parser()
    args = parser.parse_args()
    # Every subcommand sets a `func` to call
    args.func(args)


if __name__ == "__main__":
    main()
