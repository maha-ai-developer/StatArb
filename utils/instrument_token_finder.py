#!/usr/bin/env python3
"""
instrument_token_finder.py

Utility script to fetch & search instrument tokens from Zerodha Kite Connect API.

Features:
- Load all instruments from Kite.
- Search by trading symbol (exact).
- Search by partial symbol name.
- Filter by exchange.
- CLI-friendly output.

Usage:
    python instrument_token_finder.py SBIN --exchange NSE
    python instrument_token_finder.py REL --search
    python instrument_token_finder.py --list
"""

import argparse
from tabulate import tabulate
from broker.kite_auth import get_kite


def fetch_instruments():
    """
    Fetch all instruments from Kite.
    Returns a list of dicts.
    """
    kite = get_kite()
    print("[INFO] Fetching full instruments list from Kite...")
    instruments = kite.instruments()  # ~6MB list
    print(f"[INFO] Loaded {len(instruments)} instruments.")
    return instruments


def search_exact(symbol: str, exchange: str, instruments):
    """
    Exact match (e.g., SBIN @ NSE).
    """
    symbol = symbol.upper()
    exchange = exchange.upper()

    results = [
        ins for ins in instruments
        if ins["tradingsymbol"] == symbol and ins["exchange"] == exchange
    ]
    return results


def search_partial(query: str, instruments):
    """
    Partial match (e.g., REL -> RELIANCE, RELINFRA…)
    """
    query = query.upper()
    return [
        ins for ins in instruments
        if query in ins["tradingsymbol"].upper()
    ]


def list_all_for_exchange(exchange: str, instruments):
    """
    List all instruments for an exchange.
    """
    exchange = exchange.upper()
    return [
        ins for ins in instruments
        if ins["exchange"] == exchange
    ]


def print_results(results):
    if not results:
        print("\n[NO MATCHES FOUND]")
        return

    table = []
    for r in results:
        table.append([
            r["exchange"],
            r["tradingsymbol"],
            r["instrument_token"],
            r["tick_size"],
            r["lot_size"],
            r["segment"],
            r["instrument_type"],
        ])

    print(
        tabulate(
            table,
            headers=[
                "Exchange",
                "Symbol",
                "Token",
                "Tick",
                "Lot",
                "Segment",
                "Type",
            ],
            tablefmt="fancy_grid"
        )
    )


def main():
    parser = argparse.ArgumentParser(
        description="Zerodha Instrument Token Finder"
    )

    parser.add_argument(
        "symbol",
        nargs="?",
        help="Trading symbol to search (e.g., SBIN, INFY)"
    )

    parser.add_argument(
        "--exchange",
        default="NSE",
        help="Exchange (NSE/BSE/NFO/CDS/MCX/etc.)"
    )

    parser.add_argument(
        "--search",
        action="store_true",
        help="Enable partial search mode"
    )

    parser.add_argument(
        "--list",
        action="store_true",
        help="List *all* instruments for an exchange"
    )

    args = parser.parse_args()

    # Load instruments
    instruments = fetch_instruments()

    # List mode
    if args.list:
        results = list_all_for_exchange(args.exchange, instruments)
        print_results(results)
        return

    # No symbol?
    if not args.symbol:
        print("\n[ERROR] No symbol provided.\nUse: python instrument_token_finder.py SBIN")
        return

    # Partial search
    if args.search:
        results = search_partial(args.symbol, instruments)
        print_results(results)
        return

    # Exact search
    results = search_exact(args.symbol, args.exchange, instruments)
    print_results(results)


if __name__ == "__main__":
    main()
