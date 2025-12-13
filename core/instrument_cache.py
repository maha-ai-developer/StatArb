# core/instrument_cache.py

import json
import os

CACHE_PATH = "cache/instruments.json"

# -------------------------------------------------------------
def load_instruments():
    if not os.path.exists(CACHE_PATH):
        return []

    with open(CACHE_PATH, "r") as f:
        try:
            return json.load(f)
        except:
            return []


# -------------------------------------------------------------
def save_instruments(data):
    # FIX: convert non-serializable fields
    for inst in data:
        for k, v in list(inst.items()):
            if hasattr(v, "isoformat"):  # dates
                inst[k] = v.isoformat()

    with open(CACHE_PATH, "w") as f:
        json.dump(data, f, indent=2)


# -------------------------------------------------------------
def refresh_instrument_cache():
    """
    Downloads instruments via KiteConnect() and saves JSON.
    Called by: cli.py instruments-refresh
    """
    from broker.kite_auth import get_kite

    kite = get_kite()
    print("[InstrumentCache] Fetching instruments from Kite API...")
    inst = kite.instruments()

    save_instruments(inst)
    print(f"[InstrumentCache] Saved instrument cache ({len(inst)} entries).")
    return inst


# -------------------------------------------------------------
def get_instrument_token(symbol: str):
    """
    Lookup token by `tradingsymbol`.
    Required by FeedWorker, SignalEngine, ExecutionEngine.
    """
    inst = load_instruments()

    sym = symbol.strip().upper()

    for row in inst:
        if row.get("tradingsymbol", "").upper() == sym:
            return row.get("instrument_token")

    raise KeyError(f"[InstrumentCache] Symbol '{symbol}' not found in cache. "
                   "Run: python cli.py instruments-refresh")


# -------------------------------------------------------------
def get_symbol_list():
    """Returns all tradingsymbols."""
    inst = load_instruments()
    return [row.get("tradingsymbol") for row in inst]
