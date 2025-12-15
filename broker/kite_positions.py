# broker/kite_positions.py
from broker.kite_auth import get_kite

def fetch_account_snapshot():
    """
    Fetches live account data from Zerodha.
    
    CORRECTED RETURN FORMAT:
    Returns: (profile_dict, margins_dict, holdings_list, positions_dict)
    """
    try:
        kite = get_kite()
        
        # 1. Fetch Profile
        profile = kite.profile() or {}

        # 2. Fetch Margins (Funds) - This returns the crucial 'net' equity
        # Returns dict: {'net': 16390.0, 'available': {...}, 'equity': {...}}
        margins = kite.margins(segment="equity") or {}

        # 3. Fetch Holdings (Long Term)
        holdings = kite.holdings() or []

        # 4. Fetch Positions (Open Trades)
        # Returns dict: {'net': [...], 'day': [...]}\
        positions = kite.positions() or {}

        # Return the standard tuple expected by all consuming modules
        return profile, margins, holdings, positions

    except Exception as e:
        print(f"[Broker] Snapshot failed: {e}")
        # Return safe empty defaults in the correct structure
        return {}, {}, [], {}

def fetch_ltp(symbol, exchange="NSE"):
    """
    Fetches the Last Traded Price (LTP) for a given symbol.
    """
    try:
        kite = get_kite()
        instrument = f"{exchange}:{symbol}"
        quote = kite.ltp(instrument)
        
        if quote and instrument in quote:
            return quote[instrument]['last_price']
        else:
            print(f"[Broker] LTP not found for {instrument}")
            return 0.0
    except Exception as e:
        print(f"[Broker] LTP fetch failed: {e}")
        return 0.0
