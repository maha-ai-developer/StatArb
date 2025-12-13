# broker/kite_positions.py
from broker.kite_auth import get_kite

def fetch_account_snapshot():
    """
    Fetches live account data from Zerodha.
    Returns: profile, margins, holdings, positions
    """
    try:
        kite = get_kite()
        
        # 1. Fetch Margins (Funds)
        # Returns dict: {'net': 16390.0, 'available': ...}
        margins = kite.margins(segment="equity") or {}

        # 2. Fetch Positions (Open Trades)
        # Returns dict: {'net': [...], 'day': [...]}
        positions = kite.positions() or {}

        # 3. Fetch Holdings (Long Term)
        # Returns list: [...]
        holdings = kite.holdings() or []

        # Return in the order expected by your system
        return None, margins, holdings, positions

    except Exception as e:
        print(f"[Data] Snapshot failed: {e}")
        # Return empty safe defaults so nothing crashes
        return None, {}, [], {}
