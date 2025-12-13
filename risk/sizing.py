# risk/sizing.py
import math
from broker.kite_positions import fetch_account_snapshot

def size_by_risk_pct(price: float, risk_pct: float, stop_loss_pct: float = 1.0) -> int:
    """
    Calculates quantity based on Risk %.
    Returns 0 if the stock is too expensive or account balance is low.
    """
    if price <= 0: 
        return 0

    # 1. Fetch Live Capital
    net_equity = 0.0
    try:
        _, margins, _, _ = fetch_account_snapshot()
        if margins:
            # Try to get 'net' equity, fallback to 'equity' or 'available.cash'
            net_equity = float(margins.get("net", margins.get("equity", 0.0)))
    except:
        pass
    
    # Fallback if API fails (Safety net)
    if net_equity <= 0:
        net_equity = 15000.0 

    # 2. Hard Affordability Check (Margin)
    # MIS gives ~5x leverage. We use 4x to be safe.
    max_buying_power = net_equity * 4.0
    
    if price > max_buying_power:
        print(f"[Sizing] SKIP {price} > Max Buying Power {max_buying_power:.0f}")
        return 0

    # 3. Risk-Based Sizing
    # Example: Equity 15k, Risk 1% = ₹150 max loss.
    risk_amount = net_equity * (risk_pct / 100.0)
    
    # Loss per share if SL hits (e.g., 1% of 19000 = ₹190)
    loss_per_share = price * (stop_loss_pct / 100.0)
    
    if loss_per_share <= 0:
        return 0

    # Qty = 150 / 190 = 0.78 -> 0 shares
    qty = math.floor(risk_amount / loss_per_share)

    # 4. Strict Return (No forcing 1)
    if qty < 1:
        print(f"[Sizing] SKIP: Risk {risk_amount:.1f} < SL cost {loss_per_share:.1f}")
        return 0

    return int(qty)
