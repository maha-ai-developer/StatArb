# risk/sizing.py
import math
from broker.kite_positions import fetch_account_snapshot

def size_by_atr(equity: float, risk_pct: float, atr: float, stop_loss_multiplier: float = 2.0) -> int:
    """
    Calculate quantity based on Volatility (ATR).
    High Volatility = Lower Quantity.
    """
    if atr <= 0 or equity <= 0: return 0

    # 1. Determine Risk Amount (e.g., 1% of 1,00,000 = ₹1,000)
    risk_amount = equity * (risk_pct / 100.0)

    # 2. Determine Stop Loss Distance (e.g., 2 * ATR)
    stop_loss_dist = atr * stop_loss_multiplier

    # 3. Calculate Quantity
    if stop_loss_dist == 0: return 0
    qty = int(risk_amount / stop_loss_dist)
    
    return qty

def size_by_risk_pct(price: float, risk_pct: float, stop_loss_pct: float = 1.0) -> int:
    """
    Calculates quantity based on Fixed % Stop Loss.
    """
    if price <= 0: return 0

    # 1. Fetch Live Capital
    net_equity = 0.0
    try:
        _, margins, _, _ = fetch_account_snapshot()
        if margins:
            net_equity = float(margins.get("net", margins.get("equity", 0.0)))
    except:
        pass
    
    # Fallback
    if net_equity <= 0: net_equity = 15000.0 

    # 2. Hard Affordability Check (4x Leverage)
    max_buying_power = net_equity * 4.0
    if price > max_buying_power:
        return 0

    # 3. Risk-Based Sizing
    risk_amount = net_equity * (risk_pct / 100.0)
    loss_per_share = price * (stop_loss_pct / 100.0)
    
    if loss_per_share <= 0: return 0

    qty = math.floor(risk_amount / loss_per_share)

    if qty < 1: return 0

    return int(qty)
