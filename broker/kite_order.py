# broker/kite_order.py
import logging
from broker.kite_auth import get_kite

def place_order(
    symbol: str,
    transaction_type: str,
    quantity: int,
    product: str = "MIS",
    order_type: str = "LIMIT",  # Changed default to LIMIT
    price: float = 0.0,         # Required for LIMIT orders
    trigger_price: float = 0.0,
    exchange: str = "NSE",
    tag: str = "algo",
    kite = None
):
    """
    Places a LIMIT order by default to avoid slippage.
    """
    if kite is None:
        try:
            kite = get_kite()
        except Exception as e:
            print(f"[Order] Critical: Could not connect to Kite: {e}")
            raise e

    # SAFETY LOCK (Remove this if you want to trade full quantity)
    if quantity > 1:
        print(f"[Safety] Reducing Quantity from {quantity} to 1.")
        quantity = 1

    print(f"[Order] Sending: {transaction_type} {quantity} {symbol} @ {price} ({product})")

    try:
        order_id = kite.place_order(
            variety=kite.VARIETY_REGULAR,
            exchange=exchange,
            tradingsymbol=symbol,
            transaction_type=transaction_type,
            quantity=quantity,
            product=product,
            order_type=order_type,
            price=price,
            trigger_price=trigger_price,
            tag=tag
        )
        print(f"[Order] SUCCESS. ID: {order_id}")
        return order_id
    except Exception as e:
        print(f"[Order] Failed: {e}")
        return None

def place_gtt(
    symbol: str,
    exchange: str,
    transaction_type: str,
    quantity: int,
    price: float,
    stop_loss_price: float,
    target_price: float,
    kite = None
):
    """
    Places an OCO (One-Cancels-Other) GTT for Stop Loss and Target.
    This runs on Zerodha's server, so it works even if your internet fails.
    """
    if kite is None:
        kite = get_kite()

    # Determine type (BUY positions need SELL GTT, and vice versa)
    gtt_type = kite.TRANSACTION_TYPE_SELL if transaction_type == "BUY" else kite.TRANSACTION_TYPE_BUY
    
    # Create the OCO Trigger
    # Trigger 1: Stop Loss
    # Trigger 2: Target
    try:
        trigger_id = kite.place_gtt(
            trigger_type=kite.GTT_TYPE_OCO,
            tradingsymbol=symbol,
            exchange=exchange,
            trigger_values=[stop_loss_price, target_price],
            last_price=price,
            orders=[
                {
                    "exchange": exchange,
                    "tradingsymbol": symbol,
                    "transaction_type": gtt_type,
                    "quantity": quantity,
                    "order_type": "LIMIT",
                    "product": "CNCO",  # GTT usually creates CNC/NRML orders, but works for exiting
                    "price": stop_loss_price,
                },
                {
                    "exchange": exchange,
                    "tradingsymbol": symbol,
                    "transaction_type": gtt_type,
                    "quantity": quantity,
                    "order_type": "LIMIT",
                    "product": "CNCO",
                    "price": target_price,
                }
            ]
        )
        print(f"[GTT] Safety Set! SL: {stop_loss_price}, TGT: {target_price}. ID: {trigger_id}")
        return trigger_id
    except Exception as e:
        print(f"[GTT] Failed to set safety: {e}")
        return None
