import logging
from broker.kite_auth import get_kite

def place_order(
    symbol: str,
    transaction_type: str,
    quantity: int,
    product: str = "MIS",
    order_type: str = "LIMIT",
    price: float = 0.0,
    trigger_price: float = 0.0,
    exchange: str = "NSE",
    tag: str = "algo",
    kite = None
):
    """
    Places a regular Limit or Market order.
    """
    if kite is None:
        try:
            kite = get_kite()
        except Exception as e:
            print(f"[Order] Critical: Could not connect to Kite: {e}")
            return None

    print(f"[Order] Sending: {transaction_type} {quantity} {symbol} @ {price} ({order_type})")

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
    """
    if kite is None:
        kite = get_kite()

    # Determine counter-direction (If bought, we need SELL GTT)
    gtt_type = kite.TRANSACTION_TYPE_SELL if transaction_type == "BUY" else kite.TRANSACTION_TYPE_BUY
    
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
                    "product": "CNCO",
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
