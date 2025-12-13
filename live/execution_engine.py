# live/execution_engine.py

import threading
import traceback
from datetime import datetime

from broker.kite_order import place_order, place_gtt
from broker.kite_positions import fetch_account_snapshot
from risk.sizing import size_by_risk_pct
from risk.portfolio import PortfolioManager, PortfolioLimits
from db.pg import get_session, Trade, init_db

class ExecutionEngine(threading.Thread):
    def __init__(
        self,
        signal_queue,
        place_order: bool = False,
        product: str = "MIS",
        sizing_mode: str = "risk_pct",
        risk_pct: float = 1.0,
        stop_loss_pct: float = 1.0,
        target_pct: float = 2.0,
        portfolio_limits: PortfolioLimits | None = None,
    ):
        super().__init__(daemon=True)
        self.signal_queue = signal_queue
        self.place_order = place_order
        self.product = product
        self.risk_pct = risk_pct
        self.stop_loss_pct = stop_loss_pct
        self.target_pct = target_pct
        self._stopped = False
        self.portfolio = PortfolioManager(portfolio_limits)

        try:
            init_db("sqlite:///db/trades.db")
        except: pass
        self.sync_positions()

    def sync_positions(self):
        print("[ExecutionEngine] Syncing positions...")
        try:
            _, _, _, positions = fetch_account_snapshot()
            day_pos = positions.get('day', []) if positions else []
            for p in day_pos:
                if p['quantity'] != 0:
                    self.portfolio.open_trades[p['tradingsymbol']] = p['quantity']
        except: pass

    def _compute_qty(self, price: float) -> int:
        return size_by_risk_pct(price, self.risk_pct, self.stop_loss_pct)

    # ---------------------------------------------------
    # NEW HELPER: Round to nearest 0.05
    # ---------------------------------------------------
    def _round_to_tick(self, price):
        return round(price / 0.05) * 0.05

    def _handle_signal(self, sig: dict):
        symbol = sig["symbol"]
        side = sig["final_signal"]
        price = float(sig["price"])
        
        if side not in ("BUY", "SELL"): return

        # 1. ANTI-PYRAMIDING
        current_qty = self.portfolio.open_trades.get(symbol, 0)
        if (side == "BUY" and current_qty > 0) or (side == "SELL" and current_qty < 0):
            print(f"[ExecutionEngine] Skipping {side} {symbol}: Position already exists.")
            return

        qty = self._compute_qty(price)
        if qty <= 0: return

        # 2. CALCULATE LIMIT PRICE (Buffer 0.05%)
        # FIX: We now round this result to the nearest 0.05 tick
        buffer = price * 0.0005
        raw_limit = price + buffer if side == "BUY" else price - buffer
        limit_price = self._round_to_tick(raw_limit)

        # 3. CALCULATE STOP LOSS & TARGET
        # FIX: Round these to 0.05 as well
        if side == "BUY":
            sl_raw = limit_price * (1 - self.stop_loss_pct / 100)
            tgt_raw = limit_price * (1 + self.target_pct / 100)
        else:
            sl_raw = limit_price * (1 + self.stop_loss_pct / 100)
            tgt_raw = limit_price * (1 - self.target_pct / 100)

        sl_price = self._round_to_tick(sl_raw)
        tgt_price = self._round_to_tick(tgt_raw)

        # 4. PLACE ORDER
        if self.place_order:
            order_id = place_order(
                symbol=symbol,
                transaction_type=side,
                quantity=qty,
                product=self.product,
                order_type="LIMIT",
                price=limit_price,
                exchange="NSE"
            )
            
            if order_id:
                new_qty = qty if side == "BUY" else -qty
                self.portfolio.open_trades[symbol] = current_qty + new_qty
                
                print(f"[Execution] Placed {side} {qty} {symbol} @ {limit_price:.2f}")
                
                # 5. PLACE GTT
                place_gtt(
                    symbol=symbol,
                    exchange="NSE",
                    transaction_type=side,
                    quantity=qty,
                    price=limit_price,
                    stop_loss_price=sl_price,
                    target_price=tgt_price
                )

    def run(self):
        print("[ExecutionEngine] Active.")
        while not self._stopped:
            try:
                sig = self.signal_queue.get(timeout=1)
                self._handle_signal(sig)
            except Exception: pass

    def stop(self):
        self._stopped = True
