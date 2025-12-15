import threading
from broker.kite_order import place_order, place_gtt
from broker.kite_positions import fetch_account_snapshot, fetch_ltp
# IMPORT BOTH SIZING FUNCTIONS
from risk.sizing import size_by_risk_pct, size_by_atr
from risk.portfolio import PortfolioManager, PortfolioLimits
from db.pg import init_db

# --- 🛡️ SAFETY CONFIGURATION ---
SAFETY_MODE = True  # True = Force 1 Qty. False = Real Size.
# -------------------------------

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
        portfolio_limits: PortfolioLimits = None,
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

    # UPDATED: Compute Qty accepts optional ATR
    def _compute_qty(self, price: float, atr: float = 0.0) -> int:
        
        # 1. ATR Sizing (Priority)
        if atr > 0:
            try:
                # Fetch fresh equity for accurate sizing
                _, margins, _, _ = fetch_account_snapshot()
                equity = float(margins.get("net", 0))
            except:
                equity = 15000.0 # Safety Fallback
            
            qty = size_by_atr(equity, self.risk_pct, atr)
            print(f"[Sizing] Method: ATR ({atr:.2f}) -> Calculated: {qty}")
            return qty

        # 2. Fixed % Sizing (Fallback)
        qty = size_by_risk_pct(price, self.risk_pct, self.stop_loss_pct)
        print(f"[Sizing] Method: Fixed % -> Calculated: {qty}")
        return qty

    def _round_to_tick(self, price):
        return round(price / 0.05) * 0.05

    def _place_single_leg(self, symbol, side, qty, price=0):
        if not self.place_order:
            print(f"[Paper] {side} {qty} {symbol} @ {price}")
            return True
            
        order_type = "LIMIT" if price > 0 else "MARKET"
        
        try:
            order_id = place_order(
                symbol=symbol,
                transaction_type=side,
                quantity=qty,
                product=self.product,
                order_type=order_type,
                price=price,
                exchange="NSE"
            )
            if order_id:
                current_qty = self.portfolio.open_trades.get(symbol, 0)
                new_qty = qty if side == "BUY" else -qty
                self.portfolio.open_trades[symbol] = current_qty + new_qty
                return order_id
        except Exception as e:
            print(f"[Execution] FAILED {side} {symbol}: {e}")
        return None

    def _handle_signal(self, sig: dict):
        # ---------------------------------------------------------
        # 1. HANDLE PAIR TRADES
        # ---------------------------------------------------------
        if sig.get("type") == "PAIR_TRADE":
            sym_a = sig["symbol_a"]
            sym_b = sig["symbol_b"]
            direction = sig["direction"]
            z_score = sig.get("z_score", 0)
            
            print(f"\n[Tiger] 🐯 Trigger! {direction} on {sym_a}/{sym_b} (Z={z_score:.2f})")
            
            # Fetch prices
            ltp_a = fetch_ltp(sym_a)
            ltp_b = fetch_ltp(sym_b)
            
            if ltp_a == 0 or ltp_b == 0: return

            # Pairs Sizing
            capital_per_leg = 50000 
            qty_a = max(1, int(capital_per_leg / ltp_a))
            qty_b = max(1, int(capital_per_leg / ltp_b))

            if SAFETY_MODE:
                print(f"[Safety] Override Pairs Qty: {qty_a}/{qty_b} -> 1/1")
                qty_a = 1
                qty_b = 1
            
            if direction == "LONG_PAIR":
                self._place_single_leg(sym_a, "BUY", qty_a, price=ltp_a)
                self._place_single_leg(sym_b, "SELL", qty_b, price=ltp_b)
            elif direction == "SHORT_PAIR":
                self._place_single_leg(sym_a, "SELL", qty_a, price=ltp_a)
                self._place_single_leg(sym_b, "BUY", qty_b, price=ltp_b)
            return

        # ---------------------------------------------------------
        # 2. HANDLE MOMENTUM TRADES
        # ---------------------------------------------------------
        symbol = sig.get("symbol")
        side = sig.get("final_signal") # Expecting "BUY" or "SELL" from new dict strategy
        
        # Handle new dict format where signal is inside 'final_signal' or 'signal' key?
        # The engine puts strategy return into 'final_signal' usually.
        # But wait, our strategy returns {signal: "BUY", ...}. 
        # The Engine wrapper needs to handle this. Assuming Engine handles extraction.
        # Let's support both for safety:
        if isinstance(side, dict):
             side = side.get("signal")

        price = float(sig.get("price", 0))
        atr = float(sig.get("atr", 0))  # EXTRACT ATR

        if side not in ("BUY", "SELL"): return

        current_qty = self.portfolio.open_trades.get(symbol, 0)
        if (side == "BUY" and current_qty > 0) or (side == "SELL" and current_qty < 0):
            print(f"[ExecutionEngine] Skipping {side} {symbol}: Position already exists.")
            return

        # A. CALCULATE QUANTITY (PASS ATR)
        calculated_qty = self._compute_qty(price, atr)
        if calculated_qty <= 0: return

        # B. SAFETY OVERRIDE
        if SAFETY_MODE:
            print(f"[Safety] Risk Calc: {calculated_qty} qty. OVERRIDING to 1 qty.")
            qty = 1
        else:
            qty = calculated_qty

        # C. LIMIT PRICE
        buffer = price * 0.0015 
        raw_limit = price + buffer if side == "BUY" else price - buffer
        limit_price = self._round_to_tick(raw_limit)

        print(f"[Execution] Placing LIMIT {side} {qty} {symbol} @ {limit_price}")

        # D. EXECUTE
        if self._place_single_leg(symbol, side, qty, limit_price):
            # E. GTT (Stop Loss & Target)
            if side == "BUY":
                sl_price = self._round_to_tick(limit_price * (1 - self.stop_loss_pct / 100))
                tgt_price = self._round_to_tick(limit_price * (1 + self.target_pct / 100))
            else:
                sl_price = self._round_to_tick(limit_price * (1 + self.stop_loss_pct / 100))
                tgt_price = self._round_to_tick(limit_price * (1 - self.target_pct / 100))
            
            if self.place_order:
                place_gtt(symbol, "NSE", side, qty, limit_price, sl_price, tgt_price)

    def run(self):
        print(f"[ExecutionEngine] Active. Safety Mode: {'ON (1 Qty)' if SAFETY_MODE else 'OFF'}")
        while not self._stopped:
            try:
                sig = self.signal_queue.get(timeout=1)
                self._handle_signal(sig)
            except Exception: pass

    def stop(self):
        self._stopped = True
