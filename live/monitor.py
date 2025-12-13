# live/monitor.py

import threading
import time
from datetime import datetime, time as dtime
from broker.kite_positions import fetch_account_snapshot
from broker.kite_order import place_order

class RiskMonitor(threading.Thread):
    def __init__(self, interval_sec: int = 30, portfolio_manager=None):
        super().__init__(daemon=True)
        self.interval_sec = interval_sec
        self.portfolio_manager = portfolio_manager
        self.auto_squareoff_done = False

    def _is_squareoff_time(self):
        # STRICT TIME: 3:20 PM
        now = datetime.now().time()
        return now >= dtime(15, 20)

    def _auto_squareoff_mis(self):
        if self.auto_squareoff_done: return
        
        print("\n[RiskMonitor] 🚨 15:20 AUTO-SQUAREOFF TRIGGERED 🚨")
        try:
            _, _, _, positions = fetch_account_snapshot()
            day_pos = positions.get("day", []) if positions else []
            
            # Filter only Open MIS positions
            open_mis = [p for p in day_pos if p['product'] == 'MIS' and p['quantity'] != 0]

            if not open_mis:
                print("[RiskMonitor] No open MIS positions. Safe.")
                self.auto_squareoff_done = True
                return

            print(f"[RiskMonitor] Closing {len(open_mis)} positions to avoid penalty...")
            
            for p in open_mis:
                symbol = p['tradingsymbol']
                qty = p['quantity']
                side = "SELL" if qty > 0 else "BUY"
                
                print(f" -> Closing {symbol} ({side} {abs(qty)})")
                place_order(
                    symbol=symbol,
                    transaction_type=side,
                    quantity=abs(qty),
                    product="MIS",
                    order_type="MARKET", # Market order to exit FAST
                    exchange="NSE"
                )
            
            self.auto_squareoff_done = True
            print("[RiskMonitor] All positions closed.")

        except Exception as e:
            print(f"[RiskMonitor] Squareoff Error: {e}")

    def run(self):
        print(f"[RiskMonitor] Monitoring started (Tick: {self.interval_sec}s)")
        while True:
            try:
                if self.portfolio_manager:
                    self.portfolio_manager.update_snapshot()
                    eq = self.portfolio_manager.cached_equity
                    print(f"\n[RiskMonitor] Equity: {eq:.2f} | Positions: {len(self.portfolio_manager.open_trades)}")

                # Check Time
                if self._is_squareoff_time():
                    self._auto_squareoff_mis()

            except Exception as e:
                print(f"[RiskMonitor] Error: {e}")
            
            time.sleep(self.interval_sec)
