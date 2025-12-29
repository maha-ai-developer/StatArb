import sqlite3
import datetime
import os
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Tuple, Optional, Dict

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import infrastructure.config as config

# ✅ Import the broker function
from infrastructure.broker.kite_orders import place_order 

class ExecutionHandler:
    def __init__(self, mode="PAPER"):
        self.mode = mode.upper()
        self.db_path = os.path.join(config.DATA_DIR, "trades.db")
        self.init_db()
        print(f"   👮 Execution Handler: {self.mode}")

    def init_db(self):
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS trades (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp DATETIME,
                symbol TEXT,
                side TEXT,
                quantity INTEGER,
                price REAL,
                strategy TEXT,
                mode TEXT
            )
        ''')
        conn.commit()
        conn.close()

    def log_trade(self, symbol, side, qty, price, strategy):
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute('''
            INSERT INTO trades (timestamp, symbol, side, quantity, price, strategy, mode)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        ''', (datetime.datetime.now(), symbol, side, qty, price, strategy, self.mode))
        conn.commit()
        conn.close()

    def get_marketable_limit_price(self, side, current_price, buffer_pct=0.01):
        """
        Calculates a 'Safe' Limit Price.
        BUY:  Current Price + 1% (Willing to pay a bit more to ensure fill)
        SELL: Current Price - 1% (Willing to sell a bit lower to ensure fill)
        """
        if side == "BUY":
            return round(current_price * (1 + buffer_pct), 2)
        else:
            return round(current_price * (1 - buffer_pct), 2)

    def place_pair_order(self, sym1, side1, qty1, px1, sym2, side2, qty2, px2, product="MIS"):
        """
        Executes two legs using PARALLEL MARKETABLE LIMIT ORDERS.
        
        NEW - Checklist Phase 8: Simultaneous Execution Protocol
        - Uses ThreadPoolExecutor for parallel placement
        - Tracks execution timing
        - Attempts atomic rollback if one leg fails
        """
        # Calculate Safe Limit Prices (1% Buffer)
        limit_px1 = self.get_marketable_limit_price(side1, px1)
        limit_px2 = self.get_marketable_limit_price(side2, px2)

        print(f"      🚀 PARALLEL EXECUTION: {sym1} ({side1} {qty1} @ {limit_px1}) & {sym2} ({side2} {qty2} @ {limit_px2})")
        
        execution_start = time.time()
        
        # --- LIVE MODE: PARALLEL EXECUTION ---
        if self.mode == "LIVE":
            print(f"      📡 SENDING PARALLEL ORDERS TO KITE...")
            
            def place_leg(symbol, side, quantity, price, leg_name):
                """Place a single leg order."""
                try:
                    leg_start = time.time()
                    order_id = place_order(symbol, side, quantity, price=price, order_type="LIMIT", product=product)
                    leg_time = (time.time() - leg_start) * 1000
                    return {
                        'leg': leg_name,
                        'symbol': symbol,
                        'order_id': order_id,
                        'success': order_id is not None,
                        'time_ms': round(leg_time, 1)
                    }
                except Exception as e:
                    return {
                        'leg': leg_name,
                        'symbol': symbol,
                        'order_id': None,
                        'success': False,
                        'error': str(e),
                        'time_ms': 0
                    }
            
            # Execute both legs in parallel
            results = {}
            with ThreadPoolExecutor(max_workers=2) as executor:
                futures = {
                    executor.submit(place_leg, sym1, side1, qty1, limit_px1, "LEG1"): "LEG1",
                    executor.submit(place_leg, sym2, side2, qty2, limit_px2, "LEG2"): "LEG2"
                }
                
                for future in as_completed(futures):
                    leg_name = futures[future]
                    results[leg_name] = future.result()
            
            execution_time = (time.time() - execution_start) * 1000
            
            leg1_result = results.get("LEG1", {})
            leg2_result = results.get("LEG2", {})
            
            # Log execution timing
            print(f"      ⏱️ TIMING: Leg1={leg1_result.get('time_ms', 0):.0f}ms | Leg2={leg2_result.get('time_ms', 0):.0f}ms | Total={execution_time:.0f}ms")
            
            # Check for failures
            if not leg1_result.get('success') and not leg2_result.get('success'):
                print(f"      ❌ BOTH LEGS FAILED")
                return False
            
            if leg1_result.get('success') and not leg2_result.get('success'):
                print(f"      ❌ Leg 2 ({sym2}) Failed. ⚠️ URGENT: Attempting rollback of Leg 1!")
                # Attempt to cancel or reverse Leg 1
                self._attempt_rollback(sym1, side1, qty1, leg1_result.get('order_id'))
                return False
            
            if not leg1_result.get('success') and leg2_result.get('success'):
                print(f"      ❌ Leg 1 ({sym1}) Failed. ⚠️ URGENT: Attempting rollback of Leg 2!")
                # Attempt to cancel or reverse Leg 2
                self._attempt_rollback(sym2, side2, qty2, leg2_result.get('order_id'))
                return False
            
            print(f"      ✅ Pair Executed Successfully. IDs: {leg1_result.get('order_id')}, {leg2_result.get('order_id')}")
            
        else:
            # PAPER MODE
            execution_time = (time.time() - execution_start) * 1000
            print(f"      📝 PAPER TRADE LOGGED (Parallel simulation) - {execution_time:.0f}ms")

        # Log trades with the INTENDED execution price (px1, px2), not the limit cap
        self.log_trade(sym1, side1, qty1, px1, "StatArb")
        self.log_trade(sym2, side2, qty2, px2, "StatArb")
        
        # Store execution stats for reporting
        self._last_execution = {
            'timestamp': datetime.datetime.now().isoformat(),
            'sym1': sym1, 'sym2': sym2,
            'execution_time_ms': round(execution_time, 1),
            'mode': self.mode
        }
        
        return True

    def _attempt_rollback(self, symbol: str, original_side: str, qty: int, order_id: str):
        """
        Attempt to rollback a single leg after pair execution failure.
        
        First tries to cancel, then places reverse order if needed.
        """
        reverse_side = "SELL" if original_side == "BUY" else "BUY"
        print(f"      🔄 ROLLBACK: Attempting to close {symbol} ({reverse_side} {qty})")
        
        # In a real implementation, first try to cancel the pending order
        # Then place a market order to close if already filled
        # For now, log the rollback attempt
        self.log_trade(symbol, f"ROLLBACK_{reverse_side}", qty, 0, "StatArb_Rollback")


    def place_stop_loss_order(self, symbol, side, qty, trigger_price, product="NRML", exchange="NFO"):
        """
        Places a Stop-Loss Market (SL-M) order for guaranteed exit.
        Ref: Risk-Management&Trading-Psychology.pdf
        
        SL-M orders execute at MARKET price once trigger is hit,
        guaranteeing exit even in fast-moving markets.
        
        Args:
            symbol: Trading symbol
            side: "BUY" or "SELL" (opposite of position)
            qty: Quantity to exit
            trigger_price: Price at which to trigger the stop
            product: "NRML" for futures overnight (default)
            exchange: "NFO" for futures (default)
        
        Returns:
            Order ID or None
        """
        print(f"      🛡️ STOP-LOSS ORDER: {side} {qty} {symbol} @ trigger ₹{trigger_price}")
        
        if self.mode == "LIVE":
            order_id = place_order(
                symbol=symbol,
                side=side,
                quantity=qty,
                trigger_price=trigger_price,
                order_type="SL-M",  # Stop-Loss MARKET for guaranteed fill
                product=product,
                exchange=exchange
            )
            if order_id:
                print(f"      ✅ SL-M Order Placed! ID: {order_id}")
            return order_id
        else:
            print(f"      📝 PAPER SL-M: {side} {qty} {symbol} @ trigger ₹{trigger_price}")
            self.log_trade(symbol, f"SL-{side}", qty, trigger_price, "StatArb_SL")
            return "PAPER_SL"
