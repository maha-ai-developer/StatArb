# live/signal_engine.py

import threading
from queue import Queue, Empty
from typing import Dict, Any
import pandas as pd

# Import your real strategy
from strategies.combined_stack import CombinedStack


class SignalEngine(threading.Thread):
    """
    SINGLE WORKER that handles signals for ALL symbols.
    Consumes bars from a shared queue and applies the 'CombinedStack' strategy.
    """

    def __init__(
        self,
        in_queue: Queue,   # Shared queue receiving bars for ALL symbols
        out_queue: Queue,  # Queue to send signals to ExecutionEngine
        min_bars: int = 30, # Increased default to match strategy requirements
    ):
        super().__init__(daemon=True)
        self.in_queue = in_queue
        self.out_queue = out_queue
        self.min_bars = min_bars
        self._stop_flag = threading.Event()
        
        # State: symbol -> DataFrame
        self.history: Dict[str, pd.DataFrame] = {}

        # Initialize the Real Strategy
        self.strategy = CombinedStack(intraday=True, swing=True, use_ml=False)
        print(f"[SignalEngine] Loaded Strategy: {self.strategy.name}")

    def stop(self):
        self._stop_flag.set()

    # ------------------------------------------------------------------
    # MAIN LOGIC: Process Bar & Check for Signals
    # ------------------------------------------------------------------
    def _process_bar(self, bar: dict, is_warmup: bool = False):
        """
        Updates history for a specific symbol and checks for signals.
        is_warmup: If True, calculates indicators but DOES NOT emit signals.
        """
        symbol = bar["symbol"]
        
        # 1. Initialize DF if needed (With explicit types to fix FutureWarning)
        if symbol not in self.history:
            self.history[symbol] = pd.DataFrame({
                "end": pd.Series(dtype="datetime64[ns]"),
                "open": pd.Series(dtype="float64"),
                "high": pd.Series(dtype="float64"),
                "low": pd.Series(dtype="float64"),
                "close": pd.Series(dtype="float64"),
                "volume": pd.Series(dtype="float64"),
            })

        # 2. Create new row
        new_row = pd.DataFrame([{
            "end": pd.to_datetime(bar["end"]),
            "open": float(bar["open"]),
            "high": float(bar["high"]),
            "low": float(bar["low"]),
            "close": float(bar["close"]),
            "volume": float(bar["volume"])
        }])
        
        # 3. Append safely (Fixes 'FutureWarning' about empty concat)
        if self.history[symbol].empty:
            self.history[symbol] = new_row
        else:
            self.history[symbol] = pd.concat([self.history[symbol], new_row], ignore_index=True)
        
        # Keep buffer manageable (Last 300 bars is plenty for indicators)
        if len(self.history[symbol]) > 300:
             self.history[symbol] = self.history[symbol].iloc[-300:]

        df = self.history[symbol]

        # 4. Check Strategy (Only if we have enough data)
        # We use 30 as the safe minimum for EMA21/MACD
        if len(df) < max(self.min_bars, 30):
            return

        try:
            # CALL REAL STRATEGY
            side = self.strategy.generate_signal(df)

            # ----------------------------------------------------------
            # CRITICAL: If this is warmup, DO NOT TRADE.
            # ----------------------------------------------------------
            if is_warmup:
                # We calculated the math (so indicators are ready) but we exit here.
                return 

            # If strategy returns BUY or SELL, emit the signal
            if side in ["BUY", "SELL"]:
                last = df.iloc[-1]
                
                # Construct Signal Packet
                sig = {
                    "symbol": symbol,
                    "time": last["end"],
                    "side": side,
                    "price": float(last["close"]),
                    "final_signal": side, 
                    "meta": {
                        "reason": "CombinedStack_Signal", 
                        "strategy": self.strategy.name
                    }
                }
                
                print(f"[Signal] {symbol} {side} @ {last['close']}")
                self.out_queue.put(sig)
                
        except Exception as e:
            print(f"[SignalEngine] Strategy Error on {symbol}: {e}")

    # ------------------------------------------------------------------
    # WARMUP HELPER
    # ------------------------------------------------------------------
    def inject_warmup_bar(self, bar: dict):
        """
        Directly updates history without triggering 'Live' signals.
        """
        try:
            # Pass is_warmup=True to prevent accidental trading!
            self._process_bar(bar, is_warmup=True)
        except Exception as e:
            print(f"[SignalEngine] Warmup Error: {e}")

    def run(self):
        print("[SignalEngine] Worker started (Real Strategy Active)")
        while not self._stop_flag.is_set():
            try:
                # Wait for ANY bar from ANY symbol
                bar = self.in_queue.get(timeout=1.0)
                # Live bars = False (This enables trading)
                self._process_bar(bar, is_warmup=False) 
            except Empty:
                continue
            except Exception as e:
                print(f"[SignalEngine] Critical Worker Error: {e}")
