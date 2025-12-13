# live/engine.py

import time
from queue import Queue
from typing import List

from core.instrument_cache import get_instrument_token
from live.feed import FeedWorker
from live.signal_engine import SignalEngine
from live.execution_engine import ExecutionEngine
from live.pnl_monitor import PnLMonitor
from live.monitor import RiskMonitor
from live.warmup import fetch_warmup_data

class LiveEngine:
    """
    Hybrid Live Trading Engine v2.1 (With Warmup)
    """

    def __init__(
        self,
        symbols: List[str],
        timeframe: str,
        min_bars: int = 20,
        sizing_mode: str = "risk_pct",
        risk_pct: float = 1.0,
        stop_loss_pct: float = 1.0,
        target_pct: float = 2.0,
        place_order: bool = False,
        product: str = "MIS",
    ):
        self.symbols = symbols
        self.timeframe = timeframe
        self.min_bars = min_bars
        self.sizing_mode = sizing_mode
        self.risk_pct = risk_pct
        self.stop_loss_pct = stop_loss_pct
        self.target_pct = target_pct
        self.place_order = place_order
        self.product = product

        # Queues
        self.bar_queue = Queue()
        self.signal_queue = Queue()

        # Components (Initialized but not started)
        self.feed_worker = None
        
        self.signal_engine = SignalEngine(
            in_queue=self.bar_queue,
            out_queue=self.signal_queue,
            min_bars=min_bars
        )

        self.exec_worker = ExecutionEngine(
            signal_queue=self.signal_queue,
            place_order=self.place_order, # Fixed param name match
            product=self.product,
            sizing_mode=self.sizing_mode,
            risk_pct=self.risk_pct,
            stop_loss_pct=self.stop_loss_pct,
            target_pct=self.target_pct, # Added missing param
        )

        self.risk_monitor = None
        self.pnl_monitor = None

    # RENAMED FROM run() TO start() TO FIX ERROR
    def start(self):
        """
        Main startup sequence with History Warmup.
        """
        print(f"[LiveEngine] Initialized with:")
        print(f"  symbols={self.symbols}")
        print(f"  timeframe={self.timeframe}, min_bars={self.min_bars}")
        print(f"  sizing={self.sizing_mode} risk%={self.risk_pct}")
        print(f"  stop_loss={self.stop_loss_pct}% target={self.target_pct}%")
        print(f"  place_order={self.place_order} product={self.product}")

        print("[LiveEngine] Starting components...")

        # -------------------------------------------------------------
        # STEP 1: WARMUP (Time Travel)
        # -------------------------------------------------------------
        print("\n[Engine] ⏳ Starting History Warmup (downloading last 5 days)...")
        
        # 1. Fetch Data
        history_map = fetch_warmup_data(self.symbols, self.timeframe, lookback_days=5)
        
        # 2. Feed Data to Signal Engine
        total_candles = 0
        if history_map:
            print("[Engine] 🧠 Priming Strategy with historical data...")
            for symbol, candles in history_map.items():
                for c in candles:
                    # Convert Kite Candle -> Bot Bar Format
                    bar = {
                        "symbol": symbol,
                        "end": c["date"],
                        "open": c["open"],
                        "high": c["high"],
                        "low": c["low"],
                        "close": c["close"],
                        "volume": c["volume"]
                    }
                    # Inject directly (bypass queue for speed)
                    self.signal_engine.inject_warmup_bar(bar)
                    total_candles += 1
            print(f"[Engine] ✅ Warmup Complete. Processed {total_candles} candles.\n")
        else:
            print("[Engine] ⚠️ Warmup skipped or failed. Strategy will start empty.\n")

        # -------------------------------------------------------------
        # STEP 2: START THREADS
        # -------------------------------------------------------------

        # 1. Start Signal Engine (Now fully primed)
        self.signal_engine.start()

        # 2. Setup Feed (Live Data)
        tokens = []
        for s in self.symbols:
            t = get_instrument_token(s)
            if t:
                tokens.append(t)
        
        self.feed_worker = FeedWorker(
            tokens=tokens,
            symbols_map={t: s for s, t in zip(self.symbols, tokens) if t},
            bar_queue=self.bar_queue,
            timeframe=self.timeframe
        )
        self.feed_worker.start()

        # 3. Start Execution
        self.exec_worker.start()

        # 4. Start Monitors
        self.risk_monitor = RiskMonitor(
            interval_sec=30,
            portfolio_manager=self.exec_worker.portfolio
        )
        self.risk_monitor.start()

        self.pnl_monitor = PnLMonitor(interval_sec=15)
        self.pnl_monitor.start()

        print("[LiveEngine] All components started. Press Ctrl+C to stop.")

        # Keep main thread alive
        try:
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            print("\n[LiveEngine] Stopping...")
            self.stop()

    def stop(self):
        print("[LiveEngine] Shutting down threads...")
        if self.feed_worker: self.feed_worker.stop()
        if self.signal_engine: self.signal_engine.stop()
        if self.exec_worker: self.exec_worker.stop()
        print("[LiveEngine] Shutdown complete.")
