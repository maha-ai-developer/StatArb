import threading
import time
from datetime import datetime

class Dashboard(threading.Thread):
    """
    Thread 6: Live CLI dashboard.
    Reads:
        - last N candles from SignalEngine
        - last N signals
        - last N trades
        - current positions
        - P&L from PnlMonitor
    """

    def __init__(self, engine_ref, interval_sec=10):
        super().__init__(daemon=True)
        self.engine = engine_ref
        self.interval_sec = interval_sec

    def run(self):
        while True:
            try:
                candles = self.engine.get_recent_candles(5)
                signals = self.engine.get_recent_signals(5)
                trades = self.engine.get_recent_trades(5)
                pnl = self.engine.get_current_pnl()

                print("\n=================== DASHBOARD ===================")
                print(f"Time: {datetime.now().strftime('%H:%M:%S')}")

                print("\n--- Last 5 Candles ---")
                for c in candles:
                    print(f"{c['start']}  O:{c['open']} H:{c['high']} L:{c['low']} C:{c['close']} Vol:{c['volume']}")

                print("\n--- Last 5 Signals ---")
                for s in signals:
                    print(f"{s}")

                print("\n--- Last 5 Trades ---")
                for t in trades:
                    print(f"{t}")

                print("\n--- P&L ---")
                print(f"Realized: {pnl['realized']}")
                print(f"Unrealized: {pnl['unrealized']}")
                print(f"Total: {pnl['total']}")
                print("================================================")

            except Exception as e:
                print("[Dashboard] Error:", e)

            time.sleep(self.interval_sec)
