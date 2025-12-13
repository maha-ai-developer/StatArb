# live/pnl_monitor.py

import threading
import time

from broker.kite_auth import get_kite


class PnLMonitor(threading.Thread):
    """
    Periodic P&L snapshot printer.

    Uses margins("equity") to show:
      - net equity
      - m2m_realised
      - m2m_unrealised
    """

    def __init__(self, interval_sec: int = 15):
        super().__init__(daemon=True)
        self.interval_sec = interval_sec
        self._stop_flag = threading.Event()

    def stop(self):
        self._stop_flag.set()

    def run(self):
        print(f"[PnLMonitor] Started (interval={self.interval_sec}s)")
        while not self._stop_flag.is_set():
            try:
                kite = get_kite()
                margins = kite.margins("equity")

                net = None
                util = None
                try:
                    # Zerodha usually uses this structure
                    net = margins.get("net")
                    util = margins.get("utilised") or margins.get("equity", {}).get(
                        "utilised", {}
                    )
                except Exception:
                    pass

                realised = 0.0
                unrealised = 0.0
                if isinstance(util, dict):
                    realised = float(util.get("m2m_realised", 0.0))
                    unrealised = float(util.get("m2m_unrealised", 0.0))

                total = realised + unrealised

                print("\n[PnL] Snapshot:")
                print(f"  Net equity      : {net}")
                print(f"  Realised P&L    : {realised:.2f}")
                print(f"  Unrealised P&L  : {unrealised:.2f}")
                print(f"  Total P&L       : {total:.2f}")

            except Exception as e:
                print(f"[PnLMonitor] ERROR: {e}")

            time.sleep(self.interval_sec)

        print("[PnLMonitor] Stopped")
