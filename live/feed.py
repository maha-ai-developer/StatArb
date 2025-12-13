# live/feed.py

import threading
import time
from datetime import datetime, timedelta
from typing import Dict, List
from queue import Queue

from kiteconnect import KiteTicker
from broker.kite_auth import get_kite


def _parse_timeframe_to_minutes(tf: str) -> int:
    tf = tf.strip().lower()
    if tf.endswith("m"):
        return int(tf[:-1])
    raise ValueError(f"Unsupported timeframe format: {tf}")


def _floor_to_bucket(ts: datetime, minutes: int) -> datetime:
    minute_bucket = (ts.minute // minutes) * minutes
    return ts.replace(minute=minute_bucket, second=0, microsecond=0)


class FeedWorker(threading.Thread):
    def __init__(
        self,
        tokens: List[int],
        symbols_map: Dict[int, str],
        bar_queue: Queue,
        timeframe: str = "5m",
    ):
        super().__init__(daemon=True)
        self.tokens = tokens
        self.symbols_map = symbols_map
        self.bar_queue = bar_queue
        self.minutes = _parse_timeframe_to_minutes(timeframe)

        self._stop_flag = threading.Event()
        self.current_bars = {}
        
        # WATCHDOG
        self.last_tick_time = time.time()
        self.is_connected = False

    def _on_connect(self, ws, response):
        self.is_connected = True
        print(f"[FeedWorker] Connected. Subscribing {len(self.tokens)} tokens...")
        ws.subscribe(self.tokens)
        ws.set_mode(ws.MODE_FULL, self.tokens)

    def _on_close(self, ws, code, reason):
        self.is_connected = False
        print(f"[FeedWorker] Connection Closed: {code} - {reason}")

    def _on_error(self, ws, code, reason):
        print(f"[FeedWorker] Error: {code} - {reason}")

    def _on_ticks(self, ws, ticks):
        self.last_tick_time = time.time()
        now = datetime.now()
        
        for tick in ticks:
            token = tick["instrument_token"]
            price = tick["last_price"]
            qty = tick.get("volume_traded", 0) 
            
            if token not in self.symbols_map:
                continue
            
            symbol = self.symbols_map[token]
            bucket_start = _floor_to_bucket(now, self.minutes)
            bucket_end = bucket_start + timedelta(minutes=self.minutes)
            
            current_bar = self.current_bars.get(token)

            if current_bar:
                if bucket_start >= current_bar["end"]:
                    self._emit_bar(token, current_bar)
                    self._start_new_bar(token, symbol, bucket_start, bucket_end, price, qty)
                else:
                    self._update_bar(current_bar, price, qty)
            else:
                self._start_new_bar(token, symbol, bucket_start, bucket_end, price, qty)

    def _start_new_bar(self, token, symbol, start, end, price, qty):
        self.current_bars[token] = {
            "symbol": symbol,
            "start": start,
            "end": end,
            "open": price,
            "high": price,
            "low": price,
            "close": price,
            "volume": 0 
        }

    def _update_bar(self, bar, price, qty):
        bar["high"] = max(bar["high"], price)
        bar["low"] = min(bar["low"], price)
        bar["close"] = price
        # Volume could be cumulative, but for simple triggers we focus on price

    def _emit_bar(self, token, bar):
        try:
            self.bar_queue.put_nowait(bar)
            # FIX: PRINT FULL CANDLE DATA FOR VERIFICATION
            print(
                f"[FeedWorker] {bar['symbol']} {bar['start'].strftime('%H:%M')} "
                f"O:{bar['open']} H:{bar['high']} L:{bar['low']} C:{bar['close']}"
            )
        except Exception as e:
            print(f"[FeedWorker] Queue Error: {e}")

    def run(self):
        print("[FeedWorker] Starting KiteTicker...")
        while not self._stop_flag.is_set():
            try:
                kite = get_kite()
                from kiteconnect import KiteTicker
                
                self.ticker = KiteTicker(kite.api_key, kite.access_token)
                self.ticker.on_ticks = self._on_ticks
                self.ticker.on_connect = self._on_connect
                self.ticker.on_close = self._on_close
                self.ticker.on_error = self._on_error
                
                self.ticker.connect(threaded=True)
                
                while not self._stop_flag.is_set():
                    time.sleep(5)
                    if self.is_connected and (time.time() - self.last_tick_time > 15):
                        print("[FeedWorker] ⚠️ No ticks for 15s! Reconnecting...")
                        self.ticker.close()
                        self.is_connected = False
                        break
                        
            except Exception as e:
                print(f"[FeedWorker] Connection Failed: {e}. Retrying in 5s...")
                time.sleep(5)

    def stop(self):
        self._stop_flag.set()
        if hasattr(self, 'ticker'):
            self.ticker.close()
