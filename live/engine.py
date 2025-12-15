import time
import pandas as pd
import json
import os
from queue import Queue
from datetime import datetime

from core.instrument_cache import get_instrument_token
from live.feed import FeedWorker
from live.execution_engine import ExecutionEngine
from live.monitor import RiskMonitor
from live.warmup import fetch_warmup_data

# Import Strategies
from strategies.momentum_strategy import MomentumStrategy
from strategies.pair_strategy import PairStrategy

class LiveEngine:
    def __init__(
        self,
        symbols: list,         
        pair_file: str,        
        timeframe: str = "5m",
        min_bars: int = 20,
        place_order: bool = False,
        risk_pct: float = 1.0
    ):
        self.momentum_symbols = symbols
        self.pair_file = pair_file
        self.timeframe = timeframe
        self.min_bars = min_bars
        self.place_order = place_order
        
        self.bar_queue = Queue()
        self.signal_queue = Queue()
        self.history = {}

        # ----------------------------------------------
        # 🧠 INTELLIGENCE UPGRADE: Load Optimized Config
        # ----------------------------------------------
        self.strategies = {} # Dictionary: Symbol -> Specific Strategy Instance
        
        config_path = "strategies/momentum_config.json"
        mom_config = {}
        
        # 1. Load the JSON file created by optimize.py
        if os.path.exists(config_path):
            try:
                with open(config_path, "r") as f:
                    mom_config = json.load(f)
                print(f"[Engine] 🧠 Loaded optimized settings for {len(mom_config)} symbols.")
            except Exception as e:
                print(f"[Engine] ⚠️ Config Error: {e}")

        # 2. Create a specific strategy for EACH symbol
        for sym in self.momentum_symbols:
            if sym in mom_config:
                # USE OPTIMIZED SETTINGS (e.g. EMA 50, RSI 65)
                params = mom_config[sym]
                self.strategies[sym] = MomentumStrategy(
                    timeframe=timeframe,
                    ema_period=params.get('ema', 20),
                    rsi_limit=params.get('rsi', 55)
                )
            else:
                # USE DEFAULTS (Fallback)
                self.strategies[sym] = MomentumStrategy(timeframe=timeframe)

        # 3. Pair Strategy (Global)
        self.pair_strategy = PairStrategy(entry_threshold=1.5)
        
        # 4. Load Pairs List
        self.active_pairs = [] 
        try:
            with open(pair_file, "r") as f:
                for line in f:
                    if "," in line:
                        parts = line.strip().split(",")
                        self.active_pairs.append((parts[0].strip(), parts[1].strip()))
            print(f"[Engine] Loaded {len(self.active_pairs)} Pairs.")
        except Exception as e:
            print(f"[Engine] Pair file error: {e}")

        # 5. Master Symbol List
        self.pair_components = []
        for a, b in self.active_pairs:
            self.pair_components.extend([a, b])
            
        self.all_symbols = list(set(self.momentum_symbols + self.pair_components))
        
        # 6. Workers
        self.exec_worker = ExecutionEngine(
            signal_queue=self.signal_queue,
            place_order=self.place_order,
            risk_pct=risk_pct
        )
        self.feed_worker = None
        self.risk_monitor = None

    def _update_history(self, bar):
        sym = bar['symbol']
        new_row = pd.DataFrame([{
            'date': pd.to_datetime(bar['start']),
            'open': bar['open'],
            'high': bar['high'],
            'low': bar['low'],
            'close': bar['close'],
            'volume': bar.get('volume', 0)
        }])
        
        if sym not in self.history:
            self.history[sym] = new_row
        else:
            self.history[sym] = pd.concat([self.history[sym], new_row], ignore_index=True)
            
        if len(self.history[sym]) > 500:
            self.history[sym] = self.history[sym].iloc[-500:]

    def start(self):
        print(f"--- 🚀 ENGINE STARTING ({len(self.all_symbols)} Symbols) ---")
        
        # WARMUP
        print("[Engine] Fetching Warmup Data...")
        warmup_data = fetch_warmup_data(self.all_symbols, self.timeframe, lookback_days=5)
        
        for sym, candles in warmup_data.items():
            if candles:
                df = pd.DataFrame(candles)
                if 'date' in df.columns: df.rename(columns={'date': 'date'}, inplace=True)
                self.history[sym] = df
                print(f"  -> {sym}: Loaded {len(df)} candles.")
        
        # START SUBSYSTEMS
        self.exec_worker.start()
        
        tokens = []
        sym_map = {}
        for s in self.all_symbols:
            t = get_instrument_token(s)
            if t:
                tokens.append(t)
                sym_map[t] = s
        
        self.feed_worker = FeedWorker(tokens, sym_map, self.bar_queue, self.timeframe)
        self.feed_worker.start()
        
        # MAIN LOOP
        print("\n--- 🎧 LISTENING FOR TICKS ---")
        try:
            while True:
                bar = self.bar_queue.get()
                symbol = bar['symbol']
                
                self._update_history(bar)
                
                # A. CHECK MOMENTUM (Using Specific Strategy Instance)
                if symbol in self.strategies:
                    # Get result (Dict)
                    result = self.strategies[symbol].get_signal(self.history[symbol])
                    
                    if result: # If not None
                        sig_type = result['signal']
                        atr_val = result.get('atr', 0)
                        
                        print(f"  🚀 SIGNAL: {symbol} {sig_type} (ATR={atr_val:.2f})")
                        
                        self.signal_queue.put({
                            "type": "MOMENTUM",
                            "symbol": symbol,
                            "final_signal": sig_type,
                            "price": bar['close'],
                            "atr": atr_val
                        })

                # B. CHECK PAIRS
                for sym_a, sym_b in self.active_pairs:
                    if symbol == sym_a or symbol == sym_b:
                        if sym_a in self.history and sym_b in self.history:
                            res = self.pair_strategy.check_live_signal(
                                sym_a, self.history[sym_a], 
                                sym_b, self.history[sym_b]
                            )
                            if res:
                                print(f"  ⚖️ PAIR SIGNAL: {res['direction']} {sym_a}/{sym_b}")
                                self.signal_queue.put(res)

        except KeyboardInterrupt:
            self.stop()

    def stop(self):
        print("\n[Engine] Stopping...")
        if self.feed_worker: self.feed_worker.stop()
        if self.exec_worker: self.exec_worker.stop()
