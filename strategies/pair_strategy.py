import pandas as pd
import statsmodels.api as sm
import numpy as np

class PairStrategy:
    def __init__(self, entry_threshold=1.5, exit_threshold=0.0):
        self.entry_threshold = entry_threshold
        self.exit_threshold = exit_threshold
        self.lookback = 200 # Rolling window for Z-Score calculation

    def align_data(self, s1_series, s2_series):
        """
        Inner joins two price series to ensure timestamps match perfectly.
        """
        df = pd.DataFrame({'s1': s1_series, 's2': s2_series}).dropna()
        return df

    def calculate_signals(self, df, fixed_mean=None, fixed_std=None):
        """
        Calculates Spread and Z-Score.
        Supports both Rolling (Dynamic) and Fixed (Scanner) stats.
        """
        if len(df) < 20: return df
        
        # 1. Hedge Ratio Calculation (Using OLS Regression)
        # We calculate this once over the whole backtest period for stability,
        # or use a rolling OLS for live. For simplicity/speed -> Static OLS for the batch.
        model = sm.OLS(df['s1'], df['s2'])
        result = model.fit()
        hedge_ratio = result.params.iloc[0]
        
        # 2. Calculate Spread
        # Spread = Stock A - (HedgeRatio * Stock B)
        df['spread'] = df['s1'] - (hedge_ratio * df['s2'])
        
        # 3. Calculate Z-Score
        if fixed_mean is not None and fixed_std is not None:
            # SCENARIO A: Use Fixed Stats from Scanner (Daily Data)
            # Note: Scanner uses Ratio (A/B), Strategy uses Spread (A - hB).
            # To avoid mismatch, we re-calculate mean/std of THIS spread logic
            # but using the fixed window concept if needed. 
            # ideally, we stick to Rolling for intraday to handle drift.
            df['mean'] = df['spread'].rolling(window=self.lookback).mean()
            df['std'] = df['spread'].rolling(window=self.lookback).std()
        else:
            # SCENARIO B: Rolling Window (Standard for Intraday)
            df['mean'] = df['spread'].rolling(window=self.lookback).mean()
            df['std'] = df['spread'].rolling(window=self.lookback).std()

        # Final Z Calculation
        df['z_score'] = (df['spread'] - df['mean']) / (df['std'] + 1e-8)
        
        return df

    def check_live_signal(self, sym_a, df_a, sym_b, df_b):
        """
        LIVE MODE: Returns signal dict if Z-Score crosses threshold.
        """
        # 1. Align
        if df_a.empty or df_b.empty: return None
        combined = self.align_data(df_a['close'], df_b['close'])
        
        if len(combined) < self.lookback: return None
            
        # 2. Calculate
        analyzed = self.calculate_signals(combined)
        current_z = analyzed['z_score'].iloc[-1]
        
        # 3. Signal Logic
        if current_z < -self.entry_threshold:
            return {
                "type": "PAIR_TRADE",
                "symbol_a": sym_a,
                "symbol_b": sym_b,
                "direction": "LONG_PAIR", # Buy A, Sell B
                "z_score": round(current_z, 2)
            }
        elif current_z > self.entry_threshold:
            return {
                "type": "PAIR_TRADE",
                "symbol_a": sym_a,
                "symbol_b": sym_b,
                "direction": "SHORT_PAIR", # Sell A, Buy B
                "z_score": round(current_z, 2)
            }
        return None

    def run_backtest(self, s1_series, s2_series, fixed_mean=None, fixed_std=None, initial_capital=100000):
        """
        Simulates trading based on Z-Score logic.
        """
        # 1. Prepare Data
        df = self.align_data(s1_series, s2_series)
        if len(df) < self.lookback: return None
        
        # 2. Add Z-Scores
        df = self.calculate_signals(df, fixed_mean, fixed_std)
        df = df.dropna()

        # 3. Simulation Loop
        position = 0 # 0=None, 1=Long Spread, -1=Short Spread
        entry_price_spread = 0.0
        trades = []
        equity = initial_capital
        
        # We assume 1000 units spread size for simplicity in PnL calc
        qty = 10 
        
        for i in range(len(df)):
            row = df.iloc[i]
            z = row['z_score']
            spread_price = row['spread']
            
            # --- ENTRY LOGIC ---
            if position == 0:
                # Long Spread (Buy A, Sell B) -> Z < -1.5 (Undervalued)
                if z < -self.entry_threshold:
                    position = 1
                    entry_price_spread = spread_price
                
                # Short Spread (Sell A, Buy B) -> Z > 1.5 (Overvalued)
                elif z > self.entry_threshold:
                    position = -1
                    entry_price_spread = spread_price
            
            # --- EXIT LOGIC ---
            elif position == 1: # Holding Long
                # Exit when Z reverts to 0
                if z >= -self.exit_threshold: 
                    pnl = (spread_price - entry_price_spread) * qty * 100 # Multiplier
                    trades.append(pnl)
                    equity += pnl
                    position = 0

            elif position == -1: # Holding Short
                # Exit when Z reverts to 0
                if z <= self.exit_threshold:
                    pnl = (entry_price_spread - spread_price) * qty * 100
                    trades.append(pnl)
                    equity += pnl
                    position = 0

        # 4. Compile Stats
        win_rate = 0
        if len(trades) > 0:
            wins = len([t for t in trades if t > 0])
            win_rate = (wins / len(trades)) * 100
            
        return {
            "total_pnl": equity - initial_capital,
            "trades": len(trades),
            "win_rate": win_rate,
            "sharpe": 0.0, # Placeholder
            "max_z": df['z_score'].max(),
            "min_z": df['z_score'].min()
        }
