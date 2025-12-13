# strategies/combined_stack.py

import pandas as pd
import pandas_ta_classic as ta  # <--- USING THE CLASSIC VERSION
import numpy as np

# KEEP: Inherit from your base class to maintain system structure
from strategies.base import StrategyBase

# -------------------------------------------------------------------
# 1. THE CLASS (For Live Trading - Row by Row)
# -------------------------------------------------------------------
class CombinedStack(StrategyBase):
    """
    Used by Live Engine to process one candle at a time.
    """
    name = "combined_stack"  # Identifier for the strategy

    def __init__(self, intraday=True, swing=True, use_ml=True):
        # Call parent constructor if StrategyBase requires it
        super().__init__()
        
        self.intraday = intraday
        self.swing = swing
        self.use_ml = use_ml

    def generate_signal(self, df: pd.DataFrame) -> str:
        """
        Live trading logic: Reads the latest row of the dataframe.
        """
        # Ensure we have enough data to calculate indicators
        if len(df) < 30:
            return "HOLD"

        # -----------------------------------------------------------
        # LIVE INDICATORS (Calculated on the fly using pandas_ta)
        # -----------------------------------------------------------
        
        # EMA
        ema8 = ta.ema(df['close'], length=8).iloc[-1]
        ema21 = ta.ema(df['close'], length=21).iloc[-1]
        
        # MACD (12, 26, 9)
        macd = ta.macd(df['close'], fast=12, slow=26, signal=9)
        # MACD returns 3 cols: [MACD line, Histogram, Signal line]
        # We usually want the Histogram (2nd column) to check momentum
        macd_hist = macd.iloc[-1, 1] 
        
        # RSI (14)
        rsi = ta.rsi(df['close'], length=14).iloc[-1]
        
        # Supertrend (7, 3)
        st = ta.supertrend(df['high'], df['low'], df['close'], length=7, multiplier=3)
        # Supertrend returns [Trend Line, Direction (1 or -1)]
        st_dir = st.iloc[-1, 1] 

        # -----------------------------------------------------------
        # SIGNAL LOGIC
        # -----------------------------------------------------------
        
        # BUY Logic:
        # 1. Short Trend UP (EMA 8 > 21)
        # 2. Momentum Increasing (MACD Hist > 0)
        # 3. Not Overbought (RSI < 70)
        # 4. Main Trend Bullish (Supertrend Green/1)
        if (ema8 > ema21) and (macd_hist > 0) and (rsi < 70) and (st_dir == 1):
            return "BUY"
            
        # SELL Logic:
        # 1. Short Trend DOWN (EMA 8 < 21) OR
        # 2. Main Trend Bearish (Supertrend Red/-1)
        elif (ema8 < ema21) or (st_dir == -1):
            return "SELL"
            
        return "HOLD"

# -------------------------------------------------------------------
# 2. THE FUNCTION (For Backtesting - Vectorized/Fast)
# -------------------------------------------------------------------
def compute_signals(df: pd.DataFrame, use_ml: bool = False):
    """
    High-Speed Vectorized Strategy Calculation for Backtesting.
    Calculates 1 year of data in 0.1 seconds.
    """
    # Work on a copy to avoid warnings
    df = df.copy()
    
    # -----------------------------------------------------------
    # VECTORIZED INDICATORS (Calculates whole column at once)
    # -----------------------------------------------------------
    df['ema_8'] = ta.ema(df['close'], length=8)
    df['ema_21'] = ta.ema(df['close'], length=21)
    
    macd = ta.macd(df['close'], fast=12, slow=26, signal=9)
    df['macd_hist'] = macd.iloc[:, 1]
    
    df['rsi'] = ta.rsi(df['close'], length=14)
    
    st = ta.supertrend(df['high'], df['low'], df['close'], length=7, multiplier=3)
    df['supertrend_dir'] = st.iloc[:, 1]

    # -----------------------------------------------------------
    # VECTORIZED SIGNAL LOGIC
    # -----------------------------------------------------------
    df['signal'] = "HOLD"
    
    # Define Conditions (Boolean Arrays)
    buy_cond = (
        (df['ema_8'] > df['ema_21']) &
        (df['macd_hist'] > 0) &
        (df['rsi'] < 70) & 
        (df['supertrend_dir'] == 1)
    )
    
    sell_cond = (
        (df['ema_8'] < df['ema_21']) |
        (df['supertrend_dir'] == -1)
    )

    # Apply Signals
    df.loc[buy_cond, 'signal'] = "BUY"
    df.loc[sell_cond, 'signal'] = "SELL"
    
    return df
