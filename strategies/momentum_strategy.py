import pandas as pd
import pandas_ta_classic as ta

class MomentumStrategy:
    def __init__(self, timeframe="5m", ema_period=20, rsi_period=14, rsi_limit=55):
        self.timeframe = timeframe
        self.ema_period = ema_period
        self.rsi_period = rsi_period
        self.rsi_limit = rsi_limit

    def get_signal(self, df):
        if df is None or len(df) < 205: # Need 200 candles for EMA 200
            return None

        # 1. Calculate Indicators
        df['ema'] = ta.ema(df['close'], length=self.ema_period)
        df['rsi'] = ta.rsi(df['close'], length=self.rsi_period)
        df['atr'] = ta.atr(df['high'], df['low'], df['close'], length=14)
        
        # --- NEW LOGIC: THE 200 EMA FILTER ---
        df['ema_200'] = ta.ema(df['close'], length=200)

        current = df.iloc[-1]
        
        # Guard against NaNs
        if pd.isna(current['ema']) or pd.isna(current['atr']) or pd.isna(current['ema_200']):
            return None

        # 2. Logic - BUY
        # Condition: Price > Short EMA  AND  Price > Long EMA (200)  AND  Strong RSI
        if (current['close'] > current['ema'] and 
            current['close'] > current['ema_200'] and 
            current['rsi'] > self.rsi_limit):
            
            return {
                "signal": "BUY",
                "atr": current['atr'],
                "price": current['close']
            }
        
        # 3. Logic - SELL
        elif current['close'] < current['ema'] or current['rsi'] < (100 - self.rsi_limit):
            return {
                "signal": "SELL",
                "atr": current['atr'],
                "price": current['close']
            }
            
        return None
