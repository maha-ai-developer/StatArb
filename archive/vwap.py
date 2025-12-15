# strategies/vwap.py

"""
VWAP calculation helper for strategy stack.

Provides:
    compute_vwap(df) -> returns df with:
        - df["vwap"]
"""

import pandas as pd


def compute_vwap(df: pd.DataFrame) -> pd.DataFrame:
    """
    Compute intraday VWAP.

    VWAP = cumulative(price * volume) / cumulative(volume)

    This works for:
      - intraday live candles
      - backtest candles (if full OHLCV data exists)
    """

    df = df.copy()

    # VWAP requires price * volume
    typical_price = (df["high"] + df["low"] + df["close"]) / 3

    df["vwap_num"] = typical_price * df["volume"]
    df["vwap_den"] = df["volume"].replace(0, 1)   # avoid division by zero

    df["vwap"] = df["vwap_num"].cumsum() / df["vwap_den"].cumsum()

    # Clean temporary columns
    df.drop(columns=["vwap_num", "vwap_den"], inplace=True)

    return df
