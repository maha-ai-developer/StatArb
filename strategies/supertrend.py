# strategies/supertrend.py

"""
SuperTrend helper for strategy stack.

Provides:
    compute_supertrend(df) -> returns df with:
        - df["supertrend_dir"]      (1 for uptrend, -1 for downtrend)
        - df["supertrend_signal"]   ("BUY", "SELL", "HOLD")
"""

import pandas as pd
from core.indicators import supertrend  # existing function you already use


def compute_supertrend(df: pd.DataFrame,
                       period: int = 10,
                       multiplier: float = 3.0) -> pd.DataFrame:
    """
    Compute SuperTrend direction and signal columns.

    direction: 1 (uptrend), -1 (downtrend), or NaN
    """
    # Work on a copy to avoid side-effects
    df = df.copy()

    # This uses your existing core.indicators.supertrend(...)
    direction = supertrend(df, period=period, multiplier=multiplier)
    df["supertrend_dir"] = direction

    def to_signal(val):
        if pd.isna(val):
            return "HOLD"
        if val == 1:
            return "BUY"
        if val == -1:
            return "SELL"
        return "HOLD"

    df["supertrend_signal"] = df["supertrend_dir"].apply(to_signal)
    return df
