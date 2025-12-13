# strategies/orb.py

"""
ORB (Opening Range Breakout) strategy helper.

Provides:
    compute_orb_signals(df) -> returns df with:
        - df["orb_high"]
        - df["orb_low"]
        - df["orb_signal"] = BUY / SELL / HOLD
"""

import pandas as pd


def compute_orb_signals(df: pd.DataFrame,
                        orb_minutes: int = 15) -> pd.DataFrame:
    """
    ORB Definition:
        Opening range = first `orb_minutes` minutes of the session.
        ORB High = highest high in that window
        ORB Low  = lowest low in that window

        BUY  when price closes above ORB High
        SELL when price closes below ORB Low
        otherwise HOLD

    Works for:
        - intraday 1m/5m/15m candles
        - backtesting candles
    """

    df = df.copy()

    # Need datetime index
    if not isinstance(df.index, pd.DatetimeIndex):
        raise ValueError("DataFrame index must be DatetimeIndex for ORB")

    # Detect the session start day-by-day
    df["date"] = df.index.date
    orb_high_list = []
    orb_low_list = []
    signals = []

    grouped = df.groupby("date")

    for date, g in grouped:
        # Identify the opening window: first `orb_minutes` minutes
        session_start = pd.Timestamp.combine(date, pd.Timestamp("09:15").time())
        orb_end = session_start + pd.Timedelta(minutes=orb_minutes)

        # candles in first ORB window
        opening_range = g[(g.index >= session_start) & (g.index < orb_end)]

        if len(opening_range) == 0:
            # no ORB window in this date
            for _ in g.index:
                orb_high_list.append(None)
                orb_low_list.append(None)
                signals.append("HOLD")
            continue

        orb_high = opening_range["high"].max()
        orb_low = opening_range["low"].min()

        # Apply ORB logic for entire day
        for ts, row in g.iterrows():
            price = row["close"]

            if price > orb_high:
                s = "BUY"
            elif price < orb_low:
                s = "SELL"
            else:
                s = "HOLD"

            orb_high_list.append(orb_high)
            orb_low_list.append(orb_low)
            signals.append(s)

    df["orb_high"] = orb_high_list
    df["orb_low"] = orb_low_list
    df["orb_signal"] = signals

    df.drop(columns=["date"], inplace=True)

    return df
