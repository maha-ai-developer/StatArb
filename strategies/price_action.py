# strategies/price_action.py
import pandas as pd


def apply_price_action(df: pd.DataFrame) -> pd.DataFrame:
    """
    Very simple PA filters:
      - Bullish engulfing => BUY
      - Bearish engulfing => SELL
      - Else HOLD
    """

    df["body"] = df["close"] - df["open"]
    df["prev_body"] = df["body"].shift(1)
    df["prev_open"] = df["open"].shift(1)
    df["prev_close"] = df["close"].shift(1)

    def pa_sig(row):
        if any(pd.isna([row.prev_open, row.prev_close, row.open, row.close])):
            return "HOLD"

        # bullish engulfing
        if row.prev_close < row.prev_open and row.close > row.open:
            if row.close >= row.prev_open and row.open <= row.prev_close:
                return "BUY"

        # bearish engulfing
        if row.prev_close > row.prev_open and row.close < row.open:
            if row.close <= row.prev_open and row.open >= row.prev_close:
                return "SELL"

        return "HOLD"

    df["signal_pa"] = df.apply(pa_sig, axis=1)
    return df
