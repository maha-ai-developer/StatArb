# strategies/base.py
from abc import ABC, abstractmethod
import pandas as pd

class StrategyBase(ABC):
    """
    Interface all strategies should follow.
    """
    name = "base"

    @abstractmethod
    def generate_signal(self, df: pd.DataFrame) -> str:
        """
        df: OHLCV DataFrame
        return: "BUY" / "SELL" / "HOLD"
        """
        pass
