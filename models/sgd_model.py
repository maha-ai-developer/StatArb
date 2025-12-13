# models/sgd_model.py

import numpy as np
import pandas as pd
from sklearn.linear_model import SGDClassifier
from sklearn.preprocessing import StandardScaler


class SGDModel:
    """
    Simple online-learning model for real-time trade classification.
    Features (example):
        - returns
        - rolling volatility
        - rolling momentum

    Output:
        BUY / SELL / HOLD
    """

    def __init__(self):
        # online ML model
        self.model = SGDClassifier(
            loss="log_loss",
            learning_rate="optimal",
            penalty="l2",
        )

        self.scaler = StandardScaler()

        # flag to know when model is trained
        self.is_trained = False

    # ------------------------------
    # Feature extraction
    # ------------------------------
    def _make_features(self, df: pd.DataFrame):
        df = df.copy()

        df["returns"] = df["close"].pct_change()
        df["volatility"] = df["returns"].rolling(10).std()
        df["momentum"] = df["close"].diff()

        df = df.dropna()

        if df.empty:
            return None

        X = df[["returns", "volatility", "momentum"]].values
        return X

    # ------------------------------
    # Public prediction API
    # ------------------------------
    def predict(self, df: pd.DataFrame) -> str:
        """
        Return BUY / SELL / HOLD
        based on latest ML output.
        """
        X = self._make_features(df)
        if X is None:
            return "HOLD"

        x_last = X[-1].reshape(1, -1)

        if not self.is_trained:
            # No training → fallback
            return "HOLD"

        x_last = self.scaler.transform(x_last)
        pred = self.model.predict(x_last)[0]

        if pred == 1:
            return "BUY"
        elif pred == -1:
            return "SELL"
        return "HOLD"

    # ------------------------------
    # Online training API
    # ------------------------------
    def partial_fit(self, df: pd.DataFrame, label: int):
        """
        Online update.
        label:
          1 = BUY
         -1 = SELL
        """
        X = self._make_features(df)
        if X is None:
            return

        # target must match the number of samples
        y = np.array([label] * len(X))

        # first-time training requires classes to be defined
        if not self.is_trained:
            X_scaled = self.scaler.fit_transform(X)
            self.model.partial_fit(X_scaled, y, classes=[-1, 0, 1])
            self.is_trained = True
        else:
            X_scaled = self.scaler.transform(X)
            self.model.partial_fit(X_scaled, y)
