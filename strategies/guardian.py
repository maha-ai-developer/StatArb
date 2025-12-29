"""
Assumption Guardian with Caching - Optimization #5

Monitors statistical assumptions with lazy recalculation to reduce
expensive OLS/ADF computations. Runs full math every N ticks instead
of every tick.
"""

import pandas as pd
import numpy as np
import statsmodels.api as sm
from statsmodels.tsa.stattools import adfuller
from typing import Tuple, Optional


class AssumptionGuardian:
    """
    Statistical assumption monitor with caching for performance.
    
    Optimization #5: Expensive OLS + ADF runs only every CACHE_INTERVAL ticks.
    Cached result returned on intermediate ticks.
    """
    
    # Run expensive math every N diagnose() calls
    CACHE_INTERVAL = 5
    
    def __init__(self, lookback_window: int = 60):
        self.lookback = lookback_window
        self.history_y = []
        self.history_x = []
        
        # Baselines
        self.initial_beta: Optional[float] = None
        self.red_light_counter = 0
        
        # Caching (Optimization #5)
        self._diagnosis_count = 0
        self._cached_result: Optional[Tuple[str, str]] = None
        self._last_beta: Optional[float] = None
        self._last_pvalue: Optional[float] = None

    def calibrate(self, beta: float):
        """Set initial hedge ratio baseline."""
        self.initial_beta = beta
        self.red_light_counter = 0
        self._cached_result = None
        self._diagnosis_count = 0

    def update_data(self, price_y: float, price_x: float):
        """Add new price data point (filters bad data)."""
        # Filter Bad Data (NaN, Inf, Zero)
        if not np.isfinite(price_y) or not np.isfinite(price_x) or price_y == 0 or price_x == 0:
            return  # Ignore bad tick
            
        self.history_y.append(price_y)
        self.history_x.append(price_x)
        
        # Maintain window size
        if len(self.history_y) > self.lookback:
            self.history_y.pop(0)
            self.history_x.pop(0)

    def diagnose(self) -> Tuple[str, str]:
        """
        Diagnose health of cointegration relationship.
        
        Uses caching to avoid running expensive OLS/ADF on every call.
        
        Returns:
            Tuple[status, reason]: ("GREEN"/"YELLOW"/"RED", explanation)
        """
        self._diagnosis_count += 1
        
        # Not enough data
        if len(self.history_y) < 20:
            return "YELLOW", "Initializing"
        
        # OPTIMIZATION #5: Return cached result on intermediate ticks
        if (self._diagnosis_count % self.CACHE_INTERVAL != 0 
            and self._cached_result is not None):
            return self._cached_result
        
        # Run full diagnosis
        result = self._run_full_diagnosis()
        self._cached_result = result
        return result
    
    def _run_full_diagnosis(self) -> Tuple[str, str]:
        """Run complete OLS + ADF analysis (expensive)."""
        s_y = pd.Series(self.history_y)
        s_x = pd.Series(self.history_x)
        
        # Safety Check: aligned lengths
        if len(s_y) != len(s_x):
            return "YELLOW", "Data Aligning"

        try:
            # Run OLS Regression
            x_const = sm.add_constant(s_x)
            model = sm.OLS(s_y, x_const).fit()
            current_beta = model.params.iloc[1]
            residuals = model.resid
            
            # Cache beta for stats
            self._last_beta = current_beta
            
            # Check Beta Drift
            denom = self.initial_beta if self.initial_beta != 0 else 0.001
            drift_pct = abs((current_beta - self.initial_beta) / denom)

            # Check Stationarity (Safe ADF)
            if residuals.std() < 1e-6:
                p_value = 0.0  # Perfect stationarity
            else:
                adf = adfuller(residuals, maxlag=1)
                p_value = adf[1]
            
            # Cache p-value for stats
            self._last_pvalue = p_value

        except Exception as e:
            # If math fails, do NOT kill the system
            return "YELLOW", "Math Computation Skip"

        # --- TRAFFIC LIGHTS ---
        # Thresholds relaxed for 2-year backtest (drift is natural over time)
        if drift_pct > 0.50:  # Was 0.30 - too strict for long backtests
            self.red_light_counter += 1
            return "RED", f"Beta Drift ({drift_pct:.2%})"
            
        if p_value > 0.30:  # Was 0.20 - allows more flexibility
            self.red_light_counter += 1
            return "RED", f"Broken Link (P={p_value:.2f})"

        self.red_light_counter = 0

        if drift_pct > 0.25 or p_value > 0.15:  # Was 0.15/0.10
            return "YELLOW", "Weak Signal"

        return "GREEN", "Healthy"

    def needs_recalibration(self) -> bool:
        """Check if too many consecutive red lights."""
        return self.red_light_counter > 5

    def force_recalibrate_to_current(self) -> Optional[float]:
        """Force recalibration to current market beta."""
        if len(self.history_y) < 20: 
            return self.initial_beta
        
        try:
            s_y = pd.Series(self.history_y)
            s_x = pd.Series(self.history_x)
            x_const = sm.add_constant(s_x)
            model = sm.OLS(s_y, x_const).fit()
            new_beta = model.params.iloc[1]
            self.initial_beta = new_beta
            self.red_light_counter = 0
            self._cached_result = None  # Invalidate cache
            return new_beta
        except:
            return self.initial_beta
    
    def get_stats(self) -> dict:
        """Get diagnostic statistics."""
        return {
            "diagnosis_count": self._diagnosis_count,
            "cache_interval": self.CACHE_INTERVAL,
            "red_light_counter": self.red_light_counter,
            "last_beta": self._last_beta,
            "last_pvalue": self._last_pvalue,
            "cached_status": self._cached_result[0] if self._cached_result else None,
            "regime_status": self._last_regime_status if hasattr(self, '_last_regime_status') else None
        }
    
    def invalidate_cache(self):
        """Force next diagnose() to run full analysis."""
        self._cached_result = None

    # ============================================================
    # ROLLING COINTEGRATION DETECTION (NEW - Checklist Enhancement)
    # ============================================================
    
    def detect_regime_change(self, window_size: int = 30) -> Tuple[str, dict]:
        """
        Detect regime change using rolling ADF test.
        
        NEW - Checklist Nice-to-Have: Rolling Cointegration Test
        
        Args:
            window_size: Rolling window for ADF calculation
            
        Returns:
            Tuple of (regime_status, details_dict)
            regime_status: "STABLE", "WEAKENING", "BROKEN"
        """
        if len(self.history_y) < window_size or len(self.history_x) < window_size:
            return "INITIALIZING", {"reason": "Insufficient data"}
        
        try:
            s_y = pd.Series(self.history_y[-window_size:])
            s_x = pd.Series(self.history_x[-window_size:])
            
            # Run OLS on window
            x_const = sm.add_constant(s_x)
            model = sm.OLS(s_y, x_const).fit()
            residuals = model.resid
            
            # Run ADF with auto-lag selection
            adf_result = adfuller(residuals, maxlag=None, autolag='AIC')
            p_value = adf_result[1]
            statistic = adf_result[0]
            
            # Store for reporting
            self._last_regime_pvalue = p_value
            self._last_regime_statistic = statistic
            
            # Classify regime
            if p_value < 0.05:
                status = "STABLE"
                reason = f"ADF p={p_value:.3f} < 0.05 - Strong cointegration"
            elif p_value < 0.15:
                status = "WEAKENING"
                reason = f"ADF p={p_value:.3f} - Cointegration weakening"
            else:
                status = "BROKEN"
                reason = f"ADF p={p_value:.3f} > 0.15 - Cointegration broken"
            
            self._last_regime_status = status
            
            return status, {
                "p_value": round(p_value, 4),
                "statistic": round(statistic, 4),
                "window_size": window_size,
                "reason": reason
            }
            
        except Exception as e:
            return "ERROR", {"reason": str(e)}
    
    def get_adf_history(self, windows: list = None) -> list:
        """
        Get ADF p-values for multiple window sizes.
        
        Useful for detecting gradual cointegration breakdown.
        
        Args:
            windows: List of window sizes to test (default: [20, 30, 40])
            
        Returns:
            List of dicts with window size and p-value
        """
        if windows is None:
            windows = [20, 30, 40]
        
        results = []
        for w in windows:
            if len(self.history_y) >= w:
                status, details = self.detect_regime_change(window_size=w)
                results.append({
                    "window": w,
                    "p_value": details.get("p_value"),
                    "status": status
                })
        
        return results

