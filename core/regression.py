"""
Unified Pair Trading System - Linear Regression Engine (Module 2)

Implements the core regression analysis for pair trading:
Y = β·X + c + ε

Where:
- Y = Dependent stock price
- X = Independent stock price  
- β = Beta (hedge ratio)
- c = Intercept
- ε = Residual (the signal we trade)
"""

import numpy as np
from typing import Union, List
from .models import RegressionResult


def perform_regression(
    x_values: Union[np.ndarray, List[float]], 
    y_values: Union[np.ndarray, List[float]]
) -> RegressionResult:
    """
    Perform Ordinary Least Squares (OLS) regression.
    
    Algorithm from unified_pair_trading.txt Module 2:
    1. Calculate means
    2. Calculate beta (slope)
    3. Calculate intercept
    4. Calculate residuals
    5. Calculate standard errors
    6. Calculate R²
    
    Args:
        x_values: Independent variable prices (X stock)
        y_values: Dependent variable prices (Y stock)
        
    Returns:
        RegressionResult with all regression parameters
        
    Raises:
        ValueError: If arrays have different lengths or insufficient data
    """
    # Convert to numpy arrays
    x = np.array(x_values, dtype=np.float64)
    y = np.array(y_values, dtype=np.float64)
    
    n = len(x)
    
    # Validation
    if n != len(y):
        raise ValueError(f"Array length mismatch: X={n}, Y={len(y)}")
    if n < 3:
        raise ValueError(f"Insufficient data points: {n} (need at least 3)")
    
    # Step 1: Calculate means
    mean_x = np.mean(x)
    mean_y = np.mean(y)
    
    # Step 2: Calculate beta (slope)
    # β = Σ[(Xi - X̄)(Yi - Ȳ)] / Σ[(Xi - X̄)²]
    x_deviation = x - mean_x
    y_deviation = y - mean_y
    
    numerator = np.sum(x_deviation * y_deviation)
    denominator = np.sum(x_deviation ** 2)
    
    if denominator == 0:
        raise ValueError("Cannot compute beta: zero variance in X")
    
    beta = numerator / denominator
    
    # Step 3: Calculate intercept
    # c = Ȳ - β·X̄
    intercept = mean_y - (beta * mean_x)
    
    # Step 4: Calculate residuals
    # ε = Y - (c + β·X)
    predicted = intercept + beta * x
    residuals = y - predicted
    
    # Sum of Squared Errors
    sse = np.sum(residuals ** 2)
    
    # Step 5: Calculate standard errors
    # SE(residuals) = √(SSE / (n-2))
    degrees_of_freedom = n - 2
    if degrees_of_freedom <= 0:
        raise ValueError("Insufficient degrees of freedom")
    
    standard_error = np.sqrt(sse / degrees_of_freedom)
    
    # SE(intercept) = SE(residuals) × √(ΣX² / (n × Σ(X-X̄)²))
    sum_x_squared = np.sum(x ** 2)
    intercept_std_error = standard_error * np.sqrt(sum_x_squared / (n * denominator))
    
    # Step 6: Calculate R²
    # R² = 1 - (SSE / SST)
    sst = np.sum(y_deviation ** 2)
    
    if sst == 0:
        r_squared = 0.0
    else:
        r_squared = 1 - (sse / sst)
    
    return RegressionResult(
        intercept=float(intercept),
        beta=float(beta),
        residuals=residuals,
        standard_error=float(standard_error),
        intercept_std_error=float(intercept_std_error),
        r_squared=float(r_squared)
    )


def calculate_residual(
    price_x: float, 
    price_y: float, 
    intercept: float, 
    beta: float
) -> float:
    """
    Calculate current residual for live prices.
    
    ε = Y - (c + β·X)
    
    Args:
        price_x: Current X stock price
        price_y: Current Y stock price
        intercept: Regression intercept
        beta: Regression beta (hedge ratio)
        
    Returns:
        Current residual value
    """
    predicted_y = intercept + (beta * price_x)
    return price_y - predicted_y


def calculate_z_score(
    residual: float, 
    mean: float, 
    std_dev: float
) -> float:
    """
    Calculate z-score of a residual.
    
    Z = (residual - mean) / σ
    
    Args:
        residual: Current residual value
        mean: Historical mean of residuals
        std_dev: Historical standard deviation of residuals
        
    Returns:
        Z-score value
        
    Raises:
        ValueError: If std_dev is zero
    """
    if std_dev == 0:
        raise ValueError("Cannot calculate z-score: zero standard deviation")
    
    return (residual - mean) / std_dev


def calculate_rolling_statistics(
    residuals: np.ndarray, 
    lookback: int = 20
) -> tuple:
    """
    Calculate rolling mean and standard deviation for z-score calculation.
    
    Args:
        residuals: Array of residual values
        lookback: Rolling window size (default 20)
        
    Returns:
        Tuple of (rolling_mean, rolling_std) as arrays
    """
    n = len(residuals)
    
    if n < lookback:
        # Not enough data, use all available
        mean = np.mean(residuals)
        std = np.std(residuals)
        return np.full(n, mean), np.full(n, std)
    
    # Calculate rolling statistics
    rolling_mean = np.zeros(n)
    rolling_std = np.zeros(n)
    
    for i in range(n):
        start = max(0, i - lookback + 1)
        window = residuals[start:i + 1]
        rolling_mean[i] = np.mean(window)
        rolling_std[i] = np.std(window) if len(window) > 1 else 0.0
    
    return rolling_mean, rolling_std
