"""
Unified Pair Trading System - Error Ratio Calculator (Module 3)

Determines the optimal X/Y designation for a stock pair by comparing
error ratios from both regression directions.

Error Ratio = SE(intercept) / SE(residuals)
- Lower error ratio = more reliable pair
- Choose the direction with lower error ratio
"""

import numpy as np
from typing import Union, List, Dict
from .models import StockData, RegressionResult
from .regression import perform_regression


def calculate_error_ratio(regression: RegressionResult) -> float:
    """
    Calculate error ratio from regression result.
    
    Error Ratio = SE(intercept) / SE(residuals)
    
    Lower error ratio indicates:
    - More reliable intercept estimate
    - Better X/Y designation
    
    Args:
        regression: RegressionResult from perform_regression()
        
    Returns:
        Error ratio value
    """
    if regression.standard_error == 0:
        return float('inf')
    
    return regression.intercept_std_error / regression.standard_error


def calculate_optimal_direction(
    stock_a: StockData,
    stock_b: StockData
) -> Dict:
    """
    Determine optimal X/Y designation for a stock pair.
    
    Algorithm from unified_pair_trading.txt Module 3:
    1. Regress A as X, B as Y → get error_ratio_AB
    2. Regress B as X, A as Y → get error_ratio_BA
    3. Select direction with lower error ratio
    
    Args:
        stock_a: First stock data
        stock_b: Second stock data
        
    Returns:
        Dictionary with:
        - X: Independent variable stock symbol
        - Y: Dependent variable stock symbol
        - regression: RegressionResult for optimal direction
        - error_ratio: Error ratio for optimal direction
        - alternative_error_ratio: Error ratio for other direction
    """
    # Regression 1: A as X, B as Y
    reg_ab = perform_regression(stock_a.prices, stock_b.prices)
    error_ratio_ab = calculate_error_ratio(reg_ab)
    
    # Regression 2: B as X, A as Y
    reg_ba = perform_regression(stock_b.prices, stock_a.prices)
    error_ratio_ba = calculate_error_ratio(reg_ba)
    
    # Select lower error ratio
    if error_ratio_ab <= error_ratio_ba:
        return {
            'X': stock_a.symbol,
            'Y': stock_b.symbol,
            'regression': reg_ab,
            'error_ratio': error_ratio_ab,
            'alternative_error_ratio': error_ratio_ba
        }
    else:
        return {
            'X': stock_b.symbol,
            'Y': stock_a.symbol,
            'regression': reg_ba,
            'error_ratio': error_ratio_ba,
            'alternative_error_ratio': error_ratio_ab
        }


def calculate_optimal_direction_from_prices(
    prices_a: Union[np.ndarray, List[float]],
    prices_b: Union[np.ndarray, List[float]],
    symbol_a: str = "A",
    symbol_b: str = "B"
) -> Dict:
    """
    Simplified version that works directly with price arrays.
    
    Useful when you don't have full StockData objects.
    
    Args:
        prices_a: Price array for first stock
        prices_b: Price array for second stock
        symbol_a: Symbol name for first stock
        symbol_b: Symbol name for second stock
        
    Returns:
        Dictionary with optimal X/Y designation and regression result
    """
    prices_a = np.array(prices_a)
    prices_b = np.array(prices_b)
    
    # Regression 1: A as X, B as Y
    reg_ab = perform_regression(prices_a, prices_b)
    error_ratio_ab = calculate_error_ratio(reg_ab)
    
    # Regression 2: B as X, A as Y
    reg_ba = perform_regression(prices_b, prices_a)
    error_ratio_ba = calculate_error_ratio(reg_ba)
    
    if error_ratio_ab <= error_ratio_ba:
        return {
            'X': symbol_a,
            'Y': symbol_b,
            'X_prices': prices_a,
            'Y_prices': prices_b,
            'regression': reg_ab,
            'error_ratio': error_ratio_ab,
            'alternative_error_ratio': error_ratio_ba
        }
    else:
        return {
            'X': symbol_b,
            'Y': symbol_a,
            'X_prices': prices_b,
            'Y_prices': prices_a,
            'regression': reg_ba,
            'error_ratio': error_ratio_ba,
            'alternative_error_ratio': error_ratio_ab
        }


def classify_error_ratio(error_ratio: float) -> str:
    """
    Classify error ratio quality.
    
    From constants:
    - < 0.15: EXCELLENT
    - < 0.25: GOOD
    - < 0.40: FAIR
    - >= 0.40: POOR
    
    Args:
        error_ratio: Calculated error ratio
        
    Returns:
        Quality classification string
    """
    from .constants import (
        ERROR_RATIO_EXCELLENT, 
        ERROR_RATIO_GOOD, 
        ERROR_RATIO_MAX,
        QUALITY_EXCELLENT,
        QUALITY_GOOD,
        QUALITY_FAIR,
        QUALITY_POOR
    )
    
    if error_ratio < ERROR_RATIO_EXCELLENT:
        return QUALITY_EXCELLENT
    elif error_ratio < ERROR_RATIO_GOOD:
        return QUALITY_GOOD
    elif error_ratio < ERROR_RATIO_MAX:
        return QUALITY_FAIR
    else:
        return QUALITY_POOR
