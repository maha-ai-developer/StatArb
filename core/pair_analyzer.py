"""
Unified Pair Trading System - Comprehensive Pair Analyzer (Module 5)

Combines all analytics modules to produce a complete PairAnalysis:
1. Determine optimal X/Y via error ratio
2. Run regression
3. Test stationarity (ADF)
4. Calculate residual statistics
5. Compute current z-score
6. Assess quality
"""

import numpy as np
from typing import Union, List, Optional
from datetime import datetime

from .models import StockData, PairAnalysis
from .regression import perform_regression, calculate_rolling_statistics
from .error_ratio import calculate_optimal_direction_from_prices, classify_error_ratio
from .stationarity import perform_adf_test_statsmodels
from .constants import (
    ADF_THRESHOLD, ADF_EXCELLENT,
    ERROR_RATIO_EXCELLENT, ERROR_RATIO_GOOD, ERROR_RATIO_MAX,
    QUALITY_EXCELLENT, QUALITY_GOOD, QUALITY_FAIR, QUALITY_POOR
)


def analyze_pair(
    stock_a: StockData,
    stock_b: StockData,
    lookback: int = 20
) -> PairAnalysis:
    """
    Perform comprehensive pair analysis.
    
    Algorithm from unified_pair_trading.txt Module 5:
    1. Determine X/Y via error ratio
    2. Run regression to get β, intercept, residuals
    3. Test stationarity with ADF
    4. Calculate residual statistics
    5. Compute current z-score
    6. Assess quality
    
    Args:
        stock_a: First stock data
        stock_b: Second stock data
        lookback: Rolling window for z-score (default 20)
        
    Returns:
        Complete PairAnalysis object
    """
    # Step 1: Determine optimal X/Y via error ratio
    optimal = calculate_optimal_direction_from_prices(
        stock_a.prices,
        stock_b.prices,
        stock_a.symbol,
        stock_b.symbol
    )
    
    x_stock = optimal['X']
    y_stock = optimal['Y']
    regression = optimal['regression']
    error_ratio = optimal['error_ratio']
    
    # Step 2: Extract regression parameters
    intercept = regression.intercept
    beta = regression.beta
    residuals = regression.residuals
    
    # Step 3: Test stationarity
    adf_result = perform_adf_test_statsmodels(residuals)
    adf_value = adf_result['p_value']
    is_stationary = adf_result['is_stationary']
    
    # Step 4: Calculate residual statistics
    residual_mean = float(np.mean(residuals))
    residual_std_dev = float(np.std(residuals))  # FIXED sigma per Varsity
    
    # Step 5: Current z-score (using FIXED sigma per Zerodha Varsity)
    # Z-Score = Today's Residual / Fixed Sigma (NOT rolling)
    # This is the correct method per starb.pdf
    current_residual = float(residuals[-1])
    
    if residual_std_dev > 0:
        # Use FIXED sigma from regression (correct method per Varsity)
        z_score = current_residual / residual_std_dev
    else:
        z_score = 0.0
    
    # Step 6: Quality assessment
    quality, confidence_score = _assess_quality(adf_value, error_ratio)
    
    # Determine sector (use first stock's sector)
    sector = stock_a.sector if stock_a.sector else stock_b.sector
    
    return PairAnalysis(
        x_stock=x_stock,
        y_stock=y_stock,
        sector=sector,
        intercept=intercept,
        beta=beta,
        error_ratio=error_ratio,
        adf_value=adf_value,
        is_stationary=is_stationary,
        residuals=residuals,
        residual_mean=residual_mean,
        residual_std_dev=residual_std_dev,
        current_residual=current_residual,
        z_score=float(z_score),
        quality=quality,
        confidence_score=confidence_score
    )


def analyze_pair_from_prices(
    prices_a: Union[np.ndarray, List[float]],
    prices_b: Union[np.ndarray, List[float]],
    symbol_a: str,
    symbol_b: str,
    sector: str = "UNKNOWN",
    lookback: int = 20
) -> PairAnalysis:
    """
    Analyze pair directly from price arrays.
    
    Simplified interface when you don't have full StockData objects.
    
    Args:
        prices_a: Price array for first stock
        prices_b: Price array for second stock
        symbol_a: Symbol for first stock
        symbol_b: Symbol for second stock
        sector: Common sector (default "UNKNOWN")
        lookback: Rolling window for z-score
        
    Returns:
        Complete PairAnalysis object
    """
    prices_a = np.array(prices_a)
    prices_b = np.array(prices_b)
    
    # Step 1: Determine optimal X/Y
    optimal = calculate_optimal_direction_from_prices(
        prices_a, prices_b, symbol_a, symbol_b
    )
    
    x_stock = optimal['X']
    y_stock = optimal['Y']
    regression = optimal['regression']
    error_ratio = optimal['error_ratio']
    
    # Step 2: Regression parameters
    intercept = regression.intercept
    beta = regression.beta
    residuals = regression.residuals
    
    # Step 3: Stationarity
    adf_result = perform_adf_test_statsmodels(residuals)
    adf_value = adf_result['p_value']
    is_stationary = adf_result['is_stationary']
    
    # Step 4: Residual statistics
    residual_mean = float(np.mean(residuals))
    residual_std_dev = float(np.std(residuals))  # FIXED sigma per Varsity
    
    # Step 5: Z-score (using FIXED sigma per Zerodha Varsity)
    # Z-Score = Today's Residual / Fixed Sigma (NOT rolling)
    current_residual = float(residuals[-1])
    
    if residual_std_dev > 0:
        # Use FIXED sigma from regression (correct method per Varsity)
        z_score = current_residual / residual_std_dev
    else:
        z_score = 0.0
    
    # Step 6: Quality
    quality, confidence_score = _assess_quality(adf_value, error_ratio)
    
    return PairAnalysis(
        x_stock=x_stock,
        y_stock=y_stock,
        sector=sector,
        intercept=intercept,
        beta=beta,
        error_ratio=error_ratio,
        adf_value=adf_value,
        is_stationary=is_stationary,
        residuals=residuals,
        residual_mean=residual_mean,
        residual_std_dev=residual_std_dev,
        current_residual=current_residual,
        z_score=float(z_score),
        quality=quality,
        confidence_score=confidence_score
    )


def _assess_quality(adf_value: float, error_ratio: float) -> tuple:
    """
    Assess pair quality based on ADF and error ratio.
    
    From architecture spec:
    - EXCELLENT: ADF ≤ 0.01 AND error_ratio < 0.15, confidence 95
    - GOOD: ADF ≤ 0.05 AND error_ratio < 0.25, confidence 85
    - FAIR: ADF ≤ 0.10 AND error_ratio < 0.40, confidence 70
    - POOR: Otherwise, confidence 40
    
    Args:
        adf_value: ADF p-value
        error_ratio: Error ratio value
        
    Returns:
        Tuple of (quality_string, confidence_score)
    """
    if adf_value <= ADF_EXCELLENT and error_ratio < ERROR_RATIO_EXCELLENT:
        return QUALITY_EXCELLENT, 95.0
    elif adf_value <= ADF_THRESHOLD and error_ratio < ERROR_RATIO_GOOD:
        return QUALITY_GOOD, 85.0
    elif adf_value <= 0.10 and error_ratio < ERROR_RATIO_MAX:
        return QUALITY_FAIR, 70.0
    else:
        return QUALITY_POOR, 40.0


def update_pair_z_score(
    pair: PairAnalysis,
    price_x: float,
    price_y: float
) -> PairAnalysis:
    """
    Update a pair's z-score with new live prices.
    
    This creates a new PairAnalysis with updated current values.
    
    Args:
        pair: Existing PairAnalysis
        price_x: Current X stock price
        price_y: Current Y stock price
        
    Returns:
        Updated PairAnalysis (new object)
    """
    # Calculate new residual
    predicted_y = pair.intercept + (pair.beta * price_x)
    new_residual = price_y - predicted_y
    
    # Calculate new z-score using FIXED sigma (per Zerodha Varsity)
    # Z = Residual / Sigma (NOT (residual - mean) / sigma)
    if pair.residual_std_dev > 0:
        new_z_score = new_residual / pair.residual_std_dev
    else:
        new_z_score = 0.0
    
    # Create updated pair (immutable pattern)
    return PairAnalysis(
        x_stock=pair.x_stock,
        y_stock=pair.y_stock,
        sector=pair.sector,
        intercept=pair.intercept,
        beta=pair.beta,
        error_ratio=pair.error_ratio,
        adf_value=pair.adf_value,
        is_stationary=pair.is_stationary,
        residuals=pair.residuals,
        residual_mean=pair.residual_mean,
        residual_std_dev=pair.residual_std_dev,
        current_residual=float(new_residual),
        z_score=float(new_z_score),
        quality=pair.quality,
        confidence_score=pair.confidence_score
    )
