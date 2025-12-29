"""
Unified Pair Trading System - Batch Pair Screener (Module 6)

Screens multiple stock combinations within a sector to find
cointegrated pairs suitable for trading.

Process:
1. Fetch historical data for all symbols
2. Filter by sector
3. Test all pair combinations
4. Return only stationary pairs (ADF ≤ 0.05)
5. Sort by quality
"""

import numpy as np
from typing import List, Dict, Optional, Callable
from itertools import combinations

from .models import StockData, PairAnalysis
from .pair_analyzer import analyze_pair, analyze_pair_from_prices
from .constants import LOOKBACK_PERIOD, MIN_DATA_COVERAGE


def screen_sector_pairs(
    sector: str,
    stock_data: Dict[str, StockData],
    min_quality: str = "FAIR"
) -> List[PairAnalysis]:
    """
    Screen all pair combinations within a sector.
    
    Algorithm from unified_pair_trading.txt Module 6:
    1. Filter stocks by sector
    2. Test all combinations
    3. Keep only stationary pairs (ADF ≤ 0.05)
    4. Sort by quality (ADF ascending)
    
    Args:
        sector: Sector to screen (e.g., "Banking", "IT")
        stock_data: Dictionary of symbol -> StockData
        min_quality: Minimum quality to include ("EXCELLENT", "GOOD", "FAIR", "POOR")
        
    Returns:
        List of valid PairAnalysis objects, sorted by ADF value
    """
    # Filter by sector
    sector_stocks = [
        stock for stock in stock_data.values()
        if stock.sector == sector
    ]
    
    if len(sector_stocks) < 2:
        return []
    
    valid_pairs = []
    
    # Test all combinations
    for stock_a, stock_b in combinations(sector_stocks, 2):
        try:
            pair = analyze_pair(stock_a, stock_b)
            
            # Only include stationary pairs
            if pair.is_stationary and _meets_quality_threshold(pair.quality, min_quality):
                valid_pairs.append(pair)
                
        except Exception as e:
            # Skip pairs that fail analysis
            continue
    
    # Sort by ADF value (lower is better)
    valid_pairs.sort(key=lambda p: p.adf_value)
    
    return valid_pairs


def screen_all_sectors(
    stock_data: Dict[str, StockData],
    min_quality: str = "FAIR"
) -> Dict[str, List[PairAnalysis]]:
    """
    Screen all sectors for valid pairs.
    
    Args:
        stock_data: Dictionary of symbol -> StockData
        min_quality: Minimum quality threshold
        
    Returns:
        Dictionary of sector -> list of valid pairs
    """
    # Get unique sectors
    sectors = set(stock.sector for stock in stock_data.values() if stock.sector)
    
    results = {}
    for sector in sectors:
        pairs = screen_sector_pairs(sector, stock_data, min_quality)
        if pairs:
            results[sector] = pairs
    
    return results


def screen_pairs_from_symbols(
    symbol_pairs: List[tuple],
    price_fetcher: Callable[[str, int], np.ndarray],
    lookback_days: int = LOOKBACK_PERIOD,
    sector: str = "UNKNOWN"
) -> List[PairAnalysis]:
    """
    Screen specific symbol pairs using a price fetcher function.
    
    Useful when you have a pre-defined list of pairs to test.
    
    Args:
        symbol_pairs: List of (symbol_a, symbol_b) tuples
        price_fetcher: Function(symbol, days) -> price_array
        lookback_days: Days of historical data
        sector: Sector classification for all pairs
        
    Returns:
        List of valid PairAnalysis objects
    """
    valid_pairs = []
    
    for symbol_a, symbol_b in symbol_pairs:
        try:
            # Fetch prices
            prices_a = price_fetcher(symbol_a, lookback_days)
            prices_b = price_fetcher(symbol_b, lookback_days)
            
            # Check data coverage
            if len(prices_a) < lookback_days * MIN_DATA_COVERAGE:
                continue
            if len(prices_b) < lookback_days * MIN_DATA_COVERAGE:
                continue
            
            # Ensure same length
            min_len = min(len(prices_a), len(prices_b))
            prices_a = prices_a[-min_len:]
            prices_b = prices_b[-min_len:]
            
            # Analyze
            pair = analyze_pair_from_prices(
                prices_a, prices_b,
                symbol_a, symbol_b,
                sector=sector
            )
            
            if pair.is_stationary:
                valid_pairs.append(pair)
                
        except Exception as e:
            continue
    
    # Sort by ADF
    valid_pairs.sort(key=lambda p: p.adf_value)
    
    return valid_pairs


def screen_pairs_from_price_dict(
    prices: Dict[str, np.ndarray],
    sector: str = "UNKNOWN",
    min_quality: str = "FAIR"
) -> List[PairAnalysis]:
    """
    Screen all pair combinations from a dictionary of prices.
    
    Simplest interface when you already have price data loaded.
    
    Args:
        prices: Dictionary of symbol -> price_array
        sector: Sector classification
        min_quality: Minimum quality threshold
        
    Returns:
        List of valid PairAnalysis objects
    """
    symbols = list(prices.keys())
    
    if len(symbols) < 2:
        return []
    
    valid_pairs = []
    
    for sym_a, sym_b in combinations(symbols, 2):
        try:
            prices_a = prices[sym_a]
            prices_b = prices[sym_b]
            
            # Ensure same length
            min_len = min(len(prices_a), len(prices_b))
            prices_a = prices_a[-min_len:]
            prices_b = prices_b[-min_len:]
            
            pair = analyze_pair_from_prices(
                prices_a, prices_b,
                sym_a, sym_b,
                sector=sector
            )
            
            if pair.is_stationary and _meets_quality_threshold(pair.quality, min_quality):
                valid_pairs.append(pair)
                
        except Exception:
            continue
    
    valid_pairs.sort(key=lambda p: p.adf_value)
    
    return valid_pairs


def rank_pairs(pairs: List[PairAnalysis]) -> List[PairAnalysis]:
    """
    Rank pairs by multiple criteria.
    
    Ranking criteria (weighted):
    1. ADF p-value (lower is better) - 40%
    2. Error ratio (lower is better) - 30%
    3. Confidence score (higher is better) - 30%
    
    Args:
        pairs: List of PairAnalysis objects
        
    Returns:
        Sorted list with best pairs first
    """
    if not pairs:
        return []
    
    # Calculate composite score for each pair
    scored_pairs = []
    for pair in pairs:
        # Normalize each metric to 0-1 range
        adf_score = 1 - min(pair.adf_value, 1.0)  # Lower ADF is better
        error_score = 1 - min(pair.error_ratio, 1.0)  # Lower error is better
        confidence_score = pair.confidence_score / 100  # Already 0-100
        
        # Weighted composite
        composite = (adf_score * 0.4) + (error_score * 0.3) + (confidence_score * 0.3)
        scored_pairs.append((composite, pair))
    
    # Sort by composite score (descending)
    scored_pairs.sort(key=lambda x: x[0], reverse=True)
    
    return [pair for _, pair in scored_pairs]


def _meets_quality_threshold(quality: str, min_quality: str) -> bool:
    """Check if quality meets minimum threshold."""
    quality_order = {"EXCELLENT": 4, "GOOD": 3, "FAIR": 2, "POOR": 1}
    return quality_order.get(quality, 0) >= quality_order.get(min_quality, 0)


def format_screening_report(pairs: List[PairAnalysis], top_n: int = 10) -> str:
    """
    Generate a formatted screening report.
    
    Args:
        pairs: List of PairAnalysis objects
        top_n: Number of top pairs to show
        
    Returns:
        Formatted report string
    """
    if not pairs:
        return "No valid pairs found."
    
    lines = [
        "═" * 60,
        "           PAIR SCREENING REPORT",
        "═" * 60,
        f"Total Valid Pairs: {len(pairs)}",
        "",
        "Top Pairs:",
        "─" * 60,
        f"{'Pair':<20} │ {'Z-Score':>8} │ {'ADF':>7} │ {'Quality':>10}",
        "─" * 60,
    ]
    
    for pair in pairs[:top_n]:
        pair_name = f"{pair.y_stock}/{pair.x_stock}"
        lines.append(
            f"{pair_name:<20} │ {pair.z_score:>8.2f} │ "
            f"{pair.adf_value:>7.4f} │ {pair.quality:>10}"
        )
    
    lines.append("═" * 60)
    
    return "\n".join(lines)
