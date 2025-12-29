"""
Unified Pair Trading System - Signal Generator (Modules 11-12)

Module 11: Real-Time Residual & Z-Score Calculator
Module 12: Signal Generator (Entry/Exit/Stop)

Trading Signals:
- Entry: Z ≤ -2.5 (LONG) or Z ≥ +2.5 (SHORT)
- Exit: Z reverts to ±1.0 (mean reversion achieved)
- Stop Loss: Z beyond ±3.0 (structural break)
"""

from typing import Dict, Optional
from .models import PairAnalysis
from .constants import (
    ENTRY_THRESHOLD, EXIT_THRESHOLD, STOP_LOSS_THRESHOLD,
    DIRECTION_LONG_PAIR, DIRECTION_SHORT_PAIR
)


# ═══════════════════════════════════════════════════════════════════════════
# MODULE 11: Real-Time Calculator
# ═══════════════════════════════════════════════════════════════════════════

def calculate_live_z_score(
    price_x: float,
    price_y: float,
    pair: PairAnalysis
) -> Dict:
    """
    Calculate current residual and z-score from live prices.
    
    Algorithm from unified_pair_trading.txt Module 11:
    1. predicted_Y = intercept + (β × price_X)
    2. residual = price_Y - predicted_Y
    3. z_score = residual / residual_std_dev
    
    Args:
        price_x: Current X stock price
        price_y: Current Y stock price
        pair: PairAnalysis object with regression parameters
        
    Returns:
        Dictionary with:
        - residual: Current residual value
        - z_score: Current z-score
        - predicted_y: Model's predicted Y price
    """
    # Calculate current residual
    predicted_y = pair.intercept + (pair.beta * price_x)
    residual = price_y - predicted_y
    
    # Calculate z-score
    if pair.residual_std_dev > 0:
        z_score = residual / pair.residual_std_dev
    else:
        z_score = 0.0
    
    return {
        'residual': residual,
        'z_score': z_score,
        'predicted_y': predicted_y
    }


def calculate_live_z_score_from_params(
    price_x: float,
    price_y: float,
    intercept: float,
    beta: float,
    residual_std_dev: float
) -> Dict:
    """
    Calculate z-score using raw parameters instead of PairAnalysis object.
    
    Useful when you don't have a full PairAnalysis available.
    
    Args:
        price_x: Current X stock price
        price_y: Current Y stock price
        intercept: Regression intercept
        beta: Regression beta
        residual_std_dev: Historical standard deviation of residuals
        
    Returns:
        Dictionary with residual, z_score, predicted_y
    """
    predicted_y = intercept + (beta * price_x)
    residual = price_y - predicted_y
    
    if residual_std_dev > 0:
        z_score = residual / residual_std_dev
    else:
        z_score = 0.0
    
    return {
        'residual': residual,
        'z_score': z_score,
        'predicted_y': predicted_y
    }


# ═══════════════════════════════════════════════════════════════════════════
# MODULE 12: Signal Generator
# ═══════════════════════════════════════════════════════════════════════════

def generate_signal(
    z_score: float,
    current_position: str = "NONE"
) -> Dict:
    """
    Generate trading signal based on z-score.
    
    Algorithm from unified_pair_trading.txt Module 12:
    
    Entry signals (when NONE):
    - Z ≤ -2.5 → LONG PAIR (Buy Y, Sell X)
    - Z ≥ +2.5 → SHORT PAIR (Sell Y, Buy X)
    
    Exit signals (when LONG):
    - Z ≤ -3.0 → STOP LOSS
    - Z ≥ -1.0 → TARGET (mean reversion)
    
    Exit signals (when SHORT):
    - Z ≥ +3.0 → STOP LOSS
    - Z ≤ +1.0 → TARGET (mean reversion)
    
    Args:
        z_score: Current z-score value
        current_position: "NONE", "LONG", or "SHORT"
        
    Returns:
        Dictionary with:
        - action: "ENTER", "EXIT", or "HOLD"
        - type: Signal type (e.g., "LONG", "SHORT", "TARGET", "STOP_LOSS")
        - reason: Human-readable explanation
        - z_score: Input z-score value
    """
    signal = {
        'action': 'HOLD',
        'type': 'NONE',
        'reason': 'No signal',
        'z_score': z_score
    }
    
    # ─────────────────────────────────────────────────────────────
    # Entry Signals (when no position)
    # ─────────────────────────────────────────────────────────────
    if current_position == "NONE":
        if z_score <= -ENTRY_THRESHOLD:
            signal['action'] = 'ENTER'
            signal['type'] = 'LONG'
            signal['reason'] = f'Residual at {z_score:.2f} SD (undervalued)'
        elif z_score >= ENTRY_THRESHOLD:
            signal['action'] = 'ENTER'
            signal['type'] = 'SHORT'
            signal['reason'] = f'Residual at +{z_score:.2f} SD (overvalued)'
        else:
            signal['action'] = 'HOLD'
            signal['type'] = 'NONE'
            signal['reason'] = f'No signal (Z={z_score:.2f})'
    
    # ─────────────────────────────────────────────────────────────
    # Exit Signals for LONG position
    # ─────────────────────────────────────────────────────────────
    elif current_position == "LONG":
        if z_score <= -STOP_LOSS_THRESHOLD:
            signal['action'] = 'EXIT'
            signal['type'] = 'STOP_LOSS'
            signal['reason'] = f'Long stop loss at {z_score:.2f} SD'
        elif z_score >= -EXIT_THRESHOLD:
            signal['action'] = 'EXIT'
            signal['type'] = 'TARGET'
            signal['reason'] = 'Mean reversion achieved'
        else:
            signal['action'] = 'HOLD'
            signal['type'] = 'NONE'
            signal['reason'] = f'Position open (Z={z_score:.2f})'
    
    # ─────────────────────────────────────────────────────────────
    # Exit Signals for SHORT position
    # ─────────────────────────────────────────────────────────────
    elif current_position == "SHORT":
        if z_score >= STOP_LOSS_THRESHOLD:
            signal['action'] = 'EXIT'
            signal['type'] = 'STOP_LOSS'
            signal['reason'] = f'Short stop loss at +{z_score:.2f} SD'
        elif z_score <= EXIT_THRESHOLD:
            signal['action'] = 'EXIT'
            signal['type'] = 'TARGET'
            signal['reason'] = 'Mean reversion achieved'
        else:
            signal['action'] = 'HOLD'
            signal['type'] = 'NONE'
            signal['reason'] = f'Position open (Z={z_score:.2f})'
    
    return signal


def generate_signal_with_prices(
    price_x: float,
    price_y: float,
    pair: PairAnalysis,
    current_position: str = "NONE"
) -> Dict:
    """
    Combined function: calculate z-score and generate signal.
    
    Convenience function that combines Modules 11 and 12.
    
    Args:
        price_x: Current X stock price
        price_y: Current Y stock price
        pair: PairAnalysis object
        current_position: Current position status
        
    Returns:
        Dictionary with signal and live data
    """
    # Calculate live z-score
    live_data = calculate_live_z_score(price_x, price_y, pair)
    
    # Generate signal
    signal = generate_signal(live_data['z_score'], current_position)
    
    # Combine results
    return {
        **signal,
        'residual': live_data['residual'],
        'predicted_y': live_data['predicted_y']
    }


def get_trade_direction(signal_type: str) -> tuple:
    """
    Get trade actions for Y and X based on signal type.
    
    LONG PAIR: Buy Y, Sell X (residual undervalued)
    SHORT PAIR: Sell Y, Buy X (residual overvalued)
    
    Args:
        signal_type: "LONG" or "SHORT"
        
    Returns:
        Tuple of (y_action, x_action, direction)
    """
    if signal_type == "LONG":
        return ("BUY", "SELL", DIRECTION_LONG_PAIR)
    elif signal_type == "SHORT":
        return ("SELL", "BUY", DIRECTION_SHORT_PAIR)
    else:
        return (None, None, None)


def format_signal_summary(signal: Dict) -> str:
    """
    Format signal for display.
    
    Args:
        signal: Signal dictionary from generate_signal()
        
    Returns:
        Formatted string
    """
    z = signal['z_score']
    action = signal['action']
    sig_type = signal['type']
    reason = signal['reason']
    
    if action == 'ENTER':
        return f"📈 {action} {sig_type} | Z={z:.2f} | {reason}"
    elif action == 'EXIT':
        return f"📉 {action} ({sig_type}) | Z={z:.2f} | {reason}"
    else:
        return f"⏸️ {action} | Z={z:.2f} | {reason}"
