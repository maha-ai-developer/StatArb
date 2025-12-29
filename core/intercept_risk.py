"""
Unified Pair Trading System - Intercept Risk Assessor (Module 8)

Evaluates the risk posed by the regression intercept.

High intercept means the model doesn't explain much of Y's price:
- If intercept represents >70% of Y's price → VERY HIGH risk
- If intercept represents <10% of Y's price → LOW risk

This is a KEY validation that can reject otherwise good-looking pairs.
"""

from typing import Dict
from .models import RiskAssessment
from .constants import (
    INTERCEPT_LOW_RISK,
    INTERCEPT_MODERATE,
    INTERCEPT_ELEVATED,
    INTERCEPT_HIGH_RISK,
    RISK_LOW,
    RISK_MODERATE,
    RISK_ELEVATED,
    RISK_HIGH,
    RISK_VERY_HIGH
)


def assess_intercept_risk(
    intercept: float,
    beta: float,
    price_y: float,
    price_x: float
) -> RiskAssessment:
    """
    Assess risk from regression intercept.
    
    Algorithm from unified_pair_trading.txt Module 8:
    1. Calculate explained value = β × priceX
    2. Calculate explained% = (explainedValue / priceY) × 100
    3. Calculate intercept% = (intercept / priceY) × 100
    4. Classify risk level
    
    Example (ICICI-HDFC case):
    - intercept = 1626, beta = 0.79, priceY = 2024.8, priceX = 298.8
    - explained = 0.79 × 298.8 = 236
    - explained% = 236/2024.8 = 11.7%
    - intercept% = 1626/2024.8 = 80.3%
    - Risk: VERY HIGH (model only explains ~12%)
    
    Args:
        intercept: Regression intercept
        beta: Regression beta (hedge ratio)
        price_y: Current Y stock price
        price_x: Current X stock price
        
    Returns:
        RiskAssessment with intercept analysis
    """
    if price_y <= 0:
        return RiskAssessment(
            intercept_percent=100.0,
            explained_percent=0.0,
            intercept_risk=RISK_VERY_HIGH,
            tradable=False,
            recommendation="INVALID - Y price is zero or negative"
        )
    
    # Calculate explained vs unexplained portions
    explained_value = abs(beta * price_x)
    explained_percent = (explained_value / price_y) * 100
    intercept_percent = abs(intercept / price_y) * 100
    
    # Risk classification
    risk, tradable, recommendation = _classify_intercept_risk(intercept_percent)
    
    return RiskAssessment(
        intercept_percent=intercept_percent,
        explained_percent=explained_percent,
        intercept_risk=risk,
        tradable=tradable,
        recommendation=recommendation,
        warnings=[]
    )


def _classify_intercept_risk(intercept_percent: float) -> tuple:
    """
    Classify intercept risk level.
    
    From architecture spec:
    - <10%: LOW - Model explains >90%
    - <25%: MODERATE - Model explains 75-90%
    - <50%: ELEVATED - Model explains 50-75%
    - <70%: HIGH - Model explains 30-50%
    - ≥70%: VERY HIGH - Model explains <30%
    
    Args:
        intercept_percent: Intercept as % of Y price
        
    Returns:
        Tuple of (risk_level, tradable, recommendation)
    """
    if intercept_percent < INTERCEPT_LOW_RISK:
        return (
            RISK_LOW,
            True,
            "EXCELLENT - Model explains >90%"
        )
    elif intercept_percent < INTERCEPT_MODERATE:
        return (
            RISK_MODERATE,
            True,
            "GOOD - Model explains 75-90%"
        )
    elif intercept_percent < INTERCEPT_ELEVATED:
        return (
            RISK_ELEVATED,
            True,
            "ACCEPTABLE - Model explains 50-75%"
        )
    elif intercept_percent < INTERCEPT_HIGH_RISK:
        return (
            RISK_HIGH,
            False,
            "CAUTION - Model explains 30-50%"
        )
    else:
        return (
            RISK_VERY_HIGH,
            False,
            "AVOID - Model explains <30%"
        )


def calculate_intercept_score(intercept_risk: str) -> int:
    """
    Convert intercept risk to score (out of 30 points).
    
    From architecture spec:
    - LOW: 30 points
    - MODERATE: 25 points
    - ELEVATED: 15 points
    - HIGH: 0 points
    - VERY HIGH: 0 points
    
    Args:
        intercept_risk: Risk classification string
        
    Returns:
        Score value (0-30)
    """
    from .constants import SCORE_INTERCEPT
    
    score_map = {
        RISK_LOW: SCORE_INTERCEPT,  # 30
        RISK_MODERATE: 25,
        RISK_ELEVATED: 15,
        RISK_HIGH: 0,
        RISK_VERY_HIGH: 0
    }
    
    return score_map.get(intercept_risk, 0)


def format_intercept_report(risk: RiskAssessment) -> str:
    """
    Generate formatted intercept risk report.
    
    Args:
        risk: RiskAssessment from assess_intercept_risk()
        
    Returns:
        Formatted report string
    """
    lines = [
        "INTERCEPT RISK ANALYSIS",
        "─" * 40,
        f"Unexplained (Intercept): {risk.intercept_percent:.2f}%",
        f"Explained by Model:      {risk.explained_percent:.2f}%",
        f"Risk Level:              {risk.intercept_risk}",
        f"Tradable:                {'YES ✓' if risk.tradable else 'NO ✗'}",
        f"Recommendation:          {risk.recommendation}",
    ]
    
    return "\n".join(lines)
