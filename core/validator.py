"""
Unified Pair Trading System - Comprehensive Validator (Module 9)

100-point scoring system for pair validation:
- ADF Test: 25 points
- Z-Score Signal: 20 points
- Intercept Risk: 30 points
- Position Sizing: 25 points

Thresholds:
- ≥80 points: EXCELLENT - Highly recommended
- ≥60 points: GOOD - Acceptable with caution
- ≥40 points: MARGINAL - High risk
- <40 points: POOR - Not recommended
"""

from typing import Optional
from .models import PairAnalysis, PositionSizing, RiskAssessment
from .intercept_risk import assess_intercept_risk, calculate_intercept_score
from .constants import (
    SCORE_ADF, SCORE_ZSCORE, SCORE_INTERCEPT, SCORE_POSITION, SCORE_MAX,
    SCORE_EXCELLENT, SCORE_GOOD, SCORE_MARGINAL,
    ADF_THRESHOLD, ENTRY_THRESHOLD, STOP_LOSS_THRESHOLD,
    BETA_DEVIATION_ACCEPTABLE
)


def validate_pair_for_trading(
    pair: PairAnalysis,
    sizing: PositionSizing,
    risk: Optional[RiskAssessment] = None
) -> RiskAssessment:
    """
    Comprehensive pair validation with 100-point scoring.
    
    Algorithm from unified_pair_trading.txt Module 9:
    1. Check ADF test (25 points if ≤ 0.05)
    2. Check Z-Score signal (20 points if 2.5-3.0, 10 if >3.0)
    3. Check Intercept risk (30/25/15/0 based on level)
    4. Check Position sizing (25/15/0 based on deviation)
    5. Calculate overall recommendation
    
    Args:
        pair: PairAnalysis from analyze_pair()
        sizing: PositionSizing from calculate_optimal_lots()
        risk: Optional pre-computed RiskAssessment
        
    Returns:
        Updated RiskAssessment with full scoring
    """
    warnings = []
    
    # If no risk assessment provided, compute intercept risk
    if risk is None:
        risk = assess_intercept_risk(
            pair.intercept,
            pair.beta,
            1.0,  # Placeholder - will need actual Y price
            1.0   # Placeholder - will need actual X price
        )
    
    # Start with existing risk values but reset scores
    total_score = 0
    
    # ─────────────────────────────────────────────────────────────
    # Check 1: ADF Test (25 points)
    # ─────────────────────────────────────────────────────────────
    if pair.adf_value <= ADF_THRESHOLD:
        adf_score = SCORE_ADF
    else:
        adf_score = 0
        warnings.append(f"Residuals NOT stationary (ADF={pair.adf_value:.4f})")
    
    total_score += adf_score
    
    # ─────────────────────────────────────────────────────────────
    # Check 2: Z-Score Signal (20 points)
    # ─────────────────────────────────────────────────────────────
    abs_z = abs(pair.z_score)
    
    if abs_z >= ENTRY_THRESHOLD and abs_z <= STOP_LOSS_THRESHOLD:
        # Perfect entry zone
        z_score_score = SCORE_ZSCORE
    elif abs_z > STOP_LOSS_THRESHOLD:
        # Beyond 3.0 - extreme entry
        z_score_score = 10
        warnings.append(f"Z-Score beyond {STOP_LOSS_THRESHOLD:.1f} - extreme entry")
    else:
        # No signal
        z_score_score = 0
        warnings.append(f"No trading signal (Z={pair.z_score:.2f})")
    
    total_score += z_score_score
    
    # ─────────────────────────────────────────────────────────────
    # Check 3: Intercept Risk (30 points)
    # ─────────────────────────────────────────────────────────────
    intercept_score = calculate_intercept_score(risk.intercept_risk)
    
    if intercept_score == 0:
        warnings.append(
            f"High intercept risk: {risk.intercept_percent:.1f}% unexplained"
        )
    
    total_score += intercept_score
    
    # ─────────────────────────────────────────────────────────────
    # Check 4: Position Sizing (25 points)
    # ─────────────────────────────────────────────────────────────
    beta_dev = abs(sizing.beta_deviation) if sizing else 100.0
    
    if beta_dev < BETA_DEVIATION_ACCEPTABLE:
        position_score = SCORE_POSITION
    elif beta_dev < 10.0:
        position_score = 15
        warnings.append(f"Beta deviation: {beta_dev:.1f}%")
    else:
        position_score = 0
        warnings.append("Cannot achieve beta neutrality")
    
    total_score += position_score
    
    # Spot adjustment warning
    if sizing and sizing.spot_needed:
        warnings.append(f"Requires spot market: {sizing.spot_shares} shares")
    
    # ─────────────────────────────────────────────────────────────
    # Overall Decision
    # ─────────────────────────────────────────────────────────────
    score_percent = (total_score / SCORE_MAX) * 100
    
    if score_percent >= SCORE_EXCELLENT:
        recommendation = "EXCELLENT - Highly recommended"
        tradable = True
    elif score_percent >= SCORE_GOOD:
        recommendation = "GOOD - Acceptable with caution"
        tradable = True
    elif score_percent >= SCORE_MARGINAL:
        recommendation = "MARGINAL - High risk"
        tradable = False
    else:
        recommendation = "POOR - Not recommended"
        tradable = False
    
    # Return updated assessment
    return RiskAssessment(
        intercept_percent=risk.intercept_percent,
        explained_percent=risk.explained_percent,
        intercept_risk=risk.intercept_risk,
        adf_score=adf_score,
        z_score_score=z_score_score,
        intercept_score=intercept_score,
        position_score=position_score,
        total_score=total_score,
        max_score=SCORE_MAX,
        tradable=tradable,
        recommendation=recommendation,
        warnings=warnings
    )


def validate_pair_simple(
    pair: PairAnalysis,
    price_y: float,
    price_x: float,
    sizing: Optional[PositionSizing] = None
) -> RiskAssessment:
    """
    Simplified validation with automatic intercept risk calculation.
    
    Use when you have live prices and want full validation.
    
    Args:
        pair: PairAnalysis from analyze_pair()
        price_y: Current Y stock price
        price_x: Current X stock price
        sizing: Optional PositionSizing (uses dummy if not provided)
        
    Returns:
        Complete RiskAssessment with scoring
    """
    # Calculate intercept risk with actual prices
    risk = assess_intercept_risk(
        pair.intercept,
        pair.beta,
        price_y,
        price_x
    )
    
    # Create dummy sizing if not provided
    if sizing is None:
        sizing = PositionSizing(
            lots_y=1,
            lots_x=1,
            shares_y=1,
            shares_x=1,
            target_beta=pair.beta,
            actual_beta=pair.beta,
            beta_deviation=0.0,
            notional_y=price_y,
            notional_x=price_x,
            total_capital=price_y + price_x,
            spot_needed=False,
            spot_shares=0
        )
    
    return validate_pair_for_trading(pair, sizing, risk)


def format_validation_report(risk: RiskAssessment, pair: PairAnalysis = None) -> str:
    """
    Generate formatted validation report.
    
    Args:
        risk: RiskAssessment from validate_pair_for_trading()
        pair: Optional PairAnalysis for additional context
        
    Returns:
        Formatted report string
    """
    lines = [
        "═" * 50,
        "         VALIDATION SCORING",
        "═" * 50,
    ]
    
    if pair:
        lines.append(f"Pair: {pair.y_stock}/{pair.x_stock}")
        lines.append(f"Sector: {pair.sector}")
        lines.append("")
    
    lines.extend([
        f"✓ ADF Test:         {risk.adf_score:>3}/{SCORE_ADF}",
        f"✓ Z-Score Signal:   {risk.z_score_score:>3}/{SCORE_ZSCORE}",
        f"✓ Intercept Risk:   {risk.intercept_score:>3}/{SCORE_INTERCEPT}",
        f"✓ Position Sizing:  {risk.position_score:>3}/{SCORE_POSITION}",
        "─" * 50,
        f"  TOTAL SCORE:      {risk.total_score:>3}/{SCORE_MAX}",
        "",
        f"Decision: {risk.recommendation}",
        f"Tradable: {'YES ✓' if risk.tradable else 'NO ✗'}",
    ])
    
    if risk.warnings:
        lines.append("")
        lines.append("WARNINGS:")
        for warning in risk.warnings:
            lines.append(f"  ⚠ {warning}")
    
    lines.append("═" * 50)
    
    return "\n".join(lines)
