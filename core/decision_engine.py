"""
Unified Pair Trading System - Trade Decision Engine (Module 10)

Makes final trade decisions based on user risk profile:
- Conservative: Only excellent pairs (score ≥80, low intercept risk)
- Moderate: Good pairs acceptable (score ≥60, tradable)
- Aggressive: Signal + stationary sufficient (score ≥40)
"""

from typing import Dict, List, Optional
from .models import PairAnalysis, PositionSizing, RiskAssessment
from .validator import validate_pair_for_trading
from .constants import (
    PROFILE_CONSERVATIVE, PROFILE_MODERATE, PROFILE_AGGRESSIVE,
    SCORE_EXCELLENT, SCORE_GOOD, SCORE_MARGINAL,
    RISK_LOW, ENTRY_THRESHOLD
)


def make_trade_decision(
    pair: PairAnalysis,
    sizing: PositionSizing,
    risk: RiskAssessment,
    user_profile: str = PROFILE_MODERATE
) -> Dict:
    """
    Make final trade decision based on user risk profile.
    
    Algorithm from unified_pair_trading.txt Module 10:
    
    Conservative:
    - Requires score ≥80 AND low intercept risk
    - Only takes high-confidence trades
    
    Moderate:
    - Requires score ≥60 AND tradable flag
    - Balanced risk/reward approach
    
    Aggressive:
    - Requires stationary + signal present
    - Accepts higher risk for more opportunities
    
    Args:
        pair: PairAnalysis from analyze_pair()
        sizing: PositionSizing from calculate_optimal_lots()
        risk: RiskAssessment from validate_pair_for_trading()
        user_profile: "conservative", "moderate", or "aggressive"
        
    Returns:
        Dictionary with:
        - action: "TAKE TRADE" or "SKIP"
        - confidence: "HIGH", "MEDIUM", or "N/A"
        - reasons: List of explanation strings
        - pair: Original PairAnalysis
        - sizing: Original PositionSizing
        - risk: Original RiskAssessment
        - warnings: List of warning strings
    """
    decision = {
        'action': 'SKIP',
        'confidence': 'N/A',
        'reasons': [],
        'pair': pair,
        'sizing': sizing,
        'risk': risk,
        'warnings': risk.warnings.copy() if risk.warnings else []
    }
    
    score = risk.total_score
    
    # ─────────────────────────────────────────────────────────────
    # Conservative Profile
    # ─────────────────────────────────────────────────────────────
    if user_profile == PROFILE_CONSERVATIVE:
        if score >= SCORE_EXCELLENT and risk.intercept_risk == RISK_LOW:
            decision['action'] = 'TAKE TRADE'
            decision['confidence'] = 'HIGH'
            decision['reasons'].append(
                f"Excellent quality (score: {score}/{risk.max_score})"
            )
            decision['reasons'].append(
                f"Low intercept risk ({risk.intercept_percent:.1f}% unexplained)"
            )
        else:
            decision['action'] = 'SKIP'
            decision['reasons'].append("Does not meet conservative criteria")
            if score < SCORE_EXCELLENT:
                decision['reasons'].append(
                    f"Score {score}/{risk.max_score} < required {SCORE_EXCELLENT}"
                )
            if risk.intercept_risk != RISK_LOW:
                decision['reasons'].append(
                    f"Intercept risk '{risk.intercept_risk}' != required 'LOW'"
                )
    
    # ─────────────────────────────────────────────────────────────
    # Moderate Profile
    # ─────────────────────────────────────────────────────────────
    elif user_profile == PROFILE_MODERATE:
        if score >= SCORE_GOOD and risk.tradable:
            decision['action'] = 'TAKE TRADE'
            decision['confidence'] = 'HIGH' if score >= 75 else 'MEDIUM'
            decision['reasons'].append(
                f"Acceptable quality (score: {score}/{risk.max_score})"
            )
        else:
            decision['action'] = 'SKIP'
            decision['reasons'].append("Risk too high for moderate profile")
            if score < SCORE_GOOD:
                decision['reasons'].append(
                    f"Score {score}/{risk.max_score} < required {SCORE_GOOD}"
                )
            if not risk.tradable:
                decision['reasons'].append("Pair not tradable (intercept risk)")
    
    # ─────────────────────────────────────────────────────────────
    # Aggressive Profile
    # ─────────────────────────────────────────────────────────────
    elif user_profile == PROFILE_AGGRESSIVE:
        if pair.is_stationary and abs(pair.z_score) >= ENTRY_THRESHOLD:
            decision['action'] = 'TAKE TRADE'
            decision['confidence'] = 'MEDIUM'
            decision['reasons'].append(
                "Signal present despite elevated risk"
            )
            decision['reasons'].append(
                f"Stationary (ADF={pair.adf_value:.4f}), Z={pair.z_score:.2f}"
            )
        else:
            decision['action'] = 'SKIP'
            decision['reasons'].append("Fundamental criteria not met")
            if not pair.is_stationary:
                decision['reasons'].append("Not stationary")
            if abs(pair.z_score) < ENTRY_THRESHOLD:
                decision['reasons'].append(
                    f"No signal (|Z|={abs(pair.z_score):.2f} < {ENTRY_THRESHOLD})"
                )
    
    else:
        decision['action'] = 'SKIP'
        decision['reasons'].append(f"Unknown profile: {user_profile}")
    
    return decision


def batch_decisions(
    pairs: List[PairAnalysis],
    sizings: List[PositionSizing],
    risks: List[RiskAssessment],
    user_profile: str = PROFILE_MODERATE
) -> List[Dict]:
    """
    Make decisions for multiple pairs.
    
    Args:
        pairs: List of PairAnalysis objects
        sizings: List of corresponding PositionSizing objects
        risks: List of corresponding RiskAssessment objects
        user_profile: User risk profile
        
    Returns:
        List of decision dictionaries
    """
    decisions = []
    
    for pair, sizing, risk in zip(pairs, sizings, risks):
        decision = make_trade_decision(pair, sizing, risk, user_profile)
        decisions.append(decision)
    
    return decisions


def filter_tradable(decisions: List[Dict]) -> List[Dict]:
    """
    Filter decisions to only include tradable pairs.
    
    Args:
        decisions: List of decision dictionaries
        
    Returns:
        Filtered list with only "TAKE TRADE" decisions
    """
    return [d for d in decisions if d['action'] == 'TAKE TRADE']


def get_best_opportunities(
    decisions: List[Dict],
    max_count: int = 5
) -> List[Dict]:
    """
    Get top trade opportunities sorted by confidence.
    
    Args:
        decisions: List of decision dictionaries
        max_count: Maximum number to return
        
    Returns:
        Top opportunities sorted by quality
    """
    tradable = filter_tradable(decisions)
    
    # Sort by score (from risk assessment)
    tradable.sort(key=lambda d: d['risk'].total_score, reverse=True)
    
    return tradable[:max_count]


def format_decision_report(decision: Dict) -> str:
    """
    Generate formatted decision report.
    
    Args:
        decision: Decision dictionary from make_trade_decision()
        
    Returns:
        Formatted report string
    """
    pair = decision['pair']
    
    lines = [
        "═" * 50,
        "         TRADE DECISION",
        "═" * 50,
        f"Pair: {pair.y_stock}/{pair.x_stock}",
        f"Z-Score: {pair.z_score:.2f}",
        "",
        f"ACTION: {decision['action']}",
        f"Confidence: {decision['confidence']}",
        "",
        "Reasons:",
    ]
    
    for reason in decision['reasons']:
        lines.append(f"  • {reason}")
    
    if decision['warnings']:
        lines.append("")
        lines.append("Warnings:")
        for warning in decision['warnings']:
            lines.append(f"  ⚠ {warning}")
    
    lines.append("═" * 50)
    
    return "\n".join(lines)
