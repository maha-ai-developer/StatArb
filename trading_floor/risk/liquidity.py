"""
Liquidity Checker Module - Pre-Entry Validation

Validates market depth and bid-ask spread before trade entry.
Per checklist Phase 4.2: Spread Check Before Entry.
"""

from typing import Tuple, Dict, Optional
import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))))


class LiquidityChecker:
    """
    Validates market liquidity conditions before trade entry.
    
    Implements Checklist Phase 4.2:
    - Bid-ask spread check (max 0.2%)
    - Market depth validation (2x trade size needed in book)
    """
    
    # Configuration (can be overridden in constructor)
    MAX_SPREAD_PCT = 0.002       # 0.2% max bid-ask spread
    MAX_TOTAL_SPREAD = 0.004    # 0.4% total for both legs
    MIN_DEPTH_MULTIPLIER = 2    # Need 2x trade size in order book
    
    def __init__(self, max_spread_pct: float = None, min_depth_mult: float = None):
        """
        Initialize with optional custom thresholds.
        
        Args:
            max_spread_pct: Maximum allowed spread per leg (default 0.2%)
            min_depth_mult: Minimum depth multiplier (default 2x)
        """
        if max_spread_pct is not None:
            self.MAX_SPREAD_PCT = max_spread_pct
        if min_depth_mult is not None:
            self.MIN_DEPTH_MULTIPLIER = min_depth_mult
    
    def calculate_spread(self, bid: float, ask: float) -> Tuple[float, float]:
        """
        Calculate bid-ask spread.
        
        Args:
            bid: Best bid price
            ask: Best ask price
            
        Returns:
            Tuple of (spread_absolute, spread_percentage)
        """
        if bid <= 0 or ask <= 0:
            return 0.0, 0.0
        
        mid = (bid + ask) / 2
        spread_abs = ask - bid
        spread_pct = spread_abs / mid
        
        return spread_abs, spread_pct
    
    def check_spread(self, symbol: str, bid: float, ask: float) -> Tuple[bool, str]:
        """
        Check if bid-ask spread is acceptable.
        
        Args:
            symbol: Stock symbol for logging
            bid: Best bid price
            ask: Best ask price
            
        Returns:
            Tuple of (is_acceptable, reason_message)
        """
        spread_abs, spread_pct = self.calculate_spread(bid, ask)
        
        if spread_pct > self.MAX_SPREAD_PCT:
            return False, f"{symbol}: Spread {spread_pct*100:.2f}% > {self.MAX_SPREAD_PCT*100:.1f}% threshold"
        
        return True, f"{symbol}: Spread OK ({spread_pct*100:.2f}%)"
    
    def check_depth(self, symbol: str, required_qty: int, 
                    bid_qty: int, ask_qty: int) -> Tuple[bool, str]:
        """
        Check if market depth is sufficient for trade.
        
        Args:
            symbol: Stock symbol for logging
            required_qty: Quantity needed for trade
            bid_qty: Available quantity at best bid
            ask_qty: Available quantity at best ask
            
        Returns:
            Tuple of (is_acceptable, reason_message)
        """
        total_depth = bid_qty + ask_qty
        min_required = required_qty * self.MIN_DEPTH_MULTIPLIER
        
        if total_depth < min_required:
            return False, f"{symbol}: Depth {total_depth:,} < {min_required:,} required"
        
        return True, f"{symbol}: Depth OK ({total_depth:,} available)"
    
    def validate_entry(self, 
                       sym_y: str, bid_y: float, ask_y: float, qty_y: int,
                       sym_x: str, bid_x: float, ask_x: float, qty_x: int,
                       depth_y: Tuple[int, int] = None,
                       depth_x: Tuple[int, int] = None) -> Tuple[bool, str, Dict]:
        """
        Complete pre-entry validation for a pair trade.
        
        Args:
            sym_y, sym_x: Stock symbols
            bid_y, ask_y, bid_x, ask_x: Current quotes
            qty_y, qty_x: Required quantities
            depth_y: Tuple of (bid_qty, ask_qty) for Y (optional)
            depth_x: Tuple of (bid_qty, ask_qty) for X (optional)
            
        Returns:
            Tuple of (can_trade, message, details_dict)
        """
        details = {
            'y_spread_pct': 0.0,
            'x_spread_pct': 0.0,
            'total_spread_pct': 0.0,
            'y_depth_ok': True,
            'x_depth_ok': True,
            'passed': False
        }
        
        # Check spreads
        _, spread_y = self.calculate_spread(bid_y, ask_y)
        _, spread_x = self.calculate_spread(bid_x, ask_x)
        total_spread = spread_y + spread_x
        
        details['y_spread_pct'] = round(spread_y * 100, 3)
        details['x_spread_pct'] = round(spread_x * 100, 3)
        details['total_spread_pct'] = round(total_spread * 100, 3)
        
        # Check individual spreads
        y_ok, y_msg = self.check_spread(sym_y, bid_y, ask_y)
        x_ok, x_msg = self.check_spread(sym_x, bid_x, ask_x)
        
        if not y_ok:
            return False, f"Wide spread on {sym_y}: {spread_y*100:.2f}%", details
        
        if not x_ok:
            return False, f"Wide spread on {sym_x}: {spread_x*100:.2f}%", details
        
        # Check combined spread
        if total_spread > self.MAX_TOTAL_SPREAD:
            return False, f"Total spread {total_spread*100:.2f}% > {self.MAX_TOTAL_SPREAD*100:.1f}%", details
        
        # Check depths if provided
        if depth_y is not None:
            d_ok, d_msg = self.check_depth(sym_y, qty_y, depth_y[0], depth_y[1])
            details['y_depth_ok'] = d_ok
            if not d_ok:
                return False, d_msg, details
        
        if depth_x is not None:
            d_ok, d_msg = self.check_depth(sym_x, qty_x, depth_x[0], depth_x[1])
            details['x_depth_ok'] = d_ok
            if not d_ok:
                return False, d_msg, details
        
        details['passed'] = True
        return True, f"Liquidity OK: Y={spread_y*100:.2f}% X={spread_x*100:.2f}%", details
    
    def estimate_impact_cost(self, price: float, qty: int, side: str,
                             bid: float = None, ask: float = None) -> float:
        """
        Estimate market impact cost for a trade.
        
        Args:
            price: Current mid price
            qty: Trade quantity
            side: "BUY" or "SELL"
            bid: Best bid (optional, for better estimate)
            ask: Best ask (optional, for better estimate)
            
        Returns:
            Estimated cost as percentage of trade value
        """
        if bid is not None and ask is not None:
            spread_pct = (ask - bid) / price if price > 0 else 0
        else:
            spread_pct = 0.001  # Default 0.1% estimate
        
        # Simple model: half spread + size impact
        # Larger orders have more impact
        base_impact = spread_pct / 2
        size_factor = 1.0 + (qty / 10000) * 0.1  # Small adjustment for size
        
        return base_impact * size_factor
