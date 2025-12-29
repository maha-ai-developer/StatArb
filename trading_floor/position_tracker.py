"""
Position Tracker - Zerodha Varsity Style

Mirrors the Position-Tracker.xlsx from Zerodha Varsity:
- Real-time Z-score monitoring
- Entry/Current price tracking
- P&L calculation per leg and total
- Time-based logging

Usage:
    tracker = PositionTracker(pair_config)
    tracker.open_position(entry_price_y, entry_price_x, position_type='LONG')
    tracker.update(current_price_y, current_price_x)
    tracker.display()
"""

import os
import sys
import json
from datetime import datetime
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass, asdict, field
from tabulate import tabulate

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


@dataclass
class PairConfig:
    """Configuration for a trading pair (from regression output)."""
    stock_y: str            # Y stock (dependent)
    stock_x: str            # X stock (independent)
    sector: str
    beta: float             # Hedge ratio
    intercept: float        # Regression intercept
    sigma: float            # Standard error of residuals (FIXED)
    lot_size_y: int
    lot_size_x: int
    adf_value: float = 0.0
    
    @classmethod
    def from_dict(cls, d: Dict) -> 'PairConfig':
        return cls(
            stock_y=d.get('leg1') or d.get('stock_y'),
            stock_x=d.get('leg2') or d.get('stock_x'),
            sector=d.get('sector', 'UNKNOWN'),
            beta=d.get('beta') or d.get('hedge_ratio', 1.0),
            intercept=d.get('intercept', 0.0),
            sigma=d.get('sigma', 0.0),
            lot_size_y=d.get('lot_size_y', 1),
            lot_size_x=d.get('lot_size_x', 1),
            adf_value=d.get('adf', 0.0)
        )


@dataclass
class PositionState:
    """Current position state."""
    is_open: bool = False
    position_type: str = ""  # 'LONG' or 'SHORT' (on the pair/spread)
    entry_date: str = ""
    entry_time: str = ""
    entry_price_y: float = 0.0
    entry_price_x: float = 0.0
    entry_z_score: float = 0.0
    lots_y: int = 0
    lots_x: int = 0
    # Current state
    current_price_y: float = 0.0
    current_price_x: float = 0.0
    current_z_score: float = 0.0
    # P&L
    pnl_y: float = 0.0
    pnl_x: float = 0.0
    total_pnl: float = 0.0


@dataclass
class LogEntry:
    """Time-based log entry (mirrors Varsity Excel Logs sheet)."""
    date: str
    time: str
    fut_x: float
    fut_y: float
    z_score: float


class PositionTracker:
    """
    Zerodha Varsity-style Position Tracker.
    
    Features:
    - Real-time Z-score calculation using FIXED sigma
    - Entry at ±2.5 SD, Target at ±1.0 SD, Stop at ±3.0 SD
    - P&L tracking per leg and total
    - Time-series logging
    """
    
    # Zerodha Varsity thresholds (Page 47)
    ENTRY_THRESHOLD = 2.5
    TARGET_THRESHOLD = 1.0
    STOP_THRESHOLD = 3.0
    
    def __init__(self, pair_config: PairConfig):
        self.config = pair_config
        self.position = PositionState()
        self.logs: List[LogEntry] = []
        self._last_update = None
    
    def calculate_z_score(self, price_y: float, price_x: float) -> float:
        """
        Calculate Z-score using FIXED sigma (Varsity Page 49-50).
        
        Z = (Current Residual) / Sigma
        Residual = Y - (beta * X + intercept)
        """
        if self.config.sigma <= 0:
            return 0.0
        
        residual = price_y - (self.config.beta * price_x + self.config.intercept)
        return residual / self.config.sigma
    
    def check_entry_signal(self, price_y: float, price_x: float) -> Tuple[bool, str]:
        """
        Check if entry conditions are met.
        
        Returns:
            (should_enter, position_type)
            position_type: 'LONG' if z < -2.5 (buy Y, sell X)
                          'SHORT' if z > +2.5 (sell Y, buy X)
        """
        z = self.calculate_z_score(price_y, price_x)
        
        if z < -self.ENTRY_THRESHOLD:
            return True, 'LONG'  # Long Y, Short X
        elif z > self.ENTRY_THRESHOLD:
            return True, 'SHORT'  # Short Y, Long X
        
        return False, ''
    
    def check_exit_signal(self, price_y: float, price_x: float) -> Tuple[bool, str]:
        """
        Check if exit conditions are met.
        
        Returns:
            (should_exit, exit_reason)
        """
        if not self.position.is_open:
            return False, ''
        
        z = self.calculate_z_score(price_y, price_x)
        
        # Stop Loss: Z expands beyond ±3.0
        if abs(z) > self.STOP_THRESHOLD:
            return True, 'STOP_LOSS'
        
        # Target: Z reverts to ±1.0
        if self.position.position_type == 'LONG' and z > -self.TARGET_THRESHOLD:
            return True, 'TARGET'
        elif self.position.position_type == 'SHORT' and z < self.TARGET_THRESHOLD:
            return True, 'TARGET'
        
        return False, ''
    
    def open_position(
        self,
        price_y: float,
        price_x: float,
        position_type: str,
        lots_y: int = 1,
        lots_x: int = 1
    ):
        """
        Open a new position.
        
        Args:
            price_y: Entry price for Y (futures)
            price_x: Entry price for X (futures)
            position_type: 'LONG' (buy Y, sell X) or 'SHORT' (sell Y, buy X)
            lots_y: Number of lots for Y
            lots_x: Number of lots for X
        """
        now = datetime.now()
        z = self.calculate_z_score(price_y, price_x)
        
        self.position = PositionState(
            is_open=True,
            position_type=position_type,
            entry_date=now.strftime('%Y-%m-%d'),
            entry_time=now.strftime('%H:%M'),
            entry_price_y=price_y,
            entry_price_x=price_x,
            entry_z_score=z,
            lots_y=lots_y,
            lots_x=lots_x,
            current_price_y=price_y,
            current_price_x=price_x,
            current_z_score=z,
            pnl_y=0.0,
            pnl_x=0.0,
            total_pnl=0.0
        )
        
        # Log entry
        self._add_log(price_y, price_x, z)
    
    def update(self, price_y: float, price_x: float) -> Dict:
        """
        Update position with current prices.
        
        Returns dict with current state and any signals.
        """
        z = self.calculate_z_score(price_y, price_x)
        
        # Update position state
        self.position.current_price_y = price_y
        self.position.current_price_x = price_x
        self.position.current_z_score = z
        
        # Calculate P&L if position is open
        if self.position.is_open:
            self._calculate_pnl()
        
        # Check for exit signals
        should_exit, exit_reason = self.check_exit_signal(price_y, price_x)
        
        # Log update
        self._add_log(price_y, price_x, z)
        
        return {
            'z_score': round(z, 4),
            'price_y': price_y,
            'price_x': price_x,
            'pnl': self.position.total_pnl,
            'should_exit': should_exit,
            'exit_reason': exit_reason,
            'position_type': self.position.position_type,
            'is_open': self.position.is_open
        }
    
    def close_position(self, price_y: float, price_x: float, reason: str = 'MANUAL') -> Dict:
        """
        Close the current position.
        
        Returns final P&L breakdown.
        """
        if not self.position.is_open:
            return {'error': 'No open position'}
        
        # Final update
        self.update(price_y, price_x)
        
        result = {
            'pair': f"{self.config.stock_y}-{self.config.stock_x}",
            'position_type': self.position.position_type,
            'entry_date': self.position.entry_date,
            'entry_time': self.position.entry_time,
            'exit_date': datetime.now().strftime('%Y-%m-%d'),
            'exit_time': datetime.now().strftime('%H:%M'),
            'entry_z': self.position.entry_z_score,
            'exit_z': self.position.current_z_score,
            'entry_y': self.position.entry_price_y,
            'entry_x': self.position.entry_price_x,
            'exit_y': price_y,
            'exit_x': price_x,
            'pnl_y': self.position.pnl_y,
            'pnl_x': self.position.pnl_x,
            'total_pnl': self.position.total_pnl,
            'exit_reason': reason
        }
        
        # Reset position
        self.position = PositionState()
        
        return result
    
    def _calculate_pnl(self):
        """Calculate current P&L (mirrors Varsity Excel P&L section)."""
        qty_y = self.position.lots_y * self.config.lot_size_y
        qty_x = self.position.lots_x * self.config.lot_size_x
        
        if self.position.position_type == 'LONG':
            # Long Y, Short X
            # Y P&L = (Current - Entry) * Qty
            self.position.pnl_y = (self.position.current_price_y - self.position.entry_price_y) * qty_y
            # X P&L = (Entry - Current) * Qty (short position)
            self.position.pnl_x = (self.position.entry_price_x - self.position.current_price_x) * qty_x
        else:
            # Short Y, Long X
            self.position.pnl_y = (self.position.entry_price_y - self.position.current_price_y) * qty_y
            self.position.pnl_x = (self.position.current_price_x - self.position.entry_price_x) * qty_x
        
        self.position.total_pnl = self.position.pnl_y + self.position.pnl_x
    
    def _add_log(self, price_y: float, price_x: float, z_score: float):
        """Add a time-based log entry."""
        now = datetime.now()
        entry = LogEntry(
            date=now.strftime('%Y-%m-%d'),
            time=now.strftime('%H:%M'),
            fut_x=price_x,
            fut_y=price_y,
            z_score=round(z_score, 4)
        )
        self.logs.append(entry)
        self._last_update = now
    
    def display(self) -> str:
        """
        Display position tracker in Varsity Excel format.
        
        Returns formatted string for terminal display.
        """
        output = []
        output.append("")
        output.append("=" * 70)
        output.append("📊 POSITION TRACKER (Zerodha Varsity Style)")
        output.append("=" * 70)
        
        # Pair Info
        output.append(f"\n🔗 Pair: {self.config.stock_y} (Y) ↔ {self.config.stock_x} (X)")
        output.append(f"   Sector: {self.config.sector}")
        output.append(f"   Beta: {self.config.beta:.4f} | Intercept: {self.config.intercept:.2f} | Sigma: {self.config.sigma:.4f}")
        output.append(f"   Lot Y: {self.config.lot_size_y} | Lot X: {self.config.lot_size_x}")
        
        # Current State (mirrors Entry/Current columns in Excel)
        output.append("\n" + "-" * 70)
        output.append("📈 CURRENT STATE")
        output.append("-" * 70)
        
        if self.position.is_open:
            headers = ['', 'Entry', 'Current']
            data = [
                ['Fut (X)', f'{self.position.entry_price_x:.2f}', f'{self.position.current_price_x:.2f}'],
                ['Fut (Y)', f'{self.position.entry_price_y:.2f}', f'{self.position.current_price_y:.2f}'],
                ['Z-Score', f'{self.position.entry_z_score:.4f}', f'{self.position.current_z_score:.4f}'],
            ]
            output.append(tabulate(data, headers=headers, tablefmt='simple'))
        else:
            output.append("   No open position")
            if self.logs:
                last = self.logs[-1]
                output.append(f"   Last Z-Score: {last.z_score:.4f} @ {last.time}")
        
        # P&L Section (mirrors Excel P&L section)
        if self.position.is_open:
            output.append("\n" + "-" * 70)
            output.append("💰 P&L")
            output.append("-" * 70)
            
            pos_str = 'Long' if self.position.position_type == 'LONG' else 'Short'
            opp_str = 'Short' if self.position.position_type == 'LONG' else 'Long'
            
            headers = ['Stock', 'Position', 'Lot Size', 'Trade Price', 'Current Price', 'P&L']
            data = [
                [
                    f'{self.config.stock_y} (Y)',
                    pos_str,
                    self.position.lots_y * self.config.lot_size_y,
                    f'{self.position.entry_price_y:.2f}',
                    f'{self.position.current_price_y:.2f}',
                    f'{self.position.pnl_y:+,.0f}'
                ],
                [
                    f'{self.config.stock_x} (X)',
                    opp_str,
                    self.position.lots_x * self.config.lot_size_x,
                    f'{self.position.entry_price_x:.2f}',
                    f'{self.position.current_price_x:.2f}',
                    f'{self.position.pnl_x:+,.0f}'
                ],
                ['Total', '', '', '', '', f'{self.position.total_pnl:+,.0f}']
            ]
            output.append(tabulate(data, headers=headers, tablefmt='simple'))
        
        # Instructions (same as Excel)
        output.append("\n" + "-" * 70)
        output.append("📋 INSTRUCTIONS (Zerodha Varsity)")
        output.append("-" * 70)
        output.append(f"   1) Initiate trade when Z-Score is above +{self.ENTRY_THRESHOLD} or below -{self.ENTRY_THRESHOLD}")
        output.append(f"   2) Stop Loss when Z-Score hits +{self.STOP_THRESHOLD} or -{self.STOP_THRESHOLD}")
        output.append(f"   3) Target when Z-Score hits +{self.TARGET_THRESHOLD} or -{self.TARGET_THRESHOLD}")
        
        # Recent Logs (last 5)
        if self.logs:
            output.append("\n" + "-" * 70)
            output.append("📝 RECENT LOGS")
            output.append("-" * 70)
            headers = ['Date', 'Time', 'Fut (X)', 'Fut (Y)', 'Z-Score']
            data = [[l.date, l.time, f'{l.fut_x:.2f}', f'{l.fut_y:.2f}', f'{l.z_score:.4f}'] 
                    for l in self.logs[-5:]]
            output.append(tabulate(data, headers=headers, tablefmt='simple'))
        
        output.append("")
        return '\n'.join(output)
    
    def to_dict(self) -> Dict:
        """Export tracker state as dictionary (for persistence)."""
        return {
            'config': asdict(self.config),
            'position': asdict(self.position),
            'logs': [asdict(l) for l in self.logs[-100:]],  # Keep last 100 logs
            'last_update': self._last_update.isoformat() if self._last_update else None
        }
    
    def save(self, filepath: str):
        """Save tracker state to JSON file."""
        with open(filepath, 'w') as f:
            json.dump(self.to_dict(), f, indent=2)
    
    @classmethod
    def load(cls, filepath: str) -> 'PositionTracker':
        """Load tracker from JSON file."""
        with open(filepath, 'r') as f:
            data = json.load(f)
        
        config = PairConfig(**data['config'])
        tracker = cls(config)
        tracker.position = PositionState(**data['position'])
        tracker.logs = [LogEntry(**l) for l in data.get('logs', [])]
        
        if data.get('last_update'):
            tracker._last_update = datetime.fromisoformat(data['last_update'])
        
        return tracker


# ============================================================
# DEMO / TEST
# ============================================================

if __name__ == "__main__":
    # Example: Tata Motors / Tata Motors DVR (from Varsity Excel)
    config = PairConfig(
        stock_y="TATAMOTORS",
        stock_x="TATAMTRDVR",
        sector="AUTO",
        beta=1.59,           # From regression
        intercept=50.0,      # Example
        sigma=25.0,          # Standard error of residuals
        lot_size_y=1500,
        lot_size_x=2500,
        adf_value=0.02
    )
    
    tracker = PositionTracker(config)
    
    # Simulate entry (Z < -2.5)
    print("\n🔔 Checking entry signal...")
    should_enter, pos_type = tracker.check_entry_signal(311.40, 181.25)
    print(f"   Should enter: {should_enter}, Type: {pos_type}")
    
    if should_enter:
        tracker.open_position(311.40, 181.25, pos_type, lots_y=1, lots_x=1)
        print(tracker.display())
    
    # Simulate price updates
    print("\n📊 Updating with new prices...")
    tracker.update(314.65, 178.85)
    print(tracker.display())
    
    # Check exit
    print("\n🔍 Checking exit signal...")
    should_exit, reason = tracker.check_exit_signal(314.65, 178.85)
    print(f"   Should exit: {should_exit}, Reason: {reason}")
