"""
Slippage Tracker Module - Execution Quality Monitoring

Tracks execution quality by comparing expected vs actual fill prices.
Per checklist: Slippage tracking and adjustment for realistic P&L.
"""

import sqlite3
import datetime
import os
from typing import Dict, List, Optional, Tuple

import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))))
import infrastructure.config as config


class SlippageTracker:
    """
    Tracks execution quality and slippage.
    
    NEW - Checklist Nice-to-Have: Slippage Tracking System
    
    Features:
    - Records expected vs actual fill prices
    - Calculates slippage per trade and aggregate
    - Generates execution quality reports
    """
    
    def __init__(self, db_path: str = None):
        """
        Initialize SlippageTracker.
        
        Args:
            db_path: Optional custom database path
        """
        self.db_path = db_path or os.path.join(config.DATA_DIR, "slippage.db")
        self.init_db()
        
        # In-memory tracking for current session
        self._session_records: List[Dict] = []
    
    def init_db(self):
        """Initialize slippage tracking database."""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS slippage (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    timestamp DATETIME,
                    order_id TEXT,
                    symbol TEXT,
                    side TEXT,
                    quantity INTEGER,
                    expected_price REAL,
                    actual_price REAL,
                    slippage_abs REAL,
                    slippage_pct REAL,
                    slippage_cost REAL,
                    strategy TEXT
                )
            ''')
            conn.commit()
            conn.close()
        except Exception as e:
            print(f"   ⚠️ SlippageTracker DB init failed: {e}")
    
    def record_execution(self, 
                         order_id: str,
                         symbol: str,
                         side: str,
                         quantity: int,
                         expected_price: float,
                         actual_price: float,
                         strategy: str = "StatArb") -> Dict:
        """
        Record an execution and calculate slippage.
        
        Args:
            order_id: Broker order ID
            symbol: Trading symbol
            side: "BUY" or "SELL"
            quantity: Trade quantity
            expected_price: Expected fill price
            actual_price: Actual fill price
            strategy: Strategy name
            
        Returns:
            Dict with slippage details
        """
        # Calculate slippage
        slippage_abs = actual_price - expected_price
        slippage_pct = (slippage_abs / expected_price * 100) if expected_price > 0 else 0
        
        # For BUY: positive slippage = paid more = bad
        # For SELL: negative slippage = received less = bad
        if side == "BUY":
            slippage_cost = slippage_abs * quantity
        else:
            slippage_cost = -slippage_abs * quantity
        
        record = {
            "timestamp": datetime.datetime.now().isoformat(),
            "order_id": order_id,
            "symbol": symbol,
            "side": side,
            "quantity": quantity,
            "expected_price": round(expected_price, 2),
            "actual_price": round(actual_price, 2),
            "slippage_abs": round(slippage_abs, 2),
            "slippage_pct": round(slippage_pct, 4),
            "slippage_cost": round(slippage_cost, 2),
            "strategy": strategy
        }
        
        # Store in session memory
        self._session_records.append(record)
        
        # Persist to database
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            cursor.execute('''
                INSERT INTO slippage 
                (timestamp, order_id, symbol, side, quantity, expected_price, 
                 actual_price, slippage_abs, slippage_pct, slippage_cost, strategy)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                record["timestamp"], order_id, symbol, side, quantity,
                expected_price, actual_price, slippage_abs, slippage_pct, 
                slippage_cost, strategy
            ))
            conn.commit()
            conn.close()
        except Exception as e:
            print(f"   ⚠️ Failed to save slippage record: {e}")
        
        return record
    
    def get_summary(self) -> Dict:
        """
        Get summary statistics for current session.
        
        Returns:
            Dict with aggregate slippage metrics
        """
        if not self._session_records:
            return {
                "trade_count": 0,
                "total_slippage_cost": 0.0,
                "avg_slippage_pct": 0.0,
                "worst_slippage_pct": 0.0,
                "best_slippage_pct": 0.0
            }
        
        slippage_costs = [r["slippage_cost"] for r in self._session_records]
        slippage_pcts = [r["slippage_pct"] for r in self._session_records]
        
        return {
            "trade_count": len(self._session_records),
            "total_slippage_cost": round(sum(slippage_costs), 2),
            "avg_slippage_pct": round(sum(slippage_pcts) / len(slippage_pcts), 4),
            "worst_slippage_pct": round(max(slippage_pcts), 4),
            "best_slippage_pct": round(min(slippage_pcts), 4)
        }
    
    def get_symbol_summary(self, symbol: str = None) -> List[Dict]:
        """
        Get slippage summary by symbol.
        
        Args:
            symbol: Optional filter for specific symbol
            
        Returns:
            List of dicts with per-symbol statistics
        """
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            if symbol:
                cursor.execute('''
                    SELECT symbol, 
                           COUNT(*) as trades,
                           AVG(slippage_pct) as avg_slippage,
                           SUM(slippage_cost) as total_cost
                    FROM slippage
                    WHERE symbol = ?
                    GROUP BY symbol
                ''', (symbol,))
            else:
                cursor.execute('''
                    SELECT symbol, 
                           COUNT(*) as trades,
                           AVG(slippage_pct) as avg_slippage,
                           SUM(slippage_cost) as total_cost
                    FROM slippage
                    GROUP BY symbol
                    ORDER BY total_cost DESC
                ''')
            
            rows = cursor.fetchall()
            conn.close()
            
            return [
                {
                    "symbol": row[0],
                    "trades": row[1],
                    "avg_slippage_pct": round(row[2] or 0, 4),
                    "total_cost": round(row[3] or 0, 2)
                }
                for row in rows
            ]
            
        except Exception as e:
            print(f"   ⚠️ Failed to get symbol summary: {e}")
            return []
    
    def estimate_impact_cost(self, 
                             price: float, 
                             quantity: int, 
                             side: str,
                             lot_size: int = 1) -> Tuple[float, float]:
        """
        Estimate market impact cost for a planned trade.
        
        Uses historical slippage data to estimate expected slippage.
        
        Args:
            price: Current price
            quantity: Planned quantity
            side: "BUY" or "SELL"
            lot_size: Contract lot size
            
        Returns:
            Tuple of (expected_slippage_pct, estimated_cost)
        """
        summary = self.get_summary()
        
        if summary["trade_count"] > 0:
            expected_pct = summary["avg_slippage_pct"]
        else:
            # Default estimate: 0.05% slippage
            expected_pct = 0.05
        
        trade_value = price * quantity
        estimated_cost = trade_value * (expected_pct / 100)
        
        return expected_pct, round(estimated_cost, 2)
    
    def clear_session(self):
        """Clear current session records."""
        self._session_records = []
