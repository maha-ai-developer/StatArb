"""
Corporate Actions Checker Module

Uses the existing GeminiAgent with Google Search to detect:
- Stock splits and bonuses
- Dividend ex-dates
- Rights issues
- Mergers and acquisitions

This helps adjust for price discontinuities in historical data.
"""

import os
import sys
from typing import Dict, List, Optional
from datetime import datetime

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import infrastructure.config as config


class CorporateActionsChecker:
    """
    Checks for corporate actions that may affect pair trading.
    
    Uses LLM + Google Search for real-time corporate action detection.
    Caches results to avoid repeated API calls.
    """
    
    def __init__(self):
        self.cache: Dict[str, Dict] = {}
        self._agent = None  # Lazy load
    
    def _get_agent(self):
        """Lazy load GeminiAgent to avoid import errors if no API key."""
        if self._agent is None:
            try:
                from infrastructure.llm.client import GeminiAgent
                self._agent = GeminiAgent()
            except Exception as e:
                print(f"   ⚠️ CorporateActionsChecker: LLM not available ({e})")
                self._agent = False  # Mark as unavailable
        return self._agent
    
    def check_symbols(self, symbols: List[str]) -> Dict[str, List[Dict]]:
        """
        Check for recent corporate actions affecting symbols.
        
        Args:
            symbols: List of stock symbols to check
            
        Returns:
            Dict mapping symbol -> list of alerts
        """
        if not symbols:
            return {}
        
        # Filter out already cached symbols
        uncached = [s for s in symbols if s not in self.cache]
        
        if uncached:
            agent = self._get_agent()
            if agent and agent is not False:
                try:
                    result = agent.monitor_corporate_actions(uncached)
                    
                    # Organize by symbol
                    for alert in result.get('alerts', []):
                        sym = alert.get('symbol', '')
                        if sym:
                            if sym not in self.cache:
                                self.cache[sym] = []
                            self.cache[sym].append(alert)
                    
                    # Mark symbols with no alerts as checked
                    for sym in uncached:
                        if sym not in self.cache:
                            self.cache[sym] = []
                            
                except Exception as e:
                    print(f"   ⚠️ Corporate actions check failed: {e}")
                    # Mark all as checked (no alerts)
                    for sym in uncached:
                        self.cache[sym] = []
            else:
                # No LLM available, return empty
                for sym in uncached:
                    self.cache[sym] = []
        
        # Return results for requested symbols
        return {s: self.cache.get(s, []) for s in symbols}
    
    def has_critical_action(self, symbol: str) -> bool:
        """Check if a symbol has any CRITICAL corporate action."""
        alerts = self.cache.get(symbol, [])
        return any(a.get('severity') == 'CRITICAL' for a in alerts)
    
    def has_price_adjustment(self, symbol: str) -> bool:
        """Check if symbol has split/bonus that affects prices."""
        alerts = self.cache.get(symbol, [])
        adjustment_types = {'SPLIT', 'BONUS', 'RIGHTS'}
        return any(a.get('action_type') in adjustment_types for a in alerts)
    
    def get_adjustment_factor(self, symbol: str, action_date: str = None) -> float:
        """
        Get price adjustment factor for a symbol.
        
        For now returns 1.0 (no adjustment).
        In future: parse split ratios from alerts.
        
        Args:
            symbol: Stock symbol
            action_date: Date of action (YYYY-MM-DD)
            
        Returns:
            Adjustment factor (e.g., 0.5 for 1:2 split)
        """
        # TODO: Parse split ratios from alerts
        # For now, just flag and return 1.0
        return 1.0
    
    def get_summary(self, symbols: List[str]) -> Dict:
        """
        Get summary of corporate actions for symbols.
        
        Returns:
            Dict with counts and lists of affected symbols
        """
        result = {
            'total_symbols': len(symbols),
            'with_alerts': [],
            'critical': [],
            'price_adjustments': [],
            'total_alerts': 0
        }
        
        # Check all symbols first
        self.check_symbols(symbols)
        
        for sym in symbols:
            alerts = self.cache.get(sym, [])
            if alerts:
                result['with_alerts'].append(sym)
                result['total_alerts'] += len(alerts)
                
                if self.has_critical_action(sym):
                    result['critical'].append(sym)
                
                if self.has_price_adjustment(sym):
                    result['price_adjustments'].append(sym)
        
        return result
    
    def print_alerts(self, symbols: List[str]):
        """Print corporate action alerts for symbols."""
        summary = self.get_summary(symbols)
        
        print(f"\n📋 Corporate Actions Check ({summary['total_symbols']} symbols)")
        print("=" * 50)
        
        if not summary['with_alerts']:
            print("   ✅ No corporate actions detected")
            return
        
        print(f"   ⚠️ {len(summary['with_alerts'])} symbols with alerts")
        
        if summary['critical']:
            print(f"   🚨 CRITICAL: {', '.join(summary['critical'])}")
        
        if summary['price_adjustments']:
            print(f"   📊 Price adjustments needed: {', '.join(summary['price_adjustments'])}")
        
        # Detail view
        for sym in summary['with_alerts']:
            alerts = self.cache.get(sym, [])
            for alert in alerts:
                severity_icon = '🚨' if alert.get('severity') == 'CRITICAL' else '⚠️' if alert.get('severity') == 'WARNING' else 'ℹ️'
                print(f"\n   {severity_icon} {sym}: {alert.get('action_type', 'UNKNOWN')}")
                print(f"      {alert.get('headline', 'No details')}")
                if alert.get('recommendation'):
                    print(f"      → {alert.get('recommendation')}")


def check_corporate_actions(symbols: List[str]) -> Dict:
    """Convenience function to check corporate actions."""
    checker = CorporateActionsChecker()
    return checker.get_summary(symbols)


if __name__ == "__main__":
    # Test with sample symbols
    test_symbols = ["SBIN", "HDFCBANK", "RELIANCE", "TCS"]
    
    checker = CorporateActionsChecker()
    checker.print_alerts(test_symbols)
