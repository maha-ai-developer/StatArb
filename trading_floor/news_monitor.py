"""
Corporate Actions & News Alerts Monitor - Phase 11

Monitors active positions for:
- Corporate actions (dividends, splits, M&A)
- Critical news that may affect trading
- Risk/volatility alerts

Integrates with trading_floor/alerts.py for notifications.
"""

import os
import sys
import json
from datetime import datetime
from typing import Dict, List, Optional

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import infrastructure.config as config


class CorporateActionsMonitor:
    """
    Monitors positions for corporate actions and critical news.
    
    Uses GeminiAgent with Google Search for real-time intelligence.
    """
    
    def __init__(self):
        self._agent = None
        self._alert_manager = None
        self._init_dependencies()
    
    def _init_dependencies(self):
        """Initialize LLM agent and alert manager."""
        try:
            from infrastructure.llm.client import GeminiAgent
            self._agent = GeminiAgent()
        except Exception as e:
            print(f"⚠️ LLM Agent init failed: {e}")
        
        try:
            from trading_floor.alerts import get_alert_manager
            self._alert_manager = get_alert_manager()
        except Exception as e:
            print(f"⚠️ Alert Manager init failed: {e}")
    
    def scan_active_positions(self, active_trades: Dict) -> Dict:
        """
        Scan all active positions for corporate actions and news.
        
        Args:
            active_trades: Dict of active trades from engine.active_trades
            
        Returns:
            Summary with alerts and recommendations
        """
        if not active_trades:
            print("💤 No active positions to monitor")
            return {"alerts": [], "scanned": 0}
        
        print(f"\n🔍 --- CORPORATE ACTIONS SCAN ---")
        print(f"   Positions: {len(active_trades)}")
        print(f"   Time: {datetime.now().strftime('%Y-%m-%d %H:%M')}")
        
        # Extract unique symbols from positions
        symbols = set()
        for pair_key, trade in active_trades.items():
            s1, s2 = pair_key.split('-')
            symbols.add(s1)
            symbols.add(s2)
        
        symbols_list = list(symbols)
        print(f"   Symbols: {', '.join(symbols_list)}\n")
        
        if not self._agent:
            print("⚠️ LLM Agent not available - skipping corporate actions scan")
            return {"alerts": [], "scanned": len(symbols_list), "error": "No agent"}
        
        # Scan for corporate actions
        print("📰 Scanning for corporate actions...")
        corp_actions = self._agent.monitor_corporate_actions(symbols_list)
        
        alerts = corp_actions.get('alerts', [])
        
        # Process and display alerts
        critical_count = 0
        warning_count = 0
        
        for alert in alerts:
            severity = alert.get('severity', 'INFO')
            symbol = alert.get('symbol', 'UNKNOWN')
            action = alert.get('action_type', 'OTHER')
            headline = alert.get('headline', '')
            recommendation = alert.get('recommendation', '')
            
            if severity == 'CRITICAL':
                critical_count += 1
                print(f"   🚨 CRITICAL [{symbol}]: {action}")
                print(f"      {headline}")
                print(f"      → {recommendation}")
                
                # Send alert
                if self._alert_manager:
                    self._alert_manager.critical(
                        f"CORP_ACTION: {symbol}", 
                        f"{action}: {headline}"
                    )
                    
            elif severity == 'WARNING':
                warning_count += 1
                print(f"   ⚠️ WARNING [{symbol}]: {action}")
                print(f"      {headline}")
                
                if self._alert_manager:
                    self._alert_manager.warn(
                        f"CORP_ACTION: {symbol}",
                        f"{action}: {headline}",
                        "CORPORATE_ACTION"
                    )
            else:
                print(f"   ℹ️ INFO [{symbol}]: {action} - {headline}")
        
        if not alerts:
            print("   ✅ No corporate actions detected")
        
        # Summary
        print(f"\n📊 Scan Complete: {len(alerts)} alerts ({critical_count} critical, {warning_count} warnings)")
        
        return {
            "alerts": alerts,
            "scanned": len(symbols_list),
            "critical": critical_count,
            "warnings": warning_count,
            "timestamp": datetime.now().isoformat()
        }
    
    def check_position_risk(self, pair_key: str, trade: Dict) -> Dict:
        """
        Check specific position for news-based risks.
        
        Returns detailed risk assessment.
        """
        if not self._agent:
            return {"error": "Agent not available"}
        
        s1, s2 = pair_key.split('-')
        side = trade.get('side', 'LONG')
        
        print(f"\n🎯 Checking {pair_key} ({side})...")
        
        # Scan both legs
        result_y = self._agent.scan_position_news(s1, side)
        result_x = self._agent.scan_position_news(s2, "SHORT" if side == "LONG" else "LONG")
        
        # Aggregate risk
        risk_y = result_y.get('risk_level', 'LOW')
        risk_x = result_x.get('risk_level', 'LOW')
        
        risk_map = {'HIGH': 3, 'MEDIUM': 2, 'LOW': 1}
        max_risk = max(risk_map.get(risk_y, 1), risk_map.get(risk_x, 1))
        overall_risk = {3: 'HIGH', 2: 'MEDIUM', 1: 'LOW'}[max_risk]
        
        # Extract recommendations
        rec_y = result_y.get('recommendation', 'HOLD')
        rec_x = result_x.get('recommendation', 'HOLD')
        
        # Critical if either says EXIT
        if 'EXIT' in rec_y or 'EXIT' in rec_x:
            overall_recommendation = 'EXIT'
            if self._alert_manager:
                self._alert_manager.critical(
                    f"NEWS_RISK: {pair_key}",
                    f"News-based EXIT signal: Y={rec_y}, X={rec_x}"
                )
        elif 'MONITOR' in rec_y or 'MONITOR' in rec_x:
            overall_recommendation = 'MONITOR'
        else:
            overall_recommendation = 'HOLD'
        
        return {
            "pair": pair_key,
            "overall_risk": overall_risk,
            "recommendation": overall_recommendation,
            "leg_y": {
                "symbol": s1,
                "sentiment": result_y.get('sentiment', 'NEUTRAL'),
                "risk": risk_y,
                "recommendation": rec_y,
                "news": result_y.get('key_news', [])
            },
            "leg_x": {
                "symbol": s2,
                "sentiment": result_x.get('sentiment', 'NEUTRAL'),
                "risk": risk_x,
                "recommendation": rec_x,
                "news": result_x.get('key_news', [])
            },
            "volatility_alert": result_y.get('volatility_alert', False) or result_x.get('volatility_alert', False)
        }
    
    def run_full_scan(self, active_trades: Dict) -> Dict:
        """
        Run comprehensive scan: corporate actions + position news.
        """
        results = {
            "corporate_actions": self.scan_active_positions(active_trades),
            "position_risks": {},
            "overall_status": "OK",
            "timestamp": datetime.now().isoformat()
        }
        
        # Check each position for news risks
        critical_positions = []
        
        for pair_key, trade in active_trades.items():
            risk = self.check_position_risk(pair_key, trade)
            results["position_risks"][pair_key] = risk
            
            if risk.get('recommendation') == 'EXIT':
                critical_positions.append(pair_key)
        
        if critical_positions:
            results["overall_status"] = "CRITICAL"
            results["exit_candidates"] = critical_positions
            print(f"\n🚨 CRITICAL: {len(critical_positions)} positions flagged for EXIT")
        elif results["corporate_actions"].get("critical", 0) > 0:
            results["overall_status"] = "WARNING"
        
        # Save results
        output_path = os.path.join(config.DATA_DIR, "news_scan_results.json")
        with open(output_path, 'w') as f:
            json.dump(results, f, indent=2)
        print(f"\n📁 Results saved to: {output_path}")
        
        return results


def scan_positions_for_news():
    """CLI entry point for news scanning."""
    # Load active trades
    state_file = os.path.join(config.DATA_DIR, "active_trades.json")
    
    if os.path.exists(state_file):
        with open(state_file, 'r') as f:
            active_trades = json.load(f)
    else:
        print("❌ No active trades file found")
        return
    
    monitor = CorporateActionsMonitor()
    return monitor.run_full_scan(active_trades)


if __name__ == "__main__":
    scan_positions_for_news()
