"""
Signal Alerts Module - Checklist Phase 12.4

Provides alert notifications for:
- Entry signals (LONG_SPREAD, SHORT_SPREAD)
- Exit signals (mean reversion)
- Stop loss breaches
- Margin requirement alerts
- System errors

Supports multiple notification channels:
- Console (default)
- File logging
- (Extension: email, Telegram, etc.)
"""

import os
import sys
from datetime import datetime
from typing import Optional, Dict, Any
import json

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import infrastructure.config as config


class AlertLevel:
    """Alert severity levels."""
    INFO = "INFO"
    ENTRY = "ENTRY"
    EXIT = "EXIT"
    WARNING = "WARNING"
    STOP_LOSS = "STOP_LOSS"
    CRITICAL = "CRITICAL"


class AlertManager:
    """
    Manages trading alerts and notifications.
    
    Usage:
        alerts = AlertManager()
        alerts.entry_signal("SBIN-HDFCBANK", "LONG_SPREAD", z_score=-2.5)
        alerts.exit_signal("SBIN-HDFCBANK", z_score=-0.5)
        alerts.stop_loss("SBIN-HDFCBANK", z_score=-3.5)
    """
    
    # Alert icons
    ICONS = {
        AlertLevel.INFO: "ℹ️",
        AlertLevel.ENTRY: "🚀",
        AlertLevel.EXIT: "✅",
        AlertLevel.WARNING: "⚠️",
        AlertLevel.STOP_LOSS: "🛑",
        AlertLevel.CRITICAL: "🔴",
    }
    
    def __init__(self, log_file: Optional[str] = None, console: bool = True):
        """
        Initialize AlertManager.
        
        Args:
            log_file: Optional path to alert log file
            console: Whether to print alerts to console
        """
        self.log_file = log_file or os.path.join(config.LOG_DIR, "alerts.log")
        self.console = console
        self.alert_history = []
        
        # Ensure log directory exists
        os.makedirs(os.path.dirname(self.log_file), exist_ok=True)
    
    def _log(self, level: str, message: str, data: Optional[Dict] = None):
        """Internal logging function."""
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        icon = self.ICONS.get(level, "•")
        
        # Build alert record
        alert = {
            "timestamp": timestamp,
            "level": level,
            "message": message,
            "data": data or {}
        }
        self.alert_history.append(alert)
        
        # Console output
        if self.console:
            data_str = f" | {data}" if data else ""
            print(f"{icon} [{timestamp}] {level}: {message}{data_str}")
        
        # File logging
        try:
            with open(self.log_file, 'a') as f:
                f.write(json.dumps(alert) + "\n")
        except Exception as e:
            if self.console:
                print(f"⚠️ Failed to write alert log: {e}")
    
    # === Entry Alerts ===
    
    def entry_signal(self, pair_key: str, signal: str, z_score: float, 
                     price_y: float = 0, price_x: float = 0):
        """Alert for new entry signal."""
        direction = "LONG" if signal == "LONG_SPREAD" else "SHORT"
        self._log(
            AlertLevel.ENTRY,
            f"ENTRY {direction}: {pair_key}",
            {"z_score": z_score, "price_y": price_y, "price_x": price_x, "signal": signal}
        )
    
    # === Exit Alerts ===
    
    def exit_signal(self, pair_key: str, z_score: float, reason: str = "Mean Reversion"):
        """Alert for exit signal."""
        self._log(
            AlertLevel.EXIT,
            f"EXIT: {pair_key} ({reason})",
            {"z_score": z_score, "reason": reason}
        )
    
    # === Stop Loss Alerts ===
    
    def stop_loss(self, pair_key: str, z_score: float, threshold: float = 3.0):
        """Alert for stop loss triggered."""
        self._log(
            AlertLevel.STOP_LOSS,
            f"STOP LOSS: {pair_key} - Z={z_score:.2f} breached ±{threshold}",
            {"z_score": z_score, "threshold": threshold}
        )
    
    # === Warning Alerts ===
    
    def health_warning(self, pair_key: str, status: str, reason: str):
        """Alert for health/guardian warnings."""
        self._log(
            AlertLevel.WARNING,
            f"HEALTH {status}: {pair_key} - {reason}",
            {"status": status, "reason": reason}
        )
    
    def margin_warning(self, message: str, available: float = 0, required: float = 0):
        """Alert for margin warnings."""
        self._log(
            AlertLevel.WARNING,
            f"MARGIN: {message}",
            {"available": available, "required": required}
        )
    
    def time_exit_warning(self, pair_key: str, sessions: int, max_sessions: int):
        """Alert for time-based exit."""
        self._log(
            AlertLevel.WARNING,
            f"TIME EXIT: {pair_key} held {sessions} sessions (max: {max_sessions})",
            {"sessions": sessions, "max_sessions": max_sessions}
        )
    
    def negative_beta_warning(self, pair_key: str, beta: float):
        """Alert for negative beta pairs."""
        self._log(
            AlertLevel.WARNING,
            f"NEGATIVE BETA: {pair_key} has beta={beta:.3f}",
            {"beta": beta}
        )
    
    # === Critical Alerts ===
    
    def guardian_halt(self, pair_key: str, reason: str):
        """Alert for guardian system halt."""
        self._log(
            AlertLevel.CRITICAL,
            f"GUARDIAN HALT: {pair_key} - {reason}",
            {"reason": reason}
        )
    
    def system_error(self, error: str, context: str = ""):
        """Alert for system errors."""
        self._log(
            AlertLevel.CRITICAL,
            f"SYSTEM ERROR: {error}",
            {"context": context}
        )
    
    # === Utility Methods ===
    
    def get_recent_alerts(self, count: int = 10) -> list:
        """Get most recent alerts."""
        return self.alert_history[-count:]
    
    def get_alerts_by_level(self, level: str) -> list:
        """Get alerts filtered by level."""
        return [a for a in self.alert_history if a['level'] == level]
    
    def summary(self) -> Dict:
        """Get alert summary statistics."""
        counts = {}
        for alert in self.alert_history:
            level = alert['level']
            counts[level] = counts.get(level, 0) + 1
        return {
            "total": len(self.alert_history),
            "by_level": counts
        }


# Global alert manager instance
_alert_manager: Optional[AlertManager] = None


def get_alert_manager() -> AlertManager:
    """Get or create global AlertManager instance."""
    global _alert_manager
    if _alert_manager is None:
        _alert_manager = AlertManager()
    return _alert_manager


def send_alert(level: str, message: str, data: Optional[Dict] = None):
    """Convenience function to send an alert."""
    manager = get_alert_manager()
    manager._log(level, message, data)


if __name__ == "__main__":
    # Demo
    alerts = AlertManager()
    
    print("\n--- ALERT SYSTEM DEMO ---\n")
    
    alerts.entry_signal("SBIN-HDFCBANK", "LONG_SPREAD", z_score=-2.5, price_y=800, price_x=1750)
    alerts.health_warning("INFY-TCS", "YELLOW", "Beta drift 15%")
    alerts.exit_signal("SBIN-HDFCBANK", z_score=-0.3, reason="Mean Reversion")
    alerts.stop_loss("TATASTEEL-JSWSTEEL", z_score=-3.2, threshold=3.0)
    alerts.guardian_halt("MARUTI-M&M", "ADF P-Value > 0.30")
    
    print(f"\n📊 Alert Summary: {alerts.summary()}")
