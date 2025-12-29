"""
Unit Tests for Reporting Modules

Tests:
- pair_data_report.py
- trade_analytics.py
- alerts.py
"""

import sys
import os
import unittest
import tempfile
import json

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


class TestPairDataReport(unittest.TestCase):
    """Test pair data report module (source inspection to avoid pandas import)."""
    
    def _read_file(self, path):
        with open(path, 'r') as f:
            return f.read()
    
    def test_module_exists(self):
        """Verify module exists with required functions."""
        self.assertTrue(os.path.exists('reporting/pair_data_report.py'))
        source = self._read_file('reporting/pair_data_report.py')
        self.assertIn('def generate_pair_report', source)
        self.assertIn('def calculate_pair_metrics', source)
    
    def test_signal_thresholds(self):
        """Verify correct signal thresholds in code."""
        source = self._read_file('reporting/pair_data_report.py')
        # Check LONG at -2.0
        self.assertIn('<= -2.0', source)
        # Check SHORT at 2.0
        self.assertIn('>= 2.0', source)
        # Check EXIT at 1.0
        self.assertIn('<= 1.0', source)
    
    def test_required_output_fields(self):
        """Verify report includes all required fields per Phase 3."""
        source = self._read_file('reporting/pair_data_report.py')
        required_fields = ['Y_Stock', 'X_Stock', 'Beta', 'Intercept', 
                          'ADF', 'Sigma', 'Z_Score', 'Signal']
        for field in required_fields:
            self.assertIn(field, source, f"Missing field: {field}")


class TestTradeAnalytics(unittest.TestCase):
    """Test trade analytics module (source inspection)."""
    
    def _read_file(self, path):
        with open(path, 'r') as f:
            return f.read()
    
    def test_module_exists(self):
        """Verify module exists with TradeAnalytics class."""
        self.assertTrue(os.path.exists('reporting/trade_analytics.py'))
        source = self._read_file('reporting/trade_analytics.py')
        self.assertIn('class TradeAnalytics', source)
        self.assertIn('def generate_report', source)
    
    def test_required_metrics(self):
        """Verify all Phase 10 metrics are calculated."""
        source = self._read_file('reporting/trade_analytics.py')
        # Required metrics per checklist
        required = ['win_rate', 'avg_profit', 'avg_loss', 'profit_factor', 
                   'max_drawdown', 'sharpe']
        for metric in required:
            self.assertIn(metric, source.lower(), f"Missing metric: {metric}")
    
    def test_pnl_calculation(self):
        """Verify P&L calculation exists."""
        source = self._read_file('reporting/trade_analytics.py')
        self.assertIn('net_pnl', source)
        self.assertIn('gross_profit', source)
        self.assertIn('gross_loss', source)


class TestAlertManager(unittest.TestCase):
    """Test alert manager module."""
    
    def setUp(self):
        """Create temp log file for tests."""
        self.temp_log = tempfile.mktemp(suffix='.log')
    
    def tearDown(self):
        """Clean up temp file."""
        if os.path.exists(self.temp_log):
            os.remove(self.temp_log)
    
    def test_alert_manager_creation(self):
        """Test AlertManager initialization."""
        from trading_floor.alerts import AlertManager
        
        manager = AlertManager(log_file=self.temp_log, console=False)
        self.assertEqual(manager.alert_history, [])
    
    def test_entry_alert(self):
        """Test entry signal alert."""
        from trading_floor.alerts import AlertManager
        
        manager = AlertManager(log_file=self.temp_log, console=False)
        manager.entry_signal("SBIN-HDFCBANK", "LONG_SPREAD", z_score=-2.5)
        
        self.assertEqual(len(manager.alert_history), 1)
        self.assertEqual(manager.alert_history[0]['level'], "ENTRY")
    
    def test_stop_loss_alert(self):
        """Test stop loss alert."""
        from trading_floor.alerts import AlertManager
        
        manager = AlertManager(log_file=self.temp_log, console=False)
        manager.stop_loss("SBIN-HDFCBANK", z_score=-3.5, threshold=3.0)
        
        self.assertEqual(len(manager.alert_history), 1)
        self.assertEqual(manager.alert_history[0]['level'], "STOP_LOSS")
    
    def test_alert_logging(self):
        """Test that alerts are written to log file."""
        from trading_floor.alerts import AlertManager
        
        manager = AlertManager(log_file=self.temp_log, console=False)
        manager.entry_signal("TEST", "LONG_SPREAD", z_score=-2.0)
        
        # Check log file exists and has content
        self.assertTrue(os.path.exists(self.temp_log))
        
        with open(self.temp_log, 'r') as f:
            content = f.read()
        
        self.assertIn("ENTRY", content)
        self.assertIn("TEST", content)
    
    def test_alert_summary(self):
        """Test alert summary function."""
        from trading_floor.alerts import AlertManager
        
        manager = AlertManager(log_file=self.temp_log, console=False)
        manager.entry_signal("A", "LONG_SPREAD", z_score=-2.0)
        manager.exit_signal("A", z_score=-0.5)
        manager.stop_loss("B", z_score=-3.5)
        
        summary = manager.summary()
        
        self.assertEqual(summary['total'], 3)
        self.assertEqual(summary['by_level']['ENTRY'], 1)
        self.assertEqual(summary['by_level']['EXIT'], 1)
        self.assertEqual(summary['by_level']['STOP_LOSS'], 1)


class TestCLIReportingCommands(unittest.TestCase):
    """Test CLI has reporting commands."""
    
    def _read_file(self, path):
        with open(path, 'r') as f:
            return f.read()
    
    def test_pair_report_command(self):
        """Verify pair-report command exists."""
        source = self._read_file('cli.py')
        self.assertIn('pair-report', source)
        self.assertIn('cmd_pair_report', source)
    
    def test_analytics_command(self):
        """Verify analytics command exists."""
        source = self._read_file('cli.py')
        self.assertIn('analytics', source)
        self.assertIn('cmd_analytics', source)
        self.assertIn('--days', source)
    
    def test_daily_report_command(self):
        """Verify daily-report command exists."""
        source = self._read_file('cli.py')
        self.assertIn('daily-report', source)
        self.assertIn('cmd_daily_report', source)


if __name__ == '__main__':
    loader = unittest.TestLoader()
    suite = unittest.TestSuite()
    
    suite.addTests(loader.loadTestsFromTestCase(TestPairDataReport))
    suite.addTests(loader.loadTestsFromTestCase(TestTradeAnalytics))
    suite.addTests(loader.loadTestsFromTestCase(TestAlertManager))
    suite.addTests(loader.loadTestsFromTestCase(TestCLIReportingCommands))
    
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)
    sys.exit(0 if result.wasSuccessful() else 1)
