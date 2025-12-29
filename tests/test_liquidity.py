"""
Unit Tests for Liquidity Checker Module

Tests the bid-ask spread and market depth validation.
"""

import sys
import os
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from trading_floor.risk.liquidity import LiquidityChecker


class TestSpreadValidation(unittest.TestCase):
    """Test bid-ask spread calculations and thresholds."""
    
    def setUp(self):
        self.checker = LiquidityChecker()
    
    def test_calculate_spread(self):
        """Spread calculation should give correct values."""
        bid = 100.0
        ask = 100.20
        
        spread_abs, spread_pct = self.checker.calculate_spread(bid, ask)
        
        self.assertAlmostEqual(spread_abs, 0.20, places=2)
        self.assertAlmostEqual(spread_pct, 0.002, places=4)  # 0.2%
    
    def test_check_spread_acceptable(self):
        """Spread within threshold should pass."""
        # 0.1% spread - OK
        ok, msg = self.checker.check_spread("TEST", bid=100.0, ask=100.10)
        self.assertTrue(ok)
        self.assertIn("OK", msg)
    
    def test_check_spread_too_wide(self):
        """Spread above threshold should fail."""
        # 0.5% spread - Too wide
        ok, msg = self.checker.check_spread("TEST", bid=100.0, ask=100.50)
        self.assertFalse(ok)
        self.assertIn("threshold", msg)
    
    def test_spread_edge_case_zero_prices(self):
        """Zero prices should be handled gracefully."""
        spread_abs, spread_pct = self.checker.calculate_spread(0, 0)
        self.assertEqual(spread_abs, 0.0)
        self.assertEqual(spread_pct, 0.0)


class TestDepthValidation(unittest.TestCase):
    """Test market depth validation."""
    
    def setUp(self):
        self.checker = LiquidityChecker()
    
    def test_check_depth_sufficient(self):
        """Sufficient depth should pass."""
        # Need 1000, have 3000 total -> OK (2x requirement met)
        ok, msg = self.checker.check_depth("TEST", required_qty=1000, 
                                           bid_qty=1500, ask_qty=1500)
        self.assertTrue(ok)
        self.assertIn("OK", msg)
    
    def test_check_depth_insufficient(self):
        """Insufficient depth should fail."""
        # Need 1000, have 1500 total -> FAIL (need 2000)
        ok, msg = self.checker.check_depth("TEST", required_qty=1000, 
                                           bid_qty=500, ask_qty=1000)
        self.assertFalse(ok)
        self.assertIn("required", msg)


class TestPairEntryValidation(unittest.TestCase):
    """Test complete pair entry validation."""
    
    def setUp(self):
        self.checker = LiquidityChecker()
    
    def test_validate_entry_success(self):
        """Valid entry should pass all checks."""
        ok, msg, details = self.checker.validate_entry(
            sym_y="SBIN", bid_y=800.0, ask_y=800.80, qty_y=1500,
            sym_x="ICICI", bid_x=1200.0, ask_x=1201.0, qty_x=1400
        )
        self.assertTrue(ok)
        self.assertTrue(details['passed'])
        self.assertIn("Liquidity OK", msg)
    
    def test_validate_entry_fail_wide_spread(self):
        """Entry with wide spread should fail."""
        ok, msg, details = self.checker.validate_entry(
            sym_y="SBIN", bid_y=800.0, ask_y=805.0, qty_y=1500,  # 0.6% spread
            sym_x="ICICI", bid_x=1200.0, ask_x=1201.0, qty_x=1400
        )
        self.assertFalse(ok)
        self.assertFalse(details['passed'])
        self.assertIn("Wide spread", msg)
    
    def test_validate_entry_with_depth(self):
        """Entry with depth check should validate both."""
        ok, msg, details = self.checker.validate_entry(
            sym_y="SBIN", bid_y=800.0, ask_y=800.80, qty_y=1500,
            sym_x="ICICI", bid_x=1200.0, ask_x=1201.0, qty_x=1400,
            depth_y=(2000, 2000),  # 4000 total > 3000 required
            depth_x=(2000, 2000)
        )
        self.assertTrue(ok)
    
    def test_validate_entry_fail_low_depth(self):
        """Entry with insufficient depth should fail."""
        ok, msg, details = self.checker.validate_entry(
            sym_y="SBIN", bid_y=800.0, ask_y=800.80, qty_y=1500,
            sym_x="ICICI", bid_x=1200.0, ask_x=1201.0, qty_x=1400,
            depth_y=(500, 500)  # 1000 total < 3000 required
        )
        self.assertFalse(ok)
        self.assertFalse(details['y_depth_ok'])


class TestCustomThresholds(unittest.TestCase):
    """Test custom threshold configuration."""
    
    def test_custom_spread_threshold(self):
        """Custom spread threshold should be honored."""
        # Strict checker - 0.1% threshold
        strict = LiquidityChecker(max_spread_pct=0.001)
        
        # 0.15% spread - would pass default, should fail strict
        ok, _ = strict.check_spread("TEST", bid=100.0, ask=100.15)
        self.assertFalse(ok)
        
        # Lenient checker - 0.5% threshold
        lenient = LiquidityChecker(max_spread_pct=0.005)
        
        # 0.3% spread - would fail default, should pass lenient
        ok, _ = lenient.check_spread("TEST", bid=100.0, ask=100.30)
        self.assertTrue(ok)


if __name__ == '__main__':
    loader = unittest.TestLoader()
    suite = unittest.TestSuite()
    
    suite.addTests(loader.loadTestsFromTestCase(TestSpreadValidation))
    suite.addTests(loader.loadTestsFromTestCase(TestDepthValidation))
    suite.addTests(loader.loadTestsFromTestCase(TestPairEntryValidation))
    suite.addTests(loader.loadTestsFromTestCase(TestCustomThresholds))
    
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)
    sys.exit(0 if result.wasSuccessful() else 1)
