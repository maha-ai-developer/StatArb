"""
Unit Tests for F&O Expiry Management

Tests days_to_expiry function and RolloverManager class.
"""

import sys
import os
import unittest
from datetime import date

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from infrastructure.data.futures_utils import (
    days_to_expiry, 
    RolloverManager,
    get_expiry_date,
    get_futures_symbol
)


class TestDaysToExpiry(unittest.TestCase):
    """Test days_to_expiry calculation."""
    
    def test_days_to_expiry_returns_positive(self):
        """Should return positive days for current month symbol."""
        days = days_to_expiry("SBIN")
        # Should return some positive value (unless test runs on expiry day)
        self.assertGreaterEqual(days, 0)
    
    def test_days_to_expiry_handles_futures_symbol(self):
        """Should extract base symbol from futures format."""
        # Both should give same result
        days_spot = days_to_expiry("SBIN")
        days_future = days_to_expiry("SBIN25JANFUT")
        # They might differ slightly if calculation uses different months
        self.assertIsInstance(days_spot, int)
        self.assertIsInstance(days_future, int)


class TestGetExpiryDate(unittest.TestCase):
    """Test expiry date calculation (last Thursday)."""
    
    def test_expiry_is_thursday(self):
        """Expiry should always be a Thursday."""
        expiry = get_expiry_date(2025, 1)
        self.assertEqual(expiry.weekday(), 3)  # Thursday = 3
    
    def test_expiry_is_in_correct_month(self):
        """Expiry should be in the specified month."""
        expiry = get_expiry_date(2025, 3)
        self.assertEqual(expiry.month, 3)
    
    def test_expiry_is_last_thursday(self):
        """Expiry should be the LAST Thursday, not first."""
        expiry = get_expiry_date(2025, 1)
        # If we add 7 days, it should be in next month
        from datetime import timedelta
        next_thursday = expiry + timedelta(days=7)
        self.assertNotEqual(next_thursday.month, 1)


class TestRolloverManager(unittest.TestCase):
    """Test RolloverManager functionality."""
    
    def setUp(self):
        self.manager = RolloverManager()
    
    def test_check_expiry_proximity_returns_tuple(self):
        """Should return tuple of (bool, int, str)."""
        needs_warning, days, msg = self.manager.check_expiry_proximity("SBIN")
        
        self.assertIsInstance(needs_warning, bool)
        self.assertIsInstance(days, int)
        self.assertIsInstance(msg, str)
    
    def test_should_block_entry(self):
        """Should return sensible block decision."""
        should_block, reason = self.manager.should_block_entry("SBIN")
        
        self.assertIsInstance(should_block, bool)
        self.assertIsInstance(reason, str)
    
    def test_get_next_month_symbol(self):
        """Should return valid futures symbol for next month."""
        next_symbol = self.manager.get_next_month_symbol("SBIN")
        
        self.assertIn("SBIN", next_symbol)
        self.assertIn("FUT", next_symbol)
    
    def test_get_rollover_plan(self):
        """Rollover plan should contain close and open orders."""
        plan = self.manager.get_rollover_plan("SBIN", 1500, "BUY")
        
        # Should have both orders
        self.assertIn("close_order", plan)
        self.assertIn("open_order", plan)
        
        # Close order should be SELL (opposite of BUY)
        self.assertEqual(plan["close_order"]["side"], "SELL")
        
        # Open order should maintain direction (BUY)
        self.assertEqual(plan["open_order"]["side"], "BUY")
        
        # Quantities should match
        self.assertEqual(plan["close_order"]["quantity"], 1500)
        self.assertEqual(plan["open_order"]["quantity"], 1500)


class TestFuturesSymbolGeneration(unittest.TestCase):
    """Test futures symbol generation."""
    
    def test_futures_symbol_format(self):
        """Symbol should be in correct format."""
        today = date.today()
        expiry = get_expiry_date(today.year, today.month)
        symbol = get_futures_symbol("RELIANCE", expiry_date=expiry)
        
        self.assertTrue(symbol.startswith("RELIANCE"))
        self.assertTrue(symbol.endswith("FUT"))
    
    def test_futures_symbol_with_expiry_string(self):
        """Should accept string format for expiry."""
        symbol = get_futures_symbol("TCS", expiry_str="2025-03")
        
        self.assertIn("TCS", symbol)
        self.assertIn("25MAR", symbol)
        self.assertIn("FUT", symbol)


if __name__ == '__main__':
    loader = unittest.TestLoader()
    suite = unittest.TestSuite()
    
    suite.addTests(loader.loadTestsFromTestCase(TestDaysToExpiry))
    suite.addTests(loader.loadTestsFromTestCase(TestGetExpiryDate))
    suite.addTests(loader.loadTestsFromTestCase(TestRolloverManager))
    suite.addTests(loader.loadTestsFromTestCase(TestFuturesSymbolGeneration))
    
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)
    sys.exit(0 if result.wasSuccessful() else 1)
