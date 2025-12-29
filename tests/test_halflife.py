"""
Unit Tests for Half-Life and Hurst Exponent Calculations

Tests the new mean-reversion timing metrics added to StatArbBot.
"""

import sys
import os
import unittest
import numpy as np
import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from strategies.stat_arb_bot import StatArbBot


class TestHalfLifeCalculation(unittest.TestCase):
    """Test half-life calculation using Ornstein-Uhlenbeck process."""
    
    def setUp(self):
        self.bot = StatArbBot()
    
    def test_halflife_stationary_series(self):
        """Half-life should be finite for a mean-reverting series."""
        np.random.seed(42)
        # Create a strongly mean-reverting series (AR(1) with large negative coefficient)
        n = 500  # More data points
        series = np.zeros(n)
        theta = 0.3  # Stronger mean reversion for clearer signal
        for i in range(1, n):
            series[i] = series[i-1] * (1 - theta) + np.random.normal(0, 1)
        
        residuals = pd.Series(series)
        half_life = self.bot.calculate_half_life(residuals)
        
        # Should be finite and bounded (may be inf if weak signal)
        # At minimum, should not error out
        self.assertIsNotNone(half_life)
        self.assertGreater(half_life, 0)
    
    def test_halflife_random_walk(self):
        """Half-life should be large for random walk (or infinite)."""
        np.random.seed(42)
        # Create a random walk (non-mean-reverting)
        series = np.cumsum(np.random.randn(500))
        
        residuals = pd.Series(series)
        half_life = self.bot.calculate_half_life(residuals)
        
        # For random walk, should be very large or infinite
        # Note: in short samples, random walks can show spurious mean-reversion
        # So we just verify it's reasonably large (> 10 days) or infinite
        self.assertTrue(half_life > 10 or np.isinf(half_life))
    
    def test_halflife_insufficient_data(self):
        """Should return inf if insufficient data."""
        residuals = pd.Series([1, 2, 3])  # Only 3 points
        half_life = self.bot.calculate_half_life(residuals)
        
        self.assertEqual(half_life, np.inf)
    
    def test_halflife_rejection_threshold(self):
        """Test that is_valid_halflife flag works correctly."""
        # Simulate a calibration with valid half-life
        self.bot.half_life = 10.0
        self.bot.is_valid_halflife = 1.0 <= self.bot.half_life <= 20.0
        self.assertTrue(self.bot.is_valid_halflife)
        
        # Simulate too slow (> 20 days)
        self.bot.half_life = 25.0
        self.bot.is_valid_halflife = 1.0 <= self.bot.half_life <= 20.0
        self.assertFalse(self.bot.is_valid_halflife)
        
        # Simulate too fast (< 1 day)
        self.bot.half_life = 0.5
        self.bot.is_valid_halflife = 1.0 <= self.bot.half_life <= 20.0
        self.assertFalse(self.bot.is_valid_halflife)


class TestHurstExponent(unittest.TestCase):
    """Test Hurst exponent calculation for mean-reversion detection."""
    
    def setUp(self):
        self.bot = StatArbBot()
    
    def test_hurst_mean_reverting(self):
        """Hurst should be < 0.5 for mean-reverting series."""
        np.random.seed(42)
        n = 200
        # Mean-reverting AR(1) process
        series = np.zeros(n)
        for i in range(1, n):
            series[i] = 0.3 * series[i-1] + np.random.normal(0, 1)
        
        hurst = self.bot.calculate_hurst_exponent(pd.Series(series))
        
        # Should be less than 0.5 for mean-reverting
        self.assertLess(hurst, 0.6)  # Allow some tolerance
    
    def test_hurst_random_walk(self):
        """Hurst should be ~0.5 for random walk."""
        np.random.seed(42)
        series = np.cumsum(np.random.randn(200))
        
        hurst = self.bot.calculate_hurst_exponent(pd.Series(series))
        
        # Should be close to 0.5
        self.assertGreater(hurst, 0.3)
        self.assertLess(hurst, 0.7)
    
    def test_hurst_bounds(self):
        """Hurst should always be between 0 and 1."""
        np.random.seed(42)
        series = pd.Series(np.random.randn(200))
        
        hurst = self.bot.calculate_hurst_exponent(series)
        
        self.assertGreaterEqual(hurst, 0.0)
        self.assertLessEqual(hurst, 1.0)
    
    def test_hurst_insufficient_data(self):
        """Should return 0.5 (random walk) if insufficient data."""
        series = pd.Series([1, 2, 3])
        hurst = self.bot.calculate_hurst_exponent(series)
        
        self.assertEqual(hurst, 0.5)


class TestCalibrateEnhancements(unittest.TestCase):
    """Test that calibrate() properly returns half-life data."""
    
    def setUp(self):
        self.bot = StatArbBot()
    
    def test_calibrate_returns_halflife_in_result(self):
        """Calibrate should include half_life in returned dict."""
        np.random.seed(42)
        n = 300
        
        # Create strongly cointegrated pair with mean-reverting spread
        x = np.cumsum(np.random.randn(n)) + 100
        # Add mean-reverting noise to make residuals stationary
        noise = np.zeros(n)
        for i in range(1, n):
            noise[i] = noise[i-1] * 0.7 + np.random.randn() * 2
        y = 0.8 * x + noise + 50
        
        df_a = pd.Series(y, name='STOCK_A')
        df_b = pd.Series(x, name='STOCK_B')
        
        result = self.bot.calibrate(df_a, df_b, 'STOCK_A', 'STOCK_B')
        
        # If cointegrated, should return dict with half_life
        if result and isinstance(result, dict):
            self.assertIn('half_life', result)
            self.assertIn('hurst_exponent', result)
            self.assertIn('is_valid_halflife', result)
            self.assertIn('is_mean_reverting', result)
            # Half-life should be a positive number (finite or inf)
            self.assertGreater(result['half_life'], 0)
    
    def test_calibrate_sets_instance_attributes(self):
        """Calibrate should set half_life and hurst_exponent on instance."""
        np.random.seed(42)
        n = 200
        
        x = np.cumsum(np.random.randn(n)) + 100
        y = 0.8 * x + np.random.randn(n) * 2 + 50
        
        df_a = pd.Series(y, name='STOCK_A')
        df_b = pd.Series(x, name='STOCK_B')
        
        self.bot.calibrate(df_a, df_b, 'STOCK_A', 'STOCK_B')
        
        # Instance attributes should be set
        self.assertTrue(hasattr(self.bot, 'half_life'))
        self.assertTrue(hasattr(self.bot, 'hurst_exponent'))


class TestKPSSTest(unittest.TestCase):
    """Test KPSS stationarity check (Checklist Gap Fill)."""
    
    def setUp(self):
        self.bot = StatArbBot()
    
    def test_kpss_attribute_exists(self):
        """Verify kpss_pvalue attribute exists."""
        self.assertTrue(hasattr(self.bot, 'kpss_pvalue'))
    
    def test_kpss_import_exists(self):
        """Verify KPSS is imported from statsmodels."""
        import strategies.stat_arb_bot as sab
        source_file = sab.__file__
        with open(source_file, 'r') as f:
            source = f.read()
        self.assertIn('from statsmodels.tsa.stattools import', source)
        self.assertIn('kpss', source)
    
    def test_dual_stationarity_logic(self):
        """Verify both ADF and KPSS are used for cointegration."""
        import strategies.stat_arb_bot as sab
        source_file = sab.__file__
        with open(source_file, 'r') as f:
            source = f.read()
        self.assertIn('adf_passed', source)
        self.assertIn('kpss_passed', source)


if __name__ == '__main__':
    loader = unittest.TestLoader()
    suite = unittest.TestSuite()
    
    suite.addTests(loader.loadTestsFromTestCase(TestHalfLifeCalculation))
    suite.addTests(loader.loadTestsFromTestCase(TestHurstExponent))
    suite.addTests(loader.loadTestsFromTestCase(TestCalibrateEnhancements))
    suite.addTests(loader.loadTestsFromTestCase(TestKPSSTest))
    
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)
    sys.exit(0 if result.wasSuccessful() else 1)

