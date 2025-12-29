"""
Fair Value Calculator Module - Cash & Carry Arbitrage

Calculates theoretical futures fair value using:
FV = Spot × e^(r×t) - Dividends

Uses LLM (Gemini) with Google Search to source:
- Current 91-day T-bill yield (risk-free rate)
- Expected dividends for stocks
"""

import os
import sys
import math
from datetime import date, datetime
from typing import Dict, Optional, Tuple

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import infrastructure.config as config


class FairValueCalculator:
    """
    Calculates fair value of futures using Cash & Carry formula.
    
    FV = Spot × e^(r × t) - Dividends
    
    Where:
    - r = risk-free rate (91-day T-bill yield)
    - t = days to expiry / 365
    - Dividends = expected dividends during the period
    """
    
    # Default fallback rate if LLM unavailable
    DEFAULT_RISK_FREE_RATE = 0.065  # 6.5% (typical Indian T-bill)
    
    def __init__(self):
        self._agent = None  # Lazy load Gemini
        self._cached_rate = None
        self._rate_date = None
    
    def _get_agent(self):
        """Lazy load GeminiAgent."""
        if self._agent is None:
            try:
                from infrastructure.llm.client import GeminiAgent
                self._agent = GeminiAgent()
            except Exception as e:
                print(f"   ⚠️ FairValueCalculator: LLM not available ({e})")
                self._agent = False
        return self._agent
    
    def get_risk_free_rate(self, use_cache: bool = True) -> Tuple[float, str]:
        """
        Get current risk-free rate using LLM + Google Search.
        
        Searches for latest 91-day T-bill yield from RBI.
        
        Args:
            use_cache: If True, use cached rate if available and recent
            
        Returns:
            Tuple of (rate_decimal, source_description)
        """
        # Check cache (valid for 1 day)
        if use_cache and self._cached_rate and self._rate_date == date.today():
            return self._cached_rate, "Cached from earlier today"
        
        agent = self._get_agent()
        if not agent or agent is False:
            return self.DEFAULT_RISK_FREE_RATE, f"Default fallback ({self.DEFAULT_RISK_FREE_RATE*100}%)"
        
        # Use Gemini + Google Search
        try:
            schema = {
                "type": "OBJECT",
                "properties": {
                    "tbill_91_day_yield": {
                        "type": "NUMBER",
                        "description": "Current 91-day T-bill yield as percentage (e.g., 6.5 for 6.5%)"
                    },
                    "date": {
                        "type": "STRING",
                        "description": "Date of the rate (YYYY-MM-DD)"
                    },
                    "source": {
                        "type": "STRING",
                        "description": "Source of the data (e.g., RBI, CCIL)"
                    },
                    "repo_rate": {
                        "type": "NUMBER",
                        "description": "Current RBI repo rate as percentage"
                    }
                },
                "required": ["tbill_91_day_yield"]
            }
            
            prompt = """
            Find the CURRENT Indian 91-day Treasury Bill (T-bill) yield rate.
            
            Search for the latest data from:
            1. RBI (Reserve Bank of India) - Weekly T-bill auctions
            2. CCIL (Clearing Corporation of India)
            3. Money market rates
            
            Also find the current RBI repo rate for reference.
            
            Return the 91-day T-bill yield as a percentage (e.g., 6.5 means 6.5%).
            """
            
            from google.genai import types
            
            response = agent.client.models.generate_content(
                model=agent.model_id,
                contents=prompt,
                config=types.GenerateContentConfig(
                    tools=[types.Tool(google_search=types.GoogleSearch())],
                    response_mime_type="application/json",
                    response_schema=schema
                )
            )
            
            import json
            result = json.loads(response.text)
            
            rate_pct = result.get('tbill_91_day_yield', 6.5)
            rate = rate_pct / 100  # Convert percentage to decimal
            source = result.get('source', 'RBI/CCIL')
            rate_date = result.get('date', str(date.today()))
            
            # Cache the result
            self._cached_rate = rate
            self._rate_date = date.today()
            
            return rate, f"{rate_pct}% from {source} ({rate_date})"
            
        except Exception as e:
            print(f"   ⚠️ Risk-free rate lookup failed: {e}")
            return self.DEFAULT_RISK_FREE_RATE, f"Default ({self.DEFAULT_RISK_FREE_RATE*100}%) - LLM error"
    
    def calculate_fair_value(self, 
                              spot_price: float, 
                              days_to_expiry: int,
                              expected_dividend: float = 0.0,
                              risk_free_rate: float = None) -> Dict:
        """
        Calculate fair value of futures using Cash & Carry formula.
        
        FV = Spot × e^(r × t) - Dividends
        
        Args:
            spot_price: Current spot price of the underlying
            days_to_expiry: Number of days until futures expiry
            expected_dividend: Expected dividend during the period (default 0)
            risk_free_rate: Risk-free rate (if None, will fetch via LLM)
            
        Returns:
            Dict with fair_value, premium/discount, and details
        """
        # Get risk-free rate if not provided
        if risk_free_rate is None:
            rate, rate_source = self.get_risk_free_rate()
        else:
            rate = risk_free_rate
            rate_source = "Provided"
        
        # Calculate time to expiry (in years)
        t = days_to_expiry / 365.0
        
        # Cash & Carry formula: FV = Spot × e^(r×t) - Dividends
        fair_value = spot_price * math.exp(rate * t) - expected_dividend
        
        # Calculate cost of carry
        cost_of_carry = spot_price * (math.exp(rate * t) - 1)
        
        return {
            "spot_price": round(spot_price, 2),
            "fair_value": round(fair_value, 2),
            "cost_of_carry": round(cost_of_carry, 2),
            "expected_dividend": round(expected_dividend, 2),
            "days_to_expiry": days_to_expiry,
            "risk_free_rate": round(rate * 100, 2),  # As percentage
            "rate_source": rate_source
        }
    
    def detect_mispricing(self,
                          spot_price: float,
                          futures_price: float,
                          days_to_expiry: int,
                          expected_dividend: float = 0.0,
                          threshold_pct: float = 0.5) -> Dict:
        """
        Detect if futures are mispriced relative to fair value.
        
        Used for Cash & Carry arbitrage detection.
        
        Args:
            spot_price: Current spot price
            futures_price: Current futures price
            days_to_expiry: Days until expiry
            expected_dividend: Expected dividend (default 0)
            threshold_pct: Mispricing threshold percentage (default 0.5%)
            
        Returns:
            Dict with mispricing analysis
        """
        fv = self.calculate_fair_value(spot_price, days_to_expiry, expected_dividend)
        fair_value = fv['fair_value']
        
        # Calculate premium/discount
        premium = futures_price - fair_value
        premium_pct = (premium / spot_price) * 100
        
        # Detect trading opportunity
        signal = "NEUTRAL"
        if premium_pct > threshold_pct:
            signal = "OVERPRICED"  # Futures expensive, potential short
        elif premium_pct < -threshold_pct:
            signal = "UNDERPRICED"  # Futures cheap, potential long
        
        return {
            "spot_price": round(spot_price, 2),
            "futures_price": round(futures_price, 2),
            "fair_value": round(fair_value, 2),
            "premium": round(premium, 2),
            "premium_pct": round(premium_pct, 3),
            "threshold_pct": threshold_pct,
            "signal": signal,
            "risk_free_rate": fv['risk_free_rate'],
            "days_to_expiry": days_to_expiry
        }
    
    def print_fair_value_report(self, symbol: str, spot: float, futures: float, 
                                 days_to_expiry: int, dividend: float = 0.0):
        """Print formatted fair value analysis report."""
        result = self.detect_mispricing(spot, futures, days_to_expiry, dividend)
        
        print(f"\n📊 Fair Value Analysis: {symbol}")
        print("=" * 45)
        print(f"   Spot Price:      ₹{result['spot_price']:,.2f}")
        print(f"   Futures Price:   ₹{result['futures_price']:,.2f}")
        print(f"   Fair Value:      ₹{result['fair_value']:,.2f}")
        print(f"   Days to Expiry:  {result['days_to_expiry']}")
        print(f"   Risk-Free Rate:  {result['risk_free_rate']}%")
        print("-" * 45)
        print(f"   Premium:         ₹{result['premium']:,.2f} ({result['premium_pct']:.2f}%)")
        
        signal = result['signal']
        if signal == "OVERPRICED":
            print(f"   🔴 SIGNAL: Futures OVERPRICED (> {result['threshold_pct']}%)")
        elif signal == "UNDERPRICED":
            print(f"   🟢 SIGNAL: Futures UNDERPRICED (< -{result['threshold_pct']}%)")
        else:
            print(f"   ⚪ SIGNAL: Fair value within threshold")
        print("=" * 45)


def get_fair_value(spot: float, days_to_expiry: int, dividend: float = 0.0) -> Dict:
    """Convenience function to calculate fair value."""
    calc = FairValueCalculator()
    return calc.calculate_fair_value(spot, days_to_expiry, dividend)


if __name__ == "__main__":
    # Test with sample data
    calc = FairValueCalculator()
    
    # Get current risk-free rate
    rate, source = calc.get_risk_free_rate()
    print(f"\n🏦 Risk-Free Rate: {rate*100:.2f}% ({source})")
    
    # Sample fair value calculation
    calc.print_fair_value_report(
        symbol="SBIN",
        spot=850.0,
        futures=855.0,
        days_to_expiry=15,
        dividend=0.0
    )
