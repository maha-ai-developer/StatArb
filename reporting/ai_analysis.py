"""
AI Post-Trade Analysis Module - Checklist Phase 10

Integrates with Google Gemini LLM to provide:
- Structured trade analysis reports
- Performance insights and recommendations
- Strategy refinement suggestions

Built on existing trade_analytics.py foundation.
"""

import os
import sys
import json
from datetime import datetime
from typing import Dict, List, Optional, Any

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import infrastructure.config as config
from reporting.trade_analytics import TradeAnalytics


# ============================================================
# TRADE ANALYSIS SCHEMA (Phase 10.1)
# ============================================================

TRADE_ANALYSIS_SCHEMA = {
    "report_metadata": {
        "generated_at": "ISO timestamp",
        "period_days": "int",
        "total_trades": "int",
        "data_source": "string (PAPER/LIVE)"
    },
    "trade_summary": {
        "entries": [
            {
                "pair": "string (e.g., SBIN-HDFCBANK)",
                "sector": "string",
                "entry_date": "date",
                "entry_time": "time",
                "entry_zscore": "float",
                "exit_date": "date",
                "exit_time": "time",
                "exit_zscore": "float",
                "days_held": "int",
                "entry_price_y": "float",
                "entry_price_x": "float",
                "exit_price_y": "float",
                "exit_price_x": "float",
                "pnl_y": "float",
                "pnl_x": "float",
                "net_pnl": "float",
                "transaction_costs": "float",
                "exit_reason": "string (TAKE_PROFIT/STOP_LOSS/TIME_EXIT/GUARDIAN_HALT)",
                "beta": "float",
                "intercept": "float"
            }
        ]
    },
    "performance_metrics": {
        "total_trades": "int",
        "winning_trades": "int",
        "losing_trades": "int",
        "win_rate": "float (%)",
        "avg_profit": "float",
        "avg_loss": "float",
        "profit_factor": "float",
        "gross_profit": "float",
        "gross_loss": "float",
        "net_pnl": "float",
        "sharpe_ratio": "float",
        "max_drawdown": "float",
        "avg_holding_days": "float"
    },
    "sector_breakdown": [
        {
            "sector": "string",
            "trades": "int",
            "win_rate": "float",
            "net_pnl": "float",
            "best_pair": "string",
            "worst_pair": "string"
        }
    ],
    "ai_insights": {
        "summary": "string (2-3 sentence overview)",
        "strengths": ["list of what worked well"],
        "weaknesses": ["list of areas to improve"],
        "recommendations": [
            {
                "category": "string (ENTRY/EXIT/SIZING/PAIRS)",
                "suggestion": "string",
                "expected_impact": "string"
            }
        ],
        "risk_alerts": ["any concerning patterns"],
        "confidence_score": "float (0-1)"
    }
}


class AITradeAnalyzer:
    """
    AI-powered trade analysis using Google Gemini LLM.
    
    Uses infrastructure/config.py for API key and model settings.
    Generates structured reports with actionable insights.
    """
    
    def __init__(self):
        """
        Initialize with Gemini API key from config.
        
        Uses:
        - config.GENAI_API_KEY (from config.json)
        - config.GENAI_MODEL (from config.py)
        """
        self.analytics = TradeAnalytics()
        self._client = None
        self._model_id = None
        
        if config.GENAI_API_KEY:
            self._init_gemini()
        else:
            print("⚠️ GENAI_API_KEY not found in config.json - using fallback mode")
    
    def _init_gemini(self):
        """Initialize Gemini client using google.genai API."""
        try:
            from google import genai
            from google.genai import types
            
            self._client = genai.Client(api_key=config.GENAI_API_KEY)
            self._model_id = getattr(config, "GENAI_MODEL", "gemini-2.5-flash")
            self._types = types
            print(f"✅ Gemini LLM initialized: {self._model_id}")
        except ImportError:
            print("⚠️ google-genai not installed. Run: pip install google-genai")
            self._client = None
        except Exception as e:
            print(f"⚠️ Gemini init failed: {e}")
            self._client = None
    
    def build_analysis_data(self, days_back: int = 30) -> Dict:
        """
        Build structured analysis data per schema.
        """
        # Load base metrics
        df = self.analytics.get_trades(days_back)
        trades = self.analytics.calculate_pair_pnl(df)
        metrics = self.analytics.calculate_metrics(trades)
        
        # Build schema-compliant data
        analysis = {
            "report_metadata": {
                "generated_at": datetime.now().isoformat(),
                "period_days": days_back,
                "total_trades": metrics.get('total_trades', 0),
                "data_source": "PAPER"  # TODO: Detect from DB
            },
            "trade_summary": {
                "entries": trades
            },
            "performance_metrics": metrics,
            "sector_breakdown": self._calculate_sector_breakdown(trades),
            "ai_insights": None  # Populated by Gemini
        }
        
        return analysis
    
    def _calculate_sector_breakdown(self, trades: List[Dict]) -> List[Dict]:
        """Calculate performance breakdown by sector."""
        if not trades:
            return []
        
        # Group by sector (extracted from symbol pairs)
        sector_data = {}
        
        for trade in trades:
            # Try to get sector, otherwise use symbol prefix
            sector = trade.get('sector', trade.get('symbol', 'UNKNOWN')[:4])
            
            if sector not in sector_data:
                sector_data[sector] = {
                    'trades': 0,
                    'wins': 0,
                    'pnl': 0,
                    'best_pnl': float('-inf'),
                    'worst_pnl': float('inf'),
                    'best_pair': '',
                    'worst_pair': ''
                }
            
            s = sector_data[sector]
            s['trades'] += 1
            s['pnl'] += trade.get('pnl', 0)
            
            if trade.get('pnl', 0) > 0:
                s['wins'] += 1
            
            if trade.get('pnl', 0) > s['best_pnl']:
                s['best_pnl'] = trade.get('pnl', 0)
                s['best_pair'] = trade.get('symbol', '')
            
            if trade.get('pnl', 0) < s['worst_pnl']:
                s['worst_pnl'] = trade.get('pnl', 0)
                s['worst_pair'] = trade.get('symbol', '')
        
        # Format output
        breakdown = []
        for sector, data in sector_data.items():
            breakdown.append({
                'sector': sector,
                'trades': data['trades'],
                'win_rate': round(data['wins'] / data['trades'] * 100, 1) if data['trades'] > 0 else 0,
                'net_pnl': round(data['pnl'], 2),
                'best_pair': data['best_pair'],
                'worst_pair': data['worst_pair']
            })
        
        return sorted(breakdown, key=lambda x: x['net_pnl'], reverse=True)
    
    def generate_ai_insights(self, analysis_data: Dict) -> Dict:
        """
        Use Gemini LLM to generate insights and recommendations.
        Uses google.genai API with config-based model.
        """
        if not self._client:
            return self._generate_fallback_insights(analysis_data)
        
        metrics = analysis_data.get('performance_metrics', {})
        sectors = analysis_data.get('sector_breakdown', [])
        
        prompt = f"""
        Analyze this pairs trading strategy performance and provide actionable insights.
        
        PERFORMANCE METRICS:
        - Total Trades: {metrics.get('total_trades', 0)}
        - Win Rate: {metrics.get('win_rate', 0)}%
        - Profit Factor: {metrics.get('profit_factor', 0)}
        - Net P&L: ₹{metrics.get('net_pnl', 0):,.2f}
        - Sharpe Ratio: {metrics.get('sharpe_ratio', 0)}
        - Max Drawdown: ₹{metrics.get('max_drawdown', 0):,.2f}
        - Avg Profit: ₹{metrics.get('avg_profit', 0):,.2f}
        - Avg Loss: ₹{metrics.get('avg_loss', 0):,.2f}
        - Avg Holding: {metrics.get('avg_holding_days', 0)} days
        
        SECTOR BREAKDOWN:
        {json.dumps(sectors, indent=2)}
        
        Respond ONLY with valid JSON in this exact structure:
        {{
            "summary": "2-3 sentence performance overview",
            "strengths": ["strength1", "strength2"],
            "weaknesses": ["weakness1", "weakness2"],
            "recommendations": [
                {{"category": "ENTRY", "suggestion": "...", "expected_impact": "..."}},
                {{"category": "PAIRS", "suggestion": "...", "expected_impact": "..."}}
            ],
            "risk_alerts": ["any concerning patterns"],
            "confidence_score": 0.75
        }}
        """
        
        try:
            # Use google.genai API pattern
            response = self._client.models.generate_content(
                model=self._model_id,
                contents=prompt,
                config=self._types.GenerateContentConfig(
                    response_mime_type="application/json"
                )
            )
            
            text = response.text.strip()
            insights = json.loads(text)
            return insights
            
        except Exception as e:
            print(f"⚠️ Gemini analysis failed: {e}")
            return self._generate_fallback_insights(analysis_data)
    
    def _generate_fallback_insights(self, analysis_data: Dict) -> Dict:
        """Generate rule-based insights when LLM unavailable."""
        metrics = analysis_data.get('performance_metrics', {})
        
        win_rate = metrics.get('win_rate', 0)
        profit_factor = metrics.get('profit_factor', 0)
        net_pnl = metrics.get('net_pnl', 0)
        
        # Determine strengths and weaknesses
        strengths = []
        weaknesses = []
        recommendations = []
        risk_alerts = []
        
        if win_rate > 55:
            strengths.append(f"Strong win rate at {win_rate}%")
        elif win_rate < 45:
            weaknesses.append(f"Low win rate at {win_rate}%")
            recommendations.append({
                "category": "ENTRY",
                "suggestion": "Consider tightening entry Z-score to ±2.5 for higher probability setups",
                "expected_impact": "Higher win rate, fewer but better trades"
            })
        
        if profit_factor > 1.5:
            strengths.append(f"Excellent profit factor of {profit_factor}")
        elif profit_factor < 1.0:
            weaknesses.append(f"Profit factor below 1.0 indicates losing strategy")
            risk_alerts.append("Strategy is net negative - pause live trading")
        
        avg_loss = metrics.get('avg_loss', 0)
        avg_profit = metrics.get('avg_profit', 0)
        if avg_loss > avg_profit * 1.5:
            weaknesses.append("Average losses significantly exceed average profits")
            recommendations.append({
                "category": "EXIT",
                "suggestion": "Tighten stop loss from Z=3.0 to Z=2.5 to reduce average loss",
                "expected_impact": "Smaller losses, improved risk-reward"
            })
        
        max_dd = metrics.get('max_drawdown', 0)
        if max_dd > 50000:
            risk_alerts.append(f"Max drawdown of ₹{max_dd:,.0f} is concerning")
        
        # Summary
        if profit_factor > 1.5 and win_rate > 50:
            summary = f"Strong performance with {win_rate}% win rate and {profit_factor}x profit factor. Net P&L of ₹{net_pnl:,.0f} demonstrates viable strategy."
            confidence = 0.85
        elif profit_factor > 1.0:
            summary = f"Profitable but modest performance with {win_rate}% win rate. Refinements recommended to improve consistency."
            confidence = 0.65
        else:
            summary = f"Strategy underperforming with profit factor of {profit_factor}. Significant adjustments needed before live trading."
            confidence = 0.35
        
        return {
            "summary": summary,
            "strengths": strengths or ["Consistent trade execution"],
            "weaknesses": weaknesses or ["No major issues identified"],
            "recommendations": recommendations or [{
                "category": "PAIRS",
                "suggestion": "Continue monitoring current pair selection",
                "expected_impact": "Maintain current performance"
            }],
            "risk_alerts": risk_alerts or [],
            "confidence_score": confidence
        }
    
    def generate_report(self, days_back: int = 30) -> Dict:
        """
        Generate complete AI-powered analysis report.
        """
        print(f"\n🤖 --- AI POST-TRADE ANALYSIS ---")
        print(f"   Period: Last {days_back} days")
        print(f"   Date: {datetime.now().strftime('%Y-%m-%d %H:%M')}")
        print(f"   Model: {self._model_id or 'Fallback'} {'✅' if self._client else '❌'}\n")
        
        # Build analysis data
        analysis = self.build_analysis_data(days_back)
        
        if analysis['performance_metrics'].get('total_trades', 0) == 0:
            print("💤 No trades found in the specified period.")
            return analysis
        
        # Generate AI insights
        print("🧠 Generating AI Insights...")
        analysis['ai_insights'] = self.generate_ai_insights(analysis)
        
        # Display results
        self._display_report(analysis)
        
        # Save to file
        output_path = os.path.join(config.ARTIFACTS_DIR, "ai_trade_analysis.json")
        with open(output_path, 'w') as f:
            json.dump(analysis, f, indent=2, default=str)
        print(f"\n📁 Full report saved to: {output_path}")
        
        return analysis
    
    def _display_report(self, analysis: Dict):
        """Display formatted report."""
        metrics = analysis.get('performance_metrics', {})
        insights = analysis.get('ai_insights', {})
        
        print("\n" + "=" * 60)
        print("📊 PERFORMANCE SUMMARY")
        print("=" * 60)
        print(f"   Trades: {metrics.get('total_trades', 0)} | Win Rate: {metrics.get('win_rate', 0)}%")
        print(f"   Profit Factor: {metrics.get('profit_factor', 0):.2f} | Sharpe: {metrics.get('sharpe_ratio', 0):.2f}")
        print(f"   Net P&L: ₹{metrics.get('net_pnl', 0):,.2f} | Max DD: ₹{metrics.get('max_drawdown', 0):,.2f}")
        
        print("\n" + "=" * 60)
        print("🤖 AI INSIGHTS")
        print("=" * 60)
        print(f"\n📝 Summary: {insights.get('summary', 'N/A')}")
        
        print("\n✅ Strengths:")
        for s in insights.get('strengths', []):
            print(f"   • {s}")
        
        print("\n⚠️ Weaknesses:")
        for w in insights.get('weaknesses', []):
            print(f"   • {w}")
        
        print("\n💡 Recommendations:")
        for r in insights.get('recommendations', []):
            print(f"   [{r.get('category', 'GENERAL')}] {r.get('suggestion', '')}")
            print(f"      → Expected: {r.get('expected_impact', 'N/A')}")
        
        alerts = insights.get('risk_alerts', [])
        if alerts:
            print("\n🚨 Risk Alerts:")
            for a in alerts:
                print(f"   ⚠️ {a}")
        
        confidence = insights.get('confidence_score', 0)
        print(f"\n📈 Analysis Confidence: {confidence * 100:.0f}%")
        print("=" * 60)
    
    def evaluate_production_readiness(self, backtest_results: Dict) -> Dict:
        """
        Use Gemini + Google Search to evaluate if backtest is ready for live trading.
        
        Researches:
        - Industry benchmarks for Sharpe ratio, win rate, max drawdown
        - Best practices for statistical arbitrage live trading
        - Common gaps between backtest and live trading
        
        Args:
            backtest_results: Dict with metrics (return_pct, win_rate, sharpe_ratio, etc.)
            
        Returns:
            Dict with readiness assessment and recommendations
        """
        if not self._client:
            return self._fallback_readiness_check(backtest_results)
        
        try:
            from google.genai import types
            
            # Build metrics summary
            metrics_str = json.dumps(backtest_results, indent=2, default=str)
            
            schema = {
                "type": "OBJECT",
                "properties": {
                    "overall_readiness": {
                        "type": "STRING",
                        "enum": ["READY", "NEEDS_WORK", "NOT_READY"]
                    },
                    "confidence_score": {
                        "type": "NUMBER",
                        "description": "0-100 score for production readiness"
                    },
                    "summary": {
                        "type": "STRING",
                        "description": "2-3 sentence overall assessment"
                    },
                    "benchmark_comparison": {
                        "type": "OBJECT",
                        "properties": {
                            "sharpe_ratio_benchmark": {"type": "NUMBER"},
                            "win_rate_benchmark": {"type": "NUMBER"},
                            "max_drawdown_benchmark": {"type": "NUMBER"},
                            "user_meets_benchmarks": {"type": "BOOLEAN"}
                        }
                    },
                    "gaps_for_live_trading": {
                        "type": "ARRAY",
                        "items": {"type": "STRING"},
                        "description": "Missing features needed for production"
                    },
                    "recommended_features": {
                        "type": "ARRAY",
                        "items": {
                            "type": "OBJECT",
                            "properties": {
                                "feature": {"type": "STRING"},
                                "priority": {"type": "STRING", "enum": ["HIGH", "MEDIUM", "LOW"]},
                                "reason": {"type": "STRING"}
                            }
                        }
                    },
                    "risk_warnings": {
                        "type": "ARRAY",
                        "items": {"type": "STRING"}
                    },
                    "research_sources": {
                        "type": "ARRAY",
                        "items": {"type": "STRING"},
                        "description": "Sources used for benchmarks"
                    }
                },
                "required": ["overall_readiness", "confidence_score", "summary", "gaps_for_live_trading"]
            }
            
            prompt = f"""
            You are an expert quantitative trading analyst. Evaluate this BACKTEST result for LIVE TRADING readiness.
            
            BACKTEST RESULTS:
            {metrics_str}
            
            RESEARCH TASK:
            1. Search for current industry benchmarks for:
               - Statistical arbitrage / pairs trading Sharpe ratio thresholds
               - Acceptable win rates for mean-reversion strategies
               - Maximum drawdown limits for retail algo trading
               
            2. Compare the user's metrics against these benchmarks
            
            3. Identify GAPS between their backtest and production requirements:
               - Slippage modeling (do they account for 0.05%+ slippage?)
               - Transaction costs (brokerage, STT, GST for India)
               - Out-of-sample testing (train/test split)
               - Risk metrics (Sharpe, max drawdown, profit factor)
               - Rollover handling for futures
               - Circuit breakers for regime changes
               
            4. Recommend specific features they need before going live
            
            5. Give honest risk warnings based on their metrics
            
            Be specific and cite actual benchmarks from your research.
            """
            
            response = self._client.models.generate_content(
                model=self._model_id,
                contents=prompt,
                config=types.GenerateContentConfig(
                    tools=[types.Tool(google_search=types.GoogleSearch())],
                    response_mime_type="application/json",
                    response_schema=schema
                )
            )
            
            result = json.loads(response.text)
            result['analysis_timestamp'] = datetime.now().isoformat()
            result['backtest_metrics'] = backtest_results
            
            return result
            
        except Exception as e:
            print(f"⚠️ Production readiness evaluation failed: {e}")
            return self._fallback_readiness_check(backtest_results)
    
    def _fallback_readiness_check(self, metrics: Dict) -> Dict:
        """Rule-based readiness check when LLM unavailable."""
        sharpe = metrics.get('sharpe_ratio', 0)
        win_rate = metrics.get('win_rate', 0)
        profit_factor = metrics.get('profit_factor', 0)
        max_dd_pct = metrics.get('max_drawdown_pct', 0)
        
        gaps = []
        warnings = []
        
        # Check benchmarks
        if sharpe < 1.0:
            gaps.append("Sharpe ratio below 1.0 - industry minimum is 1.0-1.5")
        if win_rate < 50:
            gaps.append("Win rate below 50% - pairs trading typically needs 55%+")
        if profit_factor < 1.5:
            gaps.append("Profit factor below 1.5 - recommended minimum is 1.5-2.0")
        if max_dd_pct > 20:
            warnings.append(f"Max drawdown of {max_dd_pct}% exceeds 20% threshold")
        
        # Determine readiness
        if sharpe >= 1.5 and win_rate >= 55 and profit_factor >= 1.5:
            readiness = "READY"
            score = 85
        elif sharpe >= 1.0 and win_rate >= 50 and profit_factor >= 1.2:
            readiness = "NEEDS_WORK"
            score = 60
        else:
            readiness = "NOT_READY"
            score = 30
        
        return {
            "overall_readiness": readiness,
            "confidence_score": score,
            "summary": f"Based on metrics: Sharpe {sharpe:.2f}, Win Rate {win_rate:.1f}%, Profit Factor {profit_factor:.2f}",
            "benchmark_comparison": {
                "sharpe_ratio_benchmark": 1.5,
                "win_rate_benchmark": 55.0,
                "max_drawdown_benchmark": 15.0,
                "user_meets_benchmarks": readiness == "READY"
            },
            "gaps_for_live_trading": gaps or ["No major gaps identified"],
            "risk_warnings": warnings,
            "research_sources": ["Fallback rules - LLM unavailable"]
        }
    
    def print_readiness_report(self, backtest_results: Dict):
        """Print formatted production readiness report."""
        print("\n" + "=" * 60)
        print("🚀 PRODUCTION READINESS EVALUATION")
        print("   Using Gemini + Google Search for benchmarks")
        print("=" * 60)
        
        result = self.evaluate_production_readiness(backtest_results)
        
        readiness = result.get('overall_readiness', 'UNKNOWN')
        score = result.get('confidence_score', 0)
        
        icon = "✅" if readiness == "READY" else "🟡" if readiness == "NEEDS_WORK" else "❌"
        print(f"\n{icon} VERDICT: {readiness} (Score: {score}/100)")
        print(f"\n📝 {result.get('summary', 'N/A')}")
        
        # Benchmarks
        bench = result.get('benchmark_comparison', {})
        if bench:
            print("\n📊 BENCHMARK COMPARISON:")
            print(f"   Industry Sharpe:     >= {bench.get('sharpe_ratio_benchmark', 1.5)}")
            print(f"   Industry Win Rate:   >= {bench.get('win_rate_benchmark', 55)}%")
            print(f"   Max Drawdown Limit:  <= {bench.get('max_drawdown_benchmark', 15)}%")
        
        # Gaps
        gaps = result.get('gaps_for_live_trading', [])
        if gaps:
            print("\n🔧 GAPS FOR LIVE TRADING:")
            for g in gaps:
                print(f"   • {g}")
        
        # Recommendations
        features = result.get('recommended_features', [])
        if features:
            print("\n💡 RECOMMENDED FEATURES:")
            for f in features:
                priority = f.get('priority', 'MEDIUM')
                picon = "🔴" if priority == "HIGH" else "🟡" if priority == "MEDIUM" else "🟢"
                print(f"   {picon} [{priority}] {f.get('feature', '')}")
                print(f"       → {f.get('reason', '')}")
        
        # Warnings
        warnings = result.get('risk_warnings', [])
        if warnings:
            print("\n⚠️ RISK WARNINGS:")
            for w in warnings:
                print(f"   🚨 {w}")
        
        # Sources
        sources = result.get('research_sources', [])
        if sources:
            print("\n📚 Research Sources:")
            for s in sources[:3]:  # Limit to 3
                print(f"   • {s}")
        
        print("\n" + "=" * 60)
        return result


def generate_ai_analysis(days_back: int = 30):
    """Generate AI-powered trade analysis using config settings."""
    analyzer = AITradeAnalyzer()
    return analyzer.generate_report(days_back)


def evaluate_backtest_readiness(backtest_results: Dict) -> Dict:
    """Evaluate backtest readiness for production using LLM + Google Search."""
    analyzer = AITradeAnalyzer()
    return analyzer.print_readiness_report(backtest_results)


def analyze_backtest_file(filepath: str = None) -> Dict:
    """
    Analyze backtest results from pairs_config.json.
    
    This analyzes the BACKTEST results (from running cli.py backtest_pairs),
    NOT the live/paper trades from trades.db.
    """
    import os
    
    if filepath is None:
        # Prefer full results if available (all pairs, not just winners)
        full_path = os.path.join(config.ARTIFACTS_DIR, "backtest_full_results.json")
        if os.path.exists(full_path):
            filepath = full_path
        else:
            filepath = os.path.join(config.ARTIFACTS_DIR, "pairs_config.json")
    
    if not os.path.exists(filepath):
        print(f"❌ Backtest results not found: {filepath}")
        print("   Run: python cli.py backtest_pairs")
        return {}
    
    with open(filepath) as f:
        pairs = json.load(f)
    
    if not pairs:
        print("❌ No pairs in backtest results file.")
        return {}
    
    # Helper to get return value (supports both field names)
    def get_return(p):
        return p.get('backtest_return', p.get('return_pct', 0))
    
    # Aggregate metrics from all pairs
    total_return = sum(get_return(p) for p in pairs)
    avg_return = total_return / len(pairs)
    winning_pairs = [p for p in pairs if get_return(p) > 0]
    sectors = list(set(p.get('sector', 'UNKNOWN') for p in pairs))
    
    # Build aggregated results
    aggregated = {
        "source": filepath,
        "total_pairs_tested": len(pairs),
        "winning_pairs": len(winning_pairs),
        "losing_pairs": len(pairs) - len(winning_pairs),
        "win_rate": round(len(winning_pairs) / len(pairs) * 100, 1),
        "total_return_pct": round(total_return, 2),
        "avg_return_pct": round(avg_return, 2),
        "best_pair": max(pairs, key=lambda x: get_return(x)),
        "worst_pair": min(pairs, key=lambda x: get_return(x)),
        "sectors_tested": sectors,
        "top_5_pairs": sorted(pairs, key=lambda x: get_return(x), reverse=True)[:5]
    }
    
    print(f"\n🧪 --- AI BACKTEST RESULTS ANALYSIS ---")
    print(f"   Source: {filepath}")
    print(f"   Pairs Tested: {len(pairs)}")
    print(f"=" * 55)
    
    # Display summary
    print(f"\n📊 BACKTEST SUMMARY:")
    print(f"   Total Pairs: {aggregated['total_pairs_tested']}")
    print(f"   Winners: {aggregated['winning_pairs']} | Losers: {aggregated['losing_pairs']}")
    print(f"   Win Rate: {aggregated['win_rate']}%")
    print(f"   Total Return: {aggregated['total_return_pct']}%")
    print(f"   Avg Return/Pair: {aggregated['avg_return_pct']}%")
    
    print(f"\n🥇 TOP 5 PAIRS:")
    for p in aggregated['top_5_pairs']:
        pair_name = f"{p.get('leg1', '')}-{p.get('leg2', '')}"
        ret = p.get('backtest_return', p.get('return_pct', 0))
        sector = p.get('sector', 'UNKNOWN')
        print(f"   {ret:>6.2f}% | {pair_name:<20} ({sector})")
    
    best = aggregated['best_pair']
    worst = aggregated['worst_pair']
    best_ret = best.get('backtest_return', best.get('return_pct', 0))
    worst_ret = worst.get('backtest_return', worst.get('return_pct', 0))
    print(f"\n   Best:  {best.get('leg1')}-{best.get('leg2')} @ {best_ret:.2f}%")
    print(f"   Worst: {worst.get('leg1')}-{worst.get('leg2')} @ {worst_ret:.2f}%")
    
    # Now use LLM with Google Search for insights
    analyzer = AITradeAnalyzer()
    
    if analyzer._client:
        print(f"\n🤖 Generating AI Insights with Google Search...")
        
        try:
            from google.genai import types
            
            prompt = f"""
            Analyze these BACKTEST results for a Statistical Arbitrage Pairs Trading strategy in Indian markets (NSE).
            
            BACKTEST SUMMARY:
            - Total Pairs Tested: {aggregated['total_pairs_tested']}
            - Winning Pairs: {aggregated['winning_pairs']} ({aggregated['win_rate']}%)
            - Average Return per Pair: {aggregated['avg_return_pct']}%
            - Sectors: {', '.join(sectors)}
            
            TOP 5 PAIRS:
            {json.dumps(aggregated['top_5_pairs'], indent=2, default=str)}
            
            RESEARCH TASKS:
            1. Search for industry benchmarks for pairs trading backtests
            2. Evaluate if {aggregated['win_rate']}% win rate across {aggregated['total_pairs_tested']} pairs is competitive
            3. Compare the {aggregated['avg_return_pct']}% average return against typical stat arb returns
            4. Identify what additional features they need before live trading
            5. Give specific recommendations for improving the strategy
            
            Be specific and cite your research sources.
            """
            
            schema = {
                "type": "OBJECT",
                "properties": {
                    "overall_assessment": {"type": "STRING"},
                    "benchmark_comparison": {"type": "STRING"},
                    "strengths": {"type": "ARRAY", "items": {"type": "STRING"}},
                    "weaknesses": {"type": "ARRAY", "items": {"type": "STRING"}},
                    "recommendations": {"type": "ARRAY", "items": {"type": "STRING"}},
                    "live_trading_readiness": {"type": "STRING", "enum": ["READY", "NEEDS_WORK", "NOT_READY"]},
                    "confidence_score": {"type": "NUMBER"},
                    "research_sources": {"type": "ARRAY", "items": {"type": "STRING"}}
                },
                "required": ["overall_assessment", "live_trading_readiness"]
            }
            
            response = analyzer._client.models.generate_content(
                model=analyzer._model_id,
                contents=prompt,
                config=types.GenerateContentConfig(
                    tools=[types.Tool(google_search=types.GoogleSearch())],
                    response_mime_type="application/json",
                    response_schema=schema
                )
            )
            
            insights = json.loads(response.text)
            aggregated['ai_insights'] = insights
            
            # Display AI insights
            readiness = insights.get('live_trading_readiness', 'UNKNOWN')
            icon = "✅" if readiness == "READY" else "🟡" if readiness == "NEEDS_WORK" else "❌"
            
            print(f"\n{icon} LIVE TRADING READINESS: {readiness}")
            print(f"\n📝 Assessment:\n   {insights.get('overall_assessment', 'N/A')}")
            
            print(f"\n📊 Benchmark Comparison:\n   {insights.get('benchmark_comparison', 'N/A')}")
            
            strengths = insights.get('strengths', [])
            if strengths:
                print(f"\n✅ Strengths:")
                for s in strengths:
                    print(f"   • {s}")
            
            weaknesses = insights.get('weaknesses', [])
            if weaknesses:
                print(f"\n⚠️ Weaknesses:")
                for w in weaknesses:
                    print(f"   • {w}")
            
            recommendations = insights.get('recommendations', [])
            if recommendations:
                print(f"\n💡 Recommendations:")
                for r in recommendations:
                    print(f"   → {r}")
            
            sources = insights.get('research_sources', [])
            if sources:
                print(f"\n📚 Research Sources:")
                for s in sources[:3]:
                    print(f"   • {s}")
            
            print(f"\n📈 Confidence: {insights.get('confidence_score', 0) * 100:.0f}%")
            
        except Exception as e:
            print(f"⚠️ AI analysis failed: {e}")
    
    print("\n" + "=" * 55)
    
    # Save report
    output_path = os.path.join(config.ARTIFACTS_DIR, "ai_backtest_analysis.json")
    with open(output_path, 'w') as f:
        json.dump(aggregated, f, indent=2, default=str)
    print(f"📁 Report saved: {output_path}")
    
    return aggregated


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="AI Post-Trade Analysis")
    parser.add_argument('--days', type=int, default=30, help="Days to analyze (for live trades)")
    parser.add_argument('--check-readiness', action='store_true', help="Check production readiness")
    parser.add_argument('--backtest', action='store_true', help="Analyze backtest results from pairs_config.json")
    args = parser.parse_args()
    
    if args.backtest:
        # Analyze backtest results from pairs_config.json
        analyze_backtest_file()
    elif args.check_readiness:
        # Load latest backtest results
        import os
        config_path = os.path.join(config.ARTIFACTS_DIR, "pairs_config.json")
        if os.path.exists(config_path):
            with open(config_path) as f:
                results = json.load(f)
            if results:
                evaluate_backtest_readiness(results[0])
        else:
            print("❌ No backtest results found. Run backtest first.")
    else:
        generate_ai_analysis(args.days)
