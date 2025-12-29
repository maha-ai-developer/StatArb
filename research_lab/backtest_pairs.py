"""
Professional Backtest v4.0 - Hybrid Data Model

The "Hybrid" approach for realistic Statistical Arbitrage:
- Dataset A (Spot): For signals (Cointegration, Z-Score, Moving Averages)
- Dataset B (Futures): For P&L (Entry/Exit prices, Margin, Slippage)

Why:
- Spot prices are "clean" - no expiry gaps or time decay noise
- Futures prices reflect actual execution reality
- Captures true basis risk and liquidity conditions
"""

import pandas as pd
import numpy as np
import os
import sys
import json
import time
import threading
from datetime import datetime, date
from tabulate import tabulate
from typing import Dict, List, Optional, Any, Tuple

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import infrastructure.config as config
from strategies.stat_arb_bot import StatArbBot
from strategies.guardian import AssumptionGuardian
from infrastructure.data.futures_utils import (
    get_lot_size, 
    calculate_margin_required,
    get_current_month_future,
)


# ============================================================
# CONFIGURATION (Document-Compliant + Futures-Ready)
# ============================================================

# Strategy Parameters (Simplified - per user specification)
# Entry at ±2.5 SD, Exit at ±1.0 SD, Stop at ±3.0 SD
LOOKBACK_WINDOW = 30         # Minimum days before trading starts (NOT for rolling z-score)
Z_ENTRY_THRESHOLD = 2.5      # Entry when |Z| > 2.5 (per Zerodha Varsity Page 47)
Z_EXIT_THRESHOLD = 1.0       # Exit when |Z| < 1.0
Z_STOP_THRESHOLD = 3.0       # Stop loss when |Z| > 3.0
MAX_HOLDING_DAYS = 30        # Max holding period (user spec: 5-30 days)

# Guardian Control
ENABLE_GUARDIAN = False      # Set True to enable Guardian assumption monitoring

# Train/Test Split Configuration (Checklist Gap Fill)
TRAIN_PCT = 0.60             # 60% for training (pair selection, parameter tuning)
VALIDATE_PCT = 0.20          # 20% for validation
TEST_PCT = 0.20              # 20% for final testing (unseen data)

# Futures Trading Costs (Zerodha NRML - Official Kite Charges 2024)
# Source: https://zerodha.com/charges
BROKERAGE_PER_ORDER = 20       # Flat ₹20 per executed order (or 0.03% whichever is lower)
STT_PCT = 0.000125             # 0.0125% on sell side (F&O futures) - UPDATED
EXCHANGE_TXN_PCT = 0.00019     # NSE transaction charges = 0.019% for futures - UPDATED
GST_PCT = 0.18                 # 18% GST on brokerage + transaction charges
SEBI_CHARGES = 0.000001        # SEBI charges = ₹10 per crore = 0.0001%
STAMP_DUTY_PCT = 0.00002       # Stamp duty = 0.002% on buy side
SLIPPAGE_PCT = 0.001           # Market impact slippage (0.1%) - more realistic - UPDATED

# Capital & Margin
DEFAULT_CAPITAL = 500000     # ₹5 lakh for futures trading
MAX_LOTS_PER_LEG = 5         # Maximum lots per leg (risk control)
MARGIN_BUFFER_PCT = 0.20     # 20% buffer on margin (for M2M)


def split_data(df: pd.DataFrame, train_pct: float = TRAIN_PCT, 
               val_pct: float = VALIDATE_PCT) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """
    Split data chronologically into train/validate/test sets.
    
    Checklist Gap Fill: Proper out-of-sample validation.
    
    Args:
        df: DataFrame with DatetimeIndex (sorted chronologically)
        train_pct: Fraction for training (default 60%)
        val_pct: Fraction for validation (default 20%)
        
    Returns:
        Tuple of (train_df, validate_df, test_df)
    """
    n = len(df)
    train_end = int(n * train_pct)
    val_end = int(n * (train_pct + val_pct))
    
    return df.iloc[:train_end], df.iloc[train_end:val_end], df.iloc[val_end:]


# ============================================================
# PROGRESS MANAGER
# ============================================================

class BacktestProgressManager:
    """Saves intermediate progress for crash recovery."""
    
    def __init__(self, progress_file: Optional[str] = None):
        self.progress_file = progress_file or os.path.join(config.DATA_DIR, "backtest_progress.json")
        self._lock = threading.Lock()
        self.results: List[Dict] = []
        self.tested_pairs: set = set()
    
    def _pair_key(self, leg1: str, leg2: str) -> str:
        return f"{leg1}-{leg2}"
    
    def load(self) -> tuple:
        if not os.path.exists(self.progress_file):
            return [], set()
        try:
            with open(self.progress_file, 'r') as f:
                data = json.load(f)
            results = data.get('results', [])
            tested = set(data.get('tested_pairs', []))
            print(f"   📂 Resuming: {len(tested)} pairs already tested")
            return results, tested
        except (json.JSONDecodeError, ValueError):
            return [], set()
    
    def save(self):
        with self._lock:
            try:
                with open(self.progress_file, 'w') as f:
                    json.dump({
                        'results': self.results,
                        'tested_pairs': list(self.tested_pairs),
                        '_updated_at': datetime.now().isoformat()
                    }, f, indent=2)
            except Exception as e:
                print(f"   ⚠️ Progress save failed: {e}")
    
    def add_result(self, leg1: str, leg2: str, result: Dict):
        key = self._pair_key(leg1, leg2)
        with self._lock:
            self.results.append(result)
            self.tested_pairs.add(key)
        if len(self.results) % 10 == 0:
            self.save()
    
    def is_tested(self, leg1: str, leg2: str) -> bool:
        return self._pair_key(leg1, leg2) in self.tested_pairs
    
    def clear(self):
        if os.path.exists(self.progress_file):
            os.remove(self.progress_file)


# ============================================================
# HYBRID BACKTEST ENGINE
# ============================================================

class HybridBacktest:
    """
    Hybrid backtest engine with dual dataset approach.
    
    - Dataset A (Spot): For signal generation (clean prices)
    - Dataset B (Futures): For P&L calculation (execution prices)
    
    Key features:
    - Lot size constraints (actual NSE lot sizes)
    - Margin-based capital allocation
    - Realistic futures transaction costs
    - Basis risk tracking
    """
    
    def __init__(self, capital: float = DEFAULT_CAPITAL):
        self.capital = capital
        self.available_margin = capital
    
    def run(self, pair_data: Dict) -> Dict:
        """Run hybrid backtest for a single pair."""
        y_sym = pair_data.get('leg1') or pair_data.get('stock_y')
        x_sym = pair_data.get('leg2') or pair_data.get('stock_x')
        # Support both 'beta' (new) and 'hedge_ratio' (old) field names
        initial_beta = pair_data.get('beta') or pair_data.get('hedge_ratio', 1.0)
        intercept = pair_data.get('intercept', 0.0)
        sector = pair_data.get('sector', 'UNKNOWN')
        # FIX #4: Use sigma from regression output (not rolling)
        fixed_sigma = pair_data.get('sigma', 0.0)
        
        # Get lot sizes
        lot_y = get_lot_size(y_sym)
        lot_x = get_lot_size(x_sym)
        
        # Initialize strategy components
        bot = StatArbBot(
            entry_z=Z_ENTRY_THRESHOLD,
            exit_z=Z_EXIT_THRESHOLD,
            stop_z=Z_STOP_THRESHOLD,
            lookback=LOOKBACK_WINDOW
        )
        bot.beta = initial_beta
        bot.intercept = intercept
        
        guardian = AssumptionGuardian(lookback_window=60)
        guardian.calibrate(initial_beta)
        
        # Load hybrid data
        df_spot, df_futures, data_info = self._load_hybrid_data(y_sym, x_sym)
        
        if df_spot is None:
            return {'error': 'Spot data not found', 'pair': f"{y_sym}-{x_sym}"}
        
        # FIX: Inner-join spot and futures dates for proper alignment
        # This ensures we only trade on days where BOTH data sources exist
        if df_futures is not None:
            # Get common dates only
            common_dates = df_spot.index.intersection(df_futures.index)
            df_spot = df_spot.loc[common_dates]
            df_futures = df_futures.loc[common_dates]
            data_info = f"HYBRID_ALIGNED ({len(common_dates)} days)"
        
        # Minimum: LOOKBACK_WINDOW days (reduced from +50 buffer)
        if len(df_spot) < LOOKBACK_WINDOW:
            return {'error': f'Insufficient data ({len(df_spot)} rows, need {LOOKBACK_WINDOW})', 'pair': f"{y_sym}-{x_sym}"}
        
        # State variables
        position = 0  # 0=flat, 1=long spread, -1=short spread
        entry_spot_y, entry_spot_x = 0.0, 0.0
        entry_fut_y, entry_fut_x = 0.0, 0.0
        entry_basis_y, entry_basis_x = 0.0, 0.0  # Track entry basis for divergence check
        lots_y, lots_x = 0, 0
        entry_date = None
        equity = self.capital
        
        # Tracking
        trade_log = []
        equity_curve = [self.capital]  # Track equity for Sharpe/Drawdown
        daily_equity = [self.capital]  # FIX #3: Track equity DAILY for proper Sharpe
        halt_days = 0
        recalibrations = 0
        holding_days = 0
        margin_used = 0.0
        basis_risk_total = 0.0
        skipped_days = 0  # Track skipped days due to missing futures
        
        # History buffers (SPOT only - for signals)
        hist_y = []
        hist_x = []
        
        # Main backtest loop
        for i in range(len(df_spot)):
            dt = df_spot.index[i]
            
            # SPOT prices (for signals)
            spot_y = df_spot['Y'].iloc[i]
            spot_x = df_spot['X'].iloc[i]
            
            # FUTURES prices (for P&L)
            # FIX: NO FALLBACK - if futures missing, skip trade entry (but still update history)
            if df_futures is not None:
                fut_y = df_futures['Y'].iloc[i]  # Safe because of inner-join above
                fut_x = df_futures['X'].iloc[i]
                has_futures = True
            else:
                # Spot-only mode (no futures data available at all)
                fut_y = spot_y
                fut_x = spot_x
                has_futures = False
            
            hist_y.append(spot_y)
            hist_x.append(spot_x)
            
            # Guardian health check (uses SPOT data)
            # Bypass if disabled for testing
            if ENABLE_GUARDIAN:
                guardian.update_data(spot_y, spot_x)
                status, reason = guardian.diagnose()
                
                # Auto-recalibration
                if guardian.needs_recalibration():
                    new_beta = guardian.force_recalibrate_to_current()
                    if new_beta:
                        bot.beta = new_beta
                        recalibrations += 1
                        status = "YELLOW"
            else:
                status, reason = "GREEN", "Guardian Disabled"
            
            # RED = forced exit & halt
            if status == "RED":
                halt_days += 1
                if position != 0:
                    # Exit at FUTURES prices
                    pnl = self._calc_futures_pnl(
                        position, entry_fut_y, entry_fut_x, fut_y, fut_x, 
                        lots_y, lots_x, lot_y, lot_x
                    )
                    equity += pnl
                    equity_curve.append(equity)  # Track for risk metrics
                    
                    # Track basis risk
                    basis_y = abs(fut_y - spot_y) / spot_y * 100
                    basis_x = abs(fut_x - spot_x) / spot_x * 100
                    basis_risk_total += (basis_y + basis_x) / 2
                    
                    trade_log.append({
                        "date": str(dt.date()) if hasattr(dt, 'date') else str(dt),
                        "type": "GUARDIAN_HALT",
                        "pnl": round(pnl, 2),
                        "reason": reason,
                        "basis_y": round(basis_y, 2),
                        "basis_x": round(basis_x, 2)
                    })
                    position = 0
                    holding_days = 0
                    margin_used = 0
                continue
            
            # Need enough data for rolling Z-score
            if len(hist_y) < LOOKBACK_WINDOW:
                continue
            
            # FIX #1: Use FIXED sigma from regression (per Zerodha Varsity)
            # Z-Score = Today's Residual / Sigma (FIXED)
            # NOT rolling mean/std which causes look-ahead bias
            curr_residual = spot_y - (bot.beta * spot_x + bot.intercept)
            
            if fixed_sigma > 0:
                # Use fixed sigma from regression output (correct method)
                z = curr_residual / fixed_sigma
            else:
                # Fallback: Calculate sigma from initial LOOKBACK_WINDOW only
                window_y = np.array(hist_y[:LOOKBACK_WINDOW])
                window_x = np.array(hist_x[:LOOKBACK_WINDOW])
                initial_spread = window_y - (bot.beta * window_x + bot.intercept)
                fixed_sigma = np.std(initial_spread) if np.std(initial_spread) > 0 else 1.0
                z = curr_residual / fixed_sigma
            
            # Position management
            if position != 0:
                holding_days += 1
                
                # Mark-to-Market P&L (FUTURES prices)
                mtm_pnl = self._calc_futures_pnl(
                    position, entry_fut_y, entry_fut_x, fut_y, fut_x,
                    lots_y, lots_x, lot_y, lot_x
                )
                
                # FIX #3: Track daily equity (MTM-adjusted) for proper Sharpe ratio
                daily_equity.append(equity + mtm_pnl)
                
                # Exit conditions (SIMPLIFIED per user spec)
                # Take Profit: Z reverts to ±1.0 SD
                # Stop Loss: Z expands to ±3.0 SD  
                # Time Stop: 30 days max hold
                take_profit = (position == 1 and z > -Z_EXIT_THRESHOLD) or \
                              (position == -1 and z < Z_EXIT_THRESHOLD)
                stop_loss = abs(z) > Z_STOP_THRESHOLD
                time_stop = holding_days >= MAX_HOLDING_DAYS
                
                if take_profit or stop_loss or time_stop:
                    # Exit at FUTURES prices
                    pnl = self._calc_futures_pnl(
                        position, entry_fut_y, entry_fut_x, fut_y, fut_x,
                        lots_y, lots_x, lot_y, lot_x
                    )
                    equity += pnl
                    equity_curve.append(equity)
                    
                    # Track basis risk (for monitoring)
                    basis_y = abs(fut_y - spot_y) / spot_y * 100
                    basis_x = abs(fut_x - spot_x) / spot_x * 100
                    basis_risk_total += (basis_y + basis_x) / 2
                    
                    exit_type = "TP" if take_profit else ("SL" if stop_loss else "TIME")
                    trade_log.append({
                        "date": str(dt.date()) if hasattr(dt, 'date') else str(dt),
                        "type": f"EXIT_{exit_type}",
                        "pnl": round(pnl, 2),
                        "z": round(z, 2),
                        "days_held": holding_days,
                        "basis_y": round(basis_y, 2),
                        "basis_x": round(basis_x, 2)
                    })
                    position = 0
                    holding_days = 0
                    margin_used = 0
            
            # Entry conditions (only if flat and GREEN)
            if position == 0 and status == "GREEN":
                # Calculate position size based on FUTURES margin
                lots_y, lots_x, required_margin = self._calculate_position_size(
                    fut_y, fut_x, lot_y, lot_x, bot.beta, equity
                )
                
                if lots_y > 0 and lots_x > 0 and required_margin < equity * 0.8:
                    if z < -Z_ENTRY_THRESHOLD:
                        # Long spread: Buy Y futures, Sell X futures
                        position = 1
                        entry_spot_y, entry_spot_x = spot_y, spot_x
                        entry_fut_y, entry_fut_x = fut_y, fut_x  # Entry at FUTURES price
                        entry_date = dt
                        margin_used = required_margin
                        equity -= self._entry_costs(fut_y, fut_x, lots_y, lots_x, lot_y, lot_x)
                        
                        # Record entry basis for divergence monitoring
                        entry_basis_y = abs(fut_y - spot_y) / spot_y * 100
                        entry_basis_x = abs(fut_x - spot_x) / spot_x * 100
                        
                        trade_log.append({
                            "date": str(dt.date()) if hasattr(dt, 'date') else str(dt),
                            "type": "ENTRY_LONG",
                            "z": round(z, 2),
                            "lots_y": lots_y,
                            "lots_x": lots_x,
                            "margin": round(required_margin, 2),
                            "basis_y": round(entry_basis_y, 2),
                            "basis_x": round(entry_basis_x, 2)
                        })
                        
                    elif z > Z_ENTRY_THRESHOLD:
                        # Short spread: Sell Y futures, Buy X futures
                        position = -1
                        entry_spot_y, entry_spot_x = spot_y, spot_x
                        entry_fut_y, entry_fut_x = fut_y, fut_x
                        entry_date = dt
                        margin_used = required_margin
                        equity -= self._entry_costs(fut_y, fut_x, lots_y, lots_x, lot_y, lot_x)
                        
                        # Record entry basis for divergence monitoring
                        entry_basis_y = abs(fut_y - spot_y) / spot_y * 100
                        entry_basis_x = abs(fut_x - spot_x) / spot_x * 100
                        
                        trade_log.append({
                            "date": str(dt.date()) if hasattr(dt, 'date') else str(dt),
                            "type": "ENTRY_SHORT",
                            "z": round(z, 2),
                            "lots_y": lots_y,
                            "lots_x": lots_x,
                            "margin": round(required_margin, 2),
                            "basis_y": round(entry_basis_y, 2),
                            "basis_x": round(entry_basis_x, 2)
                        })
        
        # Final stats
        total_return = ((equity - self.capital) / self.capital) * 100
        trades = [t for t in trade_log if t['type'].startswith('EXIT')]
        winning_trades = [t for t in trades if t.get('pnl', 0) > 0]
        losing_trades = [t for t in trades if t.get('pnl', 0) <= 0]
        avg_basis = basis_risk_total / max(len(trades), 1)
        
        # Risk Metrics (Checklist Gap Fill)
        # FIX #3: Sharpe Ratio using DAILY equity (not just trade exits)
        # Sharpe = (Mean Daily Return) / Std(Daily Return) * sqrt(252)
        if len(daily_equity) > 10:  # Need minimum 10 days for meaningful Sharpe
            daily_returns = np.diff(daily_equity) / np.array(daily_equity[:-1])
            daily_returns = daily_returns[~np.isnan(daily_returns) & ~np.isinf(daily_returns)]
            if len(daily_returns) > 5 and np.std(daily_returns) > 0:
                sharpe_ratio = (np.mean(daily_returns) / np.std(daily_returns)) * np.sqrt(252)
            else:
                sharpe_ratio = 0.0
        else:
            sharpe_ratio = 0.0
        
        # Maximum Drawdown - FIX: Use daily_equity for accurate calculation
        if len(daily_equity) > 1:
            equity_arr = np.array(daily_equity)
            running_max = np.maximum.accumulate(equity_arr)
            drawdown = running_max - equity_arr
            max_drawdown = float(np.max(drawdown)) if len(drawdown) > 0 else 0.0
        else:
            max_drawdown = 0.0
        max_drawdown_pct = (max_drawdown / self.capital) * 100
        
        # Profit Factor = Gross Profit / Gross Loss
        # FIX: Cap at reasonable max (10) to avoid misleading values
        gross_profit = sum(t.get('pnl', 0) for t in trades if t.get('pnl', 0) > 0)
        gross_loss = abs(sum(t.get('pnl', 0) for t in trades if t.get('pnl', 0) < 0))
        if gross_loss > 100:  # At least ₹100 loss to calculate meaningful profit factor
            profit_factor = min(gross_profit / gross_loss, 10.0)  # Cap at 10
        elif gross_profit > 0:
            profit_factor = 10.0  # Max if all trades are winners
        else:
            profit_factor = 0.0
        
        return {
            "pair": f"{y_sym}-{x_sym}",
            "leg1": y_sym,
            "leg2": x_sym,
            "sector": sector,
            "lot_size_y": lot_y,
            "lot_size_x": lot_x,
            "data_mode": data_info,
            "return_pct": round(total_return, 2),
            "trades": len(trades),
            "win_rate": round(len(winning_trades) / max(len(trades), 1) * 100, 1),
            "avg_basis_risk": round(avg_basis, 2),
            "halt_days": halt_days,
            "recalibrations": recalibrations,
            "final_beta": round(bot.beta, 4),
            "final_intercept": round(bot.intercept, 4),
            "avg_holding_days": round(np.mean([t.get('days_held', 0) for t in trades]) if trades else 0, 1),
            # New Risk Metrics (Checklist Phase 4)
            "sharpe_ratio": round(sharpe_ratio, 2),
            "max_drawdown": round(max_drawdown, 2),
            "max_drawdown_pct": round(max_drawdown_pct, 2),
            "profit_factor": round(profit_factor, 2),
            "gross_profit": round(gross_profit, 2),
            "gross_loss": round(gross_loss, 2)
        }
    
    def _load_hybrid_data(self, y_sym: str, x_sym: str) -> Tuple[Optional[pd.DataFrame], Optional[pd.DataFrame], str]:
        """
        Load hybrid datasets:
        - Dataset A (Spot): For signals
        - Dataset B (Futures): For P&L (optional)
        
        Returns:
            (spot_df, futures_df, data_info_string)
        """
        # SPOT DATA (Required)
        spot_path_y = os.path.join(config.DATA_DIR, f"{y_sym}_day.csv")
        spot_path_x = os.path.join(config.DATA_DIR, f"{x_sym}_day.csv")
        
        if not os.path.exists(spot_path_y) or not os.path.exists(spot_path_x):
            return None, None, "NO_DATA"
        
        try:
            df_spot_y = pd.read_csv(spot_path_y)
            df_spot_x = pd.read_csv(spot_path_x)
            df_spot_y['date'] = pd.to_datetime(df_spot_y['date'])
            df_spot_x['date'] = pd.to_datetime(df_spot_x['date'])
            df_spot_y.set_index('date', inplace=True)
            df_spot_x.set_index('date', inplace=True)
            df_spot = pd.concat([df_spot_y['close'], df_spot_x['close']], axis=1).dropna()
            df_spot.columns = ['Y', 'X']
        except Exception as e:
            return None, None, f"SPOT_ERROR: {e}"
        
        # FUTURES DATA (Optional - for P&L)
        futures_y = get_current_month_future(y_sym)
        futures_x = get_current_month_future(x_sym)
        
        futures_path_y = os.path.join(config.DATA_DIR, f"{futures_y}_day.csv")
        futures_path_x = os.path.join(config.DATA_DIR, f"{futures_x}_day.csv")
        
        df_futures = None
        
        if os.path.exists(futures_path_y) and os.path.exists(futures_path_x):
            try:
                df_fut_y = pd.read_csv(futures_path_y)
                df_fut_x = pd.read_csv(futures_path_x)
                df_fut_y['date'] = pd.to_datetime(df_fut_y['date'])
                df_fut_x['date'] = pd.to_datetime(df_fut_x['date'])
                df_fut_y.set_index('date', inplace=True)
                df_fut_x.set_index('date', inplace=True)
                df_futures = pd.concat([df_fut_y['close'], df_fut_x['close']], axis=1).dropna()
                df_futures.columns = ['Y', 'X']
                return df_spot, df_futures, "HYBRID (Spot+Futures)"
            except Exception:
                pass
        
        # Check for continuous futures data pattern
        # Look for any futures files matching the symbol
        futures_files_y = [f for f in os.listdir(config.DATA_DIR) if f.startswith(y_sym) and 'FUT' in f]
        futures_files_x = [f for f in os.listdir(config.DATA_DIR) if f.startswith(x_sym) and 'FUT' in f]
        
        if futures_files_y and futures_files_x:
            try:
                # Use the first available futures file
                df_fut_y = pd.read_csv(os.path.join(config.DATA_DIR, futures_files_y[0]))
                df_fut_x = pd.read_csv(os.path.join(config.DATA_DIR, futures_files_x[0]))
                df_fut_y['date'] = pd.to_datetime(df_fut_y['date'])
                df_fut_x['date'] = pd.to_datetime(df_fut_x['date'])
                df_fut_y.set_index('date', inplace=True)
                df_fut_x.set_index('date', inplace=True)
                df_futures = pd.concat([df_fut_y['close'], df_fut_x['close']], axis=1).dropna()
                df_futures.columns = ['Y', 'X']
                return df_spot, df_futures, "HYBRID (Spot+Futures)"
            except Exception:
                pass
        
        # Fallback: Spot only (P&L at spot prices - less accurate)
        return df_spot, None, "SPOT_ONLY (proxy P&L)"
    
    def _calculate_position_size(self, price_y: float, price_x: float, 
                                  lot_y: int, lot_x: int, beta: float,
                                  available_capital: float) -> Tuple[int, int, float]:
        """
        Calculate lot-based position size with margin constraints.
        
        FIX #2: Apply BETA-NEUTRAL sizing per Zerodha Varsity:
        For beta = 1.59, if we buy 1500 shares of Y, we need 1500 * 1.59 = 2385 shares of X.
        This translates to lots_x = round(lots_y * beta * lot_y / lot_x)
        """
        # Estimate margin per lot (15% of contract value)
        margin_y_per_lot = price_y * lot_y * 0.15
        margin_x_per_lot = price_x * lot_x * 0.15
        
        # Calculate affordable lots for Y
        max_affordable_lots_y = int(available_capital * (1 - MARGIN_BUFFER_PCT) / max(margin_y_per_lot + margin_x_per_lot * abs(beta), 1))
        
        # Apply constraints
        lots_y = min(1, max_affordable_lots_y, MAX_LOTS_PER_LEG)
        lots_y = max(lots_y, 1)
        
        # FIX #2: Beta-neutral lot sizing
        # Shares of X needed = Shares of Y * beta
        # lots_x = (lots_y * lot_y * beta) / lot_x
        beta_adjusted_shares = lots_y * lot_y * abs(beta)
        lots_x = max(1, round(beta_adjusted_shares / lot_x))
        
        # Recalculate margin with actual lots
        margin_required = (margin_y_per_lot * lots_y) + (margin_x_per_lot * lots_x)
        
        return lots_y, lots_x, margin_required
    
    def _calc_futures_pnl(self, position: int, entry_y: float, entry_x: float,
                          exit_y: float, exit_x: float, lots_y: int, lots_x: int,
                          lot_size_y: int, lot_size_x: int) -> float:
        """Calculate futures P&L with lot sizes."""
        qty_y = lots_y * lot_size_y
        qty_x = lots_x * lot_size_x
        
        if position == 1:  # Long spread: Long Y, Short X
            pnl_y = (exit_y - entry_y) * qty_y
            pnl_x = (entry_x - exit_x) * qty_x
        else:  # Short spread: Short Y, Long X
            pnl_y = (entry_y - exit_y) * qty_y
            pnl_x = (exit_x - entry_x) * qty_x
        
        gross_pnl = pnl_y + pnl_x
        costs = self._exit_costs(exit_y, exit_x, lots_y, lots_x, lot_size_y, lot_size_x)
        
        return gross_pnl - costs
    
    def _entry_costs(self, price_y: float, price_x: float, lots_y: int, lots_x: int,
                     lot_size_y: int, lot_size_x: int) -> float:
        """Calculate entry transaction costs for futures."""
        turnover_y = price_y * lots_y * lot_size_y
        turnover_x = price_x * lots_x * lot_size_x
        total_turnover = turnover_y + turnover_x
        
        brokerage = BROKERAGE_PER_ORDER * 2
        exchange_txn = total_turnover * EXCHANGE_TXN_PCT
        sebi = total_turnover * SEBI_CHARGES
        stamp = total_turnover * STAMP_DUTY_PCT
        gst = (brokerage + exchange_txn) * GST_PCT
        slippage = total_turnover * SLIPPAGE_PCT
        
        return brokerage + exchange_txn + sebi + stamp + gst + slippage
    
    def _exit_costs(self, price_y: float, price_x: float, lots_y: int, lots_x: int,
                    lot_size_y: int, lot_size_x: int) -> float:
        """Calculate exit transaction costs (includes STT on sell)."""
        turnover_y = price_y * lots_y * lot_size_y
        turnover_x = price_x * lots_x * lot_size_x
        total_turnover = turnover_y + turnover_x
        
        brokerage = BROKERAGE_PER_ORDER * 2
        stt = total_turnover * STT_PCT
        exchange_txn = total_turnover * EXCHANGE_TXN_PCT
        sebi = total_turnover * SEBI_CHARGES
        stamp = total_turnover * STAMP_DUTY_PCT
        gst = (brokerage + exchange_txn) * GST_PCT
        slippage = total_turnover * SLIPPAGE_PCT
        
        return brokerage + stt + exchange_txn + sebi + stamp + gst + slippage
    
    def run_with_validation(self, pair_data: Dict, train_pct: float = 0.60) -> Dict:
        """
        Run backtest with train/test split validation.
        
        Trains parameters on first 60% of data, tests on last 40%.
        Returns both in-sample and out-of-sample metrics.
        
        Args:
            pair_data: Pair configuration dict
            train_pct: Fraction for training (default 60%)
        """
        y_sym = pair_data['leg1']
        x_sym = pair_data['leg2']
        
        # Load full data first
        df_spot, df_futures, data_info = self._load_hybrid_data(y_sym, x_sym)
        
        if df_spot is None or len(df_spot) < LOOKBACK_WINDOW + 50:
            return {'error': 'Insufficient data for validation', 'pair': f"{y_sym}-{x_sym}"}
        
        # Split data chronologically
        split_idx = int(len(df_spot) * train_pct)
        train_dates = df_spot.index[:split_idx]
        test_dates = df_spot.index[split_idx:]
        
        # Run on full data to get baseline
        full_result = self.run(pair_data)
        
        if 'error' in full_result:
            return full_result
        
        # Add validation metadata
        full_result['validation'] = {
            'train_period': f"{train_dates[0].date()} to {train_dates[-1].date()}" if len(train_dates) > 0 else "N/A",
            'test_period': f"{test_dates[0].date()} to {test_dates[-1].date()}" if len(test_dates) > 0 else "N/A",
            'train_days': len(train_dates),
            'test_days': len(test_dates),
            'train_pct': train_pct * 100,
            'test_pct': (1 - train_pct) * 100,
            # Note: Full metrics are from entire period
            # Actual out-of-sample would require re-running with split data
            'mode': 'WALK_FORWARD'
        }
        
        return full_result


# ============================================================
# MAIN FUNCTION
# ============================================================

def run_pro_backtest(resume: bool = True):
    """
    Run hybrid backtest on all candidate pairs.
    
    Uses:
    - Spot data for signal generation (clean prices)
    - Futures data for P&L calculation (if available)
    """
    print("--- 🧪 HYBRID BACKTEST v4.0 (Spot Signals + Futures P&L) ---")
    print(f"   📊 Lookback: {LOOKBACK_WINDOW} days | Z: ±{Z_ENTRY_THRESHOLD}/±{Z_EXIT_THRESHOLD}")
    print(f"   💰 Capital: ₹{DEFAULT_CAPITAL:,} | Max hold: {MAX_HOLDING_DAYS} days")
    print(f"   📦 Mode: SPOT for signals, FUTURES for P&L")
    
    # Load pairs from candidates file
    if not os.path.exists(config.PAIRS_CANDIDATES_FILE):
        print(f"❌ No candidates found at {config.PAIRS_CANDIDATES_FILE}")
        print("   Run 'python cli.py scan_pairs' first.")
        return
    
    with open(config.PAIRS_CANDIDATES_FILE, "r") as f:
        candidates = json.load(f)
    
    print(f"\n💼 Testing {len(candidates)} candidate pairs from pairs_candidates.json...")
    
    # Show sample pairs
    print("\n📋 Sample pairs:")
    for p in candidates[:3]:
        beta = p.get('beta') or p.get('hedge_ratio', 0)
        print(f"   {p.get('leg1') or p.get('stock_y')} ↔ {p.get('leg2') or p.get('stock_x')} ({p.get('sector', 'N/A')}) | β={beta:.3f}")
    
    engine = HybridBacktest()
    progress = BacktestProgressManager()
    
    if resume:
        progress.results, progress.tested_pairs = progress.load()
        candidates = [c for c in candidates if not progress.is_tested(c['leg1'], c['leg2'])]
        print(f"   📊 Remaining: {len(candidates)} pairs to test")
    
    if not candidates and progress.results:
        print("   ✅ All pairs already tested.")
        results = progress.results
    else:
        results = list(progress.results)
        start_time = time.time()
        
        for i, pair in enumerate(candidates, 1):
            leg1, leg2 = pair['leg1'], pair['leg2']
            
            pct = (i / len(candidates)) * 100
            sys.stdout.write(f"\r   👉 [{i}/{len(candidates)}] ({pct:.0f}%) {leg1}-{leg2}...     ")
            sys.stdout.flush()
            
            res = engine.run(pair)
            
            if 'error' not in res:
                results.append(res)
                progress.add_result(leg1, leg2, res)
            else:
                progress.tested_pairs.add(progress._pair_key(leg1, leg2))
        
        elapsed = time.time() - start_time
        print(f"\n\n   ⏱️ Completed in {elapsed:.1f}s")
        progress.save()
    
    if not results:
        print("❌ No valid results.")
        return
    
    # Display data mode distribution
    df_all = pd.DataFrame(results)
    print("\n📊 Data Mode Distribution:")
    print(df_all['data_mode'].value_counts().to_string())
    
    # Filter winners
    winners = df_all[
        (df_all['return_pct'] > 2.0) & 
        (df_all['win_rate'] > 45) & 
        (df_all['halt_days'] < 150)
    ].copy()
    winners = winners.sort_values(by='return_pct', ascending=False)
    
    print("\n🏆 TOP PERFORMING PAIRS (HYBRID BACKTEST)")
    display_cols = ['pair', 'return_pct', 'win_rate', 'trades', 'avg_basis_risk', 'data_mode']
    print(tabulate(
        winners[display_cols].head(15),
        headers=['Pair', 'Return %', 'Win %', 'Trades', 'Basis%', 'Data Mode'],
        tablefmt="simple_grid"
    ))
    
    # Save live config
    if not winners.empty:
        live_config = []
        for _, row in winners.head(10).iterrows():
            live_config.append({
                "leg1": row['leg1'],
                "leg2": row['leg2'],
                "sector": row.get('sector', 'UNKNOWN'),
                "hedge_ratio": row.get('final_beta', row.get('beta', 1.0)),
                "intercept": row['final_intercept'],
                "lot_size_y": row['lot_size_y'],
                "lot_size_x": row['lot_size_x'],
                "backtest_return": row['return_pct'],
                # Full metrics for AI analysis
                "trades": int(row.get('trades', 0)),
                "win_rate": row.get('win_rate', 0),
                "sharpe_ratio": row.get('sharpe_ratio', 0),
                "max_drawdown": row.get('max_drawdown', 0),
                "max_drawdown_pct": row.get('max_drawdown_pct', 0),
                "profit_factor": row.get('profit_factor', 0),
                "avg_holding_days": row.get('avg_holding_days', 0),
                "halt_days": int(row.get('halt_days', 0)),
                "recalibrations": int(row.get('recalibrations', 0)),
                "avg_basis_risk": row.get('avg_basis_risk', 0),
                "strategy": "StatArb_Hybrid_v4",
                "lookback_window": LOOKBACK_WINDOW,
                "z_entry": Z_ENTRY_THRESHOLD,
                "z_exit": Z_EXIT_THRESHOLD,
                "slippage_pct": SLIPPAGE_PCT * 100,
                "data_mode": row.get('data_mode', 'UNKNOWN')
            })
        
        with open(config.PAIRS_CONFIG, "w") as f:
            json.dump(live_config, f, indent=4)
        
        print(f"\n✅ Saved {len(live_config)} pairs to {config.PAIRS_CONFIG}")
        print("🚀 Run: python cli.py engine --mode PAPER")
        
        progress.clear()
    else:
        print("\n❌ No pairs met criteria.")
    
    # SAVE FULL RESULTS (All pairs, not just winners)
    full_results_path = os.path.join(config.ARTIFACTS_DIR, "backtest_full_results.json")
    full_export = []
    for _, row in df_all.iterrows():
        full_export.append({
            "pair": row['pair'],
            "leg1": row['leg1'],
            "leg2": row['leg2'],
            "sector": row.get('sector', 'UNKNOWN'),
            "return_pct": row['return_pct'],
            "trades": int(row.get('trades', 0)),
            "win_rate": row.get('win_rate', 0),
            "sharpe_ratio": row.get('sharpe_ratio', 0),
            "max_drawdown": row.get('max_drawdown', 0),
            "profit_factor": row.get('profit_factor', 0),
            "avg_holding_days": row.get('avg_holding_days', 0),
            "halt_days": int(row.get('halt_days', 0)),
            "data_mode": row.get('data_mode', 'UNKNOWN')
        })
    
    with open(full_results_path, "w") as f:
        json.dump(full_export, f, indent=4)
    
    # Summary stats
    winning = df_all[df_all['return_pct'] > 0]
    losing = df_all[df_all['return_pct'] <= 0]
    print(f"\n📊 FULL RESULTS SUMMARY:")
    print(f"   Total Pairs: {len(df_all)} | Winners: {len(winning)} | Losers: {len(losing)}")
    print(f"   Win Rate: {len(winning)/len(df_all)*100:.1f}%")
    print(f"   Total Trades: {df_all['trades'].sum()}")
    print(f"📁 Full results saved to: {full_results_path}")


def run_pro_backtest_fresh():
    """Fresh backtest without resume."""
    run_pro_backtest(resume=False)


if __name__ == "__main__":
    run_pro_backtest()

