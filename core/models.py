"""
Unified Pair Trading System - Data Models (Layer 1)

All data structures for the pair trading system.
Uses dataclasses for clean, immutable data objects.
"""

from dataclasses import dataclass, field
from datetime import datetime
from typing import List, Optional
import numpy as np


@dataclass
class StockData:
    """
    Raw stock data container.
    
    Attributes:
        symbol: Stock ticker symbol
        prices: Array of closing prices
        dates: Array of corresponding dates
        sector: Industry sector classification
        lot_size: F&O lot size for the stock
    """
    symbol: str
    prices: np.ndarray
    dates: List[datetime]
    sector: str
    lot_size: int
    
    def __post_init__(self):
        if isinstance(self.prices, list):
            self.prices = np.array(self.prices)


@dataclass
class RegressionResult:
    """
    Output from linear regression (Module 2).
    
    Regression: Y = β·X + c + ε
    
    Attributes:
        intercept: The constant term (c)
        beta: The slope/hedge ratio (β)
        residuals: Array of residual values (ε)
        standard_error: Standard error of residuals
        intercept_std_error: Standard error of intercept
        r_squared: Coefficient of determination (R²)
    """
    intercept: float
    beta: float
    residuals: np.ndarray
    standard_error: float
    intercept_std_error: float
    r_squared: float
    
    @property
    def error_ratio(self) -> float:
        """Error ratio = SE(intercept) / SE(residuals)"""
        if self.standard_error == 0:
            return float('inf')
        return self.intercept_std_error / self.standard_error


@dataclass
class PairAnalysis:
    """
    Complete pair analysis result (Module 5).
    
    Contains all information needed to evaluate and trade a pair.
    
    Attributes:
        x_stock: Independent variable (X) stock symbol
        y_stock: Dependent variable (Y) stock symbol
        sector: Common sector of the pair
        intercept: Regression intercept
        beta: Hedge ratio (slope)
        error_ratio: Quality metric for X/Y selection
        adf_value: ADF test p-value
        is_stationary: Whether residuals are stationary (p ≤ 0.05)
        residuals: Array of residual values
        residual_mean: Mean of residuals
        residual_std_dev: Standard deviation of residuals
        current_residual: Most recent residual value
        z_score: Current z-score signal
        quality: Quality classification (EXCELLENT/GOOD/FAIR/POOR)
        confidence_score: Confidence percentage (0-100)
    """
    x_stock: str
    y_stock: str
    sector: str
    
    # Regression parameters
    intercept: float
    beta: float
    error_ratio: float
    
    # Stationarity
    adf_value: float
    is_stationary: bool
    
    # Residuals
    residuals: np.ndarray
    residual_mean: float
    residual_std_dev: float
    
    # Current state
    current_residual: float
    z_score: float
    
    # Quality assessment
    quality: str
    confidence_score: float
    
    @property
    def pair_key(self) -> str:
        """Unique identifier for the pair."""
        return f"{self.y_stock}-{self.x_stock}"
    
    def __repr__(self):
        return (f"PairAnalysis({self.y_stock}/{self.x_stock}, "
                f"β={self.beta:.4f}, Z={self.z_score:.2f}, "
                f"ADF={self.adf_value:.4f}, Quality={self.quality})")


@dataclass
class PositionSizing:
    """
    Position sizing calculation result (Module 7).
    
    Handles beta-neutral lot size optimization.
    
    Attributes:
        lots_y: Number of lots for Y stock
        lots_x: Number of lots for X stock
        shares_y: Total shares for Y (lots_y × lot_size_y)
        shares_x: Total shares for X (lots_x × lot_size_x)
        target_beta: Desired beta ratio
        actual_beta: Achieved beta ratio
        beta_deviation: Percentage deviation from target
        notional_y: Value of Y position
        notional_x: Value of X position
        total_capital: Total capital required
        spot_needed: Whether spot adjustment is required
        spot_shares: Number of spot shares needed (if any)
    """
    lots_y: int
    lots_x: int
    shares_y: int
    shares_x: int
    
    # Beta neutrality
    target_beta: float
    actual_beta: float
    beta_deviation: float
    
    # Capital
    notional_y: float
    notional_x: float
    total_capital: float
    
    # Spot adjustment
    spot_needed: bool = False
    spot_shares: int = 0
    
    @property
    def is_valid(self) -> bool:
        """Check if sizing is valid (deviation ≤ 5%)."""
        return abs(self.beta_deviation) <= 5.0


@dataclass
class RiskAssessment:
    """
    Comprehensive risk assessment (Modules 8-9).
    
    100-point scoring system:
    - ADF Test: 25 points
    - Z-Score Signal: 20 points  
    - Intercept Risk: 30 points
    - Position Sizing: 25 points
    
    Attributes:
        intercept_percent: Unexplained percentage of Y price
        explained_percent: Explained percentage by model
        intercept_risk: Risk classification (LOW/MODERATE/ELEVATED/HIGH/VERY HIGH)
        adf_score: Points from ADF test
        z_score_score: Points from z-score signal
        intercept_score: Points from intercept analysis
        position_score: Points from position sizing
        total_score: Sum of all scores
        max_score: Maximum possible score (100)
        tradable: Whether pair is tradable
        recommendation: Human-readable recommendation
        warnings: List of warning messages
    """
    intercept_percent: float
    explained_percent: float
    intercept_risk: str
    
    # Scoring components
    adf_score: int = 0
    z_score_score: int = 0
    intercept_score: int = 0
    position_score: int = 0
    
    # Totals
    total_score: int = 0
    max_score: int = 100
    
    # Decision
    tradable: bool = False
    recommendation: str = ""
    warnings: List[str] = field(default_factory=list)
    
    @property
    def score_percent(self) -> float:
        """Score as percentage."""
        return (self.total_score / self.max_score) * 100


@dataclass
class Trade:
    """
    Trade record for position tracking (Module 13-16).
    
    Tracks both entry and exit of pair trades.
    
    Attributes:
        id: Unique trade identifier
        pair: Associated PairAnalysis object
        direction: LONG_PAIR or SHORT_PAIR
        entry_date: When trade was opened
        entry_z_score: Z-score at entry
        y_action: BUY or SELL for Y stock
        y_shares: Quantity of Y shares
        y_entry_price: Entry price for Y
        y_exit_price: Exit price for Y
        x_action: BUY or SELL for X stock
        x_shares: Quantity of X shares
        x_entry_price: Entry price for X
        x_exit_price: Exit price for X
        exit_date: When trade was closed
        exit_z_score: Z-score at exit
        exit_reason: TARGET/STOP_LOSS/END_OF_DAY/MANUAL
        realized_pnl: Closed P&L
        unrealized_pnl: Open P&L (updated live)
        status: OPEN/CLOSED/STOPPED
    """
    # Required fields (no defaults)
    id: str
    pair: PairAnalysis
    direction: str  # LONG_PAIR or SHORT_PAIR
    entry_date: datetime
    entry_z_score: float
    y_action: str
    y_shares: int
    y_entry_price: float
    x_action: str
    x_shares: int
    x_entry_price: float
    
    # Optional fields (with defaults)
    y_exit_price: float = 0.0
    x_exit_price: float = 0.0
    exit_date: Optional[datetime] = None
    exit_z_score: float = 0.0
    exit_reason: str = ""
    realized_pnl: float = 0.0
    unrealized_pnl: float = 0.0
    status: str = "OPEN"
    
    @property
    def is_open(self) -> bool:
        return self.status == "OPEN"
    
    def calculate_pnl(self, current_price_y: float, current_price_x: float) -> float:
        """Calculate unrealized P&L given current prices."""
        if self.y_action == "BUY":
            pnl_y = (current_price_y - self.y_entry_price) * self.y_shares
        else:
            pnl_y = (self.y_entry_price - current_price_y) * self.y_shares
            
        if self.x_action == "BUY":
            pnl_x = (current_price_x - self.x_entry_price) * self.x_shares
        else:
            pnl_x = (self.x_entry_price - current_price_x) * self.x_shares
            
        return pnl_y + pnl_x


@dataclass
class Portfolio:
    """
    Portfolio state container (Module 15).
    
    Manages capital and positions across multiple pairs.
    
    Attributes:
        total_capital: Initial capital allocation
        available_capital: Remaining capital for new trades
        open_trades: List of currently open Trade objects
        closed_trades: List of completed Trade objects
        total_pnl: Cumulative realized P&L
    """
    total_capital: float
    available_capital: float
    open_trades: List[Trade] = field(default_factory=list)
    closed_trades: List[Trade] = field(default_factory=list)
    total_pnl: float = 0.0
    
    @property
    def used_capital(self) -> float:
        """Capital currently in use."""
        return self.total_capital - self.available_capital
    
    @property
    def utilization(self) -> float:
        """Capital utilization percentage."""
        if self.total_capital == 0:
            return 0.0
        return (self.used_capital / self.total_capital) * 100
    
    @property
    def total_open_pnl(self) -> float:
        """Sum of unrealized P&L across open trades."""
        return sum(t.unrealized_pnl for t in self.open_trades)
    
    def has_position(self, pair_key: str) -> bool:
        """Check if already have position in a pair."""
        return any(t.pair.pair_key == pair_key for t in self.open_trades)
