class RiskManager:
    def __init__(self, max_daily_loss: float = 500.0, max_positions: int = 3):
        self.max_daily_loss = max_daily_loss  # Stop trading if we lose ₹500 today
        self.max_positions = max_positions    # Don't hold more than 3 stocks at once
        
        self.current_pnl = 0.0
        self.open_positions = 0
        self.is_locked = False  # The "Kill Switch"

    def update_pnl(self, new_pnl: float):
        """Call this whenever PnL updates from the feed."""
        self.current_pnl = new_pnl
        
        # KILL SWITCH CHECK
        if self.current_pnl <= -(self.max_daily_loss):
            print(f"[RISK ALERT] Daily Loss Limit Hit ({self.current_pnl}). Locking Engine.")
            self.is_locked = True

    def can_open_trade(self, symbol: str) -> bool:
        """Ask this before placing ANY order."""
        
        # 1. Check Kill Switch
        if self.is_locked:
            print(f"[RISK BLOCK] Engine is locked due to daily loss.")
            return False

        # 2. Check Position Limit
        if self.open_positions >= self.max_positions:
            print(f"[RISK BLOCK] Max positions ({self.max_positions}) reached.")
            return False

        return True

    def record_trade_open(self):
        self.open_positions += 1

    def record_trade_close(self):
        self.open_positions = max(0, self.open_positions - 1)
