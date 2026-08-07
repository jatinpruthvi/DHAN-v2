from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Iterable, Optional

from .models import PaperFill, PaperTrade, Quote
from .scoring import PaperFillSimulator


@dataclass(frozen=True)
class MarketBar:
    timestamp: datetime
    quote: Quote
    futures_price: float
    iv: Optional[float] = None


@dataclass(frozen=True)
class SimulatedTradeResult:
    trade: PaperTrade
    exit_reason: str
    highest_premium: float
    lowest_premium: float
    mfe_points: float
    mae_points: float
    gross_pnl_points: float
    gross_pnl_rupees: float


class SimulatedTradeLifecycle:
    def __init__(self, fill_simulator: PaperFillSimulator):
        self.fill_simulator = fill_simulator

    def run(
        self,
        trade: PaperTrade,
        bars: Iterable[MarketBar],
        target_points: float,
        stop_points: float,
        max_duration_seconds: int,
    ) -> SimulatedTradeResult:
        if not trade.entry_fill.filled or trade.entry_fill.fill_price is None:
            raise ValueError("Cannot simulate lifecycle for unfilled entry.")
        entry = trade.entry_fill.fill_price
        high = entry
        low = entry
        exit_fill: Optional[PaperFill] = None
        exit_time: Optional[datetime] = None
        exit_reason = "NO_EXIT_DATA"
        deadline = trade.entry_time + timedelta(seconds=max_duration_seconds)
        last_quote: Optional[Quote] = None
        last_time: Optional[datetime] = None
        tick = trade.entry_evaluation.candidate.instrument.tick_size
        for bar in bars:
            last_quote = bar.quote
            last_time = bar.timestamp
            premium = bar.quote.mid
            high = max(high, premium)
            low = min(low, premium)
            if premium >= entry + target_points:
                exit_fill = self.fill_simulator.exit_sell(bar.quote, tick)
                exit_time = bar.timestamp
                exit_reason = "TARGET_HIT"
                break
            if premium <= entry - stop_points:
                exit_fill = self.fill_simulator.exit_sell(bar.quote, tick)
                exit_time = bar.timestamp
                exit_reason = "STOP_HIT"
                break
            if bar.timestamp >= deadline:
                exit_fill = self.fill_simulator.exit_sell(bar.quote, tick)
                exit_time = bar.timestamp
                exit_reason = "TIME_STOP"
                break
        if exit_fill is None and last_quote is not None:
            exit_fill = self.fill_simulator.exit_sell(last_quote, tick)
            exit_time = last_time
            exit_reason = "END_OF_DATA_EXIT"
        if exit_fill is None or not exit_fill.filled or exit_fill.fill_price is None:
            exit_price = 0.0
        else:
            exit_price = exit_fill.fill_price
        lot = trade.entry_evaluation.candidate.instrument.lot_size
        gross_points = exit_price - entry
        return SimulatedTradeResult(
            PaperTrade(trade.trade_id, trade.entry_evaluation, trade.entry_fill, trade.entry_time, exit_fill, exit_time, exit_reason),
            exit_reason,
            high,
            low,
            high - entry,
            entry - low,
            gross_points,
            gross_points * lot,
        )
