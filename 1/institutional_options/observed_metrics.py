from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Hashable, Optional

from .models import OptionType, Quote


@dataclass(frozen=True)
class ElasticityObservation:
    """One rolling, bid/ask-aware observation of option response.

    The metric is deliberately diagnostic-only. It is not used to approve a
    trade until enough real observations exist for calibration by instrument,
    side, time bucket, and regime.
    """

    valid: bool
    raw_elasticity: Optional[float]
    post_cost_elasticity: Optional[float]
    underlying_move_points: float
    option_mid_move_points: float
    post_cost_option_move_points: float
    elapsed_seconds: float
    reason: str = ""


class RollingPremiumElasticity:
    """Compute observed option-premium response over a bounded rolling window."""

    def __init__(self, window_seconds: float = 60.0,
                 min_underlying_move_points: float = 30.0):
        self.window_seconds = max(1.0, float(window_seconds))
        self.min_underlying_move_points = max(0.0, float(min_underlying_move_points))
        self._last: dict[Hashable, tuple[datetime, float, Quote]] = {}

    def update(self, key: Hashable, timestamp: datetime, underlying_price: float,
               quote: Quote, side: OptionType) -> ElasticityObservation:
        previous = self._last.get(key)
        self._last[key] = (timestamp, float(underlying_price), quote)
        if previous is None:
            return ElasticityObservation(
                False, None, None, 0.0, 0.0, 0.0, 0.0,
                "No prior observation for contract.",
            )

        previous_ts, previous_underlying, previous_quote = previous
        elapsed = (timestamp - previous_ts).total_seconds()
        if elapsed <= 0:
            return ElasticityObservation(
                False, None, None, 0.0, 0.0, 0.0, elapsed,
                "Non-positive observation interval.",
            )
        if elapsed > self.window_seconds:
            return ElasticityObservation(
                False, None, None, 0.0, 0.0, 0.0, elapsed,
                "Observation interval exceeds rolling window.",
            )
        if not quote.is_valid() or not previous_quote.is_valid():
            return ElasticityObservation(
                False, None, None, 0.0, 0.0, 0.0, elapsed,
                "Current or previous quote invalid.",
            )

        underlying_move = float(underlying_price) - previous_underlying
        if abs(underlying_move) < self.min_underlying_move_points:
            return ElasticityObservation(
                False, None, None, underlying_move, 0.0, 0.0, elapsed,
                "Underlying move below identification minimum.",
            )

        option_mid_move = quote.mid - previous_quote.mid
        # A tradable long-option round trip starts at the previous ask and ends
        # at the current bid.  This removes the optimistic mid-to-mid effect.
        post_cost_option_move = quote.bid - previous_quote.ask
        side_sign = 1.0 if side is OptionType.CE else -1.0
        favorable_underlying_move = underlying_move * side_sign
        if favorable_underlying_move <= 0:
            return ElasticityObservation(
                False, None, None, underlying_move, option_mid_move,
                post_cost_option_move, elapsed,
                "Underlying move adverse to option side.",
            )
        raw = option_mid_move / favorable_underlying_move
        post_cost = post_cost_option_move / favorable_underlying_move
        return ElasticityObservation(
            True,
            raw,
            post_cost,
            underlying_move,
            option_mid_move,
            post_cost_option_move,
            elapsed,
            "",
        )
