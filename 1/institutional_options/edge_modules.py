from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

from .scoring import clamp


@dataclass(frozen=True)
class EdgeInputs:
    premium_elasticity_score: float = 0.0
    gamma_usefulness_score: float = 0.0
    expected_acceleration_score: float = 0.0
    iv_support_score: float = 0.0
    time_to_profit_quality_score: float = 0.0
    expected_value_r: float = 0.0
    vol_edge_ratio: float = 0.0
    forced_flow_score: float = 0.0
    liquidity_vacuum_score: float = 0.0
    range_expansion_quality: float = 0.0
    directional_option_breadth_score: float = 0.0
    trend_exhaustion_risk: float = 0.0
    late_entry_risk: float = 0.0
    trade_location_efficiency: float = 0.0
    reward_path_score: float = 0.0
    time_to_profit_probability: float = 0.0


class AdvancedEdgeCalculator:
    """Institutional edge calculators used as filters/ranking quality layers.

    Inputs are precomputed primitive scores. Missing values should be provided as 0
    or handled upstream as UNAVAILABLE/UNVALIDATED with penalties.
    """

    @staticmethod
    def convexity_edge_score(i: EdgeInputs) -> float:
        return clamp(0.30*i.premium_elasticity_score + 0.25*i.gamma_usefulness_score + 0.20*i.expected_acceleration_score + 0.15*i.iv_support_score + 0.10*i.time_to_profit_quality_score)

    @staticmethod
    def final_edge_approval(i: EdgeInputs, breakout_trade: bool = False, opportunity_half_life_expired: bool = False) -> tuple[bool, tuple[str, ...]]:
        reasons: list[str] = []
        if i.expected_value_r < 0.30:
            reasons.append("ExpectedValue_R below 0.30R")
        if i.vol_edge_ratio < 1.60:
            reasons.append("VolEdgeRatio below 1.60")
        if AdvancedEdgeCalculator.convexity_edge_score(i) < 80:
            reasons.append("ConvexityEdgeScore below 80")
        if i.time_to_profit_probability < 70:
            reasons.append("TimeToProfitProbability below 70")
        if i.trade_location_efficiency < 75:
            reasons.append("TradeLocationEfficiency below 75")
        if i.reward_path_score < 75:
            reasons.append("RewardPathScore below 75")
        if i.trend_exhaustion_risk > 70:
            reasons.append("TrendExhaustionRisk above 70")
        if i.late_entry_risk > 70:
            reasons.append("LateEntryRisk above 70")
        if opportunity_half_life_expired:
            reasons.append("Opportunity half-life expired")
        if breakout_trade:
            if i.forced_flow_score < 70:
                reasons.append("ForcedFlowScore below 70 for breakout")
            if i.range_expansion_quality < 75:
                reasons.append("RangeExpansionQuality below 75 for breakout")
            if i.liquidity_vacuum_score < 70:
                reasons.append("LiquidityVacuumScore below 70 for breakout")
            if i.directional_option_breadth_score < 70:
                reasons.append("DirectionalOptionBreadthScore below 70 for breakout")
        return (not reasons, tuple(reasons))
