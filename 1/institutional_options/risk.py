from __future__ import annotations

from dataclasses import dataclass

from .config import SystemConfig
from .models import RiskPlan


@dataclass(frozen=True)
class RiskContext:
    capital: float
    mode: str
    setup_grade: str
    lots: int
    entry_premium: float
    lot_size: int
    spread_points: float
    tick_size: float
    required_stop_points: float
    instrument: str
    realized_loss_today: float = 0.0


class DynamicRiskCalculator:
    def __init__(self, config: SystemConfig):
        self.config = config

    def max_allowed_risk(self, ctx: RiskContext) -> float:
        risk = self.config.section("risk")
        overrides = self.config.raw.get("instrument_risk_overrides", {})
        inst_override = overrides.get(ctx.instrument.upper(), {}) if isinstance(overrides, dict) else {}
        if ctx.mode.upper() in {"SURVIVAL", "NO_TRADE"}:
            return 0.0
        if ctx.mode.upper() == "DEFENSIVE":
            cap = float(inst_override.get("normal_risk_cap_rupees", risk["defensive_risk_max_rupees"]))
            return min(ctx.capital * float(risk["defensive_risk_max_pct_of_capital"]) / 100.0, float(risk["defensive_risk_max_rupees"]), cap)
        if ctx.setup_grade.upper() in {"A+", "A_PLUS"}:
            cap = float(inst_override.get("a_plus_risk_cap_rupees", risk["a_plus_risk_cap_rupees"]))
            return min(ctx.capital * float(risk["a_plus_risk_pct_of_capital"]) / 100.0, cap)
        cap = float(inst_override.get("normal_risk_cap_rupees", risk["normal_risk_cap_rupees"]))
        return min(ctx.capital * float(risk["normal_risk_pct_of_capital"]) / 100.0, cap)

    def plan(self, ctx: RiskContext) -> RiskPlan:
        max_allowed = self.max_allowed_risk(ctx)
        if max_allowed <= 0:
            return RiskPlan(max_allowed, 0.0, 0.0, False, 0.0, "Mode does not allow risk.")
        hard = self.config.section("hard_stop")
        is_aplus = ctx.setup_grade.upper() in {"A+", "A_PLUS"}
        point_cap = float(hard["a_plus_max_points"] if is_aplus else hard["normal_max_points"])
        pct = float(hard["a_plus_max_premium_pct"] if is_aplus else hard["normal_max_premium_pct"]) / 100.0
        premium_stop = ctx.entry_premium * pct
        risk_cap_stop = max_allowed / (ctx.lot_size * ctx.lots)
        hard_stop_points = min(point_cap, premium_stop, risk_cap_stop)
        planned_risk = hard_stop_points * ctx.lot_size * ctx.lots
        minimum_viable = max(2.0 * ctx.spread_points + 2.0 * ctx.tick_size, 0.0)
        required_stop_risk = ctx.required_stop_points * ctx.lot_size * ctx.lots
        if required_stop_risk > max_allowed:
            return RiskPlan(max_allowed, hard_stop_points, planned_risk, False, minimum_viable, "Required stop exceeds max allowed risk.")
        if hard_stop_points < minimum_viable:
            return RiskPlan(max_allowed, hard_stop_points, planned_risk, False, minimum_viable, "Hard stop is below minimum viable stop.")
        if planned_risk > max_allowed + 1e-9:
            return RiskPlan(max_allowed, hard_stop_points, planned_risk, False, minimum_viable, "Planned risk exceeds max allowed risk.")
        max_daily = float(self.config.section("risk")["max_daily_loss_rupees"])
        remaining = max_daily - max(0.0, ctx.realized_loss_today)
        if planned_risk > 0.8 * remaining:
            return RiskPlan(max_allowed, hard_stop_points, planned_risk, False, minimum_viable, "Planned risk exceeds remaining daily risk budget.")
        return RiskPlan(max_allowed, hard_stop_points, planned_risk, True, minimum_viable, "")
