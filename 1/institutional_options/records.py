from __future__ import annotations

from datetime import datetime, UTC
from typing import Any, Mapping

from .models import OpportunityEvaluation, PaperTrade
from .mtil import MTILSchema


class MTILRecordBuilder:
    STRING_SENTINEL = "UNAVAILABLE"

    @staticmethod
    def apply_schema_defaults(record: Mapping[str, Any], schema: MTILSchema) -> dict[str, Any]:
        completed = dict(record)
        for field in schema.fields:
            if completed.get(field.field) not in (None, ""):
                continue
            if field.type in {"float", "int"}:
                completed[field.field] = 0
            elif field.type == "bool":
                completed[field.field] = False
            elif field.type == "date":
                completed[field.field] = completed.get("date", "1970-01-01") or "1970-01-01"
            elif field.type == "datetime":
                completed[field.field] = completed.get("entry_time", "1970-01-01T00:00:00") or "1970-01-01T00:00:00"
            else:
                completed[field.field] = MTILRecordBuilder.STRING_SENTINEL
        return completed

    @staticmethod
    def from_paper_trade(trade: PaperTrade, net_pnl_rupees: float = 0.0, r_multiple: float = 0.0, schema: MTILSchema | None = None) -> dict[str, Any]:
        e: OpportunityEvaluation = trade.entry_evaluation
        c = e.candidate
        return {
            "trade_id": trade.trade_id,
            "ranking_cycle_id": trade.trade_id,
            "candidate_id": trade.trade_id,
            "date": trade.entry_time.date().isoformat(),
            "day_of_week": trade.entry_time.strftime("%A"),
            "month": trade.entry_time.month,
            "quarter": (trade.entry_time.month - 1)//3 + 1,
            "instrument": c.instrument.underlying,
            "option_type": c.side.value,
            "strike": c.instrument.strike,
            "expiry_date": c.instrument.expiry.isoformat(),
            "dte": 0,
            "expiry_week_flag": False,
            "entry_time": trade.entry_time.isoformat(),
            "exit_time": trade.exit_time.isoformat() if trade.exit_time else "",
            "trade_duration_seconds": int((trade.exit_time - trade.entry_time).total_seconds()) if trade.exit_time else "",
            "session_bucket": "",
            "trade_mode": "PAPER",
            "execution_mode": "simulated",
            "session_bucket": "UNVALIDATED",
            "entry_underlying_spot": c.underlying_price,
            "entry_futures_price": c.futures_price,
            "entry_underlying_vwap": 0,
            "entry_distance_from_vwap_pct": 0,
            "entry_day_high": 0,
            "entry_day_low": 0,
            "entry_option_bid": c.quote.bid,
            "entry_option_ask": c.quote.ask,
            "entry_option_mid": c.quote.mid,
            "entry_fill_price": trade.entry_fill.fill_price or "",
            "entry_order_type": "PAPER_MARKETABLE_LIMIT",
            "paper_fill_model": "bid_ask_conservative",
            "simulated_entry_fill": trade.entry_fill.fill_price or "",
            "entry_candidate_age_seconds": 0,
            "candidate_age_seconds": 0,
            "entry_revalidation_passed": True,
            "revalidation_passed": True,
            "mapping_validation_passed": True,
            "lot_size_validation_passed": True,
            "tick_size_validation_passed": True,
            "exit_fill_price": trade.exit_fill.fill_price if trade.exit_fill else "",
            "simulated_exit_fill": trade.exit_fill.fill_price if trade.exit_fill else "",
            "exit_reason_primary": trade.exit_reason or "UNAVAILABLE",
            "gross_pnl_rupees": net_pnl_rupees,
            "total_costs_rupees": 0,
            "net_pnl_rupees": net_pnl_rupees,
            "r_multiple": r_multiple,
            "planned_risk_rupees": e.risk_plan.planned_risk,
            "max_allowed_risk_rupees": e.risk_plan.max_allowed_risk,
            "OpportunityScore": e.opportunity_score,
            "ComparableOpportunityScore": e.comparable_opportunity_score,
            "DynamicExcellentThreshold": e.dynamic_excellent_threshold,
            "OpportunityGrade": e.grade.value,
            "OpportunityConfidenceScore": c.opportunity_confidence_score,
            "DirectionScore": c.instrument_direction_score,
            "TradeQualityScore": c.trade_quality_score,
            "ContractQualityScore": e.contract_quality.score,
            "ConvexityEdgeScore": c.convexity_edge_score,
            "ExecutionQualityScore": c.execution_quality_score,
            "RegimeFitScore": c.regime_fit_score,
            "MarketHostilityScore": c.market_hostility_score,
            "ExpectedValue_R": c.expected_value_r,
            "VolEdgeRatio": c.vol_edge_ratio,
            "primary_regime": "UNVALIDATED",
            "regime_confidence": c.regime_confidence,
            "trend_direction": "UNVALIDATED",
            "trend_strength_score": 0,
            "volatility_regime": "UNVALIDATED",
            "liquidity_regime": "UNVALIDATED",
            "risk_on_off_state": "UNVALIDATED",
            "raw_elasticity": c.premium_elasticity,
            "delta_adjusted_elasticity": c.premium_elasticity,
            "elasticity_trend": "UNVALIDATED",
            "elasticity_strength": "UNVALIDATED",
            "entry_spread_points": c.quote.spread,
            "entry_spread_pct": c.quote.spread / c.quote.mid * 100 if c.quote.mid else "",
            "bid_qty_lots": c.quote.bid_qty / c.instrument.lot_size,
            "ask_qty_lots": c.quote.ask_qty / c.instrument.lot_size,
            "top_book_coverage": min(c.quote.bid_qty, c.quote.ask_qty) / c.instrument.lot_size,
            "liquidity_score": e.contract_quality.liquidity_score,
            "execution_quality_score": c.execution_quality_score,
            "event_risk_state": "UNVALIDATED",
            "rbi_day_flag": False,
            "drawdown_strictness_state": "Normal",
            "initial_stop_points": e.risk_plan.hard_stop_points,
            "initial_stop_rupees": e.risk_plan.planned_risk,
            "minimum_viable_stop_points": e.risk_plan.minimum_viable_stop_points,
            "initial_target_points": c.required_move,
            "target_r_multiple": 0,
            "time_stop_seconds": 0,
            "manual_override_flag": False,
            "rule_violation_flag": False,
            "trade_archetype_code": "A00",
            "trade_archetype_name": "UNCLASSIFIED",
            "setup_category": c.setup_type,
            "signal_combination_id": f"{c.instrument.underlying}_{c.side.value}_{c.setup_type}",
            "regime_combination_id": "UNCLASSIFIED",
            "opportunity_cluster_id": f"{c.instrument.underlying}_{c.side.value}_{c.setup_type}",
            "archetype_live_status": "PAPER_ONLY",
            "gap_direction": "UNVALIDATED",
            "gap_pct": 0,
            "gap_points": 0,
            "gap_type": "UNVALIDATED",
            "gap_min_wait_required_min": 0,
            "gap_wait_completed": False,
            "atm_iv": c.greeks.iv or 0,
            "atr_value": 0,
            "atr_regime": "UNVALIDATED",
            "vwap_position": "UNVALIDATED",
            "vwap_slope": "UNVALIDATED",
            "orb_status": "UNVALIDATED",
            "opening_range_high": 0,
            "opening_range_low": 0,
            "entry_reason_code": "UNVALIDATED",
            "entry_reason_text": "UNVALIDATED",
        }
        if schema is not None:
            return MTILRecordBuilder.apply_schema_defaults(record, schema)
        return record


class SkippedCandidateRecordBuilder:
    @staticmethod
    def from_evaluation(e: OpportunityEvaluation, ranking_cycle_id: str, rank: int, why: str) -> dict[str, Any]:
        c = e.candidate
        return {
            "skip_id": f"{ranking_cycle_id}_{rank}_{c.instrument.underlying}_{c.side.value}",
            "timestamp": datetime.now(UTC).isoformat(),
            "ranking_cycle_id": ranking_cycle_id,
            "underlying": c.instrument.underlying,
            "option_type": c.side.value,
            "rank": rank,
            "OpportunityScore": e.comparable_opportunity_score,
            "DirectionScore": c.instrument_direction_score,
            "TradeQualityScore": c.trade_quality_score,
            "ContractQualityScore": e.contract_quality.score,
            "PremiumElasticity": c.premium_elasticity,
            "ExpectedMove": c.expected_move,
            "RequiredMove": c.required_move,
            "ExpectedRequiredRatio": c.expected_required_ratio,
            "IVCrushRiskScore": c.iv_crush_risk_score,
            "MarketHostilityScore": c.market_hostility_score,
            "RegimeConfidence": c.regime_confidence,
            "DataHealthStatus": "VALID" if c.data_health.valid else "INVALID",
            "hard_stop_fit": e.risk_plan.hard_stop_fit,
            "veto_reason": "; ".join(e.reasons),
            "why_not_traded": why,
            "calibration_status": f"direction={c.calibration_status_direction.value};liquidity={c.calibration_status_liquidity.value}",
        }
