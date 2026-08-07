from __future__ import annotations

from datetime import datetime
from statistics import median
from typing import Optional

from .config import SystemConfig
from .mapping import round_to_tick
from .models import (
    CalibrationStatus,
    CandidateInputs,
    ContractQualityBreakdown,
    Moneyness,
    OpportunityEvaluation,
    OpportunityGrade,
    PaperFill,
    Quote,
    TradeDecision,
)
from .risk import DynamicRiskCalculator, RiskContext


def clamp(value: float, low: float = 0.0, high: float = 100.0) -> float:
    return max(low, min(high, value))


def linear_score(value: float, ideal: float, acceptable: float, reject: float) -> float:
    if value <= ideal:
        return 100.0
    if value <= acceptable:
        return 70.0 + 30.0 * (acceptable - value) / (acceptable - ideal)
    if value <= reject:
        return 70.0 * (reject - value) / (reject - acceptable)
    return 0.0


class ContractQualityCalculator:
    def __init__(self, config: SystemConfig):
        self.config = config

    def calculate(self, candidate: CandidateInputs, quote_stale: bool = False, expiry_day: bool = False, aplus_context: bool = False, theta_ratio: Optional[float] = None) -> ContractQualityBreakdown:
        quote = candidate.quote
        if not quote.is_valid():
            return ContractQualityBreakdown(0, False, 0, 0, 0, 0, 0, 0, "Invalid bid/ask.")
        mid = quote.mid
        spread = quote.spread
        spread_pct = spread / mid * 100.0 if mid > 0 else 999.0
        liq_cfg = self.config.section("liquidity")
        if candidate.moneyness == Moneyness.ATM:
            ideal = float(liq_cfg["atm_spread_ideal_pct"]); acceptable = float(liq_cfg["atm_spread_acceptable_pct"]); reject = float(liq_cfg["atm_spread_reject_pct"])
        elif candidate.moneyness == Moneyness.ITM:
            ideal = float(liq_cfg["itm_spread_ideal_pct"]); acceptable = float(liq_cfg["itm_spread_acceptable_pct"]); reject = float(liq_cfg["itm_spread_reject_pct"])
        else:
            ideal = float(liq_cfg["otm_spread_ideal_pct"]); acceptable = float(liq_cfg["otm_spread_acceptable_pct"]); reject = float(liq_cfg["otm_spread_reject_pct"])
        if quote_stale or spread_pct > reject or spread > float(liq_cfg["absolute_spread_cap_points"]):
            return ContractQualityBreakdown(0, False, 0, 0, 0, 0, 0, 0, "Quote stale or spread hard rejected.")
        spread_score = linear_score(spread_pct, ideal, acceptable, reject)
        lot = candidate.instrument.lot_size
        min_top = min(quote.bid_qty / lot, quote.ask_qty / lot)
        if min_top >= 5:
            top_score = 100.0
        elif min_top >= 2:
            top_score = 70.0 + 30.0 * (min_top - 2.0) / 3.0
        elif min_top >= 1:
            top_score = 40.0 + 30.0 * (min_top - 1.0)
        else:
            top_score = 0.0
        if quote.cumulative_bid_qty_5depth is not None and quote.cumulative_ask_qty_5depth is not None:
            min_depth = min(quote.cumulative_bid_qty_5depth / lot, quote.cumulative_ask_qty_5depth / lot)
            if min_depth >= 25:
                depth_score = 100.0
            elif min_depth >= 10:
                depth_score = 70.0 + 30.0 * (min_depth - 10.0) / 15.0
            elif min_depth >= 5:
                depth_score = 40.0 + 30.0 * (min_depth - 5.0) / 5.0
            else:
                depth_score = 0.0
        else:
            depth_score = top_score * 0.8
        liquidity_score = 0.6 * top_score + 0.4 * depth_score
        abs_delta = abs(candidate.greeks.delta) if candidate.greeks.delta is not None else self._delta_proxy(candidate.moneyness)
        delta_score = self._delta_score(abs_delta)
        gamma_score = self._gamma_score(abs_delta, expiry_day, aplus_context)
        theta_score = self._theta_score(theta_ratio)
        iv_score = self._iv_score(candidate.iv_crush_risk_score)
        total = 0.25 * liquidity_score + 0.20 * spread_score + 0.20 * delta_score + 0.15 * gamma_score + 0.10 * theta_score + 0.10 * iv_score
        return ContractQualityBreakdown(clamp(total), total >= 60.0, clamp(liquidity_score), clamp(spread_score), clamp(delta_score), clamp(gamma_score), clamp(theta_score), clamp(iv_score), "")

    @staticmethod
    def _delta_proxy(m: Moneyness) -> float:
        return {Moneyness.ATM: 0.5, Moneyness.ITM: 0.7, Moneyness.OTM: 0.3}[m]

    @staticmethod
    def _delta_score(abs_delta: float) -> float:
        if 0.45 <= abs_delta <= 0.65:
            return 100.0
        if 0.35 <= abs_delta < 0.45 or 0.65 < abs_delta <= 0.75:
            return 80.0
        if 0.25 <= abs_delta < 0.35 or 0.75 < abs_delta <= 0.85:
            return 60.0
        if 0.15 <= abs_delta < 0.25:
            return 30.0
        if abs_delta < 0.15:
            return 0.0
        return 70.0

    @staticmethod
    def _gamma_score(abs_delta: float, expiry_day: bool, aplus_context: bool) -> float:
        if 0.40 <= abs_delta <= 0.60:
            base = 100.0
        elif 0.30 <= abs_delta <= 0.70:
            base = 80.0
        elif 0.20 <= abs_delta <= 0.80:
            base = 50.0
        else:
            base = 20.0
        if expiry_day and not aplus_context:
            return min(base, 40.0)
        return base

    @staticmethod
    def _theta_score(theta_ratio: Optional[float]) -> float:
        if theta_ratio is None:
            return 50.0
        if theta_ratio >= 3.0:
            return 100.0
        if theta_ratio >= 2.0:
            return 80.0
        if theta_ratio >= 1.5:
            return 50.0
        if theta_ratio >= 1.0:
            return 20.0
        return 0.0

    @staticmethod
    def _iv_score(iv_crush: float) -> float:
        if iv_crush <= 30:
            return 100.0
        if iv_crush <= 50:
            return 80.0
        if iv_crush <= 70:
            return 50.0
        if iv_crush <= 85:
            return 20.0
        return 0.0


class PaperFillSimulator:
    def __init__(self, config: SystemConfig):
        self.config = config

    def entry_buy(self, quote: Quote, tick_size: float, slippage_baseline: float = 0.0) -> PaperFill:
        if not quote.is_valid():
            return PaperFill(False, None, None, 0.0, "Invalid quote.")
        spread = quote.spread
        buffer = max(tick_size, 0.10 * spread, slippage_baseline)
        limit = min(quote.ask + tick_size, quote.mid + 0.60 * spread)
        limit = round_to_tick(limit, tick_size)
        fill = quote.ask + buffer
        fill = round_to_tick(fill, tick_size)
        if fill <= limit + 1e-9:
            return PaperFill(True, fill, limit, buffer, "")
        return PaperFill(False, None, limit, buffer, "No fill under conservative paper-fill model.")

    def exit_sell(self, quote: Quote, tick_size: float, slippage_baseline: float = 0.0) -> PaperFill:
        if not quote.is_valid():
            return PaperFill(False, None, None, 0.0, "Invalid quote.")
        spread = quote.spread
        buffer = max(tick_size, 0.10 * spread, slippage_baseline)
        fill = round_to_tick(quote.bid - buffer, tick_size)
        if fill > 0:
            return PaperFill(True, fill, None, buffer, "")
        return PaperFill(False, None, None, buffer, "Exit fill not positive after slippage buffer.")


class OpportunityScorer:
    def __init__(self, config: SystemConfig):
        self.config = config
        self.contract_quality = ContractQualityCalculator(config)
        self.risk = DynamicRiskCalculator(config)

    def evaluate(self, candidate: CandidateInputs, realized_loss_today: float = 0.0) -> OpportunityEvaluation:
        reasons: list[str] = []
        cq = self.contract_quality.calculate(candidate, quote_stale=not candidate.data_health.valid)
        setup_grade_hint = "A+" if candidate.opportunity_confidence_score >= 80 and candidate.convexity_edge_score >= 90 else "A"
        risk_plan = self.risk.plan(RiskContext(
            capital=float(self.config.section("capital")["starting_capital"]),
            mode="NORMAL",
            setup_grade=setup_grade_hint,
            lots=1,
            entry_premium=candidate.quote.mid,
            lot_size=candidate.instrument.lot_size,
            spread_points=candidate.quote.spread,
            tick_size=candidate.instrument.tick_size,
            required_stop_points=max(candidate.required_stop_points, 0.0),
            instrument=candidate.instrument.underlying,
            realized_loss_today=realized_loss_today,
        ))
        if not candidate.data_health.valid:
            reasons.append("DataHealth invalid")
        if not cq.valid:
            reasons.append(f"Contract invalid: {cq.reason}")
        if not risk_plan.hard_stop_fit:
            reasons.append(f"HardStopFit false: {risk_plan.reason}")
        if candidate.iv_crush_risk_score > float(self.config.section("iv_crush")["hard_veto_above"]):
            reasons.append("IVCrush hard veto")
        if candidate.premium_elasticity < float(self.config.section("premium_elasticity")["reject_or_exit_threshold"]):
            reasons.append("PremiumElasticity hard reject")
        if candidate.expected_required_ratio < float(self.config.section("expected_move")["hard_reject_ratio"]):
            reasons.append("Expected/Required hard reject")
        if candidate.market_hostility_score > float(self.config.section("scores")["market_hostility_survival_max"]):
            reasons.append("MarketHostility hard reject")
        raw = self._raw_opportunity_score(candidate, cq.score)
        comparable = raw - self._calibration_penalty(candidate)
        threshold = self._dynamic_threshold(candidate)
        grade = self._grade(comparable, threshold, reasons)
        eligible = not reasons and grade in {OpportunityGrade.A, OpportunityGrade.A_PLUS}
        decision = self._decision(candidate, eligible, reasons)
        return OpportunityEvaluation(candidate, cq, risk_plan, raw, comparable, threshold, grade, eligible, decision, tuple(reasons))

    def _raw_opportunity_score(self, c: CandidateInputs, contract_quality_score: float) -> float:
        weights = self.config.section("opportunity_selection").get("final_opportunity_score_weights", {})
        score = (
            float(weights.get("trade_quality_score", 0.25)) * c.trade_quality_score +
            float(weights.get("convexity_quality_score", 0.20)) * c.convexity_edge_score +
            float(weights.get("direction_score", 0.15)) * abs(c.instrument_direction_score) +
            float(weights.get("execution_quality_score", 0.15)) * c.execution_quality_score +
            float(weights.get("regime_fit_score", 0.10)) * c.regime_fit_score +
            float(weights.get("opportunity_confidence_score", 0.10)) * c.opportunity_confidence_score +
            float(weights.get("contract_quality_score", 0.05)) * contract_quality_score
        )
        score -= max(0.0, c.market_hostility_score - 20.0) * 0.25
        return clamp(score)

    def _calibration_penalty(self, c: CandidateInputs) -> float:
        penalty = 0.0
        if c.calibration_status_direction == CalibrationStatus.UNVALIDATED:
            penalty += 10.0
        if c.calibration_status_liquidity == CalibrationStatus.UNVALIDATED:
            penalty += 15.0 if c.instrument.underlying == "MIDCPNIFTY" else 10.0
        if c.calibration_status_direction == CalibrationStatus.RETIRED or c.calibration_status_liquidity == CalibrationStatus.RETIRED:
            penalty += 100.0
        return penalty

    def _dynamic_threshold(self, c: CandidateInputs) -> float:
        base = float(self.config.section("opportunity_selection")["excellent_opportunity_min_score"])
        if c.instrument.underlying == "MIDCPNIFTY" and c.calibration_status_liquidity != CalibrationStatus.VALIDATED:
            base += 10.0
        if c.iv_crush_risk_score >= 50.0:
            base += 5.0
        return base

    @staticmethod
    def _grade(score: float, threshold: float, reasons: list[str]) -> OpportunityGrade:
        if reasons:
            return OpportunityGrade.REJECT
        if score >= max(90.0, threshold + 10.0):
            return OpportunityGrade.A_PLUS
        if score >= threshold:
            return OpportunityGrade.A
        if score >= 70.0:
            return OpportunityGrade.B
        if score >= 60.0:
            return OpportunityGrade.C
        return OpportunityGrade.REJECT

    @staticmethod
    def _decision(c: CandidateInputs, eligible: bool, reasons: list[str]) -> TradeDecision:
        if reasons:
            if any("DataHealth" in r for r in reasons):
                return TradeDecision.DATA_INVALID
            if any("Contract" in r for r in reasons):
                return TradeDecision.CONTRACT_INVALID
            return TradeDecision.NO_TRADE
        if not eligible:
            return TradeDecision.NO_EXCELLENT_CANDIDATE
        return TradeDecision.BUY_CALL_CANDIDATE if c.side.value == "CE" else TradeDecision.BUY_PUT_CANDIDATE


class CandidateRevalidator:
    def __init__(self, config: SystemConfig):
        self.config = config

    def revalidate(self, evaluation: OpportunityEvaluation, current_quote: Quote, now: datetime, ranking_spread: float, fast_market: bool = False) -> tuple[bool, tuple[str, ...]]:
        reasons: list[str] = []
        candidate = evaluation.candidate
        created = candidate.candidate_created_at or candidate.quote.timestamp
        max_age = float(self.config.section("candidate_revalidation")["fast_market_max_candidate_age_sec" if fast_market else "normal_market_max_candidate_age_sec"])
        age = (now - created).total_seconds()
        if age > max_age:
            reasons.append("Candidate age exceeds revalidation limit")
        if not current_quote.is_valid():
            reasons.append("Current quote invalid")
        max_spread_multiple = float(self.config.section("candidate_revalidation")["max_spread_expansion_from_ranking_multiple"])
        if ranking_spread > 0 and current_quote.spread > max_spread_multiple * ranking_spread:
            reasons.append("Spread expanded beyond revalidation limit")
        if evaluation.comparable_opportunity_score < evaluation.dynamic_excellent_threshold:
            reasons.append("Opportunity score below dynamic threshold")
        if not evaluation.risk_plan.hard_stop_fit:
            reasons.append("Hard stop no longer fits")
        return (not reasons, tuple(reasons))
