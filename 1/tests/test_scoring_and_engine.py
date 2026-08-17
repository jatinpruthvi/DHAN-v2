import unittest
from dataclasses import replace
from datetime import datetime, date

from institutional_options.config import SystemConfig
from institutional_options.engine import PaperOpportunityEngine, PaperPortfolioState
from institutional_options.models import CandidateInputs, CalibrationStatus, DataHealth, Greeks, InstrumentSpec, Moneyness, OptionType, Quote, TradeDecision
from institutional_options.scoring import CandidateRevalidator, ContractQualityCalculator, OpportunityScorer, PaperFillSimulator
from institutional_options.risk import DynamicRiskCalculator, RiskContext
from institutional_options.orchestrators import DataHealthOrchestrator


def candidate(underlying="NIFTY", score=90, opt=OptionType.CE):
    now = datetime(2026, 6, 1, 10, 0, 0)
    return CandidateInputs(
        instrument=InstrumentSpec(underlying, "1", "OPTIDX", date(2026, 6, 30), 75, 0.05, 25000, opt),
        quote=Quote(100, 100.5, 1000, 1000, 100.25, now, 5000, 5000),
        moneyness=Moneyness.ATM,
        greeks=Greeks(delta=0.5, gamma=0.01, theta=-5, vega=2, iv=15),
        data_health=DataHealth(True),
        futures_price=25000,
        underlying_price=25000,
        instrument_direction_score=score if opt == OptionType.CE else -score,
        trade_quality_score=score,
        regime_confidence=80,
        market_hostility_score=10,
        iv_crush_risk_score=20,
        premium_elasticity=1.2,
        expected_move=200,
        required_move=100,
        required_stop_points=10,
        expected_value_r=0.5,
        vol_edge_ratio=2.0,
        convexity_edge_score=score,
        execution_quality_score=score,
        opportunity_confidence_score=score,
        regime_fit_score=score,
        candidate_created_at=now,
        calibration_status_direction=CalibrationStatus.VALIDATED,
        calibration_status_liquidity=CalibrationStatus.VALIDATED,
    )


class ScoringEngineTests(unittest.TestCase):
    def setUp(self):
        self.cfg = SystemConfig.from_file("uploads/PARAMETERS.json")

    def test_contract_quality_good(self):
        cq = ContractQualityCalculator(self.cfg).calculate(candidate())
        self.assertTrue(cq.valid)
        self.assertGreaterEqual(cq.score, 80)

    def test_paper_fill_uses_bid_ask(self):
        sim = PaperFillSimulator(self.cfg)
        fill = sim.entry_buy(candidate().quote, 0.05)
        self.assertTrue(fill.filled)
        self.assertGreaterEqual(fill.fill_price, 100.5)

    def test_contract_quality_minimum_is_enforced(self):
        scorer = OpportunityScorer(self.cfg)
        low_depth_quote = Quote(100.0, 100.5, 10, 10, 100.25,
                                datetime(2026, 6, 1, 10, 0, 0), 100, 100)
        evaluation = scorer.evaluate(replace(candidate(), quote=low_depth_quote))
        self.assertFalse(evaluation.eligible)
        self.assertTrue(any("ContractQuality below minimum" in r for r in evaluation.reasons))

    def test_declared_core_strategy_gates_are_enforced(self):
        scorer = OpportunityScorer(self.cfg)
        weak = replace(
            candidate(),
            instrument_direction_score=50.0,
            premium_elasticity=0.9,
            expected_move=120.0,
            required_move=100.0,
            market_hostility_score=40.0,
            trade_quality_score=60.0,
            regime_confidence=50.0,
            opportunity_confidence_score=60.0,
        )
        evaluation = scorer.evaluate(weak)
        self.assertFalse(evaluation.eligible)
        reasons = " | ".join(evaluation.reasons)
        for text in ("SideDirection", "PremiumElasticity", "Expected/Required",
                     "MarketHostility", "TradeQuality", "RegimeConfidence", "FinalConfidence"):
            self.assertIn(text, reasons)

    def test_wrong_side_candidate_is_rejected(self):
        scorer = OpportunityScorer(self.cfg)
        bearish_put = candidate(opt=OptionType.PE)
        evaluation = scorer.evaluate(bearish_put)
        self.assertFalse(evaluation.eligible)
        self.assertTrue(any("SideDirection hard reject" in r for r in evaluation.reasons))

    def test_candidate_revalidation_rejects_spread_expansion(self):
        scorer = OpportunityScorer(self.cfg)
        evaluation = scorer.evaluate(candidate())
        revalidator = CandidateRevalidator(self.cfg)
        widened = Quote(95.0, 97.0, 1000, 1000, 96.0, datetime(2026, 6, 1, 10, 0, 1), 5000, 5000)
        ok, reasons = revalidator.revalidate(
            evaluation, widened, datetime(2026, 6, 1, 10, 0, 1),
            ranking_spread=evaluation.candidate.quote.spread,
        )
        self.assertFalse(ok)
        self.assertIn("Spread expanded", " | ".join(reasons))

    def test_engine_selects_best_excellent(self):
        engine = PaperOpportunityEngine(self.cfg)
        weak = candidate("BANKNIFTY", 75)
        strong = candidate("NIFTY", 95)
        result = engine.evaluate_and_select([weak, strong])
        self.assertEqual(result.decision, TradeDecision.BUY_CALL_CANDIDATE)
        self.assertEqual(result.selected.candidate.instrument.underlying, "NIFTY")

    def test_unresolved_tie_is_explicit_no_trade(self):
        engine = PaperOpportunityEngine(self.cfg)
        result = engine.evaluate_and_select([candidate("NIFTY", 90), candidate("BANKNIFTY", 90)])
        self.assertEqual(result.decision, TradeDecision.NO_TRADE)
        self.assertTrue(any("ambiguous" in reason.lower() for reason in result.reasons))

    def test_global_position_lock_blocks(self):
        engine = PaperOpportunityEngine(self.cfg)
        result = engine.evaluate_and_select([candidate()], PaperPortfolioState(open_positions_count=1))
        self.assertEqual(result.decision, TradeDecision.GLOBAL_POSITION_LOCK_ACTIVE)

    def test_source_timestamp_is_required_for_strict_health(self):
        health = DataHealthOrchestrator(self.cfg)
        now = datetime(2026, 6, 1, 10, 0, 1)
        self.assertFalse(health.evaluate_candidate(candidate(), now).valid)
        timestamped = replace(candidate(), quote=Quote(100, 100.5, 1000, 1000, 100.25, now, 5000, 5000, True))
        self.assertTrue(health.evaluate_candidate(timestamped, now).valid)

    def test_excellent_gate_boundaries_are_hard_rejects(self):
        scorer = OpportunityScorer(self.cfg)
        for field, reason in (("execution_quality_score", "ExecutionQuality"),
                              ("convexity_edge_score", "ConvexityEdge"),
                              ("opportunity_confidence_score", "OpportunityConfidence"),
                              ("regime_fit_score", "RegimeFit")):
            boundary = 69.0 if field in {"opportunity_confidence_score", "regime_fit_score"} else 79.0
            evaluation = scorer.evaluate(replace(candidate(), **{field: boundary}))
            self.assertFalse(evaluation.eligible, field)
            self.assertTrue(any(reason in item for item in evaluation.reasons), (field, evaluation.reasons))

    def test_zero_quote_depth_is_data_invalid(self):
        health = DataHealthOrchestrator(self.cfg)
        stale_depth = replace(candidate(), quote=Quote(100, 100.5, 0, 1000, 100.25, datetime(2026, 6, 1, 10, 0)))
        result = health.evaluate_candidate(stale_depth, datetime(2026, 6, 1, 10, 0, 1))
        self.assertFalse(result.valid)
        self.assertIn("depth", result.reason.lower())

    def test_survival_daily_mode_blocks_new_risk(self):
        scorer = OpportunityScorer(self.cfg)
        scorer.set_runtime_mode("SURVIVAL")
        evaluation = scorer.evaluate(candidate())
        self.assertFalse(evaluation.risk_plan.hard_stop_fit)
        self.assertEqual(evaluation.risk_plan.max_allowed_risk, 0.0)

    def test_explicit_playbook_grade_is_used_for_risk_provenance(self):
        evaluation = OpportunityScorer(self.cfg).evaluate(replace(candidate(), setup_grade="A"))
        self.assertEqual(evaluation.candidate.notes["setup_grade_source"], "PLAYBOOK_METADATA")
        self.assertEqual(evaluation.candidate.notes["setup_grade_used"], "A")

    def test_missing_required_stop_is_fail_closed(self):
        evaluation = OpportunityScorer(self.cfg).evaluate(replace(candidate(), required_stop_points=0.0))
        self.assertFalse(evaluation.eligible)
        self.assertTrue(any("RequiredStop" in item for item in evaluation.reasons))

    def test_aplus_new_trade_cap_respects_normal_instrument_and_daily_caps(self):
        calc = DynamicRiskCalculator(self.cfg)
        plan = calc.plan(RiskContext(
            capital=100000, mode="NORMAL", setup_grade="A+", lots=1,
            entry_premium=80, lot_size=30, spread_points=1, tick_size=0.05,
            required_stop_points=10, instrument="BANKNIFTY", realized_loss_today=0,
        ))
        self.assertEqual(plan.max_allowed_risk, 750.0)


if __name__ == "__main__":
    unittest.main()
