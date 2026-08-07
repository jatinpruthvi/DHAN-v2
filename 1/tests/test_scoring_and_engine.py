import unittest
from datetime import datetime, date

from institutional_options.config import SystemConfig
from institutional_options.engine import PaperOpportunityEngine, PaperPortfolioState
from institutional_options.models import CandidateInputs, CalibrationStatus, DataHealth, Greeks, InstrumentSpec, Moneyness, OptionType, Quote, TradeDecision
from institutional_options.scoring import ContractQualityCalculator, PaperFillSimulator


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

    def test_engine_selects_best_excellent(self):
        engine = PaperOpportunityEngine(self.cfg)
        weak = candidate("BANKNIFTY", 75)
        strong = candidate("NIFTY", 95)
        result = engine.evaluate_and_select([weak, strong])
        self.assertEqual(result.decision, TradeDecision.BUY_CALL_CANDIDATE)
        self.assertEqual(result.selected.candidate.instrument.underlying, "NIFTY")

    def test_global_position_lock_blocks(self):
        engine = PaperOpportunityEngine(self.cfg)
        result = engine.evaluate_and_select([candidate()], PaperPortfolioState(open_positions_count=1))
        self.assertEqual(result.decision, TradeDecision.GLOBAL_POSITION_LOCK_ACTIVE)


if __name__ == "__main__":
    unittest.main()
