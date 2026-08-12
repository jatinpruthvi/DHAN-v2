import json
import tempfile
import unittest
from datetime import date, datetime, timedelta
from pathlib import Path

from institutional_options.config import SystemConfig
from institutional_options.engine import PaperPortfolioState
from institutional_options.models import (
    CalibrationStatus, CandidateInputs, DataHealth, Greeks, InstrumentSpec,
    Moneyness, OptionType, PaperFill, PaperTrade, Quote,
)
from institutional_options.paper_evidence import (
    AppendingCsv, PaperEvidenceCollector, build_evidence_report,
)
from institutional_options.paper_runner import now_ist
from institutional_options.paper_signal import PaperSignalCalculator
from institutional_options.scoring import OpportunityScorer

CFG_PATH = "uploads/PARAMETERS.json"


def make_quote(mid=100.0, spread=0.6):
    return Quote(bid=mid - spread / 2, ask=mid + spread / 2, bid_qty=50, ask_qty=50,
                 last=mid, timestamp=now_ist())


def make_candidate(score_components, underlying="NIFTY", side=OptionType.CE,
                   strike=25000.0, premium=100.0):
    spec = InstrumentSpec(underlying, "sec1", "OPTIDX", date(2026, 8, 25), 65, 0.05, strike, side)
    return CandidateInputs(
        instrument=spec,
        quote=make_quote(premium),
        moneyness=Moneyness.ATM,
        greeks=Greeks(),
        data_health=DataHealth(True),
        futures_price=25000.0,
        underlying_price=25000.0,
        instrument_direction_score=score_components["direction"],
        trade_quality_score=score_components["trade_quality"],
        regime_confidence=score_components["regime_confidence"],
        market_hostility_score=score_components["hostility"],
        iv_crush_risk_score=20.0,
        premium_elasticity=score_components["elasticity"],
        expected_move=score_components["expected_move"],
        required_move=score_components["required_move"],
        expected_value_r=score_components["ev_r"],
        vol_edge_ratio=score_components["vol_edge"],
        convexity_edge_score=score_components["convexity"],
        execution_quality_score=score_components["execution"],
        opportunity_confidence_score=score_components["confidence"],
        regime_fit_score=score_components["regime_fit"],
        candidate_created_at=now_ist(),
        calibration_status_direction=CalibrationStatus.VALIDATED,
        calibration_status_liquidity=CalibrationStatus.VALIDATED,
    )


class EvidenceCsvTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.dir = Path(self.tmp.name)

    def tearDown(self):
        self.tmp.cleanup()

    def test_appending_csv_creates_header_and_appends(self):
        p = self.dir / "t.csv"
        csv = AppendingCsv(p)
        csv.append({"a": 1, "b": "x"})
        csv.append({"a": 2, "b": "y"})
        text = p.read_text(encoding="utf-8")
        self.assertIn("a,b", text)
        self.assertEqual(text.count("a,"), 1)  # header only once

    def test_skipped_rows_written_and_ranked(self):
        cfg = SystemConfig.from_file(CFG_PATH)
        scorer = OpportunityScorer(cfg)
        strong = make_candidate({"direction": 90, "trade_quality": 90, "regime_confidence": 80,
                                 "hostility": 10, "elasticity": 0.8, "expected_move": 200,
                                 "required_move": 100, "ratio": 2.0, "ev_r": 1.0, "vol_edge": 2.0,
                                 "convexity": 90, "execution": 90, "confidence": 90, "regime_fit": 90})
        weak = make_candidate({"direction": 10, "trade_quality": 10, "regime_confidence": 40,
                               "hostility": 60, "elasticity": 0.2, "expected_move": 50,
                               "required_move": 200, "ratio": 0.25, "ev_r": -0.5, "vol_edge": 0.5,
                               "convexity": 10, "execution": 10, "confidence": 10, "regime_fit": 10},
                              underlying="BANKNIFTY", side=OptionType.PE, strike=57800.0)
        evals = tuple(scorer.evaluate(c) for c in (strong, weak))
        col = PaperEvidenceCollector(self.dir)
        col.record_skipped(evals, ranking_cycle_id="202608121500")
        rows = list(self._csv_rows(self.dir / "skipped.csv"))
        self.assertEqual(len(rows), 2)
        self.assertEqual(rows[0]["rank"], "1")   # highest score first
        self.assertGreater(float(rows[0]["OpportunityScore"]), float(rows[1]["OpportunityScore"]))
        self.assertIn("skip_id", rows[0])

    def test_report_builds_from_empty_state(self):
        text = build_evidence_report(self.dir, SystemConfig.from_file(CFG_PATH))
        self.assertIn("PAPER-EVIDENCE REPORT", text)
        self.assertIn("no data yet", text)

    def _csv_rows(self, path):
        import csv
        with Path(path).open("r", encoding="utf-8-sig", newline="") as f:
            yield from csv.DictReader(f)


class ReportWithTradeTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.dir = Path(self.tmp.name)
        self.cfg = SystemConfig.from_file(CFG_PATH)

    def tearDown(self):
        self.tmp.cleanup()

    def test_report_renders_calibration_buckets_after_a_trade(self):
        col = PaperEvidenceCollector(self.dir)
        # A winning, high-score trade (score ~90+) and a losing lower-score trade.
        strong = make_candidate({"direction": 90, "trade_quality": 90, "regime_confidence": 80,
                                 "hostility": 10, "elasticity": 0.8, "expected_move": 200,
                                 "required_move": 100, "ratio": 2.0, "ev_r": 1.0, "vol_edge": 2.0,
                                 "convexity": 90, "execution": 90, "confidence": 90, "regime_fit": 90})
        weak = make_candidate({"direction": 20, "trade_quality": 20, "regime_confidence": 40,
                               "hostility": 40, "elasticity": 0.4, "expected_move": 80,
                               "required_move": 160, "ratio": 0.5, "ev_r": 0.1, "vol_edge": 0.9,
                               "convexity": 30, "execution": 50, "confidence": 40, "regime_fit": 40},
                              underlying="BANKNIFTY", side=OptionType.PE, strike=57800.0)
        scorer = OpportunityScorer(self.cfg)
        for cand, pnl, r in ((strong, 3000.0, 2.0), (weak, -800.0, -1.5)):
            ev = scorer.evaluate(cand)
            fill = PaperFill(True, cand.quote.mid, None, 0.0, "")
            trade = PaperTrade(trade_id=f"t-{pnl}", entry_evaluation=ev, entry_fill=fill,
                               entry_time=now_ist(), exit_fill=fill,
                               exit_time=now_ist() + timedelta(minutes=5),
                               exit_reason="TARGET_HIT" if pnl > 0 else "STOP_HIT")
            col.record_trade(trade, net_pnl_rupees=pnl, r_multiple=r)
        text = build_evidence_report(self.dir, self.cfg)
        self.assertIn("EVIDENCE REVIEW", text)
        self.assertIn("trades=2", text)
        self.assertIn("CALIBRATION", text)
        self.assertIn("80-89", text)        # score bucket rendered
        self.assertIn("win%", text)


class DailyRunTests(unittest.TestCase):
    def test_daily_run_archives_and_flags_tests(self):
        from institutional_options.daily_evidence_run import (
            PROJECT_ROOT, archive_report, generate_report,
        )
        with tempfile.TemporaryDirectory() as tmp:
            state = Path(tmp)
            # A tiny mtil row so the report has evidence to render.
            (state / "mtil.csv").write_text(
                "trade_id,date,instrument,option_type,OpportunityScore,net_pnl_rupees,r_multiple\n"
                "t1,2026-08-12,NIFTY,CE,85,1500,1.5\n", encoding="utf-8")
            out = generate_report(state, emergency_passed=True)
            self.assertTrue(out.exists())
            text = out.read_text(encoding="utf-8")
            self.assertIn("PAPER-EVIDENCE REPORT", text)
            self.assertIn("[PASS] emergency_tests_passed", text)
            archived = archive_report(state, out)
            self.assertTrue(archived.exists())
            self.assertIn("evidence_report_", archived.name)
            # project root must be the repo folder holding pyproject.toml
            self.assertTrue((PROJECT_ROOT / "pyproject.toml").exists())

    def test_daily_run_runs_test_suite(self):
        import subprocess, sys
        from institutional_options.daily_evidence_run import run_tests
        # Simulated: patch subprocess.run to avoid actually running the suite here.
        import institutional_options.daily_evidence_run as m
        real = m.subprocess.run
        try:
            m.subprocess.run = lambda *a, **k: type("R", (), {"returncode": 0, "stdout": "ok", "stderr": ""})()
            self.assertTrue(run_tests())
            m.subprocess.run = lambda *a, **k: type("R", (), {"returncode": 1, "stdout": "fail", "stderr": ""})()
            self.assertFalse(run_tests())
        finally:
            m.subprocess.run = real


if __name__ == "__main__":
    unittest.main()
