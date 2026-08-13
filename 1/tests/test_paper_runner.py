import csv
import json
import tempfile
import time
import unittest
from datetime import date, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace
from unittest import mock

from institutional_options.config import SystemConfig
from institutional_options.fyers_client import FyersInstrument, FyersSymbolMaster
from institutional_options.fyers_parser import (
    FyersOptionChainParser, parse_expiry_calendar, parse_india_vix,
)
from institutional_options.paper_runner import PaperRunner
from institutional_options.paper_signal import PaperSignalCalculator
from institutional_options.option_chain import OptionChainSnapshot
from institutional_options.models import Moneyness, OptionType


CFG_PATH = "uploads/PARAMETERS.json"
RUNNER_CFG = {
    "poll_seconds": 1,
    "strikecount": 12,
    "underlyings": {"NIFTY": {"index_symbol": "NSE:NIFTY50-INDEX", "prefer_monthly": False}},
}


def make_chain_payload(spot=25000.0, prem=100.0, expiry_ts=1787652600):
    """Synthetic Fyers /data/options-chain-v3 payload around spot."""
    chain = [{
        "option_type": "", "strike_price": -1, "ltp": spot, "bid": 0, "ask": 0,
        "fyToken": "101000000026000", "symbol": "NSE:NIFTY50-INDEX", "description": "NIFTY50-INDEX",
    }]
    for offset in (-300, -200, -100, 0, 100, 200, 300):
        strike = spot + offset
        k = 1.0 + abs(offset) / 400.0
        for opt in ("CE", "PE"):
            bid = max(0.5, prem / k - 0.3)
            ask = bid + 0.6
            chain.append({
                "option_type": opt, "strike_price": strike, "ltp": (bid + ask) / 2,
                "bid": bid, "ask": ask, "fyToken": f"tok-{strike}-{opt}",
                "symbol": f"NSE:NIFTY{tint(expiry_ts)}{int(strike)}{opt}", "description": "",
            })
    return {"s": "ok", "data": {
        "callOi": 1, "putOi": 1,
        "indiavixData": {"ltp": 13.5, "symbol": "NSE:INDIAVIX-INDEX"},
        "expiryData": [
            {"date": "25-08-2026", "expiry": str(expiry_ts), "expiry_flag": "M"},
            {"date": "29-09-2026", "expiry": "1790676600", "expiry_flag": "M"},
        ],
        "optionsChain": chain,
    }}


def tint(ts):
    # last two digits of year + month code for a readable-ish suffix
    return "26AUG"


class FakeClient:
    """Minimal stand-in for FyersRestClient (no network)."""

    def __init__(self, payloads=None, history=None):
        self.payloads = payloads or {"NSE:NIFTY50-INDEX": make_chain_payload()}
        self.history = history

    def option_chain(self, symbol, strikecount=30, expiry_timestamp="", header=""):
        return self.payloads.get(symbol)

    def quotes(self, symbols, header=""):
        return {"s": "ok", "d": []}

    def history(self, symbol, resolution="1", range_from="", range_to="", cont_flag="1", header=""):
        if self.history is not None:
            return {"s": "ok", "candles": self.history}
        return {"s": "ok", "candles": []}

    def ensure_session(self):
        return "fake-header"


def make_master(underlying="NIFTY", expiry=date(2026, 8, 25), spot=25000):
    insts = []
    for offset in (-300, -200, -100, 0, 100, 200, 300):
        strike = spot + offset
        for opt in ("CE", "PE"):
            insts.append(FyersInstrument(
                fyers_symbol=f"NSE:NIFTY26AUG{int(strike)}{opt}", token=f"t{strike}{opt}",
                underlying=underlying, expiry_date=expiry, expiry_ts=1787652600,
                strike=strike, option_type=opt, lot_size=65, tick_size=0.05,
            ))
    return FyersSymbolMaster(insts)


def history_rising(spot=25000.0, bars=15):
    out = []
    t = 1786000000
    px = spot * 0.98
    for i in range(bars):
        px *= 1.0012
        out.append([t + i * 60, round(px, 2), round(px * 1.0005, 2), round(px * 0.9995, 2), round(px, 2), 1000])
    return out


class FyersParserTests(unittest.TestCase):
    def test_parse_chain(self):
        payload = make_chain_payload()
        chain = FyersOptionChainParser.parse(payload, "NIFTY", "2026-08-25",
                                             datetime(2026, 8, 12, 10, 0))
        self.assertIsInstance(chain, OptionChainSnapshot)
        self.assertEqual(chain.underlying, "NIFTY")
        self.assertAlmostEqual(chain.underlying_price, 25000.0)
        self.assertEqual(len(chain.strikes), 7)
        leg = chain.leg_at(25000.0, OptionType.CE)
        self.assertGreater(leg.quote.bid, 0)
        self.assertGreater(leg.quote.ask, leg.quote.bid)
        self.assertEqual(leg.security_id, "tok-25000.0-CE")

    def test_parse_calendar_and_vix(self):
        payload = make_chain_payload()
        cal = parse_expiry_calendar(payload)
        self.assertEqual(len(cal), 2)
        self.assertEqual(cal[0].flag, "M")
        self.assertEqual(parse_india_vix(payload), 13.5)

    def test_parse_raises_on_bad_payload(self):
        with self.assertRaises(Exception):
            FyersOptionChainParser.parse({"data": {}}, "NIFTY", "2026-08-25")


class SignalProxyTests(unittest.TestCase):
    def setUp(self):
        self.cfg = SystemConfig.from_file(CFG_PATH)

    def test_context_and_proxies_are_nonzero(self):
        payload = make_chain_payload()
        chain = FyersOptionChainParser.parse(payload, "NIFTY", "2026-08-25",
                                             datetime(2026, 8, 12, 10, 0))
        calc = PaperSignalCalculator(self.cfg, history_rising())
        ctx = calc.compute_context(chain, 13.5, datetime(2026, 8, 12, 10, 0))
        self.assertGreater(ctx.direction_score, 0)      # rising history -> positive
        self.assertGreater(ctx.trade_quality_score, 0)
        self.assertGreater(ctx.required_move, 0)
        proxies = calc.candidate_proxies(chain, ctx, Moneyness.ATM, OptionType.CE, spread_pct=0.6)
        self.assertGreaterEqual(proxies.premium_elasticity, 0.5)   # would pass scorer gate
        self.assertGreater(proxies.convexity_edge_score, 0)
        self.assertGreater(proxies.execution_quality_score, 0)

    def test_side_aware_confidence_breaks_ce_pe_tie(self):
        payload = make_chain_payload()
        chain = FyersOptionChainParser.parse(payload, "NIFTY", "2026-08-25",
                                             datetime(2026, 8, 12, 10, 0))
        calc = PaperSignalCalculator(self.cfg, history_rising())   # uptrend history
        ctx = calc.compute_context(chain, 13.5, datetime(2026, 8, 12, 10, 0))
        self.assertGreater(ctx.direction_score, 0)
        ce = calc.candidate_proxies(chain, ctx, Moneyness.ATM, OptionType.CE, spread_pct=0.6)
        pe = calc.candidate_proxies(chain, ctx, Moneyness.ATM, OptionType.PE, spread_pct=0.6)
        # In an uptrend the call must carry more conviction than the put.
        self.assertGreater(ce.opportunity_confidence_score, pe.opportunity_confidence_score)

    def test_per_call_history_override_beats_constructor_history(self):
        payload = make_chain_payload()
        chain = FyersOptionChainParser.parse(payload, "NIFTY", "2026-08-25",
                                             datetime(2026, 8, 12, 10, 0))
        calc = PaperSignalCalculator(self.cfg, history_rising())        # rising
        flat = [[int(time.time()) - (30 - i) * 60, 25000.0, 25001.0, 24999.0, 25000.0, 100] for i in range(30)]
        ctx = calc.compute_context(chain, 13.5, datetime(2026, 8, 12, 10, 0),
                                   history_candles=flat)               # flat beats rising
        self.assertAlmostEqual(ctx.direction_score, 0.0, delta=5.0)
        self.assertGreater(ctx.atr1, 0.0)                              # history actually consumed


class RunnerCycleTests(unittest.TestCase):
    # Pin the clock to market hours (Wed 10:00 IST) so these tests pass
    # regardless of when the suite runs; run_one_cycle short-circuits outside
    # 09:15-15:30 IST, which would yield zero candidates.
    MARKET_NOW = datetime(2026, 8, 12, 10, 0)

    def setUp(self):
        self.cfg = SystemConfig.from_file(CFG_PATH)
        self.tmp = tempfile.TemporaryDirectory()
        self.state_dir = Path(self.tmp.name)
        self._clock = mock.patch("institutional_options.paper_runner.now_ist",
                                 return_value=self.MARKET_NOW)
        self._clock.start()

    def tearDown(self):
        self._clock.stop()
        self.tmp.cleanup()

    def test_cycle_builds_candidates_with_proxies(self):
        client = FakeClient(history=history_rising())
        master = make_master()
        runner = PaperRunner(self.cfg, RUNNER_CFG, state_dir=self.state_dir,
                             client=client, master=master)
        runner.run_one_cycle()
        snap = runner.snapshot()
        cands = snap["underlyings"].get("_candidates", [])
        self.assertGreater(len(cands), 0)
        # Proxies must be filled (nonzero) so the scorer gate can pass.
        for c in cands:
            self.assertGreater(c["premium_elasticity"], 0)
        self.assertTrue(snap["last_cycle_ok"])

    def test_snapshot_serializes(self):
        client = FakeClient(history=history_rising())
        runner = PaperRunner(self.cfg, RUNNER_CFG, state_dir=self.state_dir,
                             client=client, master=make_master())
        runner.run_one_cycle()
        snap = runner.snapshot()
        json.dumps(snap)  # must not raise
        self.assertEqual(snap["mode"], "PAPER (no orders placed)")
        self.assertIn("equity", snap)

    def test_edge_scaled_target_scales_with_ratio(self):
        edge_cfg = {**RUNNER_CFG, "config_overrides": {"exit_management": {
            "edge_scaled_target": True, "edge_scale_min_ratio": 1.1,
            "edge_scale_min_r": 1.0, "edge_scale_max_r": 3.0}}}
        runner = PaperRunner(self.cfg, edge_cfg, state_dir=self.state_dir,
                             client=FakeClient(history=history_rising()),
                             master=make_master())
        strong = SimpleNamespace(candidate=SimpleNamespace(expected_move=176.0, required_move=100.0))
        self.assertAlmostEqual(runner._target_r(strong), 3.0)  # 2.0*1.6 capped
        weak = SimpleNamespace(candidate=SimpleNamespace(expected_move=90.0, required_move=100.0))
        self.assertLess(runner._target_r(weak), 2.0)           # ratio 0.9 -> below base
        self.assertGreaterEqual(runner._target_r(weak), 1.0)   # floored
        # Edge scaling disabled -> plain preferred_target_R.
        runner2 = PaperRunner(self.cfg, RUNNER_CFG, state_dir=self.state_dir,
                              client=FakeClient(history=history_rising()),
                              master=make_master())
        self.assertAlmostEqual(runner2._target_r(strong), 2.0)

    def test_cycle_writes_candidates_log(self):
        runner = PaperRunner(self.cfg, RUNNER_CFG, state_dir=self.state_dir,
                             client=FakeClient(history=history_rising()),
                             master=make_master())
        runner.run_one_cycle()
        path = self.state_dir / "candidates_log.csv"
        self.assertTrue(path.exists())
        with path.open("r", encoding="utf-8-sig", newline="") as f:
            rows = list(csv.DictReader(f))
        self.assertGreater(len(rows), 0)
        for key in ("date", "comparable_score", "threshold", "grade", "decision",
                    "direction", "execution", "exp_req_ratio", "spread_pct"):
            self.assertIn(key, rows[0])

    def test_prefilter_drops_unfillable_strikes(self):
        payload = make_chain_payload()
        for row in payload["data"]["optionsChain"]:
            if row.get("strike_price") == 25000.0 and row.get("option_type") in ("CE", "PE"):
                mid = (row["bid"] + row["ask"]) / 2
                row["bid"] = mid - 2.0
                row["ask"] = mid + 2.0
                row["ltp"] = mid
        client = FakeClient(payloads={"NSE:NIFTY50-INDEX": payload},
                            history=history_rising())
        runner = PaperRunner(self.cfg, RUNNER_CFG, state_dir=self.state_dir,
                             client=client, master=make_master())
        runner.run_one_cycle()
        snap = runner.snapshot()
        cands = snap["underlyings"].get("_candidates", [])
        strikes = {c["strike"] for c in cands}
        self.assertNotIn(25000.0, strikes)
        self.assertGreaterEqual(snap["underlyings"].get("_prefiltered", 0), 2)


if __name__ == "__main__":
    unittest.main()
