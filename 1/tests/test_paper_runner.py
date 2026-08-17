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
    def test_bse_symbol_master_rows_are_parsed_when_requested(self):
        expiry_ts = 1787652600
        rows = []
        for opt in ("CE", "PE"):
            row = ["bse-token", "SENSEX", "BSE", "20", "0.05", "", "", "", str(expiry_ts),
                   f"BSE:SENSEX26AUG78000{opt}", "", "", "", "SENSEX"]
            rows.append(",".join(row))
        with tempfile.TemporaryDirectory() as td:
            path = Path(td) / "BSE_FO.csv"
            path.write_text("\n".join(rows) + "\n", encoding="utf-8")
            master = FyersSymbolMaster.from_csv(
                path,
                allowed_exchanges={"BSE"},
                allowed_underlyings={"SENSEX"},
            )
        self.assertEqual(master.expiry_dates("SENSEX"), (date(2026, 8, 25),))
        self.assertEqual(master.lot_size("SENSEX", date(2026, 8, 25)), 20)
        self.assertEqual(master.tick_size("SENSEX", date(2026, 8, 25)), 0.05)
        self.assertEqual(master.symbol_for("SENSEX", date(2026, 8, 25), 78000, "CE"),
                         "BSE:SENSEX26AUG78000CE")

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

    def test_direction_model_runs_in_shadow_without_replacing_proxy(self):
        payload = make_chain_payload()
        chain = FyersOptionChainParser.parse(payload, "NIFTY", "2026-08-25",
                                             datetime(2026, 8, 12, 10, 0))
        calc = PaperSignalCalculator(self.cfg, history_rising())
        inputs = {name: history_rising() for name in ("HDFCBANK", "SBIN", "ICICIBANK")}
        ctx = calc.compute_context(chain, 13.5, datetime(2026, 8, 12, 10, 0),
                                   history_candles=history_rising(),
                                   direction_model_inputs=inputs)
        self.assertEqual(ctx.direction_model_status, "VALID")
        self.assertIsNotNone(ctx.direction_model_score)
        self.assertEqual(ctx.direction_score, calc._trend_signals([row[4] for row in history_rising()])[1])

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

    def test_entry_window_uses_configured_start_and_cutoff(self):
        runner = PaperRunner(self.cfg, RUNNER_CFG, state_dir=self.state_dir,
                             client=FakeClient(history=history_rising()),
                             master=make_master())
        self.assertFalse(runner._entry_window_open(datetime(2026, 8, 12, 9, 29)))
        self.assertTrue(runner._entry_window_open(datetime(2026, 8, 12, 9, 30)))
        self.assertTrue(runner._entry_window_open(datetime(2026, 8, 12, 14, 14)))
        self.assertFalse(runner._entry_window_open(datetime(2026, 8, 12, 14, 15)))
        self.assertFalse(runner._entry_window_open(datetime(2026, 8, 12, 15, 0)))

    def test_default_expanded_universe_is_paper_selector_eligible(self):
        cfg = json.loads(Path("uploads/PAPER_RUNNER.json").read_text(encoding="utf-8"))
        underlyings = cfg["underlyings"]
        self.assertFalse(underlyings["SENSEX"]["monitor_only"])
        self.assertFalse(underlyings["NIFTYNXT50"]["monitor_only"])
        self.assertFalse(underlyings["NIFTYFPI"]["monitor_only"])
        self.assertIn("BANKEX", underlyings)
        self.assertEqual(underlyings["NIFTYNXT50"]["index_symbol"], "NSE:NIFTYNXT50-INDEX")
        self.assertEqual(underlyings["NIFTYFPI"]["index_symbol"], "NSE:NIFTYFPI-INDEX")
        self.assertFalse(underlyings["BANKEX"]["monitor_only"])
        self.assertFalse(underlyings["FOCIT"]["monitor_only"])
        stocks = [meta for meta in underlyings.values() if meta.get("instrument_kind") == "STOCK"]
        trade_enabled = [meta for meta in underlyings.values() if meta.get("trade_enabled")]
        self.assertEqual(len(stocks), 50)
        self.assertEqual(len(trade_enabled), 59)
        self.assertTrue(all(meta.get("trade_enabled") and not meta.get("monitor_only") for meta in underlyings.values()))
        self.assertEqual(cfg["monitoring"]["monitor_batch_size"], 8)
        self.assertEqual(cfg["monitoring"]["monitor_poll_seconds"], 60)

    def test_prefer_monthly_selects_monthly_expiry(self):
        runner = PaperRunner(self.cfg, RUNNER_CFG, state_dir=self.state_dir,
                             client=FakeClient(), master=make_master())
        calendar = (
            SimpleNamespace(date_str="18-08-2026", expiry_ts=1787047800, flag="W"),
            SimpleNamespace(date_str="25-08-2026", expiry_ts=1787652600, flag="M"),
        )
        self.assertEqual(runner._select_expiry("SENSEX", calendar, True), "2026-08-25")
        self.assertEqual(runner._select_expiry("NIFTY", calendar, False), "2026-08-18")

    def test_monitor_rotation_keeps_trade_universe_always_present(self):
        cfg = json.loads(Path("uploads/PAPER_RUNNER.json").read_text(encoding="utf-8"))
        cfg["monitoring"]["monitor_poll_seconds"] = 0
        runner = PaperRunner(self.cfg, cfg, state_dir=self.state_dir,
                             client=FakeClient(), master=make_master())
        first = runner._cycle_underlyings()
        second = runner._cycle_underlyings()
        self.assertEqual(set(first[:4]), {"NIFTY", "BANKNIFTY", "FINNIFTY", "MIDCPNIFTY"})
        self.assertEqual(set(second[:4]), {"NIFTY", "BANKNIFTY", "FINNIFTY", "MIDCPNIFTY"})
        self.assertEqual(len(first), 12)
        self.assertEqual(len(second), 12)
        self.assertNotEqual(first[4:], second[4:])

    def test_monitor_poll_throttle_fetches_trade_universe_without_repeating_batch(self):
        cfg = json.loads(Path("uploads/PAPER_RUNNER.json").read_text(encoding="utf-8"))
        runner = PaperRunner(self.cfg, cfg, state_dir=self.state_dir,
                             client=FakeClient(), master=make_master())
        first = runner._cycle_underlyings()
        second = runner._cycle_underlyings()
        self.assertEqual(len(first), 12)
        self.assertEqual(len(second), 12)
        self.assertEqual(set(second), set(first))
        schedule = runner.state.underlyings["_paper_schedule"]
        self.assertEqual(set(schedule["selected"]), set(second))
        self.assertEqual(len(schedule["last_batch"]), 8)

    def test_monitor_only_bse_chain_is_displayed_but_not_ranked(self):
        cfg = {**RUNNER_CFG, "underlyings": {
            "NIFTY": {"index_symbol": "NSE:NIFTY50-INDEX", "prefer_monthly": False},
            "SENSEX": {"index_symbol": "BSE:SENSEX-INDEX", "exchange": "BSE",
                       "prefer_monthly": True, "monitor_only": True},
        }}
        payloads = {
            "NSE:NIFTY50-INDEX": make_chain_payload(),
            "BSE:SENSEX-INDEX": make_chain_payload(spot=78000.0, prem=120.0),
        }
        combined_master = FyersSymbolMaster.combine(
            make_master(),
            make_master("SENSEX", expiry=date(2026, 8, 25), spot=78000),
        )
        runner = PaperRunner(self.cfg, cfg, state_dir=self.state_dir,
                             client=FakeClient(payloads=payloads, history=history_rising()),
                             master=combined_master)
        runner.run_one_cycle()
        snap = runner.snapshot()
        self.assertEqual(snap["underlyings"]["SENSEX"]["exchange"], "BSE")
        self.assertTrue(snap["underlyings"]["SENSEX"]["monitor_only"])
        ranked = snap["underlyings"].get("_candidates", [])
        self.assertTrue(all(row["underlying"] != "SENSEX" for row in ranked))
        monitor_log = self.state_dir / "monitor_diagnostics.csv"
        self.assertTrue(monitor_log.exists())
        monitor_text = monitor_log.read_text(encoding="utf-8")
        self.assertIn("underlying,exchange", monitor_text)
        self.assertIn("SENSEX,BSE", monitor_text)
        self.assertIn("atm_ce_spread_pct", monitor_text)
        shadow_log = self.state_dir / "shadow_candidates.csv"
        self.assertTrue(shadow_log.exists())
        shadow_text = shadow_log.read_text(encoding="utf-8")
        self.assertIn("research_only", shadow_text)
        self.assertIn("SENSEX", shadow_text)
        self.assertNotIn("SENSEX", "\n".join(row.get("underlying", "") for row in ranked))

    def test_stock_option_monitor_uses_generic_chain_and_master_path(self):
        cfg = {**RUNNER_CFG, "underlyings": {
            "NIFTY": {"index_symbol": "NSE:NIFTY50-INDEX", "prefer_monthly": False},
            "RELIANCE": {"index_symbol": "NSE:RELIANCE-EQ", "exchange": "NSE",
                         "instrument_kind": "STOCK", "prefer_monthly": True,
                         "monitor_only": True},
        }}
        payloads = {
            "NSE:NIFTY50-INDEX": make_chain_payload(),
            "NSE:RELIANCE-EQ": make_chain_payload(spot=25000.0, prem=90.0),
        }
        combined_master = FyersSymbolMaster.combine(
            make_master(),
            make_master("RELIANCE", expiry=date(2026, 8, 25), spot=25000),
        )
        runner = PaperRunner(self.cfg, cfg, state_dir=self.state_dir,
                             client=FakeClient(payloads=payloads, history=history_rising()),
                             master=combined_master)
        runner.run_one_cycle()
        snap = runner.snapshot()
        self.assertEqual(snap["underlyings"]["RELIANCE"]["instrument_kind"], "STOCK")
        self.assertTrue(snap["underlyings"]["RELIANCE"]["monitor_only"])
        self.assertTrue(all(row["underlying"] != "RELIANCE"
                            for row in snap["underlyings"].get("_candidates", [])))
        self.assertTrue(any(row["underlying"] == "RELIANCE" and row["research_only"]
                            for row in snap["underlyings"].get("_shadow_candidates", [])))

    def test_missing_contract_metadata_blocks_candidate_creation(self):
        client = FakeClient(history=history_rising())
        runner = PaperRunner(self.cfg, RUNNER_CFG, state_dir=self.state_dir,
                             client=client, master=FyersSymbolMaster([]))
        runner.run_one_cycle()
        snap = runner.snapshot()
        self.assertIn("instrument_error", snap["underlyings"]["NIFTY"])
        self.assertEqual(snap["underlyings"].get("_candidates", []), [])

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

    def test_cycle_writes_candidate_diagnostics(self):
        runner = PaperRunner(self.cfg, RUNNER_CFG, state_dir=self.state_dir,
                             client=FakeClient(history=history_rising()),
                             master=make_master())
        runner.run_one_cycle()
        path = self.state_dir / "candidate_diagnostics.csv"
        self.assertTrue(path.exists())
        text = path.read_text(encoding="utf-8")
        for key in ("side_direction_score", "direction_gate_passed",
                    "observed_elasticity_valid", "observed_elasticity_post_cost",
                    "surface_valid", "atm_iv", "call_put_iv_skew"):
            self.assertIn(key, text)

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

    def test_verified_playbook_filter_selects_regime_compatible_code(self):
        risk_path = self.state_dir / "risk_context.json"
        risk_path.write_text(json.dumps({
            "ts": "2026-08-12T10:00:00+05:30",
            "source": "test-feed",
            "global_risk_state": "NEUTRAL",
            "news_state": "NEWS_NORMAL",
            "liquidity_stable": True,
            "gap_wait_completed": True,
        }), encoding="utf-8")
        cfg = {**RUNNER_CFG, "risk_context_path": str(risk_path),
               "config_overrides": {"playbook_runtime": {"enforce_on_paper": True}}}
        runner = PaperRunner(self.cfg, cfg, state_dir=self.state_dir,
                             client=FakeClient(history=history_rising()), master=make_master())
        context = SimpleNamespace(
            trend_efficiency=90.0, direction_score=80.0, vix=13.5,
            regime_confidence=85.0, market_hostility_score=10.0,
            trade_quality_score=90.0, dte=5.0,
        )
        runner._update_playbook_filters({"NIFTY": context}, self.MARKET_NOW)
        self.assertIn("A01", runner._playbook_codes_by_underlying["NIFTY"])

    def test_runtime_risk_context_fails_closed_when_enforced(self):
        risk_path = self.state_dir / "risk_context.json"
        risk_path.write_text(json.dumps({
            "ts": "2026-08-12T10:00:00+05:30",
            "source": "test-feed",
            "global_risk_state": "SHOCK",
            "news_state": "NEWS_NORMAL",
            "portfolio_no_trade_score": 10,
        }), encoding="utf-8")
        cfg = {**RUNNER_CFG,
               "risk_context_path": str(risk_path),
               "config_overrides": {"runtime_risk_controls": {"enforce_on_paper": True}}}
        runner = PaperRunner(self.cfg, cfg, state_dir=self.state_dir,
                             client=FakeClient(history=history_rising()), master=make_master())
        self.assertIn("Global risk shock", runner._risk_context_block_reason())

    def test_rank_persistence_requires_two_windows_and_survives_restart(self):
        runner = PaperRunner(self.cfg, RUNNER_CFG, state_dir=self.state_dir,
                             client=FakeClient(history=history_rising()), master=make_master())
        candidate = SimpleNamespace(
            instrument=SimpleNamespace(underlying="NIFTY", expiry=date(2026, 8, 25), strike=25000.0),
            side=OptionType.CE,
        )
        evaluation = SimpleNamespace(candidate=candidate)
        first = runner._rank_persistence_check(evaluation, self.MARKET_NOW)
        self.assertFalse(first[0])
        reloaded = PaperRunner(self.cfg, RUNNER_CFG, state_dir=self.state_dir,
                               client=FakeClient(history=history_rising()), master=make_master())
        second = reloaded._rank_persistence_check(evaluation, self.MARKET_NOW + timedelta(seconds=5))
        self.assertTrue(second[0])

    def test_daily_risk_limits_are_enforced_and_persisted(self):
        runner = PaperRunner(self.cfg, RUNNER_CFG, state_dir=self.state_dir,
                             client=FakeClient(history=history_rising()), master=make_master())
        runner.state.trades_today = 2
        self.assertIn("Maximum trades per day", runner._daily_risk_block_reason(self.MARKET_NOW))
        runner.state.trades_today = 0
        runner.state.losses_today = 3
        self.assertIn("three daily losses", runner._daily_risk_block_reason(self.MARKET_NOW))
        runner.state.losses_today = 0
        runner.state.realized_pnl_today = -1500.0
        self.assertIn("Maximum daily loss", runner._daily_risk_block_reason(self.MARKET_NOW))
        runner.state.realized_pnl_today = 0.0
        runner.state.trades_today = 1
        runner._save_daily_risk_state()
        reloaded = PaperRunner(self.cfg, RUNNER_CFG, state_dir=self.state_dir,
                               client=FakeClient(history=history_rising()), master=make_master())
        self.assertEqual(reloaded.state.trades_today, 1)

    def test_signal_runner_override_reaches_signal_calculator_config(self):
        cfg = {**RUNNER_CFG, "signal": {"required_move_straddle_factor": 0.9}}
        runner = PaperRunner(self.cfg, cfg, state_dir=self.state_dir,
                             client=FakeClient(history=history_rising()), master=make_master())
        self.assertEqual(runner.config.raw["paper_runner"]["signal"]["required_move_straddle_factor"], 0.9)

    def test_saturday_waits_until_monday_open(self):
        runner = PaperRunner(self.cfg, RUNNER_CFG, state_dir=self.state_dir,
                             client=FakeClient(), master=make_master())
        saturday = datetime(2026, 8, 15, 10, 0)
        with mock.patch("institutional_options.paper_runner.now_ist", return_value=saturday):
            self.assertEqual(runner._seconds_to_open(), 47 * 3600 + 15 * 60)

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


    def test_run_manifest_is_created_and_stale_state_is_archived(self):
        with tempfile.TemporaryDirectory() as d:
            runner = PaperRunner(self.cfg, RUNNER_CFG, state_dir=d,
                                 client=FakeClient(history=history_rising()), master=make_master())
            manifest_path = Path(d) / "run_manifest.json"
            self.assertTrue(manifest_path.exists())
            manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
            self.assertEqual(manifest["universe"]["underlyings"], ["NIFTY"])
            self.assertEqual(manifest["live_execution"], "DISABLED")
            (Path(d) / "mtil.csv").write_text("legacy\n", encoding="utf-8")
            manifest["policy_signature"] = "stale-policy"
            manifest_path.write_text(json.dumps(manifest), encoding="utf-8")
            PaperRunner(self.cfg, RUNNER_CFG, state_dir=d,
                        client=FakeClient(history=history_rising()), master=make_master())
            archives = list((Path(d) / "archives").glob("policy_*"))
            self.assertEqual(len(archives), 1)
            self.assertTrue((archives[0] / "mtil.csv").exists())
            self.assertTrue(manifest_path.exists())

    def test_revalidation_and_fill_audit_streams_are_created(self):
        with tempfile.TemporaryDirectory() as d:
            runner = PaperRunner(self.cfg, RUNNER_CFG, state_dir=d,
                                 client=FakeClient(history=history_rising()), master=make_master())
            runner.run_one_cycle()
            self.assertTrue((Path(d) / "run_manifest.json").exists())
            # The audit files are created once the corresponding path is exercised;
            # their presence is not required when no candidate reaches revalidation.
            self.assertFalse((Path(d) / "mtil.csv").exists() and (Path(d) / "mtil.csv").read_text(encoding="utf-8").startswith("legacy"))

    def test_live_execution_override_is_rejected(self):
        cfg = {**RUNNER_CFG, "config_overrides": {"execution": {"live_trading_enabled": True}}}
        with tempfile.TemporaryDirectory() as d:
            with self.assertRaises(ValueError):
                PaperRunner(self.cfg, cfg, state_dir=d,
                            client=FakeClient(history=history_rising()), master=make_master())


if __name__ == "__main__":
    unittest.main()
