import copy
import json
import unittest
from pathlib import Path

from institutional_options.config import SystemConfig


class ConfigTests(unittest.TestCase):
    def test_loads_project_parameters(self):
        cfg = SystemConfig.from_file("uploads/PARAMETERS.json")
        self.assertEqual(cfg.section("instrument_universe")["max_open_positions"], 1)
        self.assertFalse(cfg.section("capital")["auto_execution_mvp"])

    def test_gate_learning_policy_is_explicit_and_conservative(self):
        cfg = SystemConfig.from_file("uploads/PARAMETERS.json")
        learning = cfg.raw["instrument_gate_learning"]
        self.assertTrue(learning["enabled"])
        self.assertTrue(learning["do_not_loosen"])
        self.assertGreaterEqual(learning["minimum_learning_days"], learning["warmup_days"])
        self.assertLessEqual(learning["winning_quantile"], 0.5)
        self.assertGreaterEqual(learning["high_watermark_quantile"], 0.5)

    def test_invalid_gate_learning_quantile_is_rejected(self):
        raw = json.loads(Path("uploads/PARAMETERS.json").read_text(encoding="utf-8"))
        raw["instrument_gate_learning"] = copy.deepcopy(raw["instrument_gate_learning"])
        raw["instrument_gate_learning"]["winning_quantile"] = 0.8
        with self.assertRaises(ValueError):
            SystemConfig(raw=raw).validate()

    def test_invalid_stale_alert_threshold_is_rejected(self):
        raw = json.loads(Path("uploads/PARAMETERS.json").read_text(encoding="utf-8"))
        raw["data_health"] = copy.deepcopy(raw["data_health"])
        raw["data_health"]["stale_alert_consecutive_cycles"] = 0
        with self.assertRaises(ValueError):
            SystemConfig(raw=raw).validate()

    def test_invalid_walk_forward_grid_is_rejected(self):
        raw = json.loads(Path("uploads/PARAMETERS.json").read_text(encoding="utf-8"))
        raw["instrument_gate_learning"] = copy.deepcopy(raw["instrument_gate_learning"])
        raw["instrument_gate_learning"]["candidate_quantiles"] = [0.4]
        with self.assertRaises(ValueError):
            SystemConfig(raw=raw).validate()

    def test_expanded_runner_preserves_trade_boundary(self):
        runner_cfg = json.loads(Path("uploads/PAPER_RUNNER.json").read_text(encoding="utf-8"))
        underlyings = runner_cfg["underlyings"]
        properties = list(underlyings.values())
        trade_enabled = [meta for meta in properties if meta.get("trade_enabled") is True]
        monitor_only = [meta for meta in properties if meta.get("monitor_only") is True]
        self.assertEqual(len(properties), 59)
        self.assertEqual(len(trade_enabled), 59)
        self.assertEqual(len(monitor_only), 0)
        self.assertEqual(
            {name for name, meta in underlyings.items() if meta.get("trade_enabled") is True},
            set(underlyings),
        )
        self.assertTrue(all(meta.get("monitor_only") is False for meta in properties))
        self.assertEqual(runner_cfg["monitoring"]["monitor_batch_size"], 8)
        self.assertEqual(runner_cfg["monitoring"]["monitor_poll_seconds"], 60)


if __name__ == "__main__":
    unittest.main()
