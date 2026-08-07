import unittest

from institutional_options.config import SystemConfig


class ConfigTests(unittest.TestCase):
    def test_loads_project_parameters(self):
        cfg = SystemConfig.from_file("uploads/PARAMETERS.json")
        self.assertEqual(cfg.section("instrument_universe")["max_open_positions"], 1)
        self.assertFalse(cfg.section("capital")["auto_execution_mvp"])


if __name__ == "__main__":
    unittest.main()
