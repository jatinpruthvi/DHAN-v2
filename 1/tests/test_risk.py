import unittest

from institutional_options.config import SystemConfig
from institutional_options.risk import DynamicRiskCalculator, RiskContext


class RiskTests(unittest.TestCase):
    def setUp(self):
        self.cfg = SystemConfig.from_file("uploads/PARAMETERS.json")
        self.calc = DynamicRiskCalculator(self.cfg)

    def test_dynamic_risk_low_premium_below_cap(self):
        plan = self.calc.plan(RiskContext(
            capital=100000, mode="NORMAL", setup_grade="A", lots=1,
            entry_premium=80, lot_size=30, spread_points=1, tick_size=0.05,
            required_stop_points=10, instrument="BANKNIFTY"
        ))
        self.assertTrue(plan.hard_stop_fit)
        self.assertEqual(plan.hard_stop_points, 16)
        self.assertEqual(plan.planned_risk, 480)

    def test_required_stop_above_cap_rejects(self):
        plan = self.calc.plan(RiskContext(
            capital=100000, mode="NORMAL", setup_grade="A", lots=1,
            entry_premium=400, lot_size=30, spread_points=1, tick_size=0.05,
            required_stop_points=40, instrument="BANKNIFTY"
        ))
        self.assertFalse(plan.hard_stop_fit)


if __name__ == "__main__":
    unittest.main()
