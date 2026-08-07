from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping


class ConfigError(ValueError):
    pass


@dataclass(frozen=True)
class SystemConfig:
    raw: Mapping[str, Any]

    @classmethod
    def from_file(cls, path: str | Path) -> "SystemConfig":
        p = Path(path)
        with p.open("r", encoding="utf-8") as f:
            raw = json.load(f)
        cfg = cls(raw=raw)
        cfg.validate()
        return cfg

    def section(self, name: str) -> Mapping[str, Any]:
        value = self.raw.get(name)
        if not isinstance(value, Mapping):
            raise ConfigError(f"Missing or invalid config section: {name}")
        return value

    def validate(self) -> None:
        required = [
            "capital",
            "risk",
            "data_health",
            "liquidity",
            "premium_elasticity",
            "expected_move",
            "iv_crush",
            "hard_stop",
            "instrument_universe",
            "opportunity_selection",
            "paper_fill_simulator",
            "portfolio_no_trade_engine",
            "excellence_framework",
        ]
        for name in required:
            if name not in self.raw:
                raise ConfigError(f"Required config section missing: {name}")
        capital = self.section("capital")
        if capital.get("pledge_or_leverage_allowed") is not False:
            raise ConfigError("Pledge/leverage must be disabled for MVP.")
        if capital.get("overnight_holding_allowed") is not False:
            raise ConfigError("Overnight holding must be disabled for MVP.")
        if capital.get("auto_execution_mvp") is not False:
            raise ConfigError("Auto execution must be disabled for MVP.")
        execution = self.section("execution")
        if execution.get("live_trading_enabled") is not False:
            raise ConfigError("live_trading_enabled must be false for current MVP/Phase 1-3 implementation.")
        universe = self.section("instrument_universe")
        expected = ["BANKNIFTY", "NIFTY", "FINNIFTY", "MIDCPNIFTY"]
        if list(universe.get("eligible_underlyings", [])) != expected:
            raise ConfigError("Instrument universe does not match frozen MVP universe.")
        if universe.get("max_open_positions") != 1 or universe.get("max_pending_orders") != 1:
            raise ConfigError("Global position and pending order limits must be 1.")
        if universe.get("trade_only_best_ranked_candidate") is not True:
            raise ConfigError("MVP must trade only the best ranked candidate in paper mode.")

    def get_float(self, section: str, key: str) -> float:
        value = self.section(section).get(key)
        if not isinstance(value, (int, float)):
            raise ConfigError(f"Expected numeric config {section}.{key}")
        return float(value)

    def get_int(self, section: str, key: str) -> int:
        value = self.section(section).get(key)
        if not isinstance(value, int):
            raise ConfigError(f"Expected integer config {section}.{key}")
        return value
