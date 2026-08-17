from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Mapping

from .config import SystemConfig
from .models import CandidateInputs, DataHealth, Quote
from .option_chain import OptionChainSemanticValidator
from .snapshot import InstrumentMarketSnapshot, MultiInstrumentSnapshot


@dataclass(frozen=True)
class InstrumentHealthReport:
    underlying: str
    futures_health: DataHealth
    option_chain_health: DataHealth
    valid: bool
    reason: str


class DataHealthOrchestrator:
    def __init__(self, config: SystemConfig):
        self.config = config

    def evaluate_instrument(self, snap: InstrumentMarketSnapshot, now: datetime) -> InstrumentHealthReport:
        dh = self.config.section("data_health")
        fut_age = (now - snap.futures_quote.timestamp).total_seconds()
        reasons: list[str] = []
        fut_valid = snap.futures_quote.is_valid() and fut_age <= float(dh["futures_stale_invalid_sec"])
        if not fut_valid:
            reasons.append(f"futures invalid/stale {fut_age:.2f}s")
        chain_age = (now - snap.option_chain.timestamp).total_seconds()
        chain_valid = chain_age <= float(dh["option_chain_invalid_sec"])
        if not chain_valid:
            reasons.append(f"option chain stale {chain_age:.2f}s")
        valid = fut_valid and chain_valid
        return InstrumentHealthReport(
            snap.underlying,
            DataHealth(fut_valid, not fut_valid, reasons[0] if reasons else ""),
            DataHealth(chain_valid, not chain_valid, reasons[-1] if reasons else ""),
            valid,
            "; ".join(reasons),
        )


    def evaluate_candidate(self, candidate: CandidateInputs, now: datetime) -> DataHealth:
        dh = self.config.section("data_health")
        if not candidate.quote.is_valid():
            return DataHealth(False, False, "Candidate option quote invalid")
        age = (now - candidate.quote.timestamp).total_seconds()
        if age > float(dh["option_quote_stale_invalid_sec"]):
            return DataHealth(False, False, f"Candidate option quote stale {age:.2f}s")
        if bool(dh.get("require_source_timestamp_for_approval", False)) and not candidate.quote.source_timestamp_available:
            return DataHealth(False, False, "Candidate source timestamp unavailable")
        if age > float(dh["option_quote_stale_warning_sec"]):
            return DataHealth(True, True, f"Candidate option quote warning stale {age:.2f}s")
        return DataHealth(True, False, "")

    def evaluate_option_chain(self, chain, now: datetime) -> DataHealth:
        dh = self.config.section("data_health")
        age = (now - chain.timestamp).total_seconds()
        if age > float(dh["option_chain_invalid_sec"]):
            return DataHealth(False, False, f"Option chain stale {age:.2f}s")
        try:
            report = OptionChainSemanticValidator.validate(chain, require_tradable_quotes=False)
        except Exception as exc:
            return DataHealth(False, False, f"Option chain semantic validation error: {type(exc).__name__}")
        if bool(dh.get("require_chain_semantics_for_approval", False)) and not report.valid:
            return DataHealth(False, False, "; ".join(report.errors) or "Option chain semantics invalid")
        if report.warnings:
            return DataHealth(True, True, "; ".join(report.warnings[:3]))
        return DataHealth(True, False, "")

    def evaluate_option_chain_semantics(self, snap: InstrumentMarketSnapshot) -> DataHealth:
        report = OptionChainSemanticValidator.validate(snap.option_chain, require_tradable_quotes=False)
        if not report.valid:
            return DataHealth(False, False, "; ".join(report.errors))
        if report.warnings:
            return DataHealth(True, True, "; ".join(report.warnings[:3]))
        return DataHealth(True, False, "")

    def evaluate_global(self, snapshots: MultiInstrumentSnapshot) -> DataHealth:
        invalid = [u for u, s in snapshots.instruments.items() if not self.evaluate_instrument(s, snapshots.timestamp).valid]
        if len(invalid) >= 3:
            return DataHealth(False, False, f"3+ instruments invalid: {','.join(invalid)}")
        if invalid:
            return DataHealth(True, True, f"Some instruments invalid: {','.join(invalid)}")
        return DataHealth(True, False, "")
