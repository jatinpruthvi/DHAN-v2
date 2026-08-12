"""Paper-session evidence collection and phase-2 evidence reports.

The phase-2 dry-run gate (`phase2.DryRunValidator`) and its evidence review
(`phase2.EvidenceAnalyzer`) consume two CSV datasets:

  * `mtil.csv`      - one row per CLOSED paper trade, with the proxy scores at
                      entry plus the realised net PnL and R-multiple.
  * `skipped.csv`   - one row per evaluated-but-not-selected candidate (top-N
                      per ranking cycle), so the gate's "minimum paper trade
                      candidates" and "minimum ranking cycles" can be met even
                      on days when no trade fires.

The collector writes exactly the field names the phase-2 machinery already
reads (`OpportunityScore`, `DirectionScore`, `net_pnl_rupees`, `r_multiple`,
...), so `DryRunValidator`/`EvidenceAnalyzer` work unchanged on the
accumulated data. The score-bucket calibration in the report is the core
proxy-validation evidence: does a higher proxy score actually produce a
higher win rate / average R?

Usage:
    python -m institutional_options.paper_evidence [--state-dir paper_state]

Writes paper_state/evidence_report.txt and prints it.
"""
from __future__ import annotations

import csv
from pathlib import Path
from typing import Any, Iterable, Mapping, Optional

from .analytics import summarize_pnl
from .config import SystemConfig
from .models import PaperTrade
from .phase2 import CsvDataset, DryRunValidator, EvidenceAnalyzer
from .records import MTILRecordBuilder, SkippedCandidateRecordBuilder


class AppendingCsv:
    """Tiny append-only CSV writer that preserves the header from row keys."""

    def __init__(self, path: str | Path):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._fields: Optional[list[str]] = None
        if self.path.exists():
            with self.path.open("r", encoding="utf-8-sig", newline="") as f:
                first = f.readline().strip()
            if first:
                self._fields = [c.strip() for c in first.split(",") if c.strip()]

    def append(self, record: Mapping[str, Any]) -> None:
        if self._fields is None:
            self._fields = list(record.keys())
            with self.path.open("w", encoding="utf-8", newline="") as f:
                csv.DictWriter(f, fieldnames=self._fields).writeheader()
        # Keep the header stable; ignore keys that appear later.
        row = {k: record.get(k, "") for k in self._fields}
        with self.path.open("a", encoding="utf-8", newline="") as f:
            csv.DictWriter(f, fieldnames=self._fields, extrasaction="ignore").writerow(row)


class PaperEvidenceCollector:
    """Writes phase-2 datasets (mtil.csv / skipped.csv) from paper sessions."""

    def __init__(self, state_dir: str | Path):
        self.state_dir = Path(state_dir)
        self.mtil = AppendingCsv(self.state_dir / "mtil.csv")
        self.skipped = AppendingCsv(self.state_dir / "skipped.csv")

    def record_trade(self, trade: PaperTrade, net_pnl_rupees: float,
                     r_multiple: float) -> None:
        """One MTIL row per closed trade, including proxy scores at entry."""
        record = MTILRecordBuilder.from_paper_trade(
            trade, net_pnl_rupees=net_pnl_rupees, r_multiple=r_multiple)
        self.mtil.append(record)

    def record_skipped(self, evaluations: Iterable, ranking_cycle_id: str,
                       top_n: int = 5) -> None:
        """Top-N evaluated-but-not-selected candidates for one ranking cycle."""
        evals = list(evaluations)
        ranked = sorted(evals, key=lambda e: e.comparable_opportunity_score, reverse=True)
        for rank, e in enumerate(ranked[:top_n], start=1):
            why = "; ".join(e.reasons) if e.reasons else f"grade={e.grade.value} below threshold"
            self.skipped.append(SkippedCandidateRecordBuilder.from_evaluation(
                e, ranking_cycle_id=ranking_cycle_id, rank=rank, why=why))


def build_evidence_report(state_dir: str | Path, config: Optional[SystemConfig] = None,
                          emergency_tests_passed: bool = False) -> str:
    """Run the phase-2 gate + evidence review over the accumulated paper data."""
    cfg = config or SystemConfig.from_file("uploads/PARAMETERS.json")
    state = Path(state_dir)
    mtil = CsvDataset.from_csv(state / "mtil.csv")
    skipped = CsvDataset.from_csv(state / "skipped.csv")

    acceptance = DryRunValidator(cfg).validate(
        mtil, skipped, emergency_tests_passed=emergency_tests_passed)
    review = EvidenceAnalyzer().analyze(mtil, skipped)

    lines: list[str] = []
    lines.append("=" * 74)
    lines.append("PAPER-EVIDENCE REPORT  (proxy scores vs trade outcomes)")
    lines.append(f"state dir: {state}")
    lines.append("=" * 74)
    lines.append("")
    lines.append(acceptance.summary_text())
    lines.append("")
    lines.append(review.summary_text())
    lines.append("")
    lines.append("CALIBRATION: does a higher proxy score predict better outcomes?")
    lines.append(_calibration_table("OpportunityScore", review.opportunity_score_buckets))
    lines.append(_calibration_table("ExpectedValue_R", review.ev_buckets))
    lines.append(_calibration_table("VolEdgeRatio", review.vol_edge_buckets))
    if review.skipped_analysis is not None:
        s = review.skipped_analysis
        lines.append("")
        lines.append(f"SKIPPED CANDIDATES: {s.total_skipped} logged, "
                     f"{s.skipped_winner_rate:.2%} would-have-hit-target, "
                     f"{s.no_trade_saved_loss_rate:.2%} would-have-hit-stop")
    lines.append("")
    lines.append("NOTE: rows carry the PROXY scores from paper_signal.py. A flat or")
    lines.append("inverted calibration table is the signal that the proxies need work")
    lines.append("before any live-trading decision. Score buckets with <5 trades are")
    lines.append("not statistically meaningful.")
    return "\n".join(lines)


def _calibration_table(title: str, buckets: Iterable) -> str:
    rows = list(buckets)
    if not rows:
        return f"{title}: no data yet"
    header = f"{title:<16} {'n':>4} {'win%':>7} {'avgR':>7} {'predEV':>7} {'volEdge':>8}"
    body = [header]
    for b in rows:
        body.append(f"{b.bucket:<16} {b.count:>4} {b.win_rate:>7.1%} "
                    f"{b.avg_actual_r:>7.2f} {b.avg_predicted_ev_r:>7.2f} {b.avg_vol_edge_ratio:>8.2f}")
    return "\n".join(body)


def main() -> int:
    import argparse
    ap = argparse.ArgumentParser(description="Generate the paper-evidence phase-2 report")
    ap.add_argument("--state-dir", default="paper_state")
    ap.add_argument("--emergency-tests-passed", action="store_true",
                    help="Mark the emergency-tests check as passed (run the test suite first)")
    args = ap.parse_args()
    text = build_evidence_report(args.state_dir,
                                 emergency_tests_passed=args.emergency_tests_passed)
    out = Path(args.state_dir) / "evidence_report.txt"
    out.write_text(text, encoding="utf-8")
    print(text)
    print(f"\n[wrote {out}]")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
