"""Paper-session evidence collection and phase-2 evidence reports.

The phase-2 dry-run gate (`phase2.DryRunValidator`) and its evidence review
(`phase2.EvidenceAnalyzer`) consume two CSV datasets:

  * `mtil.csv`      - one row per CLOSED paper trade, with the proxy scores at
                      entry plus the realised net PnL and R-multiple.
  * `skipped.csv`   - one row per evaluated-but-not-selected candidate (top-N
                      per ranking cycle), so the gate's "minimum paper trade
                      candidates" and "minimum ranking cycles" can be met even
                      on days when no trade fires.
  * `candidates_log.csv` - one row per evaluated candidate (sampled 1/min),
                      source for the per-day top-N report used to calibrate the
                      excellent-gate threshold.

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
from datetime import datetime
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
    """Writes phase-2 datasets (mtil.csv / skipped.csv / candidates_log.csv)
    from paper sessions."""

    def __init__(self, state_dir: str | Path):
        self.state_dir = Path(state_dir)
        self.mtil = AppendingCsv(self.state_dir / "mtil.csv")
        self.skipped = AppendingCsv(self.state_dir / "skipped.csv")
        self.candidates = AppendingCsv(self.state_dir / "candidates_log.csv")

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

    def record_candidates(self, evaluations: Iterable,
                          ts: Optional[datetime] = None) -> None:
        """One row per evaluated candidate (every grade, not just selected) into
        candidates_log.csv, so a per-day top-N report can be built for threshold
        calibration. The runner calls this once per minute (same cadence as
        record_skipped) to bound file size; minute-level sampling is ample for
        judging where the excellent-gate threshold sits."""
        now = ts or datetime.now()
        for e in evaluations:
            self.candidates.append(self._candidate_row(e, now))

    @staticmethod
    def _candidate_row(e, now: datetime) -> dict[str, Any]:
        c = e.candidate
        try:
            dte = (c.instrument.expiry - now.date()).days
        except Exception:
            dte = ""
        mid = c.quote.mid
        return {
            "ts": now.isoformat(),
            "date": now.date().isoformat(),
            "underlying": c.instrument.underlying,
            "side": c.side.value,
            "strike": c.instrument.strike,
            "expiry": c.instrument.expiry.isoformat(),
            "dte": dte,
            "grade": e.grade.value,
            "comparable_score": round(e.comparable_opportunity_score, 2),
            "opportunity_score": round(e.opportunity_score, 2),
            "threshold": round(e.dynamic_excellent_threshold, 1),
            "eligible": e.eligible,
            "decision": e.decision.value,
            "direction": round(c.instrument_direction_score, 1),
            "trade_quality": round(c.trade_quality_score, 1),
            "market_hostility": round(c.market_hostility_score, 1),
            "iv_crush": round(c.iv_crush_risk_score, 1),
            "convexity": round(c.convexity_edge_score, 1),
            "execution": round(c.execution_quality_score, 1),
            "confidence": round(c.opportunity_confidence_score, 1),
            "regime_fit": round(c.regime_fit_score, 1),
            "premium_elasticity": round(c.premium_elasticity, 3),
            "expected_move": round(c.expected_move, 1),
            "required_move": round(c.required_move, 1),
            "exp_req_ratio": round(c.expected_required_ratio, 2),
            "bid": c.quote.bid,
            "ask": c.quote.ask,
            "mid": round(mid, 2),
            "spread_pct": round(c.quote.spread / mid * 100, 2) if mid > 0 else 99.0,
            "reasons": "; ".join(e.reasons),
        }


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


def build_top_candidates_report(state_dir: str | Path, top_n: int = 10,
                                day: Optional[str] = None) -> str:
    """Top-N candidates per day (by ComparableOpportunityScore) from the
    candidates log written by the paper runner, with per-candidate component
    detail. Purpose: threshold calibration - it shows the score ceiling and
    distribution each day, so the excellent-gate threshold can be judged
    against what the market actually offered instead of being tuned blind."""
    state = Path(state_dir)
    path = state / "candidates_log.csv"
    if not path.exists():
        return ("TOP-CANDIDATES REPORT\n"
                "no candidates_log.csv yet - run the paper runner during a market\n"
                "session first (the runner samples every candidate once per minute).\n")
    rows = []
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        for r in csv.DictReader(f):
            if day is not None and r.get("date") != day:
                continue
            rows.append(r)
    if not rows:
        return f"TOP-CANDIDATES REPORT\nno candidate rows for day={day} in {path}\n"

    def fnum(v: Any) -> float:
        try:
            return float(v)
        except (TypeError, ValueError):
            return 0.0

    by_day: dict[str, list] = {}
    for r in rows:
        by_day.setdefault(r.get("date") or "?", []).append(r)

    lines = [
        "=" * 74,
        f"TOP-{top_n} CANDIDATES BY DAY  (ComparableOpportunityScore, sampled 1/min)",
        f"state dir: {state}",
        "=" * 74,
    ]
    all_scores: list[float] = []
    for d in sorted(by_day):
        day_rows = sorted(by_day[d],
                          key=lambda r: fnum(r.get("comparable_score")), reverse=True)
        scores = [fnum(r.get("comparable_score")) for r in day_rows]
        all_scores.extend(scores)
        n = len(scores)
        mean = sum(scores) / n
        p90 = sorted(scores)[int(n * 0.90) - 1] if n else 0.0
        over80 = sum(1 for s in scores if s >= 80.0)
        lines.append("")
        lines.append(f"DAY {d}   rows={n}  max={max(scores):.1f}  mean={mean:.1f}  "
                     f"p90={p90:.1f}  candidates>=80: {over80}")
        lines.append("  #   score   thr   gr elig und      side strike   expiry    exp/req"
                     "   prem  spread%  decision")
        for rank, r in enumerate(day_rows[:top_n], start=1):
            exp = str(r.get("expiry") or "")[5:10]
            dte = str(r.get("dte") or "?")
            lines.append(
                f"  {rank:>2}  {fnum(r.get('comparable_score')):6.1f}  "
                f"{fnum(r.get('threshold')):5.1f}  {str(r.get('grade')):<2}  "
                f"{str(r.get('eligible')):<4} {str(r.get('underlying')):<9} "
                f"{str(r.get('side')):<2} {fnum(r.get('strike')):>7.0f}  "
                f"{exp}({dte}d)  {fnum(r.get('exp_req_ratio')):6.2f}  "
                f"{fnum(r.get('mid')):7.1f}  {fnum(r.get('spread_pct')):6.2f}  "
                f"{str(r.get('decision'))}")
            lines.append(
                f"      dir={fnum(r.get('direction')):5.1f} "
                f"tq={fnum(r.get('trade_quality')):5.1f} "
                f"host={fnum(r.get('market_hostility')):5.1f} "
                f"conv={fnum(r.get('convexity')):5.1f} "
                f"exec={fnum(r.get('execution')):5.1f} "
                f"conf={fnum(r.get('confidence')):5.1f} "
                f"reg={fnum(r.get('regime_fit')):5.1f}  "
                f"why: {(str(r.get('reasons')) or '-')[:70]}")
    if all_scores:
        amax = max(all_scores)
        ndays = len(by_day)
        days_over80 = sum(
            1 for d in by_day.values()
            if any(fnum(r.get("comparable_score")) >= 80.0 for r in d))
        lines.append("")
        lines.append("-" * 74)
        lines.append("SCORE DISTRIBUTION (all days, comparable score)")
        for lo in range(0, 100, 10):
            cnt = sum(1 for s in all_scores if lo <= s < lo + 10)
            lines.append(f"  {lo:>3}-{lo + 10:>3}: {cnt}")
        lines.append("")
        lines.append("THRESHOLD INSIGHT")
        lines.append(f"  all-time max score: {amax:.1f}")
        lines.append(f"  days with any candidate >= 80: {days_over80} / {ndays}")
        lines.append("  A day's max/p90 below the excellent gate means the threshold may be")
        lines.append("  unreachable in current conditions; a max well above it with no trade")
        lines.append("  suggests the gate or the score components need review.")
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
    ap.add_argument("--top-candidates", action="store_true",
                    help="Also build the per-day top-N candidates report")
    ap.add_argument("--top", type=int, default=10,
                    help="Top-N candidates per day in the top report (default 10)")
    ap.add_argument("--date", default=None,
                    help="Restrict the top-candidates report to one YYYY-MM-DD")
    args = ap.parse_args()
    text = build_evidence_report(args.state_dir,
                                 emergency_tests_passed=args.emergency_tests_passed)
    out = Path(args.state_dir) / "evidence_report.txt"
    out.write_text(text, encoding="utf-8")
    print(text)
    print(f"\n[wrote {out}]")
    if args.top_candidates:
        top_text = build_top_candidates_report(args.state_dir, top_n=args.top, day=args.date)
        top_out = Path(args.state_dir) / "top_candidates_report.txt"
        top_out.write_text(top_text, encoding="utf-8")
        print(f"\n[wrote {top_out}]")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
