"""Live paper trading runner.

Polls the Fyers market-data API (validated, read-only, no orders) and drives
the *real* strategy pipeline each cycle:

    chain -> OptionChainSnapshot -> CandidateFactory (live proxies added)
          -> OpportunityScorer -> PaperOpportunityEngine selection
          -> PaperFillSimulator entry fill
          -> SimulatedTradeLifecycle with ExitPolicy on live bars
          -> journal + shared state for the dashboard

Paper-only by construction: this module never places an order. It only reads
market data and simulates fills through the same conservative paper-fill
model the rest of the repo uses.

Run:  python -m institutional_options.paper_runner
"""
from __future__ import annotations

import csv
import gzip
import hashlib
import json
import math
import os
import shutil
import threading
import time
from dataclasses import asdict, dataclass, field, replace
from types import SimpleNamespace
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Mapping, Optional

from .config import ConfigError, SystemConfig
from .costs import ChargesConfig, CostCalculator, validate_charges_config
from .candidates import CandidateFactory, CandidateFactoryContext
from .engine import PaperOpportunityEngine, PaperPortfolioState
from .fyers_client import FyersCredentials, FyersRestClient, FyersSymbolMaster, TokenStore
from .fyers_parser import FyersOptionChainParser, parse_expiry_calendar, parse_india_vix
from .lifecycle import (
    EXIT_BREAKEVEN, EXIT_END_OF_DATA, EXIT_LOSING_TIME, EXIT_NO_DATA,
    EXIT_STOP, EXIT_TARGET, EXIT_TIME, EXIT_TRAIL, EXIT_VOL_TIME,
    ExitPolicy, MarketBar, SimulatedTradeLifecycle,
)
from .models import DataHealth, OptionType, PaperFill, PaperTrade, Quote, TradeDecision
from .market_metrics import PortfolioNoTradeCalculator
from .observed_metrics import RollingPremiumElasticity
from .option_chain import OptionChainSnapshot, OptionLeg, OptionStrike
from .playbooks import RegimeContext, RegimeLabel, RegimePlaybookSelectionEngine
from .surface_diagnostics import OptionSurfaceDiagnostics
from .paper_evidence import PaperEvidenceCollector
from .paper_signal import PaperSignalCalculator
from .orchestrators import DataHealthOrchestrator
from .operator_controls import load_daily_mode, load_market_context
from .research_controls import (
    InstrumentCalibrationStore, InstrumentLifecycle, PortfolioOverlapGuard,
    PromotionEngine, PromotionMetrics, class_for_metadata, exposure_group, gate_feature_snapshot,
    version_fingerprint,
)
from .research_ledger import ResearchEventLedger, ShadowTradeTracker
from .scoring import CandidateRevalidator, PaperFillSimulator

IST = timezone(timedelta(hours=5, minutes=30))
ACTIVE_EXIT_REASONS = {EXIT_TARGET, EXIT_STOP, EXIT_BREAKEVEN, EXIT_TRAIL, EXIT_TIME, EXIT_LOSING_TIME, EXIT_VOL_TIME}


def now_ist() -> datetime:
    return datetime.now(IST)


@dataclass
class OpenPosition:
    trade: PaperTrade
    symbol: str                 # Fyers symbol of the held option
    underlying: str
    expiry: str
    stop_points: float
    target_points: float
    max_duration_seconds: int
    bars: list[MarketBar] = field(default_factory=list)
    opened_at: datetime = field(default_factory=now_ist)
    last_premium: float = 0.0
    highest_premium: float = 0.0
    lowest_premium: float = 0.0
    last_quote: Optional[Quote] = None


@dataclass
class ClosedTradeRecord:
    trade_id: str
    underlying: str
    side: str
    expiry: str
    strike: float
    entry_time: str
    exit_time: str
    entry_fill: float
    exit_fill: float
    exit_reason: str
    gross_points: float
    gross_pnl: float
    costs: float
    net_pnl: float
    hold_seconds: int
    max_adverse_points: float
    max_favorable_points: float


@dataclass
class RunnerState:
    started_at: str = field(default_factory=lambda: now_ist().isoformat())
    last_cycle: str = ""
    last_cycle_ok: bool = False
    last_error: str = ""
    market_open: bool = False
    open_position: Optional[OpenPosition] = None
    closed_trades: list[ClosedTradeRecord] = field(default_factory=list)
    underlyings: dict[str, Any] = field(default_factory=dict)   # per-underlying display data
    equity: list[float] = field(default_factory=list)
    realized_pnl: float = 0.0
    realized_pnl_today: float = 0.0
    realized_pnl_week: float = 0.0
    trades_today: int = 0
    losses_today: int = 0
    loss_streak_today: int = 0
    last_loss_at: str = ""
    session_id: str = ""


class PaperRunner:
    """Owns the poll loop and shared state; no order placement."""

    def __init__(self, config: SystemConfig, runner_cfg: Mapping[str, Any],
                 state_dir: str | Path = "paper_state",
                 client: Optional[FyersRestClient] = None,
                 master: Optional[FyersSymbolMaster] = None,
                 replay: bool = False):
        self.base_config = config
        self.cfg = runner_cfg
        self.universe = self._universe()
        self.state_dir = Path(state_dir)
        self._prepare_state_directory()
        self._daily_risk_path = self.state_dir / "daily_risk.json"
        self._rank_persistence_path = self.state_dir / "rank_persistence.json"
        self._rank_persistence: dict[str, dict[str, Any]] = self._load_rank_persistence()
        self._daily_risk_date = now_ist().date().isoformat()
        self._risk_week_key = (now_ist().date() - timedelta(days=now_ist().weekday())).isoformat()
        self.state = RunnerState(session_id=now_ist().strftime("%Y%m%d_%H%M%S"))
        self._restore_daily_risk_state(now_ist())
        self._replay = bool(replay)

        # Paper-only config overrides (PARAMETERS.json is never modified).
        # With the frozen risk caps, a real NIFTY/BANKNIFTY ATM premium
        # (required stop = 20% of premium x lot) always exceeds the Rs 750 cap,
        # so no trade can fire. To *watch* the strategy trade in paper mode,
        # set uploads/PAPER_RUNNER.json -> config_overrides. The dashboard shows
        # a PAPER-ONLY OVERRIDE banner whenever these are active.
        self._active_overrides: dict[str, Any] = {}
        self.config = self._overlay_config(config)

        if client is not None:
            self.client = client
        elif self._replay:
            raise ValueError("replay mode requires an injected replay client")
        else:
            creds = FyersCredentials.from_env()
            self.client = FyersRestClient(creds, TokenStore(self.state_dir / "tokens.json"))
            self.client.ensure_session()

        self.signal = PaperSignalCalculator(self.config)
        elasticity_cfg = self.config.section("premium_elasticity")
        self.observed_elasticity = RollingPremiumElasticity(
            window_seconds=float(elasticity_cfg.get("smoothing_window_sec", 60.0)),
            min_underlying_move_points=float(elasticity_cfg.get("min_futures_move_points", 30.0)),
        )
        self.factory = CandidateFactory(self.config)
        self.scorer_engine = None
        self.fill_sim = PaperFillSimulator(self.config)
        self.lifecycle = SimulatedTradeLifecycle(self.fill_sim)
        self.exit_policy = ExitPolicy.from_config(self.config)
        # Phase-2 evidence collection: mtil.csv (closed trades) + skipped.csv.
        self.evidence = None
        self._last_skipped_cycle = 0.0
        self._last_shadow_cycle = 0.0
        self._incident_block_until = 0.0
        self._incident_reason = ""
        self.history_bars = int(self.cfg.get("history_bars", 30))
        self.history_cache: dict[str, list] = {}

        charges_path = Path("uploads/CHARGES_CONFIG.json")
        charges = ChargesConfig.from_file(charges_path)
        self.costs = CostCalculator(charges)
        charges_validation = validate_charges_config(charges_path)
        self._cost_model_valid = bool(charges_validation.valid)
        self._cost_model_status = "COST_MODEL_VALIDATED" if self._cost_model_valid else "COST_MODEL_UNVALIDATED"
        self.state.underlyings["_cost_model"] = {
            "status": self._cost_model_status,
            "canonical_promotion_allowed": self._canonical_promotion_allowed(),
            "reasons": list(charges_validation.reasons),
        }

        self.poll_seconds = float(self.cfg.get("poll_seconds", 5.0))
        self.strikecount = int(self.cfg.get("strikecount", 30))
        self.entry_hold_seconds = int(self.config.section("holding_time")["normal_max_hold_minutes"]) * 60
        monitoring_cfg = self.cfg.get("monitoring", {})
        self.monitor_batch_size = max(1, int(monitoring_cfg.get("monitor_batch_size", 8))) if isinstance(monitoring_cfg, Mapping) else 8
        self.monitor_poll_seconds = max(0.0, float(monitoring_cfg.get("monitor_poll_seconds", 60.0))) if isinstance(monitoring_cfg, Mapping) else 60.0
        self._monitor_cursor = 0
        self._last_monitor_refresh = float("-inf")
        self._last_monitor_batch: list[str] = []
        self._risk_context = self._load_risk_context()
        self._playbook_codes_by_underlying: dict[str, frozenset[str]] = {}
        self._playbook_grades_by_underlying: dict[str, str] = {}
        self.playbook_engine = RegimePlaybookSelectionEngine(
            excellent_threshold=float(self.config.section("opportunity_selection").get("excellent_opportunity_min_score", 80.0))
        )
        if self._active_overrides:
            parameter_profile = "PAPER_OVERRIDE"
        elif isinstance(self.cfg.get("signal"), Mapping):
            parameter_profile = "RUNNER_SIGNAL_CONFIG"
        else:
            parameter_profile = "FROZEN_PARAMETERS"
        evidence_profile = self.config.raw.get("evidence_profiles", {}).get("active_profile")
        if evidence_profile:
            parameter_profile = f"{parameter_profile}::{evidence_profile}"
        self.versions = version_fingerprint(self.config, self.universe, parameter_profile=parameter_profile)
        self.calibration = InstrumentCalibrationStore(self.state_dir, self.config)
        self.promotion = PromotionEngine(self.calibration)
        self.overlap_guard = PortfolioOverlapGuard(enabled=True, block_same_group=True, block_same_underlying=True)
        self.event_ledger = ResearchEventLedger(self.state_dir, self.versions)
        self.evidence = PaperEvidenceCollector(self.state_dir, self.versions)
        self.scorer_engine = PaperOpportunityEngine(self.config, gate_provider=self.calibration.gates_for)
        operator_controls = self.config.raw.get("operator_controls", {})
        if not isinstance(operator_controls, Mapping):
            operator_controls = {}
        self._daily_mode_path = str(operator_controls.get("daily_mode_path", "uploads/DAILY_MODE.txt"))
        self._market_context_path = str(operator_controls.get("market_context_path", "uploads/DAILY_MARKET_CONTEXT.json"))
        computed_mode = self._computed_daily_mode()
        self.daily_mode = load_daily_mode(self._daily_mode_path, computed_mode, now=now_ist())
        self.scorer_engine.set_runtime_mode(self.daily_mode.effective_mode)
        self.state.underlyings["_daily_mode"] = {
            "computed_mode": self.daily_mode.computed_mode,
            "effective_mode": self.daily_mode.effective_mode,
            "status": self.daily_mode.status,
            "reason": self.daily_mode.reason,
            "path": self.daily_mode.path,
        }
        self.event_ledger.append(
            "DAILY_MODE_CONTEXT", session_id=self.state.session_id,
            decision_source="daily_mode_operator_control", ts=now_ist(),
            payload=self.state.underlyings["_daily_mode"],
        )
        self.revalidator = CandidateRevalidator(self.config)
        self.data_health = DataHealthOrchestrator(self.config)
        self.portfolio_no_trade = PortfolioNoTradeCalculator()
        self.shadow_tracker = ShadowTradeTracker(self.fill_sim, max_hold_seconds=self.entry_hold_seconds)
        self.lifecycle_states = {}
        for und, meta in self.universe.items():
            default_state = InstrumentLifecycle.MONITOR if meta.get("monitor_only", False) else InstrumentLifecycle.PAPER_ELIGIBLE
            state = self.calibration.lifecycle_state(und, default_state)
            if not meta.get("monitor_only", False) and state in {InstrumentLifecycle.MONITOR, InstrumentLifecycle.SHADOW}:
                state = InstrumentLifecycle.PAPER_ELIGIBLE
                self.calibration.set_lifecycle_state(und, state)
            self.lifecycle_states[und] = state.value
        if master is None and self._replay:
            # Replay is offline: load every cached exchange master (no auth needed).
            cached_masters = []
            for exchange in self._configured_exchanges():
                cached = self.state_dir / f"{exchange}_FO.csv"
                if cached.exists():
                    cached_masters.append(
                        FyersSymbolMaster.from_csv(
                            cached,
                            allowed_exchanges={exchange},
                            allowed_underlyings=set(self.universe),
                        )
                    )
            if cached_masters:
                master = FyersSymbolMaster.combine(*cached_masters)
        self.master = master if master is not None else self._load_master()
        self._journal_path = self.state_dir / "trades.csv"
        self._write_journal_header()
        # Session capture: every cycle's raw chain/history payloads, gzipped
        # JSONL, for offline replay + parameter sweeps (paper_state/sessions/).
        self._capture = bool(self.cfg.get("capture", False))
        self._capture_file = None
        if self._capture:
            sess_dir = self.state_dir / "sessions"
            sess_dir.mkdir(parents=True, exist_ok=True)
            self._capture_file = gzip.open(sess_dir / f"{self.state.session_id}.jsonl.gz",
                                           "wt", encoding="utf-8")
        trade_universe = [und for und, meta in self.universe.items() if meta.get("trade_enabled", False)]
        monitor_universe = [und for und, meta in self.universe.items() if meta.get("monitor_only", False)]
        self._log(f"Paper selector universe: {', '.join(trade_universe)}; "
                  f"monitor-only: {', '.join(monitor_universe) or '-'}")

    # -- setup -----------------------------------------------------------------

    def _prepare_state_directory(self) -> None:
        """Prevent stale evidence from being mixed with a changed paper policy."""
        self.state_dir.mkdir(parents=True, exist_ok=True)
        management = self.cfg.get("state_management", {})
        if not isinstance(management, Mapping):
            management = {}
        fresh_on_change = bool(management.get("fresh_state_on_policy_change", True))
        fresh_on_legacy = bool(management.get("fresh_state_on_missing_manifest", True))
        universe_payload = {
            "underlyings": sorted(self.universe),
            "trade_enabled": sorted(und for und, meta in self.universe.items() if meta.get("trade_enabled", True)),
            "monitor_only": sorted(und for und, meta in self.universe.items() if meta.get("monitor_only", False)),
            "live_trading_enabled": bool(getattr(self.base_config, "raw", {}).get("execution", {}).get("live_trading_enabled", False)),
        }
        cfg_payload = json.dumps({
            "universe": universe_payload,
            "parameters": getattr(self.base_config, "raw", {}),
            "runner_overrides": self.cfg.get("config_overrides", {}),
            "runner_signal": self.cfg.get("signal", {}),
        }, sort_keys=True, default=str, separators=(",", ":"))
        signature = hashlib.sha256(cfg_payload.encode("utf-8")).hexdigest()[:16]
        manifest_path = self.state_dir / "run_manifest.json"
        previous = None
        if manifest_path.exists():
            try:
                previous = json.loads(manifest_path.read_text(encoding="utf-8"))
            except (OSError, json.JSONDecodeError):
                previous = None
        evidence_names = {
            "mtil.csv", "skipped.csv", "candidates_log.csv", "candidate_diagnostics.csv",
            "shadow_candidates.csv", "shadow_candidate_diagnostics.csv", "monitor_diagnostics.csv",
            "shadow_outcomes.csv", "skipped_forward_queue.csv", "skipped_forward_outcomes.csv", "revalidation_audit.csv", "paper_fill_audit.csv",
            "instrument_calibration.json", "research_events.csv", "run_manifest.json", "master_provenance.json",
            "daily_risk.json", "rank_persistence.json", "runner.log", "runner.err.log",
        }
        has_legacy_evidence = any((self.state_dir / name).exists() for name in evidence_names)
        should_archive = (fresh_on_change and previous is not None and previous.get("policy_signature") != signature) or (fresh_on_legacy and previous is None and has_legacy_evidence)
        if should_archive:
            archive_root = self.state_dir / str(management.get("archive_dir", "archives"))
            archive_root.mkdir(parents=True, exist_ok=True)
            stamp = now_ist().strftime("%Y%m%d_%H%M%S")
            archive_dir = archive_root / f"policy_{stamp}"
            suffix = 1
            while archive_dir.exists():
                archive_dir = archive_root / f"policy_{stamp}_{suffix}"
                suffix += 1
            archive_dir.mkdir(parents=True, exist_ok=False)
            preserved = {archive_root.name, "tokens.json", "creds.env"}
            for child in list(self.state_dir.iterdir()):
                if child.name in preserved:
                    continue
                shutil.move(str(child), str(archive_dir / child.name))
            self._log(f"Archived stale paper evidence to {archive_dir}")
        manifest = {
            "created_at": now_ist().isoformat(),
            "policy_signature": signature,
            "universe": universe_payload,
            "parameter_profile": "PAPER_RUNNER_CONFIG",
            "live_execution": "DISABLED",
            "state_policy": "fresh_state_on_policy_change",
        }
        tmp = manifest_path.with_suffix(".tmp")
        tmp.write_text(json.dumps(manifest, indent=2, sort_keys=True), encoding="utf-8")
        tmp.replace(manifest_path)
        self.run_manifest = manifest

    def _universe(self) -> dict[str, dict[str, str]]:
        cfg_universe = self.cfg.get("underlyings")
        if isinstance(cfg_universe, Mapping):
            out: dict[str, dict[str, str]] = {}
            for und, meta in cfg_universe.items():
                if isinstance(meta, Mapping):
                    exchange = str(meta.get("exchange", "NSE")).upper()
                    if exchange not in {"NSE", "BSE"}:
                        raise ValueError(f"Unsupported Fyers exchange for {und}: {exchange}")
                    out[str(und).upper()] = {
                        "index_symbol": str(meta.get("index_symbol", "")),
                        "exchange": exchange,
                        "prefer_monthly": str(meta.get("prefer_monthly", "false")).lower() == "true",
                        "instrument_kind": str(meta.get("instrument_kind", "INDEX")).upper(),
                        "monitor_only": bool(meta.get("monitor_only", False)),
                        "trade_enabled": bool(meta.get("trade_enabled", not bool(meta.get("monitor_only", False)))),
                    }
            if out:
                return out
        # Defaults (NSE index symbols on Fyers; note BANKNIFTY is NIFTYBANK).
        return {
            "NIFTY": {"index_symbol": "NSE:NIFTY50-INDEX", "exchange": "NSE", "prefer_monthly": "false", "monitor_only": False},
            "BANKNIFTY": {"index_symbol": "NSE:NIFTYBANK-INDEX", "exchange": "NSE", "prefer_monthly": "false", "monitor_only": False},
            "FINNIFTY": {"index_symbol": "NSE:FINNIFTY-INDEX", "exchange": "NSE", "prefer_monthly": "false", "monitor_only": False},
            "MIDCPNIFTY": {"index_symbol": "NSE:MIDCPNIFTY-INDEX", "exchange": "NSE", "prefer_monthly": "false", "monitor_only": False},
        }

    def _configured_exchanges(self) -> tuple[str, ...]:
        return tuple(sorted({meta.get("exchange", "NSE") for meta in self.universe.values()}))

    def _trade_underlyings(self) -> list[str]:
        """Return every configured paper-trade-eligible underlying.

        Live eligibility remains separately frozen in PARAMETERS.json. This
        method represents the revised paper-only policy and therefore does not
        exclude instruments merely because they belong to the expanded research
        universe.
        """
        return [und for und, meta in self.universe.items()
                if bool(meta.get("trade_enabled", not meta.get("monitor_only", False)))]

    def _cycle_underlyings(self) -> list[str]:
        """Fetch core paper names plus a rotating expanded research batch.

        The four original indices remain present on every cycle. The 55 added
        indices/stocks are paper-eligible and rotate through the main selector
        in bounded batches, so all 59 can trade on paper without overwhelming
        the quote endpoint. Any explicitly custom monitor-only names remain in
        the rotating fetch lane for diagnostics/shadow research only.
        """
        all_trade = self._trade_underlyings()
        rotation_cfg = self.cfg.get("monitoring", {})
        rotation_enabled = bool(rotation_cfg.get("paper_trade_rotation_enabled", True)) if isinstance(rotation_cfg, Mapping) else True
        if not rotation_enabled:
            self.state.underlyings["_paper_schedule"] = {
                "mode": "ALL_CONFIGURED_UNDERLYINGS",
                "selected": all_trade,
                "total_paper_underlyings": len(all_trade),
            }
            return all_trade
        core = [und for und in ("NIFTY", "BANKNIFTY", "FINNIFTY", "MIDCPNIFTY") if und in all_trade]
        monitor_only = [und for und, meta in self.universe.items()
                        if bool(meta.get("monitor_only", False)) and und not in core]
        expanded = [und for und in all_trade if und not in core] + monitor_only
        if not expanded:
            return all_trade
        batch_size = max(1, int(rotation_cfg.get("paper_trade_batch_size", self.monitor_batch_size))) if isinstance(rotation_cfg, Mapping) else self.monitor_batch_size
        now_mono = time.monotonic()
        refresh_due = (not self._last_monitor_batch
                       or now_mono - self._last_monitor_refresh >= self.monitor_poll_seconds)
        if not refresh_due:
            self.state.underlyings["_paper_schedule"] = {
                "mode": "CORE_PLUS_ROTATING_EXPANDED",
                "batch_size": batch_size,
                "total_paper_underlyings": len(all_trade),
                "selected": core + self._last_monitor_batch,
                "last_batch": self._last_monitor_batch,
                "next_cursor": self._monitor_cursor,
                "next_refresh_in_sec": round(max(0.0, self.monitor_poll_seconds - (now_mono - self._last_monitor_refresh)), 1),
            }
            return core + self._last_monitor_batch
        batch_size = min(batch_size, len(expanded))
        start = self._monitor_cursor % len(expanded)
        selected = [expanded[(start + i) % len(expanded)] for i in range(batch_size)]
        self._monitor_cursor = (start + batch_size) % len(expanded)
        self._last_monitor_batch = selected
        self._last_monitor_refresh = now_mono
        self.state.underlyings["_paper_schedule"] = {
            "mode": "CORE_PLUS_ROTATING_EXPANDED",
            "batch_size": batch_size,
            "total_paper_underlyings": len(all_trade),
            "selected": core + selected,
            "last_batch": selected,
            "next_cursor": self._monitor_cursor,
            "next_refresh_in_sec": self.monitor_poll_seconds,
        }
        return core + selected

    def _load_master(self) -> FyersSymbolMaster:
        masters = []
        for exchange in self._configured_exchanges():
            master_path = self.state_dir / f"{exchange}_FO.csv"
            path = self.client.fetch_symbol_master(master_path, exchange=exchange)
            masters.append(FyersSymbolMaster.from_csv(
                path,
                allowed_exchanges={exchange},
                allowed_underlyings=set(self.universe),
            ))
        master = FyersSymbolMaster.combine(*masters)
        self._record_master_provenance(master)
        for und, meta in self.universe.items():
            exps = master.expiry_dates(und)
            mode = "MONITOR_ONLY" if meta.get("monitor_only") else ("TRADE_ELIGIBLE" if meta.get("trade_enabled", True) else "DISABLED")
            self._log(f"  {und} [{meta.get('exchange', 'NSE')}, {mode}]: {len(exps)} expiries, nearest {exps[0] if exps else '-'}")
        return master

    def _record_master_provenance(self, master: FyersSymbolMaster) -> None:
        """Persist exact master-file hashes and per-instrument metadata coverage."""
        files = []
        for exchange in self._configured_exchanges():
            path = self.state_dir / f"{exchange}_FO.csv"
            if not path.exists():
                continue
            stat = path.stat()
            files.append({
                "exchange": exchange,
                "path": str(path),
                "sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
                "size_bytes": stat.st_size,
                "modified_at": datetime.fromtimestamp(stat.st_mtime, IST).isoformat(),
            })
        coverage = {}
        for underlying in sorted(self.universe):
            instruments = [item for item in master.instruments if item.underlying.upper() == underlying.upper()]
            coverage[underlying] = {
                "contract_rows": len(instruments),
                "expiry_count": len(master.expiry_dates(underlying)),
                "has_ce": any(item.option_type == "CE" for item in instruments),
                "has_pe": any(item.option_type == "PE" for item in instruments),
                "lot_sizes": sorted({int(item.lot_size) for item in instruments}),
                "tick_sizes": sorted({float(item.tick_size) for item in instruments}),
            }
        payload = {"captured_at": now_ist().isoformat(), "files": files, "coverage": coverage}
        out = self.state_dir / "master_provenance.json"
        tmp = out.with_suffix(".tmp")
        tmp.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
        tmp.replace(out)
        self.run_manifest["master_provenance"] = payload
        manifest_path = self.state_dir / "run_manifest.json"
        manifest_tmp = manifest_path.with_suffix(".tmp")
        manifest_tmp.write_text(json.dumps(self.run_manifest, indent=2, sort_keys=True), encoding="utf-8")
        manifest_tmp.replace(manifest_path)

    # -- main loop ---------------------------------------------------------------

    def run_forever(self, stop_event: Optional[threading.Event] = None) -> None:
        self._log("Loop started.")
        try:
            while stop_event is None or not stop_event.is_set():
                try:
                    self.run_one_cycle()
                except Exception as e:  # keep the loop alive, but block new entries during stabilization
                    self.state.last_cycle_ok = False
                    self.state.last_error = f"{type(e).__name__}: {e}"
                    self._incident_reason = f"Cycle failure: {self.state.last_error}"
                    stable_wait = float(self.config.section("data_health").get("reconnect_stable_wait_sec", 30.0))
                    self._incident_block_until = time.time() + max(0.0, stable_wait)
                    self._log(f"Cycle error; new entries blocked for {stable_wait:.1f}s: {self.state.last_error}")
                wait = self.poll_seconds if self.state.market_open else self._seconds_to_open()
                if wait > 0:
                    time.sleep(min(wait, self.poll_seconds if self.state.market_open else 60.0))
        finally:
            if self._capture_file is not None:
                try:
                    self._capture_file.close()
                except Exception:
                    pass

    def run_one_cycle(self) -> None:
        now = now_ist()
        self._roll_daily_risk_state(now)
        self._risk_context = self._load_risk_context()
        self._refresh_daily_controls(now)
        self.state.market_open = self._market_open(now)
        if not self.state.market_open:
            if self.state.open_position is not None and now.weekday() < 5 and now.hour * 60 + now.minute > 15 * 60 + 30:
                self._force_close_end_of_day(now)
            self.state.last_cycle = now.isoformat()
            return
        chains: dict[str, OptionChainSnapshot] = {}
        vix_map: dict[str, Optional[float]] = {}
        context_map = {}
        payloads: dict[str, Any] = {}
        histories: dict[str, Any] = {}
        depth_payloads: dict[str, Any] = {}
        self._depth_payloads = depth_payloads
        for und in self._cycle_underlyings():
            meta = self.universe[und]
            try:
                payload = self.client.option_chain(meta["index_symbol"], self.strikecount)
                cal = parse_expiry_calendar(payload)
                expiry = self._select_expiry(und, cal, bool(meta.get("prefer_monthly", False)))
                chain = FyersOptionChainParser.parse(payload, und, expiry, now)
                chain = self._enrich_fyers_depth(und, chain, payload)
                vix = parse_india_vix(payload)
                chains[und] = chain
                vix_map[und] = vix
                history = self._fetch_history(und, meta["index_symbol"])
                histories[und] = history
                payloads[und] = payload
                direction_inputs = self._direction_model_histories(und)
                context_map[und] = self.signal.compute_context(
                    chain, vix, now, history_candles=history,
                    direction_model_inputs=direction_inputs,
                )
            except Exception as e:
                detail = f"{type(e).__name__}: {e}"
                reason_code = "OPTIONS_CHAIN_UNAVAILABLE" if "optionsChain" in str(e) else "OPTIONS_CHAIN_ERROR"
                self.state.underlyings[und] = {
                    "error": detail,
                    "chain_health": {
                        "source": "FYERS_OPTIONS_CHAIN",
                        "status": "UNAVAILABLE",
                        "reason_code": reason_code,
                        "detail": detail,
                        "fail_closed": True,
                    },
                }
                self._log(f"  {und} chain unavailable [{reason_code}]: {detail}")
        if not chains:
            raise RuntimeError("No underlying chains fetched this cycle.")
        if self._capture_file is not None:
            self._capture_cycle(payloads, histories, now, depth_payloads)
        self._update_playbook_filters(context_map, now)
        self.state.underlyings["_risk_context"] = dict(self._risk_context)
        for und, chain in chains.items():
            meta = self.universe.get(und, {})
            if meta.get("monitor_only", False):
                try:
                    instrument_kind = meta.get("instrument_kind", "INDEX")
                    exchange = meta.get("exchange", "NSE")
                    monitor_lot = None
                    try:
                        monitor_expiry = date.fromisoformat(chain.expiry[:10])
                        monitor_lot = self.master.lot_size(und, monitor_expiry)
                    except Exception:
                        pass
                    self.evidence.record_monitor_snapshot(
                        underlying=und,
                        exchange=exchange,
                        chain=chain,
                        context=context_map[und],
                        vix=vix_map.get(und),
                        instrument_kind=instrument_kind,
                        lot_size=monitor_lot,
                        lifecycle_state=self.lifecycle_states.get(und, InstrumentLifecycle.MONITOR.value),
                        ts=now,
                    )
                    self._observe_monitor_calibration(und, chain, meta, now)
                    self.event_ledger.append(
                        "MONITOR_SNAPSHOT", session_id=self.state.session_id,
                        underlying=und, exchange=exchange, instrument_kind=instrument_kind,
                        instrument_class=class_for_metadata(exchange, instrument_kind),
                        lifecycle_state=self.lifecycle_states.get(und, InstrumentLifecycle.MONITOR.value),
                        decision_source="monitor_diagnostics", ts=now,
                        payload={"expiry": chain.expiry, "strike_count": len(chain.strikes)},
                    )
                except Exception as e:
                    self._log(f"  monitor diagnostics failed for {und}: {e}")
            else:
                try:
                    self._observe_monitor_calibration(und, chain, meta, now)
                except Exception as e:
                    self._log(f"  trade-universe calibration observation failed for {und}: {e}")

        self._refresh_lifecycle_states()
        self._update_chain_display(chains, vix_map, context_map)
        self._record_shadow_cycle(chains, context_map, now)

        # Manage the open position with fresh bars before considering new entries.
        # Existing positions remain managed after the entry window closes.
        self._manage_open_position(chains, context_map)

        # Build candidates from all fresh chains and select only inside the
        # configured entry window.  Outside the window we still fetch data and
        # manage an open position, but do not create new exposure.
        if self.state.open_position is None and self._entry_window_open(now):
            self.state.underlyings.pop("_entry_window", None)
            self._select_and_enter(chains, context_map)
        elif self.state.open_position is None:
            self.state.underlyings["_entry_window"] = {
                "status": "CLOSED",
                "reason": "New entries disabled outside configured entry window",
                "timestamp": now.isoformat(),
            }
        try:
            forward_rows = self.evidence.observe_skipped_forward(chains, now)
            self.calibration.record_forward_outcomes(forward_rows, cost_model_valid=self._cost_model_valid)
        except Exception as e:
            self._log(f"  skipped forward observation failed: {e}")

        self.state.last_cycle = now_ist().isoformat()
        self.state.last_cycle_ok = True
        self._update_equity()

    # -- candidate building + selection -----------------------------------------

    def _record_data_health_observation(self, underlying: str, health: DataHealth, now: datetime, scope: str) -> None:
        """Persist stale/invalid transitions so repeated feed failures are visible."""
        target = self.state.underlyings.setdefault(underlying, {})
        prior = dict(target.get("stale_data_alert", {}))
        threshold = max(1, int(self.config.section("data_health").get("stale_alert_consecutive_cycles", 2)))
        was_alert = str(prior.get("status", "CLEAR")) == "ALERT"
        if health.valid and not health.warning:
            alert = {
                "status": "CLEAR",
                "consecutive_bad_cycles": 0,
                "last_valid_at": now.isoformat(),
                "last_bad_at": prior.get("last_bad_at", ""),
                "last_reason": health.reason or "",
                "scope": scope,
            }
        else:
            consecutive = int(prior.get("consecutive_bad_cycles", 0) or 0) + 1
            status = "ALERT" if consecutive >= threshold else "OBSERVING"
            alert = {
                "status": status,
                "consecutive_bad_cycles": consecutive,
                "alert_threshold_cycles": threshold,
                "last_valid_at": prior.get("last_valid_at", ""),
                "last_bad_at": now.isoformat(),
                "last_reason": health.reason or "Data health warning",
                "scope": scope,
            }
            if status == "ALERT" and not was_alert:
                self._log(f"  DATA HEALTH ALERT {underlying}: {alert['last_reason']}")
                self.event_ledger.append(
                    "DATA_HEALTH_ALERT", session_id=self.state.session_id,
                    underlying=underlying,
                    exchange=self.universe.get(underlying, {}).get("exchange", "NSE"),
                    instrument_kind=self.universe.get(underlying, {}).get("instrument_kind", "INDEX"),
                    instrument_class=class_for_metadata(
                        self.universe.get(underlying, {}).get("exchange", "NSE"),
                        self.universe.get(underlying, {}).get("instrument_kind", "INDEX"),
                    ),
                    decision_source="data_health_orchestrator", ts=now,
                    payload=alert,
                )
        target["stale_data_alert"] = alert

    def _enrich_fyers_depth(self, underlying: str, chain: OptionChainSnapshot, payload: Mapping[str, Any]) -> OptionChainSnapshot:
        """Merge read-only Fyers depth into the bounded candidate strikes.

        The option-chain endpoint supplies broad strike coverage but omits
        quantities.  Fyers' separate `/data/depth` endpoint supplies the exact
        symbol's level-one and five-level book.  We request depth only for the
        nearest three strikes (the same bounded set used by CandidateFactory),
        preserving all-59 chain collection while avoiding an unbounded request
        fan-out.  Any missing or malformed depth remains invalid rather than
        being inferred.
        """
        depth_method = getattr(self.client, "market_depth", None)
        if not callable(depth_method):
            self.state.underlyings.setdefault(underlying, {})["depth_health"] = {
                "source": "FYERS_MARKET_DEPTH",
                "status": "UNAVAILABLE",
                "reason": "Depth client method unavailable",
                "requested_legs": 0,
                "successful_legs": 0,
                "failed_legs": 0,
                "five_level_legs": 0,
            }
            return chain
        raw_data = payload.get("data", {}) if isinstance(payload, Mapping) else {}
        raw_rows = raw_data.get("optionsChain", []) if isinstance(raw_data, Mapping) else []
        symbols: dict[tuple[float, str], str] = {}
        for raw in raw_rows:
            if not isinstance(raw, Mapping):
                continue
            option_type = str(raw.get("option_type", "")).upper()
            raw_symbol = str(raw.get("symbol", "")).strip()
            try:
                strike = float(raw.get("strike_price"))
            except (TypeError, ValueError):
                continue
            if option_type in {"CE", "PE"} and strike > 0 and raw_symbol:
                symbols[(strike, option_type)] = raw_symbol
        target_strikes = set(sorted((s.strike for s in chain.strikes), key=lambda strike: abs(strike - chain.nearest_strike()))[:3])
        requested = successful = five_level = failed = 0
        rate_limit_errors = api_errors = 0
        last_error = ""
        failure_reasons: list[str] = []

        def _levels(raw: Any, side: str) -> list[dict[str, float]]:
            if not isinstance(raw, list) or not raw:
                raise ValueError(f"Fyers depth {side} levels missing")
            out: list[dict[str, float]] = []
            for index, item in enumerate(raw):
                if not isinstance(item, Mapping):
                    raise ValueError(f"Fyers depth {side}[{index}] malformed")
                try:
                    price = float(item.get("price", 0.0))
                    volume = float(item.get("volume", 0.0))
                except (TypeError, ValueError) as exc:
                    raise ValueError(f"Fyers depth {side}[{index}] numeric fields invalid") from exc
                if not math.isfinite(price) or not math.isfinite(volume) or volume < 0:
                    raise ValueError(f"Fyers depth {side}[{index}] numeric fields invalid")
                out.append({"price": price, "volume": volume})
            return out

        def _optional_int(raw: Any, fallback: int) -> int:
            if raw in (None, ""):
                return fallback
            try:
                value = float(raw)
                return int(value) if math.isfinite(value) and value >= 0 else fallback
            except (TypeError, ValueError):
                return fallback

        def _optional_float(raw: Any, fallback: Optional[float]) -> Optional[float]:
            if raw in (None, ""):
                return fallback
            try:
                value = float(raw)
                return value if math.isfinite(value) else fallback
            except (TypeError, ValueError):
                return fallback

        enriched_strikes: list[OptionStrike] = []
        for strike in chain.strikes:
            if strike.strike not in target_strikes:
                enriched_strikes.append(strike)
                continue
            legs: list[tuple[str, Optional[OptionLeg]]] = [("CE", strike.ce), ("PE", strike.pe)]
            new_legs: dict[str, Optional[OptionLeg]] = {}
            for side, leg in legs:
                if leg is None:
                    new_legs[side] = None
                    continue
                symbol = symbols.get((strike.strike, side), "")
                if not symbol:
                    new_legs[side] = leg
                    failed += 1
                    failure_reasons.append(f"{side}@{strike.strike}: depth symbol missing")
                    continue
                requested += 1
                try:
                    depth_payload = depth_method(symbol, ohlcv_flag=1)
                    if hasattr(self, "_depth_payloads"):
                        self._depth_payloads[symbol] = depth_payload
                    depth_data = depth_payload.get("d", {}) if isinstance(depth_payload, Mapping) else {}
                    depth = depth_data.get(symbol, {}) if isinstance(depth_data, Mapping) else {}
                    if not isinstance(depth, Mapping):
                        raise ValueError("Fyers depth symbol payload missing")
                    bids = _levels(depth.get("bids", []), "bid")
                    asks = _levels(depth.get("ask", depth.get("asks", [])), "ask")
                    best_bid = bids[0]
                    best_ask = asks[0]
                    bid = best_bid["price"]
                    ask = best_ask["price"]
                    bid_qty = int(best_bid["volume"])
                    ask_qty = int(best_ask["volume"])
                    if bid <= 0 or ask <= 0 or ask <= bid or bid_qty <= 0 or ask_qty <= 0:
                        raise ValueError("Fyers depth best quote invalid")
                    ltt = depth.get("ltt")
                    if ltt in (None, ""):
                        raise ValueError("Fyers depth source timestamp missing")
                    ltt_seconds = float(ltt)
                    if not math.isfinite(ltt_seconds) or ltt_seconds <= 0:
                        raise ValueError("Fyers depth source timestamp invalid")
                    depth_timestamp = datetime.fromtimestamp(ltt_seconds, timezone.utc)
                    valid_five_bid = len(bids) >= 5 and all(level["price"] > 0 and level["volume"] > 0 for level in bids[:5])
                    valid_five_ask = len(asks) >= 5 and all(level["price"] > 0 and level["volume"] > 0 for level in asks[:5])
                    if valid_five_bid and valid_five_ask:
                        cumulative_bid = int(sum(level["volume"] for level in bids[:5]))
                        cumulative_ask = int(sum(level["volume"] for level in asks[:5]))
                        five_level += 1
                    else:
                        cumulative_bid = cumulative_ask = None
                    last = _optional_float(depth.get("ltp"), leg.quote.last)
                    quote = replace(
                        leg.quote,
                        bid=bid,
                        ask=ask,
                        bid_qty=bid_qty,
                        ask_qty=ask_qty,
                        last=last,
                        timestamp=depth_timestamp,
                        cumulative_bid_qty_5depth=cumulative_bid,
                        cumulative_ask_qty_5depth=cumulative_ask,
                        source_timestamp_available=True,
                    )
                    depth_oi = _optional_int(depth.get("oi"), leg.oi)
                    depth_volume = _optional_int(depth.get("v"), leg.volume)
                    new_legs[side] = replace(
                        leg,
                        quote=quote,
                        source_timestamp=depth_timestamp,
                        oi=depth_oi,
                        volume=depth_volume,
                    )
                    successful += 1
                except Exception as exc:
                    failed += 1
                    if getattr(exc, "status_code", None) == 429:
                        rate_limit_errors += 1
                    else:
                        api_errors += 1
                    last_error = f"{type(exc).__name__}: {str(exc)[:180]}"
                    failure_reasons.append(f"{symbol}: {last_error}")
                    new_legs[side] = leg
                    self._log(f"  {underlying} {symbol} depth unavailable: {last_error}")
            enriched_strikes.append(OptionStrike(strike.strike, new_legs["CE"], new_legs["PE"]))
        client_stats = {}
        stats_method = getattr(self.client, "depth_stats", None)
        if callable(stats_method):
            try:
                client_stats = stats_method()
            except Exception:
                client_stats = {}
        status = "APPLIED" if successful and not failed else "PARTIAL" if successful else "UNAVAILABLE"
        self.state.underlyings.setdefault(underlying, {})["depth_health"] = {
            "source": "FYERS_MARKET_DEPTH",
            "status": status,
            "requested_legs": requested,
            "successful_legs": successful,
            "failed_legs": failed,
            "five_level_legs": five_level,
            "rate_limit_errors": rate_limit_errors,
            "api_errors": api_errors,
            "last_error": last_error,
            "failure_reasons": failure_reasons[-20:],
            "client_stats": client_stats,
            "candidate_strikes": sorted(target_strikes),
        }
        return replace(chain, strikes=tuple(enriched_strikes))

    def _build_candidates(self, chains, context_map, scope: str = "trade") -> list:
        if scope not in {"trade", "monitor"}:
            raise ValueError(f"Unsupported candidate build scope: {scope}")
        out = []
        if scope == "trade":
            self.state.underlyings["_prefiltered"] = 0
        for und, chain in chains.items():
            meta = self.universe.get(und, {})
            is_monitor = bool(meta.get("monitor_only", False))
            is_trade_enabled = bool(meta.get("trade_enabled", not is_monitor))
            lifecycle_state = self.lifecycle_states.get(
                und, InstrumentLifecycle.MONITOR.value if is_monitor else InstrumentLifecycle.TRADE_ELIGIBLE.value
            )
            if scope == "trade" and (not is_trade_enabled or lifecycle_state in {InstrumentLifecycle.MONITOR.value, InstrumentLifecycle.SHADOW.value, InstrumentLifecycle.RETIRED.value}):
                continue
            if scope == "monitor" and (not is_monitor or lifecycle_state == InstrumentLifecycle.RETIRED.value):
                continue
            ctx = context_map[und]
            chain_health = self.data_health.evaluate_option_chain(chain, now_ist())
            try:
                expiry = date.fromisoformat(chain.expiry[:10])
            except (ValueError, TypeError):
                self.state.underlyings.setdefault(und, {})["instrument_error"] = "Invalid chain expiry; candidate blocked"
                continue
            try:
                lot = self.master.lot_size(und, expiry)
                tick = self.master.tick_size(und, expiry)
            except Exception as e:
                self.state.underlyings.setdefault(und, {})["instrument_error"] = f"Missing master metadata: {e}"
                continue
            instrument_class = class_for_metadata(meta.get("exchange", "NSE"), meta.get("instrument_kind", "INDEX"))
            lifecycle_state = self.lifecycle_states.get(
                und, InstrumentLifecycle.MONITOR.value if is_monitor else InstrumentLifecycle.TRADE_ELIGIBLE.value
            )
            cctx = CandidateFactoryContext(
                futures_price=chain.underlying_price,
                spot_price=chain.underlying_price,
                instrument_direction_score=ctx.direction_score,
                trade_quality_score=ctx.trade_quality_score,
                regime_confidence=ctx.regime_confidence,
                market_hostility_score=ctx.market_hostility_score,
                atr_remaining_move=ctx.atr_remaining_move,
                regime_projected_move=ctx.regime_projected_move,
                required_move=ctx.required_move,
                dte=ctx.dte,
                calibration_direction=ctx.calibration_direction,
                calibration_liquidity=ctx.calibration_liquidity,
                exchange=meta.get("exchange", "NSE"),
                instrument_kind=meta.get("instrument_kind", "INDEX"),
                instrument_class=instrument_class,
                lifecycle_state=lifecycle_state,
                data_health=chain_health,
            )
            cands = self.factory.candidates_from_chain(chain, expiry, lot, tick, cctx)
            candidate_health_failures = 0
            candidate_health_reasons: list[str] = []
            setup_codes = self._playbook_codes_by_underlying.get(und)
            setup_grade = self._playbook_grades_by_underlying.get(und, "")
            if scope == "trade" and self.config.section("playbook_runtime").get("enforce_on_paper", False) and not setup_codes:
                continue
            setup_type = next(iter(setup_codes), "UNKNOWN") if setup_codes else "UNKNOWN"
            surface = OptionSurfaceDiagnostics.calculate(chain)
            for c in cands:
                c = replace(c, setup_type=setup_type, setup_grade=setup_grade)
                candidate_health = self.data_health.evaluate_candidate(c, now_ist())
                if not candidate_health.valid or candidate_health.warning:
                    candidate_health_failures += 1
                    if candidate_health.reason:
                        candidate_health_reasons.append(candidate_health.reason)
                combined_health = DataHealth(
                    valid=chain_health.valid and candidate_health.valid,
                    warning=chain_health.warning or candidate_health.warning,
                    reason="; ".join(reason for reason in (chain_health.reason, candidate_health.reason) if reason),
                )
                c = replace(c, data_health=combined_health)
                spread_pct = c.quote.spread / c.quote.mid * 100.0 if c.quote.mid > 0 else 99.0
                proxies = self.signal.candidate_proxies(chain, ctx, c.moneyness, c.side, spread_pct)
                elasticity = self.observed_elasticity.update(
                    key=(und, c.instrument.expiry, c.instrument.strike, c.side.value),
                    timestamp=c.quote.timestamp,
                    underlying_price=chain.underlying_price,
                    quote=c.quote,
                    side=c.side,
                )
                notes = dict(c.notes)
                active_gates = self.calibration.gates_for(instrument_class, und)
                notes.update({
                    "gate_snapshot_id": active_gates.gate_snapshot_id,
                    "gate_learning_status": active_gates.gate_learning_status,
                    "gate_learning_observations": active_gates.gate_learning_observations,
                    "gate_learning_sessions": active_gates.gate_learning_sessions,
                    "gate_learning_outcomes": active_gates.gate_learning_outcomes,
                    "gate_contract_quality_min": active_gates.contract_quality_min,
                    "gate_direction_min": active_gates.direction_min,
                    "gate_premium_elasticity_min": active_gates.premium_elasticity_min,
                    "gate_expected_required_ratio_min": active_gates.expected_required_ratio_min,
                    "gate_trade_quality_min": active_gates.trade_quality_min,
                    "gate_final_confidence_min": active_gates.final_confidence_min,
                    "gate_market_hostility_max": active_gates.market_hostility_max,
                    "gate_iv_crush_max": active_gates.iv_crush_max,
                    "gate_spread_pct_max": active_gates.spread_pct_max,
                    "gate_min_top_book_lots": active_gates.min_top_book_lots,
                    "gate_min_5depth_lots_each_side": active_gates.min_5depth_lots_each_side,
                    "depth_evidence": (
                        "FIVE_LEVEL" if c.quote.cumulative_bid_qty_5depth is not None and c.quote.cumulative_ask_qty_5depth is not None
                        else "TOP_BOOK_ONLY" if c.quote.bid_qty > 0 and c.quote.ask_qty > 0
                        else "UNAVAILABLE"
                    ),
                    "depth_source": "FYERS_MARKET_DEPTH" if c.quote.source_timestamp_available else "UNAVAILABLE",
                    "depth_bid_levels": 5 if c.quote.cumulative_bid_qty_5depth is not None else 0,
                    "depth_ask_levels": 5 if c.quote.cumulative_ask_qty_5depth is not None else 0,
                    "gate_resolution_path": active_gates.gate_resolution_path,
                    "gate_optimization_method": active_gates.gate_optimization_method,
                    "gate_optimization_status": active_gates.gate_optimization_status,
                    "gate_optimization_quantile": active_gates.gate_optimization_quantile,
                    "gate_validation_observations": active_gates.gate_validation_observations,
                    "gate_validation_sessions": active_gates.gate_validation_sessions,
                    "gate_validation_expectancy_r": active_gates.gate_validation_expectancy_r,
                    "gate_validation_drawdown_r": active_gates.gate_validation_drawdown_r,
                    "gate_validation_retention": active_gates.gate_validation_retention,
                    "gate_last_validated_at": active_gates.gate_last_validated_at,
                    "setup_grade": setup_grade or "UNAVAILABLE",
                    "setup_grade_source": "PLAYBOOK_METADATA" if setup_grade else "SCORE_FALLBACK",
                })
                notes.update({
                    "observed_elasticity_valid": str(elasticity.valid),
                    "observed_elasticity_raw": "" if elasticity.raw_elasticity is None else f"{elasticity.raw_elasticity:.8f}",
                    "observed_elasticity_post_cost": "" if elasticity.post_cost_elasticity is None else f"{elasticity.post_cost_elasticity:.8f}",
                    "observed_elasticity_reason": elasticity.reason,
                    "surface_valid": str(surface.valid),
                    "atm_iv": "" if surface.atm_iv is None else f"{surface.atm_iv:.6f}",
                    "call_put_iv_skew": "" if surface.call_put_iv_skew is None else f"{surface.call_put_iv_skew:.6f}",
                    "call_wing_iv": "" if surface.call_wing_iv is None else f"{surface.call_wing_iv:.6f}",
                    "put_wing_iv": "" if surface.put_wing_iv is None else f"{surface.put_wing_iv:.6f}",
                    "surface_reason": surface.reason,
                    "direction_model_score": "" if ctx.direction_model_score is None else f"{ctx.direction_model_score:.4f}",
                    "direction_model_name": ctx.direction_model_name,
                    "direction_model_status": ctx.direction_model_status,
                    "direction_model_disagreement": "" if ctx.direction_model_disagreement is None else f"{ctx.direction_model_disagreement:.4f}",
                    "data_health_valid": str(c.data_health.valid),
                    "data_health_warning": str(c.data_health.warning),
                    "data_health_reason": c.data_health.reason,
                    "evidence_profile": str(self.config.raw.get("evidence_profiles", {}).get("active_profile", "UNSPECIFIED")),
                    "elasticity_status": str(self.config.raw.get("evidence_profiles", {}).get("elasticity_status", "UNSPECIFIED")),
                    "mapping_status": str(self.config.raw.get("evidence_profiles", {}).get("mapping_status", "UNSPECIFIED")),
                    "cost_model_status": self._cost_model_status,
                    "cost_model_valid": self._cost_model_valid,
                    "canonical_promotion_allowed": self._canonical_promotion_allowed(),
                    "liquidity_data_status": "MEASURED" if c.quote.bid_qty > 0 and c.quote.ask_qty > 0 and (c.quote.cumulative_bid_qty_5depth or 0) > 0 and (c.quote.cumulative_ask_qty_5depth or 0) > 0 else "LIQUIDITY_UNAVAILABLE",
                    "iv_data_status": "MEASURED" if c.greeks.iv is not None else "IV_UNAVAILABLE",
                    "iv_context_status": self.factory.market_context.status,
                    "iv_context_reason": self.factory.market_context.reason,
                    "iv_context_source": self.factory.market_context.source,
                    "iv_context_as_of": self.factory.market_context.as_of,
                    "iv_context_expires_at": self.factory.market_context.expires_at,
                })
                proxy_score = 0.5 * c.trade_quality_score + 0.5 * proxies.opportunity_confidence_score
                calibrated_probability, calibrated_expectancy, _ = self.calibration.outcome_calibration(
                    instrument_class, proxy_score
                )
                c = replace(c,
                    premium_elasticity=proxies.premium_elasticity,
                    convexity_edge_score=proxies.convexity_edge_score,
                    execution_quality_score=proxies.execution_quality_score,
                    opportunity_confidence_score=proxies.opportunity_confidence_score,
                    regime_fit_score=proxies.regime_fit_score,
                    calibrated_success_probability=calibrated_probability,
                    calibrated_net_expectancy_r=calibrated_expectancy,
                    notes=notes,
                )

                # Spread-cost pre-filter: skip strikes the conservative paper
                # fill model would refuse at entry (wide OR pathologically tight
                # spreads), so un-fillable candidates never waste an evaluation
                # or selection cycle. Uses the real simulator -> exact match.
                probe = self.fill_sim.entry_buy(c.quote, c.instrument.tick_size)
                if not probe.filled:
                    if scope == "trade":
                        self.state.underlyings["_prefiltered"] = \
                            int(self.state.underlyings.get("_prefiltered", 0)) + 1
                    continue
                out.append(c)
            if candidate_health_failures or not chain_health.valid or chain_health.warning:
                health_for_alert = DataHealth(
                    valid=chain_health.valid and candidate_health_failures == 0,
                    warning=chain_health.warning or candidate_health_failures > 0,
                    reason="; ".join(reason for reason in (chain_health.reason, *candidate_health_reasons) if reason),
                )
            else:
                health_for_alert = chain_health
            self._record_data_health_observation(und, health_for_alert, now_ist(), scope)
        return out

    def _refresh_lifecycle_states(self) -> None:
        """Apply measured promotion rules without mutating the frozen trade universe."""
        for underlying, meta in self.universe.items():
            if not bool(meta.get("trade_enabled", not meta.get("monitor_only", False))):
                continue
            current = InstrumentLifecycle(self.lifecycle_states.get(underlying, InstrumentLifecycle.MONITOR.value))
            instrument_class = class_for_metadata(meta.get("exchange", "NSE"), meta.get("instrument_kind", "INDEX"))
            raw = self.calibration.instrument_metrics(underlying)
            metrics = PromotionMetrics(
                observations=raw["observations"], sessions=raw["sessions"],
                valid_quote_rate=raw["valid_quote_rate"], paper_fill_rate=raw["paper_fill_rate"],
                shadow_outcomes=raw["shadow_outcomes"], shadow_net_expectancy_r=raw["shadow_net_expectancy_r"],
                paper_trades=raw["paper_trades"], paper_net_expectancy_r=raw["paper_net_expectancy_r"],
                max_drawdown_r=raw["max_drawdown_r"],
            )
            decision = self.promotion.evaluate(instrument_class, current, metrics, monitor_only=bool(meta.get("monitor_only", False)))
            self.state.underlyings.setdefault(underlying, {})["promotion"] = {
                "current_state": decision.current_state.value,
                "recommended_state": decision.recommended_state.value,
                "allowed": decision.allowed,
                "trade_review_ready": decision.trade_review_ready,
                "reasons": list(decision.reasons),
                "metrics": raw,
            }
            if decision.recommended_state != current and decision.allowed:
                self.lifecycle_states[underlying] = decision.recommended_state.value
                self.calibration.set_lifecycle_state(underlying, decision.recommended_state)
                self.event_ledger.append(
                    "LIFECYCLE_TRANSITION", session_id=self.state.session_id,
                    underlying=underlying, exchange=meta.get("exchange", "NSE"),
                    instrument_kind=meta.get("instrument_kind", "INDEX"),
                    instrument_class=instrument_class,
                    lifecycle_state=decision.recommended_state.value,
                    decision_source="measured_promotion_engine", payload={
                        "from": current.value, "to": decision.recommended_state.value,
                        "reasons": decision.reasons,
                    },
                )

    def _observe_monitor_calibration(self, underlying: str, chain: OptionChainSnapshot,
                                     meta: Mapping[str, Any], now: datetime) -> None:
        """Measure class-level quote validity, spread, depth, freshness, and fills."""
        instrument_class = class_for_metadata(meta.get("exchange", "NSE"), meta.get("instrument_kind", "INDEX"))
        try:
            expiry = date.fromisoformat(chain.expiry[:10])
            lot = self.master.lot_size(underlying, expiry)
        except Exception:
            self.calibration.record_observation(instrument_class, now, False, False, None, None, None, stale=True, instrument_id=underlying)
            return
        atm = chain.nearest_strike()
        legs = []
        for side in (OptionType.CE, OptionType.PE):
            try:
                legs.append(chain.leg_at(atm, side))
            except Exception:
                pass
        if not legs:
            self.calibration.record_observation(instrument_class, now, False, False, None, None, None, stale=True, instrument_id=underlying)
            return
        for leg in legs:
            quote = leg.quote
            valid = quote.is_valid()
            try:
                age = max(0.0, (now - quote.timestamp).total_seconds())
            except Exception:
                age = 999.0
            stale = age > 8.0
            spread_pct = quote.spread / quote.mid * 100.0 if valid and quote.mid > 0 else None
            top_book_lots = min(quote.bid_qty, quote.ask_qty) / max(1, lot) if valid else None
            depth_lots = None
            if quote.cumulative_bid_qty_5depth is not None and quote.cumulative_ask_qty_5depth is not None:
                depth_lots = min(quote.cumulative_bid_qty_5depth, quote.cumulative_ask_qty_5depth) / max(1, lot)
            fill = self.fill_sim.entry_buy(quote, self.master.tick_size(underlying, expiry)) if valid else None
            self.calibration.record_observation(
                instrument_class, now, valid and not stale,
                bool(fill and fill.filled), spread_pct, top_book_lots, depth_lots, stale=stale,
                instrument_id=underlying,
            )

    def _record_shadow_cycle(self, chains, context_map, now: datetime) -> None:
        shadow_candidates = self._build_candidates(chains, context_map, scope="monitor")
        if not shadow_candidates:
            self.state.underlyings["_shadow_candidates"] = []
            return
        shadow_state = PaperPortfolioState(open_positions_count=0, pending_orders_count=0,
                                           realized_loss_today=max(0.0, -self.state.realized_pnl))
        result = self.scorer_engine.evaluate_and_select(shadow_candidates, state=shadow_state)
        self._update_candidate_display(result, key="_shadow_candidates")
        best_by_underlying = {}
        for evaluation in sorted(result.evaluations, key=lambda e: e.comparable_opportunity_score, reverse=True):
            best_by_underlying.setdefault(evaluation.candidate.instrument.underlying, evaluation)
        for evaluation in best_by_underlying.values():
            outcome = self.shadow_tracker.observe(evaluation, now)
            self.event_ledger.append(
                "SHADOW_CANDIDATE", session_id=self.state.session_id,
                underlying=evaluation.candidate.instrument.underlying,
                exchange=evaluation.candidate.instrument.exchange,
                instrument_kind=evaluation.candidate.instrument.instrument_kind,
                instrument_class=evaluation.candidate.instrument.instrument_class,
                lifecycle_state=evaluation.candidate.lifecycle_state,
                exposure_group=evaluation.candidate.exposure_group,
                decision_source="shadow_ranker", ts=now,
                payload={"score": evaluation.comparable_opportunity_score, "eligible": evaluation.eligible,
                         "reasons": evaluation.reasons},
            )
            if outcome is not None:
                costs = self.costs.round_trip_cost(
                    outcome.entry_price * outcome.lot_size,
                    outcome.exit_price * outcome.lot_size,
                ).total
                net = outcome.net_pnl_rupees - costs
                net_r = outcome.r_multiple
                if outcome.net_pnl_rupees:
                    net_r = outcome.r_multiple * (net / outcome.net_pnl_rupees)
                self.evidence.record_shadow_outcome(outcome, costs=costs)
                self.calibration.record_outcome(
                    outcome.instrument_class, outcome.score, net_r, net,
                    instrument_id=outcome.underlying, paper=False,
                    observed_at=now,
                    cost_model_valid=self._cost_model_valid,
                )
                self.event_ledger.append(
                    "SHADOW_OUTCOME", session_id=self.state.session_id,
                    underlying=outcome.underlying, instrument_class=outcome.instrument_class,
                    lifecycle_state=outcome.lifecycle_state, exposure_group=outcome.exposure_group,
                    decision_source="counterfactual_paper_fill", ts=now,
                    payload={"exit_reason": outcome.exit_reason, "net_pnl": net, "net_r": net_r},
                )
        if time.time() - self._last_shadow_cycle >= 60.0:
            self._last_shadow_cycle = time.time()
            try:
                self.evidence.record_shadow_candidates(result.evaluations, ts=now)
            except Exception as e:
                self._log(f"  shadow candidate evidence failed: {e}")

    @staticmethod
    def _gate_feature_snapshot(evaluation) -> dict[str, float]:
        return gate_feature_snapshot(evaluation)

    def _record_gate_observations(self, evaluations, now: datetime) -> None:
        for evaluation in evaluations:
            c = evaluation.candidate
            try:
                self.calibration.record_gate_observation(
                    c.instrument.underlying, c.instrument.instrument_class, now,
                    self._gate_feature_snapshot(evaluation), evaluation.eligible,
                    evaluation.comparable_opportunity_score,
                )
            except Exception as e:
                self._log(f"  gate observation failed for {c.instrument.underlying}: {e}")

    def _portfolio_no_trade_snapshot(self, evaluations, now: datetime) -> dict[str, Any]:
        cfg = self.config.section("portfolio_no_trade_engine")
        if not bool(cfg.get("enabled", True)):
            return {"enabled": False, "score": 0.0, "status": "DISABLED", "timestamp": now.isoformat()}
        if not evaluations:
            components = {
                "best_candidate_weakness_risk": 100.0,
                "cross_instrument_market_hostility": 100.0,
                "data_breadth_risk": 100.0,
                "liquidity_breadth_risk": 100.0,
                "event_gap_system_risk": 100.0,
                "recent_loss_psychology_risk": 0.0,
                "calibration_uncertainty_risk": 100.0,
            }
        else:
            contract_scores = [float(e.contract_quality.score) for e in evaluations]
            invalid_health = sum(1 for e in evaluations if not e.candidate.data_health.valid)
            invalid_liquidity = sum(1 for e in evaluations if not e.contract_quality.valid)
            unvalidated = sum(
                1 for e in evaluations
                if e.candidate.calibration_status_direction.value == "UNVALIDATED"
                or e.candidate.calibration_status_liquidity.value == "UNVALIDATED"
            )
            best_score = max(float(e.comparable_opportunity_score) for e in evaluations)
            hostility = sum(float(e.candidate.market_hostility_score) for e in evaluations) / len(evaluations)
            runtime_cfg = self.config.section("runtime_risk_controls")
            event_risk = 0.0
            if bool(runtime_cfg.get("enforce_on_paper", False)) and self._risk_context.get("status") != "VALID":
                event_risk = 100.0
            components = {
                "best_candidate_weakness_risk": max(0.0, min(100.0, 100.0 - best_score)),
                "cross_instrument_market_hostility": max(0.0, min(100.0, hostility)),
                "data_breadth_risk": 100.0 * invalid_health / len(evaluations),
                "liquidity_breadth_risk": 100.0 * invalid_liquidity / len(evaluations),
                "event_gap_system_risk": event_risk,
                "recent_loss_psychology_risk": max(0.0, min(100.0, self.state.loss_streak_today * 35.0 + self.state.losses_today * 15.0)),
                "calibration_uncertainty_risk": 100.0 * unvalidated / len(evaluations),
            }
        score = self.portfolio_no_trade.calculate(**components)
        shutdown = float(cfg.get("portfolio_no_trade_score_shutdown_above", 70.0))
        hard_vetoes = []
        if evaluations and bool(cfg.get("hard_vetoes", {}).get("three_or_more_instruments_data_invalid", False)):
            invalid_count = sum(1 for e in evaluations if not e.candidate.data_health.valid)
            if invalid_count >= 3:
                hard_vetoes.append(f"{invalid_count}_instruments_data_invalid")
        return {
            "enabled": True,
            "score": score,
            "shutdown_above": shutdown,
            "status": "BLOCKED" if score >= shutdown or hard_vetoes else "CLEAR",
            "hard_vetoes": hard_vetoes,
            "components": components,
            "timestamp": now.isoformat(),
        }

    def _select_and_enter(self, chains, context_map) -> None:
        now = now_ist()
        if time.time() < self._incident_block_until:
            reason = f"{self._incident_reason}; reconnect stabilization in progress"
            self.state.underlyings["_incident"] = {"status": "BLOCKED", "reason": reason, "until": self._incident_block_until}
            self.event_ledger.append("INCIDENT_ENTRY_BLOCK", session_id=self.state.session_id, decision_source="incident_guard", ts=now, payload={"reason": reason})
            return
        if self._incident_block_until:
            self.state.underlyings.pop("_incident", None)
            self._incident_block_until = 0.0
            self._incident_reason = ""
        runtime_block = self._risk_context_block_reason()
        if runtime_block:
            self.state.underlyings["_runtime_risk"] = {"status": "BLOCKED", "reason": runtime_block, "context": dict(self._risk_context)}
            self.event_ledger.append(
                "RUNTIME_RISK_BLOCK", session_id=self.state.session_id,
                decision_source="runtime_risk_filter", ts=now,
                payload={"reason": runtime_block, "context": self._risk_context},
            )
            return
        self.state.underlyings.pop("_runtime_risk", None)
        risk_block = self._daily_risk_block_reason(now)
        if risk_block:
            self.state.underlyings["_daily_risk"] = {
                "status": "BLOCKED",
                "reason": risk_block,
                "trades_today": self.state.trades_today,
                "losses_today": self.state.losses_today,
                "realized_pnl_today": self.state.realized_pnl_today,
            }
            return
        self.state.underlyings.pop("_daily_risk", None)
        candidates = self._build_candidates(chains, context_map)
        if not candidates:
            self.state.underlyings["_candidates"] = []
            return
        state = PaperPortfolioState(
            open_positions_count=0,
            pending_orders_count=0,
            # Shadow ranking is counterfactual and must not be suppressed by
            # the live paper portfolio's daily risk budget.
            realized_loss_today=0.0,
        )
        allowed_playbooks = None
        if self.config.section("playbook_runtime").get("enforce_on_paper", False):
            allowed_playbooks = set().union(*self._playbook_codes_by_underlying.values()) if self._playbook_codes_by_underlying else set()
        result = self.scorer_engine.evaluate_and_select(candidates, state=state, allowed_playbooks=allowed_playbooks)
        self._record_gate_observations(result.evaluations, now)
        if result.decision == TradeDecision.NO_TRADE and any("ambiguous" in str(reason).lower() for reason in result.reasons):
            tie_payload = {
                "status": "UNRESOLVED",
                "reason": "Top candidates remained numerically identical after deterministic tie-break criteria",
                "candidate_count": len(result.evaluations),
                "timestamp": now.isoformat(),
            }
            self.state.underlyings["_rank_tie"] = tie_payload
            self.event_ledger.append(
                "RANK_TIE_UNRESOLVED", session_id=self.state.session_id,
                decision_source="ranking_tie_breaker", ts=now, payload=tie_payload,
            )
        else:
            self.state.underlyings.pop("_rank_tie", None)
        portfolio_snapshot = self._portfolio_no_trade_snapshot(result.evaluations, now)
        no_a_grade_required = bool(self.config.section("portfolio_no_trade_engine").get("no_trade_if_no_candidate_grade_at_least_A", True))
        if no_a_grade_required and (result.selected is None or result.selected.grade.value not in {"A", "A+"}):
            portfolio_snapshot["status"] = "BLOCKED"
            portfolio_snapshot.setdefault("hard_vetoes", []).append("no_candidate_grade_A_or_better")
        self.state.underlyings["_portfolio_no_trade"] = portfolio_snapshot
        self._update_candidate_display(result)
        # Broad research evidence must be recorded even when a portfolio veto blocks entry.
        now_ts = time.time()
        if now_ts - self._last_skipped_cycle >= 60.0:
            self._last_skipped_cycle = now_ts
            cycle_id = now_ist().strftime("%Y%m%d%H%M")
            try:
                self.evidence.record_skipped(result.evaluations, ranking_cycle_id=cycle_id)
            except Exception as e:
                self._log(f"  evidence record_skipped failed: {e}")
            try:
                self.evidence.record_candidates(result.evaluations, ts=now_ist())
            except Exception as e:
                self._log(f"  evidence record_candidates failed: {e}")
        if portfolio_snapshot.get("status") == "BLOCKED":
            reason = f"Portfolio no-trade score {portfolio_snapshot['score']:.1f} >= {portfolio_snapshot['shutdown_above']:.1f}"
            hard_vetoes = portfolio_snapshot.get("hard_vetoes", [])
            if hard_vetoes:
                reason = f"Portfolio no-trade hard veto: {', '.join(str(v) for v in hard_vetoes)}"
            self.event_ledger.append(
                "PORTFOLIO_NO_TRADE_BLOCK", session_id=self.state.session_id,
                decision_source="portfolio_no_trade_engine", ts=now,
                payload={"reason": reason, "snapshot": portfolio_snapshot},
            )
            return
        selected = result.selected
        if selected is None:
            self._rank_persistence = {}
            self._save_rank_persistence()
            return
        persistent, persistence_reason, persistence_count, persistence_required = self._rank_persistence_check(selected, now)
        self.state.underlyings["_rank_persistence"] = {
            "key": self._rank_key(selected),
            "count": persistence_count,
            "required": persistence_required,
            "status": "PASS" if persistent else "BLOCKED",
            "reason": persistence_reason,
        }
        if not persistent:
            self.event_ledger.append(
                "RANK_PERSISTENCE_BLOCK", session_id=self.state.session_id,
                underlying=selected.candidate.instrument.underlying,
                exchange=selected.candidate.instrument.exchange,
                instrument_kind=selected.candidate.instrument.instrument_kind,
                instrument_class=selected.candidate.instrument.instrument_class,
                lifecycle_state=selected.candidate.lifecycle_state,
                exposure_group=selected.candidate.exposure_group,
                decision_source="rank_persistence_gate", ts=now,
                payload={"count": persistence_count, "required": persistence_required, "reason": persistence_reason},
            )
            return
        friday_block = self._short_dated_friday_block_reason(selected.candidate, now)
        if friday_block:
            self.state.underlyings["_entry_window"] = {
                "status": "CLOSED",
                "reason": friday_block,
                "timestamp": now_ist().isoformat(),
            }
            return
        if selected.candidate.lifecycle_state in {InstrumentLifecycle.MONITOR.value, InstrumentLifecycle.SHADOW.value, InstrumentLifecycle.RETIRED.value}:
            self._log(f"Selector blocked non-paper lifecycle candidate: {selected.candidate.lifecycle_state}")
            return
        active_underlyings = {self.state.open_position.underlying} if self.state.open_position else set()
        active_groups = set()
        if self.state.open_position:
            active_candidate = self.state.open_position.trade.entry_evaluation.candidate
            active_groups.add(active_candidate.exposure_group)
        overlap = self.overlap_guard.assess(
            selected.candidate.instrument.underlying, selected.candidate.exposure_group,
            active_underlyings, active_groups,
        )
        if not overlap.allowed:
            self.event_ledger.append(
                "OVERLAP_BLOCK", session_id=self.state.session_id,
                underlying=selected.candidate.instrument.underlying,
                exchange=selected.candidate.instrument.exchange,
                instrument_kind=selected.candidate.instrument.instrument_kind,
                instrument_class=selected.candidate.instrument.instrument_class,
                lifecycle_state=selected.candidate.lifecycle_state,
                exposure_group=selected.candidate.exposure_group,
                decision_source="portfolio_overlap_guard", payload={"reason": overlap.reason},
            )
            return
        chain = chains[selected.candidate.instrument.underlying]
        leg = chain.leg_at(selected.candidate.instrument.strike, selected.candidate.side)
        revalidated, revalidation_reasons = self.revalidator.revalidate(
            selected,
            leg.quote,
            now_ist(),
            ranking_spread=selected.candidate.quote.spread,
            fast_market=selected.candidate.market_hostility_score >= 50.0,
        )
        try:
            self.evidence.record_revalidation(selected, revalidated, revalidation_reasons, stage="PRE_ENTRY", ts=now_ist())
        except Exception as e:
            self._log(f"  revalidation evidence failed: {e}")
        if not revalidated:
            self.state.underlyings["_revalidation"] = {
                "status": "BLOCKED",
                "reasons": list(revalidation_reasons),
                "underlying": selected.candidate.instrument.underlying,
            }
            self.event_ledger.append(
                "REVALIDATION_BLOCK", session_id=self.state.session_id,
                underlying=selected.candidate.instrument.underlying,
                exchange=selected.candidate.instrument.exchange,
                instrument_kind=selected.candidate.instrument.instrument_kind,
                instrument_class=selected.candidate.instrument.instrument_class,
                lifecycle_state=selected.candidate.lifecycle_state,
                exposure_group=selected.candidate.exposure_group,
                decision_source="candidate_revalidator", payload={"reasons": revalidation_reasons},
            )
            return
        refreshed_candidate = replace(selected.candidate, quote=leg.quote)
        refreshed_health = self.data_health.evaluate_candidate(refreshed_candidate, now_ist())
        refreshed_candidate = replace(
            refreshed_candidate,
            data_health=DataHealth(
                valid=refreshed_candidate.data_health.valid and refreshed_health.valid,
                warning=refreshed_candidate.data_health.warning or refreshed_health.warning,
                reason="; ".join(reason for reason in (refreshed_candidate.data_health.reason, refreshed_health.reason) if reason),
            ),
        )
        refreshed = self.scorer_engine.scorer.evaluate(
            refreshed_candidate,
            realized_loss_today=max(0.0, -self.state.realized_pnl_today),
        )
        try:
            self.evidence.record_revalidation(refreshed, refreshed.eligible, refreshed.reasons, stage="FRESH_SCORE", ts=now_ist())
        except Exception as e:
            self._log(f"  fresh-score evidence failed: {e}")
        if not refreshed.eligible:
            self.state.underlyings["_revalidation"] = {
                "status": "BLOCKED",
                "reasons": list(refreshed.reasons) or ["Fresh quote no longer passes all entry gates"],
                "underlying": selected.candidate.instrument.underlying,
            }
            return
        selected = refreshed
        self.state.underlyings.pop("_revalidation", None)
        mapping_ok, mapping_reason = self._validate_entry_mapping(selected.candidate)
        try:
            self.evidence.record_revalidation(selected, mapping_ok, (mapping_reason,) if not mapping_ok else tuple(), stage="MAPPING_VALIDATION", ts=now_ist())
        except Exception as e:
            self._log(f"  mapping-validation evidence failed: {e}")
        if not mapping_ok:
            self.state.underlyings["_revalidation"] = {
                "status": "BLOCKED",
                "reasons": [mapping_reason],
                "underlying": selected.candidate.instrument.underlying,
            }
            return
        entry_notes = dict(selected.candidate.notes or {})
        entry_notes.update({
            "entry_revalidation_passed": True,
            "mapping_validation_passed": True,
            "lot_size_validation_passed": selected.candidate.instrument.lot_size > 0,
            "tick_size_validation_passed": selected.candidate.instrument.tick_size > 0,
        })
        selected = replace(selected, candidate=replace(selected.candidate, notes=entry_notes))
        symbol = self.master.symbol_for(
            selected.candidate.instrument.underlying,
            selected.candidate.instrument.expiry,
            selected.candidate.instrument.strike,
            selected.candidate.side.value,
        )
        fill = self.fill_sim.entry_buy(selected.candidate.quote, selected.candidate.instrument.tick_size)
        try:
            self.evidence.record_fill_attempt(selected, "ENTRY", fill.filled and fill.fill_price is not None, fill.fill_price, fill.reason, ts=now_ist())
        except Exception as e:
            self._log(f"  entry-fill evidence failed: {e}")
        if not fill.filled or fill.fill_price is None:
            self._log(f"Entry not filled for {symbol}: {fill.reason}")
            return
        trade = PaperTrade(
            trade_id=f"{now_ist().strftime('%H%M%S')}-{symbol.split(':')[-1]}",
            entry_evaluation=selected,
            entry_fill=fill,
            entry_time=now_ist(),
        )
        stop = max(selected.risk_plan.hard_stop_points, 1.0)
        target_r = self._target_r(selected)
        pos = OpenPosition(
            trade=trade,
            symbol=symbol,
            underlying=selected.candidate.instrument.underlying,
            expiry=chain.expiry,
            stop_points=stop,
            target_points=stop * target_r,
            max_duration_seconds=self.entry_hold_seconds,
            last_premium=fill.fill_price,
            highest_premium=fill.fill_price,
            lowest_premium=fill.fill_price,
            last_quote=selected.candidate.quote,
        )
        self.state.open_position = pos
        self.state.trades_today += 1
        self._save_daily_risk_state()
        self.event_ledger.append(
            "PAPER_ENTRY", session_id=self.state.session_id,
            underlying=selected.candidate.instrument.underlying,
            exchange=selected.candidate.instrument.exchange,
            instrument_kind=selected.candidate.instrument.instrument_kind,
            instrument_class=selected.candidate.instrument.instrument_class,
            lifecycle_state=selected.candidate.lifecycle_state,
            exposure_group=selected.candidate.exposure_group,
            decision_source="trade_selector", ts=trade.entry_time,
            payload={"symbol": symbol, "score": selected.comparable_opportunity_score,
                     "fill": fill.fill_price, "stop": stop, "target_r": target_r},
        )
        self._log(f"OPEN {symbol} @ {fill.fill_price:.2f} stop={stop:.2f} target={stop*target_r:.2f}")

    def _validate_entry_mapping(self, candidate) -> tuple[bool, str]:
        spec = candidate.instrument
        if not spec.security_id or spec.security_id == "":
            return False, "Mapping validation unavailable: security_id missing"
        if spec.lot_size <= 0:
            return False, "Lot-size validation failed: non-positive lot size"
        if spec.tick_size <= 0:
            return False, "Tick-size validation failed: non-positive tick size"
        if not spec.buy_sell_allowed:
            return False, "Mapping validation failed: buy side not permitted"
        try:
            symbol = self.master.symbol_for(spec.underlying, spec.expiry, spec.strike, candidate.side.value)
        except Exception as exc:
            return False, f"Mapping validation failed: {type(exc).__name__}"
        if not symbol:
            return False, "Mapping validation failed: empty broker symbol"
        return True, ""

    def _target_r(self, selected) -> float:
        """Target in R multiples. Base is preferred_target_R; when edge-scaled
        targets are enabled (exit_management.edge_scaled_target), the target is
        scaled by the candidate's expected/required ratio so high-edge setups
        are given room to run and marginal ones are banked earlier. The stop is
        never changed, so worst-case per-trade risk is identical."""
        base = float(self.config.section("expected_move").get("preferred_target_R", 2.0))
        raw = self.config.raw.get("exit_management")
        if not (isinstance(raw, Mapping) and raw.get("edge_scaled_target")):
            return base
        try:
            ratio = selected.candidate.expected_move / max(selected.candidate.required_move, 1e-9)
            min_ratio = float(raw.get("edge_scale_min_ratio", 1.1))
            min_r = float(raw.get("edge_scale_min_r", 1.0))
            max_r = float(raw.get("edge_scale_max_r", 3.0))
            return min(max(base * (ratio / min_ratio), min_r), max_r)
        except (TypeError, ValueError):
            return base

    # -- open position management -----------------------------------------------

    def _manage_open_position(self, chains, context_map=None) -> None:
        pos = self.state.open_position
        if pos is None:
            return
        chain = chains.get(pos.underlying)
        if chain is None:
            return
        try:
            leg = chain.leg_at(pos.trade.entry_evaluation.candidate.instrument.strike,
                               pos.trade.entry_evaluation.candidate.side)
        except Exception:
            return
        ctx = (context_map or {}).get(pos.underlying)
        bar = MarketBar(
            timestamp=now_ist(),
            quote=leg.quote,
            futures_price=chain.underlying_price,
            iv=leg.implied_volatility,
            expected_move_remaining=ctx.regime_projected_move if ctx is not None else None,
        )
        pos.bars.append(bar)
        pos.last_quote = leg.quote
        pos.last_premium = leg.quote.mid
        pos.highest_premium = max(pos.highest_premium, leg.quote.mid)
        pos.lowest_premium = min(pos.lowest_premium, leg.quote.mid) if pos.lowest_premium > 0 else leg.quote.mid

        result = self.lifecycle.run(
            pos.trade, pos.bars,
            target_points=pos.target_points,
            stop_points=pos.stop_points,
            max_duration_seconds=pos.max_duration_seconds,
            exit_policy=self.exit_policy,
        )
        if result.exit_reason in ACTIVE_EXIT_REASONS:
            self._close_position(pos, result.exit_reason, result)
        # EXIT_END_OF_DATA / NO_DATA means no exit triggered yet on the bars so far.

    def _force_close_end_of_day(self, now: datetime) -> None:
        pos = self.state.open_position
        if pos is None:
            return
        quote = pos.last_quote
        if quote is None or not quote.is_valid():
            self.state.underlyings["_eod_guard"] = {
                "status": "BLOCKED",
                "reason": "Open paper position has no valid final quote; manual evidence reconciliation required",
            }
            return
        tick = pos.trade.entry_evaluation.candidate.instrument.tick_size
        exit_fill = self.fill_sim.exit_sell(quote, tick)
        exit_price = exit_fill.fill_price if exit_fill.filled and exit_fill.fill_price is not None else quote.bid
        trade = replace(
            pos.trade,
            exit_fill=exit_fill,
            exit_time=now,
            exit_reason="END_OF_DAY_EXIT",
        )
        entry_price = pos.trade.entry_fill.fill_price or pos.last_premium
        result = SimpleNamespace(
            trade=trade,
            exit_reason="END_OF_DAY_EXIT",
            gross_pnl_points=exit_price - entry_price,
            mae_points=entry_price - pos.lowest_premium,
            mfe_points=pos.highest_premium - entry_price,
        )
        self._close_position(pos, "END_OF_DAY_EXIT", result)

    def _close_position(self, pos: OpenPosition, reason: str, result) -> None:
        exit_fill = result.trade.exit_fill
        try:
            self.evidence.record_fill_attempt(
                pos.trade.entry_evaluation, "EXIT",
                bool(exit_fill and exit_fill.filled and exit_fill.fill_price is not None),
                exit_fill.fill_price if exit_fill else None,
                exit_fill.reason if exit_fill else "No exit fill object",
                ts=result.trade.exit_time or now_ist(),
            )
        except Exception as e:
            self._log(f"  exit-fill evidence failed: {e}")
        exit_price = exit_fill.fill_price if exit_fill and exit_fill.filled else pos.last_premium
        lot = pos.trade.entry_evaluation.candidate.instrument.lot_size
        gross_pnl = (exit_price - pos.trade.entry_fill.fill_price) * lot
        costs = self.costs.round_trip_cost(
            pos.trade.entry_fill.fill_price * lot, exit_price * lot).total
        net = gross_pnl - costs
        hold = int((result.trade.exit_time - pos.trade.entry_time).total_seconds()) if result.trade.exit_time else 0
        rec = ClosedTradeRecord(
            trade_id=pos.trade.trade_id,
            underlying=pos.underlying,
            side=pos.trade.entry_evaluation.candidate.side.value,
            expiry=pos.expiry,
            strike=pos.trade.entry_evaluation.candidate.instrument.strike,
            entry_time=pos.trade.entry_time.isoformat(),
            exit_time=(result.trade.exit_time or now_ist()).isoformat(),
            entry_fill=pos.trade.entry_fill.fill_price,
            exit_fill=exit_price,
            exit_reason=reason,
            gross_points=result.gross_pnl_points,
            gross_pnl=gross_pnl,
            costs=costs,
            net_pnl=net,
            hold_seconds=hold,
            max_adverse_points=result.mae_points,
            max_favorable_points=result.mfe_points,
        )
        self.state.closed_trades.append(rec)
        self.state.realized_pnl += net
        self.state.realized_pnl_today += net
        self.state.realized_pnl_week += net
        if net < 0:
            self.state.losses_today += 1
            self.state.loss_streak_today += 1
            self.state.last_loss_at = (result.trade.exit_time or now_ist()).isoformat()
        else:
            self.state.loss_streak_today = 0
        self._save_daily_risk_state()
        self.state.open_position = None
        self._append_journal(rec)
        # Phase-2 evidence: one MTIL row per closed trade with entry proxy scores.
        planned = pos.trade.entry_evaluation.risk_plan.planned_risk
        r_multiple = net / planned if planned and planned > 0 else 0.0
        try:
            self.evidence.record_trade(
                result.trade,
                net_pnl_rupees=net,
                r_multiple=r_multiple,
                gross_pnl_rupees=gross_pnl,
                total_costs_rupees=costs,
            )
            c = pos.trade.entry_evaluation.candidate
            self.calibration.record_outcome(
                c.instrument.instrument_class, pos.trade.entry_evaluation.comparable_opportunity_score,
                r_multiple, net, instrument_id=c.instrument.underlying, paper=True,
                features=self._gate_feature_snapshot(pos.trade.entry_evaluation),
                observed_at=result.trade.exit_time or now_ist(),
                cost_model_valid=self._cost_model_valid,
            )
            self.event_ledger.append(
                "PAPER_OUTCOME", session_id=self.state.session_id,
                underlying=c.instrument.underlying, exchange=c.instrument.exchange,
                instrument_kind=c.instrument.instrument_kind, instrument_class=c.instrument.instrument_class,
                lifecycle_state=c.lifecycle_state, exposure_group=c.exposure_group,
                decision_source="paper_lifecycle", ts=result.trade.exit_time or now_ist(),
                payload={"exit_reason": reason, "net_pnl": net, "r_multiple": r_multiple},
            )
        except Exception as e:
            self._log(f"  evidence/calibration record_trade failed: {e}")
        self._log(f"CLOSE {rec.side} {rec.strike} {rec.exit_reason} net={net:+.0f} "
                  f"({result.gross_pnl_points:+.1f}pts) hold={hold}s")

    # -- display / state ----------------------------------------------------------

    def _update_chain_display(self, chains, vix_map, context_map) -> None:
        for und, chain in chains.items():
            ctx = context_map[und]
            meta = self.universe.get(und, {})
            legs = []
            atm = chain.nearest_strike()
            for s in chain.strikes:
                if abs(s.strike - atm) > 600:
                    continue
                row = {"strike": s.strike, "atm": abs(s.strike - atm) < 1e-6}
                for name, leg in (("ce", s.ce), ("pe", s.pe)):
                    if leg is not None:
                        row[name] = {"bid": leg.quote.bid, "ask": leg.quote.ask,
                                     "ltp": leg.quote.last, "mid": leg.quote.mid}
                legs.append(row)
            instrument_kind = meta.get("instrument_kind", "INDEX")
            exchange = meta.get("exchange", "NSE")
            instrument_class = class_for_metadata(exchange, instrument_kind)
            class_gates = self.calibration.gates_for(instrument_class)
            prior_state = dict(self.state.underlyings.get(und, {}) or {})
            display_state = {
                "exchange": exchange,
                "instrument_kind": instrument_kind,
                "instrument_class": instrument_class,
                "lifecycle_state": self.lifecycle_states.get(und, InstrumentLifecycle.MONITOR.value),
                "exposure_group": exposure_group(und, instrument_kind),
                "class_gates": class_gates.to_dict(),
                "monitor_only": bool(meta.get("monitor_only", False)),
                "trade_enabled": bool(meta.get("trade_enabled", not meta.get("monitor_only", False))),
                "spot": chain.underlying_price,
                "vix": vix_map[und],
                "expiry": chain.expiry,
                "dte": ctx.dte,
                "direction": round(ctx.direction_score, 1),
                "direction_model_score": "" if ctx.direction_model_score is None else round(ctx.direction_model_score, 1),
                "direction_model_name": ctx.direction_model_name,
                "direction_model_status": ctx.direction_model_status,
                "direction_model_disagreement": "" if ctx.direction_model_disagreement is None else round(ctx.direction_model_disagreement, 1),
                "trade_quality": round(ctx.trade_quality_score, 1),
                "hostility": round(ctx.market_hostility_score, 1),
                "required_move": round(ctx.required_move, 1),
                "atr1": round(ctx.atr1, 2),
                "trend_eff": round(ctx.trend_efficiency, 1),
                "strikes": legs,
            }
            for preserved_key in ("depth_health", "stale_data_alert", "instrument_error", "promotion"):
                if preserved_key in prior_state:
                    display_state[preserved_key] = prior_state[preserved_key]
            self.state.underlyings[und] = display_state

    def _update_candidate_display(self, result, key: str = "_candidates") -> None:
        rows = []
        for e in result.evaluations:
            c = e.candidate
            rows.append({
                "underlying": c.instrument.underlying,
                "research_only": key == "_shadow_candidates",
                "side": c.side.value,
                "strike": c.instrument.strike,
                "expiry": str(c.instrument.expiry),
                "grade": e.grade.value,
                "score": round(e.comparable_opportunity_score, 1),
                "threshold": round(e.dynamic_excellent_threshold, 1),
                "eligible": e.eligible,
                "decision": e.decision.value,
                "contract_quality": round(e.contract_quality.score, 1),
                "premium_elasticity": round(c.premium_elasticity, 2),
                "convexity": round(c.convexity_edge_score, 1),
                "execution": round(c.execution_quality_score, 1),
                "confidence": round(c.opportunity_confidence_score, 1),
                "regime_fit": round(c.regime_fit_score, 1),
                "direction": round(c.instrument_direction_score, 1),
                "bid": c.quote.bid, "ask": c.quote.ask, "mid": round(c.quote.mid, 2),
                "reasons": "; ".join(e.reasons),
                "instrument_class": c.instrument.instrument_class,
                "lifecycle_state": c.lifecycle_state,
                "exposure_group": c.exposure_group,
                "calibrated_probability": c.calibrated_success_probability,
                "calibrated_expectancy_r": c.calibrated_net_expectancy_r,
            })
        self.state.underlyings[key] = rows

    def _update_equity(self) -> None:
        self.state.equity.append(round(self.state.realized_pnl, 2))
        if len(self.state.equity) > 5000:
            self.state.equity = self.state.equity[-5000:]

    def _capture_cycle(self, payloads: dict, histories: dict, now: datetime,
                       depth_payloads: Optional[dict] = None) -> None:
        """Append one cycle of raw chain/history/depth payloads to the session
        capture file for deterministic offline replay and parameter sweeps."""
        try:
            rec = {
                "ts": now.isoformat(),
                "chains": payloads,
                "history": histories,
                "depth": depth_payloads or {},
            }
            self._capture_file.write(json.dumps(rec, default=str) + "\n")
            self._capture_file.flush()
        except Exception as e:
            self._log(f"  capture failed: {e}")

    def snapshot(self) -> dict[str, Any]:
        pos = self.state.open_position
        pos_view = None
        if pos is not None:
            entry = pos.trade.entry_fill.fill_price
            pos_view = {
                "symbol": pos.symbol,
                "underlying": pos.underlying,
                "side": pos.trade.entry_evaluation.candidate.side.value,
                "strike": pos.trade.entry_evaluation.candidate.instrument.strike,
                "entry": entry,
                "last": pos.last_premium,
                "unrealized_points": pos.last_premium - entry,
                "unrealized_pnl": (pos.last_premium - entry) * pos.trade.entry_evaluation.candidate.instrument.lot_size,
                "stop_points": pos.stop_points,
                "target_points": pos.target_points,
                "max_duration_sec": pos.max_duration_seconds,
                "elapsed_sec": int((now_ist() - pos.trade.entry_time).total_seconds()),
                "highest": pos.highest_premium,
                "lowest": pos.lowest_premium,
                "bars": len(pos.bars),
                "mfe_points": pos.highest_premium - entry,
                "mae_points": entry - (pos.lowest_premium if pos.lowest_premium > 0 else entry),
                "opened_at": pos.trade.entry_time.isoformat(),
                "exit_policy": self._exit_policy_view(),
            }
        closed = [asdict(r) for r in self.state.closed_trades]
        return {
            "started_at": self.state.started_at,
            "session_id": self.state.session_id,
            "last_cycle": self.state.last_cycle,
            "last_cycle_ok": self.state.last_cycle_ok,
            "last_error": self.state.last_error,
            "market_open": self.state.market_open,
            "mode": "PAPER (no orders placed)",
            "open_position": pos_view,
            "closed_trades": closed,
            "underlyings": self.state.underlyings,
            "equity": self.state.equity[-2000:],
            "realized_pnl": self.state.realized_pnl,
            "capital": self.base_config.section("capital")["starting_capital"],
            "paper_overrides_active": bool(self._active_overrides),
            "active_overrides": self._active_overrides,
            "daily_mode": dict(self.state.underlyings.get("_daily_mode", {})),
            "strategy_version": self.versions.strategy_version,
            "score_version": self.versions.score_version,
            "universe_version": self.versions.universe_version,
            "calibration": self.calibration.snapshot(),
            "lifecycle_states": dict(self.lifecycle_states),
            "note": "Live Fyers data. All scores marked PROXY are research-grade approximations; see paper_signal.py. Shadow outcomes are counterfactual paper fills only.",
        }

    def _exit_policy_view(self) -> dict[str, Any]:
        p = self.exit_policy
        return {
            "enabled": p.enabled,
            "breakeven_trigger_r": p.breakeven_trigger_r,
            "trail_trigger_r": p.trail_trigger_r,
            "trail_distance_r": p.trail_distance_r,
            "losing_time_stop_fraction": p.losing_time_stop_fraction,
            "time_decay_tighten": p.time_decay_tighten,
            "vol_time_stop_fraction": p.vol_time_stop_fraction,
            "stop_exit_slippage_frac": p.stop_exit_slippage_frac,
        }

    # -- journal -------------------------------------------------------------------

    def _write_journal_header(self) -> None:
        cols = ["trade_id", "underlying", "side", "expiry", "strike", "entry_time", "exit_time",
                "entry_fill", "exit_fill", "exit_reason", "gross_points", "gross_pnl",
                "costs", "net_pnl", "hold_seconds", "max_adverse_points", "max_favorable_points"]
        if not self._journal_path.exists():
            with self._journal_path.open("w", encoding="utf-8", newline="") as f:
                csv.writer(f).writerow(cols)

    def _append_journal(self, rec: ClosedTradeRecord) -> None:
        with self._journal_path.open("a", encoding="utf-8", newline="") as f:
            csv.writer(f).writerow([
                rec.trade_id, rec.underlying, rec.side, rec.expiry, rec.strike,
                rec.entry_time, rec.exit_time, f"{rec.entry_fill:.2f}", f"{rec.exit_fill:.2f}",
                rec.exit_reason, f"{rec.gross_points:.2f}", f"{rec.gross_pnl:.2f}",
                f"{rec.costs:.2f}", f"{rec.net_pnl:.2f}", rec.hold_seconds,
                f"{rec.max_adverse_points:.2f}", f"{rec.max_favorable_points:.2f}",
            ])

    # -- helpers ---------------------------------------------------------------------

    def _canonical_promotion_allowed(self) -> bool:
        if not self._cost_model_valid:
            return False
        controls = self.config.raw.get("operator_controls", {})
        require_context = bool(controls.get("require_valid_market_context_for_canonical", False)) if isinstance(controls, Mapping) else False
        return not require_context or getattr(self.factory.market_context, "status", "") == "APPLIED"

    def _computed_daily_mode(self) -> str:
        global_state = str(self._risk_context.get("global_risk_state", "NEUTRAL")).upper()
        news_state = str(self._risk_context.get("news_state", "NEWS_NORMAL")).upper()
        if global_state == "SHOCK" or news_state == "NEWS_NO_TRADE":
            return "SURVIVAL"
        if global_state == "RISK_OFF" or news_state == "NEWS_CAUTION":
            return "DEFENSIVE"
        return "NORMAL"

    def _refresh_daily_controls(self, now: datetime) -> None:
        """Reload operator controls before every cycle and propagate them to all consumers."""
        computed_mode = self._computed_daily_mode()
        daily_mode = load_daily_mode(self._daily_mode_path, computed_mode, now=now)
        market_context = load_market_context(self._market_context_path, now=now)
        previous_mode = getattr(self, "daily_mode", None)
        previous_context = getattr(self.factory, "market_context", None)
        self.daily_mode = daily_mode
        self.scorer_engine.set_runtime_mode(daily_mode.effective_mode)
        self.factory.market_context = market_context
        self.signal.market_context = market_context
        self.state.underlyings["_daily_mode"] = {
            "computed_mode": daily_mode.computed_mode,
            "effective_mode": daily_mode.effective_mode,
            "status": daily_mode.status,
            "reason": daily_mode.reason,
            "path": daily_mode.path,
        }
        self.state.underlyings["_market_context"] = {
            "status": market_context.status,
            "reason": market_context.reason,
            "path": market_context.path,
            "as_of": market_context.as_of,
            "expires_at": market_context.expires_at,
            "source": market_context.source,
        }
        if previous_mode != daily_mode:
            self.event_ledger.append(
                "DAILY_MODE_CONTEXT", session_id=self.state.session_id,
                decision_source="daily_mode_operator_control", ts=now,
                payload=self.state.underlyings["_daily_mode"],
            )
        if previous_context != market_context:
            self.event_ledger.append(
                "MARKET_CONTEXT", session_id=self.state.session_id,
                decision_source="daily_market_context_operator_control", ts=now,
                payload=self.state.underlyings["_market_context"],
            )

    def _load_risk_context(self) -> dict[str, Any]:
        controls = self.config.raw.get("runtime_risk_controls", {}) if hasattr(self, "config") else self.base_config.raw.get("runtime_risk_controls", {})
        if not isinstance(controls, Mapping):
            return {"status": "UNAVAILABLE", "reason": "Risk controls not configured"}
        configured = self.cfg.get("risk_context_path") or controls.get("risk_context_path")
        path = Path(str(configured)) if configured else self.state_dir / "risk_context.json"
        if not path.is_absolute():
            if path.parts and path.parts[0] == self.state_dir.name:
                path = self.state_dir / Path(*path.parts[1:])
            else:
                path = self.state_dir / path
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            return {"status": "UNAVAILABLE", "reason": "Verified risk/news context file missing or invalid", "path": str(path)}
        if not isinstance(payload, Mapping):
            return {"status": "UNAVAILABLE", "reason": "Risk/news context is not an object", "path": str(path)}
        source = str(payload.get("source", "")).strip()
        raw_ts = payload.get("ts") or payload.get("timestamp")
        try:
            parsed = datetime.fromisoformat(str(raw_ts).replace("Z", "+00:00"))
            if parsed.tzinfo is None:
                parsed = parsed.replace(tzinfo=IST)
            current = now_ist()
            if current.tzinfo is None:
                current = current.replace(tzinfo=IST)
            age = max(0.0, (current.astimezone(IST) - parsed.astimezone(IST)).total_seconds())
        except (TypeError, ValueError):
            return {"status": "UNAVAILABLE", "reason": "Risk/news context timestamp invalid", "path": str(path)}
        stale_after = float(controls.get("stale_after_sec", 30.0))
        if bool(controls.get("source_required", True)) and not source:
            return {"status": "UNAVAILABLE", "reason": "Risk/news context source is missing", "age_sec": age, "path": str(path)}
        if age > stale_after:
            return {"status": "STALE", "reason": f"Risk/news context is {age:.1f}s old", "age_sec": age, "path": str(path)}
        out = dict(payload)
        out.update({"status": "VALID", "age_sec": age, "path": str(path)})
        return out

    def _load_rank_persistence(self) -> dict[str, dict[str, Any]]:
        try:
            raw = json.loads(self._rank_persistence_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            return {}
        if not isinstance(raw, Mapping):
            return {}
        return {str(key): dict(value) for key, value in raw.items() if isinstance(value, Mapping)}

    def _save_rank_persistence(self) -> None:
        tmp = self._rank_persistence_path.with_suffix(".tmp")
        tmp.write_text(json.dumps(self._rank_persistence, indent=2, sort_keys=True, default=str), encoding="utf-8")
        tmp.replace(self._rank_persistence_path)

    def _risk_context_block_reason(self) -> str:
        self._risk_context = self._load_risk_context()
        controls = self.config.section("runtime_risk_controls")
        if not bool(controls.get("enforce_on_paper", False)):
            return ""
        if self._risk_context.get("status") != "VALID":
            return "Risk/news context unavailable or stale; fail-closed entry block"
        global_state = str(self._risk_context.get("global_risk_state", "NEUTRAL")).upper()
        news_state = str(self._risk_context.get("news_state", "NEWS_NORMAL")).upper()
        if global_state == "SHOCK":
            return "Global risk shock active"
        if news_state == "NEWS_NO_TRADE":
            return "News no-trade state active"
        try:
            score = float(self._risk_context.get("portfolio_no_trade_score", 0.0))
            shutdown = float(self.config.section("portfolio_no_trade_engine").get("portfolio_no_trade_score_shutdown_above", 70.0))
            if score >= shutdown:
                return f"Portfolio no-trade score above shutdown: {score:.1f}"
        except (TypeError, ValueError):
            return "Portfolio no-trade score invalid"
        return ""

    def _regime_context(self, underlying: str, ctx) -> RegimeContext:
        global_state = str(self._risk_context.get("global_risk_state", "NEUTRAL")).upper()
        news_state = str(self._risk_context.get("news_state", "NEWS_UNKNOWN")).upper()
        if global_state == "SHOCK":
            primary = RegimeLabel.PANIC
        elif global_state == "RISK_OFF":
            primary = RegimeLabel.RISK_OFF
        elif global_state == "RISK_ON":
            primary = RegimeLabel.RISK_ON
        elif ctx.trend_efficiency >= 70.0 and abs(ctx.direction_score) >= 45.0:
            primary = RegimeLabel.TREND_EXPANSION
        elif ctx.vix is not None and ctx.vix < 11.0:
            primary = RegimeLabel.COMPRESSION
        else:
            primary = RegimeLabel.RANGE_BALANCE
        return RegimeContext(
            primary=primary,
            confidence=ctx.regime_confidence,
            market_hostility_score=ctx.market_hostility_score,
            iv_crush_risk_score=50.0,
            liquidity_stable=bool(self._risk_context.get("liquidity_stable", False)) if self._risk_context.get("status") == "VALID" else False,
            event_resolved=news_state in {"NEWS_NORMAL", "NEWS_CAUTION"},
            gap_wait_completed=bool(self._risk_context.get("gap_wait_completed", False)),
            trend_strength_score=ctx.trend_efficiency,
            range_expansion_quality=ctx.trade_quality_score,
            global_risk_shock=global_state == "SHOCK",
            time_bucket="OPENING" if ctx.dte >= 0 else "UNKNOWN",
        )

    def _update_playbook_filters(self, context_map: Mapping[str, Any], now: datetime) -> None:
        runtime = self.config.section("playbook_runtime")
        if not bool(runtime.get("enforce_on_paper", False)):
            self._playbook_codes_by_underlying = {}
            self._playbook_grades_by_underlying = {}
            return
        self._playbook_codes_by_underlying = {}
        self._playbook_grades_by_underlying = {}
        for underlying, ctx in context_map.items():
            selection = self.playbook_engine.evaluate(self._regime_context(underlying, ctx))
            self._playbook_codes_by_underlying[underlying] = selection.allowed_codes
            self._playbook_grades_by_underlying[underlying] = selection.selected.grade.value if selection.selected is not None else ""
            self.event_ledger.append(
                "PLAYBOOK_CONTEXT", session_id=self.state.session_id,
                underlying=underlying, exchange=self.universe.get(underlying, {}).get("exchange", "NSE"),
                instrument_kind=self.universe.get(underlying, {}).get("instrument_kind", "INDEX"),
                instrument_class=class_for_metadata(
                    self.universe.get(underlying, {}).get("exchange", "NSE"),
                    self.universe.get(underlying, {}).get("instrument_kind", "INDEX"),
                ),
                decision_source="regime_playbook_engine", ts=now,
                payload={"allowed_codes": sorted(selection.allowed_codes), "no_trade": selection.no_trade, "reasons": selection.reasons},
            )

    def _rank_key(self, evaluation) -> str:
        c = evaluation.candidate
        return f"{c.instrument.underlying}:{c.side.value}:{c.instrument.expiry.isoformat()}:{c.instrument.strike:g}"

    def _rank_persistence_check(self, selected, now: datetime) -> tuple[bool, str, int, int]:
        runtime = self.config.section("playbook_runtime")
        if not bool(runtime.get("require_rank_persistence", True)):
            return True, "disabled", 1, 1
        required = max(1, int(self.config.section("opportunity_selection").get("rank_persistence_required_windows", 2)))
        key = self._rank_key(selected)
        previous = self._rank_persistence.get(key, {})
        try:
            last = datetime.fromisoformat(str(previous.get("last_ts", "")))
            same_session = last.date() == now.date()
            gap = (now - last).total_seconds()
        except (TypeError, ValueError):
            same_session, gap = False, float("inf")
        max_gap = max(30.0, self.poll_seconds * 3.0)
        count = int(previous.get("count", 0)) + 1 if same_session and gap <= max_gap else 1
        self._rank_persistence = {key: {"count": count, "last_ts": now.isoformat(), "underlying": selected.candidate.instrument.underlying, "side": selected.candidate.side.value}}
        self._save_rank_persistence()
        if count < required:
            return False, f"Rank persistence {count}/{required} windows", count, required
        return True, "rank persistence satisfied", count, required

    def _restore_daily_risk_state(self, now: datetime) -> None:
        """Restore same-day risk counters; never carry them into a new date."""
        try:
            raw = json.loads(self._daily_risk_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            raw = {}
        stored_date = str(raw.get("date", "")) if isinstance(raw, Mapping) else ""
        stored_week = str(raw.get("week_key", "")) if isinstance(raw, Mapping) else ""
        current_week = (now.date() - timedelta(days=now.weekday())).isoformat()
        self._risk_week_key = current_week
        try:
            self.state.realized_pnl_week = float(raw.get("realized_pnl_week", 0.0)) if stored_week == current_week else 0.0
            if stored_date != now.date().isoformat():
                self._reset_daily_risk_state(now, persist=True)
                return
            self._daily_risk_date = stored_date
            self.state.realized_pnl_today = float(raw.get("realized_pnl_today", 0.0))
            self.state.trades_today = max(0, int(raw.get("trades_today", 0)))
            self.state.losses_today = max(0, int(raw.get("losses_today", 0)))
            self.state.loss_streak_today = max(0, int(raw.get("loss_streak_today", 0)))
            self.state.last_loss_at = str(raw.get("last_loss_at", ""))
        except (TypeError, ValueError):
            self._reset_daily_risk_state(now, persist=True)

    def _reset_daily_risk_state(self, now: datetime, persist: bool = False) -> None:
        self._daily_risk_date = now.date().isoformat()
        self.state.realized_pnl_today = 0.0
        self.state.trades_today = 0
        self.state.losses_today = 0
        self.state.loss_streak_today = 0
        self.state.last_loss_at = ""
        if persist:
            self._save_daily_risk_state()

    def _roll_daily_risk_state(self, now: datetime) -> None:
        current_week = (now.date() - timedelta(days=now.weekday())).isoformat()
        if current_week != self._risk_week_key:
            self._risk_week_key = current_week
            self.state.realized_pnl_week = 0.0
        if now.date().isoformat() != self._daily_risk_date:
            self._reset_daily_risk_state(now, persist=True)
        else:
            self._save_daily_risk_state()

    def _save_daily_risk_state(self) -> None:
        payload = {
            "date": self._daily_risk_date,
            "week_key": self._risk_week_key,
            "realized_pnl_today": self.state.realized_pnl_today,
            "realized_pnl_week": self.state.realized_pnl_week,
            "trades_today": self.state.trades_today,
            "losses_today": self.state.losses_today,
            "loss_streak_today": self.state.loss_streak_today,
            "last_loss_at": self.state.last_loss_at,
        }
        tmp = self._daily_risk_path.with_suffix(".tmp")
        tmp.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
        tmp.replace(self._daily_risk_path)

    def _open_position_risk_reservation(self) -> float:
        pos = self.state.open_position
        if pos is None:
            return 0.0
        entry = float(pos.trade.entry_fill.fill_price or 0.0)
        last = float(pos.last_premium or entry)
        lot = max(1, int(pos.trade.entry_evaluation.candidate.instrument.lot_size))
        stop_price = max(0.0, entry - float(pos.stop_points))
        return max(0.0, last - stop_price) * lot

    def _daily_risk_block_reason(self, now: datetime) -> str:
        risk = self.config.section("risk")
        open_risk = self._open_position_risk_reservation()
        self.state.underlyings["_risk_exposure"] = {
            "open_position_risk_reservation": open_risk,
            "realized_loss_today": max(0.0, -self.state.realized_pnl_today),
            "realized_loss_week": max(0.0, -self.state.realized_pnl_week),
            "timestamp": now.isoformat(),
        }
        max_trades = max(0, int(risk.get("max_trades_per_day", 0)))
        if max_trades and self.state.trades_today >= max_trades:
            return f"Maximum trades per day reached: {self.state.trades_today}/{max_trades}"
        if bool(risk.get("stop_trading_after_three_losses", False)) and self.state.losses_today >= 3:
            return "Stop-trading rule active after three daily losses"
        max_daily_loss = float(risk.get("max_daily_loss_rupees", 0.0))
        daily_exposure = max(0.0, -self.state.realized_pnl_today) + open_risk
        if max_daily_loss > 0 and daily_exposure >= max_daily_loss:
            return f"Maximum daily loss/risk exposure reached: {daily_exposure:.2f}/{max_daily_loss:.2f}"
        max_weekly_loss = float(risk.get("max_weekly_loss_rupees", 0.0))
        weekly_exposure = max(0.0, -self.state.realized_pnl_week) + open_risk
        if max_weekly_loss > 0 and weekly_exposure >= max_weekly_loss:
            return f"Maximum weekly loss/risk exposure reached: {weekly_exposure:.2f}/{max_weekly_loss:.2f}"
        if self.state.last_loss_at and self.state.loss_streak_today > 0:
            try:
                last_loss = datetime.fromisoformat(self.state.last_loss_at)
                elapsed_min = (now - last_loss).total_seconds() / 60.0
                cooldown = float(risk.get(
                    "cooldown_after_two_losses_minutes" if self.state.loss_streak_today >= 2
                    else "cooldown_after_one_loss_minutes", 0.0
                ))
                if elapsed_min < cooldown:
                    return f"Loss cooldown active: {cooldown - elapsed_min:.1f} minutes remaining"
            except (TypeError, ValueError):
                return "Invalid last-loss timestamp; new entries blocked"
        return ""

    @staticmethod
    def _hhmm_to_minutes(value: Any, default: int) -> int:
        try:
            hour, minute = str(value).strip().split(":", 1)
            parsed = int(hour) * 60 + int(minute)
            if 0 <= parsed <= 24 * 60:
                return parsed
        except (TypeError, ValueError):
            pass
        return default

    def _market_open(self, now: datetime) -> bool:
        if now.weekday() >= 5:
            return False
        minutes = now.hour * 60 + now.minute
        return 9 * 60 + 15 <= minutes <= 15 * 60 + 30

    def _short_dated_friday_block_reason(self, candidate, now: datetime) -> str:
        if now.weekday() != 4:
            return ""
        theta = self.config.section("theta")
        cutoff = self._hhmm_to_minutes(theta.get("no_new_short_dated_friday_after", "13:30"), 13 * 60 + 30)
        if now.hour * 60 + now.minute < cutoff:
            return ""
        try:
            dte = (candidate.instrument.expiry - now.date()).days
        except (AttributeError, TypeError):
            return "Short-dated Friday entry blocked: invalid expiry"
        if dte <= 1:
            return f"Short-dated Friday entry blocked after {theta.get('no_new_short_dated_friday_after', '13:30')}"
        return ""

    def _entry_window_open(self, now: datetime) -> bool:
        if not self._market_open(now):
            return False
        holding = self.config.section("holding_time")
        start = self._hhmm_to_minutes(holding.get("no_trade_before", "09:30"), 9 * 60 + 30)
        end = self._hhmm_to_minutes(holding.get("no_new_entries_after", "14:15"), 14 * 60 + 15)
        minutes = now.hour * 60 + now.minute
        return start <= minutes < end

    def _seconds_to_open(self) -> float:
        now = now_ist()
        target = now.replace(hour=9, minute=15, second=0, microsecond=0)
        if now >= target:
            target += timedelta(days=1)
        while target.weekday() >= 5:
            target += timedelta(days=1)
        return max(0.0, (target - now).total_seconds())

    def _direction_model_histories(self, underlying: str) -> dict[str, list]:
        runtime = self.config.raw.get("direction_model_runtime", {})
        if not isinstance(runtime, Mapping) or not bool(runtime.get("shadow_enabled", False)):
            return {}
        symbols = runtime.get("component_symbols", {}).get(str(underlying).upper(), [])
        if not isinstance(symbols, list):
            return {}
        out: dict[str, list] = {}
        for symbol in symbols:
            name = str(symbol).upper()
            out[name] = self._fetch_history(
                f"_direction_component_{name}",
                f"NSE:{name}-EQ",
            )
        return out

    def _fetch_history(self, underlying: str, index_symbol: str) -> list:
        # Replay mode is offline and per-cycle: never cache across cycles.
        if self._replay:
            try:
                resp = self.client.history(index_symbol, resolution="1")
                candles = resp.get("candles", []) if isinstance(resp, dict) else (resp or [])
                return [c for c in candles if isinstance(c, (list, tuple)) and len(c) >= 5][-self.history_bars:]
            except Exception:
                return []
        cached = self.history_cache.get(underlying)
        if cached is not None and (time.time() - cached[0]) < 60.0:
            return cached[1]
        try:
            now = datetime.now(IST)
            start = now - timedelta(days=5)
            resp = self.client.history(index_symbol, resolution="1",
                                        range_from=start.strftime("%Y-%m-%d"),
                                        range_to=now.strftime("%Y-%m-%d"))
            candles = resp.get("candles", []) if isinstance(resp, dict) else []
            candles = [c for c in candles if isinstance(c, (list, tuple)) and len(c) >= 5][-self.history_bars:]
            self.history_cache[underlying] = (time.time(), candles)
            return candles
        except Exception:
            return self.history_cache.get(underlying, (0, []))[1]

    def _select_expiry(self, underlying: str, cal, prefer_monthly: bool) -> str:
        if not cal:
            exps = self.master.expiry_dates(underlying)
            return str(exps[0]) if exps else date.today().isoformat()
        today = now_ist().date()
        future = [e for e in cal if date.fromtimestamp(e.expiry_ts) >= today]
        if prefer_monthly:
            future = [e for e in future if e.flag.upper() == "M"]
        if not future:
            future = list(cal)
        chosen = future[0]
        return date.fromtimestamp(chosen.expiry_ts).isoformat()

    def _overlay_config(self, config: SystemConfig) -> SystemConfig:
        overrides = self.cfg.get("config_overrides")
        signal_cfg = self.cfg.get("signal")
        if (not isinstance(overrides, dict) or not overrides) and not isinstance(signal_cfg, Mapping):
            return config
        import copy
        raw = copy.deepcopy(dict(config.raw))
        if isinstance(signal_cfg, Mapping):
            raw.setdefault("paper_runner", {})["signal"] = dict(signal_cfg)
        signal_applied = isinstance(signal_cfg, Mapping)
        changed: dict[str, dict[str, Any]] = {}
        if isinstance(overrides, dict):
            for section, values in overrides.items():
                if section == "_comment" or not isinstance(values, dict):
                    continue
                if section not in raw:
                    continue
                if not isinstance(raw[section], dict):
                    raw[section] = dict(values)
                    changed[section] = dict(values)
                    continue
                merged = {**raw[section], **values}
                for key, val in values.items():
                    if key == "_comment":
                        continue
                    if key not in raw[section] or raw[section][key] != val:
                        changed.setdefault(section, {})[key] = val
                raw[section] = merged
        self._active_overrides = changed
        execution = raw.get("execution", {})
        if isinstance(execution, Mapping) and execution.get("live_trading_enabled") is not False:
            raise ConfigError("PaperRunner rejects any override that enables live execution.")
        if not changed and not signal_applied:
            return config
        if changed:
            self._log(f"PAPER-ONLY config overrides active: {changed}")
        return SystemConfig(raw=raw)

    def _log(self, msg: str) -> None:
        print(f"[{now_ist().strftime('%H:%M:%S')}] {msg}", flush=True)
