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
import json
import os
import threading
import time
from dataclasses import asdict, dataclass, field, replace
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Mapping, Optional

from .config import SystemConfig
from .costs import ChargesConfig, CostCalculator
from .candidates import CandidateFactory, CandidateFactoryContext
from .engine import PaperOpportunityEngine, PaperPortfolioState
from .fyers_client import FyersCredentials, FyersRestClient, FyersSymbolMaster, TokenStore
from .fyers_parser import FyersOptionChainParser, parse_expiry_calendar, parse_india_vix
from .lifecycle import (
    EXIT_BREAKEVEN, EXIT_END_OF_DATA, EXIT_LOSING_TIME, EXIT_NO_DATA,
    EXIT_STOP, EXIT_TARGET, EXIT_TIME, EXIT_TRAIL, EXIT_VOL_TIME,
    ExitPolicy, MarketBar, SimulatedTradeLifecycle,
)
from .models import OptionType, PaperFill, PaperTrade, Quote
from .option_chain import OptionChainSnapshot
from .paper_evidence import PaperEvidenceCollector
from .paper_signal import PaperSignalCalculator
from .scoring import PaperFillSimulator

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
        self.state_dir = Path(state_dir)
        self.state_dir.mkdir(parents=True, exist_ok=True)
        self.state = RunnerState(session_id=now_ist().strftime("%Y%m%d_%H%M%S"))
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
        self.factory = CandidateFactory(self.config)
        self.scorer_engine = PaperOpportunityEngine(self.config)
        self.fill_sim = PaperFillSimulator(self.config)
        self.lifecycle = SimulatedTradeLifecycle(self.fill_sim)
        self.exit_policy = ExitPolicy.from_config(self.config)
        # Phase-2 evidence collection: mtil.csv (closed trades) + skipped.csv.
        self.evidence = PaperEvidenceCollector(self.state_dir)
        self._last_skipped_cycle = 0.0
        self.history_bars = int(self.cfg.get("history_bars", 30))
        self.history_cache: dict[str, list] = {}

        charges = ChargesConfig.from_file("uploads/CHARGES_CONFIG.json")
        self.costs = CostCalculator(charges)

        self.poll_seconds = float(self.cfg.get("poll_seconds", 5.0))
        self.strikecount = int(self.cfg.get("strikecount", 30))
        self.entry_hold_seconds = int(self.config.section("holding_time")["normal_max_hold_minutes"]) * 60
        self.universe = self._universe()
        if master is None and self._replay:
            # Replay is offline: load the cached symbol master (no auth needed).
            cached = self.state_dir / "NSE_FO.csv"
            if cached.exists():
                master = FyersSymbolMaster.from_csv(cached)
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
        self._log(f"Paper runner ready. Universe: {', '.join(self.universe)}")

    # -- setup -----------------------------------------------------------------

    def _universe(self) -> dict[str, dict[str, str]]:
        cfg_universe = self.cfg.get("underlyings")
        if isinstance(cfg_universe, Mapping):
            out: dict[str, dict[str, str]] = {}
            for und, meta in cfg_universe.items():
                if isinstance(meta, Mapping):
                    out[str(und).upper()] = {
                        "index_symbol": str(meta.get("index_symbol", "")),
                        "prefer_monthly": str(meta.get("prefer_monthly", "false")).lower() == "true",
                    }
            if out:
                return out
        # Defaults (NSE index symbols on Fyers; note BANKNIFTY is NIFTYBANK).
        return {
            "NIFTY": {"index_symbol": "NSE:NIFTY50-INDEX", "prefer_monthly": "false"},
            "BANKNIFTY": {"index_symbol": "NSE:NIFTYBANK-INDEX", "prefer_monthly": "false"},
            "FINNIFTY": {"index_symbol": "NSE:FINNIFTY-INDEX", "prefer_monthly": "false"},
            "MIDCPNIFTY": {"index_symbol": "NSE:MIDCPNIFTY-INDEX", "prefer_monthly": "false"},
        }

    def _load_master(self) -> FyersSymbolMaster:
        master_path = self.state_dir / "NSE_FO.csv"
        path = self.client.fetch_symbol_master(master_path)
        master = FyersSymbolMaster.from_csv(path)
        for und in self.universe:
            exps = master.expiry_dates(und)
            self._log(f"  {und}: {len(exps)} expiries, nearest {exps[0] if exps else '-'}")
        return master

    # -- main loop ---------------------------------------------------------------

    def run_forever(self, stop_event: Optional[threading.Event] = None) -> None:
        self._log("Loop started.")
        try:
            while stop_event is None or not stop_event.is_set():
                try:
                    self.run_one_cycle()
                except Exception as e:  # keep the loop alive across transient failures
                    self.state.last_cycle_ok = False
                    self.state.last_error = f"{type(e).__name__}: {e}"
                    self._log(f"Cycle error: {self.state.last_error}")
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
        self.state.market_open = self._market_open(now)
        if not self.state.market_open:
            self.state.last_cycle = now.isoformat()
            return
        chains: dict[str, OptionChainSnapshot] = {}
        vix_map: dict[str, Optional[float]] = {}
        context_map = {}
        payloads: dict[str, Any] = {}
        histories: dict[str, Any] = {}
        for und, meta in self.universe.items():
            try:
                payload = self.client.option_chain(meta["index_symbol"], self.strikecount)
                cal = parse_expiry_calendar(payload)
                expiry = self._select_expiry(und, cal, meta.get("prefer_monthly") == "true")
                chain = FyersOptionChainParser.parse(payload, und, expiry, now)
                vix = parse_india_vix(payload)
                chains[und] = chain
                vix_map[und] = vix
                history = self._fetch_history(und, meta["index_symbol"])
                histories[und] = history
                payloads[und] = payload
                context_map[und] = self.signal.compute_context(chain, vix, now, history_candles=history)
            except Exception as e:
                self.state.underlyings[und] = {"error": f"{type(e).__name__}: {e}"}
                self._log(f"  {und} chain error: {e}")
        if not chains:
            raise RuntimeError("No underlying chains fetched this cycle.")
        if self._capture_file is not None:
            self._capture_cycle(payloads, histories, now)
        self._update_chain_display(chains, vix_map, context_map)

        # Manage the open position with fresh bars before considering new entries.
        self._manage_open_position(chains, context_map)

        # Build candidates from all fresh chains and select.
        if self.state.open_position is None:
            self._select_and_enter(chains, context_map)

        self.state.last_cycle = now_ist().isoformat()
        self.state.last_cycle_ok = True
        self._update_equity()

    # -- candidate building + selection -----------------------------------------

    def _build_candidates(self, chains, context_map) -> list:
        out = []
        self.state.underlyings["_prefiltered"] = 0
        for und, chain in chains.items():
            ctx = context_map[und]
            try:
                expiry = date.fromisoformat(chain.expiry[:10])
            except (ValueError, TypeError):
                expiry = date.today()
            try:
                lot = self.master.lot_size(und, expiry)
                tick = self.master.tick_size(und, expiry)
            except Exception:
                lot, tick = 30, 0.05
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
            )
            cands = self.factory.candidates_from_chain(chain, expiry, lot, tick, cctx)
            for c in cands:
                spread_pct = c.quote.spread / c.quote.mid * 100.0 if c.quote.mid > 0 else 99.0
                proxies = self.signal.candidate_proxies(chain, ctx, c.moneyness, c.side, spread_pct)
                c = replace(c, 
                    premium_elasticity=proxies.premium_elasticity,
                    convexity_edge_score=proxies.convexity_edge_score,
                    execution_quality_score=proxies.execution_quality_score,
                    opportunity_confidence_score=proxies.opportunity_confidence_score,
                    regime_fit_score=proxies.regime_fit_score,
                )
                # Spread-cost pre-filter: skip strikes the conservative paper
                # fill model would refuse at entry (wide OR pathologically tight
                # spreads), so un-fillable candidates never waste an evaluation
                # or selection cycle. Uses the real simulator -> exact match.
                probe = self.fill_sim.entry_buy(c.quote, c.instrument.tick_size)
                if not probe.filled:
                    self.state.underlyings["_prefiltered"] = \
                        int(self.state.underlyings.get("_prefiltered", 0)) + 1
                    continue
                out.append(c)
        return out

    def _select_and_enter(self, chains, context_map) -> None:
        candidates = self._build_candidates(chains, context_map)
        if not candidates:
            return
        state = PaperPortfolioState(
            open_positions_count=0,
            pending_orders_count=0,
            realized_loss_today=max(0.0, -self.state.realized_pnl),
        )
        result = self.scorer_engine.evaluate_and_select(candidates, state=state)
        self._update_candidate_display(result)
        # Phase-2 evidence: log skipped candidates once per minute (throttled) so
        # the gate's minimum-candidates/ranking-cycles can accumulate even on
        # no-trade days.
        now_ts = time.time()
        if now_ts - self._last_skipped_cycle >= 60.0:
            self._last_skipped_cycle = now_ts
            cycle_id = now_ist().strftime("%Y%m%d%H%M")
            try:
                self.evidence.record_skipped(result.evaluations, ranking_cycle_id=cycle_id)
            except Exception as e:
                self._log(f"  evidence record_skipped failed: {e}")
            # Full candidate log (all grades) -> candidates_log.csv, the source
            # for the per-day top-N report used to calibrate the score threshold.
            try:
                self.evidence.record_candidates(result.evaluations, ts=now_ist())
            except Exception as e:
                self._log(f"  evidence record_candidates failed: {e}")
        selected = result.selected
        if selected is None:
            return
        chain = chains[selected.candidate.instrument.underlying]
        leg = chain.leg_at(selected.candidate.instrument.strike, selected.candidate.side)
        symbol = self.master.symbol_for(
            selected.candidate.instrument.underlying,
            selected.candidate.instrument.expiry,
            selected.candidate.instrument.strike,
            selected.candidate.side.value,
        )
        fill = self.fill_sim.entry_buy(selected.candidate.quote, selected.candidate.instrument.tick_size)
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
        )
        self.state.open_position = pos
        self._log(f"OPEN {symbol} @ {fill.fill_price:.2f} stop={stop:.2f} target={stop*target_r:.2f}")

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

    def _close_position(self, pos: OpenPosition, reason: str, result) -> None:
        exit_fill = result.trade.exit_fill
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
        self.state.open_position = None
        self._append_journal(rec)
        # Phase-2 evidence: one MTIL row per closed trade with entry proxy scores.
        planned = pos.trade.entry_evaluation.risk_plan.planned_risk
        r_multiple = net / planned if planned and planned > 0 else 0.0
        try:
            self.evidence.record_trade(result.trade, net_pnl_rupees=net, r_multiple=r_multiple)
        except Exception as e:
            self._log(f"  evidence record_trade failed: {e}")
        self._log(f"CLOSE {rec.side} {rec.strike} {rec.exit_reason} net={net:+.0f} "
                  f"({result.gross_pnl_points:+.1f}pts) hold={hold}s")

    # -- display / state ----------------------------------------------------------

    def _update_chain_display(self, chains, vix_map, context_map) -> None:
        for und, chain in chains.items():
            ctx = context_map[und]
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
            self.state.underlyings[und] = {
                "spot": chain.underlying_price,
                "vix": vix_map[und],
                "expiry": chain.expiry,
                "dte": ctx.dte,
                "direction": round(ctx.direction_score, 1),
                "trade_quality": round(ctx.trade_quality_score, 1),
                "hostility": round(ctx.market_hostility_score, 1),
                "required_move": round(ctx.required_move, 1),
                "atr1": round(ctx.atr1, 2),
                "trend_eff": round(ctx.trend_efficiency, 1),
                "strikes": legs,
            }

    def _update_candidate_display(self, result) -> None:
        rows = []
        for e in result.evaluations:
            c = e.candidate
            rows.append({
                "underlying": c.instrument.underlying,
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
            })
        self.state.underlyings["_candidates"] = rows

    def _update_equity(self) -> None:
        self.state.equity.append(round(self.state.realized_pnl, 2))
        if len(self.state.equity) > 5000:
            self.state.equity = self.state.equity[-5000:]

    def _capture_cycle(self, payloads: dict, histories: dict, now: datetime) -> None:
        """Append one cycle of raw chain/history payloads to the session capture
        file (paper_state/sessions/<session>.jsonl.gz) for offline replay and
        parameter sweeps."""
        try:
            rec = {"ts": now.isoformat(), "chains": payloads, "history": histories}
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
            "note": "Live Fyers data. All scores marked PROXY are research-grade approximations; see paper_signal.py.",
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

    def _market_open(self, now: datetime) -> bool:
        if now.weekday() >= 5:
            return False
        minutes = now.hour * 60 + now.minute
        return 9 * 60 + 15 <= minutes <= 15 * 60 + 30

    def _seconds_to_open(self) -> float:
        now = now_ist()
        if now.weekday() >= 5:
            days = 7 - now.weekday() if now.weekday() == 6 else 5 - now.weekday()
            return (days * 24 * 3600)
        if now.hour * 60 + now.minute < 9 * 60 + 15:
            return (9 * 60 + 15 - (now.hour * 60 + now.minute)) * 60
        return (24 * 3600)

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
        if not isinstance(overrides, dict) or not overrides:
            return config
        import copy
        raw = copy.deepcopy(dict(config.raw))
        changed: dict[str, dict[str, Any]] = {}
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
        if not changed:
            return config
        self._log(f"PAPER-ONLY config overrides active: {changed}")
        return SystemConfig(raw=raw)

    def _log(self, msg: str) -> None:
        print(f"[{now_ist().strftime('%H:%M:%S')}] {msg}", flush=True)
