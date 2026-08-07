from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Iterable

from .config import SystemConfig
from .market_metrics import ExpectedMoveCalculator, IVCrushRiskCalculator, atm_straddle_implied_move
from .models import CalibrationStatus, CandidateInputs, DataHealth, InstrumentSpec, Moneyness, OptionType
from .option_chain import OptionChainSnapshot


@dataclass(frozen=True)
class CandidateFactoryContext:
    futures_price: float
    spot_price: float
    instrument_direction_score: float
    trade_quality_score: float
    regime_confidence: float
    market_hostility_score: float
    atr_remaining_move: float
    regime_projected_move: float
    required_move: float
    dte: float
    calibration_direction: CalibrationStatus = CalibrationStatus.UNVALIDATED
    calibration_liquidity: CalibrationStatus = CalibrationStatus.UNVALIDATED


class CandidateFactory:
    def __init__(self, config: SystemConfig):
        self.config = config
        self.expected = ExpectedMoveCalculator(config)
        self.ivrisk = IVCrushRiskCalculator(config)

    def candidates_from_chain(self, chain: OptionChainSnapshot, option_expiry_date, lot_size: int, tick_size: float, ctx: CandidateFactoryContext) -> tuple[CandidateInputs, ...]:
        strikes = [s.strike for s in chain.strikes]
        if not strikes:
            return tuple()
        atm = chain.nearest_strike()
        # ATM plus nearest neighbours; avoids far OTM lottery in MVP.
        sorted_by_distance = sorted(strikes, key=lambda k: abs(k - atm))[:3]
        straddle_move = atm_straddle_implied_move(chain)
        expected = self.expected.calculate(chain.underlying, ctx.required_move, ctx.atr_remaining_move, ctx.regime_projected_move, straddle_move)
        out: list[CandidateInputs] = []
        now = chain.timestamp
        for strike in sorted_by_distance:
            for opt in (OptionType.CE, OptionType.PE):
                leg = chain.leg_at(strike, opt)
                spec = InstrumentSpec(chain.underlying, leg.security_id, "OPTIDX", option_expiry_date, lot_size, tick_size, strike, opt)
                m = Moneyness.ATM if abs(strike - atm) < 1e-9 else (Moneyness.OTM if (opt == OptionType.CE and strike > atm) or (opt == OptionType.PE and strike < atm) else Moneyness.ITM)
                iv_crush = self.ivrisk.calculate(leg.implied_volatility, event_risk=10, recent_iv_expansion_pct=0, iv_realized_spread_pct=0, term_structure_risk=15, dte=ctx.dte, skew_risk=15)
                direction_score = ctx.instrument_direction_score if opt == OptionType.CE else -ctx.instrument_direction_score
                out.append(CandidateInputs(
                    instrument=spec,
                    quote=leg.quote,
                    moneyness=m,
                    greeks=leg.greeks,
                    data_health=DataHealth(True),
                    futures_price=ctx.futures_price,
                    underlying_price=ctx.spot_price,
                    instrument_direction_score=direction_score,
                    trade_quality_score=ctx.trade_quality_score,
                    regime_confidence=ctx.regime_confidence,
                    market_hostility_score=ctx.market_hostility_score,
                    iv_crush_risk_score=iv_crush,
                    premium_elasticity=0.0,
                    expected_move=expected.expected_move,
                    required_move=expected.required_move,
                    required_stop_points=expected.required_move,
                    expected_value_r=0.0,
                    vol_edge_ratio=expected.ratio,
                    convexity_edge_score=0.0,
                    execution_quality_score=0.0,
                    opportunity_confidence_score=0.0,
                    regime_fit_score=0.0,
                    candidate_created_at=now,
                    calibration_status_direction=ctx.calibration_direction,
                    calibration_status_liquidity=ctx.calibration_liquidity,
                ))
        return tuple(out)
