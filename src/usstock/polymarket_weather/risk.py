"""Position sizing and risk controls for binary weather markets."""

from __future__ import annotations

from dataclasses import dataclass

from usstock.polymarket_weather.models import BucketSignal


KELLY_MULTIPLIERS = {
    "full": 1.0,
    "half": 0.5,
    "quarter": 0.25,
}


@dataclass(frozen=True)
class RiskConfig:
    bankroll: float
    kelly_mode: str = "half"
    min_edge: float = 0.03
    min_price: float = 0.01
    max_price: float = 0.95
    max_trade_fraction: float = 0.03
    max_daily_fraction: float = 0.12
    max_market_fraction: float = 0.06
    max_city_fraction: float = 0.08
    min_stake: float = 1.0

    @property
    def kelly_multiplier(self) -> float:
        text = str(self.kelly_mode).strip().lower()
        if text in KELLY_MULTIPLIERS:
            return KELLY_MULTIPLIERS[text]
        try:
            value = float(text)
        except ValueError as exc:
            raise ValueError(f"Unsupported Kelly mode: {self.kelly_mode}") from exc
        return max(0.0, min(value, 1.0))


@dataclass(frozen=True)
class PositionRecommendation:
    should_trade: bool
    reason: str
    stake: float
    shares: float
    full_kelly_fraction: float
    scaled_kelly_fraction: float
    capped_fraction: float
    max_loss: float
    potential_profit: float


class PositionSizer:
    """Kelly sizing with hard exposure caps."""

    def size_yes(
        self,
        signal: BucketSignal,
        risk: RiskConfig,
        *,
        current_daily_exposure: float = 0.0,
        current_market_exposure: float = 0.0,
        current_city_exposure: float = 0.0,
    ) -> PositionRecommendation:
        probability = max(0.0, min(1.0, signal.probability))
        price = max(0.0, min(1.0, signal.market_price))
        edge = probability - price

        if edge < risk.min_edge:
            return self._skip(f"edge below minimum: {edge:.4f}")
        if price < risk.min_price or price > risk.max_price:
            return self._skip(f"price outside allowed range: {price:.4f}")
        if risk.bankroll <= 0:
            return self._skip("bankroll must be positive")

        full_kelly_fraction = max(0.0, edge / max(1.0 - price, 1e-9))
        scaled_kelly_fraction = full_kelly_fraction * risk.kelly_multiplier

        remaining_daily = max(0.0, risk.bankroll * risk.max_daily_fraction - current_daily_exposure)
        remaining_market = max(0.0, risk.bankroll * risk.max_market_fraction - current_market_exposure)
        remaining_city = max(0.0, risk.bankroll * risk.max_city_fraction - current_city_exposure)
        cap_fraction = min(
            risk.max_trade_fraction,
            remaining_daily / risk.bankroll,
            remaining_market / risk.bankroll,
            remaining_city / risk.bankroll,
        )
        capped_fraction = max(0.0, min(scaled_kelly_fraction, cap_fraction))
        stake = risk.bankroll * capped_fraction
        if stake < risk.min_stake:
            return PositionRecommendation(
                should_trade=False,
                reason="stake below minimum after caps",
                stake=0.0,
                shares=0.0,
                full_kelly_fraction=full_kelly_fraction,
                scaled_kelly_fraction=scaled_kelly_fraction,
                capped_fraction=capped_fraction,
                max_loss=0.0,
                potential_profit=0.0,
            )

        shares = stake / price
        potential_profit = shares * (1.0 - price)
        return PositionRecommendation(
            should_trade=True,
            reason="ok",
            stake=stake,
            shares=shares,
            full_kelly_fraction=full_kelly_fraction,
            scaled_kelly_fraction=scaled_kelly_fraction,
            capped_fraction=capped_fraction,
            max_loss=stake,
            potential_profit=potential_profit,
        )

    @staticmethod
    def _skip(reason: str) -> PositionRecommendation:
        return PositionRecommendation(
            should_trade=False,
            reason=reason,
            stake=0.0,
            shares=0.0,
            full_kelly_fraction=0.0,
            scaled_kelly_fraction=0.0,
            capped_fraction=0.0,
            max_loss=0.0,
            potential_profit=0.0,
        )
