"""Simple backtesting for historical weather bucket signals."""

from __future__ import annotations

import argparse
import csv
from dataclasses import dataclass, replace
from datetime import date, datetime, time, timezone
from pathlib import Path

from usstock.polymarket_weather.buckets import parse_temperature_bucket
from usstock.polymarket_weather.ledger import PerformanceStats, TradeRecord, compute_performance_stats
from usstock.polymarket_weather.models import BucketSignal, MarketBucket
from usstock.polymarket_weather.risk import PositionSizer, RiskConfig


@dataclass(frozen=True)
class HistoricalSignal:
    city_id: str
    target_date: date
    kind: str
    outcome: str
    probability: float
    market_price: float
    settled_outcome: bool
    market_slug: str | None = None
    token_id: str | None = None


@dataclass(frozen=True)
class BacktestResult:
    trades: tuple[TradeRecord, ...]
    stats: PerformanceStats
    ending_bankroll: float


def load_historical_signals_csv(path: Path) -> tuple[HistoricalSignal, ...]:
    with path.open("r", encoding="utf-8", newline="") as file:
        reader = csv.DictReader(file)
        rows: list[HistoricalSignal] = []
        for row in reader:
            rows.append(
                HistoricalSignal(
                    city_id=row["city_id"],
                    target_date=date.fromisoformat(row["target_date"]),
                    kind=row["kind"],
                    outcome=row["outcome"],
                    probability=float(row["probability"]),
                    market_price=float(row["market_price"]),
                    settled_outcome=str(row["settled_outcome"]).strip().lower()
                    in {"1", "true", "yes", "y", "win"},
                    market_slug=row.get("market_slug") or None,
                    token_id=row.get("token_id") or None,
                )
            )
        return tuple(rows)


def run_backtest(
    signals: tuple[HistoricalSignal, ...],
    *,
    risk: RiskConfig,
) -> BacktestResult:
    sizer = PositionSizer()
    bankroll = risk.bankroll
    trades: list[TradeRecord] = []
    daily_exposure: dict[date, float] = {}
    market_exposure: dict[str, float] = {}
    city_exposure: dict[str, float] = {}

    for index, historical in enumerate(sorted(signals, key=lambda row: row.target_date)):
        market_bucket = MarketBucket(
            market_id=historical.market_slug or "historical",
            question="Historical weather market",
            slug=historical.market_slug,
            condition_id=None,
            outcome=historical.outcome,
            price=historical.market_price,
            token_id=historical.token_id,
            bucket=parse_temperature_bucket(historical.outcome),
        )
        edge = historical.probability - historical.market_price
        signal = BucketSignal(
            market_bucket=market_bucket,
            probability=historical.probability,
            market_price=historical.market_price,
            edge=edge,
            expected_value=edge,
            fair_price=historical.probability,
            recommendation="BUY_YES" if edge > 0 else "WATCH",
        )
        current_risk = replace(risk, bankroll=bankroll)
        market_key = historical.market_slug or historical.outcome
        recommendation = sizer.size_yes(
            signal,
            current_risk,
            current_daily_exposure=daily_exposure.get(historical.target_date, 0.0),
            current_market_exposure=market_exposure.get(market_key, 0.0),
            current_city_exposure=city_exposure.get(historical.city_id, 0.0),
        )
        if not recommendation.should_trade:
            continue
        if historical.settled_outcome:
            pnl = recommendation.potential_profit
        else:
            pnl = -recommendation.max_loss
        bankroll += pnl
        daily_exposure[historical.target_date] = (
            daily_exposure.get(historical.target_date, 0.0) + recommendation.stake
        )
        market_exposure[market_key] = market_exposure.get(market_key, 0.0) + recommendation.stake
        city_exposure[historical.city_id] = (
            city_exposure.get(historical.city_id, 0.0) + recommendation.stake
        )
        trades.append(
            TradeRecord(
                trade_id=f"backtest:{index}",
                timestamp=datetime.combine(
                    historical.target_date,
                    time.min,
                    tzinfo=timezone.utc,
                ),
                city_id=historical.city_id,
                target_date=historical.target_date,
                kind=historical.kind,
                market_slug=historical.market_slug,
                outcome=historical.outcome,
                token_id=historical.token_id,
                side="YES",
                price=historical.market_price,
                probability=historical.probability,
                edge=edge,
                stake=recommendation.stake,
                shares=recommendation.shares,
                status="settled",
                realized_pnl=pnl,
            )
        )

    trade_tuple = tuple(trades)
    return BacktestResult(
        trades=trade_tuple,
        stats=compute_performance_stats(trade_tuple),
        ending_bankroll=bankroll,
    )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="usstock weather-backtest",
        description="Backtest historical Polymarket weather bucket signals.",
    )
    parser.add_argument("--csv", required=True, type=Path)
    parser.add_argument("--bankroll", type=float, default=1_000.0)
    parser.add_argument("--kelly", default="half", choices=["full", "half", "quarter"])
    parser.add_argument("--min-edge", type=float, default=0.03)
    args = parser.parse_args(argv)

    signals = load_historical_signals_csv(args.csv)
    result = run_backtest(
        signals,
        risk=RiskConfig(
            bankroll=args.bankroll,
            kelly_mode=args.kelly,
            min_edge=args.min_edge,
        ),
    )
    stats = result.stats
    print(f"trades={stats.total_trades}")
    print(f"settled={stats.settled_trades}")
    print(f"win_rate={stats.win_rate:.2%}")
    print(f"total_pnl={stats.total_pnl:.2f}")
    print(f"roi={stats.roi:.2%}")
    print(f"max_drawdown={stats.max_drawdown:.2f}")
    print(f"ending_bankroll={result.ending_bankroll:.2f}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
