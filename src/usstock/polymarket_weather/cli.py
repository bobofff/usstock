"""Command line interface for Polymarket weather trading research."""

from __future__ import annotations

import argparse
from datetime import date, datetime, timezone
from pathlib import Path

from usstock.polymarket_weather.backtest import main as backtest_main
from usstock.polymarket_weather.cache import FileCache
from usstock.polymarket_weather.config import (
    WeatherTradingConfig,
    get_city,
    load_trading_config,
)
from usstock.polymarket_weather.engine import PredictionEngine
from usstock.polymarket_weather.http import JsonHttpClient
from usstock.polymarket_weather.ledger import (
    TradeLedger,
    TradeRecord,
    make_trade_id,
)
from usstock.polymarket_weather.market import GammaMarketClient
from usstock.polymarket_weather.models import BucketSignal, TemperatureKind
from usstock.polymarket_weather.risk import PositionSizer, RiskConfig
from usstock.polymarket_weather.weather import (
    NWSForecastClient,
    OpenMeteoForecastClient,
    WeatherEnsembleProvider,
)


def _parse_date(value: str) -> date:
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("日期格式必须是 YYYY-MM-DD") from exc


def _parse_models(value: str | None) -> tuple[str, ...] | None:
    if not value:
        return None
    return tuple(item.strip() for item in value.split(",") if item.strip())


def _load_config(args: argparse.Namespace) -> WeatherTradingConfig:
    config_path = Path(args.config).expanduser() if getattr(args, "config", None) else None
    return load_trading_config(config_path)


def _build_engine(config: WeatherTradingConfig, *, buy_edge_threshold: float) -> PredictionEngine:
    cache = FileCache()
    open_meteo_http = JsonHttpClient(
        base_url=config.open_meteo_base_url,
        timeout_seconds=config.request_timeout_seconds,
    )
    gamma_http = JsonHttpClient(
        base_url=config.gamma_base_url,
        timeout_seconds=config.request_timeout_seconds,
    )
    clob_http = JsonHttpClient(
        base_url=config.clob_base_url,
        timeout_seconds=config.request_timeout_seconds,
    )
    nws_http = JsonHttpClient(
        base_url="https://api.weather.gov",
        timeout_seconds=config.request_timeout_seconds,
        user_agent="usstock-polymarket-weather/0.1 contact:local",
    )
    weather_provider = WeatherEnsembleProvider(
        open_meteo=OpenMeteoForecastClient(
            base_url=config.open_meteo_base_url,
            http_client=open_meteo_http,
            cache=cache,
            cache_max_age_seconds=config.cache_max_age_seconds,
        ),
        nws=NWSForecastClient(
            http_client=nws_http,
            cache=cache,
            cache_max_age_seconds=config.cache_max_age_seconds,
        ),
    )
    market_client = GammaMarketClient(
        gamma_base_url=config.gamma_base_url,
        clob_base_url=config.clob_base_url,
        http_client=gamma_http,
        clob_http_client=clob_http,
        cache=cache,
        cache_max_age_seconds=min(config.cache_max_age_seconds, 90),
    )
    return PredictionEngine(
        weather_provider=weather_provider,
        market_client=market_client,
        buy_edge_threshold=buy_edge_threshold,
    )


def _kind(value: str) -> TemperatureKind:
    text = value.strip().lower()
    if text not in {"high", "low"}:
        raise argparse.ArgumentTypeError("--kind must be high or low")
    return text  # type: ignore[return-value]


def forecast_command(args: argparse.Namespace) -> int:
    config = _load_config(args)
    city = get_city(args.city, config)
    engine = _build_engine(config, buy_edge_threshold=args.min_edge)
    ensemble = engine.weather_provider.fetch_ensemble(
        city,
        target_date=args.date,
        kind=args.kind,
        models=_parse_models(args.models),
    )
    print(f"城市: {city.name} ({city.city_id})")
    print(f"日期: {args.date.isoformat()}  类型: {args.kind}  结算单位: {city.settlement_unit}")
    for point in ensemble.points:
        print(f"{point.source_model:18s} {point.value:8.2f} {point.unit}")
    return 0


def market_command(args: argparse.Namespace) -> int:
    config = _load_config(args)
    city = get_city(args.city, config) if args.city else None
    engine = _build_engine(config, buy_edge_threshold=args.min_edge)
    unit = city.settlement_unit if city else args.unit
    buckets = engine.market_client.get_market_buckets(
        query=args.query,
        slug=args.slug,
        condition_id=args.condition_id,
        default_unit=unit,
        refresh_clob_midpoints=args.refresh_clob,
    )
    for bucket in buckets:
        lower = "-inf" if bucket.bucket.lower is None else f"{bucket.bucket.lower:.2f}"
        upper = "inf" if bucket.bucket.upper is None else f"{bucket.bucket.upper:.2f}"
        print(
            f"{bucket.outcome:24s} price={bucket.price:.4f} "
            f"range={lower}..{upper} {bucket.bucket.unit} token={bucket.token_id or '-'}"
        )
    return 0


def signal_command(args: argparse.Namespace) -> int:
    config = _load_config(args)
    city = get_city(args.city, config)
    query = args.query
    if not query and not args.slug and not args.condition_id:
        temp_word = "high" if args.kind == "high" else "low"
        query = f"{city.name} {args.date.isoformat()} {temp_word} temperature"

    engine = _build_engine(config, buy_edge_threshold=args.min_edge)
    report = engine.build_report(
        city=city,
        target_date=args.date,
        kind=args.kind,
        market_query=query,
        market_slug=args.slug,
        condition_id=args.condition_id,
        models=_parse_models(args.models),
        refresh_clob_midpoints=args.refresh_clob,
    )
    print(f"市场: {report.market_question}")
    print(
        f"预测: mean={report.distribution.mean:.2f} "
        f"std={report.distribution.std:.2f} {report.distribution.unit} "
        f"models={','.join(report.ensemble.source_models)}"
    )
    print("outcome                  prob    price   edge    action        stake")
    print("-" * 74)
    risk = RiskConfig(
        bankroll=args.bankroll,
        kelly_mode=args.kelly,
        min_edge=args.min_edge,
        max_trade_fraction=args.max_trade_fraction,
        max_daily_fraction=args.max_daily_fraction,
        max_market_fraction=args.max_market_fraction,
        max_city_fraction=args.max_city_fraction,
    )
    sizer = PositionSizer()
    best_trade: tuple[BucketSignal, float, float] | None = None
    for signal in report.signals:
        position = sizer.size_yes(signal, risk)
        stake_text = f"{position.stake:.2f}" if position.should_trade else "-"
        print(
            f"{signal.market_bucket.outcome[:24]:24s} "
            f"{signal.probability:6.2%} {signal.market_price:7.4f} "
            f"{signal.edge:7.2%} {signal.recommendation:13s} {stake_text:>8s}"
        )
        if position.should_trade and (
            best_trade is None or position.stake > best_trade[1]
        ):
            best_trade = (signal, position.stake, position.shares)

    if args.record_best and best_trade:
        signal, stake, shares = best_trade
        ledger = TradeLedger(Path(args.ledger).expanduser() if args.ledger else TradeLedger().path)
        timestamp = datetime.now(timezone.utc)
        record = TradeRecord(
            trade_id=make_trade_id(
                city_id=city.city_id,
                target_date=args.date,
                kind=args.kind,
                outcome=signal.market_bucket.outcome,
                timestamp=timestamp,
            ),
            timestamp=timestamp,
            city_id=city.city_id,
            target_date=args.date,
            kind=args.kind,
            market_slug=signal.market_bucket.slug,
            outcome=signal.market_bucket.outcome,
            token_id=signal.market_bucket.token_id,
            side="YES",
            price=signal.market_price,
            probability=signal.probability,
            edge=signal.edge,
            stake=stake,
            shares=shares,
            notes="recorded from CLI signal command",
        )
        path = ledger.append(record)
        print(f"已记录交易日志: {path}")
    return 0


def size_command(args: argparse.Namespace) -> int:
    from usstock.polymarket_weather.buckets import parse_temperature_bucket
    from usstock.polymarket_weather.models import MarketBucket

    bucket = parse_temperature_bucket(args.outcome, default_unit=args.unit)
    market_bucket = MarketBucket(
        market_id="manual",
        question="manual sizing",
        slug=None,
        condition_id=None,
        outcome=args.outcome,
        price=args.price,
        bucket=bucket,
    )
    signal = BucketSignal(
        market_bucket=market_bucket,
        probability=args.probability,
        market_price=args.price,
        edge=args.probability - args.price,
        expected_value=args.probability - args.price,
        fair_price=args.probability,
        recommendation="BUY_YES",
    )
    recommendation = PositionSizer().size_yes(
        signal,
        RiskConfig(
            bankroll=args.bankroll,
            kelly_mode=args.kelly,
            min_edge=args.min_edge,
            max_trade_fraction=args.max_trade_fraction,
        ),
    )
    print(f"should_trade={recommendation.should_trade}")
    print(f"reason={recommendation.reason}")
    print(f"full_kelly={recommendation.full_kelly_fraction:.2%}")
    print(f"scaled_kelly={recommendation.scaled_kelly_fraction:.2%}")
    print(f"stake={recommendation.stake:.2f}")
    print(f"shares={recommendation.shares:.2f}")
    print(f"max_loss={recommendation.max_loss:.2f}")
    print(f"potential_profit={recommendation.potential_profit:.2f}")
    return 0


def performance_command(args: argparse.Namespace) -> int:
    ledger = TradeLedger(Path(args.ledger).expanduser())
    stats = ledger.stats()
    print(f"trades={stats.total_trades}")
    print(f"settled={stats.settled_trades}")
    print(f"win_rate={stats.win_rate:.2%}")
    print(f"total_stake={stats.total_stake:.2f}")
    print(f"total_pnl={stats.total_pnl:.2f}")
    print(f"roi={stats.roi:.2%}")
    print(f"max_drawdown={stats.max_drawdown:.2f}")
    print(f"average_edge={stats.average_edge:.2%}")
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="usstock weather",
        description="Polymarket weather prediction, edge, and risk toolkit.",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    def add_common(subparser: argparse.ArgumentParser) -> None:
        subparser.add_argument("--config", help="JSON/YAML config path")
        subparser.add_argument("--min-edge", type=float, default=0.03)

    forecast = subparsers.add_parser("forecast", help="Fetch ensemble forecast")
    add_common(forecast)
    forecast.add_argument("--city", required=True)
    forecast.add_argument("--date", required=True, type=_parse_date)
    forecast.add_argument("--kind", default="high", type=_kind)
    forecast.add_argument("--models", help="Comma-separated model list")
    forecast.set_defaults(func=forecast_command)

    market = subparsers.add_parser("market", help="Inspect Polymarket buckets")
    add_common(market)
    market.add_argument("--city")
    market.add_argument("--query")
    market.add_argument("--slug")
    market.add_argument("--condition-id")
    market.add_argument("--unit", default="F", choices=["F", "C"])
    market.add_argument("--refresh-clob", action="store_true")
    market.set_defaults(func=market_command)

    signal = subparsers.add_parser("signal", help="Build forecast-vs-market edge report")
    add_common(signal)
    signal.add_argument("--city", required=True)
    signal.add_argument("--date", required=True, type=_parse_date)
    signal.add_argument("--kind", default="high", type=_kind)
    signal.add_argument("--query")
    signal.add_argument("--slug")
    signal.add_argument("--condition-id")
    signal.add_argument("--models")
    signal.add_argument("--refresh-clob", action="store_true")
    signal.add_argument("--bankroll", type=float, default=1_000.0)
    signal.add_argument("--kelly", default="half", choices=["full", "half", "quarter"])
    signal.add_argument("--max-trade-fraction", type=float, default=0.03)
    signal.add_argument("--max-daily-fraction", type=float, default=0.12)
    signal.add_argument("--max-market-fraction", type=float, default=0.06)
    signal.add_argument("--max-city-fraction", type=float, default=0.08)
    signal.add_argument("--record-best", action="store_true")
    signal.add_argument("--ledger")
    signal.set_defaults(func=signal_command)

    size = subparsers.add_parser("size", help="Size a manual YES position")
    size.add_argument("--outcome", required=True)
    size.add_argument("--unit", default="F", choices=["F", "C"])
    size.add_argument("--probability", required=True, type=float)
    size.add_argument("--price", required=True, type=float)
    size.add_argument("--bankroll", required=True, type=float)
    size.add_argument("--kelly", default="half", choices=["full", "half", "quarter"])
    size.add_argument("--min-edge", type=float, default=0.03)
    size.add_argument("--max-trade-fraction", type=float, default=0.03)
    size.set_defaults(func=size_command)

    backtest = subparsers.add_parser("backtest", help="Backtest historical CSV signals")
    backtest.add_argument("--csv", required=True)
    backtest.add_argument("--bankroll", type=float, default=1_000.0)
    backtest.add_argument("--kelly", default="half", choices=["full", "half", "quarter"])
    backtest.add_argument("--min-edge", type=float, default=0.03)
    backtest.set_defaults(func=lambda args: backtest_main([
        "--csv",
        args.csv,
        "--bankroll",
        str(args.bankroll),
        "--kelly",
        args.kelly,
        "--min-edge",
        str(args.min_edge),
    ]))

    performance = subparsers.add_parser("performance", help="Summarize local trade ledger")
    performance.add_argument("--ledger", default=str(TradeLedger().path))
    performance.set_defaults(func=performance_command)

    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    return int(args.func(args))


if __name__ == "__main__":
    raise SystemExit(main())
