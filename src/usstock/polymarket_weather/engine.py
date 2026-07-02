"""Prediction engine that turns forecasts and market buckets into edge."""

from __future__ import annotations

from datetime import date
from typing import Sequence

from usstock.polymarket_weather.market import GammaMarketClient
from usstock.polymarket_weather.models import (
    BucketSignal,
    CityConfig,
    MarketBucket,
    PredictionReport,
    TemperatureBucket,
    TemperatureKind,
)
from usstock.polymarket_weather.probability import TemperatureProbabilityModel
from usstock.polymarket_weather.weather import WeatherEnsembleProvider


class PredictionEngineError(RuntimeError):
    """Raised when a prediction report cannot be built."""


class PredictionEngine:
    """High-level orchestrator for forecast, market, probability, and edge."""

    def __init__(
        self,
        *,
        weather_provider: WeatherEnsembleProvider | None = None,
        market_client: GammaMarketClient | None = None,
        probability_model: TemperatureProbabilityModel | None = None,
        buy_edge_threshold: float = 0.03,
    ) -> None:
        self.weather_provider = weather_provider or WeatherEnsembleProvider()
        self.market_client = market_client or GammaMarketClient()
        self.probability_model = probability_model or TemperatureProbabilityModel()
        self.buy_edge_threshold = buy_edge_threshold

    def build_report(
        self,
        *,
        city: CityConfig,
        target_date: date,
        kind: TemperatureKind,
        market_query: str | None = None,
        market_slug: str | None = None,
        condition_id: str | None = None,
        fallback_buckets: Sequence[TemperatureBucket] = (),
        models: tuple[str, ...] | None = None,
        refresh_clob_midpoints: bool = False,
    ) -> PredictionReport:
        market_buckets = self._load_market_buckets(
            city=city,
            market_query=market_query,
            market_slug=market_slug,
            condition_id=condition_id,
            fallback_buckets=fallback_buckets,
            refresh_clob_midpoints=refresh_clob_midpoints,
        )
        if not market_buckets:
            raise PredictionEngineError("No market buckets were found or parsed.")

        ensemble = self.weather_provider.fetch_ensemble(
            city,
            target_date=target_date,
            kind=kind,
            models=models,
        )
        buckets = tuple(market_bucket.bucket for market_bucket in market_buckets)
        distribution = self.probability_model.build_distribution(
            ensemble,
            buckets,
            unit=buckets[0].unit,
        )
        signals: list[BucketSignal] = []
        for market_bucket, probability in zip(
            market_buckets,
            distribution.probabilities,
            strict=True,
        ):
            market_price = max(0.0, min(1.0, market_bucket.price))
            edge = probability.probability - market_price
            expected_value = edge
            if edge >= self.buy_edge_threshold:
                recommendation = "BUY_YES"
            elif edge <= -self.buy_edge_threshold:
                recommendation = "TOO_EXPENSIVE"
            else:
                recommendation = "WATCH"
            signals.append(
                BucketSignal(
                    market_bucket=market_bucket,
                    probability=probability.probability,
                    market_price=market_price,
                    edge=edge,
                    expected_value=expected_value,
                    fair_price=probability.probability,
                    recommendation=recommendation,
                )
            )

        question = market_buckets[0].question or (
            f"{city.name} {target_date.isoformat()} {kind} temperature"
        )
        return PredictionReport(
            city=city,
            target_date=target_date,
            kind=kind,
            market_question=question,
            distribution=distribution,
            ensemble=ensemble,
            signals=tuple(sorted(signals, key=lambda signal: signal.edge, reverse=True)),
        )

    def _load_market_buckets(
        self,
        *,
        city: CityConfig,
        market_query: str | None,
        market_slug: str | None,
        condition_id: str | None,
        fallback_buckets: Sequence[TemperatureBucket],
        refresh_clob_midpoints: bool,
    ) -> tuple[MarketBucket, ...]:
        if market_query or market_slug or condition_id:
            return self.market_client.get_market_buckets(
                query=market_query,
                slug=market_slug,
                condition_id=condition_id,
                default_unit=city.settlement_unit,
                refresh_clob_midpoints=refresh_clob_midpoints,
            )
        if fallback_buckets:
            return tuple(
                MarketBucket(
                    market_id="manual",
                    question="Manual fallback buckets",
                    slug=None,
                    condition_id=None,
                    outcome=bucket.label,
                    price=0.0,
                    bucket=bucket,
                )
                for bucket in fallback_buckets
            )
        raise PredictionEngineError("A market query/slug/condition_id or fallback buckets is required.")
