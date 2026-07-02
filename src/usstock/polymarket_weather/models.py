"""Shared domain models for Polymarket weather markets."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from typing import Any, Literal, Mapping


TemperatureKind = Literal["high", "low"]
TemperatureUnit = Literal["C", "F"]


@dataclass(frozen=True)
class TemperatureBucket:
    """Continuous temperature interval used by market outcomes."""

    label: str
    lower: float | None
    upper: float | None
    unit: TemperatureUnit = "F"
    lower_inclusive: bool = True
    upper_inclusive: bool = False

    def contains(self, value: float) -> bool:
        if self.lower is not None:
            if self.lower_inclusive and value < self.lower:
                return False
            if not self.lower_inclusive and value <= self.lower:
                return False
        if self.upper is not None:
            if self.upper_inclusive and value > self.upper:
                return False
            if not self.upper_inclusive and value >= self.upper:
                return False
        return True

    @property
    def canonical_key(self) -> str:
        lower = "-inf" if self.lower is None else f"{self.lower:.4f}"
        upper = "inf" if self.upper is None else f"{self.upper:.4f}"
        return f"{self.unit}:{lower}:{upper}"


@dataclass(frozen=True)
class CityConfig:
    """Settlement-location configuration for a weather market city."""

    city_id: str
    name: str
    latitude: float
    longitude: float
    timezone: str = "auto"
    settlement_station: str | None = None
    settlement_unit: TemperatureUnit = "F"
    weather_models: tuple[str, ...] = ("ecmwf_ifs025", "gfs_seamless", "ukmo_seamless")
    model_weights: Mapping[str, float] = field(default_factory=dict)
    model_error_std: float = 2.5
    min_distribution_std: float = 1.0


@dataclass(frozen=True)
class ForecastPoint:
    """One forecast value from one weather model or provider."""

    city_id: str
    target_date: date
    kind: TemperatureKind
    value: float
    unit: TemperatureUnit
    source_model: str
    generated_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    raw_payload: Mapping[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class EnsembleForecast:
    """All forecast points used for one city/date/kind prediction."""

    city: CityConfig
    target_date: date
    kind: TemperatureKind
    points: tuple[ForecastPoint, ...]
    fetched_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))

    @property
    def source_models(self) -> tuple[str, ...]:
        return tuple(point.source_model for point in self.points)


@dataclass(frozen=True)
class MarketBucket:
    """A tradable Polymarket outcome mapped to a temperature bucket."""

    market_id: str
    question: str
    slug: str | None
    condition_id: str | None
    outcome: str
    price: float
    bucket: TemperatureBucket
    token_id: str | None = None
    raw_payload: Mapping[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class BucketProbability:
    bucket: TemperatureBucket
    probability: float


@dataclass(frozen=True)
class ForecastDistribution:
    """Normal approximation derived from the forecast ensemble."""

    mean: float
    std: float
    unit: TemperatureUnit
    probabilities: tuple[BucketProbability, ...]


@dataclass(frozen=True)
class BucketSignal:
    """Prediction-vs-market signal for one outcome bucket."""

    market_bucket: MarketBucket
    probability: float
    market_price: float
    edge: float
    expected_value: float
    fair_price: float
    recommendation: str


@dataclass(frozen=True)
class PredictionReport:
    """Full prediction report for one weather market."""

    city: CityConfig
    target_date: date
    kind: TemperatureKind
    market_question: str
    distribution: ForecastDistribution
    ensemble: EnsembleForecast
    signals: tuple[BucketSignal, ...]
    generated_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))

    def top_edges(self, limit: int = 5) -> tuple[BucketSignal, ...]:
        return tuple(
            sorted(self.signals, key=lambda signal: signal.edge, reverse=True)[:limit]
        )
