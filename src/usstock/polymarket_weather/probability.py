"""Probability model for daily high/low temperature buckets."""

from __future__ import annotations

import math
from collections.abc import Mapping, Sequence

from usstock.polymarket_weather.models import (
    BucketProbability,
    EnsembleForecast,
    ForecastDistribution,
    ForecastPoint,
    TemperatureBucket,
    TemperatureUnit,
)
from usstock.polymarket_weather.units import convert_temperature


class ProbabilityError(RuntimeError):
    """Raised when a forecast distribution cannot be computed."""


def _normal_cdf(value: float, *, mean: float, std: float) -> float:
    if math.isinf(value):
        return 0.0 if value < 0 else 1.0
    z = (value - mean) / (std * math.sqrt(2.0))
    return 0.5 * (1.0 + math.erf(z))


def _weighted_mean(values: Sequence[float], weights: Sequence[float]) -> float:
    total_weight = sum(weights)
    if total_weight <= 0:
        raise ProbabilityError("Model weights must sum to a positive value.")
    return sum(value * weight for value, weight in zip(values, weights, strict=True)) / total_weight


def _weighted_variance(
    values: Sequence[float],
    weights: Sequence[float],
    mean: float,
) -> float:
    total_weight = sum(weights)
    if total_weight <= 0:
        return 0.0
    return sum(weight * (value - mean) ** 2 for value, weight in zip(values, weights, strict=True)) / total_weight


def _weights_for_points(
    points: Sequence[ForecastPoint],
    model_weights: Mapping[str, float],
) -> tuple[float, ...]:
    weights: list[float] = []
    for point in points:
        weights.append(float(model_weights.get(point.source_model, 1.0)))
    return tuple(weights)


class TemperatureProbabilityModel:
    """Normal ensemble model with configurable model-error floor."""

    def __init__(self, *, normalize_market_buckets: bool = True) -> None:
        self.normalize_market_buckets = normalize_market_buckets

    def build_distribution(
        self,
        ensemble: EnsembleForecast,
        buckets: Sequence[TemperatureBucket],
        *,
        unit: TemperatureUnit | None = None,
    ) -> ForecastDistribution:
        if not ensemble.points:
            raise ProbabilityError("No forecast points available.")
        if not buckets:
            raise ProbabilityError("No temperature buckets provided.")

        distribution_unit = unit or buckets[0].unit
        values = [
            convert_temperature(
                point.value,
                from_unit=point.unit,
                to_unit=distribution_unit,
            )
            for point in ensemble.points
        ]
        weights = _weights_for_points(ensemble.points, ensemble.city.model_weights)
        mean = _weighted_mean(values, weights)
        model_variance = _weighted_variance(values, weights, mean)
        std = math.sqrt(model_variance + ensemble.city.model_error_std**2)
        std = max(std, ensemble.city.min_distribution_std)

        raw_probabilities: list[float] = []
        for bucket in buckets:
            lower = -math.inf if bucket.lower is None else convert_temperature(
                bucket.lower,
                from_unit=bucket.unit,
                to_unit=distribution_unit,
            )
            upper = math.inf if bucket.upper is None else convert_temperature(
                bucket.upper,
                from_unit=bucket.unit,
                to_unit=distribution_unit,
            )
            probability = max(
                0.0,
                _normal_cdf(upper, mean=mean, std=std)
                - _normal_cdf(lower, mean=mean, std=std),
            )
            raw_probabilities.append(probability)

        total = sum(raw_probabilities)
        if self.normalize_market_buckets and total > 0:
            raw_probabilities = [probability / total for probability in raw_probabilities]

        probabilities = tuple(
            BucketProbability(bucket=bucket, probability=probability)
            for bucket, probability in zip(buckets, raw_probabilities, strict=True)
        )
        return ForecastDistribution(
            mean=mean,
            std=std,
            unit=distribution_unit,
            probabilities=probabilities,
        )
