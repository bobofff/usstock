from __future__ import annotations

import unittest
from datetime import date

from usstock.polymarket_weather.buckets import build_regular_buckets
from usstock.polymarket_weather.models import CityConfig, EnsembleForecast, ForecastPoint
from usstock.polymarket_weather.probability import TemperatureProbabilityModel


class TemperatureProbabilityModelTest(unittest.TestCase):
    def test_distribution_normalizes_market_buckets(self) -> None:
        city = CityConfig(
            city_id="test",
            name="Test City",
            latitude=0,
            longitude=0,
            settlement_unit="F",
            model_error_std=2.0,
            min_distribution_std=1.0,
        )
        ensemble = EnsembleForecast(
            city=city,
            target_date=date(2026, 7, 3),
            kind="high",
            points=(
                ForecastPoint(
                    city_id="test",
                    target_date=date(2026, 7, 3),
                    kind="high",
                    value=80,
                    unit="F",
                    source_model="gfs",
                ),
                ForecastPoint(
                    city_id="test",
                    target_date=date(2026, 7, 3),
                    kind="high",
                    value=82,
                    unit="F",
                    source_model="ecmwf",
                ),
            ),
        )
        buckets = build_regular_buckets(start=78, end=84, unit="F")

        distribution = TemperatureProbabilityModel().build_distribution(ensemble, buckets)

        self.assertAlmostEqual(sum(item.probability for item in distribution.probabilities), 1.0)
        self.assertAlmostEqual(distribution.mean, 81.0)
        self.assertGreater(distribution.std, 2.0)


if __name__ == "__main__":
    unittest.main()
