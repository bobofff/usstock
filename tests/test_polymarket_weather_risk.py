from __future__ import annotations

import unittest

from usstock.polymarket_weather.buckets import parse_temperature_bucket
from usstock.polymarket_weather.models import BucketSignal, MarketBucket
from usstock.polymarket_weather.risk import PositionSizer, RiskConfig


def make_signal(probability: float = 0.60, price: float = 0.50) -> BucketSignal:
    market_bucket = MarketBucket(
        market_id="manual",
        question="Manual",
        slug=None,
        condition_id=None,
        outcome="80",
        price=price,
        bucket=parse_temperature_bucket("80"),
    )
    return BucketSignal(
        market_bucket=market_bucket,
        probability=probability,
        market_price=price,
        edge=probability - price,
        expected_value=probability - price,
        fair_price=probability,
        recommendation="BUY_YES",
    )


class PositionSizerTest(unittest.TestCase):
    def test_half_kelly_is_capped_by_trade_fraction(self) -> None:
        recommendation = PositionSizer().size_yes(
            make_signal(),
            RiskConfig(
                bankroll=1_000,
                kelly_mode="half",
                max_trade_fraction=0.03,
                min_edge=0.01,
            ),
        )

        self.assertTrue(recommendation.should_trade)
        self.assertAlmostEqual(recommendation.full_kelly_fraction, 0.20)
        self.assertAlmostEqual(recommendation.scaled_kelly_fraction, 0.10)
        self.assertAlmostEqual(recommendation.stake, 30.0)
        self.assertAlmostEqual(recommendation.shares, 60.0)

    def test_skip_when_edge_is_too_small(self) -> None:
        recommendation = PositionSizer().size_yes(
            make_signal(probability=0.52, price=0.50),
            RiskConfig(bankroll=1_000, min_edge=0.03),
        )

        self.assertFalse(recommendation.should_trade)
        self.assertIn("edge below", recommendation.reason)


if __name__ == "__main__":
    unittest.main()
