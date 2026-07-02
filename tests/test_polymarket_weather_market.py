from __future__ import annotations

import unittest

from usstock.polymarket_weather.market import parse_market_buckets


class MarketParsingTest(unittest.TestCase):
    def test_parse_gamma_market_outcomes_prices_and_token_ids(self) -> None:
        market = {
            "id": "123",
            "question": "NYC high temperature?",
            "slug": "nyc-high-temp",
            "conditionId": "0xabc",
            "outcomes": '["74 or below", "75 to 76", "77 or above"]',
            "outcomePrices": '["0.12", "0.33", "0.55"]',
            "clobTokenIds": '["a", "b", "c"]',
        }

        buckets = tuple(parse_market_buckets(market, default_unit="F"))

        self.assertEqual(len(buckets), 3)
        self.assertEqual(buckets[1].outcome, "75 to 76")
        self.assertEqual(buckets[1].price, 0.33)
        self.assertEqual(buckets[1].token_id, "b")
        self.assertEqual(buckets[1].bucket.lower, 74.5)
        self.assertEqual(buckets[1].bucket.upper, 76.5)


if __name__ == "__main__":
    unittest.main()
