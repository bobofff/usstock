from __future__ import annotations

import unittest

from usstock.polymarket_weather.buckets import (
    build_regular_buckets,
    parse_temperature_bucket,
)


class TemperatureBucketTest(unittest.TestCase):
    def test_parse_range_bucket_adds_integer_half_degree_boundaries(self) -> None:
        bucket = parse_temperature_bucket("84 to 85°F")

        self.assertEqual(bucket.unit, "F")
        self.assertEqual(bucket.lower, 83.5)
        self.assertEqual(bucket.upper, 85.5)

    def test_parse_tail_buckets(self) -> None:
        below = parse_temperature_bucket("74 or below")
        above = parse_temperature_bucket("90 or above")

        self.assertIsNone(below.lower)
        self.assertEqual(below.upper, 74.5)
        self.assertEqual(above.lower, 89.5)
        self.assertIsNone(above.upper)

    def test_build_regular_buckets_includes_tails(self) -> None:
        buckets = build_regular_buckets(start=70, end=72, unit="F")

        self.assertEqual([bucket.label for bucket in buckets], [
            "69 or below",
            "70",
            "71",
            "72",
            "73 or above",
        ])


if __name__ == "__main__":
    unittest.main()
