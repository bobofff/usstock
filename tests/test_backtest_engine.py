from __future__ import annotations

import unittest
from datetime import date, timedelta
from decimal import Decimal

from usstock.backtest import engine


def make_candidate(
    ticker: str = "NVDA",
    *,
    rank: int = 1,
    score: str = "72.5",
) -> engine.ReportCandidate:
    return engine.ReportCandidate(
        run_date=date(2026, 6, 1),
        profile="default",
        report_uid="daily_analysis:default:2026-06-01",
        ticker=ticker,
        company_name=f"{ticker} Corp",
        rank=rank,
        score=Decimal(score),
        attention_label="优先复核",
        event_type="订单和合作",
        risk_level="中低",
        primary_topic_slug="ai_infrastructure",
        topic_slugs=("ai_infrastructure",),
        action_bias="review",
    )


def make_points(closes: list[str]) -> list[engine.PricePoint]:
    start = date(2026, 6, 2)
    return [
        engine.PricePoint(
            price_date=start + timedelta(days=index),
            close_price=Decimal(close),
            data_source="manual_csv",
        )
        for index, close in enumerate(closes)
    ]


class BacktestEngineTest(unittest.TestCase):
    def test_compute_candidate_performance_uses_next_trading_close_as_entry(self) -> None:
        closes = [
            "100",
            "105",
            "95",
            "110",
            "108",
            "120",
            "121",
            "122",
            "123",
            "124",
            "125",
            "126",
            "127",
            "128",
            "129",
            "130",
            "131",
            "132",
            "133",
            "134",
            "135",
        ]

        performance = engine.compute_candidate_performance(
            make_candidate(),
            make_points(closes),
        )

        self.assertEqual(performance.status, "complete")
        self.assertEqual(performance.entry_date, date(2026, 6, 2))
        self.assertEqual(performance.entry_close, Decimal("100"))
        self.assertEqual(performance.horizons[1].return_pct, Decimal("5.000000"))
        self.assertEqual(performance.horizons[5].return_pct, Decimal("20.000000"))
        self.assertEqual(performance.horizons[5].max_drawdown_pct, Decimal("-5.000000"))
        self.assertEqual(performance.horizons[20].return_pct, Decimal("35.000000"))

    def test_compute_candidate_performance_marks_missing_horizons(self) -> None:
        performance = engine.compute_candidate_performance(
            make_candidate(),
            make_points(["100"]),
        )

        self.assertEqual(performance.status, "no_horizon_price")
        self.assertEqual(performance.entry_close, Decimal("100"))
        self.assertEqual(performance.horizons, {})

    def test_summarize_performances_groups_by_rank_score_and_topic(self) -> None:
        positive = engine.compute_candidate_performance(
            make_candidate("NVDA", rank=1, score="72"),
            make_points(["100", "101", "102", "103", "104", "110"] + ["111"] * 15),
        )
        negative = engine.compute_candidate_performance(
            make_candidate("AAPL", rank=8, score="45"),
            make_points(["100", "99", "98", "97", "96", "90"] + ["89"] * 15),
        )

        summary = engine.summarize_performances((positive, negative))

        self.assertEqual(summary["candidate_count"], 2)
        self.assertEqual(summary["horizons"]["5"]["evaluated"], 2)
        self.assertEqual(summary["horizons"]["5"]["win_rate_pct"], "50.0000")
        self.assertEqual(summary["horizons"]["5"]["avg_return_pct"], "0.0000")
        self.assertEqual(summary["rank_buckets"][0]["group"], "Top 10")
        self.assertEqual(summary["primary_topics"][0]["group"], "ai_infrastructure")

    def test_upsert_candidate_performance_binds_all_insert_columns(self) -> None:
        class FakeConnection:
            def __init__(self) -> None:
                self.sql = ""
                self.params: tuple[object, ...] = ()

            def execute(self, sql: str, params: tuple[object, ...]) -> None:
                self.sql = sql
                self.params = params

        performance = engine.compute_candidate_performance(
            make_candidate(),
            make_points(["100", "101", "102", "103", "104", "105"] + ["106"] * 15),
        )
        conn = FakeConnection()

        engine.upsert_candidate_performance(conn, performance)

        self.assertIn("ON CONFLICT (performance_uid)", conn.sql)
        self.assertEqual(len(conn.params), 36)

    def test_fetch_price_points_casts_nullable_price_source_parameter(self) -> None:
        class FakeResult:
            def fetchall(self) -> list[tuple[object, ...]]:
                return []

        class FakeConnection:
            def __init__(self) -> None:
                self.sql = ""
                self.params: tuple[object, ...] = ()

            def execute(self, sql: str, params: tuple[object, ...]) -> FakeResult:
                self.sql = sql
                self.params = params
                return FakeResult()

        conn = FakeConnection()

        points = engine.fetch_price_points(
            conn,
            ticker="NVDA",
            after_date=date(2026, 6, 1),
            through_date=date(2026, 6, 30),
            price_source=None,
        )

        self.assertEqual(points, [])
        self.assertIn("CAST(%s AS text) IS NULL", conn.sql)
        self.assertIsNone(conn.params[3])


if __name__ == "__main__":
    unittest.main()
