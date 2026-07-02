from __future__ import annotations

import unittest
from datetime import datetime, timezone
from decimal import Decimal
from unittest.mock import patch

from usstock.discovery import daily


class FakeConnection:
    def __enter__(self) -> "FakeConnection":
        return self

    def __exit__(self, *args: object) -> None:
        return None

    def transaction(self) -> "FakeConnection":
        return self


class RecordingConnection:
    def __init__(self) -> None:
        self.calls: list[tuple[str, tuple[object, ...]]] = []

    def execute(
        self,
        sql: str,
        params: tuple[object, ...] = (),
    ) -> "RecordingConnection":
        self.calls.append((sql, params))
        return self


class DiscoveryDailyTest(unittest.TestCase):
    def test_daily_discovery_migrates_before_opening_business_connection(self) -> None:
        events: list[tuple[str, str, bool | None]] = []
        database_url = "postgresql://example/discovery"

        def fake_ensure_schema(url: str) -> int:
            events.append(("migrate", url, None))
            return 1

        def fake_connect(url: str, *, autocommit: bool) -> FakeConnection:
            events.append(("connect", url, autocommit))
            return FakeConnection()

        with (
            patch.object(daily, "ensure_discovery_schema", side_effect=fake_ensure_schema),
            patch.object(daily.psycopg, "connect", side_effect=fake_connect),
            patch.object(daily, "upsert_market_topics", return_value=0),
            patch.object(daily, "fetch_active_topics", return_value=[]),
            patch.object(daily, "fetch_stock_universe", return_value={}),
            patch.object(daily, "fetch_sec_scan_tickers", return_value=[]),
            patch.object(daily, "fetch_fact_counts", return_value={}),
            patch.object(daily, "fetch_recent_finnhub_articles", return_value=[]),
            patch.object(daily, "fetch_recent_gdelt_articles", return_value=[]),
            patch.object(daily, "fetch_recent_sec_filings", return_value=[]),
            patch.object(daily, "upsert_candidate_scores", return_value=0),
            patch.object(daily, "upsert_daily_watchlist"),
        ):
            result = daily.run_daily_discovery(
                database_url=database_url,
                skip_finnhub_sync=True,
                skip_gdelt_sync=True,
                skip_sec_sync=True,
            )

        self.assertEqual(events[0], ("migrate", database_url, None))
        self.assertEqual(events[1], ("connect", database_url, False))
        self.assertEqual(result.stats["migrations_applied"], 1)

    def test_seed_market_topics_migrates_before_insert(self) -> None:
        events: list[str] = []
        database_url = "postgresql://example/seed"

        def fake_ensure_schema(url: str) -> int:
            self.assertEqual(url, database_url)
            events.append("migrate")
            return 1

        def fake_connect(url: str, *, autocommit: bool) -> FakeConnection:
            self.assertEqual(url, database_url)
            self.assertFalse(autocommit)
            events.append("connect")
            return FakeConnection()

        def fake_upsert(*args: object) -> int:
            events.append("upsert")
            return 10

        with (
            patch.object(daily, "ensure_discovery_schema", side_effect=fake_ensure_schema),
            patch.object(daily.psycopg, "connect", side_effect=fake_connect),
            patch.object(daily, "upsert_market_topics", side_effect=fake_upsert),
        ):
            count = daily.seed_market_topics(database_url)

        self.assertEqual(count, 10)
        self.assertEqual(events, ["migrate", "connect", "upsert"])

    def test_finnhub_mentions_map_news_to_ticker_and_topic(self) -> None:
        topic = daily.DEFAULT_MARKET_TOPICS[0]
        universe = {
            "AAPL": {
                "ticker": "AAPL",
                "company_name": "Apple Inc.",
                "sector": "Technology",
                "industry": "Consumer Electronics",
                "business_description": "AI devices and cloud services",
                "market_cap_usd": Decimal("3000000000000"),
                "avg_volume_30d": Decimal("50000000"),
                "is_active": True,
            }
        }
        candidates: dict[str, daily.CandidateAccumulator] = {}

        mentions = daily.build_finnhub_mentions(
            articles=[
                {
                    "article_uid": "finnhub:1",
                    "article_url": "https://example.com/aapl-ai",
                    "headline": "Apple supplier wins AI datacenter contract",
                    "summary": "The deal expands generative AI infrastructure.",
                    "category": "company",
                    "source_name": "Example News",
                    "related_tickers": ["AAPL"],
                    "published_at": datetime(2026, 6, 10, tzinfo=timezone.utc),
                    "endpoint": "market_news",
                }
            ],
            topics=[topic],
            universe=universe,
            candidates=candidates,
        )

        self.assertEqual(len(mentions), 1)
        self.assertEqual(mentions[0].ticker, "AAPL")
        self.assertEqual(mentions[0].topic_slug, "ai_infrastructure")
        self.assertIn("AAPL", candidates)
        self.assertEqual(candidates["AAPL"].finnhub_article_uids, {"finnhub:1"})

    def test_finnhub_mentions_mark_topic_without_related_tickers(self) -> None:
        topic = next(
            item for item in daily.DEFAULT_MARKET_TOPICS if item.topic_slug == "ma_ipos"
        )
        candidates: dict[str, daily.CandidateAccumulator] = {}

        mentions = daily.build_finnhub_mentions(
            articles=[
                {
                    "article_uid": "finnhub:merger-1",
                    "article_url": "https://example.com/acquisition",
                    "headline": "Industrial supplier announces acquisition",
                    "summary": "The company expands through a strategic deal.",
                    "category": "merger",
                    "source_name": "Example News",
                    "related_tickers": [],
                    "published_at": datetime(2026, 7, 2, tzinfo=timezone.utc),
                    "endpoint": "market_news",
                }
            ],
            topics=[topic],
            universe={},
            candidates=candidates,
        )

        self.assertEqual(len(mentions), 1)
        self.assertIsNone(mentions[0].ticker)
        self.assertEqual(mentions[0].topic_slug, "ma_ipos")
        self.assertEqual(mentions[0].source_uid, "finnhub:merger-1")
        self.assertEqual(candidates, {})

    def test_topic_match_does_not_match_short_keyword_inside_word(self) -> None:
        topic = daily.DEFAULT_MARKET_TOPICS[0]

        score, matched = daily.topic_match_score(
            topic,
            "The company said shares gained after paid advertising expanded.",
        )

        self.assertEqual(score, Decimal("0"))
        self.assertEqual(matched, [])

    def test_topic_match_keeps_short_keyword_with_word_boundary(self) -> None:
        topic = daily.DEFAULT_MARKET_TOPICS[0]

        score, matched = daily.topic_match_score(
            topic,
            "AI-driven datacenter demand lifted infrastructure spending.",
        )

        self.assertGreater(score, Decimal("0"))
        self.assertIn("ai", matched)

    def test_gdelt_topic_news_does_not_create_all_ticker_hints(self) -> None:
        topic = next(
            item for item in daily.DEFAULT_MARKET_TOPICS if item.topic_slug == "ai_infrastructure"
        )
        universe = {
            "NVDA": {
                "ticker": "NVDA",
                "company_name": "NVIDIA Corporation",
                "sector": "Technology",
                "industry": "Semiconductors",
                "business_description": "GPU and AI infrastructure",
                "market_cap_usd": Decimal("3000000000000"),
                "avg_volume_30d": Decimal("40000000"),
                "is_active": True,
            },
            "AMD": {
                "ticker": "AMD",
                "company_name": "Advanced Micro Devices, Inc.",
                "sector": "Technology",
                "industry": "Semiconductors",
                "business_description": "CPU and GPU chips",
                "market_cap_usd": Decimal("250000000000"),
                "avg_volume_30d": Decimal("30000000"),
                "is_active": True,
            },
        }
        candidates: dict[str, daily.CandidateAccumulator] = {}

        mentions = daily.build_gdelt_mentions(
            articles=[
                {
                    "article_url": "https://example.com/ai-capex",
                    "title": "AI datacenter investment boom lifts chip sector",
                    "domain": "example.com",
                    "language": "English",
                    "source_country": "US",
                    "tone": None,
                    "query_text": topic.gdelt_query,
                    "seen_at": datetime(2026, 7, 2, tzinfo=timezone.utc),
                    "request_url": "https://api.example.com",
                }
            ],
            topics=[topic],
            universe=universe,
            candidates=candidates,
        )

        self.assertEqual(len(mentions), 1)
        self.assertIsNone(mentions[0].ticker)
        self.assertEqual(candidates, {})

    def test_gdelt_company_named_news_creates_candidate(self) -> None:
        topic = next(
            item for item in daily.DEFAULT_MARKET_TOPICS if item.topic_slug == "ai_infrastructure"
        )
        universe = {
            "NVDA": {
                "ticker": "NVDA",
                "company_name": "NVIDIA Corporation",
                "sector": "Technology",
                "industry": "Semiconductors",
                "business_description": "GPU and AI infrastructure",
                "market_cap_usd": Decimal("3000000000000"),
                "avg_volume_30d": Decimal("40000000"),
                "is_active": True,
            },
            "AMD": {
                "ticker": "AMD",
                "company_name": "Advanced Micro Devices, Inc.",
                "sector": "Technology",
                "industry": "Semiconductors",
                "business_description": "CPU and GPU chips",
                "market_cap_usd": Decimal("250000000000"),
                "avg_volume_30d": Decimal("30000000"),
                "is_active": True,
            },
        }
        candidates: dict[str, daily.CandidateAccumulator] = {}

        mentions = daily.build_gdelt_mentions(
            articles=[
                {
                    "article_url": "https://example.com/nvidia-contract",
                    "title": "Nvidia wins AI datacenter contract from cloud provider",
                    "domain": "example.com",
                    "language": "English",
                    "source_country": "US",
                    "tone": None,
                    "query_text": topic.gdelt_query,
                    "seen_at": datetime(2026, 7, 2, tzinfo=timezone.utc),
                    "request_url": "https://api.example.com",
                }
            ],
            topics=[topic],
            universe=universe,
            candidates=candidates,
        )

        self.assertEqual([mention.ticker for mention in mentions], [None, "NVDA"])
        self.assertIn("NVDA", candidates)
        self.assertNotIn("AMD", candidates)
        self.assertEqual(candidates["NVDA"].gdelt_article_urls, {"https://example.com/nvidia-contract"})
        self.assertEqual(candidates["NVDA"].direct_gdelt_hits, 1)

        scores = daily.rank_candidates(
            candidates,
            run_date=daily.parse_run_date("2026-07-02"),
            top_n=5,
        )

        self.assertEqual(len(scores), 1)
        self.assertEqual(scores[0].ticker, "NVDA")
        self.assertGreater(scores[0].gdelt_score, Decimal("6"))

    def test_pure_topic_heat_does_not_rank_hint_large_caps(self) -> None:
        topic = next(
            item for item in daily.DEFAULT_MARKET_TOPICS if item.topic_slug == "ai_infrastructure"
        )
        universe = {
            "NVDA": {
                "ticker": "NVDA",
                "company_name": "NVIDIA Corporation",
                "sector": "Technology",
                "industry": "Semiconductors",
                "business_description": "GPU and AI infrastructure",
                "market_cap_usd": Decimal("3000000000000"),
                "avg_volume_30d": Decimal("40000000"),
                "is_active": True,
            },
            "MSFT": {
                "ticker": "MSFT",
                "company_name": "Microsoft Corporation",
                "sector": "Technology",
                "industry": "Software",
                "business_description": "Cloud and artificial intelligence services",
                "market_cap_usd": Decimal("3500000000000"),
                "avg_volume_30d": Decimal("25000000"),
                "is_active": True,
            },
        }
        candidates: dict[str, daily.CandidateAccumulator] = {}
        daily.build_stock_topic_mentions(
            topics=[topic],
            universe=universe,
            candidates=candidates,
        )
        daily.build_gdelt_mentions(
            articles=[
                {
                    "article_url": f"https://example.com/theme-{index}",
                    "title": "AI infrastructure demand expands across the market",
                    "domain": "example.com",
                    "language": "English",
                    "source_country": "US",
                    "tone": None,
                    "query_text": topic.gdelt_query,
                    "seen_at": datetime(2026, 7, 2, tzinfo=timezone.utc),
                    "request_url": "https://api.example.com",
                }
                for index in range(5)
            ],
            topics=[topic],
            universe=universe,
            candidates=candidates,
        )

        scores = daily.rank_candidates(
            candidates,
            run_date=daily.parse_run_date("2026-07-02"),
            top_n=10,
        )

        self.assertEqual(scores, ())

    def test_rank_candidates_combines_news_sec_and_liquidity(self) -> None:
        candidate = daily.CandidateAccumulator(
            ticker="NVDA",
            company_name="NVIDIA Corporation",
            market_cap_usd=Decimal("3000000000000"),
            avg_volume_30d=Decimal("40000000"),
            active_in_universe=True,
        )
        candidate.finnhub_article_uids.add("finnhub:10")
        candidate.gdelt_article_urls.add("https://example.com/gdelt")
        candidate.sec_accessions.add("0000000000-26-000001")
        candidate.sec_forms["8-K"] += 1
        candidate.keywords.update(["ai", "datacenter", "contract"])
        daily.add_topic_signal(candidate, "ai_infrastructure", Decimal("8"))

        scores = daily.rank_candidates(
            {"NVDA": candidate},
            run_date=daily.parse_run_date("2026-06-10"),
            top_n=5,
        )

        self.assertEqual(len(scores), 1)
        self.assertEqual(scores[0].ticker, "NVDA")
        self.assertEqual(scores[0].rank, 1)
        self.assertGreater(scores[0].score, Decimal("30"))
        self.assertEqual(scores[0].primary_topic_slug, "ai_infrastructure")

    def test_upsert_candidate_scores_deletes_stale_same_day_candidates(self) -> None:
        run_date = daily.parse_run_date("2026-07-02")

        def make_score(ticker: str, rank: int) -> daily.CandidateScore:
            return daily.CandidateScore(
                run_date=run_date,
                ticker=ticker,
                company_name=f"{ticker} Inc.",
                score=Decimal("50"),
                rank=rank,
                topic_slugs=("ai_infrastructure",),
                primary_topic_slug="ai_infrastructure",
                news_score=Decimal("20"),
                gdelt_score=Decimal("5"),
                sec_score=Decimal("0"),
                fundamental_score=Decimal("6"),
                liquidity_score=Decimal("10"),
                finnhub_article_count=1,
                gdelt_article_count=1,
                sec_filing_count=0,
                latest_news_at=datetime(2026, 7, 2, tzinfo=timezone.utc),
                latest_filing_date=None,
                action_bias="watch",
                rationale={},
            )

        conn = RecordingConnection()
        count = daily.upsert_candidate_scores(
            conn,  # type: ignore[arg-type]
            (make_score("AAPL", 1), make_score("MSFT", 2)),
            run_date=run_date,
        )

        self.assertEqual(count, 2)
        delete_sql, delete_params = conn.calls[0]
        self.assertIn("DELETE FROM daily_candidate_scores", delete_sql)
        self.assertEqual(delete_params, (run_date, ["AAPL", "MSFT"]))

        empty_conn = RecordingConnection()
        empty_count = daily.upsert_candidate_scores(
            empty_conn,  # type: ignore[arg-type]
            (),
            run_date=run_date,
        )

        self.assertEqual(empty_count, 0)
        empty_delete_sql, empty_delete_params = empty_conn.calls[0]
        self.assertIn("DELETE FROM daily_candidate_scores", empty_delete_sql)
        self.assertEqual(empty_delete_params, (run_date,))


if __name__ == "__main__":
    unittest.main()
