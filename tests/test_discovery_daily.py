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
            patch.object(daily, "fetch_recent_reddit_posts", return_value=[]),
            patch.object(daily, "fetch_recent_sec_filings", return_value=[]),
            patch.object(daily, "upsert_daily_watchlist"),
        ):
            result = daily.run_daily_discovery(
                database_url=database_url,
                skip_finnhub_sync=True,
                skip_gdelt_sync=True,
                skip_reddit_sync=True,
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

    def test_reddit_mentions_boost_existing_candidates_but_do_not_rank_alone(self) -> None:
        topic = daily.DEFAULT_MARKET_TOPICS[0]
        universe = {
            "NVDA": {
                "ticker": "NVDA",
                "company_name": "NVIDIA Corporation",
                "sector": "Technology",
                "industry": "Semiconductors",
                "business_description": "GPU and AI accelerator company",
                "market_cap_usd": Decimal("3000000000000"),
                "avg_volume_30d": Decimal("40000000"),
                "is_active": True,
            }
        }
        candidates: dict[str, daily.CandidateAccumulator] = {}

        mentions = daily.build_reddit_mentions(
            posts=[
                {
                    "post_uid": "reddit:t3_abc123",
                    "subreddit": "wallstreetbets",
                    "title": "$NVDA AI datacenter demand is everywhere",
                    "selftext": "Discussion about GPU supply.",
                    "permalink_url": "https://www.reddit.com/r/wallstreetbets/comments/abc123/example/",
                    "score": 500,
                    "comment_count": 120,
                    "candidate_tickers": ["NVDA"],
                    "candidate_keywords": ["datacenter", "gpu"],
                    "created_utc": datetime(2026, 6, 10, tzinfo=timezone.utc),
                }
            ],
            topics=[topic],
            universe=universe,
            candidates=candidates,
        )

        self.assertEqual(len(mentions), 1)
        self.assertEqual(mentions[0].source_type, "reddit_post")
        self.assertEqual(candidates["NVDA"].reddit_post_uids, {"reddit:t3_abc123"})

        scores = daily.rank_candidates(
            candidates,
            run_date=daily.parse_run_date("2026-06-10"),
            top_n=5,
        )
        self.assertEqual(scores, ())

        candidates["NVDA"].finnhub_article_uids.add("finnhub:1")
        scores = daily.rank_candidates(
            candidates,
            run_date=daily.parse_run_date("2026-06-10"),
            top_n=5,
        )
        self.assertEqual(scores[0].reddit_post_count, 1)
        self.assertGreater(scores[0].reddit_score, Decimal("0"))


if __name__ == "__main__":
    unittest.main()
