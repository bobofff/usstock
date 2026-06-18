from __future__ import annotations

import unittest
from datetime import datetime, timezone

from usstock.discovery import topic_candidates
from usstock.trends.extract import (
    ExistingTopicSignature,
    NewsDocument,
    extract_news_topics,
    prepare_news_documents,
)


class TopicExtractionTest(unittest.TestCase):
    def test_extract_news_topics_builds_novel_candidate(self) -> None:
        documents = [
            NewsDocument(
                source_type="finnhub",
                source_uid="finnhub:1",
                title="Quantum computing stocks rally after chip breakthrough",
                body="The breakthrough expands cloud security and enterprise use cases.",
                source_name="Example News",
                tickers=("IONQ", "RGTI"),
                published_at=datetime(2026, 6, 10, tzinfo=timezone.utc),
            ),
            NewsDocument(
                source_type="gdelt",
                source_uid="https://example.com/quantum-contract",
                title="Quantum computing startup wins cloud contract with major bank",
                source_name="example.com",
                published_at=datetime(2026, 6, 11, tzinfo=timezone.utc),
            ),
        ]

        candidates = extract_news_topics(
            documents,
            max_candidates=5,
            min_articles=2,
            min_score="1",
        )

        self.assertTrue(candidates)
        top = candidates[0]
        self.assertEqual(top.candidate_slug, "quantum_computing")
        self.assertEqual(top.article_count, 2)
        self.assertIn("quantum computing", top.keywords)
        self.assertIn('"quantum computing"', top.gdelt_query)
        self.assertEqual(top.ticker_hints[:2], ("IONQ", "RGTI"))

    def test_extract_news_topics_filters_existing_topic_matches(self) -> None:
        documents = [
            NewsDocument(
                source_type="finnhub",
                source_uid="finnhub:1",
                title="Quantum computing stocks rally after chip breakthrough",
            ),
            NewsDocument(
                source_type="gdelt",
                source_uid="https://example.com/quantum-contract",
                title="Quantum computing startup wins cloud contract",
            ),
        ]
        existing = [
            ExistingTopicSignature(
                topic_slug="quantum_computing",
                terms=frozenset({"quantum", "computing", "quantum computing"}),
            )
        ]

        candidates = extract_news_topics(
            documents,
            existing_topics=existing,
            max_candidates=5,
            min_articles=2,
            min_score="1",
        )

        self.assertEqual(candidates, [])

    def test_extract_news_topics_filters_generic_single_word_noise(self) -> None:
        documents = [
            NewsDocument(
                source_type="finnhub",
                source_uid="article:1",
                title="Why investors expect ACME to acquire software company",
            ),
            NewsDocument(
                source_type="gdelt",
                source_uid="article:2",
                title="Why buyers expect BRAVO to acquire cloud vendor",
            ),
            NewsDocument(
                source_type="finnhub",
                source_uid="article:3",
                title="Form 4 filed by insider",
            ),
            NewsDocument(
                source_type="gdelt",
                source_uid="article:4",
                title="Form 4 amendment filed by insider",
            ),
        ]

        candidates = extract_news_topics(
            documents,
            max_candidates=5,
            min_articles=2,
            min_score="1",
        )

        self.assertEqual(candidates, [])

    def test_prepare_news_documents_cleans_filters_and_deduplicates(self) -> None:
        documents = [
            NewsDocument(
                source_type="finnhub",
                source_uid="article:1",
                title="<b>AI chip orders surge after cloud contract</b>",
                body="Advertisement\nSubscribe for alerts\nSuppliers expand capacity.",
                url="https://example.com/news?utm_source=email&id=10",
            ),
            NewsDocument(
                source_type="gdelt",
                source_uid="article:2",
                title="AI chip orders surge after cloud contract",
                body="Suppliers expand capacity.",
                url="https://example.com/news?id=10&utm_campaign=test",
            ),
            NewsDocument(
                source_type="gdelt",
                source_uid="article:3",
                title="Advertisement",
                body="Click here to subscribe.",
                url="https://example.com/ad",
            ),
        ]

        prepared = prepare_news_documents(documents)

        self.assertEqual(len(prepared), 1)
        self.assertEqual(prepared[0].title, "AI chip orders surge after cloud contract")
        self.assertNotIn("Advertisement", prepared[0].body)
        self.assertEqual(prepared[0].url, "https://example.com/news?id=10")

    def test_extract_news_topics_extracts_explicit_ticker_mentions(self) -> None:
        documents = [
            NewsDocument(
                source_type="finnhub",
                source_uid="article:1",
                title="AI server demand lifts $NVDA and NASDAQ:MSFT suppliers",
                body="Cloud buyers keep ordering accelerators.",
            )
        ]

        candidates = extract_news_topics(
            documents,
            max_candidates=3,
            min_articles=1,
            min_score="1",
            known_tickers=("NVDA", "MSFT"),
        )

        self.assertTrue(candidates)
        self.assertIn("NVDA", candidates[0].ticker_hints)
        self.assertIn("MSFT", candidates[0].ticker_hints)

    def test_extract_news_topics_records_keyword_growth_metadata(self) -> None:
        documents = [
            NewsDocument(
                source_type="gdelt",
                source_uid="article:1",
                title="Grid storage contracts expand in Texas",
                published_at=datetime(2026, 6, 9, tzinfo=timezone.utc),
            ),
            NewsDocument(
                source_type="gdelt",
                source_uid="article:2",
                title="Grid storage contracts accelerate in California",
                published_at=datetime(2026, 6, 11, tzinfo=timezone.utc),
            ),
            NewsDocument(
                source_type="finnhub",
                source_uid="article:3",
                title="Grid storage contracts lift battery suppliers",
                published_at=datetime(2026, 6, 11, 1, tzinfo=timezone.utc),
            ),
        ]

        candidates = extract_news_topics(
            documents,
            max_candidates=3,
            min_articles=3,
            min_score="1",
            growth_window_hours=24,
        )

        self.assertTrue(candidates)
        growth = candidates[0].metadata["keyword_growth"]["grid storage contracts"]
        self.assertEqual(growth["current"], 2)
        self.assertEqual(growth["previous"], 1)
        self.assertEqual(growth["growth_rate"], "1.0000")

    def test_build_news_documents_normalizes_sources(self) -> None:
        documents = topic_candidates.build_news_documents(
            finnhub_articles=[
                {
                    "article_uid": "finnhub:10",
                    "article_url": "https://example.com/news",
                    "headline": "Battery storage demand jumps",
                    "summary": "Utilities expand grid-scale storage projects.",
                    "source_name": "Example News",
                    "related_tickers": ["FLNC", "STEM"],
                    "published_at": datetime(2026, 6, 11, tzinfo=timezone.utc),
                }
            ],
            gdelt_articles=[
                {
                    "article_url": "https://example.com/global",
                    "title": "Grid storage contracts accelerate",
                    "domain": "example.com",
                    "seen_at": datetime(2026, 6, 11, tzinfo=timezone.utc),
                }
            ],
        )

        self.assertEqual(len(documents), 2)
        self.assertEqual(documents[0].source_type, "finnhub")
        self.assertEqual(documents[0].tickers, ("FLNC", "STEM"))
        self.assertEqual(documents[1].source_type, "gdelt")


if __name__ == "__main__":
    unittest.main()
