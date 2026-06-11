from __future__ import annotations

import unittest
from datetime import datetime, timezone

from usstock.discovery import topic_candidates
from usstock.trends.extract import (
    ExistingTopicSignature,
    NewsDocument,
    extract_news_topics,
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
