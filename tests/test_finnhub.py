from __future__ import annotations

import unittest
from datetime import timezone

from usstock.data import finnhub


class FinnhubParsingTest(unittest.TestCase):
    def test_parse_article_payload_normalizes_fields(self) -> None:
        article = finnhub.parse_article_payload(
            {
                "id": 123,
                "headline": "Apple supplier shares rise",
                "summary": "Short summary",
                "category": "company",
                "source": "Reuters",
                "url": "https://example.com/news/123",
                "image": "https://example.com/news/123.jpg",
                "datetime": 1_717_200_000,
                "related": "AAPL, MSFT, AAPL",
            },
            query_uid="query-1",
            endpoint=finnhub.COMPANY_NEWS_ENDPOINT,
            request_url="https://finnhub.io/api/v1/company-news?symbol=AAPL",
        )

        self.assertIsNotNone(article)
        assert article is not None
        self.assertEqual(article.article_uid, "finnhub:123")
        self.assertEqual(article.finnhub_id, 123)
        self.assertEqual(article.headline, "Apple supplier shares rise")
        self.assertEqual(article.related_tickers, ["AAPL", "MSFT"])
        self.assertEqual(article.source_type, "financial_news")
        self.assertEqual(article.published_at.tzinfo, timezone.utc)

    def test_client_can_build_sanitized_request_url(self) -> None:
        client = finnhub.FinnhubClient(
            api_key="secret-token",
            base_url="https://example.test/api/v1",
            requests_per_second=1000,
        )

        request_url = client.build_url(
            "news",
            {"category": "general"},
            include_token=False,
        )
        fetch_url = client.build_url(
            "news",
            {"category": "general"},
            include_token=True,
        )

        self.assertNotIn("secret-token", request_url)
        self.assertEqual(request_url, "https://example.test/api/v1/news?category=general")
        self.assertIn("token=secret-token", fetch_url)


if __name__ == "__main__":
    unittest.main()
