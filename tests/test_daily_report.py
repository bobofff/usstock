from __future__ import annotations

import unittest
from datetime import datetime, timezone
from decimal import Decimal

from usstock.reports import daily_report


class DailyReportTest(unittest.TestCase):
    def test_build_candidate_analysis_from_score_payload(self) -> None:
        candidate = {
            "rank": 1,
            "ticker": "NVDA",
            "company_name": "NVIDIA Corporation",
            "score": "72.5000",
            "action_bias": "review",
            "topics": ["ai_infrastructure"],
            "primary_topic": "ai_infrastructure",
            "scores": {
                "news": "24",
                "gdelt": "5",
                "sec": "0",
                "fundamental": "10",
                "liquidity": "15",
            },
            "counts": {
                "finnhub_articles": 3,
                "gdelt_articles": 7,
                "sec_filings": 0,
            },
            "latest_news_at": "2026-06-18T10:00:00+00:00",
            "latest_filing_date": None,
            "rationale": {
                "recent_titles": [
                    "Nvidia supplier wins AI datacenter contract",
                ],
            },
        }
        evidence = (
            daily_report.SourceEvidence(
                source_type="finnhub_article",
                title="Nvidia supplier wins AI datacenter contract",
                url="https://example.com/nvda",
                published_at=datetime(2026, 6, 18, 10, tzinfo=timezone.utc),
                relevance_score=Decimal("9"),
                topic_slug="ai_infrastructure",
                ticker="NVDA",
            ),
        )

        analysis = daily_report.build_candidate_analysis(
            candidate,
            topic_names={"ai_infrastructure": "AI infrastructure"},
            evidence=evidence,
        )

        self.assertEqual(analysis.ticker, "NVDA")
        self.assertEqual(analysis.attention_label, "优先复核")
        self.assertEqual(analysis.event_type, "订单和合作")
        self.assertEqual(analysis.sentiment, "偏利好")
        self.assertEqual(analysis.risk_level, "中低")
        self.assertIn("Finnhub 相关新闻 3 篇", analysis.relation_reason)

    def test_render_markdown_includes_candidates_sources_and_disclaimer(self) -> None:
        candidate_payload = {
            "rank": 1,
            "ticker": "AAPL",
            "company_name": "Apple Inc.",
            "score": "45",
            "action_bias": "watch",
            "topics": ["ai_infrastructure"],
            "primary_topic": "ai_infrastructure",
            "scores": {
                "news": "12",
                "gdelt": "4",
                "sec": "0",
                "fundamental": "0",
                "liquidity": "15",
            },
            "counts": {
                "finnhub_articles": 1,
                "gdelt_articles": 5,
                "sec_filings": 0,
            },
            "rationale": {
                "recent_titles": ["Apple expands generative AI features"],
            },
        }
        report = daily_report.build_base_report(
            run_date=daily_report.parse_run_date("2026-06-18"),
            profile="default",
            source_watchlist_uid="default:2026-06-18",
            candidate_payloads=[candidate_payload],
            topic_names={"ai_infrastructure": "AI infrastructure"},
            evidence_by_ticker={
                "AAPL": (
                    daily_report.SourceEvidence(
                        source_type="finnhub_article",
                        title="Apple expands generative AI features",
                        url="https://example.com/aapl",
                        published_at=datetime(2026, 6, 18, 8, tzinfo=timezone.utc),
                        relevance_score=Decimal("8"),
                        topic_slug="ai_infrastructure",
                        ticker="AAPL",
                    ),
                )
            },
        )

        self.assertIn("不构成投资建议", report.markdown_body)
        self.assertIn("AAPL", report.markdown_body)
        self.assertIn("https://example.com/aapl", report.markdown_body)
        self.assertIn("新闻驱动但基本面证据不足", "\n".join(report.risk_overview))

    def test_parse_llm_json_allows_markdown_code_fence(self) -> None:
        payload = daily_report.parse_llm_json(
            """```json
            {"executive_summary": "摘要", "risk_overview": ["风险"]}
            ```"""
        )

        self.assertEqual(payload["executive_summary"], "摘要")
        self.assertEqual(payload["risk_overview"], ["风险"])


if __name__ == "__main__":
    unittest.main()
