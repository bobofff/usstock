from __future__ import annotations

from datetime import date
import unittest

from usstock.admin.app import (
    render_backtest_forms,
    render_markdown_preview_html,
    render_selected_report,
)


class AdminReportPreviewTest(unittest.TestCase):
    def test_render_selected_report_renders_markdown_as_html(self) -> None:
        html = render_selected_report(
            {
                "report_uid": "report-1",
                "markdown_body": "# 标题\n\n- profile: `default`",
                "structured_payload": {"candidates": [{"ticker": "AAPL"}]},
            }
        )

        self.assertNotIn("<pre class=\"report-preview\">", html)
        self.assertIn("<h3>标题</h3>", html)
        self.assertIn("<code>default</code>", html)
        self.assertIn("候选：AAPL", html)

    def test_render_markdown_preview_supports_tables_and_links(self) -> None:
        html = render_markdown_preview_html(
            """
# 美股新闻驱动分析报告

| Ticker | 分数 |
| --- | ---: |
| AAPL | 45 |

- 来源证据：
  - [Apple expands AI](https://example.com/aapl) · finnhub
""".strip()
        )

        self.assertIn('<table class="report-markdown-table">', html)
        self.assertIn('<td class="align-right">45</td>', html)
        self.assertIn(
            '<a href="https://example.com/aapl" target="_blank" rel="noreferrer">',
            html,
        )
        self.assertIn('class="depth-1"', html)
        self.assertNotIn("| Ticker |", html)

    def test_render_markdown_preview_escapes_html_and_unsafe_links(self) -> None:
        html = render_markdown_preview_html(
            "# <script>alert(1)</script>\n\n[bad](javascript:alert(1))"
        )

        self.assertIn("&lt;script&gt;alert(1)&lt;/script&gt;", html)
        self.assertIn(">bad<", html)
        self.assertNotIn("<script>", html)
        self.assertNotIn("javascript:", html)

    def test_render_backtest_forms_includes_automatic_price_sync(self) -> None:
        html = render_backtest_forms(
            today=date(2026, 6, 22),
            from_date=date(2026, 5, 23),
            return_path="/backtest",
        )

        self.assertIn("/actions/market-sync-stooq", html)
        self.assertIn("自动同步日线价格", html)
        self.assertIn("从日报候选自动提取 ticker", html)
        self.assertIn("value=\"stooq\"", html)


if __name__ == "__main__":
    unittest.main()
