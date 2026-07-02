from __future__ import annotations

from datetime import date
from http.client import HTTPConnection
import threading
import urllib.parse
import unittest

from usstock.admin.app import (
    ADMIN_ACCESS_CONTEXT,
    ADMIN_ACTION_COOKIE_NAME,
    AdminAccess,
    AdminHTTPServer,
    AdminRequestHandler,
    admin_token_matches,
    layout,
    market_sync_notice_is_ok,
    render_backtest_forms,
    render_discovery_action_row,
    render_articles_table,
    render_finnhub_articles_table,
    render_markdown_preview_html,
    render_selected_report,
    render_weather,
    render_topic_candidate_actions,
    table,
)
from usstock.data import market


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

        self.assertIn("/actions/market-sync-yfinance", html)
        self.assertIn("自动同步日线价格", html)
        self.assertIn("从日报候选自动提取 ticker", html)
        self.assertIn("value=\"yfinance\"", html)

    def test_table_includes_page_size_selector(self) -> None:
        html = table(
            "<tr><th>ticker</th></tr>",
            "<tr><td>AAPL</td></tr>",
            page_size=50,
        )

        self.assertIn('data-page-size="50"', html)
        self.assertIn('data-page-size-select', html)
        self.assertIn('<option value="10">10</option>', html)
        self.assertIn('<option value="20">20</option>', html)
        self.assertIn('<option value="50" selected>50</option>', html)
        self.assertIn('<option value="100">100</option>', html)

    def test_gdelt_articles_table_renders_topic_badges(self) -> None:
        html = render_articles_table(
            [
                {
                    "title": "AI infrastructure demand rises",
                    "domain": "example.com",
                    "language": "English",
                    "source_country": "US",
                    "seen_at": None,
                    "article_url": "https://example.com/news",
                    "query_text": "ai",
                    "tone": None,
                    "topic_slugs": ["ai_infrastructure", "semiconductors"],
                }
            ]
        )

        self.assertIn("<th>主题</th>", html)
        self.assertIn("ai_infrastructure", html)
        self.assertIn("semiconductors", html)

    def test_finnhub_articles_table_renders_topic_badges(self) -> None:
        html = render_finnhub_articles_table(
            [
                {
                    "headline": "Chipmaker expands AI datacenter supply",
                    "source_name": "Example News",
                    "category": "general",
                    "related_tickers": ["NVDA"],
                    "published_at": None,
                    "article_url": "https://example.com/finnhub",
                    "endpoint": "market_news",
                    "topic_slugs": ["ai_infrastructure"],
                }
            ]
        )

        self.assertIn("<th>主题</th>", html)
        self.assertIn("ai_infrastructure", html)

    def test_admin_token_matching_requires_exact_secret(self) -> None:
        self.assertTrue(admin_token_matches("secret-token", "secret-token"))
        self.assertFalse(admin_token_matches("secret-token", "wrong-token"))
        self.assertFalse(admin_token_matches("secret-token", ""))
        self.assertFalse(admin_token_matches("", "secret-token"))

    def test_layout_marks_readonly_admin_mode(self) -> None:
        token = ADMIN_ACCESS_CONTEXT.set(AdminAccess(protected=True, allowed=False))
        try:
            html = layout(
                "测试",
                '<form method="post" action="/actions/report-daily"><button>生成</button></form>',
                active="/reports",
                query={},
            )
        finally:
            ADMIN_ACCESS_CONTEXT.reset(token)

        self.assertIn("app-shell admin-readonly", html)
        self.assertIn("只读浏览", html)
        self.assertIn("window.usstockAdminAccess", html)
        self.assertIn("天气交易", html)

    def test_render_weather_panel_has_signal_and_sizing_forms(self) -> None:
        html = render_weather(database_url=None, query={})

        self.assertIn("Polymarket 天气交易", html)
        self.assertIn('action="/weather"', html)
        self.assertIn('name="mode" value="signal"', html)
        self.assertIn('name="mode" value="size"', html)
        self.assertIn("手动仓位计算", html)

    def test_layout_propagates_url_admin_token_to_posts_and_links(self) -> None:
        token = ADMIN_ACCESS_CONTEXT.set(AdminAccess(protected=True, allowed=True))
        try:
            html = layout(
                "测试",
                '<form method="post" action="/actions/report-daily"><button>生成</button></form>',
                active="/reports",
                query={"admin_token": ["secret-token"]},
            )
        finally:
            ADMIN_ACCESS_CONTEXT.reset(token)

        self.assertIn('tokenName: "admin_token"', html)
        self.assertIn("new URLSearchParams(window.location.search)", html)
        self.assertIn("input.type = \"hidden\"", html)
        self.assertIn("link.href = url.pathname + url.search + url.hash", html)

    def test_readonly_mode_disables_dynamic_actions(self) -> None:
        token = ADMIN_ACCESS_CONTEXT.set(AdminAccess(protected=True, allowed=False))
        try:
            action_row = render_discovery_action_row({"status": "idle"})
            candidate_actions = render_topic_candidate_actions(
                {"status": "pending", "candidate_slug": "ai_power"}
            )
        finally:
            ADMIN_ACCESS_CONTEXT.reset(token)

        self.assertIn('formaction="/actions/discovery-daily"', action_row)
        self.assertIn("disabled", action_row)
        self.assertEqual('<span class="subtle">只读</span>', candidate_actions)

    def test_market_sync_notice_is_error_when_every_ticker_is_missing(self) -> None:
        result = market.MarketPriceSyncResult(
            provider=market.STOOQ_DATA_SOURCE,
            requested_tickers=("NVDA", "MSFT"),
            synced_tickers=(),
            missing_tickers=("NVDA", "MSFT"),
            price_count=0,
            from_date=None,
            to_date=None,
        )

        self.assertFalse(market_sync_notice_is_ok(result))

    def test_admin_http_token_cookie_and_readonly_post_gate(self) -> None:
        admin_token = "secret token+value"
        try:
            server = AdminHTTPServer(
                ("127.0.0.1", 0),
                AdminRequestHandler,
                database_url="postgresql://example/usstock",
                admin_action_token=admin_token,
            )
        except PermissionError as exc:
            self.skipTest(f"当前沙箱不允许绑定本地测试端口: {exc}")
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        host, port = server.server_address

        try:
            conn = HTTPConnection(host, port, timeout=2)
            encoded_token = urllib.parse.quote(admin_token, safe="")
            conn.request(
                "GET",
                f"/partials/discovery-progress?admin_token={encoded_token}",
            )
            response = conn.getresponse()
            response.read()
            cookie = response.getheader("Set-Cookie") or ""
            self.assertEqual(200, response.status)
            self.assertIsNone(response.getheader("Location"))
            self.assertIn(ADMIN_ACTION_COOKIE_NAME, cookie)
            self.assertIn(encoded_token, cookie)
            self.assertIn("HttpOnly", cookie)

            conn = HTTPConnection(host, port, timeout=2)
            conn.request(
                "POST",
                "/actions/report-daily",
                body="top_n=1",
                headers={"Content-Type": "application/x-www-form-urlencoded"},
            )
            response = conn.getresponse()
            response.read()
            self.assertEqual(303, response.status)
            location = response.getheader("Location") or ""
            notice = urllib.parse.parse_qs(
                urllib.parse.urlparse(location).query
            ).get("notice", [""])[0]
            self.assertIn("只读浏览", notice)

            conn = HTTPConnection(host, port, timeout=2)
            conn.request(
                "POST",
                "/actions/unknown",
                body="",
                headers={"Cookie": cookie.split(";", 1)[0]},
            )
            response = conn.getresponse()
            response.read()
            self.assertEqual(303, response.status)
            location = response.getheader("Location") or ""
            notice = urllib.parse.parse_qs(
                urllib.parse.urlparse(location).query
            ).get("notice", [""])[0]
            self.assertEqual("未知操作。", notice)
        finally:
            server.shutdown()
            server.server_close()
            thread.join(timeout=2)


if __name__ == "__main__":
    unittest.main()
