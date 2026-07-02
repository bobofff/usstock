from __future__ import annotations

import tempfile
import unittest
import urllib.error
from datetime import date
from decimal import Decimal
from pathlib import Path
from unittest.mock import patch

import pandas as pd

from usstock.data import market


class _FakeUrlResponse:
    def __init__(self, text: str, *, content_type: str = "text/csv") -> None:
        self.text = text
        self.headers = {"Content-Type": content_type}

    def __enter__(self) -> "_FakeUrlResponse":
        return self

    def __exit__(self, exc_type: object, exc: object, traceback: object) -> None:
        return None

    def getcode(self) -> int:
        return 200

    def read(self) -> bytes:
        return self.text.encode("utf-8")


class _FakeConnection:
    def __enter__(self) -> "_FakeConnection":
        return self

    def __exit__(self, exc_type: object, exc: object, traceback: object) -> None:
        return None

    def transaction(self) -> "_FakeConnection":
        return self


class _FakeStooqClient:
    def __init__(self) -> None:
        self.calls: list[str] = []

    def daily_prices(
        self,
        *,
        ticker: str,
        from_date: date | None = None,
        to_date: date | None = None,
    ) -> list[market.DailyPrice]:
        self.calls.append(ticker)
        if ticker == "AMZN":
            raise market.MarketDataError("temporary tls failure")
        return [
            market.DailyPrice(
                ticker=ticker,
                price_date=date(2026, 6, 2),
                close_price=Decimal("100"),
                data_source=market.STOOQ_DATA_SOURCE,
            )
        ]


class _FakeYFinanceClient:
    def __init__(self) -> None:
        self.calls: list[str] = []

    def daily_prices(
        self,
        *,
        ticker: str,
        from_date: date | None = None,
        to_date: date | None = None,
    ) -> list[market.DailyPrice]:
        self.calls.append(ticker)
        if ticker == "AMZN":
            raise market.MarketDataError("temporary yahoo limit")
        return [
            market.DailyPrice(
                ticker=ticker,
                price_date=date(2026, 6, 2),
                close_price=Decimal("100"),
                data_source=market.YFINANCE_DATA_SOURCE,
            )
        ]


class MarketPriceImportTest(unittest.TestCase):
    def test_load_csv_prices_accepts_common_header_names(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            path = Path(tmp_dir) / "prices.csv"
            path.write_text(
                "\n".join(
                    [
                        "Symbol,Date,Open,High,Low,Close,Adj Close,Volume",
                        "AAPL,2026-06-02,100,106,99,105,104.5,123456",
                    ]
                ),
                encoding="utf-8",
            )

            prices = market.load_csv_prices(path)

        self.assertEqual(len(prices), 1)
        self.assertEqual(prices[0].ticker, "AAPL")
        self.assertEqual(prices[0].close_price, Decimal("105"))
        self.assertEqual(prices[0].adjusted_close_price, Decimal("104.5"))
        self.assertEqual(prices[0].volume, Decimal("123456"))
        self.assertEqual(prices[0].data_source, "manual_csv")

    def test_load_csv_prices_can_use_default_ticker(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            path = Path(tmp_dir) / "nvda.csv"
            path.write_text("Date,Close\n2026-06-02,150\n", encoding="utf-8")

            prices = market.load_csv_prices(path, default_ticker="nvda")

        self.assertEqual(prices[0].ticker, "NVDA")
        self.assertEqual(prices[0].close_price, Decimal("150"))

    def test_load_csv_prices_can_use_adjusted_close_when_close_is_missing(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            path = Path(tmp_dir) / "msft.csv"
            path.write_text("Date,Adj Close\n2026-06-02,210.25\n", encoding="utf-8")

            prices = market.load_csv_prices(path, default_ticker="MSFT")

        self.assertEqual(prices[0].close_price, Decimal("210.25"))
        self.assertEqual(prices[0].adjusted_close_price, Decimal("210.25"))

    def test_stooq_symbol_for_ticker_adds_us_suffix_and_handles_dot_class(self) -> None:
        self.assertEqual(market.stooq_symbol_for_ticker("AAPL"), "aapl.us")
        self.assertEqual(market.stooq_symbol_for_ticker("BRK.B"), "brk-b.us")

    def test_parse_stooq_csv_prices_maps_ohlcv_rows(self) -> None:
        prices = market.parse_stooq_csv_prices(
            "\n".join(
                [
                    "Date,Open,High,Low,Close,Volume",
                    "2026-06-02,100,106,99,105,123456",
                    "2026-06-03,105,108,102,107,223456",
                ]
            ),
            ticker="AAPL",
            symbol="aapl.us",
            request_url="https://stooq.com/q/d/l/?s=aapl.us&i=d",
        )

        self.assertEqual(len(prices), 2)
        self.assertEqual(prices[0].ticker, "AAPL")
        self.assertEqual(prices[0].close_price, Decimal("105"))
        self.assertEqual(prices[0].volume, Decimal("123456"))
        self.assertEqual(prices[0].data_source, market.STOOQ_DATA_SOURCE)
        self.assertEqual(prices[0].source_uid, "stooq:aapl.us:2026-06-02")

    def test_parse_ticker_list_accepts_commas_and_newlines(self) -> None:
        self.assertEqual(
            market.parse_ticker_list("aapl, NVDA\nmsft,aapl"),
            ("AAPL", "NVDA", "MSFT"),
        )

    def test_stooq_client_retries_transient_url_errors(self) -> None:
        response = _FakeUrlResponse("Date,Open,High,Low,Close,Volume\n2026-06-02,1,2,1,2,100\n")
        client = market.StooqClient(
            requests_per_second=1000,
            retry_attempts=2,
            retry_backoff_seconds=0,
        )

        with patch.object(
            market.urllib.request,
            "urlopen",
            side_effect=[urllib.error.URLError("temporary eof"), response],
        ) as urlopen:
            prices = client.daily_prices(ticker="AAPL")

        self.assertEqual(urlopen.call_count, 2)
        self.assertEqual(len(prices), 1)
        self.assertEqual(prices[0].ticker, "AAPL")

    def test_stooq_client_rejects_browser_verification_html(self) -> None:
        response = _FakeUrlResponse(
            """
            <!DOCTYPE html><html><body>
            <noscript>This site requires JavaScript to verify your browser.</noscript>
            </body></html>
            """,
            content_type="text/html",
        )
        client = market.StooqClient(
            requests_per_second=1000,
            retry_attempts=1,
            retry_backoff_seconds=0,
        )

        with (
            patch.object(market.urllib.request, "urlopen", return_value=response),
            self.assertRaises(market.MarketDataError) as raised,
        ):
            client.daily_prices(ticker="AAPL")

        self.assertIn("浏览器 JavaScript 验证页", str(raised.exception))

    def test_parse_yfinance_prices_maps_multiindex_ohlcv_rows(self) -> None:
        frame = pd.DataFrame(
            {
                ("Open", "NVDA"): [Decimal("215.73")],
                ("High", "NVDA"): [Decimal("224.87")],
                ("Low", "NVDA"): [Decimal("215.70")],
                ("Close", "NVDA"): [Decimal("224.36")],
                ("Adj Close", "NVDA"): [Decimal("224.09")],
                ("Volume", "NVDA"): [212850700],
            },
            index=pd.to_datetime(["2026-06-01"]),
        )

        prices = market.parse_yfinance_prices(
            frame,
            ticker="NVDA",
            request_params={"ticker": "NVDA", "start": "2026-06-01"},
        )

        self.assertEqual(len(prices), 1)
        self.assertEqual(prices[0].ticker, "NVDA")
        self.assertEqual(prices[0].price_date, date(2026, 6, 1))
        self.assertEqual(prices[0].close_price, Decimal("224.36"))
        self.assertEqual(prices[0].adjusted_close_price, Decimal("224.09"))
        self.assertEqual(prices[0].volume, Decimal("212850700"))
        self.assertEqual(prices[0].data_source, market.YFINANCE_DATA_SOURCE)
        self.assertEqual(prices[0].source_uid, "yfinance:NVDA:2026-06-01")

    def test_sync_stooq_daily_prices_continues_after_ticker_failure(self) -> None:
        client = _FakeStooqClient()
        tickers = ("AAPL", "AMZN", "MSFT")

        with (
            patch.object(market, "get_database_url", return_value="postgresql://local/test"),
            patch.object(market, "ensure_market_schema", return_value=0),
            patch.object(market.psycopg, "connect", return_value=_FakeConnection()),
            patch.object(market, "resolve_stooq_sync_tickers", return_value=tickers),
            patch.object(market, "upsert_daily_prices", side_effect=lambda conn, prices: len(prices)),
        ):
            result = market.sync_stooq_daily_prices(tickers=tickers, client=client)

        self.assertEqual(client.calls, ["AAPL", "AMZN", "MSFT"])
        self.assertEqual(result.synced_tickers, ("AAPL", "MSFT"))
        self.assertEqual(result.failed_tickers, ("AMZN",))
        self.assertEqual(result.price_count, 2)

    def test_sync_yfinance_daily_prices_continues_after_ticker_failure(self) -> None:
        client = _FakeYFinanceClient()
        tickers = ("AAPL", "AMZN", "MSFT")

        with (
            patch.object(market, "get_database_url", return_value="postgresql://local/test"),
            patch.object(market, "ensure_market_schema", return_value=0),
            patch.object(market.psycopg, "connect", return_value=_FakeConnection()),
            patch.object(market, "resolve_stooq_sync_tickers", return_value=tickers),
            patch.object(market, "upsert_daily_prices", side_effect=lambda conn, prices: len(prices)),
        ):
            result = market.sync_yfinance_daily_prices(tickers=tickers, client=client)

        self.assertEqual(client.calls, ["AAPL", "AMZN", "MSFT"])
        self.assertEqual(result.provider, market.YFINANCE_DATA_SOURCE)
        self.assertEqual(result.synced_tickers, ("AAPL", "MSFT"))
        self.assertEqual(result.failed_tickers, ("AMZN",))
        self.assertEqual(result.price_count, 2)


if __name__ == "__main__":
    unittest.main()
