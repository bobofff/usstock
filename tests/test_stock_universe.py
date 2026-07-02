from __future__ import annotations

import tempfile
import unittest
from decimal import Decimal
from pathlib import Path
from unittest.mock import patch

from usstock.screening import universe


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


class FakeNasdaqClient:
    def __init__(self, rows: tuple[dict[str, object], ...]) -> None:
        self.rows = rows
        self.calls: list[tuple[tuple[str, ...], int | None]] = []

    def fetch_rows(
        self,
        *,
        exchanges: tuple[str, ...] = universe.DEFAULT_ALLOWED_EXCHANGES,
        limit: int | None = None,
    ) -> tuple[dict[str, object], ...]:
        self.calls.append((exchanges, limit))
        return self.rows[:limit]


class StockUniverseImportTest(unittest.TestCase):
    def test_filter_keeps_active_common_stock_adr_and_reit(self) -> None:
        records = (
            universe.parse_universe_record(
                {
                    "Symbol": "AAPL",
                    "Name": "Apple Inc. Common Stock",
                    "Exchange": "Nasdaq Global Select",
                    "Market Cap": "$3.1T",
                    "Avg Volume 30D": "55,000,000",
                    "Last Sale": "$190.50",
                    "Sector": "Technology",
                },
                row_number=2,
                data_source="unit_test",
                source_url=None,
            ),
            universe.parse_universe_record(
                {
                    "Symbol": "BILI",
                    "Name": "Bilibili Inc. American Depositary Shares",
                    "Exchange": "NASDAQ",
                    "Market Cap": "6.2B",
                    "Avg Volume 30D": "6500000",
                    "Last Sale": "15.00",
                },
                row_number=3,
                data_source="unit_test",
                source_url=None,
            ),
            universe.parse_universe_record(
                {
                    "Symbol": "O",
                    "Name": "Realty Income Corporation REIT",
                    "Exchange": "NYSE",
                    "Market Cap": "50B",
                    "Avg Volume 30D": "4000000",
                    "Last Sale": "55",
                },
                row_number=4,
                data_source="unit_test",
                source_url=None,
            ),
            universe.parse_universe_record(
                {
                    "Symbol": "UAL",
                    "Name": "United Airlines Holdings Inc. Common Stock",
                    "Exchange": "NASDAQ",
                    "Market Cap": "18B",
                    "Avg Volume 30D": "8500000",
                    "Last Sale": "57",
                },
                row_number=5,
                data_source="unit_test",
                source_url=None,
            ),
        )

        accepted, rejected = universe.filter_stock_universe_records(
            records,
            config=universe.StockUniverseFilterConfig(),
        )

        self.assertEqual([record.ticker for record in accepted], ["AAPL", "BILI", "O", "UAL"])
        self.assertEqual(rejected, ())
        self.assertEqual(accepted[1].asset_type, "adr")
        self.assertEqual(accepted[2].asset_type, "reit")

    def test_filter_rejects_etf_warrant_otc_low_liquidity_and_low_cap(self) -> None:
        rows = [
            {
                "Symbol": "SPY",
                "Name": "SPDR S&P 500 ETF Trust",
                "Exchange": "NYSE Arca",
                "Market Cap": "500B",
                "Avg Volume 30D": "80000000",
                "Last Sale": "500",
            },
            {
                "Symbol": "ABCDW",
                "Name": "ABCD Corp Warrants",
                "Exchange": "NASDAQ",
                "Market Cap": "500M",
                "Avg Volume 30D": "300000",
                "Last Sale": "2",
            },
            {
                "Symbol": "OTCM",
                "Name": "OTC Example Inc. Common Stock",
                "Exchange": "OTCQB",
                "Market Cap": "900M",
                "Avg Volume 30D": "500000",
                "Last Sale": "9",
            },
            {
                "Symbol": "THIN",
                "Name": "Thin Trading Inc. Common Stock",
                "Exchange": "NYSE",
                "Market Cap": "900M",
                "Avg Volume 30D": "99999",
                "Last Sale": "8",
            },
            {
                "Symbol": "TINY",
                "Name": "Tiny Cap Inc. Common Stock",
                "Exchange": "AMEX",
                "Market Cap": "99M",
                "Avg Volume 30D": "900000",
                "Last Sale": "3",
            },
            {
                "Symbol": "PENNY",
                "Name": "Penny Common Inc. Common Stock",
                "Exchange": "NASDAQ",
                "Market Cap": "500M",
                "Avg Volume 30D": "900000",
                "Last Sale": "0.75",
            },
        ]
        records = tuple(
            universe.parse_universe_record(
                row,
                row_number=index,
                data_source="unit_test",
                source_url=None,
            )
            for index, row in enumerate(rows, start=2)
        )

        accepted, rejected = universe.filter_stock_universe_records(
            records,
            config=universe.StockUniverseFilterConfig(),
        )

        self.assertEqual(accepted, ())
        reasons_by_ticker = {row.ticker: set(row.reasons) for row in rejected}
        self.assertIn("unsupported_asset_type:etf", reasons_by_ticker["SPY"])
        self.assertIn("excluded_security_type", reasons_by_ticker["ABCDW"])
        self.assertIn("unsupported_exchange:OTC", reasons_by_ticker["OTCM"])
        self.assertIn("avg_volume_30d_below_min", reasons_by_ticker["THIN"])
        self.assertIn("market_cap_usd_below_min", reasons_by_ticker["TINY"])
        self.assertIn("last_price_below_min", reasons_by_ticker["PENNY"])

    def test_import_dry_run_filters_without_opening_database(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            path = Path(tmp_dir) / "universe.csv"
            path.write_text(
                "\n".join(
                    [
                        "Symbol,Name,Exchange,Market Cap,Avg Volume 30D,Last Sale",
                        "AAPL,Apple Inc. Common Stock,NASDAQ,3T,50000000,190",
                        "ETF,Example ETF,NASDAQ,1B,1000000,20",
                    ]
                ),
                encoding="utf-8",
            )

            with patch.object(universe.psycopg, "connect") as connect:
                result = universe.import_stock_universe_file(
                    file_path=path,
                    dry_run=True,
                    data_source="unit_test",
                )

        connect.assert_not_called()
        self.assertTrue(result.dry_run)
        self.assertEqual(result.total_rows, 2)
        self.assertEqual(result.accepted_count, 1)
        self.assertEqual(result.rejected_count, 1)
        self.assertEqual(result.upserted_count, 0)
        self.assertEqual(result.accepted_tickers, ("AAPL",))

    def test_nasdaq_payload_rows_are_extracted_from_table_shape(self) -> None:
        payload = {
            "data": {
                "table": {
                    "rows": [
                        {"symbol": "AAPL", "name": "Apple Inc. Common Stock"},
                        ["bad row"],
                    ]
                }
            }
        }

        rows = universe.extract_nasdaq_rows(payload)

        self.assertEqual(rows, [{"symbol": "AAPL", "name": "Apple Inc. Common Stock"}])

    def test_nasdaq_sync_dry_run_uses_interface_rows_without_database(self) -> None:
        client = FakeNasdaqClient(
            (
                {
                    "symbol": "AAPL",
                    "name": "Apple Inc. Common Stock",
                    "lastsale": "$190.00",
                    "marketCap": "3000000000000",
                    "volume": "50000000",
                    "sector": "Technology",
                    "industry": "Consumer Electronics",
                    "country": "United States",
                    "_source_exchange": "NASDAQ",
                    "_source_url": "https://api.example.test/screener",
                },
                {
                    "symbol": "SPY",
                    "name": "SPDR S&P 500 ETF Trust",
                    "lastsale": "$500.00",
                    "marketCap": "500000000000",
                    "volume": "80000000",
                    "sector": "Finance",
                    "industry": "Exchange Traded Fund",
                    "country": "United States",
                    "_source_exchange": "NYSE",
                    "_source_url": "https://api.example.test/screener",
                },
            )
        )

        with patch.object(universe.psycopg, "connect") as connect:
            result = universe.sync_nasdaq_stock_universe(
                client=client,  # type: ignore[arg-type]
                dry_run=True,
                limit=2,
            )

        connect.assert_not_called()
        self.assertEqual(client.calls, [(("NASDAQ", "NYSE", "AMEX"), 2)])
        self.assertEqual(result.total_rows, 2)
        self.assertEqual(result.accepted_count, 1)
        self.assertEqual(result.rejected_count, 1)
        self.assertEqual(result.accepted_tickers, ("AAPL",))
        self.assertIn("unsupported_asset_type:etf", result.rejection_reason_counts)

    def test_upsert_preserves_manual_watchlist_and_never_deletes(self) -> None:
        record = universe.StockUniverseRecord(
            ticker="AAPL",
            company_name="Apple Inc.",
            exchange="NASDAQ",
            sector="Technology",
            industry="Consumer Electronics",
            asset_type="equity",
            market_cap_usd=Decimal("3000000000000"),
            avg_volume_30d=Decimal("50000000"),
            last_price=Decimal("190"),
            data_source="unit_test",
            metadata={"filter_version": "test"},
        )
        conn = RecordingConnection()

        count = universe.upsert_stock_universe_records(
            conn,  # type: ignore[arg-type]
            (record,),
        )

        self.assertEqual(count, 1)
        sql = conn.calls[0][0]
        self.assertNotIn("DELETE FROM stock_universe", sql)
        self.assertIn("ON CONFLICT (ticker)", sql)
        self.assertIn("is_manual_watchlist = stock_universe.is_manual_watchlist", sql)
        self.assertIn("is_active = TRUE", sql)


if __name__ == "__main__":
    unittest.main()
