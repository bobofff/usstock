import json
import tempfile
import unittest
from datetime import datetime
from pathlib import Path
from unittest.mock import patch

from usstock import runtime_logs


class RuntimeLogsTest(unittest.TestCase):
    def test_log_sync_event_writes_daily_file(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            log_dir = Path(tmp_dir)
            now = datetime(2026, 6, 22, 11, 30, 5)

            with patch.object(runtime_logs, "LOG_DIR", log_dir):
                path = runtime_logs.log_sync_event(
                    source="Finnhub",
                    action="sync_market_news",
                    status="success",
                    details={"category": "general"},
                    result={"count": 3},
                    now=now,
                )

            self.assertEqual(path, log_dir / "2026-06-22.log")
            events = [
                json.loads(line)
                for line in (log_dir / "2026-06-22.log").read_text(
                    encoding="utf-8"
                ).splitlines()
            ]
            self.assertEqual(events[0]["timestamp"], "2026-06-22T11:30:05")
            self.assertEqual(events[0]["source"], "Finnhub")
            self.assertEqual(events[0]["action"], "sync_market_news")
            self.assertEqual(events[0]["status"], "success")
            self.assertEqual(events[0]["details"], {"category": "general"})
            self.assertEqual(events[0]["result"], {"count": 3})

    def test_log_sync_operation_records_failure_and_reraises(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            log_dir = Path(tmp_dir)

            @runtime_logs.log_sync_operation(
                source="SEC",
                action="sync_ticker",
                details_builder=lambda *, ticker: {"ticker": ticker},
            )
            def fail_sync(*, ticker: str) -> int:
                raise ValueError(f"{ticker} failed")

            with patch.object(runtime_logs, "LOG_DIR", log_dir):
                with self.assertRaises(ValueError):
                    fail_sync(ticker="AAPL")

            log_files = list(log_dir.glob("*.log"))
            self.assertEqual(len(log_files), 1)
            event = json.loads(log_files[0].read_text(encoding="utf-8").strip())
            self.assertEqual(event["source"], "SEC")
            self.assertEqual(event["action"], "sync_ticker")
            self.assertEqual(event["status"], "fail")
            self.assertEqual(event["details"], {"ticker": "AAPL"})
            self.assertEqual(event["error"]["type"], "ValueError")
            self.assertEqual(event["error"]["message"], "AAPL failed")


if __name__ == "__main__":
    unittest.main()
