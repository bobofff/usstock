from __future__ import annotations

import unittest
from datetime import date

from usstock.polymarket_weather.backtest import HistoricalSignal, run_backtest
from usstock.polymarket_weather.risk import RiskConfig


class WeatherBacktestTest(unittest.TestCase):
    def test_run_backtest_sizes_and_settles_trades(self) -> None:
        result = run_backtest(
            (
                HistoricalSignal(
                    city_id="new-york",
                    target_date=date(2026, 7, 1),
                    kind="high",
                    outcome="80",
                    probability=0.60,
                    market_price=0.50,
                    settled_outcome=True,
                ),
                HistoricalSignal(
                    city_id="new-york",
                    target_date=date(2026, 7, 2),
                    kind="high",
                    outcome="81",
                    probability=0.60,
                    market_price=0.50,
                    settled_outcome=False,
                ),
            ),
            risk=RiskConfig(
                bankroll=1_000,
                kelly_mode="half",
                min_edge=0.01,
                max_trade_fraction=0.03,
            ),
        )

        self.assertEqual(len(result.trades), 2)
        self.assertEqual(result.stats.settled_trades, 2)
        self.assertAlmostEqual(result.stats.win_rate, 0.5)
        self.assertLess(result.stats.total_pnl, 1.0)


if __name__ == "__main__":
    unittest.main()
