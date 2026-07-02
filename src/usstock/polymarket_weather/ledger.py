"""CSV trade ledger and performance statistics."""

from __future__ import annotations

import csv
from dataclasses import asdict, dataclass
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any

from usstock.config.settings import PROJECT_ROOT


DEFAULT_LEDGER_PATH = PROJECT_ROOT / "data" / "polymarket_weather_trades.csv"


@dataclass(frozen=True)
class TradeRecord:
    trade_id: str
    timestamp: datetime
    city_id: str
    target_date: date
    kind: str
    market_slug: str | None
    outcome: str
    token_id: str | None
    side: str
    price: float
    probability: float
    edge: float
    stake: float
    shares: float
    status: str = "open"
    realized_pnl: float = 0.0
    notes: str = ""


@dataclass(frozen=True)
class PerformanceStats:
    total_trades: int
    settled_trades: int
    win_rate: float
    total_stake: float
    total_pnl: float
    roi: float
    max_drawdown: float
    average_edge: float


class TradeLedger:
    fields = [
        "trade_id",
        "timestamp",
        "city_id",
        "target_date",
        "kind",
        "market_slug",
        "outcome",
        "token_id",
        "side",
        "price",
        "probability",
        "edge",
        "stake",
        "shares",
        "status",
        "realized_pnl",
        "notes",
    ]

    def __init__(self, path: Path = DEFAULT_LEDGER_PATH) -> None:
        self.path = path

    def append(self, record: TradeRecord) -> Path:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        exists = self.path.exists()
        with self.path.open("a", encoding="utf-8", newline="") as file:
            writer = csv.DictWriter(file, fieldnames=self.fields)
            if not exists:
                writer.writeheader()
            writer.writerow(_record_to_row(record))
        return self.path

    def load(self) -> tuple[TradeRecord, ...]:
        if not self.path.exists():
            return ()
        with self.path.open("r", encoding="utf-8", newline="") as file:
            reader = csv.DictReader(file)
            return tuple(_row_to_record(row) for row in reader)

    def stats(self) -> PerformanceStats:
        return compute_performance_stats(self.load())


def make_trade_id(
    *,
    city_id: str,
    target_date: date,
    kind: str,
    outcome: str,
    timestamp: datetime | None = None,
) -> str:
    now = timestamp or datetime.now(timezone.utc)
    safe_outcome = "".join(char if char.isalnum() else "-" for char in outcome.lower())
    return f"{city_id}:{target_date.isoformat()}:{kind}:{safe_outcome}:{now.timestamp():.0f}"


def _record_to_row(record: TradeRecord) -> dict[str, Any]:
    row = asdict(record)
    row["timestamp"] = record.timestamp.isoformat()
    row["target_date"] = record.target_date.isoformat()
    return row


def _row_to_record(row: dict[str, str]) -> TradeRecord:
    return TradeRecord(
        trade_id=row["trade_id"],
        timestamp=datetime.fromisoformat(row["timestamp"]),
        city_id=row["city_id"],
        target_date=date.fromisoformat(row["target_date"]),
        kind=row["kind"],
        market_slug=row.get("market_slug") or None,
        outcome=row["outcome"],
        token_id=row.get("token_id") or None,
        side=row["side"],
        price=float(row["price"]),
        probability=float(row["probability"]),
        edge=float(row["edge"]),
        stake=float(row["stake"]),
        shares=float(row["shares"]),
        status=row.get("status") or "open",
        realized_pnl=float(row.get("realized_pnl") or 0.0),
        notes=row.get("notes") or "",
    )


def compute_performance_stats(records: tuple[TradeRecord, ...]) -> PerformanceStats:
    total_trades = len(records)
    settled = tuple(record for record in records if record.status == "settled")
    total_stake = sum(record.stake for record in settled)
    total_pnl = sum(record.realized_pnl for record in settled)
    wins = sum(1 for record in settled if record.realized_pnl > 0)
    win_rate = wins / len(settled) if settled else 0.0
    roi = total_pnl / total_stake if total_stake else 0.0
    average_edge = (
        sum(record.edge for record in records) / total_trades if total_trades else 0.0
    )
    max_drawdown = compute_max_drawdown([record.realized_pnl for record in settled])
    return PerformanceStats(
        total_trades=total_trades,
        settled_trades=len(settled),
        win_rate=win_rate,
        total_stake=total_stake,
        total_pnl=total_pnl,
        roi=roi,
        max_drawdown=max_drawdown,
        average_edge=average_edge,
    )


def compute_max_drawdown(pnls: list[float]) -> float:
    equity = 0.0
    peak = 0.0
    max_drawdown = 0.0
    for pnl in pnls:
        equity += pnl
        peak = max(peak, equity)
        max_drawdown = min(max_drawdown, equity - peak)
    return max_drawdown
