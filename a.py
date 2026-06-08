import os
from datetime import datetime, timezone
from io import StringIO
from pathlib import Path
from urllib.request import Request, urlopen

import pandas as pd
import psycopg
from psycopg.types.json import Jsonb


SP500_URL = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
NASDAQ100_URL = "https://en.wikipedia.org/wiki/Nasdaq-100"
ENV_FILE = Path(__file__).resolve().parent / ".env"

USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/125.0 Safari/537.36"
)


def load_env_file(path):
    if not path.exists():
        return

    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue

        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip().strip("'\"")
        if key:
            os.environ.setdefault(key, value)


def read_wikipedia_tables(url):
    request = Request(url, headers={"User-Agent": USER_AGENT})
    with urlopen(request, timeout=30) as response:
        charset = response.headers.get_content_charset() or "utf-8"
        html = response.read().decode(charset)

    return pd.read_html(StringIO(html))


def normalize_table_columns(table):
    table = table.copy()
    if isinstance(table.columns, pd.MultiIndex):
        table.columns = [
            " ".join(str(part).strip() for part in column if str(part) != "nan").strip()
            for column in table.columns
        ]
    else:
        table.columns = [str(column).strip() for column in table.columns]

    return table


def get_first_matching_table(url, required_columns):
    tables = read_wikipedia_tables(url)
    for table in tables:
        table = normalize_table_columns(table)
        if all(column in table.columns for column in required_columns):
            return table

    raise ValueError(f"没有在 {url} 找到包含 {required_columns!r} 列的表格")


def clean_text(value):
    if pd.isna(value):
        return None

    text = str(value).strip()
    if not text or text.lower() == "nan":
        return None

    return text


def normalize_ticker(value):
    text = clean_text(value)
    if text is None:
        return None

    return text.upper().replace(" ", "")


def normalize_cik(value):
    text = clean_text(value)
    if text is None:
        return None

    digits = "".join(character for character in text if character.isdigit())
    if not digits:
        return None

    return digits.zfill(10)


def get_value(row, *column_names):
    for column_name in column_names:
        if column_name in row:
            return clean_text(row[column_name])

    return None


def build_sp500_records():
    table = get_first_matching_table(SP500_URL, ["Symbol", "Security"])
    records = {}

    for _, row in table.iterrows():
        ticker = normalize_ticker(row["Symbol"])
        company_name = clean_text(row["Security"])
        if not ticker or not company_name:
            continue

        records[ticker] = {
            "ticker": ticker,
            "company_name": company_name,
            "exchange": None,
            "sector": get_value(row, "GICS Sector"),
            "industry": get_value(row, "GICS Sub-Industry"),
            "sec_cik": normalize_cik(row["CIK"]) if "CIK" in row else None,
            "is_sp500": True,
            "is_nasdaq100": False,
            "source_url": SP500_URL,
            "metadata": {
                "wikipedia": {
                    "sp500": {
                        "sec_cik": normalize_cik(row["CIK"]) if "CIK" in row else None,
                        "date_added": get_value(row, "Date added"),
                        "headquarters": get_value(row, "Headquarters Location"),
                        "founded": get_value(row, "Founded"),
                    }
                }
            },
        }

    return records


def merge_metadata(left, right):
    result = dict(left or {})
    for key, value in (right or {}).items():
        if isinstance(value, dict) and isinstance(result.get(key), dict):
            result[key] = merge_metadata(result[key], value)
        else:
            result[key] = value

    return result


def build_nasdaq100_records(records):
    table = get_first_matching_table(NASDAQ100_URL, ["Ticker"])

    for _, row in table.iterrows():
        ticker = normalize_ticker(row["Ticker"])
        company_name = get_value(row, "Company", "Company Name")
        if not ticker or not company_name:
            continue

        existing = records.get(ticker, {})
        records[ticker] = {
            "ticker": ticker,
            "company_name": existing.get("company_name") or company_name,
            "exchange": existing.get("exchange") or "NASDAQ",
            "sector": existing.get("sector") or get_value(row, "GICS Sector"),
            "industry": existing.get("industry") or get_value(row, "GICS Sub-Industry"),
            "sec_cik": existing.get("sec_cik"),
            "is_sp500": existing.get("is_sp500", False),
            "is_nasdaq100": True,
            "source_url": existing.get("source_url") or NASDAQ100_URL,
            "metadata": merge_metadata(
                existing.get("metadata"),
                {
                    "wikipedia": {
                        "nasdaq100": {
                            "source_url": NASDAQ100_URL,
                            "company_name": company_name,
                        }
                    }
                },
            ),
        }

    return records


def build_stock_universe_records():
    records = build_sp500_records()
    build_nasdaq100_records(records)

    refreshed_at = datetime.now(timezone.utc)
    return [
        {
            **record,
            "country": "US",
            "currency": "USD",
            "asset_type": "equity",
            "is_active": True,
            "data_source": "wikipedia",
            "last_refreshed_at": refreshed_at,
        }
        for record in records.values()
    ]


def resolve_unique_cik(records, existing_cik_owner):
    used_cik_owner = dict(existing_cik_owner)

    for record in sorted(records, key=lambda item: item["ticker"]):
        sec_cik = record.get("sec_cik")
        if not sec_cik:
            continue

        owner = used_cik_owner.get(sec_cik)
        if owner and owner != record["ticker"]:
            record["sec_cik"] = None
            record["metadata"] = merge_metadata(
                record.get("metadata"),
                {
                    "sec": {
                        "cik": sec_cik,
                        "sec_cik_field_skipped_reason": (
                            f"sec_cik 已被 {owner} 使用，避免违反唯一索引"
                        ),
                    }
                },
            )
            continue

        used_cik_owner[sec_cik] = record["ticker"]

    return records


def upsert_stock_universe(records):
    if not records:
        return 0

    database_url = os.environ.get("DATABASE_URL")
    if not database_url:
        raise RuntimeError("缺少 DATABASE_URL，请在 .env 中配置 PostgreSQL 连接串")

    sql = """
        INSERT INTO stock_universe (
            ticker,
            company_name,
            exchange,
            sector,
            industry,
            country,
            currency,
            asset_type,
            sec_cik,
            is_active,
            is_sp500,
            is_nasdaq100,
            data_source,
            source_url,
            last_refreshed_at,
            metadata
        )
        VALUES (
            %(ticker)s,
            %(company_name)s,
            %(exchange)s,
            %(sector)s,
            %(industry)s,
            %(country)s,
            %(currency)s,
            %(asset_type)s,
            %(sec_cik)s,
            %(is_active)s,
            %(is_sp500)s,
            %(is_nasdaq100)s,
            %(data_source)s,
            %(source_url)s,
            %(last_refreshed_at)s,
            %(metadata)s
        )
        ON CONFLICT (ticker) DO UPDATE SET
            company_name = EXCLUDED.company_name,
            exchange = COALESCE(stock_universe.exchange, EXCLUDED.exchange),
            sector = COALESCE(EXCLUDED.sector, stock_universe.sector),
            industry = COALESCE(EXCLUDED.industry, stock_universe.industry),
            country = EXCLUDED.country,
            currency = EXCLUDED.currency,
            asset_type = EXCLUDED.asset_type,
            sec_cik = COALESCE(EXCLUDED.sec_cik, stock_universe.sec_cik),
            is_active = EXCLUDED.is_active,
            is_sp500 = stock_universe.is_sp500 OR EXCLUDED.is_sp500,
            is_nasdaq100 = stock_universe.is_nasdaq100 OR EXCLUDED.is_nasdaq100,
            data_source = EXCLUDED.data_source,
            source_url = COALESCE(EXCLUDED.source_url, stock_universe.source_url),
            last_refreshed_at = EXCLUDED.last_refreshed_at,
            metadata = stock_universe.metadata || EXCLUDED.metadata;
    """

    with psycopg.connect(database_url) as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT ticker, sec_cik
                FROM stock_universe
                WHERE sec_cik IS NOT NULL;
                """
            )
            existing_cik_owner = {
                sec_cik: ticker
                for ticker, sec_cik in cursor.fetchall()
            }
            records = resolve_unique_cik(records, existing_cik_owner)

            rows = [
                {
                    **record,
                    "metadata": Jsonb(record["metadata"]),
                }
                for record in records
            ]

            cursor.executemany(sql, rows)

    return len(rows)


def main():
    load_env_file(ENV_FILE)

    records = build_stock_universe_records()
    sp500_count = sum(1 for record in records if record["is_sp500"])
    nasdaq100_count = sum(1 for record in records if record["is_nasdaq100"])
    written_count = upsert_stock_universe(records)

    print(f"标普500股票数量: {sp500_count}")
    print(f"纳斯达克100股票数量: {nasdaq100_count}")
    print(f"写入 stock_universe 记录数量: {written_count}")


if __name__ == "__main__":
    main()
