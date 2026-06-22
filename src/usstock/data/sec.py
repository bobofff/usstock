"""SEC EDGAR data ingestion."""

from __future__ import annotations

import argparse
import json
import sys
import time
import urllib.error
import urllib.request
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal, InvalidOperation
from typing import Any

import psycopg
from psycopg import Connection
from psycopg.types.json import Jsonb

from usstock.config.settings import get_settings
from usstock.runtime_logs import log_sync_operation


CORE_FORM_TYPES = ("8-K", "10-Q", "10-K", "S-1", "20-F", "6-K")
SEC_COMPANY_TICKERS_EXCHANGE_URL = (
    "https://www.sec.gov/files/company_tickers_exchange.json"
)


class SecEdgarError(RuntimeError):
    """Raised when SEC EDGAR fetching or ingestion fails."""


@dataclass(frozen=True)
class SecCompany:
    cik: str
    ticker: str
    company_name: str
    exchange: str | None
    raw_payload: dict[str, Any]


@dataclass(frozen=True)
class SecFiling:
    cik: str
    ticker: str | None
    company_name: str | None
    accession_number: str
    accession_number_no_dash: str
    form_type: str
    filing_date: str
    report_date: str | None
    acceptance_datetime: datetime | None
    act: str | None
    file_number: str | None
    film_number: str | None
    items: str | None
    file_size_bytes: int | None
    is_inline_xbrl: bool
    is_xbrl: bool
    is_amendment: bool
    primary_document: str | None
    primary_doc_description: str | None
    filing_detail_url: str | None
    primary_document_url: str | None
    source_url: str
    raw_payload: dict[str, Any]


@dataclass(frozen=True)
class SecFinancialFact:
    fact_uid: str
    cik: str
    ticker: str | None
    accession_number: str | None
    taxonomy: str
    concept: str
    label: str | None
    description: str | None
    unit: str
    fiscal_year: int | None
    fiscal_period: str | None
    form_type: str | None
    filed_date: str | None
    start_date: str | None
    end_date: str | None
    frame: str | None
    value_numeric: Decimal | None
    value_text: str | None
    raw_payload: dict[str, Any]


class SecRateLimiter:
    """Small synchronous rate limiter for SEC fair-access requests."""

    def __init__(self, requests_per_second: float) -> None:
        if requests_per_second <= 0:
            raise SecEdgarError("SEC_RATE_LIMIT_PER_SECOND 必须大于 0。")

        self._min_interval = 1.0 / requests_per_second
        self._last_request_at = 0.0

    def wait(self) -> None:
        now = time.monotonic()
        elapsed = now - self._last_request_at
        if elapsed < self._min_interval:
            time.sleep(self._min_interval - elapsed)

        self._last_request_at = time.monotonic()


class SecEdgarClient:
    """Minimal SEC EDGAR HTTP client."""

    def __init__(
        self,
        *,
        user_agent: str,
        base_url: str = "https://data.sec.gov",
        archives_base_url: str = "https://www.sec.gov/Archives",
        requests_per_second: float = 5,
        timeout_seconds: float = 30,
        max_retries: int = 3,
    ) -> None:
        user_agent = user_agent.strip()
        if not user_agent or "@" not in user_agent:
            raise SecEdgarError(
                "SEC_USER_AGENT 不能为空，并建议包含联系邮箱，例如 "
                "'usstock research contact@example.com'。"
            )

        self.user_agent = user_agent
        self.base_url = base_url.rstrip("/")
        self.archives_base_url = archives_base_url.rstrip("/")
        self.timeout_seconds = timeout_seconds
        self.max_retries = max_retries
        self._rate_limiter = SecRateLimiter(requests_per_second)

    def fetch_json(self, url: str) -> dict[str, Any]:
        headers = {
            "User-Agent": self.user_agent,
            "Accept": "application/json",
        }
        request = urllib.request.Request(url, headers=headers)

        for attempt in range(self.max_retries + 1):
            self._rate_limiter.wait()
            try:
                with urllib.request.urlopen(
                    request,
                    timeout=self.timeout_seconds,
                ) as response:
                    body = response.read()
                return json.loads(body.decode("utf-8"))
            except urllib.error.HTTPError as exc:
                if not self._should_retry(exc.code, attempt):
                    raise SecEdgarError(f"SEC 请求失败 {exc.code}: {url}") from exc
                self._sleep_before_retry(exc, attempt)
            except urllib.error.URLError as exc:
                if attempt >= self.max_retries:
                    raise SecEdgarError(f"SEC 请求失败: {url}: {exc}") from exc
                self._sleep_before_retry(None, attempt)
            except json.JSONDecodeError as exc:
                raise SecEdgarError(f"SEC 返回不是有效 JSON: {url}") from exc

        raise SecEdgarError(f"SEC 请求失败: {url}")

    def fetch_company_registry(self) -> dict[str, Any]:
        return self.fetch_json(SEC_COMPANY_TICKERS_EXCHANGE_URL)

    def fetch_submissions(self, cik: str) -> dict[str, Any]:
        padded_cik = normalize_cik(cik)
        url = self.submissions_url(padded_cik)
        return self.fetch_json(url)

    def fetch_company_facts(self, cik: str) -> dict[str, Any]:
        padded_cik = normalize_cik(cik)
        url = self.company_facts_url(padded_cik)
        return self.fetch_json(url)

    def fetch_filing_index(self, cik: str, accession_number: str) -> dict[str, Any]:
        padded_cik = normalize_cik(cik)
        accession_number_no_dash = accession_number.replace("-", "")
        url = (
            f"{self.archives_base_url}/edgar/data/{cik_to_archive_path(padded_cik)}/"
            f"{accession_number_no_dash}/index.json"
        )
        return self.fetch_json(url)

    def submissions_url(self, cik: str) -> str:
        return f"{self.base_url}/submissions/CIK{normalize_cik(cik)}.json"

    def company_facts_url(self, cik: str) -> str:
        return f"{self.base_url}/api/xbrl/companyfacts/CIK{normalize_cik(cik)}.json"

    def filing_detail_url(self, cik: str, accession_number: str) -> str:
        padded_cik = normalize_cik(cik)
        accession_number_no_dash = accession_number.replace("-", "")
        return (
            f"{self.archives_base_url}/edgar/data/{cik_to_archive_path(padded_cik)}/"
            f"{accession_number_no_dash}/"
        )

    def filing_document_url(
        self,
        cik: str,
        accession_number: str,
        document_name: str | None,
    ) -> str | None:
        if not document_name:
            return None

        return self.filing_detail_url(cik, accession_number) + document_name

    def _should_retry(self, status_code: int, attempt: int) -> bool:
        retry_status_codes = {429, 500, 502, 503, 504}
        return attempt < self.max_retries and status_code in retry_status_codes

    def _sleep_before_retry(
        self,
        exc: urllib.error.HTTPError | None,
        attempt: int,
    ) -> None:
        retry_after = exc.headers.get("Retry-After") if exc else None
        if retry_after:
            try:
                sleep_seconds = float(retry_after)
            except ValueError:
                sleep_seconds = 2**attempt
        else:
            sleep_seconds = 2**attempt

        time.sleep(max(1.0, sleep_seconds))


def make_sec_client() -> SecEdgarClient:
    settings = get_settings()
    if not settings.sec_user_agent:
        raise SecEdgarError("缺少 SEC_USER_AGENT，请在环境变量或 .env 中配置。")

    return SecEdgarClient(
        user_agent=settings.sec_user_agent,
        base_url=settings.sec_base_url,
        archives_base_url=settings.sec_archives_base_url,
        requests_per_second=settings.sec_rate_limit_per_second,
        timeout_seconds=settings.sec_request_timeout_seconds,
    )


def normalize_cik(cik: str | int) -> str:
    cik_digits = "".join(ch for ch in str(cik).strip() if ch.isdigit())
    if not cik_digits:
        raise SecEdgarError(f"无效 CIK: {cik}")
    if len(cik_digits) > 10:
        raise SecEdgarError(f"CIK 超过 10 位: {cik}")
    return cik_digits.zfill(10)


def cik_to_archive_path(cik: str | int) -> str:
    return str(int(normalize_cik(cik)))


def parse_sec_datetime(value: str | None) -> datetime | None:
    if not value:
        return None

    text = str(value).strip()
    if not text:
        return None

    for fmt in ("%Y-%m-%dT%H:%M:%S.%fZ", "%Y-%m-%dT%H:%M:%S.%f", "%Y-%m-%dT%H:%M:%S"):
        try:
            return datetime.strptime(text, fmt)
        except ValueError:
            continue

    return None


def parse_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "t", "yes", "y"}
    return False


def parse_int(value: Any) -> int | None:
    if value is None or value == "":
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def parse_decimal(value: Any) -> Decimal | None:
    if value is None or value == "":
        return None
    if isinstance(value, bool):
        return None
    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError):
        return None


def parse_company_registry(payload: dict[str, Any]) -> list[SecCompany]:
    fields = payload.get("fields")
    data = payload.get("data")
    if not isinstance(fields, list) or not isinstance(data, list):
        raise SecEdgarError("SEC company_tickers_exchange.json 格式不符合预期。")

    companies: list[SecCompany] = []
    for row in data:
        if not isinstance(row, list):
            continue

        raw_payload = dict(zip(fields, row, strict=False))
        cik = normalize_cik(raw_payload.get("cik"))
        ticker = str(raw_payload.get("ticker") or "").strip().upper()
        company_name = str(raw_payload.get("name") or "").strip()
        exchange = raw_payload.get("exchange")
        exchange_text = str(exchange).strip() if exchange else None

        if not ticker or not company_name:
            continue

        companies.append(
            SecCompany(
                cik=cik,
                ticker=ticker,
                company_name=company_name,
                exchange=exchange_text,
                raw_payload=raw_payload,
            )
        )

    return companies


def get_recent_filings_block(submissions_payload: dict[str, Any]) -> dict[str, list[Any]]:
    filings = submissions_payload.get("filings")
    if not isinstance(filings, dict):
        return {}

    recent = filings.get("recent")
    if not isinstance(recent, dict):
        return {}

    return {
        key: value
        for key, value in recent.items()
        if isinstance(value, list)
    }


def get_recent_filing_row(
    recent: dict[str, list[Any]],
    index: int,
) -> dict[str, Any]:
    return {
        key: values[index]
        for key, values in recent.items()
        if index < len(values)
    }


def parse_submissions_filings(
    payload: dict[str, Any],
    *,
    source_url: str,
    archives_base_url: str,
    ticker: str | None = None,
    form_types: set[str] | None = None,
    limit: int | None = None,
) -> list[SecFiling]:
    cik = normalize_cik(payload.get("cik"))
    company_name = payload.get("name")
    company_name_text = str(company_name).strip() if company_name else None
    recent = get_recent_filings_block(payload)
    accession_numbers = recent.get("accessionNumber") or []

    filings: list[SecFiling] = []
    for index, accession_number_value in enumerate(accession_numbers):
        if limit is not None and len(filings) >= limit:
            break

        row = get_recent_filing_row(recent, index)
        accession_number = str(accession_number_value or "").strip()
        form_type = str(row.get("form") or "").strip()
        filing_date = str(row.get("filingDate") or "").strip()
        if not accession_number or not form_type or not filing_date:
            continue
        if form_types and form_type not in form_types:
            continue

        accession_number_no_dash = accession_number.replace("-", "")
        primary_document = clean_optional_text(row.get("primaryDocument"))
        filing_detail_url = (
            f"{archives_base_url}/edgar/data/{cik_to_archive_path(cik)}/"
            f"{accession_number_no_dash}/"
        )
        primary_document_url = (
            f"{filing_detail_url}{primary_document}" if primary_document else None
        )

        filings.append(
            SecFiling(
                cik=cik,
                ticker=ticker.upper() if ticker else None,
                company_name=company_name_text,
                accession_number=accession_number,
                accession_number_no_dash=accession_number_no_dash,
                form_type=form_type,
                filing_date=filing_date,
                report_date=clean_optional_text(row.get("reportDate")),
                acceptance_datetime=parse_sec_datetime(row.get("acceptanceDateTime")),
                act=clean_optional_text(row.get("act")),
                file_number=clean_optional_text(row.get("fileNumber")),
                film_number=clean_optional_text(row.get("filmNumber")),
                items=clean_optional_text(row.get("items")),
                file_size_bytes=parse_int(row.get("size")),
                is_inline_xbrl=parse_bool(row.get("isInlineXBRL")),
                is_xbrl=parse_bool(row.get("isXBRL")),
                is_amendment=form_type.endswith("/A"),
                primary_document=primary_document,
                primary_doc_description=clean_optional_text(
                    row.get("primaryDocDescription")
                ),
                filing_detail_url=filing_detail_url,
                primary_document_url=primary_document_url,
                source_url=source_url,
                raw_payload=row,
            )
        )

    return filings


def clean_optional_text(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def parse_company_facts(
    payload: dict[str, Any],
    *,
    ticker: str | None,
) -> list[SecFinancialFact]:
    cik = normalize_cik(payload.get("cik"))
    facts = payload.get("facts")
    if not isinstance(facts, dict):
        return []

    rows: list[SecFinancialFact] = []
    for taxonomy, concepts in facts.items():
        if not isinstance(concepts, dict):
            continue

        for concept, concept_payload in concepts.items():
            if not isinstance(concept_payload, dict):
                continue

            label = clean_optional_text(concept_payload.get("label"))
            description = clean_optional_text(concept_payload.get("description"))
            units = concept_payload.get("units")
            if not isinstance(units, dict):
                continue

            for unit, values in units.items():
                if not isinstance(values, list):
                    continue

                for item in values:
                    if not isinstance(item, dict):
                        continue

                    value = item.get("val")
                    value_numeric = parse_decimal(value)
                    value_text = None if value_numeric is not None else clean_optional_text(value)
                    accession_number = clean_optional_text(item.get("accn"))
                    fiscal_year = parse_int(item.get("fy"))
                    fiscal_period = clean_optional_text(item.get("fp"))
                    form_type = clean_optional_text(item.get("form"))
                    filed_date = clean_optional_text(item.get("filed"))
                    start_date = clean_optional_text(item.get("start"))
                    end_date = clean_optional_text(item.get("end"))
                    frame = clean_optional_text(item.get("frame"))
                    fact_uid = build_fact_uid(
                        cik=cik,
                        taxonomy=taxonomy,
                        concept=concept,
                        unit=unit,
                        accession_number=accession_number,
                        fiscal_year=fiscal_year,
                        fiscal_period=fiscal_period,
                        form_type=form_type,
                        start_date=start_date,
                        end_date=end_date,
                        frame=frame,
                    )

                    rows.append(
                        SecFinancialFact(
                            fact_uid=fact_uid,
                            cik=cik,
                            ticker=ticker.upper() if ticker else None,
                            accession_number=accession_number,
                            taxonomy=taxonomy,
                            concept=concept,
                            label=label,
                            description=description,
                            unit=str(unit),
                            fiscal_year=fiscal_year,
                            fiscal_period=fiscal_period,
                            form_type=form_type,
                            filed_date=filed_date,
                            start_date=start_date,
                            end_date=end_date,
                            frame=frame,
                            value_numeric=value_numeric,
                            value_text=value_text,
                            raw_payload=item,
                        )
                    )

    return rows


def build_fact_uid(
    *,
    cik: str,
    taxonomy: str,
    concept: str,
    unit: str,
    accession_number: str | None,
    fiscal_year: int | None,
    fiscal_period: str | None,
    form_type: str | None,
    start_date: str | None,
    end_date: str | None,
    frame: str | None,
) -> str:
    parts = (
        cik,
        taxonomy,
        concept,
        unit,
        accession_number or "",
        str(fiscal_year or ""),
        fiscal_period or "",
        form_type or "",
        start_date or "",
        end_date or "",
        frame or "",
    )
    return "|".join(parts)


def get_database_url(database_url: str | None = None) -> str:
    database_url = database_url or get_settings().database_url
    if not database_url:
        raise SecEdgarError("缺少 DATABASE_URL，请在环境变量或 .env 中配置。")
    return database_url


def _count_log_result(count: int) -> dict[str, int]:
    return {"count": count}


def _company_registry_log_details(
    *,
    database_url: str | None = None,
    client: SecEdgarClient | None = None,
) -> dict[str, str]:
    return {}


def _submissions_log_details(
    *,
    cik: str,
    ticker: str | None = None,
    form_types: set[str] | None = None,
    limit: int | None = None,
    database_url: str | None = None,
    client: SecEdgarClient | None = None,
) -> dict[str, Any]:
    return {
        "cik": cik,
        "ticker": ticker,
        "form_types": sorted(form_types) if form_types else None,
        "limit": limit,
    }


def _company_facts_log_details(
    *,
    cik: str,
    ticker: str | None = None,
    extract_facts: bool = True,
    fact_limit: int | None = None,
    database_url: str | None = None,
    client: SecEdgarClient | None = None,
) -> dict[str, Any]:
    return {
        "cik": cik,
        "ticker": ticker,
        "extract_facts": extract_facts,
        "fact_limit": fact_limit,
    }


def _ticker_log_details(
    *,
    ticker: str,
    include_company_facts: bool = False,
    form_types: set[str] | None = None,
    filing_limit: int | None = None,
    fact_limit: int | None = None,
    database_url: str | None = None,
    client: SecEdgarClient | None = None,
) -> dict[str, Any]:
    return {
        "ticker": ticker,
        "include_company_facts": include_company_facts,
        "form_types": sorted(form_types) if form_types else None,
        "filing_limit": filing_limit,
        "fact_limit": fact_limit,
    }


def _ticker_log_result(result: tuple[str, int, int]) -> dict[str, Any]:
    cik, filing_count, fact_count = result
    return {
        "cik": cik,
        "filing_count": filing_count,
        "fact_count": fact_count,
    }


def resolve_ticker_cik(conn: Connection, ticker: str) -> str | None:
    row = conn.execute(
        """
        SELECT sec_cik
        FROM stock_universe
        WHERE upper(ticker) = upper(%s)
          AND sec_cik IS NOT NULL
        LIMIT 1
        """,
        (ticker,),
    ).fetchone()
    if row:
        return row[0]

    row = conn.execute(
        """
        SELECT cik
        FROM sec_company_registry
        WHERE upper(ticker) = upper(%s)
        LIMIT 1
        """,
        (ticker,),
    ).fetchone()
    return row[0] if row else None


def resolve_cik_ticker(conn: Connection, cik: str) -> str | None:
    row = conn.execute(
        """
        SELECT ticker
        FROM sec_company_registry
        WHERE cik = %s
        ORDER BY ticker
        LIMIT 1
        """,
        (normalize_cik(cik),),
    ).fetchone()
    return row[0] if row else None


def upsert_company_registry(conn: Connection, companies: list[SecCompany]) -> int:
    count = 0
    with conn.transaction():
        for company in companies:
            conn.execute(
                """
                INSERT INTO sec_company_registry (
                    cik,
                    ticker,
                    company_name,
                    exchange,
                    source_url,
                    raw_payload,
                    last_refreshed_at
                )
                VALUES (%s, %s, %s, %s, %s, %s, now())
                ON CONFLICT (cik, ticker)
                DO UPDATE SET
                    company_name = EXCLUDED.company_name,
                    exchange = EXCLUDED.exchange,
                    source_url = EXCLUDED.source_url,
                    raw_payload = EXCLUDED.raw_payload,
                    last_refreshed_at = now(),
                    updated_at = now()
                """,
                (
                    company.cik,
                    company.ticker,
                    company.company_name,
                    company.exchange,
                    SEC_COMPANY_TICKERS_EXCHANGE_URL,
                    Jsonb(company.raw_payload),
                ),
            )
            conn.execute(
                """
                UPDATE stock_universe
                SET
                    sec_cik = %s,
                    last_refreshed_at = now(),
                    metadata = metadata || jsonb_build_object(
                        'sec_company_name', %s::text,
                        'sec_exchange', %s::text
                    ),
                    updated_at = now()
                WHERE upper(ticker) = upper(%s)
                """,
                (
                    company.cik,
                    company.company_name,
                    company.exchange,
                    company.ticker,
                ),
            )
            count += 1

    return count


def upsert_company_submissions(
    conn: Connection,
    *,
    cik: str,
    ticker: str | None,
    payload: dict[str, Any],
    source_url: str,
) -> None:
    conn.execute(
        """
        INSERT INTO sec_company_submissions (
            cik,
            ticker,
            company_name,
            sic,
            sic_description,
            entity_type,
            fiscal_year_end,
            state_of_incorporation,
            source_url,
            raw_payload,
            fetched_at
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, now())
        ON CONFLICT (cik)
        DO UPDATE SET
            ticker = EXCLUDED.ticker,
            company_name = EXCLUDED.company_name,
            sic = EXCLUDED.sic,
            sic_description = EXCLUDED.sic_description,
            entity_type = EXCLUDED.entity_type,
            fiscal_year_end = EXCLUDED.fiscal_year_end,
            state_of_incorporation = EXCLUDED.state_of_incorporation,
            source_url = EXCLUDED.source_url,
            raw_payload = EXCLUDED.raw_payload,
            fetched_at = now(),
            updated_at = now()
        """,
        (
            normalize_cik(cik),
            ticker.upper() if ticker else None,
            clean_optional_text(payload.get("name")),
            clean_optional_text(payload.get("sic")),
            clean_optional_text(payload.get("sicDescription")),
            clean_optional_text(payload.get("entityType")),
            clean_optional_text(payload.get("fiscalYearEnd")),
            clean_optional_text(payload.get("stateOfIncorporation")),
            source_url,
            Jsonb(payload),
        ),
    )


def upsert_filings(conn: Connection, filings: list[SecFiling]) -> int:
    count = 0
    for filing in filings:
        conn.execute(
            """
            INSERT INTO sec_filings (
                cik,
                ticker,
                company_name,
                accession_number,
                accession_number_no_dash,
                form_type,
                filing_date,
                report_date,
                acceptance_datetime,
                act,
                file_number,
                film_number,
                items,
                file_size_bytes,
                is_inline_xbrl,
                is_xbrl,
                is_amendment,
                primary_document,
                primary_doc_description,
                filing_detail_url,
                primary_document_url,
                source_url,
                raw_payload,
                fetched_at
            )
            VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, now()
            )
            ON CONFLICT (accession_number)
            DO UPDATE SET
                cik = EXCLUDED.cik,
                ticker = EXCLUDED.ticker,
                company_name = EXCLUDED.company_name,
                accession_number_no_dash = EXCLUDED.accession_number_no_dash,
                form_type = EXCLUDED.form_type,
                filing_date = EXCLUDED.filing_date,
                report_date = EXCLUDED.report_date,
                acceptance_datetime = EXCLUDED.acceptance_datetime,
                act = EXCLUDED.act,
                file_number = EXCLUDED.file_number,
                film_number = EXCLUDED.film_number,
                items = EXCLUDED.items,
                file_size_bytes = EXCLUDED.file_size_bytes,
                is_inline_xbrl = EXCLUDED.is_inline_xbrl,
                is_xbrl = EXCLUDED.is_xbrl,
                is_amendment = EXCLUDED.is_amendment,
                primary_document = EXCLUDED.primary_document,
                primary_doc_description = EXCLUDED.primary_doc_description,
                filing_detail_url = EXCLUDED.filing_detail_url,
                primary_document_url = EXCLUDED.primary_document_url,
                source_url = EXCLUDED.source_url,
                raw_payload = EXCLUDED.raw_payload,
                fetched_at = now(),
                updated_at = now()
            """,
            (
                filing.cik,
                filing.ticker,
                filing.company_name,
                filing.accession_number,
                filing.accession_number_no_dash,
                filing.form_type,
                filing.filing_date,
                filing.report_date,
                filing.acceptance_datetime,
                filing.act,
                filing.file_number,
                filing.film_number,
                filing.items,
                filing.file_size_bytes,
                filing.is_inline_xbrl,
                filing.is_xbrl,
                filing.is_amendment,
                filing.primary_document,
                filing.primary_doc_description,
                filing.filing_detail_url,
                filing.primary_document_url,
                filing.source_url,
                Jsonb(filing.raw_payload),
            ),
        )
        upsert_primary_document(conn, filing)
        count += 1

    return count


def upsert_primary_document(conn: Connection, filing: SecFiling) -> None:
    if not filing.primary_document or not filing.primary_document_url:
        return

    conn.execute(
        """
        INSERT INTO sec_filing_documents (
            cik,
            ticker,
            accession_number,
            accession_number_no_dash,
            document_sequence,
            document_name,
            document_type,
            document_description,
            document_url,
            is_primary,
            is_xbrl,
            raw_payload,
            fetched_at
        )
        VALUES (%s, %s, %s, %s, 1, %s, %s, %s, %s, TRUE, %s, %s, now())
        ON CONFLICT (accession_number, document_name)
        DO UPDATE SET
            cik = EXCLUDED.cik,
            ticker = EXCLUDED.ticker,
            accession_number_no_dash = EXCLUDED.accession_number_no_dash,
            document_sequence = EXCLUDED.document_sequence,
            document_type = EXCLUDED.document_type,
            document_description = EXCLUDED.document_description,
            document_url = EXCLUDED.document_url,
            is_primary = TRUE,
            is_xbrl = EXCLUDED.is_xbrl,
            raw_payload = EXCLUDED.raw_payload,
            fetched_at = now(),
            updated_at = now()
        """,
        (
            filing.cik,
            filing.ticker,
            filing.accession_number,
            filing.accession_number_no_dash,
            filing.primary_document,
            filing.form_type,
            filing.primary_doc_description,
            filing.primary_document_url,
            filing.is_xbrl or filing.is_inline_xbrl,
            Jsonb(filing.raw_payload),
        ),
    )


def upsert_company_facts_raw(
    conn: Connection,
    *,
    cik: str,
    ticker: str | None,
    payload: dict[str, Any],
    source_url: str,
) -> None:
    conn.execute(
        """
        INSERT INTO sec_company_facts (
            cik,
            ticker,
            company_name,
            entity_name,
            source_url,
            raw_payload,
            fetched_at
        )
        VALUES (%s, %s, %s, %s, %s, %s, now())
        ON CONFLICT (cik)
        DO UPDATE SET
            ticker = EXCLUDED.ticker,
            company_name = EXCLUDED.company_name,
            entity_name = EXCLUDED.entity_name,
            source_url = EXCLUDED.source_url,
            raw_payload = EXCLUDED.raw_payload,
            fetched_at = now(),
            updated_at = now()
        """,
        (
            normalize_cik(cik),
            ticker.upper() if ticker else None,
            clean_optional_text(payload.get("entityName")),
            clean_optional_text(payload.get("entityName")),
            source_url,
            Jsonb(payload),
        ),
    )


def upsert_financial_facts(
    conn: Connection,
    facts: list[SecFinancialFact],
    *,
    limit: int | None = None,
) -> int:
    count = 0
    for fact in facts[:limit]:
        conn.execute(
            """
            INSERT INTO sec_financial_facts (
                fact_uid,
                cik,
                ticker,
                accession_number,
                taxonomy,
                concept,
                label,
                description,
                unit,
                fiscal_year,
                fiscal_period,
                form_type,
                filed_date,
                start_date,
                end_date,
                frame,
                value_numeric,
                value_text,
                raw_payload,
                extracted_at
            )
            VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s, %s, %s, %s, now()
            )
            ON CONFLICT (fact_uid)
            DO UPDATE SET
                ticker = EXCLUDED.ticker,
                accession_number = EXCLUDED.accession_number,
                label = EXCLUDED.label,
                description = EXCLUDED.description,
                fiscal_year = EXCLUDED.fiscal_year,
                fiscal_period = EXCLUDED.fiscal_period,
                form_type = EXCLUDED.form_type,
                filed_date = EXCLUDED.filed_date,
                start_date = EXCLUDED.start_date,
                end_date = EXCLUDED.end_date,
                frame = EXCLUDED.frame,
                value_numeric = EXCLUDED.value_numeric,
                value_text = EXCLUDED.value_text,
                raw_payload = EXCLUDED.raw_payload,
                extracted_at = now(),
                updated_at = now()
            """,
            (
                fact.fact_uid,
                fact.cik,
                fact.ticker,
                fact.accession_number,
                fact.taxonomy,
                fact.concept,
                fact.label,
                fact.description,
                fact.unit,
                fact.fiscal_year,
                fact.fiscal_period,
                fact.form_type,
                fact.filed_date,
                fact.start_date,
                fact.end_date,
                fact.frame,
                fact.value_numeric,
                fact.value_text,
                Jsonb(fact.raw_payload),
            ),
        )
        count += 1

    return count


@log_sync_operation(
    source="SEC",
    action="sync_company_registry",
    details_builder=_company_registry_log_details,
    result_builder=_count_log_result,
)
def sync_company_registry(
    *,
    database_url: str | None = None,
    client: SecEdgarClient | None = None,
) -> int:
    client = client or make_sec_client()
    payload = client.fetch_company_registry()
    companies = parse_company_registry(payload)

    with psycopg.connect(get_database_url(database_url), autocommit=False) as conn:
        return upsert_company_registry(conn, companies)


@log_sync_operation(
    source="SEC",
    action="sync_submissions",
    details_builder=_submissions_log_details,
    result_builder=_count_log_result,
)
def sync_submissions(
    *,
    cik: str,
    ticker: str | None = None,
    form_types: set[str] | None = None,
    limit: int | None = None,
    database_url: str | None = None,
    client: SecEdgarClient | None = None,
) -> int:
    client = client or make_sec_client()
    padded_cik = normalize_cik(cik)
    source_url = client.submissions_url(padded_cik)
    payload = client.fetch_submissions(padded_cik)
    filings = parse_submissions_filings(
        payload,
        source_url=source_url,
        archives_base_url=client.archives_base_url,
        ticker=ticker,
        form_types=form_types,
        limit=limit,
    )

    with psycopg.connect(get_database_url(database_url), autocommit=False) as conn:
        with conn.transaction():
            upsert_company_submissions(
                conn,
                cik=padded_cik,
                ticker=ticker,
                payload=payload,
                source_url=source_url,
            )
            return upsert_filings(conn, filings)


@log_sync_operation(
    source="SEC",
    action="sync_company_facts",
    details_builder=_company_facts_log_details,
    result_builder=_count_log_result,
)
def sync_company_facts(
    *,
    cik: str,
    ticker: str | None = None,
    extract_facts: bool = True,
    fact_limit: int | None = None,
    database_url: str | None = None,
    client: SecEdgarClient | None = None,
) -> int:
    client = client or make_sec_client()
    padded_cik = normalize_cik(cik)
    source_url = client.company_facts_url(padded_cik)
    payload = client.fetch_company_facts(padded_cik)
    facts = parse_company_facts(payload, ticker=ticker) if extract_facts else []

    with psycopg.connect(get_database_url(database_url), autocommit=False) as conn:
        with conn.transaction():
            upsert_company_facts_raw(
                conn,
                cik=padded_cik,
                ticker=ticker,
                payload=payload,
                source_url=source_url,
            )
            return upsert_financial_facts(conn, facts, limit=fact_limit)


@log_sync_operation(
    source="SEC",
    action="sync_ticker",
    details_builder=_ticker_log_details,
    result_builder=_ticker_log_result,
)
def sync_ticker(
    *,
    ticker: str,
    include_company_facts: bool = False,
    form_types: set[str] | None = None,
    filing_limit: int | None = None,
    fact_limit: int | None = None,
    database_url: str | None = None,
    client: SecEdgarClient | None = None,
) -> tuple[str, int, int]:
    ticker = ticker.strip().upper()
    if not ticker:
        raise SecEdgarError("ticker 不能为空。")

    database_url = get_database_url(database_url)
    with psycopg.connect(database_url, autocommit=True) as conn:
        cik = resolve_ticker_cik(conn, ticker)

    if not cik:
        sync_company_registry(database_url=database_url, client=client)
        with psycopg.connect(database_url, autocommit=True) as conn:
            cik = resolve_ticker_cik(conn, ticker)

    if not cik:
        raise SecEdgarError(f"未找到 ticker 对应的 SEC CIK: {ticker}")

    client = client or make_sec_client()
    filing_count = sync_submissions(
        cik=cik,
        ticker=ticker,
        form_types=form_types,
        limit=filing_limit,
        database_url=database_url,
        client=client,
    )
    fact_count = 0
    if include_company_facts:
        fact_count = sync_company_facts(
            cik=cik,
            ticker=ticker,
            fact_limit=fact_limit,
            database_url=database_url,
            client=client,
        )

    return cik, filing_count, fact_count


def parse_form_types(values: list[str] | None) -> set[str] | None:
    if not values:
        return set(CORE_FORM_TYPES)

    form_types: set[str] = set()
    for value in values:
        for item in value.split(","):
            text = item.strip().upper()
            if text:
                form_types.add(text)

    return form_types or None


def add_common_database_arg(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--database-url", help="PostgreSQL DATABASE_URL")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Sync SEC EDGAR data.")
    subparsers = parser.add_subparsers(dest="command")

    registry_parser = subparsers.add_parser(
        "sync-registry",
        help="同步 SEC ticker / CIK 公司映射",
    )
    add_common_database_arg(registry_parser)

    submissions_parser = subparsers.add_parser(
        "sync-submissions",
        help="按 CIK 同步 SEC submissions 和 filing 元数据",
    )
    add_common_database_arg(submissions_parser)
    submissions_parser.add_argument("--cik", required=True, help="SEC CIK")
    submissions_parser.add_argument("--ticker", help="股票代码")
    submissions_parser.add_argument(
        "--form-type",
        action="append",
        help="只同步指定表单，可重复传入，默认同步核心表单",
    )
    submissions_parser.add_argument("--limit", type=int, help="最多同步多少条 filing")

    facts_parser = subparsers.add_parser(
        "sync-company-facts",
        help="按 CIK 同步 SEC company facts",
    )
    add_common_database_arg(facts_parser)
    facts_parser.add_argument("--cik", required=True, help="SEC CIK")
    facts_parser.add_argument("--ticker", help="股票代码")
    facts_parser.add_argument(
        "--raw-only",
        action="store_true",
        help="只保存原始 company facts，不抽取 sec_financial_facts",
    )
    facts_parser.add_argument("--fact-limit", type=int, help="最多抽取多少条 fact")

    ticker_parser = subparsers.add_parser(
        "sync-ticker",
        help="按 ticker 自动解析 CIK 并同步 SEC 数据",
    )
    add_common_database_arg(ticker_parser)
    ticker_parser.add_argument("ticker", help="股票代码，例如 AAPL")
    ticker_parser.add_argument(
        "--include-company-facts",
        action="store_true",
        help="同时同步 company facts 并抽取财务事实",
    )
    ticker_parser.add_argument(
        "--form-type",
        action="append",
        help="只同步指定表单，可重复传入，默认同步核心表单",
    )
    ticker_parser.add_argument("--filing-limit", type=int, help="最多同步多少条 filing")
    ticker_parser.add_argument("--fact-limit", type=int, help="最多抽取多少条 fact")

    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    try:
        if args.command == "sync-registry":
            count = sync_company_registry(database_url=args.database_url)
            print(f"[完成] SEC 公司映射同步 {count} 条")
            return 0

        if args.command == "sync-submissions":
            count = sync_submissions(
                cik=args.cik,
                ticker=args.ticker,
                form_types=parse_form_types(args.form_type),
                limit=args.limit,
                database_url=args.database_url,
            )
            print(f"[完成] SEC submissions 同步 filing {count} 条")
            return 0

        if args.command == "sync-company-facts":
            count = sync_company_facts(
                cik=args.cik,
                ticker=args.ticker,
                extract_facts=not args.raw_only,
                fact_limit=args.fact_limit,
                database_url=args.database_url,
            )
            print(f"[完成] SEC company facts 抽取 fact {count} 条")
            return 0

        if args.command == "sync-ticker":
            cik, filing_count, fact_count = sync_ticker(
                ticker=args.ticker,
                include_company_facts=args.include_company_facts,
                form_types=parse_form_types(args.form_type),
                filing_limit=args.filing_limit,
                fact_limit=args.fact_limit,
                database_url=args.database_url,
            )
            print(
                f"[完成] {args.ticker.upper()} CIK={cik} "
                f"filing={filing_count} fact={fact_count}"
            )
            return 0

        parser.print_help()
        return 2
    except SecEdgarError as exc:
        print(f"SEC 同步失败: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
