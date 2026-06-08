-- Migration: 002_create_sec_edgar_tables
-- Purpose: create SEC EDGAR ingestion tables for company mapping, filings,
-- filing documents, company facts raw payloads, and normalized fact rows.
--
-- Dialect target: PostgreSQL.
--
-- Note:
--   This migration intentionally does not add foreign key constraints. Tables
--   are linked by cik, ticker, and accession_number through application logic.

-- migrate:up

CREATE TABLE IF NOT EXISTS sec_company_registry (
    id BIGSERIAL PRIMARY KEY,
    cik TEXT NOT NULL,
    ticker TEXT NOT NULL,
    company_name TEXT NOT NULL,
    exchange TEXT,
    source_url TEXT NOT NULL DEFAULT 'https://www.sec.gov/files/company_tickers_exchange.json',
    raw_payload JSONB NOT NULL DEFAULT '{}'::jsonb,
    first_seen_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_refreshed_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),

    CONSTRAINT sec_company_registry_cik_format
        CHECK (cik ~ '^[0-9]{10}$'),
    CONSTRAINT sec_company_registry_ticker_not_blank
        CHECK (length(trim(ticker)) > 0),
    CONSTRAINT sec_company_registry_company_name_not_blank
        CHECK (length(trim(company_name)) > 0)
);

COMMENT ON TABLE sec_company_registry IS
    'SEC 公司注册映射表，用于保存 ticker、CIK、交易所和公司名称的官方映射快照。';
COMMENT ON COLUMN sec_company_registry.id IS
    '内部自增主键。';
COMMENT ON COLUMN sec_company_registry.cik IS
    'SEC Central Index Key，统一保存为 10 位补零字符串。';
COMMENT ON COLUMN sec_company_registry.ticker IS
    '股票交易代码，来自 SEC 公司 ticker 映射文件。';
COMMENT ON COLUMN sec_company_registry.company_name IS
    'SEC 记录中的公司名称。';
COMMENT ON COLUMN sec_company_registry.exchange IS
    'SEC 映射文件中的交易所名称或代码。';
COMMENT ON COLUMN sec_company_registry.source_url IS
    'SEC 公司 ticker 映射文件来源地址。';
COMMENT ON COLUMN sec_company_registry.raw_payload IS
    'SEC 映射文件中该公司的原始 JSON 数据。';
COMMENT ON COLUMN sec_company_registry.first_seen_at IS
    '系统第一次发现该 SEC 公司映射的时间。';
COMMENT ON COLUMN sec_company_registry.last_refreshed_at IS
    '该 SEC 公司映射最近一次刷新时间。';
COMMENT ON COLUMN sec_company_registry.created_at IS
    '记录创建时间。';
COMMENT ON COLUMN sec_company_registry.updated_at IS
    '记录最近更新时间，由触发器自动维护。';

CREATE UNIQUE INDEX IF NOT EXISTS idx_sec_company_registry_cik_ticker
    ON sec_company_registry (cik, ticker);

CREATE INDEX IF NOT EXISTS idx_sec_company_registry_ticker
    ON sec_company_registry (ticker);

CREATE INDEX IF NOT EXISTS idx_sec_company_registry_cik
    ON sec_company_registry (cik);

CREATE TABLE IF NOT EXISTS sec_company_submissions (
    id BIGSERIAL PRIMARY KEY,
    cik TEXT NOT NULL,
    ticker TEXT,
    company_name TEXT,
    sic TEXT,
    sic_description TEXT,
    entity_type TEXT,
    fiscal_year_end TEXT,
    state_of_incorporation TEXT,
    source_url TEXT NOT NULL,
    raw_payload JSONB NOT NULL DEFAULT '{}'::jsonb,
    fetched_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),

    CONSTRAINT sec_company_submissions_cik_format
        CHECK (cik ~ '^[0-9]{10}$')
);

COMMENT ON TABLE sec_company_submissions IS
    'SEC submissions 原始快照表，用于保存 data.sec.gov/submissions 接口按 CIK 返回的公司公告列表原始数据。';
COMMENT ON COLUMN sec_company_submissions.id IS
    '内部自增主键。';
COMMENT ON COLUMN sec_company_submissions.cik IS
    'SEC Central Index Key，统一保存为 10 位补零字符串。';
COMMENT ON COLUMN sec_company_submissions.ticker IS
    '股票交易代码，通常来自股票池或 SEC 公司映射。';
COMMENT ON COLUMN sec_company_submissions.company_name IS
    'SEC submissions 返回的公司名称。';
COMMENT ON COLUMN sec_company_submissions.sic IS
    'SEC Standard Industrial Classification 行业分类代码。';
COMMENT ON COLUMN sec_company_submissions.sic_description IS
    'SEC SIC 行业分类描述。';
COMMENT ON COLUMN sec_company_submissions.entity_type IS
    'SEC 返回的实体类型，例如 operating、other。';
COMMENT ON COLUMN sec_company_submissions.fiscal_year_end IS
    '公司财年结束日期，SEC 通常以 MMDD 字符串表示。';
COMMENT ON COLUMN sec_company_submissions.state_of_incorporation IS
    '公司注册州或注册地代码。';
COMMENT ON COLUMN sec_company_submissions.source_url IS
    'SEC submissions API 请求地址。';
COMMENT ON COLUMN sec_company_submissions.raw_payload IS
    'SEC submissions API 返回的完整原始 JSON。';
COMMENT ON COLUMN sec_company_submissions.fetched_at IS
    '该 submissions 快照的抓取时间。';
COMMENT ON COLUMN sec_company_submissions.created_at IS
    '记录创建时间。';
COMMENT ON COLUMN sec_company_submissions.updated_at IS
    '记录最近更新时间，由触发器自动维护。';

CREATE UNIQUE INDEX IF NOT EXISTS idx_sec_company_submissions_cik
    ON sec_company_submissions (cik);

CREATE INDEX IF NOT EXISTS idx_sec_company_submissions_ticker
    ON sec_company_submissions (ticker);

CREATE TABLE IF NOT EXISTS sec_filings (
    id BIGSERIAL PRIMARY KEY,
    cik TEXT NOT NULL,
    ticker TEXT,
    company_name TEXT,
    accession_number TEXT NOT NULL,
    accession_number_no_dash TEXT NOT NULL,
    form_type TEXT NOT NULL,
    filing_date DATE NOT NULL,
    report_date DATE,
    acceptance_datetime TIMESTAMPTZ,
    act TEXT,
    file_number TEXT,
    film_number TEXT,
    items TEXT,
    file_size_bytes BIGINT,
    is_inline_xbrl BOOLEAN NOT NULL DEFAULT FALSE,
    is_xbrl BOOLEAN NOT NULL DEFAULT FALSE,
    is_amendment BOOLEAN NOT NULL DEFAULT FALSE,
    primary_document TEXT,
    primary_doc_description TEXT,
    filing_detail_url TEXT,
    primary_document_url TEXT,
    source_url TEXT NOT NULL,
    raw_payload JSONB NOT NULL DEFAULT '{}'::jsonb,
    fetched_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),

    CONSTRAINT sec_filings_cik_format
        CHECK (cik ~ '^[0-9]{10}$'),
    CONSTRAINT sec_filings_accession_number_not_blank
        CHECK (length(trim(accession_number)) > 0),
    CONSTRAINT sec_filings_accession_number_no_dash_not_blank
        CHECK (length(trim(accession_number_no_dash)) > 0),
    CONSTRAINT sec_filings_form_type_not_blank
        CHECK (length(trim(form_type)) > 0),
    CONSTRAINT sec_filings_file_size_non_negative
        CHECK (file_size_bytes IS NULL OR file_size_bytes >= 0)
);

COMMENT ON TABLE sec_filings IS
    'SEC 公告元数据表，用于保存 8-K、10-Q、10-K、S-1 等 EDGAR filing 的核心索引信息。';
COMMENT ON COLUMN sec_filings.id IS
    '内部自增主键。';
COMMENT ON COLUMN sec_filings.cik IS
    'SEC Central Index Key，统一保存为 10 位补零字符串。';
COMMENT ON COLUMN sec_filings.ticker IS
    '股票交易代码，来自股票池或 SEC 公司映射。';
COMMENT ON COLUMN sec_filings.company_name IS
    '公告主体公司名称。';
COMMENT ON COLUMN sec_filings.accession_number IS
    'SEC accession number，公告唯一编号，通常格式类似 0000320193-24-000123。';
COMMENT ON COLUMN sec_filings.accession_number_no_dash IS
    '去掉横线后的 accession number，用于拼接 SEC Archives 文档路径。';
COMMENT ON COLUMN sec_filings.form_type IS
    'SEC 表单类型，例如 8-K、10-Q、10-K、S-1。';
COMMENT ON COLUMN sec_filings.filing_date IS
    '公告提交日期。';
COMMENT ON COLUMN sec_filings.report_date IS
    '公告对应报告期日期，可能为空。';
COMMENT ON COLUMN sec_filings.acceptance_datetime IS
    'SEC 接收公告的时间戳。';
COMMENT ON COLUMN sec_filings.act IS
    'SEC submissions 返回的证券法案分类字段。';
COMMENT ON COLUMN sec_filings.file_number IS
    'SEC file number 文件编号。';
COMMENT ON COLUMN sec_filings.film_number IS
    'SEC film number 影像编号。';
COMMENT ON COLUMN sec_filings.items IS
    '8-K 等表单披露事项编号，保留 SEC 原始字符串。';
COMMENT ON COLUMN sec_filings.file_size_bytes IS
    'SEC 返回的公告文件大小，单位字节。';
COMMENT ON COLUMN sec_filings.is_inline_xbrl IS
    '该公告是否包含 inline XBRL。';
COMMENT ON COLUMN sec_filings.is_xbrl IS
    '该公告是否包含 XBRL 数据。';
COMMENT ON COLUMN sec_filings.is_amendment IS
    '该公告是否为修订公告，例如 10-K/A、8-K/A。';
COMMENT ON COLUMN sec_filings.primary_document IS
    'SEC 返回的主文档文件名。';
COMMENT ON COLUMN sec_filings.primary_doc_description IS
    'SEC 返回的主文档描述。';
COMMENT ON COLUMN sec_filings.filing_detail_url IS
    'SEC EDGAR 公告详情页地址。';
COMMENT ON COLUMN sec_filings.primary_document_url IS
    'SEC Archives 主文档下载地址。';
COMMENT ON COLUMN sec_filings.source_url IS
    '该 filing 元数据来源的 SEC API 地址。';
COMMENT ON COLUMN sec_filings.raw_payload IS
    '该 filing 在 submissions 列表中的原始 JSON 片段。';
COMMENT ON COLUMN sec_filings.fetched_at IS
    '该 filing 元数据的抓取时间。';
COMMENT ON COLUMN sec_filings.created_at IS
    '记录创建时间。';
COMMENT ON COLUMN sec_filings.updated_at IS
    '记录最近更新时间，由触发器自动维护。';

CREATE UNIQUE INDEX IF NOT EXISTS idx_sec_filings_accession_number
    ON sec_filings (accession_number);

CREATE INDEX IF NOT EXISTS idx_sec_filings_cik
    ON sec_filings (cik);

CREATE INDEX IF NOT EXISTS idx_sec_filings_ticker
    ON sec_filings (ticker);

CREATE INDEX IF NOT EXISTS idx_sec_filings_form_date
    ON sec_filings (form_type, filing_date DESC);

CREATE INDEX IF NOT EXISTS idx_sec_filings_ticker_form_date
    ON sec_filings (ticker, form_type, filing_date DESC);

CREATE INDEX IF NOT EXISTS idx_sec_filings_recent_core_forms
    ON sec_filings (filing_date DESC, form_type)
    WHERE form_type IN ('8-K', '10-Q', '10-K', 'S-1', '20-F', '6-K');

CREATE TABLE IF NOT EXISTS sec_filing_documents (
    id BIGSERIAL PRIMARY KEY,
    cik TEXT NOT NULL,
    ticker TEXT,
    accession_number TEXT NOT NULL,
    accession_number_no_dash TEXT NOT NULL,
    document_sequence INTEGER,
    document_name TEXT NOT NULL,
    document_type TEXT,
    document_description TEXT,
    document_url TEXT NOT NULL,
    content_type TEXT,
    file_size_bytes BIGINT,
    local_raw_path TEXT,
    is_primary BOOLEAN NOT NULL DEFAULT FALSE,
    is_xbrl BOOLEAN NOT NULL DEFAULT FALSE,
    raw_payload JSONB NOT NULL DEFAULT '{}'::jsonb,
    fetched_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),

    CONSTRAINT sec_filing_documents_cik_format
        CHECK (cik ~ '^[0-9]{10}$'),
    CONSTRAINT sec_filing_documents_accession_number_not_blank
        CHECK (length(trim(accession_number)) > 0),
    CONSTRAINT sec_filing_documents_accession_number_no_dash_not_blank
        CHECK (length(trim(accession_number_no_dash)) > 0),
    CONSTRAINT sec_filing_documents_document_name_not_blank
        CHECK (length(trim(document_name)) > 0),
    CONSTRAINT sec_filing_documents_document_url_not_blank
        CHECK (length(trim(document_url)) > 0),
    CONSTRAINT sec_filing_documents_sequence_positive
        CHECK (document_sequence IS NULL OR document_sequence > 0),
    CONSTRAINT sec_filing_documents_file_size_non_negative
        CHECK (file_size_bytes IS NULL OR file_size_bytes >= 0)
);

COMMENT ON TABLE sec_filing_documents IS
    'SEC 公告文档表，用于保存 filing 下主文档、XBRL 文件和附件文档的索引信息。';
COMMENT ON COLUMN sec_filing_documents.id IS
    '内部自增主键。';
COMMENT ON COLUMN sec_filing_documents.cik IS
    'SEC Central Index Key，统一保存为 10 位补零字符串。';
COMMENT ON COLUMN sec_filing_documents.ticker IS
    '股票交易代码，来自股票池或 SEC 公司映射。';
COMMENT ON COLUMN sec_filing_documents.accession_number IS
    'SEC accession number，公告唯一编号。';
COMMENT ON COLUMN sec_filing_documents.accession_number_no_dash IS
    '去掉横线后的 accession number，用于拼接 SEC Archives 文档路径。';
COMMENT ON COLUMN sec_filing_documents.document_sequence IS
    'SEC 文档序号。';
COMMENT ON COLUMN sec_filing_documents.document_name IS
    'SEC 文档文件名。';
COMMENT ON COLUMN sec_filing_documents.document_type IS
    'SEC 文档类型，例如 10-K、EX-99.1、XML。';
COMMENT ON COLUMN sec_filing_documents.document_description IS
    'SEC 文档描述。';
COMMENT ON COLUMN sec_filing_documents.document_url IS
    'SEC Archives 文档下载地址。';
COMMENT ON COLUMN sec_filing_documents.content_type IS
    '下载后识别到的内容类型，例如 text/html、application/xml。';
COMMENT ON COLUMN sec_filing_documents.file_size_bytes IS
    '文档大小，单位字节。';
COMMENT ON COLUMN sec_filing_documents.local_raw_path IS
    '原始文档下载到本地后的相对路径或对象存储路径。';
COMMENT ON COLUMN sec_filing_documents.is_primary IS
    '该文档是否为 filing 主文档。';
COMMENT ON COLUMN sec_filing_documents.is_xbrl IS
    '该文档是否为 XBRL 或 inline XBRL 相关文件。';
COMMENT ON COLUMN sec_filing_documents.raw_payload IS
    '该文档在 SEC 文档索引中的原始 JSON 或解析片段。';
COMMENT ON COLUMN sec_filing_documents.fetched_at IS
    '该文档索引信息的抓取时间。';
COMMENT ON COLUMN sec_filing_documents.created_at IS
    '记录创建时间。';
COMMENT ON COLUMN sec_filing_documents.updated_at IS
    '记录最近更新时间，由触发器自动维护。';

CREATE UNIQUE INDEX IF NOT EXISTS idx_sec_filing_documents_accession_document
    ON sec_filing_documents (accession_number, document_name);

CREATE INDEX IF NOT EXISTS idx_sec_filing_documents_cik
    ON sec_filing_documents (cik);

CREATE INDEX IF NOT EXISTS idx_sec_filing_documents_ticker
    ON sec_filing_documents (ticker);

CREATE INDEX IF NOT EXISTS idx_sec_filing_documents_type
    ON sec_filing_documents (document_type);

CREATE TABLE IF NOT EXISTS sec_company_facts (
    id BIGSERIAL PRIMARY KEY,
    cik TEXT NOT NULL,
    ticker TEXT,
    company_name TEXT,
    entity_name TEXT,
    source_url TEXT NOT NULL,
    raw_payload JSONB NOT NULL DEFAULT '{}'::jsonb,
    fetched_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),

    CONSTRAINT sec_company_facts_cik_format
        CHECK (cik ~ '^[0-9]{10}$')
);

COMMENT ON TABLE sec_company_facts IS
    'SEC company facts 原始快照表，用于保存 data.sec.gov/api/xbrl/companyfacts 按 CIK 返回的完整 JSON。';
COMMENT ON COLUMN sec_company_facts.id IS
    '内部自增主键。';
COMMENT ON COLUMN sec_company_facts.cik IS
    'SEC Central Index Key，统一保存为 10 位补零字符串。';
COMMENT ON COLUMN sec_company_facts.ticker IS
    '股票交易代码，来自股票池或 SEC 公司映射。';
COMMENT ON COLUMN sec_company_facts.company_name IS
    '公司名称，通常来自股票池或 SEC 映射。';
COMMENT ON COLUMN sec_company_facts.entity_name IS
    'SEC company facts 返回的实体名称。';
COMMENT ON COLUMN sec_company_facts.source_url IS
    'SEC company facts API 请求地址。';
COMMENT ON COLUMN sec_company_facts.raw_payload IS
    'SEC company facts API 返回的完整原始 JSON。';
COMMENT ON COLUMN sec_company_facts.fetched_at IS
    '该 company facts 快照的抓取时间。';
COMMENT ON COLUMN sec_company_facts.created_at IS
    '记录创建时间。';
COMMENT ON COLUMN sec_company_facts.updated_at IS
    '记录最近更新时间，由触发器自动维护。';

CREATE UNIQUE INDEX IF NOT EXISTS idx_sec_company_facts_cik
    ON sec_company_facts (cik);

CREATE INDEX IF NOT EXISTS idx_sec_company_facts_ticker
    ON sec_company_facts (ticker);

CREATE TABLE IF NOT EXISTS sec_financial_facts (
    id BIGSERIAL PRIMARY KEY,
    fact_uid TEXT NOT NULL,
    cik TEXT NOT NULL,
    ticker TEXT,
    accession_number TEXT,
    taxonomy TEXT NOT NULL,
    concept TEXT NOT NULL,
    label TEXT,
    description TEXT,
    unit TEXT NOT NULL,
    fiscal_year INTEGER,
    fiscal_period TEXT,
    form_type TEXT,
    filed_date DATE,
    start_date DATE,
    end_date DATE,
    frame TEXT,
    value_numeric NUMERIC(38, 8),
    value_text TEXT,
    raw_payload JSONB NOT NULL DEFAULT '{}'::jsonb,
    extracted_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),

    CONSTRAINT sec_financial_facts_fact_uid_not_blank
        CHECK (length(trim(fact_uid)) > 0),
    CONSTRAINT sec_financial_facts_cik_format
        CHECK (cik ~ '^[0-9]{10}$'),
    CONSTRAINT sec_financial_facts_taxonomy_not_blank
        CHECK (length(trim(taxonomy)) > 0),
    CONSTRAINT sec_financial_facts_concept_not_blank
        CHECK (length(trim(concept)) > 0),
    CONSTRAINT sec_financial_facts_unit_not_blank
        CHECK (length(trim(unit)) > 0)
);

COMMENT ON TABLE sec_financial_facts IS
    'SEC 标准化财务事实表，用于从 company facts 原始 JSON 中抽取营收、利润、现金流、资产负债等 XBRL fact 行。';
COMMENT ON COLUMN sec_financial_facts.id IS
    '内部自增主键。';
COMMENT ON COLUMN sec_financial_facts.fact_uid IS
    '财务事实去重键，由应用层根据 CIK、taxonomy、concept、unit、accession、期间和 frame 等字段生成。';
COMMENT ON COLUMN sec_financial_facts.cik IS
    'SEC Central Index Key，统一保存为 10 位补零字符串。';
COMMENT ON COLUMN sec_financial_facts.ticker IS
    '股票交易代码，来自股票池或 SEC 公司映射。';
COMMENT ON COLUMN sec_financial_facts.accession_number IS
    '该财务事实对应的 SEC accession number，可能为空。';
COMMENT ON COLUMN sec_financial_facts.taxonomy IS
    'XBRL 分类体系，例如 us-gaap、dei。';
COMMENT ON COLUMN sec_financial_facts.concept IS
    'XBRL 概念名称，例如 Revenues、NetIncomeLoss。';
COMMENT ON COLUMN sec_financial_facts.label IS
    'XBRL 概念展示名称。';
COMMENT ON COLUMN sec_financial_facts.description IS
    'XBRL 概念说明。';
COMMENT ON COLUMN sec_financial_facts.unit IS
    '财务事实单位，例如 USD、shares、USD/shares。';
COMMENT ON COLUMN sec_financial_facts.fiscal_year IS
    '公司财年。';
COMMENT ON COLUMN sec_financial_facts.fiscal_period IS
    '公司财报期间，例如 FY、Q1、Q2、Q3、Q4。';
COMMENT ON COLUMN sec_financial_facts.form_type IS
    '该财务事实来自的 SEC 表单类型，例如 10-K、10-Q。';
COMMENT ON COLUMN sec_financial_facts.filed_date IS
    '该财务事实对应公告的提交日期。';
COMMENT ON COLUMN sec_financial_facts.start_date IS
    '该财务事实覆盖期间的开始日期。';
COMMENT ON COLUMN sec_financial_facts.end_date IS
    '该财务事实覆盖期间的结束日期。';
COMMENT ON COLUMN sec_financial_facts.frame IS
    'SEC company facts 中的 frame 字段，用于标识日历期间或报告框架。';
COMMENT ON COLUMN sec_financial_facts.value_numeric IS
    '财务事实数值，适用于金额、股数、比例等可数值化字段。';
COMMENT ON COLUMN sec_financial_facts.value_text IS
    '财务事实文本值，适用于无法数值化或需要保留原文的字段。';
COMMENT ON COLUMN sec_financial_facts.raw_payload IS
    '该财务事实在 company facts 中的原始 JSON 片段。';
COMMENT ON COLUMN sec_financial_facts.extracted_at IS
    '该财务事实从原始 JSON 抽取入库的时间。';
COMMENT ON COLUMN sec_financial_facts.created_at IS
    '记录创建时间。';
COMMENT ON COLUMN sec_financial_facts.updated_at IS
    '记录最近更新时间，由触发器自动维护。';

CREATE UNIQUE INDEX IF NOT EXISTS idx_sec_financial_facts_fact_uid
    ON sec_financial_facts (fact_uid);

CREATE INDEX IF NOT EXISTS idx_sec_financial_facts_cik_concept
    ON sec_financial_facts (cik, taxonomy, concept);

CREATE INDEX IF NOT EXISTS idx_sec_financial_facts_ticker_concept_end
    ON sec_financial_facts (ticker, taxonomy, concept, end_date DESC);

CREATE INDEX IF NOT EXISTS idx_sec_financial_facts_accession
    ON sec_financial_facts (accession_number)
    WHERE accession_number IS NOT NULL;

CREATE OR REPLACE FUNCTION set_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = now();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_sec_company_registry_updated_at ON sec_company_registry;
CREATE TRIGGER trg_sec_company_registry_updated_at
BEFORE UPDATE ON sec_company_registry
FOR EACH ROW
EXECUTE FUNCTION set_updated_at();

DROP TRIGGER IF EXISTS trg_sec_company_submissions_updated_at ON sec_company_submissions;
CREATE TRIGGER trg_sec_company_submissions_updated_at
BEFORE UPDATE ON sec_company_submissions
FOR EACH ROW
EXECUTE FUNCTION set_updated_at();

DROP TRIGGER IF EXISTS trg_sec_filings_updated_at ON sec_filings;
CREATE TRIGGER trg_sec_filings_updated_at
BEFORE UPDATE ON sec_filings
FOR EACH ROW
EXECUTE FUNCTION set_updated_at();

DROP TRIGGER IF EXISTS trg_sec_filing_documents_updated_at ON sec_filing_documents;
CREATE TRIGGER trg_sec_filing_documents_updated_at
BEFORE UPDATE ON sec_filing_documents
FOR EACH ROW
EXECUTE FUNCTION set_updated_at();

DROP TRIGGER IF EXISTS trg_sec_company_facts_updated_at ON sec_company_facts;
CREATE TRIGGER trg_sec_company_facts_updated_at
BEFORE UPDATE ON sec_company_facts
FOR EACH ROW
EXECUTE FUNCTION set_updated_at();

DROP TRIGGER IF EXISTS trg_sec_financial_facts_updated_at ON sec_financial_facts;
CREATE TRIGGER trg_sec_financial_facts_updated_at
BEFORE UPDATE ON sec_financial_facts
FOR EACH ROW
EXECUTE FUNCTION set_updated_at();

-- migrate:down

DROP TRIGGER IF EXISTS trg_sec_financial_facts_updated_at ON sec_financial_facts;
DROP TRIGGER IF EXISTS trg_sec_company_facts_updated_at ON sec_company_facts;
DROP TRIGGER IF EXISTS trg_sec_filing_documents_updated_at ON sec_filing_documents;
DROP TRIGGER IF EXISTS trg_sec_filings_updated_at ON sec_filings;
DROP TRIGGER IF EXISTS trg_sec_company_submissions_updated_at ON sec_company_submissions;
DROP TRIGGER IF EXISTS trg_sec_company_registry_updated_at ON sec_company_registry;

DROP INDEX IF EXISTS idx_sec_financial_facts_accession;
DROP INDEX IF EXISTS idx_sec_financial_facts_ticker_concept_end;
DROP INDEX IF EXISTS idx_sec_financial_facts_cik_concept;
DROP INDEX IF EXISTS idx_sec_financial_facts_fact_uid;
DROP INDEX IF EXISTS idx_sec_company_facts_ticker;
DROP INDEX IF EXISTS idx_sec_company_facts_cik;
DROP INDEX IF EXISTS idx_sec_filing_documents_type;
DROP INDEX IF EXISTS idx_sec_filing_documents_ticker;
DROP INDEX IF EXISTS idx_sec_filing_documents_cik;
DROP INDEX IF EXISTS idx_sec_filing_documents_accession_document;
DROP INDEX IF EXISTS idx_sec_filings_recent_core_forms;
DROP INDEX IF EXISTS idx_sec_filings_ticker_form_date;
DROP INDEX IF EXISTS idx_sec_filings_form_date;
DROP INDEX IF EXISTS idx_sec_filings_ticker;
DROP INDEX IF EXISTS idx_sec_filings_cik;
DROP INDEX IF EXISTS idx_sec_filings_accession_number;
DROP INDEX IF EXISTS idx_sec_company_submissions_ticker;
DROP INDEX IF EXISTS idx_sec_company_submissions_cik;
DROP INDEX IF EXISTS idx_sec_company_registry_cik;
DROP INDEX IF EXISTS idx_sec_company_registry_ticker;
DROP INDEX IF EXISTS idx_sec_company_registry_cik_ticker;

DROP TABLE IF EXISTS sec_financial_facts;
DROP TABLE IF EXISTS sec_company_facts;
DROP TABLE IF EXISTS sec_filing_documents;
DROP TABLE IF EXISTS sec_filings;
DROP TABLE IF EXISTS sec_company_submissions;
DROP TABLE IF EXISTS sec_company_registry;
