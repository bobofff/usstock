-- Migration: 010_create_daily_analysis_reports
-- Purpose: persist generated daily analysis reports and their structured payloads.
--
-- Dialect target: PostgreSQL.
--
-- Note:
--   This migration intentionally does not add foreign key constraints. Reports
--   are linked to watchlists and candidate scores by run_date, profile, ticker,
--   and source_watchlist_uid through application logic.

-- migrate:up

CREATE TABLE IF NOT EXISTS daily_analysis_reports (
    id BIGSERIAL PRIMARY KEY,
    report_uid TEXT NOT NULL,
    run_date DATE NOT NULL,
    profile TEXT NOT NULL DEFAULT 'default',
    report_type TEXT NOT NULL DEFAULT 'daily_analysis',
    candidate_count INTEGER NOT NULL DEFAULT 0,
    llm_provider TEXT,
    llm_model TEXT,
    llm_used BOOLEAN NOT NULL DEFAULT FALSE,
    status TEXT NOT NULL DEFAULT 'generated',
    summary TEXT,
    markdown_body TEXT NOT NULL,
    structured_payload JSONB NOT NULL DEFAULT '{}'::jsonb,
    source_watchlist_uid TEXT,
    generated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),

    CONSTRAINT daily_analysis_reports_report_uid_not_blank
        CHECK (length(trim(report_uid)) > 0),
    CONSTRAINT daily_analysis_reports_profile_not_blank
        CHECK (length(trim(profile)) > 0),
    CONSTRAINT daily_analysis_reports_report_type_not_blank
        CHECK (length(trim(report_type)) > 0),
    CONSTRAINT daily_analysis_reports_candidate_count_non_negative
        CHECK (candidate_count >= 0),
    CONSTRAINT daily_analysis_reports_status_known
        CHECK (status IN ('generated', 'failed', 'archived')),
    CONSTRAINT daily_analysis_reports_markdown_body_not_blank
        CHECK (length(trim(markdown_body)) > 0)
);

COMMENT ON TABLE daily_analysis_reports IS
    '每日分析报告表，用于保存新闻驱动股票候选池的 Markdown 报告、结构化结论、LLM 使用信息和来源观察清单。';
COMMENT ON COLUMN daily_analysis_reports.id IS
    '内部自增主键。';
COMMENT ON COLUMN daily_analysis_reports.report_uid IS
    '报告去重键，通常由报告类型、配置 profile 和运行日期组成。';
COMMENT ON COLUMN daily_analysis_reports.run_date IS
    '报告所属日期。';
COMMENT ON COLUMN daily_analysis_reports.profile IS
    '报告生成配置名称，例如 default、aggressive、conservative。';
COMMENT ON COLUMN daily_analysis_reports.report_type IS
    '报告类型，例如 daily_analysis。';
COMMENT ON COLUMN daily_analysis_reports.candidate_count IS
    '报告中包含的候选股票数量。';
COMMENT ON COLUMN daily_analysis_reports.llm_provider IS
    '生成或增强报告时使用的 LLM 提供方，例如 openai_compatible；未使用时为空。';
COMMENT ON COLUMN daily_analysis_reports.llm_model IS
    '生成或增强报告时使用的 LLM 模型名称；未使用时为空。';
COMMENT ON COLUMN daily_analysis_reports.llm_used IS
    '是否实际调用了 LLM 增强报告内容。';
COMMENT ON COLUMN daily_analysis_reports.status IS
    '报告状态：generated 已生成、failed 生成失败、archived 已归档。';
COMMENT ON COLUMN daily_analysis_reports.summary IS
    '报告摘要，用于列表页和快速浏览。';
COMMENT ON COLUMN daily_analysis_reports.markdown_body IS
    '完整 Markdown 报告正文。';
COMMENT ON COLUMN daily_analysis_reports.structured_payload IS
    '报告结构化 JSON，包括核心事件、候选股票、风险提示、来源证据和运行参数。';
COMMENT ON COLUMN daily_analysis_reports.source_watchlist_uid IS
    '生成报告时使用的观察清单去重键，由应用层维护关联。';
COMMENT ON COLUMN daily_analysis_reports.generated_at IS
    '报告生成时间。';
COMMENT ON COLUMN daily_analysis_reports.created_at IS
    '记录创建时间。';
COMMENT ON COLUMN daily_analysis_reports.updated_at IS
    '记录最近更新时间，由触发器自动维护。';

CREATE UNIQUE INDEX IF NOT EXISTS idx_daily_analysis_reports_report_uid
    ON daily_analysis_reports (report_uid);

CREATE INDEX IF NOT EXISTS idx_daily_analysis_reports_run_date
    ON daily_analysis_reports (run_date DESC, profile);

CREATE INDEX IF NOT EXISTS idx_daily_analysis_reports_llm_used
    ON daily_analysis_reports (llm_used, generated_at DESC);

CREATE OR REPLACE FUNCTION set_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = now();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_daily_analysis_reports_updated_at ON daily_analysis_reports;
CREATE TRIGGER trg_daily_analysis_reports_updated_at
BEFORE UPDATE ON daily_analysis_reports
FOR EACH ROW
EXECUTE FUNCTION set_updated_at();

-- migrate:down

DROP TRIGGER IF EXISTS trg_daily_analysis_reports_updated_at ON daily_analysis_reports;

DROP INDEX IF EXISTS idx_daily_analysis_reports_llm_used;
DROP INDEX IF EXISTS idx_daily_analysis_reports_run_date;
DROP INDEX IF EXISTS idx_daily_analysis_reports_report_uid;

DROP TABLE IF EXISTS daily_analysis_reports;
