"""PostgreSQL migration runner."""

from __future__ import annotations

import argparse
import hashlib
import re
import sys
import time
from dataclasses import dataclass
from pathlib import Path

import psycopg
from psycopg import Connection

from usstock.config.settings import get_settings


UP_MARKER = "-- migrate:up"
DOWN_MARKER = "-- migrate:down"


class MigrationError(RuntimeError):
    """Raised when migration discovery or execution fails."""


@dataclass(frozen=True)
class Migration:
    version: str
    name: str
    path: Path
    up_sql: str
    down_sql: str | None
    checksum_sha256: str


@dataclass(frozen=True)
class AppliedMigration:
    version: str
    name: str
    execution_time_ms: int


@dataclass(frozen=True)
class MigrationStatus:
    version: str
    name: str
    applied: bool
    checksum_sha256: str
    applied_checksum_sha256: str | None
    path: Path


SCHEMA_MIGRATIONS_SQL = """
CREATE TABLE IF NOT EXISTS schema_migrations (
    version TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    checksum_sha256 TEXT NOT NULL,
    applied_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    execution_time_ms INTEGER,

    CONSTRAINT schema_migrations_version_not_blank
        CHECK (length(trim(version)) > 0),
    CONSTRAINT schema_migrations_name_not_blank
        CHECK (length(trim(name)) > 0),
    CONSTRAINT schema_migrations_checksum_not_blank
        CHECK (length(trim(checksum_sha256)) > 0),
    CONSTRAINT schema_migrations_execution_time_non_negative
        CHECK (execution_time_ms IS NULL OR execution_time_ms >= 0)
);

COMMENT ON TABLE schema_migrations IS
    '数据库迁移记录表，用于记录已经执行过的迁移文件版本、名称、校验和和执行时间。';
COMMENT ON COLUMN schema_migrations.version IS
    '迁移版本号，通常来自迁移文件名前缀，例如 001、002。';
COMMENT ON COLUMN schema_migrations.name IS
    '迁移名称，通常来自迁移文件名。';
COMMENT ON COLUMN schema_migrations.checksum_sha256 IS
    '迁移 up SQL 内容的 SHA-256 校验和，用于发现已执行迁移被修改的情况。';
COMMENT ON COLUMN schema_migrations.applied_at IS
    '迁移执行完成并记录入库的时间。';
COMMENT ON COLUMN schema_migrations.execution_time_ms IS
    '迁移执行耗时，单位毫秒。';
"""


def extract_migration_sections(sql_text: str, path: Path) -> tuple[str, str | None]:
    up_index = sql_text.find(UP_MARKER)
    if up_index < 0:
        raise MigrationError(f"迁移文件缺少 {UP_MARKER}: {path}")

    down_index = sql_text.find(DOWN_MARKER, up_index + len(UP_MARKER))
    if down_index < 0:
        up_sql = sql_text[up_index + len(UP_MARKER) :].strip()
        down_sql = None
    else:
        up_sql = sql_text[up_index + len(UP_MARKER) : down_index].strip()
        down_sql = sql_text[down_index + len(DOWN_MARKER) :].strip() or None

    if not up_sql:
        raise MigrationError(f"迁移文件 up 段为空: {path}")

    return up_sql, down_sql


def load_migration(path: Path) -> Migration:
    match = re.match(r"^([0-9]+)_(.+)\.sql$", path.name)
    if not match:
        raise MigrationError(
            f"迁移文件名必须使用数字前缀，例如 001_create_table.sql: {path.name}"
        )

    version = match.group(1)
    name = path.stem
    sql_text = path.read_text(encoding="utf-8")
    up_sql, down_sql = extract_migration_sections(sql_text, path)
    checksum = hashlib.sha256(up_sql.encode("utf-8")).hexdigest()

    return Migration(
        version=version,
        name=name,
        path=path,
        up_sql=up_sql,
        down_sql=down_sql,
        checksum_sha256=checksum,
    )


def discover_migrations(migrations_dir: Path) -> list[Migration]:
    if not migrations_dir.exists():
        raise MigrationError(f"迁移目录不存在: {migrations_dir}")
    if not migrations_dir.is_dir():
        raise MigrationError(f"迁移路径不是目录: {migrations_dir}")

    migrations = [load_migration(path) for path in sorted(migrations_dir.glob("*.sql"))]
    seen_versions: set[str] = set()
    duplicate_versions: set[str] = set()

    for migration in migrations:
        if migration.version in seen_versions:
            duplicate_versions.add(migration.version)
        seen_versions.add(migration.version)

    if duplicate_versions:
        versions = ", ".join(sorted(duplicate_versions))
        raise MigrationError(f"发现重复迁移版本号: {versions}")

    return migrations


def ensure_schema_migrations(conn: Connection) -> None:
    conn.execute(SCHEMA_MIGRATIONS_SQL)


def fetch_applied_migrations(conn: Connection) -> dict[str, tuple[str, str]]:
    rows = conn.execute(
        """
        SELECT version, name, checksum_sha256
        FROM schema_migrations
        ORDER BY version
        """
    ).fetchall()
    return {version: (name, checksum) for version, name, checksum in rows}


def check_applied_checksum(
    migration: Migration,
    applied: dict[str, tuple[str, str]],
) -> bool:
    if migration.version not in applied:
        return False

    _name, applied_checksum = applied[migration.version]
    if applied_checksum != migration.checksum_sha256:
        raise MigrationError(
            "已执行迁移的校验和发生变化: "
            f"{migration.path.name}。数据库记录={applied_checksum}，"
            f"当前文件={migration.checksum_sha256}"
        )

    return True


def apply_migration(conn: Connection, migration: Migration) -> AppliedMigration:
    started_at = time.monotonic()

    with conn.transaction():
        conn.execute(migration.up_sql)
        execution_time_ms = int((time.monotonic() - started_at) * 1000)
        conn.execute(
            """
            INSERT INTO schema_migrations (
                version,
                name,
                checksum_sha256,
                execution_time_ms
            )
            VALUES (%s, %s, %s, %s)
            """,
            (
                migration.version,
                migration.name,
                migration.checksum_sha256,
                execution_time_ms,
            ),
        )

    return AppliedMigration(
        version=migration.version,
        name=migration.name,
        execution_time_ms=execution_time_ms,
    )


def migrate(
    database_url: str | None = None,
    migrations_dir: Path | None = None,
    *,
    dry_run: bool = False,
) -> list[AppliedMigration]:
    settings = get_settings()
    database_url = database_url or settings.database_url
    migrations_dir = migrations_dir or settings.migrations_dir

    if not database_url:
        raise MigrationError("缺少 DATABASE_URL，请在环境变量或 .env 中配置数据库连接。")

    migrations = discover_migrations(migrations_dir)
    if dry_run:
        return [
            AppliedMigration(
                version=migration.version,
                name=migration.name,
                execution_time_ms=0,
            )
            for migration in migrations
        ]

    applied_results: list[AppliedMigration] = []
    with psycopg.connect(database_url, autocommit=True) as conn:
        ensure_schema_migrations(conn)
        applied = fetch_applied_migrations(conn)

        for migration in migrations:
            if check_applied_checksum(migration, applied):
                continue

            result = apply_migration(conn, migration)
            applied_results.append(result)
            applied[migration.version] = (
                migration.name,
                migration.checksum_sha256,
            )

    return applied_results


def get_migration_status(
    database_url: str | None = None,
    migrations_dir: Path | None = None,
) -> list[MigrationStatus]:
    settings = get_settings()
    database_url = database_url or settings.database_url
    migrations_dir = migrations_dir or settings.migrations_dir

    if not database_url:
        raise MigrationError("缺少 DATABASE_URL，请在环境变量或 .env 中配置数据库连接。")

    migrations = discover_migrations(migrations_dir)
    with psycopg.connect(database_url, autocommit=True) as conn:
        ensure_schema_migrations(conn)
        applied = fetch_applied_migrations(conn)

    statuses: list[MigrationStatus] = []
    for migration in migrations:
        applied_record = applied.get(migration.version)
        statuses.append(
            MigrationStatus(
                version=migration.version,
                name=migration.name,
                applied=applied_record is not None,
                checksum_sha256=migration.checksum_sha256,
                applied_checksum_sha256=applied_record[1] if applied_record else None,
                path=migration.path,
            )
        )

    return statuses


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run usstock database migrations.")
    subparsers = parser.add_subparsers(dest="command")

    migrate_parser = subparsers.add_parser("migrate", help="执行尚未执行的迁移")
    migrate_parser.add_argument("--database-url", help="PostgreSQL DATABASE_URL")
    migrate_parser.add_argument("--migrations-dir", type=Path, help="迁移文件目录")
    migrate_parser.add_argument("--dry-run", action="store_true", help="只列出迁移，不执行")

    status_parser = subparsers.add_parser("status", help="查看迁移状态")
    status_parser.add_argument("--database-url", help="PostgreSQL DATABASE_URL")
    status_parser.add_argument("--migrations-dir", type=Path, help="迁移文件目录")

    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    command = args.command or "migrate"

    try:
        if command == "migrate":
            database_url = getattr(args, "database_url", None)
            migrations_dir = getattr(args, "migrations_dir", None)
            dry_run = getattr(args, "dry_run", False)
            results = migrate(
                database_url=database_url,
                migrations_dir=migrations_dir,
                dry_run=dry_run,
            )
            if dry_run:
                for result in results:
                    print(f"[计划] {result.version} {result.name}")
                return 0

            if not results:
                print("数据库迁移已是最新，无需执行。")
                return 0

            for result in results:
                print(
                    f"[完成] {result.version} {result.name} "
                    f"({result.execution_time_ms} ms)"
                )
            return 0

        if command == "status":
            statuses = get_migration_status(
                database_url=args.database_url,
                migrations_dir=args.migrations_dir,
            )
            for status in statuses:
                marker = "已执行" if status.applied else "未执行"
                print(f"[{marker}] {status.version} {status.name}")
            return 0

        parser.print_help()
        return 2
    except MigrationError as exc:
        print(f"迁移失败: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
