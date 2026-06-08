"""Command line entry points."""

from __future__ import annotations

from usstock.db.migrations import main as migrations_main


def main() -> int:
    return migrations_main()
